"""
vod2strm – Dispatcharr Plugin
Version: 0.0.8

Spec:
- ORM (in-process) with Celery background tasks (non-blocking UI).
- Buttons: Stats, Generate Movies, Generate Series, Generate All.
- STRM generation:
  * Movies -> <root>/Movies/{Name} ({Year})/{Name} ({Year}).strm
  * Series -> <root>/TV/{SeriesName (Year) or SeriesName + (year)}/Season {SS or 00}/S{SS}E{EE} - {Title}.strm
  * Season 00 labeled "Season 00 (Specials)".
  * .strm contents use {base_url}/proxy/vod/(movie|episode)/{uuid}?stream_id={stream_id}
- NFO generation (compare-before-write):
  * Movies: movie.nfo in movie folder
  * Seasons: season.nfo per season folder
  * Episodes: SxxExx.nfo next to episode file
- Cleanup (preview/apply) of stale files/folders.
- CSV reports -> /data/plugins/vod2strm/reports/
- Robust debug logging -> /data/plugins/vod2strm/logs/
"""

from __future__ import annotations

# Ensure plugin directory is in sys.path so Celery workers can import this module
import sys
from pathlib import Path
_plugin_parent = Path(__file__).parent.parent
if str(_plugin_parent) not in sys.path:
    sys.path.insert(0, str(_plugin_parent))

import csv
import hashlib
import io
import json
import logging
import logging.handlers
import math
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Tuple, Dict, Any

from django.db import connection, transaction  # noqa:F401
from django.db.models import Count, Exists, OuterRef, Prefetch, Q
from django.utils.timezone import now  # noqa:F401

# ORM models (plugin runs in-process with the app)
try:
    from apps.vod.models import (
        Movie,
        Series,
        Episode,
        M3UMovieRelation,
        M3UEpisodeRelation,
        M3USeriesRelation,
        M3UVODCategoryRelation,
    )
except Exception:  # pragma: no cover
    from vod.models import (  # type: ignore
        Movie,
        Series,
        Episode,
        M3UMovieRelation,
        M3UEpisodeRelation,
        M3USeriesRelation,
        M3UVODCategoryRelation,
    )

# Celery (required - Dispatcharr depends on Celery to function)
# Import is in try/except for testing purposes only
try:
    from celery import current_app as celery_app
    from celery import shared_task
except Exception:  # pragma: no cover
    celery_app = None  # type: ignore
    shared_task = None  # type: ignore

# -------------------- Constants / Defaults --------------------

DEFAULT_BASE_URL = "http://localhost:9191"
DEFAULT_ROOT = "/data/STRM"

REPORT_ROOT = "/data/plugins/vod2strm/reports"
LOG_ROOT = "/data/plugins/vod2strm/logs"

CLEANUP_OFF = "off"
CLEANUP_PREVIEW = "preview"
CLEANUP_APPLY = "apply"

# -------------------- Logging --------------------

LOGGER = logging.getLogger("plugins.vod2strm")
if not LOGGER.handlers:
    LOGGER.setLevel(logging.INFO)
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
    LOGGER.addHandler(sh)

_FILE_LOGGER_CONFIGURED = False
_LOG_LOCK = threading.Lock()
_MANIFEST_LOCK = threading.Lock()  # Protects manifest dict from concurrent modification


# -------------------- Query Helpers --------------------

def _enabled_category_subquery(account_field: str, category_field: str) -> Exists:
    """
    Build an Exists() subquery that ensures a given account/category pair is enabled.
    account_field/category_field refer to columns available on the outer queryset.
    """
    return Exists(
        M3UVODCategoryRelation.objects.filter(
            m3u_account_id=OuterRef(account_field),
            category_id=OuterRef(category_field),
            enabled=True,
        )
    )


def _eligible_movie_queryset():
    """
    Movies that have at least one active account relation and belong to an enabled group.
    """
    allowed_relations = M3UMovieRelation.objects.filter(
        movie_id=OuterRef("pk"),
        m3u_account__is_active=True,
    ).filter(
        Q(category__isnull=True) | _enabled_category_subquery("m3u_account_id", "category_id")
    )
    return Movie.objects.annotate(_vod2_allowed_movie=Exists(allowed_relations)).filter(_vod2_allowed_movie=True)


def _eligible_series_queryset():
    """
    Series that have at least one active account relation and belong to an enabled group.
    """
    allowed_relations = M3USeriesRelation.objects.filter(
        series_id=OuterRef("pk"),
        m3u_account__is_active=True,
    ).filter(
        Q(category__isnull=True) | _enabled_category_subquery("m3u_account_id", "category_id")
    )
    return Series.objects.annotate(_vod2_allowed_series=Exists(allowed_relations)).filter(_vod2_allowed_series=True)


def _get_movie_stream_id(movie: Movie) -> str | None:
    """
    Get stream_id from the highest priority active M3U provider for a movie.

    Returns stream_id or None if no active provider found.
    """
    try:
        # Get highest priority active relation
        relation = M3UMovieRelation.objects.filter(
            movie_id=movie.id,
            m3u_account__is_active=True,
        ).filter(
            Q(category__isnull=True) | _enabled_category_subquery("m3u_account_id", "category_id")
        ).select_related('m3u_account').order_by(
            '-m3u_account__priority', 'id'
        ).first()

        if relation:
            return getattr(relation, 'stream_id', None)
        return None
    except Exception as e:
        LOGGER.debug("Failed to get stream_id for movie id=%s: %s", movie.id, e)
        return None


def _get_episode_stream_id(episode: Episode) -> str | None:
    """
    Get stream_id from the highest priority active M3U provider for an episode.
    Filters by enabled categories to respect user's category preferences.

    Returns stream_id or None if no active provider found.
    """
    try:
        # Get highest priority active relation, filtering by enabled categories
        # Same logic as _get_movie_stream_id to ensure consistency
        relation = M3UEpisodeRelation.objects.filter(
            episode_id=episode.id,
            m3u_account__is_active=True,
        ).filter(
            Q(category__isnull=True) | _enabled_category_subquery("m3u_account_id", "category_id")
        ).select_related('m3u_account').order_by(
            '-m3u_account__priority', 'id'
        ).first()

        if relation:
            return getattr(relation, 'stream_id', None)
        return None
    except Exception as e:
        LOGGER.debug("Failed to get stream_id for episode id=%s: %s", episode.id, e)
        return None


def _get_stream_id_from_prefetch(instance, relation_attr: str = 'active_relation') -> str | None:
    """
    Extract stream_id from prefetched relation on a Movie or Episode instance.

    Expects the instance to have a prefetched attribute containing the highest
    priority active M3U relation. This avoids N+1 queries.

    Args:
        instance: Movie or Episode instance with prefetched relation
        relation_attr: Name of the prefetched attribute (default: 'active_relation')

    Returns:
        stream_id or None if no relation found
    """
    try:
        # Access prefetched relation - this doesn't trigger a DB query
        relations = getattr(instance, relation_attr, [])
        if relations:
            # Prefetch should order by priority, so first item is highest priority
            relation = relations[0]
            return getattr(relation, 'stream_id', None)
        return None
    except Exception as e:
        LOGGER.debug("Failed to get stream_id from prefetch: %s", e)
        return None


def _ensure_dirs() -> None:
    Path(REPORT_ROOT).mkdir(parents=True, exist_ok=True)
    Path(LOG_ROOT).mkdir(parents=True, exist_ok=True)


def _configure_file_logger(debug_enabled: bool) -> None:
    global _FILE_LOGGER_CONFIGURED
    with _LOG_LOCK:
        if _FILE_LOGGER_CONFIGURED:
            return
        _ensure_dirs()
        level = logging.DEBUG if debug_enabled else logging.INFO
        LOGGER.setLevel(level)
        try:
            fh = logging.handlers.RotatingFileHandler(
                filename=str(Path(LOG_ROOT) / "vod2strm.log"),
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8",
            )
            fmt = logging.Formatter(
                "%(asctime)s %(levelname)s %(name)s %(threadName)s %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            fh.setFormatter(fmt)
            fh.setLevel(level)
            LOGGER.addHandler(fh)
        except Exception as e:  # pragma: no cover
            LOGGER.warning("Failed to attach file logger: %s", e)
        _FILE_LOGGER_CONFIGURED = True


# -------------------- Manifest (Metadata Cache) --------------------

def _load_manifest(root: Path) -> Dict[str, Any]:
    """
    Load manifest file or return default.
    Manifest tracks written files to avoid unnecessary disk writes.
    Structure: {"files": {"/path/to/file.strm": {"uuid": "...", "type": "movie|episode", "url": "..."}}, "version": 1}
    Note: Including "url" field allows detection of URL changes (e.g., when stream_id parameter is added).
    """
    manifest_path = root / ".vod2strm_manifest.json"
    try:
        if manifest_path.exists():
            with manifest_path.open("r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        LOGGER.warning("Failed to load manifest from %s: %s", manifest_path, e)
    return {"files": {}, "version": 1}


def _save_manifest(root: Path, manifest: Dict[str, Any]) -> None:
    """
    Save manifest file atomically using temp file + rename.
    Minimizes risk of corruption if interrupted.
    """
    manifest_path = root / ".vod2strm_manifest.json"
    try:
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = manifest_path.with_suffix(f".tmp.{int(time.time() * 1000)}")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, sort_keys=True)
        tmp_path.replace(manifest_path)
    except Exception as e:
        LOGGER.warning("Failed to save manifest to %s: %s", manifest_path, e)


# -------------------- Adaptive Throttle --------------------

class AdaptiveThrottle:
    """
    Monitors write performance and adjusts concurrency dynamically.

    Strategy:
    - Start conservative with 1 worker, scale up based on performance
    - Track average write latency over rolling window (20 writes)
    - If writes are slow (>100ms), cut workers in half
    - If writes are fast (<30ms), increase workers by 50%
    - Check every 10 writes for faster response
    - Bounds: min=1, max=user_setting (capped at 4)
    """

    def __init__(self, max_workers: int, enabled: bool = True):
        # Hard cap at 4 to prevent DB connection exhaustion (Django creates 1 conn per thread)
        # Even though workers do file I/O, accessing model attributes triggers DB connections
        self.max_workers = min(max_workers, 4)
        self.enabled = enabled
        # Start conservative - begin with 1 worker and scale up based on performance
        self.current_workers = 1 if enabled else self.max_workers
        self.write_times = []  # Rolling window of last N write times
        self.window_size = 20  # Track last 20 writes (smaller window for faster response)
        self.lock = threading.Lock()

        # Thresholds (in seconds) - more aggressive to protect NAS
        self.slow_threshold = 0.100  # 100ms (down from 200ms)
        self.fast_threshold = 0.030  # 30ms (down from 50ms)

        # Adjustment rates
        self.scale_down_factor = 0.5   # Cut in half when slow (more aggressive)
        self.scale_up_factor = 1.5     # Increase by 50% when fast (scale up faster)

        # Check interval - adjust every N writes (smaller for faster response)
        self.check_interval = 10
        self.writes_since_check = 0

    def record_write(self, write_time: float) -> None:
        """Record a write operation's duration."""
        if not self.enabled:
            return

        with self.lock:
            self.write_times.append(write_time)
            if len(self.write_times) > self.window_size:
                self.write_times.pop(0)

            self.writes_since_check += 1
            if self.writes_since_check >= self.check_interval:
                self._adjust_concurrency()
                self.writes_since_check = 0

    def _adjust_concurrency(self) -> None:
        """Adjust concurrency based on average write time."""
        if not self.write_times:
            return

        avg_write_time = sum(self.write_times) / len(self.write_times)
        old_workers = self.current_workers

        if avg_write_time > self.slow_threshold:
            # NAS is slow, reduce workers
            self.current_workers = max(1, int(self.current_workers * self.scale_down_factor))
            LOGGER.info(
                "Adaptive throttle: NAS slow (avg %.3fs), reducing workers %d → %d",
                avg_write_time, old_workers, self.current_workers
            )
        elif avg_write_time < self.fast_threshold and self.current_workers < self.max_workers:
            # NAS is fast, increase workers
            self.current_workers = min(self.max_workers, math.ceil(self.current_workers * self.scale_up_factor))
            if self.current_workers != old_workers:
                LOGGER.info(
                    "Adaptive throttle: NAS fast (avg %.3fs), increasing workers %d → %d",
                    avg_write_time, old_workers, self.current_workers
                )

    def get_workers(self) -> int:
        """Get current worker count."""
        with self.lock:
            return self.current_workers if self.enabled else self.max_workers


# -------------------- Small helpers --------------------

def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _norm_fs_name(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("/", "-").replace("\\", "-").replace(":", " -")
    s = s.replace("?", "").replace("*", "").replace('"', "'")
    s = s.replace("<", "(").replace(">", ")").replace("|", "-")
    s = re.sub(r"\s+", " ", s).strip()
    return s or "Unknown"


def _clean_name(name: str, pattern: str | None) -> str:
    """
    Clean a name using the provided regex pattern.
    Used to strip prefixes like "EN - " or "TOP - " from names.
    """
    if not name or not pattern:
        return name
    try:
        # Strip the pattern from the name
        # We use sub() to replace matches with empty string
        return re.sub(pattern, "", name).strip()
    except re.error as e:
        LOGGER.warning("Invalid name cleaning regex '%s': %s", pattern, e)
        return name


def _season_folder_name(season_number: int) -> str:
    if season_number == 0:
        return "Season 00 (Specials)"
    return f"Season {season_number:02d}"


def _series_folder_name(name: str, year: int | None) -> str:
    # Avoid double "(YYYY)" if already present and matches DB year
    year_suffix = re.search(r"\((\d{4})\)\s*$", name or "")
    if year and year_suffix and int(year_suffix.group(1)) == int(year):
        return _norm_fs_name(name)
    if year:
        return _norm_fs_name(f"{name} ({year})")
    return _norm_fs_name(name or "Unknown Series")


def _movie_folder_name(name: str, year: int | None) -> str:
    """
    Generate folder name for movie.
    Strips any existing (YYYY) pattern from name to avoid duplication when adding year.
    """
    if not name:
        name = "Unknown Movie"

    # Strip trailing (YYYY) pattern if present to avoid duplication
    # Example: "The Matrix (1999)" -> "The Matrix"
    name = re.sub(r'\s*\(\d{4}\)\s*$', '', name).strip()

    if year:
        return _norm_fs_name(f"{name} ({year})")
    return _norm_fs_name(name)


def _hash_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _read_file_bytes(p: Path) -> bytes | None:
    try:
        return p.read_bytes()
    except FileNotFoundError:
        return None


def _write_if_changed(path: Path, content: bytes) -> Tuple[bool, str]:
    """
    Compare-before-write. Returns (written, reason)
    reason ∈ {"created","updated","same_contents"}
    """
    existing = _read_file_bytes(path)
    new_hash = _hash_bytes(content)
    if existing is not None and _hash_bytes(existing) == new_hash:
        return (False, "same_contents")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return (True, "created" if existing is None else "updated")


def _write_strm_if_changed(path: Path, uuid: str, url: str, manifest: Dict[str, Any], file_type: str, dry_run: bool = False) -> Tuple[bool, str]:
    """
    Write .strm file only if UUID, URL, or type changed, or file doesn't exist in manifest.
    Checks manifest first to avoid disk reads when possible.

    Returns (written, reason)
    reason ∈ {"created", "updated", "cached_skip", "dry_run"}
    """
    path_str = str(path)
    manifest_files = manifest.get("files", {})

    # Check manifest cache first - avoid disk I/O entirely
    # Include URL in cache check to detect when stream_id or other URL params change
    cache_matches = False
    with _MANIFEST_LOCK:
        if path_str in manifest_files:
            cached_entry = manifest_files[path_str]
            cache_matches = (
                cached_entry.get("uuid") == uuid and
                cached_entry.get("type") == file_type and
                cached_entry.get("url") == url
            )

    # If cache matches, verify file exists (outside lock to minimize lock time)
    if cache_matches:
        if path.exists():
            return (False, "cached_skip")
        # Manifest is stale - file was deleted/corrupted
        LOGGER.warning("Manifest entry for %s is stale (file missing); regenerating.", path_str)

    # UUID, URL, or type changed, or not in manifest
    if dry_run:
        # Don't write, but report what would happen
        with _MANIFEST_LOCK:
            is_new = path_str not in manifest_files
        return (False, f"dry_run_{'create' if is_new else 'update'}")

    # Write for real
    content = (url + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)

    # Update manifest - include URL so we can detect URL changes on next run
    with _MANIFEST_LOCK:
        is_new = path_str not in manifest_files
        manifest_files[path_str] = {"uuid": uuid, "type": file_type, "url": url}

    return (True, "created" if is_new else "updated")


def _xml_escape(text: str | None) -> str:
    if not text:
        return ""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


# -------------------- NFO Builders --------------------

def _nfo_movie(m: Movie, clean_name: str | None = None) -> bytes:
    # Use name field - title field doesn't exist in Movie model
    # Use clean_name if provided (for title tag), otherwise fall back to DB name
    title = clean_name if clean_name else (m.name or "")
    fields = {
        "title": title,
        "plot": getattr(m, "description", "") or "",
        "year": str(getattr(m, "year", "") or ""),
        "rating": str(getattr(m, "rating", "") or ""),
        "genre": getattr(m, "genre", "") or "",
        "uniqueid_tmdb": str(getattr(m, "tmdb_id", "") or ""),
        "uniqueid_imdb": str(getattr(m, "imdb_id", "") or ""),
    }
    xml = io.StringIO()
    xml.write("<movie>\n")
    for tag, val in fields.items():
        if val:
            if tag.startswith("uniqueid_"):
                typ = tag.split("_", 1)[1]
                xml.write(f'  <uniqueid type="{typ}">{_xml_escape(val)}</uniqueid>\n')
            else:
                xml.write(f"  <{tag}>{_xml_escape(val)}</{tag}>\n")
    logo = getattr(m, "logo", None)
    if logo:
        xml.write(f"  <thumb>{_xml_escape(str(logo))}</thumb>\n")
    xml.write("</movie>\n")
    return xml.getvalue().encode("utf-8")


def _nfo_season(s: Series, season_number: int, clean_series_name: str | None = None) -> bytes:
    xml = io.StringIO()
    xml.write("<season>\n")
    xml.write(f"  <seasonnumber>{season_number}</seasonnumber>\n")
    name = clean_series_name if clean_series_name else (s.name or "")
    year = getattr(s, "year", None)
    xml.write(f"  <tvshowtitle>{_xml_escape(_series_folder_name(name, year))}</tvshowtitle>\n")
    xml.write("</season>\n")
    return xml.getvalue().encode("utf-8")


def _nfo_episode(e: Episode, clean_name: str | None = None) -> bytes:
    # Use clean_name if provided (for title tag), otherwise fall back to DB name
    title = clean_name if clean_name else (e.name or "")
    fields = {
        "title": title,
        "season": str(getattr(e, "season_number", "") or ""),
        "episode": str(getattr(e, "episode_number", "") or ""),
        "aired": str(getattr(e, "air_date", "") or ""),
        "plot": getattr(e, "description", "") or "",
        "rating": str(getattr(e, "rating", "") or ""),
        "uniqueid_tmdb": str(getattr(e, "tmdb_id", "") or ""),
        "uniqueid_imdb": str(getattr(e, "imdb_id", "") or ""),
    }
    xml = io.StringIO()
    xml.write("<episodedetails>\n")
    for tag, val in fields.items():
        if val:
            if tag.startswith("uniqueid_"):
                typ = tag.split("_", 1)[1]
                xml.write(f'  <uniqueid type="{typ}">{_xml_escape(val)}</uniqueid>\n')
            else:
                xml.write(f"  <{tag}>{_xml_escape(val)}</{tag}>\n")
    xml.write("</episodedetails>\n")
    return xml.getvalue().encode("utf-8")


# -------------------- Filename helpers --------------------

def _episode_filename(e: Episode) -> str:
    ss = getattr(e, "season_number", 0) or 0
    ee = getattr(e, "episode_number", 0) or 0
    title = _norm_fs_name(getattr(e, "name", "") or "Episode")
    return f"S{ss:02d}E{ee:02d} - {title}.strm"


def _episode_nfo_filename(e: Episode) -> str:
    ss = getattr(e, "season_number", 0) or 0
    ee = getattr(e, "episode_number", 0) or 0
    return f"S{ss:02d}E{ee:02d}.nfo"


def _series_expected_count(series_id: int) -> int:
    return Episode.objects.filter(series_id=series_id).count()


def _compare_tree_quick(series_root: Path, expected_count: int, want_nfos: bool) -> bool:
    """
    Quick short-circuit: if .strm count matches expected (and NFOs if enabled),
    assume series is complete (skip expensive per-file checks).
    """
    if not series_root.exists():
        return False
    strm_count = len(list(series_root.rglob("*.strm")))
    if strm_count != expected_count:
        return False
    if want_nfos:
        nfo_eps = len(list(series_root.rglob("S??E??.nfo")))
        if nfo_eps != expected_count:
            return False
    return True


# -------------------- Generators --------------------

def _make_movie_strm_and_nfo(movie: Movie, base_url: str, root: Path, write_nfos: bool, report_rows: List[List[str]], lock: threading.Lock, manifest: Dict[str, Any], dry_run: bool = False, throttle: AdaptiveThrottle | None = None, clean_regex: str | None = None) -> None:
    # Use name field - title field doesn't exist in Movie model
    raw_name = movie.name or ""
    movie_name = _clean_name(raw_name, clean_regex)

    m_folder = root / "Movies" / _movie_folder_name(movie_name, getattr(movie, "year", None))
    strm_path = m_folder / f"{_movie_folder_name(movie_name, getattr(movie, 'year', None))}.strm"

    # Get stream_id from prefetched relation (avoids N+1 query)
    # Falls back to query if prefetch not available (backward compatibility)
    stream_id = _get_stream_id_from_prefetch(movie, 'active_relation')
    if stream_id is None:
        # Fallback to query if prefetch not available
        stream_id = _get_movie_stream_id(movie)

    url = f"{base_url.rstrip('/')}/proxy/vod/movie/{movie.uuid}"
    if stream_id:
        url = f"{url}?stream_id={stream_id}"

    # Time the write operation for adaptive throttling
    start_time = time.time()
    wrote, reason = _write_strm_if_changed(strm_path, str(movie.uuid), url, manifest, "movie", dry_run)
    if wrote and throttle:
        throttle.record_write(time.time() - start_time)

    with lock:
        report_rows.append(["movie", "", "", raw_name, getattr(movie, "year", ""), str(movie.uuid), str(strm_path), "", "written" if wrote else "skipped", reason])

    if write_nfos and not dry_run:
        nfo_start = time.time()
        nfo_path = m_folder / "movie.nfo"
        nfo_bytes = _nfo_movie(movie, clean_name=movie_name)
        wrote_nfo, nfo_reason = _write_if_changed(nfo_path, nfo_bytes)
        if wrote_nfo and throttle:
            throttle.record_write(time.time() - nfo_start)
        with lock:
            report_rows.append(["movie_nfo", "", "", raw_name, getattr(movie, "year", ""), str(movie.uuid), "", str(nfo_path), "written" if wrote_nfo else "skipped", nfo_reason])


def _make_episode_strm_and_nfo(series: Series, episode: Episode, base_url: str, root: Path, write_nfos: bool, report_rows: List[List[str]], lock: threading.Lock, manifest: Dict[str, Any], dry_run: bool = False, throttle: AdaptiveThrottle | None = None, written_seasons: set | None = None, clean_regex: str | None = None) -> None:
    # Workaround for Dispatcharr issue #556: Validate episode still exists before writing
    # Episodes can disappear mid-generation due to sync conflicts
    try:
        episode_exists = Episode.objects.filter(id=episode.id).exists()
        if not episode_exists:
            title = getattr(episode, "name", "") or ""
            season_number = getattr(episode, "season_number", 0) or 0
            LOGGER.warning(
                "Dispatcharr issue #556: Episode id=%s (S%02dE%02d - %s) vanished from database during generation. Skipping.",
                episode.id,
                season_number,
                getattr(episode, "episode_number", 0) or 0,
                title
            )
            with lock:
                report_rows.append(["episode", series.name or "", season_number, title, getattr(series, "year", ""), str(episode.uuid), "", "", "skipped", "episode_vanished"])
            return
    except Exception as validation_error:
        LOGGER.debug("Episode validation check failed: %s. Continuing anyway.", validation_error)

    s_folder = root / "TV" / _series_folder_name(_clean_name(series.name or "", clean_regex), getattr(series, "year", None))
    season_number = getattr(episode, "season_number", 0) or 0
    e_folder = s_folder / _season_folder_name(season_number)
    strm_name = _episode_filename(episode)
    strm_path = e_folder / strm_name

    # Get stream_id from prefetched relation (avoids N+1 query)
    # Falls back to query if prefetch not available (backward compatibility)
    stream_id = _get_stream_id_from_prefetch(episode, 'active_relation')
    if stream_id is None:
        # Fallback to query if prefetch not available
        stream_id = _get_episode_stream_id(episode)

    url = f"{base_url.rstrip('/')}/proxy/vod/episode/{episode.uuid}"
    if stream_id:
        url = f"{url}?stream_id={stream_id}"

    # Time the write operation for adaptive throttling
    start_time = time.time()
    wrote, reason = _write_strm_if_changed(strm_path, str(episode.uuid), url, manifest, "episode", dry_run)
    if wrote and throttle:
        throttle.record_write(time.time() - start_time)

    title = getattr(episode, "name", "") or ""
    with lock:
        report_rows.append(["episode", series.name or "", season_number, title, getattr(series, "year", ""), str(episode.uuid), str(strm_path), "", "written" if wrote else "skipped", reason])

    if write_nfos and not dry_run:
        # season.nfo (only write once per season)
        season_key = (series.id, season_number)
        should_write_season = False

        if written_seasons is not None:
            with lock:
                if season_key not in written_seasons:
                    written_seasons.add(season_key)
                    should_write_season = True
        else:
            # Fallback if no set provided (shouldn't happen)
            should_write_season = True

        if should_write_season:
            season_start = time.time()
            season_nfo_path = e_folder / "season.nfo"
            season_nfo_bytes = _nfo_season(series, season_number, clean_series_name=_clean_name(series.name or "", clean_regex))
            wrote_s, reason_s = _write_if_changed(season_nfo_path, season_nfo_bytes)
            if wrote_s and throttle:
                throttle.record_write(time.time() - season_start)
            with lock:
                report_rows.append(["season_nfo", series.name or "", season_number, "", getattr(series, "year", ""), "", "", str(season_nfo_path), "written" if wrote_s else "skipped", reason_s])

        # episode nfo
        ep_start = time.time()
        ep_nfo_path = e_folder / _episode_nfo_filename(episode)
        # Pass cleaned episode name for consistency with movie/season NFO generation
        episode_name = _clean_name(getattr(episode, "name", "") or "", clean_regex)
        ep_nfo_bytes = _nfo_episode(episode, clean_name=episode_name)
        wrote_e, reason_e = _write_if_changed(ep_nfo_path, ep_nfo_bytes)
        if wrote_e and throttle:
            throttle.record_write(time.time() - ep_start)
        with lock:
            report_rows.append(["episode_nfo", series.name or "", season_number, title, getattr(series, "year", ""), str(episode.uuid), "", str(ep_nfo_path), "written" if wrote_e else "skipped", reason_e])


# -------------------- Cleanup --------------------

def _cleanup(rows: List[List[str]], root: Path, manifest: Dict[str, Any], apply: bool) -> None:
    """
    Identify and optionally remove stale *.strm files that reference UUIDs not present in DB.
    Also deletes associated NFO files and prunes empty directories.
    """
    LOGGER.info("Cleanup started (apply=%s)", apply)
    manifest_files = manifest.get("files", {})
    movie_uuids = set(_eligible_movie_queryset().values_list("uuid", flat=True))
    allowed_series_ids = _eligible_series_queryset().values_list("id", flat=True)
    episode_uuids = set(
        Episode.objects.filter(series_id__in=allowed_series_ids).values_list("uuid", flat=True)
    )

    def check_one(p: Path):
        try:
            data = p.read_text(encoding="utf-8", errors="ignore").strip()
            # Simple UUID extraction pattern: [a-f0-9-]+
            # We control .strm generation, so UUIDs are always valid UUID v4 format.
            # No need for strict validation - simpler pattern = faster when scanning 1000s of files.
            m = re.search(r"/proxy/vod/(movie|episode)/([a-f0-9-]+)", data, flags=re.I)
            if not m:
                return ("unknown", None)
            typ, uid = m.group(1).lower(), m.group(2)
            present = (uid in movie_uuids) if typ == "movie" else (uid in episode_uuids)
            return ("ok", (typ, uid, present))
        except Exception as e:
            return ("error", str(e))

    strm_files = list(root.rglob("*.strm")) if root.exists() else []
    LOGGER.info("Cleanup scanning %d .strm files", len(strm_files))
    stale_paths = []
    deleted_nfos = 0

    for p in strm_files:
        kind, payload = check_one(p)
        if kind == "error":
            rows.append(["cleanup", "", "", p.name, "", "", str(p), "", "error", payload])
            continue
        if kind == "unknown":
            rows.append(["cleanup", "", "", p.name, "", "", str(p), "", "skip", "unknown_url_format"])
            continue
        typ, uid, present = payload
        if present:
            continue
        # stale
        stale_paths.append(p)
        if apply:
            try:
                # Delete the .strm file
                p.unlink()
                # Remove from manifest
                path_str = str(p)
                with _MANIFEST_LOCK:
                    if path_str in manifest_files:
                        del manifest_files[path_str]

                # Delete associated NFO files
                if typ == "movie":
                    # Delete movie.nfo in the same folder
                    nfo_path = p.parent / "movie.nfo"
                    if nfo_path.exists():
                        nfo_path.unlink()
                        deleted_nfos += 1
                else:  # episode
                    # Delete S##E##.nfo for this episode
                    # .strm filename is like "S01E02 - Title.strm", .nfo is "S01E02.nfo"
                    strm_stem = p.stem  # "S01E02 - Title"
                    # Extract S##E## pattern
                    m = re.match(r'(S\d+E\d+)', strm_stem, re.I)
                    if m:
                        episode_nfo = p.parent / f"{m.group(1)}.nfo"
                        if episode_nfo.exists():
                            episode_nfo.unlink()
                            deleted_nfos += 1

                rows.append(["cleanup", "", "", p.name, "", uid, str(p), "", "deleted", f"stale_{typ}"])
            except Exception as e:
                rows.append(["cleanup", "", "", p.name, "", uid, str(p), "", "error", f"delete_failed: {e}"])
        else:
            rows.append(["cleanup", "", "", p.name, "", uid, str(p), "", "would_delete", f"stale_{typ}"])

    # prune empty dirs and season.nfo files in apply mode
    if apply:
        pruned_dirs = 0
        pruned_season_nfos = 0
        # Walk deepest-first
        for d in sorted({p.parent for p in stale_paths}, key=lambda x: len(str(x)), reverse=True):
            try:
                # Check if season.nfo exists in empty season folder and delete it
                if d.exists():
                    season_nfo = d / "season.nfo"
                    if season_nfo.exists() and not any(f for f in d.iterdir() if f.suffix == '.strm'):
                        # No .strm files left in season folder, delete season.nfo
                        season_nfo.unlink()
                        pruned_season_nfos += 1

                # Delete empty directories
                cur = d
                while cur != root and cur.exists() and not any(cur.iterdir()):
                    cur.rmdir()
                    pruned_dirs += 1
                    cur = cur.parent
            except Exception:
                pass

        if deleted_nfos > 0:
            rows.append(["cleanup", "", "", "", "", "", "", "", "deleted_nfos", str(deleted_nfos)])
        if pruned_season_nfos > 0:
            rows.append(["cleanup", "", "", "", "", "", "", "", "deleted_season_nfos", str(pruned_season_nfos)])
        if pruned_dirs > 0:
            rows.append(["cleanup", "", "", "", "", "", "", "", "pruned_dirs", str(pruned_dirs)])
    LOGGER.info("Cleanup finished (apply=%s, deleted_nfos=%d)", apply, deleted_nfos)


# -------------------- Jobs --------------------

def _csv_writer(path: Path, header: List[str], rows_iter: Iterable[List[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in rows_iter:
            w.writerow(row)


def _run_job_sync(
    mode: str,
    output_root: str,
    base_url: str,
    write_nfos: bool,
    cleanup_mode: str,
    concurrency: int,
    debug_logging: bool = False,
    dry_run: bool = False,
    adaptive_throttle: bool = True,
    clean_regex: str | None = None,
) -> None:
    _configure_file_logger(debug_logging)
    LOGGER.info("RUN START mode=%s root=%s base_url=%s nfos=%s cleanup=%s conc=%s dry_run=%s adaptive=%s regex=%s",
                mode, output_root, base_url, write_nfos, cleanup_mode, concurrency, dry_run, adaptive_throttle, clean_regex)

    root = Path(output_root)
    ts = _ts()
    report_path = Path(REPORT_ROOT) / f"report_{mode}_{ts}.csv"
    header = ["type", "series_name", "season", "title", "year", "db_uuid", "strm_path", "nfo_path", "action", "reason"]
    rows: List[List[str]] = []

    try:
        # Run cleanup BEFORE generation to remove stale files first
        # This prevents the bug where newly written files get deleted because
        # cleanup loads the old manifest before the new one is saved
        if cleanup_mode in (CLEANUP_PREVIEW, CLEANUP_APPLY):
            if not dry_run:
                manifest = _load_manifest(root)
                _cleanup(rows, root, manifest, apply=(cleanup_mode == CLEANUP_APPLY))
                # Save updated manifest after cleanup (pruned stale entries)
                if cleanup_mode == CLEANUP_APPLY:
                    _save_manifest(root, manifest)
                    LOGGER.info("Manifest saved after cleanup with %d tracked files", len(manifest.get("files", {})))

        if mode in ("movies", "all"):
            _generate_movies(rows, base_url, root, write_nfos, concurrency, dry_run, adaptive_throttle, clean_regex)

        if mode in ("series", "all"):
            _generate_series(rows, base_url, root, write_nfos, concurrency, dry_run, adaptive_throttle, clean_regex)

        if mode == "stats":
            _stats_only(rows, base_url, root, write_nfos)

    except Exception as e:  # pragma: no cover
        LOGGER.exception("Job failed: %s", e)
        rows.append(["error", "", "", "", "", "", "", "", "error", str(e)])

    _csv_writer(report_path, header, rows)
    LOGGER.info("RUN END mode=%s -> report=%s", mode, report_path)


def _generate_movies(rows: List[List[str]], base_url: str, root: Path, write_nfos: bool, concurrency: int, dry_run: bool = False, adaptive_throttle: bool = True, clean_regex: str | None = None) -> None:
    LOGGER.info("Scanning movies... (dry_run=%s, adaptive=%s, regex=%s)", dry_run, adaptive_throttle, clean_regex)
    # Only generate .strm files for movies with active provider relations
    # Prefetch M3UMovieRelation to avoid N+1 queries (one query per movie)
    # Prefetch only highest priority active relation with enabled category
    active_movie_relations = M3UMovieRelation.objects.filter(
        m3u_account__is_active=True,
    ).filter(
        Q(category__isnull=True) | _enabled_category_subquery("m3u_account_id", "category_id")
    ).select_related('m3u_account').order_by('-m3u_account__priority', 'id')

    qs = _eligible_movie_queryset().prefetch_related(
        Prefetch('m3u_relations', queryset=active_movie_relations, to_attr='active_relation')
    ).only(
        "id", "uuid", "name", "year", "description", "rating", "genre", "tmdb_id", "imdb_id", "logo"
    )
    work = list(qs)
    LOGGER.info("Movies to process: %d", len(work))

    # Load manifest for caching
    manifest = _load_manifest(root)
    lock = threading.Lock()
    throttle = AdaptiveThrottle(max_workers=concurrency, enabled=adaptive_throttle)

    # Process in batches to allow adaptive concurrency adjustments
    batch_size = 100
    for i in range(0, len(work), batch_size):
        batch = work[i:i + batch_size]
        current_workers = throttle.get_workers()

        def job(m: Movie) -> None:
            try:
                _make_movie_strm_and_nfo(m, base_url, root, write_nfos, rows, lock, manifest, dry_run, throttle, clean_regex)
            except Exception as e:
                LOGGER.warning("Movie id=%s failed: %s", m.id, e)
                with lock:
                    rows.append(["movie", "", "", m.name or "", getattr(m, "year", ""), str(m.uuid), "", "", "error", str(e)])

        with ThreadPoolExecutor(max_workers=max(1, current_workers)) as ex:
            list(ex.map(job, batch))

        # Log progress every batch
        LOGGER.info("Movies: processed %d / %d (current workers: %d)", min(i + batch_size, len(work)), len(work), current_workers)

    # Save manifest after all writes (skip in dry run)
    if not dry_run:
        _save_manifest(root, manifest)
        LOGGER.info("Manifest saved with %d tracked files", len(manifest.get("files", {})))


def _cleanup_series_episodes(series_id: int) -> bool:
    """
    Targeted cleanup for a single series - deletes episodes and resets cache flags.

    This is called when Dispatcharr issue #556 duplicate key error occurs during
    series refresh. Instead of leaving corrupted data, we clean the series and
    allow a retry.

    Args:
        series_id: The series ID to clean up

    Returns:
        True if cleanup succeeded, False otherwise
    """
    try:
        with transaction.atomic():
            # Count what will be deleted
            episode_count = Episode.objects.filter(series_id=series_id).count()

            if episode_count > 0:
                # Delete episodes for this series (cascade handles M3UEpisodeRelation)
                deleted_count, _ = Episode.objects.filter(series_id=series_id).delete()
                LOGGER.info(
                    "Cleaned up series_id=%s: deleted %d objects (%d episodes)",
                    series_id,
                    deleted_count,
                    episode_count
                )

            # Reset cache flags for this series' relations
            # NOTE: Using raw SQL instead of ORM because:
            # 1. custom_properties is PostgreSQL JSONB - need to update nested keys without replacing entire object
            # 2. ORM would require N queries (fetch, modify, save for each relation) = slow + not atomic
            # 3. PostgreSQL's jsonb_set() does atomic partial updates in a single query
            # 4. Django ORM lacks native JSONB merge/partial update support
            relation_count = M3USeriesRelation.objects.filter(series_id=series_id).count()
            if relation_count > 0:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        UPDATE vod_m3useriesrelation
                        SET custom_properties = jsonb_set(
                                jsonb_set(
                                    COALESCE(custom_properties, '{}'::jsonb),
                                    '{episodes_fetched}', 'false'::jsonb,
                                    true
                                ),
                                '{detailed_fetched}', 'false'::jsonb,
                                true
                            ),
                            last_episode_refresh = NULL
                        WHERE series_id = %s
                    """, [series_id])
                    reset_count = cursor.rowcount
                    LOGGER.info(
                        "Reset %d cache flags for series_id=%s",
                        reset_count,
                        series_id
                    )

        return True

    except Exception:
        LOGGER.exception("Failed to clean up series_id=%s", series_id)
        return False


def _maybe_internal_refresh_series(series: Series) -> bool:
    """
    Refresh episodes from the highest priority provider only.

    Since Dispatcharr deduplicates series by TMDB/IMDB ID, all provider relations
    for a series point to the same series_id. Therefore we only need to call
    refresh_series_episodes once - from the highest priority provider.

    This avoids redundant refresh calls and potential race conditions when
    multiple providers try to populate the same episode rows.

    Returns True if episodes were successfully fetched.
    """
    try:
        from apps.vod.tasks import refresh_series_episodes
    except ImportError:
        try:
            from vod.tasks import refresh_series_episodes
        except ImportError:
            LOGGER.debug("Could not import refresh_series_episodes; skipping internal refresh.")
            return False

    try:
        # Get highest priority active relation only
        relation = series.m3u_relations.select_related('m3u_account').filter(
            m3u_account__is_active=True
        ).order_by('-m3u_account__priority', 'id').first()

        if not relation:
            LOGGER.debug("Series id=%s has no active M3U account relations", series.id)
            return False

        LOGGER.info(
            "Refreshing episodes for series_id=%s from provider %s (priority %s)",
            series.id,
            relation.m3u_account.name,
            relation.m3u_account.priority
        )

        try:
            # Call refresh directly (synchronous, matching UI pattern)
            refresh_series_episodes(
                relation.m3u_account,
                series,
                relation.external_series_id
            )
        except Exception as refresh_error:
            # Check if this is the duplicate key constraint error from issue #556
            error_str = str(refresh_error)
            if "duplicate key" in error_str.lower() and "vod_episode" in error_str.lower():
                LOGGER.warning(
                    "Dispatcharr issue #556: Duplicate episode constraint violation for series_id=%s. "
                    "Attempting targeted cleanup and retry...",
                    series.id
                )
                # Clean up corrupted episodes and cache flags for this series
                if _cleanup_series_episodes(series.id):
                    LOGGER.info("Cleanup succeeded, retrying refresh for series_id=%s", series.id)
                    try:
                        # Retry the refresh once after cleanup
                        refresh_series_episodes(
                            relation.m3u_account,
                            series,
                            relation.external_series_id
                        )
                        LOGGER.info("Retry succeeded for series_id=%s after cleanup", series.id)
                    except Exception as retry_error:
                        LOGGER.error(
                            "Retry failed for series_id=%s after cleanup: %s",
                            series.id,
                            retry_error
                        )
                        return False
                else:
                    LOGGER.error("Cleanup failed for series_id=%s, giving up", series.id)
                    return False
            else:
                LOGGER.warning(
                    "Episode refresh error for series_id=%s from provider %s: %s",
                    series.id,
                    relation.m3u_account.name,
                    refresh_error
                )
                return False

        # Check if we got episodes
        episode_count = Episode.objects.filter(series_id=series.id).count()
        if episode_count > 0:
            LOGGER.info(
                "Successfully fetched %d episodes from provider %s",
                episode_count,
                relation.m3u_account.name
            )
            return True

        LOGGER.debug(
            "Provider %s returned 0 episodes for series_id=%s",
            relation.m3u_account.name,
            series.id
        )
        return False

    except Exception as e:  # pragma: no cover
        LOGGER.warning("Episode refresh failed for series_id=%s: %s", series.id, e)
        return False


def _generate_series(rows: List[List[str]], base_url: str, root: Path, write_nfos: bool, concurrency: int, dry_run: bool = False, adaptive_throttle: bool = True, clean_regex: str | None = None) -> None:
    LOGGER.info("Scanning series... (dry_run=%s, adaptive=%s, regex=%s)", dry_run, adaptive_throttle, clean_regex)
    # Only generate .strm files for series with active provider relations
    # Annotate with episode count to avoid N+1 queries (distinct=True prevents inflated counts from join)
    series_qs = _eligible_series_queryset().annotate(
        episode_count=Count('episodes', distinct=True)
    ).only("id", "uuid", "name", "year", "description", "rating", "genre", "tmdb_id", "imdb_id", "logo")
    total = series_qs.count()
    LOGGER.info("Series to process: %d", total)

    # Load manifest for caching
    manifest = _load_manifest(root)
    lock = threading.Lock()

    # Track written seasons to avoid duplicate season.nfo writes
    written_seasons = set()

    throttle = AdaptiveThrottle(max_workers=concurrency, enabled=adaptive_throttle)

    for s in series_qs.iterator(chunk_size=200):
        try:
            series_folder = root / "TV" / _series_folder_name(_clean_name(s.name or "", clean_regex), getattr(s, "year", None))
            # Use annotated episode_count to avoid N+1 query
            expected = getattr(s, 'episode_count', 0)

            if expected == 0:
                LOGGER.debug("Series id=%s has 0 episodes; attempting internal refresh.", s.id)
                refreshed = _maybe_internal_refresh_series(s)
                if refreshed:
                    # Episodes were fetched, recount (no sleep needed - synchronous call)
                    expected = _series_expected_count(s.id)
                else:
                    LOGGER.debug("Could not fetch episodes for series id=%s; skipping.", s.id)

            if expected > 0 and _compare_tree_quick(series_folder, expected, write_nfos):
                with lock:
                    rows.append(["series", s.name or "", "", "", getattr(s, "year", ""), str(s.uuid), str(series_folder), "", "skipped", "tree_complete"])
                continue

            # Only generate episodes that have active provider relations
            # Workaround for Dispatcharr issue #569: Use .distinct() to handle duplicate M3UEpisodeRelation records
            # Prefetch M3UEpisodeRelation to avoid N+1 queries (one query per episode)
            active_episode_relations = M3UEpisodeRelation.objects.filter(
                m3u_account__is_active=True,
            ).filter(
                Q(category__isnull=True) | _enabled_category_subquery("m3u_account_id", "category_id")
            ).select_related('m3u_account').order_by('-m3u_account__priority', 'id')

            eps_query = Episode.objects.filter(
                series_id=s.id,
                m3u_relations__m3u_account__is_active=True
            )

            # Check for duplicate relations (issue #569 detection)
            total_before_distinct = eps_query.count()
            eps = list(eps_query.distinct().prefetch_related(
                Prefetch('m3u_relations', queryset=active_episode_relations, to_attr='active_relation')
            ).only(
                "id", "uuid", "name", "season_number", "episode_number",
                "air_date", "description", "rating", "tmdb_id", "imdb_id"
            ).order_by("season_number", "episode_number"))
            total_after_distinct = len(eps)

            if total_before_distinct > total_after_distinct:
                LOGGER.warning(
                    "Dispatcharr issue #569: Series '%s' (id=%s) has duplicate M3U relations. "
                    "Found %d episode relations but only %d unique episodes. Using distinct episodes only.",
                    s.name,
                    s.id,
                    total_before_distinct,
                    total_after_distinct
                )

            if not eps:
                with lock:
                    rows.append(["series", s.name or "", "", "", getattr(s, "year", ""), str(s.uuid), str(series_folder), "", "skipped", "no_episodes"])
                continue

            # Process episodes in batches with adaptive concurrency
            batch_size = 100
            for i in range(0, len(eps), batch_size):
                batch = eps[i:i + batch_size]
                current_workers = throttle.get_workers()

                def job(e: Episode) -> None:
                    _make_episode_strm_and_nfo(s, e, base_url, root, write_nfos, rows, lock, manifest, dry_run, throttle, written_seasons, clean_regex)

                with ThreadPoolExecutor(max_workers=max(1, current_workers)) as ex:
                    list(ex.map(job, batch))

        except Exception as e:
            LOGGER.warning("Series id=%s failed: %s", getattr(s, "id", "?"), e)
            with lock:
                rows.append(["series", s.name or "", "", "", getattr(s, "year", ""), str(getattr(s, "uuid", "")), "", "", "error", str(e)])

    # Save manifest after all writes (skip in dry run)
    if not dry_run:
        _save_manifest(root, manifest)
        LOGGER.info("Manifest saved with %d tracked files", len(manifest.get("files", {})))


def _db_stats() -> str:
    """
    Generate database statistics report showing content counts and relationships.
    Returns formatted string suitable for display in Dispatcharr UI.
    """
    from django.db import connection

    stats = []
    stats.append("=== DATABASE STATISTICS ===\n")

    # Basic counts
    movies = Movie.objects.count()
    series = Series.objects.count()
    episodes = Episode.objects.count()

    stats.append(f"CONTENT COUNTS:")
    stats.append(f"  Movies: {movies}")
    stats.append(f"  Series: {series}")
    stats.append(f"  Episodes: {episodes}\n")

    # M3U account stats
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT ma.id, ma.name, ma.priority, ma.is_active
                FROM m3u_m3uaccount ma
                ORDER BY ma.priority DESC, ma.name
            """)
            providers = cursor.fetchall()

            if providers:
                stats.append("M3U PROVIDERS:")
                for provider_id, name, priority, is_active in providers:
                    status = "✓" if is_active else "✗"
                    stats.append(f"  {status} ID {provider_id}: {name} (priority: {priority})")
                stats.append("")

            # Movies per provider
            cursor.execute("""
                SELECT ma.name, ma.id, COUNT(DISTINCT m.id) as count
                FROM vod_movie m
                INNER JOIN vod_m3umovierelation mr ON m.id = mr.movie_id
                INNER JOIN m3u_m3uaccount ma ON mr.m3u_account_id = ma.id
                GROUP BY ma.name, ma.id
                ORDER BY count DESC
            """)
            movie_counts = cursor.fetchall()

            if movie_counts:
                stats.append("MOVIES BY PROVIDER:")
                for name, provider_id, count in movie_counts:
                    stats.append(f"  {name} (ID {provider_id}): {count} movies")
                stats.append("")

            # Series per provider
            cursor.execute("""
                SELECT ma.name, ma.id, COUNT(DISTINCT s.id) as count
                FROM vod_series s
                INNER JOIN vod_m3useriesrelation sr ON s.id = sr.series_id
                INNER JOIN m3u_m3uaccount ma ON sr.m3u_account_id = ma.id
                GROUP BY ma.name, ma.id
                ORDER BY count DESC
            """)
            series_counts = cursor.fetchall()

            if series_counts:
                stats.append("SERIES BY PROVIDER:")
                for name, provider_id, count in series_counts:
                    stats.append(f"  {name} (ID {provider_id}): {count} series")
                stats.append("")

            # Series without episodes
            cursor.execute("""
                SELECT COUNT(DISTINCT s.id)
                FROM vod_series s
                LEFT JOIN vod_episode e ON s.id = e.series_id
                WHERE e.id IS NULL
            """)
            no_episodes = cursor.fetchone()[0]

            if no_episodes > 0:
                stats.append(f"WARNINGS:")
                stats.append(f"  Series without episodes: {no_episodes}")
                stats.append("")

    except Exception as e:
        stats.append(f"\nError querying database: {e}")

    return "\n".join(stats)


def _db_cleanup() -> str:
    """
    Database cleanup - deletes ALL episodes and episode relations, then resets series cache flags.
    Issue #556 - Quick database cleanup for development/debugging.

    Replicates these operations:
        1. DELETE FROM vod_episode (cascades to vod_m3uepisoderelation)
        2. RESET series cache flags (episodes_fetched, detailed_fetched, last_episode_refresh)

        This ensures Dispatcharr will re-fetch episodes on next UI interaction.

    Returns:
        Formatted string with operation results
    """
    results = []
    results.append("=== DATABASE CLEANUP (Issue #556) ===\n")

    try:
        # Count what will be deleted/reset
        episode_count = Episode.objects.count()
        series_relation_count = M3USeriesRelation.objects.count()

        results.append(f"Episodes to delete: {episode_count}")
        results.append(f"Series relations to reset: {series_relation_count}")

        if episode_count > 0 or series_relation_count > 0:
            with transaction.atomic():
                # Delete all episodes (cascade deletes will handle M3UEpisodeRelation automatically)
                if episode_count > 0:
                    deleted_count, details = Episode.objects.all().delete()
                    results.append(f"\n✓ Deleted {deleted_count} objects:")
                    for model, count in details.items():
                        if count > 0:
                            results.append(f"  - {model}: {count}")

                # Reset series cache flags so Dispatcharr re-fetches episodes on next UI interaction
                if series_relation_count > 0:
                    with connection.cursor() as cursor:
                        cursor.execute("""
                            UPDATE vod_m3useriesrelation
                            SET custom_properties = jsonb_set(
                                    jsonb_set(
                                        COALESCE(custom_properties, '{}'::jsonb),
                                        '{episodes_fetched}', 'false'::jsonb,
                                        true
                                    ),
                                    '{detailed_fetched}', 'false'::jsonb,
                                    true
                                ),
                                last_episode_refresh = NULL
                        """)
                        reset_count = cursor.rowcount
                    results.append(f"\n✓ Reset {reset_count} series relation cache flags")
        else:
            results.append("\nNothing to delete or reset.")

        results.append("\n✓ Cleanup complete. Dispatcharr will re-fetch episodes on next series view.")
        LOGGER.info("Database cleanup completed successfully")

    except Exception as e:
        LOGGER.exception("Database cleanup failed")
        results.append(f"\nERROR: {e}")

    return "\n".join(results)


def _stats_only(rows: List[List[str]], base_url: str, root: Path, write_nfos: bool) -> None:
    movies = _eligible_movie_queryset().count()
    series_qs = _eligible_series_queryset()
    series = series_qs.count()
    episodes = Episode.objects.filter(series_id__in=series_qs.values_list("id", flat=True)).count()

    rows.append(["stats", "", "", "movies", "", "", "", "", "info", str(movies)])
    rows.append(["stats", "", "", "series", "", "", "", "", "info", str(series)])
    rows.append(["stats", "", "", "episodes", "", "", "", "", "info", str(episodes)])

    movies_strm = len(list((root / "Movies").rglob("*.strm"))) if (root / "Movies").exists() else 0
    tv_strm = len(list((root / "TV").rglob("*.strm"))) if (root / "TV").exists() else 0
    nfos = len(list(root.rglob("*.nfo"))) if root.exists() else 0

    rows.append(["stats", "", "", "fs_movies_strm", "", "", "", "", "info", str(movies_strm)])
    rows.append(["stats", "", "", "fs_tv_strm", "", "", "", "", "info", str(tv_strm)])
    rows.append(["stats", "", "", "fs_nfos", "", "", "", "", "info", str(nfos)])


# -------------------- Plugin Class --------------------

class Plugin:
    name = "vod2strm"
    version = "0.0.8"
    description = "Generate .strm and NFO files for Movies & Series from the Dispatcharr DB, with cleanup and CSV reports."

    fields = [
        {
            "id": "output_root",
            "label": "Output Root Folder",
            "type": "string",
            "default": DEFAULT_ROOT,
            "help": "Where to write STRM/NFO files (e.g., /data/STRM).",
            "required": True,
        },
        {
            "id": "base_url",
            "label": "Base URL (for .strm)",
            "type": "string",
            "default": DEFAULT_BASE_URL,
            "help": "e.g., http://192.168.199.10:9191",
            "required": True,
        },
        {
            "id": "write_nfos",
            "label": "Write NFO files",
            "type": "boolean",
            "default": True,
        },
        {
            "id": "cleanup_mode",
            "label": "Cleanup",
            "type": "select",
            "options": [
                {"value": CLEANUP_OFF, "label": "Off"},
                {"value": CLEANUP_PREVIEW, "label": "Preview (report only)"},
                {"value": CLEANUP_APPLY, "label": "Apply (delete stale files)"},
            ],
            "default": CLEANUP_OFF,
        },
        {
            "id": "concurrency",
            "label": "Max Filesystem Concurrency",
            "type": "number",
            "default": 4,
            "help": "Maximum concurrent file operations (adaptive throttling will adjust based on NAS performance). Lower values protect slower storage.",
        },
        {
            "id": "adaptive_throttle",
            "label": "Adaptive Throttling",
            "type": "boolean",
            "default": True,
            "help": "Automatically reduce concurrency when NAS is slow, increase when fast. Protects against I/O overload.",
        },
        {
            "id": "auto_run_after_vod_refresh",
            "label": "Auto-run after VOD Refresh",
            "type": "boolean",
            "default": False,
            "help": "Automatically generate .strm files after Dispatcharr refreshes VOD content from providers. Uses 30-second debounce to batch multiple refreshes.",
        },
        {
            "id": "debug_logging",
            "label": "Robust debug logging",
            "type": "boolean",
            "default": False,
        },
        {
            "id": "dry_run",
            "label": "Dry Run (simulate without writing)",
            "type": "boolean",
            "default": False,
            "help": "Simulate generation without creating/modifying files. Useful for testing.",
        },
        {
            "id": "name_clean_regex",
            "label": "Name Cleaning Regex",
            "type": "string",
            "default": "",
            "help": "Optional regex to strip patterns from names (e.g., ^(?:EN|TOP)\s*-\s*). Matches are replaced with empty string.",
        },
    ]

    actions = [
        {"id": "db_stats", "label": "Database Statistics"},
        {"id": "stats", "label": "Stats (CSV)"},
        {"id": "generate_movies", "label": "Generate Movies"},
        {"id": "generate_series", "label": "Generate Series"},
        {"id": "generate_all", "label": "Generate All"},
        {"id": "db_cleanup", "label": "🗑️ Delete ALL Episodes"},
    ]

    def run(self, action_id, params, context):
        """
        Dispatcharr calls this when a button is clicked.
        We enqueue a background job (Celery if available, else a thread).

        Args:
            action_id: The action being run (e.g., "generate_movies")
            params: Parameters from the UI (usually empty dict)
            context: Dict with "settings", "logger", and "actions"
        """
        # Extract settings from context (new plugin API)
        settings = context.get("settings", {})
        action = action_id  # Keep same variable name for compatibility


        _configure_file_logger(settings.get("debug_logging", False))

        output_root = Path(settings.get("output_root") or DEFAULT_ROOT)
        base_url = settings.get("base_url") or DEFAULT_BASE_URL
        write_nfos = bool(settings.get("write_nfos", True))
        cleanup_mode = settings.get("cleanup_mode", CLEANUP_OFF)
        concurrency = int(settings.get("concurrency") or 4)
        dry_run = bool(settings.get("dry_run", False))
        adaptive_throttle = bool(settings.get("adaptive_throttle", True))
        clean_regex = (settings.get("name_clean_regex") or "").strip() or None

        LOGGER.info("Action '%s' | root=%s base_url=%s nfos=%s cleanup=%s conc=%s dry_run=%s adaptive=%s regex=%s",
                    action, output_root, base_url, write_nfos, cleanup_mode, concurrency, dry_run, adaptive_throttle, clean_regex)

        _ensure_dirs()
        output_root.mkdir(parents=True, exist_ok=True)

        if action == "db_stats":
            # Database statistics - run synchronously, return immediately
            try:
                stats_text = _db_stats()
                return {"status": "ok", "message": stats_text}
            except Exception as e:
                LOGGER.exception("Database statistics failed")
                return {"status": "error", "message": f"Failed to generate stats: {e}"}

        # Database cleanup (Issue #556) - delete all episodes and episode relations
        if action == "db_cleanup":
            try:
                result_text = _db_cleanup()
                return {"status": "ok", "message": result_text}
            except Exception as e:
                LOGGER.exception("Database cleanup failed")
                return {"status": "error", "message": f"Failed to delete episodes: {e}"}

        if action == "stats":
            self._enqueue("stats", output_root, base_url, write_nfos, cleanup_mode, concurrency, dry_run, adaptive_throttle, clean_regex)
            return {"status": "ok", "message": "Stats job queued. See CSVs in /data/plugins/vod2strm/reports/."}
        if action == "generate_movies":
            self._enqueue("movies", output_root, base_url, write_nfos, cleanup_mode, concurrency, dry_run, adaptive_throttle, clean_regex)
            msg = "Generate Movies job queued (DRY RUN - no files will be written)." if dry_run else "Generate Movies job queued."
            return {"status": "ok", "message": msg}
        if action == "generate_series":
            self._enqueue("series", output_root, base_url, write_nfos, cleanup_mode, concurrency, dry_run, adaptive_throttle, clean_regex)
            msg = "Generate Series job queued (DRY RUN - no files will be written)." if dry_run else "Generate Series job queued."
            return {"status": "ok", "message": msg}
        if action == "generate_all":
            self._enqueue("all", output_root, base_url, write_nfos, cleanup_mode, concurrency, dry_run, adaptive_throttle, clean_regex)
            msg = "Generate All job queued (DRY RUN - no files will be written)." if dry_run else "Generate All job queued."
            return {"status": "ok", "message": msg}

        return {"status": "error", "message": f"Unknown action: {action}"}

    def _enqueue(self, mode, output_root: Path, base_url: str, write_nfos: bool, cleanup_mode: str, concurrency: int, dry_run: bool = False, adaptive_throttle: bool = True, clean_regex: str | None = None):
        args = {
            "mode": mode,
            "output_root": str(output_root),
            "base_url": base_url,
            "write_nfos": write_nfos,
            "cleanup_mode": cleanup_mode,
            "concurrency": concurrency,
            "debug_logging": LOGGER.level <= logging.DEBUG,
            "dry_run": dry_run,
            "adaptive_throttle": adaptive_throttle,
            "clean_regex": clean_regex,
        }

        # REASONING: Threading fallback removed
        # This plugin runs inside Dispatcharr, which requires Celery to function.
        # If Celery is unavailable, Dispatcharr itself won't be running, so there's
        # no scenario where this plugin is active but Celery is down.
        # Removing the threading fallback simplifies the code and removes unnecessary
        # complexity for a scenario that cannot occur in production.

        if not celery_app or celery_run_job is None:
            LOGGER.error("Celery not available - cannot enqueue background task. This should not happen if Dispatcharr is running correctly.")
            return

        # Call the task using delay() - standard Celery pattern
        celery_run_job.delay(args)
        LOGGER.info("Enqueued Celery task run_job(mode=%s, dry_run=%s, adaptive=%s, regex=%s)", mode, dry_run, adaptive_throttle, clean_regex)


# -------------------- Celery task registration --------------------
# Register tasks dynamically with Celery app at module import time
# This ensures tasks are registered in both Django process and Celery workers

# Define and register Celery tasks using @shared_task decorator
# This is the standard Django/Celery pattern that works with autodiscovery

# Only define tasks if shared_task is available
if shared_task is not None:
    @shared_task(name="vod2strm.plugin.run_job")
    def celery_run_job(args: dict):
        """
        Celery task for background STRM generation.
        Worker process imports this module via autodiscovery.
        """
        LOGGER.info("Celery task run_job starting with args: %s", args)
        try:
            _run_job_sync(**args)
            LOGGER.info("Celery task run_job completed successfully")
        except Exception as e:
            LOGGER.error("Celery task run_job failed: %s", e, exc_info=True)
            raise


    @shared_task(name="vod2strm.plugin.generate_all")
    def celery_generate_all():
        """
        Celery task for scheduled STRM generation.
        Worker process imports this module via autodiscovery.
        """
        try:
            from apps.plugins.models import PluginConfig
            plugin_config = PluginConfig.objects.filter(key="vod2strm").first()
            if plugin_config and plugin_config.settings:
                settings = plugin_config.settings
            else:
                settings = {}
        except Exception as e:
            LOGGER.warning("Failed to load plugin settings for scheduled task: %s. Using defaults.", e)
            settings = {}

        LOGGER.info("Celery scheduled task generate_all starting")
        try:
            _run_job_sync(
                mode="all",
                output_root=settings.get("output_root") or DEFAULT_ROOT,
                base_url=settings.get("base_url") or DEFAULT_BASE_URL,
                write_nfos=bool(settings.get("write_nfos", True)),
                cleanup_mode=settings.get("cleanup_mode", CLEANUP_OFF),
                concurrency=int(settings.get("concurrency", 4)),
                adaptive_throttle=bool(settings.get("adaptive_throttle", True)),
                debug_logging=bool(settings.get("debug_logging", False)),
                dry_run=False,  # Scheduled runs always run for real
                clean_regex=(settings.get("name_clean_regex") or "").strip() or None,
            )
            LOGGER.info("Celery scheduled task generate_all completed successfully")
        except Exception as e:
            LOGGER.error("Celery scheduled task generate_all failed: %s", e, exc_info=True)
            raise

    # Add this module to Celery imports so workers will autodiscover our tasks
    if celery_app:
        try:
            plugin_module = 'vod2strm.plugin'
            current_imports = list(celery_app.conf.get('imports', []))
            if plugin_module not in current_imports:
                current_imports.append(plugin_module)
                celery_app.conf.update(imports=current_imports)
                LOGGER.info(f"Added {plugin_module} to Celery imports - tasks will be available after worker restart")
        except Exception as e:
            LOGGER.warning("Failed to add module to Celery imports: %s", e)
else:
    # Celery not available - define placeholder functions
    celery_run_job = None
    celery_generate_all = None


# -------------------- Auto-run after VOD refresh --------------------

# Debounce state for auto-run
_auto_run_debounce_timer = None
_auto_run_lock = threading.Lock()
_auto_run_in_progress = False  # Prevents infinite loop when our own runs create episodes


def _schedule_auto_run_after_vod_refresh():
    """
    Schedule an auto-run with 30-second debounce.
    Multiple VOD refreshes within 30 seconds will only trigger one generation.
    """
    global _auto_run_debounce_timer

    # Load settings to check if auto-run is enabled
    try:
        from apps.plugins.models import PluginConfig
        plugin_config = PluginConfig.objects.filter(key="vod2strm").first()
        if not plugin_config or not plugin_config.settings:
            return

        # Check if plugin is enabled (respects UI disable toggle)
        if not plugin_config.enabled:
            LOGGER.debug("Auto-run skipped: plugin is disabled")
            return

        settings = plugin_config.settings
        if not settings.get("auto_run_after_vod_refresh", False):
            return  # Feature disabled

        with _auto_run_lock:
            # Cancel existing timer
            if _auto_run_debounce_timer:
                _auto_run_debounce_timer.cancel()

            # Schedule new run in 30 seconds
            def run_generation():
                """
                Execute debounced STRM generation job.

                Reloads settings from database to avoid using stale settings captured
                in closure. Re-checks plugin enabled state before executing.
                Sets _auto_run_in_progress flag to prevent signal handlers from
                triggering another auto-run while this one is executing. Flag is
                cleared in finally block to ensure it's reset even on errors.
                """
                global _auto_run_in_progress
                LOGGER.info("Auto-run triggered after VOD refresh (30s debounce elapsed)")
                _auto_run_in_progress = True
                try:
                    # Reload settings from database (avoid stale settings from closure)
                    from apps.plugins.models import PluginConfig
                    plugin_config = PluginConfig.objects.filter(key="vod2strm").first()

                    # Double-check plugin is still enabled before running
                    if not plugin_config or not plugin_config.enabled:
                        LOGGER.info("Auto-run cancelled: plugin was disabled during debounce window")
                        return

                    # Double-check auto-run is still enabled
                    current_settings = plugin_config.settings or {}
                    if not current_settings.get("auto_run_after_vod_refresh", False):
                        LOGGER.info("Auto-run cancelled: feature was disabled during debounce window")
                        return

                    _run_job_sync(
                        mode="all",
                        output_root=current_settings.get("output_root") or DEFAULT_ROOT,
                        base_url=current_settings.get("base_url") or DEFAULT_BASE_URL,
                        write_nfos=bool(current_settings.get("write_nfos", True)),
                        cleanup_mode=current_settings.get("cleanup_mode", CLEANUP_OFF),
                        concurrency=int(current_settings.get("concurrency") or 4),
                        debug_logging=bool(current_settings.get("debug_logging", False)),
                        dry_run=False,
                        adaptive_throttle=bool(current_settings.get("adaptive_throttle", True)),
                        clean_regex=(current_settings.get("name_clean_regex") or "").strip() or None,
                    )
                finally:
                    _auto_run_in_progress = False
                    LOGGER.debug("Auto-run completed, flag cleared")

            _auto_run_debounce_timer = threading.Timer(30.0, run_generation)
            _auto_run_debounce_timer.daemon = True
            _auto_run_debounce_timer.start()
            LOGGER.debug("Auto-run scheduled in 30 seconds (debounced)")

    except Exception as e:
        LOGGER.warning("Failed to schedule auto-run after VOD refresh: %s", e)


# Register signal listeners for VOD content updates
try:
    from django.db.models.signals import post_save, post_delete
    from django.dispatch import receiver

    # Listen for Episode creation (bulk_created signal doesn't fire for all cases)
    @receiver(post_save, sender=Episode)
    def on_episode_saved(sender, instance, created, **kwargs):
        """
        Django signal handler triggered when Episode model is saved.

        Schedules auto-run generation only when NEW episodes are created (not updates).
        Checks _auto_run_in_progress flag to prevent infinite loops when our own
        _maybe_internal_refresh_series() creates episodes during generation.

        Args:
            sender: The model class (Episode)
            instance: The actual Episode instance being saved
            created: Boolean indicating if this is a new row (True) or update (False)
            **kwargs: Additional signal arguments
        """
        # Don't trigger if we're already running (prevents infinite loop when our own
        # _maybe_internal_refresh_series creates episodes)
        if created and not _auto_run_in_progress:
            _schedule_auto_run_after_vod_refresh()

    @receiver(post_save, sender=Movie)
    def on_movie_saved(sender, instance, created, **kwargs):
        """
        Django signal handler triggered when Movie model is saved.

        Schedules auto-run generation only when NEW movies are created (not updates).
        Checks _auto_run_in_progress flag to prevent re-triggering during active runs.

        Args:
            sender: The model class (Movie)
            instance: The actual Movie instance being saved
            created: Boolean indicating if this is a new row (True) or update (False)
            **kwargs: Additional signal arguments
        """
        # Don't trigger if we're already running
        if created and not _auto_run_in_progress:
            _schedule_auto_run_after_vod_refresh()

    LOGGER.info("VOD refresh signal handlers registered")

except ImportError:
    LOGGER.warning("Could not register VOD refresh signal handlers (Django signals not available)")
except Exception as e:
    LOGGER.warning("Failed to register VOD refresh signal handlers: %s", e)
