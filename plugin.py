"""
vod2strm – Dispatcharr Plugin
Version: 0.0.3

Spec:
- ORM (in-process) with Celery background tasks (non-blocking UI).
- Buttons: Stats, Generate Movies, Generate Series, Generate All.
- STRM generation:
  * Movies -> <root>/Movies/{Name} ({Year})/{Name} ({Year}).strm
  * Series -> <root>/TV/{SeriesName (Year) or SeriesName + (year)}/Season {SS or 00}/S{SS}E{EE} - {Title}.strm
  * Season 00 labeled "Season 00 (Specials)".
  * .strm contents use {base_url}/proxy/vod/(movie|episode)/{uuid}
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

from django.conf import settings as dj_settings  # noqa:F401  (kept for future use)
from django.db import transaction  # noqa:F401
from django.db.models import Count
from django.utils.timezone import now  # noqa:F401

# ORM models (plugin runs in-process with the app)
try:
    from apps.vod.models import Movie, Series, Episode
except Exception:  # pragma: no cover
    from vod.models import Movie, Series, Episode  # type: ignore

# Celery (optional; we fall back to threads if not available or not registered)
try:
    from celery import current_app as celery_app
    from celery import shared_task
except Exception:  # pragma: no cover
    celery_app = None  # type: ignore
    shared_task = None  # type: ignore

# -------------------- Constants / Defaults --------------------

DEFAULT_BASE_URL = "http://192.168.199.10:9191"
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
    Structure: {"files": {"/path/to/file.strm": {"uuid": "...", "type": "movie|episode"}}, "version": 1}
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
    Write .strm file only if UUID changed or file doesn't exist in manifest.
    Checks manifest first to avoid disk reads when possible.

    Returns (written, reason)
    reason ∈ {"created", "updated", "cached_skip", "dry_run"}
    """
    path_str = str(path)
    manifest_files = manifest.get("files", {})

    # Check manifest cache first - avoid disk I/O entirely
    cache_matches = False
    with _MANIFEST_LOCK:
        if path_str in manifest_files:
            cached_entry = manifest_files[path_str]
            cache_matches = cached_entry.get("uuid") == uuid and cached_entry.get("type") == file_type

    # If cache matches, verify file exists (outside lock to minimize lock time)
    if cache_matches:
        if path.exists():
            return (False, "cached_skip")
        # Manifest is stale - file was deleted/corrupted
        LOGGER.warning("Manifest entry for %s is stale (file missing); regenerating.", path_str)

    # UUID changed or not in manifest
    if dry_run:
        # Don't write, but report what would happen
        with _MANIFEST_LOCK:
            is_new = path_str not in manifest_files
        return (False, f"dry_run_{'create' if is_new else 'update'}")

    # Write for real
    content = (url + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)

    # Update manifest
    with _MANIFEST_LOCK:
        is_new = path_str not in manifest_files
        manifest_files[path_str] = {"uuid": uuid, "type": file_type}

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

def _nfo_movie(m: Movie) -> bytes:
    # Use name field - title field doesn't exist in Movie model
    fields = {
        "title": m.name or "",
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


def _nfo_season(s: Series, season_number: int) -> bytes:
    xml = io.StringIO()
    xml.write("<season>\n")
    xml.write(f"  <seasonnumber>{season_number}</seasonnumber>\n")
    name = s.name or ""
    year = getattr(s, "year", None)
    xml.write(f"  <tvshowtitle>{_xml_escape(_series_folder_name(name, year))}</tvshowtitle>\n")
    xml.write("</season>\n")
    return xml.getvalue().encode("utf-8")


def _nfo_episode(e: Episode) -> bytes:
    fields = {
        "title": e.name or "",
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

def _make_movie_strm_and_nfo(movie: Movie, base_url: str, root: Path, write_nfos: bool, report_rows: List[List[str]], lock: threading.Lock, manifest: Dict[str, Any], dry_run: bool = False, throttle: AdaptiveThrottle | None = None) -> None:
    # Use name field - title field doesn't exist in Movie model
    movie_name = movie.name or ""
    m_folder = root / "Movies" / _movie_folder_name(movie_name, getattr(movie, "year", None))
    strm_path = m_folder / f"{_movie_folder_name(movie_name, getattr(movie, 'year', None))}.strm"
    url = f"{base_url.rstrip('/')}/proxy/vod/movie/{movie.uuid}"

    # Time the write operation for adaptive throttling
    start_time = time.time()
    wrote, reason = _write_strm_if_changed(strm_path, str(movie.uuid), url, manifest, "movie", dry_run)
    if wrote and throttle:
        throttle.record_write(time.time() - start_time)

    with lock:
        report_rows.append(["movie", "", "", movie_name, getattr(movie, "year", ""), str(movie.uuid), str(strm_path), "", "written" if wrote else "skipped", reason])

    if write_nfos and not dry_run:
        nfo_start = time.time()
        nfo_path = m_folder / "movie.nfo"
        nfo_bytes = _nfo_movie(movie)
        wrote_nfo, nfo_reason = _write_if_changed(nfo_path, nfo_bytes)
        if wrote_nfo and throttle:
            throttle.record_write(time.time() - nfo_start)
        with lock:
            report_rows.append(["movie_nfo", "", "", movie.name or "", getattr(movie, "year", ""), str(movie.uuid), "", str(nfo_path), "written" if wrote_nfo else "skipped", nfo_reason])


def _make_episode_strm_and_nfo(series: Series, episode: Episode, base_url: str, root: Path, write_nfos: bool, report_rows: List[List[str]], lock: threading.Lock, manifest: Dict[str, Any], dry_run: bool = False, throttle: AdaptiveThrottle | None = None, written_seasons: set | None = None) -> None:
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

    s_folder = root / "TV" / _series_folder_name(series.name or "", getattr(series, "year", None))
    season_number = getattr(episode, "season_number", 0) or 0
    e_folder = s_folder / _season_folder_name(season_number)
    strm_name = _episode_filename(episode)
    strm_path = e_folder / strm_name
    url = f"{base_url.rstrip('/')}/proxy/vod/episode/{episode.uuid}"

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
            season_nfo_bytes = _nfo_season(series, season_number)
            wrote_s, reason_s = _write_if_changed(season_nfo_path, season_nfo_bytes)
            if wrote_s and throttle:
                throttle.record_write(time.time() - season_start)
            with lock:
                report_rows.append(["season_nfo", series.name or "", season_number, "", getattr(series, "year", ""), "", "", str(season_nfo_path), "written" if wrote_s else "skipped", reason_s])

        # episode nfo
        ep_start = time.time()
        ep_nfo_path = e_folder / _episode_nfo_filename(episode)
        ep_nfo_bytes = _nfo_episode(episode)
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
    movie_uuids = set(Movie.objects.values_list("uuid", flat=True))
    episode_uuids = set(Episode.objects.values_list("uuid", flat=True))

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
) -> None:
    _configure_file_logger(debug_logging)
    LOGGER.info("RUN START mode=%s root=%s base_url=%s nfos=%s cleanup=%s conc=%s dry_run=%s adaptive=%s",
                mode, output_root, base_url, write_nfos, cleanup_mode, concurrency, dry_run, adaptive_throttle)

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
            _generate_movies(rows, base_url, root, write_nfos, concurrency, dry_run, adaptive_throttle)

        if mode in ("series", "all"):
            _generate_series(rows, base_url, root, write_nfos, concurrency, dry_run, adaptive_throttle)

        if mode == "stats":
            _stats_only(rows, base_url, root, write_nfos)

    except Exception as e:  # pragma: no cover
        LOGGER.exception("Job failed: %s", e)
        rows.append(["error", "", "", "", "", "", "", "", "error", str(e)])

    _csv_writer(report_path, header, rows)
    LOGGER.info("RUN END mode=%s -> report=%s", mode, report_path)


def _generate_movies(rows: List[List[str]], base_url: str, root: Path, write_nfos: bool, concurrency: int, dry_run: bool = False, adaptive_throttle: bool = True) -> None:
    LOGGER.info("Scanning movies... (dry_run=%s, adaptive=%s)", dry_run, adaptive_throttle)
    # Only generate .strm files for movies with active provider relations
    qs = Movie.objects.filter(
        m3u_relations__m3u_account__is_active=True
    ).distinct().only("id", "uuid", "name", "year", "description", "rating", "genre", "tmdb_id", "imdb_id", "logo")
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
                _make_movie_strm_and_nfo(m, base_url, root, write_nfos, rows, lock, manifest, dry_run, throttle)
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


def _maybe_internal_refresh_series(series: Series) -> bool:
    """
    Refresh episodes from providers in user-configured priority order.
    Tries each provider until one successfully returns episodes.
    Calls refresh_series_episodes() directly (synchronous, matching Dispatcharr UI pattern).

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
        # Get all active relations, sorted by user-configured priority
        relations = series.m3u_relations.select_related('m3u_account').filter(
            m3u_account__is_active=True
        ).order_by('-m3u_account__priority', 'id')

        if not relations:
            LOGGER.debug("Series id=%s has no active M3U account relations", series.id)
            return False

        # Workaround for Dispatcharr issue #569: Multiple M3UEpisodeRelation records
        relation_count = relations.count()
        if relation_count > 1:
            LOGGER.warning(
                "Series id=%s has %d M3U relations (Dispatcharr issue #569). Using highest priority provider only.",
                series.id,
                relation_count
            )

        # Try each provider in priority order
        for relation in relations:
            LOGGER.info(
                "Attempting episode refresh for series_id=%s from provider %s (priority %s)",
                series.id,
                relation.m3u_account.name,
                relation.m3u_account.priority
            )

            try:
                # Call refresh directly (synchronous, matching UI pattern)
                # Workaround for Dispatcharr issue #556: Catch duplicate key errors during refresh
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
                        "Episodes may have disappeared during sync. Skipping this provider.",
                        series.id
                    )
                    continue
                else:
                    # Different error - log and try next provider
                    LOGGER.warning(
                        "Episode refresh error for series_id=%s from provider %s: %s",
                        series.id,
                        relation.m3u_account.name,
                        refresh_error
                    )
                    continue

            # Check if we got episodes
            episode_count = Episode.objects.filter(series_id=series.id).count()
            if episode_count > 0:
                LOGGER.info(
                    "Successfully fetched %d episodes from provider %s",
                    episode_count,
                    relation.m3u_account.name
                )
                return True

            LOGGER.warning(
                "Provider %s did not return episodes, trying next provider",
                relation.m3u_account.name
            )

        LOGGER.warning(
            "No provider returned episodes for series_id=%s after trying %d provider(s)",
            series.id,
            relations.count()
        )
        return False

    except Exception as e:  # pragma: no cover
        LOGGER.warning("Episode refresh failed for series_id=%s: %s", series.id, e)
        return False


def _generate_series(rows: List[List[str]], base_url: str, root: Path, write_nfos: bool, concurrency: int, dry_run: bool = False, adaptive_throttle: bool = True) -> None:
    LOGGER.info("Scanning series... (dry_run=%s, adaptive=%s)", dry_run, adaptive_throttle)
    # Only generate .strm files for series with active provider relations
    # Annotate with episode count to avoid N+1 queries (distinct=True prevents inflated counts from join)
    series_qs = Series.objects.filter(
        m3u_relations__m3u_account__is_active=True
    ).annotate(episode_count=Count('episodes', distinct=True)).distinct().only("id", "uuid", "name", "year", "description", "rating", "genre", "tmdb_id", "imdb_id", "logo")
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
            series_folder = root / "TV" / _series_folder_name(s.name or "", getattr(s, "year", None))
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
            eps_query = Episode.objects.filter(
                series_id=s.id,
                m3u_relations__m3u_account__is_active=True
            )

            # Check for duplicate relations (issue #569 detection)
            total_before_distinct = eps_query.count()
            eps = list(eps_query.distinct().only(
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
                    _make_episode_strm_and_nfo(s, e, base_url, root, write_nfos, rows, lock, manifest, dry_run, throttle, written_seasons)

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
    Database cleanup - deletes ALL episodes and episode relations.
    Issue #556 - Quick database cleanup for development/debugging.

    Replicates these SQL commands in Django ORM:
        DELETE FROM vod_episode;
        DELETE FROM vod_m3uepisoderelation;

    Returns:
        Formatted string with operation results
    """
    results = []
    results.append("=== DATABASE CLEANUP (Issue #556) ===\n")

    try:
        # Count what will be deleted
        episode_count = Episode.objects.count()

        results.append(f"Episodes to delete: {episode_count}")

        if episode_count > 0:
            with transaction.atomic():
                # Delete all episodes (cascade deletes will handle M3UEpisodeRelation automatically)
                deleted_count, details = Episode.objects.all().delete()
                results.append(f"\n✓ Deleted {deleted_count} objects:")
                for model, count in details.items():
                    if count > 0:
                        results.append(f"  - {model}: {count}")
        else:
            results.append("\nNothing to delete.")

    except Exception as e:
        LOGGER.exception("Database cleanup failed")
        results.append(f"\nERROR: {e}")

    return "\n".join(results)


def _stats_only(rows: List[List[str]], base_url: str, root: Path, write_nfos: bool) -> None:
    movies = Movie.objects.count()
    series = Series.objects.count()
    episodes = Episode.objects.count()

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
    version = "0.0.3"
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
            "help": "Simulate generation without creating/modifying files. Useful for testing. Scheduled runs ignore this and always run for real.",
        },
        {
            "id": "schedule",
            "label": "Schedule (crontab string or 'daily HH:MM')",
            "type": "string",
            "default": "",
            "help": "Leave blank to disable. Example: 'daily 03:30' or '0 30 3 * * *'",
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

        LOGGER.info("Action '%s' | root=%s base_url=%s nfos=%s cleanup=%s conc=%s dry_run=%s adaptive=%s",
                    action, output_root, base_url, write_nfos, cleanup_mode, concurrency, dry_run, adaptive_throttle)

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
            self._enqueue("stats", output_root, base_url, write_nfos, cleanup_mode, concurrency, dry_run, adaptive_throttle)
            return {"status": "ok", "message": "Stats job queued. See CSVs in /data/plugins/vod2strm/reports/."}
        if action == "generate_movies":
            self._enqueue("movies", output_root, base_url, write_nfos, cleanup_mode, concurrency, dry_run, adaptive_throttle)
            msg = "Generate Movies job queued (DRY RUN - no files will be written)." if dry_run else "Generate Movies job queued."
            return {"status": "ok", "message": msg}
        if action == "generate_series":
            self._enqueue("series", output_root, base_url, write_nfos, cleanup_mode, concurrency, dry_run, adaptive_throttle)
            msg = "Generate Series job queued (DRY RUN - no files will be written)." if dry_run else "Generate Series job queued."
            return {"status": "ok", "message": msg}
        if action == "generate_all":
            self._enqueue("all", output_root, base_url, write_nfos, cleanup_mode, concurrency, dry_run, adaptive_throttle)
            msg = "Generate All job queued (DRY RUN - no files will be written)." if dry_run else "Generate All job queued."
            return {"status": "ok", "message": msg}

        return {"status": "error", "message": f"Unknown action: {action}"}

    def on_settings_saved(self, settings):
        """
        When settings are saved, (re)register a periodic Celery schedule if provided.
        """
        _configure_file_logger(settings.get("debug_logging", False))
        sched = (settings.get("schedule") or "").strip()
        if not sched:
            LOGGER.info("No schedule configured; skipping Celery beat registration.")
            return

        try:
            if celery_app is None:
                LOGGER.warning("Celery app not available; cannot register schedule.")
                return

            beat_key = "vod2strm.periodic_generate_all"
            celery_app.conf.beat_schedule = celery_app.conf.beat_schedule or {}

            if sched.lower().startswith("daily"):
                hhmm = sched.split(" ", 1)[1].strip() if " " in sched else "03:30"
                hour, minute = [int(x) for x in hhmm.split(":")]
                celery_app.conf.beat_schedule[beat_key] = {
                    "task": "vod2strm.plugin.generate_all",
                    "schedule": {"type": "crontab", "hour": hour, "minute": minute},
                    "args": [],
                }
            else:
                parts = sched.split()
                if len(parts) == 6:
                    sec, minute, hour, dom, mon, dow = parts
                elif len(parts) == 5:
                    minute, hour, dom, mon, dow = parts
                    sec = "0"
                else:
                    raise ValueError("Invalid schedule format")
                celery_app.conf.beat_schedule[beat_key] = {
                    "task": "vod2strm.plugin.generate_all",
                    "schedule": {
                        "type": "crontab",
                        "minute": minute, "hour": hour,
                        "day_of_month": dom, "month_of_year": mon, "day_of_week": dow,
                    },
                    "args": [],
                }
            LOGGER.info("Registered Celery beat: %s -> %s", sched, beat_key)
        except Exception as e:  # pragma: no cover
            LOGGER.warning("Failed to register schedule: %s", e)

    def _enqueue(self, mode, output_root: Path, base_url: str, write_nfos: bool, cleanup_mode: str, concurrency: int, dry_run: bool = False, adaptive_throttle: bool = True):
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
        }

        # Try Celery if available
        if celery_app and celery_run_job is not None:
            try:
                # Call the task using delay() - standard Celery pattern
                celery_run_job.delay(args)
                LOGGER.info("Enqueued Celery task run_job(mode=%s, dry_run=%s, adaptive=%s)", mode, dry_run, adaptive_throttle)
                return
            except Exception as e:
                LOGGER.warning("Failed to enqueue Celery task: %s. Falling back to threading.", e)

        # Fallback to background thread
        LOGGER.info("Running in background thread (Celery not available or failed).")
        t = threading.Thread(target=_run_job_sync, name=f"vod2strm-{mode}", kwargs=args, daemon=True)
        t.start()


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

        settings = plugin_config.settings
        if not settings.get("auto_run_after_vod_refresh", False):
            return  # Feature disabled

        with _auto_run_lock:
            # Cancel existing timer
            if _auto_run_debounce_timer:
                _auto_run_debounce_timer.cancel()

            # Schedule new run in 30 seconds
            def run_generation():
                LOGGER.info("Auto-run triggered after VOD refresh (30s debounce elapsed)")
                _run_job_sync(
                    mode="all",
                    output_root=settings.get("output_root") or DEFAULT_ROOT,
                    base_url=settings.get("base_url") or DEFAULT_BASE_URL,
                    write_nfos=bool(settings.get("write_nfos", True)),
                    cleanup_mode=settings.get("cleanup_mode", CLEANUP_OFF),
                    concurrency=int(settings.get("concurrency") or 4),
                    debug_logging=bool(settings.get("debug_logging", False)),
                    dry_run=False,
                    adaptive_throttle=bool(settings.get("adaptive_throttle", True)),
                )

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

    # Listen for Episode creation/updates (bulk_created signal doesn't fire for all cases)
    @receiver(post_save, sender=Episode)
    def on_episode_saved(sender, instance, created, **kwargs):
        """Trigger auto-run when episodes are created/updated"""
        if created:  # Only on new episodes
            _schedule_auto_run_after_vod_refresh()

    @receiver(post_save, sender=Movie)
    def on_movie_saved(sender, instance, created, **kwargs):
        """Trigger auto-run when movies are created/updated"""
        if created:  # Only on new movies
            _schedule_auto_run_after_vod_refresh()

    LOGGER.info("VOD refresh signal handlers registered")

except ImportError:
    LOGGER.warning("Could not register VOD refresh signal handlers (Django signals not available)")
except Exception as e:
    LOGGER.warning("Failed to register VOD refresh signal handlers: %s", e)
