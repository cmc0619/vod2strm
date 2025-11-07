"""
vod2strm – Dispatcharr Plugin
Version: 0.0.1

Spec:
- ORM (in-process) with Celery background tasks (non-blocking UI).
- Buttons: Stats, Generate Movies, Generate Series, Generate All.
- STRM generation:
  * Movies -> <root>/Movies/{Name} ({Year})/{Name} ({Year}).strm
  * Series -> <root>/TV/{SeriesName (Year) or SeriesName + (year)}/Season {SS or 00}/S{SS}E{EE} - {Title}.strm
  * Season 00 labeled "Season 00 (Specials)".
  * .strm contents use {base_url}/proxy/vod/(movies|episodes)/{uuid}
- NFO generation (compare-before-write):
  * Movies: movie.nfo in movie folder
  * Seasons: season.nfo per season folder
  * Episodes: SxxExx.nfo next to episode file
- Cleanup (preview/apply) of stale files/folders.
- CSV reports -> /data/plugins/vod2strm/reports/
- Robust debug logging -> /data/plugins/vod2strm/logs/
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import logging.handlers
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Tuple

from django.conf import settings as dj_settings  # noqa:F401  (kept for future use)
from django.db import transaction  # noqa:F401
from django.utils.timezone import now  # noqa:F401

# ORM models (plugin runs in-process with the app)
try:
    from apps.vod.models import Movie, Series, Episode
except Exception:  # pragma: no cover
    from vod.models import Movie, Series, Episode  # type: ignore

# Celery (optional; we fall back to threads if not available or not registered)
try:
    from celery import current_app as celery_app
except Exception:  # pragma: no cover
    celery_app = None  # type: ignore

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
    if year:
        return _norm_fs_name(f"{name} ({year})")
    return _norm_fs_name(name or "Unknown Movie")


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

def _make_movie_strm_and_nfo(movie: Movie, base_url: str, root: Path, write_nfos: bool, report_rows: List[List[str]], lock: threading.Lock) -> None:
    m_folder = root / "Movies" / _movie_folder_name(movie.name or "", getattr(movie, "year", None))
    strm_path = m_folder / f"{_movie_folder_name(movie.name or '', getattr(movie, 'year', None))}.strm"
    url = f"{base_url.rstrip('/')}/proxy/vod/movie/{movie.uuid}"
    wrote, reason = _write_if_changed(strm_path, (url + "\n").encode("utf-8"))
    with lock:
        report_rows.append(["movie", "", "", movie.name or "", getattr(movie, "year", ""), str(movie.uuid), str(strm_path), "", "written" if wrote else "skipped", reason])

    if write_nfos:
        nfo_path = m_folder / "movie.nfo"
        nfo_bytes = _nfo_movie(movie)
        wrote_nfo, nfo_reason = _write_if_changed(nfo_path, nfo_bytes)
        with lock:
            report_rows.append(["movie_nfo", "", "", movie.name or "", getattr(movie, "year", ""), str(movie.uuid), "", str(nfo_path), "written" if wrote_nfo else "skipped", nfo_reason])


def _make_episode_strm_and_nfo(series: Series, episode: Episode, base_url: str, root: Path, write_nfos: bool, report_rows: List[List[str]], lock: threading.Lock) -> None:
    s_folder = root / "TV" / _series_folder_name(series.name or "", getattr(series, "year", None))
    season_number = getattr(episode, "season_number", 0) or 0
    e_folder = s_folder / _season_folder_name(season_number)
    strm_name = _episode_filename(episode)
    strm_path = e_folder / strm_name
    url = f"{base_url.rstrip('/')}/proxy/vod/episode/{episode.uuid}"
    wrote, reason = _write_if_changed(strm_path, (url + "\n").encode("utf-8"))
    title = getattr(episode, "name", "") or ""
    with lock:
        report_rows.append(["episode", series.name or "", season_number, title, getattr(series, "year", ""), str(episode.uuid), str(strm_path), "", "written" if wrote else "skipped", reason])

    if write_nfos:
        # season.nfo
        season_nfo_path = e_folder / "season.nfo"
        season_nfo_bytes = _nfo_season(series, season_number)
        wrote_s, reason_s = _write_if_changed(season_nfo_path, season_nfo_bytes)
        with lock:
            report_rows.append(["season_nfo", series.name or "", season_number, "", getattr(series, "year", ""), "", "", str(season_nfo_path), "written" if wrote_s else "skipped", reason_s])

        # episode nfo
        ep_nfo_path = e_folder / _episode_nfo_filename(episode)
        ep_nfo_bytes = _nfo_episode(episode)
        wrote_e, reason_e = _write_if_changed(ep_nfo_path, ep_nfo_bytes)
        with lock:
            report_rows.append(["episode_nfo", series.name or "", season_number, title, getattr(series, "year", ""), str(episode.uuid), "", str(ep_nfo_path), "written" if wrote_e else "skipped", reason_e])


# -------------------- Cleanup --------------------

def _cleanup(rows: List[List[str]], root: Path, apply: bool) -> None:
    """
    Identify and optionally remove stale *.strm files that reference UUIDs not present in DB.
    """
    LOGGER.info("Cleanup started (apply=%s)", apply)
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
                p.unlink()
                rows.append(["cleanup", "", "", p.name, "", uid, str(p), "", "deleted", f"stale_{typ}"])
            except Exception as e:
                rows.append(["cleanup", "", "", p.name, "", uid, str(p), "", "error", f"delete_failed: {e}"])
        else:
            rows.append(["cleanup", "", "", p.name, "", uid, str(p), "", "would_delete", f"stale_{typ}"])

    # prune empty dirs in apply mode
    if apply:
        pruned = 0
        # Walk deepest-first
        for d in sorted({p.parent for p in stale_paths}, key=lambda x: len(str(x)), reverse=True):
            try:
                cur = d
                while cur != root and cur.exists() and not any(cur.iterdir()):
                    cur.rmdir()
                    pruned += 1
                    cur = cur.parent
            except Exception:
                pass
        rows.append(["cleanup", "", "", "", "", "", "", "", "pruned_dirs", str(pruned)])
    LOGGER.info("Cleanup finished (apply=%s)", apply)


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
) -> None:
    _configure_file_logger(debug_logging)
    LOGGER.info("RUN START mode=%s root=%s base_url=%s nfos=%s cleanup=%s conc=%s",
                mode, output_root, base_url, write_nfos, cleanup_mode, concurrency)

    root = Path(output_root)
    ts = _ts()
    report_path = Path(REPORT_ROOT) / f"report_{mode}_{ts}.csv"
    header = ["type", "series_name", "season", "title", "year", "db_uuid", "strm_path", "nfo_path", "action", "reason"]
    rows: List[List[str]] = []

    try:
        if mode in ("movies", "all"):
            _generate_movies(rows, base_url, root, write_nfos, concurrency)

        if mode in ("series", "all"):
            _generate_series(rows, base_url, root, write_nfos, concurrency)

        if mode == "stats":
            _stats_only(rows, base_url, root, write_nfos)

        if cleanup_mode in (CLEANUP_PREVIEW, CLEANUP_APPLY):
            _cleanup(rows, root, apply=(cleanup_mode == CLEANUP_APPLY))

    except Exception as e:  # pragma: no cover
        LOGGER.exception("Job failed: %s", e)
        rows.append(["error", "", "", "", "", "", "", "", "error", str(e)])

    _csv_writer(report_path, header, rows)
    LOGGER.info("RUN END mode=%s -> report=%s", mode, report_path)


def _generate_movies(rows: List[List[str]], base_url: str, root: Path, write_nfos: bool, concurrency: int) -> None:
    LOGGER.info("Scanning movies...")
    qs = Movie.objects.all().only("id", "uuid", "name", "year", "description", "rating", "genre", "tmdb_id", "imdb_id", "logo")
    work = list(qs)
    LOGGER.info("Movies to process: %d", len(work))
    lock = threading.Lock()

    def job(m: Movie) -> None:
        try:
            _make_movie_strm_and_nfo(m, base_url, root, write_nfos, rows, lock)
        except Exception as e:
            LOGGER.warning("Movie id=%s failed: %s", m.id, e)
            with lock:
                rows.append(["movie", "", "", m.name or "", getattr(m, "year", ""), str(m.uuid), "", "", "error", str(e)])

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
        list(ex.map(job, work))


def _maybe_internal_refresh_series(series: Series) -> bool:
    """
    Best-effort: call an internal Celery task that refreshes/hydrates VOD episodes, if available.
    Returns True if we *requested* a refresh (not necessarily that it finished).
    """
    try:
        if celery_app is None:
            LOGGER.debug("Celery app not available; skipping internal refresh.")
            return False
        # Candidate task names (adjust if your app uses a specific one)
        task_names = [
            "apps.vod.tasks.refresh_series_episodes",
            "apps.vod.tasks.refresh_vod_for_series",
            "vod.tasks.refresh_series_episodes",
            "vod.tasks.refresh_vod_for_series",
            "apps.vod.tasks.refresh_vod",
        ]
        for tname in task_names:
            if tname in celery_app.tasks:
                celery_app.send_task(tname, args=[series.id], kwargs={}, queue="default")
                LOGGER.info("Requested internal episode refresh via '%s' for series_id=%s", tname, series.id)
                return True
        LOGGER.debug("No known VOD refresh task registered in Celery; skipping internal refresh.")
        return False
    except Exception as e:  # pragma: no cover
        LOGGER.warning("Internal refresh attempt failed for series_id=%s: %s", series.id, e)
        return False


def _generate_series(rows: List[List[str]], base_url: str, root: Path, write_nfos: bool, concurrency: int) -> None:
    LOGGER.info("Scanning series...")
    series_qs = Series.objects.all().only("id", "uuid", "name", "year", "description", "rating", "genre", "tmdb_id", "imdb_id", "logo")
    total = series_qs.count()
    LOGGER.info("Series to process: %d", total)
    lock = threading.Lock()

    for s in series_qs.iterator(chunk_size=200):
        try:
            series_folder = root / "TV" / _series_folder_name(s.name or "", getattr(s, "year", None))
            expected = _series_expected_count(s.id)

            if expected == 0:
                LOGGER.debug("Series id=%s has 0 episodes; attempting internal refresh then retry.", s.id)
                requested = _maybe_internal_refresh_series(s)
                if requested:
                    time.sleep(2.0)
                expected = _series_expected_count(s.id)

            if expected > 0 and _compare_tree_quick(series_folder, expected, write_nfos):
                with lock:
                    rows.append(["series", s.name or "", "", "", getattr(s, "year", ""), str(s.uuid), str(series_folder), "", "skipped", "tree_complete"])
                continue

            eps = list(Episode.objects.filter(series_id=s.id).only(
                "id", "uuid", "name", "season_number", "episode_number",
                "air_date", "description", "rating", "tmdb_id", "imdb_id"
            ).order_by("season_number", "episode_number"))

            if not eps:
                with lock:
                    rows.append(["series", s.name or "", "", "", getattr(s, "year", ""), str(s.uuid), str(series_folder), "", "skipped", "no_episodes"])
                continue

            def job(e: Episode) -> None:
                _make_episode_strm_and_nfo(s, e, base_url, root, write_nfos, rows, lock)

            with ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
                list(ex.map(job, eps))

        except Exception as e:
            LOGGER.warning("Series id=%s failed: %s", getattr(s, "id", "?"), e)
            with lock:
                rows.append(["series", s.name or "", "", "", getattr(s, "year", ""), str(getattr(s, "uuid", "")), "", "", "error", str(e)])


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
    version = "0.0.1"
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
            "label": "Filesystem concurrency",
            "type": "number",
            "default": 12,
            "help": "Number of concurrent file operations.",
        },
        {
            "id": "debug_logging",
            "label": "Robust debug logging",
            "type": "boolean",
            "default": False,
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
        {"id": "stats", "label": "Stats (CSV)"},
        {"id": "generate_movies", "label": "Generate Movies"},
        {"id": "generate_series", "label": "Generate Series"},
        {"id": "generate_all", "label": "Generate All"},
    ]

    def run(self, action: str, params: dict, context: dict):
        """
        Dispatcharr calls this when a button is clicked.
        We enqueue a background job (Celery if available, else a thread).
        """
        settings = context.get("settings", {})
        _configure_file_logger(settings.get("debug_logging", False))

        output_root = Path(settings.get("output_root") or DEFAULT_ROOT)
        base_url = settings.get("base_url") or DEFAULT_BASE_URL
        write_nfos = bool(settings.get("write_nfos", True))
        cleanup_mode = settings.get("cleanup_mode", CLEANUP_OFF)
        concurrency = int(settings.get("concurrency") or 12)

        LOGGER.info("Action '%s' | root=%s base_url=%s nfos=%s cleanup=%s conc=%s",
                    action, output_root, base_url, write_nfos, cleanup_mode, concurrency)

        _ensure_dirs()
        output_root.mkdir(parents=True, exist_ok=True)

        if action == "stats":
            self._enqueue("stats", output_root, base_url, write_nfos, cleanup_mode, concurrency)
            return {"status": "ok", "message": "Stats job queued. See CSVs in /data/plugins/vod2strm/reports/."}
        if action == "generate_movies":
            self._enqueue("movies", output_root, base_url, write_nfos, cleanup_mode, concurrency)
            return {"status": "ok", "message": "Generate Movies job queued."}
        if action == "generate_series":
            self._enqueue("series", output_root, base_url, write_nfos, cleanup_mode, concurrency)
            return {"status": "ok", "message": "Generate Series job queued."}
        if action == "generate_all":
            self._enqueue("all", output_root, base_url, write_nfos, cleanup_mode, concurrency)
            return {"status": "ok", "message": "Generate All job queued."}

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
                    "task": "plugins.vod2strm.tasks.generate_all",
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
                    "task": "plugins.vod2strm.tasks.generate_all",
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

    def _enqueue(self, mode, output_root: Path, base_url: str, write_nfos: bool, cleanup_mode: str, concurrency: int):
        args = {
            "mode": mode,
            "output_root": str(output_root),
            "base_url": base_url,
            "write_nfos": write_nfos,
            "cleanup_mode": cleanup_mode,
            "concurrency": concurrency,
            "debug_logging": LOGGER.level <= logging.DEBUG,
        }

        # Prefer Celery if our task is registered
        if celery_app and "plugins.vod2strm.tasks.run_job" in celery_app.tasks:
            celery_app.send_task("plugins.vod2strm.tasks.run_job", args=[args], queue="default")
            LOGGER.info("Enqueued Celery task run_job(mode=%s)", mode)
            return

        # Fallback to background thread
        LOGGER.warning("Celery task not registered; running in a background thread (fallback).")
        t = threading.Thread(target=_run_job_sync, name=f"vod2strm-{mode}", kwargs=args, daemon=True)
        t.start()


# -------------------- Celery task registration --------------------

if celery_app:
    @celery_app.task(name="plugins.vod2strm.tasks.run_job")
    def celery_run_job(args: dict):
        _run_job_sync(**args)

    @celery_app.task(name="plugins.vod2strm.tasks.generate_all")
    def celery_generate_all():
        # Load settings from database for scheduled task
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

        _run_job_sync(
            mode="all",
            output_root=settings.get("output_root") or DEFAULT_ROOT,
            base_url=settings.get("base_url") or DEFAULT_BASE_URL,
            write_nfos=bool(settings.get("write_nfos", True)),
            cleanup_mode=settings.get("cleanup_mode", CLEANUP_OFF),
            concurrency=int(settings.get("concurrency") or 12),
            debug_logging=bool(settings.get("debug_logging", False)),
        )
