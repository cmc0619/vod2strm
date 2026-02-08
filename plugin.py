#!/usr/bin/env python3
"""
VOD STRM Plugin for Dispatcharr
Generates .strm and .nfo files for VOD content with automatic post-refresh triggering.

Key features:
- Generates .strm files pointing to Dispatcharr's proxy URLs for movies and series episodes
- Generates .nfo metadata files with TMDB/IMDB IDs, plots, ratings, posters
- Pre-populates episode data using Dispatcharr's refresh_series_episodes task
- Series-by-series processing: populate episodes → write files → next series
- Smart file management: only writes when content changes
- Auto-monitor: watches for VOD refresh completion and auto-generates files
- Direct Django ORM access (no raw SQL, no external DB credentials)
"""

import json
import logging
import os
import re
import sys
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from django.db import connection
from django.utils import timezone

# Import Django models - all VOD and relation models are in apps.vod.models
from apps.vod.models import (
    Movie, Series, Episode,
    M3UMovieRelation, M3USeriesRelation, M3UEpisodeRelation,
)
from apps.m3u.models import M3UAccount


# ---------------------------------------------------------------------------
# Module-level monitor thread reference so it survives across run() calls
# ---------------------------------------------------------------------------
_monitor_thread: Optional[threading.Thread] = None
_monitor_stop_event = threading.Event()


class Plugin:
    name = "VOD STRM Generator"
    version = "0.0.14"
    description = (
        "Generate .strm and .nfo files for Dispatcharr VOD content. "
        "Supports auto-run after VOD refresh completes."
    )

    # --------------------------------------------------------------------- #
    #  Settings schema (rendered by Dispatcharr UI)
    # --------------------------------------------------------------------- #
    fields = [
        {
            "id": "output_dir",
            "label": "Output Directory",
            "type": "string",
            "default": "/data/strm_files",
            "help_text": "Directory to write .strm and .nfo files",
        },
        {
            "id": "base_url",
            "label": "Dispatcharr Base URL",
            "type": "string",
            "default": "http://localhost:9191",
            "help_text": "Base URL for Dispatcharr proxy (used in .strm files)",
        },
        {
            "id": "dry_run",
            "label": "Dry Run Mode",
            "type": "boolean",
            "default": False,
            "help_text": (
                "No files written. Limits to 1000 items. "
                "Auto-monitor runs always use real mode."
            ),
        },
        {
            "id": "verbose",
            "label": "Verbose Per-Item Logs",
            "type": "boolean",
            "default": True,
        },
        {
            "id": "debug_log",
            "label": "Debug Logging",
            "type": "boolean",
            "default": False,
            "help_text": "Write detailed debug logs to /data/vod_strm_debug.log",
        },
        {
            "id": "populate_episodes",
            "label": "Pre-populate Episodes",
            "type": "boolean",
            "default": True,
            "help_text": (
                "Automatically populate episode data for each series "
                "before generating .strm files"
            ),
        },
        {
            "id": "monitor_interval",
            "label": "Auto-Monitor Interval (seconds)",
            "type": "number",
            "default": 60,
            "help_text": (
                "How often (in seconds) the auto-monitor checks for "
                "completed VOD refreshes. Default 60."
            ),
        },
    ]

    # --------------------------------------------------------------------- #
    #  Action buttons shown in UI
    # --------------------------------------------------------------------- #
    actions = [
        {"id": "show_stats", "label": "Show Database Statistics"},
        {
            "id": "populate_episodes_only",
            "label": "Populate Episodes Only",
            "description": "Pre-populate episode data for all series without generating files",
        },
        {"id": "write_movies", "label": "Write Movie .STRM Files"},
        {"id": "write_series", "label": "Write Series .STRM Files"},
        {
            "id": "write_all",
            "label": "Write All (Movies + Series)",
            "description": "Process movies first, then series",
        },
        {
            "id": "start_auto_monitor",
            "label": "Start Auto-Monitor",
            "description": (
                "Start background monitor that triggers STRM generation "
                "after each VOD refresh completes"
            ),
            "confirm": {
                "required": True,
                "title": "Start Auto-Monitor?",
                "message": (
                    "This starts a background thread that polls the database "
                    "every N seconds for VOD refresh completions and auto-generates "
                    ".strm/.nfo files. The thread runs until you stop it."
                ),
            },
        },
        {
            "id": "stop_auto_monitor",
            "label": "Stop Auto-Monitor",
            "description": "Stop the background VOD refresh monitor",
        },
        {
            "id": "monitor_status",
            "label": "Monitor Status",
            "description": "Check if the auto-monitor is currently running",
        },
    ]

    # --------------------------------------------------------------------- #
    #  Initialisation
    # --------------------------------------------------------------------- #
    def __init__(self):
        self.last_tmdb_request = 0
        self.debug_logger = None

    # ===================================================================== #
    #  run() — entry-point called by Dispatcharr plugin system
    # ===================================================================== #
    def run(self, action: str, params: dict, context: dict):
        settings = context.get("settings", {})
        logger = context.get("logger") or logging.getLogger("vod_strm")

        try:
            if action == "show_stats":
                return self._action_show_stats(settings, logger)

            elif action == "populate_episodes_only":
                return self._action_populate_episodes(settings, logger)

            elif action == "write_movies":
                return self._action_write_movies(settings, logger)

            elif action == "write_series":
                return self._action_write_series(settings, logger)

            elif action == "write_all":
                return self._action_write_all(settings, logger)

            elif action == "start_auto_monitor":
                return self._action_start_monitor(settings, logger)

            elif action == "stop_auto_monitor":
                return self._action_stop_monitor(logger)

            elif action == "monitor_status":
                return self._action_monitor_status()

            else:
                return {"status": "error", "message": f"Unknown action: {action}"}

        except Exception as e:
            logger.exception("VOD STRM plugin error")
            return {"status": "error", "message": str(e)}

    # ===================================================================== #
    #  Action handlers
    # ===================================================================== #

    def _action_show_stats(self, settings, logger):
        stats = self._get_vod_stats(logger)
        return {"status": "ok", **stats}

    def _action_populate_episodes(self, settings, logger):
        count = self._populate_all_episodes(settings, logger)
        return {"status": "ok", "message": f"Populated episodes for {count} series"}

    def _action_write_movies(self, settings, logger):
        dry = settings.get("dry_run", False)
        result = self._process_movies(settings, logger, dry_run=dry)
        return {"status": "ok", **result}

    def _action_write_series(self, settings, logger):
        dry = settings.get("dry_run", False)
        result = self._process_series(settings, logger, dry_run=dry)
        return {"status": "ok", **result}

    def _action_write_all(self, settings, logger):
        dry = settings.get("dry_run", False)
        m = self._process_movies(settings, logger, dry_run=dry)
        s = self._process_series(settings, logger, dry_run=dry)
        return {
            "status": "ok",
            "movies": m,
            "series": s,
        }

    # ------------------------------------------------------------------ #
    #  Auto-monitor actions
    # ------------------------------------------------------------------ #

    def _action_start_monitor(self, settings, logger):
        global _monitor_thread, _monitor_stop_event

        if _monitor_thread is not None and _monitor_thread.is_alive():
            return {
                "status": "ok",
                "message": "Auto-monitor is already running.",
            }

        _monitor_stop_event.clear()
        interval = int(settings.get("monitor_interval", 60))

        _monitor_thread = threading.Thread(
            target=self._monitor_loop,
            args=(settings, interval),
            daemon=True,
            name="vod-strm-monitor",
        )
        _monitor_thread.start()

        return {
            "status": "ok",
            "message": (
                f"Auto-monitor started. Checking every {interval}s "
                f"for VOD refresh completions."
            ),
        }

    def _action_stop_monitor(self, logger):
        global _monitor_thread, _monitor_stop_event

        if _monitor_thread is None or not _monitor_thread.is_alive():
            return {"status": "ok", "message": "Auto-monitor is not running."}

        _monitor_stop_event.set()
        _monitor_thread.join(timeout=10)
        _monitor_thread = None
        logger.info("Auto-monitor stopped.")
        return {"status": "ok", "message": "Auto-monitor stopped."}

    def _action_monitor_status(self):
        global _monitor_thread
        running = _monitor_thread is not None and _monitor_thread.is_alive()
        return {
            "status": "ok",
            "monitor_running": running,
            "message": "Auto-monitor is RUNNING" if running else "Auto-monitor is STOPPED",
        }

    # ------------------------------------------------------------------ #
    #  Monitor loop (runs in daemon thread)
    # ------------------------------------------------------------------ #

    def _monitor_loop(self, settings, interval: int):
        """
        Periodically check M3UAccount.updated_at for VOD-enabled accounts.
        When an account's updated_at advances past our last-seen timestamp
        AND its last_message indicates a successful VOD refresh, trigger
        STRM/NFO generation for that account's content.

        This approach:
        - Runs in the web process (where plugins are loaded)
        - Reads DB written by the Celery worker (cross-process safe)
        - 2 lightweight queries per check cycle
        - No monkey-patching, no upstream changes
        """
        logger = logging.getLogger("vod_strm.monitor")
        logger.info("Auto-monitor thread started (interval=%ds)", interval)

        # Track when we last saw each account's updated_at
        last_seen: Dict[int, datetime] = {}

        # Initialise last_seen with current timestamps so we don't
        # immediately trigger on existing data
        try:
            for acct in M3UAccount.objects.filter(is_active=True, enable_vod=True):
                if acct.updated_at:
                    last_seen[acct.id] = acct.updated_at
            logger.info(
                "Initialised tracking for %d VOD-enabled accounts", len(last_seen)
            )
        except Exception as e:
            logger.error("Failed to initialise monitor: %s", e)

        while not _monitor_stop_event.is_set():
            _monitor_stop_event.wait(timeout=interval)
            if _monitor_stop_event.is_set():
                break

            try:
                self._monitor_check(settings, logger, last_seen)
            except Exception as e:
                logger.error("Monitor check error: %s", e)

        logger.info("Auto-monitor thread exiting.")

    def _monitor_check(
        self,
        settings: dict,
        logger: logging.Logger,
        last_seen: Dict[int, datetime],
    ):
        """Single check cycle: look for accounts whose VOD refresh completed."""
        accounts = M3UAccount.objects.filter(is_active=True, enable_vod=True)

        for acct in accounts:
            acct_updated = acct.updated_at
            if acct_updated is None:
                continue

            prev = last_seen.get(acct.id)

            # Account is newer than what we last saw
            if prev is None or acct_updated > prev:
                # Check if last_message indicates VOD refresh success
                msg = (acct.last_message or "").lower()
                if self._is_vod_refresh_complete(msg):
                    logger.info(
                        "VOD refresh detected for account '%s' (id=%d). "
                        "Triggering STRM generation.",
                        acct.name,
                        acct.id,
                    )
                    try:
                        self._run_full_generation(settings, logger)
                    except Exception as e:
                        logger.error(
                            "Auto-generation failed for account %d: %s",
                            acct.id,
                            e,
                        )

                # Update tracking regardless (so we don't re-trigger)
                last_seen[acct.id] = acct_updated

    @staticmethod
    def _is_vod_refresh_complete(msg: str) -> bool:
        """
        Heuristic to detect whether an M3UAccount.last_message indicates
        a completed VOD refresh.  Dispatcharr sets messages like:
          - "Batch VOD refresh completed ..."
          - "VOD refresh completed: X movies, Y series"
          - Could also be "success" status
        """
        indicators = [
            "vod refresh completed",
            "vod refresh complete",
            "batch vod refresh",
            "vod content refresh",
        ]
        return any(ind in msg for ind in indicators)

    def _run_full_generation(self, settings: dict, logger: logging.Logger):
        """Run full movies + series generation (used by auto-monitor)."""
        logger.info("=== Auto-monitor: starting full STRM generation ===")
        m = self._process_movies(settings, logger, dry_run=False)
        logger.info("Movies result: %s", m)
        s = self._process_series(settings, logger, dry_run=False)
        logger.info("Series result: %s", s)
        logger.info("=== Auto-monitor: STRM generation complete ===")

    # ===================================================================== #
    #  Database statistics
    # ===================================================================== #

    def _get_vod_stats(self, logger) -> Dict[str, Any]:
        movie_count = Movie.objects.count()
        series_count = Series.objects.count()
        episode_count = Episode.objects.count()
        movie_rel_count = M3UMovieRelation.objects.count()
        series_rel_count = M3USeriesRelation.objects.count()
        episode_rel_count = M3UEpisodeRelation.objects.count()

        # VOD-enabled accounts
        vod_accounts = list(
            M3UAccount.objects.filter(is_active=True, enable_vod=True)
            .values_list("id", "name")
        )

        return {
            "movies": movie_count,
            "series": series_count,
            "episodes": episode_count,
            "movie_relations": movie_rel_count,
            "series_relations": series_rel_count,
            "episode_relations": episode_rel_count,
            "vod_accounts": [
                {"id": aid, "name": aname} for aid, aname in vod_accounts
            ],
        }

    # ===================================================================== #
    #  Episode population (uses Dispatcharr's own task)
    # ===================================================================== #

    def _populate_all_episodes(self, settings, logger) -> int:
        """
        Trigger episode population for every series that has an
        M3USeriesRelation.  Uses Dispatcharr's refresh_series_episodes.
        """
        from apps.vod.tasks import refresh_series_episodes

        relations = (
            M3USeriesRelation.objects
            .filter(m3u_account__is_active=True)
            .select_related("series", "m3u_account")
        )

        count = 0
        for rel in relations:
            series = rel.series
            logger.info(
                "Populating episodes for series '%s' (account=%s)",
                series.name,
                rel.m3u_account.name,
            )
            try:
                refresh_series_episodes(
                    series.id,
                    rel.m3u_account.id,
                    rel.external_series_id,
                )
                count += 1
            except Exception as e:
                logger.error(
                    "Failed to populate episodes for '%s': %s", series.name, e
                )

        return count

    # ===================================================================== #
    #  Movie processing
    # ===================================================================== #

    def _process_movies(self, settings, logger, dry_run=False) -> dict:
        output_dir = settings.get("output_dir", "/data/strm_files")
        base_url = settings.get("base_url", "http://localhost:9191").rstrip("/")
        verbose = settings.get("verbose", True)
        movies_dir = os.path.join(output_dir, "Movies")

        if not dry_run:
            os.makedirs(movies_dir, exist_ok=True)

        # Get all movie relations with active accounts
        relations = (
            M3UMovieRelation.objects
            .filter(m3u_account__is_active=True)
            .select_related("movie", "movie__logo", "m3u_account")
            .order_by("movie__name")
        )

        # Deduplicate by movie id (pick highest-priority account)
        best: Dict[int, M3UMovieRelation] = {}
        for rel in relations:
            mid = rel.movie_id
            if mid not in best or rel.m3u_account.priority > best[mid].m3u_account.priority:
                best[mid] = rel

        total = len(best)
        if dry_run:
            total = min(total, 1000)

        written = 0
        skipped = 0
        errors = 0

        for idx, rel in enumerate(list(best.values())[:total]):
            movie = rel.movie
            try:
                # Build filename-safe title
                title = self._sanitise_filename(movie.name)
                year = movie.year
                if year:
                    # Strip trailing year if already in title
                    title = re.sub(r"\s*\(\d{4}\)\s*$", "", title).strip()
                    folder_name = f"{title} ({year})"
                else:
                    folder_name = title

                movie_folder = os.path.join(movies_dir, folder_name)

                # .strm URL
                strm_url = f"{base_url}/proxy/vod/movie/{movie.uuid}"

                # .nfo content
                nfo_xml = self._build_movie_nfo(movie, rel)

                if not dry_run:
                    os.makedirs(movie_folder, exist_ok=True)
                    strm_path = os.path.join(movie_folder, f"{folder_name}.strm")
                    nfo_path = os.path.join(movie_folder, f"{folder_name}.nfo")

                    self._write_if_changed(strm_path, strm_url)
                    self._write_if_changed(nfo_path, nfo_xml)

                written += 1
                if verbose and idx % 100 == 0:
                    logger.info("Movies progress: %d / %d", idx + 1, total)

            except Exception as e:
                errors += 1
                logger.error("Error processing movie '%s': %s", movie.name, e)

        logger.info(
            "Movies done: %d written, %d skipped, %d errors out of %d",
            written, skipped, errors, total,
        )
        return {
            "total": total,
            "written": written,
            "skipped": skipped,
            "errors": errors,
        }

    # ===================================================================== #
    #  Series processing (series-by-series with episode population)
    # ===================================================================== #

    def _process_series(self, settings, logger, dry_run=False) -> dict:
        output_dir = settings.get("output_dir", "/data/strm_files")
        base_url = settings.get("base_url", "http://localhost:9191").rstrip("/")
        verbose = settings.get("verbose", True)
        populate = settings.get("populate_episodes", True)
        series_dir = os.path.join(output_dir, "TV Shows")

        if not dry_run:
            os.makedirs(series_dir, exist_ok=True)

        # Get all series relations with active accounts
        series_rels = (
            M3USeriesRelation.objects
            .filter(m3u_account__is_active=True)
            .select_related("series", "series__logo", "m3u_account")
            .order_by("series__name")
        )

        # Deduplicate by series id
        best_series: Dict[int, M3USeriesRelation] = {}
        for rel in series_rels:
            sid = rel.series_id
            if sid not in best_series or rel.m3u_account.priority > best_series[sid].m3u_account.priority:
                best_series[sid] = rel

        total_series = len(best_series)
        if dry_run:
            total_series = min(total_series, 50)

        series_written = 0
        episodes_written = 0
        errors = 0

        for idx, srel in enumerate(list(best_series.values())[:total_series]):
            series = srel.series
            try:
                # Step 1: Populate episodes for this series if enabled
                if populate and not dry_run:
                    self._populate_series_episodes(srel, logger)

                # Step 2: Get episodes for this series
                ep_rels = (
                    M3UEpisodeRelation.objects
                    .filter(
                        episode__series=series,
                        m3u_account__is_active=True,
                    )
                    .select_related("episode", "m3u_account")
                    .order_by("episode__season_number", "episode__episode_number")
                )

                if not ep_rels.exists():
                    if verbose:
                        logger.info(
                            "Series '%s' has no episodes, skipping.", series.name
                        )
                    continue

                # Build series folder
                title = self._sanitise_filename(series.name)
                year = series.year
                if year:
                    title = re.sub(r"\s*\(\d{4}\)\s*$", "", title).strip()
                    series_folder_name = f"{title} ({year})"
                else:
                    series_folder_name = title

                series_folder = os.path.join(series_dir, series_folder_name)

                if not dry_run:
                    os.makedirs(series_folder, exist_ok=True)

                    # Write series-level tvshow.nfo
                    nfo_xml = self._build_series_nfo(series, srel)
                    nfo_path = os.path.join(series_folder, "tvshow.nfo")
                    self._write_if_changed(nfo_path, nfo_xml)

                # Step 3: Write episode files
                # Deduplicate episodes
                best_eps: Dict[int, M3UEpisodeRelation] = {}
                for erel in ep_rels:
                    eid = erel.episode_id
                    if eid not in best_eps or erel.m3u_account.priority > best_eps[eid].m3u_account.priority:
                        best_eps[eid] = erel

                for erel in best_eps.values():
                    ep = erel.episode
                    snum = ep.season_number or 0
                    epnum = ep.episode_number or 0

                    season_folder = os.path.join(
                        series_folder, f"Season {snum:02d}"
                    )

                    ep_title = self._sanitise_filename(ep.name or "Episode")
                    ep_filename = (
                        f"{series_folder_name} - "
                        f"S{snum:02d}E{epnum:02d} - {ep_title}"
                    )

                    strm_url = f"{base_url}/proxy/vod/episode/{ep.uuid}"

                    if not dry_run:
                        os.makedirs(season_folder, exist_ok=True)
                        strm_path = os.path.join(
                            season_folder, f"{ep_filename}.strm"
                        )
                        nfo_path = os.path.join(
                            season_folder, f"{ep_filename}.nfo"
                        )
                        nfo_xml = self._build_episode_nfo(ep, erel, series)
                        self._write_if_changed(strm_path, strm_url)
                        self._write_if_changed(nfo_path, nfo_xml)

                    episodes_written += 1

                series_written += 1
                if verbose:
                    logger.info(
                        "Series %d/%d: '%s' — %d episodes",
                        idx + 1,
                        total_series,
                        series.name,
                        len(best_eps),
                    )

            except Exception as e:
                errors += 1
                logger.error("Error processing series '%s': %s", series.name, e)

        logger.info(
            "Series done: %d series, %d episodes, %d errors",
            series_written, episodes_written, errors,
        )
        return {
            "total_series": total_series,
            "series_written": series_written,
            "episodes_written": episodes_written,
            "errors": errors,
        }

    def _populate_series_episodes(self, srel: M3USeriesRelation, logger):
        """Populate episodes for a single series using Dispatcharr's task."""
        try:
            from apps.vod.tasks import refresh_series_episodes

            refresh_series_episodes(
                srel.series.id,
                srel.m3u_account.id,
                srel.external_series_id,
            )
        except Exception as e:
            logger.warning(
                "Could not populate episodes for '%s': %s",
                srel.series.name,
                e,
            )

    # ===================================================================== #
    #  NFO builders
    # ===================================================================== #

    def _build_movie_nfo(self, movie: Movie, rel: M3UMovieRelation) -> str:
        root = ET.Element("movie")
        self._add_text_el(root, "title", movie.name)
        if movie.year:
            self._add_text_el(root, "year", str(movie.year))
        if movie.description:
            self._add_text_el(root, "plot", movie.description)
        if movie.rating:
            self._add_text_el(root, "rating", movie.rating)
        if movie.genre:
            for g in movie.genre.split(","):
                self._add_text_el(root, "genre", g.strip())
        if movie.tmdb_id:
            self._add_text_el(root, "tmdbid", str(movie.tmdb_id))
            uid = ET.SubElement(root, "uniqueid", type="tmdb", default="true")
            uid.text = str(movie.tmdb_id)
        if movie.imdb_id:
            self._add_text_el(root, "imdbid", str(movie.imdb_id))
            uid = ET.SubElement(root, "uniqueid", type="imdb")
            uid.text = str(movie.imdb_id)
        if movie.duration_secs:
            self._add_text_el(
                root, "runtime", str(movie.duration_secs // 60)
            )

        # Poster from logo or custom_properties
        poster_url = self._get_poster_url(movie)
        if poster_url:
            thumb = ET.SubElement(root, "thumb", aspect="poster")
            thumb.text = poster_url

        # Provider info from custom_properties
        cp = rel.custom_properties or {}
        if cp.get("director"):
            self._add_text_el(root, "director", cp["director"])
        if cp.get("cast"):
            cast_str = cp["cast"]
            if isinstance(cast_str, str):
                for actor_name in cast_str.split(","):
                    actor = ET.SubElement(root, "actor")
                    self._add_text_el(actor, "name", actor_name.strip())

        return self._xml_to_string(root)

    def _build_series_nfo(self, series: Series, srel: M3USeriesRelation) -> str:
        root = ET.Element("tvshow")
        self._add_text_el(root, "title", series.name)
        if series.year:
            self._add_text_el(root, "year", str(series.year))
        if series.description:
            self._add_text_el(root, "plot", series.description)
        if series.rating:
            self._add_text_el(root, "rating", series.rating)
        if series.genre:
            for g in series.genre.split(","):
                self._add_text_el(root, "genre", g.strip())
        if series.tmdb_id:
            self._add_text_el(root, "tmdbid", str(series.tmdb_id))
            uid = ET.SubElement(root, "uniqueid", type="tmdb", default="true")
            uid.text = str(series.tmdb_id)
        if series.imdb_id:
            self._add_text_el(root, "imdbid", str(series.imdb_id))
            uid = ET.SubElement(root, "uniqueid", type="imdb")
            uid.text = str(series.imdb_id)

        poster_url = self._get_poster_url(series)
        if poster_url:
            thumb = ET.SubElement(root, "thumb", aspect="poster")
            thumb.text = poster_url

        # Provider custom properties
        cp = srel.custom_properties or {}
        if cp.get("cast"):
            cast_str = cp["cast"]
            if isinstance(cast_str, str):
                for actor_name in cast_str.split(","):
                    actor = ET.SubElement(root, "actor")
                    self._add_text_el(actor, "name", actor_name.strip())

        return self._xml_to_string(root)

    def _build_episode_nfo(
        self, ep: Episode, erel: M3UEpisodeRelation, series: Series
    ) -> str:
        root = ET.Element("episodedetails")
        self._add_text_el(root, "title", ep.name)
        self._add_text_el(root, "showtitle", series.name)
        if ep.season_number is not None:
            self._add_text_el(root, "season", str(ep.season_number))
        if ep.episode_number is not None:
            self._add_text_el(root, "episode", str(ep.episode_number))
        if ep.description:
            self._add_text_el(root, "plot", ep.description)
        if ep.air_date:
            self._add_text_el(root, "aired", str(ep.air_date))
        if ep.rating:
            self._add_text_el(root, "rating", ep.rating)
        if ep.duration_secs:
            self._add_text_el(root, "runtime", str(ep.duration_secs // 60))
        if ep.tmdb_id:
            uid = ET.SubElement(root, "uniqueid", type="tmdb", default="true")
            uid.text = str(ep.tmdb_id)
        if ep.imdb_id:
            uid = ET.SubElement(root, "uniqueid", type="imdb")
            uid.text = str(ep.imdb_id)

        # Episode thumbnail from custom_properties
        cp = ep.custom_properties or {}
        if cp.get("info", {}).get("movie_image"):
            thumb = ET.SubElement(root, "thumb")
            thumb.text = cp["info"]["movie_image"]

        return self._xml_to_string(root)

    # ===================================================================== #
    #  Utility helpers
    # ===================================================================== #

    @staticmethod
    def _get_poster_url(obj) -> Optional[str]:
        """Extract poster URL from a Movie or Series object."""
        # Try the logo FK first
        if hasattr(obj, "logo") and obj.logo:
            return obj.logo.url
        # Fallback to custom_properties
        cp = obj.custom_properties or {}
        if cp.get("cover"):
            return cp["cover"]
        if cp.get("cover_big"):
            return cp["cover_big"]
        if cp.get("stream_icon"):
            return cp["stream_icon"]
        return None

    @staticmethod
    def _sanitise_filename(name: str) -> str:
        """Remove or replace characters unsafe for filenames."""
        # Replace path-unsafe chars
        name = re.sub(r'[<>:"/\\|?*]', "", name)
        # Collapse whitespace
        name = re.sub(r"\s+", " ", name).strip()
        # Truncate
        if len(name) > 200:
            name = name[:200]
        return name

    @staticmethod
    def _write_if_changed(path: str, content: str) -> bool:
        """Write file only if content differs (avoids unnecessary I/O)."""
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    if f.read() == content:
                        return False
        except Exception:
            pass
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return True

    @staticmethod
    def _add_text_el(parent: ET.Element, tag: str, text: str):
        el = ET.SubElement(parent, tag)
        el.text = text
        return el

    @staticmethod
    def _xml_to_string(root: ET.Element) -> str:
        """Pretty-print XML with declaration."""
        ET.indent(root)
        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            + ET.tostring(root, encoding="unicode")
        )
