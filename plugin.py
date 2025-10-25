#!/usr/bin/env python3
"""
VOD STRM Plugin for Dispatcharr - ThreadPool Version
Generates .strm and .nfo files for VOD content with threaded file operations
"""

import json
import logging
import os
import re
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from django.db import connection

# Import Django models - all VOD and relation models are in apps.vod.models
from apps.vod.models import Movie, Series, Episode, M3UMovieRelation, M3USeriesRelation, M3UEpisodeRelation
from apps.m3u.models import M3UAccount

class Plugin:
    name = "VOD STRM Generator (ThreadPool)"
    version = "0.0.1-threadpool"
    description = "Generate .strm and .nfo files for Dispatcharr VOD content with threaded file operations"

    fields = [
        {"id": "output_dir", "label": "Output Directory", "type": "string", "default": "/data/strm_files", "help_text": "Directory to write .strm and .nfo files"},
        {"id": "base_url", "label": "Dispatcharr Base URL", "type": "string", "default": "http://localhost:9191", "help_text": "Base URL for Dispatcharr (used in .strm files)"},
        {"id": "dry_run", "label": "Dry Run Mode", "type": "boolean", "default": False, "help_text": "When enabled, no files are written. Limits processing to 1000 items. Note: Scheduled runs always run for real."},
        {"id": "verbose", "label": "Verbose Per-Item Logs", "type": "boolean", "default": True},
        {"id": "debug_log", "label": "Debug Logging", "type": "boolean", "default": False, "help_text": "Write detailed debug logs to /data/vod_strm_debug.log (file is overwritten each run)"},
        {"id": "populate_episodes", "label": "Pre-populate Episodes", "type": "boolean", "default": True, "help_text": "Automatically populate episode data for all series before generating files"},
        {"id": "max_workers", "label": "Max File Writer Threads", "type": "number", "default": 4, "help_text": "Number of concurrent file writing threads (1-10)"},
        {"id": "batch_size", "label": "Batch Size", "type": "number", "default": 100, "help_text": "Number of files to process in each batch"},
        {"id": "throttle_delay", "label": "Throttle Delay (ms)", "type": "number", "default": 10, "help_text": "Delay between batches in milliseconds"},
    ]

    actions = [
        {"id": "show_stats", "label": "Show Database Statistics"},
        {"id": "populate_episodes_only", "label": "Populate Episodes Only", "description": "Pre-populate episode data for all series without generating files"},
        {"id": "write_movies", "label": "Write Movie .STRM Files"},
        {"id": "write_series", "label": "Write Series .STRM Files"},
    ]

    def __init__(self):
        self.settings_file = "/data/vod_strm_settings.json"
        self.debug_log_file = "/data/vod_strm_debug.log"
        self.debug_logger = None
        # Shared HTTP session with optimizations
        self.http = self._make_http_session()
        self._load_settings()

    def _make_http_session(self) -> requests.Session:
        """Create optimized HTTP session with retries and proper User-Agent."""
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()
        session.headers.update({"User-Agent": f"dispatcharr-vodstrmpg/{self.version} (+STRM+NFO)"})

        # Setup retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _save_settings(self, settings: Dict[str, Any]) -> None:
        """Save settings to file for scheduler persistence."""
        try:
            with open(self.settings_file, "w", encoding="utf-8") as f:
                json.dump(settings, f, indent=2)
        except Exception:
            pass

    def _load_settings(self) -> Dict[str, Any]:
        """Load settings from file."""
        try:
            with open(self.settings_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def _get_db_connection(self):
        """Use Django's existing database connection."""
        return connection

    def populate_all_episodes(self, logger):
        """Pre-populate episodes for all series that don't have them yet"""
        try:
            from apps.vod.models import Series, M3USeriesRelation
            from apps.vod.tasks import refresh_series_episodes

            logger.info("Starting episode population for all series...")

            # Get all series that need episode data
            series_relations = M3USeriesRelation.objects.filter(
                m3u_account__is_active=True
            ).select_related('series', 'm3u_account')

            total_series = series_relations.count()
            processed = 0
            populated = 0

            logger.info(f"Found {total_series} active series relationships to check")

            for relation in series_relations:
                try:
                    series = relation.series
                    account = relation.m3u_account
                    external_series_id = relation.external_series_id

                    # Check if episodes already populated
                    custom_props = relation.custom_properties or {}
                    episodes_fetched = custom_props.get('episodes_fetched', False)

                    if not episodes_fetched:
                        logger.info(f"Populating episodes for: {series.name} ({processed+1}/{total_series})")

                        # This is the magic call that populates episodes!
                        refresh_series_episodes(account, series, external_series_id)

                        # Refresh objects from database
                        series.refresh_from_db()
                        relation.refresh_from_db()

                        logger.info(f"  ✓ Episodes populated for {series.name}")
                        populated += 1
                    else:
                        logger.debug(f"  ↷ Episodes already exist for {series.name}")

                except Exception as e:
                    logger.error(f"  ✗ Error processing {series.name}: {e}")

                processed += 1

            logger.info(f"Episode population complete! Processed {processed} series, populated {populated} new series.")
            return {"processed": processed, "populated": populated}

        except Exception as e:
            logger.error(f"Error during episode population: {e}")
            raise

    def _get_debug_logger(self):
        """Create debug logger that writes to file."""
        debug_logger = logging.getLogger("vod_strm_debug")
        debug_logger.setLevel(logging.DEBUG)
        # Clear existing handlers
        debug_logger.handlers.clear()
        # Create file handler (overwrite mode)
        handler = logging.FileHandler(self.debug_log_file, mode='w', encoding='utf-8')
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        debug_logger.addHandler(handler)
        debug_logger.propagate = False
        return debug_logger

    def _get_vod_stats(self, logger) -> Dict[str, Any]:
        """Get VOD statistics from database using Django ORM."""
        try:
            # Get movie counts
            total_movies = Movie.objects.count()
            imported_movies = Movie.objects.filter(
                m3u_relations__m3u_account__is_active=True
            ).distinct().count()

            # Get series counts
            total_series = Series.objects.count()
            imported_series = Series.objects.filter(
                m3u_relations__m3u_account__is_active=True
            ).distinct().count()

            # Get episode counts
            total_episodes = Episode.objects.count()
            imported_episodes = Episode.objects.filter(
                m3u_relations__m3u_account__is_active=True
            ).distinct().count()

            # Get active providers
            providers = list(M3UAccount.objects.filter(
                is_active=True,
                enable_vod=True
            ).values('id', 'name').order_by('name'))

            return {
                'total_movies': total_movies,
                'imported_movies': imported_movies,
                'total_series': total_series,
                'imported_series': imported_series,
                'total_episodes': total_episodes,
                'imported_episodes': imported_episodes,
                'providers': providers
            }

        except Exception as e:
            logger.error(f"Error getting VOD stats: {e}")
            return {}

    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename by removing invalid characters."""
        # Remove or replace invalid characters
        filename = re.sub(r'[<>:"/\\|?*]', '', filename)
        filename = re.sub(r'[^\w\s\-_\.]', '', filename)
        filename = re.sub(r'\s+', ' ', filename).strip()
        return filename[:200]  # Limit length

    def _escape_xml(self, text: str) -> str:
        """Escape XML special characters."""
        if not text:
            return ""
        return (str(text)
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
                .replace("'", "&apos;"))

    def _should_write_file(self, file_path: Path, new_content: str) -> bool:
        """Check if file should be written (doesn't exist or content differs)."""
        if not file_path.exists():
            return True
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                existing_content = f.read()
            return existing_content != new_content
        except Exception:
            # If we can't read the file, assume we should write it
            return True

    def _create_nfo_content(self, content_data: Dict[str, Any], content_type: str) -> str:
        """Create NFO content for movies or TV shows."""
        basic_data = content_data.get('basic_data', {})

        # Get data with fallbacks
        title = basic_data.get('name', content_data.get('name', 'Unknown'))
        plot = basic_data.get('plot', content_data.get('description', ''))
        year = basic_data.get('year', content_data.get('year'))
        rating = basic_data.get('rating', content_data.get('rating'))
        genre = basic_data.get('genre', content_data.get('genre', ''))

        if content_type == 'movie':
            root_tag = 'movie'
        else:  # tv show
            root_tag = 'tvshow'

        nfo_content = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<{root_tag}>
    <title>{self._escape_xml(title)}</title>
    <plot>{self._escape_xml(plot)}</plot>"""

        if year:
            nfo_content += f"\n    <year>{year}</year>"
        if rating:
            nfo_content += f"\n    <rating>{rating}</rating>"
        if genre:
            nfo_content += f"\n    <genre>{self._escape_xml(genre)}</genre>"

        nfo_content += f"\n</{root_tag}>"
        return nfo_content

    def _create_episode_nfo(self, episode_data: Dict[str, Any]) -> str:
        """Create NFO content for individual episode."""
        basic_data = episode_data.get('basic_data', {})

        title = basic_data.get('title', episode_data.get('episode_name', 'Unknown'))
        plot = basic_data.get('plot', episode_data.get('episode_description', ''))
        rating = basic_data.get('rating', episode_data.get('episode_rating'))
        air_date = episode_data.get('air_date')
        season = episode_data.get('season_number', 0)
        episode = episode_data.get('episode_number', 0)

        # Get series info from episode data
        show_title = episode_data.get('series_name', '')

        nfo_content = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<episodedetails>
    <title>{self._escape_xml(title)}</title>
    <showtitle>{self._escape_xml(show_title)}</showtitle>
    <season>{season}</season>
    <episode>{episode}</episode>
    <plot>{self._escape_xml(plot)}</plot>"""

        if rating:
            nfo_content += f"\n    <rating>{rating}</rating>"
        if air_date:
            nfo_content += f"\n    <aired>{air_date}</aired>"

        nfo_content += "\n</episodedetails>"
        return nfo_content

    def _write_file_task(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Write a single file (used by thread pool)."""
        try:
            file_path = file_info['path']
            content = file_info['content']
            
            # Ensure directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            return {
                'status': 'success',
                'path': str(file_path),
                'type': file_info['type']
            }
        except Exception as e:
            return {
                'status': 'error',
                'path': str(file_info['path']),
                'error': str(e),
                'type': file_info['type']
            }

    def _write_movies_threaded(self, settings: Dict[str, Any], logger) -> Dict[str, Any]:
        """Write movie .strm files using thread pool."""
        output_dir = Path(settings.get("output_dir", "/data/strm_files"))
        base_url = settings.get("base_url", "http://localhost:9191").rstrip("/")
        dry_run = settings.get("dry_run", False)
        verbose = settings.get("verbose", True)
        debug_log = settings.get("debug_log", False)
        max_workers = min(max(int(settings.get("max_workers", 4)), 1), 10)
        batch_size = int(settings.get("batch_size", 100))
        throttle_delay = float(settings.get("throttle_delay", 10)) / 1000.0

        if debug_log:
            self.debug_logger = self._get_debug_logger()

        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Processing movies with {max_workers} threads, batch size {batch_size}")

        try:
            # Get all movie relations with active M3U accounts
            movie_relations = M3UMovieRelation.objects.filter(
                m3u_account__is_active=True
            ).select_related('movie', 'm3u_account').order_by('movie__name')

            # Apply debug limit
            if debug_log:
                movie_relations = movie_relations[:20]
                logger.info("Debug mode: limiting to 20 movies")
            elif dry_run:
                movie_relations = movie_relations[:1000]  # Limit for dry run

            total = movie_relations.count()
            processed = 0
            created = 0
            skipped = 0
            errors = 0

            logger.info(f"Found {total} movie relations to process")

            movies_dir = output_dir / "Movies"
            
            # Prepare file operations (only for files that need writing)
            file_operations = []
            
            for relation in movie_relations:
                try:
                    movie = relation.movie
                    
                    # Get basic data from custom_properties
                    movie_basic_data = relation.custom_properties.get('basic_data', {}) if relation.custom_properties else {}

                    # Clean movie name for directory
                    clean_movie_name = self._sanitize_filename(movie.name)
                    if movie.year:
                        clean_movie_name = f"{clean_movie_name} ({movie.year})"

                    movie_dir = movies_dir / clean_movie_name

                    # Create filename
                    filename = clean_movie_name
                    safe_filename = self._sanitize_filename(filename)

                    strm_file = movie_dir / f"{safe_filename}.strm"
                    nfo_file = movie_dir / f"{safe_filename}.nfo"

                    # Create STRM content
                    strm_content = f"{base_url}/proxy/vod/movie/{movie.uuid}\n"

                    # Create movie NFO content
                    movie_data = {
                        'name': movie.name,
                        'description': movie.description,
                        'year': movie.year,
                        'rating': movie.rating,
                        'genre': movie.genre,
                        'basic_data': movie_basic_data
                    }
                    nfo_content = self._create_nfo_content(movie_data, 'movie')

                    # Check if files need writing
                    write_strm = self._should_write_file(strm_file, strm_content)
                    write_nfo = self._should_write_file(nfo_file, nfo_content)

                    if not write_strm and not write_nfo:
                        skipped += 1
                        if verbose:
                            logger.info(f"{'[DRY RUN] ' if dry_run else ''}Skipped (up-to-date): {clean_movie_name}")
                    else:
                        if not dry_run:
                            if write_strm:
                                file_operations.append({
                                    'path': strm_file,
                                    'content': strm_content,
                                    'type': 'strm',
                                    'name': clean_movie_name
                                })
                            if write_nfo:
                                file_operations.append({
                                    'path': nfo_file,
                                    'content': nfo_content,
                                    'type': 'nfo',
                                    'name': clean_movie_name
                                })

                        if verbose:
                            action = "Updated" if (strm_file.exists() or nfo_file.exists()) else "Created"
                            logger.info(f"{'[DRY RUN] ' if dry_run else ''}Prepared {action}: {clean_movie_name}")

                except Exception as e:
                    errors += 1
                    movie_name = getattr(relation.movie, 'name', 'Unknown') if hasattr(relation, 'movie') else 'Unknown'
                    logger.error(f"Error preparing movie {movie_name}: {e}")

            # Process files in batches using thread pool
            if not dry_run and file_operations:
                logger.info(f"Writing {len(file_operations)} files using thread pool...")
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    for i in range(0, len(file_operations), batch_size):
                        batch = file_operations[i:i + batch_size]
                        
                        # Submit batch to thread pool
                        future_to_file = {
                            executor.submit(self._write_file_task, file_info): file_info
                            for file_info in batch
                        }
                        
                        # Process completed files
                        for future in as_completed(future_to_file):
                            file_info = future_to_file[future]
                            try:
                                result = future.result()
                                if result['status'] == 'success':
                                    created += 1
                                    if verbose and result['type'] == 'strm':  # Only log once per movie
                                        logger.info(f"✓ Written: {file_info['name']}")
                                else:
                                    errors += 1
                                    logger.error(f"✗ Failed: {file_info['name']} - {result['error']}")
                            except Exception as e:
                                errors += 1
                                logger.error(f"✗ Exception: {file_info['name']} - {e}")
                        
                        processed += len(batch)
                        
                        # Throttle between batches
                        if throttle_delay > 0 and i + batch_size < len(file_operations):
                            time.sleep(throttle_delay)
                        
                        # Progress update
                        logger.info(f"Progress: {processed}/{len(file_operations)} files written")
            else:
                processed = len(file_operations)

            summary = f"Movies processed: {total}, files created: {created}, skipped: {skipped}, errors: {errors}"
            if dry_run:
                summary = f"[DRY RUN] {summary}"

            logger.info(summary)
            return {"status": "success", "message": summary}

        except Exception as e:
            logger.error(f"Error in _write_movies_threaded: {e}")
            return {"status": "error", "message": f"Movie processing failed: {e}"}

    def _write_series_threaded(self, settings: Dict[str, Any], logger) -> Dict[str, Any]:
        """Write series .strm files using series-by-series processing with thread pool."""
        output_dir = Path(settings.get("output_dir", "/data/strm_files"))
        base_url = settings.get("base_url", "http://localhost:9191").rstrip("/")
        dry_run = settings.get("dry_run", False)
        verbose = settings.get("verbose", True)
        debug_log = settings.get("debug_log", False)
        populate_episodes = settings.get("populate_episodes", True)
        max_workers = min(max(int(settings.get("max_workers", 4)), 1), 10)
        batch_size = int(settings.get("batch_size", 100))
        throttle_delay = float(settings.get("throttle_delay", 10)) / 1000.0

        if debug_log:
            self.debug_logger = self._get_debug_logger()

        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Processing series with {max_workers} threads, batch size {batch_size} (series-by-series)")

        try:
            # Get all series that have M3U relations (active accounts only)
            series_relations = M3USeriesRelation.objects.filter(
                m3u_account__is_active=True
            ).select_related('series', 'm3u_account').order_by('series__name')

            # Apply debug limit
            if debug_log:
                series_relations = series_relations[:20]
                logger.info("Debug mode: limiting to 20 series")
            elif dry_run:
                series_relations = series_relations[:20]  # Limit for dry run

            total_series = series_relations.count()
            processed_series = 0
            total_episodes_processed = 0
            total_files_created = 0
            total_files_skipped = 0
            total_errors = 0

            logger.info(f"Found {total_series} series to process")

            tv_shows_dir = output_dir / "TV Shows"

            # Process each series individually
            for series_relation in series_relations:
                try:
                    series = series_relation.series
                    account = series_relation.m3u_account
                    external_series_id = series_relation.external_series_id

                    logger.info(f"Processing series {processed_series + 1}/{total_series}: {series.name}")

                    # Step 1: Populate episodes for this series if needed
                    if populate_episodes:
                        custom_props = series_relation.custom_properties or {}
                        episodes_fetched = custom_props.get('episodes_fetched', False)

                        if not episodes_fetched:
                            logger.info(f"  → Populating episodes for: {series.name}")
                            try:
                                from apps.vod.tasks import refresh_series_episodes
                                refresh_series_episodes(account, series, external_series_id)
                                series.refresh_from_db()
                                series_relation.refresh_from_db()
                                logger.info(f"  ✓ Episodes populated for {series.name}")
                            except Exception as e:
                                logger.error(f"  ✗ Error populating episodes for {series.name}: {e}")
                                processed_series += 1
                                continue
                        else:
                            logger.info(f"  ↷ Episodes already exist for {series.name}")

                    # Step 2: Get episode relations for this specific series
                    episode_relations = M3UEpisodeRelation.objects.filter(
                        m3u_account=account,
                        episode__series=series
                    ).select_related('episode').order_by(
                        'episode__season_number', 'episode__episode_number'
                    )

                    series_episodes_count = episode_relations.count()
                    
                    if series_episodes_count == 0:
                        logger.info(f"  ⚠ No episodes found for {series.name} - skipping")
                        processed_series += 1
                        continue

                    logger.info(f"  → Processing {series_episodes_count} episodes for {series.name}")

                    # Get series metadata for NFO
                    series_basic_data = {}
                    if series_relation.custom_properties:
                        series_basic_data = series_relation.custom_properties.get('basic_data', {})

                    # Clean series name for directory
                    clean_series_name = self._sanitize_filename(series.name)
                    if series.year:
                        clean_series_name = f"{clean_series_name} ({series.year})"

                    series_dir = tv_shows_dir / clean_series_name

                    # Prepare file operations for this series
                    series_file_operations = []
                    series_files_skipped = 0

                    # Create series NFO content once
                    series_data = {
                        'name': clean_series_name,
                        'description': series.description,
                        'year': series.year,
                        'rating': series.rating,
                        'basic_data': series_basic_data
                    }
                    series_nfo_content = self._create_nfo_content(series_data, 'tv')
                    series_nfo_file = series_dir / "tvshow.nfo"

                    # Check if series NFO needs writing
                    if self._should_write_file(series_nfo_file, series_nfo_content):
                        series_file_operations.append({
                            'path': series_nfo_file,
                            'content': series_nfo_content,
                            'type': 'series_nfo',
                            'name': f"{clean_series_name}/tvshow.nfo"
                        })

                    # Process each episode for this series
                    for episode_relation in episode_relations:
                        try:
                            episode = episode_relation.episode

                            # Get episode metadata
                            episode_basic_data = episode_relation.custom_properties.get('basic_data', {}) if episode_relation.custom_properties else {}

                            # Create season directory
                            season_num = episode.season_number or 0
                            season_dir = series_dir / f"Season {season_num:02d}"

                            # Create episode filename
                            ep_num = episode.episode_number or 0
                            filename = f"S{season_num:02d}E{ep_num:02d} - {episode.name}"
                            safe_filename = self._sanitize_filename(filename)

                            strm_file = season_dir / f"{safe_filename}.strm"
                            nfo_file = season_dir / f"{safe_filename}.nfo"

                            # Create STRM content
                            strm_content = f"{base_url}/proxy/vod/episode/{episode.uuid}\n"

                            # Create episode NFO content
                            episode_data = {
                                'series_name': series.name,
                                'episode_name': episode.name,
                                'episode_description': episode.description,
                                'season_number': episode.season_number,
                                'episode_number': episode.episode_number,
                                'air_date': episode.air_date,
                                'episode_rating': episode.rating,
                                'basic_data': episode_basic_data
                            }
                            nfo_content = self._create_episode_nfo(episode_data)

                            # Check if files need writing
                            write_strm = self._should_write_file(strm_file, strm_content)
                            write_nfo = self._should_write_file(nfo_file, nfo_content)

                            if not write_strm and not write_nfo:
                                series_files_skipped += 1
                                if verbose:
                                    logger.info(f"    {'[DRY RUN] ' if dry_run else ''}Skipped (up-to-date): Season {season_num:02d}/{safe_filename}")
                            else:
                                if not dry_run:
                                    if write_strm:
                                        series_file_operations.append({
                                            'path': strm_file,
                                            'content': strm_content,
                                            'type': 'strm',
                                            'name': f"{clean_series_name}/Season {season_num:02d}/{safe_filename}"
                                        })
                                    if write_nfo:
                                        series_file_operations.append({
                                            'path': nfo_file,
                                            'content': nfo_content,
                                            'type': 'nfo',
                                            'name': f"{clean_series_name}/Season {season_num:02d}/{safe_filename}"
                                        })

                                if verbose:
                                    action = "Updated" if (strm_file.exists() or nfo_file.exists()) else "Created"
                                    logger.info(f"    {'[DRY RUN] ' if dry_run else ''}Prepared {action}: Season {season_num:02d}/{safe_filename}")

                            total_episodes_processed += 1

                        except Exception as e:
                            total_errors += 1
                            episode_name = getattr(episode_relation.episode, 'name', 'Unknown') if hasattr(episode_relation, 'episode') else 'Unknown'
                            logger.error(f"    ✗ Error processing episode {episode_name}: {e}")

                    # Write files for this series using thread pool
                    series_files_created = 0
                    if not dry_run and series_file_operations:
                        logger.info(f"  → Writing {len(series_file_operations)} files for {series.name}")
                        
                        with ThreadPoolExecutor(max_workers=max_workers) as executor:
                            for i in range(0, len(series_file_operations), batch_size):
                                batch = series_file_operations[i:i + batch_size]
                                
                                # Submit batch to thread pool
                                future_to_file = {
                                    executor.submit(self._write_file_task, file_info): file_info
                                    for file_info in batch
                                }
                                
                                # Process completed files
                                for future in as_completed(future_to_file):
                                    file_info = future_to_file[future]
                                    try:
                                        result = future.result()
                                        if result['status'] == 'success':
                                            series_files_created += 1
                                        else:
                                            total_errors += 1
                                            logger.error(f"✗ Failed: {file_info['name']} - {result['error']}")
                                    except Exception as e:
                                        total_errors += 1
                                        logger.error(f"✗ Exception: {file_info['name']} - {e}")
                                
                                # Throttle between batches
                                if throttle_delay > 0 and i + batch_size < len(series_file_operations):
                                    time.sleep(throttle_delay)

                    total_files_created += series_files_created
                    total_files_skipped += series_files_skipped
                    
                    logger.info(f"  ✓ Completed {series.name}: {series_episodes_count} episodes, {series_files_created} files created, {series_files_skipped} skipped")

                except Exception as e:
                    total_errors += 1
                    logger.error(f"  ✗ Error processing series {series.name}: {e}")

                processed_series += 1

            summary = f"Series processed: {processed_series}, episodes processed: {total_episodes_processed}, files created: {total_files_created}, files skipped: {total_files_skipped}, errors: {total_errors}"
            if dry_run:
                summary = f"[DRY RUN] {summary}"

            logger.info(summary)
            return {"status": "success", "message": summary}

        except Exception as e:
            logger.error(f"Error in _write_series_threaded: {e}")
            return {"status": "error", "message": f"Series processing failed: {e}"}

    def run(self, action: str, params: dict, context: dict) -> Dict[str, Any]:
        """Main entry point for plugin actions."""
        logger = context.get("logger")
        settings = context.get("settings", {})

        logger.info("VOD STRM Plugin (ThreadPool) v%s - Action: %s", self.version, action)

        self._save_settings(settings)

        try:
            if action == "show_stats":
                stats = self._get_vod_stats(logger)

                provider_lines = []
                for p in stats['providers']:
                    provider_lines.append(f"  • ID {p['id']}: {p['name']}")
                providers_text = "\n".join(provider_lines) if provider_lines else "  None found"

                message = f"""Database Statistics:

CONTENT COUNTS:
Movies: {stats['imported_movies']} active / {stats['total_movies']} total
Series: {stats['imported_series']} active / {stats['total_series']} total
Episodes: {stats['imported_episodes']} active / {stats['total_episodes']} total

ACTIVE PROVIDERS:
{providers_text}

Note: "Active" means VODs with active M3U provider relations.
Only active VODs will be processed when creating .strm files."""
                return {"status": "success", "message": message}

            if action == "populate_episodes_only":
                try:
                    stats = self.populate_all_episodes(logger)
                    message = f"Episode population completed! Processed {stats['processed']} series, populated {stats['populated']} new series with episodes."
                    return {"status": "success", "message": message}
                except Exception as e:
                    return {"status": "error", "message": f"Episode population failed: {e}"}

            if action == "write_movies":
                return self._write_movies_threaded(settings, logger)
            if action == "write_series":
                return self._write_series_threaded(settings, logger)
            return {"status": "error", "message": f"Unknown action: {action}"}

        except Exception as e:
            # Check if it's a database-related error
            if any(keyword in str(e).lower() for keyword in ['database', 'connection', 'sql', 'cursor']):
                logger.exception("Database error during plugin execution: %s", e)
                return {"status": "error", "message": f"Database Error: {e}"}
            else:
                logger.exception("An unexpected error occurred during plugin execution.")
                return {"status": "error", "message": f"An unexpected error occurred: {e}"}
