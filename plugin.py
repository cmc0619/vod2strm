import os
import json
import re
import time
import threading
import requests
from datetime import datetime
from typing import Dict, Any, List, Tuple, Set, Optional

# Import existing Dispatcharr tasks and utilities
from apps.m3u.tasks import refresh_m3u_accounts
from apps.epg.tasks import refresh_all_epg_data
from core.utils import send_websocket_update
from django.db import transaction

# Import models directly
from apps.vod.models import Movie, Series, Episode, M3UMovieRelation, M3USeriesRelation, M3UEpisodeRelation
from apps.m3u.models import M3UAccount
from apps.channels.models import Logo


# Global background processing state
_bg_thread = None
_stop_event = threading.Event()

# Constants
DEFAULT_MAX_NAME_LENGTH = 180
DRY_RUN_LIMIT = 1000
SAFE_NAME_RE = re.compile(r'[\\/:*?"<>|\t\r\n\']+')


class Plugin:
    name = "VOD to STRM (Celery Simplified)"
    version = "2.1.0"
    description = "Generate STRM files from VOD content using series-by-series processing with existing Dispatcharr tasks"

    fields = [
        {"id": "base_path", "label": "Base Path", "type": "string", "default": "/data/strm_files"},
        {"id": "base_url", "label": "Base URL", "type": "string", "default": "http://192.168.199.10:9191"},
        {"id": "username", "label": "Username", "type": "string", "default": "admin"},
        {"id": "password", "label": "Password", "type": "string", "default": "admin"},
        {"id": "max_name_length", "label": "Max filename length", "type": "number", "default": 180},
        {"id": "debug", "label": "Debug mode (limit items)", "type": "boolean", "default": True},
        {"id": "dry_run", "label": "Dry run (don't write files)", "type": "boolean", "default": False},
        {"id": "skip_existing", "label": "Skip existing files", "type": "boolean", "default": True},
        {"id": "populate_first", "label": "Refresh M3U data first", "type": "boolean", "default": False},
        {"id": "process_movies", "label": "Process Movies", "type": "boolean", "default": True},
        {"id": "process_series", "label": "Process Series", "type": "boolean", "default": True},
    ]

    actions = [
        {
            "id": "generate_all", 
            "label": "Generate All STRM Files",
            "description": "Process all movies and series with series-by-series episode population"
        },
        {
            "id": "stop_generation", 
            "label": "Stop Generation",
            "description": "Stop the background generation process"
        },
    ]

    def run(self, action: str, params: dict, context: dict):
        settings = context.get("settings", {})
        logger = context.get("logger")
        
        if action == "generate_all":
            return self._start_generation(settings, logger)
        elif action == "stop_generation":
            return self._stop_generation()
        
        return {"status": "error", "message": f"Unknown action: {action}"}

    def _start_generation(self, settings: dict, logger):
        """Start the background generation process"""
        global _bg_thread, _stop_event
        
        # Check if already running
        if _bg_thread and _bg_thread.is_alive():
            return {"status": "error", "message": "Generation already in progress"}
        
        # Reset stop event
        _stop_event.clear()
        
        # Optional: Refresh M3U data first if requested
        if settings.get("populate_first", False):
            logger.info("Triggering M3U refresh first...")
            refresh_m3u_accounts.delay()
            # Note: This runs async, so we continue immediately
        
        # Start background thread for processing
        _bg_thread = threading.Thread(
            target=self._background_generate,
            args=(settings, logger),
            daemon=True
        )
        _bg_thread.start()
        
        return {
            "status": "started",
            "message": "STRM file generation started in background"
        }

    def _stop_generation(self):
        """Stop the background generation process"""
        global _stop_event
        _stop_event.set()
        return {"status": "stopped", "message": "Stop signal sent"}

    def _background_generate(self, settings: dict, logger):
        """Background thread for STRM file generation"""
        try:
            logger.info("Starting STRM file generation...")
            
            # Extract settings
            base_path = settings.get("base_path", "/data/strm_files")
            base_url = settings.get("base_url", "http://192.168.199.10:9191")
            username = settings.get("username", "admin")
            password = settings.get("password", "admin")
            max_name_length = int(settings.get("max_name_length", 180))
            debug = settings.get("debug", True)
            dry_run = settings.get("dry_run", False)
            skip_existing = settings.get("skip_existing", True)
            process_movies = settings.get("process_movies", True)
            process_series = settings.get("process_series", True)

            stats = {
                "movies_processed": 0,
                "movies_created": 0,
                "movies_skipped": 0,
                "series_processed": 0,
                "series_created": 0,
                "series_skipped": 0,
                "episodes_processed": 0,
                "episodes_created": 0,
                "episodes_skipped": 0,
            }

            # Process Movies
            if process_movies and not _stop_event.is_set():
                logger.info("Processing movies...")
                stats.update(self._process_movies(
                    base_path, base_url, username, password, 
                    max_name_length, debug, dry_run, skip_existing, logger
                ))

            # Process Series (series-by-series)
            if process_series and not _stop_event.is_set():
                logger.info("Processing series (series-by-series)...")
                series_stats = self._process_series_by_series(
                    base_path, base_url, username, password,
                    max_name_length, debug, dry_run, skip_existing, logger
                )
                stats.update(series_stats)

            # Final report
            if not _stop_event.is_set():
                logger.info("STRM generation completed!")
                logger.info(f"Final stats: {stats}")
                send_websocket_update('updates', 'update', {
                    "type": "plugin",
                    "plugin": "vod2strm_celery_simplified",
                    "message": f"Completed! Movies: {stats['movies_created']}, Episodes: {stats['episodes_created']}"
                })
            else:
                logger.info("STRM generation stopped by user")
                send_websocket_update('updates', 'update', {
                    "type": "plugin", 
                    "plugin": "vod2strm_celery_simplified",
                    "message": "Generation stopped by user"
                })

        except Exception as e:
            logger.exception(f"Error in background generation: {e}")
            send_websocket_update('updates', 'update', {
                "type": "plugin",
                "plugin": "vod2strm_celery_simplified", 
                "message": f"Error: {str(e)}"
            })

    def _process_movies(self, base_path, base_url, username, password, max_name_length, debug, dry_run, skip_existing, logger):
        """Process movies to generate STRM files"""
        stats = {"movies_processed": 0, "movies_created": 0, "movies_skipped": 0}
        
        # Get movies with M3U relations
        movies_query = Movie.objects.filter(m3u_relations__isnull=False).distinct()
        if debug:
            movies_query = movies_query[:20]  # Limit for debug
            
        movies = list(movies_query)
        logger.info(f"Found {len(movies)} movies to process")
        
        for movie in movies:
            if _stop_event.is_set():
                break
                
            try:
                stats["movies_processed"] += 1
                result = self._create_movie_strm(
                    movie, base_path, base_url, username, password,
                    max_name_length, dry_run, skip_existing
                )
                
                if result == "created":
                    stats["movies_created"] += 1
                    logger.info(f"Created: {movie.name}")
                elif result == "skipped":
                    stats["movies_skipped"] += 1
                    
                # Progress update every 10 movies
                if stats["movies_processed"] % 10 == 0:
                    send_websocket_update('updates', 'update', {
                        "type": "plugin",
                        "plugin": "vod2strm_celery_simplified",
                        "message": f"Movies: {stats['movies_processed']}/{len(movies)} processed"
                    })
                    
            except Exception as e:
                logger.error(f"Error processing movie {movie.name}: {e}")
        
        return stats

    def _process_series_by_series(self, base_path, base_url, username, password, max_name_length, debug, dry_run, skip_existing, logger):
        """Process series one-by-one: populate episodes, then write files"""
        stats = {"series_processed": 0, "series_created": 0, "series_skipped": 0, 
                "episodes_processed": 0, "episodes_created": 0, "episodes_skipped": 0}
        
        # Get series with M3U relations
        series_query = Series.objects.filter(m3u_relations__isnull=False).distinct()
        if debug:
            series_query = series_query[:20]  # Limit for debug
            
        all_series = list(series_query)
        logger.info(f"Found {len(all_series)} series to process")
        
        for series in all_series:
            if _stop_event.is_set():
                break
                
            try:
                logger.info(f"Processing series: {series.name}")
                stats["series_processed"] += 1
                
                # STEP 1: Populate episodes for this series
                self._populate_series_episodes(series, logger)
                
                # STEP 2: Process episodes for this series immediately
                series_stats = self._process_episodes_for_series(
                    series, base_path, base_url, username, password,
                    max_name_length, dry_run, skip_existing, logger
                )
                
                # Update stats
                stats["episodes_processed"] += series_stats["episodes_processed"]
                stats["episodes_created"] += series_stats["episodes_created"] 
                stats["episodes_skipped"] += series_stats["episodes_skipped"]
                
                if series_stats["episodes_created"] > 0:
                    stats["series_created"] += 1
                    logger.info(f"Series completed: {series.name} ({series_stats['episodes_created']} episodes created)")
                else:
                    stats["series_skipped"] += 1
                
                # Progress update after each series
                send_websocket_update('updates', 'update', {
                    "type": "plugin",
                    "plugin": "vod2strm_celery_simplified",
                    "message": f"Series: {stats['series_processed']}/{len(all_series)} - {series.name} ({series_stats['episodes_created']} episodes)"
                })
                
            except Exception as e:
                logger.error(f"Error processing series {series.name}: {e}")
                stats["series_skipped"] += 1
        
        return stats

    def _populate_series_episodes(self, series, logger):
        """Populate episodes for a specific series using Dispatcharr's internal API"""
        try:
            # Use the same API endpoint that the UI calls when you click a series
            # This should trigger episode population just like clicking in the UI
            
            # Method 1: Try to use the provider-info endpoint to refresh series data
            from django.test import Client
            client = Client()
            
            # This mimics what happens when you click a series in the UI
            response = client.get(f'/api/vod/series/{series.id}/episodes/')
            if response.status_code == 200:
                logger.info(f"Successfully populated episodes for series: {series.name}")
            else:
                logger.warning(f"Episode population response {response.status_code} for series: {series.name}")
                
            # Method 2: Alternative - directly call provider info endpoint
            provider_response = client.get(f'/api/vod/series/{series.id}/provider-info/')
            if provider_response.status_code == 200:
                logger.info(f"Successfully refreshed provider info for series: {series.name}")
                
        except Exception as e:
            logger.warning(f"Could not populate episodes for series {series.name}: {e}")
            # Continue anyway - some episodes might already exist

    def _process_episodes_for_series(self, series, base_path, base_url, username, password, max_name_length, dry_run, skip_existing, logger):
        """Process all episodes for a specific series"""
        stats = {"episodes_processed": 0, "episodes_created": 0, "episodes_skipped": 0}
        
        # Get episodes for this series that have M3U relations
        episodes = Episode.objects.filter(
            series=series,
            m3u_relations__isnull=False
        ).distinct().order_by('season_number', 'episode_number')
        
        logger.info(f"Found {episodes.count()} episodes for series {series.name}")
        
        for episode in episodes:
            if _stop_event.is_set():
                break
                
            try:
                stats["episodes_processed"] += 1
                result = self._create_episode_strm(
                    episode, base_path, base_url, username, password,
                    max_name_length, dry_run, skip_existing
                )
                
                if result == "created":
                    stats["episodes_created"] += 1
                elif result == "skipped":
                    stats["episodes_skipped"] += 1
                    
            except Exception as e:
                logger.error(f"Error processing episode {episode.name}: {e}")
        
        return stats

    def _create_movie_strm(self, movie, base_path, base_url, username, password, max_name_length, dry_run, skip_existing):
        """Create STRM file for a movie"""
        # Get the first M3U relation for the movie
        relation = movie.m3u_relations.first()
        if not relation:
            return "skipped"
        
        # Create safe filename
        year_str = f" ({movie.year})" if movie.year else ""
        safe_filename = self._safe_name(f"{movie.name}{year_str}", max_name_length)
        
        # Create directory structure
        movies_dir = os.path.join(base_path, "Movies")
        movie_dir = os.path.join(movies_dir, safe_filename)
        
        if not dry_run:
            os.makedirs(movie_dir, exist_ok=True)
        
        # STRM file path
        strm_file = os.path.join(movie_dir, f"{safe_filename}.strm")
        
        # Skip if exists and skip_existing is True
        if skip_existing and os.path.exists(strm_file):
            return "skipped"
        
        # Create STRM content - use the working movie URL format
        stream_url = f"{base_url}/live/{username}/{password}/{relation.stream_id}"
        
        # Write STRM file
        if not dry_run:
            with open(strm_file, 'w') as f:
                f.write(stream_url)
        
        return "created"

    def _create_episode_strm(self, episode, base_path, base_url, username, password, max_name_length, dry_run, skip_existing):
        """Create STRM file for an episode"""
        # Get the first M3U relation for the episode
        relation = episode.m3u_relations.first()
        if not relation:
            return "skipped"
        
        # Create safe names
        series_name = self._safe_name(episode.series.name, max_name_length)
        episode_name = self._safe_name(episode.name, max_name_length)
        
        # Create directory structure
        tv_dir = os.path.join(base_path, "TV Shows")
        series_dir = os.path.join(tv_dir, series_name)
        
        if episode.season_number:
            season_dir = os.path.join(series_dir, f"Season {episode.season_number:02d}")
        else:
            season_dir = series_dir
        
        if not dry_run:
            os.makedirs(season_dir, exist_ok=True)
        
        # Create episode filename
        if episode.season_number and episode.episode_number:
            episode_filename = f"S{episode.season_number:02d}E{episode.episode_number:02d} - {episode_name}.strm"
        else:
            episode_filename = f"{episode_name}.strm"
        
        strm_file = os.path.join(season_dir, episode_filename)
        
        # Skip if exists and skip_existing is True
        if skip_existing and os.path.exists(strm_file):
            return "skipped"
        
        # Create STRM content - use the working episode URL format
        stream_url = f"{base_url}/live/{username}/{password}/{relation.stream_id}"
        
        # Write STRM file
        if not dry_run:
            with open(strm_file, 'w') as f:
                f.write(stream_url)
        
        return "created"

    def _safe_name(self, s: str, maxlen: int = DEFAULT_MAX_NAME_LENGTH) -> str:
        """Sanitize a string for use as a filename"""
        if not s:
            return "Unknown"
        
        # Remove problematic characters
        safe = SAFE_NAME_RE.sub('_', s)
        
        # Remove multiple underscores
        safe = re.sub(r'_+', '_', safe)
        
        # Trim and remove leading/trailing underscores
        safe = safe.strip('_')
        
        # Truncate if too long
        if len(safe) > maxlen:
            safe = safe[:maxlen].rstrip('_')
        
        return safe or "Unknown"
