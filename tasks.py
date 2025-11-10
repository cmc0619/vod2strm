"""
Celery tasks for vod2strm plugin.

This module must be explicitly imported by Celery workers to register the tasks.
Import path: plugins.vod2strm.tasks (assuming plugin is in /data/plugins/vod2strm/)
"""
import logging
from celery import shared_task

LOGGER = logging.getLogger(__name__)


@shared_task(name="plugins.vod2strm.tasks.run_job", bind=False)
def run_job(args: dict):
    """
    Celery task for background STRM generation.

    Args:
        args: Dictionary of arguments to pass to _run_job_sync
    """
    # Import here to avoid circular dependencies and ensure Django is ready
    from .plugin import _run_job_sync

    LOGGER.info("Celery task run_job starting with args: %s", args)
    try:
        _run_job_sync(**args)
        LOGGER.info("Celery task run_job completed successfully")
    except Exception as e:
        LOGGER.error("Celery task run_job failed: %s", e, exc_info=True)
        raise


@shared_task(name="plugins.vod2strm.tasks.generate_all", bind=False)
def generate_all():
    """
    Scheduled Celery task to generate all STRM files.

    Loads settings from database and runs full generation.
    """
    # Import here to avoid circular dependencies
    from .plugin import _run_job_sync, DEFAULT_ROOT, DEFAULT_BASE_URL, CLEANUP_OFF

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
