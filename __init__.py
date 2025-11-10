from .plugin import Plugin

# Import tasks to ensure they're registered with Celery when plugin loads
# This works because PluginManager imports the plugin package, which triggers
# this module-level import, which loads tasks.py and registers @shared_task functions
try:
    from . import tasks  # noqa: F401
except ImportError:
    # Celery may not be available in some environments
    pass

# Optional: module-level re-exports so either style works
name = Plugin.name
version = Plugin.version
description = Plugin.description
fields = Plugin.fields
actions = Plugin.actions
