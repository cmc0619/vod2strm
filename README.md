vod2strm – Dispatcharr Plugin
Author: ChatGPT (per user spec)

Overview

vod2strm is a Dispatcharr plugin that exports your VOD library (Movies and Series/Episodes) from the internal database into a structured filesystem tree of .strm and .nfo files.
This allows media servers such as Plex, Emby, or Jellyfin to index Dispatcharr’s VOD items directly without duplicating the underlying media.

The plugin runs completely inside Dispatcharr, using the ORM for speed and Celery for background jobs (non-blocking UI).
If Celery isn’t available, it falls back to background threads automatically.

Features

✅ Movies + Series Support
Builds .strm files pointing to Dispatcharr’s proxy endpoints
(/proxy/vod/movies/{uuid} and /proxy/vod/episodes/{uuid}).

✅ NFO Generation
Creates movie.nfo, season.nfo, and SxxExx.nfo beside each .strm.
Writes only when content changes.

✅ Season 00 (Specials)
Automatically places season_number = 0 episodes in Season 00 (Specials) folders.

✅ Cleanup
Detects and optionally deletes .strm files whose UUIDs no longer exist in the DB.
Modes: Off | Preview | Apply.

✅ Fast & Smart
Skips entire series instantly when filesystem already matches the DB.
Writes as it goes — no waiting for all series to finish.

✅ Robust Debug Logging
Verbose logs written to /data/plugins/vod2strm/logs/vod2strm.log.

✅ CSV Reporting
Every action writes a detailed CSV report to /data/plugins/vod2strm/reports/.

✅ Optional Scheduling
Run automatically via Celery beat using a daily time or full crontab string.

Output Structure
/data/STRM/
├── Movies/
│   └── Movie Name (2023)/
│       ├── Movie Name (2023).strm
│       └── movie.nfo
└── TV/
    └── Series Name (2021)/
        ├── Season 01/
        │   ├── S01E01 – Episode Title.strm
        │   ├── S01E01.nfo
        │   └── season.nfo
        └── Season 00 (Specials)/
            └── S00E01 – Special Title.strm

Settings
Setting	Type	Default	Description
Output Root Folder	Text	/data/STRM	Destination for .strm and .nfo files.
Base URL (for .strm)	Text	http://192.168.199.10:9191	Written inside each .strm file.
Write NFO files	Boolean	✅	Generate NFO metadata alongside .strms.
Cleanup	Select	Off	Off / Preview / Apply – removes stale files.
Filesystem concurrency	Number	12	Parallel file-write workers.
Robust debug logging	Boolean	☐	Enable verbose logging to /data/plugins/vod2strm/logs/.
Schedule	Text	(blank)	daily HH:MM or 0 30 3 * * * (crontab) for Celery beat.
Actions / Buttons
Action	Description
Stats (CSV)	Writes a summary CSV (counts from DB + filesystem).
Generate Movies	Builds .strm + NFO files for all movies.
Generate Series	Builds .strm + NFO files for all series/episodes.
Generate All	Runs both Movies + Series and optional Cleanup.
Reports & Logs

CSV Reports:
/data/plugins/vod2strm/reports/report_<mode>_<timestamp>.csv
Columns: type, series_name, season, title, uuid, action, reason

Logs:
/data/plugins/vod2strm/logs/vod2strm.log

Installation

Create plugin directory

mkdir -p /opt/dispatcharr/plugins/vod2strm


Copy files

vod2strm/
├── __init__.py
└── plugin.py


Restart Dispatcharr (or reload plugins).

Configure via Settings → Plugins → vod2strm.

Versioning Policy

Semantic format MAJOR.MINOR.PATCH
Starts at 0.0.1 and increments the third digit (e.g., 0.0.2, 0.0.3 …).

Safety Notes

Read-only access to Dispatcharr DB (no modifications).

Writes only within the configured output root.

Compare-before-write prevents redundant I/O.

Cleanup → Apply permanently deletes stale files — use with care.

Future Ideas

Inline CSV preview in the Dispatcharr UI.

Manual “Hydrate Episodes” button for zero-episode series.

Optional filters (year range, group, language).

License
MIT / Public Domain — use freely, attribution appreciated.
