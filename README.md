# vod2strm – Dispatcharr Plugin

A high-performance Dispatcharr plugin that exports your VOD library (Movies and Series/Episodes) into a structured filesystem of `.strm` and `.nfo` files for Plex, Emby, Jellyfin, or Kodi.

## Overview

vod2strm transforms Dispatcharr's VOD database into media server-compatible `.strm` files without duplicating content. It runs entirely inside Dispatcharr using the Django ORM for performance and Celery for background jobs. If Celery isn't available, it gracefully falls back to background threads.

## Features

### Core Functionality
- **Movies + Series Support**: Generates `.strm` files pointing to Dispatcharr proxy endpoints (`/proxy/vod/movie/{uuid}` and `/proxy/vod/episode/{uuid}`)
- **NFO Generation**: Creates `movie.nfo`, `season.nfo`, and `SxxExx.nfo` with TMDB/IMDB metadata
- **Season 00 Specials**: Automatically organizes season 0 episodes into "Season 00 (Specials)" folders
- **Manifest Caching**: Tracks generated files to skip unnecessary writes (protects SD cards/NAS from wear)
- **Cleanup**: Detects and removes `.strm` files for content no longer in the database (Preview or Apply modes)

### Performance & Protection
- **Adaptive Throttling**: Monitors NAS write performance and dynamically adjusts concurrency to prevent I/O overload
- **Smart Skipping**: Instantly skips entire series when filesystem already matches database
- **Batch Processing**: Processes files in batches with progress logging
- **Compare-Before-Write**: Only writes when content changes (hash-based comparison for NFO files)

### Automation
- **Auto-run After VOD Refresh**: Optionally trigger generation automatically when Dispatcharr refreshes VOD content (30-second debounce)

### Debugging & Reports
- **Dry Run Mode**: Simulate generation without touching filesystem (testing)
- **Database Statistics**: View content counts and provider breakdown
- **CSV Reports**: Detailed action logs for every run (`/data/plugins/vod2strm/reports/`)
- **Verbose Logging**: Optional debug logs (`/data/plugins/vod2strm/logs/vod2strm.log`)

## Output Structure

```
/data/STRM/
├── Movies/
│   └── Movie Name (2023)/
│       ├── Movie Name (2023).strm
│       └── movie.nfo
└── TV/
    └── Series Name (2021)/
        ├── Season 01/
        │   ├── S01E01 - Episode Title.strm
        │   ├── S01E01.nfo
        │   └── season.nfo
        └── Season 00 (Specials)/
            └── S00E01 - Special Title.strm
```

## Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| **Output Root Folder** | Text | `/data/STRM` | Destination for `.strm` and `.nfo` files |
| **Base URL (for .strm)** | Text | `http://dispatcharr:9191` | URL written inside each `.strm` file |
| **Use Direct URLs** | Boolean | ☐ | Write provider URLs directly instead of Dispatcharr proxy URLs |
| **Write NFO files** | Boolean | ✅ | Generate NFO metadata alongside `.strm` files |
| **Cleanup** | Select | Off | Off / Preview / Apply – removes stale files |
| **Max Filesystem Concurrency** | Number | 4 | Maximum concurrent file operations (adaptive throttling adjusts automatically) |
| **Adaptive Throttling** | Boolean | ✅ | Auto-adjust concurrency based on NAS performance |
| **Auto-run after VOD Refresh** | Boolean | ☐ | Automatically generate files when Dispatcharr refreshes VOD content |
| **Dry Run** | Boolean | ☐ | Simulate without writing (testing mode) |
| **Robust debug logging** | Boolean | ☐ | Enable verbose logging to `/data/plugins/vod2strm/logs/` |

## Actions

| Action | Description |
|--------|-------------|
| **Database Statistics** | Shows content counts, provider breakdown, and series without episodes |
| **Stats (CSV)** | Writes summary CSV (counts from DB + filesystem) |
| **Generate Movies** | Builds `.strm` + NFO files for all movies |
| **Generate Series** | Builds `.strm` + NFO files for all series/episodes |
| **Generate All** | Runs Movies + Series generation plus optional Cleanup |

## Installation

### 1. Create plugin directory

```bash
mkdir -p /opt/dispatcharr/plugins/vod2strm
```

### 2. Copy files

```
vod2strm/
├── __init__.py
└── plugin.py
```

### 3. Restart Dispatcharr

Restart Dispatcharr or reload plugins via the UI.

### 4. Configure

Navigate to **Settings → Plugins → vod2strm** and configure your settings.

## Reports & Logs

### CSV Reports
`/data/plugins/vod2strm/reports/report_<mode>_<timestamp>.csv`

Columns: `type`, `series_name`, `season`, `title`, `year`, `db_uuid`, `strm_path`, `nfo_path`, `action`, `reason`

### Logs
`/data/plugins/vod2strm/logs/vod2strm.log`

Rotating log files with debug information (when enabled).

## Performance Tips

### First Run (100K+ files)
- **Adaptive throttling** starts at 4 workers and adjusts based on your NAS performance
- Slow NAS (>200ms writes): Automatically reduces to 3→2→1 workers
- Fast NAS (<50ms writes): Increases up to your max concurrency setting
- Progress logged every 100 files: `"Movies: processed 100 / 10000 (current workers: 3)"`

### Subsequent Runs
- **Manifest caching** skips writes for unchanged files (most files skip on 2nd+ runs)
- Only new content or changed metadata triggers writes
- Minimal SD card/NAS wear

## Safety Notes

- ✅ Read-only access to Dispatcharr database (no modifications)
- ✅ Writes only within configured output root
- ✅ Compare-before-write prevents redundant I/O
- ⚠️ **Cleanup → Apply** permanently deletes stale files — use Preview first!
- ⚠️ First run on large libraries may take hours (adaptive throttling helps)

## Troubleshooting

### NAS appears "stuck" on first run
**Expected behavior** - Processing 100K files takes time. Check logs for progress:
```
Movies: processed 100 / 10000 (current workers: 3)
Adaptive throttle: NAS slow (avg 0.350s), reducing workers 4 → 3
```

### Files not updating after VOD refresh
- Enable **Auto-run after VOD Refresh** for automatic generation
- Or manually click **Generate All** after refreshing content
- Check **Database Statistics** to verify content is in Dispatcharr

### Some series missing episodes
- Use **Database Statistics** to find series without episodes
- Plugin auto-refreshes series with 0 episodes on first encounter
- Check Dispatcharr VOD refresh to ensure provider has episode data

## Versioning

Semantic versioning: `MAJOR.MINOR.PATCH`
- Current: `0.0.10`

## License

MIT / Public Domain — use freely, attribution appreciated.

---

**Made for Dispatcharr** | Report issues on GitHub
