# Database Cleanup Scripts

## cleanup_db.py

Flexible script to delete VOD content from the Dispatcharr database.

### Quick Start

```bash
# Install dependency (if not already installed)
pip install psycopg2-binary

# List series to find the ID you want to delete
python scripts/cleanup_db.py --list

# Delete episodes for a specific series (preview first)
python scripts/cleanup_db.py --series 665 --dry-run

# Actually delete it
python scripts/cleanup_db.py --series 665

# Delete without removing relations
python scripts/cleanup_db.py --series 665 --no-relations
```

### Environment Variables

Set database password to avoid typing it:
```bash
export DB_PASSWORD='your_password'
```

### All Options

```bash
# List series
--list

# Delete specific series episodes
--series ID [--no-relations] [--dry-run]

# Delete all episodes (keeps series)
--all-episodes [--dry-run]

# Delete all series (cascades to episodes)
--all-series [--dry-run]

# Delete all movies
--all-movies [--dry-run]

# Database connection (override defaults)
--host localhost
--port 5432
--user dispatch
--password YOUR_PASSWORD
--dbname dispatcharr
```

### Safety Features

- **Dry run mode**: Preview what would be deleted with `--dry-run`
- **Confirmation prompts**: Must type 'yes' or 'DELETE ALL' for destructive operations
- **Transaction rollback**: Any errors rollback the entire operation
- **List first**: Use `--list` to find series IDs before deleting

### Examples

```bash
# Find which series to delete
python scripts/cleanup_db.py --list

# Preview deletion
python scripts/cleanup_db.py --series 665 --dry-run

# Delete series 665 episodes and relations
python scripts/cleanup_db.py --series 665

# Nuclear option: delete all episodes (keeps series metadata)
python scripts/cleanup_db.py --all-episodes

# Delete all series (cascades to episodes too)
python scripts/cleanup_db.py --all-series
```
