#!/usr/bin/env python3
"""
Database Cleanup Script for VOD Content
Deletes episodes and relations for a specific series or all content.
"""

import argparse
import sys
import os
import psycopg2
from psycopg2 import sql

# Default DB credentials (override with args or env vars)
DEFAULT_DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'dispatcharr',
    'user': os.getenv('DB_USER', 'dispatch'),
    'password': os.getenv('DB_PASSWORD', ''),
}


def connect_db(config):
    """Connect to PostgreSQL database."""
    try:
        conn = psycopg2.connect(**config)
        return conn
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}", file=sys.stderr)
        sys.exit(1)


def delete_series_episodes(conn, series_id, delete_relations=True, dry_run=False):
    """
    Delete all episodes for a specific series.

    Args:
        conn: Database connection
        series_id: Series ID to delete episodes for
        delete_relations: Also delete M3U episode relations
        dry_run: Preview what would be deleted without deleting
    """
    cursor = conn.cursor()

    try:
        # Count what will be deleted
        cursor.execute("SELECT COUNT(*) FROM vod_episode WHERE series_id = %s", (series_id,))
        episode_count = cursor.fetchone()[0]

        if delete_relations:
            # Note: This deletes relations where the relation ID matches series_id
            # You might want to adjust this logic based on your schema
            cursor.execute("SELECT COUNT(*) FROM vod_m3uepisoderelation WHERE id = %s", (series_id,))
            relation_count = cursor.fetchone()[0]
        else:
            relation_count = 0

        print(f"\n=== Series ID: {series_id} ===")
        print(f"Episodes to delete: {episode_count}")
        if delete_relations:
            print(f"Relations to delete: {relation_count}")

        if dry_run:
            print("\n[DRY RUN] No changes made.")
            return

        # Confirm deletion
        if episode_count > 0 or relation_count > 0:
            confirm = input(f"\nDelete {episode_count} episodes" +
                          (f" and {relation_count} relations" if delete_relations else "") +
                          f"? (yes/no): ")
            if confirm.lower() != 'yes':
                print("Aborted.")
                return
        else:
            print("Nothing to delete.")
            return

        # Delete episodes
        if episode_count > 0:
            cursor.execute("DELETE FROM vod_episode WHERE series_id = %s", (series_id,))
            print(f"✓ Deleted {cursor.rowcount} episodes")

        # Delete relations
        if delete_relations and relation_count > 0:
            cursor.execute("DELETE FROM vod_m3uepisoderelation WHERE id = %s", (series_id,))
            print(f"✓ Deleted {cursor.rowcount} relations")

        conn.commit()
        print("\n✓ Changes committed")

    except Exception as e:
        conn.rollback()
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        cursor.close()


def delete_all_episodes(conn, dry_run=False):
    """Delete ALL episodes from all series."""
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM vod_episode")
        count = cursor.fetchone()[0]

        print(f"\n=== ALL EPISODES ===")
        print(f"Total episodes to delete: {count}")

        if dry_run:
            print("\n[DRY RUN] No changes made.")
            return

        if count > 0:
            confirm = input(f"\n⚠️  DELETE ALL {count} EPISODES? THIS CANNOT BE UNDONE! (type 'DELETE ALL'): ")
            if confirm != 'DELETE ALL':
                print("Aborted.")
                return

            cursor.execute("DELETE FROM vod_episode")
            print(f"✓ Deleted {cursor.rowcount} episodes")
            conn.commit()
            print("✓ Changes committed")
        else:
            print("No episodes to delete.")

    except Exception as e:
        conn.rollback()
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        cursor.close()


def delete_all_series(conn, dry_run=False):
    """Delete ALL series (cascades to episodes)."""
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM vod_series")
        series_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM vod_episode")
        episode_count = cursor.fetchone()[0]

        print(f"\n=== ALL SERIES ===")
        print(f"Series to delete: {series_count}")
        print(f"Episodes to cascade delete: {episode_count}")

        if dry_run:
            print("\n[DRY RUN] No changes made.")
            return

        if series_count > 0:
            confirm = input(f"\n⚠️  DELETE ALL {series_count} SERIES AND {episode_count} EPISODES? (type 'DELETE ALL'): ")
            if confirm != 'DELETE ALL':
                print("Aborted.")
                return

            cursor.execute("DELETE FROM vod_series")
            print(f"✓ Deleted {cursor.rowcount} series (+ cascaded episodes)")
            conn.commit()
            print("✓ Changes committed")
        else:
            print("No series to delete.")

    except Exception as e:
        conn.rollback()
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        cursor.close()


def delete_all_movies(conn, dry_run=False):
    """Delete ALL movies."""
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM vod_movie")
        count = cursor.fetchone()[0]

        print(f"\n=== ALL MOVIES ===")
        print(f"Movies to delete: {count}")

        if dry_run:
            print("\n[DRY RUN] No changes made.")
            return

        if count > 0:
            confirm = input(f"\n⚠️  DELETE ALL {count} MOVIES? (type 'DELETE ALL'): ")
            if confirm != 'DELETE ALL':
                print("Aborted.")
                return

            cursor.execute("DELETE FROM vod_movie")
            print(f"✓ Deleted {cursor.rowcount} movies")
            conn.commit()
            print("✓ Changes committed")
        else:
            print("No movies to delete.")

    except Exception as e:
        conn.rollback()
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        cursor.close()


def list_series(conn, limit=50):
    """List series with episode counts."""
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT s.id, s.name, s.year, COUNT(e.id) as episode_count
            FROM vod_series s
            LEFT JOIN vod_episode e ON e.series_id = s.id
            GROUP BY s.id, s.name, s.year
            ORDER BY s.id DESC
            LIMIT %s
        """, (limit,))

        results = cursor.fetchall()

        print(f"\n=== Series List (most recent {limit}) ===")
        print(f"{'ID':<8} {'Name':<50} {'Year':<8} {'Episodes':<10}")
        print("-" * 80)

        for row in results:
            series_id, name, year, ep_count = row
            print(f"{series_id:<8} {name[:50]:<50} {year or 'N/A':<8} {ep_count:<10}")

    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
    finally:
        cursor.close()


def main():
    parser = argparse.ArgumentParser(
        description='Clean up VOD database (episodes, series, movies)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List series to find IDs
  %(prog)s --list

  # Delete episodes for series 665 (preview first)
  %(prog)s --series 665 --dry-run
  %(prog)s --series 665

  # Delete episodes for series 665 without deleting relations
  %(prog)s --series 665 --no-relations

  # Delete all episodes (keeps series)
  %(prog)s --all-episodes --dry-run
  %(prog)s --all-episodes

  # Delete all series (cascades to episodes)
  %(prog)s --all-series

  # Delete all movies
  %(prog)s --all-movies

  # Custom database connection
  %(prog)s --series 665 --host localhost --port 5432 --user dispatch --dbname dispatcharr
        """
    )

    # Actions
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument('--series', type=int, metavar='ID',
                             help='Delete episodes for specific series ID')
    action_group.add_argument('--all-episodes', action='store_true',
                             help='Delete ALL episodes (keeps series)')
    action_group.add_argument('--all-series', action='store_true',
                             help='Delete ALL series (cascades to episodes)')
    action_group.add_argument('--all-movies', action='store_true',
                             help='Delete ALL movies')
    action_group.add_argument('--list', action='store_true',
                             help='List series with episode counts')

    # Options
    parser.add_argument('--no-relations', action='store_true',
                       help='Skip deleting M3U episode relations (only with --series)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview what would be deleted without deleting')

    # Database connection
    parser.add_argument('--host', default=DEFAULT_DB_CONFIG['host'],
                       help=f"Database host (default: {DEFAULT_DB_CONFIG['host']})")
    parser.add_argument('--port', type=int, default=DEFAULT_DB_CONFIG['port'],
                       help=f"Database port (default: {DEFAULT_DB_CONFIG['port']})")
    parser.add_argument('--user', default=DEFAULT_DB_CONFIG['user'],
                       help=f"Database user (default: {DEFAULT_DB_CONFIG['user']})")
    parser.add_argument('--password', default=DEFAULT_DB_CONFIG['password'],
                       help="Database password (default: from env DB_PASSWORD)")
    parser.add_argument('--dbname', default=DEFAULT_DB_CONFIG['dbname'],
                       help=f"Database name (default: {DEFAULT_DB_CONFIG['dbname']})")

    args = parser.parse_args()

    # Build connection config
    db_config = {
        'host': args.host,
        'port': args.port,
        'user': args.user,
        'password': args.password,
        'dbname': args.dbname,
    }

    # Connect to database
    conn = connect_db(db_config)

    try:
        if args.list:
            list_series(conn)
        elif args.series:
            delete_series_episodes(conn, args.series,
                                  delete_relations=not args.no_relations,
                                  dry_run=args.dry_run)
        elif args.all_episodes:
            delete_all_episodes(conn, dry_run=args.dry_run)
        elif args.all_series:
            delete_all_series(conn, dry_run=args.dry_run)
        elif args.all_movies:
            delete_all_movies(conn, dry_run=args.dry_run)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
