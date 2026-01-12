"""Utility functions for export operations."""

import csv
import json
import logging
import sqlite3
import time
from pathlib import Path

from gooddata_export.constants import DEFAULT_DB_NAME, LOCAL_MODE_STALE_TABLES

logger = logging.getLogger(__name__)


def serialize_content(content: dict, config) -> str | None:
    """Serialize content to JSON if INCLUDE_CONTENT is enabled.

    Args:
        content: The content dictionary to serialize
        config: ExportConfig instance with INCLUDE_CONTENT setting

    Returns:
        JSON string if INCLUDE_CONTENT is True, None otherwise
    """
    return json.dumps(content) if config.INCLUDE_CONTENT else None


def execute_with_retry(cursor, sql, params=None, max_retries=5):
    """Execute SQL with retry mechanism for database locks"""
    for attempt in range(max_retries):
        try:
            if params:
                if (
                    isinstance(params, list)
                    and len(params) > 0
                    and isinstance(params[0], (tuple, list))
                ):
                    # executemany case
                    cursor.executemany(sql, params)
                else:
                    # execute case
                    cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            return cursor
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < max_retries - 1:
                # Exponential backoff with jitter
                delay = (2**attempt) * 0.1 + (attempt * 0.05)
                logger.warning(
                    "Database locked, retrying in %.2fs (attempt %d/%d)",
                    delay,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(delay)
                continue
            else:
                raise
    raise sqlite3.OperationalError(
        f"Database remained locked after {max_retries} attempts"
    )


def clean_field(value):
    """Replace actual newlines with literal '\\n' string"""
    if isinstance(value, str):
        return value.replace("\n", "\\n").replace("\r", "")
    return value


def ensure_export_directory(export_dir):
    """Create export directory if it doesn't exist"""
    Path(export_dir).mkdir(parents=True, exist_ok=True)
    return export_dir


def write_to_csv(data, export_dir, filename, fieldnames, exclude_fields=None):
    """Write data to CSV file in specified directory"""
    ensure_export_directory(export_dir)
    filepath = Path(export_dir) / filename

    if exclude_fields is None:
        exclude_fields = set()
    csv_fieldnames = [f for f in fieldnames if f not in exclude_fields]

    with open(filepath, "w", encoding="utf-8-sig", newline="\n") as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=csv_fieldnames, quoting=csv.QUOTE_ALL
        )
        writer.writeheader()
        for row in data:
            cleaned_row = {
                k: clean_field(v) for k, v in row.items() if k not in exclude_fields
            }
            writer.writerow(cleaned_row)
    return len(data)


def log_export(name, count, csv_path):
    """Standardized logging for exports"""
    logger.info("Exported %d %s to %s and %s", count, name, csv_path, DEFAULT_DB_NAME)


def truncate_tables_for_local_mode(db_path):
    """Truncate tables that won't have data when using local layout.json.

    When using layout_json mode, certain tables won't receive data because
    that data isn't available in the layout JSON (requires separate API calls).
    This function truncates those tables to prevent stale data from previous
    API-mode exports from confusing users.

    Args:
        db_path: Path to the database file

    See LOCAL_MODE_STALE_TABLES in constants.py for the list of truncated tables.
    """
    db_path_obj = Path(db_path)
    if not db_path_obj.exists():
        # Database doesn't exist yet, nothing to truncate
        return

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Get list of existing tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = {row[0] for row in cursor.fetchall()}

        truncated = []
        for table in LOCAL_MODE_STALE_TABLES:
            if table in existing_tables:
                # Defense-in-depth: verify table name is alphanumeric/underscore only
                # This prevents SQL injection if LOCAL_MODE_STALE_TABLES is ever modified
                if not table.replace("_", "").isalnum():
                    raise ValueError(f"Invalid table name: {table}")
                cursor.execute(f"DELETE FROM {table}")  # noqa: S608
                truncated.append(table)

        if truncated:
            conn.commit()
            conn.execute("VACUUM")  # Reclaim disk space from deleted rows
            logger.info(
                "Truncated stale tables (not in layout.json): %s", ", ".join(truncated)
            )
    except Exception as e:
        # Don't fail the export if truncation fails
        logger.warning("Could not truncate stale tables: %s", e)
    finally:
        if conn:
            conn.close()
