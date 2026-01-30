"""Utility functions for export operations."""

import csv
import json
import logging
import sqlite3
import time
from pathlib import Path

from gooddata_export.constants import DEFAULT_DB_NAME

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
    logger.debug("Exported %d %s to %s and %s", count, name, csv_path, DEFAULT_DB_NAME)
