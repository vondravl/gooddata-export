"""Database utilities for GoodData metadata export."""
import datetime
import logging
import os
import sqlite3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def connect_database(db_name):
    """Connect to SQLite database, creating directory if needed."""
    # Ensure the database directory exists
    os.makedirs(os.path.dirname(db_name) if os.path.dirname(db_name) else ".", exist_ok=True)

    # Create connection
    conn = sqlite3.connect(db_name)

    return conn


def setup_table(conn, table_name, columns):
    """Create or recreate a table with specified columns"""
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    columns_sql = ", ".join(
        [f"{col_name} {col_type}" for col_name, col_type in columns.items()]
    )
    cursor.execute(f"CREATE TABLE {table_name} ({columns_sql})")
    return cursor


def ensure_dictionary_metadata_table(conn):
    """Ensure the dictionary_metadata table exists without dropping it."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dictionary_metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    conn.commit()


def upsert_dictionary_metadata(conn, data):
    """Upsert multiple key/value pairs into dictionary_metadata.

    Ensures the table exists, then performs INSERT OR REPLACE for each item
    and commits the transaction.
    """
    ensure_dictionary_metadata_table(conn)
    cursor = conn.cursor()
    rows = []
    for key, value in (data or {}).items():
        # Normalize None to empty string to avoid NULL surprises in UI reads
        rows.append((str(key), "" if value is None else str(value)))
    if rows:
        cursor.executemany(
            "INSERT OR REPLACE INTO dictionary_metadata (key, value) VALUES (?, ?)",
            rows,
        )
        conn.commit()


def store_workspace_metadata(db_path, config, update_timestamp: bool = True):
    """Store the current workspace_id and optionally update timestamp in the database metadata.

    Args:
        db_path: Path to the database file
        config: ExportConfig instance with workspace and API information
        update_timestamp: Whether to update the last_updated timestamp

    When switching databases for viewing, pass update_timestamp=False to preserve the original
    refresh time stored in the workspace-specific database file.
    """
    try:
        db = connect_database(db_path)

        # Build upsert payload
        payload = {
            "workspace_id": config.WORKSPACE_ID,
            "base_url": config.BASE_URL or "",
        }
        if update_timestamp:
            payload["last_updated"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Record child workspace processing flags if they were used
        if config.INCLUDE_CHILD_WORKSPACES:
            payload["include_child_workspaces"] = "true"
            if config.CHILD_WORKSPACE_DATA_TYPES:
                payload["child_data_types"] = ",".join(config.CHILD_WORKSPACE_DATA_TYPES)

        upsert_dictionary_metadata(db, payload)

        last_updated = payload.get("last_updated")
        if last_updated:
            logger.info(
                f"Stored workspace metadata with last update time: {last_updated}"
            )
        else:
            logger.info("Stored workspace metadata (timestamp unchanged)")

        db.commit()
        db.close()
    except sqlite3.Error as e:
        logger.error(f"Failed to store workspace metadata: {str(e)}")

