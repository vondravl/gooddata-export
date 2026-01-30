"""Export module for GoodData metadata.

This module provides functions for exporting GoodData workspace metadata
to SQLite databases and CSV files.

Main entry point:
    export_all_metadata - Orchestrates the full export process
"""

import logging
import os
import shutil
import sqlite3
from pathlib import Path

from gooddata_export.common import ExportError
from gooddata_export.db import store_workspace_metadata
from gooddata_export.export.fetch import fetch_all_workspace_data
from gooddata_export.export.writers import (
    export_dashboards,
    export_dashboards_metrics,
    export_dashboards_permissions,
    export_filter_contexts,
    export_ldm,
    export_metrics,
    export_plugins,
    export_users_and_user_groups,
    export_visualizations,
    export_workspaces,
)
from gooddata_export.post_export import run_post_export_sql
from gooddata_export.process import validate_workspace_exists

logger = logging.getLogger(__name__)


def export_all_metadata(
    config,
    csv_dir=None,
    db_path="output/db/gooddata_export.db",
    export_formats=None,
    run_post_export=True,
    layout_json: dict | None = None,
):
    """Export all metadata to SQLite database and CSV files.

    Args:
        config: ExportConfig instance with GoodData credentials and options
        csv_dir: Directory for CSV files (default: None, uses "output/metadata_csv" if csv in formats)
        db_path: Path to SQLite database file (default: "output/db/gooddata_export.db")
        export_formats: List of formats to export ("sqlite", "csv") (default: both)
        run_post_export: Whether to run post-export SQL processing (default: True)
        layout_json: Optional local layout JSON data. When provided, skips API fetch
                     and uses this data directly. Expected format:
                     {"analytics": {"metrics": [...], ...}, "ldm": {"datasets": [...], ...}}

    Returns:
        dict: Export results with db_path, csv_dir, and workspace_count
    """
    if export_formats is None:
        export_formats = ["sqlite", "csv"]

    # Set up CSV directory
    if "csv" in export_formats and csv_dir is None:
        csv_dir = "output/metadata_csv"
    export_dir = csv_dir if "csv" in export_formats else None

    # Set up database path
    db_path_obj = Path(db_path)
    errors = []

    # Ensure database directory exists (databases overwrite themselves, no cleanup needed)
    db_path_obj.parent.mkdir(parents=True, exist_ok=True)

    # Clean CSV directory completely to avoid stale data (files mix together, so we need a clean slate)
    if export_dir and Path(export_dir).exists():
        logger.info("Cleaning CSV directory: %s", export_dir)
        shutil.rmtree(export_dir)

    # Ensure CSV directory exists if needed
    if export_dir:
        Path(export_dir).mkdir(parents=True, exist_ok=True)

    if layout_json is not None:
        # Local mode: use layout_json directly (already in layout format)
        logger.info("Using local layout JSON for workspace %s", config.WORKSPACE_ID)

        # Validate layout_json structure and warn about missing keys
        if "analytics" not in layout_json:
            logger.warning(
                "layout_json missing 'analytics' key - no analytics data will be exported"
            )
        if "ldm" not in layout_json:
            logger.warning(
                "layout_json missing 'ldm' key - no LDM data will be exported"
            )

        analytics = layout_json.get("analytics", {})
        ldm = layout_json.get("ldm", {})

        all_workspace_data = [
            {
                "workspace_id": config.WORKSPACE_ID,
                "workspace_name": f"Local Layout ({config.WORKSPACE_ID})",
                "is_parent": True,
                "data": {
                    "metrics": analytics.get("metrics") or [],
                    "dashboards": analytics.get("analyticalDashboards") or [],
                    "visualizations": analytics.get("visualizationObjects") or [],
                    "filter_contexts": analytics.get("filterContexts") or [],
                    # Not available in local mode:
                    # - plugins: layout.json only contains plugins used on dashboards, not all
                    #   workspace plugins. Using partial data could be confusing.
                    # - child_workspaces, users_and_user_groups: require separate API calls
                    "plugins": None,
                    "child_workspaces": None,
                    "users_and_user_groups": None,
                    # LDM and analytics_model come from layout.json directly:
                    "ldm": {"ldm": ldm} if ldm else None,
                    "analytics_model": layout_json,
                },
            }
        ]
    else:
        # API mode: validate workspace and fetch from GoodData API
        logger.info("Validating workspace access...")
        validate_workspace_exists(config=config)

        logger.info("Fetching data from GoodData API...")
        all_workspace_data = fetch_all_workspace_data(config)

    if config.DEBUG_WORKSPACE_PROCESSING:
        logger.debug(
            "Successfully fetched data from %d workspace(s)", len(all_workspace_data)
        )
        for ws in all_workspace_data:
            logger.debug("  - %s (%s)", ws["workspace_name"], ws["workspace_id"])

    # Export functions to run sequentially (workspaces first for reference)
    export_functions = [
        export_workspaces,
        export_metrics,
        export_visualizations,
        export_dashboards,
        export_dashboards_metrics,
        export_dashboards_permissions,
        export_plugins,
        export_ldm,
        export_filter_contexts,
        export_users_and_user_groups,
    ]

    # Execute each export function with all workspace data
    # Note: Database writes are kept sequential to avoid SQLite concurrency issues
    logger.info("Processing and writing data to database...")
    logger.info("=" * 80)
    for export_func in export_functions:
        try:
            export_func(all_workspace_data, export_dir, config, db_path)
        except Exception as e:
            # Log full exception with traceback for debugging
            logger.exception("Error in %s", export_func.__name__)
            # Store truncated message for user-facing error summary
            error_msg = str(e).split("\n")[0]
            errors.append(f"{export_func.__name__}: {error_msg}")

    if errors:
        # Raise detailed error messages
        workspace_id = config.WORKSPACE_ID
        error_details = "\n  - ".join(errors)
        raise ExportError(
            f"Export failed for workspace: {workspace_id}\n"
            f"Errors encountered:\n  - {error_details}"
        )

    # Run post-export processing if requested
    # When child workspaces are included, filter enrichment to parent workspace only
    if run_post_export:
        logger.info("")
        logger.info("Running post-export processing...")
        logger.info("=" * 80)
        if config.INCLUDE_CHILD_WORKSPACES:
            # Multi-workspace: enrich only parent workspace to avoid confusing duplicates
            logger.info("Multi-workspace mode: enriching parent workspace only")
            run_post_export_sql(db_path, parent_workspace_id=config.WORKSPACE_ID)
        else:
            # Single workspace: enrich all data (no filter needed)
            run_post_export_sql(db_path)
    else:
        logger.info("Skipping post-export processing (disabled)")

    # Store workspace metadata
    workspace_id = config.WORKSPACE_ID
    export_mode = "local" if layout_json is not None else "api"
    store_workspace_metadata(db_path, config, export_mode=export_mode)

    # Vacuum database to reclaim space (especially after content exclusion or truncation)
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("VACUUM")
        conn.close()
        final_size = os.path.getsize(db_path) / 1024 / 1024
        logger.info("Database vacuumed (final size: %.1f MB)", final_size)
    except Exception as e:
        logger.warning("Could not vacuum database: %s", e)

    # Create workspace-specific database copy
    workspace_db = None
    try:
        workspace_db = db_path_obj.parent / f"{workspace_id}.db"
        # Create a copy of the database with workspace_id name
        shutil.copy(db_path, workspace_db)
        logger.info("Created workspace-specific database: %s", workspace_db)
    except Exception as e:
        logger.warning("Could not create workspace-specific database: %s", e)

    # Success message
    total_workspaces = len(all_workspace_data)
    if total_workspaces > 1:
        logger.info(
            "Successfully processed %d workspaces (%d child workspaces)",
            total_workspaces,
            total_workspaces - 1,
        )
    else:
        logger.info("Successfully processed parent workspace")

    return {
        "db_path": db_path,
        "workspace_db_path": workspace_db,
        "csv_dir": export_dir,
        "workspace_count": total_workspaces,
        "workspace_id": workspace_id,
    }


__all__ = ["export_all_metadata"]
