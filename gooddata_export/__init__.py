"""GoodData Export - Extract and export GoodData metadata.

This library provides functionality to export GoodData workspace metadata
to SQLite databases and CSV files.

Basic usage:
    from gooddata_export import export_metadata

    result = export_metadata(
        base_url="https://your-instance.gooddata.com",
        workspace_id="your_workspace_id",
        bearer_token="your_api_token",
        export_formats=["sqlite", "csv"]
    )
    # result contains: db_path, workspace_db_path, csv_dir, workspace_count, workspace_id
"""

from importlib.metadata import PackageNotFoundError, version

from gooddata_export.common import ExportError
from gooddata_export.config import ExportConfig
from gooddata_export.export import export_all_metadata


def export_metadata(
    base_url: str,
    workspace_id: str,
    bearer_token: str | None = None,
    csv_dir: str = "output/metadata_csv",
    export_formats=None,
    include_child_workspaces: bool = False,
    child_workspace_data_types=None,
    max_parallel_workspaces: int = 5,
    enable_rich_text_extraction: bool = False,
    run_post_export: bool = True,
    debug: bool = False,
    db_path: str = "output/db/gooddata_export.db",
    layout_json: dict | None = None,
):
    """Export GoodData metadata to SQLite and/or CSV.

    Supports two modes:
    1. API mode (default): Fetches data from GoodData API using bearer_token
    2. Local mode: Uses provided layout_json data directly (no API calls)

    Args:
        base_url: GoodData API base URL (e.g., "https://your-instance.gooddata.com")
        workspace_id: GoodData workspace ID to export
        bearer_token: API authentication token (required for API mode, optional for local mode)
        csv_dir: Directory for CSV files (default: "output/metadata_csv")
        export_formats: List of formats to export - ["sqlite"], ["csv"], or ["sqlite", "csv"] (default: both)
        include_child_workspaces: Whether to process child workspaces (default: False)
        child_workspace_data_types: List of data types to fetch from child workspaces
            Options: "metrics", "dashboards", "visualizations", "filter_contexts"
            (default: None - fetches all if include_child_workspaces is True)
        max_parallel_workspaces: Number of workspaces to process in parallel (default: 5)
        enable_rich_text_extraction: Whether to extract from rich text widgets (default: False)
        run_post_export: Whether to run post-export SQL processing for duplicate detection (default: True)
        debug: Enable debug logging (default: False)
        db_path: Custom path for the SQLite database (default: "output/db/gooddata_export.db")
        layout_json: Optional local layout JSON data. When provided, skips API fetch
            and uses this data directly. Expected format:
            {"analytics": {"metrics": [...], ...}, "ldm": {"datasets": [...], ...}}

    Returns:
        dict: Export results containing:
            - db_path: Path to the main SQLite database
            - workspace_db_path: Path to workspace-specific database copy (if created)
            - csv_dir: Directory containing CSV files (if CSV export was requested)
            - workspace_count: Number of workspaces processed
            - workspace_id: ID of the exported workspace

    Example:
        # Export from GoodData API
        result = export_metadata(
            base_url="https://my-instance.gooddata.com",
            workspace_id="production_workspace",
            bearer_token="your_token_here"
        )

        # Export from local layout.json file (no API calls)
        import json
        with open("layout.json") as f:
            layout = json.load(f)
        result = export_metadata(
            base_url="https://my-instance.gooddata.com",  # Used for URL generation
            workspace_id="my_workspace",
            layout_json=layout,
            export_formats=["sqlite"]
        )

        # Export with child workspaces (API mode only)
        result = export_metadata(
            base_url="https://my-instance.gooddata.com",
            workspace_id="parent_workspace",
            bearer_token="your_token_here",
            include_child_workspaces=True,
            child_workspace_data_types=["dashboards", "visualizations"],
            max_parallel_workspaces=5
        )
    """
    # Validate: bearer_token required unless layout_json provided
    if layout_json is None and bearer_token is None:
        raise ValueError("bearer_token is required when not using layout_json")

    if export_formats is None:
        export_formats = ["sqlite", "csv"]

    # Create config object
    config = ExportConfig(
        base_url=base_url,
        workspace_id=workspace_id,
        bearer_token=bearer_token or "",  # Empty string when not needed
        include_child_workspaces=include_child_workspaces,
        child_workspace_data_types=child_workspace_data_types,
        max_parallel_workspaces=max_parallel_workspaces,
        enable_rich_text_extraction=enable_rich_text_extraction,
        debug_workspace_processing=debug,
        load_from_env=False,  # Don't load from .env when using this API
    )

    # Call the export function
    return export_all_metadata(
        config=config,
        csv_dir=csv_dir if "csv" in export_formats else None,
        db_path=db_path,
        export_formats=export_formats,
        run_post_export=run_post_export,
        layout_json=layout_json,
    )


__all__ = ["export_metadata", "ExportConfig", "ExportError", "export_all_metadata"]

try:
    __version__ = version("gooddata-export")
except PackageNotFoundError:
    __version__ = "0.0.0.dev"  # Fallback for development without install
