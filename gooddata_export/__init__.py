"""GoodData Export - Extract and export GoodData metadata.

This library provides functionality to export GoodData workspace metadata
to SQLite databases and CSV files.

Basic usage:
    from gooddata_export import export_metadata

    result = export_metadata(
        base_url="https://your-instance.gooddata.com",
        workspace_id="your_workspace_id",
        bearer_token="your_api_token",
        output_dir="output",
        export_formats=["sqlite", "csv"]
    )

    print(f"Database created at: {result['db_path']}")
"""

from gooddata_export.config import ExportConfig
from gooddata_export.export import export_all_metadata


def export_metadata(
    base_url: str,
    workspace_id: str,
    bearer_token: str,
    csv_dir: str = "output/metadata_csv",
    export_formats=None,
    include_child_workspaces: bool = False,
    child_workspace_data_types=None,
    max_parallel_workspaces: int = 5,
    enable_rich_text_extraction: bool = False,
    run_post_export: bool = True,
    debug: bool = False,
    db_path: str = "output/db/gooddata_export.db",
):
    """Export GoodData metadata to SQLite and/or CSV.

    Args:
        base_url: GoodData API base URL (e.g., "https://your-instance.gooddata.com")
        workspace_id: GoodData workspace ID to export
        bearer_token: API authentication token
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

    Returns:
        dict: Export results containing:
            - db_path: Path to the main SQLite database
            - workspace_db_path: Path to workspace-specific database copy (if created)
            - csv_dir: Directory containing CSV files (if CSV export was requested)
            - workspace_count: Number of workspaces processed
            - workspace_id: ID of the exported workspace

    Example:
        # Export everything to both SQLite and CSV
        result = export_metadata(
            base_url="https://my-instance.gooddata.com",
            workspace_id="production_workspace",
            bearer_token="your_token_here"
        )

        # Export only to SQLite for maximum speed
        result = export_metadata(
            base_url="https://my-instance.gooddata.com",
            workspace_id="production_workspace",
            bearer_token="your_token_here",
            export_formats=["sqlite"],
            run_post_export=False  # Skip duplicate detection for speed
        )

        # Export with child workspaces
        result = export_metadata(
            base_url="https://my-instance.gooddata.com",
            workspace_id="parent_workspace",
            bearer_token="your_token_here",
            include_child_workspaces=True,
            child_workspace_data_types=["dashboards", "visualizations"],
            max_parallel_workspaces=5
        )
    """
    if export_formats is None:
        export_formats = ["sqlite", "csv"]

    # Create config object
    config = ExportConfig(
        base_url=base_url,
        workspace_id=workspace_id,
        bearer_token=bearer_token,
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
    )


__all__ = ["export_metadata", "ExportConfig", "export_all_metadata"]
__version__ = "1.0.0"
