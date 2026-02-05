"""GoodData data processing module.

This module provides functions for fetching and processing GoodData metadata.
It is organized into submodules:
- entities: Entity API functions (process_*, fetch_child_workspaces, validate_workspace_exists)
- layout: Layout API functions (fetch_ldm, fetch_users_and_user_groups, fetch_analytics_model)
- rich_text: Rich text extraction utilities
- common: Shared utilities (sort_tags, etc.)
"""

# Re-export public API from submodules
from gooddata_export.process.common import (
    DEBUG_RICH_TEXT,
    UniqueRelationshipTracker,
    get_debug_output_dir,
    import_time_iso,
    sort_tags,
)
from gooddata_export.process.entities import (
    fetch_child_workspaces,
    process_dashboards,
    process_dashboards_plugins,
    process_dashboards_references,
    process_dashboards_visualizations,
    process_dashboards_widget_filters,
    process_filter_context_fields,
    process_filter_contexts,
    process_metrics,
    process_plugins,
    process_visualizations,
    process_visualizations_references,
    process_workspaces,
    validate_workspace_exists,
)
from gooddata_export.process.layout import (
    fetch_analytics_model,
    fetch_ldm,
    fetch_users_and_user_groups,
    process_dashboards_permissions_from_analytics_model,
    process_ldm,
    process_user_group_members,
    process_user_groups,
    process_users,
)
from gooddata_export.process.rich_text import (
    debug_rich_text_extraction,
    extract_all_ids_from_content,
    extract_from_rich_text,
    extract_values_from_curly_braces,
    process_dashboards_metrics_from_rich_text,
    process_rich_text_insights,
    process_rich_text_metrics,
)

__all__ = [
    # Common
    "DEBUG_RICH_TEXT",
    "UniqueRelationshipTracker",
    "get_debug_output_dir",
    "import_time_iso",
    "sort_tags",
    # Entities - fetching
    "fetch_child_workspaces",
    "validate_workspace_exists",
    # Entities - processing
    "process_metrics",
    "process_plugins",
    "process_visualizations",
    "process_visualizations_references",
    "process_dashboards",
    "process_dashboards_plugins",
    "process_dashboards_references",
    "process_dashboards_visualizations",
    "process_dashboards_widget_filters",
    "process_filter_contexts",
    "process_filter_context_fields",
    "process_workspaces",
    # Layout
    "fetch_ldm",
    "fetch_users_and_user_groups",
    "fetch_analytics_model",
    "process_ldm",
    "process_users",
    "process_user_groups",
    "process_user_group_members",
    "process_dashboards_permissions_from_analytics_model",
    # Rich text
    "debug_rich_text_extraction",
    "extract_all_ids_from_content",
    "extract_from_rich_text",
    "extract_values_from_curly_braces",
    "process_rich_text_insights",
    "process_rich_text_metrics",
    "process_dashboards_metrics_from_rich_text",
]
