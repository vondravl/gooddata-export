"""Constants used across the gooddata_export package."""

# Default database filename used in log messages
DEFAULT_DB_NAME = "gooddata_export.db"

# Data types that can be fetched from child workspaces
# Used by CLI prompts and as default when --child-workspace-data-types is not specified
CHILD_WORKSPACE_DATA_TYPES = (
    "dashboards",
    "visualizations",
    "metrics",
    "filter_contexts",
)
