"""Constants used across the gooddata_export package."""

# Default database filename used in log messages
DEFAULT_DB_NAME = "gooddata_export.db"

# Maximum number of parallel workers for child workspace data fetching
# within a single workspace. Limits concurrency to avoid overwhelming the API.
MAX_CHILD_WORKSPACE_FETCH_WORKERS = 6

# Data types that can be fetched from child workspaces
# Used by CLI prompts and as default when --child-workspace-data-types is not specified
CHILD_WORKSPACE_DATA_TYPES = (
    "dashboards",
    "visualizations",
    "metrics",
    "filter_contexts",
)
