"""Constants used across the gooddata_export package."""

# Default database filename used in log messages
DEFAULT_DB_NAME = "gooddata_export.db"

# Maximum number of parallel workers for child workspace data fetching
# within a single workspace. Limits concurrency to avoid overwhelming the API.
MAX_CHILD_WORKSPACE_FETCH_WORKERS = 6

# Tables that should be truncated when using local layout.json mode.
# These tables require API data that isn't available in layout.json files:
# - users: Requires usersAndUserGroups API
# - user_groups: Requires usersAndUserGroups API
# - user_group_members: Requires usersAndUserGroups API
# - plugins: layout.json only contains plugins used on dashboards, not all workspace
#            plugins. Using partial data could be confusing. The dashboards_plugins
#            junction table (extracted from dashboard content) IS reliable in local mode.
LOCAL_MODE_STALE_TABLES = [
    "users",
    "user_groups",
    "user_group_members",
    "plugins",
]
