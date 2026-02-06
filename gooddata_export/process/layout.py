"""Layout API functions for fetching and processing GoodData layout data."""

import logging
from typing import TYPE_CHECKING, Any

import requests

if TYPE_CHECKING:
    from gooddata_export.config import ExportConfig

from gooddata_export.common import (
    get_api_client,
    raise_for_api_error,
    raise_for_request_error,
)
from gooddata_export.process.common import sort_tags

logger = logging.getLogger(__name__)

# Maps GoodData date granularity names to human-readable titles.
# Used when creating synthetic ldm_columns entries for date instance granularities.
GRANULARITY_TITLES = {
    "MINUTE": "Minute",
    "HOUR": "Hour",
    "DAY": "Day",
    "WEEK": "Week",
    "MONTH": "Month",
    "QUARTER": "Quarter",
    "YEAR": "Year",
    "MINUTE_OF_HOUR": "Minute of Hour",
    "HOUR_OF_DAY": "Hour of Day",
    "DAY_OF_WEEK": "Day of Week",
    "DAY_OF_MONTH": "Day of Month",
    "DAY_OF_QUARTER": "Day of Quarter",
    "DAY_OF_YEAR": "Day of Year",
    "WEEK_OF_YEAR": "Week of Year",
    "MONTH_OF_YEAR": "Month of Year",
    "QUARTER_OF_YEAR": "Quarter of Year",
}


def _fetch_from_layout_api(
    endpoint: str,
    name: str,
    client: dict[str, Any] | None = None,
    config: "ExportConfig | None" = None,
    timeout: int = 30,
    workspace_scoped: bool = False,
    session: "requests.Session | None" = None,
):
    """Generic fetcher for GoodData layout API endpoints.

    Args:
        endpoint: API endpoint path (e.g., 'usersAndUserGroups' or 'logicalModel')
        name: Human-readable name for logging (e.g., 'users and user groups')
        client: API client dict (optional if config provided)
        config: ExportConfig instance (optional if client provided)
        timeout: Request timeout in seconds
        workspace_scoped: If True, endpoint is workspace-specific
                         (uses /workspaces/{id}/{endpoint})
        session: Optional requests.Session for connection pooling.
            If None, uses requests module directly (no connection reuse).

    Returns:
        Parsed JSON response or None if empty
    """
    try:
        client = get_api_client(config=config, client=client)
        # Use session if provided, otherwise fall back to requests module
        http = session if session is not None else requests

        if workspace_scoped:
            url = f"{client['base_url']}/api/v1/layout/workspaces/{client['workspace_id']}/{endpoint}"
            logger.debug("Fetching %s from workspace: %s", name, client["workspace_id"])
        else:
            url = f"{client['base_url']}/api/v1/layout/{endpoint}"
            logger.debug("Fetching %s", name)

        response = http.get(url, headers=client["headers"], timeout=timeout)

        if response.status_code == 200:
            json_input = response.json()

            if not json_input:
                logger.warning(
                    f"{name.capitalize()}: No data received (empty response)"
                )
                return None

            logger.debug("%s: Successfully fetched", name.capitalize())
            return json_input

        else:
            workspace_id = client["workspace_id"] if workspace_scoped else None
            raise_for_api_error(response, name, workspace_id)

    except requests.exceptions.RequestException as e:
        raise_for_request_error(
            name, e, base_url=client.get("base_url") if client else None
        )

    except RuntimeError:
        # Re-raise RuntimeErrors without wrapping
        raise

    except Exception as e:
        error_msg = f"Unexpected error fetching {name}: {str(e)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)


def fetch_users_and_user_groups(
    client: dict[str, Any] | None = None,
    config: "ExportConfig | None" = None,
    session: "requests.Session | None" = None,
):
    """Fetch users and user groups from GoodData API."""
    return _fetch_from_layout_api(
        endpoint="usersAndUserGroups",
        name="users and user groups",
        client=client,
        config=config,
        timeout=30,
        workspace_scoped=False,
        session=session,
    )


def fetch_ldm(
    client: dict[str, Any] | None = None,
    config: "ExportConfig | None" = None,
    session: "requests.Session | None" = None,
):
    """Fetch logical model from GoodData API."""
    return _fetch_from_layout_api(
        endpoint="logicalModel",
        name="logical model",
        client=client,
        config=config,
        timeout=30,
        workspace_scoped=True,
        session=session,
    )


def fetch_analytics_model(
    client: dict[str, Any] | None = None,
    config: "ExportConfig | None" = None,
    session: "requests.Session | None" = None,
):
    """Fetch analytics model from GoodData layout API.

    This endpoint returns all analytics objects (dashboards, visualizations, metrics)
    with their full layout including permissions.

    Endpoint: /api/v1/layout/workspaces/{workspace_id}/analyticsModel
    """
    return _fetch_from_layout_api(
        endpoint="analyticsModel",
        name="analytics model",
        client=client,
        config=config,
        timeout=60,  # Larger timeout for analytics model (can be big)
        workspace_scoped=True,
        session=session,
    )


def process_ldm(data):
    """Parse logical model data into datasets, columns, and labels.

    Returns:
        tuple: (datasets, columns, labels) where:
            - datasets: List of dataset records (including date instances)
            - columns: List of column records (attributes, facts, references, date granularities)
            - labels: List of attribute label records
    """
    datasets = []
    columns = []
    labels = []

    # Process date instances first (they can be referenced by regular datasets)
    # Date instances have granularities like DAY, WEEK, MONTH that become label references
    # e.g., {label/process_date.day} references the DAY granularity of process_date
    for date_instance in data.get("ldm", {}).get("dateInstances", []):
        date_id = date_instance["id"]

        # Add date instance as a dataset (type: date)
        datasets.append(
            {
                "title": date_instance.get("title", date_id),
                "description": date_instance.get("description", ""),
                "id": date_id,
                "tags": str(sort_tags(date_instance.get("tags", []))),
                "attributes_count": len(date_instance.get("granularities", [])),
                "facts_count": 0,
                "references_count": 0,
                "workspace_data_filter_columns_count": 0,
                "total_columns": len(date_instance.get("granularities", [])),
                "data_source_id": "",
                "source_table": "",
                "source_table_path": "",
            }
        )

        # Create synthetic attribute columns for each granularity
        # These are what {label/date_id.granularity} references in MAQL
        for granularity in date_instance.get("granularities", []):
            # ID format matches MAQL: {label/process_date.day} -> process_date.day
            granularity_id = f"{date_id}.{granularity.lower()}"
            columns.append(
                {
                    "dataset_id": date_id,
                    "dataset_name": date_instance.get("title", date_id),
                    "title": GRANULARITY_TITLES.get(granularity, granularity),
                    "description": "",
                    "id": granularity_id,
                    "tags": "",
                    "data_type": "DATE",
                    "source_column": "",
                    "type": "attribute",  # Date granularities are treated as attributes
                    "grain": "No",
                    "reference_to_id": "",
                    "reference_to_title": "",
                }
            )

    # First pass: Process all regular datasets basic info
    dataset_map = {}
    for dataset in data["ldm"]["datasets"]:
        dataset_info = {
            "title": dataset["title"],
            "description": dataset.get("description", ""),
            "id": dataset["id"],
            "tags": str(sort_tags(dataset.get("tags", ""))),
            "attributes_count": len(dataset.get("attributes", [])),
            "facts_count": len(dataset.get("facts", [])),
            "references_count": len(dataset.get("references", [])),
            "workspace_data_filter_columns_count": len(
                dataset.get("workspaceDataFilterColumns", [])
            ),
            "total_columns": (
                len(dataset.get("attributes", []))
                + len(dataset.get("facts", []))
                + len(dataset.get("references", []))
                + len(dataset.get("workspaceDataFilterColumns", []))
            ),
            "data_source_id": (
                dataset.get("dataSourceTableId", {}).get("dataSourceId", "")
                or dataset.get("sql", {}).get("dataSourceId", "")
            ),
            "source_table": (
                dataset.get("dataSourceTableId", {}).get("id", "")
                if dataset.get("dataSourceTableId")
                else dataset.get("sql", {}).get("statement", "")
            ),
            "source_table_path": str(
                dataset.get("dataSourceTableId", {}).get("path", [])
            )
            if dataset.get("dataSourceTableId")
            else "SQL Query",
        }
        datasets.append(dataset_info)
        dataset_map[dataset["id"]] = dataset_info

    # Second pass: Process columns including references
    for dataset in data["ldm"]["datasets"]:
        # Add attributes
        for attr in dataset.get("attributes", []):
            columns.append(
                {
                    "dataset_id": dataset["id"],
                    "dataset_name": dataset["title"],
                    "title": attr["title"],
                    "description": attr.get("description", ""),
                    "id": attr["id"],
                    "tags": str(sort_tags(attr.get("tags", ""))),
                    "data_type": attr["sourceColumnDataType"],
                    "source_column": attr.get("sourceColumn", ""),
                    "type": "attribute",
                    "grain": "Yes"
                    if any(g["id"] == attr["id"] for g in dataset.get("grain", []))
                    else "No",
                    "reference_to_id": "",
                    "reference_to_title": "",
                }
            )

            # Extract labels from attribute
            default_view_id = attr.get("defaultView", {}).get("id", "")
            for label in attr.get("labels", []):
                labels.append(
                    {
                        "dataset_id": dataset["id"],
                        "attribute_id": attr["id"],
                        "id": label["id"],
                        "title": label.get("title", ""),
                        "description": label.get("description", ""),
                        "source_column": label.get("sourceColumn", ""),
                        "source_column_data_type": label.get(
                            "sourceColumnDataType", ""
                        ),
                        "value_type": label.get("valueType", ""),
                        "tags": str(sort_tags(label.get("tags", []))),
                        "is_default": "Yes" if label["id"] == default_view_id else "No",
                    }
                )

        # Add facts
        for fact in dataset.get("facts", []):
            columns.append(
                {
                    "dataset_id": dataset["id"],
                    "dataset_name": dataset["title"],
                    "title": fact["title"],
                    "description": fact.get("description", ""),
                    "id": fact["id"],
                    "tags": str(sort_tags(fact.get("tags", ""))),
                    "data_type": fact["sourceColumnDataType"],
                    "source_column": fact.get("sourceColumn", ""),
                    "type": "fact",
                    "grain": "No",
                    "reference_to_id": "",
                    "reference_to_title": "",
                }
            )

        # Add references
        # Note: id includes target dataset because the same source column can reference
        # multiple target datasets (star schema pattern)
        for ref in dataset.get("references", []):
            target_dataset_id = ref["identifier"]["id"]
            target_dataset_info = dataset_map.get(target_dataset_id)
            source_column = ref["sources"][0]["column"]

            columns.append(
                {
                    "dataset_id": dataset["id"],
                    "dataset_name": dataset["title"],
                    "title": ref["identifier"]["id"],
                    "description": "",
                    "id": f"{source_column}__ref__{target_dataset_id}",
                    "tags": "",
                    "data_type": ref["sources"][0]["dataType"],
                    "source_column": source_column,
                    "type": "reference",
                    "grain": "No",
                    "reference_to_id": target_dataset_id,
                    "reference_to_title": target_dataset_info["title"]
                    if target_dataset_info
                    else "",
                }
            )

        # Add workspace data filter columns
        # These are columns in the underlying table that are not used in the dataset
        # but are available (typically for row-level security/filtering)
        for wdf_col in dataset.get("workspaceDataFilterColumns", []):
            columns.append(
                {
                    "dataset_id": dataset["id"],
                    "dataset_name": dataset["title"],
                    "title": wdf_col.get("name", ""),
                    "description": "",
                    "id": wdf_col.get("name", ""),
                    "tags": "",
                    "data_type": wdf_col.get("dataType", ""),
                    "source_column": wdf_col.get("name", ""),
                    "type": "workspace_data_filter",
                    "grain": "No",
                    "reference_to_id": "",
                    "reference_to_title": "",
                }
            )

    return datasets, columns, labels


def process_users(data):
    """Process users data from usersAndUserGroups API response.

    Args:
        data: Raw API response from /api/v1/layout/usersAndUserGroups

    Returns:
        List of processed user dictionaries
    """
    processed_data = []

    users = data.get("users", [])
    for user in users:
        # Extract user group memberships
        user_groups = user.get("userGroups", [])
        user_group_ids = [ug.get("id", "") for ug in user_groups if ug.get("id")]

        processed_data.append(
            {
                "user_id": user.get("id", ""),
                "firstname": user.get("firstname", ""),
                "lastname": user.get("lastname", ""),
                "email": user.get("email", ""),
                "authentication_id": user.get("authenticationId", ""),
                "user_group_ids": str(user_group_ids) if user_group_ids else "",
                "user_group_count": len(user_group_ids),
                "content": user,
            }
        )

    return processed_data


def process_user_groups(data):
    """Process user groups data from usersAndUserGroups API response.

    Args:
        data: Raw API response from /api/v1/layout/usersAndUserGroups

    Returns:
        List of processed user group dictionaries
    """
    processed_data = []

    user_groups = data.get("userGroups", [])
    for group in user_groups:
        # Extract parent group IDs
        parents = group.get("parents", [])
        parent_ids = [p.get("id", "") for p in parents if p.get("id")]

        processed_data.append(
            {
                "user_group_id": group.get("id", ""),
                "name": group.get("name", ""),
                "parent_ids": str(parent_ids) if parent_ids else "",
                "parent_count": len(parent_ids),
                "content": group,
            }
        )

    return processed_data


def process_user_group_members(data):
    """Process user-to-group membership relationships from usersAndUserGroups API response.

    Args:
        data: Raw API response from /api/v1/layout/usersAndUserGroups

    Returns:
        List of user-group membership dictionaries (junction table)
    """
    relationships = []

    users = data.get("users", [])
    for user in users:
        user_id = user.get("id", "")
        user_groups = user.get("userGroups", [])

        for group in user_groups:
            group_id = group.get("id", "")
            if user_id and group_id:
                relationships.append(
                    {
                        "user_id": user_id,
                        "user_group_id": group_id,
                    }
                )

    return sorted(relationships, key=lambda x: (x["user_id"], x["user_group_id"]))


def process_dashboards_permissions_from_analytics_model(analytics_model, workspace_id):
    """Extract permissions from dashboards in the analytics model.

    Args:
        analytics_model: Raw analytics model data from layout API
        workspace_id: Workspace ID for the permissions

    Returns:
        List of permission dictionaries with dashboard_id, assignee_id, assignee_type, permission_name
    """
    permissions = []

    if not analytics_model:
        logger.warning("Analytics model is None or empty - no permissions to extract")
        return permissions

    # Log the top-level keys to understand the structure
    logger.debug("Analytics model top-level keys: %s", list(analytics_model.keys()))

    # Dashboards are under analytics.analyticalDashboards in the layout API response
    analytics = analytics_model.get("analytics", {})
    dashboards = analytics.get("analyticalDashboards", [])
    logger.debug("Found %d dashboards in analytics model", len(dashboards))

    for dash in dashboards:
        dashboard_id = dash.get("id", "")
        if not dashboard_id:
            continue

        # Permissions are at the top level of each dashboard object in layout API
        dash_permissions = dash.get("permissions", [])

        for perm in dash_permissions:
            permission_name = perm.get("name", "")

            # Handle direct assignee (user or userGroup)
            assignee = perm.get("assignee", {})
            assignee_id = assignee.get("id", "")
            assignee_type = assignee.get("type", "")

            if assignee_id:
                permissions.append(
                    {
                        "dashboard_id": dashboard_id,
                        "workspace_id": workspace_id,
                        "assignee_id": assignee_id,
                        "assignee_type": assignee_type,
                        "permission_name": permission_name,
                    }
                )
            else:
                # Handle assigneeRule (e.g., allWorkspaceUsers)
                assignee_rule = perm.get("assigneeRule", {})
                rule_type = assignee_rule.get("type", "")
                if rule_type:
                    permissions.append(
                        {
                            "dashboard_id": dashboard_id,
                            "workspace_id": workspace_id,
                            "assignee_id": rule_type,  # e.g., "allWorkspaceUsers"
                            "assignee_type": "rule",
                            "permission_name": permission_name,
                        }
                    )

    logger.debug(
        "Found %d dashboard permissions from analytics model", len(permissions)
    )

    return sorted(
        permissions,
        key=lambda x: (x["dashboard_id"], x["assignee_type"], x["assignee_id"]),
    )
