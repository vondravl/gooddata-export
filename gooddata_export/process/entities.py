"""Entity API functions for fetching and processing GoodData entities."""

import logging
import time
from typing import TYPE_CHECKING, Any

import requests

if TYPE_CHECKING:
    from gooddata_export.config import ExportConfig

from gooddata_export.common import (
    get_api_client,
    raise_for_api_error,
    raise_for_connection_error,
    raise_for_request_error,
)
from gooddata_export.process.common import sort_tags

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Entity to Layout Transformation Functions
# -----------------------------------------------------------------------------


def entity_to_layout(obj: dict) -> dict:
    """Transform entity API format to layout format.

    Entity format (from /api/v1/entities/...):
        {"id": "x", "attributes": {"title": "...", ...}, "meta": {"origin": {...}}}

    Layout format (from /api/v1/layout/.../analyticsModel or local layout.json):
        {"id": "x", "title": "...", "content": {...}}

    The layout format is simpler and matches local layout.json files,
    making it the canonical internal format for processing.
    """
    attrs = obj.get("attributes", {})
    meta = obj.get("meta", {})
    origin = meta.get("origin", {})

    return {
        "id": obj["id"],
        "title": attrs.get("title", ""),
        "description": attrs.get("description", ""),
        "tags": attrs.get("tags") or [],
        "content": attrs.get("content", {}),
        "createdAt": attrs.get("createdAt", ""),
        "modifiedAt": attrs.get("modifiedAt", attrs.get("createdAt", "")),
        "areRelationsValid": attrs.get("areRelationsValid", True),
        "isHidden": attrs.get("isHidden", False),
        "originType": origin.get("originType", "NATIVE"),
    }


def transform_entities_to_layout(entities: list[dict]) -> list[dict]:
    """Transform list of entity API objects to layout format.

    Used when fetching from child workspaces via entity API, converting
    to the canonical layout format for uniform processing.
    """
    return [entity_to_layout(e) for e in entities]


def fetch_data(endpoint, client=None, config=None, max_retries=3):
    """Fetch data from GoodData API with pagination and retry mechanism"""
    client = get_api_client(config=config, client=client)

    base_url = f"{client['base_url']}/api/v1/entities/workspaces/{client['workspace_id']}/{endpoint}"
    logger.info("Fetching %s from workspace: %s", endpoint, client["workspace_id"])

    all_data = []
    page = 0

    while True:
        # Add page parameter to params
        params = client["params"].copy()
        params["page"] = str(page)

        url = base_url
        logger.debug("Fetching %s page %d", endpoint, page)

        page_success = False

        for attempt in range(max_retries + 1):
            try:
                # Increase timeout for parallel requests and add backoff
                timeout = 60 + (attempt * 15)  # 60s, 75s, 90s, 105s
                if attempt > 0:
                    # Add exponential backoff delay
                    delay = 2**attempt
                    logger.info(
                        "Retrying %s page %d (attempt %d/%d) after %ds delay",
                        endpoint,
                        page,
                        attempt + 1,
                        max_retries + 1,
                        delay,
                    )
                    time.sleep(delay)

                response = requests.get(
                    url, params=params, headers=client["headers"], timeout=timeout
                )

                if response.status_code == 200:
                    json_input = response.json()
                    page_data = json_input.get("data", [])

                    # If no data returned, we've reached the end
                    if not page_data:
                        page_success = True
                        break

                    all_data.extend(page_data)
                    logger.debug("Page %d: Found %d items", page, len(page_data))
                    page_success = True
                    break  # Success, exit retry loop

                elif response.status_code >= 500:
                    # Server errors (5xx) are retryable
                    if attempt < max_retries:
                        logger.warning(
                            "Server error for %s page %d (HTTP %d, attempt %d)",
                            endpoint,
                            page,
                            response.status_code,
                            attempt + 1,
                        )
                        continue
                    # Terminal - raises, never returns
                    raise_for_api_error(response, endpoint, client["workspace_id"])

                else:
                    # Client errors (4xx) - not retryable
                    # Terminal - raises, never returns
                    raise_for_api_error(response, endpoint, client["workspace_id"])

            except (
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
            ) as e:
                if attempt < max_retries:
                    logger.warning(
                        "Timeout/connection error for %s page %d (attempt %d): %s",
                        endpoint,
                        page,
                        attempt + 1,
                        e,
                    )
                    continue
                else:
                    raise_for_connection_error(
                        endpoint,
                        e,
                        base_url=client.get("base_url"),
                        retry_info=f"after {max_retries + 1} attempts",
                    )

            except requests.exceptions.RequestException as e:
                raise_for_request_error(endpoint, e, base_url=client.get("base_url"))

            except Exception as e:
                logger.error("Unexpected error fetching %s: %s", endpoint, e)
                raise RuntimeError(f"Unexpected error fetching {endpoint}: {e}")

        # If page fetch was not successful, break pagination loop
        if not page_success:
            break

        # If we got data, increment page and continue
        # If we got no data (empty page), we've already broken out above
        if page_data:
            page += 1
        else:
            break

    # Process all accumulated data
    if not all_data:
        logger.warning("%s: No data received (empty array)", endpoint)
        return None

    # Filter to NATIVE origin for all entity endpoints fetched via this function
    filtered_data = []
    for obj in all_data:
        origin_type = obj.get("meta", {}).get("origin", {}).get("originType", "NATIVE")
        if str(origin_type).upper() == "NATIVE":
            filtered_data.append(obj)

    try:
        sorted_data = sorted(
            filtered_data,
            key=lambda obj: obj.get("attributes", {}).get("title", ""),
        )
        # Log with page count only if more than 1 page
        if page > 1:
            logger.info(
                "%s: Successfully fetched %d items across %d pages",
                endpoint,
                len(sorted_data),
                page,
            )
        else:
            logger.info("%s: Successfully fetched %d items", endpoint, len(sorted_data))
        return sorted_data
    except KeyError as sort_error:
        logger.warning("%s: Could not sort data - %s", endpoint, sort_error)
        return filtered_data


def validate_workspace_exists(
    client: dict[str, Any] | None = None,
    config: "ExportConfig | None" = None,
) -> None:
    """Check if workspace exists and is accessible.

    Raises:
        RuntimeError: If workspace is not accessible (auth, permissions, or not found)
                      or if there's a connection error.
    """
    client = get_api_client(config=config, client=client)
    workspace_id = client["workspace_id"]
    url = f"{client['base_url']}/api/v1/entities/workspaces/{workspace_id}"

    logger.info("Validating workspace: %s", workspace_id)
    try:
        response = requests.get(url, headers=client["headers"], timeout=30)
        if response.status_code == 200:
            return
        raise_for_api_error(response, f"workspace {workspace_id}", workspace_id)
    except requests.exceptions.RequestException as e:
        raise_for_request_error(
            f"workspace {workspace_id}", e, base_url=client.get("base_url")
        )


def fetch_child_workspaces(client=None, config=None, size=2000):
    """Fetch child workspaces from GoodData API with pagination support"""
    try:
        client = get_api_client(config=config, client=client)
        parent_workspace_id = client["workspace_id"]
        all_workspaces = []
        page = 0

        while True:
            url = f"{client['base_url']}/api/v1/entities/workspaces"
            params = {
                "filter": f"parent.id=={parent_workspace_id}",
                "include": "workspaces",
                "size": str(size),
                "page": str(page),
            }

            logger.debug(
                "Fetching child workspaces for parent: %s (page %d)",
                parent_workspace_id,
                page,
            )
            logger.debug("Request URL: %s", url)
            logger.debug("Request params: %s", params)

            response = requests.get(
                url, params=params, headers=client["headers"], timeout=30
            )

            logger.debug(
                "Child workspaces API response status: %d", response.status_code
            )

            if response.status_code == 200:
                json_input = response.json()
                data = json_input.get("data", [])

                # If no data returned, we've reached the end
                if not data:
                    break

                all_workspaces.extend(data)
                page += 1

                logger.debug("Page %d: Found %d child workspaces", page - 1, len(data))

            else:
                raise_for_api_error(response, "child workspaces", parent_workspace_id)

        if all_workspaces:
            # Log with page count only if more than 1 page
            if page > 1:
                logger.info(
                    "Found %d total child workspaces across %d pages",
                    len(all_workspaces),
                    page,
                )
            else:
                logger.info("Found %d total child workspaces", len(all_workspaces))
            for child in all_workspaces:
                logger.debug(
                    "Child workspace: %s (%s)",
                    child["attributes"]["name"],
                    child["id"],
                )
            return all_workspaces
        else:
            logger.debug("No child workspaces found - empty data array")
            return []

    except requests.exceptions.RequestException as e:
        raise_for_request_error(
            "child workspaces", e, base_url=client.get("base_url") if client else None
        )

    except Exception as e:
        logger.error("Unexpected error fetching child workspaces: %s", e)
        raise RuntimeError(f"Unexpected error fetching child workspaces: {e}")


def process_metrics(data, workspace_id=None):
    """Process metrics data (layout format) into uniform format.

    Accepts layout format where fields are at top level:
        {"id": "x", "title": "...", "content": {"maql": "...", "format": "..."}}
    """
    processed_data = []
    for obj in data:
        content = obj.get("content", {})
        processed_data.append(
            {
                "metric_id": obj["id"],
                "title": obj.get("title", ""),
                "description": obj.get("description", ""),
                "tags": str(sort_tags(obj.get("tags") or [])),
                "maql": content.get("maql", ""),
                "format": content.get("format", ""),
                "created_at": obj.get("createdAt", ""),
                "modified_at": obj.get("modifiedAt", obj.get("createdAt", "")),
                "is_valid": obj.get("areRelationsValid", True),
                "is_hidden": obj.get("isHidden", False),
                "workspace_id": workspace_id,
                "origin_type": obj.get("originType", "NATIVE"),
                "content": obj,  # Store the original JSON object
            }
        )
    return processed_data


def process_visualizations(data, base_url, workspace_id):
    """Process visualization data (layout format) into uniform format.

    Accepts layout format where fields are at top level:
        {"id": "x", "title": "...", "content": {"visualizationUrl": "..."}}
    """
    processed_data = []
    for obj in data:
        url_link = f"{base_url}/analyze/#/{workspace_id}/{obj['id']}/edit"
        content = obj.get("content", {})

        processed_data.append(
            {
                "visualization_id": obj["id"],
                "title": obj.get("title", ""),
                "description": obj.get("description", ""),
                "tags": str(sort_tags(obj.get("tags") or [])),
                "visualization_url": content.get("visualizationUrl", ""),
                "created_at": obj.get("createdAt", ""),
                "modified_at": obj.get("modifiedAt", obj.get("createdAt", "")),
                "url_link": url_link,
                "workspace_id": workspace_id,
                "origin_type": obj.get("originType", "NATIVE"),
                "content": obj,
                "is_valid": obj.get("areRelationsValid", True),
                "is_hidden": obj.get("isHidden", False),
            }
        )
    return processed_data


def process_visualizations_metrics(visualization_data, workspace_id=None):
    """Extract unique metric IDs used in each visualization with their labels.

    Accepts layout format where content is at top level:
        {"id": "x", "content": {"buckets": [...]}}
    """
    # Using dict to store unique combinations with labels
    # Key: (viz_id, metric_id, workspace_id), Value: label
    unique_relationships = {}

    for viz in visualization_data:
        content = viz.get("content", {})

        if "buckets" not in content:
            continue

        for bucket in content["buckets"]:
            if "items" not in bucket:
                continue

            for item in bucket["items"]:
                measure = item.get("measure", {})
                measure_def = measure.get("definition", {}).get("measureDefinition", {})
                metric_id = measure_def.get("item", {}).get("identifier", {}).get("id")

                if metric_id:
                    # Get label: prefer alias, fall back to title, then None
                    label = measure.get("alias") or measure.get("title")
                    key = (viz["id"], metric_id, workspace_id)
                    # Only store if not already present (keep first occurrence)
                    if key not in unique_relationships:
                        unique_relationships[key] = label

    # Convert dict to list of dictionaries
    result = [
        {
            "visualization_id": viz_id,
            "metric_id": metric_id,
            "workspace_id": ws_id,
            "label": label,
        }
        for (viz_id, metric_id, ws_id), label in sorted(unique_relationships.items())
    ]

    return result


def process_visualizations_attributes(visualization_data, workspace_id=None):
    """Extract unique attribute IDs (display forms) used in each visualization with their labels.

    Accepts layout format where content is at top level:
        {"id": "x", "content": {"buckets": [...]}}
    """
    # Using dict to store unique combinations with labels
    # Key: (viz_id, attribute_id, workspace_id), Value: label
    unique_relationships = {}

    for viz in visualization_data:
        content = viz.get("content", {})

        if "buckets" not in content:
            continue

        for bucket in content["buckets"]:
            if "items" not in bucket:
                continue

            for item in bucket["items"]:
                # Attributes are stored with displayForm reference
                attribute_def = item.get("attribute", {})
                display_form = attribute_def.get("displayForm", {})
                attribute_id = display_form.get("identifier", {}).get("id")

                if attribute_id:
                    # Get label: prefer alias, fall back to None
                    label = attribute_def.get("alias")
                    key = (viz["id"], attribute_id, workspace_id)
                    # Only store if not already present (keep first occurrence)
                    if key not in unique_relationships:
                        unique_relationships[key] = label

    # Convert dict to list of dictionaries
    result = [
        {
            "visualization_id": viz_id,
            "attribute_id": attr_id,
            "workspace_id": ws_id,
            "label": label,
        }
        for (viz_id, attr_id, ws_id), label in sorted(unique_relationships.items())
    ]

    return result


def process_dashboards(data, base_url, workspace_id):
    """Process dashboard data (layout format) into uniform format.

    Accepts layout format where fields are at top level:
        {"id": "x", "title": "...", "content": {"layout": {...}}}
    """
    processed_data = []
    for obj in data:
        dashboard_url = (
            f"{base_url}/dashboards/#/workspace/{workspace_id}/dashboard/{obj['id']}"
        )
        content = obj.get("content", {})

        processed_data.append(
            {
                "dashboard_id": obj["id"],
                "title": obj.get("title", ""),
                "description": obj.get("description", ""),
                "tags": str(sort_tags(obj.get("tags") or [])),
                "created_at": obj.get("createdAt", ""),
                "modified_at": obj.get("modifiedAt", obj.get("createdAt", "")),
                "dashboard_url": dashboard_url,
                "workspace_id": workspace_id,
                "origin_type": obj.get("originType", "NATIVE"),
                "content": content,
                "is_valid": obj.get("areRelationsValid", True),
                "is_hidden": obj.get("isHidden", False),
                "filter_context_id": content.get("filterContextRef", {})
                .get("identifier", {})
                .get("id"),
            }
        )
    return processed_data


def process_dashboards_visualizations(
    dashboard_data, workspace_id=None, known_insights=None, config=None
):
    """Extract unique visualization IDs used in each dashboard, including rich text references.

    Supports both tabbed dashboards (content.tabs[]) and legacy non-tabbed dashboards
    (content.layout.sections). For tabbed dashboards, each relationship includes the
    tab_id (localIdentifier). For legacy dashboards, tab_id is None.
    """
    # Import here to avoid circular imports
    from gooddata_export.process.common import UniqueRelationshipTracker
    from gooddata_export.process.dashboard_traversal import iterate_dashboard_widgets
    from gooddata_export.process.rich_text import process_rich_text_insights

    # Track unique relationships: (dashboard_id, viz_id, tab_id, from_rich_text)
    tracker = UniqueRelationshipTracker(
        key_fields=["dashboard_id", "visualization_id", "tab_id", "from_rich_text"]
    )

    # First get a list of all known insights for better matching (only if rich text enabled)
    # Require known_insights to be provided by the caller; do not refetch here to avoid duplication
    enable_rich_text = config.ENABLE_RICH_TEXT_EXTRACTION if config else False
    if enable_rich_text:
        if known_insights is None:
            known_insights = set()
        elif not isinstance(known_insights, set):
            known_insights = set(known_insights)
        logger.info("Found %d known insights for validation", len(known_insights))

    def add_relationship(
        dashboard_id,
        viz_id,
        tab_id,
        from_rich_text,
        widget_title=None,
        widget_description=None,
        widget_local_identifier=None,
        widget_type=None,
        switcher_local_identifier=None,
    ):
        """Add a relationship if not already present."""
        tracker.add(
            {
                "dashboard_id": dashboard_id,
                "visualization_id": viz_id,
                "tab_id": tab_id,
                "from_rich_text": from_rich_text,
                "widget_title": widget_title,
                "widget_description": widget_description,
                "widget_local_identifier": widget_local_identifier,
                "widget_type": widget_type,
                "switcher_local_identifier": switcher_local_identifier,
                "workspace_id": workspace_id,
            }
        )

    # Use shared traversal utility
    for dashboard_id, tab_id, widget in iterate_dashboard_widgets(dashboard_data):
        # Extract widget's local identifier
        widget_local_id = widget.get("localIdentifier")

        # Single visualization widgets
        viz_id = widget.get("insight", {}).get("identifier", {}).get("id")
        if viz_id:
            widget_title = widget.get("title")
            widget_description = (
                widget.get("description") or None
            )  # Convert empty string to None
            add_relationship(
                dashboard_id,
                viz_id,
                tab_id,
                0,
                widget_title,
                widget_description,
                widget_local_id,
                "insight",
            )

        # Multiple visualization widgets (visualizationSwitcher)
        for viz in widget.get("visualizations", []):
            viz_id = viz.get("insight", {}).get("identifier", {}).get("id")
            if viz_id:
                widget_title = viz.get("title")
                widget_description = viz.get("description") or None
                # Inner viz has its own localIdentifier, parent switcher ID for grouping
                inner_viz_local_id = viz.get("localIdentifier")
                add_relationship(
                    dashboard_id,
                    viz_id,
                    tab_id,
                    0,
                    widget_title,
                    widget_description,
                    inner_viz_local_id,
                    "visualizationSwitcher",
                    widget_local_id,  # Parent switcher's ID for grouping
                )

        # Rich text extraction (single feature-flag gate)
        if enable_rich_text:
            # Rich text widgets
            if widget.get("type") == "richText":
                rich_text_content = widget.get("content", "")
                for insight in process_rich_text_insights(
                    rich_text_content, dashboard_id, known_insights
                ):
                    add_relationship(
                        dashboard_id,
                        insight["visualization_id"],
                        tab_id,
                        1,
                        widget_local_identifier=widget_local_id,
                        widget_type="richText",
                    )

            # Other widget content that might contain insight references
            widget_content = widget.get("content")
            if isinstance(widget_content, str) and any(
                pattern in widget_content
                for pattern in [
                    "insightFirstAttribute",
                    "insightFirstMeasure",
                    "insightFirstMeasureChange",
                    "comparisonFromInsightMeasure",
                    "insightFirstTotal",
                ]
            ):
                for insight in process_rich_text_insights(
                    widget_content, dashboard_id, known_insights
                ):
                    add_relationship(
                        dashboard_id,
                        insight["visualization_id"],
                        tab_id,
                        1,
                        widget_local_identifier=widget_local_id,
                        widget_type="richText",
                    )

    return tracker.get_sorted(
        sort_key=lambda x: (
            x["dashboard_id"],
            x["visualization_id"],
            x["from_rich_text"],
        )
    )


def process_filter_contexts(data, workspace_id=None):
    """Process filter context data (layout format) into uniform format.

    Accepts layout format where fields are at top level:
        {"id": "x", "title": "...", "content": {"filters": [...]}}
    """
    processed_data = []
    for obj in data:
        processed_data.append(
            {
                "filter_context_id": obj["id"],
                "workspace_id": workspace_id,
                "title": obj.get("title", ""),
                "description": obj.get("description", ""),
                "origin_type": obj.get("originType", "NATIVE"),
                "content": obj.get("content", {}),
            }
        )
    return processed_data


def process_filter_context_fields(data, workspace_id=None):
    """Process filter context data (layout format) to extract individual filter fields.

    Accepts layout format where content is at top level:
        {"id": "x", "content": {"filters": [...]}}
    """
    processed_fields = []

    for obj in data:
        filter_context_id = obj["id"]
        content = obj.get("content", {})
        filters = content.get("filters", [])

        for filter_index, filter_obj in enumerate(filters):
            # Determine filter type
            if "dateFilter" in filter_obj:
                date_filter = filter_obj["dateFilter"]
                processed_fields.append(
                    {
                        "filter_context_id": filter_context_id,
                        "workspace_id": workspace_id,
                        "filter_index": filter_index,
                        "filter_type": "dateFilter",
                        "local_identifier": date_filter.get("localIdentifier", ""),
                        "display_form_id": None,
                        "title": None,
                        "negative_selection": None,
                        "selection_mode": None,
                        "date_granularity": date_filter.get("granularity", ""),
                        "date_from": date_filter.get("from"),
                        "date_to": date_filter.get("to"),
                        "date_type": date_filter.get("type", ""),
                        "attribute_elements_count": None,
                    }
                )

            elif "attributeFilter" in filter_obj:
                attr_filter = filter_obj["attributeFilter"]
                display_form_id = (
                    attr_filter.get("displayForm", {})
                    .get("identifier", {})
                    .get("id", "")
                )

                # Count attribute elements
                attribute_elements = attr_filter.get("attributeElements", {}).get(
                    "uris", []
                )
                elements_count = len(attribute_elements) if attribute_elements else 0

                processed_fields.append(
                    {
                        "filter_context_id": filter_context_id,
                        "workspace_id": workspace_id,
                        "filter_index": filter_index,
                        "filter_type": "attributeFilter",
                        "local_identifier": attr_filter.get("localIdentifier", ""),
                        "display_form_id": display_form_id,
                        "title": attr_filter.get("title", ""),
                        "negative_selection": attr_filter.get("negativeSelection"),
                        "selection_mode": attr_filter.get("selectionMode", ""),
                        "date_granularity": None,
                        "date_from": None,
                        "date_to": None,
                        "date_type": None,
                        "attribute_elements_count": elements_count,
                    }
                )

    return processed_fields


def process_workspaces(
    parent_workspace_id, parent_workspace_name, child_workspaces_data
):
    """Process workspace data into uniform format"""
    processed_data = []

    # Add parent workspace
    processed_data.append(
        {
            "workspace_id": parent_workspace_id,
            "workspace_name": parent_workspace_name,
            "is_parent": True,
            "parent_workspace_id": None,
            "created_at": "",
            "modified_at": "",
        }
    )

    # Add child workspaces
    for workspace in child_workspaces_data:
        processed_data.append(
            {
                "workspace_id": workspace["id"],
                "workspace_name": workspace["attributes"]["name"],
                "is_parent": False,
                "parent_workspace_id": parent_workspace_id,
                "created_at": workspace["attributes"].get("createdAt", ""),
                "modified_at": workspace["attributes"].get(
                    "modifiedAt", workspace["attributes"].get("createdAt", "")
                ),
            }
        )

    return processed_data


def process_plugins(data: list[dict], workspace_id: str | None = None) -> list[dict]:
    """Process dashboard plugin data (layout format) into uniform format.

    Accepts layout format where fields are at top level:
        {"id": "x", "title": "...", "content": {"url": "...", "version": "..."}}
    """
    processed_data = []
    for obj in data:
        content = obj.get("content", {})

        processed_data.append(
            {
                "plugin_id": obj["id"],
                "title": obj.get("title", ""),
                "description": obj.get("description", ""),
                "url": content.get("url", ""),
                "version": content.get("version", ""),
                "created_at": obj.get("createdAt", ""),
                "workspace_id": workspace_id,
                "origin_type": obj.get("originType", "NATIVE"),
                "content": obj,
            }
        )
    return processed_data


def process_dashboards_plugins(
    dashboard_data: list[dict], workspace_id: str | None = None
) -> list[dict]:
    """Extract plugin IDs used in each dashboard (layout format).

    Accepts layout format where content is at top level:
        {"id": "x", "content": {"plugins": [...]}}
    """
    from gooddata_export.process.common import UniqueRelationshipTracker

    tracker = UniqueRelationshipTracker(
        key_fields=["dashboard_id", "plugin_id", "workspace_id"]
    )

    for dash in dashboard_data:
        content = dash.get("content", {})
        plugins = content.get("plugins", [])

        for plugin_entry in plugins:
            plugin_id = plugin_entry.get("plugin", {}).get("identifier", {}).get("id")

            if plugin_id:
                tracker.add(
                    {
                        "dashboard_id": dash["id"],
                        "plugin_id": plugin_id,
                        "workspace_id": workspace_id,
                    }
                )

    return tracker.get_sorted(sort_key=lambda x: (x["dashboard_id"], x["plugin_id"]))


def process_dashboards_widget_filters(
    dashboard_data: list[dict], workspace_id: str | None = None
) -> list[dict]:
    """Extract widget-level filter configuration from dashboards.

    Extracts:
    - ignoreDashboardFilters: Which dashboard filters each widget ignores
      - attributeFilterReference: displayForm reference (type: label)
      - dateFilterReference: dataSet reference (type: dataset)
    - dateDataSet: Date dataset override for each widget (type: dataset)

    Handles both single insight widgets and visualizations within visualizationSwitcher widgets.

    Accepts layout format where content is at top level:
        {"id": "x", "content": {"tabs": [...] or "layout": {...}}}
    """
    from gooddata_export.process.common import UniqueRelationshipTracker
    from gooddata_export.process.dashboard_traversal import iterate_dashboard_widgets

    tracker = UniqueRelationshipTracker(
        key_fields=[
            "dashboard_id",
            "widget_local_identifier",
            "filter_type",
            "reference_id",
            "workspace_id",
        ]
    )

    def add_filter_record(
        dashboard_id: str,
        tab_id: str | None,
        widget_local_identifier: str,
        visualization_id: str | None,
        filter_type: str,
        reference_type: str,
        reference_id: str,
        reference_object_type: str,
    ) -> None:
        """Add a filter record to the tracker."""
        tracker.add(
            {
                "dashboard_id": dashboard_id,
                "visualization_id": visualization_id,
                "tab_id": tab_id,
                "widget_local_identifier": widget_local_identifier,
                "filter_type": filter_type,
                "reference_type": reference_type,
                "reference_id": reference_id,
                "reference_object_type": reference_object_type,
                "workspace_id": workspace_id,
            }
        )

    def extract_widget_filters(
        dashboard_id: str,
        tab_id: str | None,
        widget_local_identifier: str,
        visualization_id: str | None,
        widget: dict,
    ) -> None:
        """Extract filter configuration from a single widget."""
        # Extract ignoreDashboardFilters
        # Format: {"type": "attributeFilterReference", "displayForm": {"identifier": {"id": "..."}}}
        #      or {"type": "dateFilterReference", "dataSet": {"identifier": {"id": "..."}}}
        ignore_filters = widget.get("ignoreDashboardFilters", [])
        for filter_ref in ignore_filters:
            ref_type = filter_ref.get("type")

            if ref_type == "attributeFilterReference":
                # attributeFilterReference: displayForm.identifier.id
                display_form_id = (
                    filter_ref.get("displayForm", {}).get("identifier", {}).get("id")
                )
                if display_form_id:
                    add_filter_record(
                        dashboard_id=dashboard_id,
                        tab_id=tab_id,
                        widget_local_identifier=widget_local_identifier,
                        visualization_id=visualization_id,
                        filter_type="ignoreDashboardFilters",
                        reference_type="attributeFilterReference",
                        reference_id=display_form_id,
                        reference_object_type="label",
                    )

            elif ref_type == "dateFilterReference":
                # dateFilterReference: dataSet.identifier.id
                data_set_id = (
                    filter_ref.get("dataSet", {}).get("identifier", {}).get("id")
                )
                if data_set_id:
                    add_filter_record(
                        dashboard_id=dashboard_id,
                        tab_id=tab_id,
                        widget_local_identifier=widget_local_identifier,
                        visualization_id=visualization_id,
                        filter_type="ignoreDashboardFilters",
                        reference_type="dateFilterReference",
                        reference_id=data_set_id,
                        reference_object_type="dataset",
                    )

        # Extract dateDataSet override
        date_data_set = widget.get("dateDataSet", {})
        date_data_set_id = date_data_set.get("identifier", {}).get("id")
        if date_data_set_id:
            add_filter_record(
                dashboard_id=dashboard_id,
                tab_id=tab_id,
                widget_local_identifier=widget_local_identifier,
                visualization_id=visualization_id,
                filter_type="dateDataSet",
                reference_type="dataset",
                reference_id=date_data_set_id,
                reference_object_type="dataset",
            )

    # Use shared traversal utility
    for dashboard_id, tab_id, widget in iterate_dashboard_widgets(dashboard_data):
        widget_local_id = widget.get("localIdentifier")
        if not widget_local_id:
            continue

        # Single insight widgets
        viz_id = widget.get("insight", {}).get("identifier", {}).get("id")
        if viz_id or widget.get("ignoreDashboardFilters") or widget.get("dateDataSet"):
            extract_widget_filters(
                dashboard_id=dashboard_id,
                tab_id=tab_id,
                widget_local_identifier=widget_local_id,
                visualization_id=viz_id,
                widget=widget,
            )

        # visualizationSwitcher widgets - process each inner visualization
        for viz in widget.get("visualizations", []):
            inner_viz_id = viz.get("insight", {}).get("identifier", {}).get("id")
            inner_local_id = viz.get("localIdentifier")
            if inner_local_id:
                composite_local_id = f"{widget_local_id}:{inner_local_id}"
                extract_widget_filters(
                    dashboard_id=dashboard_id,
                    tab_id=tab_id,
                    widget_local_identifier=composite_local_id,
                    visualization_id=inner_viz_id,
                    widget=viz,
                )

    return tracker.get_sorted(
        sort_key=lambda x: (
            x["dashboard_id"],
            x["widget_local_identifier"],
            x["filter_type"],
            x["reference_id"],
        )
    )
