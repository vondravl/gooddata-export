"""Entity API functions for fetching and processing GoodData entities."""

import logging
import time

import requests

from gooddata_export.common import get_api_client
from gooddata_export.process.common import sort_tags

logger = logging.getLogger(__name__)


def fetch_data(endpoint, client=None, config=None, max_retries=3):
    """Fetch data from GoodData API with pagination and retry mechanism"""
    if client is None:
        if config is None:
            raise ValueError("Either client or config must be provided")
        client = get_api_client(config)
        logger.debug("Created new API client")

    base_url = f"{client['base_url']}/api/v1/entities/workspaces/{client['workspace_id']}/{endpoint}"
    logger.info(f"Fetching {endpoint} from workspace: {client['workspace_id']}")

    all_data = []
    page = 0

    while True:
        # Add page parameter to params
        params = client["params"].copy()
        params["page"] = str(page)

        url = base_url
        logger.debug(f"Fetching {endpoint} page {page}")

        page_success = False

        for attempt in range(max_retries + 1):
            try:
                # Increase timeout for parallel requests and add backoff
                timeout = 60 + (attempt * 15)  # 60s, 75s, 90s, 105s
                if attempt > 0:
                    # Add exponential backoff delay
                    delay = 2**attempt
                    logger.info(
                        f"Retrying {endpoint} page {page} (attempt {attempt + 1}/{max_retries + 1}) after {delay}s delay"
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
                    logger.debug(f"Page {page}: Found {len(page_data)} items")
                    page_success = True
                    break  # Success, exit retry loop

                elif response.status_code == 404:
                    error_msg = (
                        f"Failed to fetch {endpoint} (404)\n"
                        f"Workspace: {client['workspace_id']}\n"
                        f"Please verify workspace ID in .env file\n"
                        f"Response: {response.text}"
                    )
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)

                elif response.status_code == 401:
                    error_msg = (
                        f"Authentication failed for {endpoint}\n"
                        f"Please check API token in .env file\n"
                        f"Response: {response.text}"
                    )
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)

                else:
                    error_msg = (
                        f"Failed to fetch {endpoint} (HTTP {response.status_code})\n"
                        f"Response: {response.text[:200]}"
                    )
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)

            except (
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
            ) as e:
                if attempt < max_retries:
                    logger.warning(
                        f"Timeout/connection error for {endpoint} page {page} (attempt {attempt + 1}): {str(e)}"
                    )
                    continue
                else:
                    base_url_str = client.get("base_url") if client else "unknown"
                    error_msg = (
                        f"Connection error fetching {endpoint} after {max_retries + 1} attempts\n"
                        f"Please verify HOST in .env file: {base_url_str}\n"
                        f"Last error: {str(e)}"
                    )
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)

            except requests.exceptions.RequestException as e:
                error_msg = f"Request failed for {endpoint}: {str(e)}"
                logger.error(error_msg)
                raise RuntimeError(error_msg)

            except Exception as e:
                error_msg = f"Unexpected error fetching {endpoint}: {str(e)}"
                logger.error(error_msg)
                raise RuntimeError(error_msg)

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
        logger.warning(f"{endpoint}: No data received (empty array)")
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
                f"{endpoint}: Successfully fetched {len(sorted_data)} items across {page} pages"
            )
        else:
            logger.info(f"{endpoint}: Successfully fetched {len(sorted_data)} items")
        return sorted_data
    except KeyError as sort_error:
        logger.warning(f"{endpoint}: Could not sort data - {sort_error}")
        return filtered_data


def fetch_child_workspaces(client=None, config=None, size=2000):
    """Fetch child workspaces from GoodData API with pagination support"""
    try:
        if client is None:
            if config is None:
                raise ValueError("Either client or config must be provided")
            client = get_api_client(config)
            logger.debug("Created new API client")

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
                f"Fetching child workspaces for parent: {parent_workspace_id} (page {page})"
            )
            logger.debug(f"Request URL: {url}")
            logger.debug(f"Request params: {params}")

            response = requests.get(
                url, params=params, headers=client["headers"], timeout=30
            )

            logger.debug(
                f"Child workspaces API response status: {response.status_code}"
            )

            if response.status_code == 200:
                json_input = response.json()
                data = json_input.get("data", [])

                # If no data returned, we've reached the end
                if not data:
                    break

                all_workspaces.extend(data)
                page += 1

                logger.debug(f"Page {page - 1}: Found {len(data)} child workspaces")

            elif response.status_code == 404:
                error_msg = (
                    "Failed to fetch child workspaces (404)\n"
                    f"Parent workspace: {parent_workspace_id}\n"
                    "Please verify workspace ID in .env file\n"
                    f"Response: {response.text}"
                )
                logger.error(error_msg)
                raise RuntimeError(error_msg)

            elif response.status_code == 401:
                error_msg = (
                    "Authentication failed for child workspaces\n"
                    "Please check API token in .env file\n"
                    f"Response: {response.text}"
                )
                logger.error(error_msg)
                raise RuntimeError(error_msg)

            else:
                error_msg = (
                    f"Failed to fetch child workspaces (HTTP {response.status_code})\n"
                    f"Response: {response.text[:200]}"
                )
                logger.error(error_msg)
                raise RuntimeError(error_msg)

        if all_workspaces:
            # Log with page count only if more than 1 page
            if page > 1:
                logger.info(
                    f"Found {len(all_workspaces)} total child workspaces across {page} pages"
                )
            else:
                logger.info(f"Found {len(all_workspaces)} total child workspaces")
            for child in all_workspaces:
                logger.debug(
                    f"Child workspace: {child['attributes']['name']} ({child['id']})"
                )
            return all_workspaces
        else:
            logger.debug("No child workspaces found - empty data array")
            return []

    except requests.exceptions.ConnectionError as e:
        base_url = client.get("base_url") if client else "unknown"
        error_msg = (
            "Connection error fetching child workspaces\n"
            f"Please verify HOST in .env file: {base_url}\n"
            f"Error: {str(e)}"
        )
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    except requests.exceptions.RequestException as e:
        error_msg = f"Request failed for child workspaces: {str(e)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    except Exception as e:
        error_msg = f"Unexpected error fetching child workspaces: {str(e)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)


def process_metrics(data, workspace_id=None):
    """Process metrics data into uniform format"""
    processed_data = []
    for obj in data:
        # Determine origin type - check if meta.origin exists and get originType
        origin_type = "NATIVE"  # default
        if "meta" in obj and "origin" in obj["meta"]:
            origin_type = obj["meta"]["origin"].get("originType", "NATIVE")

        processed_data.append(
            {
                "metric_id": obj["id"],
                "title": obj["attributes"]["title"],
                "description": obj["attributes"]["description"],
                "tags": str(sort_tags(obj["attributes"].get("tags", ""))),
                "maql": obj["attributes"]["content"]["maql"],
                "format": obj["attributes"]["content"]["format"],
                "created_at": obj["attributes"]["createdAt"],
                "modified_at": obj["attributes"].get(
                    "modifiedAt", obj["attributes"]["createdAt"]
                ),
                "is_valid": obj["attributes"]["areRelationsValid"],
                "is_hidden": obj["attributes"].get("isHidden", False),
                "workspace_id": workspace_id,
                "origin_type": origin_type,
                "content": obj,  # Store the original JSON object
            }
        )
    return processed_data


def process_visualizations(data, base_url, workspace_id):
    """Process visualization data into uniform format"""
    processed_data = []
    for obj in data:
        url_link = f"{base_url}/analyze/#/{workspace_id}/{obj['id']}/edit"

        # Determine origin type - check if meta.origin exists and get originType
        origin_type = "NATIVE"  # default
        if "meta" in obj and "origin" in obj["meta"]:
            origin_type = obj["meta"]["origin"].get("originType", "NATIVE")

        processed_data.append(
            {
                "visualization_id": obj["id"],
                "title": obj["attributes"]["title"],
                "description": obj["attributes"]["description"],
                "tags": str(sort_tags(obj["attributes"].get("tags", ""))),
                "visualization_url": obj["attributes"]["content"]["visualizationUrl"],
                "created_at": obj["attributes"]["createdAt"],
                "modified_at": obj["attributes"].get(
                    "modifiedAt", obj["attributes"]["createdAt"]
                ),
                "url_link": url_link,
                "workspace_id": workspace_id,
                "origin_type": origin_type,
                "content": obj,
                "is_valid": obj["attributes"]["areRelationsValid"],
                "is_hidden": obj["attributes"].get("isHidden", False),
            }
        )
    return processed_data


def process_visualization_metrics(visualization_data, workspace_id=None):
    """Extract unique metric IDs used in each visualization with their labels"""
    # Using dict to store unique combinations with labels
    # Key: (viz_id, metric_id, workspace_id), Value: label
    unique_relationships = {}

    for viz in visualization_data:
        content = viz["attributes"]["content"]

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


def process_visualization_attributes(visualization_data, workspace_id=None):
    """Extract unique attribute IDs (display forms) used in each visualization with their labels"""
    # Using dict to store unique combinations with labels
    # Key: (viz_id, attribute_id, workspace_id), Value: label
    unique_relationships = {}

    for viz in visualization_data:
        content = viz["attributes"]["content"]

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
    """Process dashboard data into uniform format"""
    processed_data = []
    for obj in data:
        dashboard_url = (
            f"{base_url}/dashboards/#/workspace/{workspace_id}/dashboard/{obj['id']}"
        )

        # Determine origin type - check if meta.origin exists and get originType
        origin_type = "NATIVE"  # default
        if "meta" in obj and "origin" in obj["meta"]:
            origin_type = obj["meta"]["origin"].get("originType", "NATIVE")

        processed_data.append(
            {
                "dashboard_id": obj["id"],
                "title": obj["attributes"]["title"],
                "description": obj["attributes"]["description"],
                "tags": str(sort_tags(obj["attributes"].get("tags", ""))),
                "created_at": obj["attributes"]["createdAt"],
                "modified_at": obj["attributes"].get(
                    "modifiedAt", obj["attributes"]["createdAt"]
                ),
                "dashboard_url": dashboard_url,
                "workspace_id": workspace_id,
                "origin_type": origin_type,
                "content": obj["attributes"]["content"],
                "is_valid": obj["attributes"]["areRelationsValid"],
                "is_hidden": obj["attributes"].get("isHidden", False),
                "filter_context_id": obj["attributes"]
                .get("content", {})
                .get("filterContextRef", {})
                .get("identifier", {})
                .get("id"),
            }
        )
    return processed_data


def process_dashboards_visualizations(
    dashboard_data, workspace_id=None, known_insights=None, config=None
):
    """Extract unique visualization IDs used in each dashboard, including rich text references"""
    # Import here to avoid circular imports
    from gooddata_export.process.rich_text import process_rich_text_insights

    # Using list to store all relationships with their source info
    relationships = []

    # Track what's been added - avoid duplicates with same from_rich_text flag
    added_relationships = set()

    # First get a list of all known insights for better matching (only if rich text enabled)
    # Require known_insights to be provided by the caller; do not refetch here to avoid duplication
    enable_rich_text = config.ENABLE_RICH_TEXT_EXTRACTION if config else False
    if enable_rich_text:
        if known_insights is None:
            known_insights = set()
        elif not isinstance(known_insights, set):
            known_insights = set(known_insights)
        logger.info(f"Found {len(known_insights)} known insights for validation")

    def process_items(items, dashboard_id):
        """Recursively process dashboard items to extract visualizations"""
        for item in items:
            widget = item.get("widget", {})

            # Single visualization widgets
            viz_id = widget.get("insight", {}).get("identifier", {}).get("id")
            if viz_id:
                key = (dashboard_id, viz_id, 0)  # 0 = regular reference
                if key not in added_relationships:
                    relationships.append(
                        {
                            "dashboard_id": dashboard_id,
                            "visualization_id": viz_id,
                            "from_rich_text": 0,
                            "workspace_id": workspace_id,
                        }
                    )
                    added_relationships.add(key)

            # Multiple visualization widgets (visualizationSwitcher)
            visualizations = widget.get("visualizations", [])
            for viz in visualizations:
                viz_id = viz.get("insight", {}).get("identifier", {}).get("id")
                if viz_id:
                    key = (dashboard_id, viz_id, 0)  # 0 = regular reference
                    if key not in added_relationships:
                        relationships.append(
                            {
                                "dashboard_id": dashboard_id,
                                "visualization_id": viz_id,
                                "from_rich_text": 0,
                                "workspace_id": workspace_id,
                            }
                        )
                        added_relationships.add(key)

            # Nested IDashboardLayout widgets - recurse into their sections
            if widget.get("type") == "IDashboardLayout":
                nested_sections = widget.get("sections", [])
                for nested_section in nested_sections:
                    nested_items = nested_section.get("items", [])
                    if nested_items:
                        process_items(nested_items, dashboard_id)

            # Rich text extraction (single feature-flag gate)
            if enable_rich_text:
                # Rich text widgets
                if widget.get("type") == "richText":
                    rich_text_content = widget.get("content", "")
                    rich_text_insights = process_rich_text_insights(
                        rich_text_content, dashboard_id, known_insights
                    )
                    for insight in rich_text_insights:
                        key = (dashboard_id, insight["visualization_id"], 1)
                        if key not in added_relationships:
                            relationships.append(
                                {
                                    "dashboard_id": dashboard_id,
                                    "visualization_id": insight["visualization_id"],
                                    "from_rich_text": 1,
                                    "workspace_id": workspace_id,
                                }
                            )
                            added_relationships.add(key)

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
                    additional_insights = process_rich_text_insights(
                        widget_content, dashboard_id, known_insights
                    )
                    for insight in additional_insights:
                        key = (dashboard_id, insight["visualization_id"], 1)
                        if key not in added_relationships:
                            relationships.append(
                                {
                                    "dashboard_id": dashboard_id,
                                    "visualization_id": insight["visualization_id"],
                                    "from_rich_text": 1,
                                    "workspace_id": workspace_id,
                                }
                            )
                            added_relationships.add(key)

    for dash in dashboard_data:
        content = dash["attributes"]["content"]
        if "layout" not in content or "sections" not in content["layout"]:
            continue

        for section in content["layout"]["sections"]:
            items = section.get("items", [])
            if items:
                process_items(items, dash["id"])

    return sorted(
        relationships,
        key=lambda x: (x["dashboard_id"], x["visualization_id"], x["from_rich_text"]),
    )


def process_filter_contexts(data, workspace_id=None):
    """Process filter context data into uniform format"""
    processed_data = []
    for obj in data:
        # Determine origin type
        origin_type = "NATIVE"  # default
        if "meta" in obj and "origin" in obj["meta"]:
            origin_type = obj["meta"]["origin"].get("originType", "NATIVE")

        processed_data.append(
            {
                "filter_context_id": obj["id"],
                "workspace_id": workspace_id,
                "title": obj.get("attributes", {}).get("title", ""),
                "description": obj.get("attributes", {}).get("description", ""),
                "origin_type": origin_type,
                "content": obj.get("attributes", {}).get("content", {}),
            }
        )
    return processed_data


def process_filter_context_fields(data, workspace_id=None):
    """Process filter context data to extract individual filter fields"""
    processed_fields = []

    for obj in data:
        filter_context_id = obj["id"]
        content = obj.get("attributes", {}).get("content", {})
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
