import logging
import re
import requests
import json
import os
from gooddata_export.common import get_api_client

logger = logging.getLogger(__name__)

# Environment flag to control debug output (off by default)
# Set DEBUG_RICH_TEXT to one of: 1/true/yes/on to enable
DEBUG_RICH_TEXT = os.environ.get('DEBUG_RICH_TEXT', '0').lower() in ('1', 'true', 'yes', 'on')


def debug_rich_text_extraction(content_str, extraction_type, found_items, context_info=None, output_file=None):
    """
    Debug function to log details about what's extracted from rich text content
    
    Args:
        content_str: The original rich text content
        extraction_type: String indicating what we're extracting ('metrics' or 'insights')
        found_items: List of items found in the extraction
        context_info: Dictionary with additional context (dashboard ID, etc.)
        output_file: Optional file path to write the debug info to
    """
    # Skip if debugging is disabled
    if not DEBUG_RICH_TEXT:
        return None
    
    if not output_file:
        # Default to a debug file in the project
        import os
        output_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                                   "debug_output", f"rich_text_{extraction_type}_extraction.json")
    
    # Prepare debug data structure
    debug_data = {
        "extraction_type": extraction_type,
        "timestamp": import_time_iso(),
        "content_length": len(content_str) if content_str else 0,
        "content_preview": content_str[:500] if content_str else "(empty)",
        "context_info": context_info or {},
        "extraction_results": {
            "found_items_count": len(found_items),
            "found_items": found_items
        },
        "extraction_details": {}
    }
    
    # Add extraction-specific details
    if extraction_type == "insights":
        # Extract pattern matches for insights
        all_curly = extract_values_from_curly_braces(content_str)
        all_ids = extract_all_ids_from_content(content_str)
        insight_prefixes = [
            'insightFirstAttribute', 'insightFirstMeasure', 'insightFirstMeasureChange',
            'comparisonFromInsightMeasure', 'insightFirstTotal', 'insightMeasure',
            'insightAttribute', 'insightTotal'
        ]
        
        debug_data["extraction_details"] = {
            "all_uuids_found": all_ids,
            "all_curly_braces": all_curly,
            "insight_related_curly": [item for item in all_curly if item[0] in insight_prefixes],
            "insight_prefixes_used": insight_prefixes,
        }
    elif extraction_type == "metrics":
        # Extract pattern matches for metrics
        all_curly = extract_values_from_curly_braces(content_str)
        all_ids = extract_all_ids_from_content(content_str)
        metric_prefixes = [
            'measure', 'measureChange', 'measureValue', 'measureTotal',
            'measuresComparison', 'measuresShareComparison'
        ]
        
        # Also get special metric ID patterns
        metric_id_pattern = re.compile(r"'([a-z0-9_-]+)'")
        metric_patterns = [match.group(1) for match in metric_id_pattern.finditer(content_str)
                          if "_-_" in match.group(1) and len(match.group(1)) > 10]
        
        debug_data["extraction_details"] = {
            "all_uuids_found": all_ids,
            "all_curly_braces": all_curly,
            "metric_related_curly": [item for item in all_curly if item[0] in metric_prefixes],
            "metric_prefixes_used": metric_prefixes,
            "metric_patterns_found": metric_patterns
        }
    
    # Make sure directory exists
    import os
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Write to file
    try:
        with open(output_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(debug_data, indent=2) + "\n\n")
        logger.info(f"Debug info for {extraction_type} extraction written to {output_file}")
    except Exception as e:
        logger.warning(f"Failed to write debug info to file: {str(e)}")
        
    return debug_data


def import_time_iso():
    """Get current time in ISO format for debugging purposes"""
    from datetime import datetime
    return datetime.now().isoformat()


def fetch_data(endpoint, client=None, config=None, max_retries=3):
    """Fetch data from GoodData API with pagination and retry mechanism"""
    import time
    
    if client is None:
        if config is None:
            raise ValueError("Either client or config must be provided")
        client = get_api_client(config)
        logging.debug("Created new API client")

    base_url = f"{client['base_url']}/api/v1/entities/workspaces/{client['workspace_id']}/{endpoint}"
    logging.info(f"Fetching {endpoint} from workspace: {client['workspace_id']}")

    all_data = []
    page = 0
    
    while True:
        # Add page parameter to params
        params = client["params"].copy()
        params["page"] = str(page)
        
        url = base_url
        logging.debug(f"Fetching {endpoint} page {page}")
        
        page_success = False
        
        for attempt in range(max_retries + 1):
            try:
                # Increase timeout for parallel requests and add backoff
                timeout = 60 + (attempt * 15)  # 60s, 75s, 90s, 105s
                if attempt > 0:
                    # Add exponential backoff delay
                    delay = 2 ** attempt
                    logging.info(f"Retrying {endpoint} page {page} (attempt {attempt + 1}/{max_retries + 1}) after {delay}s delay")
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
                    logging.debug(f"Page {page}: Found {len(page_data)} items")
                    page_success = True
                    break  # Success, exit retry loop

                elif response.status_code == 404:
                    error_msg = (
                        f"Failed to fetch {endpoint} (404)\n"
                        f"Workspace: {client['workspace_id']}\n"
                        f"Please verify workspace ID in .env file\n"
                        f"Response: {response.text}"
                    )
                    logging.error(error_msg)
                    raise RuntimeError(error_msg)

                elif response.status_code == 401:
                    error_msg = (
                        f"Authentication failed for {endpoint}\n"
                        f"Please check API token in .env file\n"
                        f"Response: {response.text}"
                    )
                    logging.error(error_msg)
                    raise RuntimeError(error_msg)

                else:
                    error_msg = (
                        f"Failed to fetch {endpoint} (HTTP {response.status_code})\n"
                        f"Response: {response.text[:200]}"
                    )
                    logging.error(error_msg)
                    raise RuntimeError(error_msg)

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if attempt < max_retries:
                    logging.warning(f"Timeout/connection error for {endpoint} page {page} (attempt {attempt + 1}): {str(e)}")
                    continue
                else:
                    base_url_str = client.get('base_url') if client else 'unknown'
                    error_msg = (
                        f"Connection error fetching {endpoint} after {max_retries + 1} attempts\n"
                        f"Please verify HOST in .env file: {base_url_str}\n"
                        f"Last error: {str(e)}"
                    )
                    logging.error(error_msg)
                    raise RuntimeError(error_msg)

            except requests.exceptions.RequestException as e:
                error_msg = f"Request failed for {endpoint}: {str(e)}"
                logging.error(error_msg)
                raise RuntimeError(error_msg)

            except Exception as e:
                error_msg = f"Unexpected error fetching {endpoint}: {str(e)}"
                logging.error(error_msg)
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
        logging.warning(f"{endpoint}: No data received (empty array)")
        return None
    
    # Filter to NATIVE origin for all entity endpoints fetched via this function
    filtered_data = []
    for obj in all_data:
        origin_type = (
            obj.get("meta", {})
            .get("origin", {})
            .get("originType", "NATIVE")
        )
        if str(origin_type).upper() == "NATIVE":
            filtered_data.append(obj)

    try:
        sorted_data = sorted(
            filtered_data,
            key=lambda obj: obj.get("attributes", {}).get("title", ""),
        )
        # Log with page count only if more than 1 page
        if page > 1:
            logging.info(
                f"{endpoint}: Successfully fetched {len(sorted_data)} items across {page} pages"
            )
        else:
            logging.info(
                f"{endpoint}: Successfully fetched {len(sorted_data)} items"
            )
        return sorted_data
    except KeyError as sort_error:
        logging.warning(f"{endpoint}: Could not sort data - {sort_error}")
        return filtered_data


def sort_tags(tags):
    """Sort tags alphabetically if they are a list"""
    if isinstance(tags, list):
        return sorted(tags)
    return tags


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
    """Extract unique metric IDs used in each visualization"""
    # Using set to store unique combinations including workspace context
    unique_relationships = set()
    
    # For debugging
    metrics_found = 0
    visualizations_with_metrics = 0

    for viz in visualization_data:
        content = viz["attributes"]["content"]
        viz_metrics_found = 0
        
        if "buckets" not in content:
            continue

        for bucket in content["buckets"]:
            if "items" not in bucket:
                continue

            for item in bucket["items"]:
                measure_def = (
                    item.get("measure", {})
                    .get("definition", {})
                    .get("measureDefinition", {})
                )
                metric_id = measure_def.get("item", {}).get("identifier", {}).get("id")

                if metric_id:
                    unique_relationships.add((viz["id"], metric_id, workspace_id))
                    viz_metrics_found += 1
        
        if viz_metrics_found > 0:
            metrics_found += viz_metrics_found
            visualizations_with_metrics += 1

    # Convert set of tuples back to list of dictionaries
    result = [
        {"visualization_id": viz_id, "metric_id": metric_id, "workspace_id": ws_id}
        for viz_id, metric_id, ws_id in sorted(unique_relationships)
    ]
    
    # Write debug data if debugging is enabled
    if DEBUG_RICH_TEXT:
        debug_data = {
            "extraction_type": "visualization_metrics",
            "timestamp": import_time_iso(),
            "metrics_found": metrics_found,
            "visualizations_with_metrics": visualizations_with_metrics,
            "total_visualizations": len(visualization_data),
            "unique_relationships": len(result)
        }
        
        try:
            import os
            import json
            output_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                                "debug_output", "visualization_metrics_extraction.json")
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(debug_data, indent=2) + "\n\n")
            logger.info(f"Debug info for visualization metrics written to {output_file}")
        except Exception as e:
            logger.warning(f"Failed to write debug info to file: {str(e)}")
    
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


def process_dashboard_visualizations(dashboard_data, workspace_id=None, known_insights=None, config=None):
    """Extract unique visualization IDs used in each dashboard, including rich text references"""
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
                    relationships.append({
                        "dashboard_id": dashboard_id, 
                        "visualization_id": viz_id,
                        "from_rich_text": 0,
                        "workspace_id": workspace_id,
                    })
                    added_relationships.add(key)

            # Multiple visualization widgets (visualizationSwitcher)
            visualizations = widget.get("visualizations", [])
            for viz in visualizations:
                viz_id = viz.get("insight", {}).get("identifier", {}).get("id")
                if viz_id:
                    key = (dashboard_id, viz_id, 0)  # 0 = regular reference
                    if key not in added_relationships:
                        relationships.append({
                            "dashboard_id": dashboard_id, 
                            "visualization_id": viz_id,
                            "from_rich_text": 0,
                            "workspace_id": workspace_id,
                        })
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
                    rich_text_insights = process_rich_text_insights(rich_text_content, dashboard_id, known_insights)
                    for insight in rich_text_insights:
                        key = (dashboard_id, insight["visualization_id"], 1)
                        if key not in added_relationships:
                            relationships.append({
                                "dashboard_id": dashboard_id,
                                "visualization_id": insight["visualization_id"],
                                "from_rich_text": 1,
                                "workspace_id": workspace_id,
                            })
                            added_relationships.add(key)
                
                # Other widget content that might contain insight references
                widget_content = widget.get("content")
                if isinstance(widget_content, str) and any(pattern in widget_content for pattern in [
                    "insightFirstAttribute", "insightFirstMeasure", "insightFirstMeasureChange",
                    "comparisonFromInsightMeasure", "insightFirstTotal"
                ]):
                    additional_insights = process_rich_text_insights(widget_content, dashboard_id, known_insights)
                    for insight in additional_insights:
                        key = (dashboard_id, insight["visualization_id"], 1)
                        if key not in added_relationships:
                            relationships.append({
                                "dashboard_id": dashboard_id,
                                "visualization_id": insight["visualization_id"],
                                "from_rich_text": 1,
                                "workspace_id": workspace_id,
                            })
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
        key=lambda x: (x["dashboard_id"], x["visualization_id"], x["from_rich_text"]) 
    )


def fetch_ldm(client=None, config=None):
    """Fetch logical model from GoodData API"""
    try:
        if client is None:
            if config is None:
                raise ValueError("Either client or config must be provided")
            client = get_api_client(config)
            logging.debug("Created new API client")

        url = f"{client['base_url']}/api/v1/layout/workspaces/{client['workspace_id']}/logicalModel"
        logging.info(f"Fetching logical model from workspace: {client['workspace_id']}")

        response = requests.get(url, headers=client["headers"], timeout=30)

        if response.status_code == 200:
            json_input = response.json()

            if not json_input:
                logging.warning("Logical model: No data received (empty response)")
                return None

            logging.info("Logical model: Successfully fetched")
            return json_input

        elif response.status_code == 404:
            error_msg = (
                "Failed to fetch logical model (404)\n"
                f"Workspace: {client['workspace_id']}\n"
                "Please verify workspace ID in .env file\n"
                f"Response: {response.text}"
            )
            logging.error(error_msg)
            raise RuntimeError(error_msg)

        elif response.status_code == 401:
            error_msg = (
                "Authentication failed for logical model\n"
                "Please check API token in .env file\n"
                f"Response: {response.text}"
            )
            logging.error(error_msg)
            raise RuntimeError(error_msg)

        else:
            error_msg = (
                f"Failed to fetch logical model (HTTP {response.status_code})\n"
                f"Response: {response.text[:200]}"
            )
            logging.error(error_msg)
            raise RuntimeError(error_msg)

    except requests.exceptions.ConnectionError as e:
        base_url = client.get('base_url') if client else 'unknown'
        error_msg = (
            "Connection error fetching logical model\n"
            f"Please verify HOST in .env file: {base_url}\n"
            f"Error: {str(e)}"
        )
        logging.error(error_msg)
        raise RuntimeError(error_msg)

    except requests.exceptions.RequestException as e:
        error_msg = f"Request failed for logical model: {str(e)}"
        logging.error(error_msg)
        raise RuntimeError(error_msg)

    except Exception as e:
        error_msg = f"Unexpected error fetching logical model: {str(e)}"
        logging.error(error_msg)
        raise RuntimeError(error_msg)


def process_ldm(data, workspace_id=None):
    """Parse logical model data into datasets and columns"""
    datasets = []
    columns = []

    # First pass: Process all datasets basic info
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
            "workspace_data_filter_columns_count": len(dataset.get("workspaceDataFilterColumns", [])),
            "total_columns": (
                len(dataset.get("attributes", []))
                + len(dataset.get("facts", []))
                + len(dataset.get("references", []))
                + len(dataset.get("workspaceDataFilterColumns", []))
            ),
            "data_source_id": (
                dataset.get("dataSourceTableId", {}).get("dataSourceId", "") or
                dataset.get("sql", {}).get("dataSourceId", "")
            ),
            "source_table": (
                dataset.get("dataSourceTableId", {}).get("id", "") 
                if dataset.get("dataSourceTableId") 
                else dataset.get("sql", {}).get("statement", "")
            ),
            "source_table_path": str(
                dataset.get("dataSourceTableId", {}).get("path", [])
            ) if dataset.get("dataSourceTableId") else "SQL Query",
            "workspace_id": workspace_id,
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
                    "workspace_id": workspace_id,
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
                    "workspace_id": workspace_id,
                }
            )

        # Add references
        for ref in dataset.get("references", []):
            target_dataset_id = ref["identifier"]["id"]
            target_dataset_info = dataset_map.get(target_dataset_id)

            columns.append(
                {
                    "dataset_id": dataset["id"],
                    "dataset_name": dataset["title"],
                    "title": ref["identifier"]["id"],
                    "description": "",
                    "id": ref["sources"][0]["column"],
                    "tags": "",
                    "data_type": ref["sources"][0]["dataType"],
                    "source_column": ref["sources"][0]["column"],
                    "type": "reference",
                    "grain": "No",
                    "reference_to_id": target_dataset_id,
                    "reference_to_title": target_dataset_info["title"]
                    if target_dataset_info
                    else "",
                    "workspace_id": workspace_id,
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
                    "workspace_id": workspace_id,
                }
            )

    return datasets, columns


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
                processed_fields.append({
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
                })
            
            elif "attributeFilter" in filter_obj:
                attr_filter = filter_obj["attributeFilter"]
                display_form_id = (
                    attr_filter.get("displayForm", {})
                    .get("identifier", {})
                    .get("id", "")
                )
                
                # Count attribute elements
                attribute_elements = attr_filter.get("attributeElements", {}).get("uris", [])
                elements_count = len(attribute_elements) if attribute_elements else 0
                
                processed_fields.append({
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
                })
    
    return processed_fields


def fetch_child_workspaces(client=None, config=None, size=2000):
    """Fetch child workspaces from GoodData API with pagination support"""
    try:
        if client is None:
            if config is None:
                raise ValueError("Either client or config must be provided")
            client = get_api_client(config)
            logging.debug("Created new API client")

        parent_workspace_id = client['workspace_id']
        all_workspaces = []
        page = 0
        
        while True:
            url = f"{client['base_url']}/api/v1/entities/workspaces"
            params = {
                "filter": f"parent.id=={parent_workspace_id}",
                "include": "workspaces",
                "size": str(size),
                "page": str(page)
            }
            
            logging.debug(f"Fetching child workspaces for parent: {parent_workspace_id} (page {page})")
            logging.debug(f"Request URL: {url}")
            logging.debug(f"Request params: {params}")

            response = requests.get(url, params=params, headers=client["headers"], timeout=30)

            logging.debug(f"Child workspaces API response status: {response.status_code}")
            
            if response.status_code == 200:
                json_input = response.json()
                data = json_input.get("data", [])
                
                # If no data returned, we've reached the end
                if not data:
                    break
                    
                all_workspaces.extend(data)
                page += 1
                
                logging.debug(f"Page {page - 1}: Found {len(data)} child workspaces")
            
            elif response.status_code == 404:
                error_msg = (
                    "Failed to fetch child workspaces (404)\n"
                    f"Parent workspace: {parent_workspace_id}\n"
                    "Please verify workspace ID in .env file\n"
                    f"Response: {response.text}"
                )
                logging.error(error_msg)
                raise RuntimeError(error_msg)

            elif response.status_code == 401:
                error_msg = (
                    "Authentication failed for child workspaces\n"
                    "Please check API token in .env file\n"
                    f"Response: {response.text}"
                )
                logging.error(error_msg)
                raise RuntimeError(error_msg)

            else:
                error_msg = (
                    f"Failed to fetch child workspaces (HTTP {response.status_code})\n"
                    f"Response: {response.text[:200]}"
                )
                logging.error(error_msg)
                raise RuntimeError(error_msg)

        if all_workspaces:
            # Log with page count only if more than 1 page
            if page > 1:
                logging.info(f"Found {len(all_workspaces)} total child workspaces across {page} pages")
            else:
                logging.info(f"Found {len(all_workspaces)} total child workspaces")
            for child in all_workspaces:
                logging.debug(f"Child workspace: {child['attributes']['name']} ({child['id']})")
            return all_workspaces
        else:
            logging.debug("No child workspaces found - empty data array")
            return []

    except requests.exceptions.ConnectionError as e:
        base_url = client.get('base_url') if client else 'unknown'
        error_msg = (
            "Connection error fetching child workspaces\n"
            f"Please verify HOST in .env file: {base_url}\n"
            f"Error: {str(e)}"
        )
        logging.error(error_msg)
        raise RuntimeError(error_msg)

    except requests.exceptions.RequestException as e:
        error_msg = f"Request failed for child workspaces: {str(e)}"
        logging.error(error_msg)
        raise RuntimeError(error_msg)

    except Exception as e:
        error_msg = f"Unexpected error fetching child workspaces: {str(e)}"
        logging.error(error_msg)
        raise RuntimeError(error_msg)


def process_workspaces(parent_workspace_id, parent_workspace_name, child_workspaces_data):
    """Process workspace data into uniform format"""
    processed_data = []
    
    # Add parent workspace
    processed_data.append({
        "workspace_id": parent_workspace_id,
        "workspace_name": parent_workspace_name,
        "is_parent": True,
        "parent_workspace_id": None,
        "created_at": "",
        "modified_at": "",
    })
    
    # Add child workspaces
    for workspace in child_workspaces_data:
        processed_data.append({
            "workspace_id": workspace["id"],
            "workspace_name": workspace["attributes"]["name"],
            "is_parent": False,
            "parent_workspace_id": parent_workspace_id,
            "created_at": workspace["attributes"].get("createdAt", ""),
            "modified_at": workspace["attributes"].get("modifiedAt", workspace["attributes"].get("createdAt", "")),
        })
    
    return processed_data
def extract_all_ids_from_content(content_str):
    """
    Extract all potential UUIDs from content using regex pattern matching
    
    Args:
        content_str: String containing the content to search
        
    Returns:
        List of potential UUID strings found in the content
    """
    if not content_str or not isinstance(content_str, str):
        return []
    
    import re
    
    # Standard UUID pattern
    uuid_pattern = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
    
    # Find all UUIDs in the content
    all_ids = set()
    matches = []  # For debug purposes
    
    for match in uuid_pattern.finditer(content_str):
        uuid = match.group(0)
        if uuid and len(uuid) == 36:  # Standard UUID length
            all_ids.add(uuid)
            # Store context around the match for debugging
            start_idx = max(0, match.start() - 20)
            end_idx = min(len(content_str), match.end() + 20)
            context = content_str[start_idx:end_idx]
            matches.append({
                "uuid": uuid, 
                "position": match.start(),
                "context": context
            })
    
    # Write detailed debug info for UUID matches
    if DEBUG_RICH_TEXT and len(matches) > 0:
        try:
            import os
            import json
            import random
            # Only write debug data for a sample of calls to avoid flooding the disk
            if random.random() < 0.1:  # 10% of calls
                output_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                                      "debug_output", "uuid_extraction.json")
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                debug_data = {
                    "timestamp": import_time_iso(),
                    "content_length": len(content_str),
                    "content_preview": content_str[:100] + ("..." if len(content_str) > 100 else ""),
                    "uuids_found": len(all_ids),
                    "uuid_matches": matches[:10]  # Limit to 10 matches to avoid huge files
                }
                with open(output_file, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(debug_data, indent=2) + "\n\n")
        except Exception as e:
            # Don't fail the main function for debug issues
            pass
    
    return list(all_ids)


def extract_from_rich_text(content_str, pattern_start, pattern_end=None):
    """
    Extract IDs from rich text content based on patterns
    
    Args:
        content_str: String containing the rich text content
        pattern_start: Starting pattern to look for (e.g. '{insightFirstAttribute:')
        pattern_end: Optional ending pattern (defaults to '}')
        
    Returns:
        List of IDs found in the content
    """
    if not content_str or not isinstance(content_str, str):
        return []
        
    if pattern_end is None:
        pattern_end = "}"
        
    results = []
    start_pos = 0
    
    while True:
        # Find the next occurrence of the pattern
        pos = content_str.find(pattern_start, start_pos)
        if pos == -1:
            break
            
        # Find the position after the pattern
        start_id = pos + len(pattern_start)
        
        # Check for both single and double quotes
        single_quote_pos = content_str.find("'", start_id)
        double_quote_pos = content_str.find('"', start_id)
        
        # Find which quote type is being used (if any)
        quote_type = None
        end_id = -1
        
        if single_quote_pos != -1 and (double_quote_pos == -1 or single_quote_pos < double_quote_pos):
            quote_type = "'"
            end_id = single_quote_pos
        elif double_quote_pos != -1:
            quote_type = '"'
            end_id = double_quote_pos
        
        if end_id == -1:
            # No quotes found, try to find the pattern end directly
            end_id = content_str.find(pattern_end, start_id)
            if end_id == -1:
                # Try finding a comma which might separate parameters
                end_id = content_str.find(",", start_id)
                if end_id == -1:
                    break
                
            # Extract the ID between pattern_start and the end marker
            id_value = content_str[start_id:end_id].strip()
            
            # Clean any remaining quotes or braces
            id_value = id_value.strip("'\"")
            
            # Add to results if it looks like an ID (contains hyphens and is reasonably long)
            if id_value and "-" in id_value and len(id_value) > 30:
                results.append(id_value)
                
            # Move start position for next search
            start_pos = end_id + 1
            continue
            
        # If quotes were found, find the closing quote
        close_quote = content_str.find(quote_type, end_id + 1)
        if close_quote == -1:
            break
            
        # Extract the ID between quotes
        id_value = content_str[end_id + 1:close_quote].strip()
            
        # Add to results if it's a valid ID (not empty)
        if id_value:
            results.append(id_value)
            
        # Move start position for next search
        start_pos = close_quote + 1
        
    return results


def process_rich_text_insights(content_str, dashboard_id, known_insights=None):
    """
    Process rich text content to extract insight IDs
    
    Args:
        content_str: String containing the rich text content
        dashboard_id: ID of the dashboard containing the rich text
        known_insights: Optional list of known insight IDs to filter against
        
    Returns:
        List of dictionaries with dashboard_id, visualization_id, from_rich_text
    """
    if not content_str or not isinstance(content_str, str):
        return []
    
    # Extract all values within curly braces
    all_curly_values = extract_values_from_curly_braces(content_str)
    
    # Find all potential UUIDs in the content
    all_ids = extract_all_ids_from_content(content_str)
    
    # Insight-related prefixes in rich text
    insight_prefixes = [
        'insightFirstAttribute', 'insightFirstMeasure', 'insightFirstMeasureChange',
        'comparisonFromInsightMeasure', 'insightFirstTotal', 'insightMeasure',
        'insightAttribute', 'insightTotal'
    ]
    
    # If we have known insights, filter to only include those
    if known_insights:
        # First filter all UUIDs against known insights
        uuid_insights = set(id for id in all_ids if id in known_insights)
        
        # Then look for IDs in curly braces that match known insights
        curly_insights = set()
        for prefix, value in all_curly_values:
            if prefix in insight_prefixes and value in known_insights:
                curly_insights.add(value)
                
        unique_ids = uuid_insights.union(curly_insights)
    else:
        # Otherwise, use curly brace extraction and context clues
        unique_ids = set()
        
        # First add all values from curly braces with insight prefixes
        for prefix, value in all_curly_values:
            if prefix in insight_prefixes:
                if '-' in value and len(value) >= 36:  # Looks like a UUID
                    unique_ids.add(value)
        
        # If we didn't find any IDs from curly braces, fall back to all UUIDs
        # with some basic filtering
        if not unique_ids:
            # Find UUIDs in the content that appear near insight patterns
            for pattern in insight_prefixes:
                pattern_pos = content_str.find(pattern)
                if pattern_pos > -1:
                    # Look within a reasonable distance of the pattern
                    for id in all_ids:
                        if content_str.find(id, max(0, pattern_pos - 50), pattern_pos + 100) != -1:
                            unique_ids.add(id)
    
    # Create result in the same format as dashboard_visualizations
    result = [
        {
            "dashboard_id": dashboard_id,
            "visualization_id": viz_id,
            "from_rich_text": 1
        }
        for viz_id in unique_ids
    ]
    
    # Debug the extraction process
    debug_rich_text_extraction(
        content_str=content_str,
        extraction_type="insights",
        found_items=result,
        context_info={
            "dashboard_id": dashboard_id,
            "known_insights_count": len(known_insights) if known_insights else 0,
            "uuid_count": len(all_ids),
            "curly_values_count": len(all_curly_values),
            "final_unique_count": len(unique_ids)
        }
    )
    
    return result


def process_rich_text_metrics(content_str, dashboard_id, known_metrics=None):
    """
    Process rich text content to extract metric IDs
    
    Args:
        content_str: String containing the rich text content
        dashboard_id: ID of the dashboard containing the rich text
        known_metrics: Optional list of known metric IDs to filter against
        
    Returns:
        List of dictionaries with dashboard_id, metric_id
    """
    if not content_str or not isinstance(content_str, str):
        return []
    
    try:
        # Extract all values within curly braces
        all_curly_values = extract_values_from_curly_braces(content_str)
        
        # Find all potential UUIDs in the content as fallback
        all_ids = extract_all_ids_from_content(content_str)
        
        # Metric-related prefixes in rich text
        metric_prefixes = [
            'measure', 'measureChange', 'measureValue', 'measureTotal',
            'measuresComparison', 'measuresShareComparison'
        ]
        
        # Extract metrics from curly braces
        unique_ids = set()
        
        # Look for values with metric-related prefixes
        for prefix, value in all_curly_values:
            if prefix in metric_prefixes:
                # If value looks like a UUID or a metric ID with underscores
                if ('-' in value and len(value) >= 36) or '_-_' in value:
                    unique_ids.add(value)
                    
        # Also look for metrics with the special naming convention used in your system
        import re
        metric_id_pattern = re.compile(r"'([a-z0-9_-]+)'")
        metric_pattern_matches = []
        for match in metric_id_pattern.finditer(content_str):
            metric_id = match.group(1)
            if "_-_" in metric_id and len(metric_id) > 10:
                unique_ids.add(metric_id)
                metric_pattern_matches.append(metric_id)
                
        # If we have known metrics, filter to only include those
        # This prevents false positives
        filtered_ids = set()
        if known_metrics:
            # First pass - exact matches to known metrics
            filtered_ids = set(id for id in unique_ids if id in known_metrics)
            
            # If we didn't find any exact matches, check for UUID pattern
            if not filtered_ids:
                filtered_ids = set(id for id in all_ids if id in known_metrics)
                
            unique_ids = filtered_ids
                    
    except Exception as e:
        logger.warning(f"Error in process_rich_text_metrics: {str(e)}")
        unique_ids = set()
        metric_pattern_matches = []
        
    # Create the result
    result = [
        {
            "dashboard_id": dashboard_id,
            "metric_id": metric_id
        }
        for metric_id in unique_ids
    ]
    
    # Debug the extraction process
    debug_rich_text_extraction(
        content_str=content_str,
        extraction_type="metrics",
        found_items=result,
        context_info={
            "dashboard_id": dashboard_id,
            "known_metrics_count": len(known_metrics) if known_metrics else 0,
            "uuid_count": len(all_ids),
            "curly_values_count": len(all_curly_values),
            "metric_pattern_matches": len(metric_pattern_matches),
            "final_unique_count": len(unique_ids),
            "after_filtering": len(filtered_ids) if known_metrics else "n/a"
        }
    )
        
    return result


def extract_values_from_curly_braces(content_str):
    """
    Extract all values inside curly braces from content string
    
    Args:
        content_str: String containing the content to search
        
    Returns:
        List of tuples with (tag, value) where tag is the prefix before the colon
        and value is what follows the colon (usually an ID or parameter)
    """
    if not content_str or not isinstance(content_str, str):
        return []
    
    import re
    
    # Match pattern: {tag:value} or {tag:[value1,value2]} or {tag:value,param:value}
    # This is a simplified version, real JSON parsing would be more robust
    pattern = re.compile(r'{([^{}:]+):([^{}]+?)}')
    
    results = []
    matches = []  # For debug purposes
    
    for match in pattern.finditer(content_str):
        tag = match.group(1).strip()
        value = match.group(2).strip()
        full_match = match.group(0)
        matches.append({
            "full_match": full_match,
            "tag": tag,
            "raw_value": value
        })
        
        # If it's an array like [value1,value2]
        if value.startswith('[') and value.endswith(']'):
            array_values = value[1:-1].split(',')
            for arr_val in array_values:
                clean_val = arr_val.strip().strip("'\"")
                if clean_val:
                    results.append((tag, clean_val))
        else:
            # Clean up quotes if present
            clean_value = value.strip().strip("'\"")
            if clean_value:
                results.append((tag, clean_value))
    
    # Write detailed debug info for complex pattern matches
    if DEBUG_RICH_TEXT and len(matches) > 0:
        try:
            import os
            import json
            import random
            # Only write debug data for a sample of calls to avoid flooding the disk
            if random.random() < 0.1:  # 10% of calls
                output_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                                      "debug_output", "curly_brace_extraction.json")
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                debug_data = {
                    "timestamp": import_time_iso(),
                    "content_preview": content_str[:100] + ("..." if len(content_str) > 100 else ""),
                    "matches_found": len(matches),
                    "matches": matches,
                    "results": results
                }
                with open(output_file, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(debug_data, indent=2) + "\n\n")
        except Exception as e:
            # Don't fail the main function for debug issues
            pass
    
    return results


def process_dashboard_metrics_from_rich_text(dashboard_data, config=None):
    """Extract metrics directly referenced in dashboard rich text"""
    # If rich text extraction is disabled, return empty result
    enable_rich_text = config.ENABLE_RICH_TEXT_EXTRACTION if config else False
    if not enable_rich_text:
        return []
    # Using list to store all relationships
    relationships = []
    
    # Track what's been added to avoid duplicates
    added_relationships = set()
    
    # Get a list of all known metrics for better matching
    known_metrics = set()
    
    # Let's retrieve all metric IDs from the API first
    try:
        if not config:
            raise ValueError("Config must be provided for process_dashboard_metrics_from_rich_text")
        client = get_api_client(config)
        metrics_data = fetch_data("metrics", client, config)
        if metrics_data:
            for metric in metrics_data:
                known_metrics.add(metric["id"])
        if DEBUG_RICH_TEXT:
            logger.info(f"Found {len(known_metrics)} known metrics for validation")
    except Exception:
        if DEBUG_RICH_TEXT:
            logger.warning("Could not fetch metrics for validation")
    
    if DEBUG_RICH_TEXT:
        logger.info("Scanning dashboards for metrics in rich text...")
    for dash in dashboard_data:
        content = dash["attributes"]["content"]
        if "layout" not in content or "sections" not in content["layout"]:
            continue

        for section in content["layout"]["sections"]:
            if "items" not in section:
                continue

            for item in section["items"]:
                # Rich text widgets - extract metrics from these
                if item.get("widget", {}).get("type") == "richText":
                    rich_text_content = item.get("widget", {}).get("content", "")
                    # Process rich text content to extract metric IDs
                    rich_text_metrics = process_rich_text_metrics(rich_text_content, dash["id"], known_metrics)
                    for metric_ref in rich_text_metrics:
                        # Add metric reference if not already added
                        key = (dash["id"], metric_ref["metric_id"])
                        if key not in added_relationships:
                            relationships.append({
                                "dashboard_id": dash["id"], 
                                "metric_id": metric_ref["metric_id"],
                                "from_rich_text": 1
                            })
                            added_relationships.add(key)
                
                # Check widget content fields for metrics
                widget_content = item.get("widget", {}).get("content")
                if isinstance(widget_content, str) and any(pattern in widget_content for pattern in [
                    "measure", "measureChange", "measureValue", "measureTotal"
                ]):
                    # Process any widget content that might contain metric references
                    content_metrics = process_rich_text_metrics(widget_content, dash["id"], known_metrics)
                    for metric_ref in content_metrics:
                        # Add metric reference if not already added
                        key = (dash["id"], metric_ref["metric_id"])
                        if key not in added_relationships:
                            relationships.append({
                                "dashboard_id": dash["id"], 
                                "metric_id": metric_ref["metric_id"],
                                "from_rich_text": 1
                            })
                            added_relationships.add(key)
    
    if DEBUG_RICH_TEXT:
        logger.info(f"Found {len(relationships)} metric references in dashboard rich text")
    
    # Sort the results for consistency
    return sorted(relationships, key=lambda x: (x["dashboard_id"], x["metric_id"]))
