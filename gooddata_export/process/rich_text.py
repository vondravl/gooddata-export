"""Rich text extraction utilities for GoodData dashboards."""

import json
import logging
import re

from gooddata_export.process.common import (
    DEBUG_RICH_TEXT,
    get_debug_output_dir,
    import_time_iso,
)

logger = logging.getLogger(__name__)


def debug_rich_text_extraction(
    content_str, extraction_type, found_items, context_info=None, output_file=None
):
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
        output_file = (
            get_debug_output_dir() / f"rich_text_{extraction_type}_extraction.json"
        )

    # Prepare debug data structure
    debug_data = {
        "extraction_type": extraction_type,
        "timestamp": import_time_iso(),
        "content_length": len(content_str) if content_str else 0,
        "content_preview": content_str[:500] if content_str else "(empty)",
        "context_info": context_info or {},
        "extraction_results": {
            "found_items_count": len(found_items),
            "found_items": found_items,
        },
        "extraction_details": {},
    }

    # Add extraction-specific details
    if extraction_type == "insights":
        # Extract pattern matches for insights
        all_curly = extract_values_from_curly_braces(content_str)
        all_ids = extract_all_ids_from_content(content_str)
        insight_prefixes = [
            "insightFirstAttribute",
            "insightFirstMeasure",
            "insightFirstMeasureChange",
            "comparisonFromInsightMeasure",
            "insightFirstTotal",
            "insightMeasure",
            "insightAttribute",
            "insightTotal",
        ]

        debug_data["extraction_details"] = {
            "all_uuids_found": all_ids,
            "all_curly_braces": all_curly,
            "insight_related_curly": [
                item for item in all_curly if item[0] in insight_prefixes
            ],
            "insight_prefixes_used": insight_prefixes,
        }
    elif extraction_type == "metrics":
        # Extract pattern matches for metrics
        all_curly = extract_values_from_curly_braces(content_str)
        all_ids = extract_all_ids_from_content(content_str)
        metric_prefixes = [
            "measure",
            "measureChange",
            "measureValue",
            "measureTotal",
            "measuresComparison",
            "measuresShareComparison",
        ]

        # Also get special metric ID patterns
        metric_id_pattern = re.compile(r"'([a-z0-9_-]+)'")
        metric_patterns = [
            match.group(1)
            for match in metric_id_pattern.finditer(content_str)
            if "_-_" in match.group(1) and len(match.group(1)) > 10
        ]

        debug_data["extraction_details"] = {
            "all_uuids_found": all_ids,
            "all_curly_braces": all_curly,
            "metric_related_curly": [
                item for item in all_curly if item[0] in metric_prefixes
            ],
            "metric_prefixes_used": metric_prefixes,
            "metric_patterns_found": metric_patterns,
        }

    # Make sure directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Write to file
    try:
        with open(output_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(debug_data, indent=2) + "\n\n")
        logger.debug(
            "Debug info for %s extraction written to %s", extraction_type, output_file
        )
    except Exception as e:
        logger.warning("Failed to write debug info to file: %s", e)

    return debug_data


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

    # Standard UUID pattern
    uuid_pattern = re.compile(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    )

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
            matches.append(
                {"uuid": uuid, "position": match.start(), "context": context}
            )

    # Write detailed debug info for UUID matches
    if DEBUG_RICH_TEXT and len(matches) > 0:
        try:
            import random

            # Only write debug data for a sample of calls to avoid flooding the disk
            if random.random() < 0.1:  # 10% of calls
                output_file = get_debug_output_dir() / "uuid_extraction.json"
                output_file.parent.mkdir(parents=True, exist_ok=True)
                debug_data = {
                    "timestamp": import_time_iso(),
                    "content_length": len(content_str),
                    "content_preview": content_str[:100]
                    + ("..." if len(content_str) > 100 else ""),
                    "uuids_found": len(all_ids),
                    "uuid_matches": matches[
                        :10
                    ],  # Limit to 10 matches to avoid huge files
                }
                with open(output_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps(debug_data, indent=2) + "\n\n")
        except Exception:
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

        if single_quote_pos != -1 and (
            double_quote_pos == -1 or single_quote_pos < double_quote_pos
        ):
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
        id_value = content_str[end_id + 1 : close_quote].strip()

        # Add to results if it's a valid ID (not empty)
        if id_value:
            results.append(id_value)

        # Move start position for next search
        start_pos = close_quote + 1

    return results


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

    # Match pattern: {tag:value} or {tag:[value1,value2]} or {tag:value,param:value}
    # This is a simplified version, real JSON parsing would be more robust
    pattern = re.compile(r"{([^{}:]+):([^{}]+?)}")

    results = []
    matches = []  # For debug purposes

    for match in pattern.finditer(content_str):
        tag = match.group(1).strip()
        value = match.group(2).strip()
        full_match = match.group(0)
        matches.append({"full_match": full_match, "tag": tag, "raw_value": value})

        # If it's an array like [value1,value2]
        if value.startswith("[") and value.endswith("]"):
            array_values = value[1:-1].split(",")
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
            import random

            # Only write debug data for a sample of calls to avoid flooding the disk
            if random.random() < 0.1:  # 10% of calls
                output_file = get_debug_output_dir() / "curly_brace_extraction.json"
                output_file.parent.mkdir(parents=True, exist_ok=True)
                debug_data = {
                    "timestamp": import_time_iso(),
                    "content_preview": content_str[:100]
                    + ("..." if len(content_str) > 100 else ""),
                    "matches_found": len(matches),
                    "matches": matches,
                    "results": results,
                }
                with open(output_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps(debug_data, indent=2) + "\n\n")
        except Exception:
            # Don't fail the main function for debug issues
            pass

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
        "insightFirstAttribute",
        "insightFirstMeasure",
        "insightFirstMeasureChange",
        "comparisonFromInsightMeasure",
        "insightFirstTotal",
        "insightMeasure",
        "insightAttribute",
        "insightTotal",
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
                if "-" in value and len(value) >= 36:  # Looks like a UUID
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
                        if (
                            content_str.find(
                                id, max(0, pattern_pos - 50), pattern_pos + 100
                            )
                            != -1
                        ):
                            unique_ids.add(id)

    # Create result in the same format as dashboards_visualizations
    result = [
        {"dashboard_id": dashboard_id, "visualization_id": viz_id, "from_rich_text": 1}
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
            "final_unique_count": len(unique_ids),
        },
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
            "measure",
            "measureChange",
            "measureValue",
            "measureTotal",
            "measuresComparison",
            "measuresShareComparison",
        ]

        # Extract metrics from curly braces
        unique_ids = set()

        # Look for values with metric-related prefixes
        for prefix, value in all_curly_values:
            if prefix in metric_prefixes:
                # If value looks like a UUID or a metric ID with underscores
                if ("-" in value and len(value) >= 36) or "_-_" in value:
                    unique_ids.add(value)

        # Also look for metrics with the special naming convention used in your system
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
        logger.warning("Error in process_rich_text_metrics: %s", e)
        unique_ids = set()
        filtered_ids = set()
        metric_pattern_matches = []

    # Create the result
    result = [
        {"dashboard_id": dashboard_id, "metric_id": metric_id}
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
            "after_filtering": len(filtered_ids) if known_metrics else "n/a",
        },
    )

    return result


def process_dashboards_metrics_from_rich_text(
    dashboard_data,
    workspace_id=None,
    known_metrics=None,
    config=None,
):
    """Extract metrics directly referenced in dashboard rich text (layout format).

    Supports both tabbed dashboards (content.tabs[]) and legacy non-tabbed
    dashboards (content.layout.sections).

    Args:
        dashboard_data: List of dashboard objects with "id" and "content" fields
        workspace_id: Workspace ID to include in results
        known_metrics: Set of known metric IDs for validation/filtering
        config: ExportConfig instance (used to check ENABLE_RICH_TEXT_EXTRACTION)

    Returns:
        List of dicts with dashboard_id, metric_id, workspace_id keys
    """
    # Import here to avoid circular imports
    from gooddata_export.process.common import UniqueRelationshipTracker
    from gooddata_export.process.dashboard_traversal import iterate_dashboard_widgets

    # If rich text extraction is disabled, return empty result
    enable_rich_text = config.ENABLE_RICH_TEXT_EXTRACTION if config else False
    if not enable_rich_text:
        return []

    # Normalize known_metrics to a set
    if known_metrics is None:
        known_metrics = set()
    elif not isinstance(known_metrics, set):
        known_metrics = set(known_metrics)

    # Track unique relationships: (dashboard_id, metric_id, workspace_id)
    tracker = UniqueRelationshipTracker(
        key_fields=["dashboard_id", "metric_id", "workspace_id"]
    )

    # Patterns that indicate metric references in widget content
    metric_patterns = [
        "measureChange",
        "measuresComparison",
        "measureValue",
        "measuresShareComparison",
        "measureTotal",
        "measure:",
    ]

    if DEBUG_RICH_TEXT:
        logger.debug("Scanning dashboards for metrics in rich text...")

    # Use shared traversal utility
    for dashboard_id, _tab_id, widget in iterate_dashboard_widgets(dashboard_data):
        # Rich text widgets - extract metrics from these
        if widget.get("type") == "richText":
            rich_text_content = widget.get("content", "")
            rich_text_metrics = process_rich_text_metrics(
                rich_text_content, dashboard_id, known_metrics
            )
            for metric_ref in rich_text_metrics:
                tracker.add(
                    {
                        "dashboard_id": dashboard_id,
                        "metric_id": metric_ref["metric_id"],
                        "workspace_id": workspace_id,
                    }
                )

        # Check widget content fields for metrics
        widget_content = widget.get("content")
        if isinstance(widget_content, str) and any(
            pattern in widget_content for pattern in metric_patterns
        ):
            content_metrics = process_rich_text_metrics(
                widget_content, dashboard_id, known_metrics
            )
            for metric_ref in content_metrics:
                tracker.add(
                    {
                        "dashboard_id": dashboard_id,
                        "metric_id": metric_ref["metric_id"],
                        "workspace_id": workspace_id,
                    }
                )

    if DEBUG_RICH_TEXT:
        logger.debug("Found %d metric references in dashboard rich text", len(tracker))

    # Sort the results for consistency
    return tracker.get_sorted(sort_key=lambda x: (x["dashboard_id"], x["metric_id"]))
