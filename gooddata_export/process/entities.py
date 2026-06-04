"""Entity API functions for fetching and processing GoodData entities."""

import json
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

# Derived (computed) measures carry a localIdentifier but no catalog object id —
# they are built from other bucket measures (PoP, arithmetic, previous-period) or
# from raw MAQL (inline). We record them for inventory ("which visuals use
# computed measures, and of what kind"); object_type carries the flavor and
# referenced_id falls back to the localIdentifier. Keys match GoodData's
# declarative visualization object measure definitions.
DERIVED_MEASURE_TYPES = {
    "arithmeticMeasureDefinition": "derived_arithmetic",
    "popMeasureDefinition": "derived_pop",
    "previousPeriodMeasureDefinition": "derived_previous_period",
    "inlineDefinition": "derived_inline",
}


def validate_workspace_exists(
    client: dict[str, Any] | None = None,
    config: "ExportConfig | None" = None,
    session: "requests.Session | None" = None,
) -> None:
    """Check if workspace exists and is accessible.

    Args:
        client: API client dict (optional if config provided)
        config: ExportConfig instance (optional if client provided)
        session: Optional requests.Session for connection pooling.
            If None, uses requests module directly (no connection reuse).

    Raises:
        RuntimeError: If workspace is not accessible (auth, permissions, or not found)
                      or if there's a connection error.
    """
    client = get_api_client(config=config, client=client)
    # Use session if provided, otherwise fall back to requests module
    http = session if session is not None else requests
    workspace_id = client["workspace_id"]
    url = f"{client['base_url']}/api/v1/entities/workspaces/{workspace_id}"

    logger.debug("Validating workspace: %s", workspace_id)
    try:
        response = http.get(url, headers=client["headers"], timeout=30)
        if response.status_code == 200:
            return
        raise_for_api_error(response, f"workspace {workspace_id}", workspace_id)
    except requests.exceptions.RequestException as e:
        raise_for_request_error(
            f"workspace {workspace_id}", e, base_url=client.get("base_url")
        )


def fetch_child_workspaces(
    client: dict[str, Any] | None = None,
    config: "ExportConfig | None" = None,
    size: int = 2000,
    session: "requests.Session | None" = None,
) -> list[dict[str, Any]]:
    """Fetch child workspaces from GoodData API with pagination support.

    Args:
        client: API client dict (optional if config provided)
        config: ExportConfig instance (optional if client provided)
        size: Number of results per page
        session: Optional requests.Session for connection pooling.
            If None, uses requests module directly (no connection reuse).

    Returns:
        List of child workspace entities (may be empty if no children exist).
    """
    try:
        client = get_api_client(config=config, client=client)
        # Use session if provided, otherwise fall back to requests module
        http = session if session is not None else requests
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

            response = http.get(
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
                logger.debug(
                    "Found %d total child workspaces across %d pages",
                    len(all_workspaces),
                    page,
                )
            else:
                logger.debug("Found %d total child workspaces", len(all_workspaces))
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
                "is_valid": obj.get(
                    "areRelationsValid"
                ),  # None if missing, computed in post-export
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
                "is_valid": obj.get("areRelationsValid"),  # None in local mode
                "is_hidden": obj.get("isHidden", False),
            }
        )
    return processed_data


def process_visualizations_references(visualization_data, workspace_id=None):
    """Extract all object references from visualizations with source context.

    Accepts layout format where content is at top level:
        {"id": "x", "content": {"buckets": [...], "filters": [...]}}

    Extracts with two dimensions:
        - object_type: what is being referenced — a catalog object (metric, fact,
          attribute, label), a sort ('sort'/'sort_invalid'), or a derived measure
          with no catalog object ('derived_pop', 'derived_arithmetic',
          'derived_previous_period', 'derived_inline', 'derived_other')
        - source: where in the visualization (measure, attribute, filter,
          attributeFilterConfig, rankingFilter, measureValueFilter, sort)

    ``referenced_id`` holds the catalog object id, or is NULL when the row points
    at no catalog object (derived measures, and sorts whose target is a derived
    measure or is missing). ``local_identifier`` is the in-visualization handle
    (e.g. ``m1``/``a1``) for measure/attribute/sort rows; it is NULL for filter
    and attributeFilterConfig rows, which reference display forms directly. So for
    non-catalog rows, local_identifier — not referenced_id — identifies the row.

    Sort references are emitted with source='sort'. Sorts point at bucket items by
    localIdentifier; a sort targeting a localIdentifier that is absent from the
    buckets is dangling (the visualization fails to render) and is flagged with
    object_type='sort_invalid'. Valid sort targets get object_type='sort'.

    Derived (computed) measures — PoP, arithmetic, previous-period, inline MAQL —
    have a localIdentifier but no catalog object id; they are recorded for
    inventory with object_type='derived_*' (filter by object_type LIKE 'derived_%'
    to find visualizations using computed measures).

    This allows answering questions like:
        - "What metrics does this viz use?" (filter by object_type='metric')
        - "Is this attribute used as a filter or dimension?" (check source)
        - "Which visualizations sort by a missing localIdentifier?"
          (filter by object_type='sort_invalid')
    """
    from gooddata_export.process.common import UniqueRelationshipTracker

    tracker = UniqueRelationshipTracker(
        key_fields=[
            "visualization_id",
            "referenced_id",
            "workspace_id",
            "object_type",
            "source",
            "local_identifier",
        ]
    )

    for viz in visualization_data:
        content = viz.get("content", {})

        # Build two lookups over bucket items:
        #   bucket_local_ids: every measure/attribute localIdentifier present in
        #     the buckets — including derived measures (PoP, arithmetic, inline)
        #     that have a localIdentifier but no resolvable object id. This is the
        #     complete set of identifiers a sort is allowed to reference.
        #   local_id_map: localIdentifier → (object id, type) for items that
        #     reference a concrete object. Used to resolve rankingFilter,
        #     measureValueFilter, and sort references back to the real object.
        bucket_local_ids = set()
        local_id_map = {}
        for bucket in content.get("buckets", []):
            for item in bucket.get("items", []):
                measure = item.get("measure", {})
                local_id = measure.get("localIdentifier")
                if local_id:
                    bucket_local_ids.add(local_id)
                    measure_def = measure.get("definition", {}).get(
                        "measureDefinition", {}
                    )
                    identifier = measure_def.get("item", {}).get("identifier", {})
                    if identifier.get("id"):
                        local_id_map[local_id] = {
                            "id": identifier["id"],
                            "type": identifier.get("type", "metric"),
                        }

                attribute = item.get("attribute", {})
                attr_local_id = attribute.get("localIdentifier")
                if attr_local_id:
                    bucket_local_ids.add(attr_local_id)
                    attr_identifier = attribute.get("displayForm", {}).get(
                        "identifier", {}
                    )
                    if attr_identifier.get("id"):
                        local_id_map[attr_local_id] = {
                            "id": attr_identifier["id"],
                            "type": attr_identifier.get("type", "label"),
                        }

        # Extract references from buckets
        for bucket in content.get("buckets", []):
            for item in bucket.get("items", []):
                # Extract metric/fact/attribute references from measures
                measure = item.get("measure", {})
                definition = measure.get("definition", {})
                measure_def = definition.get("measureDefinition", {})
                identifier = measure_def.get("item", {}).get("identifier", {})
                ref_id = identifier.get("id")
                # Default to 'metric' for backwards compatibility with older data
                object_type = identifier.get("type", "metric")
                measure_local_id = measure.get("localIdentifier")

                if ref_id:
                    tracker.add(
                        {
                            "visualization_id": viz["id"],
                            "referenced_id": ref_id,
                            "workspace_id": workspace_id,
                            "object_type": object_type,
                            "source": "measure",
                            "label": measure.get("alias") or measure.get("title"),
                            "local_identifier": measure_local_id,
                        }
                    )
                elif measure_local_id:
                    # Derived/computed measure (PoP, arithmetic, previous-period,
                    # inline MAQL): no catalog object id, so referenced_id is NULL
                    # and local_identifier carries the in-viz handle. Recorded for
                    # inventory. Classify the flavor by which *Definition key is
                    # present; unknown variants fall back to 'derived_other' so a
                    # computed measure is never silently dropped if GoodData adds
                    # a new kind.
                    derived_type = next(
                        (
                            v
                            for k, v in DERIVED_MEASURE_TYPES.items()
                            if k in definition
                        ),
                        "derived_other",
                    )
                    tracker.add(
                        {
                            "visualization_id": viz["id"],
                            "referenced_id": None,
                            "workspace_id": workspace_id,
                            "object_type": derived_type,
                            "source": "measure",
                            "label": measure.get("alias") or measure.get("title"),
                            "local_identifier": measure_local_id,
                        }
                    )

                # Extract label/display form references from attributes (rows/columns)
                attribute_def = item.get("attribute", {})
                display_form = attribute_def.get("displayForm", {})
                label_id = display_form.get("identifier", {}).get("id")
                label_type = display_form.get("identifier", {}).get("type", "label")

                if label_id:
                    tracker.add(
                        {
                            "visualization_id": viz["id"],
                            "referenced_id": label_id,
                            "workspace_id": workspace_id,
                            "object_type": label_type,
                            "source": "attribute",
                            "label": attribute_def.get("alias"),
                            "local_identifier": attribute_def.get("localIdentifier"),
                        }
                    )

        # Extract label references from attributeFilterConfigs
        # These specify which label to use for displaying attribute filter values
        # In visualizations, attributeFilterConfigs is a dict keyed by UUID:
        #   {"uuid": {"displayAsLabel": {"identifier": {"id": ..., "type": ...}}}}
        # (Dashboards use a list format instead — see process_dashboards_references)
        for config in content.get("attributeFilterConfigs", {}).values():
            display_as_label = config.get("displayAsLabel", {})
            label_id = display_as_label.get("identifier", {}).get("id")
            if label_id:
                label_type = display_as_label.get("identifier", {}).get("type", "label")
                tracker.add(
                    {
                        "visualization_id": viz["id"],
                        "referenced_id": label_id,
                        "workspace_id": workspace_id,
                        "object_type": label_type,
                        "source": "attributeFilterConfig",
                        "label": None,
                        "local_identifier": None,
                    }
                )

        # Extract references from filters
        for filter_def in content.get("filters", []):
            # Handle attribute filters (positive and negative)
            for filter_type in ("negativeAttributeFilter", "positiveAttributeFilter"):
                attr_filter = filter_def.get(filter_type, {})
                display_form = attr_filter.get("displayForm", {})
                identifier = display_form.get("identifier", {})
                filter_id = identifier.get("id")
                object_type = identifier.get("type", "label")

                if filter_id:
                    tracker.add(
                        {
                            "visualization_id": viz["id"],
                            "referenced_id": filter_id,
                            "workspace_id": workspace_id,
                            "object_type": object_type,
                            "source": "filter",
                            "label": None,
                            "local_identifier": None,
                        }
                    )

            # Handle ranking filters (TOP/BOTTOM N by measure) and measure-value
            # filters (filter rows by a measure's value). Both reference a bucket
            # measure via measure.localIdentifier — same resolution path.
            for filter_key, source_label in (
                ("rankingFilter", "rankingFilter"),
                ("measureValueFilter", "measureValueFilter"),
            ):
                measure_filter = filter_def.get(filter_key, {})
                measure_local_id = measure_filter.get("measure", {}).get(
                    "localIdentifier"
                )
                if measure_local_id:
                    resolved = local_id_map.get(measure_local_id)
                    if resolved:
                        tracker.add(
                            {
                                "visualization_id": viz["id"],
                                "referenced_id": resolved["id"],
                                "workspace_id": workspace_id,
                                "object_type": resolved["type"],
                                "source": source_label,
                                "label": None,
                                "local_identifier": measure_local_id,
                            }
                        )

        # Extract sort references. Sort items reference bucket measures/attributes
        # by their localIdentifier (NOT the object id). A sort that targets a
        # localIdentifier absent from the buckets is dangling — the visualization
        # fails to render — and is flagged with object_type='sort_invalid'. It is
        # surfaced via v_visualizations_invalid_sorts and, in local mode (where we
        # compute validity ourselves), drives is_valid=0. API-mode is_valid comes
        # from GoodData's areRelationsValid and is left untouched.
        # Valid sort targets get object_type='sort'. referenced_id holds the real
        # catalog object id when the localIdentifier maps to a concrete object;
        # when it doesn't (a derived-measure target, or a dangling sort) there is
        # no catalog object, so referenced_id is NULL and local_identifier carries
        # the in-viz handle.
        def add_sort_target(target_local_id):
            if not target_local_id:
                return
            resolved = local_id_map.get(target_local_id)
            tracker.add(
                {
                    "visualization_id": viz["id"],
                    "referenced_id": resolved["id"] if resolved else None,
                    "workspace_id": workspace_id,
                    "object_type": (
                        "sort"
                        if target_local_id in bucket_local_ids
                        else "sort_invalid"
                    ),
                    "source": "sort",
                    "label": None,
                    "local_identifier": target_local_id,
                }
            )

        for sort_item in content.get("sorts", []) or []:
            measure_sort = sort_item.get("measureSortItem", {})
            for locator in measure_sort.get("locators", []):
                add_sort_target(
                    locator.get("measureLocatorItem", {}).get("measureIdentifier")
                )
                add_sort_target(
                    locator.get("attributeLocatorItem", {}).get("attributeIdentifier")
                )
            attribute_sort = sort_item.get("attributeSortItem", {})
            add_sort_target(attribute_sort.get("attributeIdentifier"))

    return tracker.get_sorted(
        sort_key=lambda x: (
            x["visualization_id"],
            x["referenced_id"] or "",  # NULL for derived/unresolved-sort rows
            x["workspace_id"],
            x["object_type"],
            x["source"],
            x["local_identifier"] or "",
        )
    )


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
                "is_valid": obj.get("areRelationsValid"),  # None in local mode
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
        logger.debug("Found %d known insights for validation", len(known_insights))

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


def process_filter_context_validate_by(data, workspace_id=None):
    """Extract validateElementsBy and filterElementsBy from attribute filters.

    Attribute filters can constrain valid elements in two ways:
    - validateElementsBy: references a metric (only non-null results shown)
    - filterElementsBy: references another dashboard filter by localIdentifier,
      optionally scoped via 'over.attributes'

    Returns list of dicts with: filter_context_id, workspace_id, filter_index,
    source (validateElementsBy|filterElementsBy), referenced_id, referenced_type,
    over_attributes (JSON array of attribute IDs, only for filterElementsBy).
    """
    results = []

    for obj in data:
        filter_context_id = obj["id"]
        content = obj.get("content", {})
        filters = content.get("filters", [])

        for filter_index, filter_obj in enumerate(filters):
            if "attributeFilter" not in filter_obj:
                continue

            attr_filter = filter_obj["attributeFilter"]

            # validateElementsBy: metric references
            for ref in attr_filter.get("validateElementsBy", []):
                identifier = ref.get("identifier", {})
                referenced_id = identifier.get("id", "")
                referenced_type = identifier.get("type", "")

                if referenced_id:
                    results.append(
                        {
                            "filter_context_id": filter_context_id,
                            "workspace_id": workspace_id,
                            "filter_index": filter_index,
                            "source": "validateElementsBy",
                            "referenced_id": referenced_id,
                            "referenced_type": referenced_type,
                            "over_attributes": None,
                        }
                    )

            # filterElementsBy: parent filter references
            for ref in attr_filter.get("filterElementsBy", []):
                filter_local_id = ref.get("filterLocalIdentifier", "")
                over_attrs = ref.get("over", {}).get("attributes", [])
                # Extract attribute IDs from identifier objects
                over_attr_ids = []
                for a in over_attrs:
                    if not a:
                        continue
                    attr_id = (
                        a.get("identifier", {}).get("id", "")
                        if isinstance(a, dict)
                        else str(a)
                    )
                    if attr_id:
                        over_attr_ids.append(attr_id)

                if filter_local_id:
                    results.append(
                        {
                            "filter_context_id": filter_context_id,
                            "workspace_id": workspace_id,
                            "filter_index": filter_index,
                            "source": "filterElementsBy",
                            "referenced_id": filter_local_id,
                            "referenced_type": "attributeFilter",
                            "over_attributes": (
                                json.dumps(over_attr_ids) if over_attr_ids else None
                            ),
                        }
                    )

    return results


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


def process_dashboards_references(
    dashboard_data: list[dict], workspace_id: str | None = None
) -> list[dict]:
    """Extract all object references from dashboards.

    Extracts references from dashboard-level and tab-level configurations:
        - attributeFilterConfigs[].displayAsLabel (object_type='label', source='attributeFilterConfig')
        - dateFilterConfig.dateDataSet (object_type='dataset', source='dateFilterConfig')
        - filterContextRef (object_type='filterContext', source='filterContextRef')

    For tabbed dashboards, each tab can have its own filter configs that override
    the dashboard-level defaults. Both top-level and tab-level configs are
    processed because tabs may inherit from top-level (UniqueRelationshipTracker
    deduplicates any overlapping references).

    Widget-level references (insight, dateDataSet) are tracked in dashboards_visualizations
    and dashboards_widget_filters tables respectively.

    Args:
        dashboard_data: List of dashboard objects with "id" and "content" fields
        workspace_id: Workspace ID to include in output

    Returns:
        List of reference dictionaries with dashboard_id, referenced_id, workspace_id,
        object_type, and source fields.
    """
    from gooddata_export.process.common import UniqueRelationshipTracker

    tracker = UniqueRelationshipTracker(
        key_fields=[
            "dashboard_id",
            "referenced_id",
            "workspace_id",
            "object_type",
            "source",
        ]
    )

    for dash in dashboard_data:
        dashboard_id = dash["id"]
        content = dash.get("content", {})
        if not content:
            continue

        # Collect config sources: top-level content + each tab
        # Unlike widget traversal (iterate_dashboard_widgets), we can't use
        # `tabs or [content]` here because tabs may not have their own
        # filterContextRef/attributeFilterConfigs — they can inherit from
        # the top-level content. We must always include top-level content.
        # UniqueRelationshipTracker deduplicates any overlapping refs.
        config_sources = [content]
        for tab in content.get("tabs", []):
            config_sources.append(tab)

        for config_source in config_sources:
            # Extract attributeFilterConfigs[].displayAsLabel references
            # These specify which label to use for displaying attribute filter values
            # In dashboards, attributeFilterConfigs is a list of config objects:
            #   [{"displayAsLabel": {"identifier": {"id": ..., "type": ...}}, ...}]
            # (Visualizations use a dict keyed by UUID — see process_visualizations_references)
            attribute_filter_configs = config_source.get("attributeFilterConfigs", [])
            for config in attribute_filter_configs:
                display_as_label = config.get("displayAsLabel", {})
                label_id = display_as_label.get("identifier", {}).get("id")
                if label_id:
                    tracker.add(
                        {
                            "dashboard_id": dashboard_id,
                            "referenced_id": label_id,
                            "workspace_id": workspace_id,
                            "object_type": "label",
                            "source": "attributeFilterConfig",
                        }
                    )

            # Extract dateFilterConfig.dateDataSet reference
            # This specifies which date dataset the dashboard's date filter applies to
            date_filter_config = config_source.get("dateFilterConfig", {})
            date_data_set = date_filter_config.get("dateDataSet", {})
            dataset_id = date_data_set.get("identifier", {}).get("id")
            if dataset_id:
                tracker.add(
                    {
                        "dashboard_id": dashboard_id,
                        "referenced_id": dataset_id,
                        "workspace_id": workspace_id,
                        "object_type": "dataset",
                        "source": "dateFilterConfig",
                    }
                )

            # Extract filterContextRef reference
            # This specifies the filter context used by the dashboard (or tab)
            filter_context_ref = config_source.get("filterContextRef", {})
            filter_context_id = filter_context_ref.get("identifier", {}).get("id")
            if filter_context_id:
                tracker.add(
                    {
                        "dashboard_id": dashboard_id,
                        "referenced_id": filter_context_id,
                        "workspace_id": workspace_id,
                        "object_type": "filterContext",
                        "source": "filterContextRef",
                    }
                )

    return tracker.get_sorted(
        sort_key=lambda x: (x["dashboard_id"], x["object_type"], x["referenced_id"])
    )


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
