"""Dashboard layout traversal utilities.

Provides generators for iterating through dashboard layout structures,
supporting both tabbed dashboards (content.tabs[]) and legacy non-tabbed
dashboards (content.layout.sections).
"""

from collections.abc import Generator


def iterate_dashboard_widgets(
    dashboards: list[dict],
) -> Generator[tuple[str, str | None, dict], None, None]:
    """Iterate through all widgets in dashboard layouts.

    Yields (dashboard_id, tab_id, widget) tuples for each widget found.
    Handles both tabbed and legacy dashboard formats, and recursively
    processes nested IDashboardLayout widgets.

    Args:
        dashboards: List of dashboard objects with "id" and "content" fields

    Yields:
        Tuples of (dashboard_id, tab_id, widget) where:
        - dashboard_id: The dashboard's ID
        - tab_id: Tab localIdentifier (None for legacy non-tabbed dashboards)
        - widget: The widget dict from the layout item
    """
    for dash in dashboards:
        content = dash.get("content", {})
        if not content:
            continue

        dashboard_id = dash["id"]

        # New format: tabbed dashboards (content.tabs[])
        tabs = content.get("tabs", [])
        if tabs:
            for tab in tabs:
                tab_id = tab.get("localIdentifier")
                tab_layout = tab.get("layout", {})
                tab_sections = tab_layout.get("sections", [])
                yield from _iterate_sections_widgets(tab_sections, dashboard_id, tab_id)
        else:
            # Legacy format: non-tabbed dashboards (content.layout.sections)
            layout = content.get("layout", {})
            sections = layout.get("sections", [])
            yield from _iterate_sections_widgets(sections, dashboard_id, tab_id=None)


def _iterate_sections_widgets(
    sections: list[dict], dashboard_id: str, tab_id: str | None
) -> Generator[tuple[str, str | None, dict], None, None]:
    """Iterate through widgets in layout sections.

    Args:
        sections: List of section dicts containing "items"
        dashboard_id: The dashboard's ID
        tab_id: Tab localIdentifier (None for legacy dashboards)

    Yields:
        Tuples of (dashboard_id, tab_id, widget)
    """
    for section in sections:
        items = section.get("items", [])
        yield from _iterate_items_widgets(items, dashboard_id, tab_id)


def _iterate_items_widgets(
    items: list[dict], dashboard_id: str, tab_id: str | None
) -> Generator[tuple[str, str | None, dict], None, None]:
    """Recursively iterate through widgets in layout items.

    Handles nested IDashboardLayout widgets by recursing into their sections.

    Args:
        items: List of item dicts containing "widget"
        dashboard_id: The dashboard's ID
        tab_id: Tab localIdentifier (None for legacy dashboards)

    Yields:
        Tuples of (dashboard_id, tab_id, widget)
    """
    for item in items:
        widget = item.get("widget", {})
        if not widget:
            continue

        # Yield this widget
        yield (dashboard_id, tab_id, widget)

        # Recursively handle nested IDashboardLayout widgets
        if widget.get("type") == "IDashboardLayout":
            nested_sections = widget.get("sections", [])
            yield from _iterate_sections_widgets(nested_sections, dashboard_id, tab_id)
