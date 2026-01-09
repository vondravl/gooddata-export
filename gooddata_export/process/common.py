"""Common utilities for GoodData data processing."""

import os
from collections.abc import Callable, Hashable
from pathlib import Path
from typing import Any

# Environment flag to control debug output (off by default)
# Set DEBUG_RICH_TEXT to one of: 1/true/yes/on to enable
DEBUG_RICH_TEXT = os.environ.get("DEBUG_RICH_TEXT", "0").lower() in (
    "1",
    "true",
    "yes",
    "on",
)


class UniqueRelationshipTracker:
    """Track unique relationships using a set-based deduplication pattern.

    This consolidates the repeated pattern of:
        added_relationships = set()
        key = (field1, field2, ...)
        if key not in added_relationships:
            relationships.append({...})
            added_relationships.add(key)

    Usage:
        tracker = UniqueRelationshipTracker(key_fields=["dashboard_id", "viz_id"])
        tracker.add({"dashboard_id": "d1", "viz_id": "v1", "extra": "data"})
        tracker.add({"dashboard_id": "d1", "viz_id": "v1", "extra": "other"})  # Skipped
        result = tracker.get_sorted(sort_key=lambda x: (x["dashboard_id"], x["viz_id"]))
    """

    def __init__(
        self,
        key_fields: list[str] | None = None,
        key_func: Callable[[dict], Hashable] | None = None,
    ):
        """Initialize the tracker.

        Args:
            key_fields: List of field names to use as the deduplication key.
                        The key is a tuple of values from these fields.
            key_func: Alternative custom function to generate the key from a dict.
                      If provided, key_fields is ignored.
        """
        if key_func is None and key_fields is None:
            raise ValueError("Either key_fields or key_func must be provided")

        self._key_func = key_func or (lambda d: tuple(d.get(f) for f in key_fields))
        self._seen: set[Hashable] = set()
        self._items: list[dict[str, Any]] = []

    def add(self, item: dict[str, Any]) -> bool:
        """Add an item if its key hasn't been seen.

        Returns:
            True if the item was added, False if it was a duplicate.
        """
        key = self._key_func(item)
        if key not in self._seen:
            self._items.append(item)
            self._seen.add(key)
            return True
        return False

    def get_items(self) -> list[dict[str, Any]]:
        """Get all unique items in insertion order."""
        return self._items

    def get_sorted(
        self, sort_key: Callable[[dict], Any] | None = None
    ) -> list[dict[str, Any]]:
        """Get all unique items, optionally sorted.

        Args:
            sort_key: Optional function to generate sort key from each item.
        """
        if sort_key is None:
            return self._items
        return sorted(self._items, key=sort_key)

    def __len__(self) -> int:
        return len(self._items)


def sort_tags(tags):
    """Sort tags alphabetically if they are a list"""
    if isinstance(tags, list):
        return sorted(tags)
    return tags


def import_time_iso():
    """Get current time in ISO format for debugging purposes"""
    from datetime import datetime

    return datetime.now().isoformat()


def get_debug_output_dir():
    """Get the debug output directory path."""
    return Path(__file__).parent.parent.parent / "debug_output"
