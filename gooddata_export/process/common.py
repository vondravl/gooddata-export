"""Common utilities for GoodData data processing."""

import os
from pathlib import Path

# Environment flag to control debug output (off by default)
# Set DEBUG_RICH_TEXT to one of: 1/true/yes/on to enable
DEBUG_RICH_TEXT = os.environ.get("DEBUG_RICH_TEXT", "0").lower() in (
    "1",
    "true",
    "yes",
    "on",
)


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
