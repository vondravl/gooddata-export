"""
GoodData Export - Command Line Interface

This package provides the CLI entry point and reusable prompt functions.

Public API:
    main: CLI entry point
    is_interactive: Check if running in interactive terminal
    prompt_checkbox_selection: Multi-select checkbox prompt
    prompt_yes_no: Yes/no confirmation prompt
"""

from gooddata_export.cli.main import main
from gooddata_export.cli.prompts import (
    is_interactive,
    prompt_checkbox_selection,
    prompt_yes_no,
)

__all__ = [
    "main",
    "is_interactive",
    "prompt_checkbox_selection",
    "prompt_yes_no",
]
