"""
Reusable CLI prompt functions for interactive user input.

This module provides interactive prompts that can be imported by other projects
to avoid code duplication. All functions are designed to be CI-friendly with
automatic fallback to defaults in non-interactive environments.

Example usage:
    from gooddata_export.cli.prompts import (
        is_interactive,
        prompt_checkbox_selection,
        prompt_yes_no,
    )

    if is_interactive():
        selected = prompt_checkbox_selection(
            options=["metrics", "dashboards", "visualizations"],
            message="Select data types to fetch:",
            default_all=True,
        )
"""

import os
import sys

# CI environment variables that indicate non-interactive mode
CI_ENVIRONMENT_VARS = (
    "CI",
    "GITHUB_ACTIONS",
    "GITLAB_CI",
    "JENKINS_URL",
    "CIRCLECI",
    "TRAVIS",
    "BUILDKITE",
    "TEAMCITY_VERSION",
    "TF_BUILD",  # Azure DevOps
)


def is_interactive() -> bool:
    """Check if the current environment supports interactive prompts.

    Returns True if:
    - stdin is a TTY (interactive terminal)
    - No CI environment variables are set

    Returns:
        bool: True if interactive prompts are supported
    """
    # Check if stdin is a TTY
    if not sys.stdin.isatty():
        return False

    # Check for CI environment variables
    for var in CI_ENVIRONMENT_VARS:
        if os.environ.get(var):
            return False

    return True


def prompt_checkbox_selection(
    options: tuple[str, ...] | list[str],
    message: str = "Select options:",
    default_all: bool = True,
    skip_prompt: bool = False,
) -> list[str]:
    """Display a multi-select checkbox prompt for user selection.

    Provides a simple text-based interface where users can:
    - Enter numbers to toggle individual options (e.g., "1 3")
    - Enter 'a' to select all options
    - Enter 'n' to select none
    - Press Enter to confirm selection

    Args:
        options: Tuple or list of options to display
        message: Prompt message to display
        default_all: If True, all options start selected; if False, none selected
        skip_prompt: If True, returns default selection without prompting (for testing)

    Returns:
        list[str]: List of selected option values

    Example:
        >>> selected = prompt_checkbox_selection(
        ...     options=("metrics", "dashboards", "visualizations"),
        ...     message="Select data types:",
        ...     default_all=True,
        ... )
        Select data types:
        [X] 1. metrics
        [X] 2. dashboards
        [X] 3. visualizations

        Toggle: numbers (1 3), 'a'=all, 'n'=none, Enter=confirm
        >
    """
    options_list = list(options)

    # Handle skip_prompt or non-interactive mode
    if skip_prompt or not is_interactive():
        return options_list if default_all else []

    # Initialize selection state
    selected = [default_all] * len(options_list)

    while True:
        # Display current selection state
        print(f"\n{message}")
        for i, option in enumerate(options_list, 1):
            checkbox = "[X]" if selected[i - 1] else "[ ]"
            print(f"  {checkbox} {i}. {option}")

        print("\nToggle: numbers (1 3), 'a'=all, 'n'=none, Enter=confirm")

        try:
            user_input = input("> ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            # Return current selection on Ctrl+C or EOF
            print()
            return [opt for i, opt in enumerate(options_list) if selected[i]]

        if user_input == "":
            # Confirm selection
            break
        elif user_input == "a":
            # Select all
            selected = [True] * len(options_list)
        elif user_input == "n":
            # Select none
            selected = [False] * len(options_list)
        else:
            # Toggle individual options by number
            for part in user_input.split():
                try:
                    idx = int(part) - 1
                    if 0 <= idx < len(options_list):
                        selected[idx] = not selected[idx]
                except ValueError:
                    # Ignore invalid input
                    pass

    return [opt for i, opt in enumerate(options_list) if selected[i]]


def prompt_yes_no(
    message: str,
    default: bool = True,
    skip_prompt: bool = False,
) -> bool:
    """Display a yes/no confirmation prompt.

    Args:
        message: Question to display
        default: Default value if user presses Enter
        skip_prompt: If True, returns default without prompting (for testing)

    Returns:
        bool: True for yes, False for no

    Example:
        >>> if prompt_yes_no("Include child workspaces?", default=False):
        ...     print("Including children")
        Include child workspaces? [y/N]:
    """
    if skip_prompt or not is_interactive():
        return default

    hint = "[Y/n]" if default else "[y/N]"
    prompt_text = f"{message} {hint}: "

    while True:
        try:
            user_input = input(prompt_text).strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            return default

        if user_input == "":
            return default
        elif user_input in ("y", "yes"):
            return True
        elif user_input in ("n", "no"):
            return False
        # Invalid input, prompt again
