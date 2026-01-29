"""Tests for CLI prompt functions."""

import os
from unittest.mock import patch

from gooddata_export.cli.prompts import (
    CI_ENVIRONMENT_VARS,
    is_interactive,
    prompt_checkbox_selection,
    prompt_yes_no,
)


class TestIsInteractive:
    """Tests for is_interactive() function."""

    def test_returns_false_when_not_tty(self):
        """Non-TTY stdin should return False."""
        with patch("sys.stdin.isatty", return_value=False):
            assert is_interactive() is False

    def test_returns_false_with_ci_env_vars(self):
        """CI environment variables should return False."""
        with patch("sys.stdin.isatty", return_value=True):
            for var in CI_ENVIRONMENT_VARS:
                with patch.dict(os.environ, {var: "true"}):
                    assert is_interactive() is False, (
                        f"{var} should disable interactive"
                    )

    def test_returns_true_when_tty_and_no_ci(self):
        """TTY without CI vars should return True."""
        # Clear all CI vars from environment
        clean_env = {
            k: v for k, v in os.environ.items() if k not in CI_ENVIRONMENT_VARS
        }
        with (
            patch("sys.stdin.isatty", return_value=True),
            patch.dict(os.environ, clean_env, clear=True),
        ):
            assert is_interactive() is True


class TestPromptCheckboxSelection:
    """Tests for prompt_checkbox_selection() function."""

    def test_skip_prompt_returns_all_when_default_all(self):
        """skip_prompt=True with default_all=True returns all options."""
        options = ("a", "b", "c")
        result = prompt_checkbox_selection(options, skip_prompt=True, default_all=True)
        assert result == ["a", "b", "c"]

    def test_skip_prompt_returns_empty_when_default_none(self):
        """skip_prompt=True with default_all=False returns empty list."""
        options = ("a", "b", "c")
        result = prompt_checkbox_selection(options, skip_prompt=True, default_all=False)
        assert result == []

    def test_non_interactive_returns_all_when_default_all(self):
        """Non-interactive mode with default_all=True returns all options."""
        options = ("metrics", "dashboards")
        with patch("gooddata_export.cli.prompts.is_interactive", return_value=False):
            result = prompt_checkbox_selection(options, default_all=True)
            assert result == ["metrics", "dashboards"]

    def test_non_interactive_returns_empty_when_default_none(self):
        """Non-interactive mode with default_all=False returns empty list."""
        options = ("metrics", "dashboards")
        with patch("gooddata_export.cli.prompts.is_interactive", return_value=False):
            result = prompt_checkbox_selection(options, default_all=False)
            assert result == []

    def test_accepts_list_input(self):
        """Should accept list as well as tuple."""
        options = ["a", "b", "c"]
        result = prompt_checkbox_selection(options, skip_prompt=True, default_all=True)
        assert result == ["a", "b", "c"]

    def test_user_confirms_default_all(self):
        """User pressing Enter keeps all selected."""
        options = ("a", "b", "c")
        with (
            patch("gooddata_export.cli.prompts.is_interactive", return_value=True),
            patch("builtins.input", return_value=""),
            patch("builtins.print"),
        ):
            result = prompt_checkbox_selection(options, default_all=True)
            assert result == ["a", "b", "c"]

    def test_user_selects_all(self):
        """User entering 'a' selects all options."""
        options = ("a", "b", "c")
        inputs = iter(["a", ""])  # Select all, then confirm
        with (
            patch("gooddata_export.cli.prompts.is_interactive", return_value=True),
            patch("builtins.input", lambda _: next(inputs)),
            patch("builtins.print"),
        ):
            result = prompt_checkbox_selection(options, default_all=False)
            assert result == ["a", "b", "c"]

    def test_user_selects_none(self):
        """User entering 'n' deselects all options."""
        options = ("a", "b", "c")
        inputs = iter(["n", ""])  # Select none, then confirm
        with (
            patch("gooddata_export.cli.prompts.is_interactive", return_value=True),
            patch("builtins.input", lambda _: next(inputs)),
            patch("builtins.print"),
        ):
            result = prompt_checkbox_selection(options, default_all=True)
            assert result == []

    def test_user_toggles_individual_options(self):
        """User can toggle individual options by number."""
        options = ("a", "b", "c")
        inputs = iter(["1 3", ""])  # Toggle 1 and 3, then confirm
        with (
            patch("gooddata_export.cli.prompts.is_interactive", return_value=True),
            patch("builtins.input", lambda _: next(inputs)),
            patch("builtins.print"),
        ):
            # Starting with all selected, toggle 1 and 3 off
            result = prompt_checkbox_selection(options, default_all=True)
            assert result == ["b"]  # Only b remains

    def test_eof_returns_current_selection(self):
        """EOFError returns current selection state."""
        options = ("a", "b", "c")
        with (
            patch("gooddata_export.cli.prompts.is_interactive", return_value=True),
            patch("builtins.input", side_effect=EOFError),
            patch("builtins.print"),
        ):
            result = prompt_checkbox_selection(options, default_all=True)
            assert result == ["a", "b", "c"]

    def test_keyboard_interrupt_returns_current_selection(self):
        """KeyboardInterrupt returns current selection state."""
        options = ("a", "b")
        with (
            patch("gooddata_export.cli.prompts.is_interactive", return_value=True),
            patch("builtins.input", side_effect=KeyboardInterrupt),
            patch("builtins.print"),
        ):
            result = prompt_checkbox_selection(options, default_all=False)
            assert result == []

    def test_invalid_input_is_ignored(self):
        """Invalid input (non-numbers, out of range) is ignored."""
        options = ("a", "b")
        inputs = iter(["xyz", "99", ""])  # Invalid inputs then confirm
        with (
            patch("gooddata_export.cli.prompts.is_interactive", return_value=True),
            patch("builtins.input", lambda _: next(inputs)),
            patch("builtins.print"),
        ):
            result = prompt_checkbox_selection(options, default_all=True)
            assert result == ["a", "b"]  # No change from defaults


class TestPromptYesNo:
    """Tests for prompt_yes_no() function."""

    def test_skip_prompt_returns_default_true(self):
        """skip_prompt=True returns default value (True)."""
        result = prompt_yes_no("Test?", default=True, skip_prompt=True)
        assert result is True

    def test_skip_prompt_returns_default_false(self):
        """skip_prompt=True returns default value (False)."""
        result = prompt_yes_no("Test?", default=False, skip_prompt=True)
        assert result is False

    def test_non_interactive_returns_default(self):
        """Non-interactive mode returns default."""
        with patch("gooddata_export.cli.prompts.is_interactive", return_value=False):
            assert prompt_yes_no("Test?", default=True) is True
            assert prompt_yes_no("Test?", default=False) is False

    def test_user_enters_yes(self):
        """User entering 'y' or 'yes' returns True."""
        with patch("gooddata_export.cli.prompts.is_interactive", return_value=True):
            for response in ["y", "Y", "yes", "YES", "Yes"]:
                with patch("builtins.input", return_value=response):
                    assert prompt_yes_no("Test?", default=False) is True

    def test_user_enters_no(self):
        """User entering 'n' or 'no' returns False."""
        with patch("gooddata_export.cli.prompts.is_interactive", return_value=True):
            for response in ["n", "N", "no", "NO", "No"]:
                with patch("builtins.input", return_value=response):
                    assert prompt_yes_no("Test?", default=True) is False

    def test_user_enters_empty_returns_default(self):
        """User pressing Enter returns default value."""
        with (
            patch("gooddata_export.cli.prompts.is_interactive", return_value=True),
            patch("builtins.input", return_value=""),
        ):
            assert prompt_yes_no("Test?", default=True) is True
            assert prompt_yes_no("Test?", default=False) is False

    def test_eof_returns_default(self):
        """EOFError returns default value."""
        with (
            patch("gooddata_export.cli.prompts.is_interactive", return_value=True),
            patch("builtins.input", side_effect=EOFError),
            patch("builtins.print"),
        ):
            assert prompt_yes_no("Test?", default=True) is True

    def test_keyboard_interrupt_returns_default(self):
        """KeyboardInterrupt returns default value."""
        with (
            patch("gooddata_export.cli.prompts.is_interactive", return_value=True),
            patch("builtins.input", side_effect=KeyboardInterrupt),
            patch("builtins.print"),
        ):
            assert prompt_yes_no("Test?", default=False) is False
