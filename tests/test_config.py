"""Tests for gooddata_export/config.py ExportConfig class."""

import os
from unittest.mock import patch

import pytest

from gooddata_export.config import ExportConfig


class TestExportConfigDefaults:
    """Tests for ExportConfig default values."""

    def test_defaults_without_env(self):
        """Default values when no environment variables are set."""
        with patch.dict(os.environ, {}, clear=True):
            config = ExportConfig(load_from_env=False)

            assert config.BASE_URL is None
            assert config.WORKSPACE_ID is None
            assert config.BEARER_TOKEN is None
            assert config.INCLUDE_CHILD_WORKSPACES is False
            assert config.ENABLE_POST_EXPORT is True  # default is True
            assert config.ENABLE_RICH_TEXT_EXTRACTION is True  # default is True
            assert config.MAX_PARALLEL_WORKSPACES == 5  # default

    def test_explicit_values_override_defaults(self):
        """Explicit constructor args override defaults."""
        config = ExportConfig(
            base_url="https://test.example.com",
            workspace_id="test-workspace",
            bearer_token="test-token",
            include_child_workspaces=True,
            enable_post_export=False,
            enable_rich_text_extraction=False,
            max_parallel_workspaces=10,
            load_from_env=False,
        )

        assert config.BASE_URL == "https://test.example.com"
        assert config.WORKSPACE_ID == "test-workspace"
        assert config.BEARER_TOKEN == "test-token"
        assert config.INCLUDE_CHILD_WORKSPACES is True
        assert config.ENABLE_POST_EXPORT is False
        assert config.ENABLE_RICH_TEXT_EXTRACTION is False
        assert config.MAX_PARALLEL_WORKSPACES == 10


class TestExportConfigWorkspaceId:
    """Tests for WORKSPACE_ID property."""

    def test_workspace_id_getter(self):
        """WORKSPACE_ID property returns correct value."""
        config = ExportConfig(workspace_id="my-workspace", load_from_env=False)
        assert config.WORKSPACE_ID == "my-workspace"

    def test_workspace_id_setter(self):
        """WORKSPACE_ID can be changed via setter."""
        config = ExportConfig(workspace_id="initial", load_from_env=False)
        assert config.WORKSPACE_ID == "initial"

        config.WORKSPACE_ID = "changed"
        assert config.WORKSPACE_ID == "changed"


class TestExportConfigChildWorkspaces:
    """Tests for INCLUDE_CHILD_WORKSPACES parsing."""

    @pytest.mark.parametrize(
        "env_value,expected",
        [
            ("true", True),
            ("True", True),
            ("TRUE", True),
            ("1", True),
            ("yes", True),
            ("on", True),
            ("false", False),
            ("False", False),
            ("0", False),
            ("no", False),
            ("", False),
            ("random", False),
        ],
    )
    def test_boolean_parsing_from_env(self, env_value, expected):
        """INCLUDE_CHILD_WORKSPACES parses boolean strings correctly."""
        with patch.dict(os.environ, {"INCLUDE_CHILD_WORKSPACES": env_value}):
            config = ExportConfig(load_from_env=True)
            assert config.INCLUDE_CHILD_WORKSPACES is expected

    def test_explicit_true_overrides_env(self):
        """Explicit True overrides environment variable."""
        with patch.dict(os.environ, {"INCLUDE_CHILD_WORKSPACES": "false"}):
            config = ExportConfig(include_child_workspaces=True, load_from_env=True)
            assert config.INCLUDE_CHILD_WORKSPACES is True


class TestExportConfigRichTextExtraction:
    """Tests for ENABLE_RICH_TEXT_EXTRACTION property."""

    def test_default_is_true(self):
        """Rich text extraction is enabled by default."""
        config = ExportConfig(load_from_env=False)
        assert config.ENABLE_RICH_TEXT_EXTRACTION is True

    def test_explicit_false(self):
        """Can explicitly disable rich text extraction."""
        config = ExportConfig(enable_rich_text_extraction=False, load_from_env=False)
        assert config.ENABLE_RICH_TEXT_EXTRACTION is False

    def test_setter_works(self):
        """ENABLE_RICH_TEXT_EXTRACTION setter works."""
        config = ExportConfig(load_from_env=False)
        assert config.ENABLE_RICH_TEXT_EXTRACTION is True

        config.ENABLE_RICH_TEXT_EXTRACTION = False
        assert config.ENABLE_RICH_TEXT_EXTRACTION is False


class TestExportConfigChildDataTypes:
    """Tests for CHILD_WORKSPACE_DATA_TYPES parsing."""

    def test_explicit_list(self):
        """Explicit list of data types is used."""
        config = ExportConfig(
            child_workspace_data_types=["metrics", "dashboards"],
            load_from_env=False,
        )
        assert config.CHILD_WORKSPACE_DATA_TYPES == ["metrics", "dashboards"]

    def test_env_parsing(self):
        """Data types from environment are parsed and lowercased."""
        with patch.dict(
            os.environ,
            {"CHILD_WORKSPACE_DATA_TYPES": "Metrics, Dashboards , Visualizations"},
        ):
            config = ExportConfig(load_from_env=True)
            assert config.CHILD_WORKSPACE_DATA_TYPES == [
                "metrics",
                "dashboards",
                "visualizations",
            ]

    def test_empty_default(self):
        """Default is empty list when not specified."""
        with patch.dict(os.environ, {}, clear=True):
            config = ExportConfig(load_from_env=False)
            assert config.CHILD_WORKSPACE_DATA_TYPES == []


class TestExportConfigWithRichTextDisabled:
    """Tests for with_rich_text_disabled() method."""

    def test_returns_new_instance(self):
        """with_rich_text_disabled() returns a new config instance."""
        config = ExportConfig(load_from_env=False)
        new_config = config.with_rich_text_disabled()

        assert new_config is not config

    def test_new_instance_has_rich_text_disabled(self):
        """Returned config has ENABLE_RICH_TEXT_EXTRACTION set to False."""
        config = ExportConfig(enable_rich_text_extraction=True, load_from_env=False)
        assert config.ENABLE_RICH_TEXT_EXTRACTION is True

        new_config = config.with_rich_text_disabled()

        assert new_config.ENABLE_RICH_TEXT_EXTRACTION is False

    def test_original_config_unchanged(self):
        """Original config is not mutated by with_rich_text_disabled()."""
        config = ExportConfig(enable_rich_text_extraction=True, load_from_env=False)

        config.with_rich_text_disabled()

        assert config.ENABLE_RICH_TEXT_EXTRACTION is True

    def test_works_when_already_disabled(self):
        """Method works correctly when rich text is already disabled."""
        config = ExportConfig(enable_rich_text_extraction=False, load_from_env=False)
        assert config.ENABLE_RICH_TEXT_EXTRACTION is False

        new_config = config.with_rich_text_disabled()

        assert new_config.ENABLE_RICH_TEXT_EXTRACTION is False
        assert new_config is not config

    def test_preserves_other_config_properties(self):
        """Other config properties are preserved in the copy."""
        config = ExportConfig(
            base_url="https://test.example.com",
            workspace_id="test-workspace",
            bearer_token="test-token",
            include_child_workspaces=True,
            enable_post_export=False,
            enable_rich_text_extraction=True,
            max_parallel_workspaces=10,
            load_from_env=False,
        )

        new_config = config.with_rich_text_disabled()

        # Rich text should be disabled
        assert new_config.ENABLE_RICH_TEXT_EXTRACTION is False

        # All other properties should be preserved
        assert new_config.BASE_URL == "https://test.example.com"
        assert new_config.WORKSPACE_ID == "test-workspace"
        assert new_config.BEARER_TOKEN == "test-token"
        assert new_config.INCLUDE_CHILD_WORKSPACES is True
        assert new_config.ENABLE_POST_EXPORT is False
        assert new_config.MAX_PARALLEL_WORKSPACES == 10
