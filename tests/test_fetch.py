"""Tests for gooddata_export/export/fetch.py data fetching functions."""

import logging
from unittest.mock import MagicMock, patch

from gooddata_export.export.fetch import fetch_all_data_parallel


class TestFetchAllDataParallelErrorHandling:
    """Tests for error handling in fetch_all_data_parallel()."""

    def _create_mock_config(self):
        """Create a mock config for testing."""
        config = MagicMock()
        config.BASE_URL = "https://test.gooddata.com"
        config.WORKSPACE_ID = "test-workspace"
        config.BEARER_TOKEN = "test-token"
        return config

    @patch("gooddata_export.export.fetch.get_api_client")
    @patch("gooddata_export.export.fetch.fetch_ldm")
    @patch("gooddata_export.export.fetch.fetch_child_workspaces")
    @patch("gooddata_export.export.fetch.fetch_users_and_user_groups")
    @patch("gooddata_export.export.fetch.fetch_analytics_model")
    def test_single_fetch_error_does_not_stop_others(
        self,
        mock_analytics_model,
        mock_users,
        mock_child_workspaces,
        mock_ldm,
        mock_get_client,
    ):
        """When one fetch fails, other fetches still complete."""
        mock_get_client.return_value = {"base_url": "https://test.com", "headers": {}}
        mock_ldm.side_effect = RuntimeError("LDM API error")
        mock_child_workspaces.return_value = [{"id": "child-1"}]
        mock_users.return_value = {"users": [], "user_groups": []}
        mock_analytics_model.return_value = {
            "analytics": {"metrics": [{"id": "m1"}], "analyticalDashboards": []}
        }

        config = self._create_mock_config()
        result = fetch_all_data_parallel(config)

        # ldm should be None due to error
        assert result["ldm"] is None

        # Other fetches should succeed
        assert result["child_workspaces"] == [{"id": "child-1"}]
        assert result["users_and_user_groups"] == {"users": [], "user_groups": []}
        assert result["metrics"] == [{"id": "m1"}]

    @patch("gooddata_export.export.fetch.get_api_client")
    @patch("gooddata_export.export.fetch.fetch_ldm")
    @patch("gooddata_export.export.fetch.fetch_child_workspaces")
    @patch("gooddata_export.export.fetch.fetch_users_and_user_groups")
    @patch("gooddata_export.export.fetch.fetch_analytics_model")
    def test_multiple_fetch_errors(
        self,
        mock_analytics_model,
        mock_users,
        mock_child_workspaces,
        mock_ldm,
        mock_get_client,
    ):
        """When multiple fetches fail, errors are handled gracefully."""
        mock_get_client.return_value = {"base_url": "https://test.com", "headers": {}}
        mock_ldm.side_effect = RuntimeError("LDM error")
        mock_child_workspaces.side_effect = RuntimeError("Child workspaces error")
        mock_users.return_value = {"users": [], "user_groups": []}
        mock_analytics_model.return_value = {"analytics": {"metrics": []}}

        config = self._create_mock_config()
        result = fetch_all_data_parallel(config)

        # Failed fetches should be None
        assert result["ldm"] is None
        assert result["child_workspaces"] is None

        # Successful fetches should have data
        assert result["users_and_user_groups"] == {"users": [], "user_groups": []}

    @patch("gooddata_export.export.fetch.get_api_client")
    @patch("gooddata_export.export.fetch.fetch_ldm")
    @patch("gooddata_export.export.fetch.fetch_child_workspaces")
    @patch("gooddata_export.export.fetch.fetch_users_and_user_groups")
    @patch("gooddata_export.export.fetch.fetch_analytics_model")
    def test_all_fetches_fail(
        self,
        mock_analytics_model,
        mock_users,
        mock_child_workspaces,
        mock_ldm,
        mock_get_client,
    ):
        """When all fetches fail, returns structure with None/empty values."""
        mock_get_client.return_value = {"base_url": "https://test.com", "headers": {}}
        mock_ldm.side_effect = RuntimeError("LDM error")
        mock_child_workspaces.side_effect = RuntimeError("Child workspaces error")
        mock_users.side_effect = RuntimeError("Users error")
        mock_analytics_model.side_effect = RuntimeError("Analytics error")

        config = self._create_mock_config()
        result = fetch_all_data_parallel(config)

        # All should be None or empty
        assert result["ldm"] is None
        assert result["child_workspaces"] is None
        assert result["users_and_user_groups"] is None
        # analytics_model failure means empty lists for derived values
        assert result["metrics"] == []
        assert result["dashboards"] == []
        assert result["visualizations"] == []

    @patch("gooddata_export.export.fetch.get_api_client")
    @patch("gooddata_export.export.fetch.fetch_ldm")
    @patch("gooddata_export.export.fetch.fetch_child_workspaces")
    @patch("gooddata_export.export.fetch.fetch_users_and_user_groups")
    @patch("gooddata_export.export.fetch.fetch_analytics_model")
    def test_error_is_logged(
        self,
        mock_analytics_model,
        mock_users,
        mock_child_workspaces,
        mock_ldm,
        mock_get_client,
        caplog,
    ):
        """Fetch errors are logged with error level."""
        mock_get_client.return_value = {"base_url": "https://test.com", "headers": {}}
        mock_ldm.side_effect = RuntimeError("LDM API connection failed")
        mock_child_workspaces.return_value = []
        mock_users.return_value = {}
        mock_analytics_model.return_value = {"analytics": {}}

        config = self._create_mock_config()

        with caplog.at_level(logging.ERROR):
            fetch_all_data_parallel(config)

        # Verify error was logged
        assert any("Error fetching ldm" in record.message for record in caplog.records)
        assert any(
            "LDM API connection failed" in record.message for record in caplog.records
        )

    @patch("gooddata_export.export.fetch.get_api_client")
    @patch("gooddata_export.export.fetch.fetch_ldm")
    @patch("gooddata_export.export.fetch.fetch_child_workspaces")
    @patch("gooddata_export.export.fetch.fetch_users_and_user_groups")
    @patch("gooddata_export.export.fetch.fetch_analytics_model")
    def test_result_always_has_expected_keys(
        self,
        mock_analytics_model,
        mock_users,
        mock_child_workspaces,
        mock_ldm,
        mock_get_client,
    ):
        """Result dict always contains all expected keys regardless of errors."""
        mock_get_client.return_value = {"base_url": "https://test.com", "headers": {}}
        mock_ldm.side_effect = RuntimeError("Error")
        mock_child_workspaces.side_effect = RuntimeError("Error")
        mock_users.side_effect = RuntimeError("Error")
        mock_analytics_model.side_effect = RuntimeError("Error")

        config = self._create_mock_config()
        result = fetch_all_data_parallel(config)

        expected_keys = [
            "metrics",
            "dashboards",
            "visualizations",
            "filter_contexts",
            "plugins",
            "ldm",
            "child_workspaces",
            "users_and_user_groups",
            "analytics_model",
        ]

        for key in expected_keys:
            assert key in result, f"Expected key '{key}' missing from result"

    @patch("gooddata_export.export.fetch.get_api_client")
    @patch("gooddata_export.export.fetch.fetch_ldm")
    @patch("gooddata_export.export.fetch.fetch_child_workspaces")
    @patch("gooddata_export.export.fetch.fetch_users_and_user_groups")
    @patch("gooddata_export.export.fetch.fetch_analytics_model")
    def test_analytics_model_error_returns_empty_lists_for_derived_data(
        self,
        mock_analytics_model,
        mock_users,
        mock_child_workspaces,
        mock_ldm,
        mock_get_client,
    ):
        """When analytics_model fails, derived data returns empty lists."""
        mock_get_client.return_value = {"base_url": "https://test.com", "headers": {}}
        mock_ldm.return_value = {"datasets": []}
        mock_child_workspaces.return_value = []
        mock_users.return_value = {}
        mock_analytics_model.side_effect = RuntimeError("Analytics API error")

        config = self._create_mock_config()
        result = fetch_all_data_parallel(config)

        # analytics_model failure means derived values default to empty lists
        assert result["metrics"] == []
        assert result["dashboards"] == []
        assert result["visualizations"] == []
        assert result["filter_contexts"] == []
        assert result["plugins"] == []
        # analytics_model itself should be empty dict (from None)
        assert result["analytics_model"] is None or result["analytics_model"] == {}
