"""Tests for local mode functionality (layout_json parameter)."""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from gooddata_export.constants import LOCAL_MODE_STALE_TABLES
from gooddata_export.export.utils import truncate_tables_for_local_mode


@pytest.fixture
def mock_config():
    """Create a mock config for testing."""
    config = MagicMock()
    config.WORKSPACE_ID = "test-workspace"
    config.BASE_URL = "https://test.gooddata.com"
    config.DEBUG_WORKSPACE_PROCESSING = False
    config.INCLUDE_CHILD_WORKSPACES = False
    return config


class TestTruncateTablesForLocalMode:
    """Tests for the truncate_tables_for_local_mode function."""

    def test_truncates_existing_tables(self, tmp_path):
        """Truncates tables that exist in the database."""
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)

        # Create tables with data
        conn.execute("CREATE TABLE users (id TEXT, name TEXT)")
        conn.execute("INSERT INTO users VALUES ('u1', 'User 1')")
        conn.execute("CREATE TABLE plugins (id TEXT, title TEXT)")
        conn.execute("INSERT INTO plugins VALUES ('p1', 'Plugin 1')")
        conn.execute("INSERT INTO plugins VALUES ('p2', 'Plugin 2')")
        conn.commit()

        # Verify data exists
        assert conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM plugins").fetchone()[0] == 2
        conn.close()

        # Truncate
        truncate_tables_for_local_mode(db_path)

        # Verify tables are empty
        conn = sqlite3.connect(db_path)
        assert conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM plugins").fetchone()[0] == 0
        conn.close()

    def test_skips_nonexistent_tables(self, tmp_path):
        """Skips tables that don't exist without error."""
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)

        # Create only one of the stale tables
        conn.execute("CREATE TABLE users (id TEXT)")
        conn.execute("INSERT INTO users VALUES ('u1')")
        conn.commit()
        conn.close()

        # Should not raise even though other tables don't exist
        truncate_tables_for_local_mode(db_path)

        # Verify users was truncated
        conn = sqlite3.connect(db_path)
        assert conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0
        conn.close()

    def test_no_op_when_database_does_not_exist(self, tmp_path):
        """Does nothing when database doesn't exist yet."""
        db_path = tmp_path / "nonexistent.db"
        assert not db_path.exists()

        # Should not raise or create the database
        truncate_tables_for_local_mode(db_path)

        assert not db_path.exists()

    def test_preserves_other_tables(self, tmp_path):
        """Does not truncate tables not in LOCAL_MODE_STALE_TABLES."""
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)

        # Create a table that should NOT be truncated
        conn.execute("CREATE TABLE metrics (id TEXT, title TEXT)")
        conn.execute("INSERT INTO metrics VALUES ('m1', 'Metric 1')")
        # And a stale table
        conn.execute("CREATE TABLE users (id TEXT)")
        conn.execute("INSERT INTO users VALUES ('u1')")
        conn.commit()
        conn.close()

        truncate_tables_for_local_mode(db_path)

        conn = sqlite3.connect(db_path)
        # metrics should be preserved
        assert conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0] == 1
        # users should be truncated
        assert conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0
        conn.close()

    def test_handles_truncation_error_gracefully(self, tmp_path, caplog):
        """Handles errors gracefully and logs warning."""
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE users (id TEXT)")
        conn.execute("INSERT INTO users VALUES ('u1')")
        conn.commit()
        conn.close()

        # Make the file read-only to cause an error
        db_path.chmod(0o444)

        try:
            # Should not raise, just log warning
            truncate_tables_for_local_mode(db_path)
            assert any(
                "Could not truncate" in record.message for record in caplog.records
            )
        finally:
            # Restore permissions for cleanup
            db_path.chmod(0o644)

    def test_all_stale_tables_defined(self):
        """Verify LOCAL_MODE_STALE_TABLES contains expected tables."""
        expected_tables = {"users", "user_groups", "user_group_members", "plugins"}
        assert set(LOCAL_MODE_STALE_TABLES) == expected_tables


class TestLayoutJsonParameterFlow:
    """Tests for the layout_json parameter flow in export_all_metadata."""

    @patch("gooddata_export.export.truncate_tables_for_local_mode")
    @patch("gooddata_export.export.store_workspace_metadata")
    @patch("gooddata_export.export.run_post_export_sql")
    @patch("gooddata_export.export.export_workspaces")
    @patch("gooddata_export.export.export_metrics")
    @patch("gooddata_export.export.export_visualizations")
    @patch("gooddata_export.export.export_dashboards")
    @patch("gooddata_export.export.export_dashboards_metrics")
    @patch("gooddata_export.export.export_dashboards_permissions")
    @patch("gooddata_export.export.export_plugins")
    @patch("gooddata_export.export.export_ldm")
    @patch("gooddata_export.export.export_filter_contexts")
    @patch("gooddata_export.export.export_users_and_user_groups")
    def test_local_mode_skips_api_fetch(
        self,
        mock_users,
        mock_filter_contexts,
        mock_ldm,
        mock_plugins,
        mock_dashboards_permissions,
        mock_dashboards_metrics,
        mock_dashboards,
        mock_visualizations,
        mock_metrics,
        mock_workspaces,
        mock_post_export,
        mock_store_metadata,
        mock_truncate,
        tmp_path,
        mock_config,
    ):
        """Using layout_json skips API fetch and uses data directly."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test.db"

        layout_json = {
            "analytics": {
                "metrics": [{"id": "m1", "title": "Metric 1"}],
                "analyticalDashboards": [{"id": "d1", "title": "Dashboard 1"}],
                "visualizationObjects": [],
                "filterContexts": [],
            },
            "ldm": {"datasets": []},
        }

        with patch("gooddata_export.export.validate_workspace_exists") as mock_validate:
            with patch("gooddata_export.export.fetch_all_workspace_data") as mock_fetch:
                export_all_metadata(
                    mock_config,
                    db_path=str(db_path),
                    export_formats=["sqlite"],
                    run_post_export=False,
                    layout_json=layout_json,
                )

                # Should NOT call validate or fetch
                mock_validate.assert_not_called()
                mock_fetch.assert_not_called()

        # Should call truncate
        mock_truncate.assert_called_once()

        # Should set export_mode to "local"
        mock_store_metadata.assert_called_once()
        call_kwargs = mock_store_metadata.call_args[1]
        assert call_kwargs.get("export_mode") == "local"

    @patch("gooddata_export.export.truncate_tables_for_local_mode")
    @patch("gooddata_export.export.store_workspace_metadata")
    @patch("gooddata_export.export.export_workspaces")
    @patch("gooddata_export.export.export_metrics")
    @patch("gooddata_export.export.export_visualizations")
    @patch("gooddata_export.export.export_dashboards")
    @patch("gooddata_export.export.export_dashboards_metrics")
    @patch("gooddata_export.export.export_dashboards_permissions")
    @patch("gooddata_export.export.export_plugins")
    @patch("gooddata_export.export.export_ldm")
    @patch("gooddata_export.export.export_filter_contexts")
    @patch("gooddata_export.export.export_users_and_user_groups")
    def test_local_mode_data_structure(
        self,
        mock_users,
        mock_filter_contexts,
        mock_ldm,
        mock_plugins,
        mock_dashboards_permissions,
        mock_dashboards_metrics,
        mock_dashboards,
        mock_visualizations,
        mock_metrics,
        mock_workspaces,
        mock_store_metadata,
        mock_truncate,
        tmp_path,
        mock_config,
    ):
        """Verifies workspace data structure in local mode."""
        from gooddata_export.export import export_all_metadata

        mock_config.WORKSPACE_ID = "my-workspace"
        db_path = tmp_path / "test.db"

        layout_json = {
            "analytics": {
                "metrics": [{"id": "m1", "title": "Metric 1"}],
                "analyticalDashboards": [{"id": "d1", "title": "Dashboard 1"}],
                "visualizationObjects": [{"id": "v1", "title": "Viz 1"}],
                "filterContexts": [{"id": "fc1", "title": "Filter 1"}],
            },
            "ldm": {"datasets": [{"id": "ds1", "title": "Dataset 1"}]},
        }

        export_all_metadata(
            mock_config,
            db_path=str(db_path),
            export_formats=["sqlite"],
            run_post_export=False,
            layout_json=layout_json,
        )

        # Check the workspace data passed to export_metrics
        call_args = mock_metrics.call_args[0]
        all_workspace_data = call_args[0]

        assert len(all_workspace_data) == 1
        ws_data = all_workspace_data[0]

        assert ws_data["workspace_id"] == "my-workspace"
        assert ws_data["workspace_name"] == "Local Layout (my-workspace)"
        assert ws_data["is_parent"] is True

        data = ws_data["data"]
        assert data["metrics"] == [{"id": "m1", "title": "Metric 1"}]
        assert data["dashboards"] == [{"id": "d1", "title": "Dashboard 1"}]
        assert data["visualizations"] == [{"id": "v1", "title": "Viz 1"}]
        assert data["filter_contexts"] == [{"id": "fc1", "title": "Filter 1"}]
        assert data["plugins"] is None  # Not available in local mode
        assert data["ldm"]["ldm"]["datasets"] == [{"id": "ds1", "title": "Dataset 1"}]
        assert data["child_workspaces"] is None
        assert data["users_and_user_groups"] is None

    @patch("gooddata_export.export.truncate_tables_for_local_mode")
    @patch("gooddata_export.export.store_workspace_metadata")
    @patch("gooddata_export.export.validate_workspace_exists")
    @patch("gooddata_export.export.fetch_all_workspace_data")
    @patch("gooddata_export.export.export_workspaces")
    @patch("gooddata_export.export.export_metrics")
    @patch("gooddata_export.export.export_visualizations")
    @patch("gooddata_export.export.export_dashboards")
    @patch("gooddata_export.export.export_dashboards_metrics")
    @patch("gooddata_export.export.export_dashboards_permissions")
    @patch("gooddata_export.export.export_plugins")
    @patch("gooddata_export.export.export_ldm")
    @patch("gooddata_export.export.export_filter_contexts")
    @patch("gooddata_export.export.export_users_and_user_groups")
    def test_api_mode_fetches_from_api(
        self,
        mock_users,
        mock_filter_contexts,
        mock_ldm,
        mock_plugins,
        mock_dashboards_permissions,
        mock_dashboards_metrics,
        mock_dashboards,
        mock_visualizations,
        mock_metrics,
        mock_workspaces,
        mock_fetch,
        mock_validate,
        mock_store_metadata,
        mock_truncate,
        tmp_path,
        mock_config,
    ):
        """Without layout_json, fetches from API."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test.db"

        mock_fetch.return_value = [
            {
                "workspace_id": "test-workspace",
                "workspace_name": "Test Workspace",
                "is_parent": True,
                "data": {
                    "metrics": [],
                    "dashboards": [],
                    "visualizations": [],
                    "filter_contexts": [],
                    "plugins": [],
                    "ldm": None,
                    "child_workspaces": None,
                    "users_and_user_groups": None,
                    "analytics_model": {},
                },
            }
        ]

        export_all_metadata(
            mock_config,
            db_path=str(db_path),
            export_formats=["sqlite"],
            run_post_export=False,
            layout_json=None,  # API mode
        )

        # Should call validate and fetch
        mock_validate.assert_called_once()
        mock_fetch.assert_called_once()

        # Should NOT call truncate
        mock_truncate.assert_not_called()

        # Should set export_mode to "api"
        mock_store_metadata.assert_called_once()
        call_kwargs = mock_store_metadata.call_args[1]
        assert call_kwargs.get("export_mode") == "api"


class TestLayoutJsonEdgeCases:
    """Tests for edge cases with layout_json parameter."""

    @patch("gooddata_export.export.truncate_tables_for_local_mode")
    @patch("gooddata_export.export.store_workspace_metadata")
    @patch("gooddata_export.export.export_workspaces")
    @patch("gooddata_export.export.export_metrics")
    @patch("gooddata_export.export.export_visualizations")
    @patch("gooddata_export.export.export_dashboards")
    @patch("gooddata_export.export.export_dashboards_metrics")
    @patch("gooddata_export.export.export_dashboards_permissions")
    @patch("gooddata_export.export.export_plugins")
    @patch("gooddata_export.export.export_ldm")
    @patch("gooddata_export.export.export_filter_contexts")
    @patch("gooddata_export.export.export_users_and_user_groups")
    def test_empty_analytics_key(
        self,
        mock_users,
        mock_filter_contexts,
        mock_ldm,
        mock_plugins,
        mock_dashboards_permissions,
        mock_dashboards_metrics,
        mock_dashboards,
        mock_visualizations,
        mock_metrics,
        mock_workspaces,
        mock_store_metadata,
        mock_truncate,
        tmp_path,
        mock_config,
    ):
        """Handles layout_json with empty analytics key."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test.db"

        layout_json = {
            "analytics": {},  # Empty analytics
            "ldm": {"datasets": [{"id": "ds1"}]},
        }

        export_all_metadata(
            mock_config,
            db_path=str(db_path),
            export_formats=["sqlite"],
            run_post_export=False,
            layout_json=layout_json,
        )

        # Verify empty lists are passed for analytics entities
        call_args = mock_metrics.call_args[0]
        ws_data = call_args[0][0]

        assert ws_data["data"]["metrics"] == []
        assert ws_data["data"]["dashboards"] == []
        assert ws_data["data"]["visualizations"] == []
        assert ws_data["data"]["filter_contexts"] == []

    @patch("gooddata_export.export.truncate_tables_for_local_mode")
    @patch("gooddata_export.export.store_workspace_metadata")
    @patch("gooddata_export.export.export_workspaces")
    @patch("gooddata_export.export.export_metrics")
    @patch("gooddata_export.export.export_visualizations")
    @patch("gooddata_export.export.export_dashboards")
    @patch("gooddata_export.export.export_dashboards_metrics")
    @patch("gooddata_export.export.export_dashboards_permissions")
    @patch("gooddata_export.export.export_plugins")
    @patch("gooddata_export.export.export_ldm")
    @patch("gooddata_export.export.export_filter_contexts")
    @patch("gooddata_export.export.export_users_and_user_groups")
    def test_empty_ldm_key(
        self,
        mock_users,
        mock_filter_contexts,
        mock_ldm,
        mock_plugins,
        mock_dashboards_permissions,
        mock_dashboards_metrics,
        mock_dashboards,
        mock_visualizations,
        mock_metrics,
        mock_workspaces,
        mock_store_metadata,
        mock_truncate,
        tmp_path,
        mock_config,
    ):
        """Handles layout_json with empty ldm key."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test.db"

        layout_json = {
            "analytics": {"metrics": [{"id": "m1"}]},
            "ldm": {},  # Empty ldm
        }

        export_all_metadata(
            mock_config,
            db_path=str(db_path),
            export_formats=["sqlite"],
            run_post_export=False,
            layout_json=layout_json,
        )

        # Verify ldm is None when empty
        call_args = mock_ldm.call_args[0]
        ws_data = call_args[0][0]

        # Empty ldm dict results in None (falsy check in code)
        assert ws_data["data"]["ldm"] is None

    @patch("gooddata_export.export.truncate_tables_for_local_mode")
    @patch("gooddata_export.export.store_workspace_metadata")
    @patch("gooddata_export.export.export_workspaces")
    @patch("gooddata_export.export.export_metrics")
    @patch("gooddata_export.export.export_visualizations")
    @patch("gooddata_export.export.export_dashboards")
    @patch("gooddata_export.export.export_dashboards_metrics")
    @patch("gooddata_export.export.export_dashboards_permissions")
    @patch("gooddata_export.export.export_plugins")
    @patch("gooddata_export.export.export_ldm")
    @patch("gooddata_export.export.export_filter_contexts")
    @patch("gooddata_export.export.export_users_and_user_groups")
    def test_missing_analytics_key(
        self,
        mock_users,
        mock_filter_contexts,
        mock_ldm,
        mock_plugins,
        mock_dashboards_permissions,
        mock_dashboards_metrics,
        mock_dashboards,
        mock_visualizations,
        mock_metrics,
        mock_workspaces,
        mock_store_metadata,
        mock_truncate,
        tmp_path,
        mock_config,
    ):
        """Handles layout_json with missing analytics key."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test.db"

        layout_json = {
            # analytics key missing entirely
            "ldm": {"datasets": []},
        }

        export_all_metadata(
            mock_config,
            db_path=str(db_path),
            export_formats=["sqlite"],
            run_post_export=False,
            layout_json=layout_json,
        )

        # Verify empty lists are passed
        call_args = mock_metrics.call_args[0]
        ws_data = call_args[0][0]

        assert ws_data["data"]["metrics"] == []
        assert ws_data["data"]["dashboards"] == []

    @patch("gooddata_export.export.truncate_tables_for_local_mode")
    @patch("gooddata_export.export.store_workspace_metadata")
    @patch("gooddata_export.export.export_workspaces")
    @patch("gooddata_export.export.export_metrics")
    @patch("gooddata_export.export.export_visualizations")
    @patch("gooddata_export.export.export_dashboards")
    @patch("gooddata_export.export.export_dashboards_metrics")
    @patch("gooddata_export.export.export_dashboards_permissions")
    @patch("gooddata_export.export.export_plugins")
    @patch("gooddata_export.export.export_ldm")
    @patch("gooddata_export.export.export_filter_contexts")
    @patch("gooddata_export.export.export_users_and_user_groups")
    def test_missing_ldm_key(
        self,
        mock_users,
        mock_filter_contexts,
        mock_ldm,
        mock_plugins,
        mock_dashboards_permissions,
        mock_dashboards_metrics,
        mock_dashboards,
        mock_visualizations,
        mock_metrics,
        mock_workspaces,
        mock_store_metadata,
        mock_truncate,
        tmp_path,
        mock_config,
    ):
        """Handles layout_json with missing ldm key."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test.db"

        layout_json = {
            "analytics": {"metrics": [{"id": "m1"}]},
            # ldm key missing entirely
        }

        export_all_metadata(
            mock_config,
            db_path=str(db_path),
            export_formats=["sqlite"],
            run_post_export=False,
            layout_json=layout_json,
        )

        # Verify ldm is None
        call_args = mock_ldm.call_args[0]
        ws_data = call_args[0][0]

        assert ws_data["data"]["ldm"] is None

    @patch("gooddata_export.export.truncate_tables_for_local_mode")
    @patch("gooddata_export.export.store_workspace_metadata")
    @patch("gooddata_export.export.export_workspaces")
    @patch("gooddata_export.export.export_metrics")
    @patch("gooddata_export.export.export_visualizations")
    @patch("gooddata_export.export.export_dashboards")
    @patch("gooddata_export.export.export_dashboards_metrics")
    @patch("gooddata_export.export.export_dashboards_permissions")
    @patch("gooddata_export.export.export_plugins")
    @patch("gooddata_export.export.export_ldm")
    @patch("gooddata_export.export.export_filter_contexts")
    @patch("gooddata_export.export.export_users_and_user_groups")
    def test_completely_empty_layout_json(
        self,
        mock_users,
        mock_filter_contexts,
        mock_ldm,
        mock_plugins,
        mock_dashboards_permissions,
        mock_dashboards_metrics,
        mock_dashboards,
        mock_visualizations,
        mock_metrics,
        mock_workspaces,
        mock_store_metadata,
        mock_truncate,
        tmp_path,
        mock_config,
    ):
        """Handles completely empty layout_json dict."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test.db"

        layout_json = {}  # Completely empty

        export_all_metadata(
            mock_config,
            db_path=str(db_path),
            export_formats=["sqlite"],
            run_post_export=False,
            layout_json=layout_json,
        )

        # Should still work, just with empty data
        call_args = mock_metrics.call_args[0]
        ws_data = call_args[0][0]

        assert ws_data["data"]["metrics"] == []
        assert ws_data["data"]["dashboards"] == []
        assert ws_data["data"]["visualizations"] == []
        assert ws_data["data"]["ldm"] is None

    @patch("gooddata_export.export.truncate_tables_for_local_mode")
    @patch("gooddata_export.export.store_workspace_metadata")
    @patch("gooddata_export.export.export_workspaces")
    @patch("gooddata_export.export.export_metrics")
    @patch("gooddata_export.export.export_visualizations")
    @patch("gooddata_export.export.export_dashboards")
    @patch("gooddata_export.export.export_dashboards_metrics")
    @patch("gooddata_export.export.export_dashboards_permissions")
    @patch("gooddata_export.export.export_plugins")
    @patch("gooddata_export.export.export_ldm")
    @patch("gooddata_export.export.export_filter_contexts")
    @patch("gooddata_export.export.export_users_and_user_groups")
    def test_null_values_in_analytics(
        self,
        mock_users,
        mock_filter_contexts,
        mock_ldm,
        mock_plugins,
        mock_dashboards_permissions,
        mock_dashboards_metrics,
        mock_dashboards,
        mock_visualizations,
        mock_metrics,
        mock_workspaces,
        mock_store_metadata,
        mock_truncate,
        tmp_path,
        mock_config,
    ):
        """Handles None values for analytics entity lists."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test.db"

        layout_json = {
            "analytics": {
                "metrics": None,  # Explicitly None
                "analyticalDashboards": None,
                "visualizationObjects": None,
                "filterContexts": None,
            },
            "ldm": None,  # Also None
        }

        export_all_metadata(
            mock_config,
            db_path=str(db_path),
            export_formats=["sqlite"],
            run_post_export=False,
            layout_json=layout_json,
        )

        # Verify None values are converted to empty lists (via `or []`)
        call_args = mock_metrics.call_args[0]
        ws_data = call_args[0][0]

        assert ws_data["data"]["metrics"] == []
        assert ws_data["data"]["dashboards"] == []
        assert ws_data["data"]["visualizations"] == []
        assert ws_data["data"]["filter_contexts"] == []
        assert ws_data["data"]["ldm"] is None


class TestLocalModeIntegration:
    """Integration tests using sample_layout.json fixture.

    These tests run the actual export pipeline without mocking processing functions.
    """

    @pytest.fixture
    def sample_layout(self):
        """Load the sample layout.json fixture."""
        import json
        from pathlib import Path

        fixture_path = Path(__file__).parent / "fixtures" / "sample_layout.json"
        with open(fixture_path) as f:
            return json.load(f)

    @pytest.fixture
    def mock_config(self):
        """Create a mock config for testing."""
        config = MagicMock()
        config.WORKSPACE_ID = "test-workspace"
        config.BASE_URL = "https://test.gooddata.com"
        config.DEBUG_WORKSPACE_PROCESSING = False
        config.INCLUDE_CHILD_WORKSPACES = False
        config.ENABLE_RICH_TEXT_EXTRACTION = True
        config.CHILD_WORKSPACE_DATA_TYPES = []
        config.MAX_PARALLEL_WORKSPACES = 5
        return config

    def test_full_export_creates_database(self, sample_layout, mock_config, tmp_path):
        """Full export with sample fixture creates valid database."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test_export.db"

        with patch("gooddata_export.export.run_post_export_sql"):
            with patch("gooddata_export.export.store_workspace_metadata"):
                result = export_all_metadata(
                    mock_config,
                    db_path=str(db_path),
                    export_formats=["sqlite"],
                    run_post_export=False,
                    layout_json=sample_layout,
                )

        assert db_path.exists()
        assert result["workspace_id"] == "test-workspace"
        assert result["workspace_count"] == 1

    def test_metrics_exported_correctly(self, sample_layout, mock_config, tmp_path):
        """Metrics from fixture are exported to database."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test_export.db"

        with patch("gooddata_export.export.run_post_export_sql"):
            with patch("gooddata_export.export.store_workspace_metadata"):
                export_all_metadata(
                    mock_config,
                    db_path=str(db_path),
                    export_formats=["sqlite"],
                    run_post_export=False,
                    layout_json=sample_layout,
                )

        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            "SELECT metric_id, title, maql FROM metrics ORDER BY metric_id"
        )
        metrics = cursor.fetchall()
        conn.close()

        assert len(metrics) == 3
        metric_ids = [m[0] for m in metrics]
        assert "metric_total_revenue" in metric_ids
        assert "metric_avg_order_value" in metric_ids
        assert "metric_order_count" in metric_ids

        # Verify MAQL is preserved
        revenue_metric = next(m for m in metrics if m[0] == "metric_total_revenue")
        assert "SUM" in revenue_metric[2]

    def test_visualizations_exported_correctly(
        self, sample_layout, mock_config, tmp_path
    ):
        """Visualizations from fixture are exported to database."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test_export.db"

        with patch("gooddata_export.export.run_post_export_sql"):
            with patch("gooddata_export.export.store_workspace_metadata"):
                export_all_metadata(
                    mock_config,
                    db_path=str(db_path),
                    export_formats=["sqlite"],
                    run_post_export=False,
                    layout_json=sample_layout,
                )

        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            "SELECT visualization_id, title FROM visualizations ORDER BY visualization_id"
        )
        visualizations = cursor.fetchall()
        conn.close()

        assert len(visualizations) == 3
        viz_ids = [v[0] for v in visualizations]
        assert "viz_revenue_trend" in viz_ids
        assert "viz_orders_by_region" in viz_ids
        assert "viz_avg_order_value" in viz_ids

    def test_visualization_metrics_relationships(
        self, sample_layout, mock_config, tmp_path
    ):
        """Visualization-metric relationships are extracted."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test_export.db"

        with patch("gooddata_export.export.run_post_export_sql"):
            with patch("gooddata_export.export.store_workspace_metadata"):
                export_all_metadata(
                    mock_config,
                    db_path=str(db_path),
                    export_formats=["sqlite"],
                    run_post_export=False,
                    layout_json=sample_layout,
                )

        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            "SELECT visualization_id, metric_id FROM visualizations_metrics"
        )
        relationships = cursor.fetchall()
        conn.close()

        # viz_revenue_trend uses metric_total_revenue
        # viz_orders_by_region uses metric_order_count
        # viz_avg_order_value uses metric_avg_order_value
        assert len(relationships) == 3
        assert ("viz_revenue_trend", "metric_total_revenue") in relationships
        assert ("viz_orders_by_region", "metric_order_count") in relationships
        assert ("viz_avg_order_value", "metric_avg_order_value") in relationships

    def test_dashboards_exported_correctly(self, sample_layout, mock_config, tmp_path):
        """Dashboards from fixture are exported to database."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test_export.db"

        with patch("gooddata_export.export.run_post_export_sql"):
            with patch("gooddata_export.export.store_workspace_metadata"):
                export_all_metadata(
                    mock_config,
                    db_path=str(db_path),
                    export_formats=["sqlite"],
                    run_post_export=False,
                    layout_json=sample_layout,
                )

        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            "SELECT dashboard_id, title, filter_context_id FROM dashboards ORDER BY dashboard_id"
        )
        dashboards = cursor.fetchall()
        conn.close()

        # 2 dashboards: legacy (executive_overview) and tabbed (tabbed_analytics)
        assert len(dashboards) == 2
        dashboard_ids = [d[0] for d in dashboards]
        assert "dashboard_executive_overview" in dashboard_ids
        assert "dashboard_tabbed_analytics" in dashboard_ids

        # Verify the legacy dashboard details
        legacy_dash = next(
            d for d in dashboards if d[0] == "dashboard_executive_overview"
        )
        assert legacy_dash[1] == "Executive Overview"
        assert legacy_dash[2] == "filter_context_default"

    def test_dashboard_visualization_relationships(
        self, sample_layout, mock_config, tmp_path
    ):
        """Dashboard-visualization relationships are extracted."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test_export.db"

        with patch("gooddata_export.export.run_post_export_sql"):
            with patch("gooddata_export.export.store_workspace_metadata"):
                export_all_metadata(
                    mock_config,
                    db_path=str(db_path),
                    export_formats=["sqlite"],
                    run_post_export=False,
                    layout_json=sample_layout,
                )

        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            "SELECT dashboard_id, visualization_id FROM dashboards_visualizations"
        )
        relationships = cursor.fetchall()
        conn.close()

        # Legacy dashboard has 2 visualizations, tabbed dashboard has 3
        # (but viz_revenue_trend appears in both so 5 total relationships)
        assert len(relationships) >= 5
        viz_ids = [r[1] for r in relationships]
        assert "viz_revenue_trend" in viz_ids
        assert "viz_orders_by_region" in viz_ids
        assert "viz_avg_order_value" in viz_ids

    def test_tabbed_dashboard_extracts_tab_id(
        self, sample_layout, mock_config, tmp_path
    ):
        """Tabbed dashboards have tab_id populated in relationships."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test_export.db"

        with patch("gooddata_export.export.run_post_export_sql"):
            with patch("gooddata_export.export.store_workspace_metadata"):
                export_all_metadata(
                    mock_config,
                    db_path=str(db_path),
                    export_formats=["sqlite"],
                    run_post_export=False,
                    layout_json=sample_layout,
                )

        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            """SELECT dashboard_id, visualization_id, tab_id
               FROM dashboards_visualizations
               WHERE dashboard_id = 'dashboard_tabbed_analytics'
               ORDER BY tab_id, visualization_id"""
        )
        tabbed_relationships = cursor.fetchall()
        conn.close()

        # Tabbed dashboard has 3 visualizations across 2 tabs
        assert len(tabbed_relationships) == 3

        # Tab 1 (tab_overview) has viz_revenue_trend
        tab_overview = [r for r in tabbed_relationships if r[2] == "tab_overview"]
        assert len(tab_overview) == 1
        assert tab_overview[0][1] == "viz_revenue_trend"

        # Tab 2 (tab_details) has viz_orders_by_region and viz_avg_order_value
        tab_details = [r for r in tabbed_relationships if r[2] == "tab_details"]
        assert len(tab_details) == 2
        detail_viz_ids = {r[1] for r in tab_details}
        assert detail_viz_ids == {"viz_orders_by_region", "viz_avg_order_value"}

    def test_legacy_dashboard_has_null_tab_id(
        self, sample_layout, mock_config, tmp_path
    ):
        """Legacy non-tabbed dashboards have NULL tab_id."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test_export.db"

        with patch("gooddata_export.export.run_post_export_sql"):
            with patch("gooddata_export.export.store_workspace_metadata"):
                export_all_metadata(
                    mock_config,
                    db_path=str(db_path),
                    export_formats=["sqlite"],
                    run_post_export=False,
                    layout_json=sample_layout,
                )

        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            """SELECT dashboard_id, visualization_id, tab_id
               FROM dashboards_visualizations
               WHERE dashboard_id = 'dashboard_executive_overview'"""
        )
        legacy_relationships = cursor.fetchall()
        conn.close()

        # Legacy dashboard has 2 visualizations with NULL tab_id
        assert len(legacy_relationships) == 2
        for r in legacy_relationships:
            assert r[2] is None  # tab_id should be NULL

    def test_ldm_datasets_exported(self, sample_layout, mock_config, tmp_path):
        """LDM datasets from fixture are exported to database."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test_export.db"

        with patch("gooddata_export.export.run_post_export_sql"):
            with patch("gooddata_export.export.store_workspace_metadata"):
                export_all_metadata(
                    mock_config,
                    db_path=str(db_path),
                    export_formats=["sqlite"],
                    run_post_export=False,
                    layout_json=sample_layout,
                )

        conn = sqlite3.connect(db_path)
        cursor = conn.execute("SELECT id, title FROM ldm_datasets ORDER BY id")
        datasets = cursor.fetchall()
        conn.close()

        assert len(datasets) == 2
        dataset_ids = [d[0] for d in datasets]
        assert "orders" in dataset_ids
        assert "date" in dataset_ids

    def test_ldm_columns_exported(self, sample_layout, mock_config, tmp_path):
        """LDM columns (attributes and facts) are exported."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test_export.db"

        with patch("gooddata_export.export.run_post_export_sql"):
            with patch("gooddata_export.export.store_workspace_metadata"):
                export_all_metadata(
                    mock_config,
                    db_path=str(db_path),
                    export_formats=["sqlite"],
                    run_post_export=False,
                    layout_json=sample_layout,
                )

        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            "SELECT dataset_id, id, type FROM ldm_columns WHERE dataset_id = 'orders'"
        )
        columns = cursor.fetchall()
        conn.close()

        # orders dataset has: 2 attributes (order_id, region) + 2 facts (revenue, quantity)
        # + 1 reference to date
        assert len(columns) >= 4

        column_types = {c[1]: c[2] for c in columns}
        assert column_types.get("order_id") == "attribute"
        assert column_types.get("revenue") == "fact"

    def test_filter_contexts_exported(self, sample_layout, mock_config, tmp_path):
        """Filter contexts from fixture are exported."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test_export.db"

        with patch("gooddata_export.export.run_post_export_sql"):
            with patch("gooddata_export.export.store_workspace_metadata"):
                export_all_metadata(
                    mock_config,
                    db_path=str(db_path),
                    export_formats=["sqlite"],
                    run_post_export=False,
                    layout_json=sample_layout,
                )

        conn = sqlite3.connect(db_path)
        cursor = conn.execute("SELECT filter_context_id, title FROM filter_contexts")
        filter_contexts = cursor.fetchall()
        conn.close()

        assert len(filter_contexts) == 1
        assert filter_contexts[0][0] == "filter_context_default"

    def test_tags_preserved(self, sample_layout, mock_config, tmp_path):
        """Tags are properly preserved in exports."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test_export.db"

        with patch("gooddata_export.export.run_post_export_sql"):
            with patch("gooddata_export.export.store_workspace_metadata"):
                export_all_metadata(
                    mock_config,
                    db_path=str(db_path),
                    export_formats=["sqlite"],
                    run_post_export=False,
                    layout_json=sample_layout,
                )

        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            "SELECT tags FROM metrics WHERE metric_id = 'metric_total_revenue'"
        )
        tags = cursor.fetchone()[0]
        conn.close()

        # Tags should be sorted and stringified
        assert "finance" in tags
        assert "kpi" in tags

    def test_minimal_gitops_format_works(self, mock_config, tmp_path):
        """Minimal gitops format (without server timestamps) works correctly.

        GitOps workflows use declarative layouts that omit server-assigned metadata
        like createdAt, createdBy. Tags ARE included (user-defined).
        Fields like areRelationsValid, isHidden only come from entity API, not layout API.
        """
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test_export.db"

        # Minimal format - only essential fields, no timestamps or metadata
        minimal_layout = {
            "analytics": {
                "metrics": [
                    {
                        "id": "revenue",
                        "title": "Revenue",
                        "description": "Total revenue",
                        "content": {
                            "maql": "SELECT SUM({fact/amount})",
                            "format": "#,##0",
                        },
                        # NO: createdAt (server-assigned), areRelationsValid/isHidden (entity API only)
                    }
                ],
                "analyticalDashboards": [
                    {
                        "id": "overview",
                        "title": "Overview",
                        "description": "",
                        "content": {"layout": {"sections": []}},
                        # NO: createdAt (server-assigned)
                    }
                ],
                "visualizationObjects": [],
                "filterContexts": [],
            },
            "ldm": {
                "datasets": [
                    {
                        "id": "transactions",
                        "title": "Transactions",
                        "grain": [],
                        "attributes": [],
                        "facts": [
                            {
                                "id": "amount",
                                "title": "Amount",
                                "sourceColumn": "AMOUNT",
                                "sourceColumnDataType": "NUMERIC",
                            }
                        ],
                        "references": [],
                    }
                ]
            },
        }

        with patch("gooddata_export.export.run_post_export_sql"):
            with patch("gooddata_export.export.store_workspace_metadata"):
                export_all_metadata(
                    mock_config,
                    db_path=str(db_path),
                    export_formats=["sqlite"],
                    run_post_export=False,
                    layout_json=minimal_layout,
                )

        assert db_path.exists()

        # Verify data exported with defaults for missing fields
        conn = sqlite3.connect(db_path)

        # Metric should have empty timestamps and default validity
        cursor = conn.execute(
            "SELECT metric_id, created_at, is_valid, tags FROM metrics"
        )
        metric = cursor.fetchone()
        assert metric[0] == "revenue"
        assert metric[1] == ""  # Empty created_at
        assert metric[2] == 1  # Default is_valid = True
        assert metric[3] == "[]"  # Empty tags

        # Dashboard should also work
        cursor = conn.execute("SELECT dashboard_id, created_at FROM dashboards")
        dashboard = cursor.fetchone()
        assert dashboard[0] == "overview"
        assert dashboard[1] == ""  # Empty created_at

        conn.close()
