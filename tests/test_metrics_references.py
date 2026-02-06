"""Tests for metrics_references table and is_valid computation.

These tests verify that the consolidated metrics_references table correctly:
1. Extracts all reference types (metric, attribute, label, fact) from MAQL
2. Excludes self-references for metric type
3. Supports metrics_ancestry (metric-to-metric only)
4. Works with v_metrics_relationships and v_metrics_relationships_root views
5. Computes is_valid correctly for local mode (metrics and visualizations)
6. Computes is_used_maql correctly
7. Validates label references against ldm_columns (not ldm_labels)
"""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from gooddata_export.post_export import populate_metrics_references


class TestPopulateMetricsReferences:
    """Unit tests for the populate_metrics_references function."""

    @pytest.fixture
    def db_connection(self):
        """Create in-memory database with metrics table."""
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()

        # Create metrics table
        cursor.execute("""
            CREATE TABLE metrics (
                metric_id TEXT,
                workspace_id TEXT,
                maql TEXT,
                PRIMARY KEY (metric_id, workspace_id)
            )
        """)

        # Create metrics_references table (same as production schema)
        cursor.execute("""
            CREATE TABLE metrics_references (
                source_metric_id TEXT,
                source_workspace_id TEXT,
                referenced_id TEXT,
                reference_type TEXT,
                PRIMARY KEY (source_metric_id, source_workspace_id, referenced_id, reference_type)
            )
        """)

        conn.commit()
        yield conn
        conn.close()

    def test_extracts_metric_references(self, db_connection):
        """Extracts {metric/...} patterns with reference_type='metric'."""
        cursor = db_connection.cursor()
        cursor.execute(
            "INSERT INTO metrics VALUES (?, ?, ?)",
            ("m1", "ws1", "SELECT {metric/m2} + {metric/m3}"),
        )
        db_connection.commit()

        populate_metrics_references(cursor)
        db_connection.commit()

        cursor.execute(
            "SELECT referenced_id, reference_type FROM metrics_references ORDER BY referenced_id"
        )
        results = cursor.fetchall()

        assert len(results) == 2
        assert results[0] == ("m2", "metric")
        assert results[1] == ("m3", "metric")

    def test_extracts_attribute_references(self, db_connection):
        """Extracts {attr/...} patterns with reference_type='attribute'."""
        cursor = db_connection.cursor()
        cursor.execute(
            "INSERT INTO metrics VALUES (?, ?, ?)",
            ("m1", "ws1", "SELECT COUNT({attr/order_id})"),
        )
        db_connection.commit()

        populate_metrics_references(cursor)
        db_connection.commit()

        cursor.execute("SELECT referenced_id, reference_type FROM metrics_references")
        results = cursor.fetchall()

        assert len(results) == 1
        assert results[0] == ("order_id", "attribute")

    def test_extracts_fact_references(self, db_connection):
        """Extracts {fact/...} patterns with reference_type='fact'."""
        cursor = db_connection.cursor()
        cursor.execute(
            "INSERT INTO metrics VALUES (?, ?, ?)",
            ("m1", "ws1", "SELECT SUM({fact/revenue})"),
        )
        db_connection.commit()

        populate_metrics_references(cursor)
        db_connection.commit()

        cursor.execute("SELECT referenced_id, reference_type FROM metrics_references")
        results = cursor.fetchall()

        assert len(results) == 1
        assert results[0] == ("revenue", "fact")

    def test_extracts_label_references(self, db_connection):
        """Extracts {label/...} patterns with reference_type='label'."""
        cursor = db_connection.cursor()
        cursor.execute(
            "INSERT INTO metrics VALUES (?, ?, ?)",
            ("m1", "ws1", 'SELECT COUNT(1) WHERE {label/card_present_code} = "0"'),
        )
        db_connection.commit()

        populate_metrics_references(cursor)
        db_connection.commit()

        cursor.execute("SELECT referenced_id, reference_type FROM metrics_references")
        results = cursor.fetchall()

        assert len(results) == 1
        assert results[0] == ("card_present_code", "label")

    def test_extracts_all_reference_types(self, db_connection):
        """Single metric with all four reference types."""
        cursor = db_connection.cursor()
        cursor.execute(
            "INSERT INTO metrics VALUES (?, ?, ?)",
            (
                "m1",
                "ws1",
                'SELECT {metric/base_metric} * {fact/amount} / COUNT({attr/customer_id}) WHERE {label/status_code} = "1"',
            ),
        )
        db_connection.commit()

        populate_metrics_references(cursor)
        db_connection.commit()

        cursor.execute(
            "SELECT referenced_id, reference_type FROM metrics_references ORDER BY reference_type"
        )
        results = cursor.fetchall()

        assert len(results) == 4
        # Sorted by reference_type: attribute, fact, label, metric
        assert ("customer_id", "attribute") in results
        assert ("amount", "fact") in results
        assert ("status_code", "label") in results
        assert ("base_metric", "metric") in results

    def test_excludes_metric_self_references(self, db_connection):
        """Self-references are excluded for metric type only."""
        cursor = db_connection.cursor()
        # Metric that references itself (edge case in recursive definitions)
        cursor.execute(
            "INSERT INTO metrics VALUES (?, ?, ?)",
            ("m1", "ws1", "SELECT IFNULL({metric/m1}, 0) + {metric/m2}"),
        )
        db_connection.commit()

        populate_metrics_references(cursor)
        db_connection.commit()

        cursor.execute("SELECT referenced_id, reference_type FROM metrics_references")
        results = cursor.fetchall()

        # Should only have m2, not m1 (self-reference excluded)
        assert len(results) == 1
        assert results[0] == ("m2", "metric")

    def test_handles_null_maql(self, db_connection):
        """Metrics with NULL maql are skipped."""
        cursor = db_connection.cursor()
        cursor.execute("INSERT INTO metrics VALUES (?, ?, ?)", ("m1", "ws1", None))
        db_connection.commit()

        populate_metrics_references(cursor)
        db_connection.commit()

        cursor.execute("SELECT COUNT(*) FROM metrics_references")
        assert cursor.fetchone()[0] == 0

    def test_handles_multiple_workspaces(self, db_connection):
        """References are workspace-scoped."""
        cursor = db_connection.cursor()
        cursor.execute(
            "INSERT INTO metrics VALUES (?, ?, ?)",
            ("m1", "ws1", "SELECT {metric/base}"),
        )
        cursor.execute(
            "INSERT INTO metrics VALUES (?, ?, ?)",
            ("m1", "ws2", "SELECT {metric/other}"),
        )
        db_connection.commit()

        populate_metrics_references(cursor)
        db_connection.commit()

        cursor.execute(
            "SELECT source_workspace_id, referenced_id FROM metrics_references ORDER BY source_workspace_id"
        )
        results = cursor.fetchall()

        assert len(results) == 2
        assert results[0] == ("ws1", "base")
        assert results[1] == ("ws2", "other")

    def test_filters_out_commented_lines(self, db_connection):
        """Lines starting with # are MAQL comments and should be ignored."""
        cursor = db_connection.cursor()
        # MAQL with commented-out old code containing references
        maql = """#SELECT AVG({fact/old_fact})
#  BY {label/old_label}
SELECT SUM({fact/active_fact})
  WHERE {label/active_label} = "value"
"""
        cursor.execute("INSERT INTO metrics VALUES (?, ?, ?)", ("m1", "ws1", maql))
        db_connection.commit()

        populate_metrics_references(cursor)
        db_connection.commit()

        cursor.execute(
            "SELECT referenced_id, reference_type FROM metrics_references ORDER BY referenced_id"
        )
        results = cursor.fetchall()

        # Should only have active references, not commented ones
        assert len(results) == 2
        assert ("active_fact", "fact") in results
        assert ("active_label", "label") in results
        # Commented references should NOT be extracted
        assert ("old_fact", "fact") not in results
        assert ("old_label", "label") not in results

    def test_filters_comments_with_leading_whitespace(self, db_connection):
        """Comment lines with leading whitespace are also filtered."""
        cursor = db_connection.cursor()
        maql = """SELECT {metric/active}
  # This is an indented comment with {metric/commented_ref}
    #Another indented comment {fact/old_fact}
  + {metric/another_active}"""
        cursor.execute("INSERT INTO metrics VALUES (?, ?, ?)", ("m1", "ws1", maql))
        db_connection.commit()

        populate_metrics_references(cursor)
        db_connection.commit()

        cursor.execute(
            "SELECT referenced_id FROM metrics_references ORDER BY referenced_id"
        )
        results = [row[0] for row in cursor.fetchall()]

        assert results == ["active", "another_active"]
        assert "commented_ref" not in results
        assert "old_fact" not in results

    def test_filters_inline_comments(self, db_connection):
        """Inline comments after code should be stripped (MAQL supports # anywhere)."""
        cursor = db_connection.cursor()
        maql = """SELECT {metric/active} # this references {metric/commented_out}
  + {fact/real_fact} # old: {fact/old_fact}
  WHERE {label/real_label} = "x" """
        cursor.execute("INSERT INTO metrics VALUES (?, ?, ?)", ("m1", "ws1", maql))
        db_connection.commit()

        populate_metrics_references(cursor)
        db_connection.commit()

        cursor.execute(
            "SELECT referenced_id, reference_type FROM metrics_references ORDER BY referenced_id"
        )
        results = cursor.fetchall()

        assert ("active", "metric") in results
        assert ("real_fact", "fact") in results
        assert ("real_label", "label") in results
        # Inline-commented references should NOT be extracted
        assert ("commented_out", "metric") not in results
        assert ("old_fact", "fact") not in results
        assert len(results) == 3


class TestMetricsReferencesIntegration:
    """Integration tests running full post-export processing."""

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
        config.INCLUDE_CHILD_WORKSPACES = False
        config.ENABLE_RICH_TEXT_EXTRACTION = True
        config.CHILD_WORKSPACE_DATA_TYPES = []
        config.MAX_PARALLEL_WORKSPACES = 5
        return config

    @pytest.fixture
    def exported_db(self, sample_layout, mock_config, tmp_path):
        """Run full export with post-processing and return db path."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test_export.db"

        with patch("gooddata_export.export.store_workspace_metadata"):
            export_all_metadata(
                mock_config,
                db_path=str(db_path),
                export_formats=["sqlite"],
                run_post_export=True,  # Run post-export processing
                layout_json=sample_layout,
            )

        return db_path

    def test_metrics_references_table_created(self, exported_db):
        """metrics_references table exists after export."""
        conn = sqlite3.connect(exported_db)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='metrics_references'"
        )
        assert cursor.fetchone() is not None

        conn.close()

    def test_metrics_references_has_all_types(self, exported_db):
        """All three reference types are extracted from fixture data."""
        conn = sqlite3.connect(exported_db)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT DISTINCT reference_type FROM metrics_references ORDER BY reference_type"
        )
        types = [row[0] for row in cursor.fetchall()]

        # Fixture has metrics with {metric/...}, {attr/...}, {fact/...}
        assert "attribute" in types
        assert "fact" in types
        assert "metric" in types

        conn.close()

    def test_metric_to_metric_reference_extracted(self, exported_db):
        """metric_avg_order_value -> metric_total_revenue reference is captured."""
        conn = sqlite3.connect(exported_db)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT source_metric_id, referenced_id
            FROM metrics_references
            WHERE reference_type = 'metric'
        """)
        results = cursor.fetchall()

        # metric_avg_order_value references metric_total_revenue
        assert ("metric_avg_order_value", "metric_total_revenue") in results

        conn.close()

    def test_fact_reference_extracted(self, exported_db):
        """metric_total_revenue -> fact/revenue reference is captured."""
        conn = sqlite3.connect(exported_db)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT source_metric_id, referenced_id
            FROM metrics_references
            WHERE reference_type = 'fact'
        """)
        results = cursor.fetchall()

        assert ("metric_total_revenue", "revenue") in results

        conn.close()

    def test_attribute_reference_extracted(self, exported_db):
        """metric_avg_order_value -> attr/order_id reference is captured."""
        conn = sqlite3.connect(exported_db)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT source_metric_id, referenced_id
            FROM metrics_references
            WHERE reference_type = 'attribute'
        """)
        results = cursor.fetchall()

        # Both metric_avg_order_value and metric_order_count reference order_id
        assert ("metric_avg_order_value", "order_id") in results
        assert ("metric_order_count", "order_id") in results

        conn.close()

    def test_metrics_ancestry_only_metric_refs(self, exported_db):
        """metrics_ancestry contains only metric-to-metric relationships."""
        conn = sqlite3.connect(exported_db)
        cursor = conn.cursor()

        cursor.execute("SELECT metric_id, ancestor_metric_id FROM metrics_ancestry")
        results = cursor.fetchall()

        # metric_avg_order_value depends on metric_total_revenue
        assert ("metric_avg_order_value", "metric_total_revenue") in results

        # Verify no fact/attribute IDs leaked into ancestry
        cursor.execute("SELECT DISTINCT ancestor_metric_id FROM metrics_ancestry")
        ancestors = [row[0] for row in cursor.fetchall()]

        # Should only be metric_total_revenue, not 'revenue' or 'order_id'
        assert "revenue" not in ancestors
        assert "order_id" not in ancestors

        conn.close()

    def test_v_metrics_relationships_view_works(self, exported_db):
        """v_metrics_relationships view filters to metric type only."""
        conn = sqlite3.connect(exported_db)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT source_metric_id, referenced_metric_id, reference_status
            FROM v_metrics_relationships
        """)
        results = cursor.fetchall()

        # Should have the metric_avg_order_value -> metric_total_revenue relationship
        found = False
        for row in results:
            if row[0] == "metric_avg_order_value" and row[1] == "metric_total_revenue":
                found = True
                assert row[2] == "EXISTS"  # Referenced metric exists
                break

        assert found, "Expected metric relationship not found in view"

        conn.close()

    def test_v_metrics_relationships_root_view_works(self, exported_db):
        """v_metrics_relationships_root identifies root metrics correctly."""
        conn = sqlite3.connect(exported_db)
        cursor = conn.cursor()

        cursor.execute("SELECT metric_id FROM v_metrics_relationships_root")
        root_metrics = [row[0] for row in cursor.fetchall()]

        # metric_total_revenue and metric_order_count are roots (don't depend on other metrics)
        assert "metric_total_revenue" in root_metrics
        assert "metric_order_count" in root_metrics

        # metric_avg_order_value is NOT a root (depends on metric_total_revenue)
        assert "metric_avg_order_value" not in root_metrics

        conn.close()

    def test_is_used_maql_computed(self, exported_db):
        """is_used_maql is set for metrics referenced by other metrics."""
        conn = sqlite3.connect(exported_db)
        cursor = conn.cursor()

        cursor.execute("SELECT metric_id, is_used_maql FROM metrics")
        results = {row[0]: row[1] for row in cursor.fetchall()}

        # metric_total_revenue is used by metric_avg_order_value
        assert results["metric_total_revenue"] == 1

        # metric_avg_order_value and metric_order_count are not used by other metrics
        assert results["metric_avg_order_value"] == 0
        assert results["metric_order_count"] == 0

        conn.close()


class TestMetricsIsValidComputation:
    """Tests for is_valid computation in local mode."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock config for testing."""
        config = MagicMock()
        config.WORKSPACE_ID = "test-workspace"
        config.BASE_URL = "https://test.gooddata.com"
        config.INCLUDE_CHILD_WORKSPACES = False
        config.ENABLE_RICH_TEXT_EXTRACTION = False
        config.CHILD_WORKSPACE_DATA_TYPES = []
        config.MAX_PARALLEL_WORKSPACES = 5
        return config

    def test_is_valid_computed_for_local_mode(self, mock_config, tmp_path):
        """Local mode metrics get is_valid computed from reference analysis."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test.db"

        # Layout with metrics - no areRelationsValid field (local mode)
        layout_json = {
            "analytics": {
                "metrics": [
                    {
                        "id": "valid_metric",
                        "title": "Valid Metric",
                        "content": {"maql": "SELECT SUM({fact/existing_fact})"},
                        # No areRelationsValid - will be NULL, computed in post-export
                    },
                    {
                        "id": "invalid_metric",
                        "title": "Invalid Metric",
                        "content": {"maql": "SELECT {metric/nonexistent_metric}"},
                    },
                ],
                "analyticalDashboards": [],
                "visualizationObjects": [],
                "filterContexts": [],
            },
            "ldm": {
                "datasets": [
                    {
                        "id": "test_dataset",
                        "title": "Test Dataset",
                        "attributes": [],
                        "facts": [
                            {
                                "id": "existing_fact",
                                "title": "Existing Fact",
                                "sourceColumn": "COL",
                                "sourceColumnDataType": "NUMERIC",
                            }
                        ],
                        "references": [],
                    }
                ]
            },
        }

        with patch("gooddata_export.export.store_workspace_metadata"):
            export_all_metadata(
                mock_config,
                db_path=str(db_path),
                export_formats=["sqlite"],
                run_post_export=True,
                layout_json=layout_json,
            )

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT metric_id, is_valid FROM metrics ORDER BY metric_id")
        results = {row[0]: row[1] for row in cursor.fetchall()}

        # invalid_metric references nonexistent metric -> is_valid = 0
        assert results["invalid_metric"] == 0

        # valid_metric references existing fact -> is_valid = 1
        assert results["valid_metric"] == 1

        conn.close()

    def test_is_valid_invalid_attribute_reference(self, mock_config, tmp_path):
        """Metric referencing nonexistent attribute is marked invalid."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test.db"

        layout_json = {
            "analytics": {
                "metrics": [
                    {
                        "id": "bad_attr_metric",
                        "title": "Bad Attr Metric",
                        "content": {"maql": "SELECT COUNT({attr/nonexistent_attr})"},
                    },
                ],
                "analyticalDashboards": [],
                "visualizationObjects": [],
                "filterContexts": [],
            },
            "ldm": {
                "datasets": [
                    {
                        "id": "some_dataset",
                        "title": "Some Dataset",
                        "grain": [],
                        "attributes": [],  # No attributes - reference will be invalid
                        "facts": [
                            {
                                "id": "unrelated_fact",
                                "title": "Unrelated Fact",
                                "sourceColumn": "COL",
                                "sourceColumnDataType": "NUMERIC",
                            }
                        ],
                        "references": [],
                    }
                ]
            },
        }

        with patch("gooddata_export.export.store_workspace_metadata"):
            export_all_metadata(
                mock_config,
                db_path=str(db_path),
                export_formats=["sqlite"],
                run_post_export=True,
                layout_json=layout_json,
            )

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT is_valid FROM metrics WHERE metric_id = 'bad_attr_metric'"
        )
        result = cursor.fetchone()

        assert result[0] == 0  # Invalid due to missing attribute

        conn.close()

    def test_is_valid_invalid_fact_reference(self, mock_config, tmp_path):
        """Metric referencing nonexistent fact is marked invalid."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test.db"

        layout_json = {
            "analytics": {
                "metrics": [
                    {
                        "id": "bad_fact_metric",
                        "title": "Bad Fact Metric",
                        "content": {"maql": "SELECT SUM({fact/nonexistent_fact})"},
                    },
                ],
                "analyticalDashboards": [],
                "visualizationObjects": [],
                "filterContexts": [],
            },
            "ldm": {
                "datasets": [
                    {
                        "id": "some_dataset",
                        "title": "Some Dataset",
                        "grain": [],
                        "attributes": [
                            {
                                "id": "unrelated_attr",
                                "title": "Unrelated Attr",
                                "sourceColumn": "COL",
                                "sourceColumnDataType": "STRING",
                            }
                        ],
                        "facts": [],  # No facts - reference will be invalid
                        "references": [],
                    }
                ]
            },
        }

        with patch("gooddata_export.export.store_workspace_metadata"):
            export_all_metadata(
                mock_config,
                db_path=str(db_path),
                export_formats=["sqlite"],
                run_post_export=True,
                layout_json=layout_json,
            )

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT is_valid FROM metrics WHERE metric_id = 'bad_fact_metric'"
        )
        result = cursor.fetchone()

        assert result[0] == 0  # Invalid due to missing fact

        conn.close()

    def test_is_valid_no_references(self, mock_config, tmp_path):
        """Metric with no references is valid."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test.db"

        layout_json = {
            "analytics": {
                "metrics": [
                    {
                        "id": "simple_metric",
                        "title": "Simple Metric",
                        "content": {"maql": "SELECT 1"},  # No references
                    },
                ],
                "analyticalDashboards": [],
                "visualizationObjects": [],
                "filterContexts": [],
            },
            "ldm": {
                "datasets": [
                    {
                        "id": "some_dataset",
                        "title": "Some Dataset",
                        "grain": [],
                        "attributes": [],
                        "facts": [
                            {
                                "id": "some_fact",
                                "title": "Some Fact",
                                "sourceColumn": "COL",
                                "sourceColumnDataType": "NUMERIC",
                            }
                        ],
                        "references": [],
                    }
                ]
            },
        }

        with patch("gooddata_export.export.store_workspace_metadata"):
            export_all_metadata(
                mock_config,
                db_path=str(db_path),
                export_formats=["sqlite"],
                run_post_export=True,
                layout_json=layout_json,
            )

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT is_valid FROM metrics WHERE metric_id = 'simple_metric'")
        result = cursor.fetchone()

        assert result[0] == 1  # Valid (no references to check)

        conn.close()

    def test_is_valid_label_reference_validated_against_ldm_columns(
        self, mock_config, tmp_path
    ):
        """Label references are validated against ldm_columns (type='attribute').

        In MAQL, {label/id} references the attribute's default label, which shares
        the same ID as the attribute in ldm_columns. This test ensures the
        validation correctly uses ldm_columns, not ldm_labels.
        """
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test.db"

        layout_json = {
            "analytics": {
                "metrics": [
                    {
                        "id": "valid_label_metric",
                        "title": "Valid Label Metric",
                        # Label reference that matches an attribute ID in ldm_columns
                        "content": {
                            "maql": 'SELECT COUNT(1) WHERE {label/status_code} = "1"'
                        },
                    },
                    {
                        "id": "invalid_label_metric",
                        "title": "Invalid Label Metric",
                        # Label reference that doesn't exist
                        "content": {
                            "maql": 'SELECT COUNT(1) WHERE {label/nonexistent_attr} = "1"'
                        },
                    },
                ],
                "analyticalDashboards": [],
                "visualizationObjects": [],
                "filterContexts": [],
            },
            "ldm": {
                "datasets": [
                    {
                        "id": "test_dataset",
                        "title": "Test Dataset",
                        "grain": [],
                        "attributes": [
                            {
                                # This attribute ID matches the {label/status_code} reference
                                "id": "status_code",
                                "title": "Status Code",
                                "sourceColumn": "STATUS",
                                "sourceColumnDataType": "STRING",
                            }
                        ],
                        "facts": [
                            {
                                "id": "some_fact",
                                "title": "Some Fact",
                                "sourceColumn": "COL",
                                "sourceColumnDataType": "NUMERIC",
                            }
                        ],
                        "references": [],
                    }
                ]
            },
        }

        with patch("gooddata_export.export.store_workspace_metadata"):
            export_all_metadata(
                mock_config,
                db_path=str(db_path),
                export_formats=["sqlite"],
                run_post_export=True,
                layout_json=layout_json,
            )

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT metric_id, is_valid FROM metrics ORDER BY metric_id")
        results = {row[0]: row[1] for row in cursor.fetchall()}

        # invalid_label_metric references nonexistent attr -> is_valid = 0
        assert results["invalid_label_metric"] == 0

        # valid_label_metric references existing attribute (status_code) -> is_valid = 1
        assert results["valid_label_metric"] == 1

        conn.close()


class TestVisualizationsIsValidComputation:
    """Tests for is_valid computation on visualizations in local mode."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock config for testing."""
        config = MagicMock()
        config.WORKSPACE_ID = "test-workspace"
        config.BASE_URL = "https://test.gooddata.com"
        config.INCLUDE_CHILD_WORKSPACES = False
        config.ENABLE_RICH_TEXT_EXTRACTION = False
        config.CHILD_WORKSPACE_DATA_TYPES = []
        config.MAX_PARALLEL_WORKSPACES = 5
        return config

    # Minimal dataset with one fact so LDM export doesn't fail on empty column list
    _BASE_DATASET = {
        "id": "base_ds",
        "title": "Base Dataset",
        "attributes": [],
        "facts": [
            {
                "id": "placeholder_fact",
                "title": "Placeholder",
                "sourceColumn": "COL",
                "sourceColumnDataType": "NUMERIC",
            }
        ],
        "references": [],
    }

    def _make_layout(self, visualizations, metrics=None, datasets=None):
        """Build a layout_json with the given visualizations and LDM.

        Always includes a base dataset to ensure ldm_columns is non-empty
        (required by the export pipeline).
        """
        all_datasets = [self._BASE_DATASET]
        if datasets:
            all_datasets.extend(datasets)
        return {
            "analytics": {
                "metrics": metrics or [],
                "analyticalDashboards": [],
                "visualizationObjects": visualizations,
                "filterContexts": [],
            },
            "ldm": {"datasets": all_datasets},
        }

    def _make_viz(
        self,
        viz_id,
        measures=None,
        attributes=None,
        filters=None,
        attr_filter_configs=None,
    ):
        """Build a visualization object with specified references.

        Args:
            viz_id: Visualization ID
            measures: List of (ref_id, ref_type) tuples for measure buckets
            attributes: List of (label_id, label_type) tuples for attribute buckets
            filters: List of (filter_id, filter_type, pos_or_neg) tuples
            attr_filter_configs: List of (label_id, label_type) tuples for attributeFilterConfigs
        """
        items = []
        if measures:
            for ref_id, ref_type in measures:
                items.append(
                    {
                        "measure": {
                            "localIdentifier": f"m_{ref_id}",
                            "definition": {
                                "measureDefinition": {
                                    "item": {
                                        "identifier": {
                                            "id": ref_id,
                                            "type": ref_type,
                                        }
                                    }
                                }
                            },
                            "title": ref_id,
                        }
                    }
                )
        if attributes:
            for label_id, label_type in attributes:
                items.append(
                    {
                        "attribute": {
                            "localIdentifier": f"a_{label_id}",
                            "displayForm": {
                                "identifier": {"id": label_id, "type": label_type}
                            },
                        }
                    }
                )

        filter_list = []
        if filters:
            for filter_id, obj_type, kind in filters:
                filter_key = (
                    "positiveAttributeFilter"
                    if kind == "positive"
                    else "negativeAttributeFilter"
                )
                filter_list.append(
                    {
                        filter_key: {
                            "displayForm": {
                                "identifier": {"id": filter_id, "type": obj_type}
                            },
                            "in": {"values": []},
                        }
                    }
                )

        content = {
            "visualizationUrl": "local:table",
            "buckets": [{"localIdentifier": "measures", "items": items}],
            "filters": filter_list,
        }

        if attr_filter_configs:
            configs = {}
            for i, (label_id, label_type) in enumerate(attr_filter_configs):
                configs[f"config_{i}"] = {
                    "displayAsLabel": {
                        "identifier": {"id": label_id, "type": label_type}
                    }
                }
            content["attributeFilterConfigs"] = configs

        return {
            "id": viz_id,
            "title": viz_id,
            "content": content,
        }

    def _export(self, mock_config, tmp_path, layout_json):
        """Run export with post-processing and return db path."""
        from gooddata_export.export import export_all_metadata

        db_path = tmp_path / "test.db"

        with patch("gooddata_export.export.store_workspace_metadata"):
            export_all_metadata(
                mock_config,
                db_path=str(db_path),
                export_formats=["sqlite"],
                run_post_export=True,
                layout_json=layout_json,
            )

        return db_path

    def test_valid_metric_reference(self, mock_config, tmp_path):
        """Visualization referencing an existing metric is valid."""
        layout = self._make_layout(
            visualizations=[
                self._make_viz("viz1", measures=[("m_exists", "metric")]),
            ],
            metrics=[
                {
                    "id": "m_exists",
                    "title": "Existing Metric",
                    "content": {"maql": "SELECT 1"},
                },
            ],
        )

        db_path = self._export(mock_config, tmp_path, layout)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT is_valid FROM visualizations WHERE visualization_id = 'viz1'"
        )
        assert cursor.fetchone()[0] == 1

        conn.close()

    def test_invalid_missing_metric_reference(self, mock_config, tmp_path):
        """Visualization referencing a nonexistent metric is invalid."""
        layout = self._make_layout(
            visualizations=[
                self._make_viz("viz1", measures=[("m_missing", "metric")]),
            ],
        )

        db_path = self._export(mock_config, tmp_path, layout)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT is_valid FROM visualizations WHERE visualization_id = 'viz1'"
        )
        assert cursor.fetchone()[0] == 0

        conn.close()

    def test_valid_fact_reference(self, mock_config, tmp_path):
        """Visualization referencing an existing fact is valid."""
        layout = self._make_layout(
            visualizations=[
                self._make_viz("viz1", measures=[("revenue", "fact")]),
            ],
            datasets=[
                {
                    "id": "ds1",
                    "title": "DS1",
                    "attributes": [],
                    "facts": [
                        {
                            "id": "revenue",
                            "title": "Revenue",
                            "sourceColumn": "COL",
                            "sourceColumnDataType": "NUMERIC",
                        }
                    ],
                    "references": [],
                }
            ],
        )

        db_path = self._export(mock_config, tmp_path, layout)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT is_valid FROM visualizations WHERE visualization_id = 'viz1'"
        )
        assert cursor.fetchone()[0] == 1

        conn.close()

    def test_invalid_missing_fact_reference(self, mock_config, tmp_path):
        """Visualization referencing a nonexistent fact is invalid."""
        layout = self._make_layout(
            visualizations=[
                self._make_viz("viz1", measures=[("no_such_fact", "fact")]),
            ],
        )

        db_path = self._export(mock_config, tmp_path, layout)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT is_valid FROM visualizations WHERE visualization_id = 'viz1'"
        )
        assert cursor.fetchone()[0] == 0

        conn.close()

    def test_valid_label_reference_in_ldm_labels(self, mock_config, tmp_path):
        """Visualization referencing a label that exists in ldm_labels is valid."""
        layout = self._make_layout(
            visualizations=[
                self._make_viz("viz1", attributes=[("region.name", "label")]),
            ],
            datasets=[
                {
                    "id": "ds1",
                    "title": "DS1",
                    "attributes": [
                        {
                            "id": "region",
                            "title": "Region",
                            "sourceColumn": "COL",
                            "sourceColumnDataType": "STRING",
                            "labels": [
                                {
                                    "id": "region.name",
                                    "title": "Region Name",
                                    "sourceColumn": "NAME",
                                    "sourceColumnDataType": "STRING",
                                }
                            ],
                        }
                    ],
                    "facts": [],
                    "references": [],
                },
            ],
        )

        db_path = self._export(mock_config, tmp_path, layout)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT is_valid FROM visualizations WHERE visualization_id = 'viz1'"
        )
        assert cursor.fetchone()[0] == 1

        conn.close()

    def test_valid_label_reference_in_ldm_columns(self, mock_config, tmp_path):
        """Visualization referencing an attribute ID as label is valid.

        When {label/attr_id} is used, the attribute ID is in ldm_columns (type='attribute')
        but not in ldm_labels. The validation should accept it.
        """
        layout = self._make_layout(
            visualizations=[
                self._make_viz("viz1", attributes=[("status_code", "label")]),
            ],
            datasets=[
                {
                    "id": "ds1",
                    "title": "DS1",
                    "attributes": [
                        {
                            "id": "status_code",
                            "title": "Status Code",
                            "sourceColumn": "COL",
                            "sourceColumnDataType": "STRING",
                            # No labels defined - ID is only in ldm_columns
                        }
                    ],
                    "facts": [],
                    "references": [],
                },
            ],
        )

        db_path = self._export(mock_config, tmp_path, layout)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT is_valid FROM visualizations WHERE visualization_id = 'viz1'"
        )
        assert cursor.fetchone()[0] == 1

        conn.close()

    def test_invalid_missing_label_reference(self, mock_config, tmp_path):
        """Visualization referencing a nonexistent label is invalid."""
        layout = self._make_layout(
            visualizations=[
                self._make_viz("viz1", attributes=[("no_such_label", "label")]),
            ],
        )

        db_path = self._export(mock_config, tmp_path, layout)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT is_valid FROM visualizations WHERE visualization_id = 'viz1'"
        )
        assert cursor.fetchone()[0] == 0

        conn.close()

    def test_invalid_filter_label_reference(self, mock_config, tmp_path):
        """Visualization with a filter referencing a nonexistent label is invalid."""
        layout = self._make_layout(
            visualizations=[
                self._make_viz(
                    "viz1",
                    filters=[("missing_label", "label", "negative")],
                ),
            ],
        )

        db_path = self._export(mock_config, tmp_path, layout)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT is_valid FROM visualizations WHERE visualization_id = 'viz1'"
        )
        assert cursor.fetchone()[0] == 0

        conn.close()

    def test_invalid_attr_filter_config_label(self, mock_config, tmp_path):
        """Visualization with attributeFilterConfigs referencing nonexistent label is invalid."""
        layout = self._make_layout(
            visualizations=[
                self._make_viz(
                    "viz1",
                    attr_filter_configs=[("missing_label", "label")],
                ),
            ],
        )

        db_path = self._export(mock_config, tmp_path, layout)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT is_valid FROM visualizations WHERE visualization_id = 'viz1'"
        )
        assert cursor.fetchone()[0] == 0

        conn.close()

    def test_valid_attr_filter_config_label(self, mock_config, tmp_path):
        """Visualization with attributeFilterConfigs referencing existing label is valid."""
        layout = self._make_layout(
            visualizations=[
                self._make_viz(
                    "viz1",
                    attr_filter_configs=[("region.name", "label")],
                ),
            ],
            datasets=[
                {
                    "id": "ds1",
                    "title": "DS1",
                    "attributes": [
                        {
                            "id": "region",
                            "title": "Region",
                            "sourceColumn": "COL",
                            "sourceColumnDataType": "STRING",
                            "labels": [
                                {
                                    "id": "region.name",
                                    "title": "Region Name",
                                    "sourceColumn": "COL",
                                }
                            ],
                        }
                    ],
                    "facts": [],
                    "references": [],
                }
            ],
        )

        db_path = self._export(mock_config, tmp_path, layout)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT is_valid FROM visualizations WHERE visualization_id = 'viz1'"
        )
        assert cursor.fetchone()[0] == 1

        conn.close()

    def test_no_references_is_valid(self, mock_config, tmp_path):
        """Visualization with no references is valid."""
        layout = self._make_layout(
            visualizations=[
                {
                    "id": "viz_empty",
                    "title": "Empty Viz",
                    "content": {
                        "visualizationUrl": "local:table",
                        "buckets": [],
                        "filters": [],
                    },
                },
            ],
        )

        db_path = self._export(mock_config, tmp_path, layout)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT is_valid FROM visualizations WHERE visualization_id = 'viz_empty'"
        )
        assert cursor.fetchone()[0] == 1

        conn.close()

    def test_api_mode_is_valid_preserved(self, mock_config, tmp_path):
        """Visualization with areRelationsValid already set is not overwritten."""
        layout = self._make_layout(
            visualizations=[
                {
                    "id": "viz_api",
                    "title": "API Viz",
                    "areRelationsValid": True,
                    "content": {
                        "visualizationUrl": "local:table",
                        "buckets": [
                            {
                                "localIdentifier": "measures",
                                "items": [
                                    {
                                        "measure": {
                                            "localIdentifier": "m1",
                                            "definition": {
                                                "measureDefinition": {
                                                    "item": {
                                                        "identifier": {
                                                            "id": "m_missing",
                                                            "type": "metric",
                                                        }
                                                    }
                                                }
                                            },
                                            "title": "Missing",
                                        }
                                    }
                                ],
                            }
                        ],
                        "filters": [],
                    },
                },
            ],
        )

        db_path = self._export(mock_config, tmp_path, layout)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT is_valid FROM visualizations WHERE visualization_id = 'viz_api'"
        )
        # API set it to True (1), post-export should NOT overwrite it
        # even though the metric reference is missing
        assert cursor.fetchone()[0] == 1

        conn.close()

    def test_mixed_valid_and_invalid(self, mock_config, tmp_path):
        """Multiple visualizations get correct individual is_valid values."""
        layout = self._make_layout(
            visualizations=[
                self._make_viz("viz_good", measures=[("m_ok", "metric")]),
                self._make_viz("viz_bad", measures=[("m_gone", "metric")]),
                self._make_viz(
                    "viz_mixed",
                    measures=[("m_ok", "metric")],
                    attributes=[("no_such_label", "label")],
                ),
            ],
            metrics=[
                {
                    "id": "m_ok",
                    "title": "OK Metric",
                    "content": {"maql": "SELECT 1"},
                },
            ],
        )

        db_path = self._export(mock_config, tmp_path, layout)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT visualization_id, is_valid FROM visualizations ORDER BY visualization_id"
        )
        results = {row[0]: row[1] for row in cursor.fetchall()}

        assert results["viz_good"] == 1  # All references exist
        assert results["viz_bad"] == 0  # Missing metric
        assert results["viz_mixed"] == 0  # Valid metric but missing label

        conn.close()
