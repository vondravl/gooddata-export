"""Reachability semantics of v_objects_not_comparable, on a minimal SQLite fixture.

Focused on the ranking/measure-value dimension rows, whose pairing rule differs
from an axis attribute's: they scope to the measure their filter names, while an
axis attribute applies to the whole result and pairs with every measure.
"""

import sqlite3
from pathlib import Path

VIEW_SQL = (
    Path(__file__).resolve().parents[1]
    / "gooddata_export"
    / "sql"
    / "views"
    / "v_objects_not_comparable.sql"
)

WS = "ws1"

SCHEMA = """
CREATE TABLE ldm_reference_sources (dataset_id TEXT, reference_to_id TEXT);
CREATE TABLE ldm_columns (id TEXT, dataset_id TEXT, type TEXT);
CREATE TABLE ldm_labels (id TEXT, attribute_id TEXT, dataset_id TEXT);
CREATE TABLE ldm_datasets (id TEXT);
CREATE TABLE visualizations (visualization_id TEXT, workspace_id TEXT,
                             title TEXT, url_link TEXT);
CREATE TABLE visualizations_references (visualization_id TEXT, referenced_id TEXT,
                                        workspace_id TEXT, object_type TEXT,
                                        source TEXT, label TEXT,
                                        local_identifier TEXT);
CREATE TABLE visualizations_filters (visualization_id TEXT, workspace_id TEXT,
                                     filter_index INTEGER, filter_type TEXT,
                                     measure_local_identifier TEXT);
CREATE TABLE v_metrics_datasets_ancestry (metric_id TEXT, workspace_id TEXT,
                                          dataset_id TEXT, reference_type TEXT);
"""


def _db():
    """A two-measure visualization on two unrelated facts, plus one dimension.

    f_sales reaches dim_product; f_costs reaches nothing. So a dimension on
    dim_product is comparable to metric_revenue and not to metric_cost.
    """
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    con.execute("INSERT INTO ldm_reference_sources VALUES ('f_sales', 'dim_product')")
    con.execute(
        "INSERT INTO ldm_columns VALUES ('product', 'dim_product', 'attribute')"
    )
    con.execute("INSERT INTO ldm_datasets VALUES ('dim_product')")
    con.execute("INSERT INTO visualizations VALUES ('viz1', ?, 'Viz', NULL)", (WS,))
    con.executemany(
        "INSERT INTO v_metrics_datasets_ancestry VALUES (?, ?, ?, 'fact')",
        [("metric_revenue", WS, "f_sales"), ("metric_cost", WS, "f_costs")],
    )
    con.executemany(
        "INSERT INTO visualizations_references VALUES (?, ?, ?, ?, ?, NULL, ?)",
        [
            ("viz1", "metric_revenue", WS, "metric", "measure", "m_rev"),
            ("viz1", "metric_cost", WS, "metric", "measure", "m_cost"),
        ],
    )
    con.executescript(VIEW_SQL.read_text())
    return con


def _rows(con):
    return [dict(r) for r in con.execute("SELECT * FROM v_objects_not_comparable")]


def test_ranking_dimension_scopes_to_its_own_measure():
    """The sibling measure the filter does not rank by is never flagged.

    Regression guard: joining slice rows to every measure manufactured a
    violation for any measure that legitimately never touches the dimension.
    """
    con = _db()
    con.execute(
        "INSERT INTO visualizations_references VALUES "
        "('viz1', 'product', ?, 'attribute', 'rankingFilter', NULL, NULL)",
        (WS,),
    )
    con.execute(
        "INSERT INTO visualizations_filters VALUES ('viz1', ?, 0, 'rankingFilter', 'm_rev')",
        (WS,),
    )
    # metric_revenue reaches dim_product, so nothing is flagged at all.
    assert _rows(con) == []


def test_ranking_dimension_flags_the_ranked_measure_when_unreachable():
    con = _db()
    con.execute(
        "INSERT INTO visualizations_references VALUES "
        "('viz1', 'product', ?, 'attribute', 'rankingFilter', NULL, NULL)",
        (WS,),
    )
    # Ranked by the measure that CANNOT reach dim_product.
    con.execute(
        "INSERT INTO visualizations_filters VALUES ('viz1', ?, 0, 'rankingFilter', 'm_cost')",
        (WS,),
    )
    rows = _rows(con)
    assert len(rows) == 1
    assert rows[0]["measure_id"] == "metric_cost"
    assert rows[0]["slice_source"] == "rankingFilter"


def test_axis_attribute_still_pairs_with_every_measure():
    """An axis attribute applies to the whole result — the pre-existing rule."""
    con = _db()
    con.execute(
        "INSERT INTO visualizations_references VALUES "
        "('viz1', 'product', ?, 'label', 'attribute', NULL, 'a_prod')",
        (WS,),
    )
    rows = _rows(con)
    assert [r["measure_id"] for r in rows] == ["metric_cost"]


def test_attribute_typed_dimension_is_admitted():
    """object_type='attribute' is a legal AfmObjectIdentifier for a dimension."""
    con = _db()
    con.execute(
        "INSERT INTO visualizations_references VALUES "
        "('viz1', 'product', ?, 'attribute', 'rankingFilter', NULL, NULL)",
        (WS,),
    )
    con.execute(
        "INSERT INTO visualizations_filters VALUES ('viz1', ?, 0, 'rankingFilter', 'm_cost')",
        (WS,),
    )
    assert len(_rows(con)) == 1


def test_ambiguous_filter_count_drops_the_row():
    """Two filters of one type make the handle ambiguous — err to a false negative."""
    con = _db()
    con.execute(
        "INSERT INTO visualizations_references VALUES "
        "('viz1', 'product', ?, 'attribute', 'rankingFilter', NULL, NULL)",
        (WS,),
    )
    con.executemany(
        "INSERT INTO visualizations_filters VALUES ('viz1', ?, ?, 'rankingFilter', ?)",
        [(WS, 0, "m_cost"), (WS, 1, "m_rev")],
    )
    assert _rows(con) == []


def _db_axis():
    """Same fixture, but dim_product is unreachable from the only measure."""
    con = _db()
    con.execute(
        "DELETE FROM visualizations_references WHERE local_identifier = 'm_rev'"
    )
    return con


def test_direct_dimension_duplicating_an_axis_attribute_is_suppressed():
    """A dimension naming an object already on the axis must not report twice.

    The axis row wins — it is the broader claim, holding for every measure rather
    than only the ranked one.
    """
    con = _db_axis()
    con.execute(
        "INSERT INTO visualizations_references VALUES "
        "('viz1', 'product', ?, 'label', 'attribute', NULL, 'a_prod')",
        (WS,),
    )
    con.execute(
        "INSERT INTO visualizations_references VALUES "
        "('viz1', 'product', ?, 'attribute', 'rankingFilter', NULL, NULL)",
        (WS,),
    )
    con.execute(
        "INSERT INTO visualizations_filters VALUES ('viz1', ?, 0, 'rankingFilter', 'm_cost')",
        (WS,),
    )
    rows = _rows(con)
    assert len(rows) == 1
    assert rows[0]["slice_source"] == "attribute"


def test_bucket_handle_dimension_is_not_double_counted():
    """A bucket-handle dimension row exists for inventory but never adds a hit."""
    con = _db_axis()
    con.execute(
        "INSERT INTO visualizations_references VALUES "
        "('viz1', 'product', ?, 'label', 'attribute', NULL, 'a_prod')",
        (WS,),
    )
    # The inventory row the extractor now emits for a bucket handle.
    con.execute(
        "INSERT INTO visualizations_references VALUES "
        "('viz1', 'product', ?, 'label', 'rankingFilter', NULL, 'a_prod')",
        (WS,),
    )
    con.execute(
        "INSERT INTO visualizations_filters VALUES ('viz1', ?, 0, 'rankingFilter', 'm_cost')",
        (WS,),
    )
    rows = _rows(con)
    assert len(rows) == 1
    assert rows[0]["slice_source"] == "attribute"
