"""Tests for gooddata_export.export.utils."""

import csv

from gooddata_export.export.utils import is_constraint_clause, write_to_csv


def test_is_constraint_clause():
    """Table-constraint pseudo-keys are detected; real columns are not."""
    assert is_constraint_clause("PRIMARY KEY")
    assert is_constraint_clause("FOREIGN KEY (dataset_id, reference_id)")
    assert is_constraint_clause("UNIQUE (id)")
    assert not is_constraint_clause("dataset_id")
    assert not is_constraint_clause("source_column")


def test_write_to_csv_drops_constraint_clauses(tmp_path):
    """Schema-dict constraint keys must not leak into the CSV header.

    Callers pass a setup_table schema dict's keys() as fieldnames; those keys
    mix data columns with constraints like 'PRIMARY KEY'. The CSV should only
    contain the data columns.
    """
    schema = {
        "dataset_id": "TEXT",
        "reference_id": "TEXT",
        "ordinal": "INTEGER",
        "PRIMARY KEY": "(dataset_id, reference_id, ordinal)",
        "FOREIGN KEY (dataset_id, reference_id)": "REFERENCES ldm_columns(dataset_id, id)",
    }
    rows = [{"dataset_id": "fact", "reference_id": "a__ref__b", "ordinal": 0}]

    write_to_csv(rows, str(tmp_path), "out.csv", fieldnames=schema.keys())

    with open(tmp_path / "out.csv", encoding="utf-8-sig") as f:
        header = next(csv.reader(f))

    assert header == ["dataset_id", "reference_id", "ordinal"]
    assert "PRIMARY KEY" not in header
    assert not any(h.startswith("FOREIGN KEY") for h in header)
