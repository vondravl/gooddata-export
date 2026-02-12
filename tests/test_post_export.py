"""Tests for post_export.py - topological sort and parameter substitution."""

import pytest

from gooddata_export.post_export import substitute_parameters, topological_sort


class TestTopologicalSort:
    """Tests for the topological_sort function."""

    def test_linear_chain(self):
        """Linear A->B->C resolves to [A, B, C]."""
        items = {
            "A": {"dependencies": []},
            "B": {"dependencies": ["A"]},
            "C": {"dependencies": ["B"]},
        }
        result = topological_sort(items)
        assert result == ["A", "B", "C"]

    def test_diamond_dependency(self):
        """Diamond dependency (A->B,C; B->D; C->D) resolves correctly."""
        items = {
            "A": {"dependencies": []},
            "B": {"dependencies": ["A"]},
            "C": {"dependencies": ["A"]},
            "D": {"dependencies": ["B", "C"]},
        }
        result = topological_sort(items)

        # A must come first
        assert result[0] == "A"
        # D must come last
        assert result[-1] == "D"
        # B and C must come after A and before D
        assert result.index("B") > result.index("A")
        assert result.index("C") > result.index("A")
        assert result.index("B") < result.index("D")
        assert result.index("C") < result.index("D")

    def test_no_dependencies(self):
        """Items with no dependencies are sorted alphabetically."""
        items = {
            "C": {"dependencies": []},
            "A": {"dependencies": []},
            "B": {"dependencies": []},
        }
        result = topological_sort(items)
        # With no dependencies, should be sorted alphabetically
        assert result == ["A", "B", "C"]

    def test_empty_dict(self):
        """Empty dict returns empty list."""
        result = topological_sort({})
        assert result == []

    def test_circular_raises(self):
        """Circular dependency A->B->C->A raises ValueError."""
        items = {
            "A": {"dependencies": ["C"]},
            "B": {"dependencies": ["A"]},
            "C": {"dependencies": ["B"]},
        }
        with pytest.raises(ValueError, match="Circular dependency"):
            topological_sort(items)

    def test_missing_dependency_raises(self):
        """Reference to non-existent dependency raises ValueError."""
        items = {
            "A": {"dependencies": ["nonexistent"]},
        }
        with pytest.raises(ValueError, match="doesn't exist"):
            topological_sort(items)

    def test_self_dependency_raises(self):
        """Self-referencing dependency raises ValueError."""
        items = {
            "A": {"dependencies": ["A"]},
        }
        with pytest.raises(ValueError, match="Circular dependency"):
            topological_sort(items)


class TestSubstituteParameters:
    """Tests for the substitute_parameters function."""

    def test_literal_dollar_substitution(self):
        """$${VAR} becomes ${VAR} (literal variable)."""
        sql = "token='{token}'"
        parameters = {"token": "$${TOKEN_VAR}"}

        result = substitute_parameters(sql, parameters)
        assert "${TOKEN_VAR}" in result
        assert "$$" not in result

    def test_direct_string_substitution(self):
        """Direct string values are substituted as-is."""
        sql = "value='{custom}'"
        parameters = {"custom": "my-direct-value"}

        result = substitute_parameters(sql, parameters)
        assert "my-direct-value" in result

    def test_no_parameters(self):
        """Empty parameters returns unchanged SQL."""
        sql = "SELECT * FROM table"
        result = substitute_parameters(sql, None)
        assert result == sql

        result = substitute_parameters(sql, {})
        assert result == sql
