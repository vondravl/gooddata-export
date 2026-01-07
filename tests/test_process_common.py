"""Tests for gooddata_export/process/common.py utilities."""

from gooddata_export.process.common import sort_tags


class TestSortTags:
    """Tests for the sort_tags function."""

    def test_basic_sorting(self):
        """Sorts tag list alphabetically."""
        tags = ["zebra", "apple", "mango"]
        result = sort_tags(tags)
        assert result == ["apple", "mango", "zebra"]

    def test_empty_list(self):
        """Empty list returns empty list."""
        result = sort_tags([])
        assert result == []

    def test_none_returns_none(self):
        """None input returns None."""
        result = sort_tags(None)
        assert result is None

    def test_non_list_returns_unchanged(self):
        """Non-list input (e.g., string) returns unchanged."""
        result = sort_tags("not-a-list")
        assert result == "not-a-list"

    def test_single_element(self):
        """Single element list returns as-is."""
        result = sort_tags(["only"])
        assert result == ["only"]

    def test_already_sorted(self):
        """Already sorted list returns same order."""
        tags = ["a", "b", "c"]
        result = sort_tags(tags)
        assert result == ["a", "b", "c"]

    def test_case_sensitive(self):
        """Sorting is case-sensitive (uppercase comes before lowercase)."""
        tags = ["banana", "Apple", "cherry"]
        result = sort_tags(tags)
        # 'A' < 'b' < 'c' in ASCII
        assert result == ["Apple", "banana", "cherry"]
