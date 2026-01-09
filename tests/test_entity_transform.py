"""Tests for entity to layout transformation functions."""

from gooddata_export.process.entities import (
    entity_to_layout,
    transform_entities_to_layout,
)


class TestEntityToLayout:
    """Tests for the entity_to_layout function."""

    def test_basic_transformation(self):
        """Transforms entity format to layout format."""
        entity = {
            "id": "metric_1",
            "attributes": {
                "title": "My Metric",
                "description": "A description",
                "tags": ["tag1", "tag2"],
                "content": {"maql": "SELECT SUM(amount)"},
                "createdAt": "2024-01-01T00:00:00Z",
                "modifiedAt": "2024-01-02T00:00:00Z",
                "areRelationsValid": True,
                "isHidden": False,
            },
            "meta": {"origin": {"originType": "NATIVE"}},
        }
        result = entity_to_layout(entity)

        assert result["id"] == "metric_1"
        assert result["title"] == "My Metric"
        assert result["description"] == "A description"
        assert result["tags"] == ["tag1", "tag2"]
        assert result["content"] == {"maql": "SELECT SUM(amount)"}
        assert result["createdAt"] == "2024-01-01T00:00:00Z"
        assert result["modifiedAt"] == "2024-01-02T00:00:00Z"
        assert result["areRelationsValid"] is True
        assert result["isHidden"] is False
        assert result["originType"] == "NATIVE"

    def test_missing_attributes_uses_defaults(self):
        """Missing attributes get default values."""
        entity = {"id": "metric_1", "attributes": {}, "meta": {}}
        result = entity_to_layout(entity)

        assert result["id"] == "metric_1"
        assert result["title"] == ""
        assert result["description"] == ""
        assert result["tags"] == []
        assert result["content"] == {}
        assert result["createdAt"] == ""
        assert result["modifiedAt"] == ""
        assert result["areRelationsValid"] is True
        assert result["isHidden"] is False
        assert result["originType"] == "NATIVE"

    def test_missing_meta_section(self):
        """Missing meta section uses defaults."""
        entity = {
            "id": "metric_1",
            "attributes": {"title": "Test"},
        }
        result = entity_to_layout(entity)

        assert result["originType"] == "NATIVE"

    def test_null_tags_becomes_empty_list(self):
        """Null tags are converted to empty list."""
        entity = {
            "id": "metric_1",
            "attributes": {"title": "Test", "tags": None},
        }
        result = entity_to_layout(entity)

        assert result["tags"] == []

    def test_inherited_origin_type(self):
        """Preserves PARENT origin type."""
        entity = {
            "id": "metric_1",
            "attributes": {"title": "Test"},
            "meta": {"origin": {"originType": "PARENT"}},
        }
        result = entity_to_layout(entity)

        assert result["originType"] == "PARENT"

    def test_modified_at_falls_back_to_created_at(self):
        """Missing modifiedAt falls back to createdAt."""
        entity = {
            "id": "metric_1",
            "attributes": {
                "title": "Test",
                "createdAt": "2024-01-01T00:00:00Z",
            },
        }
        result = entity_to_layout(entity)

        assert result["createdAt"] == "2024-01-01T00:00:00Z"
        assert result["modifiedAt"] == "2024-01-01T00:00:00Z"


class TestTransformEntitiesToLayout:
    """Tests for the transform_entities_to_layout function."""

    def test_transforms_list(self):
        """Transforms list of entities."""
        entities = [
            {"id": "m1", "attributes": {"title": "Metric 1"}},
            {"id": "m2", "attributes": {"title": "Metric 2"}},
        ]
        result = transform_entities_to_layout(entities)

        assert len(result) == 2
        assert result[0]["id"] == "m1"
        assert result[0]["title"] == "Metric 1"
        assert result[1]["id"] == "m2"
        assert result[1]["title"] == "Metric 2"

    def test_empty_list(self):
        """Empty list returns empty list."""
        result = transform_entities_to_layout([])
        assert result == []
