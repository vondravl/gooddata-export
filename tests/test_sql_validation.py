"""Tests for SQL file integrity and configuration validation.

These tests ensure that:
- The YAML configuration is valid and can be loaded
- All SQL files referenced in the config exist
- There are no circular dependencies
- All dependencies reference existing items
"""

from pathlib import Path

from gooddata_export.post_export import load_post_export_config, topological_sort


def test_yaml_config_loads():
    """post_export_config.yaml loads without error."""
    config = load_post_export_config()
    assert config is not None
    assert isinstance(config, dict)
    # Should have at least one of the main sections
    assert any(key in config for key in ["tables", "views", "updates", "procedures"])


def test_yaml_config_has_required_keys():
    """Each item in config has required 'sql_file' key."""
    config = load_post_export_config()

    for section in ["tables", "views", "updates", "procedures"]:
        items = config.get(section, {})
        for name, item_config in items.items():
            assert "sql_file" in item_config, f"{section}/{name} missing 'sql_file' key"


def test_yaml_config_no_circular_deps():
    """Config has no circular dependencies."""
    config = load_post_export_config()

    # Combine all items
    all_items = {}
    for section in ["tables", "views", "updates", "procedures"]:
        all_items.update(config.get(section, {}))

    # Should not raise ValueError for circular dependencies
    result = topological_sort(all_items)
    assert len(result) == len(all_items)


def test_yaml_all_dependencies_exist():
    """All dependency references exist in config."""
    config = load_post_export_config()

    # Combine all items
    all_items = {}
    for section in ["tables", "views", "updates", "procedures"]:
        all_items.update(config.get(section, {}))

    all_names = set(all_items.keys())

    for name, item_config in all_items.items():
        deps = item_config.get("dependencies", [])
        for dep in deps:
            assert dep in all_names, (
                f"Item '{name}' depends on '{dep}' which doesn't exist"
            )


def test_all_sql_files_exist():
    """All SQL files referenced in config exist on disk."""
    config = load_post_export_config()
    sql_dir = Path(__file__).parent.parent / "gooddata_export" / "sql"

    missing = []
    for section in ["tables", "views", "updates", "procedures"]:
        items = config.get(section, {})
        for name, item_config in items.items():
            sql_file = sql_dir / item_config["sql_file"]
            if not sql_file.exists():
                missing.append(f"{section}/{name}: {item_config['sql_file']}")

    assert not missing, f"Missing SQL files: {missing}"


def test_all_sql_files_valid_syntax():
    """All SQL files have valid syntax (basic check - files are readable and non-empty)."""
    config = load_post_export_config()
    sql_dir = Path(__file__).parent.parent / "gooddata_export" / "sql"

    errors = []
    for section in ["tables", "views", "updates", "procedures"]:
        items = config.get(section, {})
        for name, item_config in items.items():
            sql_file = sql_dir / item_config["sql_file"]
            if sql_file.exists():
                sql_content = sql_file.read_text()
                # Basic validation: file should be non-empty and contain SQL keywords
                if not sql_content.strip():
                    errors.append(f"{section}/{name}: SQL file is empty")
                elif not any(
                    keyword in sql_content.upper()
                    for keyword in [
                        "SELECT",
                        "CREATE",
                        "DROP",
                        "UPDATE",
                        "INSERT",
                        "ALTER",
                    ]
                ):
                    errors.append(
                        f"{section}/{name}: SQL file doesn't contain recognizable SQL keywords"
                    )

    assert not errors, f"SQL file issues: {errors}"
