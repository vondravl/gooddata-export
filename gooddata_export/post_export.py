"""
Post-export processing functions for GoodData metadata export.
These functions run after data export to enhance or modify the exported data.

Configuration is loaded from sql/post_export_config.yaml and executed in
dependency order using topological sort.
"""

import logging
import re
import sqlite3
from collections import defaultdict, deque
from pathlib import Path

import yaml

from gooddata_export.common import ExportError
from gooddata_export.config import ExportConfig
from gooddata_export.db import connect_database

logger = logging.getLogger(__name__)


def populate_metrics_references(cursor):
    """Populate metrics_references table by extracting all references from MAQL.

    This requires Python regex since SQLite doesn't support regex extraction.
    The table must already exist (created via SQL file in post_export_config.yaml).

    Patterns matched:
        {metric/metric_id} - stored with reference_type='metric' (self-references excluded)
        {attr/attribute_id} - stored with reference_type='attribute'
        {label/label_id} - stored with reference_type='label'
        {fact/fact_id} - stored with reference_type='fact'

    Note: Lines starting with # are MAQL comments and are filtered out before extraction.
    """
    cursor.execute("""
        SELECT metric_id, workspace_id, maql
        FROM metrics
        WHERE maql IS NOT NULL AND workspace_id IS NOT NULL
    """)

    metric_pattern = re.compile(r"\{metric/([^}]+)\}")
    attr_pattern = re.compile(r"\{attr/([^}]+)\}")
    label_pattern = re.compile(r"\{label/([^}]+)\}")
    fact_pattern = re.compile(r"\{fact/([^}]+)\}")
    references = []

    for row in cursor.fetchall():
        source_metric_id, workspace_id, maql = row

        # Filter out commented lines (MAQL comments start with #)
        active_maql = "\n".join(
            line for line in maql.split("\n") if not line.lstrip().startswith("#")
        )

        # Extract metric references (exclude self-references)
        for ref_metric_id in metric_pattern.findall(active_maql):
            if ref_metric_id != source_metric_id:
                references.append(
                    (source_metric_id, workspace_id, ref_metric_id, "metric")
                )

        # Extract attribute references
        for attr_id in attr_pattern.findall(active_maql):
            references.append((source_metric_id, workspace_id, attr_id, "attribute"))

        # Extract label references
        for label_id in label_pattern.findall(active_maql):
            references.append((source_metric_id, workspace_id, label_id, "label"))

        # Extract fact references
        for fact_id in fact_pattern.findall(active_maql):
            references.append((source_metric_id, workspace_id, fact_id, "fact"))

    cursor.executemany(
        """
        INSERT OR IGNORE INTO metrics_references
        (source_metric_id, source_workspace_id, referenced_id, reference_type)
        VALUES (?, ?, ?, ?)
    """,
        references,
    )

    logger.debug(
        "Populated metrics_references table with %d references",
        len(references),
    )


# Registry of Python populate functions that can be called from YAML config
# These are used for operations that require Python (e.g., regex extraction)
PYTHON_POPULATE_FUNCTIONS = {
    "populate_metrics_references": populate_metrics_references,
}


def _log_post_export_failure(error_msg: str) -> None:
    """Log post-export failure at INFO level so users see it without --debug."""
    logger.info("")
    logger.info("=" * 70)
    logger.info("POST-EXPORT PROCESSING FAILED")
    logger.info("=" * 70)
    logger.info("Error: %s", error_msg)


def load_post_export_config():
    """Load post-export configuration from YAML file.

    Returns:
        dict: Configuration with 'views' and 'updates' sections
    """
    config_path = Path(__file__).parent / "sql" / "post_export_config.yaml"

    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def topological_sort(items_dict):
    """Sort items by dependencies using topological sort (Kahn's algorithm).

    Args:
        items_dict: Dictionary where each item has a 'dependencies' list

    Returns:
        list: Ordered list of item names (keys from items_dict)

    Raises:
        ValueError: If circular dependencies are detected
    """
    # Build adjacency list and in-degree count
    graph = defaultdict(list)
    in_degree = defaultdict(int)
    all_items = set(items_dict.keys())

    # Initialize in-degree for all items
    for item in all_items:
        in_degree[item] = 0

    # Build graph
    for item, config in items_dict.items():
        dependencies = config.get("dependencies", [])
        for dep in dependencies:
            if dep not in all_items:
                raise ValueError(
                    f"Item '{item}' depends on '{dep}' which doesn't exist in configuration"
                )
            graph[dep].append(item)
            in_degree[item] += 1

    # Find all items with no dependencies (in-degree = 0)
    queue = deque([item for item in all_items if in_degree[item] == 0])
    result = []

    # Process queue
    while queue:
        # Sort to ensure deterministic order when multiple items have no dependencies
        current = sorted(queue)[0]
        queue.remove(current)
        result.append(current)

        # Reduce in-degree for dependent items
        for dependent in graph[current]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    # Check for circular dependencies
    if len(result) != len(all_items):
        remaining = all_items - set(result)
        raise ValueError(f"Circular dependency detected. Cannot process: {remaining}")

    return result


def substitute_parameters(sql_script, parameters, config):
    """Substitute parameters in SQL script.

    Args:
        sql_script: The SQL script content
        parameters: Dict of parameter definitions from config
        config: ExportConfig instance for accessing runtime values

    Returns:
        str: SQL script with substituted parameters
    """
    if not parameters:
        return sql_script

    result = sql_script
    for param_name, param_template in parameters.items():
        # Handle special template syntax
        if param_template.startswith("{{") and param_template.endswith("}}"):
            # {{WORKSPACE_ID}} -> get actual value from config
            config_key = param_template[2:-2].strip()
            if hasattr(config, config_key):
                value = getattr(config, config_key)
                result = result.replace(f"{{{param_name}}}", str(value))
                logger.debug("  Substituted {%s} with %s", param_name, value)
            else:
                logger.warning(
                    "  Config key %s not found, skipping substitution", config_key
                )
        elif param_template.startswith("$$"):
            # $${TOKEN_GOODDATA_DEV} -> replace with ${TOKEN_GOODDATA_DEV} (literal string, remove one $)
            value = param_template[1:]  # Remove one $ to get ${...}
            result = result.replace(f"{{{param_name}}}", value)
            logger.debug("  Substituted {%s} with literal %s", param_name, value)
        else:
            # Direct string substitution
            result = result.replace(f"{{{param_name}}}", param_template)
            logger.debug("  Substituted {%s} with %s", param_name, param_template)

    return result


def execute_sql_file(
    cursor,
    sql_path,
    parameters=None,
    config=None,
    parent_workspace_id: str | None = None,
):
    """Execute a SQL file with optional parameter substitution.

    Args:
        cursor: Database cursor
        sql_path: Path to SQL file
        parameters: Optional dict of parameters to substitute
        config: Optional ExportConfig instance for parameter values
        parent_workspace_id: Optional workspace ID for {parent_workspace_filter} substitution

    Returns:
        bool: True if successful, False otherwise
    """
    sql_path = Path(sql_path)
    if not sql_path.exists():
        logger.warning("SQL file not found: %s", sql_path)
        return False

    logger.debug("  Executing: %s", sql_path.name)

    with open(sql_path, "r") as f:
        sql_script = f.read()

    # Perform parameter substitution if needed
    if parameters and config:
        sql_script = substitute_parameters(sql_script, parameters, config)

    # Substitute {parent_workspace_filter} placeholder for workspace-scoped updates
    if parent_workspace_id:
        # Add workspace filter when parent_workspace_id is provided
        workspace_filter = f"AND workspace_id = '{parent_workspace_id}'"
        sql_script = sql_script.replace("{parent_workspace_filter}", workspace_filter)
    else:
        # Remove the placeholder when no filter needed (single workspace export)
        sql_script = sql_script.replace("{parent_workspace_filter}", "")

    try:
        # Try to execute the entire script as a single transaction
        cursor.executescript(sql_script)
        return True
    except sqlite3.OperationalError:
        # If that fails, fall back to executing statements individually
        logger.debug("  Executing statement by statement...")
        statements = sql_script.split(";")
        for statement in statements:
            if statement.strip():
                try:
                    cursor.execute(statement)
                except sqlite3.Error as stmt_error:
                    logger.error("  Error: %s", stmt_error)
                    logger.debug("  Statement: %s...", statement[:100])
                    raise
        return True
    except Exception as e:
        logger.error("Error executing %s: %s", sql_path.name, e)
        return False


def ensure_columns_exist(cursor, table_name, required_columns):
    """Ensure required columns exist in a table, adding them if necessary.

    Args:
        cursor: Database cursor
        table_name: Name of the table
        required_columns: Dict of {column_name: column_type}
    """
    if not required_columns:
        return

    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = [column[1] for column in cursor.fetchall()]

    for column_name, column_type in required_columns.items():
        if column_name not in existing_columns:
            logger.debug("  Adding column: %s.%s", table_name, column_name)
            cursor.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
            )


def run_post_export_sql(db_path, parent_workspace_id: str | None = None) -> None:
    """Run all post-export SQL operations on the database.

    This is the main entry point for post-export processing.
    Operations are executed in dependency order using topological sort.

    Args:
        db_path: Path to the SQLite database
        parent_workspace_id: Optional workspace ID to filter updates to.
            When provided, UPDATE statements only affect rows for this workspace.
            Used in multi-workspace exports to enrich only the parent workspace.

    Raises:
        ExportError: If post-export processing fails
    """
    logger.debug("")
    logger.debug("=" * 70)
    logger.debug("POST-EXPORT PROCESSING")
    logger.debug("=" * 70)

    try:
        # Load configuration
        yaml_config = load_post_export_config()
        sql_dir = Path(__file__).parent / "sql"

        # Load export config for parameter substitution
        export_config = ExportConfig(load_from_env=True)

        # Connect to database
        conn = connect_database(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Combine tables, views, updates, and procedures for dependency sorting
        all_items = {}
        if "tables" in yaml_config:
            all_items.update(yaml_config["tables"])
        if "views" in yaml_config:
            all_items.update(yaml_config["views"])
        if "updates" in yaml_config:
            all_items.update(yaml_config["updates"])
        if "procedures" in yaml_config:
            all_items.update(yaml_config["procedures"])

        # Sort by dependencies
        try:
            execution_order = topological_sort(all_items)
        except ValueError as e:
            error_msg = f"Configuration error: {e}"
            _log_post_export_failure(error_msg)
            raise ExportError(error_msg) from e

        logger.debug(
            "Executing %d operations in dependency order...", len(execution_order)
        )

        # Execute each operation in order
        success_count = 0
        for item_name in execution_order:
            item_config = all_items[item_name]

            # Determine type: table, view, update, or procedure
            is_table = item_name in yaml_config.get("tables", {})
            is_view = item_name in yaml_config.get("views", {})
            is_update = item_name in yaml_config.get("updates", {})
            is_procedure = item_name in yaml_config.get("procedures", {})

            if is_procedure:
                item_type = "PROCEDURE"
            elif is_table:
                item_type = "TABLE"
            elif is_view:
                item_type = "VIEW"
            else:
                item_type = "UPDATE"

            category = item_config.get("category", "unknown")

            logger.debug("\n[%s] %s (%s)", item_type, item_name, category)
            logger.debug("  Description: %s", item_config.get("description", "N/A"))

            # Show dependencies if any
            deps = item_config.get("dependencies", [])
            if deps:
                logger.debug("  Dependencies: %s", ", ".join(deps))

            # For updates, ensure required columns exist
            if is_update:
                table_name = item_config.get("table")
                required_columns = item_config.get("required_columns", {})
                if table_name and required_columns:
                    ensure_columns_exist(cursor, table_name, required_columns)

            # Execute SQL file
            sql_file = item_config["sql_file"]
            sql_path = sql_dir / sql_file

            # Get parameters for procedure items
            parameters = item_config.get("parameters", {}) if is_procedure else None

            # Pass parent_workspace_id only for UPDATE operations (not tables/views)
            workspace_filter = parent_workspace_id if is_update else None

            if execute_sql_file(
                cursor,
                sql_path,
                parameters=parameters,
                config=export_config,
                parent_workspace_id=workspace_filter,
            ):
                # Run Python populate function if specified (for tables needing regex)
                python_populate = item_config.get("python_populate")
                if python_populate:
                    populate_func = PYTHON_POPULATE_FUNCTIONS.get(python_populate)
                    if populate_func:
                        logger.debug("  Running Python populate: %s", python_populate)
                        populate_func(cursor)
                    else:
                        logger.warning(
                            "  Unknown python_populate function: %s", python_populate
                        )

                success_count += 1
                logger.debug("  ✓ Success")
            else:
                error_msg = f"Failed to execute {item_name}"
                conn.rollback()
                conn.close()
                _log_post_export_failure(error_msg)
                raise ExportError(error_msg)

        # Commit all changes
        conn.commit()
        conn.close()

        logger.debug("")
        logger.debug("=" * 70)
        logger.debug(
            "ALL OPERATIONS COMPLETED SUCCESSFULLY (%d/%d)",
            success_count,
            len(execution_order),
        )
        logger.debug("=" * 70)

        # Simple info message for regular users
        table_count = len(yaml_config.get("tables", {}))
        view_count = len(yaml_config.get("views", {}))
        update_count = len(yaml_config.get("updates", {}))
        procedure_count = len(yaml_config.get("procedures", {}))

        logger.debug(
            "Successfully created %d tables, %d views, %d procedures, and %d table updates",
            table_count,
            view_count,
            procedure_count,
            update_count,
        )
        logger.debug("")

    except ExportError:
        # Re-raise ExportError as-is (already logged)
        raise
    except Exception as e:
        error_msg = f"Error during post-export processing: {e}"
        _log_post_export_failure(error_msg)
        raise ExportError(error_msg) from e
