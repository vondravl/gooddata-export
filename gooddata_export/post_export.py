"""
Post-export processing functions for GoodData metadata export.
These functions run after data export to enhance or modify the exported data.

Configuration is loaded from sql/post_export_config.yaml and executed in
dependency order using topological sort.
"""

import os
import sqlite3
import yaml
import logging
from collections import defaultdict, deque
from gooddata_export.db import connect_database

logger = logging.getLogger(__name__)


def load_post_export_config():
    """Load post-export configuration from YAML file.
    
    Returns:
        dict: Configuration with 'views' and 'updates' sections
    """
    config_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "sql",
        "post_export_config.yaml"
    )
    
    with open(config_path, 'r') as f:
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
        dependencies = config.get('dependencies', [])
        for dep in dependencies:
            if dep not in all_items:
                raise ValueError(f"Item '{item}' depends on '{dep}' which doesn't exist in configuration")
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


def execute_sql_file(cursor, sql_path):
    """Execute a SQL file.
    
    Args:
        cursor: Database cursor
        sql_path: Path to SQL file
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not os.path.exists(sql_path):
        logger.warning(f"SQL file not found: {sql_path}")
        return False
    
    logger.debug(f"  Executing: {os.path.basename(sql_path)}")
    
    with open(sql_path, 'r') as f:
        sql_script = f.read()
    
    try:
        # Try to execute the entire script as a single transaction
        cursor.executescript(sql_script)
        return True
    except sqlite3.OperationalError:
        # If that fails, fall back to executing statements individually
        logger.debug("  Executing statement by statement...")
        statements = sql_script.split(';')
        for statement in statements:
            if statement.strip():
                try:
                    cursor.execute(statement)
                except sqlite3.Error as stmt_error:
                    logger.error(f"  Error: {stmt_error}")
                    logger.debug(f"  Statement: {statement[:100]}...")
                    raise
        return True
    except Exception as e:
        logger.error(f"Error executing {os.path.basename(sql_path)}: {str(e)}")
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
            logger.debug(f"  Adding column: {table_name}.{column_name}")
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def run_post_export_sql(db_path):
    """Run all post-export SQL operations on the database.
    
    This is the main entry point for post-export processing.
    Operations are executed in dependency order using topological sort.
    
    Args:
        db_path: Path to the SQLite database
        
    Returns:
        bool: True if all operations successful, False otherwise
    """
    logger.debug("="*70)
    logger.debug("POST-EXPORT PROCESSING")
    logger.debug("="*70)
    
    try:
        # Load configuration
        config = load_post_export_config()
        sql_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "sql"
        )
        
        # Connect to database
        conn = connect_database(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Combine views and updates for dependency sorting
        all_items = {}
        if 'views' in config:
            all_items.update(config['views'])
        if 'updates' in config:
            all_items.update(config['updates'])
        
        # Sort by dependencies
        try:
            execution_order = topological_sort(all_items)
        except ValueError as e:
            logger.error(f"Configuration error: {e}")
            return False
        
        logger.debug(f"Executing {len(execution_order)} operations in dependency order:")
        logger.debug("-" * 70)
        
        # Execute each operation in order
        success_count = 0
        for item_name in execution_order:
            item_config = all_items[item_name]
            
            # Determine if it's a view or update
            is_view = item_name in config.get('views', {})
            item_type = "VIEW" if is_view else "UPDATE"
            category = item_config.get('category', 'unknown')
            
            logger.debug(f"\n[{item_type}] {item_name} ({category})")
            logger.debug(f"  Description: {item_config.get('description', 'N/A')}")
            
            # Show dependencies if any
            deps = item_config.get('dependencies', [])
            if deps:
                logger.debug(f"  Dependencies: {', '.join(deps)}")
            
            # For updates, ensure required columns exist
            if not is_view:
                table_name = item_config.get('table')
                required_columns = item_config.get('required_columns', {})
                if table_name and required_columns:
                    ensure_columns_exist(cursor, table_name, required_columns)
            
            # Execute SQL file
            sql_file = item_config['sql_file']
            sql_path = os.path.join(sql_dir, sql_file)
            
            if execute_sql_file(cursor, sql_path):
                success_count += 1
                logger.debug("  ✓ Success")
            else:
                logger.error(f"Failed to execute {item_name}")
                conn.rollback()
                conn.close()
                return False
        
        # Commit all changes
        conn.commit()
        conn.close()
        
        logger.debug("="*70)
        logger.debug(f"ALL OPERATIONS COMPLETED SUCCESSFULLY ({success_count}/{len(execution_order)})")
        logger.debug("="*70)
        
        # Simple info message for regular users
        view_count = len(config.get('views', {}))
        update_count = len(config.get('updates', {}))
        
        logger.info(f"Successfully created {view_count} views and {update_count} table updates in database")
        
        return True
        
    except Exception as e:
        logger.error(f"Error during post-export processing: {str(e)}")
        return False
