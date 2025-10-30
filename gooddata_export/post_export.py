"""
Post-export processing functions for GoodData metadata export.
These functions run after data export to enhance or modify the exported data.
"""

import os
import sqlite3
from gooddata_export.db import connect_database


# Post-export configuration: defines SQL scripts and required columns for each table
POST_EXPORT_CONFIG = {
    "visualizations": {
        "sql_scripts": [
            "updates/visuals_with_same_content.sql",
            "updates/visualizations_usage_check.sql",
            "views/v_visualization_usage.sql",
        ],
        "required_columns": {
            "columns": "TEXT",
            "same_columns_id": "INTEGER",
            "same_visuals_id": "INTEGER",
            "same_visuals_id_with_tags": "INTEGER",
            "is_used": "INTEGER DEFAULT 0",
        }
    },
    "metrics": {
        "sql_scripts": [
            "updates/metrics_probable_duplicates.sql",
            "updates/metrics_usage_check.sql",
            "views/v_metric_usage.sql",
            "views/v_metric_dependencies.sql",
        ],
        "required_columns": {
            "similar_metric_id": "INTEGER",
            "is_used_insight": "INTEGER DEFAULT 0",
            "is_used_maql": "INTEGER DEFAULT 0",
        }
    },
}


def run_post_export_sql(db_path):
    """Run all post-export SQL operations on the database.
    This is the main entry point for post-export processing.
    """
    success = True
    
    # Process each table defined in POST_EXPORT_CONFIG
    for table_name in POST_EXPORT_CONFIG.keys():
        if not post_export_table(db_path, table_name):
            success = False
            print(f"Warning: Failed to process {table_name}")
    
    return success


def post_export_table(db_path, table_name):
    """Generic function to run SQL post-processing for a specific table.
    
    Args:
        db_path: Path to the SQLite database
        table_name: Name of the table to process (must exist in POST_EXPORT_CONFIG)
    
    Returns:
        bool: True if successful, False otherwise
    """
    if table_name not in POST_EXPORT_CONFIG:
        print(f"Error: No configuration found for table '{table_name}'")
        return False
    
    config = POST_EXPORT_CONFIG[table_name]
    sql_scripts = config["sql_scripts"]
    required_columns = config["required_columns"]
    
    # Base directory for SQL scripts
    sql_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "sql"
    )
    
    # Connect to database once for all scripts
    conn = connect_database(db_path)
    conn.row_factory = sqlite3.Row
    
    try:
        cursor = conn.cursor()
        
        # Check if required columns exist and create if not
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing_columns = [column[1] for column in cursor.fetchall()]

        for column_name, column_type in required_columns.items():
            if column_name not in existing_columns:
                print(f"Adding {column_name} column to {table_name} table")
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
        
        # Process each SQL script
        for script_name in sql_scripts:
            script_path = os.path.join(sql_dir, script_name)
            
            if not os.path.exists(script_path):
                print(f"Warning: SQL script not found at {script_path}")
                continue
                
            print(f"Executing SQL script: {script_name}")
            
            # Read the SQL script
            with open(script_path, 'r') as f:
                sql_script = f.read()
            
            try:
                # First try to execute the entire script as a single transaction
                cursor.executescript(sql_script)
            except sqlite3.OperationalError:
                # If that fails, fall back to executing statements individually
                print(f"Executing {script_name} statement by statement")
                statements = sql_script.split(';')
                for statement in statements:
                    if statement.strip():
                        try:
                            cursor.execute(statement)
                        except sqlite3.Error as e:
                            print(f"Error executing statement: {e}")
                            print(f"Statement: {statement[:100]}...")
            
        conn.commit()
        print(f"Successfully ran all post-export SQL scripts for {table_name} on {db_path}")
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error running post-export SQL for {table_name}: {str(e)}")
        return False
    finally:
        conn.close()


# Backward compatibility: Keep original function names as aliases
def post_export_visualisations(db_path):
    """Run SQL post-processing scripts on exported database for visualizations.
    
    Note: This function is kept for backward compatibility.
    New code should use post_export_table(db_path, "visualizations") instead.
    """
    return post_export_table(db_path, "visualizations")


def post_export_metrics(db_path):
    """Run SQL post-processing scripts on exported database for metrics.
    
    Note: This function is kept for backward compatibility.
    New code should use post_export_table(db_path, "metrics") instead.
    """
    return post_export_table(db_path, "metrics")