"""
Post-export processing functions for GoodData metadata export.
These functions run after data export to enhance or modify the exported data.
"""

import os
import sqlite3
from gooddata_export.db import connect_database


# Post-export configuration: defines SQL scripts and required columns for each table
# NOTE: Script execution order matters! See gooddata_export/sql/EXECUTION_ORDER.md for details
#
# Structure:
# 1. VIEWS: Created first (read-only, safe to create early, can be used by table updates)
# 2. TABLE UPDATES: Grouped by table name, includes required_columns for ALTER TABLE
#
# This order allows table update scripts to reference views (e.g., visuals_with_same_content.sql uses v_visualization_tags)

POST_EXPORT_CONFIG = {
    # ===== VIEWS (read-only, can reference any tables) =====
    # Created FIRST so table updates can reference them
    "views": {
        "sql_scripts": [
            # Tag views (simple, table-specific) - created first
            "views/v_metric_tags.sql",
            "views/v_visualization_tags.sql",  # Used by visuals_with_same_content.sql
            "views/v_dashboard_tags.sql",
            # Usage/relationship views (can join multiple tables)
            "views/v_metric_usage.sql",
            "views/v_metric_dependencies.sql",
            "views/v_visualization_usage.sql",
        ],
        # Note: No required_columns - views don't modify tables
    },
    
    # ===== TABLE UPDATES (modify existing tables) =====
    "visualizations": {
        "sql_scripts": [
            "updates/visuals_with_same_content.sql",  # Uses v_visualization_tags view
            "updates/visualizations_usage_check.sql",
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
    
    Uses a retry mechanism to handle dependency ordering issues:
    - First pass: Try to execute all scripts
    - If any fail, retry them once (they might depend on scripts that ran later)
    - If still failing after retry, it's a real error
    """
    max_retries = 1
    all_sections = list(POST_EXPORT_CONFIG.keys())
    failed_sections = []
    
    # First pass: Try all sections
    print("Starting post-export processing...")
    for table_name in all_sections:
        if not post_export_table(db_path, table_name):
            failed_sections.append(table_name)
            print(f"Warning: Failed to process {table_name} (will retry)")
    
    # Retry logic: Keep trying failed sections
    retry_count = 0
    while failed_sections and retry_count < max_retries:
        retry_count += 1
        print(f"\nRetry attempt {retry_count}/{max_retries} for {len(failed_sections)} failed section(s): {', '.join(failed_sections)}")
        
        still_failing = []
        for table_name in failed_sections:
            # Retry quietly (verbose=False) to avoid duplicate error messages
            if not post_export_table(db_path, table_name, verbose=False):
                still_failing.append(table_name)
            else:
                print(f"  ✓ Successfully processed '{table_name}' on retry {retry_count}")
        
        failed_sections = still_failing
    
    # Final result
    if failed_sections:
        print(f"\n❌ Failed to process {len(failed_sections)} section(s) after {max_retries} retries: {', '.join(failed_sections)}")
        return False
    else:
        print("\n✓ All post-export processing completed successfully")
        return True


def post_export_table(db_path, table_name, verbose=True):
    """Generic function to run SQL post-processing for a specific table.
    
    Args:
        db_path: Path to the SQLite database
        table_name: Name of the table to process (must exist in POST_EXPORT_CONFIG)
        verbose: If False, suppress non-critical output (for retries)
    
    Returns:
        bool: True if successful, False otherwise
    """
    if table_name not in POST_EXPORT_CONFIG:
        if verbose:
            print(f"Error: No configuration found for table '{table_name}'")
        return False
    
    config = POST_EXPORT_CONFIG[table_name]
    sql_scripts = config["sql_scripts"]
    required_columns = config.get("required_columns", {})
    
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
        
        # Check if required columns exist and create if not (only for table sections, not views)
        if required_columns:
            cursor.execute(f"PRAGMA table_info({table_name})")
            existing_columns = [column[1] for column in cursor.fetchall()]

            for column_name, column_type in required_columns.items():
                if column_name not in existing_columns:
                    if verbose:
                        print(f"Adding {column_name} column to {table_name} table")
                    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
        
        # Process each SQL script
        for script_name in sql_scripts:
            script_path = os.path.join(sql_dir, script_name)
            
            if not os.path.exists(script_path):
                if verbose:
                    print(f"Warning: SQL script not found at {script_path}")
                continue
            
            if verbose:
                print(f"Executing SQL script: {script_name}")
            
            # Read the SQL script
            with open(script_path, 'r') as f:
                sql_script = f.read()
            
            try:
                # First try to execute the entire script as a single transaction
                cursor.executescript(sql_script)
            except sqlite3.OperationalError as e:
                # If that fails, fall back to executing statements individually
                if verbose:
                    print(f"Executing {script_name} statement by statement")
                statements = sql_script.split(';')
                for statement in statements:
                    if statement.strip():
                        try:
                            cursor.execute(statement)
                        except sqlite3.Error as stmt_error:
                            if verbose:
                                print(f"Error executing statement: {stmt_error}")
                                print(f"Statement: {statement[:100]}...")
                            raise  # Re-raise to fail the whole section
            
        conn.commit()
        if verbose:
            print(f"Successfully ran all post-export SQL scripts for {table_name} on {db_path}")
        return True
    except Exception as e:
        conn.rollback()
        if verbose:
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