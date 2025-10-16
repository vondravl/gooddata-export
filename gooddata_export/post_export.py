"""
Post-export processing functions for GoodData metadata export.
These functions run after data export to enhance or modify the exported data.
"""

import os
import sqlite3
from gooddata_export.db import connect_database


def run_post_export_sql(db_path):
    """Run all post-export SQL operations on the database.
    This is the main entry point for post-export processing.
    """
    # First process visualizations
    post_export_visualisations(db_path)
    
    # Process metrics
    post_export_metrics(db_path)
    
    # Additional post-export functions can be added here:
    # etc.
    
    return True


def post_export_visualisations(db_path):
    """Run SQL post-processing scripts on exported database for visualizations"""
    # Define SQL scripts to run post-export
    sql_scripts = [
        "visuals_with_same_content.sql",
        # Add future SQL scripts here
    ]
    
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
        
        # Check if required columns exist in visualizations table and create if not
        cursor.execute("PRAGMA table_info(visualizations)")
        columns = [column[1] for column in cursor.fetchall()]

        # Add columns column if it doesn't exist
        if "columns" not in columns:
            print("Adding columns column to visualizations table")
            cursor.execute("ALTER TABLE visualizations ADD COLUMN columns TEXT")

        # Add same_columns_id column if it doesn't exist
        if "same_columns_id" not in columns:
            print("Adding same_columns_id column to visualizations table")
            cursor.execute("ALTER TABLE visualizations ADD COLUMN same_columns_id INTEGER")
            
        # Add same_visuals_id column if it doesn't exist
        if "same_visuals_id" not in columns:
            print("Adding same_visuals_id column to visualizations table")
            cursor.execute("ALTER TABLE visualizations ADD COLUMN same_visuals_id INTEGER")
            
        # Add same_visuals_id_with_tags column if it doesn't exist
        if "same_visuals_id_with_tags" not in columns:
            print("Adding same_visuals_id_with_tags column to visualizations table")
            cursor.execute("ALTER TABLE visualizations ADD COLUMN same_visuals_id_with_tags INTEGER")
        
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
        print(f"Successfully ran all post-export SQL scripts on {db_path}")
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error running post-export SQL: {str(e)}")
        return False
    finally:
        conn.close()


def post_export_metrics(db_path):
    """Run SQL post-processing scripts on exported database for metrics"""
    # Define SQL scripts to run post-export
    sql_scripts = [
        "metrics_probable_duplicates.sql",
        # Add future SQL scripts here
    ]
    
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
        
        # Check if required columns exist in metrics table and create if not
        cursor.execute("PRAGMA table_info(metrics)")
        columns = [column[1] for column in cursor.fetchall()]

        # Add similar_metric_id column if it doesn't exist
        if "similar_metric_id" not in columns:
            print("Adding similar_metric_id column to metrics table")
            cursor.execute("ALTER TABLE metrics ADD COLUMN similar_metric_id INTEGER")
        
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
        print(f"Successfully ran all post-export SQL scripts for metrics on {db_path}")
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error running post-export SQL for metrics: {str(e)}")
        return False
    finally:
        conn.close()