import concurrent.futures
import csv
import json
import logging
import os
import shutil
import sqlite3
import time


from gooddata_export.common import get_api_client
from gooddata_export.post_export import run_post_export_sql
from gooddata_export.process import (
    fetch_data,
    fetch_ldm,
    fetch_child_workspaces,
    process_dashboard_visualizations,
    process_dashboards,
    process_filter_contexts,
    process_ldm,
    process_metrics,
    process_rich_text_metrics,
    process_visualization_metrics,
    process_visualizations,
    process_workspaces,
)
from gooddata_export.db import (
    connect_database,
    setup_table,
    store_workspace_metadata,
)

DB_EXPORT_DIR = "db"
DB_NAME = os.path.join(DB_EXPORT_DIR, "gooddata_export.db")


def execute_with_retry(cursor, sql, params=None, max_retries=5):
    """Execute SQL with retry mechanism for database locks"""
    for attempt in range(max_retries):
        try:
            if params:
                if (
                    isinstance(params, list)
                    and len(params) > 0
                    and isinstance(params[0], (tuple, list))
                ):
                    # executemany case
                    cursor.executemany(sql, params)
                else:
                    # execute case
                    cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            return cursor
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < max_retries - 1:
                # Exponential backoff with jitter
                delay = (2**attempt) * 0.1 + (attempt * 0.05)
                print(
                    f"Database locked, retrying in {delay:.2f}s (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(delay)
                continue
            else:
                raise
    raise sqlite3.OperationalError(
        f"Database remained locked after {max_retries} attempts"
    )


def clean_field(value):
    """Replace actual newlines with literal '\n' string"""
    if isinstance(value, str):
        return value.replace("\n", "\\n").replace("\r", "")
    return value


def ensure_export_directory(export_dir):
    """Create export directory if it doesn't exist"""
    if not os.path.exists(export_dir):
        os.makedirs(export_dir)
    return export_dir


def write_to_csv(data, export_dir, filename, fieldnames, exclude_fields=None):
    """Write data to CSV file in specified directory"""
    ensure_export_directory(export_dir)
    filepath = os.path.join(export_dir, filename)

    if exclude_fields is None:
        exclude_fields = set()
    csv_fieldnames = [f for f in fieldnames if f not in exclude_fields]

    with open(filepath, "w", encoding="utf-8-sig", newline="\n") as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=csv_fieldnames, quoting=csv.QUOTE_ALL
        )
        writer.writeheader()
        for row in data:
            cleaned_row = {
                k: clean_field(v) for k, v in row.items() if k not in exclude_fields
            }
            writer.writerow(cleaned_row)
    return len(data)


def log_export(name, count, csv_path):
    """Standardized logging for exports"""
    print(f"Exported {count} {name} to {csv_path} and {DB_NAME}")


def fetch_all_data_parallel(config):
    """Fetch all required data from API in parallel"""
    client = get_api_client(config)

    # Define fetch tasks with their data types and params
    fetch_tasks = [
        {"function": fetch_data, "args": ("metrics", client, config), "key": "metrics"},
        {
            "function": fetch_data,
            "args": ("analyticalDashboards", client, config),
            "key": "dashboards",
        },
        {
            "function": fetch_data,
            "args": ("visualizationObjects", client, config),
            "key": "visualizations",
        },
        {"function": fetch_data, "args": ("filterContexts", client, config), "key": "filter_contexts"},
        {"function": fetch_ldm, "args": (client, config), "key": "ldm"},
    ]

    # Add child workspaces fetch if enabled
    if config.INCLUDE_CHILD_WORKSPACES:
        fetch_tasks.append(
            {
                "function": fetch_child_workspaces,
                "args": (client, config),
                "key": "child_workspaces",
            }
        )

    results = {}

    # Use ThreadPoolExecutor to run all fetch operations in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Create futures for each fetch task
        future_to_key = {}
        for task in fetch_tasks:
            future = executor.submit(task["function"], *task["args"])
            future_to_key[future] = task["key"]

        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_key):
            key = future_to_key[future]
            try:
                results[key] = future.result()
            except Exception as e:
                print(f"Error fetching {key}: {str(e)}")
                results[key] = None

    return results


def fetch_data_from_workspace(workspace_id, workspace_name, config):
    """Fetch selected data from a specific workspace (except LDM which is shared)"""
    import time

    start_time = time.time()

    # Create a client for this specific workspace
    original_client = get_api_client(config)
    workspace_client = {
        "base_url": original_client["base_url"],
        "workspace_id": workspace_id,
        "headers": original_client["headers"],
        "params": original_client["params"],
    }

    workspace_data = {}

    # Define all possible fetch tasks (excluding LDM - it's shared across workspaces)
    all_fetch_tasks = {
        "metrics": {
            "function": fetch_data,
            "args": ("metrics", workspace_client, config),
            "key": "metrics",
        },
        "dashboards": {
            "function": fetch_data,
            "args": ("analyticalDashboards", workspace_client, config),
            "key": "dashboards",
        },
        "visualizations": {
            "function": fetch_data,
            "args": ("visualizationObjects", workspace_client, config),
            "key": "visualizations",
        },
        "filter_contexts": {
            "function": fetch_data,
            "args": ("filterContexts", workspace_client, config),
            "key": "filter_contexts",
        },
    }

    # Filter tasks based on configuration - only fetch requested data types
    fetch_tasks = []
    for data_type in config.CHILD_WORKSPACE_DATA_TYPES:
        if data_type in all_fetch_tasks:
            fetch_tasks.append(all_fetch_tasks[data_type])
        else:
            print(
                f"Warning: Unknown data type '{data_type}' in CHILD_WORKSPACE_DATA_TYPES"
            )

    # Set all non-requested data types to None
    for data_type, task_info in all_fetch_tasks.items():
        if data_type not in config.CHILD_WORKSPACE_DATA_TYPES:
            workspace_data[task_info["key"]] = None

    if config.DEBUG_WORKSPACE_PROCESSING:
        requested_types = ", ".join(config.CHILD_WORKSPACE_DATA_TYPES)
        print(f"Fetching from {workspace_name}: {requested_types}")

    # Only proceed with parallel fetching if there are tasks to execute
    if not fetch_tasks:
        if config.DEBUG_WORKSPACE_PROCESSING:
            print(f"No data types configured for fetching from {workspace_name}")
    else:
        # Use parallel fetching within this workspace too but limit concurrency
        max_workers = min(len(fetch_tasks), 6)  # Increased for better performance
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_key = {}
            for task in fetch_tasks:
                future = executor.submit(task["function"], *task["args"])
                future_to_key[future] = task["key"]

            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    workspace_data[key] = future.result()
                except Exception as e:
                    # Check if this is a permission issue or just missing data
                    error_msg = str(e).lower()
                    if any(
                        keyword in error_msg
                        for keyword in ["401", "403", "unauthorized", "forbidden"]
                    ):
                        if config.DEBUG_WORKSPACE_PROCESSING:
                            print(
                                f"Permission issue accessing {key} in workspace {workspace_id}: {str(e)}"
                            )
                        # Don't show warnings in normal mode for permission issues in parallel execution
                    elif config.DEBUG_WORKSPACE_PROCESSING:
                        print(
                            f"Error fetching {key} from workspace {workspace_id}: {str(e)}"
                        )
                    workspace_data[key] = None

    # LDM is not fetched for child workspaces as it's shared from parent
    workspace_data["ldm"] = None

    # Add workspace info to the data
    workspace_data["workspace_id"] = workspace_id
    workspace_data["workspace_name"] = workspace_name

    duration = time.time() - start_time
    if config.DEBUG_WORKSPACE_PROCESSING:
        print(
            f"Workspace {workspace_name} data fetch completed in {duration:.2f} seconds"
        )

    return workspace_data


def fetch_all_workspace_data(config):
    """Fetch data from parent workspace and optionally child workspaces"""
    import time

    # Start with parent workspace data (includes LDM which is shared across all workspaces)
    start_time = time.time()
    print("Fetching parent workspace data...")
    parent_data = fetch_all_data_parallel(config)
    client = get_api_client(config)
    parent_workspace_id = client["workspace_id"]

    parent_duration = time.time() - start_time
    print(f"Parent workspace data fetch completed in {parent_duration:.2f} seconds")

    if config.DEBUG_WORKSPACE_PROCESSING:
        print(f"Parent workspace ID: {parent_workspace_id}")
        print(f"Child workspaces enabled: {config.INCLUDE_CHILD_WORKSPACES}")

    all_workspace_data = [
        {
            "workspace_id": parent_workspace_id,
            "workspace_name": f"Parent Workspace ({parent_workspace_id})",
            "is_parent": True,
            "data": parent_data,
        }
    ]

    # If child workspaces are enabled, fetch from them in parallel
    # Note: LDM is not fetched from child workspaces as it's shared from the parent
    if config.INCLUDE_CHILD_WORKSPACES:
        child_workspaces = parent_data.get("child_workspaces")
        if child_workspaces:
            # Show what data types will be fetched from child workspaces
            data_types = (
                ", ".join(config.CHILD_WORKSPACE_DATA_TYPES)
                if config.CHILD_WORKSPACE_DATA_TYPES
                else "none"
            )
            print(f"Processing {len(child_workspaces)} child workspaces in parallel...")
            print(f"Fetching data types: {data_types}")
            print(f"Using {config.MAX_PARALLEL_WORKSPACES} parallel workers")

            if not config.CHILD_WORKSPACE_DATA_TYPES:
                print(
                    "Warning: No data types configured for child workspaces - only workspace metadata will be collected"
                )

            # Fetch data from all child workspaces in parallel
            child_start_time = time.time()

            # Use configurable thread count for better performance
            max_workers = min(len(child_workspaces), config.MAX_PARALLEL_WORKSPACES)
            completed_count = 0
            total_count = len(child_workspaces)

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as executor:
                # Create futures for each child workspace
                future_to_workspace = {}
                for child_workspace in child_workspaces:
                    child_workspace_id = child_workspace["id"]
                    child_workspace_name = child_workspace["attributes"]["name"]

                    if config.DEBUG_WORKSPACE_PROCESSING:
                        print(
                            f"Starting fetch for child workspace: {child_workspace_name} ({child_workspace_id})"
                        )

                    future = executor.submit(
                        fetch_data_from_workspace,
                        child_workspace_id,
                        child_workspace_name,
                        config,
                    )
                    future_to_workspace[future] = {
                        "id": child_workspace_id,
                        "name": child_workspace_name,
                        "workspace_data": child_workspace,
                    }

                # Collect results as they complete with progress reporting
                for future in concurrent.futures.as_completed(future_to_workspace):
                    workspace_info = future_to_workspace[future]
                    completed_count += 1

                    try:
                        child_data = future.result()
                        all_workspace_data.append(
                            {
                                "workspace_id": workspace_info["id"],
                                "workspace_name": workspace_info["name"],
                                "is_parent": False,
                                "data": child_data,
                            }
                        )

                        # Progress reporting
                        percentage = (completed_count / total_count) * 100
                        elapsed_time = time.time() - child_start_time

                        if completed_count > 0:
                            avg_time_per_workspace = elapsed_time / completed_count
                            remaining_workspaces = total_count - completed_count
                            estimated_remaining_time = (
                                avg_time_per_workspace * remaining_workspaces
                            )

                            print(
                                f"Progress: {completed_count}/{total_count} ({percentage:.1f}%) - "
                                f"Elapsed: {elapsed_time:.1f}s - "
                                f"ETA: {estimated_remaining_time:.1f}s - "
                                f"Completed: {workspace_info['name']}"
                            )

                        if config.DEBUG_WORKSPACE_PROCESSING:
                            print(
                                f"✓ Completed fetch for child workspace: {workspace_info['name']}"
                            )

                    except Exception as e:
                        error_msg = str(e)
                        percentage = (completed_count / total_count) * 100
                        elapsed_time = time.time() - child_start_time

                        print(
                            f"Progress: {completed_count}/{total_count} ({percentage:.1f}%) - "
                            f"Elapsed: {elapsed_time:.1f}s - "
                            f"Error: {workspace_info['name']}"
                        )

                        if config.DEBUG_WORKSPACE_PROCESSING:
                            print(
                                f"✗ Error fetching data from child workspace {workspace_info['name']}: {error_msg}"
                            )
                        else:
                            print(
                                f"Warning: Could not fetch data from child workspace {workspace_info['name']}"
                            )

            child_duration = time.time() - child_start_time
            print(
                f"Child workspaces data fetch completed in {child_duration:.2f} seconds"
            )
            print(
                f"Average time per workspace: {child_duration / total_count:.2f} seconds"
            )

        else:
            if config.DEBUG_WORKSPACE_PROCESSING:
                print("No child workspaces found for this parent workspace")
                print("This could mean:")
                print("1. This workspace has no child workspaces")
                print("2. This workspace is not a parent workspace")
                print("3. There was an error fetching child workspaces")
    else:
        if config.DEBUG_WORKSPACE_PROCESSING:
            print("Child workspace processing is disabled")

    total_duration = time.time() - start_time

    # Final summary for API FETCH PHASE only (export phase runs next)
    print("\n" + "=" * 60)
    print("API FETCH PHASE SUMMARY")
    print("(Only API calls; export/write phase runs next)")
    print("=" * 60)
    print(f"Total workspaces fetched: {len(all_workspace_data)}")
    print("Parent workspace: 1")
    print(f"Child workspaces: {len(all_workspace_data) - 1}")
    print(f"API fetch time: {total_duration:.2f} seconds")
    print(
        f"Average fetch time per workspace: {total_duration / len(all_workspace_data):.2f} seconds"
    )

    if len(all_workspace_data) > 1:
        child_count = len(all_workspace_data) - 1
        child_duration = total_duration - parent_duration
        print(f"Child workspaces API fetch time: {child_duration:.2f} seconds")
        print(
            f"Average fetch time per child workspace: {child_duration / child_count:.2f} seconds"
        )
        print(f"Parallel workers used: {config.MAX_PARALLEL_WORKSPACES}")

        # Performance metrics
        if child_duration > 0:
            workspaces_per_minute = (child_count / child_duration) * 60
            print(f"Processing rate: {workspaces_per_minute:.1f} workspaces/minute")

    print("=" * 60)
    print()

    return all_workspace_data


def export_metrics(all_workspace_data, export_dir, config, db_name):
    """Export metrics to both CSV and SQLite"""
    all_processed_data = []

    # Process metrics from all workspaces
    for workspace_info in all_workspace_data:
        workspace_id = workspace_info["workspace_id"]
        raw_data = workspace_info["data"].get("metrics")

        if raw_data is None:
            if config.DEBUG_WORKSPACE_PROCESSING:
                print(f"No metrics data for workspace {workspace_id}")
            continue

        processed_data = process_metrics(raw_data, workspace_id)
        all_processed_data.extend(processed_data)

    if not all_processed_data:
        raise RuntimeError("No metrics data found in any workspace")

    metrics_columns = {
        "metric_id": "TEXT",
        "workspace_id": "TEXT",
        "title": "TEXT",
        "description": "TEXT",
        "tags": "TEXT",
        "maql": "TEXT",
        "format": "TEXT",
        "created_at": "DATE",
        "modified_at": "DATE",
        "is_valid": "BOOLEAN",
        "origin_type": "TEXT",
        "content": "JSON",  # Add content field to store the original JSON
        "PRIMARY KEY": "(metric_id, workspace_id)",
    }

    # Export to CSV (if requested)
    records_count = len(all_processed_data)
    if export_dir is not None:
        csv_filename = "gooddata_metrics.csv"
        records_count = write_to_csv(
            all_processed_data,
            export_dir,
            csv_filename,
            fieldnames=metrics_columns.keys(),
            exclude_fields={"content"},  # Exclude content from CSV since it's large
        )

    # Export to SQLite
    conn = connect_database(db_name)
    cursor = setup_table(conn, "metrics", metrics_columns)

    # Minify content for child references to parent objects
    execute_with_retry(
        cursor,
        """
        INSERT INTO metrics 
        (metric_id, workspace_id, title, description, tags, maql, format, created_at, modified_at, is_valid, origin_type, content)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                d["metric_id"],
                d["workspace_id"],
                d["title"],
                d["description"],
                d["tags"],
                d["maql"],
                d["format"],
                d["created_at"],
                d["modified_at"],
                d["is_valid"],
                d["origin_type"],
                json.dumps(d["content"]),
            )
            for d in all_processed_data
        ],
    )

    conn.commit()
    if export_dir is not None:
        log_export("metrics", records_count, os.path.join(export_dir, "gooddata_metrics.csv"))
    else:
        print(f"Exported {records_count} metrics to {db_name}")
    conn.close()


def export_visualizations(all_workspace_data, export_dir, config, db_name):
    """Export visualizations and visualization-metrics relationships"""

    client = get_api_client(config)
    all_processed_visualizations = []
    all_processed_relationships = []

    # Process visualizations from all workspaces
    for workspace_info in all_workspace_data:
        workspace_id = workspace_info["workspace_id"]
        raw_data = workspace_info["data"].get("visualizations")

        if raw_data is None:
            continue

        # Process both visualizations and relationships from same raw data
        processed_visualizations = process_visualizations(
            raw_data, client["base_url"], workspace_id
        )
        processed_relationships = process_visualization_metrics(raw_data, workspace_id)

        all_processed_visualizations.extend(processed_visualizations)
        all_processed_relationships.extend(processed_relationships)

    if not all_processed_visualizations:
        raise RuntimeError("No visualizations data found in any workspace")

    # Export visualizations
    visualization_columns = {
        "visualization_id": "TEXT",
        "workspace_id": "TEXT",
        "title": "TEXT",
        "description": "TEXT",
        "tags": "TEXT",
        "visualization_url": "TEXT",
        "created_at": "DATE",
        "modified_at": "DATE",
        "url_link": "TEXT",
        "origin_type": "TEXT",
        "content": "JSON",
        "is_valid": "BOOLEAN",
        "PRIMARY KEY": "(visualization_id, workspace_id)",
    }

    vis_count = len(all_processed_visualizations)
    if export_dir is not None:
        csv_filename = "gooddata_visualizations.csv"
        vis_count = write_to_csv(
            all_processed_visualizations,
            export_dir,
            csv_filename,
            fieldnames=visualization_columns.keys(),
            exclude_fields={"content"},
        )

    conn = connect_database(db_name)
    cursor = setup_table(conn, "visualizations", visualization_columns)

    execute_with_retry(
        cursor,
        """
        INSERT INTO visualizations 
        (visualization_id, workspace_id, title, description, tags, visualization_url, created_at, modified_at, url_link, origin_type, content, is_valid)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                d["visualization_id"],
                d["workspace_id"],
                d["title"],
                d["description"],
                d["tags"],
                d["visualization_url"],
                d["created_at"],
                d["modified_at"],
                d["url_link"],
                d["origin_type"],
                json.dumps(d["content"]),
                d["is_valid"],
            )
            for d in all_processed_visualizations
        ],
    )

    # Export visualization-metrics relationships
    relationship_columns = {
        "visualization_id": "TEXT",
        "metric_id": "TEXT",
        "workspace_id": "TEXT",
        "PRIMARY KEY": "(visualization_id, metric_id, workspace_id)",
    }

    rel_count = len(all_processed_relationships)
    if export_dir is not None:
        rel_csv = "gooddata_visualization_metrics.csv"
        rel_count = write_to_csv(
            all_processed_relationships,
            export_dir,
            rel_csv,
            fieldnames=["visualization_id", "metric_id", "workspace_id"],
        )

    cursor = setup_table(conn, "visualization_metrics", relationship_columns)
    execute_with_retry(
        cursor,
        """
        INSERT INTO visualization_metrics 
        (visualization_id, metric_id, workspace_id)
        VALUES (?, ?, ?)
        """,
        [
            (d["visualization_id"], d["metric_id"], d["workspace_id"])
            for d in all_processed_relationships
        ],
    )

    conn.commit()
    if export_dir is not None:
        log_export("visualizations", vis_count, os.path.join(export_dir, "gooddata_visualizations.csv"))
        log_export(
            "visualization-metric relationships",
            rel_count,
            os.path.join(export_dir, "gooddata_visualization_metrics.csv"),
        )
    else:
        print(f"Exported {vis_count} visualizations to {db_name}")
        print(f"Exported {rel_count} visualization-metric relationships to {db_name}")
    conn.close()


def export_dashboards(all_workspace_data, export_dir, config, db_name):
    """Export dashboards and dashboard-visualization relationships"""

    client = get_api_client(config)
    all_processed_dashboards = []
    all_processed_relationships = []

    # Process dashboards from all workspaces
    for workspace_info in all_workspace_data:
        workspace_id = workspace_info["workspace_id"]
        raw_data = workspace_info["data"].get("dashboards")

        if raw_data is None:
            continue

        # Process both dashboards and relationships from same raw data
        processed_dashboards = process_dashboards(
            raw_data, client["base_url"], workspace_id
        )
        # Build in-memory set of known insight (visualization) IDs from the already fetched visualization objects
        known_insights = set()
        try:
            visualizations_raw = workspace_info["data"].get("visualizations")
            if visualizations_raw:
                known_insights = {
                    viz["id"]
                    for viz in visualizations_raw
                    if isinstance(viz, dict) and viz.get("id")
                }
        except Exception:
            known_insights = set()

        processed_relationships = process_dashboard_visualizations(
            raw_data, workspace_id, known_insights, config
        )

        all_processed_dashboards.extend(processed_dashboards)
        all_processed_relationships.extend(processed_relationships)

    if not all_processed_dashboards:
        raise RuntimeError("No dashboards data found in any workspace")

    # Export dashboards
    dashboard_columns = {
        "dashboard_id": "TEXT",
        "workspace_id": "TEXT",
        "title": "TEXT",
        "description": "TEXT",
        "tags": "TEXT",
        "created_at": "DATE",
        "modified_at": "DATE",
        "dashboard_url": "TEXT",
        "origin_type": "TEXT",
        "content": "JSON",
        "is_valid": "BOOLEAN",
        "filter_context_id": "TEXT",
        "PRIMARY KEY": "(dashboard_id, workspace_id)",
    }

    dash_count = len(all_processed_dashboards)
    if export_dir is not None:
        csv_filename = "gooddata_dashboards.csv"
        dash_count = write_to_csv(
            all_processed_dashboards,
            export_dir,
            csv_filename,
            fieldnames=dashboard_columns.keys(),
            exclude_fields={"content"},
        )

    conn = connect_database(db_name)
    cursor = setup_table(conn, "dashboards", dashboard_columns)

    execute_with_retry(
        cursor,
        """
        INSERT INTO dashboards 
        (dashboard_id, workspace_id, title, description, tags, created_at, modified_at, dashboard_url, origin_type, content, is_valid, filter_context_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                d["dashboard_id"],
                d["workspace_id"],
                d["title"],
                d["description"],
                d["tags"],
                d["created_at"],
                d["modified_at"],
                d["dashboard_url"],
                d["origin_type"],
                json.dumps(d["content"]),
                d["is_valid"],
                d.get("filter_context_id"),  # Use get() to handle potential None values
            )
            for d in all_processed_dashboards
        ],
    )

    # Export dashboard-visualization relationships
    relationship_columns = {
        "dashboard_id": "TEXT",
        "visualization_id": "TEXT",
        "from_rich_text": "INTEGER DEFAULT 0",
        "workspace_id": "TEXT",
        "PRIMARY KEY": "(dashboard_id, visualization_id, from_rich_text, workspace_id)",
    }

    rel_count = len(all_processed_relationships)
    if export_dir is not None:
        rel_csv = "gooddata_dashboard_visualizations.csv"
        rel_count = write_to_csv(
            all_processed_relationships,
            export_dir,
            rel_csv,
            fieldnames=[
                "dashboard_id",
                "visualization_id",
                "from_rich_text",
                "workspace_id",
            ],
        )

    cursor = setup_table(conn, "dashboard_visualizations", relationship_columns)
    execute_with_retry(
        cursor,
        """
        INSERT INTO dashboard_visualizations 
        (dashboard_id, visualization_id, from_rich_text, workspace_id)
        VALUES (?, ?, ?, ?)
        """,
        [
            (
                d["dashboard_id"],
                d["visualization_id"],
                d.get("from_rich_text", 0),
                d["workspace_id"],
            )
            for d in all_processed_relationships
        ],
    )

    conn.commit()
    if export_dir is not None:
        log_export("dashboards", dash_count, os.path.join(export_dir, "gooddata_dashboards.csv"))
        log_export(
            "dashboard-visualization relationships",
            rel_count,
            os.path.join(export_dir, "gooddata_dashboard_visualizations.csv"),
        )
    else:
        print(f"Exported {dash_count} dashboards to {db_name}")
        print(f"Exported {rel_count} dashboard-visualization relationships to {db_name}")
    conn.close()


def export_ldm(all_workspace_data, export_dir, config, db_name):
    """Export logical data model to both CSV and SQLite"""
    # LDM is only available from the parent workspace (first in the list)
    # It's shared across all workspaces and cannot be modified in child workspaces
    if not all_workspace_data:
        raise RuntimeError("No workspace data available")

    parent_workspace_info = all_workspace_data[0]  # Parent workspace is always first
    parent_workspace_id = parent_workspace_info["workspace_id"]
    raw_data = parent_workspace_info["data"].get("ldm")

    if raw_data is None:
        raise RuntimeError("No LDM data found in parent workspace")

    # Process LDM data with parent workspace ID (since it's shared across all workspaces)
    datasets, column_records = process_ldm(raw_data, parent_workspace_id)

    # Export datasets
    dataset_columns = {
        "title": "TEXT",
        "description": "TEXT",
        "id": "TEXT PRIMARY KEY",
        "attributes_count": "INTEGER",
        "facts_count": "INTEGER",
        "references_count": "INTEGER",
        "total_columns": "INTEGER",
        "data_source_id": "TEXT",
        "source_table": "TEXT",
        "source_table_path": "TEXT",
        "workspace_id": "TEXT",
    }

    dataset_count = len(datasets)
    if export_dir is not None:
        dataset_csv = "gooddata_ldm_datasets.csv"
        dataset_count = write_to_csv(
            datasets, export_dir, dataset_csv, fieldnames=dataset_columns.keys()
        )

    conn = connect_database(db_name)
    cursor = setup_table(conn, "ldm_datasets", dataset_columns)

    execute_with_retry(
        cursor,
        """
        INSERT INTO ldm_datasets 
        (title, description, id, attributes_count, facts_count, references_count, total_columns, 
         data_source_id, source_table, source_table_path, workspace_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                d["title"],
                d["description"],
                d["id"],
                d["attributes_count"],
                d["facts_count"],
                d["references_count"],
                d["total_columns"],
                d["data_source_id"],
                d["source_table"],
                d["source_table_path"],
                d["workspace_id"],
            )
            for d in datasets
        ],
    )

    # Export columns
    column_columns = {
        "dataset_id": "TEXT",
        "dataset_name": "TEXT",
        "title": "TEXT",
        "description": "TEXT",
        "id": "TEXT",
        "tags": "TEXT",
        "data_type": "TEXT",
        "source_column": "TEXT",
        "type": "TEXT",
        "grain": "TEXT",
        "reference_to_id": "TEXT",
        "reference_to_title": "TEXT",
        "workspace_id": "TEXT",
    }

    column_count = len(column_records)
    if export_dir is not None:
        column_csv = "gooddata_ldm_columns.csv"
        column_count = write_to_csv(
            column_records, export_dir, column_csv, fieldnames=column_columns.keys()
        )

    cursor = setup_table(conn, "ldm_columns", column_columns)
    execute_with_retry(
        cursor,
        """
        INSERT INTO ldm_columns 
        (dataset_id, dataset_name, title, description, id, tags, data_type, source_column, type, grain, reference_to_id, reference_to_title, workspace_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                d["dataset_id"],
                d["dataset_name"],
                d["title"],
                d["description"],
                d["id"],
                d["tags"],
                d["data_type"],
                d["source_column"],
                d["type"],
                d["grain"],
                d["reference_to_id"],
                d["reference_to_title"],
                d["workspace_id"],
            )
            for d in column_records
        ],
    )

    conn.commit()
    if export_dir is not None:
        log_export("datasets", dataset_count, os.path.join(export_dir, "gooddata_ldm_datasets.csv"))
        log_export("columns", column_count, os.path.join(export_dir, "gooddata_ldm_columns.csv"))
    else:
        print(f"Exported {dataset_count} datasets to {db_name}")
        print(f"Exported {column_count} columns to {db_name}")
    conn.close()


def export_filter_contexts(all_workspace_data, export_dir, config, db_name):
    """Export filter contexts to both CSV and SQLite"""
    all_processed_data = []

    # Process filter contexts from all workspaces
    for workspace_info in all_workspace_data:
        workspace_id = workspace_info["workspace_id"]
        raw_data = workspace_info["data"].get("filter_contexts")

        if raw_data is None:
            continue

        processed = process_filter_contexts(raw_data, workspace_id)
        all_processed_data.extend(processed)

    if not all_processed_data:
        raise RuntimeError("No filter contexts data found in any workspace")

    filter_contexts_columns = {
        "filter_context_id": "TEXT",
        "workspace_id": "TEXT",
        "PRIMARY KEY": "(filter_context_id, workspace_id)",
    }

    # Export to CSV (if requested)
    records_count = len(all_processed_data)
    if export_dir is not None:
        csv_filename = "gooddata_filter_contexts.csv"
        records_count = write_to_csv(
            all_processed_data,
            export_dir,
            csv_filename,
            fieldnames=filter_contexts_columns.keys(),
        )

    # Export to SQLite
    conn = connect_database(db_name)
    cursor = setup_table(conn, "filter_contexts", filter_contexts_columns)

    execute_with_retry(
        cursor,
        """
        INSERT INTO filter_contexts 
        (filter_context_id, workspace_id)
        VALUES (?, ?)
        """,
        [(d["filter_context_id"], d["workspace_id"]) for d in all_processed_data],
    )

    conn.commit()
    if export_dir is not None:
        log_export("filter contexts", records_count, os.path.join(export_dir, "gooddata_filter_contexts.csv"))
    else:
        print(f"Exported {records_count} filter contexts to {db_name}")
    conn.close()


def export_workspaces(all_workspace_data, export_dir, config, db_name):
    """Export workspaces to both CSV and SQLite"""
    if not config.INCLUDE_CHILD_WORKSPACES:
        # If child workspaces are not enabled, don't export workspaces table
        return

    # Get the parent workspace info
    client = get_api_client(config)
    parent_workspace_id = client["workspace_id"]
    parent_workspace_name = f"Parent Workspace ({parent_workspace_id})"

    # Get child workspaces from the first workspace data (parent)
    child_workspaces_data = []
    if all_workspace_data and all_workspace_data[0]["data"].get("child_workspaces"):
        child_workspaces_data = all_workspace_data[0]["data"]["child_workspaces"]

    processed_data = process_workspaces(
        parent_workspace_id, parent_workspace_name, child_workspaces_data
    )

    workspaces_columns = {
        "workspace_id": "TEXT PRIMARY KEY",
        "workspace_name": "TEXT",
        "is_parent": "BOOLEAN",
        "parent_workspace_id": "TEXT",
        "created_at": "DATE",
        "modified_at": "DATE",
    }

    # Export to CSV (if requested)
    records_count = len(processed_data)
    if export_dir is not None:
        csv_filename = "gooddata_workspaces.csv"
        records_count = write_to_csv(
            processed_data,
            export_dir,
            csv_filename,
            fieldnames=workspaces_columns.keys(),
        )

    # Export to SQLite
    conn = connect_database(db_name)
    cursor = setup_table(conn, "workspaces", workspaces_columns)

    execute_with_retry(
        cursor,
        """
        INSERT INTO workspaces 
        (workspace_id, workspace_name, is_parent, parent_workspace_id, created_at, modified_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                d["workspace_id"],
                d["workspace_name"],
                d["is_parent"],
                d["parent_workspace_id"],
                d["created_at"],
                d["modified_at"],
            )
            for d in processed_data
        ],
    )

    conn.commit()
    if export_dir is not None:
        log_export("workspaces", records_count, os.path.join(export_dir, "gooddata_workspaces.csv"))
    else:
        print(f"Exported {records_count} workspaces to {db_name}")
    conn.close()


def export_dashboard_metrics(all_workspace_data, export_dir, config, db_name):
    """Export metrics used in rich text widgets on dashboards across all workspaces"""
    if not config.ENABLE_RICH_TEXT_EXTRACTION:
        return

    rich_text_metrics = []

    # First get a list of all known metric IDs for better matching
    known_metrics = set()

    # Prefer in-memory metrics from already fetched workspace data
    try:
        for workspace_info in all_workspace_data:
            raw_metrics = workspace_info.get("data", {}).get("metrics")
            if not raw_metrics:
                continue
            for metric in raw_metrics:
                # Raw metric objects are dicts with an "id" field
                metric_id = metric.get("id") if isinstance(metric, dict) else None
                if metric_id:
                    known_metrics.add(metric_id)
        if known_metrics:
            logging.info(
                f"Found {len(known_metrics)} known metrics for validation (source: memory)"
            )
    except Exception as e:
        logging.warning(f"Failed to load in-memory metrics for validation: {str(e)}")

    # If still empty, proceed without known metrics filtering (no API fallback)
    if not known_metrics:
        logging.info(
            "No known metrics available for validation from memory or db; proceeding without filter"
        )

    try:
        # Process dashboards from all workspaces to extract metrics from rich text widgets
        for workspace_info in all_workspace_data:
            workspace_id = workspace_info["workspace_id"]
            workspace_dashboards = workspace_info["data"].get("dashboards")
            if not workspace_dashboards:
                continue
            for dash in workspace_dashboards:
                content = dash["attributes"].get("content", {})
                if (
                    not content
                    or "layout" not in content
                    or "sections" not in content["layout"]
                ):
                    continue

                dashboard_id = dash["id"]

                for section in content["layout"]["sections"]:
                    if "items" not in section:
                        continue

                    for item in section["items"]:
                        # Process rich text widgets
                        if item.get("widget", {}).get("type") == "richText":
                            rich_text_content = item.get("widget", {}).get(
                                "content", ""
                            )
                            # Extract metrics from rich text content using known metrics for filtering
                            dashboard_metrics = process_rich_text_metrics(
                                rich_text_content, dashboard_id, known_metrics
                            )
                            if dashboard_metrics:
                                # Add workspace_id to each metric
                                for metric in dashboard_metrics:
                                    metric["workspace_id"] = workspace_id
                                rich_text_metrics.extend(dashboard_metrics)

                        # Check for any widget content field that might contain metric references
                        widget_content = item.get("widget", {}).get("content")
                        if isinstance(widget_content, str) and any(
                            pattern in widget_content
                            for pattern in [
                                "measureChange",
                                "measuresComparison",
                                "measureValue",
                                "measuresShareComparison",
                                "measureTotal",
                                "measure:",
                            ]
                        ):
                            additional_metrics = process_rich_text_metrics(
                                widget_content, dashboard_id, known_metrics
                            )
                            if additional_metrics:
                                # Add workspace_id to each metric
                                for metric in additional_metrics:
                                    metric["workspace_id"] = workspace_id
                                rich_text_metrics.extend(additional_metrics)

        # Skip if no metrics found
        if not rich_text_metrics:
            print("No metrics found in rich text widgets")
            return

        # Remove duplicates by converting to dict and back to list
        unique_metrics = {}
        for item in rich_text_metrics:
            key = (item["dashboard_id"], item["metric_id"], item["workspace_id"])
            unique_metrics[key] = item

        rich_text_metrics = list(unique_metrics.values())

        # Define columns for the new table
        dashboard_metrics_columns = {
            "dashboard_id": "TEXT",
            "metric_id": "TEXT",
            "workspace_id": "TEXT",
            "PRIMARY KEY": "(dashboard_id, metric_id, workspace_id)",
        }

        # Export to CSV (if requested)
        records_count = len(rich_text_metrics)
        if export_dir is not None:
            csv_filename = "gooddata_dashboard_metrics.csv"
            records_count = write_to_csv(
                rich_text_metrics,
                export_dir,
                csv_filename,
                fieldnames=["dashboard_id", "metric_id", "workspace_id"],
            )

        # Export to SQLite - check if table exists first and drop it to avoid conflicts
        conn = connect_database(db_name)
        cursor = conn.cursor()

        # First drop the table if it exists to avoid conflicts
        cursor.execute("DROP TABLE IF EXISTS dashboard_metrics")
        conn.commit()

        # Then recreate it with our schema
        cursor = setup_table(conn, "dashboard_metrics", dashboard_metrics_columns)

        cursor.executemany(
            """
            INSERT INTO dashboard_metrics 
            (dashboard_id, metric_id, workspace_id)
            VALUES (?, ?, ?)
            """,
            [(d["dashboard_id"], d["metric_id"], d["workspace_id"]) for d in rich_text_metrics],
        )

        conn.commit()
        if export_dir is not None:
            log_export(
                "dashboard metrics from rich text",
                records_count,
                os.path.join(export_dir, "gooddata_dashboard_metrics.csv"),
            )
        else:
            print(f"Exported {records_count} dashboard metrics from rich text to {db_name}")
        conn.close()

    except Exception as e:
        print(f"Error in export_dashboard_metrics: {str(e)}")
        # Re-raise with more specific message
        raise RuntimeError(f"Error processing dashboard metrics: {str(e)}")


def export_all_metadata(config, csv_dir=None, db_path="output/db/gooddata_export.db", export_formats=None, run_post_export=True):
    """Export all metadata to SQLite database and CSV files.
    
    Args:
        config: ExportConfig instance with GoodData credentials and options
        csv_dir: Directory for CSV files (default: None, uses "output/metadata_csv" if csv in formats)
        db_path: Path to SQLite database file (default: "output/db/gooddata_export.db")
        export_formats: List of formats to export ("sqlite", "csv") (default: both)
        run_post_export: Whether to run post-export SQL processing (default: True)
    
    Returns:
        dict: Export results with db_path, csv_dir, and workspace_count
    """
    if export_formats is None:
        export_formats = ["sqlite", "csv"]
    
    # Set up CSV directory
    if "csv" in export_formats and csv_dir is None:
        csv_dir = "output/metadata_csv"
    export_dir = csv_dir if "csv" in export_formats else None
    
    # Set up database path
    db_export_dir = os.path.dirname(db_path)
    errors = []

    # Ensure database directory exists (databases overwrite themselves, no cleanup needed)
    if db_export_dir:
        os.makedirs(db_export_dir, exist_ok=True)
    
    # Clean CSV directory completely to avoid stale data (files mix together, so we need a clean slate)
    if export_dir and os.path.exists(export_dir):
        print(f"Cleaning CSV directory: {export_dir}")
        shutil.rmtree(export_dir)
    
    # Ensure CSV directory exists if needed
    if export_dir:
        os.makedirs(export_dir, exist_ok=True)

    # Fetch all data from all workspaces
    print("Fetching data from GoodData API...")
    all_workspace_data = fetch_all_workspace_data(config)

    if config.DEBUG_WORKSPACE_PROCESSING:
        print(f"Successfully fetched data from {len(all_workspace_data)} workspace(s)")
        for ws in all_workspace_data:
            print(f"  - {ws['workspace_name']} ({ws['workspace_id']})")

    # Define export functions to run sequentially with their respective data
    # Ensure workspaces are exported first if child workspaces are enabled
    export_functions = []
    if config.INCLUDE_CHILD_WORKSPACES:
        export_functions.append({"func": export_workspaces, "data_key": "workspaces"})
    export_functions.extend(
        [
            {"func": export_metrics, "data_key": "metrics"},
            {"func": export_visualizations, "data_key": "visualizations"},
            {"func": export_dashboards, "data_key": "dashboards"},
            {"func": export_dashboard_metrics, "data_key": "dashboard_metrics"},
            {"func": export_ldm, "data_key": "ldm"},
            {"func": export_filter_contexts, "data_key": "filter_contexts"},
        ]
    )

    # Execute each export function with all workspace data
    # Note: Database writes are kept sequential to avoid SQLite concurrency issues
    print("Processing and writing data to database...")
    print("=" * 80)
    for export_func in export_functions:
        try:
            # Skip CSV export for functions if CSV not requested
            func_export_dir = export_dir if export_dir else None
            export_func["func"](all_workspace_data, func_export_dir, config, db_path)
        except Exception as e:
            error_msg = str(e).split("\n")[0]
            errors.append(f"{export_func['func'].__name__}: {error_msg}")

    if errors:
        # Raise detailed error messages
        workspace_id = config.WORKSPACE_ID
        error_details = "\n  - ".join(errors)
        raise Exception(
            f"Export failed for workspace: {workspace_id}\n"
            f"Errors encountered:\n  - {error_details}"
        )

    # Only run post-export processing for single workspace (parent only) and if requested
    # Multi-workspace data would produce confusing duplicate detection results
    if run_post_export and not config.INCLUDE_CHILD_WORKSPACES:
        print()
        print("Running post-export processing...")
        print("=" * 80)
        run_post_export_sql(db_path)
    elif not run_post_export:
        print("Skipping post-export processing (disabled)")
    else:
        print("Skipping post-export processing (multi-workspace mode)")

    # Get workspace ID from API client
    client = get_api_client(config)
    workspace_id = client.get("workspace_id", "default")
    store_workspace_metadata(db_path, config)

    # Create workspace-specific database copy
    workspace_db = None
    if db_export_dir:
        try:
            workspace_db = os.path.join(db_export_dir, f"{workspace_id}.db")
            # Create a copy of the database with workspace_id name
            shutil.copy(db_path, workspace_db)
            print(f"Created workspace-specific database: {workspace_db}")
        except Exception as e:
            print(f"Warning: Could not create workspace-specific database: {str(e)}")

    # Success message
    total_workspaces = len(all_workspace_data)
    if total_workspaces > 1:
        print(
            f"Successfully processed {total_workspaces} workspaces ({total_workspaces - 1} child workspaces)"
        )
    else:
        print("Successfully processed parent workspace")

    return {
        "db_path": db_path,
        "workspace_db_path": workspace_db,
        "csv_dir": export_dir,
        "workspace_count": total_workspaces,
        "workspace_id": workspace_id
    }
