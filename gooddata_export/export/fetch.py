"""Data fetching functions for GoodData API."""

import concurrent.futures
import logging
import time

from gooddata_export.common import get_api_client
from gooddata_export.constants import MAX_CHILD_WORKSPACE_FETCH_WORKERS
from gooddata_export.process import (
    fetch_analytics_model,
    fetch_child_workspaces,
    fetch_data,
    fetch_ldm,
    fetch_users_and_user_groups,
    transform_entities_to_layout,
)

logger = logging.getLogger(__name__)


def fetch_all_data_parallel(config):
    """Fetch all required data from API in parallel.

    For parent workspace, uses the analyticsModel endpoint which returns all
    analytics objects (metrics, dashboards, visualizations, etc.) in layout format.
    This is more efficient than multiple entity API calls and produces the same
    format as local layout.json files.
    """
    client = get_api_client(config=config)

    # Fetch tasks for parent workspace:
    # - analyticsModel: Contains all analytics objects in layout format
    # - ldm: Logical Data Model (datasets, attributes, facts)
    # - child_workspaces: List of child workspaces
    # - users_and_user_groups: Organization-level user data
    fetch_tasks = [
        {"function": fetch_ldm, "args": (client, config), "key": "ldm"},
        {
            "function": fetch_child_workspaces,
            "args": (client, config),
            "key": "child_workspaces",
        },
        {
            "function": fetch_users_and_user_groups,
            "args": (client, config),
            "key": "users_and_user_groups",
        },
        {
            "function": fetch_analytics_model,
            "args": (client, config),
            "key": "analytics_model",
        },
    ]

    results = {}

    # Use ThreadPoolExecutor to run all fetch operations in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_key = {}
        for task in fetch_tasks:
            future = executor.submit(task["function"], *task["args"])
            future_to_key[future] = task["key"]

        for future in concurrent.futures.as_completed(future_to_key):
            key = future_to_key[future]
            try:
                results[key] = future.result()
            except Exception as e:
                logger.error("Error fetching %s: %s", key, e)
                results[key] = None

    # Extract analytics objects from analyticsModel response (layout format)
    # The analyticsModel endpoint returns: {"analytics": {"metrics": [...], ...}}
    analytics_model = results.get("analytics_model") or {}
    analytics = analytics_model.get("analytics", {})

    # Return data in layout format (same structure as local layout.json)
    return {
        "metrics": analytics.get("metrics") or [],
        "dashboards": analytics.get("analyticalDashboards") or [],
        "visualizations": analytics.get("visualizationObjects") or [],
        "filter_contexts": analytics.get("filterContexts") or [],
        "plugins": analytics.get("dashboardPlugins") or [],
        "ldm": results.get("ldm"),
        "child_workspaces": results.get("child_workspaces"),
        "users_and_user_groups": results.get("users_and_user_groups"),
        "analytics_model": analytics_model,  # Keep full response for permissions
    }


def fetch_data_from_workspace(workspace_id, workspace_name, config):
    """Fetch selected data from a specific child workspace.

    Uses entity API for selective fetching (only requested data types),
    then transforms results to layout format for uniform processing.
    LDM is not fetched as it's shared from parent workspace.
    """
    start_time = time.time()

    # Create a client for this specific workspace
    original_client = get_api_client(config=config)
    workspace_client = {
        "base_url": original_client["base_url"],
        "workspace_id": workspace_id,
        "headers": original_client["headers"],
        "params": original_client["params"],
    }

    workspace_data = {}

    # Define all possible fetch tasks (excluding LDM - it's shared across workspaces)
    # NOTE: analytics_model (layout API) is intentionally NOT fetched for child workspaces
    # to minimize API calls. This means dashboards_permissions will only contain parent
    # workspace data. Can be added here later if child workspace permissions are needed.
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
        "plugins": {
            "function": fetch_data,
            "args": ("dashboardPlugins", workspace_client, config),
            "key": "plugins",
        },
    }

    # Filter tasks based on configuration - only fetch requested data types
    fetch_tasks = []
    for data_type in config.CHILD_WORKSPACE_DATA_TYPES:
        if data_type in all_fetch_tasks:
            fetch_tasks.append(all_fetch_tasks[data_type])
        else:
            logger.warning(
                "Unknown data type '%s' in CHILD_WORKSPACE_DATA_TYPES", data_type
            )

    # Set all non-requested data types to None
    for data_type, task_info in all_fetch_tasks.items():
        if data_type not in config.CHILD_WORKSPACE_DATA_TYPES:
            workspace_data[task_info["key"]] = None

    requested_types = ", ".join(config.CHILD_WORKSPACE_DATA_TYPES)
    logger.debug("Fetching from %s: %s", workspace_name, requested_types)

    # Only proceed with parallel fetching if there are tasks to execute
    if not fetch_tasks:
        logger.debug("No data types configured for fetching from %s", workspace_name)
    else:
        # Use parallel fetching within this workspace too but limit concurrency
        max_workers = min(len(fetch_tasks), MAX_CHILD_WORKSPACE_FETCH_WORKERS)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_key = {}
            for task in fetch_tasks:
                future = executor.submit(task["function"], *task["args"])
                future_to_key[future] = task["key"]

            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    entity_data = future.result()
                    # Transform entity API format to layout format
                    if entity_data:
                        workspace_data[key] = transform_entities_to_layout(entity_data)
                    else:
                        workspace_data[key] = None
                except Exception as e:
                    # Check if this is a permission issue or just missing data
                    error_msg = str(e).lower()
                    if any(
                        keyword in error_msg
                        for keyword in ["401", "403", "unauthorized", "forbidden"]
                    ):
                        logger.debug(
                            "Permission issue accessing %s in workspace %s: %s",
                            key,
                            workspace_id,
                            e,
                        )
                    else:
                        logger.debug(
                            "Error fetching %s from workspace %s: %s",
                            key,
                            workspace_id,
                            e,
                        )
                    workspace_data[key] = None

    # LDM is not fetched for child workspaces as it's shared from parent
    workspace_data["ldm"] = None

    # Add workspace info to the data
    workspace_data["workspace_id"] = workspace_id
    workspace_data["workspace_name"] = workspace_name

    duration = time.time() - start_time
    logger.debug(
        "Workspace %s data fetch completed in %.2f seconds",
        workspace_name,
        duration,
    )

    return workspace_data


def fetch_all_workspace_data(config):
    """Fetch data from parent workspace and optionally child workspaces"""
    # Start with parent workspace data (includes LDM which is shared across all workspaces)
    start_time = time.time()
    logger.debug("Fetching parent workspace data...")
    parent_data = fetch_all_data_parallel(config)
    client = get_api_client(config=config)
    parent_workspace_id = client["workspace_id"]

    parent_duration = time.time() - start_time
    logger.info(
        "Parent workspace data fetch completed in %.2f seconds", parent_duration
    )

    logger.debug("Parent workspace ID: %s", parent_workspace_id)
    logger.debug("Child workspaces enabled: %s", config.INCLUDE_CHILD_WORKSPACES)

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
            logger.info(
                "Processing %d child workspaces in parallel...", len(child_workspaces)
            )
            logger.info("Fetching data types: %s", data_types)
            logger.info("Using %d parallel workers", config.MAX_PARALLEL_WORKSPACES)

            if not config.CHILD_WORKSPACE_DATA_TYPES:
                logger.warning(
                    "No data types configured for child workspaces - "
                    "only workspace metadata will be collected"
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

                    logger.debug(
                        "Starting fetch for child workspace: %s (%s)",
                        child_workspace_name,
                        child_workspace_id,
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

                        # Progress reporting (user-facing output)
                        percentage = (completed_count / total_count) * 100
                        elapsed_time = time.time() - child_start_time

                        if completed_count > 0:
                            avg_time_per_workspace = elapsed_time / completed_count
                            remaining_workspaces = total_count - completed_count
                            estimated_remaining_time = (
                                avg_time_per_workspace * remaining_workspaces
                            )

                            logger.info(
                                "Progress: %d/%d (%.1f%%) - Elapsed: %.1fs - "
                                "ETA: %.1fs - Completed: %s",
                                completed_count,
                                total_count,
                                percentage,
                                elapsed_time,
                                estimated_remaining_time,
                                workspace_info["name"],
                            )

                    except Exception as e:
                        percentage = (completed_count / total_count) * 100
                        elapsed_time = time.time() - child_start_time

                        logger.info(
                            "Progress: %d/%d (%.1f%%) - Elapsed: %.1fs - Error: %s",
                            completed_count,
                            total_count,
                            percentage,
                            elapsed_time,
                            workspace_info["name"],
                        )

                        logger.warning(
                            "Could not fetch data from child workspace %s: %s",
                            workspace_info["name"],
                            e,
                        )

            child_duration = time.time() - child_start_time
            logger.info(
                "Child workspaces data fetch completed in %.2f seconds", child_duration
            )
            logger.info(
                "Average time per workspace: %.2f seconds",
                child_duration / total_count,
            )

        else:
            logger.debug("No child workspaces found for this parent workspace")
            logger.debug("This could mean:")
            logger.debug("1. This workspace has no child workspaces")
            logger.debug("2. This workspace is not a parent workspace")
            logger.debug("3. There was an error fetching child workspaces")
    else:
        logger.debug("Child workspace processing is disabled")

    total_duration = time.time() - start_time

    # Show detailed summary only when processing multiple workspaces
    if len(all_workspace_data) > 1:
        child_count = len(all_workspace_data) - 1
        child_duration = total_duration - parent_duration

        logger.info("")
        logger.info("=" * 70)
        logger.info("API FETCH PHASE SUMMARY")
        logger.info("=" * 70)
        logger.info("Total workspaces fetched: %d", len(all_workspace_data))
        logger.info("Parent workspace: 1")
        logger.info("Child workspaces: %d", child_count)
        logger.info("API fetch time: %.2f seconds", total_duration)
        logger.info(
            "Average fetch time per workspace: %.2f seconds",
            total_duration / len(all_workspace_data),
        )
        logger.info("Child workspaces API fetch time: %.2f seconds", child_duration)
        logger.info(
            "Average fetch time per child workspace: %.2f seconds",
            child_duration / child_count,
        )
        logger.info("Parallel workers used: %d", config.MAX_PARALLEL_WORKSPACES)

        # Performance metrics
        if child_duration > 0:
            workspaces_per_minute = (child_count / child_duration) * 60
            logger.info(
                "Processing rate: %.1f workspaces/minute", workspaces_per_minute
            )

    return all_workspace_data
