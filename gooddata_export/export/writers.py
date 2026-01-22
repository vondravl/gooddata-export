"""Database and CSV writer functions for exporting GoodData metadata."""

import logging
from pathlib import Path

from gooddata_export.common import get_api_client
from gooddata_export.db import database_connection, setup_table, setup_tables
from gooddata_export.export.utils import (
    execute_with_retry,
    log_export,
    serialize_content,
    write_to_csv,
)
from gooddata_export.process import (
    process_dashboards,
    process_dashboards_metrics_from_rich_text,
    process_dashboards_permissions_from_analytics_model,
    process_dashboards_plugins,
    process_dashboards_visualizations,
    process_dashboards_widget_filters,
    process_filter_context_fields,
    process_filter_contexts,
    process_ldm,
    process_metrics,
    process_plugins,
    process_user_group_members,
    process_user_groups,
    process_users,
    process_visualizations,
    process_visualizations_attributes,
    process_visualizations_metrics,
    process_workspaces,
)

logger = logging.getLogger(__name__)


def export_metrics(all_workspace_data, export_dir, config, db_name) -> None:
    """Export metrics to both CSV and SQLite"""
    all_processed_data = []

    # Process metrics from all workspaces
    for workspace_info in all_workspace_data:
        workspace_id = workspace_info["workspace_id"]
        raw_data = workspace_info["data"].get("metrics")

        if raw_data is None:
            if config.DEBUG_WORKSPACE_PROCESSING:
                logger.debug("No metrics data for workspace %s", workspace_id)
            continue

        processed_data = process_metrics(raw_data, workspace_id)
        all_processed_data.extend(processed_data)

    # Define column schema
    columns = {
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
        "is_hidden": "BOOLEAN",
        "origin_type": "TEXT",
        "content": "JSON",
        "PRIMARY KEY": "(metric_id, workspace_id)",
    }

    with database_connection(db_name) as conn:
        setup_table(conn, "metrics", columns)

        if not all_processed_data:
            logger.info("No metrics found - table created but empty")
            return

        # Export to CSV if requested
        count = len(all_processed_data)
        if export_dir is not None:
            count = write_to_csv(
                all_processed_data,
                export_dir,
                "gooddata_metrics.csv",
                fieldnames=columns.keys(),
                exclude_fields={"content"},
            )

        execute_with_retry(
            conn.cursor(),
            """
            INSERT INTO metrics
            (metric_id, workspace_id, title, description, tags, maql, format,
             created_at, modified_at, is_valid, is_hidden, origin_type, content)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    d["is_hidden"],
                    d["origin_type"],
                    serialize_content(d["content"], config),
                )
                for d in all_processed_data
            ],
        )
        conn.commit()

    if export_dir is not None:
        log_export("metrics", count, Path(export_dir) / "gooddata_metrics.csv")
    else:
        logger.info("Exported %d metrics to %s", count, db_name)


def export_visualizations(all_workspace_data, export_dir, config, db_name) -> None:
    """Export visualizations, visualization-metrics, and visualization-attributes relationships"""

    client = get_api_client(config=config)
    all_processed_visualizations = []
    all_processed_metric_relationships = []
    all_processed_attribute_relationships = []

    # Process visualizations from all workspaces
    for workspace_info in all_workspace_data:
        workspace_id = workspace_info["workspace_id"]
        raw_data = workspace_info["data"].get("visualizations")

        if raw_data is None:
            continue

        # Process visualizations and both relationship types from same raw data
        processed_visualizations = process_visualizations(
            raw_data, client["base_url"], workspace_id
        )
        processed_metric_relationships = process_visualizations_metrics(
            raw_data, workspace_id
        )
        processed_attribute_relationships = process_visualizations_attributes(
            raw_data, workspace_id
        )

        all_processed_visualizations.extend(processed_visualizations)
        all_processed_metric_relationships.extend(processed_metric_relationships)
        all_processed_attribute_relationships.extend(processed_attribute_relationships)

    # Define column schemas
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
        "is_hidden": "BOOLEAN",
        "PRIMARY KEY": "(visualization_id, workspace_id)",
    }
    metric_relationship_columns = {
        "visualization_id": "TEXT",
        "metric_id": "TEXT",
        "workspace_id": "TEXT",
        "label": "TEXT",
        "PRIMARY KEY": "(visualization_id, metric_id, workspace_id)",
    }
    attribute_relationship_columns = {
        "visualization_id": "TEXT",
        "attribute_id": "TEXT",
        "workspace_id": "TEXT",
        "label": "TEXT",
        "PRIMARY KEY": "(visualization_id, attribute_id, workspace_id)",
    }

    # Always create all tables (even if empty) for consistency
    with database_connection(db_name) as conn:
        setup_tables(
            conn,
            [
                ("visualizations", visualization_columns),
                ("visualizations_metrics", metric_relationship_columns),
                ("visualizations_attributes", attribute_relationship_columns),
            ],
        )

        if not all_processed_visualizations:
            logger.info("No visualizations found - tables created but empty")
            return

        # Export to CSV (if requested)
        vis_count = len(all_processed_visualizations)
        if export_dir is not None:
            vis_count = write_to_csv(
                all_processed_visualizations,
                export_dir,
                "gooddata_visualizations.csv",
                fieldnames=visualization_columns.keys(),
                exclude_fields={"content"},
            )

        execute_with_retry(
            conn.cursor(),
            """
            INSERT INTO visualizations
            (visualization_id, workspace_id, title, description, tags, visualization_url, created_at, modified_at, url_link, origin_type, content, is_valid, is_hidden)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    serialize_content(d["content"], config),
                    d["is_valid"],
                    d["is_hidden"],
                )
                for d in all_processed_visualizations
            ],
        )

        # Export visualization-metrics relationships
        metric_rel_count = len(all_processed_metric_relationships)
        if export_dir is not None and all_processed_metric_relationships:
            metric_rel_count = write_to_csv(
                all_processed_metric_relationships,
                export_dir,
                "gooddata_visualizations_metrics.csv",
                fieldnames=["visualization_id", "metric_id", "workspace_id", "label"],
            )

        if all_processed_metric_relationships:
            execute_with_retry(
                conn.cursor(),
                """
                INSERT INTO visualizations_metrics
                (visualization_id, metric_id, workspace_id, label)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (
                        d["visualization_id"],
                        d["metric_id"],
                        d["workspace_id"],
                        d.get("label"),
                    )
                    for d in all_processed_metric_relationships
                ],
            )

        # Export visualization-attributes relationships
        attr_rel_count = len(all_processed_attribute_relationships)
        if export_dir is not None and all_processed_attribute_relationships:
            attr_rel_count = write_to_csv(
                all_processed_attribute_relationships,
                export_dir,
                "gooddata_visualizations_attributes.csv",
                fieldnames=[
                    "visualization_id",
                    "attribute_id",
                    "workspace_id",
                    "label",
                ],
            )

        if all_processed_attribute_relationships:
            execute_with_retry(
                conn.cursor(),
                """
                INSERT INTO visualizations_attributes
                (visualization_id, attribute_id, workspace_id, label)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (
                        d["visualization_id"],
                        d["attribute_id"],
                        d["workspace_id"],
                        d.get("label"),
                    )
                    for d in all_processed_attribute_relationships
                ],
            )

        conn.commit()

    if export_dir is not None:
        log_export(
            "visualizations",
            vis_count,
            Path(export_dir) / "gooddata_visualizations.csv",
        )
        log_export(
            "visualization-metric relationships",
            metric_rel_count,
            Path(export_dir) / "gooddata_visualizations_metrics.csv",
        )
        log_export(
            "visualization-attribute relationships",
            attr_rel_count,
            Path(export_dir) / "gooddata_visualizations_attributes.csv",
        )
    else:
        logger.info("Exported %d visualizations to %s", vis_count, db_name)
        logger.info(
            "Exported %d visualization-metric relationships to %s",
            metric_rel_count,
            db_name,
        )
        logger.info(
            "Exported %d visualization-attribute relationships to %s",
            attr_rel_count,
            db_name,
        )


def export_dashboards(all_workspace_data, export_dir, config, db_name) -> None:
    """Export dashboards, dashboard-visualization relationships, and dashboard-plugin relationships"""

    client = get_api_client(config=config)
    all_processed_dashboards = []
    all_processed_relationships = []
    all_processed_plugin_relationships = []
    all_processed_widget_filters = []

    # Build known insights once from all workspaces before the loop
    # Parent insights come first, then child-specific insights are added (duplicates ignored by set)
    # This is more efficient than rebuilding per-workspace and ensures child workspaces
    # can reference inherited parent visualizations
    known_insights = set()
    for workspace_info in all_workspace_data:
        visualizations_raw = workspace_info["data"].get("visualizations")
        if visualizations_raw:
            for viz in visualizations_raw:
                if isinstance(viz, dict) and viz.get("id"):
                    known_insights.add(viz["id"])

    if known_insights:
        logger.info("Found %d known insights for validation", len(known_insights))

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

        # When child workspaces are included, only enable rich text extraction for parent workspace
        workspace_config = config
        if config.INCLUDE_CHILD_WORKSPACES and not workspace_info.get(
            "is_parent", False
        ):
            workspace_config = config.with_rich_text_disabled()
            logger.debug(
                "Rich text extraction disabled for child workspace: %s", workspace_id
            )

        processed_relationships = process_dashboards_visualizations(
            raw_data, workspace_id, known_insights, workspace_config
        )

        # Process dashboard-plugin relationships
        processed_plugin_relationships = process_dashboards_plugins(
            raw_data, workspace_id
        )

        # Process widget-level filter configuration
        processed_widget_filters = process_dashboards_widget_filters(
            raw_data, workspace_id
        )

        all_processed_dashboards.extend(processed_dashboards)
        all_processed_relationships.extend(processed_relationships)
        all_processed_plugin_relationships.extend(processed_plugin_relationships)
        all_processed_widget_filters.extend(processed_widget_filters)

    # Define column schemas
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
        "is_hidden": "BOOLEAN",
        "filter_context_id": "TEXT",
        "PRIMARY KEY": "(dashboard_id, workspace_id)",
    }
    relationship_columns = {
        "dashboard_id": "TEXT",
        "visualization_id": "TEXT",
        "tab_id": "TEXT",  # Tab localIdentifier, NULL for legacy non-tabbed dashboards
        "from_rich_text": "INTEGER DEFAULT 0",
        "widget_title": "TEXT",  # Overridden title on dashboard, NULL if not set
        "widget_description": "TEXT",  # Overridden description on dashboard, NULL if not set
        "workspace_id": "TEXT",
        "PRIMARY KEY": "(dashboard_id, visualization_id, tab_id, from_rich_text, workspace_id)",
    }
    plugin_relationship_columns = {
        "dashboard_id": "TEXT",
        "plugin_id": "TEXT",
        "workspace_id": "TEXT",
        "PRIMARY KEY": "(dashboard_id, plugin_id, workspace_id)",
    }
    widget_filters_columns = {
        "dashboard_id": "TEXT",
        "visualization_id": "TEXT",  # NULL for non-insight widgets
        "tab_id": "TEXT",  # NULL for legacy non-tabbed dashboards
        "widget_local_identifier": "TEXT",
        "filter_type": "TEXT",  # 'ignoreDashboardFilters' or 'dateDataSet'
        "reference_type": "TEXT",  # 'attributeFilterReference', 'dateFilterReference', or 'dataset'
        "reference_id": "TEXT",  # The display form ID or dataset ID
        "reference_object_type": "TEXT",  # 'label', 'dataset', etc.
        "workspace_id": "TEXT",
        "PRIMARY KEY": "(dashboard_id, widget_local_identifier, filter_type, reference_id, workspace_id)",
    }

    # Always create all tables (even if empty) for consistency
    with database_connection(db_name) as conn:
        setup_tables(
            conn,
            [
                ("dashboards", dashboard_columns),
                ("dashboards_visualizations", relationship_columns),
                ("dashboards_plugins", plugin_relationship_columns),
                ("dashboards_widget_filters", widget_filters_columns),
            ],
        )

        if not all_processed_dashboards:
            logger.info("No dashboards found - tables created but empty")
            return

        # Export to CSV (if requested)
        dash_count = len(all_processed_dashboards)
        if export_dir is not None:
            dash_count = write_to_csv(
                all_processed_dashboards,
                export_dir,
                "gooddata_dashboards.csv",
                fieldnames=dashboard_columns.keys(),
                exclude_fields={"content"},
            )

        execute_with_retry(
            conn.cursor(),
            """
            INSERT INTO dashboards
            (dashboard_id, workspace_id, title, description, tags, created_at, modified_at, dashboard_url, origin_type, content, is_valid, is_hidden, filter_context_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    serialize_content(d["content"], config),
                    d["is_valid"],
                    d["is_hidden"],
                    d.get("filter_context_id"),
                )
                for d in all_processed_dashboards
            ],
        )

        # Export dashboard-visualization relationships
        rel_count = len(all_processed_relationships)
        if export_dir is not None and all_processed_relationships:
            rel_count = write_to_csv(
                all_processed_relationships,
                export_dir,
                "gooddata_dashboards_visualizations.csv",
                fieldnames=[
                    "dashboard_id",
                    "visualization_id",
                    "tab_id",
                    "from_rich_text",
                    "widget_title",
                    "widget_description",
                    "workspace_id",
                ],
            )

        if all_processed_relationships:
            execute_with_retry(
                conn.cursor(),
                """
                INSERT INTO dashboards_visualizations
                (dashboard_id, visualization_id, tab_id, from_rich_text, widget_title, widget_description, workspace_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        d["dashboard_id"],
                        d["visualization_id"],
                        d.get("tab_id"),  # NULL for legacy non-tabbed dashboards
                        d.get("from_rich_text", 0),
                        d.get("widget_title"),  # NULL if not set
                        d.get("widget_description"),  # NULL if not set
                        d["workspace_id"],
                    )
                    for d in all_processed_relationships
                ],
            )

        # Export dashboard-plugin relationships
        plugin_rel_count = len(all_processed_plugin_relationships)
        if export_dir is not None and all_processed_plugin_relationships:
            plugin_rel_count = write_to_csv(
                all_processed_plugin_relationships,
                export_dir,
                "gooddata_dashboards_plugins.csv",
                fieldnames=["dashboard_id", "plugin_id", "workspace_id"],
            )

        if all_processed_plugin_relationships:
            execute_with_retry(
                conn.cursor(),
                """
                INSERT INTO dashboards_plugins
                (dashboard_id, plugin_id, workspace_id)
                VALUES (?, ?, ?)
                """,
                [
                    (d["dashboard_id"], d["plugin_id"], d["workspace_id"])
                    for d in all_processed_plugin_relationships
                ],
            )

        # Export widget filter configuration
        widget_filters_count = len(all_processed_widget_filters)
        if export_dir is not None and all_processed_widget_filters:
            widget_filters_count = write_to_csv(
                all_processed_widget_filters,
                export_dir,
                "gooddata_dashboards_widget_filters.csv",
                fieldnames=[
                    "dashboard_id",
                    "visualization_id",
                    "tab_id",
                    "widget_local_identifier",
                    "filter_type",
                    "reference_type",
                    "reference_id",
                    "reference_object_type",
                    "workspace_id",
                ],
            )

        if all_processed_widget_filters:
            execute_with_retry(
                conn.cursor(),
                """
                INSERT INTO dashboards_widget_filters
                (dashboard_id, visualization_id, tab_id, widget_local_identifier,
                 filter_type, reference_type, reference_id, reference_object_type, workspace_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        d["dashboard_id"],
                        d.get("visualization_id"),
                        d.get("tab_id"),
                        d["widget_local_identifier"],
                        d["filter_type"],
                        d["reference_type"],
                        d["reference_id"],
                        d["reference_object_type"],
                        d["workspace_id"],
                    )
                    for d in all_processed_widget_filters
                ],
            )

        conn.commit()

    if export_dir is not None:
        log_export(
            "dashboards",
            dash_count,
            Path(export_dir) / "gooddata_dashboards.csv",
        )
        log_export(
            "dashboard-visualization relationships",
            rel_count,
            Path(export_dir) / "gooddata_dashboards_visualizations.csv",
        )
        if plugin_rel_count > 0:
            log_export(
                "dashboard-plugin relationships",
                plugin_rel_count,
                Path(export_dir) / "gooddata_dashboards_plugins.csv",
            )
        if widget_filters_count > 0:
            log_export(
                "widget filter configurations",
                widget_filters_count,
                Path(export_dir) / "gooddata_dashboards_widget_filters.csv",
            )
    else:
        logger.info("Exported %d dashboards to %s", dash_count, db_name)
        logger.info(
            "Exported %d dashboard-visualization relationships to %s",
            rel_count,
            db_name,
        )
        if plugin_rel_count > 0:
            logger.info(
                "Exported %d dashboard-plugin relationships to %s",
                plugin_rel_count,
                db_name,
            )
        if widget_filters_count > 0:
            logger.info(
                "Exported %d widget filter configurations to %s",
                widget_filters_count,
                db_name,
            )


def export_ldm(all_workspace_data, export_dir, _config, db_name) -> None:
    """Export logical data model to both CSV and SQLite.

    Args:
        all_workspace_data: List of workspace data dictionaries
        export_dir: Directory for CSV output (None to skip CSV)
        _config: ExportConfig instance (unused, kept for interface consistency)
        db_name: Path to SQLite database
    """
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
        "tags": "TEXT",
        "attributes_count": "INTEGER",
        "facts_count": "INTEGER",
        "references_count": "INTEGER",
        "workspace_data_filter_columns_count": "INTEGER",
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

    with database_connection(db_name) as conn:
        cursor = setup_table(conn, "ldm_datasets", dataset_columns)

        execute_with_retry(
            cursor,
            """
            INSERT INTO ldm_datasets
            (title, description, id, tags, attributes_count, facts_count, references_count,
             workspace_data_filter_columns_count, total_columns,
             data_source_id, source_table, source_table_path, workspace_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    d["title"],
                    d["description"],
                    d["id"],
                    d["tags"],
                    d["attributes_count"],
                    d["facts_count"],
                    d["references_count"],
                    d["workspace_data_filter_columns_count"],
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
            log_export(
                "datasets",
                dataset_count,
                Path(export_dir) / "gooddata_ldm_datasets.csv",
            )
            log_export(
                "columns",
                column_count,
                Path(export_dir) / "gooddata_ldm_columns.csv",
            )
        else:
            logger.info("Exported %d datasets to %s", dataset_count, db_name)
            logger.info("Exported %d columns to %s", column_count, db_name)


def export_filter_contexts(all_workspace_data, export_dir, config, db_name) -> None:
    """Export filter contexts and filter context fields to both CSV and SQLite.

    Args:
        all_workspace_data: List of workspace data dictionaries
        export_dir: Directory for CSV output (None to skip CSV)
        config: ExportConfig instance
        db_name: Path to SQLite database
    """
    all_processed_data = []
    all_processed_fields = []

    # Process filter contexts from all workspaces
    for workspace_info in all_workspace_data:
        workspace_id = workspace_info["workspace_id"]
        raw_data = workspace_info["data"].get("filter_contexts")

        if raw_data is None:
            continue

        # Process main filter contexts table
        processed = process_filter_contexts(raw_data, workspace_id)
        all_processed_data.extend(processed)

        # Process filter context fields (individual filters)
        processed_fields = process_filter_context_fields(raw_data, workspace_id)
        all_processed_fields.extend(processed_fields)

    # Define column schemas
    filter_contexts_columns = {
        "filter_context_id": "TEXT",
        "workspace_id": "TEXT",
        "title": "TEXT",
        "description": "TEXT",
        "origin_type": "TEXT",
        "content": "JSON",
        "PRIMARY KEY": "(filter_context_id, workspace_id)",
    }
    filter_context_fields_columns = {
        "filter_context_id": "TEXT",
        "workspace_id": "TEXT",
        "filter_index": "INTEGER",
        "filter_type": "TEXT",
        "local_identifier": "TEXT",
        "display_form_id": "TEXT",
        "title": "TEXT",
        "negative_selection": "BOOLEAN",
        "selection_mode": "TEXT",
        "date_granularity": "TEXT",
        "date_from": "INTEGER",
        "date_to": "INTEGER",
        "date_type": "TEXT",
        "attribute_elements_count": "INTEGER",
        "PRIMARY KEY": "(filter_context_id, workspace_id, filter_index)",
    }

    # Always create all tables (even if empty) for consistency
    with database_connection(db_name) as conn:
        setup_tables(
            conn,
            [
                ("filter_contexts", filter_contexts_columns),
                ("filter_context_fields", filter_context_fields_columns),
            ],
        )

        if not all_processed_data:
            logger.info("No filter contexts found - tables created but empty")
            return

        # Export to CSV (if requested)
        records_count = len(all_processed_data)
        if export_dir is not None:
            records_count = write_to_csv(
                all_processed_data,
                export_dir,
                "gooddata_filter_contexts.csv",
                fieldnames=filter_contexts_columns.keys(),
                exclude_fields={"content"},
            )

        execute_with_retry(
            conn.cursor(),
            """
            INSERT INTO filter_contexts
            (filter_context_id, workspace_id, title, description, origin_type, content)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    d["filter_context_id"],
                    d["workspace_id"],
                    d["title"],
                    d["description"],
                    d["origin_type"],
                    serialize_content(d["content"], config),
                )
                for d in all_processed_data
            ],
        )

        # Export filter_context_fields table
        fields_count = len(all_processed_fields)
        if export_dir is not None and all_processed_fields:
            fields_count = write_to_csv(
                all_processed_fields,
                export_dir,
                "gooddata_filter_context_fields.csv",
                fieldnames=filter_context_fields_columns.keys(),
            )

        if all_processed_fields:
            execute_with_retry(
                conn.cursor(),
                """
                INSERT INTO filter_context_fields
                (filter_context_id, workspace_id, filter_index, filter_type, local_identifier,
                 display_form_id, title, negative_selection, selection_mode, date_granularity,
                 date_from, date_to, date_type, attribute_elements_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        d["filter_context_id"],
                        d["workspace_id"],
                        d["filter_index"],
                        d["filter_type"],
                        d["local_identifier"],
                        d["display_form_id"],
                        d["title"],
                        d["negative_selection"],
                        d["selection_mode"],
                        d["date_granularity"],
                        d["date_from"],
                        d["date_to"],
                        d["date_type"],
                        d["attribute_elements_count"],
                    )
                    for d in all_processed_fields
                ],
            )

        conn.commit()

    if export_dir is not None:
        log_export(
            "filter contexts",
            records_count,
            Path(export_dir) / "gooddata_filter_contexts.csv",
        )
        if fields_count > 0:
            log_export(
                "filter context fields",
                fields_count,
                Path(export_dir) / "gooddata_filter_context_fields.csv",
            )
    else:
        logger.info("Exported %d filter contexts to %s", records_count, db_name)
        if fields_count > 0:
            logger.info(
                "Exported %d filter context fields to %s", fields_count, db_name
            )


def export_workspaces(all_workspace_data, export_dir, config, db_name) -> None:
    """Export workspaces to both CSV and SQLite"""
    # Always export workspaces table (quick to create and useful for reference)
    # Note: Child workspace DATA (metrics, dashboards, etc.) is still conditional on INCLUDE_CHILD_WORKSPACES

    # Get the parent workspace info
    client = get_api_client(config=config)
    parent_workspace_id = client["workspace_id"]
    parent_workspace_name = f"Parent Workspace ({parent_workspace_id})"

    # Get child workspaces from the first workspace data (parent)
    child_workspaces_data = []
    if all_workspace_data and all_workspace_data[0]["data"].get("child_workspaces"):
        child_workspaces_data = all_workspace_data[0]["data"]["child_workspaces"]

    processed_data = process_workspaces(
        parent_workspace_id, parent_workspace_name, child_workspaces_data
    )

    # Define column schema
    columns = {
        "workspace_id": "TEXT PRIMARY KEY",
        "workspace_name": "TEXT",
        "is_parent": "BOOLEAN",
        "parent_workspace_id": "TEXT",
        "created_at": "DATE",
        "modified_at": "DATE",
    }

    with database_connection(db_name) as conn:
        setup_table(conn, "workspaces", columns)

        if not processed_data:
            logger.info("No workspaces found - table created but empty")
            return

        # Export to CSV if requested
        count = len(processed_data)
        if export_dir is not None:
            count = write_to_csv(
                processed_data,
                export_dir,
                "gooddata_workspaces.csv",
                fieldnames=columns.keys(),
            )

        execute_with_retry(
            conn.cursor(),
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
        log_export("workspaces", count, Path(export_dir) / "gooddata_workspaces.csv")
    else:
        logger.info("Exported %d workspaces to %s", count, db_name)


def export_dashboards_metrics(all_workspace_data, export_dir, config, db_name) -> None:
    """Export metrics used in rich text widgets on dashboards across all workspaces"""

    # Always create the table structure (even if empty) to ensure post-processing SQL works
    # This is needed because views like v_metrics_usage depend on this table existing
    dashboards_metrics_columns = {
        "dashboard_id": "TEXT",
        "metric_id": "TEXT",
        "workspace_id": "TEXT",
        "PRIMARY KEY": "(dashboard_id, metric_id, workspace_id)",
    }

    # Connect to database and ensure table exists
    # Note: setup_table already does DROP TABLE IF EXISTS + CREATE TABLE
    with database_connection(db_name) as conn:
        setup_table(conn, "dashboards_metrics", dashboards_metrics_columns)
        conn.commit()

        # If rich text extraction is disabled, keep empty table and return
        if not config.ENABLE_RICH_TEXT_EXTRACTION:
            logger.info(
                "Rich text extraction disabled - dashboards_metrics table created but empty"
            )
            return

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
                logger.info(
                    "Found %d known metrics for validation (source: memory)",
                    len(known_metrics),
                )
        except Exception as e:
            logger.warning("Failed to load in-memory metrics for validation: %s", e)

        # If still empty, proceed without known metrics filtering (no API fallback)
        if not known_metrics:
            logger.info(
                "No known metrics available for validation from memory or db; "
                "proceeding without filter"
            )

        # When child workspaces are included, only process parent workspace for rich text
        workspaces_to_process = all_workspace_data
        if config.INCLUDE_CHILD_WORKSPACES:
            workspaces_to_process = [
                ws for ws in all_workspace_data if ws.get("is_parent", False)
            ]
            logger.info(
                "Rich text extraction: Processing only parent workspace "
                "(found %d parent workspace(s) out of %d total)",
                len(workspaces_to_process),
                len(all_workspace_data),
            )

        # Process dashboards from workspaces using the process function
        rich_text_metrics = []
        for workspace_info in workspaces_to_process:
            workspace_id = workspace_info["workspace_id"]
            workspace_dashboards = workspace_info["data"].get("dashboards")
            if not workspace_dashboards:
                continue

            # Use the process function for dashboard traversal
            workspace_metrics = process_dashboards_metrics_from_rich_text(
                workspace_dashboards,
                workspace_id=workspace_id,
                known_metrics=known_metrics,
                config=config,
            )
            rich_text_metrics.extend(workspace_metrics)

        # If no metrics found, table already exists (created earlier), just log and return
        if not rich_text_metrics:
            logger.info(
                "No metrics found in rich text widgets - "
                "dashboards_metrics table created but empty"
            )
            return

        # Export to CSV (if requested)
        records_count = len(rich_text_metrics)
        if export_dir is not None:
            csv_filename = "gooddata_dashboards_metrics.csv"
            records_count = write_to_csv(
                rich_text_metrics,
                export_dir,
                csv_filename,
                fieldnames=["dashboard_id", "metric_id", "workspace_id"],
            )

        # Insert data into the table (table was already created earlier)
        execute_with_retry(
            conn.cursor(),
            """
            INSERT INTO dashboards_metrics
            (dashboard_id, metric_id, workspace_id)
            VALUES (?, ?, ?)
            """,
            [
                (d["dashboard_id"], d["metric_id"], d["workspace_id"])
                for d in rich_text_metrics
            ],
        )

        conn.commit()
        if export_dir is not None:
            log_export(
                "dashboard metrics from rich text",
                records_count,
                Path(export_dir) / "gooddata_dashboards_metrics.csv",
            )
        else:
            logger.info(
                "Exported %d dashboard metrics from rich text to %s",
                records_count,
                db_name,
            )


def export_users_and_user_groups(
    all_workspace_data, export_dir, config, db_name
) -> None:
    """Export users, user_groups, and user_group_members tables.

    Users and user groups are organization-level (not workspace-specific),
    so we only need data from the parent workspace.

    Args:
        all_workspace_data: List of workspace data dictionaries
        export_dir: Directory for CSV output (None to skip CSV)
        config: ExportConfig instance
        db_name: Path to SQLite database
    """
    # Users and user groups are fetched from the parent workspace only
    if not all_workspace_data:
        raise RuntimeError("No workspace data available")

    parent_workspace_info = all_workspace_data[0]
    raw_data = parent_workspace_info["data"].get("users_and_user_groups")

    # Define table schemas upfront (needed for empty table creation)
    users_columns = {
        "user_id": "TEXT PRIMARY KEY",
        "firstname": "TEXT",
        "lastname": "TEXT",
        "email": "TEXT",
        "authentication_id": "TEXT",
        "user_group_ids": "TEXT",
        "user_group_count": "INTEGER",
        "content": "JSON",
    }
    user_groups_columns = {
        "user_group_id": "TEXT PRIMARY KEY",
        "name": "TEXT",
        "parent_ids": "TEXT",
        "parent_count": "INTEGER",
        "content": "JSON",
    }
    membership_columns = {
        "user_id": "TEXT",
        "user_group_id": "TEXT",
        "PRIMARY KEY": "(user_id, user_group_id)",
    }

    # Always create tables (even if empty) for consistency with other export functions
    with database_connection(db_name) as conn:
        setup_tables(
            conn,
            [
                ("users", users_columns),
                ("user_groups", user_groups_columns),
                ("user_group_members", membership_columns),
            ],
        )

        if raw_data is None:
            logger.info(
                "No users and user groups data found - tables created but empty"
            )
            return

        # Process data
        processed_users = process_users(raw_data)
        processed_user_groups = process_user_groups(raw_data)
        processed_memberships = process_user_group_members(raw_data)

        # --- Export users ---
        users_count = len(processed_users)
        if export_dir is not None:
            users_count = write_to_csv(
                processed_users,
                export_dir,
                "gooddata_users.csv",
                fieldnames=users_columns.keys(),
                exclude_fields={"content"},
            )

        if processed_users:
            execute_with_retry(
                conn.cursor(),
                """
                INSERT INTO users
                (user_id, firstname, lastname, email, authentication_id, user_group_ids, user_group_count, content)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        d["user_id"],
                        d["firstname"],
                        d["lastname"],
                        d["email"],
                        d["authentication_id"],
                        d["user_group_ids"],
                        d["user_group_count"],
                        serialize_content(d["content"], config),
                    )
                    for d in processed_users
                ],
            )
            conn.commit()

        # --- Export user_groups ---
        user_groups_count = len(processed_user_groups)
        if export_dir is not None:
            user_groups_count = write_to_csv(
                processed_user_groups,
                export_dir,
                "gooddata_user_groups.csv",
                fieldnames=user_groups_columns.keys(),
                exclude_fields={"content"},
            )

        if processed_user_groups:
            execute_with_retry(
                conn.cursor(),
                """
                INSERT INTO user_groups
                (user_group_id, name, parent_ids, parent_count, content)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        d["user_group_id"],
                        d["name"],
                        d["parent_ids"],
                        d["parent_count"],
                        serialize_content(d["content"], config),
                    )
                    for d in processed_user_groups
                ],
            )
            conn.commit()

        # --- Export user_group_members ---
        membership_count = len(processed_memberships)
        if export_dir is not None:
            membership_count = write_to_csv(
                processed_memberships,
                export_dir,
                "gooddata_user_group_members.csv",
                fieldnames=["user_id", "user_group_id"],
            )

        if processed_memberships:
            execute_with_retry(
                conn.cursor(),
                """
                INSERT INTO user_group_members
                (user_id, user_group_id)
                VALUES (?, ?)
                """,
                [(d["user_id"], d["user_group_id"]) for d in processed_memberships],
            )
            conn.commit()

    # Log exports
    if export_dir is not None:
        log_export("users", users_count, Path(export_dir) / "gooddata_users.csv")
        log_export(
            "user groups",
            user_groups_count,
            Path(export_dir) / "gooddata_user_groups.csv",
        )
        log_export(
            "user-group memberships",
            membership_count,
            Path(export_dir) / "gooddata_user_group_members.csv",
        )
    else:
        logger.info("Exported %d users to %s", users_count, db_name)
        logger.info("Exported %d user groups to %s", user_groups_count, db_name)
        logger.info(
            "Exported %d user-group memberships to %s", membership_count, db_name
        )


def export_dashboards_permissions(
    all_workspace_data, export_dir, _config, db_name
) -> None:
    """Export dashboard permissions (assignee relationships) to database and CSV.

    Permissions are extracted from the analytics model (layout API).

    Args:
        all_workspace_data: List of workspace data dictionaries
        export_dir: Directory for CSV output (None to skip CSV)
        _config: ExportConfig instance (unused, kept for interface consistency with other export_* functions)
        db_name: Path to SQLite database

    NOTE: analytics_model is only fetched for the parent workspace to minimize API calls.
    Child workspace permissions are not included. To add them, fetch analytics_model
    in fetch_data_from_workspace() and add 'analytics_model' to CHILD_WORKSPACE_DATA_TYPES.
    """
    all_permissions = []

    # Process permissions from parent workspace only (analytics_model not fetched for children)
    for workspace_info in all_workspace_data:
        workspace_id = workspace_info["workspace_id"]
        analytics_model = workspace_info["data"].get("analytics_model")

        if analytics_model is None:
            continue

        permissions = process_dashboards_permissions_from_analytics_model(
            analytics_model, workspace_id
        )
        all_permissions.extend(permissions)

    # Define column schema
    # Note: PRIMARY KEY includes permission_name to allow multiple permission levels
    # (e.g., VIEW, EDIT) for the same assignee on the same dashboard
    columns = {
        "dashboard_id": "TEXT",
        "workspace_id": "TEXT",
        "assignee_id": "TEXT",
        "assignee_type": "TEXT",
        "permission_name": "TEXT",
        "PRIMARY KEY": "(dashboard_id, workspace_id, assignee_id, assignee_type, permission_name)",
    }

    with database_connection(db_name) as conn:
        setup_table(conn, "dashboards_permissions", columns)

        if not all_permissions:
            logger.info("No dashboard permissions found - table created but empty")
            return

        # Export to CSV if requested
        count = len(all_permissions)
        if export_dir is not None:
            count = write_to_csv(
                all_permissions,
                export_dir,
                "gooddata_dashboards_permissions.csv",
                fieldnames=columns.keys(),
            )

        execute_with_retry(
            conn.cursor(),
            """
            INSERT INTO dashboards_permissions
            (dashboard_id, workspace_id, assignee_id, assignee_type, permission_name)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    d["dashboard_id"],
                    d["workspace_id"],
                    d["assignee_id"],
                    d["assignee_type"],
                    d["permission_name"],
                )
                for d in all_permissions
            ],
        )
        conn.commit()

    if export_dir is not None:
        log_export(
            "dashboard permissions",
            count,
            Path(export_dir) / "gooddata_dashboards_permissions.csv",
        )
    else:
        logger.info("Exported %d dashboard permissions to %s", count, db_name)


def export_plugins(all_workspace_data, export_dir, config, db_name) -> None:
    """Export dashboard plugins to both CSV and SQLite"""
    all_processed_data = []

    # Process plugins from all workspaces
    for workspace_info in all_workspace_data:
        workspace_id = workspace_info["workspace_id"]
        raw_data = workspace_info["data"].get("plugins")

        if raw_data is None:
            if config.DEBUG_WORKSPACE_PROCESSING:
                logger.debug("No plugins data for workspace %s", workspace_id)
            continue

        processed_data = process_plugins(raw_data, workspace_id)
        all_processed_data.extend(processed_data)

    # Define column schema
    columns = {
        "plugin_id": "TEXT",
        "workspace_id": "TEXT",
        "title": "TEXT",
        "description": "TEXT",
        "url": "TEXT",
        "version": "TEXT",
        "created_at": "DATE",
        "origin_type": "TEXT",
        "content": "JSON",
        "PRIMARY KEY": "(plugin_id, workspace_id)",
    }

    with database_connection(db_name) as conn:
        setup_table(conn, "plugins", columns)

        if not all_processed_data:
            logger.info("No plugins found - table created but empty")
            return

        # Export to CSV if requested
        count = len(all_processed_data)
        if export_dir is not None:
            count = write_to_csv(
                all_processed_data,
                export_dir,
                "gooddata_plugins.csv",
                fieldnames=columns.keys(),
                exclude_fields={"content"},
            )

        execute_with_retry(
            conn.cursor(),
            """
            INSERT INTO plugins
            (plugin_id, workspace_id, title, description, url, version, created_at, origin_type, content)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    d["plugin_id"],
                    d["workspace_id"],
                    d["title"],
                    d["description"],
                    d["url"],
                    d["version"],
                    d["created_at"],
                    d["origin_type"],
                    serialize_content(d["content"], config),
                )
                for d in all_processed_data
            ],
        )
        conn.commit()

    if export_dir is not None:
        log_export("plugins", count, Path(export_dir) / "gooddata_plugins.csv")
    else:
        logger.info("Exported %d plugins to %s", count, db_name)
