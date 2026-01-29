-- Create view showing dashboard-visualization relationships
-- Simple join of dashboards and visualizations through the junction table
--
-- Useful for seeing all visualizations on a specific dashboard
-- Includes tab_id for tabbed dashboards (NULL for legacy non-tabbed dashboards)
-- widget_title/widget_description: overridden values on dashboard (NULL if not set)
-- has_title_override/has_description_override: 1 if widget has different value than original
-- has_ignored_filters: 1 if widget ignores any dashboard filters (see v_dashboards_widget_filters for details)
-- widget_local_identifier: the widget's own localIdentifier
-- widget_type: 'insight', 'visualizationSwitcher', or 'richText'
-- switcher_local_identifier: parent switcher's localIdentifier (NULL if not in a switcher, use for grouping)

DROP VIEW IF EXISTS v_dashboards_visualizations;

CREATE VIEW v_dashboards_visualizations AS
SELECT
    d.dashboard_id,
    d.title AS dashboard_title,
    v.visualization_id,
    v.title AS visualization_title,
    dv.widget_title,
    CASE WHEN dv.widget_title IS NOT NULL AND dv.widget_title != v.title THEN 1 ELSE 0 END AS has_title_override,
    v.description AS visualization_description,
    dv.widget_description,
    CASE WHEN dv.widget_description IS NOT NULL AND dv.widget_description != COALESCE(v.description, '') THEN 1 ELSE 0 END AS has_description_override,
    v.tags,
    dv.tab_id,
    dv.from_rich_text,
    dv.widget_local_identifier,
    dv.widget_type,
    dv.switcher_local_identifier,
    CASE WHEN EXISTS (
        SELECT 1 FROM dashboards_widget_filters wf
        WHERE wf.dashboard_id = dv.dashboard_id
          AND wf.visualization_id = dv.visualization_id
          AND wf.workspace_id = dv.workspace_id
          AND wf.filter_type = 'ignoreDashboardFilters'
    ) THEN 1 ELSE 0 END AS has_ignored_filters,
    dv.workspace_id
FROM dashboards d
JOIN dashboards_visualizations dv ON d.dashboard_id = dv.dashboard_id AND d.workspace_id = dv.workspace_id
JOIN visualizations v ON dv.visualization_id = v.visualization_id AND dv.workspace_id = v.workspace_id
ORDER BY d.dashboard_id, COALESCE(dv.tab_id, ''), COALESCE(dv.switcher_local_identifier, ''), COALESCE(dv.widget_local_identifier, ''), v.visualization_id;
