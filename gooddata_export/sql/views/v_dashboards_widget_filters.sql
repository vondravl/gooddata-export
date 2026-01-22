-- Create view showing widget-level filter configurations
-- Shows which widgets ignore dashboard filters or have date dataset overrides
--
-- Joins with dashboards and visualizations to provide readable titles
-- Uses LEFT JOIN to include widgets even if visualization is NULL

DROP VIEW IF EXISTS v_dashboards_widget_filters;

CREATE VIEW v_dashboards_widget_filters AS
SELECT
    wf.dashboard_id,
    d.title AS dashboard_title,
    wf.visualization_id,
    v.title AS visualization_title,
    dv.widget_title,
    wf.tab_id,
    wf.widget_local_identifier,
    wf.filter_type,
    wf.reference_type,
    wf.reference_id,
    wf.reference_object_type,
    wf.workspace_id
FROM dashboards_widget_filters wf
LEFT JOIN dashboards d ON wf.dashboard_id = d.dashboard_id AND wf.workspace_id = d.workspace_id
LEFT JOIN dashboards_visualizations dv ON wf.dashboard_id = dv.dashboard_id
    AND wf.visualization_id = dv.visualization_id
    AND wf.tab_id = dv.tab_id
    AND wf.workspace_id = dv.workspace_id
LEFT JOIN visualizations v ON wf.visualization_id = v.visualization_id AND wf.workspace_id = v.workspace_id
ORDER BY wf.dashboard_id, wf.widget_local_identifier, wf.filter_type;
