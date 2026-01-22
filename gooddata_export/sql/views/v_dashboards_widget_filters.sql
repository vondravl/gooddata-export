-- Create view showing widget-level filter configurations
-- Shows which widgets ignore dashboard filters or have date dataset overrides
--
-- Joins with dashboards and visualizations to provide readable titles
-- Uses LEFT JOIN to include widgets even if visualization is NULL

DROP VIEW IF EXISTS v_dashboards_widget_filters;

CREATE VIEW v_dashboards_widget_filters AS
SELECT
    d.dashboard_id,
    d.title AS dashboard_title,
    wf.widget_local_identifier,
    dv.visualization_id,
    v.title AS visualization_title,
    wf.tab_id,
    wf.filter_type,
    wf.reference_type,
    wf.reference_id,
    wf.reference_object_type,
    wf.workspace_id
FROM dashboards d 
LEFT JOIN dashboards_visualizations dv ON dv.dashboard_id = d.dashboard_id 
LEFT JOIN visualizations v ON dv.visualization_id = v.visualization_id AND dv.workspace_id = v.workspace_id
LEFT JOIN dashboards_widget_filters wf ON wf.dashboard_id = dv.dashboard_id AND wf.visualization_id = dv.visualization_id and wf.workspace_id = dv.workspace_id
where d.dashboard_id = 'portfolio_health_insights_6ce994e2' and filter_type = 'ignoreDashboardFilters'
ORDER BY d.dashboard_id, wf.widget_local_identifier, wf.filter_type;
