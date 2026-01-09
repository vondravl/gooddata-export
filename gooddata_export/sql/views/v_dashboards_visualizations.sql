-- Create view showing dashboard-visualization relationships
-- Simple join of dashboards and visualizations through the junction table
--
-- Useful for seeing all visualizations on a specific dashboard
-- Includes tab_id for tabbed dashboards (NULL for legacy non-tabbed dashboards)

DROP VIEW IF EXISTS v_dashboards_visualizations;

CREATE VIEW v_dashboards_visualizations AS
SELECT
    d.dashboard_id,
    d.title AS dashboard_title,
    v.visualization_id,
    v.title AS visualization_title,
    v.tags,
    dv.tab_id,
    dv.from_rich_text,
    dv.workspace_id
FROM dashboards d
JOIN dashboards_visualizations dv ON d.dashboard_id = dv.dashboard_id AND d.workspace_id = dv.workspace_id
JOIN visualizations v ON dv.visualization_id = v.visualization_id AND dv.workspace_id = v.workspace_id
ORDER BY d.dashboard_id, COALESCE(dv.tab_id, ''), v.visualization_id;
