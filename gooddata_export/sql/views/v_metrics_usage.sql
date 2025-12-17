-- Create view showing where metrics are used across dashboards and visualizations
-- This allows easy lookup of metric usage by metric_id
-- Combines both direct dashboard usage (from rich text) and indirect usage (through visualizations)
-- Note: dashboards_metrics table is always created (may be empty if rich text extraction disabled)

DROP VIEW IF EXISTS v_metrics_usage;

CREATE VIEW v_metrics_usage AS
-- Direct dashboard usage (from rich text)
SELECT 
    m.metric_id,
    m.title AS metric_title,
    'dashboard_direct' AS usage_type,
    d.dashboard_id,
    d.title AS dashboard_title,
    NULL AS visualization_id,
    NULL AS visualization_title,
    1 AS from_rich_text
FROM metrics m
JOIN dashboards_metrics dm ON m.metric_id = dm.metric_id AND m.workspace_id = dm.workspace_id
JOIN dashboards d ON dm.dashboard_id = d.dashboard_id AND dm.workspace_id = d.workspace_id

UNION ALL

-- Indirect usage through visualizations
SELECT 
    m.metric_id,
    m.title AS metric_title,
    'visualization' AS usage_type,
    d.dashboard_id,
    d.title AS dashboard_title,
    v.visualization_id,
    v.title AS visualization_title,
    dv.from_rich_text
FROM metrics m
JOIN visualizations_metrics vm ON m.metric_id = vm.metric_id AND m.workspace_id = vm.workspace_id
JOIN visualizations v ON vm.visualization_id = v.visualization_id AND vm.workspace_id = v.workspace_id
JOIN dashboards_visualizations dv ON v.visualization_id = dv.visualization_id AND v.workspace_id = dv.workspace_id
JOIN dashboards d ON dv.dashboard_id = d.dashboard_id AND dv.workspace_id = d.workspace_id

ORDER BY metric_id, usage_type, dashboard_id;

