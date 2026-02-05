-- Create view showing where metrics are used across dashboards and visualizations
-- This allows easy lookup of metric usage by metric_id
-- Combines both direct dashboard usage (from rich text) and indirect usage (through visualizations)
-- Uses LEFT JOINs to include unused metrics (visualization/dashboard columns will be NULL)
-- Note: dashboards_metrics table is always created (may be empty if rich text extraction disabled)
--
-- To find unused metrics: WHERE is_used_insight = 0 (or is_used_maql = 0 for MAQL references)

DROP VIEW IF EXISTS v_metrics_usage;

CREATE VIEW v_metrics_usage AS
-- Direct dashboard usage (from rich text)
-- Uses INNER JOINs: if a metric is in dashboards_metrics, it is inherently used (scraped from dashboard)
SELECT
    m.metric_id,
    m.title AS metric_title,
    m.is_used_insight,
    m.is_used_maql,
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
    m.is_used_insight,
    m.is_used_maql,
    'visualization' AS usage_type,
    d.dashboard_id,
    d.title AS dashboard_title,
    v.visualization_id,
    v.title AS visualization_title,
    dv.from_rich_text
FROM metrics m
LEFT JOIN visualizations_references vr ON m.metric_id = vr.referenced_id AND m.workspace_id = vr.workspace_id AND vr.object_type = 'metric'
LEFT JOIN visualizations v ON vr.visualization_id = v.visualization_id AND vr.workspace_id = v.workspace_id
LEFT JOIN dashboards_visualizations dv ON v.visualization_id = dv.visualization_id AND v.workspace_id = dv.workspace_id
LEFT JOIN dashboards d ON dv.dashboard_id = d.dashboard_id AND dv.workspace_id = d.workspace_id

ORDER BY 1, 3, 4;

