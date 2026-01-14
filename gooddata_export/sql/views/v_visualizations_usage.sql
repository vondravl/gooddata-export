-- Create view showing all visualizations and where they are used across dashboards
-- Uses LEFT JOINs to include unused visualizations (dashboard columns will be NULL)
--
-- To find unused visualizations: WHERE is_used = 0

DROP VIEW IF EXISTS v_visualizations_usage;

CREATE VIEW v_visualizations_usage AS
SELECT
    DISTINCT
    v.visualization_id,
    v.title AS visualization_title,
    v.url_link,
    v.is_used,
    d.dashboard_id,
    d.title AS dashboard_title,
    dv.from_rich_text
FROM visualizations v
LEFT JOIN dashboards_visualizations dv ON v.visualization_id = dv.visualization_id AND v.workspace_id = dv.workspace_id
LEFT JOIN dashboards d ON dv.dashboard_id = d.dashboard_id AND dv.workspace_id = d.workspace_id
ORDER BY v.visualization_id, d.dashboard_id;

