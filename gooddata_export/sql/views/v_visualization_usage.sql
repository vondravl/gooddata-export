-- Create view showing where visualizations are used across dashboards
-- This allows easy lookup of visualization usage by visualization_id
--
-- The from_rich_text column (from dashboard_visualizations table) indicates whether 
-- the visualization appears in a rich text widget (1) or standard dashboard widget (0).

CREATE VIEW IF NOT EXISTS v_visualization_usage AS
SELECT 
    DISTINCT 
    v.visualization_id,
    v.title AS visualization_title,
    v.url_link,
    d.dashboard_id,
    d.title AS dashboard_title,
    dv.from_rich_text
FROM visualizations v
JOIN dashboard_visualizations dv ON v.visualization_id = dv.visualization_id AND v.workspace_id = dv.workspace_id
JOIN dashboards d ON dv.dashboard_id = d.dashboard_id AND dv.workspace_id = d.workspace_id
ORDER BY v.visualization_id, d.dashboard_id;

