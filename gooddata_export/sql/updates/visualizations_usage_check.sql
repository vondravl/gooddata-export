-- Check if visualizations are used in dashboards
-- A visualization is considered "used" if it appears in dashboards_visualizations table

UPDATE visualizations
SET is_used = CASE
    WHEN EXISTS (
        SELECT 1
        FROM dashboards_visualizations dv
        WHERE dv.visualization_id = visualizations.visualization_id
          AND dv.workspace_id = visualizations.workspace_id
    ) THEN 1
    ELSE 0
END
WHERE 1=1 {parent_workspace_filter};
