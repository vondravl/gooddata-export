-- Create view showing dashboard-plugin relationships
-- Shows which dashboards use which plugins (LEFT JOIN to include dashboards without plugins)
--
-- Useful for seeing plugin usage across dashboards

DROP VIEW IF EXISTS v_dashboards_plugins;

CREATE VIEW v_dashboards_plugins AS
SELECT
    d.dashboard_id,
    d.title AS dashboard_title,
    p.plugin_id,
    p.title AS plugin_title,
    p.url AS plugin_url,
    d.workspace_id
FROM dashboards d
LEFT JOIN dashboards_plugins dp ON d.dashboard_id = dp.dashboard_id AND d.workspace_id = dp.workspace_id
LEFT JOIN plugins p ON dp.plugin_id = p.plugin_id AND dp.workspace_id = p.workspace_id
ORDER BY d.dashboard_id;
