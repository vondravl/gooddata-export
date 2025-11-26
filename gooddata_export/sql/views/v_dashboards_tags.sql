-- Create view showing dashboards with tags unnested (one row per tag)
-- This makes it easier to query and filter by individual tags
-- 
-- The tags column in the dashboards table is stored as a string like "['tag1', 'tag2']"
-- This view converts it to proper JSON and unnests each tag into its own row

DROP VIEW IF EXISTS v_dashboards_tags;

CREATE VIEW v_dashboards_tags AS
SELECT 
    d.dashboard_id,
    d.workspace_id,
    d.title,
    d.tags AS original_tags,
    tags_json.value AS tag
FROM dashboards d
CROSS JOIN (
    SELECT 
        dashboard_id,
        workspace_id,
        ('{' || REPLACE(
            REPLACE(tags, '''', '"'),  -- Replace single quotes with double quotes
            '[', '"values": [')        -- Make it a valid JSON object
            || '}') as tags_adj
    FROM dashboards
) d_adj ON d.dashboard_id = d_adj.dashboard_id AND d.workspace_id = d_adj.workspace_id
LEFT JOIN JSON_EACH(d_adj.tags_adj, '$.values') AS tags_json
WHERE tags_json.value IS NOT NULL
ORDER BY d.dashboard_id, tags_json.value;

