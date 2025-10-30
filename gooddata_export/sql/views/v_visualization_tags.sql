-- Create view showing visualizations with tags unnested (one row per tag)
-- This makes it easier to query and filter by individual tags
-- 
-- The tags column in the visualizations table is stored as a string like "['tag1', 'tag2']"
-- This view converts it to proper JSON and unnests each tag into its own row

CREATE VIEW IF NOT EXISTS v_visualization_tags AS
SELECT 
    v.visualization_id,
    v.workspace_id,
    v.title,
    v.visualization_url,
    v.tags AS original_tags,
    tags_json.value AS tag
FROM visualizations v
CROSS JOIN (
    SELECT 
        visualization_id,
        workspace_id,
        ('{' || REPLACE(
            REPLACE(tags, '''', '"'),  -- Replace single quotes with double quotes
            '[', '"values": [')        -- Make it a valid JSON object
            || '}') as tags_adj
    FROM visualizations
) v_adj ON v.visualization_id = v_adj.visualization_id AND v.workspace_id = v_adj.workspace_id
LEFT JOIN JSON_EACH(v_adj.tags_adj, '$.values') AS tags_json
WHERE tags_json.value IS NOT NULL
ORDER BY v.visualization_id, tags_json.value;

