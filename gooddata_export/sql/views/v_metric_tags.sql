-- Create view showing metrics with tags unnested (one row per tag)
-- This makes it easier to query and filter by individual tags
-- 
-- The tags column in the metrics table is stored as a string like "['tag1', 'tag2']"
-- This view converts it to proper JSON and unnests each tag into its own row

CREATE VIEW IF NOT EXISTS v_metric_tags AS
SELECT 
    m.metric_id,
    m.workspace_id,
    m.title,
    m.maql,
    m.tags AS original_tags,
    tags_json.value AS tag
FROM metrics m
CROSS JOIN (
    SELECT 
        metric_id,
        workspace_id,
        ('{' || REPLACE(
            REPLACE(tags, '''', '"'),  -- Replace single quotes with double quotes
            '[', '"values": [')        -- Make it a valid JSON object
            || '}') as tags_adj
    FROM metrics
) m_adj ON m.metric_id = m_adj.metric_id AND m.workspace_id = m_adj.workspace_id
LEFT JOIN JSON_EACH(m_adj.tags_adj, '$.values') AS tags_json
WHERE tags_json.value IS NOT NULL
ORDER BY m.metric_id, tags_json.value;

