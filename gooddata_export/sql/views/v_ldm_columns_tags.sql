-- Create view showing LDM columns with tags unnested (one row per tag)
-- This makes it easier to query and filter by individual tags
--
-- The tags column in the ldm_columns table is stored as a string like "['tag1', 'tag2']"
-- This view converts it to proper JSON and unnests each tag into its own row
--
-- Note: References are included in this view (even though they typically don't have tags)
--       for completeness. You can filter them out with WHERE type != 'reference' if needed.

DROP VIEW IF EXISTS v_ldm_columns_tags;

CREATE VIEW IF NOT EXISTS v_ldm_columns_tags AS
SELECT 
    col.dataset_id,
    col.dataset_name,
    col.id AS column_id,
    col.title AS column_title,
    col.type AS column_type,
    col.data_type,
    col.tags AS original_tags,
    tags_json.value AS tag
FROM ldm_columns col
CROSS JOIN (
    SELECT 
        id,
        ('{' || REPLACE(
            REPLACE(tags, '''', '"'),  -- Replace single quotes with double quotes
            '[', '"values": [')        -- Make it a valid JSON object
            || '}') as tags_adj
    FROM ldm_columns
    WHERE tags != '[]' AND tags != ''
) col_adj ON col.id = col_adj.id
LEFT JOIN JSON_EACH(col_adj.tags_adj, '$.values') AS tags_json
WHERE tags_json.value IS NOT NULL
ORDER BY col.dataset_id, col.id, tags_json.value;

