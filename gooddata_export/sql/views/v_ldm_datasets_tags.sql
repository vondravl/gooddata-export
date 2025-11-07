-- Create view showing LDM datasets with tags unnested (one row per tag)
-- This makes it easier to query and filter by individual tags
-- 
-- The tags column in the ldm_datasets table is stored as a string like "['tag1', 'tag2']"
-- This view converts it to proper JSON and unnests each tag into its own row

CREATE VIEW IF NOT EXISTS v_ldm_datasets_tags AS
SELECT 
    ds.id AS dataset_id,
    ds.title AS dataset_title,
    ds.attributes_count,
    ds.facts_count,
    ds.references_count,
    ds.total_columns,
    ds.tags AS original_tags,
    tags_json.value AS tag
FROM ldm_datasets ds
CROSS JOIN (
    SELECT 
        id,
        ('{' || REPLACE(
            REPLACE(tags, '''', '"'),  -- Replace single quotes with double quotes
            '[', '"values": [')        -- Make it a valid JSON object
            || '}') as tags_adj
    FROM ldm_datasets
    WHERE tags != '[]' AND tags != ''
) ds_adj ON ds.id = ds_adj.id
LEFT JOIN JSON_EACH(ds_adj.tags_adj, '$.values') AS tags_json
WHERE tags_json.value IS NOT NULL
ORDER BY ds.id, tags_json.value;

