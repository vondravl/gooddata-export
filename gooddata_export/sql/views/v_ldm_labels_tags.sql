-- Create view showing LDM labels with tags unnested (one row per tag)
-- This makes it easier to query and filter by individual tags
--
-- The tags column in the ldm_labels table is stored as a string like "['tag1', 'tag2']"
-- This view converts it to proper JSON and unnests each tag into its own row
--
-- Labels are display forms for attributes (e.g., different text representations)

DROP VIEW IF EXISTS v_ldm_labels_tags;

CREATE VIEW v_ldm_labels_tags AS
SELECT
    lbl.dataset_id,
    lbl.attribute_id,
    lbl.id AS label_id,
    lbl.title AS label_title,
    lbl.is_default,
    lbl.tags AS original_tags,
    tags_json.value AS tag
FROM ldm_labels lbl
CROSS JOIN (
    SELECT
        id,
        ('{' || REPLACE(
            REPLACE(tags, '''', '"'),  -- Replace single quotes with double quotes
            '[', '"values": [')        -- Make it a valid JSON object
            || '}') as tags_adj
    FROM ldm_labels
    WHERE tags != '[]' AND tags != ''
) lbl_adj ON lbl.id = lbl_adj.id
LEFT JOIN JSON_EACH(lbl_adj.tags_adj, '$.values') AS tags_json
WHERE tags_json.value IS NOT NULL
ORDER BY lbl.dataset_id, lbl.attribute_id, lbl.id, tags_json.value;
