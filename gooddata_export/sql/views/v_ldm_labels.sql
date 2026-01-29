-- View showing all LDM labels with dataset_id derived from parent attribute
-- This provides a convenient way to query labels with their dataset context
-- without needing to manually JOIN to ldm_columns
--
-- dataset_id is derived via ldm_columns (normalized design - not stored in ldm_labels)

DROP VIEW IF EXISTS v_ldm_labels;

CREATE VIEW IF NOT EXISTS v_ldm_labels AS
SELECT
    col.dataset_id,
    col.dataset_name,
    lbl.attribute_id,
    col.title AS attribute_title,
    lbl.id AS label_id,
    lbl.title AS label_title,
    lbl.description,
    lbl.source_column,
    lbl.source_column_data_type,
    lbl.value_type,
    lbl.tags,
    lbl.is_default,
    lbl.workspace_id
FROM ldm_labels lbl
JOIN ldm_columns col ON lbl.attribute_id = col.id
ORDER BY col.dataset_id, lbl.attribute_id, lbl.id;
