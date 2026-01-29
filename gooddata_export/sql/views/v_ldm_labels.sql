-- View showing all LDM labels with attribute context
-- Joins to ldm_columns to get attribute_title and dataset_name

DROP VIEW IF EXISTS v_ldm_labels;

CREATE VIEW v_ldm_labels AS
SELECT
    lbl.dataset_id,
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
    lbl.is_default
FROM ldm_labels lbl
JOIN ldm_columns col ON lbl.dataset_id = col.dataset_id
                    AND lbl.attribute_id = col.id
ORDER BY lbl.dataset_id, lbl.attribute_id, lbl.id;
