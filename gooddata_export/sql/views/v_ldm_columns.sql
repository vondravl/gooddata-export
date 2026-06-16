-- View showing ldm_columns with composite reference join keys expanded.
--
-- ldm_columns stores each dataset->dataset reference as a SINGLE row (one
-- logical relationship), keyed by the first source column. A reference whose
-- join is a composite key (e.g. CHILD_ACQUIRER_ICA + CHILD_ISSUER_ICA) keeps
-- its individual source columns in the normalized ldm_reference_sources table.
--
-- This view re-joins the two so a reference row appears once PER source column
-- (duplicated by the number of join columns), while every non-reference column
-- (attribute, fact, date granularity, ...) appears exactly once. Duplicating
-- reference rows is invalid in a base table (it would break the
-- (dataset_id, id) primary key) but is fine in a read-only view.
--
-- For a reference row, source_column is the individual join column and ordinal
-- gives its position in the composite key; for all other rows source_column is
-- the column's own source and ordinal is NULL.

DROP VIEW IF EXISTS v_ldm_columns;

CREATE VIEW v_ldm_columns AS
SELECT
    col.dataset_id,
    col.dataset_name,
    col.title,
    col.description,
    col.id,
    col.tags,
    -- For an expanded reference row use that source column's own data type;
    -- otherwise the column's own type. (ldm_columns.data_type only holds the
    -- first source's type, so a composite reference with mixed types would
    -- show the wrong type for later sources without this COALESCE.)
    COALESCE(src.data_type, col.data_type) AS data_type,
    COALESCE(src.source_column, col.source_column) AS source_column,
    src.ordinal,
    col.type,
    col.grain,
    col.reference_to_id,
    col.reference_to_title
FROM ldm_columns col
LEFT JOIN ldm_reference_sources src
    ON src.dataset_id = col.dataset_id
   AND src.reference_id = col.id
ORDER BY col.dataset_id, col.id, src.ordinal;
