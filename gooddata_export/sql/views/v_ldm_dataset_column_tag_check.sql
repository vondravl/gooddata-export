-- View to check if all columns of a dataset have at least one tag matching the dataset's tags
-- This helps identify columns that might have outdated tags (e.g., former dataset names)
-- or columns missing proper tag categorization
--
-- NOTE: Reference columns are excluded from this check as they typically don't need tags
--
-- The view shows:
-- - Datasets with their tags
-- - Count of columns in the dataset (including references)
-- - Count of reference columns (excluded from tag check)
-- - Count of non-reference columns
-- - Count of columns that have at least one tag matching the dataset
-- - Count of columns with mismatched tags
-- - List of columns with mismatched tags (if any)

CREATE VIEW IF NOT EXISTS v_ldm_dataset_column_tag_check AS
WITH 
-- Extract individual tags from datasets
dataset_tags AS (
    SELECT 
        ds.id AS dataset_id,
        ds.title AS dataset_title,
        ds.tags AS dataset_tags,
        tags_json.value AS dataset_tag
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
),
-- Extract individual tags from columns (excluding references - they don't need tags)
column_tags AS (
    SELECT 
        col.dataset_id,
        col.id AS column_id,
        col.title AS column_title,
        col.tags AS column_tags,
        tags_json.value AS column_tag
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
            AND type != 'reference'  -- References don't need tags
    ) col_adj ON col.id = col_adj.id
    LEFT JOIN JSON_EACH(col_adj.tags_adj, '$.values') AS tags_json
    WHERE tags_json.value IS NOT NULL
),
-- Check if each column has at least one matching tag with its dataset
-- Note: References are completely excluded from this check as they don't need tags
-- Tags can match either the dataset's tags array OR the dataset title
column_match_status AS (
    SELECT 
        col.dataset_id,
        col.id AS column_id,
        col.title AS column_title,
        col.tags AS column_tags,
        col.type AS column_type,
        CASE 
            WHEN col.tags = '[]' OR col.tags = '' THEN 0  -- No tags on column
            -- Check if column tag matches dataset tags array
            WHEN EXISTS (
                SELECT 1 
                FROM column_tags ct
                JOIN dataset_tags dt ON ct.dataset_id = dt.dataset_id 
                    AND ct.column_tag = dt.dataset_tag
                WHERE ct.column_id = col.id
            ) THEN 1  -- Has matching tag from dataset tags array
            -- Check if column tag matches dataset title
            WHEN EXISTS (
                SELECT 1
                FROM column_tags ct
                JOIN ldm_datasets ds ON ct.dataset_id = ds.id
                    AND ct.column_tag = ds.title
                WHERE ct.column_id = col.id
            ) THEN 1  -- Has matching tag from dataset title
            ELSE 0  -- Has tags but none match
        END AS has_matching_tag
    FROM ldm_columns col
    WHERE col.type != 'reference'  -- Exclude references entirely
),
-- Aggregate by dataset
dataset_summary AS (
    SELECT 
        ds.id AS dataset_id,
        ds.title AS dataset_title,
        ds.tags AS dataset_tags,
        COUNT(DISTINCT col.id) AS total_columns,
        COUNT(DISTINCT CASE WHEN col.type = 'reference' THEN col.id END) AS reference_columns,
        SUM(CASE WHEN cms.has_matching_tag = 1 THEN 1 ELSE 0 END) AS columns_with_matching_tags,
        SUM(CASE WHEN cms.has_matching_tag = 0 THEN 1 ELSE 0 END) AS columns_with_mismatched_tags,
        GROUP_CONCAT(
            CASE WHEN cms.has_matching_tag = 0 
            THEN cms.column_title || ' (type: ' || cms.column_type || ', tags: ' || cms.column_tags || ')' 
            ELSE NULL END, 
            '; '
        ) AS mismatched_columns
    FROM ldm_datasets ds
    LEFT JOIN ldm_columns col ON ds.id = col.dataset_id
    LEFT JOIN column_match_status cms ON col.id = cms.column_id 
        AND col.dataset_id = cms.dataset_id
    GROUP BY ds.id, ds.title, ds.tags
)
SELECT 
    dataset_id,
    dataset_title,
    dataset_tags,
    total_columns,
    reference_columns,
    (total_columns - reference_columns) AS non_reference_columns,
    columns_with_matching_tags,
    columns_with_mismatched_tags,
    ROUND(CAST(columns_with_matching_tags AS FLOAT) / NULLIF(total_columns - reference_columns, 0) * 100, 2) AS match_percentage,
    CASE 
        WHEN total_columns = 0 THEN 'N/A - No columns'
        WHEN total_columns = reference_columns THEN 'N/A - Only references (no tags needed)'
        WHEN dataset_tags = '[]' OR dataset_tags = '' THEN 'WARNING - Dataset has no tags'
        WHEN columns_with_matching_tags = (total_columns - reference_columns) THEN 'OK - All columns match'
        WHEN columns_with_matching_tags = 0 THEN 'CRITICAL - No columns match'
        ELSE 'ATTENTION - Partial match'
    END AS status,
    mismatched_columns
FROM dataset_summary
ORDER BY 
    CASE 
        WHEN total_columns = reference_columns THEN 5  -- Only references, move to end
        WHEN columns_with_matching_tags = 0 AND total_columns > 0 AND dataset_tags != '[]' AND dataset_tags != '' THEN 1  -- Critical first
        WHEN columns_with_mismatched_tags > 0 THEN 2  -- Partial matches second
        WHEN dataset_tags = '[]' OR dataset_tags = '' THEN 3  -- No dataset tags third
        ELSE 4  -- OK cases last
    END,
    dataset_title;

