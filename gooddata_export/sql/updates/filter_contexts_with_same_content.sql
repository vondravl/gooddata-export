DROP TABLE IF EXISTS filter_context_duplicities;

-- Create temporary table with filter context duplicities
-- Identifies filter contexts with the same set of filters
CREATE TEMPORARY TABLE filter_context_duplicities AS
WITH filter_signatures AS (
    -- Create a signature for each filter based on its type and key attributes
    SELECT 
        filter_context_id,
        workspace_id,
        filter_index,
        CASE 
            -- Date filters: include granularity, from, to, and type
            WHEN filter_type = 'dateFilter' THEN 
                'DATE:' || COALESCE(date_granularity, '') || ':' || 
                COALESCE(CAST(date_from AS TEXT), '') || ':' || 
                COALESCE(CAST(date_to AS TEXT), '') || ':' || 
                COALESCE(date_type, '')
            -- Attribute filters: include display_form_id, selection_mode, and negative_selection
            WHEN filter_type = 'attributeFilter' THEN 
                'ATTR:' || COALESCE(display_form_id, '') || ':' || 
                COALESCE(selection_mode, '') || ':' || 
                COALESCE(CAST(negative_selection AS TEXT), '')
            ELSE 'UNKNOWN'
        END AS filter_signature
    FROM filter_context_fields
),
same_fields AS (
    -- Concatenate all filter signatures for each filter context (sorted for consistency)
    SELECT 
        fc.filter_context_id,
        fc.workspace_id,
        fc.title,
        fc.description,
        GROUP_CONCAT(fs.filter_signature, '||' ORDER BY fs.filter_signature) AS filter_fields,
        COUNT(*) OVER (
            PARTITION BY GROUP_CONCAT(fs.filter_signature, '||' ORDER BY fs.filter_signature)
        ) AS same_fields_count
    FROM filter_contexts fc
    LEFT JOIN filter_signatures fs 
        ON fc.filter_context_id = fs.filter_context_id 
        AND fc.workspace_id = fs.workspace_id
    GROUP BY fc.filter_context_id, fc.workspace_id, fc.title, fc.description
)
SELECT 
    filter_context_id,
    workspace_id,
    title,
    description,
    filter_fields,
    same_fields_count,
    -- Assign a unique ID to each group of filter contexts with the same fields
    DENSE_RANK() OVER (ORDER BY filter_fields) AS same_fields_id
FROM same_fields
WHERE same_fields_count > 1  -- Only include filter contexts that have duplicates
;

-- Update filter_fields column with the concatenated filter signature
UPDATE filter_contexts
SET filter_fields = (
    SELECT sf.filter_fields
    FROM (
        SELECT
            fc.filter_context_id,
            fc.workspace_id,
            GROUP_CONCAT(
                CASE
                    WHEN fcf.filter_type = 'dateFilter' THEN
                        'DATE:' || COALESCE(fcf.date_granularity, '') || ':' ||
                        COALESCE(CAST(fcf.date_from AS TEXT), '') || ':' ||
                        COALESCE(CAST(fcf.date_to AS TEXT), '') || ':' ||
                        COALESCE(fcf.date_type, '')
                    WHEN fcf.filter_type = 'attributeFilter' THEN
                        'ATTR:' || COALESCE(fcf.display_form_id, '') || ':' ||
                        COALESCE(fcf.selection_mode, '') || ':' ||
                        COALESCE(CAST(fcf.negative_selection AS TEXT), '')
                    ELSE 'UNKNOWN'
                END,
                '||' ORDER BY
                    CASE
                        WHEN fcf.filter_type = 'dateFilter' THEN
                            'DATE:' || COALESCE(fcf.date_granularity, '') || ':' ||
                            COALESCE(CAST(fcf.date_from AS TEXT), '') || ':' ||
                            COALESCE(CAST(fcf.date_to AS TEXT), '') || ':' ||
                            COALESCE(fcf.date_type, '')
                        WHEN fcf.filter_type = 'attributeFilter' THEN
                            'ATTR:' || COALESCE(fcf.display_form_id, '') || ':' ||
                            COALESCE(fcf.selection_mode, '') || ':' ||
                            COALESCE(CAST(fcf.negative_selection AS TEXT), '')
                        ELSE 'UNKNOWN'
                    END
            ) AS filter_fields
        FROM filter_contexts fc
        LEFT JOIN filter_context_fields fcf
            ON fc.filter_context_id = fcf.filter_context_id
            AND fc.workspace_id = fcf.workspace_id
        GROUP BY fc.filter_context_id, fc.workspace_id
    ) sf
    WHERE sf.filter_context_id = filter_contexts.filter_context_id
      AND sf.workspace_id = filter_contexts.workspace_id
)
WHERE 1=1 {parent_workspace_filter};

-- Update same_fields_id for filter contexts with duplicates
UPDATE filter_contexts
SET same_fields_id = (
    SELECT same_fields_id
    FROM filter_context_duplicities
    WHERE filter_context_duplicities.filter_context_id = filter_contexts.filter_context_id
      AND filter_context_duplicities.workspace_id = filter_contexts.workspace_id
)
WHERE 1=1 {parent_workspace_filter};

-- Update same_filter_contexts_count for filter contexts with duplicates
UPDATE filter_contexts
SET same_filter_contexts_count = (
    SELECT same_fields_count
    FROM filter_context_duplicities
    WHERE filter_context_duplicities.filter_context_id = filter_contexts.filter_context_id
      AND filter_context_duplicities.workspace_id = filter_contexts.workspace_id
)
WHERE 1=1 {parent_workspace_filter};

-- Drop the temporary table
DROP TABLE filter_context_duplicities;
