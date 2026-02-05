-- Compute is_valid for metrics where it's NULL (local mode)
-- A metric is invalid if it references an object that doesn't exist
--
-- Checks via metrics_references table:
-- 1. All metric references (reference_type='metric') exist in metrics table
-- 2. All attribute references (reference_type='attribute') exist in ldm_columns
-- 3. All label references (reference_type='label') exist in ldm_columns (type='attribute')
--    Note: {label/id} in MAQL references the attribute's default label, which shares the attribute ID
-- 4. All fact references (reference_type='fact') exist in ldm_columns
--
-- Metrics with is_valid already set (API mode) are unchanged

UPDATE metrics
SET is_valid = CASE
    WHEN EXISTS (
        -- Check for missing metric references
        SELECT 1
        FROM metrics_references mr
        LEFT JOIN metrics rm
            ON mr.referenced_id = rm.metric_id
            AND mr.source_workspace_id = rm.workspace_id
        WHERE mr.source_metric_id = metrics.metric_id
          AND mr.source_workspace_id = metrics.workspace_id
          AND mr.reference_type = 'metric'
          AND rm.metric_id IS NULL
    ) THEN 0  -- Invalid: references missing metric
    WHEN EXISTS (
        -- Check for missing LDM column references (attributes, facts, and labels)
        -- Note: {label/id} references the attribute's default label, which shares the attribute ID
        SELECT 1
        FROM metrics_references mr
        LEFT JOIN ldm_columns lc
            ON mr.referenced_id = lc.id
            AND (
                (mr.reference_type = 'attribute' AND lc.type = 'attribute') OR
                (mr.reference_type = 'label' AND lc.type = 'attribute') OR
                (mr.reference_type = 'fact' AND lc.type = 'fact')
            )
        WHERE mr.source_metric_id = metrics.metric_id
          AND mr.source_workspace_id = metrics.workspace_id
          AND mr.reference_type IN ('attribute', 'fact', 'label')
          AND lc.id IS NULL
    ) THEN 0  -- Invalid: references missing attribute, fact, or label
    ELSE 1    -- Valid: all references exist (or no references)
END
WHERE is_valid IS NULL {parent_workspace_filter};
