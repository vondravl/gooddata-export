-- Compute is_valid for metrics where it's NULL (local mode)
-- A metric is invalid if it references an object that doesn't exist
--
-- Checks via metrics_references table:
-- 1. All metric references (reference_type='metric') exist in metrics table
-- 2. All attribute references (reference_type='attribute') exist in ldm_columns (type='attribute')
-- 3. All fact references (reference_type='fact') exist in ldm_columns (type='fact')
-- 4. All label references (reference_type='label') exist in ldm_labels OR ldm_columns (type='attribute')
--
-- Label validation checks both tables because {label/id} in MAQL can reference:
--   - An attribute ID (default label shares attribute ID) -> ldm_columns
--   - A specific label ID -> ldm_labels
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
        -- Check for missing attribute references
        SELECT 1
        FROM metrics_references mr
        LEFT JOIN ldm_columns lc
            ON mr.referenced_id = lc.id
            AND lc.type = 'attribute'
        WHERE mr.source_metric_id = metrics.metric_id
          AND mr.source_workspace_id = metrics.workspace_id
          AND mr.reference_type = 'attribute'
          AND lc.id IS NULL
    ) THEN 0  -- Invalid: references missing attribute
    WHEN EXISTS (
        -- Check for missing fact references
        SELECT 1
        FROM metrics_references mr
        LEFT JOIN ldm_columns lc
            ON mr.referenced_id = lc.id
            AND lc.type = 'fact'
        WHERE mr.source_metric_id = metrics.metric_id
          AND mr.source_workspace_id = metrics.workspace_id
          AND mr.reference_type = 'fact'
          AND lc.id IS NULL
    ) THEN 0  -- Invalid: references missing fact
    WHEN EXISTS (
        -- Check for missing label references
        -- Valid if found in ldm_labels OR ldm_columns (type='attribute')
        SELECT 1
        FROM metrics_references mr
        LEFT JOIN ldm_labels ll
            ON mr.referenced_id = ll.id
        LEFT JOIN ldm_columns lc
            ON mr.referenced_id = lc.id
            AND lc.type = 'attribute'
        WHERE mr.source_metric_id = metrics.metric_id
          AND mr.source_workspace_id = metrics.workspace_id
          AND mr.reference_type = 'label'
          AND ll.id IS NULL
          AND lc.id IS NULL  -- Invalid only if not in EITHER table
    ) THEN 0  -- Invalid: references missing label
    ELSE 1    -- Valid: all references exist (or no references)
END
WHERE is_valid IS NULL {parent_workspace_filter};
