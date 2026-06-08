-- Compute is_valid for visualizations where it's NULL (local mode)
-- A visualization is invalid if it references an object that doesn't exist
--
-- is_valid reflects validity per mode, without mixing the two:
--   - API mode: is_valid comes straight from GoodData's areRelationsValid and is
--     left untouched here (this UPDATE only touches NULL rows).
--   - Local mode (no live workspace): we compute is_valid ourselves from the
--     local reference tables, so the dangling-sort check is included here too.
--
-- Checks (via visualizations_references table):
-- 1. All metrics (object_type='metric') exist in metrics table
-- 2. All facts (object_type='fact') exist in ldm_columns table
-- 3. All attributes (object_type='attribute') exist in ldm_columns table
-- 4. All labels (object_type='label') exist in ldm_labels OR ldm_columns (type='attribute')
-- 5. No dangling sorts (object_type='sort_invalid'): a sort referencing a
--    localIdentifier absent from the visualization's buckets makes it fail to
--    render. Flagged at export time by process_visualizations_references and
--    also surfaced (in both modes) via v_visualizations_invalid_sorts.
--
-- Label validation checks both tables because displayForm IDs can be:
--   - Label IDs like "region.name" (in ldm_labels)
--   - Attribute IDs like "date.month" where label shares the attribute ID (in ldm_columns)
--   - Date granularities like "process_date.day" (only in ldm_columns, not ldm_labels)
--
-- Derived-measure rows (object_type='derived_*') are inventory-only: they have no
-- catalog object id, so they are intentionally NOT catalog-validated here. Keep the
-- checks below keyed to explicit object_types — a generic "row exists in catalog?"
-- check would wrongly flag every visualization that uses a computed measure.
--
-- Visualizations with is_valid already set (API mode) are unchanged

UPDATE visualizations
SET is_valid = CASE
    WHEN EXISTS (
        -- Check for missing metric references
        SELECT 1
        FROM visualizations_references vr
        LEFT JOIN metrics m
            ON vr.referenced_id = m.metric_id
            AND vr.workspace_id = m.workspace_id
        WHERE vr.visualization_id = visualizations.visualization_id
          AND vr.workspace_id = visualizations.workspace_id
          AND vr.object_type = 'metric'
          AND m.metric_id IS NULL
    ) THEN 0  -- Invalid: references missing metric
    WHEN EXISTS (
        -- Check for missing fact references
        SELECT 1
        FROM visualizations_references vr
        LEFT JOIN ldm_columns lc
            ON vr.referenced_id = lc.id
            AND lc.type = 'fact'
        WHERE vr.visualization_id = visualizations.visualization_id
          AND vr.workspace_id = visualizations.workspace_id
          AND vr.object_type = 'fact'
          AND lc.id IS NULL
    ) THEN 0  -- Invalid: references missing fact
    WHEN EXISTS (
        -- Check for missing attribute references (used in COUNT aggregations)
        SELECT 1
        FROM visualizations_references vr
        LEFT JOIN ldm_columns lc
            ON vr.referenced_id = lc.id
            AND lc.type = 'attribute'
        WHERE vr.visualization_id = visualizations.visualization_id
          AND vr.workspace_id = visualizations.workspace_id
          AND vr.object_type = 'attribute'
          AND lc.id IS NULL
    ) THEN 0  -- Invalid: references missing attribute
    WHEN EXISTS (
        -- Check for missing label references (display forms in rows/columns/filters)
        -- Valid if found in ldm_labels OR ldm_columns (type='attribute')
        SELECT 1
        FROM visualizations_references vr
        LEFT JOIN ldm_labels ll
            ON vr.referenced_id = ll.id
        LEFT JOIN ldm_columns lc
            ON vr.referenced_id = lc.id
            AND lc.type = 'attribute'
        WHERE vr.visualization_id = visualizations.visualization_id
          AND vr.workspace_id = visualizations.workspace_id
          AND vr.object_type = 'label'
          AND ll.id IS NULL
          AND lc.id IS NULL  -- Invalid only if not in EITHER table
    ) THEN 0  -- Invalid: references missing label
    WHEN EXISTS (
        -- Check for dangling sorts: sort references a localIdentifier that is
        -- not present in the visualization's buckets (flagged at export time)
        SELECT 1
        FROM visualizations_references vr
        WHERE vr.visualization_id = visualizations.visualization_id
          AND vr.workspace_id = visualizations.workspace_id
          AND vr.object_type = 'sort_invalid'
    ) THEN 0  -- Invalid: sort references a missing localIdentifier
    ELSE 1    -- Valid: all references exist (or no references)
END
WHERE is_valid IS NULL {parent_workspace_filter};
