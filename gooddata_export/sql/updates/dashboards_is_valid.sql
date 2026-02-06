-- Compute is_valid for dashboards where it's NULL (local mode)
-- A dashboard is invalid if it references an object that doesn't exist
--
-- Checks:
-- 1. All visualizations in dashboards_visualizations exist in visualizations table
-- 2. All labels (object_type='label') in dashboards_references exist in ldm_labels OR ldm_columns (type='attribute')
-- 3. All filter contexts (object_type='filterContext') in dashboards_references exist in filter_contexts table
-- 4. All plugins in dashboards_plugins exist in plugins table
--
-- Label validation checks both tables because displayAsLabel IDs can be:
--   - Label IDs like "region.name" (in ldm_labels)
--   - Attribute IDs like "date.month" where label shares the attribute ID (in ldm_columns)
--
-- Dataset references (dateFilterConfig) are not validated because the date dataset ID
-- in a dashboard's dateFilterConfig refers to a dateInstance (e.g., "date"), which is
-- stored in ldm_datasets. While the ID exists there, skipping validation avoids
-- false negatives if the dateInstance is missing from the layout (e.g., partial exports).
--
-- Dashboards with is_valid already set (API mode) are unchanged

UPDATE dashboards
SET is_valid = CASE
    WHEN EXISTS (
        -- Check for missing visualization references
        SELECT 1
        FROM dashboards_visualizations dv
        LEFT JOIN visualizations v
            ON dv.visualization_id = v.visualization_id
            AND dv.workspace_id = v.workspace_id
        WHERE dv.dashboard_id = dashboards.dashboard_id
          AND dv.workspace_id = dashboards.workspace_id
          AND v.visualization_id IS NULL
    ) THEN 0  -- Invalid: references missing visualization
    WHEN EXISTS (
        -- Check for missing label references (attributeFilterConfigs)
        -- Valid if found in ldm_labels OR ldm_columns (type='attribute')
        SELECT 1
        FROM dashboards_references dr
        LEFT JOIN ldm_labels ll
            ON dr.referenced_id = ll.id
        LEFT JOIN ldm_columns lc
            ON dr.referenced_id = lc.id
            AND lc.type = 'attribute'
        WHERE dr.dashboard_id = dashboards.dashboard_id
          AND dr.workspace_id = dashboards.workspace_id
          AND dr.object_type = 'label'
          AND ll.id IS NULL
          AND lc.id IS NULL  -- Invalid only if not in EITHER table
    ) THEN 0  -- Invalid: references missing label
    WHEN EXISTS (
        -- Check for missing filter context references
        SELECT 1
        FROM dashboards_references dr
        LEFT JOIN filter_contexts fc
            ON dr.referenced_id = fc.filter_context_id
            AND dr.workspace_id = fc.workspace_id
        WHERE dr.dashboard_id = dashboards.dashboard_id
          AND dr.workspace_id = dashboards.workspace_id
          AND dr.object_type = 'filterContext'
          AND fc.filter_context_id IS NULL
    ) THEN 0  -- Invalid: references missing filter context
    WHEN EXISTS (
        -- Check for missing plugin references
        SELECT 1
        FROM dashboards_plugins dp
        LEFT JOIN plugins p
            ON dp.plugin_id = p.plugin_id
            AND dp.workspace_id = p.workspace_id
        WHERE dp.dashboard_id = dashboards.dashboard_id
          AND dp.workspace_id = dashboards.workspace_id
          AND p.plugin_id IS NULL
    ) THEN 0  -- Invalid: references missing plugin
    ELSE 1    -- Valid: all references exist (or no references)
END
WHERE is_valid IS NULL {parent_workspace_filter};
