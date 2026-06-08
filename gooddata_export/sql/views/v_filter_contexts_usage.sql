-- View showing where filter contexts are used
-- Joins filter contexts with dashboards via dashboards_references, which holds
-- every filterContextRef for both layouts:
--   - legacy (non-tabbed) dashboards: top-level ref with tab_id = NULL
--   - tabbed dashboards: one ref per tab with tab_id = the tab localIdentifier

DROP VIEW IF EXISTS v_filter_contexts_usage;

CREATE VIEW v_filter_contexts_usage AS
WITH all_references AS (
    -- Distinct dashboard-level usage (collapses per-tab refs to the dashboard)
    SELECT DISTINCT
        referenced_id AS filter_context_id,
        workspace_id,
        dashboard_id
    FROM dashboards_references
    WHERE object_type = 'filterContext'
)
SELECT
    fc.filter_context_id,
    fc.workspace_id,
    fc.title as filter_context_title,
    fc.description as filter_context_description,
    fc.origin_type,
    fc.filter_fields,
    fc.same_fields_id,
    fc.same_filter_contexts_count,
    d.dashboard_id,
    d.title as dashboard_title,
    d.dashboard_url,
    d.is_valid as dashboard_is_valid,
    d.is_hidden as dashboard_is_hidden,
    fc.is_used,
    -- Count statistics
    COUNT(d.dashboard_id) OVER (PARTITION BY fc.filter_context_id, fc.workspace_id) as dashboard_count
FROM filter_contexts fc
LEFT JOIN all_references ar
    ON fc.filter_context_id = ar.filter_context_id
    AND fc.workspace_id = ar.workspace_id
LEFT JOIN dashboards d
    ON ar.dashboard_id = d.dashboard_id
    AND ar.workspace_id = d.workspace_id
ORDER BY fc.workspace_id, fc.filter_context_id;
