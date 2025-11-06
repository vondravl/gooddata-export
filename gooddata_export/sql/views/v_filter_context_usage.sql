-- View showing where filter contexts are used
-- Joins filter contexts with dashboards to show usage relationships

DROP VIEW IF EXISTS v_filter_context_usage;

CREATE VIEW v_filter_context_usage AS
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
    -- Count statistics
    COUNT(*) OVER (PARTITION BY fc.filter_context_id, fc.workspace_id) as dashboard_count,
    -- Flag for unused filter contexts
    CASE WHEN d.dashboard_id IS NULL THEN 1 ELSE 0 END as is_unused
FROM filter_contexts fc
LEFT JOIN dashboards d 
    ON fc.filter_context_id = d.filter_context_id
    AND fc.workspace_id = d.workspace_id
ORDER BY fc.workspace_id, fc.filter_context_id;

