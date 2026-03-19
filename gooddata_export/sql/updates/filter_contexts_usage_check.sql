-- Mark filter contexts as used or unused based on dashboard references
-- A filter context is considered "used" if it's referenced by at least one dashboard,
-- either via the top-level filterContextRef (dashboards.filter_context_id) or via
-- tab-level filterContextRef (dashboards_references with object_type='filterContext')

UPDATE filter_contexts
SET is_used = CASE
    WHEN EXISTS (
        SELECT 1
        FROM dashboards d
        WHERE d.filter_context_id = filter_contexts.filter_context_id
          AND d.workspace_id = filter_contexts.workspace_id
    ) OR EXISTS (
        SELECT 1
        FROM dashboards_references dr
        WHERE dr.referenced_id = filter_contexts.filter_context_id
          AND dr.workspace_id = filter_contexts.workspace_id
          AND dr.object_type = 'filterContext'
    ) THEN 1
    ELSE 0
END
WHERE 1=1 {parent_workspace_filter};
