-- Mark filter contexts as used or unused based on dashboard references
-- A filter context is considered "used" if it's referenced by at least one
-- dashboard via dashboards_references (object_type='filterContext'). That table
-- holds every filterContextRef for both layouts: legacy dashboards as a top-level
-- ref (tab_id=NULL) and tabbed dashboards as one ref per tab.

UPDATE filter_contexts
SET is_used = CASE
    WHEN EXISTS (
        SELECT 1
        FROM dashboards_references dr
        WHERE dr.referenced_id = filter_contexts.filter_context_id
          AND dr.workspace_id = filter_contexts.workspace_id
          AND dr.object_type = 'filterContext'
    ) THEN 1
    ELSE 0
END
WHERE 1=1 {parent_workspace_filter};
