-- Mark filter contexts as used or unused based on dashboard references
-- A filter context is considered "used" if it's referenced by at least one dashboard

UPDATE filter_contexts
SET is_used = CASE
    WHEN EXISTS (
        SELECT 1 
        FROM dashboards d 
        WHERE d.filter_context_id = filter_contexts.filter_context_id
          AND d.workspace_id = filter_contexts.workspace_id
    ) THEN 1
    ELSE 0
END;

