-- View showing filter element validation dependencies
-- Joins filter_context_validate_by with filter_context_fields to provide
-- human-readable filter names and titles instead of raw filter_index values.
-- For filterElementsBy entries, also resolves the referenced parent filter name.

DROP VIEW IF EXISTS v_filter_contexts_validate_by;

CREATE VIEW v_filter_contexts_validate_by AS
SELECT
    vb.filter_context_id,
    vb.workspace_id,
    vb.filter_index,
    vb.source,
    vb.referenced_id,
    vb.referenced_type,
    vb.over_attributes,
    -- Filter that has the validation dependency
    fcf.local_identifier AS filter_local_identifier,
    fcf.display_form_id AS filter_display_form_id,
    fcf.title AS filter_title,
    -- For filterElementsBy: resolve the parent filter's title
    parent.title AS parent_filter_title,
    parent.display_form_id AS parent_filter_display_form_id,
    -- Filter context metadata
    fc.title AS filter_context_title
FROM filter_context_validate_by vb
JOIN filter_context_fields fcf
    ON vb.filter_context_id = fcf.filter_context_id
    AND vb.workspace_id = fcf.workspace_id
    AND vb.filter_index = fcf.filter_index
LEFT JOIN filter_context_fields parent
    ON vb.source = 'filterElementsBy'
    AND vb.referenced_id = parent.local_identifier
    AND vb.filter_context_id = parent.filter_context_id
    AND vb.workspace_id = parent.workspace_id
LEFT JOIN filter_contexts fc
    ON vb.filter_context_id = fc.filter_context_id
    AND vb.workspace_id = fc.workspace_id
ORDER BY vb.workspace_id, vb.filter_context_id, vb.filter_index;
