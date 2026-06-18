-- Convenience view over visualizations_references with visualization title
--
-- Joins with visualizations to include the visualization title
-- so you don't have to do the join manually every time.
--
-- For filter rows (source='filter') filter_active answers, at a glance,
-- whether the attribute is *actively* filtered:
--   1    -> at least one filter on this attribute selects elements
--           (an active positive/negative filter that constrains the result)
--   0    -> the attribute is filtered, but every filter is an empty placeholder
--           (e.g. a negativeAttributeFilter with empty notIn — filters nothing)
--   NULL -> non-filter rows (the question doesn't apply)
-- A single attribute may carry both a positive and a negative filter; those
-- collapse to one reference row, so filter_active ORs over all of them (any
-- active -> 1). The per-filter detail (direction, element_count, the selected
-- elements) lives in visualizations_filters.

DROP VIEW IF EXISTS v_visualizations_references;

CREATE VIEW v_visualizations_references AS
SELECT
    vr.visualization_id,
    v.title AS visualization_title,
    v.url_link,
    vr.referenced_id,
    vr.object_type,
    vr.source,
    vr.label,
    vr.local_identifier,
    CASE WHEN vr.source = 'filter' THEN (
        SELECT MAX(CASE WHEN vf.element_count > 0 THEN 1 ELSE 0 END)
        FROM visualizations_filters vf
        WHERE vf.visualization_id = vr.visualization_id
            AND vf.workspace_id = vr.workspace_id
            AND vf.display_form_id = vr.referenced_id
    ) END AS filter_active,
    vr.workspace_id
FROM visualizations_references vr
JOIN visualizations v
    ON vr.visualization_id = v.visualization_id
    AND vr.workspace_id = v.workspace_id
ORDER BY vr.workspace_id, v.title, vr.object_type, vr.source;
