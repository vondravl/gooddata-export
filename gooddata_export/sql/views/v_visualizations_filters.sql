-- Convenience view over visualizations_filters with visualization title
--
-- One row per attribute, ranking, or measure-value filter on a visualization,
-- joined to the visualization title so you don't have to do the join manually
-- every time.
--
-- Attribute filters (positiveAttributeFilter / negativeAttributeFilter):
--   element_count > 0  -> the filter actively constrains results
--   element_count = 0  -> no-op placeholder (e.g. negativeAttributeFilter with
--                         empty notIn — filters nothing)
--   elements           -> JSON array of the selected element values/uris
--   A positive and a negative filter on the same attribute appear as two rows
--   (distinct filter_index).
--
-- Ranking filters (rankingFilter, TOP/BOTTOM N by measure):
--   display_form_id/object_type/element_count/elements are NULL.
--   ranking_operator   -> 'TOP' / 'BOTTOM'
--   ranking_value      -> N
--   ranking_strict     -> strictLimitOfRows (1 = cut off at exactly N rows,
--                         0 = ties at the N-th rank are all included)
--
-- Measure-value filters (measureValueFilter, filter rows by a measure's value):
--   display_form_id/object_type/element_count/elements are NULL.
--   condition_type     -> 'comparison' / 'range'
--   condition_operator -> e.g. 'GREATER_THAN' (comparison) / 'BETWEEN' (range)
--   condition_value    -> JSON, e.g. {"value": 0} or {"from": 10, "to": 20}
--
-- Both ranking and measure-value rows carry measure_local_identifier (the
-- filtered/ranked measure's in-viz handle) and resolve referenced_metric_id
-- via visualizations_references (NULL if the measure didn't resolve to a
-- catalog object — a derived measure, or a dangling reference; see
-- v_visualizations_invalid_filters for the latter).

DROP VIEW IF EXISTS v_visualizations_filters;

CREATE VIEW v_visualizations_filters AS
SELECT
    vf.visualization_id,
    v.title AS visualization_title,
    v.url_link,
    vf.filter_index,
    vf.display_form_id,
    vf.object_type,
    vf.filter_type,
    vf.element_count,
    vf.elements,
    vf.measure_local_identifier,
    vf.ranking_operator,
    vf.ranking_value,
    vf.ranking_strict,
    vf.condition_type,
    vf.condition_operator,
    vf.condition_value,
    CASE WHEN vf.measure_local_identifier IS NOT NULL THEN (
        SELECT vr.referenced_id
        FROM visualizations_references vr
        WHERE vr.visualization_id = vf.visualization_id
            AND vr.workspace_id = vf.workspace_id
            AND vr.source = vf.filter_type
            AND vr.local_identifier = vf.measure_local_identifier
    ) END AS referenced_metric_id,
    vf.workspace_id
FROM visualizations_filters vf
JOIN visualizations v
    ON vf.visualization_id = v.visualization_id
    AND vf.workspace_id = v.workspace_id
ORDER BY vf.workspace_id, v.title, vf.filter_index;
