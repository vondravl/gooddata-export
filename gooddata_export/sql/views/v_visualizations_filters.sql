-- Convenience view over visualizations_filters with visualization title
--
-- One row per attribute filter on a visualization, joined to the visualization
-- title so you don't have to do the join manually every time.
--
-- element_count > 0  -> the filter actively constrains results
-- element_count = 0  -> no-op placeholder (e.g. negativeAttributeFilter with
--                       empty notIn — filters nothing)
-- elements           -> JSON array of the selected element values/uris
-- A positive and a negative filter on the same attribute appear as two rows
-- (distinct filter_index).

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
    vf.workspace_id
FROM visualizations_filters vf
JOIN visualizations v
    ON vf.visualization_id = v.visualization_id
    AND vf.workspace_id = v.workspace_id
ORDER BY vf.workspace_id, v.title, vf.filter_index;
