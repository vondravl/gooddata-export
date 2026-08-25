-- Visualizations whose rankingFilter/measureValueFilter references a
-- localIdentifier not present in buckets
--
-- rankingFilter and measureValueFilter target a bucket measure by
-- localIdentifier, and may additionally name the dimension they rank/filter
-- OVER. When either localIdentifier no longer exists among the visualization's
-- bucket items (e.g. the measure was removed but the filter was left behind),
-- the visualization fails to render.
-- process_visualizations_references flags these rows with
-- object_type='{filter}_invalid' for a missing measure handle and
-- '{filter}_dimension_invalid' for a missing dimension handle; this view lists
-- the affected visualizations together with the offending localIdentifier and
-- which of the two it is (invalid_target). The distinction matters because the
-- fixes differ: restore/repoint the measure, versus the ranking dimension.
-- Mirrors v_visualizations_invalid_sorts.
--
-- A localIdentifier that resolves to a derived measure (PoP, arithmetic, ...)
-- rather than a catalog object is a valid config, not flagged here.

DROP VIEW IF EXISTS v_visualizations_invalid_filters;

CREATE VIEW v_visualizations_invalid_filters AS
SELECT
    vr.visualization_id,
    v.title AS visualization_title,
    vr.source AS filter_type,
    CASE
        WHEN vr.object_type LIKE '%_dimension_invalid' THEN 'dimension'
        ELSE 'measure'
    END AS invalid_target,
    vr.local_identifier AS missing_local_identifier,
    v.url_link,
    vr.workspace_id
FROM visualizations_references vr
JOIN visualizations v
    ON vr.visualization_id = v.visualization_id
    AND vr.workspace_id = v.workspace_id
WHERE vr.object_type IN (
    'rankingFilter_invalid',
    'measureValueFilter_invalid',
    'rankingFilter_dimension_invalid',
    'measureValueFilter_dimension_invalid'
)
ORDER BY vr.workspace_id, v.title, vr.source, vr.local_identifier;
