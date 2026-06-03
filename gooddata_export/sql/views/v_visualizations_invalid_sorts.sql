-- Visualizations whose sort references a localIdentifier not present in buckets
--
-- A visualization sorts by measure/attribute localIdentifiers. When a sort
-- targets a localIdentifier that no longer exists among the visualization's
-- bucket items (e.g. the measure/attribute was removed but the sort was left
-- behind), the visualization fails to render. process_visualizations_references
-- flags these rows with object_type='sort_invalid'; this view lists the
-- affected visualizations together with the offending localIdentifier.

DROP VIEW IF EXISTS v_visualizations_invalid_sorts;

CREATE VIEW v_visualizations_invalid_sorts AS
SELECT
    vr.visualization_id,
    v.title AS visualization_title,
    vr.local_identifier AS missing_local_identifier,
    v.url_link,
    vr.workspace_id
FROM visualizations_references vr
JOIN visualizations v
    ON vr.visualization_id = v.visualization_id
    AND vr.workspace_id = v.workspace_id
WHERE vr.object_type = 'sort_invalid'
ORDER BY vr.workspace_id, v.title, vr.local_identifier;
