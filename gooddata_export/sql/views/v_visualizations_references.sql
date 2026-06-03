-- Convenience view over visualizations_references with visualization title
--
-- Joins with visualizations to include the visualization title
-- so you don't have to do the join manually every time.

DROP VIEW IF EXISTS v_visualizations_references;

CREATE VIEW v_visualizations_references AS
SELECT
    vr.visualization_id,
    v.title AS visualization_title,
    vr.referenced_id,
    vr.object_type,
    vr.source,
    vr.label,
    vr.local_identifier,
    vr.workspace_id
FROM visualizations_references vr
JOIN visualizations v
    ON vr.visualization_id = v.visualization_id
    AND vr.workspace_id = v.workspace_id
ORDER BY vr.workspace_id, v.title, vr.object_type, vr.source;
