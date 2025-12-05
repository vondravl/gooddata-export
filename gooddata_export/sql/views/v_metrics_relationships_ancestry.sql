-- Create view showing full metric ancestry with titles and tags
-- Joins metrics_ancestry table with metrics for human-readable output
--
-- Use this view to:
--   - Find all ancestors of a metric (direct and transitive)
--   - See which tags ancestors have (for inheritance analysis)
--   - Understand the depth of metric dependencies

DROP VIEW IF EXISTS v_metrics_relationships_ancestry;

CREATE VIEW v_metrics_relationships_ancestry AS
SELECT
    ma.metric_id,
    ma.workspace_id,
    m.title AS metric_title,
    m.tags AS metric_tags,
    ma.ancestor_metric_id,
    am.title AS ancestor_title,
    am.tags AS ancestor_tags,
    ma.min_depth,
    ma.max_depth
FROM metrics_ancestry ma
LEFT JOIN metrics m
    ON ma.metric_id = m.metric_id
    AND ma.workspace_id = m.workspace_id
LEFT JOIN metrics am
    ON ma.ancestor_metric_id = am.metric_id
    AND ma.workspace_id = am.workspace_id
ORDER BY ma.workspace_id, ma.metric_id, ma.min_depth;
