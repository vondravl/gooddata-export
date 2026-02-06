-- Create view showing root metrics (metrics that don't depend on other metrics)
-- These are the "base" metrics that other metrics build upon
--
-- Root metrics are potential tag sources - if they have a tag,
-- all derived metrics might need to inherit it
-- Filters to reference_type='metric' from the consolidated metrics_references table

DROP VIEW IF EXISTS v_metrics_relationships_root;

CREATE VIEW v_metrics_relationships_root AS
SELECT DISTINCT
    m.metric_id,
    m.workspace_id,
    m.title,
    m.tags,
    m.maql,
    m.is_valid,
    m.is_hidden
FROM metrics m
WHERE NOT EXISTS (
    SELECT 1 FROM metrics_references mr
    WHERE mr.source_metric_id = m.metric_id
      AND mr.source_workspace_id = m.workspace_id
      AND mr.reference_type = 'metric'
)
ORDER BY m.workspace_id, m.title;
