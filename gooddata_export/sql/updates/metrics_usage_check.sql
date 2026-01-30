-- Check if metrics are used in dashboards/visualizations or other metrics' MAQL formulas
-- Two separate columns for granular tracking:
-- 1. is_used_insight: Used in dashboards (rich text) or visualizations
-- 2. is_used_maql: Referenced in another metric's MAQL formula (as {metric/metric_id})

-- Update is_used_insight: Check if metric appears in dashboards or visualizations
UPDATE metrics
SET is_used_insight = CASE
    WHEN EXISTS (
        SELECT 1
        FROM dashboards_metrics dm
        WHERE dm.metric_id = metrics.metric_id
          AND dm.workspace_id = metrics.workspace_id
    ) OR EXISTS (
        SELECT 1
        FROM visualizations_metrics vm
        WHERE vm.metric_id = metrics.metric_id
          AND vm.workspace_id = metrics.workspace_id
    ) THEN 1
    ELSE 0
END
WHERE 1=1 {parent_workspace_filter};

-- Update is_used_maql: Check if metric is referenced in other metrics' MAQL formulas
-- Uses metrics_relationships table (populated by Python regex extraction)
UPDATE metrics
SET is_used_maql = CASE
    WHEN EXISTS (
        SELECT 1
        FROM metrics_relationships mr
        WHERE mr.referenced_metric_id = metrics.metric_id
          AND mr.source_workspace_id = metrics.workspace_id
    ) THEN 1
    ELSE 0
END
WHERE 1=1 {parent_workspace_filter};
