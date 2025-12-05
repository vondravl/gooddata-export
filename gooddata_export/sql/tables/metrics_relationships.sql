-- Create metrics_relationships table to store direct metric-to-metric references
-- This table is populated by Python (regex extraction from MAQL) after creation
--
-- Pattern matched in MAQL: {metric/metric_id}
-- Self-references are excluded

DROP TABLE IF EXISTS metrics_relationships;

CREATE TABLE metrics_relationships (
    source_metric_id TEXT,
    source_workspace_id TEXT,
    referenced_metric_id TEXT,
    PRIMARY KEY (source_metric_id, source_workspace_id, referenced_metric_id)
);

-- Index for fast lookups when checking if a metric is referenced by others
-- Used by: metrics_usage_check (is_used_maql), v_metrics_relationships views
CREATE INDEX idx_metrics_relationships_referenced
ON metrics_relationships(referenced_metric_id, source_workspace_id);
