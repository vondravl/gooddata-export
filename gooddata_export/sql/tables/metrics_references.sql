-- Create metrics_references table to store all metric references from MAQL formulas
-- This table is populated by Python (regex extraction from MAQL) after creation
--
-- Patterns matched in MAQL:
--   {metric/metric_id} - metric references (reference_type='metric')
--   {attr/attribute_id} - attribute references (reference_type='attribute')
--   {label/label_id} - label references (reference_type='label')
--   {fact/fact_id} - fact references (reference_type='fact')
--
-- Validation mapping (is_valid computation):
--   Metric references -> metrics table
--   Attribute references -> ldm_columns table (type='attribute')
--   Fact references -> ldm_columns table (type='fact')
--   Label references -> ldm_labels OR ldm_columns (type='attribute')
--     Note: {label/id} in MAQL can reference either an attribute ID (default label)
--     or a specific label ID, so both tables are checked.

DROP TABLE IF EXISTS metrics_references;

CREATE TABLE metrics_references (
    source_metric_id TEXT,
    source_workspace_id TEXT,
    referenced_id TEXT,
    reference_type TEXT,  -- 'metric', 'attribute', 'label', or 'fact'
    PRIMARY KEY (source_metric_id, source_workspace_id, referenced_id, reference_type),
    FOREIGN KEY (source_metric_id, source_workspace_id) REFERENCES metrics(metric_id, workspace_id)
);

-- Index for fast lookups when checking if an object is referenced
-- Used by: metrics_usage_check (is_used_maql), v_metrics_relationships views
CREATE INDEX idx_metrics_references_referenced
ON metrics_references(referenced_id, reference_type, source_workspace_id);

-- Index for metric-type lookups specifically (used by ancestry CTE)
CREATE INDEX idx_metrics_references_metric_type
ON metrics_references(source_metric_id, source_workspace_id) WHERE reference_type = 'metric';
