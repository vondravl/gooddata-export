-- Create view mapping each metric to the LDM datasets it depends on,
-- including transitive dependencies through metric ancestry.
--
-- Resolves dataset_id for each reference via:
--   - facts qualified as {dataset}.{fact}  -> split before the dot
--   - facts as bare ids (e.g. bmk_*)       -> resolve through ldm_columns (type='fact')
--   - attributes                            -> resolve through ldm_columns (type='attribute')
--   - default labels (label_id == attribute_id) -> resolve through ldm_columns (type='attribute')
--   - specific labels                       -> resolve through ldm_labels
--   - datasets ({dataset/...})              -> referenced_id is the dataset_id
--
-- For every metric, considers both its own direct references and the
-- references of all its ancestor metrics (from metrics_ancestry).
--
-- Note: ldm_columns / ldm_labels are shared (parent workspace owns the LDM,
-- child workspaces inherit it), so those joins do not need a workspace
-- predicate. metrics / metrics_references are per-workspace and do.

DROP VIEW IF EXISTS v_metrics_datasets_ancestry;

CREATE VIEW v_metrics_datasets_ancestry AS
WITH refs AS (
    -- Qualified facts: {dataset}.{fact}
    SELECT source_metric_id AS metric_id,
           source_workspace_id AS workspace_id,
           substr(referenced_id, 1, instr(referenced_id, '.') - 1) AS dataset_id,
           reference_type
    FROM metrics_references
    WHERE reference_type = 'fact' AND instr(referenced_id, '.') > 0
    UNION ALL
    -- Bare facts -> ldm_columns
    SELECT mr.source_metric_id, mr.source_workspace_id, c.dataset_id, mr.reference_type
    FROM metrics_references mr
    JOIN ldm_columns c ON c.id = mr.referenced_id AND c.type = 'fact'
    WHERE mr.reference_type = 'fact' AND instr(mr.referenced_id, '.') = 0
    UNION ALL
    -- Attributes -> ldm_columns
    SELECT mr.source_metric_id, mr.source_workspace_id, c.dataset_id, mr.reference_type
    FROM metrics_references mr
    JOIN ldm_columns c ON c.id = mr.referenced_id AND c.type = 'attribute'
    WHERE mr.reference_type = 'attribute'
    UNION ALL
    -- Default labels (label id shares its attribute's id) -> ldm_columns
    SELECT mr.source_metric_id, mr.source_workspace_id, c.dataset_id, mr.reference_type
    FROM metrics_references mr
    JOIN ldm_columns c ON c.id = mr.referenced_id AND c.type = 'attribute'
    WHERE mr.reference_type = 'label'
    UNION ALL
    -- Specific labels -> ldm_labels
    SELECT mr.source_metric_id, mr.source_workspace_id, l.dataset_id, mr.reference_type
    FROM metrics_references mr
    JOIN ldm_labels l ON l.id = mr.referenced_id
    WHERE mr.reference_type = 'label'
    UNION ALL
    -- Direct dataset references -> referenced_id is the dataset_id
    SELECT mr.source_metric_id, mr.source_workspace_id, d.id, mr.reference_type
    FROM metrics_references mr
    JOIN ldm_datasets d ON d.id = mr.referenced_id
    WHERE mr.reference_type = 'dataset'
),
all_metric_pairs AS (
    SELECT metric_id, workspace_id, metric_id AS effective_metric FROM metrics
    UNION ALL
    SELECT metric_id, workspace_id, ancestor_metric_id FROM metrics_ancestry
)
SELECT DISTINCT
    p.metric_id,
    p.workspace_id,
    r.dataset_id,
    r.reference_type
FROM all_metric_pairs p
JOIN refs r
    ON r.metric_id = p.effective_metric
    AND r.workspace_id = p.workspace_id
ORDER BY p.workspace_id, p.metric_id, r.dataset_id;
