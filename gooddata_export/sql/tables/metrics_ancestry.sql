-- Create metrics_ancestry table with full transitive ancestry using recursive CTE
-- This traces the full lineage: if A uses B, and B uses C, then C is an ancestor of A
--
-- Depends on: metrics_references table (must be populated first)
-- Only considers metric-to-metric references (reference_type = 'metric')
--
-- Columns:
--   metric_id: The metric that has ancestors
--   workspace_id: Workspace context
--   ancestor_metric_id: A metric that this metric depends on (directly or transitively)
--   min_depth: Shortest path to this ancestor (1 = direct reference)
--   max_depth: Longest path to this ancestor (for metrics reachable via multiple paths)

DROP TABLE IF EXISTS metrics_ancestry;

-- Create table structure with FK constraints
CREATE TABLE metrics_ancestry (
    metric_id TEXT,
    workspace_id TEXT,
    ancestor_metric_id TEXT,
    min_depth INTEGER,
    max_depth INTEGER,
    PRIMARY KEY (metric_id, workspace_id, ancestor_metric_id),
    FOREIGN KEY (metric_id, workspace_id) REFERENCES metrics(metric_id, workspace_id),
    FOREIGN KEY (ancestor_metric_id, workspace_id) REFERENCES metrics(metric_id, workspace_id)
);

-- Populate with recursive CTE
-- Only metric-to-metric references are used for ancestry (facts/attributes don't have transitive deps)
INSERT INTO metrics_ancestry (metric_id, workspace_id, ancestor_metric_id, min_depth, max_depth)
WITH RECURSIVE ancestors AS (
    -- Base case: direct metric references (depth 1)
    SELECT
        source_metric_id AS metric_id,
        source_workspace_id AS workspace_id,
        referenced_id AS ancestor_metric_id,
        1 AS depth,
        '|' || source_metric_id || '|' || referenced_id || '|' AS path
    FROM metrics_references
    WHERE reference_type = 'metric'

    UNION ALL

    -- Recursive case: ancestors of ancestors
    SELECT
        a.metric_id,
        a.workspace_id,
        mr.referenced_id AS ancestor_metric_id,
        a.depth + 1 AS depth,
        a.path || mr.referenced_id || '|' AS path
    FROM ancestors a
    JOIN metrics_references mr
        ON a.ancestor_metric_id = mr.source_metric_id
        AND a.workspace_id = mr.source_workspace_id
        AND mr.reference_type = 'metric'
    WHERE a.depth < 10  -- Prevent infinite recursion
      AND a.path NOT LIKE '%|' || mr.referenced_id || '|%'  -- Prevent cycles (delimited match)
)
SELECT DISTINCT
    metric_id,
    workspace_id,
    ancestor_metric_id,
    MIN(depth) AS min_depth,
    MAX(depth) AS max_depth
FROM ancestors
GROUP BY metric_id, workspace_id, ancestor_metric_id;
