-- Create metrics_ancestry table with full transitive ancestry using recursive CTE
-- This traces the full lineage: if A uses B, and B uses C, then C is an ancestor of A
--
-- Depends on: metrics_relationships table (must be populated first)
--
-- Columns:
--   metric_id: The metric that has ancestors
--   workspace_id: Workspace context
--   ancestor_metric_id: A metric that this metric depends on (directly or transitively)
--   min_depth: Shortest path to this ancestor (1 = direct reference)
--   max_depth: Longest path to this ancestor (for metrics reachable via multiple paths)

DROP TABLE IF EXISTS metrics_ancestry;

CREATE TABLE metrics_ancestry AS
WITH RECURSIVE ancestors AS (
    -- Base case: direct references (depth 1)
    SELECT
        source_metric_id AS metric_id,
        source_workspace_id AS workspace_id,
        referenced_metric_id AS ancestor_metric_id,
        1 AS depth,
        '|' || source_metric_id || '|' || referenced_metric_id || '|' AS path
    FROM metrics_relationships

    UNION ALL

    -- Recursive case: ancestors of ancestors
    SELECT
        a.metric_id,
        a.workspace_id,
        mr.referenced_metric_id AS ancestor_metric_id,
        a.depth + 1 AS depth,
        a.path || mr.referenced_metric_id || '|' AS path
    FROM ancestors a
    JOIN metrics_relationships mr
        ON a.ancestor_metric_id = mr.source_metric_id
        AND a.workspace_id = mr.source_workspace_id
    WHERE a.depth < 10  -- Prevent infinite recursion
      AND a.path NOT LIKE '%|' || mr.referenced_metric_id || '|%'  -- Prevent cycles (delimited match)
)
SELECT DISTINCT
    metric_id,
    workspace_id,
    ancestor_metric_id,
    MIN(depth) AS min_depth,
    MAX(depth) AS max_depth
FROM ancestors
GROUP BY metric_id, workspace_id, ancestor_metric_id;
