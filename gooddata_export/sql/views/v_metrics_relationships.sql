-- Create view showing direct metric-to-metric relationships with titles
--
-- Shows which metrics reference other metrics in their MAQL formulas
-- Includes reference_status to identify broken references (MISSING)

DROP VIEW IF EXISTS v_metrics_relationships;

CREATE VIEW v_metrics_relationships AS
SELECT
    mr.source_metric_id,
    mr.source_workspace_id,
    sm.title AS source_metric_title,
    mr.referenced_metric_id,
    rm.title AS referenced_metric_title,
    rm.workspace_id AS referenced_workspace_id,
    CASE
        WHEN rm.metric_id IS NOT NULL THEN 'EXISTS'
        ELSE 'MISSING'
    END AS reference_status
FROM metrics_relationships mr
LEFT JOIN metrics sm
    ON mr.source_metric_id = sm.metric_id
    AND mr.source_workspace_id = sm.workspace_id
LEFT JOIN metrics rm
    ON mr.referenced_metric_id = rm.metric_id
    AND mr.source_workspace_id = rm.workspace_id
ORDER BY mr.source_workspace_id, mr.source_metric_id;
