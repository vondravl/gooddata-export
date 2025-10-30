-- Create view showing metric dependencies (which metrics are used in other metrics' MAQL formulas)
-- This helps identify metrics that are building blocks for other metrics
-- 
-- A metric can be a "component" metric used in other metrics' calculations
-- This view shows parent-child relationships between metrics via MAQL references

CREATE VIEW IF NOT EXISTS metric_dependencies AS
SELECT 
    m_child.metric_id AS used_metric_id,
    m_child.title AS used_metric_title,
    m_parent.metric_id AS parent_metric_id,
    m_parent.title AS parent_metric_title,
    m_parent.maql AS parent_maql
FROM metrics m_child
JOIN metrics m_parent ON m_parent.maql LIKE '%{metric/' || m_child.metric_id || '}%' 
    AND m_parent.workspace_id = m_child.workspace_id
WHERE m_child.metric_id != m_parent.metric_id
ORDER BY m_child.metric_id, m_parent.metric_id;


