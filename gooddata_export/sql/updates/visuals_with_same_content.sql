DROP TABLE IF EXISTS duplicities;

-- Create temporary table with duplicities
-- Uses visualizations_references table instead of parsing JSON
-- This works regardless of whether content field is populated
CREATE TEMPORARY TABLE duplicities AS
WITH visualization_columns AS (
    -- Get all references (metrics, facts, labels) per visualization
    SELECT visualization_id, workspace_id, referenced_id AS col_id
    FROM visualizations_references
),
same_columns AS (
    SELECT visualization_id, workspace_id, title, visualization_url, columns, tags
    , count(*) OVER (PARTITION BY columns ORDER BY columns) AS same_columns_count
    FROM (
        SELECT
        v.visualization_id, v.workspace_id, v.title, v.visualization_url, v.tags
        , GROUP_CONCAT(vc.col_id, ',' ORDER BY vc.col_id) AS columns
        FROM visualizations v
        LEFT JOIN visualization_columns vc
            ON v.visualization_id = vc.visualization_id AND v.workspace_id = vc.workspace_id
        GROUP BY v.visualization_id, v.workspace_id, v.title, v.visualization_url, v.tags
    )
),
with_tags AS (
    SELECT 
        sc.visualization_id, 
        sc.workspace_id, 
        sc.title, 
        sc.visualization_url, 
        sc.columns,
        sc.same_columns_count,
        GROUP_CONCAT(vt.tag, ',' ORDER BY vt.tag) AS tags_sorted
    FROM same_columns sc
    LEFT JOIN v_visualizations_tags vt ON sc.visualization_id = vt.visualization_id 
        AND sc.workspace_id = vt.workspace_id
    WHERE sc.same_columns_count > 1
    GROUP BY sc.visualization_id, sc.workspace_id, sc.title, sc.visualization_url, sc.columns, sc.same_columns_count
)
SELECT *,
    dense_rank() OVER (ORDER BY columns) same_columns_id,
    count(*) OVER (PARTITION BY visualization_url, tags_sorted, columns ORDER BY columns) AS same_visuals_count_with_tags,
    count(*) OVER (PARTITION BY visualization_url, columns ORDER BY columns) AS same_visuals_count
FROM with_tags
;

-- Update columns
UPDATE visualizations
SET columns = (
    SELECT columns
    FROM duplicities
    WHERE duplicities.visualization_id = visualizations.visualization_id
      AND duplicities.workspace_id = visualizations.workspace_id
)
WHERE 1=1 {parent_workspace_filter};

-- Update same_columns_id
UPDATE visualizations
SET same_columns_id = (
    SELECT same_columns_id
    FROM duplicities
    WHERE duplicities.visualization_id = visualizations.visualization_id
      AND duplicities.workspace_id = visualizations.workspace_id
)
WHERE 1=1 {parent_workspace_filter};

-- Update same_visuals_id (simple version - just URL + columns)
UPDATE visualizations
SET same_visuals_id = (
    SELECT same_visuals_id
    FROM (
          SELECT *
          , dense_rank() OVER (ORDER BY visualization_url, columns) same_visuals_id
          FROM duplicities
          WHERE same_visuals_count > 1
         ) d
    WHERE d.visualization_id = visualizations.visualization_id
      AND d.workspace_id = visualizations.workspace_id
)
WHERE 1=1 {parent_workspace_filter};

-- Update same_visuals_id_with_tags (precise version - URL + columns + tags)
UPDATE visualizations
SET same_visuals_id_with_tags = (
    SELECT same_visuals_id_with_tags
    FROM (
          SELECT *
          , dense_rank() OVER (ORDER BY visualization_url, tags_sorted, columns) same_visuals_id_with_tags
          FROM duplicities
          WHERE same_visuals_count_with_tags > 1
         ) d
    WHERE d.visualization_id = visualizations.visualization_id
      AND d.workspace_id = visualizations.workspace_id
)
WHERE 1=1 {parent_workspace_filter};

-- Drop the temporary table
DROP TABLE duplicities;