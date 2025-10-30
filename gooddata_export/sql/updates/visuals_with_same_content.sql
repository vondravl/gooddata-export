DROP TABLE IF EXISTS duplicities;

-- Create temporary table with duplicities
CREATE TEMPORARY TABLE duplicities AS
with same_columns as (
    SELECT visualization_id, title, visualization_url, columns, tags
    , ('{' || REPLACE(
                REPLACE(tags, '''', '"'),  -- Replace single quotes with double quotes
                '[', '"values": [')        -- Make it a valid JSON object
                || '}') as tags_adj        -- Only one closing bracket needed for array
    , count(*) OVER (PARTITION BY columns ORDER BY columns) AS same_columns_count
    FROM (
        SELECT 
        visualization_id, title, visualization_url, tags
        , GROUP_CONCAT(IFNULL(metric_id,attribute_id), ',' ORDER BY IFNULL(metric_id,attribute_id)) AS columns
        FROM (
            SELECT visualization_id, title, visualization_url, tags
            , JSON_EXTRACT(items.value, '$.measure.definition.measureDefinition.item.identifier.id') AS metric_id
            , JSON_EXTRACT(items.value, '$.attribute.displayForm.identifier.id') AS attribute_id
            FROM visualizations
            , JSON_EACH(JSON_EXTRACT(content, '$.attributes.content.buckets')) AS buckets
            , JSON_EACH(JSON_EXTRACT(buckets.value,'$.items')) AS items
        )
        GROUP BY visualization_id, title, visualization_url, tags
    )
)
SELECT *
, dense_rank() OVER (ORDER BY columns) same_columns_id
, count(*) OVER (PARTITION BY visualization_url, tags_sorted, columns ORDER BY columns) AS same_visuals_count_with_tags
, count(*) OVER (PARTITION BY visualization_url, columns ORDER BY columns) AS same_visuals_count
from (
    select visualization_id, title, visualization_url, columns
    , same_columns_count
    , GROUP_CONCAT(tag, ',' ORDER BY tag) AS tags_sorted
    from (
        select a.*
        , tags_.value as tag
        from same_columns a
        LEFT JOIN JSON_EACH(tags_adj, '$.values') as tags_ ON tags_adj IS NOT NULL
    )
    GROUP BY visualization_id
)
WHERE same_columns_count > 1
;

-- Update columns
UPDATE visualizations
SET columns = (
    SELECT columns 
    FROM duplicities 
    WHERE duplicities.visualization_id = visualizations.visualization_id
      AND duplicities.workspace_id = visualizations.workspace_id
);

-- Update same_columns_id
UPDATE visualizations
SET same_columns_id = (
    SELECT same_columns_id 
    FROM duplicities 
    WHERE duplicities.visualization_id = visualizations.visualization_id
      AND duplicities.workspace_id = visualizations.workspace_id
);

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
);

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
);

-- Drop the temporary table
DROP TABLE duplicities;