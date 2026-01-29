DROP TABLE IF EXISTS duplicities;

CREATE TEMPORARY TABLE duplicities AS
select *
  ,dense_rank() over (order by case 
                                    when dupl_maql > 1 then maql
                                    when dupl_title > 1 then title
                                    when dupl_metric > 1 then metric_id_adj
                                end) rn
from (
    select *
    ,count(*) over (partition by maql order by maql) dupl_maql
    ,count(*) over (partition by title order by title) dupl_title
    ,count(*) over (partition by metric_id_adj order by metric_id_adj) dupl_metric
    from (
        select *
        ,    CASE 
                -- If last two characters are underscore followed by number, remove both
                WHEN 
                    SUBSTR(metric_id, -2, 1) = '_' AND 
                    (SUBSTR(metric_id, -1) >= '0' AND SUBSTR(metric_id, -1) <= '9')
                THEN SUBSTR(metric_id, 1, LENGTH(metric_id) - 2)
        
                -- Check if last character is a letter (using ASCII values)
                WHEN 
                    (SUBSTR(metric_id, -1) >= 'a' AND SUBSTR(metric_id, -1) <= 'z') OR 
                    (SUBSTR(metric_id, -1) >= 'A' AND SUBSTR(metric_id, -1) <= 'Z')
                THEN metric_id
                
                -- For any other non-letter character, remove the last character
                ELSE SUBSTR(metric_id, 1, LENGTH(metric_id) - 1)
            END AS metric_id_adj
        from metrics
        --where metric_id not like 'bmk_%'
    )
)
where dupl_maql > 1 or dupl_title > 1 or dupl_metric > 1
;

-- Update similar_metric_id
UPDATE metrics
SET similar_metric_id = (
    SELECT rn
    FROM duplicities
    WHERE duplicities.metric_id = metrics.metric_id
      AND duplicities.workspace_id = metrics.workspace_id
)
WHERE 1=1 {parent_workspace_filter};

-- Drop the temporary table
DROP TABLE duplicities;