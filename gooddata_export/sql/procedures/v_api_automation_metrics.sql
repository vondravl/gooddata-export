-- Procedure: v_api_automation_metrics
-- Description: Parameterized procedure for generating API metric curl commands
-- Purpose: SQLite doesn't support stored procedures, so we simulate them with parameterized views
--
-- Parameters (supplied via post_export_config.yaml):
--   {base_url} - API base URL from config ({{BASE_URL}})
--   {workspace_id} - Workspace ID from config ({{WORKSPACE_ID}})
--   {bearer_token} - Bearer token as shell variable ($${TOKEN_GOODDATA_DEV})
--
-- Returns: Table with curl commands and Excel formulas for API operations
--
-- Columns returned:
--   metric_id, title, tags, format, description, maql - Metric attributes
--   curl_post - POST command to create metric
--   formula_post - Excel formula for POST substitution
--   curl_put - PUT command to update metric
--   formula_put - Excel formula for PUT substitution
--   curl_delete - DELETE command to remove metric

DROP VIEW IF EXISTS v_api_automation_metrics;

CREATE VIEW v_api_automation_metrics AS
SELECT 
    metric_id,
    -- Use json_quote to escape special characters, then remove surrounding quotes
    substr(json_quote(title), 2, length(json_quote(title)) - 2) as title,
    COALESCE(tags, '[]') as tags,
    substr(json_quote(COALESCE(format, '#,##0')), 2, length(json_quote(COALESCE(format, '#,##0'))) - 2) as format,
    substr(json_quote(COALESCE(description, '')), 2, length(json_quote(COALESCE(description, ''))) - 2) as description,
    substr(json_quote(maql), 2, length(json_quote(maql)) - 2) as maql,
    
    -- POST curl command (for creating new metrics)
    -- Note: metric_id left as placeholder {metric_id} so you can change it to create new metrics
    'curl -X POST "{base_url}/api/v1/entities/workspaces/{workspace_id}/metrics" -H "Authorization: Bearer {bearer_token}" -H "Content-Type: application/vnd.gooddata.api+json" -d ''{"data":{"id":"{metric_id}","type":"metric","attributes":{"title":"{title}","description":"{description}","content":{"format":"{format}","maql":"{maql}"}}}}''' AS curl_post,
    
    -- Excel formula for POST command
    -- Assuming columns are: A=metric_id, B=title, C=tags, D=format, E=description, F=maql, G=curl_post
    -- Note: workspace_id and bearer_token already substituted; metric_id kept as placeholder for flexibility
    '=SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(G2,"{metric_id}",A2),"{title}",B2),"{tags}",C2),"{format}",D2),"{description}",E2),"{maql}",F2)' as formula_post,
    
    -- PUT curl command (for updating existing metrics)
    'curl -X PUT "{base_url}/api/v1/entities/workspaces/{workspace_id}/metrics/' || metric_id || '" -H "Authorization: Bearer {bearer_token}" -H "Content-Type: application/vnd.gooddata.api+json" -d ''{"data":{"id":"' || metric_id || '","type":"metric","attributes":{"title":"{title}","description":"{description}","content":{"format":"{format}","maql":"{maql}"}}}}''' AS curl_put,
    
    -- Excel formula for PUT command
    -- Assuming columns are: B=title, C=tags, D=format, E=description, F=maql, I=curl_put
    -- Note: workspace_id, bearer_token, and metric_id are already substituted
    '=SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(I2,"{title}",B2),"{tags}",C2),"{format}",D2),"{description}",E2),"{maql}",F2)' as formula_put,
    
    -- DELETE curl command (for deleting metrics)
    'curl -X DELETE "{base_url}/api/v1/entities/workspaces/{workspace_id}/metrics/' || metric_id || '" -H "Authorization: Bearer {bearer_token}"' AS curl_delete

FROM metrics
WHERE metric_id IS NOT NULL
ORDER BY metric_id;


