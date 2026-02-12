-- Procedure: v_api_automation_metrics
-- Description: Parameterized procedure for generating API metric curl commands
-- Purpose: SQLite doesn't support stored procedures, so we simulate them with parameterized views
--
-- Parameters (supplied via post_export_config.yaml):
--   {bearer_token} - Bearer token as shell variable ($${TOKEN_GOODDATA_DEV})
--
-- Runtime values (read from dictionary_metadata table via CTE):
--   base_url - API base URL stored during export
--   workspace_id - Workspace ID stored during export
--
-- Returns: Table with curl commands and Excel formulas for API operations
--
-- Columns returned:
--   metric_id, title, tags, format, description, maql, is_hidden - Metric attributes
--   curl_post - POST command to create metric
--   formula_post - Excel formula for POST substitution
--   curl_put - PUT command to update metric
--   formula_put - Excel formula for PUT substitution
--   curl_delete - DELETE command to remove metric
--
-- Note: is_hidden is a boolean field:
--   In Excel/SQLite: Use 0 for false (visible), 1 for true (hidden)

DROP VIEW IF EXISTS v_api_automation_metrics;

CREATE VIEW v_api_automation_metrics AS
WITH config AS (
    SELECT
        COALESCE(MAX(CASE WHEN key = 'base_url' THEN value END), '{{base_url}}') AS base_url,
        COALESCE(MAX(CASE WHEN key = 'workspace_id' THEN value END), '{{workspace_id}}') AS workspace_id
    FROM dictionary_metadata
)
SELECT
    m.metric_id,
    -- Use json_quote to escape special characters, then remove surrounding quotes
    substr(json_quote(m.title), 2, length(json_quote(m.title)) - 2) as title,
    COALESCE(m.tags, '[]') as tags,
    substr(json_quote(COALESCE(m.format, '#,##0')), 2, length(json_quote(COALESCE(m.format, '#,##0'))) - 2) as format,
    substr(json_quote(COALESCE(m.description, '')), 2, length(json_quote(COALESCE(m.description, ''))) - 2) as description,
    substr(json_quote(m.maql), 2, length(json_quote(m.maql)) - 2) as maql,
    CASE WHEN m.is_hidden = 1 THEN 'true' ELSE 'false' END as is_hidden,

    -- POST curl command (for creating new metrics)
    -- Note: metric_id left as placeholder {metric_id} so you can change it to create new metrics
    'curl -X POST "' || c.base_url || '/api/v1/entities/workspaces/' || c.workspace_id || '/metrics" -H "Authorization: Bearer {bearer_token}" -H "Content-Type: application/vnd.gooddata.api+json" -d ''{"data":{"id":"{metric_id}","type":"metric","attributes":{"title":"{title}","description":"{description}","isHidden":{is_hidden},"content":{"format":"{format}","maql":"{maql}"}}}}''' AS curl_post,

    -- Excel formula for POST command
    -- Assuming columns are: A=metric_id, B=title, C=tags, D=format, E=description, F=maql, G=is_hidden, H=curl_post
    -- Note: workspace_id and bearer_token already substituted; metric_id kept as placeholder for flexibility
    '=SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(H2,"{metric_id}",A2),"{title}",B2),"{tags}",C2),"{format}",D2),"{description}",E2),"{maql}",F2),"{is_hidden}",G2)' as formula_post,

    -- PUT curl command (for updating existing metrics)
    'curl -X PUT "' || c.base_url || '/api/v1/entities/workspaces/' || c.workspace_id || '/metrics/' || m.metric_id || '" -H "Authorization: Bearer {bearer_token}" -H "Content-Type: application/vnd.gooddata.api+json" -d ''{"data":{"id":"' || m.metric_id || '","type":"metric","attributes":{"title":"{title}","description":"{description}","isHidden":{is_hidden},"content":{"format":"{format}","maql":"{maql}"}}}}''' AS curl_put,

    -- Excel formula for PUT command
    -- Assuming columns are: B=title, C=tags, D=format, E=description, F=maql, G=is_hidden, J=curl_put
    -- Note: workspace_id, bearer_token, and metric_id are already substituted
    '=SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(J2,"{title}",B2),"{tags}",C2),"{format}",D2),"{description}",E2),"{maql}",F2),"{is_hidden}",G2)' as formula_put,

    -- DELETE curl command (for deleting metrics)
    'curl -X DELETE "' || c.base_url || '/api/v1/entities/workspaces/' || c.workspace_id || '/metrics/' || m.metric_id || '" -H "Authorization: Bearer {bearer_token}"' AS curl_delete

FROM metrics m
CROSS JOIN config c
WHERE m.metric_id IS NOT NULL
ORDER BY m.metric_id;
