-- Procedure: v_api_automation_workspaces
-- Description: Parameterized procedure for generating API workspace curl commands
-- Purpose: SQLite doesn't support stored procedures, so we simulate them with parameterized views
--
-- Parameters (supplied via post_export_config.yaml):
--   {bearer_token} - Bearer token as shell variable ($${TOKEN_GOODDATA_DEV})
--
-- Runtime values (read from dictionary_metadata table via CTE):
--   base_url - API base URL stored during export
--
-- Returns: Table with curl commands and Excel formulas for API operations
--
-- Columns returned:
--   workspace_id, workspace_name - Workspace attributes
--   curl_post - POST command to create workspace
--   formula_post - Excel formula for POST substitution
--   curl_put - PUT command to update workspace
--   formula_put - Excel formula for PUT substitution
--   curl_delete - DELETE command to remove workspace

DROP VIEW IF EXISTS v_api_automation_workspaces;

CREATE VIEW v_api_automation_workspaces AS
WITH config AS (
    SELECT
        COALESCE(MAX(CASE WHEN key = 'base_url' THEN value END), '{{base_url}}') AS base_url
    FROM dictionary_metadata
)
SELECT
    w.workspace_id,
    -- Use json_quote to escape special characters, then remove surrounding quotes
    substr(json_quote(w.workspace_name), 2, length(json_quote(w.workspace_name)) - 2) as workspace_name,

    -- POST curl command (for creating new workspaces)
    -- Note: workspace_id left as placeholder {workspace_id} so you can change it to create new workspaces
    'curl -X POST "' || c.base_url || '/api/v1/entities/workspaces" -H "Authorization: Bearer {bearer_token}" -H "Content-Type: application/vnd.gooddata.api+json" -d ''{"data":{"id":"{workspace_id}","type":"workspace","attributes":{"name":"{workspace_name}"}}}''' AS curl_post,

    -- Excel formula for POST command
    -- Assuming columns are: A=workspace_id, B=workspace_name, C=curl_post
    -- Note: bearer_token already substituted; workspace_id kept as placeholder for flexibility
    '=SUBSTITUTE(SUBSTITUTE(C2,"{workspace_id}",A2),"{workspace_name}",B2)' as formula_post,

    -- PUT curl command (for updating existing workspaces)
    'curl -X PUT "' || c.base_url || '/api/v1/entities/workspaces/' || w.workspace_id || '" -H "Authorization: Bearer {bearer_token}" -H "Content-Type: application/vnd.gooddata.api+json" -d ''{"data":{"id":"' || w.workspace_id || '","type":"workspace","attributes":{"name":"{workspace_name}"}}}''' AS curl_put,

    -- Excel formula for PUT command
    -- Assuming columns are: B=workspace_name, E=curl_put
    -- Note: bearer_token and workspace_id are already substituted
    '=SUBSTITUTE(E2,"{workspace_name}",B2)' as formula_put,

    -- DELETE curl command (for deleting workspaces)
    'curl -X DELETE "' || c.base_url || '/api/v1/entities/workspaces/' || w.workspace_id || '" -H "Authorization: Bearer {bearer_token}"' AS curl_delete

FROM workspaces w
CROSS JOIN config c
WHERE w.workspace_id IS NOT NULL
ORDER BY w.workspace_id;
