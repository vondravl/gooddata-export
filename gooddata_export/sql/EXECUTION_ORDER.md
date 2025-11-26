# SQL Scripts Execution Order and Dependencies

This document describes how SQL scripts are executed in post-export processing.

## Overview

Post-export processing is now configured via **`post_export_config.yaml`**. This provides:
- ✅ **Self-documenting configuration** with descriptions and categories
- ✅ **Explicit dependency management** - no more retry logic needed
- ✅ **Automatic execution ordering** via topological sort
- ✅ **Clear separation** between views (read-only), procedures (parameterized), and updates (table modifications)
- ✅ **Stored procedure simulation** - SQLite doesn't have procedures, so we use parameterized views

## Configuration Structure

The YAML file defines three sections:

### 1. Views (Read-Only)
Database views that can reference any tables or other views.
- Created first
- Safe to create early
- Can be used by table update scripts
- No table modifications

### 2. Procedures (Parameterized Views)
SQLite doesn't support stored procedures, so we simulate them with parameterized views.
- Accept input parameters via YAML configuration
- Support config value substitution (e.g., `{{WORKSPACE_ID}}`)
- Support literal string substitution (e.g., `$${TOKEN_VAR}`)
- Return result sets like regular views
- Parameters substituted at execution time

### 3. Updates (Table Modifications)
Scripts that modify existing tables (ALTER TABLE, UPDATE).
- Executed after views
- Can reference any views
- Grouped by target table
- Include required column definitions

## Current Execution Order

Operations are executed in **dependency order** using topological sort. The actual execution order is determined automatically based on the `dependencies` field in the YAML configuration.

### Phase 1: Views (no dependencies between them currently)
1. **`v_metrics_tags`** - Unnests metric tags into individual rows
2. **`v_visualizations_tags`** - Unnests visualization tags into individual rows
3. **`v_dashboards_tags`** - Unnests dashboard tags into individual rows
4. **`v_metrics_usage`** - Shows where metrics are used
5. **`v_metrics_dependencies`** - Shows metric dependencies via MAQL
6. **`v_visualizations_usage`** - Shows where visualizations are used

### Phase 1.5: Procedures (with parameter substitution)
1. **`v_procedures_api_metrics`** - Procedure to generate curl commands for API metric operations
   - Parameters: `workspace_id` (from config), `bearer_token` (literal `${TOKEN_GOODDATA_DEV}`)
   - Returns: POST, PUT, DELETE commands with Excel formula helpers

### Phase 2: Updates (executed in dependency order)
1. **`visuals_with_same_content`** (depends on: `v_visualizations_tags`)
   - Updates: `visualizations` table
   - Columns: `columns`, `same_columns_id`, `same_visuals_id`, `same_visuals_id_with_tags`

2. **`visualizations_usage_check`** (no dependencies)
   - Updates: `visualizations` table
   - Column: `is_used`

3. **`metrics_probable_duplicates`** (no dependencies)
   - Updates: `metrics` table
   - Column: `similar_metric_id`

4. **`metrics_usage_check`** (no dependencies)
   - Updates: `metrics` table
   - Columns: `is_used_insight`, `is_used_maql`

## Adding New Scripts

### To Add a View:
```yaml
views:
  v_your_new_view:
    sql_file: views/v_your_new_view.sql
    description: What this view does
    category: tagging|usage|analytics
    dependencies: []  # or list other views it depends on
```

### To Add a Procedure:
```yaml
procedures:
  v_procedures_your_operation:
    sql_file: procedures/v_procedures_your_operation.sql
    description: What this procedure does and returns
    category: procedures
    dependencies: []  # or list views it depends on
    parameters:
      workspace_id: "{{WORKSPACE_ID}}"  # Config value substitution
      bearer_token: "$${TOKEN_VAR}"     # Literal string substitution (shell var)
      api_url: "https://api.example.com"  # Direct string substitution
```

**Parameter Types (Simulating Procedure Arguments):**
- `{{CONFIG_KEY}}` - Replaced with value from ExportConfig (e.g., WORKSPACE_ID, BASE_URL)
- `$${LITERAL}` - Replaced with literal string `${LITERAL}` (for shell variables)
- Plain string - Replaced as-is

**Note:** SQLite doesn't support stored procedures. This system simulates procedures by:
1. Accepting parameters via YAML configuration
2. Substituting parameters into the SQL at runtime
3. Creating views that return computed results

### To Add an Update:
```yaml
updates:
  your_new_update:
    sql_file: updates/your_new_update.sql
    description: What this update does
    category: usage|deduplication|automation
    table: table_name  # which table it modifies
    dependencies:
      - v_some_view  # if it uses any views
    required_columns:
      column_name: COLUMN_TYPE
      another_column: INTEGER DEFAULT 0
```

## Dependency Resolution

The system uses **Kahn's algorithm** for topological sorting:
1. All operations are loaded from YAML
2. A dependency graph is built
3. Operations are sorted so dependencies are always executed first
4. Execution proceeds in sorted order
5. If circular dependencies exist, the system fails with a clear error

## Critical Dependencies

⚠️ **Current critical dependency**:
- `visuals_with_same_content` **depends on** `v_visualizations_tags`
  - The update uses this view to compare visualization tags
  - The view MUST be created before the update runs
  - This is enforced automatically via dependency declaration

## Benefits of YAML Configuration

| Old Approach | New Approach |
|-------------|-------------|
| Hard-coded dictionary in Python | Self-documenting YAML file |
| Retry mechanism for ordering issues | Explicit dependencies with topological sort |
| Manual ordering required | Automatic dependency resolution |
| Difficult to understand relationships | Clear dependency declarations |
| Split between views/updates by section | Unified dependency graph |
| No parameter support | Parameterized procedures (stored procedure simulation) |

## Troubleshooting

### "Circular dependency detected"
- Check your YAML configuration
- Look for circular references (A depends on B, B depends on A)
- Review the `dependencies` lists

### "Item 'X' depends on 'Y' which doesn't exist"
- You've declared a dependency that isn't defined in the YAML
- Check spelling of the dependency name
- Ensure the dependency is in the same YAML file

### SQL Execution Errors
- Check the SQL file exists at the specified path
- Review SQL syntax
- Ensure table/column names are correct
- Check that dependencies are correctly declared (e.g., views exist before they're used)

## File Structure
```
gooddata_export/sql/
├── post_export_config.yaml    # Main configuration file
├── EXECUTION_ORDER.md          # This documentation
├── DEPENDENCY_GRAPH.md         # Dependency visualization
├── views/                      # View SQL files
│   ├── v_metrics_tags.sql
│   ├── v_visualizations_tags.sql
│   └── ...
├── procedures/                 # Procedures (parameterized views)
│   ├── v_procedures_api_metrics.sql
│   └── ...
└── updates/                    # Update SQL files
    ├── visuals_with_same_content.sql
    ├── metrics_usage_check.sql
    └── ...
```
