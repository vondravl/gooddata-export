# SQL Scripts Execution Order and Dependencies

This document describes the execution order and dependencies of SQL scripts in post-export processing.

## Execution Structure

Post-export processing is divided into two main phases:

### Phase 1: CREATE VIEWS (Read-Only)
All views are created first. Views are read-only and can reference any tables or other views.
- ✅ Safe to create early
- ✅ Can be used by table update scripts
- ✅ Combined views (joining multiple tables) go here

### Phase 2: TABLE UPDATES (Modify Tables)
Table update scripts modify existing tables (ADD COLUMN, UPDATE).
- Grouped by which table they modify
- Can reference views created in Phase 1

---

## Phase 1: Views Section

All views are created in this order:

1. **`v_metric_tags.sql`**
   - Dependencies: `metrics` table
   - Description: Unnests metric tags into individual rows
   - Type: Simple entity-specific view

2. **`v_visualization_tags.sql`** ⚠️ CRITICAL
   - Dependencies: `visualizations` table
   - Description: Unnests visualization tags into individual rows
   - Type: Simple entity-specific view
   - **Used by**: `visuals_with_same_content.sql` in Phase 2

3. **`v_dashboard_tags.sql`**
   - Dependencies: `dashboards` table
   - Description: Unnests dashboard tags into individual rows
   - Type: Simple entity-specific view

4. **`v_metric_usage.sql`**
   - Dependencies: `metrics`, `dashboard_metrics`, `dashboards`, `visualizations`, `visualization_metrics`, `dashboard_visualizations` tables
   - Description: Shows where metrics are used (joins multiple tables)
   - Type: **Combined view** (crosses entity boundaries)

5. **`v_metric_dependencies.sql`**
   - Dependencies: `metrics` table (self-join)
   - Description: Shows metric dependencies via MAQL references
   - Type: Relationship view

6. **`v_visualization_usage.sql`**
   - Dependencies: `visualizations`, `dashboard_visualizations`, `dashboards` tables
   - Description: Shows where visualizations are used (joins multiple tables)
   - Type: **Combined view** (crosses entity boundaries)

---

## Phase 2: Table Updates

### Visualizations Table Updates

1. **`visuals_with_same_content.sql`**
   - Dependencies: `visualizations` table, `v_visualization_tags` view ⚠️
   - Updates columns: `columns`, `same_columns_id`, `same_visuals_id`, `same_visuals_id_with_tags`
   - Description: Identifies duplicate visualizations

2. **`visualizations_usage_check.sql`**
   - Dependencies: `visualizations`, `dashboard_visualizations` tables
   - Updates column: `is_used`
   - Description: Marks visualizations as used/unused

### Metrics Table Updates

1. **`metrics_probable_duplicates.sql`**
   - Dependencies: `metrics` table
   - Updates column: `similar_metric_id`
   - Description: Identifies probable duplicate metrics

2. **`metrics_usage_check.sql`**
   - Dependencies: `metrics`, `dashboard_metrics`, `visualization_metrics` tables
   - Updates columns: `is_used_insight`, `is_used_maql`
   - Description: Marks metrics as used/unused

---

## Adding New Scripts

### For Entity-Specific Views
Add to the `"views"` section in `POST_EXPORT_CONFIG`

### For Combined Views (Joining Multiple Tables)
Add to the `"views"` section in `POST_EXPORT_CONFIG` - that's the beauty of this structure!

Example combined view:
```sql
-- v_metric_visualization_dashboard_usage.sql
CREATE VIEW IF NOT EXISTS v_metric_visualization_dashboard_usage AS
SELECT 
    m.metric_id,
    v.visualization_id,
    d.dashboard_id
FROM metrics m
JOIN visualization_metrics vm ON m.metric_id = vm.metric_id
JOIN visualizations v ON vm.visualization_id = v.visualization_id
JOIN dashboard_visualizations dv ON v.visualization_id = dv.visualization_id
JOIN dashboards d ON dv.dashboard_id = d.dashboard_id;
```

### For Table Updates
Add to the appropriate table section (`"visualizations"`, `"metrics"`, etc.) with `required_columns`

---

## Critical Dependency

⚠️ **IMPORTANT**: 
- ALL views are created in Phase 1
- ALL table updates happen in Phase 2
- This ensures table updates can safely reference any view

