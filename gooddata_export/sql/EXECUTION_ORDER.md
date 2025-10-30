# SQL Scripts Execution Order and Dependencies

This document describes the execution order and dependencies of SQL scripts in post-export processing.

## Visualizations Processing

**Execution order (must be maintained):**

1. **`views/v_visualization_tags.sql`**
   - **Dependencies**: `visualizations` table
   - **Description**: Creates view that unnests tags into individual rows
   - **Must run first**: Used by `visuals_with_same_content.sql`

2. **`updates/visuals_with_same_content.sql`**
   - **Dependencies**: `visualizations` table, `v_visualization_tags` view
   - **Description**: Identifies duplicate visualizations, updates columns for deduplication
   - **Must run after**: `v_visualization_tags.sql`

3. **`updates/visualizations_usage_check.sql`**
   - **Dependencies**: `visualizations` table, `dashboard_visualizations` table
   - **Description**: Marks visualizations as used/unused
   - **Can run anytime after**: `v_visualization_tags.sql`

4. **`views/v_visualization_usage.sql`**
   - **Dependencies**: `visualizations` table, `dashboard_visualizations` table, `dashboards` table
   - **Description**: Shows where visualizations are used
   - **Can run anytime after**: `v_visualization_tags.sql`

## Metrics Processing

**Execution order (flexible - no interdependencies):**

1. **`updates/metrics_probable_duplicates.sql`**
   - **Dependencies**: `metrics` table
   - **Description**: Identifies probable duplicate metrics

2. **`updates/metrics_usage_check.sql`**
   - **Dependencies**: `metrics`, `dashboard_metrics`, `visualization_metrics` tables
   - **Description**: Marks metrics as used/unused

3. **`views/v_metric_usage.sql`**
   - **Dependencies**: `metrics`, `dashboard_metrics`, `dashboards`, `visualizations`, `visualization_metrics`, `dashboard_visualizations` tables
   - **Description**: Shows where metrics are used

4. **`views/v_metric_dependencies.sql`**
   - **Dependencies**: `metrics` table
   - **Description**: Shows metric dependencies via MAQL references

5. **`views/v_metric_tags.sql`**
   - **Dependencies**: `metrics` table
   - **Description**: Unnests metric tags into individual rows

## Dashboards Processing

**Execution order (no dependencies):**

1. **`views/v_dashboard_tags.sql`**
   - **Dependencies**: `dashboards` table
   - **Description**: Unnests dashboard tags into individual rows

## Critical Dependencies

⚠️ **IMPORTANT**: The following dependency MUST be maintained:
- `visuals_with_same_content.sql` depends on `v_visualization_tags.sql` existing
- Therefore, `v_visualization_tags.sql` MUST be created before `visuals_with_same_content.sql` runs

