# Post-Export SQL Dependency Graph

This document visualizes the dependencies between views and updates in the post-export processing system.

## Visual Dependency Graph

```
┌─────────────────────────────────────────────────────────────────┐
│                          VIEWS (Phase 1)                        │
│                        (No Dependencies)                        │
└─────────────────────────────────────────────────────────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        │                       │                       │
    ┌───▼────┐          ┌──────▼──────┐        ┌──────▼──────┐
    │ v_metric│          │v_dashboard  │        │v_visualization│
    │  _tags  │          │   _tags     │        │    _tags      │ ◄───┐
    └────┬────┘          └─────────────┘        └───────────────┘     │
         │                                                             │
    ┌────▼────┐                                                       │
    │ v_metric│                                                       │
    │_dependencies│                                                   │
    └─────────┘                                                       │
         │                                                            │
    ┌────▼────┐                                                       │
    │ v_metric│                                                       │
    │ _usage  │                                                       │
    └─────────┘                                                       │
         │                                                            │
    ┌────▼────┐                                                       │
    │v_visualization│                                                 │
    │  _usage │                                                       │
    └─────────┘                                                       │
                                                                      │
┌─────────────────────────────────────────────────────────────────┐ │
│                       UPDATES (Phase 2)                         │ │
└─────────────────────────────────────────────────────────────────┘ │
                                │                                    │
        ┌───────────────────────┼────────────────────────┐          │
        │                       │                        │          │
┌───────▼────────┐    ┌─────────▼───────┐    ┌──────────▼─────────┐│
│metrics_probable│    │metrics_usage    │    │visualizations_usage││
│  _duplicates   │    │    _check       │    │      _check        ││
│                │    │                 │    │                    ││
│ (metrics)      │    │   (metrics)     │    │ (visualizations)   ││
└────────────────┘    └─────────────────┘    └────────────────────┘│
                                                                    │
                                             ┌──────────────────────┘
                                             │
                                    ┌────────▼──────────┐
                                    │visuals_with_same  │
                                    │     _content      │
                                    │                   │
                                    │(visualizations)   │
                                    │                   │
                                    │DEPENDS ON:        │
                                    │v_visualization    │
                                    │     _tags         │
                                    └───────────────────┘
```

## Dependency Matrix

| Operation | Type | Depends On | Used By |
|-----------|------|------------|---------|
| `v_metric_tags` | VIEW | (none) | (none) |
| `v_visualization_tags` | VIEW | (none) | `visuals_with_same_content` ⚠️ |
| `v_dashboard_tags` | VIEW | (none) | (none) |
| `v_metric_usage` | VIEW | (none) | (none) |
| `v_metric_dependencies` | VIEW | (none) | (none) |
| `v_visualization_usage` | VIEW | (none) | (none) |
| `metrics_probable_duplicates` | UPDATE | (none) | (none) |
| `metrics_usage_check` | UPDATE | (none) | (none) |
| `visualizations_usage_check` | UPDATE | (none) | (none) |
| `visuals_with_same_content` | UPDATE | `v_visualization_tags` | (none) |

## Critical Dependencies

### ⚠️ Current Dependencies

**`visuals_with_same_content` → `v_visualization_tags`**
- The update script uses this view to compare visualization tags
- Topological sort ensures the view is created before the update runs
- Declared in `post_export_config.yaml`:
  ```yaml
  updates:
    visuals_with_same_content:
      dependencies:
        - v_visualization_tags
  ```

## Topological Sort Result

The system automatically computes this execution order:

```
1. metrics_probable_duplicates    ← UPDATE (no dependencies)
2. metrics_usage_check             ← UPDATE (no dependencies)
3. v_dashboard_tags                ← VIEW (no dependencies)
4. v_metric_dependencies           ← VIEW (no dependencies)
5. v_metric_tags                   ← VIEW (no dependencies)
6. v_metric_usage                  ← VIEW (no dependencies)
7. v_visualization_tags            ← VIEW (no dependencies) ⚠️
8. v_visualization_usage           ← VIEW (no dependencies)
9. visualizations_usage_check      ← UPDATE (no dependencies)
10. visuals_with_same_content      ← UPDATE (depends on #7) ⚠️
```

**Key observation**: Items 1-9 have no dependencies, so they can execute in any order (alphabetically sorted for determinism). Item 10 MUST come after item 7.

## How Dependencies Work

### 1. Configuration (YAML)
```yaml
views:
  v_visualization_tags:
    sql_file: views/v_visualization_tags.sql
    dependencies: []

updates:
  visuals_with_same_content:
    sql_file: updates/visuals_with_same_content.sql
    dependencies:
      - v_visualization_tags  # Explicit dependency
```

### 2. Topological Sort
The system builds a dependency graph and sorts operations so dependencies are always executed first.

### 3. Execution
Operations are executed in the sorted order, ensuring all dependencies are satisfied.

## Adding Dependencies

### When to Add a Dependency

Add a dependency when your SQL script:
- **References a view** (e.g., `FROM v_some_view` or `JOIN v_some_view`)
- **Uses a computed column** from another update (rare, usually split phases)
- **Requires data** populated by another operation

### When NOT to Add a Dependency

Don't add a dependency when your SQL script:
- **Only uses base tables** (metrics, visualizations, dashboards, etc.)
- **Doesn't reference other views** directly
- **Is completely independent** of other operations

### Example: Adding a New Dependent Operation

If you create a new update that uses `v_metric_usage`:

```yaml
updates:
  analyze_metric_usage:
    sql_file: updates/analyze_metric_usage.sql
    description: Analyzes metric usage patterns
    category: analytics
    table: metrics
    dependencies:
      - v_metric_usage  # ← Explicit dependency
    required_columns:
      usage_pattern: TEXT
```

The system will automatically ensure `v_metric_usage` is created before `analyze_metric_usage` runs.

## Circular Dependency Detection

The topological sort will **fail fast** if circular dependencies exist:

```yaml
views:
  view_a:
    dependencies: [view_b]
  view_b:
    dependencies: [view_a]  # ← Circular!
```

Error message:
```
❌ Configuration error: Circular dependency detected. 
Cannot process: {'view_a', 'view_b'}
```

## Benefits of Explicit Dependencies

1. **Guaranteed Correctness**: Dependencies are always satisfied
2. **Clear Documentation**: Easy to see what depends on what
3. **Fast Failure**: Configuration errors detected before execution
4. **No Retry Logic**: Single-pass execution
5. **Maintainable**: Easy to understand and modify

## Related Documentation

- **Configuration**: See `post_export_config.yaml` for current setup
- **Execution Order**: See `EXECUTION_ORDER.md` for detailed execution flow
- **Migration Guide**: See `MIGRATION_TO_YAML.md` for refactoring details
- **Implementation**: See `post_export.py` for topological sort algorithm

