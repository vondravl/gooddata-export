# GoodData Export - Usage Guide

## Quick Start

### Option 1: Using Make Commands (Recommended)

```bash
# Full workflow: Export + Enrichment
make export-enrich

# Export only (skip post-processing)
make export

# Enrichment only (on existing database)
make enrich

# Enrich a specific database
make enrich DB=output/db/custom.db
```

### Option 2: Using Python Directly

```bash
# Full workflow: Export + Enrichment
python main.py export

# Export only (skip post-processing)
python main.py export --skip-post-export

# Enrichment only
python main.py enrich --db-path output/db/gooddata_export.db
```

## Three Workflows

### 1. Export Only (`make export`)

Exports data from GoodData to SQLite/CSV **without** running post-processing.

**When to use:**
- You want raw data quickly
- You'll run enrichment later
- Testing export functionality

**What it does:**
- Fetches metadata from GoodData API
- Saves to SQLite database
- Optionally exports to CSV
- **Skips:** Views, procedures, table updates

**Example:**
```bash
make export
# or
python main.py export --skip-post-export
```

### 2. Enrich Only (`make enrich`)

Runs post-processing on an existing database.

**When to use:**
- You already have exported data
- You modified SQL procedures/views
- You want to re-run enrichment
- Testing enrichment logic

**What it does:**
- Creates views (v_metric_tags, v_visualization_tags, etc.)
- Executes procedures (v_procedures_api_metrics, etc.)
- Updates tables (duplicate detection, usage checks)
- Applies all SQL from `post_export_config.yaml`

**Example:**
```bash
# Default database
make enrich

# Specific database
make enrich DB=output/db/mic_diagnose_master.db

# With Python
python main.py enrich --db-path output/db/gooddata_export.db
python main.py enrich --db-path output/db/custom.db --debug
```

### 3. Full Workflow (`make export-enrich`)

Complete end-to-end process: export data, then enrich it.

**When to use:**
- Normal daily workflow
- Fresh export with all processing
- Production runs

**What it does:**
1. Exports data from GoodData
2. Saves to database
3. Runs all post-processing
4. Database ready to query

**Example:**
```bash
make export-enrich
# or
python main.py export  # (enrichment runs by default)
```

## Post-Processing (Enrichment) Details

When you run enrichment, it executes all SQL operations defined in `gooddata_export/sql/post_export_config.yaml`:

### Views Created
- `v_metric_tags` - Metric tags unnested
- `v_visualization_tags` - Visualization tags unnested
- `v_dashboard_tags` - Dashboard tags unnested
- `v_ldm_datasets_tags` - LDM dataset tags unnested
- `v_ldm_columns_tags` - LDM column tags unnested
- `v_metric_usage` - Where metrics are used
- `v_metric_dependencies` - Metric MAQL dependencies
- `v_visualization_usage` - Where visualizations are used
- `v_filter_context_usage` - Where filter contexts are used in dashboards
- `v_ldm_dataset_column_tag_check` - Validates dataset-column tag consistency

### Procedures Executed
- `v_procedures_api_metrics` - Generates curl commands for API operations
  - Parameters substituted: workspace_id, bearer_token
  - Returns: POST, PUT, DELETE commands with Excel formulas

### Table Updates Applied
- `visuals_with_same_content` - Duplicate visualization detection
- `visualizations_usage_check` - Mark used/unused visualizations
- `metrics_probable_duplicates` - Find similar metrics
- `metrics_usage_check` - Mark used metrics in insights/MAQL
- `filter_contexts_with_same_content` - Duplicate filter context detection
- `filter_contexts_usage_check` - Mark used/unused filter contexts

## Configuration

Create `.env.gdcloud` file:

```bash
BASE_URL=https://your-instance.gooddata.com
WORKSPACE_ID=your_workspace_id
BEARER_TOKEN=your_api_token

# Optional
ENABLE_RICH_TEXT_EXTRACTION=true
INCLUDE_CHILD_WORKSPACES=false
MAX_WORKERS=5
DEBUG=false
```

## Common Use Cases

### Daily Export

```bash
# Full workflow - export fresh data with enrichment
make export-enrich
```

### Development: Testing SQL Changes

```bash
# 1. Export once (keep data)
make export

# 2. Modify SQL procedures/views in gooddata_export/sql/

# 3. Re-run enrichment to test changes
make enrich

# 4. Repeat step 2-3 as needed
```

### Multiple Workspaces

```bash
# Export workspace A
python main.py export --workspace-id workspace-a --db-name workspace-a.db

# Export workspace B  
python main.py export --workspace-id workspace-b --db-name workspace-b.db

# Enrich both
python main.py enrich --db-path output/db/workspace-a.db
python main.py enrich --db-path output/db/workspace-b.db
```

### Production Export with Custom Settings

```bash
python main.py export \
  --format sqlite \
  --include-child-workspaces \
  --max-workers 10 \
  --db-name production-$(date +%Y%m%d).db
```

## Querying Results

After enrichment, query the enriched database:

```sql
-- Use views
SELECT * FROM v_metric_usage WHERE usage_count > 5;

-- Call procedures (query parameterized views)
SELECT * FROM v_procedures_api_metrics WHERE metric_id LIKE 'revenue%';

-- Check table updates
SELECT * FROM metrics WHERE is_used_insight = 1;
```

## Troubleshooting

### "Database not found" when running enrich

Make sure you specify the correct path:
```bash
make enrich DB=output/db/gooddata_export.db
```

### Enrichment fails

Run with debug flag:
```bash
python main.py enrich --db-path output/db/gooddata_export.db --debug
```

### Want to skip enrichment temporarily

```bash
make export  # or python main.py export --skip-post-export
```

### Re-run enrichment after SQL changes

```bash
# Enrichment is idempotent - safe to run multiple times
make enrich
```

## Legacy Command

The old `make run` command still works but shows a deprecation warning. Use the new commands:

```bash
# Old
make run

# New (equivalent)
make export-enrich
```

## Advanced: Custom Procedures

You can add your own procedures to `gooddata_export/sql/procedures/`:

1. Create SQL file with parameterized view
2. Add entry to `post_export_config.yaml`
3. Run `make enrich` to execute

See `gooddata_export/sql/procedures/README.md` for details.

## Summary Table

| Command | Exports Data | Runs Enrichment | Use Case |
|---------|--------------|-----------------|----------|
| `make export` | ✅ Yes | ❌ No | Quick raw data export |
| `make enrich` | ❌ No | ✅ Yes | Re-process existing DB |
| `make export-enrich` | ✅ Yes | ✅ Yes | Full workflow (default) |
| `make run` | ✅ Yes | ✅ Yes | Legacy (use export-enrich) |

## Getting Help

```bash
# Makefile help
make help

# Python CLI help
python main.py --help
python main.py export --help
python main.py enrich --help
```

