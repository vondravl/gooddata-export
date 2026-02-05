# GoodData Export

A Python library for exporting GoodData workspace metadata to SQLite databases and CSV files.

## Features

- **Multiple Export Formats**: Export to SQLite, CSV, or both
- **Multi-Workspace Support**: Process parent and child workspaces in parallel
- **Local Layout JSON Support**: Process local JSON-based layout files without API calls
- **Flexible Configuration**: Configure via Python API or environment variables
- **Post-Processing**: Automatic duplicate detection and relationship analysis
- **Rich Text Extraction**: Optional extraction of metrics/insights from dashboard rich text widgets
- **Standalone**: Zero Flask dependencies - pure Python library
- **Clean CSV Exports**: Automatically clears CSV directory before each export to prevent stale data

## Installation

### From Git

```bash
pip install git+https://github.com/vondravl/gooddata-export.git
```

### From source (local development)

```bash
git clone https://github.com/vondravl/gooddata-export.git
cd gooddata-export
pip install -e .
```

## Quick Start

### Command Line Interface

1. Create a `.env.gdcloud` configuration file:

```env
BASE_URL=https://your-instance.gooddata.com
WORKSPACE_ID=your_workspace_id
BEARER_TOKEN=your_api_token
```

2. Run the export:

```bash
# Basic export (both SQLite and CSV)
gooddata-export export

# Export only SQLite (fastest)
gooddata-export export --format sqlite

# Export with child workspaces
gooddata-export export --include-child-workspaces --max-workers 10

# Custom directories
gooddata-export export --db-dir my_databases --csv-dir my_csvs

# Enable debug mode
gooddata-export export --debug

# Run enrichment on existing database
gooddata-export enrich --db-path output/db/gooddata_export.db

# Get help
gooddata-export --help
```

### Python API

```python
from gooddata_export import export_metadata

result = export_metadata(
    base_url="https://your-instance.gooddata.com",
    workspace_id="your_workspace_id",
    bearer_token="your_api_token"
)

print(f"Database created at: {result['db_path']}")  # output/db/gooddata_export.db
print(f"CSV files in: {result['csv_dir']}")  # output/metadata_csv/
print(f"Processed {result['workspace_count']} workspace(s)")
```

### Using Environment Variables (Python API)

Create a `.env.gdcloud` file:

```env
BASE_URL=https://your-instance.gooddata.com
WORKSPACE_ID=your_workspace_id
BEARER_TOKEN=your_api_token
```

Then in Python:

```python
from gooddata_export.config import ExportConfig
from gooddata_export.export import export_all_metadata

# Load config from .env files
config = ExportConfig(load_from_env=True)

result = export_all_metadata(
    config=config,
    output_dir="output"
)
```

## CLI Options

### Commands

- `gooddata-export export` - Export metadata from GoodData
- `gooddata-export enrich` - Run post-export enrichment on existing database

### Connection Options
- `--base-url URL` - GoodData API base URL (overrides .env.gdcloud)
- `--workspace-id ID` - Workspace ID to export (overrides .env.gdcloud)
- `--bearer-token TOKEN` - API authentication token (overrides .env.gdcloud)

### Export Configuration
- `--db-dir DIR` - Directory for SQLite database files (default: `output/db`)
- `--csv-dir DIR` - Directory for CSV export files (default: `output/metadata_csv`)
- `--format {sqlite,csv}` - Export format(s): `sqlite`, `csv`, or both (default: both)
- `--db-name FILENAME` - Custom SQLite database filename (default: `gooddata_export.db`)

### Child Workspace Options
- `--include-child-workspaces` - Include child workspaces in export
- `--child-workspace-data-types {metrics,dashboards,visualizations,filter_contexts}` - Data types to fetch from children
- `--max-workers N` - Maximum parallel workers (default: 5)

### Feature Flags
- `--enable-rich-text-extraction` - Enable extraction from rich text widgets
- `--skip-post-export` - Skip post-export SQL processing (duplicate detection)
- `--debug` - Enable debug logging

### Examples

```bash
# SQLite only (fastest)
gooddata-export export --format sqlite --skip-post-export

# CSV only
gooddata-export export --format csv

# Multi-workspace with specific data types
gooddata-export export --include-child-workspaces --child-workspace-data-types dashboards visualizations --max-workers 15

# Override config with command-line args
gooddata-export export --workspace-id prod_workspace --db-dir exports/prod/db --debug
```

## Usage Examples

### SQLite-Only Export (Fastest)

For maximum speed, export only to SQLite and skip post-processing:

```python
from gooddata_export import export_metadata

result = export_metadata(
    base_url="https://your-instance.gooddata.com",
    workspace_id="your_workspace_id",
    bearer_token="your_token",
    export_formats=["sqlite"],  # SQLite only
    run_post_export=False       # Skip duplicate detection
)
```

This is ideal for:
- Programmatic access to metadata
- Custom post-processing pipelines
- Integration with other tools

### Multi-Workspace Export

Export from a parent workspace and all its children:

```python
result = export_metadata(
    base_url="https://your-instance.gooddata.com",
    workspace_id="parent_workspace_id",
    bearer_token="your_token",
    include_child_workspaces=True,
    child_workspace_data_types=["dashboards", "visualizations"],
    max_parallel_workspaces=5  # Process 5 workspaces at once (default)
)
```

### Local Layout JSON Export (No API Calls)

Process local layout files without connecting to GoodData API. This is useful for:
- Tagging workflows on feature branches before changes are deployed
- Offline analysis of exported layout files
- CI/CD pipelines without API access

```python
import json
from gooddata_export import export_metadata

# Load layout from file (exported via gooddata-cli or API)
with open("layout.json") as f:
    layout = json.load(f)

result = export_metadata(
    base_url="https://your-instance.gooddata.com",  # Used for URL generation only
    workspace_id="my_workspace",
    layout_json=layout,  # No API calls made
    export_formats=["sqlite"],
    run_post_export=True
)
```

Expected layout format:
```json
{
  "analytics": {
    "metrics": [...],
    "visualizationObjects": [...],
    "analyticalDashboards": [...],
    "filterContexts": [...],
    "dashboardPlugins": [...]
  },
  "ldm": {
    "datasets": [...],
    ...
  }
}
```

Note: When using `layout_json`, tables that would be stale (users, user_groups, user_group_members) are automatically truncated.

### Complete Export with All Features

```python
result = export_metadata(
    base_url="https://your-instance.gooddata.com",
    workspace_id="your_workspace_id",
    bearer_token="your_token",
    export_formats=["sqlite", "csv"],
    enable_rich_text_extraction=True,
    run_post_export=True,
    debug=True
)
```

## Configuration Options

### Required Parameters

- `base_url`: GoodData API base URL
- `workspace_id`: Workspace ID to export
- `bearer_token`: API authentication token (required unless `layout_json` is provided)

### Optional Parameters

- `layout_json`: Local layout data dict - when provided, skips API fetch and uses this data directly
- `export_formats`: List of ["sqlite"], ["csv"], or both (default: both)
- `include_child_workspaces`: Fetch data from child workspaces (default: False)
  - Note: The workspaces table is always created with child workspace list; this flag controls whether to fetch child workspace DATA (metrics, dashboards, etc.)
- `child_workspace_data_types`: Data types to fetch from children (default: all)
  - Options: "metrics", "dashboards", "visualizations", "filter_contexts"
- `max_parallel_workspaces`: Parallel processing limit (default: 5)
- `enable_rich_text_extraction`: Extract from rich text widgets (default: False)
- `run_post_export`: Run duplicate detection SQL (default: True)
- `debug`: Enable debug logging (default: False)
- `db_name`: Custom database path (default: output_dir/db/gooddata_export.db)

## Output Structure

**Note**: Before each export, the CSV directory (`output/metadata_csv/`) is automatically cleaned to prevent stale data from mixing with new exports. Database files naturally overwrite themselves and are not cleaned, allowing you to keep workspace-specific databases from multiple exports.

### SQLite Database

The SQLite database contains the following tables:

- **metrics**: Metric definitions, MAQL, and metadata
- **visualizations**: Visualization configurations
- **dashboards**: Dashboard definitions and layouts
- **ldm_datasets**: Logical data model datasets with tags
- **ldm_columns**: LDM columns (attributes, facts, references) with tags
- **ldm_labels**: Attribute label definitions (display forms)
- **filter_contexts**: Filter context definitions
- **filter_context_fields**: Individual filters within each filter context (date filters and attribute filters)
- **workspaces**: Workspace information (always included; child workspaces listed when available)
- **visualizations_references**: Visualization references to metrics, facts, and labels
- **dashboards_visualizations**: Visualization-to-dashboard relationships
- **dashboards_metrics**: Metric-to-dashboard relationships (rich text only)
- **dashboards_references**: Dashboard-level references to labels, datasets, and filter contexts
- **dictionary_metadata**: Export metadata (timestamp, workspace ID, etc.)
- **metrics_references**: All metric references extracted from MAQL - metrics, attributes, labels, and facts (created by post-export)
- **metrics_ancestry**: Full transitive metric ancestry (created by post-export)

### CSV Files

When CSV export is enabled, the following files are created:

- `gooddata_metrics.csv`
- `gooddata_visualizations.csv`
- `gooddata_dashboards.csv`
- `gooddata_ldm_datasets.csv`
- `gooddata_ldm_columns.csv`
- `gooddata_ldm_labels.csv`
- `gooddata_filter_contexts.csv`
- `gooddata_filter_context_fields.csv`
- `gooddata_workspaces.csv` (always included; child workspaces listed when available)
- `gooddata_visualizations_references.csv`
- `gooddata_dashboards_visualizations.csv`
- `gooddata_dashboards_metrics.csv` (rich text only)

## Post-Export Processing

When `run_post_export=True` (default for single workspace exports), the library runs SQL scripts to:

1. **Build metric relationships**: Extracts metric-to-metric references from MAQL formulas
2. **Compute metric ancestry**: Creates transitive closure of metric dependencies
3. **Detect duplicates**: Identifies visualizations and metrics with identical content
4. **Track usage**: Marks which metrics/visualizations are used in dashboards
5. **Create analytical views**: Tag views, usage views, relationship views

Key views created:
- `v_metrics_relationships_*` - Metric dependency analysis and tag inheritance
- `v_metrics_usage`, `v_visualizations_usage` - Usage tracking
- `v_*_tags` - Unnested tag views for filtering

See [USAGE_GUIDE.md](USAGE_GUIDE.md) for detailed post-processing documentation.

Note: Post-export processing is automatically skipped for multi-workspace exports to avoid confusion.

## Performance Tuning

### For Large Multi-Workspace Deployments (1000+ workspaces)

```python
result = export_metadata(
    base_url="...",
    workspace_id="...",
    bearer_token="...",
    include_child_workspaces=True,
    child_workspace_data_types=["dashboards"],  # Fetch only dashboards
    max_parallel_workspaces=20,  # Higher parallelization
    export_formats=["sqlite"],   # Skip CSV
    run_post_export=False        # Skip post-processing
)
```

Expected performance: 10-20 workspaces/minute

### For Smaller Deployments (<100 workspaces)

```python
result = export_metadata(
    base_url="...",
    workspace_id="...",
    bearer_token="...",
    include_child_workspaces=True,
    child_workspace_data_types=["metrics", "dashboards", "visualizations", "filter_contexts"],
    max_parallel_workspaces=8
)
```

## Development

### Running Tests

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest
```

### Project Structure

```
gooddata-export/
├── gooddata_export/           # Core library package
│   ├── __init__.py           # Main API exports
│   ├── cli.py                # Command-line interface
│   ├── config.py             # Configuration handling
│   ├── export.py             # Export orchestration
│   ├── process.py            # Data processing logic
│   ├── common.py             # API client utilities
│   ├── db.py                 # Database utilities
│   ├── post_export.py        # Post-processing orchestration
│   └── sql/                  # SQL scripts (auto-executed during post-export)
│       ├── procedures/       # Stored procedures and automation views
│       ├── updates/          # Data enrichment scripts (duplicates, usage analysis)
│       ├── views/            # Analytical views (dependencies, tags, usage)
│       └── *.yaml, *.md      # Execution config and documentation
├── main.py                   # Development CLI wrapper (convenience for local dev)
├── pyproject.toml            # Package configuration
├── README.md                 # This file
├── LICENSE                   # MIT License
├── USAGE_GUIDE.md            # Detailed usage examples
├── .env.gdcloud              # Configuration file (create this)
└── output/                   # Export destination (auto-created)
    ├── db/                   # SQLite databases
    └── metadata_csv/         # CSV exports
```

**Note**: The `sql/` directory contains various analytical scripts that are automatically applied during post-export processing. These scripts evolve frequently as new analysis capabilities are added.

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please submit pull requests or open issues on GitHub.

## Support

For issues and questions, please open an issue on GitHub.
