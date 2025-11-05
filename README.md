# GoodData Export

A Python library for exporting GoodData workspace metadata to SQLite databases and CSV files.

## Features

- **Multiple Export Formats**: Export to SQLite, CSV, or both
- **Multi-Workspace Support**: Process parent and child workspaces in parallel
- **Flexible Configuration**: Configure via Python API or environment variables
- **Post-Processing**: Automatic duplicate detection and relationship analysis
- **Rich Text Extraction**: Optional extraction of metrics/insights from dashboard rich text widgets
- **Standalone**: Zero Flask dependencies - pure Python library
- **Clean CSV Exports**: Automatically clears CSV directory before each export to prevent stale data

## Installation

### From source (local development)

```bash
git clone <repository-url>
cd gooddata-export
pip install -e .
```

### As a dependency

```bash
pip install git+<repository-url>
```

## Quick Start

### Command Line Interface (Easiest)

1. Create a `.env.gdcloud` configuration file:

```env
BASE_URL=https://your-instance.gooddata.com
WORKSPACE_ID=your_workspace_id
BEARER_TOKEN=your_api_token
```

2. Run the export:

```bash
# Basic export (both SQLite and CSV)
python main.py

# Export only SQLite (fastest)
python main.py --format sqlite

# Export with child workspaces
python main.py --include-child-workspaces --max-workers 10

# Custom directories
python main.py --db-dir my_databases --csv-dir my_csvs

# Enable debug mode
python main.py --debug

# Get help
python main.py --help
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

The `main.py` script supports the following command-line options:

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
python main.py --format sqlite --skip-post-export

# CSV only
python main.py --format csv

# Multi-workspace with specific data types
python main.py --include-child-workspaces --child-workspace-data-types dashboards visualizations --max-workers 15

# Override config with command-line args
python main.py --workspace-id prod_workspace --db-dir exports/prod/db --debug
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

### Complete Export with All Features

```python
result = export_metadata(
    base_url="https://your-instance.gooddata.com",
    workspace_id="your_workspace_id",
    bearer_token="your_token",
    output_dir="output",
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
- `bearer_token`: API authentication token

### Optional Parameters

- `output_dir`: Output directory (default: "output")
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
- **ldm_datasets**: Logical data model datasets
- **ldm_columns**: LDM columns (attributes, facts, references)
- **filter_contexts**: Filter context definitions
- **workspaces**: Workspace information (always included; child workspaces listed when available)
- **visualization_metrics**: Metric-to-visualization relationships
- **dashboard_visualizations**: Visualization-to-dashboard relationships
- **dashboard_metrics**: Metric-to-dashboard relationships (rich text only)
- **dictionary_metadata**: Export metadata (timestamp, workspace ID, etc.)

### CSV Files

When CSV export is enabled, the following files are created:

- `gooddata_metrics.csv`
- `gooddata_visualizations.csv`
- `gooddata_dashboards.csv`
- `gooddata_ldm_datasets.csv`
- `gooddata_ldm_columns.csv`
- `gooddata_filter_contexts.csv`
- `gooddata_workspaces.csv` (always included; child workspaces listed when available)
- `gooddata_visualization_metrics.csv`
- `gooddata_dashboard_visualizations.csv`
- `gooddata_dashboard_metrics.csv` (rich text only)

## Post-Export Processing

When `run_post_export=True` (default for single workspace exports), the library runs SQL scripts to:

1. **Detect duplicate visualizations**: Identifies visualizations with identical content
2. **Detect similar metrics**: Finds metrics with similar MAQL definitions
3. **Add helper columns**: Adds grouping IDs for duplicate analysis

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
├── gooddata_export/
│   ├── __init__.py         # Main API
│   ├── config.py           # Configuration handling
│   ├── export.py           # Export orchestration
│   ├── process.py          # Data processing
│   ├── common.py           # API client utilities
│   ├── db.py               # Database utilities
│   ├── post_export.py      # Post-processing
│   └── sql/
│       ├── metrics_probable_duplicates.sql
│       └── visuals_with_same_content.sql
├── main.py                 # Command-line interface
├── setup.py
├── requirements.txt
├── README.md
└── .env.gdcloud           # Configuration file (create from template)
```

## License

[Your License Here]

## Contributing

Contributions are welcome! Please submit pull requests or open issues on GitHub.

## Support

For issues and questions, please open an issue on GitHub.

