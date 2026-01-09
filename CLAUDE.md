# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/claude-code) when working with this repository.

## Project Overview

GoodData Export is a Python library for exporting GoodData workspace metadata to SQLite databases and CSV files. It fetches metrics, dashboards, visualizations, and LDM (Logical Data Model) information from the GoodData API and stores them locally for analysis.

The library supports two modes:
1. **API mode** (default): Fetches data from GoodData API
2. **Local mode**: Processes local `layout.json` files without API calls (useful for tagging workflows on feature branches)

**This is a public package.** When making changes:

1. Bump the version in `pyproject.toml` (single source of truth):
   ```toml
   version = "1.0.0"  # Increment appropriately
   ```
   Note: `__version__` in `__init__.py` is derived automatically via `importlib.metadata`

2. Update `CHANGELOG.md` with the changes (follow [Keep a Changelog](https://keepachangelog.com/) format)

## Security Considerations

This is a **public package**. Before committing:

- Never commit `.env*` files (already in .gitignore)
- Never include real API tokens, workspace IDs, or customer data in code/tests
- Use mock data or placeholders in examples and tests
- Review diffs for accidentally exposed credentials or PII

## Key Commands

```bash
# Full workflow: export + enrichment
make run              # or: make export-enrich

# Export only (skip post-processing)
make export

# Enrichment only (on existing database)
make enrich
make enrich DB=output/db/custom.db

# Run with Python directly
python main.py export
python main.py enrich --db-path output/db/gooddata_export.db

# Run tests
pytest

# Format code
python formatting_ruff.py
```

## Architecture

### Core Components

```
gooddata_export/
├── __init__.py          # Public API exports
├── config.py            # ExportConfig class, environment loading
├── constants.py         # Shared constants (LOCAL_MODE_STALE_TABLES)
├── common.py            # API client utilities (GoodDataClient)
├── db.py                # SQLite database utilities
├── post_export.py       # Post-processing orchestration, topological sort
├── export/              # Export module (orchestration, fetching, writing)
│   ├── __init__.py      # Main orchestration (export_all_metadata)
│   ├── fetch.py         # Data fetching functions (API calls)
│   ├── writers.py       # Database/CSV writer functions (export_*)
│   └── utils.py         # Export utilities (write_to_csv, execute_with_retry)
├── process/             # Data processing modules
│   ├── __init__.py      # Exports all process functions
│   ├── entities.py      # Entity processing (metrics, dashboards, visualizations)
│   ├── ldm.py           # LDM processing (datasets, columns)
│   ├── users.py         # User/group processing
│   └── fetching.py      # API fetching utilities
└── sql/                 # SQL scripts for post-export processing
    ├── post_export_config.yaml  # YAML configuration for all SQL operations
    ├── tables/          # Table creation scripts (metrics_relationships, etc.)
    ├── views/           # Analytical views (v_metrics_*, v_*_tags, etc.)
    ├── updates/         # Table modification scripts (duplicate detection)
    └── procedures/      # Parameterized views for API automation
```

### Data Flow

1. **Export Phase** (`export/` → `process/`)
   - **API mode**: Fetches from `analyticsModel` endpoint (parent) or entity APIs (children)
   - **Local mode**: Uses provided `layout_json` directly (no API calls)
   - All data is processed in **layout format** (flat structure with `obj["title"]`)
   - Child workspace entity data is transformed via `entity_to_layout()`
   - Stores in SQLite tables: metrics, visualizations, dashboards, ldm_*, etc.

2. **Post-Export Phase** (`post_export.py`)
   - Loads `sql/post_export_config.yaml`
   - Topologically sorts operations by dependencies
   - Executes tables → views → procedures → updates in order
   - Python populate functions run for tables needing regex (e.g., `metrics_relationships`)

### Key Tables

| Table | Description |
|-------|-------------|
| `metrics` | Metric definitions with MAQL formulas |
| `visualizations` | Visualization configurations |
| `dashboards` | Dashboard definitions |
| `metrics_relationships` | Direct metric-to-metric references (Python populates) |
| `metrics_ancestry` | Transitive metric ancestry (recursive CTE) |

### Key Views

| View | Purpose |
|------|---------|
| `v_metrics_relationships` | Direct metric references with titles |
| `v_metrics_relationships_ancestry` | Full ancestry with titles/tags |
| `v_metrics_relationships_root` | Root metrics (no outgoing dependencies) |
| `v_*_tags` | Unnested tags for each entity type |
| `v_*_usage` | Usage tracking views |

## Configuration

### Environment Variables

Create `.env.gdcloud`:
```env
BASE_URL=https://your-instance.gooddata.com
WORKSPACE_ID=your_workspace_id
BEARER_TOKEN=your_api_token
```

### Post-Export YAML Structure

`sql/post_export_config.yaml` defines:
- **tables**: Created tables (some with `python_populate` for Python processing)
- **views**: Read-only analytical views
- **procedures**: Parameterized views with `{{CONFIG_KEY}}` substitution
- **updates**: Table modifications with `required_columns`

Each entry has:
- `sql_file`: Path to SQL file
- `dependencies`: List of items that must run first
- `category`: Grouping (tagging/usage/deduplication/procedures)

## Common Patterns

### Adding a New View

1. Create SQL file in `sql/views/v_your_view.sql`
2. Add to `sql/post_export_config.yaml`:
```yaml
views:
  v_your_view:
    sql_file: views/v_your_view.sql
    description: What this view does
    category: usage
    dependencies: []  # or list dependencies
```

### Adding a Table with Python Processing

1. Create SQL file in `sql/tables/your_table.sql` (structure only)
2. Add Python function in `post_export.py`
3. Register in `PYTHON_POPULATE_FUNCTIONS` dict
4. Add to YAML with `python_populate: your_function_name`

### Dependency Management

- Dependencies are resolved via topological sort (Kahn's algorithm)
- Circular dependencies will raise `ValueError`
- Items without dependencies execute in alphabetical order

### Adding Tables with Separate API Calls

When adding a new table that requires data from a separate API endpoint (not included in `analyticsModel` or `layout.json`):

1. Add the table name to `LOCAL_MODE_STALE_TABLES` in `constants.py`
2. This ensures the table is truncated in local mode to prevent stale data

Tables currently in this list: `users`, `user_groups`, `user_group_members`, `plugins`

### Export Function Interface

All `export_*` functions in `export/writers.py` share the same signature for uniform orchestration:

```python
def export_something(all_workspace_data, export_dir, config, db_name):
```

This allows `export/__init__.py` to call them in a loop:

```python
for export_func in export_functions:
    export_func(all_workspace_data, export_dir, config, db_path)
```

**Important**: Some functions don't use all parameters (e.g., `export_dashboards_permissions` doesn't use `config`). Use underscore prefix (`_config`) for unused parameters and document in the docstring why it's kept. Don't remove unused parameters - it would break the uniform interface.

## Testing Changes

```bash
# Test imports work
python3 -c "from gooddata_export.post_export import load_post_export_config; print(load_post_export_config())"

# Run enrichment on existing DB to test SQL changes
make enrich

# Full export + enrich
make export-enrich
```

## Code Style

### Python Formatting (Ruff)

Python files must be formatted and linted with [Ruff](https://docs.astral.sh/ruff/) after changes:

```bash
make ruff-format
# or directly: ruff check --fix . && ruff format .
```

### Type Hints (Modern Syntax)

This project targets **Python 3.13+**. Use built-in generics and `|` union syntax - no `typing` imports needed:

```python
def process(items: list[str], config: dict[str, int] | None = None) -> set[str]: ...
def get_class() -> type[MyClass]: ...
def fetch(id: str | int) -> tuple[str, bool]: ...
```

**Only import from `typing`:** `Any`, `TypeVar`, `TYPE_CHECKING`, `Protocol`, `Literal`, `TypedDict`

### Avoiding Over-Engineering

Don't consolidate every repeated pattern. Small, simple duplications (2-3 lines appearing a few times) are often clearer than adding another abstraction layer. Consolidate when the pattern is complex (5+ lines), appears in many places (5+), or requires consistent behavior that might need updating.

### Logging (Use `logger` Instead of `print`)

Use Python's `logging` module instead of `print()` for all output:

```python
import logging

logger = logging.getLogger(__name__)

# Use logger methods instead of print()
logger.info("Processing workspace %s", workspace_id)    # Not: print(f"Processing workspace {workspace_id}")
logger.warning("Could not fetch data: %s", error)       # Not: print(f"Warning: Could not fetch data: {error}")
logger.debug("Debug info: %s", details)                 # For debug-only output
```

Benefits:
- Consistent output format across the codebase
- Log levels allow filtering (INFO, WARNING, DEBUG, ERROR)
- Easier to redirect output to files or external logging systems
- Use `%s` formatting (not f-strings) for lazy evaluation

### SQL Style

- SQL files use `DROP ... IF EXISTS` then `CREATE`
- SQL comments explain purpose at top of file
- **Table naming convention**: Use plural form for grouping
  - Main tables: `dashboards`, `metrics`, `visualizations`
  - Junction tables: `dashboards_visualizations`, `dashboards_metrics`, `dashboards_permissions`
- **View naming convention**: `v_{table_plural}_{suffix}` - views are grouped by table name
  - `v_dashboards_tags` (dashboards group)
  - `v_metrics_tags`, `v_metrics_usage`, `v_metrics_relationships` (metrics group)
  - `v_visualizations_tags`, `v_visualizations_usage` (visualizations group)
