# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.7.0] - 2026-02-05

### Changed
- **`is_valid` defaults to `NULL` in local mode**: Metrics, visualizations, and dashboards now default to `NULL` when `areRelationsValid` field is absent (local layout files). Previously defaulted to `True` which was misleading.
  - API mode: retains original `is_valid` value from the API
  - Local mode: `NULL` (unknown) - computed in post-export for metrics and visualizations; dashboards remain `NULL`
- **BREAKING: `visualizations_references` replaces `visualizations_metrics` and `visualizations_attributes`**: Consolidated into single table with two-dimensional classification
  - `object_type`: 'metric', 'fact', 'attribute', or 'label' (WHAT is being referenced)
  - `source`: 'measure', 'attribute', or 'filter' (WHERE in the visualization it's used)
  - Columns: `visualization_id`, `referenced_id`, `workspace_id`, `object_type`, `source`, `label`
  - Enables answering "Is this attribute used as a filter or dimension?" queries
  - Extracts filter references from `negativeAttributeFilter` and `positiveAttributeFilter`
  - CSV file renamed to `gooddata_visualizations_references.csv`
- **BREAKING: `metrics_references` replaces `metrics_relationships` and `metrics_ldm_references`**: Consolidated into single table with `reference_type` column
  - `reference_type`: 'metric', 'attribute', 'label', or 'fact'
  - Columns: `source_metric_id`, `source_workspace_id`, `referenced_id`, `reference_type`
  - Follows the same pattern as `visualizations_references` for consistency
  - Views `v_metrics_relationships` and `v_metrics_relationships_root` now filter by `reference_type = 'metric'`
  - `metrics_ancestry` CTE unchanged (still metric-to-metric only, as transitive closure only applies to metrics)
  - **Note**: `{label/id}` in MAQL references the attribute's default label, which shares the same ID as the attribute in `ldm_columns`. Label references are validated against `ldm_columns` (type='attribute'), not `ldm_labels`.

### Added
- **`{label/...}` extraction from MAQL**: Now extracts label references in addition to metric, attribute, and fact references
  - Label references are stored with `reference_type='label'` in `metrics_references`
  - Validated against `ldm_columns` (type='attribute') since `{label/id}` uses the attribute's ID
- **Date instance support in LDM export**: Now processes `ldm.dateInstances` in addition to `ldm.datasets`
  - Date instances (e.g., `process_date`, `FirstDayQuarter`) are stored as datasets in `ldm_datasets`
  - Date granularities (e.g., `process_date.day`, `process_date.month`) are stored as attributes in `ldm_columns`
  - Enables proper validation of `{label/date.granularity}` references in MAQL
- **`metrics_is_valid` post-export update**: Computes `is_valid` for metrics in local mode based on reference analysis:
  - Invalid (0): Metric references another metric, attribute, label, or fact that doesn't exist
  - Valid (1): All referenced objects exist, or metric has no references
  - Checks `metrics_references` with appropriate `reference_type` filters
- **`visualizations_is_valid` post-export update**: Computes `is_valid` for visualizations in local mode:
  - Invalid (0): Visualization references a metric/fact/label that doesn't exist
  - Valid (1): All referenced objects exist, or visualization has no references
  - Checks via `visualizations_references`: metrics → `metrics`, facts → `ldm_columns`, labels → `ldm_labels`

## [1.6.2] - 2026-02-04

### Changed
- **Optimized child workspace fetching**: Always uses layout API (`analyticsModel`) for ~2.7x faster performance
  - Benchmarks: 64 child workspaces in 8.4s (layout API) vs 22.5s (entity API)
  - Faster despite fewer parallel API calls (5 vs up to 25) - single call returns all data
  - Returns native objects only (no inherited duplicates from parent)
  - Simpler code path (removed conditional API selection logic)
- **Simplified retry logic using `urllib3.Retry`**: Replaced ~40 lines of manual retry/backoff code in `fetch_data()` with built-in retry handling via `HTTPAdapter`
  - Automatic retries on rate limiting (429) and server errors (5xx) with exponential backoff
  - Honors `Retry-After` header from server for optimal backoff timing
  - Configurable via `create_api_session(max_retries=3, backoff_factor=1.0)`
- **Shared session for child workspace fetching**: All parallel child workspace fetches now share a single `requests.Session`
  - TCP/TLS connection reuse across parallel workers (session pool sized to match worker count)
  - Eliminates per-workspace session creation/teardown overhead

### Added
- **`create_api_session()` function**: Creates `requests.Session` with connection pooling and automatic retries
  - Connection pooling via `HTTPAdapter` for TCP connection reuse
  - Built-in retry strategy for transient failures
- **`session` parameter for fetch functions**: `fetch_child_workspaces`, `fetch_ldm`, etc. now accept optional session parameter for connection reuse
- **Consistent retry handling**: Both parent and child workspace API calls now use session with automatic retries

### Removed
- **`fetch_data()` function**: No longer used - child workspaces now use layout API exclusively
- **`entity_to_layout()` and `transform_entities_to_layout()` functions**: No longer needed without entity API usage

## [1.6.1] - 2026-01-30

### Changed
- **Cleaner CLI output**: Moved verbose export details to DEBUG level for concise default output
  - Default output shows only essential progress (timing, child workspace progress, final summary)
  - Use `--debug` flag to see detailed export logs, API calls, and item counts
- **Removed `DEBUG_WORKSPACE_PROCESSING` config option**: Replaced with standard `--debug` flag that controls Python logging level
- **Consistent section formatting**: Standardized phase headers (FETCH PHASE, EXPORT PHASE, POST-EXPORT PROCESSING) and 70-character delimiters in debug output
- **API FETCH PHASE SUMMARY**: Now only displayed when processing child workspaces (multi-workspace mode)
- **`run_post_export_sql()` now raises `ExportError` on failure** instead of returning `bool`. This is the standard Pythonic pattern for error handling. Callers should use try/except instead of checking return values.
- **Child workspace data types default changed to "dashboards"**: Previously defaulted to all types in non-interactive mode and none in interactive mode; now defaults to dashboards only in both modes

### Added
- **`configure_logging()` in public API**: Exported from `gooddata_export` for programmatic users who want debug output
- **`default_selected` parameter for `prompt_checkbox_selection()`**: Allows pre-selecting specific options instead of all-or-none

### Removed
- `DEBUG_WORKSPACE_PROCESSING` configuration option from `ExportConfig` and `.env.gdcloud` (use `--debug` CLI flag instead)
- `debug` parameter from `export_metadata()` - use `configure_logging(debug=True)` before calling instead
- Migration note from `.env.gdcloud.example` (legacy variable names no longer mentioned)

## [1.6.0] - 2026-01-30

### Added
- **Reusable CLI prompts module** (`gooddata_export.cli.prompts`): Importable prompt functions for interactive CLI workflows
  - `is_interactive()`: Check if running in interactive terminal (TTY) without CI environment
  - `prompt_checkbox_selection()`: Multi-select checkbox prompt with toggle support (numbers, 'a'=all, 'n'=none)
  - `prompt_yes_no()`: Simple yes/no confirmation prompt
  - All prompts gracefully fall back to defaults in non-interactive/CI environments
  - CI detection for: `CI`, `GITHUB_ACTIONS`, `GITLAB_CI`, `JENKINS_URL`, `CIRCLECI`, `TRAVIS`, `BUILDKITE`, `TEAMCITY_VERSION`, `TF_BUILD`
- **Interactive child workspace data type selection**: When using `--include-child-workspaces` without `--child-workspace-data-types`, an interactive prompt allows selecting which data types to fetch (default: none selected; selecting nothing skips child workspace fetching)
- **`CHILD_WORKSPACE_DATA_TYPES` constant**: Tuple of valid child workspace data types (`dashboards`, `visualizations`, `metrics`, `filter_contexts`)
- **Parent-only enrichment in multi-workspace mode**: Post-export SQL updates now filter to parent workspace only when child workspaces are included
  - Prevents confusing duplicate detection results across workspace hierarchies
  - SQL updates use `{parent_workspace_filter}` placeholder for workspace-scoped operations
- **`dashboards_visualizations` table**: New columns for widget tracking
  - `widget_local_identifier`: The widget's own `localIdentifier` from the dashboard layout
  - `widget_type`: The type of widget containing the visualization: `insight`, `visualizationSwitcher`, or `richText`
  - `switcher_local_identifier`: The parent switcher's `localIdentifier` (NULL for non-switcher widgets). Use this for GROUP BY to find all visualizations in a switcher.
- **`v_dashboards_visualizations` view**: Added `widget_local_identifier`, `widget_type`, and `switcher_local_identifier` columns
- **`ldm_labels` table**: Captures attribute labels (display forms) from the Logical Data Model
  - Labels contain display representations of attributes with their own metadata
  - Columns: `dataset_id`, `attribute_id`, `id`, `title`, `description`, `source_column`, `source_column_data_type`, `value_type`, `tags`, `is_default`, `workspace_id`
  - `is_default` indicates which label is the default view for the attribute
- **`v_ldm_labels` view**: Labels with dataset context (dataset_id, dataset_name, attribute_title)
- **`v_ldm_labels_tags` view**: Unnests LDM label tags into individual rows for tag analysis
- **Foreign key constraints**: Added FK definitions to all junction/child tables for better tooling support (DBeaver ER diagrams). FKs are schema metadata only (not enforced by default in SQLite).
  - `ldm_columns` → `ldm_datasets`
  - `ldm_labels` → `ldm_columns`
  - `visualizations_metrics` → `visualizations`, `metrics`
  - `visualizations_attributes` → `visualizations`
  - `dashboards` → `filter_contexts`
  - `dashboards_visualizations` → `dashboards`, `visualizations`
  - `dashboards_plugins` → `dashboards`, `plugins`
  - `dashboards_widget_filters` → `dashboards`
  - `dashboards_metrics` → `dashboards`, `metrics`
  - `dashboards_permissions` → `dashboards`
  - `filter_context_fields` → `filter_contexts`
  - `user_group_members` → `users`, `user_groups`
  - `metrics_relationships` → `metrics` (both source and referenced)
  - `metrics_ancestry` → `metrics` (both metric and ancestor)

### Changed
- **CLI package structure**: Converted `cli.py` to `cli/` package for better organization
  - `cli/__init__.py`: Re-exports public API (`main`, `is_interactive`, `prompt_checkbox_selection`, `prompt_yes_no`)
  - `cli/main.py`: CLI entry point and command handlers
  - `cli/prompts.py`: Reusable prompt functions
- **`ldm_datasets`**: Changed PRIMARY KEY from `id` to `(id, workspace_id)` for consistency
- **`ldm_columns`**: Added PRIMARY KEY `(dataset_id, id, workspace_id)` - includes `dataset_id` because column IDs are only unique within their parent dataset
- **`v_ldm_columns_tags.sql`**: Added `DROP VIEW IF EXISTS` for consistency with other views

### Removed
- **`truncate_tables_for_local_mode()` function**: Removed automatic truncation of "stale" tables in local mode
  - Tables (users, user_groups, user_group_members, plugins) now use DROP/CREATE pattern like all other tables
  - No special handling needed - each export creates a fresh database
- **`LOCAL_MODE_STALE_TABLES` constant**: Removed from `constants.py` as it's no longer needed

## [1.5.1] - 2026-01-22

### Changed
- **`v_dashboards_widget_filters` view**: Added `widget_title` column from `dashboards_visualizations` table
  - Shows the overridden title (if set) alongside the original `visualization_title`
  - Useful for comparing widget filter configurations between environments

## [1.5.0] - 2026-01-22

### Added
- **`dashboards_widget_filters` table**: Extract widget-level filter configuration from dashboards
  - `ignoreDashboardFilters`: Which dashboard filters each widget ignores (attribute/date filters)
  - `dateDataSet`: Date dataset override for each widget
  - Supports both regular insight widgets and visualizationSwitcher inner widgets
- **`v_dashboards_widget_filters` view**: Readable filter details with dashboard/visualization titles
- **`dashboards_visualizations` table**: New columns for widget overrides
  - `widget_title`: Overridden title when insight is placed on dashboard
  - `widget_description`: Overridden description when insight is placed on dashboard
- **`v_dashboards_visualizations` view**: New flags for quick identification
  - `has_title_override`: 1 if widget title differs from original visualization title
  - `has_description_override`: 1 if widget description differs from original
  - `has_ignored_filters`: 1 if widget ignores any dashboard filters

## [1.4.3] - 2026-01-14

### Changed
- **`v_visualizations_usage` view**: Changed from INNER JOINs to LEFT JOINs to include all visualizations
  - Unused visualizations now appear with NULL dashboard columns
  - Added `is_used` column from base table for convenient filtering
- **`v_metrics_usage` view**: Changed from INNER JOINs to LEFT JOINs in visualization usage part
  - Unused metrics now appear with NULL visualization/dashboard columns
  - Added `is_used_insight` and `is_used_maql` columns from base table
- **`v_filter_contexts_usage` view**: Replaced computed `is_unused` with `is_used` from base table for consistency

## [1.4.2] - 2026-01-12

### Fixed
- **Unconditional VACUUM**: Run `VACUUM` at end of every export to reclaim disk space
  - Previously only ran when stale tables were truncated in local mode
  - Now runs regardless of export mode, content exclusion, or database state
  - Ensures ~50% size reduction when using `--no-content` flag

## [1.4.1] - 2026-01-12

### Fixed
- **Local mode VACUUM**: Added `VACUUM` after truncating stale tables to reclaim disk space and ensure database file size accurately reflects actual content

## [1.4.0] - 2026-01-12

### Added
- **`--no-content` CLI flag**: Exclude full JSON content fields from database to reduce size by ~50%
  - Affects 7 tables: `dashboards`, `visualizations`, `metrics`, `filter_contexts`, `plugins`, `users`, `user_groups`
  - Content columns remain in schema (with NULL values) for compatibility
  - Environment variable: `INCLUDE_CONTENT=false`
  - API parameter: `include_content=False` in `export_metadata()`
  - Default behavior unchanged (content included)

### Changed
- **`visuals_with_same_content.sql`**: Rewritten to use junction tables (`visualizations_metrics`, `visualizations_attributes`) instead of parsing JSON from content field
  - Visualization deduplication now works regardless of whether `--no-content` is used
  - More efficient (no JSON parsing at query time)

## [1.3.0] - 2026-01-09

### Added
- **Local layout JSON support**: New `layout_json` parameter in `export_metadata()` allows processing local JSON-based layout data instead of fetching from GoodData API
  - Enables tagging workflows on feature branches before changes are deployed
  - When `layout_json` is provided, `bearer_token` becomes optional (no API calls made)
  - Expected format: `{"analytics": {"metrics": [...], ...}, "ldm": {"datasets": [...], ...}}`
- **Dashboard tabs support**: Tabbed dashboards (`content.tabs[]`) are now properly processed
  - New `tab_id` column in `dashboards_visualizations` table (NULL for legacy non-tabbed dashboards)
  - Visualizations from all tabs are extracted with their tab identifier (`localIdentifier`)
  - View `v_dashboards_visualizations` updated to include `tab_id` and `from_rich_text` columns
  - Backward compatible: legacy dashboards (`content.layout.sections`) continue to work
- New `entity_to_layout()` and `transform_entities_to_layout()` functions for converting entity API format to layout format
- New `constants.py` module with `LOCAL_MODE_STALE_TABLES` for centralized configuration
- Automatic truncation of stale tables (`users`, `user_groups`, `user_group_members`, `plugins`) when using `layout_json` mode to prevent confusion from previous API exports
- New `export_mode` field in `dictionary_metadata` table: `"api"` or `"local"` to indicate data source

### Changed
- Parent workspace now fetches analytics data from `analyticsModel` endpoint (single API call) instead of multiple entity API calls
  - More efficient: one call vs five separate calls
  - Returns data in layout format (same as local layout.json files)
- Child workspace data is transformed from entity format to layout format for uniform processing
- All `process_*` functions now accept layout format (flat structure with `obj["title"]` instead of `obj["attributes"]["title"]`)
- Process functions now handle missing fields gracefully with defaults for `createdAt`, `modifiedAt`, `areRelationsValid`, `isHidden`, `originType`
- **Refactored `export.py`** (2173 lines) into modular `export/` package for better maintainability:
  - `export/__init__.py`: Main orchestration (`export_all_metadata`)
  - `export/fetch.py`: API data fetching functions
  - `export/writers.py`: Database/CSV writing functions
  - `export/utils.py`: Export utilities (write_to_csv, execute_with_retry)

## [1.2.1] - 2026-01-07

### Added
- Fail-fast workspace validation at export start via `validate_workspace_exists()`
- Retry logic for 5xx server errors in `fetch_data()` (uses existing exponential backoff)

### Changed
- Centralized API error handling into `raise_for_api_error()` in `common.py`
- Centralized connection error handling into `raise_for_connection_error()` in `common.py`
- Unified request exception handling via `raise_for_request_error()` (dispatches to appropriate handler based on exception type)
- Refactored `get_api_client()` to use keyword-only arguments and handle client passthrough
- Consistent 200-char truncation for all API error response messages

## [1.2.0] - 2026-01-07

### Added
- New `plugins` table: exports dashboard plugin definitions from `/api/v1/entities/workspaces/:workspaceId/dashboardPlugins`
- New `dashboards_plugins` junction table: links dashboards to their plugins (extracted from dashboard content)
- New view `v_dashboards_plugins`: shows dashboard-plugin relationships with plugin details
- Test suite with pytest (49 tests)
  - SQL/YAML configuration validation tests
  - Topological sort and parameter substitution tests
  - ExportConfig tests with boolean parsing
  - `sort_tags()` utility tests
- `make test` and `make test-cov` targets
- pytest configuration in `pyproject.toml`

## [1.1.2] - 2026-01-06

### Added
- New view `v_dashboards_visualizations`: shows dashboard-visualization relationships with tags

## [1.1.1] - 2025-12-17

### Changed
- Renamed visualization junction tables for consistency with dashboards pattern:
  - `visualizations_metrics` (was `visualization_metrics`)
  - `visualizations_attributes` (was `visualization_attributes`)
  - CSV files renamed accordingly (`gooddata_visualizations_metrics.csv`, `gooddata_visualizations_attributes.csv`)

## [1.1.0] - 2025-12-16

### Added
- `make run` alias for export-enrich workflow
- CLAUDE.md: Python 3.13+ type hints guidelines
- CLAUDE.md: Ruff formatting requirements
- CLAUDE.md: Versioning reminder for package releases

### Changed
- **Breaking**: Minimum Python version raised from 3.11 to 3.13
- Use `astral-sh/ruff-action@v3` in GitHub workflow instead of custom script
- Makefile ruff targets now use venv for consistency with other targets
- Update config.py to use modern Python 3.13+ type hints (`list[str] | None` instead of `Optional[List[str]]`)
- Renamed junction tables for consistency: `dashboard_*` → `dashboards_*`
  - `dashboards_visualizations` (was `dashboard_visualizations`)
  - `dashboards_metrics` (was `dashboard_metrics`)
  - `dashboards_permissions` (was `dashboard_permissions`)

### Removed
- `formatting_ruff.py` - replaced by direct ruff commands

### Fixed
- Dashboard permissions primary key now includes `permission_name`, allowing multiple permission levels (VIEW, EDIT) per assignee on the same dashboard

## [1.0.0] - 2025-12-15

### Added
- Initial release
- Export GoodData workspace metadata to SQLite and CSV
- Support for metrics, dashboards, visualizations, LDM, filter contexts
- Dashboard permissions export from analytics model
- Users and user groups export
- Post-export SQL processing with topological sort
- Child workspace support with parallel fetching
