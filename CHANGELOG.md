# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
