# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
