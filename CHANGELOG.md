# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2025-12-16

### Added
- `make run` alias for export-enrich workflow
- CLAUDE.md: Python 3.13+ type hints guidelines
- CLAUDE.md: Ruff formatting requirements
- CLAUDE.md: Versioning reminder for package releases

### Changed
- Use `astral-sh/ruff-action@v3` in GitHub workflow instead of custom script
- Simplify Makefile ruff targets to use ruff directly
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
