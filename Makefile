.DEFAULT_GOAL := help

# Tool runner: override with `make run RUN=` when entry points are on PATH
RUN ?= uv run

help:
	@echo "GoodData Export - Available commands:"
	@echo ""
	@echo "Setup:"
	@echo "  make install      - Install package (required dependencies only)"
	@echo "  make dev          - Install with dev dependencies (ruff, pytest)"
	@echo "  make venv         - Create Python venv with pip (non-uv fallback)"
	@echo ""
	@echo "Export & Enrichment:"
	@echo "  make run          - Full export + enrichment (alias for export-enrich)"
	@echo "  make run-lite     - Export without content fields (smaller DB for browser)"
	@echo "  make run-children - Full export with child workspaces (skips enrichment)"
	@echo "  make export       - Export data only (skip post-processing)"
	@echo "  make enrich       - Run enrichment/procedures on existing database"
	@echo "  make export-enrich - Full export + enrichment"
	@echo ""
	@echo "Testing:"
	@echo "  make test         - Run tests with pytest"
	@echo "  make test-cov     - Run tests with coverage report"
	@echo ""
	@echo "Code Quality:"
	@echo "  make ruff-lint    - Check and auto-fix linting issues with ruff"
	@echo "  make ruff-format  - Format code with ruff"
	@echo ""
	@echo "Other:"
	@echo "  make clean        - Remove build artifacts and virtual environments"
	@echo ""
	@echo "Override RUN variable: make run RUN=  (uses entry points from activated venv)"

install:
	@if command -v uv >/dev/null 2>&1; then \
		echo "Installing with uv..."; \
		uv sync; \
	else \
		echo "Installing with pip..."; \
		pip install -e .; \
	fi

dev:
	@if command -v uv >/dev/null 2>&1; then \
		echo "Installing with uv (including dev dependencies)..."; \
		uv sync --extra dev; \
	else \
		echo "Installing with pip (including dev dependencies)..."; \
		pip install -e ".[dev]"; \
	fi

venv: pyproject.toml
	@if [ ! -d "venv" ]; then \
		python3 -m venv venv --upgrade-deps; \
		venv/bin/pip3 install -e ".[dev]"; \
		touch venv/bin/activate; \
	elif [ ! -f "venv/bin/activate" ]; then \
		rm -rf venv; \
		python3 -m venv venv --upgrade-deps; \
		venv/bin/pip3 install -e ".[dev]"; \
		touch venv/bin/activate; \
	fi

export:
	@echo "Running export only (skipping post-processing)..."
	$(RUN) gooddata-export export --skip-post-export

enrich:
	@if [ -z "$(DB)" ]; then \
		echo "Running enrichment on default database..."; \
		$(RUN) gooddata-export enrich --db-path output/db/gooddata_export.db; \
	else \
		echo "Running enrichment on $(DB)..."; \
		$(RUN) gooddata-export enrich --db-path $(DB); \
	fi

run: export-enrich

export-enrich:
	@echo "Running full export + enrichment workflow..."
	$(RUN) gooddata-export export

run-lite:
	@echo "Running export without content fields (smaller DB)..."
	INCLUDE_CONTENT=false $(RUN) gooddata-export export

run-children: export-children

export-children:
	@echo "Running export with child workspaces..."
	INCLUDE_CHILD_WORKSPACES=true $(RUN) gooddata-export export

ruff-lint:
	@echo "Checking Python with Ruff..."
	@$(RUN) ruff check . && $(RUN) ruff format --check --diff .

ruff-format:
	@echo "Formatting Python with Ruff..."
	@$(RUN) ruff check --fix . && $(RUN) ruff format .

test:
	@echo "Running tests..."
	@$(RUN) pytest tests/ -v

test-cov:
	@echo "Running tests with coverage..."
	@$(RUN) pytest tests/ --cov=gooddata_export --cov-report=term-missing

clean:
	rm -rf venv .venv build/ dist/ *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

.PHONY: help install dev venv export enrich run run-lite export-enrich run-children export-children ruff-lint ruff-format test test-cov clean
