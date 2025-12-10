.DEFAULT_GOAL := help

help:
	@echo "GoodData Export - Available commands:"
	@echo ""
	@echo "Setup:"
	@echo "  make venv         - Create/update Python virtual environment"
	@echo "  make dev          - Set up development environment (alias for venv)"
	@echo "  make install      - Install package in development mode (current env)"
	@echo ""
	@echo "Export & Enrichment:"
	@echo "  make export       - Export data only (skip post-processing)"
	@echo "  make enrich       - Run enrichment/procedures on existing database"
	@echo "  make export-enrich- Full export + enrichment (default workflow)"
	@echo ""
	@echo "Code Quality:"
	@echo "  make ruff-lint    - Check and auto-fix linting issues with ruff"
	@echo "  make ruff-format  - Format code with ruff"
	@echo ""
	@echo "Other:"
	@echo "  make clean        - Remove build artifacts and virtual environment"

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

dev: venv

install:
	pip install -e .

export:
	@if [ ! -d "venv" ]; then \
		echo "Virtual environment not found. Run 'make venv' first."; \
		exit 1; \
	fi
	@echo "📤 Running export only (skipping post-processing)..."
	venv/bin/python main.py export --skip-post-export

enrich:
	@if [ ! -d "venv" ]; then \
		echo "Virtual environment not found. Run 'make venv' first."; \
		exit 1; \
	fi
	@if [ -z "$(DB)" ]; then \
		echo "📊 Running enrichment on default database..."; \
		venv/bin/python main.py enrich --db-path output/db/gooddata_export.db; \
	else \
		echo "📊 Running enrichment on $(DB)..."; \
		venv/bin/python main.py enrich --db-path $(DB); \
	fi

export-enrich:
	@if [ ! -d "venv" ]; then \
		echo "Virtual environment not found. Run 'make venv' first."; \
		exit 1; \
	fi
	@echo "📤📊 Running full export + enrichment workflow..."
	venv/bin/python main.py export

ruff-lint:
	@if [ ! -d "venv" ]; then \
		echo "Virtual environment not found. Run 'make venv' first."; \
		exit 1; \
	fi
	venv/bin/ruff check --fix .

ruff-format:
	@if [ ! -d "venv" ]; then \
		echo "Virtual environment not found. Run 'make venv' first."; \
		exit 1; \
	fi
	venv/bin/python formatting_ruff.py

clean:
	rm -rf venv build/ dist/ *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

.PHONY: help venv dev install export enrich export-enrich ruff-lint ruff-format clean

