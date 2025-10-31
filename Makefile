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
	@echo "Other:"
	@echo "  make run          - Run the export script (legacy, use export-enrich)"
	@echo "  make clean        - Remove build artifacts and virtual environment"

venv: setup.py
	@if [ ! -d "venv" ]; then \
		python3 -m venv venv --upgrade-deps; \
		venv/bin/pip3 install -e .; \
		touch venv/bin/activate; \
	elif [ ! -f "venv/bin/activate" ]; then \
		rm -rf venv; \
		python3 -m venv venv --upgrade-deps; \
		venv/bin/pip3 install -e .; \
		touch venv/bin/activate; \
	fi

dev: venv

install:
	pip install -e .

run:
	@if [ ! -d "venv" ]; then \
		echo "Virtual environment not found. Run 'make venv' first."; \
		exit 1; \
	fi
	@echo "⚠️  Note: 'make run' is legacy. Use 'make export-enrich' instead."
	venv/bin/python main.py export

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

clean:
	rm -rf venv build/ dist/ *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

.PHONY: help venv dev install run export enrich export-enrich clean

