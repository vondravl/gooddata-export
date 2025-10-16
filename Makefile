.DEFAULT_GOAL := help

help:
	@echo "GoodData Export - Available commands:"
	@echo "  make            - Show this help message"
	@echo "  make venv       - Create/update Python virtual environment"
	@echo "  make dev        - Set up development environment (alias for venv)"
	@echo "  make install    - Install package in development mode (current env)"
	@echo "  make clean      - Remove build artifacts and virtual environment"

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

clean:
	rm -rf venv build/ dist/ *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

.PHONY: help venv dev install clean

