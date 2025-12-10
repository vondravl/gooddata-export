#!/usr/bin/env python3
"""
GoodData Export - Command Line Interface

This script provides backward compatibility.
For the installed package, use: gooddata-export

Usage:
    python main.py export
    python main.py enrich --db-path output/db/gooddata_export.db

For help: python main.py --help
"""

import sys

from gooddata_export.cli import main

if __name__ == "__main__":
    sys.exit(main())
