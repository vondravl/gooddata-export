#!/usr/bin/env python3
"""
GoodData Export - Development CLI wrapper.

Convenience script for running the CLI without installing the package.
For production use, install the package and use: gooddata-export

Usage:
    python main.py export
    python main.py enrich --db-path output/db/gooddata_export.db
"""

import sys

from gooddata_export.cli import main

if __name__ == "__main__":
    sys.exit(main())
