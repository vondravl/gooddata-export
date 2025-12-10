"""
GoodData Export - Command Line Interface

This module provides the CLI entry point for the gooddata-export package.

Usage:
    # Using .env.gdcloud configuration file:
    gooddata-export export

    # Run only enrichment (post-processing) on existing database:
    gooddata-export enrich --db-path output/db/gooddata_export.db

    # With command-line arguments:
    gooddata-export export --workspace-id your_workspace --db-dir my_databases --csv-dir my_csvs

    # Export with format (sqlite/csv, default sqlite csv):
    gooddata-export export --format sqlite

    # With child workspaces (fetch only dashboards from children):
    gooddata-export export --include-child-workspaces --child-workspace-data-types dashboards

    # With debug mode:
    gooddata-export export --debug

Configuration:
    Create a .env.gdcloud file with:
        BASE_URL=https://your-instance.gooddata.com
        WORKSPACE_ID=your_workspace_id
        BEARER_TOKEN=your_api_token
        ENABLE_RICH_TEXT_EXTRACTION=true  # Optional
        INCLUDE_CHILD_WORKSPACES=false     # Optional
        MAX_WORKERS=5                      # Optional
        DEBUG=false                        # Optional
"""

import argparse
import sys
from pathlib import Path

from gooddata_export import export_metadata
from gooddata_export.config import ExportConfig
from gooddata_export.post_export import run_post_export_sql


def _create_parser():
    """Create and return the argument parser."""
    parser = argparse.ArgumentParser(
        description="Export GoodData workspace metadata to SQLite and/or CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic export using .env.gdcloud:
  gooddata-export export

  # Export only (skip enrichment):
  gooddata-export export --skip-post-export

  # Enrich existing database:
  gooddata-export enrich --db-path output/db/gooddata_export.db

  # Export only to SQLite (fastest):
  gooddata-export export --format sqlite

  # Export with child workspaces:
  gooddata-export export --include-child-workspaces --max-workers 10

  # Custom directories:
  gooddata-export export --db-dir exports/production/db --csv-dir exports/production/csv

  # Debug mode:
  gooddata-export export --debug
        """,
    )

    # Subcommands
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Export command (default behavior)
    export_parser = subparsers.add_parser(
        "export", help="Export metadata from GoodData"
    )
    enrich_parser = subparsers.add_parser(
        "enrich", help="Run post-export enrichment on existing database"
    )

    # Enrich-specific arguments
    enrich_parser.add_argument(
        "--db-path",
        type=str,
        help="Path to SQLite database to enrich (required for enrich command)",
    )
    enrich_parser.add_argument(
        "--debug", action="store_true", help="Enable debug logging"
    )

    # Add export arguments to export_parser
    _add_export_arguments(export_parser)

    return parser


def _add_export_arguments(parser):
    """Add export-specific arguments to parser."""
    # Connection arguments (override .env.gdcloud)
    parser.add_argument(
        "--base-url", type=str, help="GoodData API base URL (env: BASE_URL)"
    )
    parser.add_argument(
        "--workspace-id", type=str, help="Workspace ID to export (env: WORKSPACE_ID)"
    )
    parser.add_argument(
        "--bearer-token", type=str, help="API authentication token (env: BEARER_TOKEN)"
    )

    # Export configuration
    parser.add_argument(
        "--db-dir",
        type=str,
        default="output/db",
        help="Directory for SQLite database files (default: output/db)",
    )
    parser.add_argument(
        "--csv-dir",
        type=str,
        default="output/metadata_csv",
        help="Directory for CSV export files (default: output/metadata_csv)",
    )
    parser.add_argument(
        "--format",
        nargs="+",
        choices=["sqlite", "csv"],
        default=["sqlite", "csv"],
        help="Export format(s): sqlite, csv, or both (default: both)",
    )
    parser.add_argument(
        "--db-name",
        type=str,
        help="Custom SQLite database filename (default: gooddata_export.db in db-dir)",
    )

    # Child workspace options
    parser.add_argument(
        "--include-child-workspaces",
        action="store_true",
        help="Include child workspaces in export (env: INCLUDE_CHILD_WORKSPACES)",
    )
    parser.add_argument(
        "--child-workspace-data-types",
        nargs="+",
        choices=["metrics", "dashboards", "visualizations", "filter_contexts"],
        help="Data types to fetch from child workspaces - default: all (env: CHILD_WORKSPACE_DATA_TYPES)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Maximum parallel workers for child workspace processing - default: 5 (env: MAX_WORKERS)",
    )

    # Feature flags
    parser.add_argument(
        "--enable-rich-text-extraction",
        action="store_true",
        help="Enable extraction from rich text widgets (env: ENABLE_RICH_TEXT_EXTRACTION)",
    )
    parser.add_argument(
        "--skip-post-export",
        action="store_true",
        help="Skip post-export SQL processing (views, updates, procedures). Note: env uses ENABLE_POST_EXPORT=true/false",
    )
    parser.add_argument(
        "--debug", action="store_true", help="Enable debug logging (env: DEBUG)"
    )


def run_enrich_command(args):
    """Run enrichment (post-export processing) on existing database."""
    print("=" * 70)
    print("GoodData Database Enrichment")
    print("=" * 70)

    # Validate db_path
    if not args.db_path:
        print("\nError: --db-path is required for enrich command")
        print("\nExample: gooddata-export enrich --db-path output/db/gooddata_export.db")
        return 1

    if not Path(args.db_path).exists():
        print(f"\nError: Database not found: {args.db_path}")
        return 1

    print("\nConfiguration:")
    print(f"   Database: {args.db_path}")
    print(f"   Debug Mode: {'Enabled' if args.debug else 'Disabled'}")
    print()

    try:
        # Set up logging level
        if args.debug:
            import logging

            logging.basicConfig(level=logging.DEBUG)

        # Run post-export processing
        success = run_post_export_sql(args.db_path)

        if success:
            print("\n" + "=" * 70)
            print("Enrichment Completed Successfully!")
            print("=" * 70)
            print(f"\nDatabase enriched: {args.db_path}")
            print("   - Views created")
            print("   - Procedures executed")
            print("   - Table updates applied")
            print("\n" + "=" * 70)
            return 0
        else:
            print("\n" + "=" * 70)
            print("Enrichment Failed!")
            print("=" * 70)
            return 1

    except Exception as e:
        print("\n" + "=" * 70)
        print("Enrichment Failed!")
        print("=" * 70)
        print(f"\nError: {str(e)}")

        if args.debug:
            import traceback

            print("\nFull traceback:")
            traceback.print_exc()
        else:
            print("\nRun with --debug flag for detailed error information.")

        print("\n" + "=" * 70)
        return 1


def run_export_command(args):
    """Run export command."""
    print("=" * 70)
    print("GoodData Metadata Export")
    print("=" * 70)

    # If command-line args are not provided, try loading from .env.gdcloud
    loaded_config = None
    if not args.base_url and not args.workspace_id and not args.bearer_token:
        print("\nLoading configuration from .env.gdcloud file...")

        # Check if .env.gdcloud exists
        if not Path(".env.gdcloud").exists():
            print(
                "\nError: No .env.gdcloud file found and no command-line arguments provided."
            )
            print("\nPlease either:")
            print(
                "  1. Create a .env.gdcloud file with BASE_URL, WORKSPACE_ID, and BEARER_TOKEN"
            )
            print(
                "  2. Provide --base-url, --workspace-id, and --bearer-token arguments"
            )
            print("\nFor help: gooddata-export --help")
            return 1

        # Load config from env
        loaded_config = ExportConfig(load_from_env=True)

        # Use values from config
        base_url = loaded_config.BASE_URL
        workspace_id = loaded_config.WORKSPACE_ID
        bearer_token = loaded_config.BEARER_TOKEN
    else:
        # Use command-line arguments
        base_url = args.base_url
        workspace_id = args.workspace_id
        bearer_token = args.bearer_token

    # Validate required parameters
    if not base_url or not workspace_id or not bearer_token:
        print("\nError: Missing required configuration.")
        print("\nRequired parameters:")
        print("  - BASE_URL (or --base-url)")
        print("  - WORKSPACE_ID (or --workspace-id)")
        print("  - BEARER_TOKEN (or --bearer-token)")
        return 1

    # Determine effective settings (from .env or args)
    if loaded_config:
        # Use settings from loaded config, but allow CLI args to override
        include_child_workspaces = (
            args.include_child_workspaces
            if args.include_child_workspaces
            else loaded_config.INCLUDE_CHILD_WORKSPACES
        )
        child_workspace_data_types = (
            args.child_workspace_data_types
            if args.child_workspace_data_types
            else loaded_config.CHILD_WORKSPACE_DATA_TYPES
        )
        max_workers = (
            args.max_workers
            if args.max_workers is not None
            else loaded_config.MAX_PARALLEL_WORKSPACES
        )
        enable_rich_text_extraction = (
            args.enable_rich_text_extraction
            if args.enable_rich_text_extraction
            else loaded_config.ENABLE_RICH_TEXT_EXTRACTION
        )
        # Handle post-export: CLI --skip-post-export overrides .env ENABLE_POST_EXPORT
        enable_post_export = (
            not args.skip_post_export
            if args.skip_post_export
            else loaded_config.ENABLE_POST_EXPORT
        )
        debug = args.debug if args.debug else loaded_config.DEBUG_WORKSPACE_PROCESSING
    else:
        # Use CLI args with defaults
        include_child_workspaces = args.include_child_workspaces
        child_workspace_data_types = args.child_workspace_data_types
        max_workers = args.max_workers if args.max_workers is not None else 5
        enable_rich_text_extraction = args.enable_rich_text_extraction
        enable_post_export = not args.skip_post_export  # Invert the skip flag
        debug = args.debug

    # Display configuration
    print("\nConfiguration:")
    print(f"   Base URL: {base_url}")
    print(f"   Workspace ID: {workspace_id}")
    if "sqlite" in args.format:
        print(f"   Database Directory: {args.db_dir}")
    if "csv" in args.format:
        print(f"   CSV Directory: {args.csv_dir}")
    print(f"   Export Formats: {', '.join(args.format)}")
    print(f"   Include Child Workspaces: {'Yes' if include_child_workspaces else 'No'}")
    if include_child_workspaces:
        print(f"   Max Workers: {max_workers}")
        if child_workspace_data_types:
            print(f"   Child Data Types: {', '.join(child_workspace_data_types)}")
    print(
        f"   Rich Text Extraction: {'Enabled' if enable_rich_text_extraction else 'Disabled'}"
    )
    print(
        f"   Post-Export Processing: {'Disabled' if args.skip_post_export else 'Enabled'}"
    )
    print(f"   Debug Mode: {'Enabled' if debug else 'Disabled'}")
    print()

    try:
        # Set up database path
        if args.db_name:
            # Custom database path provided
            db_name_path = Path(args.db_name)
            db_path = (
                str(db_name_path)
                if db_name_path.is_absolute()
                else str(Path(args.db_dir) / args.db_name)
            )
        else:
            # Default database name
            db_path = str(Path(args.db_dir) / "gooddata_export.db")

        # Run the export
        result = export_metadata(
            base_url=base_url,
            workspace_id=workspace_id,
            bearer_token=bearer_token,
            csv_dir=args.csv_dir if "csv" in args.format else None,
            export_formats=args.format,
            include_child_workspaces=include_child_workspaces,
            child_workspace_data_types=child_workspace_data_types,
            max_parallel_workspaces=max_workers,
            enable_rich_text_extraction=enable_rich_text_extraction,
            run_post_export=enable_post_export,
            debug=debug,
            db_path=db_path,
        )

        # Display results
        print("\n" + "=" * 70)
        print("Export Completed Successfully!")
        print("=" * 70)
        print("\nResults:")
        print(f"   Workspaces Processed: {result['workspace_count']}")

        if "sqlite" in args.format:
            print(f"   SQLite Database: {result['db_path']}")
            if result.get("workspace_db_path"):
                print(f"   Workspace DB: {result['workspace_db_path']}")

        if "csv" in args.format and result.get("csv_dir"):
            print(f"   CSV Files Directory: {result['csv_dir']}")

        print("\n" + "=" * 70)
        return 0

    except Exception as e:
        print("\n" + "=" * 70)
        print("Export Failed!")
        print("=" * 70)
        print(f"\nError: {str(e)}")

        if args.debug:
            import traceback

            print("\nFull traceback:")
            traceback.print_exc()
        else:
            print("\nRun with --debug flag for detailed error information.")

        print("\n" + "=" * 70)
        return 1


def main(argv=None):
    """Main entry point for the CLI.

    Args:
        argv: Command line arguments (defaults to sys.argv[1:])
    """
    parser = _create_parser()
    args = parser.parse_args(argv)

    if args.command == "enrich":
        return run_enrich_command(args)
    elif args.command == "export":
        return run_export_command(args)
    else:
        parser.print_help()
        return 1
