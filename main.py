"""
GoodData Export - Command Line Interface

Run this script to export GoodData metadata to SQLite and/or CSV files.

Usage:
    # Using .env.gdcloud configuration file:
    python main.py export

    # Run only enrichment (post-processing) on existing database:
    python main.py enrich --db-path output/db/gooddata_export.db

    # With command-line arguments:
    python main.py export --workspace-id your_workspace --output-dir my_output

    # Export only SQLite (fastest):
    python main.py export --format sqlite

    # Export only CSV:
    python main.py export --format csv

    # Export both (default):
    python main.py export --format sqlite csv

    # With child workspaces:
    python main.py export --include-children --max-workers 10

    # With debug mode:
    python main.py export --debug

Configuration:
    Create a .env.gdcloud file with:
        BASE_URL=https://your-instance.gooddata.com
        WORKSPACE_ID=your_workspace_id
        BEARER_TOKEN=your_api_token
        ENABLE_RICH_TEXT_EXTRACTION=true  # Optional
        INCLUDE_CHILD_WORKSPACES=false     # Optional
        MAX_PARALLEL_WORKSPACES=5          # Optional
"""

import argparse
import sys
import os
from gooddata_export import export_metadata
from gooddata_export.config import ExportConfig
from gooddata_export.post_export import run_post_export_sql


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Export GoodData workspace metadata to SQLite and/or CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic export using .env.gdcloud:
  python main.py export

  # Export only (skip enrichment):
  python main.py export --skip-post-export

  # Enrich existing database:
  python main.py enrich --db-path output/db/gooddata_export.db

  # Export only to SQLite (fastest):
  python main.py export --format sqlite

  # Export with child workspaces:
  python main.py export --include-children --max-workers 10

  # Custom output directory:
  python main.py export --output-dir exports/production

  # Debug mode:
  python main.py export --debug
        """
    )
    
    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Export command (default behavior)
    export_parser = subparsers.add_parser('export', help='Export metadata from GoodData')
    enrich_parser = subparsers.add_parser('enrich', help='Run post-export enrichment on existing database')
    
    # Enrich-specific arguments
    enrich_parser.add_argument(
        '--db-path',
        type=str,
        help='Path to SQLite database to enrich (required for enrich command)'
    )
    enrich_parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    # Add export arguments to export_parser
    _add_export_arguments(export_parser)
    
    return parser.parse_args()


def _add_export_arguments(parser):

    """Add export-specific arguments to parser."""
    # Connection arguments (override .env.gdcloud)
    parser.add_argument(
        "--base-url",
        type=str,
        help="GoodData API base URL (overrides .env.gdcloud)"
    )
    parser.add_argument(
        "--workspace-id",
        type=str,
        help="Workspace ID to export (overrides .env.gdcloud)"
    )
    parser.add_argument(
        "--bearer-token",
        type=str,
        help="API authentication token (overrides .env.gdcloud)"
    )

    # Export configuration
    parser.add_argument(
        "--db-dir",
        type=str,
        default="output/db",
        help="Directory for SQLite database files (default: output/db)"
    )
    parser.add_argument(
        "--csv-dir",
        type=str,
        default="output/metadata_csv",
        help="Directory for CSV export files (default: output/metadata_csv)"
    )
    parser.add_argument(
        "--format",
        nargs="+",
        choices=["sqlite", "csv"],
        default=["sqlite", "csv"],
        help="Export format(s): sqlite, csv, or both (default: both)"
    )
    parser.add_argument(
        "--db-name",
        type=str,
        help="Custom SQLite database filename (default: gooddata_export.db in db-dir)"
    )

    # Child workspace options
    parser.add_argument(
        "--include-children",
        action="store_true",
        help="Include child workspaces in export"
    )
    parser.add_argument(
        "--child-data-types",
        nargs="+",
        choices=["metrics", "dashboards", "visualizations", "filter_contexts"],
        help="Data types to fetch from child workspaces (default: all)"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=5,
        help="Maximum parallel workers for child workspace processing (default: 5)"
    )

    # Feature flags
    parser.add_argument(
        "--enable-rich-text",
        action="store_true",
        help="Enable extraction from rich text widgets"
    )
    parser.add_argument(
        "--skip-post-export",
        action="store_true",
        help="Skip post-export SQL processing (duplicate detection)"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )


def run_enrich_command(args):
    """Run enrichment (post-export processing) on existing database."""
    print("=" * 70)
    print("GoodData Database Enrichment")
    print("=" * 70)
    
    # Validate db_path
    if not args.db_path:
        print("\n❌ Error: --db-path is required for enrich command")
        print("\nExample: python main.py enrich --db-path output/db/gooddata_export.db")
        return 1
    
    if not os.path.exists(args.db_path):
        print(f"\n❌ Error: Database not found: {args.db_path}")
        return 1
    
    print(f"\n📋 Configuration:")
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
            print("✅ Enrichment Completed Successfully!")
            print("=" * 70)
            print(f"\n📊 Database enriched: {args.db_path}")
            print("   - Views created")
            print("   - Procedures executed")
            print("   - Table updates applied")
            print("\n" + "=" * 70)
            return 0
        else:
            print("\n" + "=" * 70)
            print("❌ Enrichment Failed!")
            print("=" * 70)
            return 1
            
    except Exception as e:
        print("\n" + "=" * 70)
        print("❌ Enrichment Failed!")
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
        print("\nℹ️  Loading configuration from .env.gdcloud file...")
        
        # Check if .env.gdcloud exists
        if not os.path.exists(".env.gdcloud"):
            print("\n❌ Error: No .env.gdcloud file found and no command-line arguments provided.")
            print("\nPlease either:")
            print("  1. Create a .env.gdcloud file with BASE_URL, WORKSPACE_ID, and BEARER_TOKEN")
            print("  2. Provide --base-url, --workspace-id, and --bearer-token arguments")
            print("\nFor help: python main.py --help")
            sys.exit(1)
        
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
        print("\n❌ Error: Missing required configuration.")
        print("\nRequired parameters:")
        print("  - BASE_URL (or --base-url)")
        print("  - WORKSPACE_ID (or --workspace-id)")
        print("  - BEARER_TOKEN (or --bearer-token)")
        sys.exit(1)

    # Determine effective settings (from .env or args)
    if loaded_config:
        # Use settings from loaded config, but allow CLI args to override
        include_children = loaded_config.INCLUDE_CHILD_WORKSPACES if not args.include_children else args.include_children
        child_data_types = loaded_config.CHILD_WORKSPACE_DATA_TYPES if not args.child_data_types else args.child_data_types
        max_workers = loaded_config.MAX_PARALLEL_WORKSPACES
        enable_rich_text = loaded_config.ENABLE_RICH_TEXT_EXTRACTION if not args.enable_rich_text else args.enable_rich_text
        debug = loaded_config.DEBUG_WORKSPACE_PROCESSING if not args.debug else args.debug
    else:
        # Use CLI args
        include_children = args.include_children
        child_data_types = args.child_data_types
        max_workers = args.max_workers
        enable_rich_text = args.enable_rich_text
        debug = args.debug

    # Display configuration
    print(f"\n📋 Configuration:")
    print(f"   Base URL: {base_url}")
    print(f"   Workspace ID: {workspace_id}")
    if "sqlite" in args.format:
        print(f"   Database Directory: {args.db_dir}")
    if "csv" in args.format:
        print(f"   CSV Directory: {args.csv_dir}")
    print(f"   Export Formats: {', '.join(args.format)}")
    print(f"   Include Child Workspaces: {'Yes' if include_children else 'No'}")
    if include_children:
        print(f"   Max Parallel Workers: {max_workers}")
        if child_data_types:
            print(f"   Child Data Types: {', '.join(child_data_types)}")
    print(f"   Rich Text Extraction: {'Enabled' if enable_rich_text else 'Disabled'}")
    print(f"   Post-Export Processing: {'Disabled' if args.skip_post_export else 'Enabled'}")
    print(f"   Debug Mode: {'Enabled' if debug else 'Disabled'}")
    print()

    try:
        # Set up database path
        if args.db_name:
            # Custom database path provided
            db_path = args.db_name if os.path.isabs(args.db_name) else os.path.join(args.db_dir, args.db_name)
        else:
            # Default database name
            db_path = os.path.join(args.db_dir, "gooddata_export.db")
        
        # Run the export
        result = export_metadata(
            base_url=base_url,
            workspace_id=workspace_id,
            bearer_token=bearer_token,
            csv_dir=args.csv_dir if "csv" in args.format else None,
            export_formats=args.format,
            include_child_workspaces=include_children,
            child_workspace_data_types=child_data_types,
            max_parallel_workspaces=max_workers,
            enable_rich_text_extraction=enable_rich_text,
            run_post_export=not args.skip_post_export,
            debug=debug,
            db_path=db_path
        )

        # Display results
        print("\n" + "=" * 70)
        print("✅ Export Completed Successfully!")
        print("=" * 70)
        print(f"\n📊 Results:")
        print(f"   Workspaces Processed: {result['workspace_count']}")
        
        if "sqlite" in args.format:
            print(f"   SQLite Database: {result['db_path']}")
            if result.get('workspace_db_path'):
                print(f"   Workspace DB: {result['workspace_db_path']}")
        
        if "csv" in args.format and result.get('csv_dir'):
            print(f"   CSV Files Directory: {result['csv_dir']}")
        
        print("\n" + "=" * 70)
        return 0

    except Exception as e:
        print("\n" + "=" * 70)
        print("❌ Export Failed!")
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


def main():
    """Main entry point for the CLI."""
    args = parse_args()
    
    # Default to export+enrich if no command specified (backward compatibility)
    if not args.command:
        print("⚠️  No command specified. Use 'python main.py export' or 'python main.py enrich'")
        print("   Defaulting to 'export' (with enrichment) for backward compatibility...")
        print()
        # Re-parse with 'export' command to get all default values automatically
        import sys
        sys.argv.append('export')
        args = parse_args()
    
    if args.command == 'enrich':
        return run_enrich_command(args)
    elif args.command == 'export':
        return run_export_command(args)
    else:
        print(f"❌ Unknown command: {args.command}")
        print("   Available commands: export, enrich")
        return 1


if __name__ == "__main__":
    sys.exit(main())

