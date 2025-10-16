"""
GoodData Export - Command Line Interface

Run this script to export GoodData metadata to SQLite and/or CSV files.

Usage:
    # Using .env.gdcloud configuration file:
    python main.py

    # With command-line arguments:
    python main.py --workspace-id your_workspace --output-dir my_output

    # Export only SQLite (fastest):
    python main.py --format sqlite

    # Export only CSV:
    python main.py --format csv

    # Export both (default):
    python main.py --format sqlite csv

    # With child workspaces:
    python main.py --include-children --max-workers 10

    # With debug mode:
    python main.py --debug

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


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Export GoodData workspace metadata to SQLite and/or CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic export using .env.gdcloud:
  python main.py

  # Export only to SQLite (fastest):
  python main.py --format sqlite

  # Export with child workspaces:
  python main.py --include-children --max-workers 10

  # Custom output directory:
  python main.py --output-dir exports/production

  # Debug mode:
  python main.py --debug
        """
    )

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

    return parser.parse_args()


def main():
    """Main entry point for the export script."""
    args = parse_args()

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


if __name__ == "__main__":
    sys.exit(main())

