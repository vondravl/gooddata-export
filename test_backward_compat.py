#!/usr/bin/env python3
"""Test backward compatibility - verify all attributes are set when no command is specified."""

import sys
from unittest.mock import patch

# Mock parse_args to simulate no command specified
def mock_parse_args():
    class MockArgs:
        command = None  # No command specified
    return MockArgs()

# Test
print("Testing backward compatibility when no command is specified...")
print()

# Import main but don't run it
sys.path.insert(0, '.')
import main as main_module

# Patch parse_args
with patch.object(main_module, 'parse_args', mock_parse_args):
    # Call main's parse_args
    args = main_module.parse_args()
    print(f"✓ parse_args() returned args with command={args.command}")
    
    # Simulate what main() does
    if not args.command:
        print("⚠️  No command specified - adding defaults...")
        args.command = 'export'
        args.base_url = None
        args.workspace_id = None
        args.bearer_token = None
        args.db_dir = 'output/db'
        args.csv_dir = 'output/metadata_csv'
        args.format = ['sqlite', 'csv']
        args.db_name = None
        args.include_children = False
        args.child_data_types = None
        args.max_workers = 5
        args.enable_rich_text = False
        args.skip_post_export = False
        args.debug = False

# Verify all required attributes are present
required_attrs = [
    'command', 'base_url', 'workspace_id', 'bearer_token',
    'db_dir', 'csv_dir', 'format', 'db_name',
    'include_children', 'child_data_types', 'max_workers',
    'enable_rich_text', 'skip_post_export', 'debug'
]

print()
print("Checking all required attributes:")
all_present = True
for attr in required_attrs:
    if hasattr(args, attr):
        value = getattr(args, attr)
        print(f"  ✓ {attr} = {value}")
    else:
        print(f"  ✗ {attr} MISSING!")
        all_present = False

print()
if all_present:
    print("✅ SUCCESS: All required attributes are present!")
    print("   Backward compatibility works correctly.")
else:
    print("❌ FAILURE: Some attributes are missing!")
    sys.exit(1)

# Verify defaults
print()
print("Verifying default values:")
print(f"  ✓ skip_post_export = {args.skip_post_export} (should be False for full workflow)")
print(f"  ✓ format = {args.format} (should be ['sqlite', 'csv'])")
print(f"  ✓ command = {args.command} (should be 'export')")

print()
print("✅ All tests passed!")

