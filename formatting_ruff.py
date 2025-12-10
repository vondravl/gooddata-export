#!/usr/bin/env python3
"""
Ruff formatting tool for Python code.

This script can format Python files or check if files are properly formatted.

USAGE:
    # Format all Python files in the repository
    python formatting_ruff.py

    # Check formatting (for CI/CD - exits with error if not formatted)
    python formatting_ruff.py --check

    # Format specific directory only
    python formatting_ruff.py --path gooddata_export/

MODES:
    --check: Validation mode (for CI/CD)
        - Runs ruff format in check mode
        - Exits with error if formatting changes needed
        - Used in PR checks to block merge if formatting is incorrect

    Default: Formatting mode (for local development)
        - Formats files in place
        - Use before committing to ensure consistent formatting

WHAT IT DOES:
    - Runs ruff format to apply consistent Python code formatting
    - Similar to black but faster and part of the ruff toolchain
"""

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List


def find_ruff() -> str:
    """Find ruff executable in the same environment as this Python interpreter."""
    # Get ruff from the same directory as the current Python executable
    python_dir = Path(sys.executable).parent
    ruff_path = python_dir / "ruff"

    if ruff_path.exists():
        return str(ruff_path)

    # Fall back to system ruff (searches PATH)
    return "ruff"


def run_ruff_format(paths: List[str], check_only: bool = False) -> int:
    """
    Run ruff format on specified paths.

    Args:
        paths: List of paths to format
        check_only: If True, only check formatting without making changes

    Returns:
        0 if successful (or no changes needed in check mode), 1 otherwise
    """
    ruff_cmd = find_ruff()
    cmd = [ruff_cmd, "format"]

    if check_only:
        cmd.append("--check")
        cmd.append("--diff")

    cmd.extend(paths)

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)

        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)

        return result.returncode
    except FileNotFoundError:
        print(f"❌ Error: ruff not found at {ruff_cmd}. Install with: pip install ruff")
        return 1
    except Exception as e:
        print(f"❌ Error running ruff: {e}")
        return 1


def check_mode(paths: List[str]) -> int:
    """
    Check mode: Verify formatting without making changes (for CI/CD).

    Returns:
        0 if formatting is correct, 1 if changes are needed
    """
    print("=" * 70)
    print("🔍 Ruff Formatting Check (Pre-Merge Validation)")
    print("=" * 70)
    print("\nThis check ensures all Python files follow ruff formatting.")
    print("If this fails, run without --check locally and commit the changes.\n")

    print(f"📋 Checking Python files in: {', '.join(paths)}\n")
    print("🔧 Running ruff format --check...\n")

    returncode = run_ruff_format(paths, check_only=True)

    print()
    if returncode == 0:
        print("=" * 70)
        print("✅ FORMATTING CHECK PASSED")
        print("=" * 70)
        print("\nAll Python files are properly formatted! 🎉")
        print("=" * 70)
        return 0
    else:
        print("=" * 70)
        print("❌ FORMATTING CHECK FAILED")
        print("=" * 70)
        print("\nSome files need formatting.\n")
        print("📝 TO FIX THIS:")
        print("=" * 70)
        print("1. Run locally (without --check):")
        print("   python formatting_ruff.py")
        print("2. Review changes:")
        print("   git diff")
        print("3. Commit and push:")
        print("   git add .")
        print("   git commit -m 'Apply ruff formatting'")
        print("   git push")
        print("=" * 70)
        return 1


def format_mode(paths: List[str]) -> int:
    """
    Format mode: Format files in place (for local development).

    Returns:
        0 if successful, 1 if errors occurred
    """
    print("=" * 60)
    print("🔧 Ruff Code Formatter")
    print("=" * 60)
    print("\nFormatting Python files with ruff...\n")

    print(f"📋 Formatting paths: {', '.join(paths)}\n")

    returncode = run_ruff_format(paths, check_only=False)

    print("\n" + "=" * 60)
    if returncode == 0:
        print("✅ All files formatted successfully!")
        print("\n💡 Next steps:")
        print("   1. Run 'git diff' to see the formatting changes")
        print("   2. Review and commit the formatted files")
    else:
        print("⚠️  Some files had errors during formatting")
    print("=" * 60)

    return returncode


def main():
    parser = argparse.ArgumentParser(description="Ruff formatting tool for Python code")
    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "Check mode: Validate formatting (for CI/CD). "
            "Exits with error if formatting is incorrect."
        ),
    )
    parser.add_argument(
        "--path",
        type=str,
        action="append",
        help=(
            "Specific path(s) to process. Can be specified multiple times. "
            "If not specified, formats all Python directories in the repository."
        ),
    )

    args = parser.parse_args()

    # Determine base path (repo root)
    script_dir = Path(__file__).parent

    # Determine paths to format
    if args.path:
        paths = [str(script_dir / p) for p in args.path]
    else:
        # Default: format all Python files/directories in the repository
        paths = [
            str(script_dir / "gooddata_export"),
            str(script_dir / "main.py"),
            str(script_dir / "formatting_ruff.py"),
        ]

    # Filter to existing paths only
    paths = [p for p in paths if Path(p).exists()]
    if not paths:
        print("Error: No valid paths to format")
        return 1

    # Run appropriate mode
    if args.check:
        return check_mode(paths)
    else:
        return format_mode(paths)


if __name__ == "__main__":
    sys.exit(main())
