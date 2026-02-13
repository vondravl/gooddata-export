"""Create a git tag from the version in pyproject.toml.

Reads the version from pyproject.toml, checks if the corresponding git tag
already exists, and creates + pushes it if not. Designed to run from CI
on merge to main, or manually with --dry-run for preview.

Usage:
    python scripts/create_tag.py            # Create and push tag
    python scripts/create_tag.py --dry-run  # Preview without creating/pushing
"""

import argparse
import subprocess
import sys
import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PYPROJECT_TOML = PROJECT_ROOT / "pyproject.toml"


def get_version() -> str:
    """Extract the version string from pyproject.toml."""
    with open(PYPROJECT_TOML, "rb") as f:
        data = tomllib.load(f)
    return data["project"]["version"]


def tag_exists(tag: str) -> bool:
    """Check if a git tag already exists (locally or on remote)."""
    result = subprocess.run(
        ["git", "tag", "-l", tag],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    return tag in result.stdout.strip().splitlines()


def create_and_push_tag(tag: str, *, dry_run: bool = False) -> None:
    """Create an annotated git tag and push it to origin."""
    if dry_run:
        print(f"[dry-run] Would create tag: {tag}")
        print(f"[dry-run] Would push tag: {tag} to origin")
        return

    subprocess.run(
        ["git", "tag", "-a", tag, "-m", f"Release {tag}"],
        check=True,
        cwd=PROJECT_ROOT,
    )
    print(f"Created tag: {tag}")

    subprocess.run(
        ["git", "push", "origin", tag],
        check=True,
        cwd=PROJECT_ROOT,
    )
    print(f"Pushed tag: {tag} to origin")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a git tag from pyproject.toml version."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would happen without creating or pushing the tag.",
    )
    args = parser.parse_args()

    version = get_version()
    tag = f"v{version}"
    print(f"Version from pyproject.toml: {version}")
    print(f"Tag: {tag}")

    if tag_exists(tag):
        print(f"Tag {tag} already exists. Nothing to do.")
        sys.exit(0)

    create_and_push_tag(tag, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
