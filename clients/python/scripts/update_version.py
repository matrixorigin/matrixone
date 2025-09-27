#!/usr/bin/env python3
"""
Version management script for MatrixOne Python SDK.

This script helps manage version numbers in pyproject.toml for different release scenarios:
- TestPyPI releases: frequent patch version increments
- Official PyPI releases: stable version numbers

Usage:
    python scripts/update_version.py 1.2.3                    # Set specific version
    python scripts/update_version.py --increment patch        # Increment patch version
    python scripts/update_version.py --increment minor        # Increment minor version
    python scripts/update_version.py --increment major        # Increment major version
"""

import argparse
import re
import sys
from pathlib import Path


def get_current_version():
    """Get current version from pyproject.toml."""
    pyproject_path = Path("pyproject.toml")
    if not pyproject_path.exists():
        print("Error: pyproject.toml not found")
        sys.exit(1)
    
    content = pyproject_path.read_text()
    match = re.search(r'^version\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
    if not match:
        print("Error: Could not find version in pyproject.toml")
        sys.exit(1)
    
    return match.group(1)


def increment_version(version, part):
    """Increment version number by the specified part."""
    try:
        major, minor, patch = map(int, version.split('.'))
    except ValueError:
        print(f"Error: Invalid version format: {version}")
        sys.exit(1)
    
    if part == "major":
        major += 1
        minor = 0
        patch = 0
    elif part == "minor":
        minor += 1
        patch = 0
    elif part == "patch":
        patch += 1
    else:
        print(f"Error: Invalid part '{part}'. Must be 'major', 'minor', or 'patch'")
        sys.exit(1)
    
    return f"{major}.{minor}.{patch}"


def update_version(new_version):
    """Update version in pyproject.toml."""
    pyproject_path = Path("pyproject.toml")
    content = pyproject_path.read_text()
    
    # Replace version line
    new_content = re.sub(
        r'^version\s*=\s*["\'][^"\']+["\']',
        f'version = "{new_version}"',
        content,
        flags=re.MULTILINE
    )
    
    if new_content == content:
        print(f"Error: Could not update version in pyproject.toml")
        sys.exit(1)
    
    pyproject_path.write_text(new_content)
    print(f"Version updated to {new_version}")


def main():
    parser = argparse.ArgumentParser(description="Update version in pyproject.toml")
    parser.add_argument("version", nargs="?", help="New version number (e.g., 1.2.3)")
    parser.add_argument("--increment", choices=["major", "minor", "patch"], 
                       help="Increment version by specified part")
    
    args = parser.parse_args()
    
    if args.version and args.increment:
        print("Error: Cannot specify both version and --increment")
        sys.exit(1)
    
    if not args.version and not args.increment:
        print("Error: Must specify either version or --increment")
        parser.print_help()
        sys.exit(1)
    
    current_version = get_current_version()
    print(f"Current version: {current_version}")
    
    if args.version:
        new_version = args.version
    else:
        new_version = increment_version(current_version, args.increment)
        print(f"Incrementing {args.increment} version: {current_version} -> {new_version}")
    
    update_version(new_version)


if __name__ == "__main__":
    main()