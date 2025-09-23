#!/usr/bin/env python3
"""
MatrixOne Python SDK Version Update Script

This script updates the version number across all relevant files.
"""

import os
import sys
import re
import argparse
from pathlib import Path

def update_pyproject_toml(version):
    """Update version in pyproject.toml."""
    file_path = Path("pyproject.toml")
    if not file_path.exists():
        return False
    
    content = file_path.read_text()
    # Update version line
    content = re.sub(
        r'^version = ".*"$',
        f'version = "{version}"',
        content,
        flags=re.MULTILINE
    )
    file_path.write_text(content)
    return True

def update_setup_py(version):
    """Update version in setup.py (if it has a hardcoded version)."""
    file_path = Path("setup.py")
    if not file_path.exists():
        return False
    
    content = file_path.read_text()
    # Check if there's a hardcoded version to update
    if 'version=' in content and not 'get_version()' in content:
        content = re.sub(
            r'version="[^"]*"',
            f'version="{version}"',
            content
        )
        file_path.write_text(content)
        return True
    return False

def update_init_py(version):
    """Update version in __init__.py."""
    file_path = Path("matrixone/__init__.py")
    if not file_path.exists():
        return False
    
    content = file_path.read_text()
    # Update __version__ line
    content = re.sub(
        r'^__version__ = ".*"$',
        f'__version__ = "{version}"',
        content,
        flags=re.MULTILINE
    )
    file_path.write_text(content)
    return True

def validate_version(version):
    """Validate version format (semantic versioning)."""
    pattern = r'^\d+\.\d+\.\d+(-[a-zA-Z0-9.-]+)?(\+[a-zA-Z0-9.-]+)?$'
    if not re.match(pattern, version):
        raise ValueError(f"Invalid version format: {version}. Expected format: X.Y.Z[-pre][+build]")
    return True

def get_current_version():
    """Get current version from pyproject.toml."""
    file_path = Path("pyproject.toml")
    if not file_path.exists():
        return None
    
    content = file_path.read_text()
    match = re.search(r'^version = "([^"]*)"$', content, re.MULTILINE)
    return match.group(1) if match else None

def increment_version(version, part='patch'):
    """Increment version number."""
    parts = version.split('.')
    if len(parts) < 3:
        raise ValueError(f"Invalid version format: {version}")
    
    major, minor, patch = parts[0], parts[1], parts[2]
    
    # Handle pre-release and build metadata
    patch_parts = patch.split('-')
    patch_base = patch_parts[0]
    pre_release = '-'.join(patch_parts[1:]) if len(patch_parts) > 1 else None
    
    if part == 'major':
        major = str(int(major) + 1)
        minor = '0'
        patch_base = '0'
        pre_release = None
    elif part == 'minor':
        minor = str(int(minor) + 1)
        patch_base = '0'
        pre_release = None
    elif part == 'patch':
        patch_base = str(int(patch_base) + 1)
        pre_release = None
    else:
        raise ValueError(f"Invalid increment part: {part}. Use 'major', 'minor', or 'patch'")
    
    new_version = f"{major}.{minor}.{patch_base}"
    if pre_release:
        new_version += f"-{pre_release}"
    
    return new_version

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Update MatrixOne Python SDK version")
    parser.add_argument("version", nargs="?", help="New version number (e.g., 1.2.3)")
    parser.add_argument("--increment", choices=['major', 'minor', 'patch'], 
                       help="Increment current version")
    parser.add_argument("--current", action="store_true", help="Show current version")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be changed without making changes")
    
    args = parser.parse_args()
    
    # Change to project directory
    project_root = Path(__file__).parent.parent
    os.chdir(project_root)
    
    current_version = get_current_version()
    
    if args.current:
        print(f"Current version: {current_version}")
        return
    
    if args.increment:
        if not current_version:
            print("❌ Could not determine current version")
            sys.exit(1)
        new_version = increment_version(current_version, args.increment)
        print(f"Incrementing {args.increment} version: {current_version} -> {new_version}")
    elif args.version:
        new_version = args.version
        print(f"Setting version: {current_version} -> {new_version}")
    else:
        print("❌ Please specify either --version or --increment")
        sys.exit(1)
    
    # Validate new version
    try:
        validate_version(new_version)
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(1)
    
    if args.dry_run:
        print("Dry run - would update:")
        print(f"  pyproject.toml: version = \"{new_version}\"")
        print(f"  matrixone/__init__.py: __version__ = \"{new_version}\"")
        return
    
    # Update files
    updated_files = []
    
    if update_pyproject_toml(new_version):
        updated_files.append("pyproject.toml")
    
    if update_init_py(new_version):
        updated_files.append("matrixone/__init__.py")
    
    if update_setup_py(new_version):
        updated_files.append("setup.py")
    
    if updated_files:
        print("✅ Updated version in:")
        for file in updated_files:
            print(f"  {file}")
    else:
        print("❌ No files were updated")
        sys.exit(1)
    
    print(f"✅ Version updated to {new_version}")

if __name__ == "__main__":
    main()
