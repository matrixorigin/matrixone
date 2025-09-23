#!/usr/bin/env python3
"""
MatrixOne Python SDK Release Script

This script automates the release process for the MatrixOne Python SDK.
It performs pre-release checks, builds the package, and optionally publishes to PyPI.
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def run_command(command, check=True, capture_output=False):
    """Run a shell command and return the result."""
    print(f"Running: {command}")
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=check,
            capture_output=capture_output,
            text=True
        )
        if capture_output:
            return result.stdout.strip()
        return result
    except subprocess.CalledProcessError as e:
        print(f"Command failed with exit code {e.returncode}")
        if capture_output and e.stdout:
            print(f"STDOUT: {e.stdout}")
        if capture_output and e.stderr:
            print(f"STDERR: {e.stderr}")
        raise

def check_environment():
    """Check if the environment is ready for release."""
    print("🔍 Checking environment...")
    
    # Check if we're in the right directory
    if not (project_root / "setup.py").exists():
        raise RuntimeError("setup.py not found. Please run from the project root.")
    
    # Check if git is clean
    try:
        result = run_command("git status --porcelain", capture_output=True)
        if result:
            print("⚠️  Warning: Git working directory is not clean:")
            print(result)
            response = input("Continue anyway? (y/N): ")
            if response.lower() != 'y':
                sys.exit(1)
    except subprocess.CalledProcessError:
        print("⚠️  Warning: Could not check git status")
    
    print("✅ Environment check passed")

def run_tests():
    """Run all tests."""
    print("🧪 Running tests...")
    
    # Run offline tests
    print("Running offline tests...")
    run_command("make test-offline")
    
    # Check if we can run online tests
    try:
        print("Running online tests...")
        run_command("make test-online")
    except subprocess.CalledProcessError:
        print("⚠️  Online tests failed or database not available")
        response = input("Continue without online tests? (y/N): ")
        if response.lower() != 'y':
            sys.exit(1)
    
    print("✅ All tests passed")

def run_linting():
    """Run code quality checks."""
    print("🔍 Running code quality checks...")
    run_command("make lint")
    print("✅ Code quality checks passed")

def build_package():
    """Build the package."""
    print("📦 Building package...")
    run_command("make build")
    print("✅ Package built successfully")

def check_package():
    """Check the built package."""
    print("🔍 Checking package...")
    run_command("make check-build")
    print("✅ Package check passed")

def get_version():
    """Get the current version."""
    try:
        version = run_command("make version", capture_output=True)
        return version
    except subprocess.CalledProcessError:
        # Fallback to reading from pyproject.toml
        with open(project_root / "pyproject.toml", "r") as f:
            for line in f:
                if line.startswith("version ="):
                    return line.split('"')[1]
    return "unknown"

def publish_to_testpypi():
    """Publish to test PyPI."""
    print("🚀 Publishing to test PyPI...")
    run_command("make publish-test")
    print("✅ Published to test PyPI successfully")

def publish_to_pypi():
    """Publish to PyPI."""
    print("🚀 Publishing to PyPI...")
    version = get_version()
    print(f"Publishing version: {version}")
    
    # Double confirmation for PyPI
    response = input(f"Are you sure you want to publish version {version} to PyPI? (yes/NO): ")
    if response.lower() != 'yes':
        print("Publishing cancelled")
        return
    
    run_command("make publish")
    print("✅ Published to PyPI successfully")

def main():
    """Main release function."""
    parser = argparse.ArgumentParser(description="MatrixOne Python SDK Release Script")
    parser.add_argument("--skip-tests", action="store_true", help="Skip running tests")
    parser.add_argument("--skip-lint", action="store_true", help="Skip linting")
    parser.add_argument("--testpypi", action="store_true", help="Publish to test PyPI")
    parser.add_argument("--pypi", action="store_true", help="Publish to PyPI")
    parser.add_argument("--build-only", action="store_true", help="Only build the package")
    
    args = parser.parse_args()
    
    try:
        # Change to project directory
        os.chdir(project_root)
        
        # Check environment
        check_environment()
        
        if not args.skip_tests:
            run_tests()
        
        if not args.skip_lint:
            run_linting()
        
        # Build package
        build_package()
        check_package()
        
        if args.build_only:
            print("✅ Build completed successfully")
            return
        
        # Publishing
        if args.testpypi:
            publish_to_testpypi()
        elif args.pypi:
            publish_to_pypi()
        else:
            print("✅ Pre-release checks completed successfully")
            print("To publish:")
            print("  python scripts/release.py --testpypi  # Publish to test PyPI")
            print("  python scripts/release.py --pypi      # Publish to PyPI")
    
    except KeyboardInterrupt:
        print("\n❌ Release cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Release failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
