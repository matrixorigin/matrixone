#!/bin/bash
# MatrixOne Python SDK Clean Script

echo "🧹 Cleaning MatrixOne Python SDK build artifacts..."

# Remove build directories
echo "Removing build directories..."
rm -rf build/
rm -rf dist/
rm -rf *.egg-info/

# Remove Python cache
echo "Removing Python cache..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -name "*.pyc" -delete 2>/dev/null || true
find . -name "*.pyo" -delete 2>/dev/null || true

# Remove test artifacts
echo "Removing test artifacts..."
rm -rf .pytest_cache/
rm -rf .coverage
rm -rf htmlcov/
rm -rf .tox/

echo "✅ Clean completed!"
echo ""
echo "To rebuild the package, run:"
echo "  python -m build"
echo ""
echo "To install in development mode, run:"
echo "  pip install -e ."
