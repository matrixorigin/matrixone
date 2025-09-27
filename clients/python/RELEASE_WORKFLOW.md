# Release Workflow Guide

This document describes the release workflow for MatrixOne Python SDK, including TestPyPI and official PyPI releases.

## Overview

The release workflow is designed to handle two different scenarios:

1. **TestPyPI Releases**: Frequent releases for testing and validation
2. **Official PyPI Releases**: Stable releases for production use

## TestPyPI Release Workflow

TestPyPI is used for testing and validation. Version numbers can be incremented frequently.

### Quick TestPyPI Release

```bash
# Automatically increment patch version and publish to TestPyPI
make release-test
```

This command will:
1. Increment the patch version (e.g., 1.0.5 → 1.0.6)
2. Build the package
3. Check the package
4. Publish to TestPyPI

### Manual TestPyPI Release

```bash
# Step 1: Increment version for TestPyPI
make version-test

# Step 2: Publish to TestPyPI
make publish-test
```

### Install from TestPyPI

```bash
# Install from TestPyPI
pip install --index-url https://test.pypi.org/simple/ matrixone-python-sdk

# Or with extra index for dependencies
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ matrixone-python-sdk
```

## Official PyPI Release Workflow

Official PyPI releases should use stable version numbers that have been tested on TestPyPI.

### Setting Version for Official Release

```bash
# Set a specific version for official release
make version-release VERSION=1.2.0
```

### Official PyPI Release

```bash
# Publish to official PyPI (requires confirmation)
make release-official
```

This command will:
1. Build the package
2. Check the package
3. Ask for confirmation
4. Publish to official PyPI

### Install from Official PyPI

```bash
# Install from official PyPI
pip install matrixone-python-sdk
```

## Version Management Commands

### View Current Version

```bash
make version
```

### Increment Version

```bash
# Increment patch version (1.0.1 → 1.0.2)
make version-increment PART=patch

# Increment minor version (1.0.1 → 1.1.0)
make version-increment PART=minor

# Increment major version (1.0.1 → 2.0.0)
make version-increment PART=major
```

### Set Specific Version

```bash
# Set specific version
make version-update VERSION=1.2.3
```

## Release Checklist

### Before TestPyPI Release

- [ ] Run tests: `make test`
- [ ] Check code quality: `make lint`
- [ ] Update documentation if needed: `make docs-check`
- [ ] Verify version number is appropriate for testing

### Before Official PyPI Release

- [ ] Test on TestPyPI first
- [ ] Verify all functionality works
- [ ] Update CHANGELOG.md
- [ ] Set appropriate version number
- [ ] Run full test suite: `make test`
- [ ] Check code quality: `make lint`
- [ ] Generate documentation: `make docs`

## Version Numbering Strategy

### TestPyPI Versions

- Use patch version increments for testing (1.0.1, 1.0.2, 1.0.3, ...)
- Can have multiple versions for the same feature
- Version numbers don't need to be sequential

### Official PyPI Versions

- Use semantic versioning (MAJOR.MINOR.PATCH)
- MAJOR: Breaking changes
- MINOR: New features, backward compatible
- PATCH: Bug fixes, backward compatible
- Version numbers should be stable and tested

## Troubleshooting

### "File already exists" Error

This happens when trying to upload the same version to TestPyPI. Solution:

```bash
# Increment version and try again
make version-test
make publish-test
```

### Dependency Issues on TestPyPI

TestPyPI may not have all dependencies. Solution:

```bash
# Install with extra index
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ matrixone-python-sdk
```

### Build Warnings

Some build warnings are normal and don't affect functionality:

- `SetuptoolsWarning: install_requires overwritten`
- `warning: no previously-included files`

These can be ignored for now.

## Examples

### Complete TestPyPI Release

```bash
# 1. Run tests
make test

# 2. Quick release to TestPyPI
make release-test

# 3. Test installation
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ matrixone-python-sdk
```

### Complete Official Release

```bash
# 1. Test on TestPyPI first
make release-test

# 2. Set stable version
make version-release VERSION=1.2.0

# 3. Release to official PyPI
make release-official

# 4. Verify installation
pip install matrixone-python-sdk
```

## Best Practices

1. **Always test on TestPyPI first** before official release
2. **Use semantic versioning** for official releases
3. **Keep TestPyPI versions frequent** for testing
4. **Document changes** in CHANGELOG.md
5. **Run full test suite** before any release
6. **Check code quality** before release
7. **Verify installation** after release

## Automation

The release process can be automated with GitHub Actions:

```yaml
# Example GitHub Actions workflow
name: Release
on:
  push:
    tags:
      - 'v*'

jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'
      
      - name: Install dependencies
        run: make install
      
      - name: Run tests
        run: make test
      
      - name: Build and publish
        run: make release-official
        env:
          TWINE_USERNAME: __token__
          TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}
```

This workflow would automatically release when a version tag is pushed.
