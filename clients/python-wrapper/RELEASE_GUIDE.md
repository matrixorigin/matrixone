# MatrixOne Python SDK Release Guide

This guide explains how to release and publish the MatrixOne Python SDK to PyPI.

## Prerequisites

1. **PyPI Account**: Create an account on [PyPI](https://pypi.org/) if you don't have one
2. **TestPyPI Account**: Create an account on [TestPyPI](https://test.pypi.org/) for testing
3. **API Tokens**: Generate API tokens for both PyPI and TestPyPI
4. **Build Tools**: Install required build tools

```bash
pip install build twine
```

## Version Management

### Version Numbering

Follow [Semantic Versioning](https://semver.org/) (MAJOR.MINOR.PATCH):

- **MAJOR**: Breaking changes to the API
- **MINOR**: New features that are backward compatible
- **PATCH**: Bug fixes that are backward compatible

### Updating Version

1. Update version in `matrixone/__init__.py`:
   ```python
   __version__ = "1.0.0"  # Update to new version
   ```

2. Update version in `pyproject.toml`:
   ```toml
   version = "1.0.0"  # Update to new version
   ```

3. Update `CHANGELOG.md` with new version section

## Pre-Release Checklist

### 1. Code Quality

```bash
# Format code
black matrixone tests examples

# Lint code
flake8 matrixone tests examples

# Type checking
mypy matrixone

# Run tests
# Core version management tests (recommended for release)
python -m pytest test_version_standalone.py test_matrixone_version_parsing.py -v

# Run all tests (may have some failures due to API changes)
python -m pytest --tb=short
```

### 2. Documentation

- [ ] Update README.md if needed
- [ ] Update CHANGELOG.md with new features/changes
- [ ] Ensure all examples work correctly
- [ ] Update API documentation if needed

### 3. Version Compatibility

- [ ] Test with supported Python versions (3.8+)
- [ ] Verify MatrixOne version compatibility
- [ ] Test with different MatrixOne backend versions

## Build and Test Release

### 1. Clean Previous Builds

```bash
rm -rf build/
rm -rf dist/
rm -rf *.egg-info/
```

### 2. Build Package

```bash
# Build wheel and source distribution
python -m build

# Verify build
ls -la dist/
```

### 3. Test Installation

```bash
# Install from local build
pip install dist/matrixone_python_sdk-1.0.0-py3-none-any.whl

# Test import
python -c "import matrixone; print(matrixone.__version__)"

# Test CLI
matrixone-client --help

# Uninstall
pip uninstall matrixone-python-sdk -y
```

## TestPyPI Release (Recommended First)

### 1. Upload to TestPyPI

```bash
# Upload to TestPyPI
twine upload --repository testpypi dist/*

# Or use specific repository
twine upload --repository-url https://test.pypi.org/legacy/ dist/*
```

### 2. Test from TestPyPI

```bash
# Install from TestPyPI
pip install --index-url https://test.pypi.org/simple/ matrixone-python-sdk

# Test functionality
python -c "import matrixone; print(matrixone.__version__)"

# Clean up
pip uninstall matrixone-python-sdk -y
```

## Production PyPI Release

### 1. Upload to PyPI

```bash
# Upload to PyPI
twine upload dist/*

# Or use specific repository
twine upload --repository-url https://upload.pypi.org/legacy/ dist/*
```

### 2. Verify Release

```bash
# Install from PyPI
pip install matrixone-python-sdk

# Test installation
python -c "import matrixone; print(matrixone.__version__)"

# Test CLI
matrixone-client --help
```

## Post-Release Tasks

### 1. GitHub Release

1. Go to GitHub repository releases page
2. Click "Create a new release"
3. Create tag with version number (e.g., `v1.0.0`)
4. Add release notes from CHANGELOG.md
5. Upload distribution files if needed

### 2. Documentation Updates

- [ ] Update documentation website if applicable
- [ ] Update installation instructions
- [ ] Notify community via appropriate channels

### 3. Monitoring

- [ ] Monitor PyPI download statistics
- [ ] Watch for issues or bug reports
- [ ] Monitor GitHub issues for release-related problems

## Configuration Files

### .pypirc (Optional)

Create `~/.pypirc` for easier uploads:

```ini
[distutils]
index-servers =
    pypi
    testpypi

[pypi]
username = __token__
password = pypi-your-api-token-here

[testpypi]
repository = https://test.pypi.org/legacy/
username = __token__
password = pypi-your-testpypi-api-token-here
```

### Environment Variables (Alternative)

```bash
# Set environment variables
export TWINE_USERNAME=__token__
export TWINE_PASSWORD=pypi-your-api-token-here

# Upload
twine upload dist/*
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify API token is correct
   - Ensure token has proper permissions
   - Check if using correct repository URL

2. **Build Errors**
   - Check Python version compatibility
   - Verify all dependencies are installed
   - Clean build directories and retry

3. **Upload Errors**
   - Ensure package name is unique
   - Check if version already exists
   - Verify file permissions

4. **Installation Errors**
   - Check package name spelling
   - Verify version compatibility
   - Check for conflicting packages

### Getting Help

- Check [PyPI documentation](https://packaging.python.org/)
- Review [twine documentation](https://twine.readthedocs.io/)
- Check [setuptools documentation](https://setuptools.pypa.io/)

## Security Considerations

1. **API Tokens**: Never commit API tokens to version control
2. **Credentials**: Use environment variables or .pypirc file
3. **Package Verification**: Always verify package contents before upload
4. **Dependencies**: Keep dependencies up to date and secure

## Release Automation (Future)

Consider automating releases with:

- **GitHub Actions**: Automated testing and publishing
- **Semantic Release**: Automated version bumping
- **Pre-commit Hooks**: Code quality checks
- **CI/CD Pipeline**: Full automated release workflow

## Version History

- **1.0.0** - Initial stable release
  - Complete MatrixOne Python SDK
  - Version management framework
  - Comprehensive test suite
  - Full documentation

---

For questions or issues, please open a GitHub issue or contact the MatrixOne team.
