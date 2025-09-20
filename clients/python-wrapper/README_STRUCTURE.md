# README File Structure Guide

## 📁 File Description

### `README.md` - Developer Version
- **Purpose**: For developers and contributors
- **Content**: Complete information including development, testing, building, publishing
- **Includes**: Makefile commands, development environment setup, testing workflow, publishing workflow
- **Target Users**: Project maintainers, contributors, developers

### `README_USER.md` - User Version
- **Purpose**: For end users
- **Content**: Only user-related information including installation, usage, API documentation
- **Includes**: Installation guide, quick start, API examples, configuration guide
- **Target Users**: Developers using the SDK

## 🔧 Configuration

### pyproject.toml
```toml
readme = "README_USER.md"  # Use user README for publishing
```

### MANIFEST.in
```
include README_USER.md     # Ensure user README is included in release package
```

## 🚀 Publishing Workflow

### Development Phase
- Use `README.md` for development
- Include all development-related information

### Publishing Phase
- Use `make build-release` to build release package
- Automatically use `README_USER.md` as PyPI README
- Source package includes both README files
- Wheel package embeds README content in METADATA

## 📦 Package Contents

### Source Package (tar.gz)
- Includes `README.md` (developer version)
- Includes `README_USER.md` (user version)
- Includes all example files
- Excludes test files

### Wheel Package (.whl)
- Does not include README files
- README content embedded in METADATA
- Only includes core code and examples
- Excludes test files

## 🎯 Usage Recommendations

### Developers
- Check `README.md` for complete development workflow
- Use `make help` to view all available commands
- Use `make dev-setup` to setup development environment

### End Users
- See `README_USER.md` content on PyPI
- Focus on installation and usage guides
- Check example files to learn usage
