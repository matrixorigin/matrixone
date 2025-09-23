# MatrixOne Python SDK Makefile
# Provides unified commands for development, testing, building and publishing

# Configuration variables
# Users can override these by setting environment variables or passing them to make
# Example: make test PYTHON=/path/to/python3
# Example: PYTHON=/path/to/python3 make test
PYTHON ?= /Users/xupeng/miniconda3/envs/ai_env/bin/python3
PIP ?= pip
PACKAGE_NAME := matrixone_python_sdk
VERSION := $(shell grep '^version' pyproject.toml | cut -d'"' -f2)
BUILD_DIR := dist
TEST_DIR := test-results
COVERAGE_DIR := htmlcov

# Color definitions
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[0;33m
BLUE := \033[0;34m
NC := \033[0m # No Color

# Default target
.PHONY: help
help: ## Show help information
	@echo "$(BLUE)MatrixOne Python SDK - Available Commands:$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)Current version: $(VERSION)$(NC)"
	@echo "$(YELLOW)Python path: $(PYTHON)$(NC)"

# Development environment setup
.PHONY: install
install: ## Install development dependencies
	@echo "$(BLUE)Installing development dependencies...$(NC)"
	$(PIP) install -r requirements.txt
	$(PIP) install pytest pytest-cov pytest-asyncio build twine
	@echo "$(GREEN)Dependencies installed!$(NC)"

.PHONY: install-dev
install-dev: install ## Install development environment (including testing tools)
	@echo "$(BLUE)Installing development tools...$(NC)"
	$(PIP) install black flake8 mypy isort sphinx sphinx-rtd-theme
	@echo "$(GREEN)Development tools installed!$(NC)"

# Code quality checks
.PHONY: lint
lint: ## Run code quality checks
	@echo "$(BLUE)Running code quality checks...$(NC)"
	@echo "$(YELLOW)Checking code format...$(NC)"
	$(PYTHON) -m flake8 matrixone/ --max-line-length=120 --ignore=E203,W503
	@echo "$(YELLOW)Checking import sorting...$(NC)"
	$(PYTHON) -m isort matrixone/ --check-only --diff
	@echo "$(GREEN)Code quality checks completed!$(NC)"

.PHONY: format
format: ## Format code
	@echo "$(BLUE)Formatting code...$(NC)"
	$(PYTHON) -m black matrixone/ --line-length=120
	$(PYTHON) -m isort matrixone/
	@echo "$(GREEN)Code formatting completed!$(NC)"

.PHONY: type-check
type-check: ## Run type checking
	@echo "$(BLUE)Running type checking...$(NC)"
	$(PYTHON) -m mypy matrixone/ --ignore-missing-imports
	@echo "$(GREEN)Type checking completed!$(NC)"

# Testing related
.PHONY: test
test: test-offline ## Run all tests (offline + online)
	@echo "$(BLUE)Running online tests...$(NC)"
	@echo "$(YELLOW)Note: This requires a running MatrixOne database$(NC)"
	@$(PYTHON) scripts/check_connection.py && $(PYTHON) -m pytest tests/online/ -v --tb=short && echo "$(GREEN)Online tests completed!$(NC)" || echo "$(YELLOW)⚠️  Online tests skipped (database not available)$(NC)"

.PHONY: test-offline
test-offline: ## Run offline tests (mock-based unit tests)
	@echo "$(BLUE)Running offline tests...$(NC)"
	$(PYTHON) -m pytest tests/offline/ -v --tb=short
	@echo "$(GREEN)Offline tests completed!$(NC)"

.PHONY: test-online
test-online: ## Run online tests (requires database connection)
	@echo "$(BLUE)Running online tests...$(NC)"
	@echo "$(YELLOW)Note: This requires a running MatrixOne database$(NC)"
	@$(PYTHON) scripts/check_connection.py || (echo "$(RED)❌ Cannot connect to MatrixOne database. Skipping online tests.$(NC)" && exit 1)
	$(PYTHON) -m pytest tests/online/ -v --tb=short
	@echo "$(GREEN)Online tests completed!$(NC)"

.PHONY: test-all
test-all: test ## Alias for test (run all tests)

.PHONY: coverage
coverage: coverage-offline ## Run all coverage tests
	@echo "$(BLUE)Running online tests with coverage...$(NC)"
	@echo "$(YELLOW)Note: This requires a running MatrixOne database$(NC)"
	@$(PYTHON) scripts/check_connection.py && $(PYTHON) -m pytest tests/online/ --cov=matrixone --cov-report=html --cov-report=term && echo "$(GREEN)Online coverage report generated! View: $(COVERAGE_DIR)/index.html$(NC)" || echo "$(YELLOW)⚠️  Online coverage tests skipped (database not available)$(NC)"

.PHONY: coverage-offline
coverage-offline: ## Run offline tests with coverage report
	@echo "$(BLUE)Running offline tests with coverage...$(NC)"
	$(PYTHON) -m pytest tests/offline/ --cov=matrixone --cov-report=html --cov-report=term
	@echo "$(GREEN)Offline coverage report generated! View: $(COVERAGE_DIR)/index.html$(NC)"

.PHONY: coverage-online
coverage-online: ## Run online tests with coverage report (requires database)
	@echo "$(BLUE)Running online tests with coverage...$(NC)"
	@echo "$(YELLOW)Note: This requires a running MatrixOne database$(NC)"
	@$(PYTHON) scripts/check_connection.py || (echo "$(RED)❌ Cannot connect to MatrixOne database. Skipping online coverage tests.$(NC)" && exit 1)
	$(PYTHON) -m pytest tests/online/ --cov=matrixone --cov-report=html --cov-report=term
	@echo "$(GREEN)Online coverage report generated! View: $(COVERAGE_DIR)/index.html$(NC)"

.PHONY: test-fast
test-fast: ## Run fast tests (skip slow tests)
	@echo "$(BLUE)Running fast tests...$(NC)"
	$(PYTHON) -m pytest tests/ -v -m "not slow" --tb=short
	@echo "$(GREEN)Fast tests completed!$(NC)"

.PHONY: test-fulltext
test-fulltext: ## Run fulltext index tests
	@echo "$(BLUE)Running fulltext index tests...$(NC)"
	@echo "$(YELLOW)Note: This requires a running MatrixOne database$(NC)"
	@$(PYTHON) scripts/check_connection.py || (echo "$(RED)❌ Cannot connect to MatrixOne database. Skipping fulltext tests.$(NC)" && exit 1)
	$(PYTHON) -m pytest tests/online/test_fulltext_index_online.py -v --tb=short
	$(PYTHON) -m pytest tests/offline/test_fulltext_index.py -v --tb=short
	@echo "$(GREEN)Fulltext tests completed!$(NC)"

.PHONY: test-vector
test-vector: ## Run vector search tests
	@echo "$(BLUE)Running vector search tests...$(NC)"
	@echo "$(YELLOW)Note: This requires a running MatrixOne database$(NC)"
	@$(PYTHON) scripts/check_connection.py || (echo "$(RED)❌ Cannot connect to MatrixOne database. Skipping vector tests.$(NC)" && exit 1)
	$(PYTHON) -m pytest tests/online/test_vector_* -v --tb=short
	@echo "$(GREEN)Vector tests completed!$(NC)"

.PHONY: test-vector-index
test-vector-index: ## Run vector index tests
	@echo "$(BLUE)Running vector index tests...$(NC)"
	@echo "$(YELLOW)Note: This requires a running MatrixOne database$(NC)"
	@$(PYTHON) scripts/check_connection.py || (echo "$(RED)❌ Cannot connect to MatrixOne database. Skipping vector index tests.$(NC)" && exit 1)
	$(PYTHON) -m pytest tests/online/test_vector_index_* -v --tb=short
	@echo "$(GREEN)Vector index tests completed!$(NC)"

.PHONY: test-verbose
test-verbose: ## Run verbose tests
	@echo "$(BLUE)Running verbose tests...$(NC)"
	$(PYTHON) -m pytest tests/ -v -s --tb=long
	@echo "$(GREEN)Verbose tests completed!$(NC)"

.PHONY: test-release
test-release: ## Run pre-release tests
	@echo "$(BLUE)Running pre-release tests...$(NC)"
	$(PYTHON) -m pytest tests/ -v --tb=short --strict-markers
	@echo "$(GREEN)Pre-release tests completed!$(NC)"

# Legacy aliases for backward compatibility
.PHONY: test-coverage
test-coverage: coverage-offline ## Legacy alias for coverage-offline

.PHONY: test-coverage-offline
test-coverage-offline: coverage-offline ## Legacy alias for coverage-offline

.PHONY: test-coverage-online
test-coverage-online: coverage-online ## Legacy alias for coverage-online

# Example running
.PHONY: examples
examples: ## Run all examples (use env vars to override connection: MATRIXONE_HOST, MATRIXONE_PORT, MATRIXONE_USER, MATRIXONE_PASSWORD, MATRIXONE_DATABASE)
	@echo "$(BLUE)Running all examples...$(NC)"
	@echo "$(YELLOW)Connection config:$(NC)"
	@echo "  Host: $${MATRIXONE_HOST:-127.0.0.1}"
	@echo "  Port: $${MATRIXONE_PORT:-6001}"
	@echo "  User: $${MATRIXONE_USER:-root}"
	@echo "  Password: $${MATRIXONE_PASSWORD:-111}"
	@echo "  Database: $${MATRIXONE_DATABASE:-test}"
	@echo ""
	@for example in examples/example_*.py; do \
		echo "$(YELLOW)Running $$example...$(NC)"; \
		PYTHONPATH=. $(PYTHON) $$example || echo "$(RED)$$example failed$(NC)"; \
		echo ""; \
	done
	@echo "$(GREEN)All examples completed!$(NC)"

.PHONY: example-basic
example-basic: ## Run basic connection example (use env vars to override connection)
	@echo "$(BLUE)Running basic connection example...$(NC)"
	@echo "$(YELLOW)Connection config:$(NC)"
	@echo "  Host: $${MATRIXONE_HOST:-127.0.0.1}"
	@echo "  Port: $${MATRIXONE_PORT:-6001}"
	@echo "  User: $${MATRIXONE_USER:-root}"
	@echo "  Password: $${MATRIXONE_PASSWORD:-111}"
	@echo "  Database: $${MATRIXONE_DATABASE:-test}"
	@echo ""
	PYTHONPATH=. $(PYTHON) examples/example_01_basic_connection.py

.PHONY: example-async
example-async: ## Run async operations example (use env vars to override connection)
	@echo "$(BLUE)Running async operations example...$(NC)"
	@echo "$(YELLOW)Connection config:$(NC)"
	@echo "  Host: $${MATRIXONE_HOST:-127.0.0.1}"
	@echo "  Port: $${MATRIXONE_PORT:-6001}"
	@echo "  User: $${MATRIXONE_USER:-root}"
	@echo "  Password: $${MATRIXONE_PASSWORD:-111}"
	@echo "  Database: $${MATRIXONE_DATABASE:-test}"
	@echo ""
	PYTHONPATH=. $(PYTHON) examples/example_03_async_operations.py

.PHONY: example-account
example-account: ## Run account management example (use env vars to override connection)
	@echo "$(BLUE)Running account management example...$(NC)"
	@echo "$(YELLOW)Connection config:$(NC)"
	@echo "  Host: $${MATRIXONE_HOST:-127.0.0.1}"
	@echo "  Port: $${MATRIXONE_PORT:-6001}"
	@echo "  User: $${MATRIXONE_USER:-root}"
	@echo "  Password: $${MATRIXONE_PASSWORD:-111}"
	@echo "  Database: $${MATRIXONE_DATABASE:-test}"
	@echo ""
	PYTHONPATH=. $(PYTHON) examples/example_02_account_management.py

.PHONY: example-fulltext
example-fulltext: ## Run fulltext index example (use env vars to override connection)
	@echo "$(BLUE)Running fulltext index example...$(NC)"
	@echo "$(YELLOW)Connection config:$(NC)"
	@echo "  Host: $${MATRIXONE_HOST:-127.0.0.1}"
	@echo "  Port: $${MATRIXONE_PORT:-6001}"
	@echo "  User: $${MATRIXONE_USER:-root}"
	@echo "  Password: $${MATRIXONE_PASSWORD:-111}"
	@echo "  Database: $${MATRIXONE_DATABASE:-test}"
	@echo ""
	PYTHONPATH=. $(PYTHON) examples/example_fulltext_index.py

.PHONY: example-vector
example-vector: ## Run vector search example (use env vars to override connection)
	@echo "$(BLUE)Running vector search example...$(NC)"
	@echo "$(YELLOW)Connection config:$(NC)"
	@echo "  Host: $${MATRIXONE_HOST:-127.0.0.1}"
	@echo "  Port: $${MATRIXONE_PORT:-6001}"
	@echo "  User: $${MATRIXONE_USER:-root}"
	@echo "  Password: $${MATRIXONE_PASSWORD:-111}"
	@echo "  Database: $${MATRIXONE_DATABASE:-test}"
	@echo ""
	PYTHONPATH=. $(PYTHON) examples/example_12_vector_search.py

.PHONY: example-vector-index
example-vector-index: ## Run vector index example (use env vars to override connection)
	@echo "$(BLUE)Running vector index example...$(NC)"
	@echo "$(YELLOW)Connection config:$(NC)"
	@echo "  Host: $${MATRIXONE_HOST:-127.0.0.1}"
	@echo "  Port: $${MATRIXONE_PORT:-6001}"
	@echo "  User: $${MATRIXONE_USER:-root}"
	@echo "  Password: $${MATRIXONE_PASSWORD:-111}"
	@echo "  Database: $${MATRIXONE_DATABASE:-test}"
	@echo ""
	PYTHONPATH=. $(PYTHON) examples/example_13_vector_index.py

.PHONY: example-vector-comprehensive
example-vector-comprehensive: ## Run comprehensive vector examples (use env vars to override connection)
	@echo "$(BLUE)Running comprehensive vector examples...$(NC)"
	@echo "$(YELLOW)Connection config:$(NC)"
	@echo "  Host: $${MATRIXONE_HOST:-127.0.0.1}"
	@echo "  Port: $${MATRIXONE_PORT:-6001}"
	@echo "  User: $${MATRIXONE_USER:-root}"
	@echo "  Password: $${MATRIXONE_PASSWORD:-111}"
	@echo "  Database: $${MATRIXONE_DATABASE:-test}"
	@echo ""
	PYTHONPATH=. $(PYTHON) examples/example_16_vector_comprehensive.py

.PHONY: example-hnsw
example-hnsw: ## Run HNSW vector index example (use env vars to override connection)
	@echo "$(BLUE)Running HNSW vector index example...$(NC)"
	@echo "$(YELLOW)Connection config:$(NC)"
	@echo "  Host: $${MATRIXONE_HOST:-127.0.0.1}"
	@echo "  Port: $${MATRIXONE_PORT:-6001}"
	@echo "  User: $${MATRIXONE_USER:-root}"
	@echo "  Password: $${MATRIXONE_PASSWORD:-111}"
	@echo "  Database: $${MATRIXONE_DATABASE:-test}"
	@echo ""
	PYTHONPATH=. $(PYTHON) examples/example_17_hnsw_vector_index.py

# Build related
.PHONY: build
build: clean ## Build package
	@echo "$(BLUE)Building package...$(NC)"
	$(PYTHON) -m build --wheel --sdist --outdir $(BUILD_DIR)
	@echo "$(GREEN)Package built! Output directory: $(BUILD_DIR)/$(NC)"

.PHONY: build-release
build-release: clean ## Build release package (using user README)
	@echo "$(BLUE)Building release package...$(NC)"
	@echo "$(YELLOW)Using user README: README_USER.md$(NC)"
	$(PYTHON) -m build --wheel --sdist --outdir $(BUILD_DIR)
	@echo "$(GREEN)Release package built! Output directory: $(BUILD_DIR)/$(NC)"

.PHONY: build-wheel
build-wheel: clean ## Build wheel package only
	@echo "$(BLUE)Building wheel package...$(NC)"
	$(PYTHON) -m build --wheel --outdir $(BUILD_DIR)
	@echo "$(GREEN)Wheel package built!$(NC)"

.PHONY: build-sdist
build-sdist: clean ## Build source package only
	@echo "$(BLUE)Building source package...$(NC)"
	$(PYTHON) -m build --sdist --outdir $(BUILD_DIR)
	@echo "$(GREEN)Source package built!$(NC)"

.PHONY: check-build
check-build: build ## Check built package
	@echo "$(BLUE)Checking built package...$(NC)"
	$(PYTHON) -m twine check $(BUILD_DIR)/*
	@echo "$(GREEN)Package check completed!$(NC)"

# Publishing related
.PHONY: publish-test
publish-test: build-release check-build ## Publish to test PyPI
	@echo "$(BLUE)Publishing to test PyPI...$(NC)"
	$(PYTHON) -m twine upload --repository testpypi $(BUILD_DIR)/*
	@echo "$(GREEN)Published to test PyPI!$(NC)"

.PHONY: publish
publish: build-release check-build ## Publish to PyPI
	@echo "$(BLUE)Publishing to PyPI...$(NC)"
	@echo "$(RED)Warning: This will publish to official PyPI, please confirm version is correct!$(NC)"
	@read -p "Confirm publish to PyPI? (y/N): " confirm && [ "$$confirm" = "y" ]
	$(PYTHON) -m twine upload $(BUILD_DIR)/*
	@echo "$(GREEN)Published to PyPI!$(NC)"

# Cleanup related
.PHONY: clean
clean: ## Clean build artifacts
	@echo "$(BLUE)Cleaning build artifacts...$(NC)"
	rm -rf $(BUILD_DIR)/
	rm -rf build/
	rm -rf *.egg-info/
	rm -rf $(COVERAGE_DIR)/
	rm -rf $(TEST_DIR)/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	@echo "$(GREEN)Cleanup completed!$(NC)"

.PHONY: clean-all
clean-all: clean ## Clean all artifacts (including virtual environments)
	@echo "$(BLUE)Cleaning all artifacts...$(NC)"
	rm -rf .venv/
	rm -rf venv/
	@echo "$(GREEN)Complete cleanup finished!$(NC)"

# Development workflow
.PHONY: dev-setup
dev-setup: install-dev ## Setup development environment
	@echo "$(BLUE)Setting up development environment...$(NC)"
	@echo "$(GREEN)Development environment setup completed!$(NC)"
	@echo "$(YELLOW)Available commands:$(NC)"
	@echo "  make test        - Run tests"
	@echo "  make lint        - Code quality checks"
	@echo "  make format      - Format code"
	@echo "  make examples    - Run examples"
	@echo "  make build       - Build package"

.PHONY: pre-commit
pre-commit: format lint test ## Full pre-commit checks (format + lint + test)
	@echo "$(GREEN)Full pre-commit checks completed!$(NC)"

# Documentation
.PHONY: docs
docs: ## Generate documentation
	@echo "$(BLUE)Generating documentation...$(NC)"
	$(PYTHON) -m sphinx -b html docs docs/_build/html
	@echo "$(GREEN)Documentation generated! View: docs/_build/html/index.html$(NC)"

.PHONY: docs-clean
docs-clean: ## Clean documentation build
	@echo "$(BLUE)Cleaning documentation...$(NC)"
	rm -rf docs/_build/
	@echo "$(GREEN)Documentation cleaned!$(NC)"

.PHONY: docs-serve
docs-serve: docs ## Generate and serve documentation
	@echo "$(BLUE)Serving documentation...$(NC)"
	@echo "$(YELLOW)Open http://localhost:8000 in your browser$(NC)"
	cd docs/_build/html && $(PYTHON) -m http.server 8000

.PHONY: docs-api
docs-api: ## Generate API documentation only
	@echo "$(BLUE)Generating API documentation...$(NC)"
	$(PYTHON) -m sphinx -b html -D extensions=sphinx.ext.autodoc docs docs/_build/api
	@echo "$(GREEN)API documentation generated!$(NC)"

# Version management
.PHONY: version-update
version-update: ## Update version number (usage: make version-update VERSION=1.2.3)
	@if [ -z "$(VERSION)" ]; then \
		echo "$(RED)Error: VERSION is required$(NC)"; \
		echo "Usage: make version-update VERSION=1.2.3"; \
		exit 1; \
	fi
	@echo "$(BLUE)Updating version to $(VERSION)...$(NC)"
	$(PYTHON) scripts/update_version.py $(VERSION)
	@echo "$(GREEN)Version updated to $(VERSION)!$(NC)"

.PHONY: version-increment
version-increment: ## Increment version (usage: make version-increment PART=patch)
	@if [ -z "$(PART)" ]; then \
		echo "$(RED)Error: PART is required$(NC)"; \
		echo "Usage: make version-increment PART=patch|minor|major"; \
		exit 1; \
	fi
	@echo "$(BLUE)Incrementing $(PART) version...$(NC)"
	$(PYTHON) scripts/update_version.py --increment $(PART)
	@echo "$(GREEN)Version incremented!$(NC)"

.PHONY: version-current
version-current: ## Show current version
	@echo "$(BLUE)Current version:$(NC)"
	$(PYTHON) scripts/update_version.py --current

# Release management
.PHONY: release-check
release-check: lint test build-release check-build ## Pre-release complete checks
	@echo "$(GREEN)Pre-release checks completed!$(NC)"

.PHONY: release-testpypi
release-testpypi: ## Publish to test PyPI
	@echo "$(BLUE)Publishing to test PyPI...$(NC)"
	$(PYTHON) scripts/release.py --testpypi
	@echo "$(GREEN)Published to test PyPI!$(NC)"

.PHONY: release-pypi
release-pypi: ## Publish to PyPI
	@echo "$(BLUE)Publishing to PyPI...$(NC)"
	$(PYTHON) scripts/release.py --pypi
	@echo "$(GREEN)Published to PyPI!$(NC)"

.PHONY: release-full
release-full: ## Full release process (check + build + publish to test PyPI)
	@echo "$(BLUE)Running full release process...$(NC)"
	$(PYTHON) scripts/release.py --testpypi
	@echo "$(GREEN)Full release process completed!$(NC)"

# Information display
.PHONY: info
info: ## Show project information
	@echo "$(BLUE)MatrixOne Python SDK Project Information:$(NC)"
	@echo "  Package name: $(PACKAGE_NAME)"
	@echo "  Version: $(VERSION)"
	@echo "  Python: $(PYTHON)"
	@echo "  Pip: $(PIP)"
	@echo "  Build directory: $(BUILD_DIR)"
	@echo ""
	@echo "$(YELLOW)File statistics:$(NC)"
	@echo "  Test files: $$(ls tests/test_*.py 2>/dev/null | wc -l)"
	@echo "  Example files: $$(ls examples/example_*.py 2>/dev/null | wc -l)"
	@echo "  Core modules: $$(ls matrixone/*.py 2>/dev/null | wc -l)"

.PHONY: check-env
check-env: ## Check Python environment configuration
	@echo "$(BLUE)Checking Python environment...$(NC)"
	@echo "  Python command: $(PYTHON)"
	@echo "  Pip command: $(PIP)"
	@echo ""
	@echo "$(YELLOW)Python version:$(NC)"
	@$(PYTHON) --version || echo "$(RED)Python not found or not working$(NC)"
	@echo ""
	@echo "$(YELLOW)Pip version:$(NC)"
	@$(PIP) --version || echo "$(RED)Pip not found or not working$(NC)"
	@echo ""
	@echo "$(YELLOW)Python path:$(NC)"
	@which $(PYTHON) || echo "$(RED)Python path not found$(NC)"

.PHONY: version
version: ## Show current version
	@echo "$(VERSION)"

# Quick commands
.PHONY: check
check: lint test ## Check alias (lint + test)

# Default target
.DEFAULT_GOAL := help
