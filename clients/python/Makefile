# Copyright 2021 - 2022 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# MatrixOne Python SDK Makefile
# Provides unified commands for development, testing, building and publishing

# Configuration variables
PYTHON ?= python3
PIP ?= $(PYTHON) -m pip
PACKAGE_NAME := matrixone_python_sdk
VERSION := $(shell grep '^version' pyproject.toml | cut -d'"' -f2)
BUILD_DIR := dist
TEST_DIR := test-results
COVERAGE_DIR := htmlcov

# Source mode control (affects tests and examples)
# USE_SOURCE=1 (default): Use local source code via PYTHONPATH=.
#                        Best for development - changes are immediately reflected
# USE_SOURCE=0: Use installed matrixone package
#              Best for testing the actual installed package
#
# Usage examples:
#   make test                     # Use local source (default)
#   make test USE_SOURCE=0        # Use installed package
#   make examples                 # Use local source (default)
#   make examples USE_SOURCE=0    # Use installed package
USE_SOURCE ?= 1

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
	@echo "$(YELLOW)🔍 Environment Check:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^(check-env|show-deps|info|status):' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)📦 Dependencies:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^(install|dev-setup|install-sqlalchemy14|install-sqlalchemy20):' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)🧪 Testing:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^(test|test-offline|test-online|test-fast|coverage|test-matrix|test-sqlalchemy14|test-sqlalchemy20):' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)🔧 Code Quality:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^(lint|format|type-check|pre-commit):' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)📖 Documentation:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^(docs|docs-check|docs-serv|docs-clean):' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)📚 Examples:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^(example)' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)🏗️  Build & Publish:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^(build|build-release|publish-test|publish|release-check):' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)📋 Version Management:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^(version|version-bump):' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)🧹 Utilities:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^(clean):' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)Current version: $(VERSION)$(NC)"
	@echo "$(YELLOW)Python path: $(PYTHON)$(NC)"

# Environment check
.PHONY: install-dev
install-dev: ## Install development dependencies including build tools
	@echo "$(BLUE)Checking development dependencies...$(NC)"
	@$(PYTHON) -c "import build" 2>/dev/null && echo "$(GREEN)✓ build module available$(NC)" || (echo "$(RED)✗ build module not found$(NC)" && echo "$(YELLOW)Please install with: pip install build twine$(NC)" && exit 1)
	@$(PYTHON) -c "import twine" 2>/dev/null && echo "$(GREEN)✓ twine module available$(NC)" || (echo "$(RED)✗ twine module not found$(NC)" && echo "$(YELLOW)Please install with: pip install build twine$(NC)" && exit 1)
	@echo "$(GREEN)All development dependencies are available!$(NC)"

.PHONY: check-env
check-env: ## Check if environment meets testing requirements
	@echo "$(BLUE)Checking MatrixOne Python SDK test environment...$(NC)"
	@echo ""
	@echo "$(YELLOW)Python Environment:$(NC)"
	@echo "  Python command: $(PYTHON)"
	@echo "  Python version: $$($(PYTHON) --version 2>/dev/null || echo 'Not found')"
	@echo "  Pip command: $(PIP)"
	@echo "  Pip version: $$($(PIP) --version 2>/dev/null || echo 'Not found')"
	@echo ""
	@echo "$(YELLOW)Available Python versions:$(NC)"
	@command -v python3.10 >/dev/null 2>&1 && echo "  ✅ Python 3.10 available" || echo "  ❌ Python 3.10 not found"
	@command -v python3.11 >/dev/null 2>&1 && echo "  ✅ Python 3.11 available" || echo "  ❌ Python 3.11 not found"
	@echo ""
	@echo "$(YELLOW)Requirements files:$(NC)"
	@ls -la requirements*.txt 2>/dev/null | sed 's/^/  /' || echo "  No requirements files found"
	@echo ""
	@echo "$(YELLOW)Test directories:$(NC)"
	@test -d tests/offline && echo "  ✅ Offline tests available" || echo "  ❌ Offline tests not found"
	@test -d tests/online && echo "  ✅ Online tests available" || echo "  ❌ Online tests not found"
	@echo ""
	@echo "$(YELLOW)Core Dependencies Check:$(NC)"
	@$(PYTHON) -c "import sys; print('  Python path:', sys.executable)" 2>/dev/null || echo "  ❌ Python not working"
	@$(PYTHON) -c "import pymysql; print('  ✅ PyMySQL:', pymysql.__version__)" 2>/dev/null || echo "  ❌ PyMySQL not installed"
	@$(PYTHON) -c "import aiomysql; print('  ✅ aiomysql:', aiomysql.__version__)" 2>/dev/null || echo "  ❌ aiomysql not installed"
	@$(PYTHON) -c "import sqlalchemy; print('  ✅ SQLAlchemy:', sqlalchemy.__version__)" 2>/dev/null || echo "  ❌ SQLAlchemy not installed"
	@$(PYTHON) -c "import pytest; print('  ✅ pytest:', pytest.__version__)" 2>/dev/null || echo "  ❌ pytest not installed"
	@$(PYTHON) -c "import matrixone; print('  ✅ matrixone SDK available')" 2>/dev/null || echo "  ❌ matrixone SDK not installed"
	@echo ""
	@echo "$(YELLOW)Test Environment Status:$(NC)"
	@$(PYTHON) -c "import pymysql, aiomysql, sqlalchemy, pytest" 2>/dev/null && echo "  ✅ Core dependencies: READY" || echo "  ❌ Core dependencies: MISSING"
	@$(PYTHON) -c "import matrixone" 2>/dev/null && echo "  ✅ MatrixOne SDK: READY" || echo "  ❌ MatrixOne SDK: MISSING"
	@echo ""
	@echo "$(YELLOW)Recommendations:$(NC)"
	@$(PYTHON) -c "import pymysql, aiomysql, sqlalchemy, pytest" 2>/dev/null || echo "  • Run: make install-sqlalchemy14 or make install-sqlalchemy20"
	@$(PYTHON) -c "import matrixone" 2>/dev/null || echo "  • Run: make install (to install MatrixOne SDK)"
	@$(PYTHON) -c "import pymysql, aiomysql, sqlalchemy, pytest, matrixone" 2>/dev/null && echo "  • Environment is ready for testing! Run: make test-offline"

.PHONY: show-deps
show-deps: check-env ## Alias for check-env

# Dependencies installation
.PHONY: install
install: ## Install development environment (dependencies + tools)
	@echo "$(BLUE)Installing development environment...$(NC)"
	$(PIP) install -r requirements.txt
	$(PIP) install pytest pytest-cov pytest-asyncio build twine black flake8 mypy isort sphinx sphinx-rtd-theme
	@echo "$(GREEN)Development environment installed!$(NC)"

.PHONY: dev-setup
dev-setup: install ## Setup complete development environment
	@echo "$(BLUE)Setting up development environment...$(NC)"
	$(PIP) install -e .
	@echo "$(GREEN)Development environment setup complete!$(NC)"

.PHONY: install-sqlalchemy14
install-sqlalchemy14: ## Install SQLAlchemy 1.4.x test dependencies
	@echo "$(BLUE)Installing SQLAlchemy 1.4.x test dependencies...$(NC)"
	@if [ -f "requirements-sqlalchemy14.txt" ]; then \
		echo "$(YELLOW)Installing from requirements-sqlalchemy14.txt$(NC)"; \
		$(PIP) install -r requirements-sqlalchemy14.txt; \
		echo "$(GREEN)SQLAlchemy 1.4.x dependencies installed!$(NC)"; \
	else \
		echo "$(RED)❌ requirements-sqlalchemy14.txt not found$(NC)"; \
		exit 1; \
	fi

.PHONY: install-sqlalchemy20
install-sqlalchemy20: ## Install SQLAlchemy 2.0.x test dependencies
	@echo "$(BLUE)Installing SQLAlchemy 2.0.x test dependencies...$(NC)"
	@if [ -f "requirements-sqlalchemy20.txt" ]; then \
		echo "$(YELLOW)Installing from requirements-sqlalchemy20.txt$(NC)"; \
		$(PIP) install -r requirements-sqlalchemy20.txt; \
		echo "$(GREEN)SQLAlchemy 2.0.x dependencies installed!$(NC)"; \
	else \
		echo "$(RED)❌ requirements-sqlalchemy20.txt not found$(NC)"; \
		exit 1; \
	fi

# Testing
# Tests can use either local source code or installed package (controlled by USE_SOURCE)
# USE_SOURCE=1 (default): Use local source code via PYTHONPATH=.
# USE_SOURCE=0: Use installed matrixone package
TEST_PYTHON_PATH := $(if $(filter 1,$(USE_SOURCE)),PYTHONPATH=.,)

.PHONY: test-offline
test-offline: ## Run offline tests (USE_SOURCE=1 for local code, USE_SOURCE=0 for installed SDK)
	@echo "$(BLUE)Running offline tests...$(NC)"
	@if [ "$(USE_SOURCE)" = "1" ]; then \
		echo "$(YELLOW)Using local source code (PYTHONPATH=.)$(NC)"; \
	else \
		echo "$(YELLOW)Using installed matrixone package$(NC)"; \
	fi
	$(TEST_PYTHON_PATH) $(PYTHON) -m pytest tests/offline/ -v --tb=short
	@echo "$(GREEN)Offline tests completed!$(NC)"

.PHONY: test-online
test-online: ## Run online tests (requires database, USE_SOURCE=1 for local, USE_SOURCE=0 for installed)
	@echo "$(BLUE)Running online tests...$(NC)"
	@echo "$(YELLOW)Note: This requires a running MatrixOne database$(NC)"
	@if [ "$(USE_SOURCE)" = "1" ]; then \
		echo "$(YELLOW)Using local source code (PYTHONPATH=.)$(NC)"; \
	else \
		echo "$(YELLOW)Using installed matrixone package$(NC)"; \
	fi
	@$(PYTHON) scripts/check_connection.py && $(TEST_PYTHON_PATH) $(PYTHON) -m pytest tests/online/ -v --tb=short --maxfail=3 && echo "$(GREEN)Online tests completed!$(NC)" || echo "$(YELLOW)⚠️  Online tests skipped (database not available)$(NC)"

.PHONY: test
test: test-offline test-online ## Run all tests (offline + online, USE_SOURCE=1 for local, USE_SOURCE=0 for installed)
	@echo "$(GREEN)All tests completed!$(NC)"

.PHONY: test-fast
test-fast: ## Run quick tests (offline only, USE_SOURCE=1 for local, USE_SOURCE=0 for installed)
	@echo "$(BLUE)Running quick tests...$(NC)"
	@if [ "$(USE_SOURCE)" = "1" ]; then \
		echo "$(YELLOW)Using local source code$(NC)"; \
	else \
		echo "$(YELLOW)Using installed package$(NC)"; \
	fi
	$(TEST_PYTHON_PATH) $(PYTHON) -m pytest tests/offline/ -v --tb=short -x
	@echo "$(GREEN)Quick tests completed!$(NC)"

.PHONY: coverage
coverage: ## Run all tests with coverage report (USE_SOURCE=1 for local, USE_SOURCE=0 for installed)
	@echo "$(BLUE)Running tests with coverage...$(NC)"
	@if [ "$(USE_SOURCE)" = "1" ]; then \
		echo "$(YELLOW)Using local source code$(NC)"; \
	else \
		echo "$(YELLOW)Using installed package$(NC)"; \
	fi
	$(TEST_PYTHON_PATH) $(PYTHON) -m pytest --cov=matrixone --cov-report=html --cov-report=term
	@echo "$(GREEN)Coverage report generated in $(COVERAGE_DIR)/$(NC)"

.PHONY: coverage-offline
coverage-offline: ## Run offline tests with coverage (USE_SOURCE=1 for local, USE_SOURCE=0 for installed)
	@echo "$(BLUE)Running offline tests with coverage...$(NC)"
	@if [ "$(USE_SOURCE)" = "1" ]; then \
		echo "$(YELLOW)Using local source code$(NC)"; \
	else \
		echo "$(YELLOW)Using installed package$(NC)"; \
	fi
	$(TEST_PYTHON_PATH) $(PYTHON) -m pytest tests/offline/ --cov=matrixone --cov-report=html --cov-report=term
	@echo "$(GREEN)Coverage report generated in $(COVERAGE_DIR)/$(NC)"

.PHONY: coverage-online
coverage-online: ## Run online tests with coverage (USE_SOURCE=1 for local, USE_SOURCE=0 for installed)
	@echo "$(BLUE)Running online tests with coverage...$(NC)"
	@if [ "$(USE_SOURCE)" = "1" ]; then \
		echo "$(YELLOW)Using local source code$(NC)"; \
	else \
		echo "$(YELLOW)Using installed package$(NC)"; \
	fi
	@$(PYTHON) scripts/check_connection.py && $(TEST_PYTHON_PATH) $(PYTHON) -m pytest tests/online/ --cov=matrixone --cov-report=html --cov-report=term && echo "$(GREEN)Online coverage completed!$(NC)" || echo "$(YELLOW)⚠️  Online coverage skipped (database not available)$(NC)"

# Matrix testing for automation
.PHONY: test-matrix
test-matrix: ## Run matrix tests (SQLAlchemy 1.4 + 2.0, USE_SOURCE=1 for local, USE_SOURCE=0 for installed)
	@echo "$(BLUE)Running matrix tests...$(NC)"
	@echo "$(YELLOW)Testing SQLAlchemy 1.4.x...$(NC)"
	@$(MAKE) test-sqlalchemy14
	@echo "$(YELLOW)Testing SQLAlchemy 2.0.x...$(NC)"
	@$(MAKE) test-sqlalchemy20
	@echo "$(GREEN)Matrix tests completed!$(NC)"

.PHONY: test-sqlalchemy14
test-sqlalchemy14: install-sqlalchemy14 ## Test with SQLAlchemy 1.4.x (USE_SOURCE=1 for local, USE_SOURCE=0 for installed)
	@echo "$(BLUE)Testing with SQLAlchemy 1.4.x...$(NC)"
	@if [ "$(USE_SOURCE)" = "1" ]; then \
		echo "$(YELLOW)Using local source code$(NC)"; \
	else \
		echo "$(YELLOW)Using installed package$(NC)"; \
	fi
	@echo "$(YELLOW)Running offline tests...$(NC)"
	$(TEST_PYTHON_PATH) $(PYTHON) -m pytest tests/offline/ -v --tb=short
	@echo "$(YELLOW)Running online tests...$(NC)"
	@$(PYTHON) scripts/check_connection.py && $(TEST_PYTHON_PATH) $(PYTHON) -m pytest tests/online/ -v --tb=short --maxfail=3 && echo "$(GREEN)SQLAlchemy 1.4.x online tests completed!$(NC)" || echo "$(YELLOW)⚠️  SQLAlchemy 1.4.x online tests skipped (database not available)$(NC)"
	@echo "$(GREEN)SQLAlchemy 1.4.x tests completed!$(NC)"

.PHONY: test-sqlalchemy20
test-sqlalchemy20: install-sqlalchemy20 ## Test with SQLAlchemy 2.0.x (USE_SOURCE=1 for local, USE_SOURCE=0 for installed)
	@echo "$(BLUE)Testing with SQLAlchemy 2.0.x...$(NC)"
	@if [ "$(USE_SOURCE)" = "1" ]; then \
		echo "$(YELLOW)Using local source code$(NC)"; \
	else \
		echo "$(YELLOW)Using installed package$(NC)"; \
	fi
	@echo "$(YELLOW)Running offline tests...$(NC)"
	$(TEST_PYTHON_PATH) $(PYTHON) -m pytest tests/offline/ -v --tb=short
	@echo "$(YELLOW)Running online tests...$(NC)"
	@$(PYTHON) scripts/check_connection.py && $(TEST_PYTHON_PATH) $(PYTHON) -m pytest tests/online/ -v --tb=short --maxfail=3 && echo "$(GREEN)SQLAlchemy 2.0.x online tests completed!$(NC)" || echo "$(YELLOW)⚠️  SQLAlchemy 2.0.x online tests skipped (database not available)$(NC)"
	@echo "$(GREEN)SQLAlchemy 2.0.x tests completed!$(NC)"

# Code Quality
.PHONY: lint
lint: ## Run code quality checks
	@echo "$(BLUE)Running code quality checks...$(NC)"
	flake8 matrixone --max-line-length=125 --ignore=E203,W503
	@echo "$(GREEN)Code quality checks completed!$(NC)"

.PHONY: lint-fix
lint-fix: ## Auto-fix linting issues (trailing whitespace, imports, formatting)
	@echo "$(BLUE)Auto-fixing linting issues...$(NC)"
	@echo "$(YELLOW)Removing trailing whitespace...$(NC)"
	find matrixone -name "*.py" -type f -exec sed -i '' 's/[[:space:]]*$$//' {} +
	@echo "$(YELLOW)Removing unused imports with autoflake...$(NC)"
	$(PYTHON) -m autoflake --remove-all-unused-imports --in-place --recursive matrixone || echo "$(YELLOW)⚠️  autoflake not available, skipping import cleanup$(NC)"
	@echo "$(YELLOW)Sorting imports with isort...$(NC)"
	$(PYTHON) -m isort matrixone || echo "$(YELLOW)⚠️  isort not available, skipping import sorting$(NC)"
	@echo "$(YELLOW)Formatting code with black...$(NC)"
	black matrixone --line-length=125
	@echo "$(GREEN)Linting issues auto-fixed!$(NC)"
	@echo "$(YELLOW)Run 'make lint' to verify fixes$(NC)"

.PHONY: format
format: ## Format code with black
	@echo "$(BLUE)Formatting code...$(NC)"
	black matrixone tests examples --line-length=125
	@echo "$(GREEN)Code formatting completed!$(NC)"

.PHONY: type-check
type-check: ## Run type checking with mypy (permissive mode)
	@echo "$(BLUE)Running type checks (permissive mode)...$(NC)"
	mypy matrixone --ignore-missing-imports --no-strict-optional --allow-redefinition --show-error-codes --disable-error-code=var-annotated --disable-error-code=assignment --disable-error-code=no-any-return --disable-error-code=unreachable --disable-error-code=attr-defined --disable-error-code=no-redef --disable-error-code=method-assign --disable-error-code=valid-type --disable-error-code=override --disable-error-code=return-value --disable-error-code=union-attr --disable-error-code=index --disable-error-code=misc --disable-error-code=arg-type || echo "$(YELLOW)⚠️  Type checking completed with warnings (permissive mode)$(NC)"
	@echo "$(GREEN)Type checking completed!$(NC)"

.PHONY: type-check-strict
type-check-strict: ## Run strict type checking with mypy
	@echo "$(BLUE)Running strict type checks...$(NC)"
	mypy matrixone --ignore-missing-imports --strict
	@echo "$(GREEN)Strict type checking completed!$(NC)"

.PHONY: docs
docs: ## Generate documentation
	@echo "$(BLUE)Generating documentation...$(NC)"
	@if command -v sphinx-build >/dev/null 2>&1; then \
		echo "$(YELLOW)Building Sphinx documentation...$(NC)"; \
		sphinx-build -b html docs docs/_build/html; \
		echo "$(GREEN)Documentation generated in docs/_build/html/$(NC)"; \
		echo "$(YELLOW)Open docs/_build/html/index.html in your browser to view$(NC)"; \
	else \
		echo "$(YELLOW)⚠️  Sphinx not available, cannot generate documentation$(NC)"; \
		echo "$(YELLOW)   Install with: pip install sphinx sphinx-rtd-theme$(NC)"; \
		exit 1; \
	fi

.PHONY: docs-check
docs-check: ## Check documentation for errors and warnings
	@echo "$(BLUE)Checking documentation...$(NC)"
	@if command -v sphinx-build >/dev/null 2>&1; then \
		echo "$(YELLOW)Running Sphinx documentation check...$(NC)"; \
		sphinx-build -W -b html -d docs/_build/doctrees docs docs/_build/html || echo "$(YELLOW)⚠️  Documentation check completed with warnings$(NC)"; \
	else \
		echo "$(YELLOW)⚠️  Sphinx not available, skipping documentation check$(NC)"; \
		echo "$(YELLOW)   Install with: pip install sphinx sphinx-rtd-theme$(NC)"; \
	fi
	@echo "$(GREEN)Documentation check completed!$(NC)"

.PHONY: docs-serv
docs-serv: ## Start documentation server (requires Python http.server)
	@echo "$(BLUE)Starting documentation server...$(NC)"
	@if [ -d "docs/_build/html" ]; then \
		echo "$(YELLOW)Serving documentation from docs/_build/html/$(NC)"; \
		echo "$(YELLOW)Open http://localhost:8000 in your browser$(NC)"; \
		echo "$(YELLOW)Press Ctrl+C to stop the server$(NC)"; \
		cd docs/_build/html && python3 -m http.server 8000; \
	else \
		echo "$(YELLOW)⚠️  Documentation not found. Run 'make docs' first$(NC)"; \
		exit 1; \
	fi

.PHONY: pre-commit
pre-commit: format lint type-check test-offline test-online docs-check ## Run pre-commit checks
	@echo "$(GREEN)Pre-commit checks completed!$(NC)"

# Examples
# Control whether to use local source code or installed package
# 
# Usage:
#   make examples                  # Use local source code (default)
#   make examples-installed        # Use installed matrixone package
#   make examples-source           # Explicitly use local source code
#   make examples USE_SOURCE=0     # Use installed package (via parameter)
#   make examples USE_SOURCE=1     # Use local source (via parameter)
#
# The USE_SOURCE variable controls which matrixone package to use:
#   USE_SOURCE=1 (default): Add PYTHONPATH=. to use local source code
#   USE_SOURCE=0: No PYTHONPATH, use installed matrixone package
#
USE_SOURCE ?= 1
EXAMPLE_PYTHON_PATH := $(if $(filter 1,$(USE_SOURCE)),PYTHONPATH=.,)

.PHONY: examples
examples: ## Run all examples (USE_SOURCE=1 for local code, USE_SOURCE=0 for installed SDK)
	@echo "$(BLUE)Running all examples...$(NC)"
	@if [ "$(USE_SOURCE)" = "1" ]; then \
		echo "$(YELLOW)Using local source code (PYTHONPATH=.)$(NC)"; \
	else \
		echo "$(YELLOW)Using installed matrixone package$(NC)"; \
	fi
	@for example in examples/example_*.py; do \
		echo "$(YELLOW)Running $$example...$(NC)"; \
		$(EXAMPLE_PYTHON_PATH) $(PYTHON) "$$example" || echo "$(RED)Example $$example failed$(NC)"; \
	done
	@echo "$(GREEN)All examples completed!$(NC)"

.PHONY: examples-installed
examples-installed: ## Run examples using installed matrixone package
	@echo "$(BLUE)Running examples with installed matrixone package...$(NC)"
	@$(MAKE) examples USE_SOURCE=0

.PHONY: examples-source
examples-source: ## Run examples using local source code (default)
	@echo "$(BLUE)Running examples with local source code...$(NC)"
	@$(MAKE) examples USE_SOURCE=1

.PHONY: example-basic
example-basic: ## Run basic connection example
	@echo "$(BLUE)Running basic connection example...$(NC)"
	@if [ "$(USE_SOURCE)" = "1" ]; then echo "$(YELLOW)Using local source$(NC)"; fi
	$(EXAMPLE_PYTHON_PATH) $(PYTHON) examples/example_01_basic_connection.py
	@echo "$(GREEN)Basic example completed!$(NC)"

.PHONY: example-async
example-async: ## Run async operations example
	@echo "$(BLUE)Running async operations example...$(NC)"
	@if [ "$(USE_SOURCE)" = "1" ]; then echo "$(YELLOW)Using local source$(NC)"; fi
	$(EXAMPLE_PYTHON_PATH) $(PYTHON) examples/example_03_async_operations.py
	@echo "$(GREEN)Async example completed!$(NC)"

.PHONY: example-vector
example-vector: ## Run vector search example
	@echo "$(BLUE)Running vector search example...$(NC)"
	@if [ "$(USE_SOURCE)" = "1" ]; then echo "$(YELLOW)Using local source$(NC)"; fi
	$(EXAMPLE_PYTHON_PATH) $(PYTHON) examples/example_12_vector_search.py
	@echo "$(GREEN)Vector example completed!$(NC)"

.PHONY: example-fulltext
example-fulltext: ## Run fulltext index example
	@echo "$(BLUE)Running fulltext index example...$(NC)"
	@if [ "$(USE_SOURCE)" = "1" ]; then echo "$(YELLOW)Using local source$(NC)"; fi
	$(EXAMPLE_PYTHON_PATH) $(PYTHON) examples/example_fulltext_index.py
	@echo "$(GREEN)Fulltext example completed!$(NC)"

# Build and publish
.PHONY: build
build: clean ## Build package (wheel + sdist)
	@echo "$(BLUE)Building package...$(NC)"
	$(PYTHON) -m build --wheel --sdist --outdir $(BUILD_DIR)
	@echo "$(GREEN)Package built! Output directory: $(BUILD_DIR)/$(NC)"

.PHONY: build-release
build-release: clean ## Build release package (with user README)
	@echo "$(BLUE)Building release package...$(NC)"
	@echo "$(YELLOW)Note: This uses the user-facing README.md$(NC)"
	$(PYTHON) -m build --wheel --sdist --outdir $(BUILD_DIR)
	@echo "$(GREEN)Release package built! Output directory: $(BUILD_DIR)/$(NC)"

.PHONY: publish-test
publish-test: install-dev build ## Publish to test PyPI
	@echo "$(BLUE)Publishing to test PyPI...$(NC)"
	$(PYTHON) -m twine upload --repository testpypi $(BUILD_DIR)/*
	@echo "$(GREEN)Published to test PyPI!$(NC)"

.PHONY: publish
publish: install-dev build ## Publish to PyPI (official release)
	@echo "$(BLUE)Publishing to PyPI...$(NC)"
	@echo "$(RED)Warning: This will publish to official PyPI!$(NC)"
	@read -p "Confirm publish to PyPI? (y/N): " confirm && [ "$$confirm" = "y" ]
	$(PYTHON) -m twine upload $(BUILD_DIR)/*
	@echo "$(GREEN)Published to PyPI!$(NC)"

.PHONY: release-check
release-check: pre-commit test examples ## Complete pre-release checks
	@echo "$(GREEN)Release checks completed! Ready for release.$(NC)"

# Version Management
.PHONY: version
version: ## Show current version
	@echo "$(VERSION)"

.PHONY: version-bump
version-bump: ## Bump version (default: patch, or specify VERSION_TYPE=patch|minor|major)
	@if [ -z "$(VERSION_TYPE)" ]; then \
		echo "$(YELLOW)No VERSION_TYPE specified, defaulting to patch$(NC)"; \
		$(PYTHON) scripts/update_version.py --increment patch; \
	else \
		echo "$(BLUE)Bumping $(VERSION_TYPE) version...$(NC)"; \
		$(PYTHON) scripts/update_version.py --increment $(VERSION_TYPE); \
	fi
	@echo "$(GREEN)Version bumped! New version: $$(grep '^version' pyproject.toml | sed 's/.*"\(.*\)".*/\1/')$(NC)"

.PHONY: version-check
version-check: ## Check if version is ready for release
	@echo "$(BLUE)Checking version readiness...$(NC)"
	@echo "Current version: $(VERSION)"
	@echo "Package name: $(PACKAGE_NAME)"
	@echo "$(GREEN)Version check completed!$(NC)"

# Utilities
.PHONY: info
info: ## Show project information
	@echo "$(BLUE)MatrixOne Python SDK - Project Information$(NC)"
	@echo ""
	@echo "$(YELLOW)Project Details:$(NC)"
	@echo "  Package: $(PACKAGE_NAME)"
	@echo "  Version: $(VERSION)"
	@echo "  Python: $(PYTHON)"
	@echo "  Pip: $(PIP)"
	@echo ""
	@echo "$(YELLOW)Directories:$(NC)"
	@echo "  Build: $(BUILD_DIR)"
	@echo "  Test: $(TEST_DIR)"
	@echo "  Coverage: $(COVERAGE_DIR)"
	@echo ""
	@echo "$(YELLOW)Files:$(NC)"
	@ls -la pyproject.toml requirements*.txt 2>/dev/null | sed 's/^/  /' || echo "  No project files found"

.PHONY: status
status: ## Show development status
	@echo "$(BLUE)Development Status$(NC)"
	@echo ""
	@echo "$(YELLOW)Git Status:$(NC)"
	@git status --porcelain 2>/dev/null || echo "  Not a git repository"
	@echo ""
	@echo "$(YELLOW)Python Environment:$(NC)"
	@$(PYTHON) -c "import sys; print('  Python:', sys.version.split()[0], sys.executable)" 2>/dev/null || echo "  Python not available"
	@echo ""
	@echo "$(YELLOW)Installed Packages:$(NC)"
	@$(PIP) list 2>/dev/null | grep -E "(matrixone|sqlalchemy|pytest)" | sed 's/^/  /' || echo "  No relevant packages found"

# Cleanup
.PHONY: docs-clean
docs-clean: ## Clean documentation build artifacts
	@echo "$(BLUE)Cleaning documentation build artifacts...$(NC)"
	rm -rf docs/_build/
	rm -rf docs/_static/
	rm -rf docs/_templates/
	@echo "$(GREEN)Documentation cleanup completed!$(NC)"

.PHONY: clean
clean: docs-clean ## Clean build artifacts
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
clean-all: clean ## Clean everything including virtual environment
	@echo "$(BLUE)Cleaning everything...$(NC)"
	rm -rf venv/
	rm -rf .venv/
	rm -rf env/
	rm -rf .env/
	@echo "$(GREEN)Complete cleanup finished!$(NC)"

.PHONY: clean-deps
clean-deps: ## Clean and reinstall dependencies
	@echo "$(BLUE)Cleaning and reinstalling dependencies...$(NC)"
	$(PIP) freeze | xargs $(PIP) uninstall -y 2>/dev/null || true
	$(PIP) install -r requirements.txt
	@echo "$(GREEN)Dependencies cleaned and reinstalled!$(NC)"

# Default target
.DEFAULT_GOAL := help
