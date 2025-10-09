#!/usr/bin/env python3

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

"""
Pytest configuration for offline tests

This file manages Mock settings to prevent conflicts between different test files.
"""

import pytest
import sys
from unittest.mock import Mock


def pytest_configure(config):
    """Configure pytest with offline test settings"""
    config.addinivalue_line("markers", "offline: mark test as offline unit test")
    config.addinivalue_line("markers", "vector: mark test as vector-related test")


def pytest_runtest_setup(item):
    """Setup for each test - restore SQLAlchemy before vector tests"""
    # Check if this is a vector test by looking at the file path
    test_path = str(item.fspath)
    is_vector_test = any(
        keyword in test_path
        for keyword in [
            'test_vector_type.py',
            'test_table_builder.py',
            'test_case_sensitivity.py',
            'test_sqlalchemy_vector_integration.py',
        ]
    )

    if is_vector_test:
        # For vector tests, ensure we have real SQLAlchemy
        _restore_sqlalchemy_for_vector_tests()


def _restore_sqlalchemy_for_vector_tests():
    """Restore real SQLAlchemy modules for vector tests"""
    # List of SQLAlchemy modules that might be mocked
    sqlalchemy_modules = [
        'sqlalchemy',
        'sqlalchemy.engine',
        'sqlalchemy.orm',
        'sqlalchemy.ext',
        'sqlalchemy.ext.asyncio',
    ]

    for module_name in sqlalchemy_modules:
        # If the module is currently mocked, remove it to allow real import
        if module_name in sys.modules and isinstance(sys.modules[module_name], Mock):
            del sys.modules[module_name]

    # Force reload of SQLAlchemy modules
    try:
        import sqlalchemy
        import sqlalchemy.engine
        import sqlalchemy.orm
        import sqlalchemy.ext
        import sqlalchemy.ext.asyncio
    except ImportError:
        pass  # If import fails, that's ok
