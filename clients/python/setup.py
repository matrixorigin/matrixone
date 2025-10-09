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
MatrixOne Python SDK Setup Script

This script configures the package for distribution via PyPI.
"""

import os
from setuptools import setup, find_packages

# Read the contents of README file
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Read version from __init__.py
def get_version():
    """Extract version from __init__.py"""
    with open(os.path.join(this_directory, 'matrixone', '__init__.py'), 'r', encoding='utf-8') as f:
        for line in f:
            if line.startswith('__version__'):
                return line.split('=')[1].strip().strip('"\'')
    return '1.0.0'

# Read requirements from requirements.txt if it exists
def get_requirements():
    """Read requirements from requirements.txt"""
    requirements = []
    requirements_file = os.path.join(this_directory, 'requirements.txt')
    if os.path.exists(requirements_file):
        with open(requirements_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    requirements.append(line)
    return requirements

setup(
    name="matrixone-python-sdk",
    version=get_version(),
    author="MatrixOne Team",
    author_email="dev@matrixone.cloud",
    description="A comprehensive Python SDK for MatrixOne database operations with vector search, fulltext search, and advanced features",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/matrixorigin/matrixone",
    project_urls={
        "Bug Reports": "https://github.com/matrixorigin/matrixone/issues",
        "Source": "https://github.com/matrixorigin/matrixone/tree/main/clients/python-wrapper",
        "Documentation": "https://github.com/matrixorigin/matrixone/tree/main/clients/python-wrapper#readme",
    },
    packages=find_packages(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    python_requires=">=3.8",
    install_requires=get_requirements(),
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-asyncio>=0.18.0",
            "pytest-cov>=3.0",
            "Faker>=10.0.0",
            "black>=22.0",
            "flake8>=4.0",
            "mypy>=0.950",
            "isort>=5.0",
            "sphinx>=4.0",
            "sphinx-rtd-theme>=1.0",
            "build>=0.8.0",
            "twine>=4.0.0",
        ],
        "test": [
            "pytest>=6.0",
            "pytest-asyncio>=0.18.0",
            "pytest-cov>=3.0",
            "Faker>=10.0.0",
        ],
    },
    keywords="matrixone, database, sql, python, sdk, sqlalchemy, async, fulltext, search, vector, similarity, ai, ml, embedding, hnsw, ivf",
    include_package_data=True,
    zip_safe=False,
    entry_points={
        "sqlalchemy.dialects": [
            "matrixone = matrixone.sqlalchemy_ext.dialect:MatrixOneDialect",
        ],
    },
)