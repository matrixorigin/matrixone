"""
Setup script for MatrixOne Python SDK
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="matrixone",
    version="0.1.0",
    author="MatrixOne Team",
    author_email="support@matrixorigin.cn",
    description="A high-level Python SDK for MatrixOne database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/matrixorigin/matrixone",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.10",
    install_requires=[
        "PyMySQL>=1.0.2",
        "SQLAlchemy>=2.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=1.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "matrixone-test=test_basic:test_basic_connection",
        ],
    },
)
