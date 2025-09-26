Contributing
============

We welcome contributions to the MatrixOne Python SDK! This document provides guidelines for contributing to the project.

Getting Started
---------------

1. Fork the repository on GitHub
2. Clone your fork locally::

   git clone https://github.com/matrixorigin/matrixone
   cd matrixone/clients/python-wrapper

3. Set up development environment::

   # Create virtual environment
   python -m venv venv
   source venv/bin/activate  # On macOS/Linux

   # Install development dependencies
   make dev-setup

4. Create a feature branch::

   git checkout -b feature/your-feature-name

Development Workflow
--------------------

Code Style
~~~~~~~~~~

We use several tools to maintain code quality:

* **Black**: Code formatting
* **isort**: Import sorting
* **flake8**: Linting
* **mypy**: Type checking

Run all checks::

   make pre-commit-full

Or run individual checks::

   make format    # Format code
   make lint      # Run linting
   make type-check # Type checking

Testing
~~~~~~~

Write tests for new features and bug fixes::

   # Run all tests
   make test

   # Run specific test file
    python -m pytest test_client.py -v

   # Run with coverage
    python -m pytest --cov=matrixone test_*.py

Documentation
~~~~~~~~~~~~~

Update documentation for new features::

   # Generate documentation
   make docs

   # Serve documentation locally
   make docs-serve

Documentation Guidelines
~~~~~~~~~~~~~~~~~~~~~~~~

* Use Google-style docstrings for all public methods
* Include examples in docstrings where appropriate
* Update API documentation in ``docs/api/``
* Add examples to ``docs/examples.rst``

Example docstring::

   def create_snapshot(self, name: str, level: str) -> Snapshot:
       """
       Create a new database snapshot.

       Args:
           name (str): Name of the snapshot
           level (str): Snapshot level ('cluster', 'database', 'table')

       Returns:
           Snapshot: Created snapshot object

       Raises:
           SnapshotError: If snapshot creation fails
           VersionError: If feature not supported

       Examples:
           >>> snapshot = client.snapshots.create('backup', 'cluster')
           >>> print(snapshot.name)
           backup
       """
       # Implementation here

Pull Request Process
--------------------

1. Ensure all tests pass::

   make test

2. Run code quality checks::

   make pre-commit-full

3. Update documentation if needed::

   make docs

4. Commit your changes::

   git add .
   git commit -m "Add feature: brief description"

5. Push to your fork::

   git push origin feature/your-feature-name

6. Create a Pull Request on GitHub

Pull Request Guidelines
~~~~~~~~~~~~~~~~~~~~~~~

* Provide a clear description of changes
* Reference any related issues
* Include tests for new functionality
* Update documentation as needed
* Ensure CI passes

Issue Reporting
---------------

When reporting issues, please include:

* Python version
* MatrixOne version
* Operating system
* Steps to reproduce
* Expected vs actual behavior
* Error messages/logs

Code Review Process
-------------------

* All PRs require review from maintainers
* Address review feedback promptly
* Keep PRs focused and reasonably sized
* Squash commits before merging

Release Process
---------------

Releases are managed by maintainers:

1. Update version in ``pyproject.toml``
2. Update ``CHANGELOG.md``
3. Create release tag
4. Build and publish to PyPI::

   make publish

Development Tools
-----------------

Useful commands for development::

   make help              # Show all available commands
   make check-env         # Check Python environment
   make clean             # Clean build artifacts
   make build             # Build package
   make examples          # Run example scripts

Environment Variables
~~~~~~~~~~~~~~~~~~~~~

You can override default Python paths::

   export PYTHON=/path/to/python3
   export PIP=/path/to/pip
   make test

Or pass them directly::

   make test PYTHON=/path/to/python3

Community Guidelines
--------------------

* Be respectful and inclusive
* Help others learn and grow
* Follow the code of conduct
* Ask questions in discussions or issues

Getting Help
------------

* Check existing issues and discussions
* Join our community channels
* Ask questions in GitHub discussions
* Contact maintainers for urgent issues

Thank you for contributing to MatrixOne Python SDK! 🚀
