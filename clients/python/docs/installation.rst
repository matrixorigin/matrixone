Installation
============

The MatrixOne Python SDK can be installed using pip.

Production Installation
-----------------------

.. code-block:: bash

   pip install matrixone-python-sdk

Using Virtual Environment (Recommended)
---------------------------------------

.. code-block:: bash

   # Create virtual environment
   python -m venv venv

   # Activate virtual environment
   # On macOS/Linux:
   source venv/bin/activate
   # On Windows:
   # venv\Scripts\activate

   # Install MatrixOne SDK
   pip install matrixone-python-sdk

   # Verify installation
   python -c "import matrixone; print('MatrixOne SDK installed successfully')"

Using Conda
-----------

.. code-block:: bash

   # Create conda environment
   conda create -n matrixone python=3.10
   conda activate matrixone

   # Install MatrixOne SDK
   pip install matrixone-python-sdk

Development Installation
------------------------

For development and contributing to the SDK:

.. code-block:: bash

   git clone https://github.com/matrixorigin/matrixone.git
   cd matrixone/clients/python-wrapper

   # Create virtual environment
   python -m venv venv
   source venv/bin/activate  # On macOS/Linux

   # Install development dependencies
   make dev-setup

   # Or manual setup
   pip install -e .

Requirements
------------

* Python 3.8 or higher
* PyMySQL >= 1.0.0
* aiomysql >= 0.1.0
* SQLAlchemy >= 1.4.0
* typing-extensions >= 4.0.0
* python-dateutil >= 2.8.0

Optional Dependencies
---------------------

For development:

* pytest >= 6.0
* pytest-asyncio >= 0.18.0
* black >= 22.0
* flake8 >= 4.0
* mypy >= 0.950
* sphinx >= 4.0
* sphinx-rtd-theme >= 1.0
