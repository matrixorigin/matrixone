MatrixOne Python SDK Documentation
==================================

Welcome to the MatrixOne Python SDK documentation!

The MatrixOne Python SDK provides a high-level interface for MatrixOne database operations,
including SQLAlchemy-like interface, snapshot management, PITR, restore operations,
table cloning, and mo-ctl integration.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   quickstart
   orm_guide
   vector_guide
   fulltext_guide
   orm_vector_search_guide
   best_practices
   api/index
   examples
   contributing

Features
--------

* 🚀 **High Performance**: Optimized for MatrixOne database operations
* 🔄 **Async Support**: Full async/await support with AsyncClient
* 📸 **Snapshot Management**: Create and manage database snapshots
* ⏰ **Point-in-Time Recovery**: PITR functionality for data recovery
* 🔄 **Table Cloning**: Clone databases and tables efficiently
* 👥 **Account Management**: User and role management
* 📊 **Pub/Sub**: Publication and subscription support
* 🔧 **Version Management**: Automatic backend version detection and compatibility checking
* 🛡️ **Type Safety**: Full type hints support
* 📚 **SQLAlchemy Integration**: Seamless SQLAlchemy integration

Quick Start
-----------

.. code-block:: python

   from matrixone import Client

   # Create and connect to MatrixOne
   client = Client()
   client.connect(
       host='localhost',
       port=6001,
       user='root',
       password='111',
       database='test'
   )

   # Execute queries
   result = client.execute("SELECT 1 as test")
   print(result.fetchall())

   # Get backend version (auto-detected)
   version = client.get_backend_version()
   print(f"MatrixOne version: {version}")

   client.disconnect()

Installation
------------

.. code-block:: bash

   pip install matrixone-python-sdk

For development installation, see the :doc:`installation` page.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
