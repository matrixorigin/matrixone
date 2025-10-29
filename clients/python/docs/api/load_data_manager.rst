Load Data Manager
=================

LoadDataManager
---------------

Pandas-style data loading manager for synchronous operations.

Provides ``read_csv()``, ``read_json()``, and ``read_parquet()`` methods for loading
data files into MatrixOne tables with pandas-compatible parameter naming.

.. autoclass:: matrixone.load_data.LoadDataManager
   :members:
   :undoc-members:
   :show-inheritance:

AsyncLoadDataManager
--------------------

Pandas-style data loading manager for asynchronous operations.

Provides async versions of all loading methods from LoadDataManager for non-blocking
data loading operations.

.. autoclass:: matrixone.load_data.AsyncLoadDataManager
   :members:
   :undoc-members:
   :show-inheritance:

Enumerations
------------

LoadDataFormat
~~~~~~~~~~~~~~

Supported file formats for data loading.

.. autoclass:: matrixone.load_data.LoadDataFormat
   :members:
   :undoc-members:
   :show-inheritance:

CompressionFormat
~~~~~~~~~~~~~~~~~

Supported compression formats for data files.

.. autoclass:: matrixone.load_data.CompressionFormat
   :members:
   :undoc-members:
   :show-inheritance:

JsonDataStructure
~~~~~~~~~~~~~~~~~

JSON data structure types (for compatibility, prefer using ``orient`` parameter in ``read_json()``).

.. autoclass:: matrixone.load_data.JsonDataStructure
   :members:
   :undoc-members:
   :show-inheritance:
