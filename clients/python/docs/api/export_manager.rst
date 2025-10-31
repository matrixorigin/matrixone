Export Manager
==============

ExportManager
-------------

Pandas-style export manager for synchronous operations.

Provides ``to_csv()`` and ``to_jsonl()`` methods for exporting query results
to files or external stages.

.. autoclass:: matrixone.export.ExportManager
   :members:
   :undoc-members:
   :show-inheritance:

AsyncExportManager
------------------

Pandas-style export manager for asynchronous operations.

Provides async ``to_csv()`` and ``to_jsonl()`` methods for non-blocking
export operations.

.. autoclass:: matrixone.export.AsyncExportManager
   :members:
   :undoc-members:
   :show-inheritance:
