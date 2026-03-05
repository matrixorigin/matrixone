Branch Statement Builders
=========================

SQLAlchemy-style statement builders for branch operations.

.. module:: matrixone.branch_builder

Top-Level Functions
-------------------

.. autofunction:: create_table_branch
.. autofunction:: create_database_branch
.. autofunction:: delete_table_branch
.. autofunction:: delete_database_branch
.. autofunction:: diff_table_branch
.. autofunction:: merge_table_branch

Statement Classes
-----------------

.. autoclass:: BranchStatement
   :members:

.. autoclass:: CreateTableBranch
   :members:

.. autoclass:: CreateDatabaseBranch
   :members:

.. autoclass:: DeleteTableBranch
   :members:

.. autoclass:: DeleteDatabaseBranch
   :members:

.. autoclass:: DiffTableBranch
   :members:

.. autoclass:: MergeTableBranch
   :members:

Enums
-----

.. autoclass:: DiffOutputOption
   :members:
   :undoc-members:
