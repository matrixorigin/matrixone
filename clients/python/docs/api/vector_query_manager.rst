Vector Query Manager
====================

This section documents the vector query management classes that provide vector similarity search capabilities.

VectorQueryManager
------------------

.. autoclass:: matrixone.client.VectorQueryManager
   :members:
   :undoc-members:
   :show-inheritance:

Vector Distance Functions
-------------------------

.. autofunction:: matrixone.sqlalchemy_ext.vector_type.l2_distance

.. autofunction:: matrixone.sqlalchemy_ext.vector_type.cosine_distance

.. autofunction:: matrixone.sqlalchemy_ext.vector_type.inner_product

.. autofunction:: matrixone.sqlalchemy_ext.vector_type.negative_inner_product

.. autofunction:: matrixone.sqlalchemy_ext.vector_type.l2_distance_sq

Vector Utility Functions
------------------------

.. autofunction:: matrixone.sqlalchemy_ext.vector_type.vector_similarity_search

.. autofunction:: matrixone.sqlalchemy_ext.vector_type.most_similar

.. autofunction:: matrixone.sqlalchemy_ext.vector_type.within_distance
