Vector Manager
==============

This section documents the vector management classes that provide vector operations and indexing capabilities.

VectorManager
-------------

The VectorManager class provides comprehensive vector operations including similarity search, range search, and vector indexing capabilities.

.. autoclass:: matrixone.client.VectorManager
   :members:
   :undoc-members:
   :show-inheritance:

Key Features:
- Vector similarity search with configurable distance metrics (L2, cosine, inner product)
- Vector range search for finding vectors within a distance threshold
- Vector indexing support (IVF, HNSW)
- Vector data insertion and management
- Integration with MatrixOne's vector functions

Supported Distance Metrics:
- L2 (Euclidean) distance: Standard Euclidean distance
- Cosine similarity: Cosine of the angle between vectors  
- Inner product: Dot product of vectors

Usage Examples:
::

    # Initialize vector manager
    vector_ops = client.vector_ops
    
    # Similarity search with L2 distance
    results = vector_ops.similarity_search(
        table_name_or_model="documents",
        vector_column="embedding",
        query_vector=[0.1, 0.2, 0.3, ...],  # 384-dimensional vector
        limit=10,
        distance_type="l2"
    )
    
    # Range search with cosine similarity
    results = vector_ops.range_search(
        table_name_or_model="products",
        vector_column="features",
        query_vector=[0.5, 0.6, 0.7, ...],
        max_distance=0.8,
        distance_type="cosine"
    )


VectorTableBuilder
------------------

.. autoclass:: matrixone.sqlalchemy_ext.table_builder.VectorTableBuilder
   :members:
   :undoc-members:
   :show-inheritance:
