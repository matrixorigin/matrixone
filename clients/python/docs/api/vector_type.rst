Vector Type Extensions
======================

The MatrixOne Python SDK provides vector data type support through SQLAlchemy extensions.

VectorType
----------

.. autoclass:: matrixone.sqlalchemy_ext.vector_type.VectorType
   :members:
   :undoc-members:
   :show-inheritance:

Usage Examples
--------------

Basic Vector Type Usage
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from sqlalchemy import Column, Integer, String, create_engine
    from matrixone.orm import declarative_base
    from matrixone.sqlalchemy_ext import VectorType

    Base = declarative_base()

    class Document(Base):
        __tablename__ = 'documents'
        
        id = Column(Integer, primary_key=True)
        title = Column(String(200))
        content = Column(String(1000))
        embedding = Column(VectorType(384))  # 384-dimensional vector

    # Create table
    engine = create_engine('mysql+pymysql://root:111@localhost:6001/test')
    Base.metadata.create_all(engine)

Vector Operations
~~~~~~~~~~~~~~~~~

.. code-block:: python

    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import text

    Session = sessionmaker(bind=engine)
    session = Session()

    # Insert vector data
    doc = Document(
        title='Sample Document',
        content='This is a sample document',
        embedding=[0.1, 0.2, 0.3, ...]  # 384-dimensional vector
    )
    session.add(doc)
    session.commit()

    # Search similar vectors using L2 distance
    query_vector = [0.1, 0.2, 0.3, ...]
    result = session.execute(text("""
        SELECT id, title, embedding <-> :query_vector as distance
        FROM documents
        ORDER BY embedding <-> :query_vector
        LIMIT 10
    """), {'query_vector': query_vector})

    for row in result:
        print(f"Document {row.id}: {row.title} (Distance: {row.distance})")

    # Search using cosine similarity
    result = session.execute(text("""
        SELECT id, title, embedding <#> :query_vector as cosine_distance
        FROM documents
        ORDER BY embedding <#> :query_vector
        LIMIT 10
    """), {'query_vector': query_vector})

    session.close()

Vector Distance Functions
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # L2 distance (Euclidean distance)
    result = session.execute(text("""
        SELECT id, embedding <-> :query_vector as l2_distance
        FROM documents
        ORDER BY embedding <-> :query_vector
    """), {'query_vector': query_vector})

    # Cosine distance
    result = session.execute(text("""
        SELECT id, embedding <#> :query_vector as cosine_distance
        FROM documents
        ORDER BY embedding <#> :query_vector
    """), {'query_vector': query_vector})

    # Inner product
    result = session.execute(text("""
        SELECT id, embedding <*> :query_vector as inner_product
        FROM documents
        ORDER BY embedding <*> :query_vector DESC
    """), {'query_vector': query_vector})

Vector Index Integration
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone.sqlalchemy_ext import VectorIndex, HNSWConfig

    # Create vector index on the embedding column
    config = HNSWConfig(m=16, ef_construction=200)
    index = VectorIndex(
        name='idx_document_embedding',
        column='embedding',
        algorithm='hnsw',
        config=config
    )

    # Add index to table
    Document.__table__.append_constraint(index)

    # Create index in database
    index.create(engine)

    # Now searches will use the index for better performance
    result = session.execute(text("""
        SELECT id, title, embedding <-> :query_vector as distance
        FROM documents
        ORDER BY embedding <-> :query_vector
        LIMIT 10
    """), {'query_vector': query_vector})

Vector Validation
~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Vector dimension validation
    try:
        # This will raise an error if vector dimension doesn't match
        doc = Document(
            title='Invalid Document',
            embedding=[0.1, 0.2]  # Wrong dimension (should be 384)
        )
        session.add(doc)
        session.commit()
    except Exception as e:
        print(f"Vector dimension error: {e}")

    # Valid vector
    doc = Document(
        title='Valid Document',
        embedding=[0.1] * 384  # Correct dimension
    )
    session.add(doc)
    session.commit()
