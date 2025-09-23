#!/usr/bin/env python3
"""
Example 18: Specialized Vector Indexes

This example demonstrates the new specialized vector index classes:
- IVFVectorIndex: Specialized for IVFFLAT indexes with type safety
- HnswVectorIndex: Specialized for HNSW indexes with type safety

These classes provide better type safety and clearer APIs compared to the generic VectorIndex.
"""

import sys
import os

# Add the parent directory to the path to import matrixone
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import Column, Integer, String, BigInteger, text
from sqlalchemy.orm import declarative_base, sessionmaker

from matrixone import Client
from matrixone.config import get_connection_params, print_config
from matrixone.sqlalchemy_ext import (
    IVFVectorIndex, HnswVectorIndex, VectorIndex,  # New specialized classes
    VectorIndexType, VectorOpType, create_vector_column
)


def main():
    """Main function demonstrating specialized vector indexes"""
    
    # Print configuration
    print_config()
    
    # Get connection parameters
    host, port, user, password, database = get_connection_params()
    
    # Create client and connect
    client = Client()
    client.connect(host=host, port=port, user=user, password=password, database=database)
    
    print("=" * 80)
    print("MatrixOne Specialized Vector Indexes Demo")
    print("=" * 80)
    
    try:
        # Clean up any existing tables first
        print("\n--- Cleanup ---")
        try:
            client.drop_table("vector_docs_ivf_specialized")
            client.drop_table("vector_docs_hnsw_specialized")
            client.drop_table("vector_docs_legacy")
        except:
            pass
        
        # Get SQLAlchemy engine
        engine = client.get_sqlalchemy_engine()
        Base = declarative_base()
        Session = sessionmaker(bind=engine)
        
        print("\n--- Demo 1: IVFVectorIndex (Specialized for IVFFLAT) ---")
        
        # Create table without index first
        class DocumentWithIVFIndex(Base):
            __tablename__ = 'vector_docs_ivf_specialized'
            
            id = Column(Integer, primary_key=True, autoincrement=True)
            title = Column(String(200))
            content = Column(String(1000))
            embedding = create_vector_column(128, "f32")
        
        # Create table first
        Base.metadata.create_all(engine, tables=[DocumentWithIVFIndex.__table__])
        print("✅ Created table")
        
        # Enable IVF indexing and create specialized index separately
        with engine.begin() as conn:
            conn.execute(text("SET experimental_ivf_index = 1"))
            conn.execute(text("SET probe_limit = 1"))
            
            # Create specialized IVFVectorIndex
            ivf_index = IVFVectorIndex('idx_ivf_specialized', 'embedding', 
                                     lists=50, op_type=VectorOpType.VECTOR_L2_OPS)
            sql = ivf_index.create_sql('vector_docs_ivf_specialized')
            conn.execute(text(sql))
        
        print("✅ Created specialized IVFVectorIndex")
        
        # Insert sample data
        session = Session()
        try:
            sample_docs = [
                ("Machine Learning Guide", "Comprehensive guide to ML", [0.1] * 128),
                ("Deep Learning Tutorial", "Neural networks tutorial", [0.2] * 128),
                ("Data Science Handbook", "Data science techniques", [0.3] * 128),
            ]
            
            for title, content, embedding in sample_docs:
                doc = DocumentWithIVFIndex(title=title, content=content, embedding=embedding)
                session.add(doc)
            
            session.commit()
            print("✅ Inserted sample data")
            
        finally:
            session.close()
        
        # Verify index was created
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM vector_docs_ivf_specialized"))
            rows = result.fetchall()
            indexes = [row[2] for row in rows if row[2] != 'PRIMARY']
            print(f"✅ Indexes created: {indexes}")
        
        print("\n--- Demo 2: HnswVectorIndex (Specialized for HNSW) ---")
        
        # Create table without index first
        class DocumentWithHNSWIndex(Base):
            __tablename__ = 'vector_docs_hnsw_specialized'
            
            id = Column(BigInteger, primary_key=True, autoincrement=True)
            title = Column(String(200))
            content = Column(String(1000))
            embedding = create_vector_column(128, "f32")
        
        # Create table first
        Base.metadata.create_all(engine, tables=[DocumentWithHNSWIndex.__table__])
        print("✅ Created table")
        
        # Enable HNSW indexing and create specialized index separately
        with engine.begin() as conn:
            conn.execute(text("SET experimental_hnsw_index = 1"))
            
            # Create specialized HnswVectorIndex
            hnsw_index = HnswVectorIndex('idx_hnsw_specialized', 'embedding',
                                       m=32, ef_construction=64, ef_search=32,
                                       op_type=VectorOpType.VECTOR_L2_OPS)
            sql = hnsw_index.create_sql('vector_docs_hnsw_specialized')
            conn.execute(text(sql))
        
        print("✅ Created specialized HnswVectorIndex")
        
        # Insert sample data
        session = Session()
        try:
            sample_docs = [
                ("AI Research Paper", "Latest AI research", [0.1, 0.2, 0.3] + [0.0] * 125),
                ("Computer Vision Guide", "CV techniques", [0.2, 0.3, 0.4] + [0.0] * 125),
                ("NLP Handbook", "Natural language processing", [0.3, 0.4, 0.5] + [0.0] * 125),
            ]
            
            for title, content, embedding in sample_docs:
                doc = DocumentWithHNSWIndex(title=title, content=content, embedding=embedding)
                session.add(doc)
            
            session.commit()
            print("✅ Inserted sample data")
            
        finally:
            session.close()
        
        # Verify index was created
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM vector_docs_hnsw_specialized"))
            rows = result.fetchall()
            indexes = [row[2] for row in rows if row[2] != 'PRIMARY']
            print(f"✅ Indexes created: {indexes}")
        
        print("\n--- Demo 3: Comparison with Legacy VectorIndex ---")
        
        # Create table without index first
        class DocumentWithLegacyIndex(Base):
            __tablename__ = 'vector_docs_legacy'
            
            id = Column(Integer, primary_key=True, autoincrement=True)
            title = Column(String(200))
            embedding = create_vector_column(128, "f32")
        
        # Create table first
        Base.metadata.create_all(engine, tables=[DocumentWithLegacyIndex.__table__])
        print("✅ Created table")
        
        # Enable IVF indexing and create legacy index separately
        with engine.begin() as conn:
            conn.execute(text("SET experimental_ivf_index = 1"))
            conn.execute(text("SET probe_limit = 1"))
            
            # Create legacy VectorIndex
            legacy_index = VectorIndex('idx_legacy', 'embedding', 
                                     index_type=VectorIndexType.IVFFLAT,
                                     lists=25, op_type=VectorOpType.VECTOR_L2_OPS)
            sql = legacy_index.create_sql('vector_docs_legacy')
            conn.execute(text(sql))
        
        print("✅ Created legacy VectorIndex")
        
        # Verify index was created
        with engine.begin() as conn:
            result = conn.execute(text("SHOW INDEX FROM vector_docs_legacy"))
            rows = result.fetchall()
            indexes = [row[2] for row in rows if row[2] != 'PRIMARY']
            print(f"✅ Indexes created: {indexes}")
        
        print("\n--- Demo 4: Class Method Usage ---")
        
        # Demonstrate using class methods for index creation
        print("Creating new tables to demonstrate class methods...")
        
        # Create new table for IVFFLAT class method demo
        class DocumentForClassMethod(Base):
            __tablename__ = 'vector_docs_class_method_demo'
            
            id = Column(Integer, primary_key=True, autoincrement=True)
            title = Column(String(200))
            embedding = create_vector_column(128, "f32")
        
        # Create table first
        Base.metadata.create_all(engine, tables=[DocumentForClassMethod.__table__])
        print("✅ Created new table for class method demo")
        
        # Create IVFFLAT index using class method
        success = IVFVectorIndex.create_index(
            engine=engine,
            table_name="vector_docs_class_method_demo",
            name="idx_ivf_class_method",
            column="embedding",
            lists=75,
            op_type=VectorOpType.VECTOR_COSINE_OPS
        )
        print(f"✅ IVFVectorIndex.create_index: {'Success' if success else 'Failed'}")
        
        # Create new table for HNSW class method demo
        class DocumentForHNSWClassMethod(Base):
            __tablename__ = 'vector_docs_hnsw_class_method_demo'
            
            id = Column(BigInteger, primary_key=True, autoincrement=True)
            title = Column(String(200))
            embedding = create_vector_column(128, "f32")
        
        # Create table first
        Base.metadata.create_all(engine, tables=[DocumentForHNSWClassMethod.__table__])
        print("✅ Created new table for HNSW class method demo")
        
        # Create HNSW index using class method
        success = HnswVectorIndex.create_index(
            engine=engine,
            table_name="vector_docs_hnsw_class_method_demo",
            name="idx_hnsw_class_method",
            column="embedding",
            m=48,
            ef_construction=128,
            ef_search=64,
            op_type=VectorOpType.VECTOR_IP_OPS
        )
        print(f"✅ HnswVectorIndex.create_index: {'Success' if success else 'Failed'}")
        
        # Demonstrate MatrixOne limitation
        print("\n--- MatrixOne Limitation Demo ---")
        print("ℹ️ MatrixOne supports only ONE index per vector column")
        print("ℹ️ Attempting to create additional indexes on the same column will fail:")
        
        # Try to create additional index on existing column (should fail)
        success = IVFVectorIndex.create_index(
            engine=engine,
            table_name="vector_docs_ivf_specialized",
            name="idx_ivf_additional",
            column="embedding",
            lists=50,
            op_type=VectorOpType.VECTOR_L2_OPS
        )
        print(f"❌ Additional IVFFLAT index on same column: {'Success' if success else 'Failed (Expected)'}")
        
        success = HnswVectorIndex.create_index(
            engine=engine,
            table_name="vector_docs_hnsw_specialized",
            name="idx_hnsw_additional",
            column="embedding",
            m=32,
            ef_construction=64,
            ef_search=32,
            op_type=VectorOpType.VECTOR_L2_OPS
        )
        print(f"❌ Additional HNSW index on same column: {'Success' if success else 'Failed (Expected)'}")
        
        print("\n--- Demo 5: Type Safety Benefits ---")
        print("Specialized classes provide better type safety:")
        print("- IVFVectorIndex: Only accepts IVFFLAT-specific parameters (lists)")
        print("- HnswVectorIndex: Only accepts HNSW-specific parameters (m, ef_construction, ef_search)")
        print("- VectorIndex: Generic class that accepts all parameters (legacy)")
        
        print("\n--- Demo 6: SQL Generation ---")
        
        # Demonstrate SQL generation
        ivf_index = IVFVectorIndex("demo_ivf", "embedding", lists=100)
        hnsw_index = HnswVectorIndex("demo_hnsw", "embedding", m=16, ef_construction=200)
        
        print("IVFVectorIndex SQL:")
        print(f"  {ivf_index.create_sql('demo_table')}")
        
        print("HnswVectorIndex SQL:")
        print(f"  {hnsw_index.create_sql('demo_table')}")
        
        print("\n--- Summary ---")
        print("✅ Specialized vector index classes provide:")
        print("   - Better type safety")
        print("   - Clearer APIs")
        print("   - Reduced parameter confusion")
        print("   - Better IDE support and autocomplete")
        print("   - Easier maintenance and debugging")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Clean up
        print("\n--- Cleanup ---")
        try:
            client.drop_table("vector_docs_ivf_specialized")
            client.drop_table("vector_docs_hnsw_specialized")
            client.drop_table("vector_docs_legacy")
            client.drop_table("vector_docs_class_method_demo")
            client.drop_table("vector_docs_hnsw_class_method_demo")
            print("✅ Cleaned up test tables")
        except Exception as e:
            print(f"⚠️ Cleanup warning: {e}")
        
        client.disconnect()
        print("✅ Disconnected from MatrixOne")


if __name__ == "__main__":
    main()
