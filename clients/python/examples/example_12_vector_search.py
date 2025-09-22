#!/usr/bin/env python3
"""
MatrixOne Vector Distance Functions Example

Demonstrates the new ORM-level vector distance functions:
- l2_distance
- l2_distance_sq  
- cosine_distance
- negative_inner_product
- inner_product

This example shows comprehensive vector similarity search capabilities using
MatrixOne's vector distance functions with SQLAlchemy ORM.
"""

import random
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base
from matrixone import Client
from matrixone.sqlalchemy_ext import create_vector_column
from matrixone.logger import create_default_logger
from matrixone.config import get_connection_params, print_config

# Create MatrixOne logger for all logging
logger = create_default_logger(
    enable_performance_logging=True,
    enable_sql_logging=True
)


def main():
    """Demonstrate vector distance functions."""
    logger.info("🚀 MatrixOne Vector Distance Functions Example")
    logger.info("=" * 60)
    
    # Print current configuration
    print_config()
    
    try:
        # Get connection parameters
        host, port, user, password, database = get_connection_params()
        
        # Connect to MatrixOne
        logger.info("🔌 Connecting to MatrixOne...")
        client = Client(logger=logger, enable_full_sql_logging=True)
        client.connect(host=host, port=port, user=user, password=password, database=database)
        engine = client.get_sqlalchemy_engine()
        
        # Enable SQL logging for SQLAlchemy engine
        engine.echo = True
        logger.info("✅ Connected to MatrixOne successfully with SQL logging enabled")
        
        # Create declarative base
        Base = declarative_base()
        Session = sessionmaker(bind=engine)
    
        # Define Document model using the new create_vector_column function
        class Document(Base):
            __tablename__ = 'vector_distance_demo'
            
            id = Column(Integer, primary_key=True, autoincrement=True)
            embedding = create_vector_column(10, precision="f32")  # 10D vector with distance functions
            description = Column(String(200))
            
            @classmethod
            def setup_table(cls, engine):
                """Drop and create table."""
                cls.metadata.drop_all(engine, checkfirst=False)
                cls.metadata.create_all(engine, checkfirst=True)
                logger.info("✅ Table created: vector_distance_demo")
        
        # Setup table
        logger.info("🗂️ Setting up vector distance demo table...")
        Document.setup_table(engine)
        
        # Insert sample data
        logger.info("📝 Inserting sample data...")
        session = Session()
    
        try:
            # Create 5 sample documents with different vector patterns
            documents = [
                ([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], "Document A - Unit vector X"),
                ([0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], "Document B - Unit vector Y"),
                ([0.5, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], "Document C - Diagonal vector"),
                ([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], "Document D - Ascending vector"),
                ([0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0], "Document E - Descending vector"),
            ]
            
            for embedding, description in documents:
                doc = Document(embedding=embedding, description=description)
                session.add(doc)
            
            session.commit()
            logger.info("✅ Inserted 5 sample documents")
        
        except Exception as e:
            session.rollback()
            logger.error(f"❌ Error inserting documents: {e}")
            return
        finally:
            session.close()
    
        # Query vector for similarity search
        query_vector = [0.5, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        logger.info(f"\n🔍 Query vector: {query_vector}")
        
        session = Session()
        try:
            logger.info("\n=== Vector Distance Functions Demo ===")
        
            # 1. L2 Distance (Euclidean distance)
            logger.info("\n1️⃣ L2 Distance (Euclidean distance):")
            results = session.query(
                Document.id,
                Document.description,
                Document.embedding.l2_distance(query_vector).label('l2_dist')
            ).order_by(Document.embedding.l2_distance(query_vector)).all()
            
            for result in results:
                logger.info(f"   ID: {result.id}, L2 Distance: {result.l2_dist:.4f}, Desc: {result.description}")
            
            # 2. Squared L2 Distance
            logger.info("\n2️⃣ Squared L2 Distance:")
            results = session.query(
                Document.id,
                Document.description,
                Document.embedding.l2_distance_sq(query_vector).label('l2_dist_sq')
            ).order_by(Document.embedding.l2_distance_sq(query_vector)).all()
            
            for result in results:
                logger.info(f"   ID: {result.id}, L2 Distance²: {result.l2_dist_sq:.4f}, Desc: {result.description}")
            
            # 3. Cosine Distance
            logger.info("\n3️⃣ Cosine Distance:")
            results = session.query(
                Document.id,
                Document.description,
                Document.embedding.cosine_distance(query_vector).label('cos_dist')
            ).order_by(Document.embedding.cosine_distance(query_vector)).all()
            
            for result in results:
                logger.info(f"   ID: {result.id}, Cosine Distance: {result.cos_dist:.4f}, Desc: {result.description}")
            
            # 4. Inner Product (Dot Product)
            logger.info("\n4️⃣ Inner Product (Dot Product):")
            results = session.query(
                Document.id,
                Document.description,
                Document.embedding.inner_product(query_vector).label('inner_prod')
            ).order_by(Document.embedding.inner_product(query_vector).desc()).all()
            
            for result in results:
                logger.info(f"   ID: {result.id}, Inner Product: {result.inner_prod:.4f}, Desc: {result.description}")
            
            # 5. Negative Inner Product
            logger.info("\n5️⃣ Negative Inner Product:")
            results = session.query(
                Document.id,
                Document.description,
                Document.embedding.negative_inner_product(query_vector).label('neg_inner_prod')
            ).order_by(Document.embedding.negative_inner_product(query_vector)).all()
            
            for result in results:
                logger.info(f"   ID: {result.id}, Negative Inner Product: {result.neg_inner_prod:.4f}, Desc: {result.description}")
            
            # 6. Combined query: Filter by distance and order by similarity
            logger.info("\n6️⃣ Combined Query: Filter by L2 distance < 1.0, order by inner product:")
            results = session.query(
                Document.id,
                Document.description,
                Document.embedding.l2_distance(query_vector).label('l2_dist'),
                Document.embedding.inner_product(query_vector).label('inner_prod')
            ).filter(
                Document.embedding.l2_distance(query_vector) < 1.0
            ).order_by(
                Document.embedding.inner_product(query_vector).desc()
            ).all()
            
            for result in results:
                logger.info(f"   ID: {result.id}, L2: {result.l2_dist:.4f}, Inner: {result.inner_prod:.4f}, Desc: {result.description}")
            
            # 7. Top K similarity search using cosine distance
            logger.info("\n7️⃣ Top 3 Most Similar (by cosine distance):")
            results = session.query(
                Document.id,
                Document.description,
                Document.embedding.cosine_distance(query_vector).label('cos_dist')
            ).order_by(
                Document.embedding.cosine_distance(query_vector)
            ).limit(3).all()
            
            for i, result in enumerate(results, 1):
                logger.info(f"   {i}. ID: {result.id}, Cosine Distance: {result.cos_dist:.4f}, Desc: {result.description}")
            
            # 8. Top 5 similarity search with different distance metrics
            logger.info("\n8️⃣ Top 5 Most Similar - Multiple Distance Metrics:")
            logger.info("   🔸 L2 Distance (Euclidean):")
            results_l2 = session.query(
                Document.id,
                Document.description,
                Document.embedding.l2_distance(query_vector).label('l2_dist')
            ).order_by(
                Document.embedding.l2_distance(query_vector)
            ).limit(5).all()
            
            for i, result in enumerate(results_l2, 1):
                logger.info(f"      {i}. ID: {result.id}, L2 Distance: {result.l2_dist:.4f}, Desc: {result.description}")
            
            logger.info("\n   🔸 Cosine Distance:")
            results_cos = session.query(
                Document.id,
                Document.description,
                Document.embedding.cosine_distance(query_vector).label('cos_dist')
            ).order_by(
                Document.embedding.cosine_distance(query_vector)
            ).limit(5).all()
            
            for i, result in enumerate(results_cos, 1):
                logger.info(f"      {i}. ID: {result.id}, Cosine Distance: {result.cos_dist:.4f}, Desc: {result.description}")
            
            logger.info("\n   🔸 Inner Product (highest similarity):")
            results_inner = session.query(
                Document.id,
                Document.description,
                Document.embedding.inner_product(query_vector).label('inner_prod')
            ).order_by(
                Document.embedding.inner_product(query_vector).desc()
            ).limit(5).all()
            
            for i, result in enumerate(results_inner, 1):
                logger.info(f"      {i}. ID: {result.id}, Inner Product: {result.inner_prod:.4f}, Desc: {result.description}")
            
            # 9. Comprehensive Top 5 with all metrics
            logger.info("\n9️⃣ Comprehensive Top 5 - All Distance Metrics Combined:")
            logger.info("   (Showing ID, Description, L2, Cosine, Inner Product)")
            logger.info("   " + "-" * 80)
            
            # Get all documents with all distance metrics
            all_results = session.query(
                Document.id,
                Document.description,
                Document.embedding.l2_distance(query_vector).label('l2_dist'),
                Document.embedding.cosine_distance(query_vector).label('cos_dist'),
                Document.embedding.inner_product(query_vector).label('inner_prod')
            ).all()
            
            # Sort by L2 distance and show top 5
            all_results.sort(key=lambda x: x.l2_dist)
            top_5 = all_results[:5]
            
            for i, result in enumerate(top_5, 1):
                logger.info(f"   {i}. ID: {result.id:2d} | L2: {result.l2_dist:.4f} | Cos: {result.cos_dist:.4f} | Inner: {result.inner_prod:.4f}")
                logger.info(f"      Desc: {result.description}")
                logger.info("")
            
            # 10. Projection example - Only return ID and description for top 5
            logger.info("🔟 Top 5 Projection - ID and Description Only:")
            logger.info("   (Using L2 distance for ranking)")
            logger.info("   " + "-" * 50)
            
            projection_results = session.query(
                Document.id,
                Document.description
            ).order_by(
                Document.embedding.l2_distance(query_vector)
            ).limit(5).all()
            
            for i, result in enumerate(projection_results, 1):
                logger.info(f"   {i}. ID: {result.id} | {result.description}")
        
        except Exception as e:
            logger.error(f"❌ Query error: {e}")
        finally:
            session.close()
            client.disconnect()
        
        logger.info("\n✅ Vector distance functions demo completed!")
        logger.info("\nKey achievements:")
        logger.info("- ✅ ORM-level vector distance functions")
        logger.info("- ✅ Multiple distance metrics (L2, Cosine, Inner Product)")
        logger.info("- ✅ Top-K similarity search")
        logger.info("- ✅ Combined filtering and sorting")
        logger.info("- ✅ Projection queries")
        
    except Exception as e:
        logger.error(f"❌ Vector search example failed: {e}")


if __name__ == "__main__":
    main()
