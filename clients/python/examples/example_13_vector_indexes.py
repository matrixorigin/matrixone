#!/usr/bin/env python3

# Copyright 2021 - 2022 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Example 13: Vector Indexes - Index Management and Configuration

This example demonstrates comprehensive vector index management in MatrixOne:
- IVF (Inverted File) index creation and configuration
- HNSW (Hierarchical Navigable Small World) index creation and configuration
- Index performance comparison
- Index management best practices
- Error handling and validation

This replaces examples 13, 14, 15, 17, and 18 with integrated functionality.
"""

import sys
import os
import numpy as np
import time
from typing import List, Dict, Any, Optional

# Add the parent directory to the path so we can import matrixone
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import Column, Integer, BigInteger, String, Text, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from matrixone import Client
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger
from matrixone.sqlalchemy_ext import create_vector_column, boolean_match


class VectorIndexDemo:
    """Demonstrates comprehensive vector index management with performance testing."""

    def __init__(self):
        self.client = None
        self.engine = None
        self.session = None
        self.Base = declarative_base()
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
            'performance_results': {},
        }

    def setup_connection(self):
        """Setup database connection."""
        print("=" * 80)
        print("Vector Index Management Demo - Setup")
        print("=" * 80)

        # Print configuration
        print_config()

        # Get connection parameters
        host, port, user, password, database = get_connection_params()

        # Create logger
        logger = create_default_logger()

        # Create client and connect
        self.client = Client(logger=logger, sql_log_mode="full")
        self.client.connect(host=host, port=port, user=user, password=password, database=database)

        # Get SQLAlchemy engine
        self.engine = self.client.get_sqlalchemy_engine()

        # Create session
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        print("✓ Connected to MatrixOne")

    def create_test_tables(self):
        """Create test tables for different index types."""
        print("\n" + "-" * 60)
        print("Creating Test Tables")
        print("-" * 60)

        # Define test models
        class IVFDocument(self.Base):
            __tablename__ = 'ivf_test_docs'

            id = Column(Integer, primary_key=True, autoincrement=True)
            title = Column(String(200), nullable=False)
            content = Column(Text)
            embedding = create_vector_column(128, precision='f32')  # 128-dimensional vector

        class HNSWDocument(self.Base):
            __tablename__ = 'hnsw_test_docs'

            id = Column(BigInteger, primary_key=True, autoincrement=True)  # HNSW requires BIGINT primary key
            title = Column(String(200), nullable=False)
            content = Column(Text)
            embedding = create_vector_column(128, precision='f32')  # 128-dimensional vector

        class ComparisonDocument(self.Base):
            __tablename__ = 'comparison_test_docs'

            id = Column(Integer, primary_key=True, autoincrement=True)
            title = Column(String(200), nullable=False)
            content = Column(Text)
            embedding = create_vector_column(128, precision='f32')  # 128-dimensional vector

        # Clean up first
        with self.engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS ivf_test_docs"))
            conn.execute(text("DROP TABLE IF EXISTS hnsw_test_docs"))
            conn.execute(text("DROP TABLE IF EXISTS comparison_test_docs"))

        # Create tables
        self.Base.metadata.create_all(self.engine)
        print("✓ Created test tables: ivf_test_docs, hnsw_test_docs, comparison_test_docs")

        return IVFDocument, HNSWDocument, ComparisonDocument

    def insert_test_data(self, models, num_docs=1000):
        """Insert test data for performance testing."""
        print(f"\n" + "-" * 60)
        print(f"Inserting Test Data ({num_docs} documents per table)")
        print("-" * 60)

        IVFDocument, HNSWDocument, ComparisonDocument = models

        # Generate test documents
        test_docs = []
        for i in range(num_docs):
            test_docs.append(
                {
                    'title': f'Document {i+1}',
                    'content': f'This is the content of document {i+1} with some sample text.',
                    'embedding': np.random.rand(128).tolist(),
                }
            )

        # Insert into IVF table
        for doc_data in test_docs:
            doc = IVFDocument(**doc_data)
            self.session.add(doc)
        self.session.commit()
        print(f"✓ Inserted {num_docs} documents into ivf_test_docs")

        # Insert into HNSW table
        for doc_data in test_docs:
            doc = HNSWDocument(**doc_data)
            self.session.add(doc)
        self.session.commit()
        print(f"✓ Inserted {num_docs} documents into hnsw_test_docs")

        # Insert into comparison table (no index)
        for doc_data in test_docs:
            doc = ComparisonDocument(**doc_data)
            self.session.add(doc)
        self.session.commit()
        print(f"✓ Inserted {num_docs} documents into comparison_test_docs")

    def test_ivf_index_creation(self, IVFDocument):
        """Test IVF index creation and configuration."""
        print("\n" + "-" * 60)
        print("Testing IVF Index Creation")
        print("-" * 60)

        self.results['tests_run'] += 1

        try:
            # Enable IVF indexing
            self.client.vector_ops.enable_ivf()
            print("✓ Enabled IVF indexing")

            # Create IVF index (only one per vector column is allowed)
            ivf_config = {
                'name': 'ivf_l2_index',
                'column': 'embedding',
                'lists': 100,
                'op_type': 'vector_l2_ops',
            }

            start_time = time.time()
            self.client.vector_ops.create_ivf('ivf_test_docs', **ivf_config)
            creation_time = time.time() - start_time
            print(f"✓ Created IVF index '{ivf_config['name']}' in {creation_time:.2f}s")

            self.results['performance_results'][f'ivf_creation_{ivf_config["name"]}'] = creation_time

            # Test limitation: Try to create second index on same column (should fail)
            print("\nTesting limitation: Only one IVFFLAT index per vector column")
            try:
                duplicate_config = {
                    'name': 'ivf_cosine_index',
                    'column': 'embedding',
                    'lists': 50,
                    'op_type': 'vector_cosine_ops',
                }
                self.client.vector_ops.create_ivf('ivf_test_docs', **duplicate_config)
                print("✗ Unexpected: Second index creation succeeded")
            except Exception as e:
                print(f"✓ Expected: Second index creation failed - {e}")

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"✗ IVF index creation failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'ivf_index_creation', 'error': str(e)})

    def test_hnsw_index_creation(self, HNSWDocument):
        """Test HNSW index creation and configuration."""
        print("\n" + "-" * 60)
        print("Testing HNSW Index Creation")
        print("-" * 60)

        self.results['tests_run'] += 1

        try:
            # Enable HNSW indexing
            self.client.vector_ops.enable_hnsw()
            print("✓ Enabled HNSW indexing")

            # Create HNSW index (only one per vector column is allowed)
            hnsw_config = {
                'name': 'hnsw_l2_index',
                'column': 'embedding',
                'm': 16,
                'ef_construction': 200,
                'ef_search': 50,
                'op_type': 'vector_l2_ops',
            }

            start_time = time.time()
            self.client.vector_ops.create_hnsw('hnsw_test_docs', **hnsw_config)
            creation_time = time.time() - start_time
            print(f"✓ Created HNSW index '{hnsw_config['name']}' in {creation_time:.2f}s")

            self.results['performance_results'][f'hnsw_creation_{hnsw_config["name"]}'] = creation_time

            # Test limitation: Try to create second index on same column (should fail)
            print("\nTesting limitation: Only one HNSW index per vector column")
            try:
                duplicate_config = {
                    'name': 'hnsw_cosine_index',
                    'column': 'embedding',
                    'm': 32,
                    'ef_construction': 400,
                    'ef_search': 100,
                    'op_type': 'vector_cosine_ops',
                }
                self.client.vector_ops.create_hnsw('hnsw_test_docs', **duplicate_config)
                print("✗ Unexpected: Second index creation succeeded")
            except Exception as e:
                print(f"✓ Expected: Second index creation failed - {e}")

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"✗ HNSW index creation failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'hnsw_index_creation', 'error': str(e)})

    def test_index_performance_comparison(self, models):
        """Compare performance of different index types."""
        print("\n" + "-" * 60)
        print("Index Performance Comparison")
        print("-" * 60)

        IVFDocument, HNSWDocument, ComparisonDocument = models

        # Generate query vector
        query_vector = np.random.rand(128).tolist()

        # Test configurations
        test_configs = [
            ('No Index', ComparisonDocument, 'comparison_test_docs'),
            ('IVF Index', IVFDocument, 'ivf_test_docs'),
            ('HNSW Index', HNSWDocument, 'hnsw_test_docs'),
        ]

        for index_type, model, table_name in test_configs:
            self.results['tests_run'] += 1

            try:
                # Perform similarity search
                start_time = time.time()

                results = (
                    self.session.query(
                        model.id,
                        model.title,
                        model.embedding.l2_distance(query_vector).label('distance'),
                    )
                    .order_by(model.embedding.l2_distance(query_vector))
                    .limit(10)
                    .all()
                )

                search_time = time.time() - start_time

                print(f"\n{index_type} Results:")
                print(f"  Search Time: {search_time:.4f}s")
                print(f"  Results Found: {len(results)}")

                if results:
                    print(f"  Best Match: Document {results[0].id} (Distance: {results[0].distance:.4f})")

                self.results['performance_results'][f'search_time_{index_type.lower().replace(" ", "_")}'] = search_time
                self.results['tests_passed'] += 1

            except Exception as e:
                print(f"✗ {index_type} performance test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append(
                    {'test': f'performance_{index_type.lower().replace(" ", "_")}', 'error': str(e)}
                )

    def test_index_management_operations(self):
        """Test index management operations (drop, show, etc.)."""
        print("\n" + "-" * 60)
        print("Index Management Operations")
        print("-" * 60)

        self.results['tests_run'] += 1

        try:
            # Show indexes
            indexes = self.client.execute("SHOW INDEX FROM ivf_test_docs")
            print(f"✓ Found {len(indexes)} indexes on ivf_test_docs")

            # Show indexes for HNSW table
            indexes = self.client.execute("SHOW INDEX FROM hnsw_test_docs")
            print(f"✓ Found {len(indexes)} indexes on hnsw_test_docs")

            # Test index dropping (optional - comment out if you want to keep indexes)
            # self.client.vector_ops.drop('ivf_l2_index', 'ivf_test_docs')
            # print("✓ Dropped IVF index")

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"✗ Index management operations failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'index_management_operations', 'error': str(e)})

    def test_index_limitations(self):
        """Test index limitations and constraints."""
        print("\n" + "-" * 60)
        print("Testing Index Limitations")
        print("-" * 60)

        self.results['tests_run'] += 1

        try:
            # Test: Only one index per vector column
            print("Testing: Only one index per vector column")

            # Try to create a second index on the same column
            try:
                self.client.vector_ops.create_ivf(
                    name='duplicate_ivf_index',
                    table_name='ivf_test_docs',
                    column='embedding',
                    lists=50,
                    op_type='vector_l2_ops',
                )
                print("✗ Unexpected: Second index creation succeeded (should have failed)")
                self.results['unexpected_results'].append(
                    {
                        'test': 'index_limitation_duplicate',
                        'error': 'Second index creation succeeded when it should have failed',
                    }
                )
            except Exception as e:
                print(f"✓ Expected: Second index creation failed - {e}")

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"✗ Index limitations test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'index_limitations', 'error': str(e)})

    def test_fulltext_index_operations(self):
        """Test fulltext index creation and search operations."""
        print("\n" + "-" * 60)
        print("Fulltext Index Operations")
        print("-" * 60)

        self.results['tests_run'] += 1

        try:
            # Define fulltext test model
            class FulltextDocument(self.Base):
                __tablename__ = 'fulltext_test_docs'

                id = Column(Integer, primary_key=True, autoincrement=True)
                title = Column(String(200), nullable=False)
                content = Column(Text, nullable=False)
                author = Column(String(100))

            # Clean up first
            with self.engine.begin() as conn:
                conn.execute(text("DROP TABLE IF EXISTS fulltext_test_docs"))

            # Create table
            FulltextDocument.__table__.create(self.engine)
            print("✓ Created fulltext test table")

            # Enable fulltext indexing
            self.client.fulltext_index.enable_fulltext()
            self.client.execute('SET ft_relevancy_algorithm = "BM25"')
            print("✓ Enabled fulltext indexing with BM25 algorithm")

            # Create fulltext index
            self.client.fulltext_index.create(
                "fulltext_test_docs",
                name="ftidx_docs",
                columns=["title", "content"],
                algorithm="BM25",
            )
            print("✓ Created fulltext index on title and content columns")

            # Insert test data
            documents = [
                (
                    1,
                    "Introduction to Python",
                    "Python is a powerful programming language used for web development, data science, and automation.",
                    "John Doe",
                ),
                (
                    2,
                    "Machine Learning Basics",
                    "Machine learning is a subset of artificial intelligence that focuses on algorithms and statistical models.",
                    "Jane Smith",
                ),
                (
                    3,
                    "Database Design Principles",
                    "Good database design is crucial for application performance and data integrity.",
                    "Mike Johnson",
                ),
                (
                    4,
                    "Python Web Development",
                    "Learn how to build web applications using Python frameworks like Django and Flask.",
                    "Sarah Wilson",
                ),
                (
                    5,
                    "Data Science with Python",
                    "Python provides excellent libraries for data analysis, visualization, and machine learning.",
                    "David Brown",
                ),
            ]

            for doc_id, title, content, author in documents:
                self.client.execute(
                    f"INSERT INTO fulltext_test_docs (id, title, content, author) VALUES ({doc_id}, '{title}', '{content}', '{author}')"
                )
            print(f"✓ Inserted {len(documents)} test documents")

            # Test fulltext search - Natural Language Mode
            print("\n--- Testing Natural Language Mode ---")
            result = self.client.query(
                "fulltext_test_docs.id",
                "fulltext_test_docs.title",
                "fulltext_test_docs.content",
                "fulltext_test_docs.author",
                boolean_match("title", "content").phrase("Python"),
            ).execute()

            print(f"✓ Natural language search for 'Python': {len(result.fetchall())} results")
            for row in result.fetchall():
                try:
                    score = float(row[2]) if len(row) > 2 and row[2] is not None else 0.0
                    print(f"    ID: {row[0]}, Title: {row[1]}, Score: {score:.4f}")
                except (ValueError, TypeError, IndexError):
                    print(f"    ID: {row[0]}, Title: {row[1]}, Score: N/A")

            # Test fulltext search - Boolean Mode
            print("\n--- Testing Boolean Mode ---")
            result = self.client.query(
                "fulltext_test_docs.id",
                "fulltext_test_docs.title",
                "fulltext_test_docs.content",
                "fulltext_test_docs.author",
                boolean_match("title", "content").must("Python", "web"),
            ).execute()

            print(f"✓ Boolean search for '+Python +web': {len(result.fetchall())} results")
            for row in result.fetchall():
                print(f"    ID: {row[0]}, Title: {row[1]}")

            # Test different algorithms using API
            print("\n--- Testing TF-IDF Algorithm ---")
            self.client.execute('SET ft_relevancy_algorithm = "TF-IDF"')
            result = self.client.query(
                "fulltext_test_docs.id",
                "fulltext_test_docs.title",
                "fulltext_test_docs.content",
                "fulltext_test_docs.author",
                boolean_match("title", "content").phrase("Python"),
            ).execute()

            print(f"✓ TF-IDF search for 'Python': {len(result.fetchall())} results")
            for row in result.fetchall():
                try:
                    score = float(row[2]) if len(row) > 2 and row[2] is not None else 0.0
                    print(f"    ID: {row[0]}, Title: {row[1]}, Score: {score:.4f}")
                except (ValueError, TypeError, IndexError):
                    print(f"    ID: {row[0]}, Title: {row[1]}, Score: N/A")

            # Drop the index
            self.client.fulltext_index.drop("fulltext_test_docs", "ftidx_docs")
            print("✓ Dropped fulltext index")

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"✗ Fulltext index operations failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'fulltext_index_operations', 'error': str(e)})

    def test_async_fulltext_operations(self):
        """Test async fulltext index operations."""
        print("\n" + "-" * 60)
        print("Async Fulltext Index Operations")
        print("-" * 60)

        self.results['tests_run'] += 1

        try:
            import asyncio
            from matrixone import AsyncClient, FulltextAlgorithmType, FulltextModeType

            async def run_async_test():
                # Create async client
                host, port, user, password, database = get_connection_params()
                async_client = AsyncClient()
                await async_client.connect(host=host, port=port, user=user, password=password, database=database)

                try:
                    # Enable fulltext indexing
                    await async_client.fulltext_index.enable_fulltext()
                    await async_client.execute('SET ft_relevancy_algorithm = "BM25"')
                    print("✓ Enabled async fulltext indexing")

                    # Create test table
                    await async_client.execute("DROP TABLE IF EXISTS async_fulltext_docs")
                    await async_client.execute(
                        """
                        CREATE TABLE async_fulltext_docs (
                            id INT PRIMARY KEY,
                            headline VARCHAR(200),
                            body TEXT,
                            category VARCHAR(50)
                        )
                    """
                    )
                    print("✓ Created async fulltext test table")

                    # Create fulltext index
                    await async_client.fulltext_index.create(
                        "async_fulltext_docs",
                        name="ftidx_async_docs",
                        columns=["headline", "body"],
                        algorithm=FulltextAlgorithmType.BM25,
                    )
                    print("✓ Created async fulltext index")

                    # Insert test data
                    articles = [
                        (
                            1,
                            "Tech News: AI Breakthrough",
                            "Artificial intelligence researchers have made significant progress in natural language processing.",
                            "Technology",
                        ),
                        (
                            2,
                            "Sports Update: Championship Results",
                            "The annual championship concluded with exciting matches and surprising outcomes.",
                            "Sports",
                        ),
                        (
                            3,
                            "Health Research: New Study Findings",
                            "Medical researchers published findings that could lead to new treatment options.",
                            "Health",
                        ),
                    ]

                    for article_id, headline, body, category in articles:
                        await async_client.execute(
                            f"INSERT INTO async_fulltext_docs (id, headline, body, category) VALUES ({article_id}, '{headline}', '{body}', '{category}')"
                        )
                    print(f"✓ Inserted {len(articles)} async test articles")

                    # Test async fulltext search
                    result = await async_client.query(
                        "async_fulltext_docs.id",
                        "async_fulltext_docs.headline",
                        "async_fulltext_docs.body",
                        "async_fulltext_docs.category",
                        boolean_match("headline", "body").phrase("technology"),
                    ).execute()

                    print(f"✓ Async fulltext search for 'technology': {len(result.fetchall())} results")
                    for row in result.fetchall():
                        try:
                            score = float(row[2]) if len(row) > 2 and row[2] is not None else 0.0
                            print(f"    ID: {row[0]}, Headline: {row[1]}, Score: {score:.4f}")
                        except (ValueError, TypeError, IndexError):
                            print(f"    ID: {row[0]}, Headline: {row[1]}, Score: N/A")

                    # Cleanup
                    await async_client.fulltext_index.drop("async_fulltext_docs", "ftidx_async_docs")
                    await async_client.execute("DROP TABLE IF EXISTS async_fulltext_docs")
                    print("✓ Cleaned up async fulltext test")

                finally:
                    await async_client.disconnect()

            # Run async test
            asyncio.run(run_async_test())

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"✗ Async fulltext operations failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'async_fulltext_operations', 'error': str(e)})

    def cleanup(self):
        """Clean up test resources."""
        print("\n" + "-" * 60)
        print("Cleanup")
        print("-" * 60)

        try:
            if self.session:
                self.session.close()

            if self.engine:
                with self.engine.begin() as conn:
                    conn.execute(text("DROP TABLE IF EXISTS ivf_test_docs"))
                    conn.execute(text("DROP TABLE IF EXISTS hnsw_test_docs"))
                    conn.execute(text("DROP TABLE IF EXISTS comparison_test_docs"))
                print("✓ Cleaned up test tables")

            if self.client:
                self.client.disconnect()
                print("✓ Disconnected from MatrixOne")

        except Exception as e:
            print(f"✗ Cleanup failed: {e}")

    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        print("\n" + "=" * 80)
        print("Vector Index Management Demo - Summary Report")
        print("=" * 80)

        total_tests = self.results['tests_run']
        passed_tests = self.results['tests_passed']
        failed_tests = self.results['tests_failed']
        unexpected_results = self.results['unexpected_results']
        performance_results = self.results['performance_results']

        print(f"Total Tests Run: {total_tests}")
        print(f"Tests Passed: {passed_tests}")
        print(f"Tests Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests*100):.1f}%" if total_tests > 0 else "N/A")

        # Performance summary
        if performance_results:
            print(f"\nPerformance Results:")
            for test_name, time_taken in performance_results.items():
                print(f"  {test_name}: {time_taken:.4f}s")

        # Unexpected results
        if unexpected_results:
            print(f"\nUnexpected Results ({len(unexpected_results)}):")
            for i, result in enumerate(unexpected_results, 1):
                print(f"  {i}. Test: {result['test']}")
                print(f"     Error: {result['error']}")
        else:
            print("\n✓ No unexpected results - all tests behaved as expected")

        return self.results


def main():
    """Main function to run the vector index management demo."""
    demo = VectorIndexDemo()

    try:
        # Setup
        demo.setup_connection()
        models = demo.create_test_tables()

        # Insert test data
        demo.insert_test_data(models, num_docs=500)  # Reduced for faster testing

        # Run tests
        demo.test_ivf_index_creation(models[0])
        demo.test_hnsw_index_creation(models[1])
        demo.test_index_performance_comparison(models)
        demo.test_index_management_operations()
        demo.test_index_limitations()
        demo.test_fulltext_index_operations()
        demo.test_async_fulltext_operations()

        # Generate report
        results = demo.generate_summary_report()

        return results

    except Exception as e:
        print(f"Demo failed with error: {e}")
        return None

    finally:
        demo.cleanup()


if __name__ == "__main__":
    main()
