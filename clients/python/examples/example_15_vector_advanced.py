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
Example 15: Vector Advanced - Advanced Operations and Best Practices

This example demonstrates advanced vector operations and best practices in MatrixOne:
- Transaction management with vectors
- Async vector operations
- Vector data migration and transformation
- Performance monitoring and optimization
- Error handling and recovery
- Production-ready patterns

This replaces examples with advanced functionality and best practices.
"""

import sys
import os
import numpy as np
import time
import asyncio
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

# Add the parent directory to the path so we can import matrixone
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import Column, Integer, String, Text, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from matrixone import Client, AsyncClient
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger
from matrixone.sqlalchemy_ext import create_vector_column


class VectorAdvancedDemo:
    """Demonstrates advanced vector operations with production-ready patterns."""

    def __init__(self):
        self.client = None
        self.async_client = None
        self.engine = None
        self.session = None
        self.Base = declarative_base()
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
            'performance_metrics': {},
        }

    def setup_connection(self):
        """Setup database connection."""
        print("=" * 80)
        print("Vector Advanced Operations Demo - Setup")
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

    async def setup_async_connection(self):
        """Setup async database connection."""
        print("\n" + "-" * 60)
        print("Setting up Async Connection")
        print("-" * 60)

        # Get connection parameters
        host, port, user, password, database = get_connection_params()

        # Create async client and connect
        self.async_client = AsyncClient()
        await self.async_client.connect(host=host, port=port, user=user, password=password, database=database)

        print("✓ Connected to MatrixOne (Async)")

    def create_test_table(self):
        """Create test table with vector column."""
        print("\n" + "-" * 60)
        print("Creating Test Table")
        print("-" * 60)

        # Define test model
        class AdvancedDocument(self.Base):
            __tablename__ = 'vector_advanced_docs'

            id = Column(Integer, primary_key=True, autoincrement=True)
            title = Column(String(200), nullable=False)
            content = Column(Text)
            category = Column(String(50))
            metadata_json = Column(Text)  # JSON metadata
            embedding = create_vector_column(256, precision='f32')  # 256-dimensional vector
            created_at = Column(Integer)  # Timestamp

        # Clean up first
        with self.engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS vector_advanced_docs"))

        # Create table
        self.Base.metadata.create_all(self.engine)
        print("✓ Created table: vector_advanced_docs")

        return AdvancedDocument

    def test_transaction_management(self, AdvancedDocument):
        """Test transaction management with vector operations."""
        print("\n" + "-" * 60)
        print("Testing Transaction Management")
        print("-" * 60)

        self.results['tests_run'] += 1

        try:
            # Test 1: Successful transaction
            print("1. Testing successful transaction:")

            with self.client.transaction() as tx:
                # Insert multiple documents
                docs_data = [
                    {
                        'title': f'Transaction Doc {i}',
                        'content': f'Content for transaction document {i}',
                        'category': 'Transaction',
                        'metadata_json': f'{{"batch": "test", "index": {i}}}',
                        'embedding': np.random.rand(256).tolist(),
                        'created_at': int(time.time()),
                    }
                    for i in range(1, 6)
                ]

                for doc_data in docs_data:
                    doc = AdvancedDocument(**doc_data)
                    self.session.add(doc)

                self.session.commit()
                print("  ✓ Inserted 5 documents in transaction")

            # Verify documents were inserted
            count = self.client.query(AdvancedDocument).filter(AdvancedDocument.category == 'Transaction').count()
            print(f"  ✓ Verified {count} documents in database")

            # Test 2: Failed transaction (rollback)
            print("\n2. Testing failed transaction (rollback):")

            initial_count = self.client.query(AdvancedDocument).count()

            try:
                with self.client.transaction() as tx:
                    # Insert a document
                    doc = AdvancedDocument(
                        title='Should Fail',
                        content='This should be rolled back',
                        category='Rollback',
                        metadata_json='{"test": "rollback"}',
                        embedding=np.random.rand(256).tolist(),
                        created_at=int(time.time()),
                    )
                    self.session.add(doc)
                    self.session.commit()

                    # Simulate an error
                    raise Exception("Simulated transaction failure")

            except Exception as e:
                print(f"  ✓ Transaction failed as expected: {e}")

            # Verify rollback
            final_count = self.client.query(AdvancedDocument).count()
            if final_count == initial_count:
                print("  ✓ Transaction was properly rolled back")
            else:
                print(f"  ✗ Transaction rollback failed: {initial_count} -> {final_count}")

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"✗ Transaction management test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'transaction_management', 'error': str(e)})

    async def test_async_operations(self, AdvancedDocument):
        """Test async vector operations."""
        print("\n" + "-" * 60)
        print("Testing Async Operations")
        print("-" * 60)

        self.results['tests_run'] += 1

        try:
            # Test async document insertion
            print("1. Testing async document insertion:")

            async_docs_data = [
                {
                    'title': f'Async Doc {i}',
                    'content': f'Content for async document {i}',
                    'category': 'Async',
                    'metadata_json': f'{{"async": true, "index": {i}}}',
                    'embedding': np.random.rand(256).tolist(),
                    'created_at': int(time.time()),
                }
                for i in range(1, 4)
            ]

            for doc_data in async_docs_data:
                # Convert vector to string format for async operations
                vector_str = str(doc_data['embedding'])
                await self.async_client.execute(
                    "INSERT INTO vector_advanced_docs (title, content, category, metadata_json, embedding, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        doc_data['title'],
                        doc_data['content'],
                        doc_data['category'],
                        doc_data['metadata_json'],
                        vector_str,
                        doc_data['created_at'],
                    ),
                )

            print("  ✓ Inserted 3 documents asynchronously")

            # Test async vector search
            print("\n2. Testing async vector search:")

            query_vector = np.random.rand(256).tolist()
            query_vector_str = str(query_vector)

            # Use raw SQL for async search (since ORM async is more complex)
            search_sql = """
                SELECT id, title, category, l2_distance(embedding, ?) as distance
                FROM vector_advanced_docs
                WHERE category = 'Async'
                ORDER BY l2_distance(embedding, ?)
                LIMIT 3
            """

            results = await self.async_client.execute(search_sql, (query_vector_str, query_vector_str))

            print(f"  ✓ Found {len(results)} async documents")
            for result in results:
                print(f"    - {result[1]} (Distance: {result[3]:.4f})")

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"✗ Async operations test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'async_operations', 'error': str(e)})

    def test_batch_operations(self, AdvancedDocument):
        """Test batch operations for performance."""
        print("\n" + "-" * 60)
        print("Testing Batch Operations")
        print("-" * 60)

        self.results['tests_run'] += 1

        try:
            # Test batch insertion
            print("1. Testing batch insertion:")

            batch_sizes = [10, 50, 100, 200]

            for batch_size in batch_sizes:
                start_time = time.time()

                # Generate batch data
                batch_data = [
                    {
                        'title': f'Batch Doc {i}',
                        'content': f'Content for batch document {i}',
                        'category': 'Batch',
                        'metadata_json': f'{{"batch_size": {batch_size}, "index": {i}}}',
                        'embedding': np.random.rand(256).tolist(),
                        'created_at': int(time.time()),
                    }
                    for i in range(1, batch_size + 1)
                ]

                # Insert batch
                for doc_data in batch_data:
                    doc = AdvancedDocument(**doc_data)
                    self.session.add(doc)
                self.session.commit()

                insertion_time = time.time() - start_time
                print(f"  Batch size {batch_size}: {insertion_time:.4f}s ({batch_size/insertion_time:.1f} docs/sec)")

                self.results['performance_metrics'][f'batch_insertion_{batch_size}'] = {
                    'time': insertion_time,
                    'throughput': batch_size / insertion_time,
                }

            # Test batch search
            print("\n2. Testing batch search:")

            query_vectors = [np.random.rand(256).tolist() for _ in range(10)]

            start_time = time.time()

            for i, query_vector in enumerate(query_vectors, 1):
                results = (
                    self.client.query(AdvancedDocument)
                    .select(
                        AdvancedDocument.id,
                        AdvancedDocument.title,
                        AdvancedDocument.embedding.l2_distance(query_vector).label('distance'),
                    )
                    .order_by(AdvancedDocument.embedding.l2_distance(query_vector))
                    .limit(5)
                    .all()
                )

                print(f"  Query {i}: {len(results)} results")

            batch_search_time = time.time() - start_time
            print(f"  Total batch search time: {batch_search_time:.4f}s")

            self.results['performance_metrics']['batch_search'] = batch_search_time
            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"✗ Batch operations test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'batch_operations', 'error': str(e)})

    def test_data_migration(self, AdvancedDocument):
        """Test vector data migration and transformation."""
        print("\n" + "-" * 60)
        print("Testing Data Migration")
        print("-" * 60)

        self.results['tests_run'] += 1

        try:
            # Test 1: Vector dimension transformation
            print("1. Testing vector dimension transformation:")

            # Create source data with different dimensions
            source_vectors = [np.random.rand(128).tolist() for _ in range(5)]

            # Transform to target dimension (256) by padding
            target_vectors = []
            for vec in source_vectors:
                # Pad with zeros to reach 256 dimensions
                padded_vec = vec + [0.0] * (256 - len(vec))
                target_vectors.append(padded_vec)

            # Insert transformed data
            for i, vec in enumerate(target_vectors):
                doc = AdvancedDocument(
                    title=f'Migrated Doc {i+1}',
                    content=f'Migrated from 128D to 256D',
                    category='Migration',
                    metadata_json=f'{{"original_dim": 128, "target_dim": 256, "index": {i+1}}}',
                    embedding=vec,
                    created_at=int(time.time()),
                )
                self.session.add(doc)

            self.session.commit()
            print("  ✓ Migrated 5 documents from 128D to 256D")

            # Test 2: Data validation after migration
            print("\n2. Testing data validation after migration:")

            migrated_docs = self.client.query(AdvancedDocument).filter(AdvancedDocument.category == 'Migration').all()

            for doc in migrated_docs:
                if len(doc.embedding) == 256:
                    print(f"  ✓ Document {doc.id}: Vector dimension correct (256)")
                else:
                    print(f"  ✗ Document {doc.id}: Vector dimension incorrect ({len(doc.embedding)})")

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"✗ Data migration test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'data_migration', 'error': str(e)})

    def test_performance_monitoring(self, AdvancedDocument):
        """Test performance monitoring and optimization."""
        print("\n" + "-" * 60)
        print("Testing Performance Monitoring")
        print("-" * 60)

        self.results['tests_run'] += 1

        try:
            # Test query performance monitoring
            print("1. Testing query performance monitoring:")

            query_vector = np.random.rand(256).tolist()

            # Test different query patterns
            query_patterns = [
                {
                    'name': 'Simple Search',
                    'query': lambda: self.client.query(AdvancedDocument)
                    .select(
                        AdvancedDocument.id,
                        AdvancedDocument.title,
                        AdvancedDocument.embedding.l2_distance(query_vector).label('distance'),
                    )
                    .order_by(AdvancedDocument.embedding.l2_distance(query_vector))
                    .limit(10)
                    .all(),
                },
                {
                    'name': 'Filtered Search',
                    'query': lambda: self.client.query(AdvancedDocument)
                    .select(
                        AdvancedDocument.id,
                        AdvancedDocument.title,
                        AdvancedDocument.embedding.l2_distance(query_vector).label('distance'),
                    )
                    .filter(AdvancedDocument.category == 'Batch')
                    .order_by(AdvancedDocument.embedding.l2_distance(query_vector))
                    .limit(10)
                    .all(),
                },
                {
                    'name': 'Range Search',
                    'query': lambda: self.client.query(AdvancedDocument)
                    .select(
                        AdvancedDocument.id,
                        AdvancedDocument.title,
                        AdvancedDocument.embedding.l2_distance(query_vector).label('distance'),
                    )
                    .filter(AdvancedDocument.embedding.l2_distance(query_vector) < 2.0)
                    .order_by(AdvancedDocument.embedding.l2_distance(query_vector))
                    .limit(10)
                    .all(),
                },
            ]

            for pattern in query_patterns:
                start_time = time.time()
                results = pattern['query']()
                query_time = time.time() - start_time

                print(f"  {pattern['name']}: {query_time:.4f}s ({len(results)} results)")

                self.results['performance_metrics'][f'query_{pattern["name"].lower().replace(" ", "_")}'] = query_time

            # Test memory usage monitoring
            print("\n2. Testing memory usage monitoring:")

            # Get table statistics
            table_stats = self.client.execute("SHOW TABLE STATUS LIKE 'vector_advanced_docs'")
            if table_stats:
                # Convert ResultSet to list for easier access
                stats_list = list(table_stats)
                if stats_list and len(stats_list) > 0:
                    stats_row = stats_list[0]
                    if len(stats_row) > 6:
                        print(f"  Table size: {stats_row[6]} rows")
                    if len(stats_row) > 7:
                        print(f"  Data length: {stats_row[7]} bytes")

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"✗ Performance monitoring test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'performance_monitoring', 'error': str(e)})

    def test_error_handling_recovery(self, AdvancedDocument):
        """Test error handling and recovery patterns."""
        print("\n" + "-" * 60)
        print("Testing Error Handling and Recovery")
        print("-" * 60)

        self.results['tests_run'] += 1

        try:
            # Test 1: Invalid vector dimension handling
            print("1. Testing invalid vector dimension handling:")

            try:
                doc = AdvancedDocument(
                    title='Invalid Vector',
                    content='This should fail',
                    category='Error',
                    metadata_json='{"test": "invalid_vector"}',
                    embedding=[1.0, 2.0],  # Wrong dimension (2 instead of 256)
                    created_at=int(time.time()),
                )
                self.session.add(doc)
                self.session.commit()
                print("  ✗ Unexpected: Invalid vector was accepted")
            except Exception as e:
                print(f"  ✓ Expected: Invalid vector rejected - {e}")
                # Rollback the session to clear the failed transaction
                self.session.rollback()

            # Test 2: Connection recovery
            print("\n2. Testing connection recovery:")

            # Simulate connection issues by testing reconnection
            try:
                # Test if connection is still alive
                test_result = self.client.execute("SELECT 1")
                if test_result:
                    print("  ✓ Connection is healthy")

                # Test reconnection
                self.client.disconnect()
                # Get connection parameters for reconnection
                host, port, user, password, database = get_connection_params()
                self.client.connect(host=host, port=port, user=user, password=password, database=database)
                test_result = self.client.execute("SELECT 1")
                if test_result:
                    print("  ✓ Reconnection successful")

            except Exception as e:
                print(f"  ✗ Connection recovery failed: {e}")

            # Test 3: Data consistency checks
            print("\n3. Testing data consistency checks:")

            # Check for orphaned or corrupted data
            total_docs = self.client.query(AdvancedDocument).count()
            docs_with_embeddings = self.client.query(AdvancedDocument).filter(AdvancedDocument.embedding.isnot(None)).count()

            print(f"  Total documents: {total_docs}")
            print(f"  Documents with embeddings: {docs_with_embeddings}")

            if total_docs == docs_with_embeddings:
                print("  ✓ Data consistency check passed")
            else:
                print(f"  ✗ Data consistency issue: {total_docs - docs_with_embeddings} documents missing embeddings")

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"✗ Error handling and recovery test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'error_handling_recovery', 'error': str(e)})

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
                    conn.execute(text("DROP TABLE IF EXISTS vector_advanced_docs"))
                print("✓ Cleaned up test table")

            if self.client:
                self.client.disconnect()
                print("✓ Disconnected from MatrixOne (Sync)")

        except Exception as e:
            print(f"✗ Cleanup failed: {e}")

    async def async_cleanup(self):
        """Clean up async resources."""
        try:
            if self.async_client:
                await self.async_client.disconnect()
                print("✓ Disconnected from MatrixOne (Async)")
        except Exception as e:
            print(f"✗ Async cleanup failed: {e}")

    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        print("\n" + "=" * 80)
        print("Vector Advanced Operations Demo - Summary Report")
        print("=" * 80)

        total_tests = self.results['tests_run']
        passed_tests = self.results['tests_passed']
        failed_tests = self.results['tests_failed']
        unexpected_results = self.results['unexpected_results']
        performance_metrics = self.results['performance_metrics']

        print(f"Total Tests Run: {total_tests}")
        print(f"Tests Passed: {passed_tests}")
        print(f"Tests Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests*100):.1f}%" if total_tests > 0 else "N/A")

        # Performance summary
        if performance_metrics:
            print(f"\nPerformance Metrics:")
            for metric_name, value in performance_metrics.items():
                if isinstance(value, dict):
                    print(f"  {metric_name}:")
                    for sub_metric, sub_value in value.items():
                        print(f"    {sub_metric}: {sub_value}")
                else:
                    print(f"  {metric_name}: {value}")

        # Unexpected results
        if unexpected_results:
            print(f"\nUnexpected Results ({len(unexpected_results)}):")
            for i, result in enumerate(unexpected_results, 1):
                print(f"  {i}. Test: {result['test']}")
                print(f"     Error: {result['error']}")
        else:
            print("\n✓ No unexpected results - all tests behaved as expected")

        return self.results


async def main():
    """Main function to run the vector advanced operations demo."""
    demo = VectorAdvancedDemo()

    try:
        # Setup
        demo.setup_connection()
        await demo.setup_async_connection()
        AdvancedDocument = demo.create_test_table()

        # Run tests
        demo.test_transaction_management(AdvancedDocument)
        await demo.test_async_operations(AdvancedDocument)
        demo.test_batch_operations(AdvancedDocument)
        demo.test_data_migration(AdvancedDocument)
        demo.test_performance_monitoring(AdvancedDocument)
        demo.test_error_handling_recovery(AdvancedDocument)

        # Generate report
        results = demo.generate_summary_report()

        return results

    except Exception as e:
        print(f"Demo failed with error: {e}")
        return None

    finally:
        # Clean up async resources first
        await demo.async_cleanup()
        # Then clean up sync resources
        demo.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
