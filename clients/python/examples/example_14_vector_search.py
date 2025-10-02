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
Example 14: Vector Search - Advanced Search and Query Operations

This example demonstrates advanced vector search capabilities in MatrixOne:
- Similarity search with different distance metrics
- Range search and filtering
- Hybrid search (vector + text)
- Batch search operations
- Search result ranking and scoring
- Performance optimization techniques

This replaces examples with enhanced search functionality.
"""

import sys
import os
import numpy as np
import time
from typing import List, Dict, Any, Optional, Tuple

# Add the parent directory to the path so we can import matrixone
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import Column, Integer, String, Text, func, text, and_, or_
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from matrixone import Client
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger
from matrixone.sqlalchemy_ext import create_vector_column


class VectorSearchDemo:
    """Demonstrates advanced vector search capabilities with comprehensive testing."""

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
            'search_performance': {},
        }

    def setup_connection(self):
        """Setup database connection."""
        print("=" * 80)
        print("Vector Search Demo - Setup")
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

    def create_test_table(self):
        """Create test table with vector column and index."""
        print("\n" + "-" * 60)
        print("Creating Test Table and Index")
        print("-" * 60)

        # Define test model
        class SearchDocument(self.Base):
            __tablename__ = 'vector_search_docs'

            id = Column(Integer, primary_key=True, autoincrement=True)
            title = Column(String(200), nullable=False)
            content = Column(Text)
            category = Column(String(50))
            tags = Column(String(200))
            embedding = create_vector_column(128, precision='f32')  # 128-dimensional vector

        # Clean up first
        with self.engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS vector_search_docs"))

        # Create table
        self.Base.metadata.create_all(self.engine)
        print("✓ Created table: vector_search_docs")

        # Create vector index for better performance
        try:
            self.client.vector_ops.enable_ivf()
            self.client.vector_ops.create_ivf(
                name='search_ivf_index',
                table_name='vector_search_docs',
                column='embedding',
                lists=100,
                op_type='vector_l2_ops',
            )
            print("✓ Created IVF index for search optimization")
        except Exception as e:
            print(f"⚠ Index creation failed (continuing without index): {e}")

        return SearchDocument

    def insert_test_data(self, SearchDocument, num_docs=1000):
        """Insert diverse test data for comprehensive search testing."""
        print(f"\n" + "-" * 60)
        print(f"Inserting Test Data ({num_docs} documents)")
        print("-" * 60)

        # Generate diverse test documents
        categories = [
            'AI',
            'Machine Learning',
            'Deep Learning',
            'NLP',
            'Computer Vision',
            'Robotics',
        ]
        tags_list = [
            'research',
            'tutorial',
            'application',
            'theory',
            'practice',
            'advanced',
            'beginner',
        ]

        test_docs = []
        for i in range(num_docs):
            category = categories[i % len(categories)]
            tags = ', '.join(np.random.choice(tags_list, size=np.random.randint(1, 4), replace=False))

            test_docs.append(
                {
                    'title': f'{category} Document {i+1}',
                    'content': f'This is a comprehensive document about {category.lower()}. It covers various aspects including theory, applications, and practical examples. Document {i+1} provides detailed insights into the field.',
                    'category': category,
                    'tags': tags,
                    'embedding': np.random.rand(128).tolist(),
                }
            )

        # Insert documents in batches
        batch_size = 100
        for i in range(0, len(test_docs), batch_size):
            batch = test_docs[i : i + batch_size]
            for doc_data in batch:
                doc = SearchDocument(**doc_data)
                self.session.add(doc)
            self.session.commit()
            print(f"✓ Inserted batch {i//batch_size + 1}/{(len(test_docs)-1)//batch_size + 1}")

        print(f"✓ Inserted {num_docs} test documents")

    def test_similarity_search(self, SearchDocument):
        """Test similarity search with different distance metrics."""
        print("\n" + "-" * 60)
        print("Testing Similarity Search")
        print("-" * 60)

        # Generate query vector
        query_vector = np.random.rand(128).tolist()

        # Test different distance metrics
        distance_metrics = [
            ('L2 Distance', SearchDocument.embedding.l2_distance),
            ('Cosine Distance', SearchDocument.embedding.cosine_distance),
            ('Inner Product', SearchDocument.embedding.inner_product),
        ]

        for metric_name, distance_func in distance_metrics:
            self.results['tests_run'] += 1

            try:
                start_time = time.time()

                # Perform similarity search
                results = (
                    self.session.query(
                        SearchDocument.id,
                        SearchDocument.title,
                        SearchDocument.category,
                        distance_func(query_vector).label('distance'),
                    )
                    .order_by(distance_func(query_vector))
                    .limit(10)
                    .all()
                )

                search_time = time.time() - start_time

                print(f"\n{metric_name} Results:")
                print(f"  Search Time: {search_time:.4f}s")
                print(f"  Results Found: {len(results)}")

                if results:
                    print(f"  Top 3 Results:")
                    for i, result in enumerate(results[:3], 1):
                        print(f"    {i}. {result.title} (Category: {result.category}, Distance: {result.distance:.4f})")

                self.results['search_performance'][f'{metric_name.lower().replace(" ", "_")}_search'] = search_time
                self.results['tests_passed'] += 1

            except Exception as e:
                print(f"✗ {metric_name} search failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append(
                    {
                        'test': f'similarity_search_{metric_name.lower().replace(" ", "_")}',
                        'error': str(e),
                    }
                )

    def test_range_search(self, SearchDocument):
        """Test range search with distance thresholds."""
        print("\n" + "-" * 60)
        print("Testing Range Search")
        print("-" * 60)

        self.results['tests_run'] += 1

        try:
            query_vector = np.random.rand(128).tolist()

            # Test different distance thresholds
            thresholds = [0.5, 1.0, 1.5, 2.0]

            for threshold in thresholds:
                start_time = time.time()

                results = (
                    self.session.query(
                        SearchDocument.id,
                        SearchDocument.title,
                        SearchDocument.embedding.l2_distance(query_vector).label('distance'),
                    )
                    .filter(SearchDocument.embedding.l2_distance(query_vector) < threshold)
                    .order_by(SearchDocument.embedding.l2_distance(query_vector))
                    .all()
                )

                search_time = time.time() - start_time

                print(f"  Threshold {threshold}: {len(results)} results in {search_time:.4f}s")

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"✗ Range search failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'range_search', 'error': str(e)})

    def test_hybrid_search(self, SearchDocument):
        """Test hybrid search combining vector similarity and text filtering."""
        print("\n" + "-" * 60)
        print("Testing Hybrid Search")
        print("-" * 60)

        self.results['tests_run'] += 1

        try:
            query_vector = np.random.rand(128).tolist()

            # Test different hybrid search scenarios
            scenarios = [
                {
                    'name': 'AI Category + Vector Similarity',
                    'filter': SearchDocument.category == 'AI',
                    'description': 'Find AI documents by vector similarity',
                },
                {
                    'name': 'Title Contains + Vector Similarity',
                    'filter': SearchDocument.title.like('%Machine%'),
                    'description': 'Find Machine Learning documents by vector similarity',
                },
                {
                    'name': 'Multiple Categories + Vector Similarity',
                    'filter': SearchDocument.category.in_(['AI', 'Machine Learning']),
                    'description': 'Find AI/ML documents by vector similarity',
                },
            ]

            for scenario in scenarios:
                start_time = time.time()

                results = (
                    self.session.query(
                        SearchDocument.id,
                        SearchDocument.title,
                        SearchDocument.category,
                        SearchDocument.embedding.l2_distance(query_vector).label('distance'),
                    )
                    .filter(
                        and_(
                            scenario['filter'],
                            SearchDocument.embedding.l2_distance(query_vector) < 2.0,
                        )
                    )
                    .order_by(SearchDocument.embedding.l2_distance(query_vector))
                    .limit(5)
                    .all()
                )

                search_time = time.time() - start_time

                print(f"\n{scenario['name']}:")
                print(f"  {scenario['description']}")
                print(f"  Results: {len(results)} in {search_time:.4f}s")

                for result in results:
                    print(f"    - {result.title} (Distance: {result.distance:.4f})")

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"✗ Hybrid search failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'hybrid_search', 'error': str(e)})

    def test_batch_search(self, SearchDocument):
        """Test batch search operations."""
        print("\n" + "-" * 60)
        print("Testing Batch Search")
        print("-" * 60)

        self.results['tests_run'] += 1

        try:
            # Generate multiple query vectors
            query_vectors = [np.random.rand(128).tolist() for _ in range(5)]

            print(f"Performing batch search with {len(query_vectors)} queries:")

            total_start_time = time.time()

            for i, query_vector in enumerate(query_vectors, 1):
                start_time = time.time()

                results = (
                    self.session.query(
                        SearchDocument.id,
                        SearchDocument.title,
                        SearchDocument.embedding.l2_distance(query_vector).label('distance'),
                    )
                    .order_by(SearchDocument.embedding.l2_distance(query_vector))
                    .limit(3)
                    .all()
                )

                search_time = time.time() - start_time

                print(f"  Query {i}: {len(results)} results in {search_time:.4f}s")
                if results:
                    print(f"    Best match: {results[0].title} (Distance: {results[0].distance:.4f})")

            total_time = time.time() - total_start_time
            print(f"  Total batch search time: {total_time:.4f}s")

            self.results['search_performance']['batch_search_total'] = total_time
            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"✗ Batch search failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'batch_search', 'error': str(e)})

    def test_search_optimization(self, SearchDocument):
        """Test search optimization techniques."""
        print("\n" + "-" * 60)
        print("Testing Search Optimization")
        print("-" * 60)

        self.results['tests_run'] += 1

        try:
            query_vector = np.random.rand(128).tolist()

            # Test 1: Column projection optimization
            print("1. Column Projection Optimization:")

            # Without projection (select all columns)
            start_time = time.time()
            results_all = (
                self.session.query(SearchDocument)
                .order_by(SearchDocument.embedding.l2_distance(query_vector))
                .limit(10)
                .all()
            )
            time_all = time.time() - start_time

            # With projection (select only needed columns)
            start_time = time.time()
            results_proj = (
                self.session.query(
                    SearchDocument.id,
                    SearchDocument.title,
                    SearchDocument.embedding.l2_distance(query_vector).label('distance'),
                )
                .order_by(SearchDocument.embedding.l2_distance(query_vector))
                .limit(10)
                .all()
            )
            time_proj = time.time() - start_time

            print(f"  Without projection: {time_all:.4f}s")
            print(f"  With projection: {time_proj:.4f}s")
            print(f"  Improvement: {((time_all - time_proj) / time_all * 100):.1f}%")

            # Test 2: Limit optimization
            print("\n2. Limit Optimization:")
            limits = [5, 10, 20, 50, 100]

            for limit in limits:
                start_time = time.time()
                results = (
                    self.session.query(
                        SearchDocument.id,
                        SearchDocument.title,
                        SearchDocument.embedding.l2_distance(query_vector).label('distance'),
                    )
                    .order_by(SearchDocument.embedding.l2_distance(query_vector))
                    .limit(limit)
                    .all()
                )
                search_time = time.time() - start_time

                print(f"  Limit {limit}: {search_time:.4f}s")

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"✗ Search optimization test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'search_optimization', 'error': str(e)})

    def test_search_result_ranking(self, SearchDocument):
        """Test search result ranking and scoring."""
        print("\n" + "-" * 60)
        print("Testing Search Result Ranking")
        print("-" * 60)

        self.results['tests_run'] += 1

        try:
            query_vector = np.random.rand(128).tolist()

            # Test different ranking strategies
            print("1. Distance-based Ranking:")
            results = (
                self.session.query(
                    SearchDocument.id,
                    SearchDocument.title,
                    SearchDocument.embedding.l2_distance(query_vector).label('l2_distance'),
                    SearchDocument.embedding.cosine_distance(query_vector).label('cosine_distance'),
                    SearchDocument.embedding.inner_product(query_vector).label('inner_product'),
                )
                .order_by(SearchDocument.embedding.l2_distance(query_vector))
                .limit(5)
                .all()
            )

            print("  Top 5 results by L2 distance:")
            for i, result in enumerate(results, 1):
                print(f"    {i}. {result.title}")
                print(
                    f"       L2: {result.l2_distance:.4f}, Cosine: {result.cosine_distance:.4f}, Inner: {result.inner_product:.4f}"
                )

            # Test score normalization
            print("\n2. Score Normalization:")
            if results:
                l2_scores = [r.l2_distance for r in results]
                min_l2, max_l2 = min(l2_scores), max(l2_scores)

                print(f"  L2 Distance Range: {min_l2:.4f} - {max_l2:.4f}")

                for result in results:
                    normalized_score = (result.l2_distance - min_l2) / (max_l2 - min_l2) if max_l2 > min_l2 else 0
                    print(f"    {result.title}: Normalized Score = {normalized_score:.4f}")

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"✗ Search result ranking test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'search_result_ranking', 'error': str(e)})

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
                    conn.execute(text("DROP TABLE IF EXISTS vector_search_docs"))
                print("✓ Cleaned up test table")

            if self.client:
                self.client.disconnect()
                print("✓ Disconnected from MatrixOne")

        except Exception as e:
            print(f"✗ Cleanup failed: {e}")

    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        print("\n" + "=" * 80)
        print("Vector Search Demo - Summary Report")
        print("=" * 80)

        total_tests = self.results['tests_run']
        passed_tests = self.results['tests_passed']
        failed_tests = self.results['tests_failed']
        unexpected_results = self.results['unexpected_results']
        search_performance = self.results['search_performance']

        print(f"Total Tests Run: {total_tests}")
        print(f"Tests Passed: {passed_tests}")
        print(f"Tests Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests*100):.1f}%" if total_tests > 0 else "N/A")

        # Performance summary
        if search_performance:
            print(f"\nSearch Performance Results:")
            for test_name, time_taken in search_performance.items():
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
    """Main function to run the vector search demo."""
    demo = VectorSearchDemo()

    try:
        # Setup
        demo.setup_connection()
        SearchDocument = demo.create_test_table()

        # Insert test data
        demo.insert_test_data(SearchDocument, num_docs=500)  # Reduced for faster testing

        # Run tests
        demo.test_similarity_search(SearchDocument)
        demo.test_range_search(SearchDocument)
        demo.test_hybrid_search(SearchDocument)
        demo.test_batch_search(SearchDocument)
        demo.test_search_optimization(SearchDocument)
        demo.test_search_result_ranking(SearchDocument)

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
