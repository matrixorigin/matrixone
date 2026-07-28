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
Example 12: Vector Basics - Distance Functions and Basic Operations

This example demonstrates the fundamental vector operations in MatrixOne:
- Vector data types and column creation
- Basic distance functions (L2, Cosine, Inner Product)
- Vector parameter formats (list vs string)
- Basic CRUD operations with vectors
- Error handling and validation

This replaces the original example_12_vector_search.py with enhanced functionality.
"""

import sys
import os
import numpy as np
from typing import List, Dict, Any, Optional

# Add the parent directory to the path so we can import matrixone
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import Column, Integer, String, Text, func, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from matrixone import Client
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger
from matrixone.sqlalchemy_ext import create_vector_column


class VectorBasicsDemo:
    """Demonstrates basic vector operations with comprehensive error handling."""

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
        }

    def setup_connection(self):
        """Setup database connection and create test table."""
        print("=" * 80)
        print("Vector Basics Demo - Setup")
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
        """Create test table with vector column."""
        print("\n" + "-" * 60)
        print("Creating Test Table")
        print("-" * 60)

        # Define test model
        class TestDocument(self.Base):
            __tablename__ = 'vector_basics_docs'

            id = Column(Integer, primary_key=True, autoincrement=True)
            title = Column(String(200), nullable=False)
            content = Column(Text)
            embedding = create_vector_column(16, precision='f32', nullable=False)  # 16-dimensional vector

        # Clean up first
        with self.engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS vector_basics_docs"))

        # Create table
        self.Base.metadata.create_all(self.engine)
        print("✓ Created table: vector_basics_docs")

        return TestDocument

    def test_vector_parameter_formats(self, TestDocument):
        """Test different vector parameter formats."""
        print("\n" + "-" * 60)
        print("Testing Vector Parameter Formats")
        print("-" * 60)

        self.results['tests_run'] += 1

        try:
            # Generate test vectors in both formats
            vector_list = np.random.rand(16).tolist()  # List format
            vector_str = str(vector_list)  # String format

            print(f"List format: {vector_list[:5]}...")
            print(f"String format: {vector_str[:50]}...")

            # Test both formats work in distance calculations
            list_distance = self.session.query(TestDocument.embedding.l2_distance(vector_list).label('distance')).scalar()

            str_distance = self.session.query(TestDocument.embedding.l2_distance(vector_str).label('distance')).scalar()

            print(f"✓ List format distance: {list_distance}")
            print(f"✓ String format distance: {str_distance}")

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"✗ Vector parameter format test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'vector_parameter_formats', 'error': str(e)})

    def test_distance_functions(self, TestDocument):
        """Test all available distance functions."""
        print("\n" + "-" * 60)
        print("Testing Distance Functions")
        print("-" * 60)

        # Insert test data
        test_docs = [
            (
                'AI Introduction',
                'Introduction to artificial intelligence',
                np.random.rand(16).tolist(),
            ),
            ('Machine Learning', 'Machine learning fundamentals', np.random.rand(16).tolist()),
            (
                'Deep Learning',
                'Deep learning concepts and applications',
                np.random.rand(16).tolist(),
            ),
            (
                'Natural Language Processing',
                'NLP techniques and models',
                np.random.rand(16).tolist(),
            ),
            ('Computer Vision', 'Computer vision applications', np.random.rand(16).tolist()),
        ]

        for title, content, embedding in test_docs:
            doc = TestDocument(title=title, content=content, embedding=embedding)
            self.session.add(doc)

        self.session.commit()
        print(f"✓ Inserted {len(test_docs)} test documents")

        # Generate query vector
        query_vector = np.random.rand(16).tolist()

        # Test all distance functions
        distance_functions = [
            ('L2 Distance', TestDocument.embedding.l2_distance),
            ('L2 Distance Squared', TestDocument.embedding.l2_distance_sq),
            ('Cosine Distance', TestDocument.embedding.cosine_distance),
            ('Inner Product', TestDocument.embedding.inner_product),
            ('Negative Inner Product', TestDocument.embedding.negative_inner_product),
        ]

        for func_name, func in distance_functions:
            self.results['tests_run'] += 1

            try:
                # Test distance function
                result = (
                    self.session.query(TestDocument.id, TestDocument.title, func(query_vector).label('distance'))
                    .order_by(func(query_vector))
                    .limit(3)
                    .all()
                )

                print(f"\n{func_name} Results:")
                for row in result:
                    print(f"  Document {row.id}: {row.title} (Distance: {row.distance:.4f})")

                self.results['tests_passed'] += 1

            except Exception as e:
                print(f"✗ {func_name} test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append(
                    {
                        'test': f'distance_function_{func_name.lower().replace(" ", "_")}',
                        'error': str(e),
                    }
                )

    def test_vector_crud_operations(self, TestDocument):
        """Test CRUD operations with vector data."""
        print("\n" + "-" * 60)
        print("Testing Vector CRUD Operations")
        print("-" * 60)

        self.results['tests_run'] += 1

        try:
            # Create
            new_doc = TestDocument(
                title='New Document',
                content='This is a new document',
                embedding=np.random.rand(16).tolist(),
            )
            self.session.add(new_doc)
            self.session.commit()

            doc_id = new_doc.id
            print(f"✓ Created document with ID: {doc_id}")

            # Read
            retrieved_doc = self.session.query(TestDocument).filter(TestDocument.id == doc_id).first()
            if retrieved_doc:
                print(f"✓ Retrieved document: {retrieved_doc.title}")
            else:
                raise Exception("Failed to retrieve document")

            # Update
            new_embedding = np.random.rand(16).tolist()
            retrieved_doc.embedding = new_embedding
            retrieved_doc.title = 'Updated Document'
            self.session.commit()
            print("✓ Updated document")

            # Delete
            self.session.delete(retrieved_doc)
            self.session.commit()
            print("✓ Deleted document")

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"✗ CRUD operations test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'vector_crud_operations', 'error': str(e)})

    def test_vector_validation(self, TestDocument):
        """Test vector data validation and error handling."""
        print("\n" + "-" * 60)
        print("Testing Vector Validation")
        print("-" * 60)

        validation_tests = [
            ('Wrong dimension', np.random.rand(8).tolist(), "Should fail with wrong dimension"),
            ('Empty vector', [], "Should fail with empty vector"),
            ('Invalid string', "invalid_vector", "Should fail with invalid string"),
            ('None value', None, "Should fail with None value"),
        ]

        for test_name, invalid_vector, expected_behavior in validation_tests:
            self.results['tests_run'] += 1

            try:
                doc = TestDocument(title=f'Test {test_name}', content='Test document', embedding=invalid_vector)
                self.session.add(doc)
                self.session.commit()

                # If we get here, the test should have failed
                print(f"✗ {test_name}: Expected failure but succeeded")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append(
                    {
                        'test': f'validation_{test_name.lower().replace(" ", "_")}',
                        'error': f'Expected failure but succeeded: {expected_behavior}',
                    }
                )

                # Clean up
                self.session.delete(doc)
                self.session.commit()

            except Exception as e:
                print(f"✓ {test_name}: Failed as expected - {e}")
                self.results['tests_passed'] += 1
                self.session.rollback()

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
                    conn.execute(text("DROP TABLE IF EXISTS vector_basics_docs"))
                print("✓ Cleaned up test table")

            if self.client:
                self.client.disconnect()
                print("✓ Disconnected from MatrixOne")

        except Exception as e:
            print(f"✗ Cleanup failed: {e}")

    def generate_summary_report(self):
        """Generate summary report of all tests."""
        print("\n" + "=" * 80)
        print("Vector Basics Demo - Summary Report")
        print("=" * 80)

        total_tests = self.results['tests_run']
        passed_tests = self.results['tests_passed']
        failed_tests = self.results['tests_failed']
        unexpected_results = self.results['unexpected_results']

        print(f"Total Tests Run: {total_tests}")
        print(f"Tests Passed: {passed_tests}")
        print(f"Tests Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests*100):.1f}%" if total_tests > 0 else "N/A")

        if unexpected_results:
            print(f"\nUnexpected Results ({len(unexpected_results)}):")
            for i, result in enumerate(unexpected_results, 1):
                print(f"  {i}. Test: {result['test']}")
                print(f"     Error: {result['error']}")
        else:
            print("\n✓ No unexpected results - all tests behaved as expected")

        return self.results


def main():
    """Main function to run the vector basics demo."""
    demo = VectorBasicsDemo()

    try:
        # Setup
        demo.setup_connection()
        TestDocument = demo.create_test_table()

        # Run tests
        demo.test_vector_parameter_formats(TestDocument)
        demo.test_distance_functions(TestDocument)
        demo.test_vector_crud_operations(TestDocument)
        demo.test_vector_validation(TestDocument)

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
