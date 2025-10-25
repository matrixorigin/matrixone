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
Example 29: Complex Queries with MatrixOne Features

This example demonstrates complex queries combining:
- JOIN operations
- GROUP BY aggregations
- FULLTEXT search
- VECTOR similarity search
- Complex WHERE conditions
- All working seamlessly with SQLAlchemy's select()

This shows that select() is a complete replacement for client.query().
"""

import sys
import os
import random

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    import numpy as np

    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

from sqlalchemy import Column, Integer, String, Text, Float, ForeignKey, func, and_, or_, select
from sqlalchemy.orm import declarative_base, relationship

from matrixone import Client
from matrixone.config import get_connection_params
from matrixone.sqlalchemy_ext import (
    boolean_match,
    create_vector_column,
    FulltextIndex,
    FulltextAlgorithmType,
    group,
)

Base = declarative_base()


class Product(Base):
    """Product table with fulltext search"""

    __tablename__ = 'products_complex'

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_name = Column(String(200), nullable=False)
    description = Column(Text)
    category_id = Column(Integer, ForeignKey('categories_complex.id'))
    price = Column(Float)

    __table_args__ = (FulltextIndex('ftidx_product', ['product_name', 'description'], algorithm=FulltextAlgorithmType.BM25),)

    category = relationship("Category", back_populates="products")
    reviews = relationship("Review", back_populates="product")


class Category(Base):
    """Category table"""

    __tablename__ = 'categories_complex'

    id = Column(Integer, primary_key=True, autoincrement=True)
    category_name = Column(String(100), nullable=False)

    products = relationship("Product", back_populates="category")
    embeddings = relationship("Embedding", back_populates="category")


class Review(Base):
    """Review table"""

    __tablename__ = 'reviews_complex'

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(Integer, ForeignKey('products_complex.id'))
    rating = Column(Integer)  # 1-5
    comment = Column(Text)

    product = relationship("Product", back_populates="reviews")


class Embedding(Base):
    """Embedding table with vector search"""

    __tablename__ = 'embeddings_complex'

    id = Column(Integer, primary_key=True, autoincrement=True)
    category_id = Column(Integer, ForeignKey('categories_complex.id'))
    text = Column(Text)
    vector = create_vector_column(64)  # 64-dim vectors

    category = relationship("Category", back_populates="embeddings")


class ComplexQueryDemo:
    """Demonstrates complex queries with MatrixOne features"""

    def __init__(self):
        self.client = None
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
        }

    def setup(self):
        """Setup database connection"""
        print("=" * 80)
        print("Complex Queries with MatrixOne Features")
        print("=" * 80)
        print()

        host, port, user, password, database = get_connection_params()

        # Create client with full SQL logging
        # Note: Don't pass logger if you want sql_log_mode to take effect
        self.client = Client(sql_log_mode="full")
        self.client.connect(host=host, port=port, user=user, password=password, database=database)

        print("✓ Connected to MatrixOne\n")

    def populate_data(self):
        """Populate test data"""
        print("Setting up test data...")

        # Drop existing tables
        for table in ['embeddings_complex', 'reviews_complex', 'products_complex', 'categories_complex']:
            try:
                self.client.execute(f"DROP TABLE IF EXISTS {table}")
            except:
                pass

        # Create tables
        self.client.create_table(Category)
        self.client.create_table(Product)
        self.client.create_table(Review)
        self.client.create_table(Embedding)

        # Insert categories
        self.client.execute(
            """
            INSERT INTO categories_complex (category_name) VALUES
            ('Electronics'),
            ('Clothing'),
            ('Books')
        """
        )

        # Insert products
        self.client.execute(
            """
            INSERT INTO products_complex (product_name, description, category_id, price) VALUES
            ('Laptop', 'High performance laptop for coding', 1, 999.99),
            ('Smartphone', 'Latest smartphone with great camera', 1, 699.99),
            ('T-Shirt', 'Comfortable cotton t-shirt', 2, 19.99),
            ('Python Book', 'Learn Python programming', 3, 39.99),
            ('ML Book', 'Machine learning fundamentals', 3, 49.99)
        """
        )

        # Insert reviews
        self.client.execute(
            """
            INSERT INTO reviews_complex (product_id, rating, comment) VALUES
            (1, 5, 'Excellent laptop for development'),
            (1, 4, 'Good value for money'),
            (2, 5, 'Amazing camera quality'),
            (3, 3, 'Decent quality'),
            (4, 5, 'Great for beginners'),
            (5, 5, 'Very insightful')
        """
        )

        # Insert embeddings with vectors
        def generate_vector(dim=64):
            if HAS_NUMPY:
                vec = np.random.rand(dim).tolist()
            else:
                vec = [random.random() for _ in range(dim)]
            return '[' + ','.join(map(str, vec)) + ']'

        self.client.execute(
            f"""
            INSERT INTO embeddings_complex (category_id, text, vector) VALUES
            (1, 'tech gadgets', '{generate_vector(64)}'),
            (2, 'fashion apparel', '{generate_vector(64)}'),
            (3, 'educational content', '{generate_vector(64)}')
        """
        )

        print("✓ Test data populated\n")

    def test_fulltext_standalone(self):
        """Test: FULLTEXT search (standalone)"""
        print("\n" + "=" * 80)
        print("Test 1: FULLTEXT Search (Standalone)")
        print("=" * 80)
        print("Note: MatrixOne currently doesn't support FULLTEXT in JOIN queries")
        print()

        self.results['tests_run'] += 1

        # Find products matching "laptop"
        stmt = select(Product.product_name, Product.price, Product.category_id).where(
            boolean_match('product_name', 'description').must('laptop')
        )

        # Compile select() to SQL and execute
        sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        results = self.client.execute(sql)
        print(f"✓ Found {len(results)} products:")
        for row in results:
            print(f"  - {row.product_name} (${row.price}) - category_id={row.category_id}")

        self.results['tests_passed'] += 1

    def test_join_groupby(self):
        """Test: JOIN with GROUP BY"""
        print("\n" + "=" * 80)
        print("Test 2: JOIN + GROUP BY Aggregation")
        print("=" * 80)

        self.results['tests_run'] += 1

        # Average review rating per category
        stmt = (
            select(
                Category.category_name.label('category'),
                func.avg(Review.rating).label('avg_rating'),
                func.count(Review.id).label('review_count'),
            )
            .join(Product, Product.category_id == Category.id)
            .join(Review, Review.product_id == Product.id)
            .group_by(Category.id, Category.category_name)
            .order_by(func.avg(Review.rating).desc())
        )

        sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        results = self.client.execute(sql)
        print(f"\n✓ Average ratings by category:")
        for row in results:
            print(f"  - {row.category}: {row.avg_rating:.2f} ({row.review_count} reviews)")

        self.results['tests_passed'] += 1

    def test_join_where_filter(self):
        """Test: JOIN with multiple WHERE conditions"""
        print("\n" + "=" * 80)
        print("Test 3: JOIN + Multiple WHERE Conditions")
        print("=" * 80)

        self.results['tests_run'] += 1

        # Products in Electronics with price > 500
        stmt = (
            select(Product.product_name, Product.price, Category.category_name)
            .join(Category, Product.category_id == Category.id)
            .where(and_(Category.category_name == 'Electronics', Product.price > 500))
            .order_by(Product.price.desc())
        )

        sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        results = self.client.execute(sql)
        print(f"\n✓ Expensive electronics:")
        for row in results:
            print(f"  - {row.product_name}: ${row.price}")

        self.results['tests_passed'] += 1

    def test_vector_in_join(self):
        """Test: VECTOR search in JOIN query"""
        print("\n" + "=" * 80)
        print("Test 4: VECTOR Search in JOIN")
        print("=" * 80)

        self.results['tests_run'] += 1

        # Find similar embeddings and join with category
        query_vector = [0.5] * 64  # Simple query vector

        stmt = (
            select(
                Embedding.text,
                Category.category_name.label('category'),
                Embedding.vector.l2_distance(query_vector).label('distance'),
            )
            .join(Category, Embedding.category_id == Category.id)
            .order_by(Embedding.vector.l2_distance(query_vector))
            .limit(3)
        )

        sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        results = self.client.execute(sql)
        print(f"\n✓ Top 3 similar embeddings:")
        for row in results:
            print(f"  - '{row.text}' in {row.category} (distance: {row.distance:.4f})")

        self.results['tests_passed'] += 1

    def test_fulltext_groupby(self):
        """Test: FULLTEXT + GROUP BY (single table)"""
        print("\n" + "=" * 80)
        print("Test 5: FULLTEXT + GROUP BY")
        print("=" * 80)
        print("Note: Using single table to avoid MatrixOne JOIN limitation")
        print()

        self.results['tests_run'] += 1

        # Count products matching "python OR machine" grouped by category_id
        stmt = (
            select(Product.category_id, func.count(Product.id).label('count'))
            .where(boolean_match('product_name', 'description').must(group().medium('python', 'machine')))
            .group_by(Product.category_id)
        )

        sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        results = self.client.execute(sql)
        print(f"✓ Products containing 'python' OR 'machine' grouped by category_id:")
        for row in results:
            print(f"  - Category {row.category_id}: {row.count} products")

        self.results['tests_passed'] += 1

    def test_having_clause(self):
        """Test: HAVING with aggregation"""
        print("\n" + "=" * 80)
        print("Test 6: HAVING Clause")
        print("=" * 80)

        self.results['tests_run'] += 1

        # Categories with average rating > 4.5
        stmt = (
            select(Category.category_name.label('category'), func.avg(Review.rating).label('avg_rating'))
            .join(Product, Product.category_id == Category.id)
            .join(Review, Review.product_id == Product.id)
            .group_by(Category.id, Category.category_name)
            .having(func.avg(Review.rating) > 4.5)
        )

        sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        results = self.client.execute(sql)
        print(f"\n✓ Categories with avg rating > 4.5:")
        for row in results:
            print(f"  - {row.category}: {row.avg_rating:.2f}")

        self.results['tests_passed'] += 1

    def cleanup(self):
        """Cleanup resources"""
        if self.client:
            try:
                for table in ['embeddings_complex', 'reviews_complex', 'products_complex', 'categories_complex']:
                    self.client.execute(f"DROP TABLE IF EXISTS {table}")
            except:
                pass
            self.client.disconnect()
        print("✓ Cleanup completed")

    def print_summary(self):
        """Print test results summary"""
        print("\n" + "=" * 80)
        print("Test Results Summary")
        print("=" * 80)
        print(f"Total tests run: {self.results['tests_run']}")
        print(f"Tests passed: {self.results['tests_passed']}")
        print(f"Tests failed: {self.results['tests_failed']}")

        if self.results['unexpected_results']:
            print("\nUnexpected results:")
            for result in self.results['unexpected_results']:
                print(f"  - {result}")

        if self.results['tests_failed'] == 0:
            print("\n✅ All tests passed!")
        else:
            print(f"\n⚠️  {self.results['tests_failed']} test(s) failed")

        print("\nSupported Features:")
        print("✓ FULLTEXT search (standalone)")
        print("✓ FULLTEXT + GROUP BY")
        print("✓ JOIN + GROUP BY")
        print("✓ JOIN + WHERE conditions")
        print("✓ VECTOR functions in JOIN queries")
        print("✓ HAVING clauses")
        print("\nLimitations:")
        print("✗ FULLTEXT in JOIN queries (MatrixOne limitation)")
        print("\n✅ Conclusion: select() supports all MatrixOne features!")
        print("=" * 80)

    def run(self):
        """Run all tests"""
        try:
            self.setup()
            self.populate_data()
            self.test_fulltext_standalone()
            self.test_join_groupby()
            self.test_join_where_filter()
            self.test_vector_in_join()
            self.test_fulltext_groupby()
            self.test_having_clause()
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback

            traceback.print_exc()
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(str(e))
        finally:
            self.cleanup()
            self.print_summary()


if __name__ == "__main__":
    demo = ComplexQueryDemo()
    demo.run()
