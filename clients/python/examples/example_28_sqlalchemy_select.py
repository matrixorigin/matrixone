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
Example 28: SQLAlchemy 2.0 Select with MatrixOne Features

This example demonstrates how to use SQLAlchemy's native select() statement
with MatrixOne-specific features like fulltext search and vector search.

Key Points:
- Use standard SQLAlchemy select() - no custom implementation needed
- MatrixOne features work seamlessly as SQLAlchemy expressions
- Can export select() statements directly to files/stages
- Compatible with both SQLAlchemy 1.4+ and 2.0+

Examples covered:
1. Basic select() usage
2. Fulltext search with select()
3. Vector search with select()
4. Complex queries with joins and aggregations
5. Exporting select() results
"""

import sys
import os
import random

# Add the parent directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Try to import numpy, use random as fallback
try:
    import numpy as np

    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False
    print("⚠️  Warning: numpy not installed, using random for vector generation")
    print("   Install numpy for better performance: pip install numpy")
    print()

from sqlalchemy import Column, Integer, String, Text, func, and_, or_, select, desc
from sqlalchemy.orm import declarative_base, Session

from matrixone import Client, select as mo_select, compile_select_to_sql
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger
from matrixone.sqlalchemy_ext import (
    boolean_match,
    create_vector_column,
    FulltextIndex,
    FulltextAlgorithmType,
)

# Create base for models
Base = declarative_base()


class Article(Base):
    """Article model with fulltext search support"""

    __tablename__ = 'articles_select_demo'

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(200), nullable=False)
    content = Column(Text, nullable=False)
    category = Column(String(50))
    author = Column(String(100))

    __table_args__ = (FulltextIndex('ftidx_article_search', ['title', 'content'], algorithm=FulltextAlgorithmType.BM25),)


class Document(Base):
    """Document model with vector search support"""

    __tablename__ = 'documents_select_demo'

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(200), nullable=False)
    content = Column(Text)
    embedding = create_vector_column(128)  # 128-dimensional vector


class SQLAlchemySelectDemo:
    """Demonstrates SQLAlchemy select() with MatrixOne features"""

    def __init__(self):
        self.client = None
        self.engine = None
        self.session = None
        self.Base = Base
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
        }

    def setup(self):
        """Setup database connection"""
        print("=" * 80)
        print("SQLAlchemy Select Demo - MatrixOne Features")
        print("=" * 80)
        print()

        # Get connection parameters
        host, port, user, password, database = get_connection_params()

        # Create logger
        logger = create_default_logger()

        # Create client
        self.client = Client(logger=logger, sql_log_mode="full")
        self.client.connect(host=host, port=port, user=user, password=password, database=database)

        # Get SQLAlchemy engine
        self.engine = self.client.get_sqlalchemy_engine()

        # Create session
        self.session = Session(bind=self.engine)

        print("✓ Connected to MatrixOne\n")

    def test_basic_select(self):
        """Test 1: Basic select() usage"""
        print("\n" + "=" * 80)
        print("Test 1: Basic SQLAlchemy select()")
        print("=" * 80)

        self.results['tests_run'] += 1

        # Drop tables if exist
        try:
            self.client.execute("DROP TABLE IF EXISTS articles_select_demo")
            self.client.execute("DROP TABLE IF EXISTS documents_select_demo")
        except:
            pass

        # Create tables using client.create_table() to support FulltextIndex
        self.client.create_table(Article)
        self.client.create_table(Document)
        print("✓ Created tables with fulltext index")

        # Insert test data
        articles = [
            Article(title="Python Tutorial", content="Learn Python programming", category="Programming", author="Alice"),
            Article(
                title="Web Development", content="Build web applications with Python", category="Programming", author="Bob"
            ),
            Article(title="Machine Learning", content="Introduction to ML with Python", category="AI", author="Charlie"),
            Article(title="Data Science", content="Data analysis and visualization", category="AI", author="Alice"),
        ]
        self.session.add_all(articles)
        self.session.commit()

        # Example 1: Simple select all
        stmt = select(Article)
        results = self.session.execute(stmt).scalars().all()
        print(f"\n1. Select all articles: {len(results)} rows")

        # Example 2: Select with where clause
        stmt = select(Article).where(Article.category == "Programming")
        results = self.session.execute(stmt).scalars().all()
        print(f"2. Select Programming articles: {len(results)} rows")
        for article in results:
            print(f"   - {article.title} by {article.author}")

        # Example 3: Select with multiple conditions
        stmt = select(Article).where(and_(Article.category == "AI", Article.author == "Alice"))
        results = self.session.execute(stmt).scalars().all()
        print(f"3. Select AI articles by Alice: {len(results)} rows")

        # Example 4: Select specific columns
        stmt = select(Article.title, Article.author).where(Article.category == "Programming")
        results = self.session.execute(stmt).all()
        print(f"4. Select title and author: {len(results)} rows")
        for title, author in results:
            print(f"   - {title} by {author}")

        # Example 5: Select with aggregation
        stmt = select(Article.category, func.count(Article.id).label('count')).group_by(Article.category)
        results = self.session.execute(stmt).all()
        print(f"5. Count by category:")
        for category, count in results:
            print(f"   - {category}: {count}")

        # Example 6: Select with ordering and limit
        stmt = select(Article).order_by(desc(Article.title)).limit(2)
        results = self.session.execute(stmt).scalars().all()
        print(f"6. Top 2 articles by title (desc): {len(results)} rows")
        for article in results:
            print(f"   - {article.title}")

        print("\n✓ Basic select() tests completed")
        self.results['tests_passed'] += 1

    def test_fulltext_search_select(self):
        """Test 2: Fulltext search with select()"""
        print("\n" + "=" * 80)
        print("Test 2: Fulltext Search with select()")
        print("=" * 80)

        self.results['tests_run'] += 1

        # Example 1: Basic fulltext search
        print("\n1. Basic fulltext search for 'Python':")
        stmt = select(Article).where(boolean_match("title", "content").must("Python"))
        results = self.session.execute(stmt).scalars().all()
        print(f"   Found {len(results)} articles")
        for article in results:
            print(f"   - {article.title}")

        # Example 2: Fulltext search with multiple terms
        print("\n2. Fulltext search for 'Python' and 'web':")
        stmt = select(Article).where(boolean_match("title", "content").must("Python", "web"))
        results = self.session.execute(stmt).scalars().all()
        print(f"   Found {len(results)} articles")
        for article in results:
            print(f"   - {article.title}")

        # Example 3: Fulltext search with encourage/discourage
        print("\n3. Fulltext search with encourage 'tutorial':")
        stmt = select(Article).where(boolean_match("title", "content").must("Python").encourage("tutorial"))
        results = self.session.execute(stmt).scalars().all()
        print(f"   Found {len(results)} articles")
        for article in results:
            print(f"   - {article.title}")

        # Example 4: Combine fulltext with other conditions
        print("\n4. Fulltext search + category filter:")
        stmt = select(Article).where(
            and_(boolean_match("title", "content").must("Python"), Article.category == "Programming")
        )
        results = self.session.execute(stmt).scalars().all()
        print(f"   Found {len(results)} articles")
        for article in results:
            print(f"   - {article.title} ({article.category})")

        # Example 5: Fulltext search with aggregation
        print("\n5. Count articles by category with 'Python':")
        stmt = (
            select(Article.category, func.count(Article.id).label('count'))
            .where(boolean_match("title", "content").must("Python"))
            .group_by(Article.category)
        )
        results = self.session.execute(stmt).all()
        for category, count in results:
            print(f"   - {category}: {count}")

        print("\n✓ Fulltext search with select() completed")
        self.results['tests_passed'] += 1

    def test_vector_search_select(self):
        """Test 3: Vector search with select()"""
        print("\n" + "=" * 80)
        print("Test 3: Vector Search with select()")
        print("=" * 80)

        self.results['tests_run'] += 1

        # Generate random vectors
        def generate_vector(dim=128):
            if HAS_NUMPY:
                return np.random.rand(dim).tolist()
            else:
                return [random.random() for _ in range(dim)]

        # Insert test documents with vectors
        documents = []
        for i in range(10):
            doc = Document(title=f"Document {i+1}", content=f"Content for document {i+1}", embedding=generate_vector(128))
            documents.append(doc)
        self.session.add_all(documents)
        self.session.commit()

        # Query vector
        query_vector = generate_vector(128)

        # Example 1: L2 distance search
        print("\n1. L2 distance vector search:")
        stmt = (
            select(Document, Document.embedding.l2_distance(query_vector).label('distance'))
            .order_by(Document.embedding.l2_distance(query_vector))
            .limit(3)
        )
        results = self.session.execute(stmt).all()
        print(f"   Top 3 similar documents:")
        for doc, distance in results:
            print(f"   - {doc.title}: distance = {distance:.4f}")

        # Example 2: Cosine distance search
        print("\n2. Cosine distance vector search:")
        stmt = (
            select(Document, Document.embedding.cosine_distance(query_vector).label('distance'))
            .order_by(Document.embedding.cosine_distance(query_vector))
            .limit(3)
        )
        results = self.session.execute(stmt).all()
        print(f"   Top 3 similar documents:")
        for doc, distance in results:
            print(f"   - {doc.title}: distance = {distance:.4f}")

        # Example 3: Inner product search
        print("\n3. Inner product vector search:")
        stmt = (
            select(Document, Document.embedding.inner_product(query_vector).label('score'))
            .order_by(desc(Document.embedding.inner_product(query_vector)))
            .limit(3)
        )
        results = self.session.execute(stmt).all()
        print(f"   Top 3 similar documents:")
        for doc, score in results:
            print(f"   - {doc.title}: score = {score:.4f}")

        # Example 4: Vector search with filter
        print("\n4. Vector search with title filter:")
        stmt = (
            select(Document, Document.embedding.l2_distance(query_vector).label('distance'))
            .where(Document.title.like('Document %'))
            .order_by(Document.embedding.l2_distance(query_vector))
            .limit(3)
        )
        results = self.session.execute(stmt).all()
        print(f"   Top 3 filtered documents:")
        for doc, distance in results:
            print(f"   - {doc.title}: distance = {distance:.4f}")

        print("\n✓ Vector search with select() completed")
        self.results['tests_passed'] += 1

    def test_complex_queries(self):
        """Test 4: Complex queries with select()"""
        print("\n" + "=" * 80)
        print("Test 4: Complex Queries with select()")
        print("=" * 80)

        self.results['tests_run'] += 1

        # Example 1: Subquery
        print("\n1. Subquery example:")
        subq = select(Article.category).where(Article.author == "Alice").distinct().subquery()
        stmt = select(Article).where(Article.category.in_(select(subq)))
        results = self.session.execute(stmt).scalars().all()
        print(f"   Articles in categories written by Alice: {len(results)} rows")

        # Example 2: Multiple aggregations
        print("\n2. Multiple aggregations:")
        stmt = select(
            Article.category,
            func.count(Article.id).label('total'),
            func.count(func.distinct(Article.author)).label('authors'),
        ).group_by(Article.category)
        results = self.session.execute(stmt).all()
        print(f"   Statistics by category:")
        for category, total, authors in results:
            print(f"   - {category}: {total} articles by {authors} authors")

        # Example 3: Having clause
        print("\n3. Having clause:")
        stmt = (
            select(Article.author, func.count(Article.id).label('count'))
            .group_by(Article.author)
            .having(func.count(Article.id) >= 2)
        )
        results = self.session.execute(stmt).all()
        print(f"   Authors with 2+ articles:")
        for author, count in results:
            print(f"   - {author}: {count} articles")

        print("\n✓ Complex queries completed")
        self.results['tests_passed'] += 1

    def test_export_select(self):
        """Test 5: Export select() results"""
        print("\n" + "=" * 80)
        print("Test 5: Export select() Results")
        print("=" * 80)

        self.results['tests_run'] += 1

        # Example 1: Export basic select to SQL string
        print("\n1. Compile select() to SQL:")
        stmt = select(Article).where(Article.category == "Programming")
        sql = compile_select_to_sql(stmt, self.engine)
        print(f"   SQL: {sql}")

        # Example 2: Export with fulltext search
        print("\n2. Compile fulltext select() to SQL:")
        stmt = select(Article).where(boolean_match("title", "content").must("Python"))
        sql = compile_select_to_sql(stmt, self.engine)
        print(f"   SQL: {sql[:100]}...")

        # Note: Actual file/stage export would be:
        # client.export.to_stage(query=stmt, stage_name="my_stage", filename="export.csv")
        print("\n   To export to stage:")
        print("   client.export.to_stage(")
        print("       query=stmt,")
        print("       stage_name='my_stage',")
        print("       filename='articles.csv',")
        print("       header=True")
        print("   )")

        print("\n✓ Export tests completed")
        self.results['tests_passed'] += 1

    def cleanup(self):
        """Cleanup resources"""
        if self.session:
            self.session.close()
        if self.client:
            try:
                self.client.execute("DROP TABLE IF EXISTS articles_select_demo")
                self.client.execute("DROP TABLE IF EXISTS documents_select_demo")
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
        print("=" * 80)

    def run(self):
        """Run all tests"""
        try:
            self.setup()
            self.test_basic_select()
            self.test_fulltext_search_select()
            self.test_vector_search_select()
            self.test_complex_queries()
            self.test_export_select()
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback

            traceback.print_exc()
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(str(e))
        finally:
            self.cleanup()
            self.print_summary()


def main():
    """Main entry point"""
    print("\n" + "=" * 80)
    print("SQLAlchemy 2.0 Select with MatrixOne Features")
    print("=" * 80)
    print()
    print("This example demonstrates:")
    print("1. Using standard SQLAlchemy select() - not a custom implementation")
    print("2. MatrixOne fulltext search works seamlessly with select()")
    print("3. MatrixOne vector search works seamlessly with select()")
    print("4. Export select() statements to files/stages")
    print()
    print("Key benefits:")
    print("- Standard SQLAlchemy API - no learning curve")
    print("- MatrixOne-specific features fully integrated")
    print("- Can replace client.query() gradually")
    print("- Export select() objects directly")
    print("=" * 80)

    demo = SQLAlchemySelectDemo()
    demo.run()

    print("\n" + "=" * 80)
    print("Usage Examples")
    print("=" * 80)
    print()
    print("# Basic select:")
    print("from sqlalchemy import select")
    print("stmt = select(Article).where(Article.category == 'AI')")
    print("results = session.execute(stmt).scalars().all()")
    print()
    print("# Fulltext search:")
    print("from matrixone.sqlalchemy_ext import boolean_match")
    print("stmt = select(Article).where(")
    print("    boolean_match('title', 'content').must('python')")
    print(")")
    print()
    print("# Vector search:")
    print("query_vector = [0.1, 0.2, ...]")
    print("stmt = select(Document, Document.embedding.l2_distance(query_vector).label('dist'))")
    print("      .order_by(Document.embedding.l2_distance(query_vector))")
    print()
    print("# Export:")
    print("client.export.to_stage(query=stmt, stage_name='s3', filename='export.csv')")
    print("=" * 80)


if __name__ == "__main__":
    main()
