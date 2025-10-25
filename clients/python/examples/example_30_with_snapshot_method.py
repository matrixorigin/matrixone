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
Example 31: Using with_snapshot() Method

This example demonstrates the with_snapshot() method for cleaner, 
more SQLAlchemy-style query building with snapshots.

Features demonstrated:
- Basic select().with_snapshot() usage
- with_snapshot() + WHERE clauses
- with_snapshot() + FULLTEXT search
- with_snapshot() + Complex filtering
- with_snapshot() + Aggregation functions
- Comparing current vs snapshot data
- Equivalence of with_snapshot() and snapshot parameter
"""

import sys
import os
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import Column, Integer, String, Text, func
from sqlalchemy.orm import Session

from matrixone import Client, SnapshotLevel, compile_select_to_sql
from matrixone.config import get_connection_params
from matrixone.orm import declarative_base
from matrixone.sqlalchemy_ext import boolean_match, Vectorf32, FulltextIndex
from matrixone.sqlalchemy_ext.snapshot import select

Base = declarative_base()


class Article(Base):
    """Article model with fulltext and vector search support"""
    __tablename__ = 'articles_with_snapshot'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(200), nullable=False)
    content = Column(Text, nullable=False)
    category = Column(String(50))
    views = Column(Integer, default=0)
    embedding = Column(Vectorf32(384))
    
    __table_args__ = (
        FulltextIndex('ft_title_content', ['title', 'content']),
    )


class WithSnapshotExample:
    """Demonstrates with_snapshot() method for point-in-time queries"""
    
    def __init__(self):
        self.client = None
        self.session = None
        self.snapshot_name = None
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
        }
    
    def setup(self):
        """Setup database connection"""
        print("=" * 80)
        print("Example 31: with_snapshot() Method for Point-in-Time Queries")
        print("=" * 80)
        print()
        
        host, port, user, password, database = get_connection_params()
        
        self.client = Client(sql_log_mode='full')
        self.client.connect(host=host, port=port, user=user, password=password, database=database)
        
        # Get SQLAlchemy session
        self.session = Session(self.client.get_sqlalchemy_engine())
        
        print("✓ Connected to MatrixOne\n")
    
    def create_tables(self):
        """Create test tables"""
        print("Creating tables...")
        
        # Drop table if exists
        self.client.execute("DROP TABLE IF EXISTS articles_with_snapshot")
        
        # Create table
        self.client.create_table(Article)
        
        print("✓ Tables created\n")
    
    def insert_sample_data(self):
        """Insert sample data"""
        print("Inserting sample data...")
        
        articles = [
            Article(
                title='Python Tutorial',
                content='Learn Python basics',
                category='Programming',
                views=100,
                embedding=[0.1] * 384
            ),
            Article(
                title='Machine Learning',
                content='AI and ML concepts',
                category='AI',
                views=200,
                embedding=[0.2] * 384
            ),
            Article(
                title='Database Design',
                content='SQL best practices',
                category='Database',
                views=150,
                embedding=[0.3] * 384
            ),
        ]
        
        for article in articles:
            self.session.add(article)
        self.session.commit()
        
        print(f"✓ Inserted {len(articles)} articles\n")
    
    def create_snapshot(self):
        """Create snapshot for point-in-time queries"""
        print("Creating snapshot...")
        
        self.snapshot_name = f"snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        host, port, user, password, database = get_connection_params()
        self.client.snapshots.create(
            name=self.snapshot_name,
            level=SnapshotLevel.TABLE,
            database=database,
            table='articles_with_snapshot'
        )
        
        print(f"✓ Created snapshot: {self.snapshot_name}\n")
    
    def modify_data(self):
        """Modify data after snapshot creation"""
        print("Modifying data after snapshot...")
        
        # Add new article
        new_article = Article(
            title='Advanced Python',
            content='Python advanced topics',
            category='Programming',
            views=50,
            embedding=[0.4] * 384
        )
        self.session.add(new_article)
        self.session.commit()
        
        print("✓ Added new article (total now: 4, snapshot has: 3)\n")
    
    def test_basic_with_snapshot(self):
        """Test 1: Basic with_snapshot() usage"""
        print("=" * 80)
        print("Test 1: Basic with_snapshot() Usage")
        print("=" * 80)
        
        self.results['tests_run'] += 1
        
        try:
            # Query current data
            stmt_current = select(Article).order_by(Article.id)
            current_results = self.session.execute(stmt_current).scalars().all()
            print(f"Current state: {len(current_results)} articles")
            
            # Query snapshot data using with_snapshot()
            stmt_snapshot = (select(Article)
                            .order_by(Article.id)
                            .with_snapshot(self.snapshot_name))
            
            sql = compile_select_to_sql(stmt_snapshot)
            snapshot_results = self.client.execute(sql)
            print(f"Snapshot state: {len(snapshot_results.rows)} articles")
            
            # Verify
            assert len(current_results) == 4, f"Expected 4 current articles, got {len(current_results)}"
            assert len(snapshot_results.rows) == 3, f"Expected 3 snapshot articles, got {len(snapshot_results.rows)}"
            
            print("✓ Test passed: Snapshot correctly shows historical state")
            self.results['tests_passed'] += 1
            
        except Exception as e:
            print(f"✗ Test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"test_basic_with_snapshot: {e}")
    
    def test_with_snapshot_where(self):
        """Test 2: with_snapshot() + WHERE clause"""
        print("\n" + "=" * 80)
        print("Test 2: with_snapshot() + WHERE Clause")
        print("=" * 80)
        
        self.results['tests_run'] += 1
        
        try:
            stmt = (select(Article)
                    .where(Article.category == 'Programming')
                    .with_snapshot(self.snapshot_name))
            
            sql = compile_select_to_sql(stmt)
            results = self.client.execute(sql)
            
            print(f"Programming articles in snapshot: {len(results.rows)}")
            for row in results.rows:
                print(f"  - [{row[0]}] {row[1]}")
            
            assert len(results.rows) == 1, f"Expected 1 Programming article, got {len(results.rows)}"
            
            print("✓ Test passed: WHERE clause works with snapshot")
            self.results['tests_passed'] += 1
            
        except Exception as e:
            print(f"✗ Test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"test_with_snapshot_where: {e}")
    
    def test_with_snapshot_fulltext(self):
        """Test 3: with_snapshot() + FULLTEXT search"""
        print("\n" + "=" * 80)
        print("Test 3: with_snapshot() + FULLTEXT Search")
        print("=" * 80)
        
        self.results['tests_run'] += 1
        
        try:
            stmt = (select(Article)
                    .where(boolean_match("title", "content").must("python"))
                    .with_snapshot(self.snapshot_name)
                    .order_by(Article.id))
            
            sql = compile_select_to_sql(stmt)
            results = self.client.execute(sql)
            
            print(f"Found {len(results.rows)} Python-related articles:")
            for row in results.rows:
                print(f"  - [{row[0]}] {row[1]}")
            
            assert len(results.rows) >= 1, f"Expected at least 1 Python article, got {len(results.rows)}"
            
            print("✓ Test passed: FULLTEXT search works with snapshot")
            self.results['tests_passed'] += 1
            
        except Exception as e:
            print(f"✗ Test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"test_with_snapshot_fulltext: {e}")
    
    def test_with_snapshot_complex_filter(self):
        """Test 4: with_snapshot() + Complex filtering"""
        print("\n" + "=" * 80)
        print("Test 4: with_snapshot() + Complex Filtering")
        print("=" * 80)
        
        self.results['tests_run'] += 1
        
        try:
            stmt = (select(Article)
                    .where(Article.views >= 100)
                    .where(Article.category.in_(['Programming', 'AI']))
                    .with_snapshot(self.snapshot_name)
                    .order_by(Article.views.desc()))
            
            sql = compile_select_to_sql(stmt)
            results = self.client.execute(sql)
            
            print(f"Popular articles (views >= 100):")
            for row in results.rows:
                print(f"  - [{row[0]}] {row[1]} - {row[4]} views ({row[3]})")
            
            assert len(results.rows) == 2, f"Expected 2 articles, got {len(results.rows)}"
            
            print("✓ Test passed: Complex filtering works with snapshot")
            self.results['tests_passed'] += 1
            
        except Exception as e:
            print(f"✗ Test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"test_with_snapshot_complex_filter: {e}")
    
    def test_with_snapshot_aggregation(self):
        """Test 5: with_snapshot() + Aggregation"""
        print("\n" + "=" * 80)
        print("Test 5: with_snapshot() + Aggregation Functions")
        print("=" * 80)
        
        self.results['tests_run'] += 1
        
        try:
            stmt = (select(
                        Article.category,
                        func.count(Article.id).label('count'),
                        func.sum(Article.views).label('total_views')
                    )
                    .group_by(Article.category)
                    .with_snapshot(self.snapshot_name)
                    .order_by(Article.category))
            
            sql = compile_select_to_sql(stmt)
            results = self.client.execute(sql)
            
            print("Category statistics in snapshot:")
            for row in results.rows:
                print(f"  - {row[0]}: {row[1]} articles, {row[2]} total views")
            
            assert len(results.rows) == 3, f"Expected 3 categories, got {len(results.rows)}"
            
            print("✓ Test passed: Aggregation works with snapshot")
            self.results['tests_passed'] += 1
            
        except Exception as e:
            print(f"✗ Test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"test_with_snapshot_aggregation: {e}")
    
    def test_complex_query(self):
        """Test 6: Complex query with with_snapshot()"""
        print("\n" + "=" * 80)
        print("Test 6: Complex Query with with_snapshot()")
        print("=" * 80)
        
        self.results['tests_run'] += 1
        
        try:
            stmt = (select(
                        Article.category,
                        func.count(Article.id).label('article_count'),
                        func.avg(Article.views).label('avg_views')
                    )
                    .where(Article.views > 50)
                    .group_by(Article.category)
                    .having(func.count(Article.id) >= 1)
                    .with_snapshot(self.snapshot_name)
                    .order_by(func.avg(Article.views).desc()))
            
            sql = compile_select_to_sql(stmt)
            results = self.client.execute(sql)
            
            print("Popular categories (views > 50):")
            for row in results.rows:
                print(f"  - {row[0]}: {row[1]} articles, {row[2]:.1f} avg views")
            
            print("✓ Test passed: Complex query with HAVING works")
            self.results['tests_passed'] += 1
            
        except Exception as e:
            print(f"✗ Test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"test_complex_query: {e}")
    
    def test_method_equivalence(self):
        """Test 7: Compare with_snapshot() vs snapshot parameter"""
        print("\n" + "=" * 80)
        print("Test 7: Method Equivalence Test")
        print("=" * 80)
        print("Comparing with_snapshot() method vs snapshot parameter")
        
        self.results['tests_run'] += 1
        
        try:
            # Method 1: with_snapshot()
            stmt1 = (select(func.count(Article.id))
                    .where(Article.category == 'Programming')
                    .with_snapshot(self.snapshot_name))
            
            # Method 2: snapshot parameter
            stmt2 = (select(func.count(Article.id), snapshot=self.snapshot_name)
                    .where(Article.category == 'Programming'))
            
            sql1 = compile_select_to_sql(stmt1)
            sql2 = compile_select_to_sql(stmt2)
            
            result1 = self.client.execute(sql1).rows[0][0]
            result2 = self.client.execute(sql2).rows[0][0]
            
            print(f"  Method 1 (with_snapshot): {result1}")
            print(f"  Method 2 (snapshot param): {result2}")
            print(f"  Results match: {result1 == result2}")
            
            assert result1 == result2, f"Results differ: {result1} vs {result2}"
            
            print("✓ Test passed: Both methods are equivalent")
            self.results['tests_passed'] += 1
            
        except Exception as e:
            print(f"✗ Test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"test_method_equivalence: {e}")
    
    def cleanup(self):
        """Cleanup resources"""
        print("\n" + "=" * 80)
        print("Cleanup")
        print("=" * 80)
        
        try:
            if self.snapshot_name:
                self.client.execute(f"DROP SNAPSHOT IF EXISTS {self.snapshot_name}")
                print(f"✓ Dropped snapshot: {self.snapshot_name}")
            
            self.client.execute("DROP TABLE IF EXISTS articles_with_snapshot")
            print("✓ Dropped tables")
            
        except Exception as e:
            print(f"Warning: Cleanup error: {e}")
        
        finally:
            if self.session:
                self.session.close()
            if self.client:
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
        
        print("\nFeatures demonstrated:")
        print("✓ Basic with_snapshot() usage")
        print("✓ with_snapshot() + WHERE clauses")
        print("✓ with_snapshot() + FULLTEXT search")
        print("✓ with_snapshot() + Complex filtering")
        print("✓ with_snapshot() + Aggregation functions")
        print("✓ Complex queries with HAVING")
        print("✓ Equivalence of with_snapshot() and snapshot parameter")
        print("\n✅ Conclusion: with_snapshot() provides clean, SQLAlchemy-style snapshot queries!")
        print("=" * 80)
    
    def run(self):
        """Run all tests"""
        try:
            self.setup()
            self.create_tables()
            self.insert_sample_data()
            self.create_snapshot()
            self.modify_data()
            
            # Run all tests
            self.test_basic_with_snapshot()
            self.test_with_snapshot_where()
            self.test_with_snapshot_fulltext()
            self.test_with_snapshot_complex_filter()
            self.test_with_snapshot_aggregation()
            self.test_complex_query()
            self.test_method_equivalence()
            
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(str(e))
        finally:
            self.cleanup()
            self.print_summary()


if __name__ == '__main__':
    example = WithSnapshotExample()
    example.run()
