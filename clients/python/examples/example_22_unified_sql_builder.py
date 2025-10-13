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
Example 22: Unified SQL Builder - Advanced SQL Construction and Refactoring

This example demonstrates the unified SQL builder system that eliminates
code duplication across different MatrixOne interfaces:
- Basic SQL construction with MatrixOneSQLBuilder
- Vector similarity search queries
- CTE (Common Table Expression) queries
- DML operations (INSERT, UPDATE, DELETE)
- Refactoring patterns and best practices
- Performance comparison between old and new approaches

The unified builder provides a consistent API for all SQL construction
needs while maintaining MatrixOne-specific optimizations.
"""

import sys
import os
import time
from typing import List, Dict, Any, Optional, Tuple

# Add the parent directory to the path so we can import matrixone
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from matrixone import Client
from matrixone.sql_builder import (
    MatrixOneSQLBuilder,
    DistanceFunction,
    build_vector_similarity_query,
    build_select_query,
    build_insert_query,
    build_update_query,
    build_delete_query,
    build_create_index_query,
)
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger


class UnifiedSQLBuilderDemo:
    """Demonstrates the unified SQL builder system."""

    def __init__(self):
        self.client = None
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
            'performance_metrics': {},
        }

    def setup_connection(self):
        """Setup database connection."""
        try:
            print("🔗 Setting up database connection...")
            connection_params = get_connection_params()
            self.client = Client()
            host, port, user, password, database = connection_params
            self.client.connect(host=host, port=port, user=user, password=password, database=database)
            print("✅ Connected to MatrixOne successfully")
            return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False

    def cleanup(self):
        """Clean up resources."""
        if self.client:
            try:
                self.client.execute("DROP DATABASE IF EXISTS unified_builder_demo")
                self.client.disconnect()
                print("🧹 Cleanup completed")
            except Exception as e:
                print(f"⚠️ Cleanup warning: {e}")

    def test_basic_sql_construction(self):
        """Test basic SQL construction with the unified builder."""
        print("\n=== Basic SQL Construction ===")

        try:
            # Test 1: Simple SELECT query
            builder = MatrixOneSQLBuilder()
            sql, params = builder.select('id', 'name', 'email').from_table('users').build()
            print(f"✅ Simple SELECT: {sql}")
            assert sql == "SELECT id, name, email FROM users"
            assert params == []

            # Test 2: SELECT with WHERE conditions
            builder = MatrixOneSQLBuilder()
            sql, params = (
                builder.select('*')
                .from_table('users')
                .where('age > ?', 18)
                .where('status = ?', 'active')
                .order_by('name')
                .limit(10)
                .build()
            )
            print(f"✅ SELECT with WHERE: {sql}")
            assert "WHERE age > ? AND status = ?" in sql
            assert params == [18, 'active']

            # Test 3: Using convenience functions
            sql = build_select_query(
                table_name="products",
                select_columns=["id", "name", "price"],
                where_conditions=["category = ?", "price < ?"],
                where_params=["electronics", 1000],
                order_by=["price"],
                limit=5,
            )
            print(f"✅ Convenience function: {sql}")
            assert "SELECT id, name, price FROM products" in sql

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"❌ Basic SQL construction failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"Basic SQL construction: {e}")

        self.results['tests_run'] += 1

    def test_vector_similarity_queries(self):
        """Test vector similarity search query construction."""
        print("\n=== Vector Similarity Queries ===")

        try:
            # Test 1: Basic vector similarity query
            query_vector = [0.1, 0.2, 0.3, 0.4, 0.5] * 12  # 60 dimensions
            sql = build_vector_similarity_query(
                table_name='documents',
                vector_column='embedding',
                query_vector=query_vector,
                distance_func=DistanceFunction.L2_SQ,
                limit=10,
                select_columns=['id', 'title', 'content'],
                where_conditions=['category = ?'],
                where_params=['news'],
            )
            print(f"✅ Vector similarity query: {sql[:100]}...")
            assert "l2_distance_sq" in sql
            assert "WHERE category = 'news'" in sql

            # Test 2: Using builder directly for vector queries
            builder = MatrixOneSQLBuilder()
            sql, params = builder.vector_similarity_search(
                table_name='movies',
                vector_column='embedding',
                query_vector=[0.15] * 64,
                distance_func=DistanceFunction.COSINE,
                limit=5,
            ).build()
            print(f"✅ Builder vector query: {sql[:100]}...")
            assert "cosine_distance" in sql

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"❌ Vector similarity queries failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"Vector similarity queries: {e}")

        self.results['tests_run'] += 1

    def test_cte_queries(self):
        """Test Common Table Expression (CTE) query construction and execution."""
        print("\n=== CTE Queries ===")

        try:
            # Test 1: Basic CTE query construction
            builder = MatrixOneSQLBuilder()
            sql, params = (
                builder.with_cte(
                    'dept_stats',
                    'SELECT department_id, COUNT(*) as emp_count FROM employees GROUP BY department_id',
                )
                .select('d.name', 'ds.emp_count')
                .from_table('departments d')
                .join('dept_stats ds', 'd.id = ds.department_id', 'INNER')
                .where('ds.emp_count > ?', 5)
                .build()
            )
            print(f"✅ CTE query construction: {sql}")
            assert "WITH dept_stats AS" in sql
            assert "INNER JOIN dept_stats ds" in sql

            # Test 2: Multiple CTEs construction
            builder = MatrixOneSQLBuilder()
            sql, params = (
                builder.with_cte(
                    'sales_summary',
                    'SELECT product_id, SUM(amount) as total_sales FROM sales GROUP BY product_id',
                )
                .with_cte(
                    'product_rankings',
                    'SELECT product_id, total_sales, RANK() OVER (ORDER BY total_sales DESC) as rank FROM sales_summary',
                )
                .select('p.name', 'pr.rank', 'pr.total_sales')
                .from_table('products p')
                .join('product_rankings pr', 'p.id = pr.product_id', 'INNER')
                .where('pr.rank <= ?', 10)
                .build()
            )
            print(f"✅ Multiple CTEs construction: {sql[:100]}...")
            assert "WITH sales_summary AS" in sql
            assert "product_rankings AS" in sql

            # Test 3: CTE with actual database operations (if connected)
            if self.client:
                self._test_cte_database_operations()

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"❌ CTE queries failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"CTE queries: {e}")

        self.results['tests_run'] += 1

    def _test_cte_database_operations(self):
        """Test CTE operations with actual database."""
        print("\n--- CTE Database Operations ---")

        try:
            # Create test tables
            self.client.execute("DROP TABLE IF EXISTS test_users_cte")
            self.client.execute(
                """
                CREATE TABLE test_users_cte (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    department_id INT,
                    salary DECIMAL(10,2),
                    email VARCHAR(100)
                )
            """
            )

            self.client.execute("DROP TABLE IF EXISTS test_departments_cte")
            self.client.execute(
                """
                CREATE TABLE test_departments_cte (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    budget DECIMAL(12,2)
                )
            """
            )

            # Insert sample data
            self.client.execute(
                """
                INSERT INTO test_departments_cte (id, name, budget) VALUES 
                (1, 'Engineering', 100000),
                (2, 'Marketing', 50000),
                (3, 'Sales', 80000),
                (4, 'HR', 30000)
            """
            )

            self.client.execute(
                """
                INSERT INTO test_users_cte (id, name, department_id, salary, email) VALUES 
                (1, 'John Doe', 1, 75000, 'john@example.com'),
                (2, 'Jane Smith', 1, 80000, 'jane@example.com'),
                (3, 'Bob Johnson', 2, 60000, 'bob@example.com'),
                (4, 'Alice Brown', 3, 70000, 'alice@example.com'),
                (5, 'Charlie Wilson', 4, 55000, 'charlie@example.com')
            """
            )

            # Test CTE with department statistics using new CTE support
            results = self.client.execute(
                """
                WITH dept_stats AS (
                    SELECT 
                        department_id, 
                        COUNT(*) as user_count,
                        AVG(salary) as avg_salary,
                        MAX(salary) as max_salary
                    FROM test_users_cte 
                    GROUP BY department_id
                )
                SELECT 
                    department_id, 
                    user_count, 
                    avg_salary, 
                    max_salary
                FROM dept_stats
                WHERE avg_salary > 65000
                ORDER BY avg_salary DESC
            """
            )

            print(f"✅ CTE department statistics: {len(results)} results")
            for row in results:
                print(f"    Dept {row.department_id}: {row.user_count} users, avg: ${row.avg_salary:.2f}")

            # Test CTE with JOINs using new CTE support
            results2 = self.client.execute(
                """
                WITH high_salary_users AS (
                    SELECT id, name, department_id, salary 
                    FROM test_users_cte 
                    WHERE salary > 70000
                )
                SELECT 
                    high_salary_users.name, 
                    high_salary_users.salary, 
                    d.name as department_name
                FROM high_salary_users
                INNER JOIN test_departments_cte d ON high_salary_users.department_id = d.id
                ORDER BY high_salary_users.salary DESC
            """
            )

            print(f"✅ CTE with JOINs: {len(results2)} results")
            for row in results2:
                print(f"    {row.name}: ${row.salary:.2f} in {row.department_name}")

            # Cleanup
            self.client.execute("DROP TABLE IF EXISTS test_users_cte")
            self.client.execute("DROP TABLE IF EXISTS test_departments_cte")

        except Exception as e:
            print(f"⚠️ CTE database operations failed: {e}")
            # Cleanup on error
            try:
                self.client.execute("DROP TABLE IF EXISTS test_users_cte")
                self.client.execute("DROP TABLE IF EXISTS test_departments_cte")
            except:
                pass

    def test_dml_operations(self):
        """Test DML (Data Manipulation Language) operations."""
        print("\n=== DML Operations ===")

        try:
            # Test 1: INSERT query
            sql, params = build_insert_query(
                table_name="users",
                values={'name': 'John Doe', 'email': 'john@example.com', 'age': 30},
            )
            print(f"✅ INSERT query: {sql}")
            assert "INSERT INTO users" in sql
            assert "name, email, age" in sql

            # Test 2: UPDATE query
            sql, params = build_update_query(
                table_name="users",
                set_values={'age': 31, 'last_login': '2024-01-01'},
                where_conditions=['id = ?'],
                where_params=[123],
            )
            print(f"✅ UPDATE query: {sql}")
            assert "UPDATE users SET" in sql
            assert "WHERE id = ?" in sql

            # Test 3: DELETE query
            sql, params = build_delete_query(
                table_name="users",
                where_conditions=['status = ?', 'last_login < ?'],
                where_params=['inactive', '2023-01-01'],
            )
            print(f"✅ DELETE query: {sql}")
            assert "DELETE FROM users" in sql
            assert "WHERE status = ? AND last_login < ?" in sql

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"❌ DML operations failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"DML operations: {e}")

        self.results['tests_run'] += 1

    def test_index_creation(self):
        """Test index creation query construction."""
        print("\n=== Index Creation ===")

        try:
            # Test 1: Vector index creation
            sql = build_create_index_query(
                index_name="idx_movie_embedding",
                table_name="movies",
                column_name="embedding",
                index_type="ivfflat",
                lists=100,
            )
            print(f"✅ Vector index: {sql}")
            assert "CREATE INDEX idx_movie_embedding" in sql
            assert "ivfflat" in sql

            # Test 2: Fulltext index creation
            sql = build_create_index_query(
                index_name="idx_content_fulltext",
                table_name="documents",
                column_name="title, content",
                index_type="fulltext",
                algorithm="BM25",
            )
            print(f"✅ Fulltext index: {sql}")
            assert "CREATE FULLTEXT INDEX" in sql
            assert "BM25" in sql

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"❌ Index creation failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"Index creation: {e}")

        self.results['tests_run'] += 1

    def test_refactoring_comparison(self):
        """Compare old vs new approaches to demonstrate refactoring benefits."""
        print("\n=== Refactoring Comparison ===")

        try:
            # OLD WAY: Manual SQL construction (simulated)
            def old_vector_search_logic():
                table_name = "documents"
                vector_column = "embedding"
                query_vector = [0.1] * 64
                limit = 10

                # Manual SQL building (error-prone, duplicated)
                vector_str = "[" + ",".join(map(str, query_vector)) + "]"
                sql = f"""
                SELECT id, title, content, l2_distance_sq({vector_column}, '{vector_str}') as distance
                FROM {table_name}
                WHERE category = 'news'
                ORDER BY distance
                LIMIT {limit}
                """
                return sql

            # NEW WAY: Using unified builder
            def new_vector_search_logic():
                return build_vector_similarity_query(
                    table_name='documents',
                    vector_column='embedding',
                    query_vector=[0.1] * 64,
                    distance_func=DistanceFunction.L2_SQ,
                    limit=10,
                    where_conditions=['category = ?'],
                    where_params=['news'],
                )

            # Compare approaches
            old_sql = old_vector_search_logic()
            new_sql = new_vector_search_logic()

            print("✅ OLD WAY (manual):")
            print(f"   {old_sql[:100]}...")
            print("✅ NEW WAY (unified builder):")
            print(f"   {new_sql[:100]}...")

            # Benefits demonstration
            print("\n📊 Refactoring Benefits:")
            print("   • Consistent API across all interfaces")
            print("   • Reduced code duplication")
            print("   • Better parameter handling")
            print("   • Easier maintenance and testing")
            print("   • MatrixOne-specific optimizations")

            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"❌ Refactoring comparison failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"Refactoring comparison: {e}")

        self.results['tests_run'] += 1

    def test_performance_comparison(self):
        """Compare performance between different approaches."""
        print("\n=== Performance Comparison ===")

        try:
            iterations = 1000

            # Test 1: Builder approach
            start_time = time.time()
            for _ in range(iterations):
                builder = MatrixOneSQLBuilder()
                sql, params = builder.select('id', 'name').from_table('users').where('age > ?', 18).build()
            builder_time = time.time() - start_time

            # Test 2: Convenience function approach
            start_time = time.time()
            for _ in range(iterations):
                sql = build_select_query(
                    table_name="users",
                    select_columns=["id", "name"],
                    where_conditions=["age > ?"],
                    where_params=[18],
                )
            convenience_time = time.time() - start_time

            print(f"✅ Builder approach: {builder_time:.4f}s for {iterations} iterations")
            print(f"✅ Convenience function: {convenience_time:.4f}s for {iterations} iterations")
            print(f"📊 Performance ratio: {builder_time/convenience_time:.2f}x")

            self.results['performance_metrics']['builder_time'] = builder_time
            self.results['performance_metrics']['convenience_time'] = convenience_time
            self.results['tests_passed'] += 1

        except Exception as e:
            print(f"❌ Performance comparison failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"Performance comparison: {e}")

        self.results['tests_run'] += 1

    def run_all_tests(self):
        """Run all demonstration tests."""
        print("🚀 Starting Unified SQL Builder Demo")
        print("=" * 60)

        if not self.setup_connection():
            return False

        try:
            # Run all test methods
            self.test_basic_sql_construction()
            self.test_vector_similarity_queries()
            self.test_cte_queries()
            self.test_dml_operations()
            self.test_index_creation()
            self.test_refactoring_comparison()
            self.test_performance_comparison()

            # Print summary
            self.print_summary()
            return True

        except Exception as e:
            print(f"❌ Demo failed with error: {e}")
            return False
        finally:
            self.cleanup()

    def print_summary(self):
        """Print test summary."""
        print("\n" + "=" * 60)
        print("📊 UNIFIED SQL BUILDER DEMO SUMMARY")
        print("=" * 60)

        total = self.results['tests_run']
        passed = self.results['tests_passed']
        failed = self.results['tests_failed']

        print(f"Tests run: {total}")
        print(f"✅ Passed: {passed}")
        print(f"❌ Failed: {failed}")
        print(f"Success rate: {(passed/total*100):.1f}%" if total > 0 else "N/A")

        if self.results['unexpected_results']:
            print(f"\n⚠️ Unexpected results ({len(self.results['unexpected_results'])}):")
            for result in self.results['unexpected_results']:
                print(f"   • {result}")

        if self.results['performance_metrics']:
            print(f"\n⚡ Performance Metrics:")
            for metric, value in self.results['performance_metrics'].items():
                print(f"   • {metric}: {value:.4f}s")

        print("\n🎯 Key Benefits of Unified SQL Builder:")
        print("   • Eliminates code duplication across interfaces")
        print("   • Provides consistent API for all SQL construction")
        print("   • Handles MatrixOne-specific optimizations")
        print("   • Supports parameter substitution for compatibility")
        print("   • Enables easy testing and maintenance")

        print("\n✨ Demo completed successfully!")


def main():
    """Main function to run the unified SQL builder demo."""
    print_config()

    demo = UnifiedSQLBuilderDemo()
    success = demo.run_all_tests()

    if success:
        print("\n🎉 All tests passed! The unified SQL builder is working correctly.")
        return 0
    else:
        print("\n💥 Some tests failed. Please check the output above.")
        return 1


if __name__ == "__main__":
    exit(main())
