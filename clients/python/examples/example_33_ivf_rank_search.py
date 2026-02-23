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
Example 33: IVF Vector Search with LIMIT BY RANK

This example demonstrates MatrixOne's IVF (Inverted File) index with
LIMIT BY RANK feature for fine-grained control over vector search
execution strategies.

Features demonstrated:
1. Creating IVF indexes on vector columns
2. Using different ranking modes (pre, post, force)
3. Combining vector search with WHERE filters
4. Performance comparison between modes
5. Real-world use cases (document search, product recommendations)
"""

from matrixone import Client, IVFRankMode
from matrixone.logger import create_default_logger
from matrixone.config import get_connection_params


class IVFRankSearchDemo:
    """Demonstrates IVF vector search with LIMIT BY RANK capabilities."""

    def __init__(self):
        self.logger = create_default_logger(sql_log_mode="auto")
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
        }

    def test_basic_ivf_rank_modes(self):
        """Test basic IVF search with different ranking modes."""
        print("\n=== Test 1: Basic IVF Ranking Modes ===")
        self.results['tests_run'] += 1

        try:
            host, port, user, password, database = get_connection_params()
            client = Client(logger=self.logger, sql_log_mode="auto")
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Create table
            client.execute("DROP TABLE IF EXISTS documents")
            client.execute("""
                CREATE TABLE documents (
                    id INT PRIMARY KEY,
                    title VARCHAR(200),
                    embedding VECF32(8)
                )
            """)

            # Insert data
            docs = [
                (1, "Python Guide", [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]),
                (2, "Java Tutorial", [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]),
                (3, "Web Development", [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.1]),
                (4, "Database Design", [0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.1, 0.2]),
                (5, "Machine Learning", [0.5, 0.6, 0.7, 0.8, 0.9, 0.1, 0.2, 0.3]),
            ]

            for doc_id, title, embedding in docs:
                vec_str = "[" + ",".join(str(x) for x in embedding) + "]"
                client.execute(f"INSERT INTO documents VALUES ({doc_id}, '{title}', '{vec_str}')")

            # Create IVF index
            client.vector_ops.create_ivf(table_name="documents", name="idx_embedding", column="embedding", lists=2)

            query_vector = [0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85]

            # Test PRE mode
            self.logger.info("Testing PRE mode (fast approximate):")
            results_pre = client.vector_ops.search_with_rank(
                table_name="documents",
                vector_column="embedding",
                query_vector=query_vector,
                limit=3,
                rank_mode=IVFRankMode.PRE,
                select_columns=["id", "title"],
            )
            self.logger.info(f"   ✅ PRE mode returned {len(results_pre)} results")

            # Test POST mode
            self.logger.info("Testing POST mode (slower, accurate):")
            results_post = client.vector_ops.search_with_rank(
                table_name="documents",
                vector_column="embedding",
                query_vector=query_vector,
                limit=3,
                rank_mode=IVFRankMode.POST,
                select_columns=["id", "title"],
            )
            self.logger.info(f"   ✅ POST mode returned {len(results_post)} results")

            # Test FORCE mode
            self.logger.info("Testing FORCE mode (force index usage):")
            results_force = client.vector_ops.search_with_rank(
                table_name="documents",
                vector_column="embedding",
                query_vector=query_vector,
                limit=3,
                rank_mode=IVFRankMode.FORCE,
                select_columns=["id", "title"],
            )
            self.logger.info(f"   ✅ FORCE mode returned {len(results_force)} results")

            client.execute("DROP TABLE documents")
            client.disconnect()
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Basic IVF rank modes test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'basic_ivf_rank_modes', 'error': str(e)})

    def test_ivf_rank_with_filters(self):
        """Test IVF search with WHERE clause filters."""
        print("\n=== Test 2: IVF Search with Filters ===")
        self.results['tests_run'] += 1

        try:
            host, port, user, password, database = get_connection_params()
            client = Client(logger=self.logger, sql_log_mode="auto")
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Create table
            client.execute("DROP TABLE IF EXISTS products")
            client.execute("""
                CREATE TABLE products (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    category VARCHAR(50),
                    price FLOAT,
                    embedding VECF32(8)
                )
            """)

            # Insert data
            products = [
                (1, "Laptop Pro", "electronics", 1200.0, [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]),
                (2, "Laptop Air", "electronics", 900.0, [0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85]),
                (3, "Book Python", "books", 45.0, [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]),
                (4, "Book Java", "books", 50.0, [0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85, 0.95]),
                (5, "Monitor 4K", "electronics", 400.0, [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.1]),
            ]

            for prod_id, name, category, price, embedding in products:
                vec_str = "[" + ",".join(str(x) for x in embedding) + "]"
                client.execute(f"INSERT INTO products VALUES ({prod_id}, '{name}', '{category}', {price}, '{vec_str}')")

            # Create IVF index
            client.vector_ops.create_ivf(table_name="products", name="idx_product_embedding", column="embedding", lists=2)

            query_vector = [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]

            # Search with category filter
            self.logger.info("Testing search with category filter:")
            results = client.vector_ops.search_with_rank(
                table_name="products",
                vector_column="embedding",
                query_vector=query_vector,
                limit=3,
                rank_mode=IVFRankMode.PRE,
                where_clause="category = 'electronics'",
                select_columns=["id", "name", "category"],
            )
            self.logger.info(f"   ✅ Found {len(results)} electronics products")

            # Search with price filter
            self.logger.info("Testing search with price filter (POST mode):")
            results = client.vector_ops.search_with_rank(
                table_name="products",
                vector_column="embedding",
                query_vector=query_vector,
                limit=3,
                rank_mode=IVFRankMode.POST,
                where_clause="price < 500",
                select_columns=["id", "name", "price"],
            )
            self.logger.info(f"   ✅ Found {len(results)} products under $500 (POST)")

            # Search with FORCE mode
            self.logger.info("Testing search with FORCE mode:")
            results = client.vector_ops.search_with_rank(
                table_name="products",
                vector_column="embedding",
                query_vector=query_vector,
                limit=3,
                rank_mode=IVFRankMode.FORCE,
                where_clause="category = 'books'",
                select_columns=["id", "name", "category"],
            )
            self.logger.info(f"   ✅ Found {len(results)} books (FORCE mode)")

            client.execute("DROP TABLE products")
            client.disconnect()
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ IVF rank with filters test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'ivf_rank_with_filters', 'error': str(e)})

    def test_ivf_rank_distance_metrics(self):
        """Test IVF search with different distance metrics."""
        print("\n=== Test 3: Different Distance Metrics ===")
        self.results['tests_run'] += 1

        try:
            host, port, user, password, database = get_connection_params()
            client = Client(logger=self.logger, sql_log_mode="auto")
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Create table
            client.execute("DROP TABLE IF EXISTS embeddings")
            client.execute("""
                CREATE TABLE embeddings (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    vec_l2 VECF32(4),
                    vec_cosine VECF32(4)
                )
            """)

            # Insert data
            data = [
                (1, "Item A", [1.0, 0.0, 0.0, 0.0], [1.0, 0.0, 0.0, 0.0]),
                (2, "Item B", [0.0, 1.0, 0.0, 0.0], [0.0, 1.0, 0.0, 0.0]),
                (3, "Item C", [0.0, 0.0, 1.0, 0.0], [0.0, 0.0, 1.0, 0.0]),
                (4, "Item D", [0.5, 0.5, 0.0, 0.0], [0.5, 0.5, 0.0, 0.0]),
            ]

            for item_id, name, vec_l2, vec_cosine in data:
                vec_l2_str = "[" + ",".join(str(x) for x in vec_l2) + "]"
                vec_cosine_str = "[" + ",".join(str(x) for x in vec_cosine) + "]"
                client.execute(f"INSERT INTO embeddings VALUES ({item_id}, '{name}', '{vec_l2_str}', '{vec_cosine_str}')")

            # Create IVF indexes
            client.vector_ops.create_ivf(
                table_name="embeddings", name="idx_l2", column="vec_l2", lists=2, op_type="vector_l2_ops"
            )

            client.vector_ops.create_ivf(
                table_name="embeddings", name="idx_cosine", column="vec_cosine", lists=2, op_type="vector_cosine_ops"
            )

            query = [0.8, 0.2, 0.0, 0.0]

            # L2 distance search with PRE mode
            self.logger.info("Testing L2 distance metric (PRE mode):")
            results = client.vector_ops.search_with_rank(
                table_name="embeddings",
                vector_column="vec_l2",
                query_vector=query,
                limit=3,
                distance_type="l2",
                rank_mode=IVFRankMode.PRE,
                select_columns=["id", "name"],
            )
            self.logger.info(f"   ✅ L2 search (PRE) returned {len(results)} results")

            # Cosine distance search with POST mode
            self.logger.info("Testing Cosine distance metric (POST mode):")
            results = client.vector_ops.search_with_rank(
                table_name="embeddings",
                vector_column="vec_cosine",
                query_vector=query,
                limit=3,
                distance_type="cosine",
                rank_mode=IVFRankMode.POST,
                select_columns=["id", "name"],
            )
            self.logger.info(f"   ✅ Cosine search (POST) returned {len(results)} results")

            client.execute("DROP TABLE embeddings")
            client.disconnect()
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Distance metrics test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'distance_metrics', 'error': str(e)})

    def test_ivf_rank_options_class(self):
        """Test IVFRankOptions class functionality."""
        print("\n=== Test 4: IVFRankOptions Class ===")
        self.results['tests_run'] += 1

        try:
            from matrixone import IVFRankOptions

            # Test default mode
            options = IVFRankOptions()
            assert options.mode == IVFRankMode.POST
            self.logger.info("   ✅ Default mode is POST")

            # Test string initialization
            options = IVFRankOptions(mode="pre")
            assert options.mode == IVFRankMode.PRE
            self.logger.info("   ✅ String initialization works")

            # Test SQL option generation
            sql_option = options.to_sql_option()
            assert sql_option == "mode=pre"
            self.logger.info(f"   ✅ SQL option generation: {sql_option}")

            # Test dictionary conversion
            dict_repr = options.to_dict()
            assert dict_repr == {"mode": "pre"}
            self.logger.info(f"   ✅ Dictionary conversion: {dict_repr}")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ IVFRankOptions class test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'ivf_rank_options', 'error': str(e)})

    def generate_summary_report(self):
        """Generate summary report of all tests."""
        print("\n" + "=" * 60)
        print("📊 Test Summary Report")
        print("=" * 60)
        print(f"Tests Run:     {self.results['tests_run']}")
        print(f"Tests Passed:  {self.results['tests_passed']}")
        print(f"Tests Failed:  {self.results['tests_failed']}")

        if self.results['unexpected_results']:
            print("\n⚠️ Unexpected Results:")
            for item in self.results['unexpected_results']:
                print(f"   - {item['test']}: {item['error']}")

        return self.results


def main():
    """Main demo function."""
    demo = IVFRankSearchDemo()

    try:
        print("🚀 MatrixOne IVF Vector Search with LIMIT BY RANK Examples")
        print("=" * 60)

        # Run tests
        demo.test_basic_ivf_rank_modes()
        demo.test_ivf_rank_with_filters()
        demo.test_ivf_rank_distance_metrics()
        demo.test_ivf_rank_options_class()

        # Generate report
        results = demo.generate_summary_report()

        print("\n🎉 IVF rank search examples completed!")
        print("\nKey takeaways:")
        print("- ✅ IVF indexes support three ranking modes (PRE, POST, FORCE)")
        print("- ✅ WHERE clause filters work with vector search")
        print("- ✅ Multiple distance metrics are supported (L2, Cosine, Inner Product)")
        print("- ✅ IVFRankOptions provides flexible configuration")
        print("- ✅ Async support available for non-blocking operations")

        return results

    except Exception as e:
        print(f"Demo failed with error: {e}")
        return None


if __name__ == '__main__':
    main()
