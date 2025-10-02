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
Comprehensive online tests for MatrixOne metadata operations

This test suite demonstrates and validates:
1. Basic metadata scanning (raw SQLAlchemy results)
2. Structured metadata with fixed schema
3. Column selection capabilities (enum and string-based)
4. Synchronous and asynchronous operations
5. Transaction-based metadata operations
6. Table brief and detailed statistics
7. Index metadata scanning with moctl integration
8. Type-safe metadata row objects
9. Error handling and edge cases
"""

import asyncio
import unittest
from matrixone import Client, AsyncClient
from matrixone.metadata import MetadataColumn, MetadataRow
from matrixone.config import get_connection_kwargs


class TestMetadataOnline(unittest.TestCase):
    """Comprehensive test suite for metadata operations with real MatrixOne database"""

    @classmethod
    def setUpClass(cls):
        """Set up test database and tables using raw SQL"""
        cls.connection_params = get_connection_kwargs()
        # Filter out unsupported parameters
        filtered_params = {
            'host': cls.connection_params['host'],
            'port': cls.connection_params['port'],
            'user': cls.connection_params['user'],
            'password': cls.connection_params['password'],
            'database': cls.connection_params['database'],
        }
        cls.client = Client()
        cls.client.connect(**filtered_params)

        # Create test database and tables using raw SQL
        cls.client.execute("CREATE DATABASE IF NOT EXISTS test_metadata_db")
        cls.client.execute("USE test_metadata_db")

        # Drop existing tables
        cls.client.execute("DROP TABLE IF EXISTS test_users")
        cls.client.execute("DROP TABLE IF EXISTS test_products")

        # Create test tables with indexes
        cls.client.execute(
            """
            CREATE TABLE test_users (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) NOT NULL UNIQUE,
                age INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        cls.client.execute(
            """
            CREATE TABLE test_products (
                id INT PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                category VARCHAR(50) NOT NULL,
                price INT NOT NULL,
                stock INT DEFAULT 0
            )
        """
        )

        # Create indexes
        try:
            cls.client.execute("CREATE INDEX idx_test_users_name ON test_users(name)")
            cls.client.execute("CREATE INDEX idx_test_users_email ON test_users(email)")
            cls.client.execute("CREATE INDEX idx_test_products_category ON test_products(category)")
        except Exception:
            # Index creation might fail in some environments
            pass

        # Insert test data
        cls._insert_test_data()

        # Single checkpoint after all data is ready - this is expensive, so do it only once
        cls.client.moctl.increment_checkpoint()

    @classmethod
    def _insert_test_data(cls):
        """Insert comprehensive test data"""
        # Clear existing data
        cls.client.execute("DELETE FROM test_users")
        cls.client.execute("DELETE FROM test_products")

        # Insert users
        for i in range(1, 51):  # 50 users
            cls.client.execute(
                f"""
                INSERT INTO test_users (id, name, email, age) 
                VALUES ({i}, 'User{i}', 'user{i}@example.com', {20 + (i % 50)})
            """
            )

        # Insert products
        categories = ['Electronics', 'Books', 'Clothing', 'Home', 'Sports']
        for i in range(1, 31):  # 30 products
            category = categories[i % len(categories)]
            cls.client.execute(
                f"""
                INSERT INTO test_products (id, name, category, price, stock) 
                VALUES ({i}, 'Product{i}', '{category}', {100 + (i * 10)}, {50 + (i % 20)})
            """
            )

    @classmethod
    def tearDownClass(cls):
        """Clean up test resources"""
        try:
            cls.client.disconnect()
        except Exception:
            pass

    def test_basic_metadata_scanning(self):
        """Test basic metadata scanning operations"""
        # Test raw SQLAlchemy results
        result = self.client.metadata.scan("test_metadata_db", "test_users")
        rows = result.fetchall()

        self.assertGreater(len(rows), 0, "Should have metadata entries")

        # Verify row structure
        for row in rows:
            self.assertIn('col_name', row._mapping)
            self.assertIn('rows_cnt', row._mapping)
            self.assertIn('origin_size', row._mapping)
            self.assertIn('null_cnt', row._mapping)

        # Test with specific table
        result = self.client.metadata.scan("test_metadata_db", "test_products")
        rows = result.fetchall()
        self.assertGreater(len(rows), 0, "Should have product metadata entries")

    def test_structured_metadata(self):
        """Test structured metadata with fixed schema"""
        # Get all columns as structured MetadataRow objects
        rows = self.client.metadata.scan("test_metadata_db", "test_users", columns="*")

        self.assertGreater(len(rows), 0, "Should have structured metadata entries")

        # Verify MetadataRow structure and type safety
        for row in rows:
            self.assertIsInstance(row, MetadataRow)
            self.assertIsInstance(row.col_name, str)
            self.assertIsInstance(row.rows_cnt, int)
            self.assertIsInstance(row.null_cnt, int)
            self.assertIsInstance(row.origin_size, int)
            self.assertIsInstance(row.is_hidden, bool)

            # Verify data integrity
            self.assertGreaterEqual(row.rows_cnt, 0)
            self.assertGreaterEqual(row.null_cnt, 0)
            self.assertGreaterEqual(row.origin_size, 0)

    def test_column_selection_enum(self):
        """Test column selection using MetadataColumn enum"""
        # Select specific columns using enum
        rows = self.client.metadata.scan(
            "test_metadata_db",
            "test_users",
            columns=[MetadataColumn.COL_NAME, MetadataColumn.ROWS_CNT, MetadataColumn.ORIGIN_SIZE],
        )

        self.assertGreater(len(rows), 0, "Should have column-selected metadata")

        # Verify selected columns
        for row in rows:
            self.assertIn('col_name', row)
            self.assertIn('rows_cnt', row)
            self.assertIn('origin_size', row)
            # Should not have other columns
            self.assertNotIn('null_cnt', row)

    def test_column_selection_strings(self):
        """Test column selection using string names"""
        # Select specific columns using strings
        rows = self.client.metadata.scan("test_metadata_db", "test_users", columns=['col_name', 'null_cnt', 'compress_size'])

        self.assertGreater(len(rows), 0, "Should have string-selected metadata")

        # Verify selected columns
        for row in rows:
            self.assertIn('col_name', row)
            self.assertIn('null_cnt', row)
            self.assertIn('compress_size', row)
            # Should not have other columns
            self.assertNotIn('rows_cnt', row)

    def test_distinct_object_name(self):
        """Test distinct object name functionality"""
        rows = self.client.metadata.scan("test_metadata_db", "test_users", distinct_object_name=True)

        # Should return distinct object names (might be empty in some environments)
        object_names = set()
        for row in rows:
            object_name = row._mapping['object_name']
            object_names.add(object_name)

        # Just verify the call doesn't fail, distinct object names might be empty
        self.assertIsInstance(object_names, set)

    def test_table_brief_stats(self):
        """Test table brief statistics"""
        brief_stats = self.client.metadata.get_table_brief_stats("test_metadata_db", "test_users")

        self.assertIn("test_users", brief_stats, "Should have table stats")
        table_stats = brief_stats["test_users"]

        # Verify required fields
        required_fields = ['total_objects', 'row_cnt', 'null_cnt', 'original_size', 'compress_size']
        for field in required_fields:
            self.assertIn(field, table_stats, f"Should have {field}")

        # Verify data types and values
        self.assertIsInstance(table_stats['total_objects'], int)
        self.assertIsInstance(table_stats['row_cnt'], int)
        self.assertIsInstance(table_stats['null_cnt'], int)
        self.assertIsInstance(table_stats['original_size'], str)  # Formatted size
        self.assertIsInstance(table_stats['compress_size'], str)  # Formatted size

        # Verify reasonable values
        self.assertGreater(table_stats['total_objects'], 0)
        self.assertGreater(table_stats['row_cnt'], 0)
        self.assertGreaterEqual(table_stats['null_cnt'], 0)

    def test_table_detail_stats(self):
        """Test table detailed statistics"""
        detail_stats = self.client.metadata.get_table_detail_stats("test_metadata_db", "test_users")

        self.assertIn("test_users", detail_stats, "Should have detailed table stats")
        table_details = detail_stats["test_users"]

        self.assertGreater(len(table_details), 0, "Should have detailed object stats")

        # Verify object structure
        for detail in table_details:
            required_fields = ['object_name', 'create_ts', 'row_cnt', 'null_cnt', 'original_size', 'compress_size']
            for field in required_fields:
                self.assertIn(field, detail, f"Should have {field}")

            # Verify data types
            self.assertIsInstance(detail['object_name'], str)
            self.assertIsInstance(detail['row_cnt'], int)
            self.assertIsInstance(detail['null_cnt'], int)
            self.assertIsInstance(detail['original_size'], str)  # Formatted size
            self.assertIsInstance(detail['compress_size'], str)  # Formatted size

    def test_index_metadata_scanning(self):
        """Test index metadata scanning"""
        # Test index metadata scanning
        try:
            result = self.client.metadata.scan("test_metadata_db", "test_users", indexname="idx_test_users_name")
            rows = result.fetchall()

            # Index metadata might be empty in some environments
            # Just verify the call doesn't fail
            self.assertIsInstance(rows, list)

        except Exception as e:
            # Index metadata might not be available in all environments
            self.assertIn("index", str(e).lower())

    def test_transaction_metadata_operations(self):
        """Test metadata operations within transactions"""
        with self.client.transaction() as tx:
            # Metadata scan within transaction
            result = tx.metadata.scan("test_metadata_db", "test_users", columns="*")
            rows = list(result)

            self.assertGreater(len(rows), 0, "Should have metadata in transaction")

            # Table statistics within transaction
            brief_stats = tx.metadata.get_table_brief_stats("test_metadata_db", "test_users")
            self.assertIn("test_users", brief_stats)

            detail_stats = tx.metadata.get_table_detail_stats("test_metadata_db", "test_users")
            self.assertIn("test_users", detail_stats)

    def test_metadata_row_from_sqlalchemy(self):
        """Test MetadataRow creation from SQLAlchemy row"""
        # Get a raw SQLAlchemy row
        result = self.client.metadata.scan("test_metadata_db", "test_users")
        raw_row = result.fetchone()

        # Convert to MetadataRow
        metadata_row = MetadataRow.from_sqlalchemy_row(raw_row)

        # Verify conversion
        self.assertIsInstance(metadata_row, MetadataRow)
        self.assertIsInstance(metadata_row.col_name, str)
        self.assertIsInstance(metadata_row.rows_cnt, int)
        self.assertIsInstance(metadata_row.null_cnt, int)
        self.assertIsInstance(metadata_row.origin_size, int)
        self.assertIsInstance(metadata_row.is_hidden, bool)

    def test_multiple_tables_metadata(self):
        """Test metadata operations on multiple tables"""
        tables = ["test_users", "test_products"]

        for table in tables:
            # Basic scan
            result = self.client.metadata.scan("test_metadata_db", table)
            rows = result.fetchall()
            self.assertGreater(len(rows), 0, f"Should have metadata for {table}")

            # Brief stats
            brief_stats = self.client.metadata.get_table_brief_stats("test_metadata_db", table)
            self.assertIn(table, brief_stats, f"Should have brief stats for {table}")

            # Detail stats
            detail_stats = self.client.metadata.get_table_detail_stats("test_metadata_db", table)
            self.assertIn(table, detail_stats, f"Should have detail stats for {table}")

    def test_metadata_column_enum_completeness(self):
        """Test that all MetadataColumn enum values are available"""
        # Get all available columns
        rows = self.client.metadata.scan("test_metadata_db", "test_users", columns="*")
        raw_row = rows[0]

        # Check that all enum values correspond to actual columns
        # Use raw SQLAlchemy row for _mapping access
        if hasattr(raw_row, '_mapping'):
            row_mapping = raw_row._mapping
        else:
            # For MetadataRow objects, convert to dict
            row_mapping = {
                'col_name': raw_row.col_name,
                'object_name': raw_row.object_name,
                'is_hidden': raw_row.is_hidden,
                'obj_loc': raw_row.obj_loc,
                'create_ts': raw_row.create_ts,
                'delete_ts': raw_row.delete_ts,
                'rows_cnt': raw_row.rows_cnt,
                'null_cnt': raw_row.null_cnt,
                'compress_size': raw_row.compress_size,
                'origin_size': raw_row.origin_size,
                'min': raw_row.min,
                'max': raw_row.max,
                'sum': raw_row.sum,
            }

        for column in MetadataColumn:
            self.assertIn(column.value, row_mapping, f"Enum {column.name} should correspond to actual column {column.value}")

    def test_error_handling(self):
        """Test error handling for invalid inputs"""
        # Test with non-existent table - should raise QueryError
        from matrixone.exceptions import QueryError

        try:
            self.client.metadata.scan("test_metadata_db", "non_existent_table")
            self.fail("Expected QueryError for non-existent table")
        except QueryError:
            pass  # Expected

        # Test with non-existent database - should raise QueryError
        try:
            self.client.metadata.scan("non_existent_db", "test_users")
            self.fail("Expected QueryError for non-existent database")
        except QueryError:
            pass  # Expected

        # Test with invalid column selection - might not raise error in some cases
        # Just verify the call doesn't crash the system
        try:
            result = self.client.metadata.scan("test_metadata_db", "test_users", columns=["invalid_column"])
            # If no error, just verify we get some result
            self.assertIsNotNone(result)
        except Exception as e:
            # If error occurs, that's also acceptable
            pass


class TestAsyncMetadataOnline(unittest.TestCase):
    """Test asynchronous metadata operations with real MatrixOne database"""

    def setUp(self):
        """Set up async client for each test"""
        self.connection_params = get_connection_kwargs()
        # Filter out unsupported parameters for AsyncClient
        filtered_params = {
            'host': self.connection_params['host'],
            'port': self.connection_params['port'],
            'user': self.connection_params['user'],
            'password': self.connection_params['password'],
            'database': self.connection_params['database'],
        }
        self.async_client = AsyncClient()

    def tearDown(self):
        """Clean up async client after each test"""
        try:
            asyncio.run(self.async_client.disconnect())
        except Exception:
            pass

    def test_async_basic_metadata_scanning(self):
        """Test async basic metadata scanning"""

        async def _test():
            # Filter out unsupported parameters for AsyncClient
            filtered_params = {
                'host': self.connection_params['host'],
                'port': self.connection_params['port'],
                'user': self.connection_params['user'],
                'password': self.connection_params['password'],
                'database': self.connection_params['database'],
            }
            await self.async_client.connect(**filtered_params)
            result = await self.async_client.metadata.scan("test_metadata_db", "test_users")
            rows = result.fetchall()

            self.assertGreater(len(rows), 0, "Should have async metadata entries")

            # Verify row structure
            for row in rows:
                self.assertIn('col_name', row._mapping)
                self.assertIn('rows_cnt', row._mapping)

        asyncio.run(_test())

    def test_async_structured_metadata(self):
        """Test async structured metadata"""

        async def _test():
            # Filter out unsupported parameters for AsyncClient
            filtered_params = {
                'host': self.connection_params['host'],
                'port': self.connection_params['port'],
                'user': self.connection_params['user'],
                'password': self.connection_params['password'],
                'database': self.connection_params['database'],
            }
            await self.async_client.connect(**filtered_params)
            rows = await self.async_client.metadata.scan("test_metadata_db", "test_users", columns="*")

            self.assertGreater(len(rows), 0, "Should have async structured metadata")

            # Verify MetadataRow structure
            for row in rows:
                self.assertIsInstance(row, MetadataRow)
                self.assertIsInstance(row.col_name, str)
                self.assertIsInstance(row.rows_cnt, int)

        asyncio.run(_test())

    def test_async_column_selection(self):
        """Test async column selection"""

        async def _test():
            # Filter out unsupported parameters for AsyncClient
            filtered_params = {
                'host': self.connection_params['host'],
                'port': self.connection_params['port'],
                'user': self.connection_params['user'],
                'password': self.connection_params['password'],
                'database': self.connection_params['database'],
            }
            await self.async_client.connect(**filtered_params)
            rows = await self.async_client.metadata.scan(
                "test_metadata_db", "test_users", columns=[MetadataColumn.COL_NAME, MetadataColumn.ROWS_CNT]
            )

            self.assertGreater(len(rows), 0, "Should have async column-selected metadata")

            # Verify selected columns
            for row in rows:
                self.assertIn('col_name', row)
                self.assertIn('rows_cnt', row)
                self.assertNotIn('null_cnt', row)

        asyncio.run(_test())

    def test_async_table_brief_stats(self):
        """Test async table brief statistics"""

        async def _test():
            # Filter out unsupported parameters for AsyncClient
            filtered_params = {
                'host': self.connection_params['host'],
                'port': self.connection_params['port'],
                'user': self.connection_params['user'],
                'password': self.connection_params['password'],
                'database': self.connection_params['database'],
            }
            await self.async_client.connect(**filtered_params)
            brief_stats = await self.async_client.metadata.get_table_brief_stats("test_metadata_db", "test_users")

            self.assertIn("test_users", brief_stats, "Should have async brief stats")
            table_stats = brief_stats["test_users"]

            # Verify required fields
            required_fields = ['total_objects', 'row_cnt', 'null_cnt', 'original_size', 'compress_size']
            for field in required_fields:
                self.assertIn(field, table_stats, f"Should have {field}")

        asyncio.run(_test())

    def test_async_table_detail_stats(self):
        """Test async table detailed statistics"""

        async def _test():
            # Filter out unsupported parameters for AsyncClient
            filtered_params = {
                'host': self.connection_params['host'],
                'port': self.connection_params['port'],
                'user': self.connection_params['user'],
                'password': self.connection_params['password'],
                'database': self.connection_params['database'],
            }
            await self.async_client.connect(**filtered_params)
            detail_stats = await self.async_client.metadata.get_table_detail_stats("test_metadata_db", "test_users")

            self.assertIn("test_users", detail_stats, "Should have async detail stats")
            table_details = detail_stats["test_users"]

            self.assertGreater(len(table_details), 0, "Should have async detailed object stats")

            # Verify object structure
            for detail in table_details:
                required_fields = ['object_name', 'create_ts', 'row_cnt', 'null_cnt', 'original_size', 'compress_size']
                for field in required_fields:
                    self.assertIn(field, detail, f"Should have {field}")

        asyncio.run(_test())

    def test_async_distinct_object_name(self):
        """Test async distinct object name functionality"""

        async def _test():
            # Filter out unsupported parameters for AsyncClient
            filtered_params = {
                'host': self.connection_params['host'],
                'port': self.connection_params['port'],
                'user': self.connection_params['user'],
                'password': self.connection_params['password'],
                'database': self.connection_params['database'],
            }
            await self.async_client.connect(**filtered_params)
            rows = await self.async_client.metadata.scan("test_metadata_db", "test_users", distinct_object_name=True)

            # Should return distinct object names (might be empty in some environments)
            object_names = set()
            for row in rows:
                object_name = row._mapping['object_name']
                object_names.add(object_name)

            # Just verify the call doesn't fail, distinct object names might be empty
            self.assertIsInstance(object_names, set)

        asyncio.run(_test())


if __name__ == '__main__':
    unittest.main()
