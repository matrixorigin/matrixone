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
Online tests for Client functionality - tests actual database operations
"""

import unittest
import os
import sys
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone import Client
from matrixone.exceptions import ConnectionError, QueryError


class TestClientOnline(unittest.TestCase):
    """Online tests for Client functionality"""

    @classmethod
    def setUpClass(cls):
        """Set up test database connection"""
        cls.client = Client(
            host=os.getenv('MATRIXONE_HOST', '127.0.0.1'),
            port=int(os.getenv('MATRIXONE_PORT', '6001')),
            user=os.getenv('MATRIXONE_USER', 'root'),
            password=os.getenv('MATRIXONE_PASSWORD', '111'),
            database=os.getenv('MATRIXONE_DATABASE', 'test'),
        )

        # Create test database and table
        cls.test_db = "test_client_db"
        cls.test_table = "test_client_table"

        try:
            cls.client.execute(f"CREATE DATABASE IF NOT EXISTS {cls.test_db}")
            cls.client.execute(f"USE {cls.test_db}")
            cls.client.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {cls.test_table} (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    value INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Insert test data
            cls.client.execute(f"INSERT INTO {cls.test_table} VALUES (1, 'test1', 100, NOW())")
            cls.client.execute(f"INSERT INTO {cls.test_table} VALUES (2, 'test2', 200, NOW())")
            cls.client.execute(f"INSERT INTO {cls.test_table} VALUES (3, 'test3', 300, NOW())")

        except Exception as e:
            print(f"Setup failed: {e}")
            raise

    @classmethod
    def tearDownClass(cls):
        """Clean up test database"""
        try:
            cls.client.execute(f"DROP DATABASE IF EXISTS {cls.test_db}")
        except Exception as e:
            print(f"Cleanup failed: {e}")

    def test_basic_connection_and_query(self):
        """Test basic connection and query functionality"""
        # Test simple query
        result = self.client.execute("SELECT 1 as test_value")
        rows = result.fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 1)

        # Test query with parameters
        result = self.client.execute("SELECT ? as param_value", (42,))
        rows = result.fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 42)

    def test_table_operations(self):
        """Test table operations"""
        # Test SELECT
        result = self.client.execute(f"SELECT * FROM {self.test_table} ORDER BY id")
        rows = result.fetchall()
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0][0], 1)  # id
        self.assertEqual(rows[0][1], 'test1')  # name

        # Test INSERT
        self.client.execute(f"INSERT INTO {self.test_table} VALUES (4, 'test4', 400, NOW())")

        # Verify INSERT
        result = self.client.execute(f"SELECT COUNT(*) FROM {self.test_table}")
        count = result.fetchone()[0]
        self.assertEqual(count, 4)

        # Test UPDATE
        self.client.execute(f"UPDATE {self.test_table} SET value = 500 WHERE id = 4")

        # Verify UPDATE
        result = self.client.execute(f"SELECT value FROM {self.test_table} WHERE id = 4")
        value = result.fetchone()[0]
        self.assertEqual(value, 500)

        # Test DELETE
        self.client.execute(f"DELETE FROM {self.test_table} WHERE id = 4")

        # Verify DELETE
        result = self.client.execute(f"SELECT COUNT(*) FROM {self.test_table}")
        count = result.fetchone()[0]
        self.assertEqual(count, 3)

    def test_transaction_operations(self):
        """Test transaction operations"""
        with self.client.transaction() as tx:
            # Insert within transaction
            tx.execute(f"INSERT INTO {self.test_table} VALUES (5, 'test5', 500, NOW())")

            # Verify within transaction
            result = tx.execute(f"SELECT COUNT(*) FROM {self.test_table}")
            count = result.fetchone()[0]
            self.assertEqual(count, 4)

        # Verify transaction commit
        result = self.client.execute(f"SELECT COUNT(*) FROM {self.test_table}")
        count = result.fetchone()[0]
        self.assertEqual(count, 4)

        # Test transaction rollback
        try:
            with self.client.transaction() as tx:
                tx.execute(f"INSERT INTO {self.test_table} VALUES (6, 'test6', 600, NOW())")
                # Force rollback by raising exception
                raise Exception("Test rollback")
        except Exception:
            pass

        # Verify rollback
        result = self.client.execute(f"SELECT COUNT(*) FROM {self.test_table}")
        count = result.fetchone()[0]
        self.assertEqual(count, 4)  # Should still be 4, not 5

        # Clean up
        self.client.execute(f"DELETE FROM {self.test_table} WHERE id = 5")

    def test_error_handling(self):
        """Test error handling"""
        # Test connection error handling
        with self.assertRaises(QueryError):
            self.client.execute("SELECT * FROM non_existent_table")

        # Test invalid SQL
        with self.assertRaises(QueryError):
            self.client.execute("INVALID SQL STATEMENT")

    def test_result_set_operations(self):
        """Test result set operations"""
        result = self.client.execute(f"SELECT * FROM {self.test_table} ORDER BY id")

        # Test fetchone
        first_row = result.fetchone()
        self.assertIsNotNone(first_row)
        self.assertEqual(first_row[0], 1)

        # Test fetchmany
        next_rows = result.fetchmany(2)
        self.assertEqual(len(next_rows), 2)
        self.assertEqual(next_rows[0][0], 2)
        self.assertEqual(next_rows[1][0], 3)

        # Test fetchall (should return remaining rows)
        remaining_rows = result.fetchall()
        self.assertEqual(len(remaining_rows), 0)  # Should be empty

        # Test column names
        result = self.client.execute(f"SELECT id, name, value FROM {self.test_table} LIMIT 1")
        columns = result.keys()
        expected_columns = ['id', 'name', 'value']
        self.assertEqual(list(columns), expected_columns)

    def test_parameter_binding(self):
        """Test parameter binding"""
        # Test string parameters
        result = self.client.execute(f"SELECT * FROM {self.test_table} WHERE name = ?", ('test1',))
        rows = result.fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], 'test1')

        # Test numeric parameters
        result = self.client.execute(f"SELECT * FROM {self.test_table} WHERE value = ?", (200,))
        rows = result.fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][2], 200)

        # Test multiple parameters
        result = self.client.execute(f"SELECT * FROM {self.test_table} WHERE name = ? AND value > ?", ('test2', 150))
        rows = result.fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], 'test2')
        self.assertEqual(rows[0][2], 200)

    def test_connection_pooling(self):
        """Test connection pooling and reuse"""
        # Execute multiple queries to test connection reuse
        for i in range(5):
            result = self.client.execute("SELECT 1 as test")
            rows = result.fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], 1)

        # Test concurrent-like operations
        results = []
        for i in range(3):
            result = self.client.execute(f"SELECT COUNT(*) FROM {self.test_table}")
            count = result.fetchone()[0]
            results.append(count)

        # All should return the same count
        for count in results:
            self.assertEqual(count, 3)


if __name__ == '__main__':
    unittest.main()
