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
Online tests for secondary index verification functionality
"""

import unittest
import os
import sys

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone import Client
from matrixone.orm import declarative_base
from sqlalchemy import Column, String, Integer, Index


class TestIndexVerificationOnline(unittest.TestCase):
    """Online tests for secondary index verification"""

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

        cls.test_table = "test_index_table"

        try:
            # Define ORM model with secondary indexes
            Base = declarative_base()

            class TestTable(Base):
                __tablename__ = cls.test_table

                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                category = Column(String(50))
                value = Column(Integer)

                # Define secondary indexes
                __table_args__ = (
                    Index('idx_name', 'name'),
                    Index('idx_category', 'category'),
                    Index('idx_value', 'value'),
                )

            cls.TestTable = TestTable

            # Drop table if exists and create new one
            try:
                cls.client.drop_table(TestTable)
            except:
                pass

            # Create table with indexes using SDK API
            cls.client.create_table(TestTable)

            # Insert test data using SDK API
            test_data = [{'id': i, 'name': f'name_{i}', 'category': f'cat_{i % 5}', 'value': i * 10} for i in range(1, 101)]
            cls.client.batch_insert(TestTable, test_data)

        except Exception as e:
            print(f"Setup failed: {e}")
            raise

    @classmethod
    def tearDownClass(cls):
        """Clean up test database"""
        try:
            cls.client.drop_table(cls.TestTable)
        except Exception as e:
            print(f"Cleanup failed: {e}")

    def test_get_secondary_index_tables(self):
        """Test getting all secondary index table names"""
        index_tables = self.client.get_secondary_index_tables(self.test_table)

        # Should have 3 secondary indexes
        self.assertEqual(len(index_tables), 3, f"Expected 3 indexes, got {len(index_tables)}")

        # All index tables should start with __mo_index_secondary_
        for index_table in index_tables:
            self.assertTrue(
                index_table.startswith('__mo_index_secondary_'),
                f"Index table name should start with '__mo_index_secondary_', got {index_table}",
            )

    def test_get_secondary_index_table_by_name(self):
        """Test getting physical table name of a secondary index by its name"""
        # Test getting idx_name
        idx_name_table = self.client.get_secondary_index_table_by_name(self.test_table, 'idx_name')
        self.assertIsNotNone(idx_name_table, "idx_name should exist")
        self.assertTrue(idx_name_table.startswith('__mo_index_secondary_'))

        # Test getting idx_category
        idx_category_table = self.client.get_secondary_index_table_by_name(self.test_table, 'idx_category')
        self.assertIsNotNone(idx_category_table, "idx_category should exist")
        self.assertTrue(idx_category_table.startswith('__mo_index_secondary_'))

        # Test getting idx_value
        idx_value_table = self.client.get_secondary_index_table_by_name(self.test_table, 'idx_value')
        self.assertIsNotNone(idx_value_table, "idx_value should exist")
        self.assertTrue(idx_value_table.startswith('__mo_index_secondary_'))

        # Test non-existent index
        non_existent = self.client.get_secondary_index_table_by_name(self.test_table, 'idx_non_existent')
        self.assertIsNone(non_existent, "Non-existent index should return None")

    def test_verify_table_index_counts(self):
        """Test verify_table_index_counts when counts match"""
        # All indexes should have the same count as the main table
        count = self.client.verify_table_index_counts(self.test_table)

        # Should return 100 (number of inserted rows)
        self.assertEqual(count, 100, f"Expected count 100, got {count}")

        # Verify count matches query result using SDK API
        actual_count = self.client.query(self.TestTable).count()
        self.assertEqual(count, actual_count, "Verify result should match actual count")

    def test_verify_table_without_indexes(self):
        """Test verify_table_index_counts on table without secondary indexes"""
        # Define a simple model without secondary indexes
        Base = declarative_base()

        class SimpleTable(Base):
            __tablename__ = "test_simple_table"
            id = Column(Integer, primary_key=True)
            value = Column(Integer)

        # Create table using SDK API
        try:
            self.client.drop_table(SimpleTable)
        except:
            pass

        self.client.create_table(SimpleTable)

        # Insert data using SDK API
        test_data = [{'id': 1, 'value': 100}, {'id': 2, 'value': 200}]
        self.client.batch_insert(SimpleTable, test_data)

        # Verification should succeed and return count
        count = self.client.verify_table_index_counts("test_simple_table")
        self.assertEqual(count, 2, f"Expected count 2, got {count}")

        # Cleanup using SDK API
        self.client.drop_table(SimpleTable)

    def test_index_table_mapping(self):
        """Test that all indexes can be retrieved both ways"""
        # Get all index tables
        all_index_tables = self.client.get_secondary_index_tables(self.test_table)

        # Get each index by name
        index_names = ['idx_name', 'idx_category', 'idx_value']
        retrieved_tables = []

        for index_name in index_names:
            table = self.client.get_secondary_index_table_by_name(self.test_table, index_name)
            self.assertIsNotNone(table, f"Index {index_name} should exist")
            retrieved_tables.append(table)

        # All retrieved tables should be in the all_index_tables list
        for table in retrieved_tables:
            self.assertIn(table, all_index_tables, f"Retrieved table {table} should be in all index tables")

        # Both lists should have the same length
        self.assertEqual(
            len(retrieved_tables), len(all_index_tables), "Number of retrieved tables should match total index tables"
        )


if __name__ == '__main__':
    unittest.main()
