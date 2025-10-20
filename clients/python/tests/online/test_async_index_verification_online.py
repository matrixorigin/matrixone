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
Online tests for async secondary index verification functionality
"""

import pytest
import pytest_asyncio
import os
import sys

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone import AsyncClient
from matrixone.orm import declarative_base
from sqlalchemy import Column, String, Integer, Index
from .test_config import online_config


class TestAsyncIndexVerificationOnline:
    """Online tests for async secondary index verification"""

    @pytest_asyncio.fixture(scope="function")
    async def async_client_with_test_table(self):
        """Create AsyncClient with test table"""
        host, port, user, password, database = online_config.get_connection_params()
        client = AsyncClient()
        await client.connect(host=host, port=port, user=user, password=password, database=database)

        test_table = "test_async_index_table"

        try:
            # Define ORM model with secondary indexes
            Base = declarative_base()

            class TestTable(Base):
                __tablename__ = test_table

                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                category = Column(String(50))
                value = Column(Integer)

                # Define secondary indexes
                __table_args__ = (
                    Index('idx_async_name', 'name'),
                    Index('idx_async_category', 'category'),
                    Index('idx_async_value', 'value'),
                )

            # Drop table if exists and create new one using SDK API
            try:
                await client.drop_table(TestTable)
            except:
                pass

            # Create table with indexes using SDK API
            await client.create_table(TestTable)

            # Insert test data using SDK API
            test_data = [
                {'id': i, 'name': f'async_name_{i}', 'category': f'cat_{i % 5}', 'value': i * 10} for i in range(1, 101)
            ]
            await client.batch_insert(TestTable, test_data)

            yield client, test_table, TestTable

        finally:
            # Clean up using SDK API
            try:
                await client.drop_table(TestTable)
                await client.disconnect()
            except Exception as e:
                print(f"Cleanup failed: {e}")

    @pytest.mark.asyncio
    async def test_async_get_secondary_index_tables(self, async_client_with_test_table):
        """Test async getting all secondary index table names"""
        client, test_table, TestTable = async_client_with_test_table

        index_tables = await client.get_secondary_index_tables(test_table)

        # Should have 3 secondary indexes
        assert len(index_tables) == 3, f"Expected 3 indexes, got {len(index_tables)}"

        # All index tables should start with __mo_index_secondary_
        for index_table in index_tables:
            assert index_table.startswith(
                '__mo_index_secondary_'
            ), f"Index table name should start with '__mo_index_secondary_', got {index_table}"


class TestAsyncIndexVerificationOnline:
    """Online tests for async secondary index verification"""

    @pytest_asyncio.fixture(scope="function")
    async def async_client_with_test_table(self):
        """Create AsyncClient with test table"""
        host, port, user, password, database = online_config.get_connection_params()
        client = AsyncClient()
        await client.connect(host=host, port=port, user=user, password=password, database=database)

        test_table = "test_async_index_table"

        try:
            # Define ORM model with secondary indexes
            Base = declarative_base()

            class TestTable(Base):
                __tablename__ = test_table

                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                category = Column(String(50))
                value = Column(Integer)

                # Define secondary indexes
                __table_args__ = (
                    Index('idx_async_name', 'name'),
                    Index('idx_async_category', 'category'),
                    Index('idx_async_value', 'value'),
                )

            # Drop table if exists and create new one using SDK API
            try:
                await client.drop_table(TestTable)
            except:
                pass

            # Create table with indexes using SDK API
            await client.create_table(TestTable)

            # Insert test data using SDK API
            test_data = [
                {'id': i, 'name': f'async_name_{i}', 'category': f'cat_{i % 5}', 'value': i * 10} for i in range(1, 101)
            ]
            await client.batch_insert(TestTable, test_data)

            yield client, test_table, TestTable

        finally:
            # Clean up using SDK API
            try:
                await client.drop_table(TestTable)
                await client.disconnect()
            except Exception as e:
                print(f"Cleanup failed: {e}")

    @pytest.mark.asyncio
    async def test_async_get_secondary_index_table_by_name(self, async_client_with_test_table):
        """Test async getting physical table name of a secondary index by its name"""
        client, test_table, TestTable = async_client_with_test_table

        # Test getting idx_async_name
        idx_name_table = await client.get_secondary_index_table_by_name(test_table, 'idx_async_name')
        assert idx_name_table is not None, "idx_async_name should exist"
        assert idx_name_table.startswith('__mo_index_secondary_')

        # Test getting idx_async_category
        idx_category_table = await client.get_secondary_index_table_by_name(test_table, 'idx_async_category')
        assert idx_category_table is not None, "idx_async_category should exist"
        assert idx_category_table.startswith('__mo_index_secondary_')

        # Test getting idx_async_value
        idx_value_table = await client.get_secondary_index_table_by_name(test_table, 'idx_async_value')
        assert idx_value_table is not None, "idx_async_value should exist"
        assert idx_value_table.startswith('__mo_index_secondary_')

        # Test non-existent index
        non_existent = await client.get_secondary_index_table_by_name(test_table, 'idx_non_existent')
        assert non_existent is None, "Non-existent index should return None"

    @pytest.mark.asyncio
    async def test_async_verify_table_index_counts(self, async_client_with_test_table):
        """Test async verify_table_index_counts when counts match"""
        client, test_table, TestTable = async_client_with_test_table

        # All indexes should have the same count as the main table
        count = await client.verify_table_index_counts(test_table)

        # Should return 100 (number of inserted rows)
        assert count == 100, f"Expected count 100, got {count}"

        # Verify count matches query result using SDK API
        actual_count = await client.query(TestTable).count()
        assert count == actual_count, "Verify result should match actual count"

    @pytest.mark.asyncio
    async def test_async_verify_table_without_indexes(self, async_client_with_test_table):
        """Test async verify_table_index_counts on table without secondary indexes"""
        client, test_table, TestTable = async_client_with_test_table

        # Define a simple model without secondary indexes
        Base = declarative_base()

        class SimpleTable(Base):
            __tablename__ = "test_async_simple_table"
            id = Column(Integer, primary_key=True)
            value = Column(Integer)

        # Create table using SDK API
        try:
            await client.drop_table(SimpleTable)
        except:
            pass

        await client.create_table(SimpleTable)

        # Insert data using SDK API
        test_data = [{'id': 1, 'value': 100}, {'id': 2, 'value': 200}]
        await client.batch_insert(SimpleTable, test_data)

        # Verification should succeed and return count
        count = await client.verify_table_index_counts("test_async_simple_table")
        assert count == 2, f"Expected count 2, got {count}"

        # Cleanup using SDK API
        await client.drop_table(SimpleTable)

    @pytest.mark.asyncio
    async def test_async_index_table_mapping(self, async_client_with_test_table):
        """Test that all indexes can be retrieved both ways (async)"""
        client, test_table, TestTable = async_client_with_test_table

        # Get all index tables
        all_index_tables = await client.get_secondary_index_tables(test_table)

        # Get each index by name
        index_names = ['idx_async_name', 'idx_async_category', 'idx_async_value']
        retrieved_tables = []

        for index_name in index_names:
            table = await client.get_secondary_index_table_by_name(test_table, index_name)
            assert table is not None, f"Index {index_name} should exist"
            retrieved_tables.append(table)

        # All retrieved tables should be in the all_index_tables list
        for table in retrieved_tables:
            assert table in all_index_tables, f"Retrieved table {table} should be in all index tables"

        # Both lists should have the same length
        assert len(retrieved_tables) == len(all_index_tables), "Number of retrieved tables should match total index tables"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
