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
Offline tests for VectorManager and AsyncVectorManager.

Tests:
1. Executor pattern initialization
2. SQL generation consistency across Client/Session/AsyncClient/AsyncSession
3. All vector operations generate expected SQL
"""

import unittest
import pytest
from unittest.mock import Mock, AsyncMock
from matrixone.vector_manager import VectorManager, AsyncVectorManager, _VectorManagerBase
from matrixone.sqlalchemy_ext import VectorOpType


class TestVectorManagerExecutor(unittest.TestCase):
    """Test VectorManager executor pattern"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_session = Mock()

    def test_vector_manager_init_with_client(self):
        """Test VectorManager initialization with client (default executor)"""
        manager = VectorManager(self.mock_client)
        self.assertEqual(manager.client, self.mock_client)
        self.assertEqual(manager.executor, self.mock_client)

    def test_vector_manager_init_with_session(self):
        """Test VectorManager initialization with session as executor"""
        manager = VectorManager(self.mock_client, executor=self.mock_session)
        self.assertEqual(manager.client, self.mock_client)
        self.assertEqual(manager.executor, self.mock_session)


class TestAsyncVectorManagerExecutor(unittest.TestCase):
    """Test AsyncVectorManager executor pattern"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_session = Mock()

    def test_async_vector_manager_init_with_client(self):
        """Test AsyncVectorManager initialization with client"""
        manager = AsyncVectorManager(self.mock_client)
        self.assertEqual(manager.client, self.mock_client)
        self.assertEqual(manager.executor, self.mock_client)

    def test_async_vector_manager_init_with_session(self):
        """Test AsyncVectorManager initialization with session"""
        manager = AsyncVectorManager(self.mock_client, executor=self.mock_session)
        self.assertEqual(manager.client, self.mock_client)
        self.assertEqual(manager.executor, self.mock_session)


if __name__ == '__main__':
    unittest.main()


class TestVectorSQLGeneration(unittest.TestCase):
    """Test SQL generation consistency across sync/async and client/session contexts."""

    def setUp(self):
        """Setup mock client and managers for each test."""
        # Mock client
        self.mock_client = Mock()
        self.mock_client.database = 'test_db'

        # Mock session
        self.mock_session = Mock()

        # Create managers
        self.client_manager = VectorManager(self.mock_client)  # Client mode
        self.session_manager = VectorManager(self.mock_client, executor=self.mock_session)  # Session mode

    def test_create_ivf_sql_consistency(self):
        """Test that create_ivf generates correct and consistent SQL."""
        table_name = "test_table"
        index_name = "idx_test"
        column_name = "embedding"
        lists = 100

        # Get expected SQL from base class
        setup_sqls, create_sql = _VectorManagerBase._build_create_ivf_sql(
            table_name, index_name, column_name, lists, VectorOpType.VECTOR_L2_OPS
        )

        # Test Client mode
        self.client_manager.create_ivf(table_name, index_name, column_name, lists=lists)

        # Verify calls
        calls = self.mock_client.execute.call_args_list
        self.assertEqual(len(calls), 3)  # 2 setup + 1 create
        self.assertEqual(calls[0][0][0], setup_sqls[0])
        self.assertEqual(calls[1][0][0], setup_sqls[1])
        self.assertEqual(calls[2][0][0], create_sql)

        # Test Session mode
        self.mock_session.reset_mock()
        self.session_manager.create_ivf(table_name, index_name, column_name, lists=lists)

        # Verify session generates same SQL
        session_calls = self.mock_session.execute.call_args_list
        self.assertEqual(len(session_calls), 3)
        self.assertEqual(session_calls[0][0][0], setup_sqls[0])
        self.assertEqual(session_calls[1][0][0], setup_sqls[1])
        self.assertEqual(session_calls[2][0][0], create_sql)

    def test_similarity_search_sql_consistency(self):
        """Test similarity_search generates correct SQL for all distance types."""
        table_name = "test_table"
        vector_column = "embedding"
        query_vector = [0.1, 0.2, 0.3]
        limit = 10

        # Mock return value
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))
        self.mock_client.execute.return_value = mock_result
        self.mock_session.execute.return_value = mock_result

        # Test all distance types
        for distance_type in ["l2", "cosine", "inner_product"]:
            expected_sql = _VectorManagerBase._build_similarity_search_sql(
                table_name, vector_column, query_vector, limit, None, None, distance_type
            )

            # Verify expected SQL structure
            self.assertIn("SELECT *", expected_sql)
            if distance_type == "l2":
                self.assertIn("l2_distance", expected_sql)
            elif distance_type == "cosine":
                self.assertIn("cosine_distance", expected_sql)
            elif distance_type == "inner_product":
                self.assertIn("inner_product", expected_sql)

            # Test Client mode
            self.mock_client.reset_mock()
            self.mock_client.execute.return_value = mock_result
            self.client_manager.similarity_search(
                table_name, vector_column, query_vector, limit=limit, distance_type=distance_type
            )
            self.assertEqual(self.mock_client.execute.call_args[0][0], expected_sql)

            # Test Session mode
            self.mock_session.reset_mock()
            self.mock_session.execute.return_value = mock_result
            self.session_manager.similarity_search(
                table_name, vector_column, query_vector, limit=limit, distance_type=distance_type
            )
            self.assertEqual(self.mock_session.execute.call_args[0][0], expected_sql)

    def test_insert_sql_consistency(self):
        """Test that insert generates correct SQL."""
        table_name = "test_table"
        data = {"id": 1, "title": "test", "embedding": [0.1, 0.2, 0.3]}

        # Get expected SQL
        expected_sql = _VectorManagerBase._build_insert_sql(table_name, data)
        self.assertIn("INSERT INTO test_table", expected_sql)
        self.assertIn("'[0.1,0.2,0.3]'", expected_sql)

        # Test Client mode
        self.client_manager.insert(table_name, data)
        self.assertEqual(self.mock_client.execute.call_args[0][0], expected_sql)

        # Test Session mode
        self.mock_session.reset_mock()
        self.session_manager.insert(table_name, data)
        self.assertEqual(self.mock_session.execute.call_args[0][0], expected_sql)

    def test_enable_disable_ivf_sql(self):
        """Test enable/disable IVF SQL generation."""
        probe_limit = 5

        # Enable
        expected_enable = _VectorManagerBase._build_enable_ivf_sql(probe_limit)
        self.assertEqual(expected_enable, ["SET experimental_ivf_index = 1", "SET probe_limit = 5"])

        self.client_manager.enable_ivf(probe_limit=probe_limit)
        calls = self.mock_client.execute.call_args_list
        self.assertEqual(calls[0][0][0], expected_enable[0])
        self.assertEqual(calls[1][0][0], expected_enable[1])

        # Disable
        expected_disable = _VectorManagerBase._build_disable_ivf_sql()
        self.assertEqual(expected_disable, "SET experimental_ivf_index = 0")

        self.mock_client.reset_mock()
        self.client_manager.disable_ivf()
        self.assertEqual(self.mock_client.execute.call_args[0][0], expected_disable)

    def test_get_ivf_stats_sql_builders(self):
        """Test get_ivf_stats SQL generation."""
        table_name = "test_table"
        column_name = "embedding"
        database = "test_db"

        # Test column inference SQL
        column_sql = _VectorManagerBase._build_column_inference_sql(table_name, database)
        self.assertIn("information_schema.columns", column_sql)
        self.assertIn(f"table_name = '{table_name}'", column_sql)

        # Test index tables SQL
        index_sql = _VectorManagerBase._build_index_tables_sql(table_name, column_name, database)
        self.assertIn("mo_catalog", index_sql)
        self.assertIn(f"column_name = '{column_name}'", index_sql)
        self.assertIn("algo='ivfflat'", index_sql)

        # Test distribution SQL
        entries_table = "test_entries"
        dist_sql = _VectorManagerBase._build_distribution_sql(database, entries_table)
        self.assertIn("COUNT(*)", dist_sql)
        self.assertIn("__mo_index_centroid_fk_id", dist_sql)
        self.assertIn(f"`{database}`.`{entries_table}`", dist_sql)


class TestAsyncVectorSQLGeneration:
    """Test async vector manager SQL generation."""

    def setup_method(self):
        """Setup mock async client and managers."""
        self.mock_async_client = Mock()
        self.mock_async_client.database = 'test_db'

        self.mock_async_session = Mock()

        self.async_client_manager = AsyncVectorManager(self.mock_async_client)
        self.async_session_manager = AsyncVectorManager(self.mock_async_client, executor=self.mock_async_session)

    @pytest.mark.asyncio
    async def test_async_create_ivf_sql_consistency(self):
        """Test async create_ivf generates same SQL as sync."""
        table_name = "test_table"
        index_name = "idx_test"
        column_name = "embedding"

        # Get expected SQL
        setup_sqls, create_sql = _VectorManagerBase._build_create_ivf_sql(
            table_name, index_name, column_name, 100, VectorOpType.VECTOR_L2_OPS
        )

        # Mock async execute
        self.mock_async_client.execute = AsyncMock()

        await self.async_client_manager.create_ivf(table_name, index_name, column_name)

        calls = self.mock_async_client.execute.call_args_list
        assert len(calls) == 3
        assert calls[0][0][0] == setup_sqls[0]
        assert calls[1][0][0] == setup_sqls[1]
        assert calls[2][0][0] == create_sql

    @pytest.mark.asyncio
    async def test_async_similarity_search_sql_consistency(self):
        """Test async similarity_search generates same SQL as sync."""
        table_name = "test_table"
        vector_column = "embedding"
        query_vector = [0.1, 0.2, 0.3]

        expected_sql = _VectorManagerBase._build_similarity_search_sql(
            table_name, vector_column, query_vector, 10, None, None, "l2"
        )

        # Mock async execute
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))
        self.mock_async_client.execute = AsyncMock(return_value=mock_result)

        await self.async_client_manager.similarity_search(table_name, vector_column, query_vector)

        actual_sql = self.mock_async_client.execute.call_args[0][0]
        assert actual_sql == expected_sql


class TestVectorSQLConsistency(unittest.TestCase):
    """Test that SQL is consistent across all execution contexts."""

    def test_all_contexts_generate_identical_sql(self):
        """Verify Client, Session, AsyncClient, AsyncSession all generate the same SQL."""
        table_name = "test_table"
        column_name = "embedding"
        query_vector = [0.1, 0.2, 0.3]

        # Build expected SQL once
        expected_insert_sql = _VectorManagerBase._build_insert_sql(table_name, {"id": 1, "embedding": query_vector})
        expected_search_sql = _VectorManagerBase._build_similarity_search_sql(
            table_name, column_name, query_vector, 10, None, None, "l2"
        )

        # Test sync contexts
        sync_client = Mock()
        sync_session = Mock()

        vm_client = VectorManager(sync_client)
        vm_session = VectorManager(sync_client, executor=sync_session)

        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))
        sync_client.execute.return_value = mock_result
        sync_session.execute.return_value = mock_result

        # Insert
        vm_client.insert(table_name, {"id": 1, "embedding": query_vector})
        vm_session.insert(table_name, {"id": 1, "embedding": query_vector})

        self.assertEqual(sync_client.execute.call_args[0][0], expected_insert_sql)
        self.assertEqual(sync_session.execute.call_args[0][0], expected_insert_sql)

        # Search
        vm_client.similarity_search(table_name, column_name, query_vector)
        vm_session.similarity_search(table_name, column_name, query_vector)

        client_search_sql = [call[0][0] for call in sync_client.execute.call_args_list if "SELECT" in call[0][0]][0]
        session_search_sql = [call[0][0] for call in sync_session.execute.call_args_list if "SELECT" in call[0][0]][0]

        self.assertEqual(client_search_sql, expected_search_sql)
        self.assertEqual(session_search_sql, expected_search_sql)


if __name__ == '__main__':
    unittest.main()
