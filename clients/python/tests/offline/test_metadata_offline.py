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
Offline tests for MatrixOne metadata operations
"""

import asyncio
import unittest
from unittest.mock import Mock, patch, AsyncMock
from matrixone.metadata import MetadataManager, AsyncMetadataManager


class TestMetadataManager(unittest.TestCase):
    """Test MetadataManager class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.metadata_manager = MetadataManager(self.mock_client)

    def test_init(self):
        """Test MetadataManager initialization"""
        self.assertEqual(self.metadata_manager.client, self.mock_client)

    def test_scan_basic(self):
        """Test basic scan functionality"""
        # Mock the execute method
        mock_result = Mock()
        mock_result.fetchall.return_value = [
            Mock(_mapping={'col_name': 'id', 'rows_cnt': 100, 'null_cnt': 0, 'origin_size': 1000})
        ]
        self.mock_client.execute.return_value = mock_result

        # Test basic scan
        result = self.metadata_manager.scan("test_db", "test_table")

        # Verify SQL was called correctly
        expected_sql = "SELECT * FROM metadata_scan('test_db.test_table', '*')"
        self.mock_client.execute.assert_called_once()
        call_args = self.mock_client.execute.call_args[0][0]
        self.assertIn("metadata_scan('test_db.test_table', '*')", str(call_args))

    def test_scan_with_index(self):
        """Test scan with index name"""
        mock_result = Mock()
        mock_result.fetchall.return_value = []
        self.mock_client.execute.return_value = mock_result

        # Test scan with index
        result = self.metadata_manager.scan("test_db", "test_table", indexname="idx_name")

        # Verify SQL includes index
        call_args = self.mock_client.execute.call_args[0][0]
        self.assertIn("metadata_scan('test_db.test_table.?idx_name', '*')", str(call_args))

    def test_scan_with_tombstone(self):
        """Test scan with tombstone flag"""
        mock_result = Mock()
        mock_result.fetchall.return_value = []
        self.mock_client.execute.return_value = mock_result

        # Test scan with tombstone
        result = self.metadata_manager.scan("test_db", "test_table", is_tombstone=True)

        # Verify SQL includes tombstone
        call_args = self.mock_client.execute.call_args[0][0]
        self.assertIn("metadata_scan('test_db.test_table.#', '*')", str(call_args))

    def test_scan_with_index_and_tombstone(self):
        """Test scan with both index and tombstone"""
        mock_result = Mock()
        mock_result.fetchall.return_value = []
        self.mock_client.execute.return_value = mock_result

        # Test scan with both index and tombstone
        result = self.metadata_manager.scan("test_db", "test_table", indexname="idx_name", is_tombstone=True)

        # Verify SQL includes both
        call_args = self.mock_client.execute.call_args[0][0]
        self.assertIn("metadata_scan('test_db.test_table.?idx_name.#', '*')", str(call_args))

    def test_format_size(self):
        """Test _format_size method"""
        # Test various size formats
        self.assertEqual(self.metadata_manager._format_size(0), "0 B")
        self.assertEqual(self.metadata_manager._format_size(1024), "1 KB")
        self.assertEqual(self.metadata_manager._format_size(1536), "1.50 KB")
        self.assertEqual(self.metadata_manager._format_size(1048576), "1 MB")
        self.assertEqual(self.metadata_manager._format_size(1073741824), "1 GB")
        self.assertEqual(self.metadata_manager._format_size(1073741824 * 2), "2 GB")

    def test_get_table_brief_stats(self):
        """Test get_table_brief_stats method"""
        # Mock the execute method
        mock_result = Mock()
        mock_result.fetchall.return_value = [
            Mock(
                _mapping={
                    'object_name': 'obj1',
                    'rows_cnt': 100,
                    'null_cnt': 5,
                    'origin_size': 1048576,  # 1 MB
                    'compress_size': 524288,  # 512 KB
                }
            ),
            Mock(
                _mapping={
                    'object_name': 'obj2',
                    'rows_cnt': 50,
                    'null_cnt': 2,
                    'origin_size': 2097152,  # 2 MB
                    'compress_size': 1048576,  # 1 MB
                }
            ),
        ]
        self.mock_client.execute.return_value = mock_result

        # Test basic brief stats
        stats = self.metadata_manager.get_table_brief_stats("test_db", "test_table")

        self.assertIn("test_table", stats)
        table_stats = stats["test_table"]
        self.assertEqual(table_stats["total_objects"], 2)
        self.assertEqual(table_stats["original_size"], "3 MB")
        self.assertEqual(table_stats["compress_size"], "1.50 MB")
        self.assertEqual(table_stats["row_cnt"], 150)
        self.assertEqual(table_stats["null_cnt"], 7)

    def test_get_table_brief_stats_with_tombstone(self):
        """Test get_table_brief_stats with tombstone"""
        # Mock multiple execute calls
        mock_results = [
            # Table results
            Mock(
                fetchall=Mock(
                    return_value=[
                        Mock(
                            _mapping={
                                'object_name': 'obj1',
                                'rows_cnt': 100,
                                'null_cnt': 5,
                                'origin_size': 1048576,
                                'compress_size': 524288,
                            }
                        )
                    ]
                )
            ),
            # Tombstone results
            Mock(
                fetchall=Mock(
                    return_value=[
                        Mock(
                            _mapping={
                                'object_name': 'tomb1',
                                'rows_cnt': 20,
                                'null_cnt': 1,
                                'origin_size': 512000,
                                'compress_size': 256000,
                            }
                        )
                    ]
                )
            ),
        ]
        self.mock_client.execute.side_effect = mock_results

        # Test with tombstone
        stats = self.metadata_manager.get_table_brief_stats("test_db", "test_table", include_tombstone=True)

        self.assertIn("test_table", stats)
        self.assertIn("tombstone", stats)
        self.assertEqual(stats["tombstone"]["row_cnt"], 20)

    def test_get_table_brief_stats_with_indexes(self):
        """Test get_table_brief_stats with indexes"""
        # Mock multiple execute calls
        mock_results = [
            # Table results
            Mock(
                fetchall=Mock(
                    return_value=[
                        Mock(
                            _mapping={
                                'object_name': 'obj1',
                                'rows_cnt': 100,
                                'null_cnt': 5,
                                'origin_size': 1048576,
                                'compress_size': 524288,
                            }
                        )
                    ]
                )
            ),
            # Index1 results
            Mock(
                fetchall=Mock(
                    return_value=[
                        Mock(
                            _mapping={
                                'object_name': 'idx1_obj1',
                                'rows_cnt': 100,
                                'null_cnt': 0,
                                'origin_size': 512000,
                                'compress_size': 256000,
                            }
                        )
                    ]
                )
            ),
            # Index2 results
            Mock(
                fetchall=Mock(
                    return_value=[
                        Mock(
                            _mapping={
                                'object_name': 'idx2_obj1',
                                'rows_cnt': 100,
                                'null_cnt': 0,
                                'origin_size': 256000,
                                'compress_size': 128000,
                            }
                        )
                    ]
                )
            ),
        ]
        self.mock_client.execute.side_effect = mock_results

        # Test with indexes
        stats = self.metadata_manager.get_table_brief_stats("test_db", "test_table", include_indexes=["idx1", "idx2"])

        self.assertIn("test_table", stats)
        self.assertIn("idx1", stats)
        self.assertIn("idx2", stats)
        self.assertEqual(stats["idx1"]["row_cnt"], 100)
        self.assertEqual(stats["idx2"]["row_cnt"], 100)

    def test_get_table_detail_stats(self):
        """Test get_table_detail_stats method"""
        # Mock the execute method
        mock_result = Mock()
        mock_result.fetchall.return_value = [
            Mock(
                _mapping={
                    'object_name': 'obj1',
                    'create_ts': '2023-01-01 10:00:00',
                    'delete_ts': None,
                    'rows_cnt': 100,
                    'null_cnt': 5,
                    'origin_size': 1048576,
                    'compress_size': 524288,
                }
            ),
            Mock(
                _mapping={
                    'object_name': 'obj2',
                    'create_ts': '2023-01-02 11:00:00',
                    'delete_ts': '2023-01-03 12:00:00',
                    'rows_cnt': 50,
                    'null_cnt': 2,
                    'origin_size': 2097152,
                    'compress_size': 1048576,
                }
            ),
        ]
        self.mock_client.execute.return_value = mock_result

        # Test basic detail stats
        stats = self.metadata_manager.get_table_detail_stats("test_db", "test_table")

        self.assertIn("test_table", stats)
        table_details = stats["test_table"]
        self.assertEqual(len(table_details), 2)

        # Check first object
        obj1 = table_details[0]
        self.assertEqual(obj1["object_name"], "obj1")
        self.assertEqual(obj1["create_ts"], "2023-01-01 10:00:00")
        self.assertEqual(obj1["delete_ts"], None)
        self.assertEqual(obj1["row_cnt"], 100)
        self.assertEqual(obj1["null_cnt"], 5)
        self.assertEqual(obj1["original_size"], "1 MB")
        self.assertEqual(obj1["compress_size"], "512 KB")

        # Check second object
        obj2 = table_details[1]
        self.assertEqual(obj2["object_name"], "obj2")
        self.assertEqual(obj2["delete_ts"], "2023-01-03 12:00:00")
        self.assertEqual(obj2["original_size"], "2 MB")
        self.assertEqual(obj2["compress_size"], "1 MB")


class TestSessionMetadataManager(unittest.TestCase):
    """Test MetadataManager with session executor"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_session = Mock()
        self.metadata_manager = MetadataManager(self.mock_client, executor=self.mock_session)

    def test_init(self):
        """Test MetadataManager with session initialization"""
        self.assertEqual(self.metadata_manager.client, self.mock_client)
        self.assertEqual(self.metadata_manager.executor, self.mock_session)
        self.assertEqual(self.metadata_manager._get_executor(), self.mock_session)

    def test_scan_uses_session(self):
        """Test that scan uses session executor"""
        mock_result = Mock()
        mock_result.fetchall.return_value = []
        self.mock_session.execute.return_value = mock_result

        result = self.metadata_manager.scan("test_db", "test_table")

        # Verify session.execute was called
        self.mock_session.execute.assert_called_once()

    def test_get_table_brief_stats_uses_session(self):
        """Test that get_table_brief_stats uses session executor"""
        mock_result = Mock()
        mock_result.fetchall.return_value = [
            Mock(
                _mapping={
                    'object_name': 'obj1',
                    'rows_cnt': 100,
                    'null_cnt': 5,
                    'origin_size': 1048576,
                    'compress_size': 524288,
                }
            )
        ]
        self.mock_session.execute.return_value = mock_result

        stats = self.metadata_manager.get_table_brief_stats("test_db", "test_table")

        # Verify session.execute was called
        self.mock_session.execute.assert_called_once()
        self.assertIn("test_table", stats)

    def test_get_table_detail_stats_uses_session(self):
        """Test that get_table_detail_stats uses session executor"""
        mock_result = Mock()
        mock_result.fetchall.return_value = [
            Mock(
                _mapping={
                    'object_name': 'obj1',
                    'create_ts': '2023-01-01 10:00:00',
                    'delete_ts': None,
                    'rows_cnt': 100,
                    'null_cnt': 5,
                    'origin_size': 1048576,
                    'compress_size': 524288,
                }
            )
        ]
        self.mock_session.execute.return_value = mock_result

        stats = self.metadata_manager.get_table_detail_stats("test_db", "test_table")

        # Verify session.execute was called
        self.mock_session.execute.assert_called_once()
        self.assertIn("test_table", stats)


class TestAsyncMetadataManager(unittest.TestCase):
    """Test AsyncMetadataManager class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = AsyncMock()
        self.metadata_manager = AsyncMetadataManager(self.mock_client)

    def test_init(self):
        """Test AsyncMetadataManager initialization"""
        self.assertEqual(self.metadata_manager.client, self.mock_client)

    def test_scan_basic(self):
        """Test basic async scan functionality"""

        async def _test():
            # Mock the execute method
            mock_result = Mock()
            mock_result.fetchall.return_value = [
                Mock(_mapping={'col_name': 'id', 'rows_cnt': 100, 'null_cnt': 0, 'origin_size': 1000})
            ]
            self.mock_client.execute.return_value = mock_result

            # Test basic scan
            result = await self.metadata_manager.scan("test_db", "test_table")

            # Verify SQL was called correctly
            self.mock_client.execute.assert_called_once()
            call_args = self.mock_client.execute.call_args[0][0]
            self.assertIn("metadata_scan('test_db.test_table', '*')", str(call_args))

        asyncio.run(_test())

    def test_get_table_brief_stats(self):
        """Test async get_table_brief_stats method"""

        async def _test():
            # Mock the execute method
            mock_result = Mock()
            mock_result.fetchall.return_value = [
                Mock(
                    _mapping={
                        'object_name': 'obj1',
                        'rows_cnt': 100,
                        'null_cnt': 5,
                        'origin_size': 1048576,
                        'compress_size': 524288,
                    }
                )
            ]
            self.mock_client.execute.return_value = mock_result

            stats = await self.metadata_manager.get_table_brief_stats("test_db", "test_table")

            self.assertIn("test_table", stats)
            table_stats = stats["test_table"]
            self.assertEqual(table_stats["total_objects"], 1)
            self.assertEqual(table_stats["original_size"], "1 MB")
            self.assertEqual(table_stats["compress_size"], "512 KB")

        asyncio.run(_test())

    def test_get_table_detail_stats(self):
        """Test async get_table_detail_stats method"""

        async def _test():
            # Mock the execute method
            mock_result = Mock()
            mock_result.fetchall.return_value = [
                Mock(
                    _mapping={
                        'object_name': 'obj1',
                        'create_ts': '2023-01-01 10:00:00',
                        'delete_ts': None,
                        'rows_cnt': 100,
                        'null_cnt': 5,
                        'origin_size': 1048576,
                        'compress_size': 524288,
                    }
                )
            ]
            self.mock_client.execute.return_value = mock_result

            stats = await self.metadata_manager.get_table_detail_stats("test_db", "test_table")

            self.assertIn("test_table", stats)
            table_details = stats["test_table"]
            self.assertEqual(len(table_details), 1)
            self.assertEqual(table_details[0]["object_name"], "obj1")

        asyncio.run(_test())


class TestAsyncSessionMetadataManager(unittest.TestCase):
    """Test AsyncMetadataManager with async session executor"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = AsyncMock()
        self.mock_session = AsyncMock()
        self.metadata_manager = AsyncMetadataManager(self.mock_client, executor=self.mock_session)

    def test_init(self):
        """Test AsyncMetadataManager with async session initialization"""
        self.assertEqual(self.metadata_manager.client, self.mock_client)
        self.assertEqual(self.metadata_manager.executor, self.mock_session)
        self.assertEqual(self.metadata_manager._get_executor(), self.mock_session)

    def test_scan_uses_session(self):
        """Test that async scan uses session executor"""

        async def _test():
            mock_result = Mock()
            mock_result.fetchall.return_value = []
            self.mock_session.execute.return_value = mock_result

            result = await self.metadata_manager.scan("test_db", "test_table")

            # Verify session.execute was called
            self.mock_session.execute.assert_called_once()

        asyncio.run(_test())

    def test_get_table_brief_stats_uses_session(self):
        """Test that async get_table_brief_stats uses session executor"""

        async def _test():
            mock_result = Mock()
            mock_result.fetchall.return_value = [
                Mock(
                    _mapping={
                        'object_name': 'obj1',
                        'rows_cnt': 100,
                        'null_cnt': 5,
                        'origin_size': 1048576,
                        'compress_size': 524288,
                    }
                )
            ]
            self.mock_session.execute.return_value = mock_result

            stats = await self.metadata_manager.get_table_brief_stats("test_db", "test_table")

            # Verify session.execute was called
            self.mock_session.execute.assert_called_once()
            self.assertIn("test_table", stats)

        asyncio.run(_test())

    def test_get_table_detail_stats_uses_session(self):
        """Test that async get_table_detail_stats uses session executor"""

        async def _test():
            mock_result = Mock()
            mock_result.fetchall.return_value = [
                Mock(
                    _mapping={
                        'object_name': 'obj1',
                        'create_ts': '2023-01-01 10:00:00',
                        'delete_ts': None,
                        'rows_cnt': 100,
                        'null_cnt': 5,
                        'origin_size': 1048576,
                        'compress_size': 524288,
                    }
                )
            ]
            self.mock_session.execute.return_value = mock_result

            stats = await self.metadata_manager.get_table_detail_stats("test_db", "test_table")

            # Verify session.execute was called
            self.mock_session.execute.assert_called_once()
            self.assertIn("test_table", stats)

        asyncio.run(_test())


class TestMetadataSQLConsistency(unittest.TestCase):
    """Test that sync/async/session generate identical SQL"""

    def test_scan_sql_consistency(self):
        """Test scan() SQL consistency across all contexts"""
        # Setup mocks
        mock_client = Mock()
        mock_session = Mock()
        mock_async_client = AsyncMock()
        mock_async_session = AsyncMock()

        mock_result = Mock()
        mock_result.fetchall.return_value = []
        mock_client.execute.return_value = mock_result
        mock_session.execute.return_value = mock_result
        mock_async_client.execute.return_value = mock_result
        mock_async_session.execute.return_value = mock_result

        # Create managers
        sync_manager = MetadataManager(mock_client)
        session_manager = MetadataManager(mock_client, executor=mock_session)
        async_manager = AsyncMetadataManager(mock_async_client)
        async_session_manager = AsyncMetadataManager(mock_async_client, executor=mock_async_session)

        # Execute scan
        sync_manager.scan("test_db", "test_table")
        session_manager.scan("test_db", "test_table")

        async def _test():
            await async_manager.scan("test_db", "test_table")
            await async_session_manager.scan("test_db", "test_table")

        asyncio.run(_test())

        # Extract SQL
        sync_sql = mock_client.execute.call_args[0][0]
        session_sql = mock_session.execute.call_args[0][0]
        async_sql = mock_async_client.execute.call_args[0][0]
        async_session_sql = mock_async_session.execute.call_args[0][0]

        # Verify all SQL is identical
        expected_sql = "SELECT * FROM metadata_scan('test_db.test_table', '*') g"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)
        self.assertEqual(async_sql, expected_sql)
        self.assertEqual(async_session_sql, expected_sql)

    def test_scan_with_index_sql_consistency(self):
        """Test scan with index SQL consistency"""
        mock_client = Mock()
        mock_session = Mock()
        mock_async_client = AsyncMock()
        mock_async_session = AsyncMock()

        mock_result = Mock()
        mock_result.fetchall.return_value = []
        mock_client.execute.return_value = mock_result
        mock_session.execute.return_value = mock_result
        mock_async_client.execute.return_value = mock_result
        mock_async_session.execute.return_value = mock_result

        sync_manager = MetadataManager(mock_client)
        session_manager = MetadataManager(mock_client, executor=mock_session)
        async_manager = AsyncMetadataManager(mock_async_client)
        async_session_manager = AsyncMetadataManager(mock_async_client, executor=mock_async_session)

        sync_manager.scan("test_db", "test_table", indexname="idx_test")
        session_manager.scan("test_db", "test_table", indexname="idx_test")

        async def _test():
            await async_manager.scan("test_db", "test_table", indexname="idx_test")
            await async_session_manager.scan("test_db", "test_table", indexname="idx_test")

        asyncio.run(_test())

        sync_sql = mock_client.execute.call_args[0][0]
        session_sql = mock_session.execute.call_args[0][0]
        async_sql = mock_async_client.execute.call_args[0][0]
        async_session_sql = mock_async_session.execute.call_args[0][0]

        expected_sql = "SELECT * FROM metadata_scan('test_db.test_table.?idx_test', '*') g"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)
        self.assertEqual(async_sql, expected_sql)
        self.assertEqual(async_session_sql, expected_sql)

    def test_scan_with_tombstone_sql_consistency(self):
        """Test scan with tombstone SQL consistency"""
        mock_client = Mock()
        mock_session = Mock()
        mock_async_client = AsyncMock()
        mock_async_session = AsyncMock()

        mock_result = Mock()
        mock_result.fetchall.return_value = []
        mock_client.execute.return_value = mock_result
        mock_session.execute.return_value = mock_result
        mock_async_client.execute.return_value = mock_result
        mock_async_session.execute.return_value = mock_result

        sync_manager = MetadataManager(mock_client)
        session_manager = MetadataManager(mock_client, executor=mock_session)
        async_manager = AsyncMetadataManager(mock_async_client)
        async_session_manager = AsyncMetadataManager(mock_async_client, executor=mock_async_session)

        sync_manager.scan("test_db", "test_table", is_tombstone=True)
        session_manager.scan("test_db", "test_table", is_tombstone=True)

        async def _test():
            await async_manager.scan("test_db", "test_table", is_tombstone=True)
            await async_session_manager.scan("test_db", "test_table", is_tombstone=True)

        asyncio.run(_test())

        sync_sql = mock_client.execute.call_args[0][0]
        session_sql = mock_session.execute.call_args[0][0]
        async_sql = mock_async_client.execute.call_args[0][0]
        async_session_sql = mock_async_session.execute.call_args[0][0]

        expected_sql = "SELECT * FROM metadata_scan('test_db.test_table.#', '*') g"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)
        self.assertEqual(async_sql, expected_sql)
        self.assertEqual(async_session_sql, expected_sql)

    def test_scan_with_distinct_sql_consistency(self):
        """Test scan with distinct_object_name SQL consistency"""
        mock_client = Mock()
        mock_session = Mock()
        mock_async_client = AsyncMock()
        mock_async_session = AsyncMock()

        mock_result = Mock()
        mock_result.fetchall.return_value = []
        mock_client.execute.return_value = mock_result
        mock_session.execute.return_value = mock_result
        mock_async_client.execute.return_value = mock_result
        mock_async_session.execute.return_value = mock_result

        sync_manager = MetadataManager(mock_client)
        session_manager = MetadataManager(mock_client, executor=mock_session)
        async_manager = AsyncMetadataManager(mock_async_client)
        async_session_manager = AsyncMetadataManager(mock_async_client, executor=mock_async_session)

        sync_manager.scan("test_db", "test_table", distinct_object_name=True)
        session_manager.scan("test_db", "test_table", distinct_object_name=True)

        async def _test():
            await async_manager.scan("test_db", "test_table", distinct_object_name=True)
            await async_session_manager.scan("test_db", "test_table", distinct_object_name=True)

        asyncio.run(_test())

        sync_sql = mock_client.execute.call_args[0][0]
        session_sql = mock_session.execute.call_args[0][0]
        async_sql = mock_async_client.execute.call_args[0][0]
        async_session_sql = mock_async_session.execute.call_args[0][0]

        expected_sql = "SELECT DISTINCT(object_name) as object_name, * FROM metadata_scan('test_db.test_table', '*') g"
        self.assertEqual(sync_sql, expected_sql)
        self.assertEqual(session_sql, expected_sql)
        self.assertEqual(async_sql, expected_sql)
        self.assertEqual(async_session_sql, expected_sql)


if __name__ == '__main__':
    unittest.main()
