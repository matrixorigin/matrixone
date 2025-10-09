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
from matrixone.metadata import MetadataManager, TransactionMetadataManager
from matrixone.async_metadata_manager import AsyncMetadataManager, AsyncTransactionMetadataManager


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


class TestTransactionMetadataManager(unittest.TestCase):
    """Test TransactionMetadataManager class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_transaction = Mock()
        self.metadata_manager = TransactionMetadataManager(self.mock_client, self.mock_transaction)

    def test_init(self):
        """Test TransactionMetadataManager initialization"""
        self.assertEqual(self.metadata_manager.client, self.mock_client)
        self.assertEqual(self.metadata_manager.transaction_wrapper, self.mock_transaction)

    def test_scan_uses_transaction(self):
        """Test that scan uses transaction wrapper"""
        mock_result = Mock()
        mock_result.fetchall.return_value = []
        self.mock_transaction.execute.return_value = mock_result

        result = self.metadata_manager.scan("test_db", "test_table")

        # Verify transaction.execute was called
        self.mock_transaction.execute.assert_called_once()

    def test_get_table_brief_stats_uses_transaction(self):
        """Test that get_table_brief_stats uses transaction wrapper"""
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
        self.mock_transaction.execute.return_value = mock_result

        stats = self.metadata_manager.get_table_brief_stats("test_db", "test_table")

        # Verify transaction.execute was called
        self.mock_transaction.execute.assert_called_once()
        self.assertIn("test_table", stats)

    def test_get_table_detail_stats_uses_transaction(self):
        """Test that get_table_detail_stats uses transaction wrapper"""
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
        self.mock_transaction.execute.return_value = mock_result

        stats = self.metadata_manager.get_table_detail_stats("test_db", "test_table")

        # Verify transaction.execute was called
        self.mock_transaction.execute.assert_called_once()
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


class TestAsyncTransactionMetadataManager(unittest.TestCase):
    """Test AsyncTransactionMetadataManager class"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = AsyncMock()
        self.mock_transaction = AsyncMock()
        self.metadata_manager = AsyncTransactionMetadataManager(self.mock_client, self.mock_transaction)

    def test_init(self):
        """Test AsyncTransactionMetadataManager initialization"""
        self.assertEqual(self.metadata_manager.client, self.mock_client)
        self.assertEqual(self.metadata_manager.transaction_wrapper, self.mock_transaction)

    def test_scan_uses_transaction(self):
        """Test that async scan uses transaction wrapper"""

        async def _test():
            mock_result = Mock()
            mock_result.fetchall.return_value = []
            self.mock_transaction.execute.return_value = mock_result

            result = await self.metadata_manager.scan("test_db", "test_table")

            # Verify transaction.execute was called
            self.mock_transaction.execute.assert_called_once()

        asyncio.run(_test())

    def test_get_table_brief_stats_uses_transaction(self):
        """Test that async get_table_brief_stats uses transaction wrapper"""

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
            self.mock_transaction.execute.return_value = mock_result

            stats = await self.metadata_manager.get_table_brief_stats("test_db", "test_table")

            # Verify transaction.execute was called
            self.mock_transaction.execute.assert_called_once()
            self.assertIn("test_table", stats)

        asyncio.run(_test())

    def test_get_table_detail_stats_uses_transaction(self):
        """Test that async get_table_detail_stats uses transaction wrapper"""

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
            self.mock_transaction.execute.return_value = mock_result

            stats = await self.metadata_manager.get_table_detail_stats("test_db", "test_table")

            # Verify transaction.execute was called
            self.mock_transaction.execute.assert_called_once()
            self.assertIn("test_table", stats)

        asyncio.run(_test())


if __name__ == '__main__':
    unittest.main()
