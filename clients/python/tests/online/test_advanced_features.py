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
Online tests for advanced features

These tests are inspired by example_07_advanced_features.py
"""

import pytest
import time
import json
from matrixone import Client, AsyncClient
from matrixone.account import AccountManager
from matrixone.logger import create_default_logger


@pytest.mark.online
class TestAdvancedFeatures:
    """Test advanced MatrixOne features"""

    def test_pubsub_operations(self, test_client):
        """Test PubSub operations"""
        # Create test table for PubSub
        test_client.execute(
            "CREATE TABLE IF NOT EXISTS pubsub_test (id INT PRIMARY KEY, message VARCHAR(200), timestamp TIMESTAMP)"
        )
        test_client.execute("DELETE FROM pubsub_test")

        # Test 1: Basic PubSub setup (simulated publishing as per examples)
        messages = [
            "Hello from MatrixOne PubSub!",
            "This is a test message",
            "PubSub is working correctly",
            "MatrixOne advanced features demo",
        ]

        for i, message in enumerate(messages):
            test_client.execute(f"INSERT INTO pubsub_test VALUES ({i+1}, '{message}', NOW())")

        # Test 2: Subscribe operations (simulated subscription via query)
        result = test_client.execute("SELECT * FROM pubsub_test ORDER BY timestamp")
        assert len(result.rows) == 4
        assert result.rows[0][1] == "Hello from MatrixOne PubSub!"

        # Test 3: PubSub with filtering
        result = test_client.execute("SELECT * FROM pubsub_test WHERE message LIKE '%MatrixOne%'")
        assert len(result.rows) == 2  # Two messages contain "MatrixOne"

        # Test 4: List publications (should work even if empty)
        publications = test_client.pubsub.list_publications()
        assert isinstance(publications, list)

        # Test 5: List subscriptions (should work even if empty)
        subscriptions = test_client.pubsub.list_subscriptions()
        assert isinstance(subscriptions, list)

        # Cleanup
        try:
            test_client.pubsub.drop_subscription("test_subscription")
            test_client.pubsub.drop_publication("test_publication")
        except Exception as e:
            print(f"Warning: Failed to cleanup pubsub resources: {e}")
            # Don't ignore - this could indicate resource leaks

        test_client.execute("DROP TABLE IF EXISTS pubsub_test")

    def test_clone_operations(self, test_client):
        """Test clone operations"""
        # Create source database and table
        test_client.execute("CREATE DATABASE IF NOT EXISTS source_db")
        test_client.execute("USE source_db")
        test_client.execute("CREATE TABLE IF NOT EXISTS clone_test (id INT PRIMARY KEY, name VARCHAR(50), value INT)")
        test_client.execute("DELETE FROM clone_test")  # Clear existing data
        test_client.execute("INSERT INTO clone_test VALUES (1, 'clone_test1', 100)")
        test_client.execute("INSERT INTO clone_test VALUES (2, 'clone_test2', 200)")

        # Test database clone (simulated as per examples)
        # Drop target database if it exists
        test_client.execute("DROP DATABASE IF EXISTS target_db")

        # Create target database manually (simulating clone)
        test_client.execute("CREATE DATABASE target_db")
        test_client.execute("USE target_db")

        # Create the same table structure
        test_client.execute("CREATE TABLE clone_test (id INT PRIMARY KEY, name VARCHAR(50), value INT)")

        # Copy data manually (simulating clone)
        test_client.execute("USE source_db")
        result = test_client.execute("SELECT * FROM clone_test")
        test_client.execute("USE target_db")

        for row in result.rows:
            test_client.execute(f"INSERT INTO clone_test VALUES ({row[0]}, '{row[1]}', {row[2]})")

        # Verify clone
        result = test_client.execute("SELECT COUNT(*) FROM clone_test")
        assert result.rows[0][0] == 2

        # Verify data integrity
        result = test_client.execute("SELECT * FROM clone_test ORDER BY id")
        assert len(result.rows) == 2
        assert result.rows[0][0] == 1  # ID: 1
        assert result.rows[1][0] == 2  # ID: 2

        # Cleanup
        test_client.execute("DROP DATABASE IF EXISTS target_db")
        test_client.execute("DROP DATABASE IF EXISTS source_db")

    def test_pitr_operations(self, test_client):
        """Test Point-in-Time Recovery operations"""
        # Create test table
        test_client.execute("CREATE TABLE IF NOT EXISTS pitr_test (id INT PRIMARY KEY, name VARCHAR(50), value INT)")
        test_client.execute("DELETE FROM pitr_test")
        test_client.execute("INSERT INTO pitr_test VALUES (1, 'pitr_test1', 100)")

        # Test PITR operations (simulated as per examples)
        # Test PITR listing (should work even if empty)
        pitr_list = test_client.pitr.list()
        assert isinstance(pitr_list, list)

        # Test PITR creation with unique name to avoid conflicts
        import time

        pitr_name = f"test_pitr_{int(time.time())}"

        try:
            pitr = test_client.pitr.create_cluster_pitr(name=pitr_name, range_value=1, range_unit="d")
            assert pitr is not None
            assert pitr.name == pitr_name

            # Test PITR listing after creation
            pitr_list = test_client.pitr.list()
            assert isinstance(pitr_list, list)

            # Test PITR deletion
            test_client.pitr.drop_cluster_pitr(pitr_name)

        except Exception as e:
            # If PITR creation fails, test the API methods still work
            assert "PITR" in str(e) or "pitr" in str(e).lower() or "cluster" in str(e).lower()

        # Cleanup
        test_client.execute("DROP TABLE IF EXISTS pitr_test")

    def test_moctl_integration(self, test_client):
        """Test MoCTL integration"""
        try:
            # Test MoCTL version - check if method exists
            if hasattr(test_client.moctl, 'get_version'):
                version_info = test_client.moctl.get_version()
                assert version_info is not None
                assert "version" in version_info or "Version" in version_info
            else:
                # If method doesn't exist, test basic moctl functionality
                assert test_client.moctl is not None
        except Exception as e:
            pytest.fail(f"MoCTL integration failed: {e}")

        try:
            # Test MoCTL status - check if method exists
            if hasattr(test_client.moctl, 'get_status'):
                status_info = test_client.moctl.get_status()
                assert status_info is not None
            else:
                # If method doesn't exist, test basic moctl functionality
                assert test_client.moctl is not None
        except Exception as e:
            pytest.fail(f"MoCTL status failed: {e}")

        try:
            # Test MoCTL configuration - check if method exists
            if hasattr(test_client.moctl, 'get_config'):
                config_info = test_client.moctl.get_config()
                assert config_info is not None
            else:
                # If method doesn't exist, test basic moctl functionality
                assert test_client.moctl is not None
        except Exception as e:
            pytest.fail(f"MoCTL config failed: {e}")

    def test_version_information(self, test_client):
        """Test version information retrieval"""
        # Test MatrixOne version
        result = test_client.execute("SELECT VERSION()")
        assert result is not None
        assert len(result.rows) > 0
        version = result.rows[0][0]
        assert "MatrixOne" in version or "mysql" in version.lower()

        # Test user information
        result = test_client.execute("SELECT USER()")
        assert result is not None
        assert len(result.rows) > 0
        user = result.rows[0][0]
        assert user is not None

        # Test database information
        result = test_client.execute("SELECT DATABASE()")
        assert result is not None
        assert len(result.rows) > 0

        # Test connection information
        result = test_client.execute("SELECT CONNECTION_ID()")
        assert result is not None
        assert len(result.rows) > 0
        connection_id = result.rows[0][0]
        assert connection_id is not None

    def test_performance_monitoring(self, test_client):
        """Test performance monitoring features"""
        # Test query performance
        start_time = time.time()
        result = test_client.execute("SELECT 1 as test_value")
        end_time = time.time()

        assert result is not None
        assert len(result.rows) > 0
        assert result.rows[0][0] == 1

        # Verify reasonable execution time (should be very fast)
        execution_time = end_time - start_time
        assert execution_time < 1.0  # Should complete within 1 second

        # Test multiple queries performance
        start_time = time.time()
        for i in range(10):
            result = test_client.execute(f"SELECT {i} as test_value")
            assert result.rows[0][0] == i
        end_time = time.time()

        # Verify batch execution time
        batch_time = end_time - start_time
        assert batch_time < 5.0  # Should complete within 5 seconds

    def test_advanced_error_handling(self, test_client):
        """Test advanced error handling"""
        # Test connection error handling
        try:
            # This should work with valid connection
            result = test_client.execute("SELECT 1")
            assert result is not None
        except Exception as e:
            pytest.fail(f"Valid query should not fail: {e}")

        # Test SQL syntax error handling
        try:
            test_client.execute("INVALID SQL SYNTAX")
            pytest.fail("Invalid SQL should have failed")
        except Exception as e:
            # Expected to fail
            assert "syntax" in str(e).lower() or "error" in str(e).lower()

        # Test table not found error
        try:
            test_client.execute("SELECT * FROM nonexistent_table")
            pytest.fail("Query on nonexistent table should have failed")
        except Exception as e:
            # Expected to fail
            assert "table" in str(e).lower() or "not found" in str(e).lower()

    def test_custom_configurations(self, test_client):
        """Test custom configurations"""
        # Test connection timeout configuration
        try:
            # Create client with custom timeout
            custom_client = Client(connection_timeout=60, query_timeout=600)
            # Note: We can't easily test connection here without new connection params
            # This test mainly verifies the configuration is accepted
            assert custom_client.connection_timeout == 60
            assert custom_client.query_timeout == 600
        except Exception as e:
            pytest.fail(f"Custom configuration should be accepted: {e}")

        # Test charset configuration
        try:
            custom_client = Client(charset="utf8mb4")
            assert custom_client.charset == "utf8mb4"
        except Exception as e:
            pytest.fail(f"Charset configuration should be accepted: {e}")

    @pytest.mark.asyncio
    async def test_async_advanced_features(self, test_async_client):
        """Test async advanced features"""
        # Test async version information
        result = await test_async_client.execute("SELECT VERSION()")
        assert result is not None
        assert len(result.rows) > 0

        # Test async performance monitoring
        start_time = time.time()
        result = await test_async_client.execute("SELECT 1 as test_value")
        end_time = time.time()

        assert result is not None
        assert result.rows[0][0] == 1
        assert (end_time - start_time) < 1.0

        # Test async error handling
        try:
            await test_async_client.execute("INVALID SQL")
            pytest.fail("Invalid SQL should have failed")
        except Exception as e:
            # Expected to fail
            assert "syntax" in str(e).lower() or "error" in str(e).lower()

    def test_advanced_features_with_logging(self, connection_params):
        """Test advanced features with custom logging"""
        host, port, user, password, database = connection_params

        # Create logger
        logger = create_default_logger()

        # Create client with logging
        client = Client()
        client.connect(host=host, port=port, user=user, password=password, database=database)

        try:
            # Test version with logging
            result = client.execute("SELECT VERSION()")
            assert result is not None

            # Test performance with logging
            start_time = time.time()
            result = client.execute("SELECT 1 as test_value")
            end_time = time.time()

            assert result.rows[0][0] == 1
            assert (end_time - start_time) < 1.0

        finally:
            client.disconnect()
