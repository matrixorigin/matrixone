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
Online tests for logger integration

These tests are inspired by example_09_logger_integration.py
"""

import pytest
import logging
from matrixone import Client, AsyncClient
from matrixone.logger import MatrixOneLogger, create_default_logger, create_custom_logger


@pytest.mark.online
class TestLoggerIntegration:
    """Test logger integration functionality"""

    def test_default_logger(self, test_client):
        """Test default logger configuration"""
        # Test basic operations with default logger
        result = test_client.execute("SELECT 1 as test_value")
        assert result is not None
        assert result.rows[0][0] == 1

        result = test_client.execute("SELECT VERSION() as version")
        assert result is not None
        assert len(result.rows) > 0

        result = test_client.execute("SHOW DATABASES")
        assert result is not None
        assert len(result.rows) > 0

    def test_custom_logger(self, connection_params):
        """Test custom logger integration"""
        host, port, user, password, database = connection_params

        # Create custom logger
        custom_logger_instance = logging.getLogger("test_custom_logger")
        custom_logger = create_custom_logger(logger=custom_logger_instance, sql_log_mode="auto")

        # Create client with custom logger
        client = Client(logger=custom_logger)
        client.connect(host=host, port=port, user=user, password=password, database=database)

        try:
            # Test operations with custom logger
            result = client.execute("SELECT 1 as test_value")
            assert result is not None
            assert result.rows[0][0] == 1

            result = client.execute("SELECT USER() as user_info")
            assert result is not None
            assert len(result.rows) > 0

        finally:
            client.disconnect()

    def test_performance_logging(self, connection_params):
        """Test performance logging"""
        host, port, user, password, database = connection_params

        # Create logger with SQL logging enabled
        logger = create_default_logger(sql_log_mode="auto")

        # Create client with SQL logging
        client = Client(logger=logger)
        client.connect(host=host, port=port, user=user, password=password, database=database)

        try:
            # Execute queries to test performance logging
            result = client.execute("SELECT 1 as test_value")
            assert result is not None

            # Execute multiple queries to see performance differences
            for i in range(5):
                result = client.execute(f"SELECT {i} as test_value")
                assert result.rows[0][0] == i

        finally:
            client.disconnect()

    def test_sql_logging(self, connection_params):
        """Test SQL query logging"""
        host, port, user, password, database = connection_params

        # Create logger with full SQL logging enabled
        logger = create_default_logger(sql_log_mode="full")

        # Create client with full SQL logging
        client = Client(logger=logger)
        client.connect(host=host, port=port, user=user, password=password, database=database)

        try:
            # Execute various SQL queries to test logging
            client.execute("SELECT 1 as test_value")
            client.execute("SELECT VERSION() as version")
            client.execute("SHOW DATABASES")
            client.execute("SHOW TABLES")

            # Test with parameters (use SQLAlchemy parameter syntax)
            client.execute("SELECT :param as param_value", {"param": 42})

        finally:
            client.disconnect()

    def test_structured_logging(self, connection_params):
        """Test structured logging"""
        host, port, user, password, database = connection_params

        # Create structured logger
        structured_logger_instance = logging.getLogger("structured_logger")
        logger = create_custom_logger(
            logger=structured_logger_instance,
            sql_log_mode="auto",
        )

        # Create client with structured logger
        client = Client(logger=logger)
        client.connect(host=host, port=port, user=user, password=password, database=database)

        try:
            # Test structured logging with various operations
            result = client.execute("SELECT 1 as test_value")
            assert result is not None

            # Test error logging
            try:
                client.execute("INVALID SQL")
            except Exception as e:
                # Expected to fail, verify it's a SQL error
                assert "syntax" in str(e).lower() or "error" in str(e).lower()
                # The error should have been logged by the client

        finally:
            client.disconnect()

    def test_error_tracking(self, connection_params):
        """Test error tracking and logging"""
        host, port, user, password, database = connection_params

        # Create logger for error tracking
        logger = create_default_logger(sql_log_mode="auto")

        # Create client with error tracking
        client = Client(logger=logger)
        client.connect(host=host, port=port, user=user, password=password, database=database)

        try:
            # Test successful operations
            result = client.execute("SELECT 1 as test_value")
            assert result is not None

            # Test error scenarios that should be logged
            try:
                client.execute("SELECT * FROM nonexistent_table")
            except Exception as e:
                # Expected to fail, verify it's a table not found error
                assert "table" in str(e).lower() or "not found" in str(e).lower()
                # The error should have been logged by the client

            try:
                client.execute("INVALID SQL SYNTAX")
            except Exception as e:
                # Expected to fail, verify it's a syntax error
                assert "syntax" in str(e).lower() or "error" in str(e).lower()
                # The error should have been logged by the client

        finally:
            client.disconnect()

    def test_logger_with_transactions(self, connection_params):
        """Test logger with transaction operations"""
        host, port, user, password, database = connection_params

        # Create logger
        logger = create_default_logger(sql_log_mode="auto")

        # Create client with logging
        client = Client(logger=logger)
        client.connect(host=host, port=port, user=user, password=password, database=database)

        try:
            # Test transaction logging
            with client.transaction():
                client.execute("SELECT 1 as test_value")
                client.execute("SELECT 2 as test_value")

            # Test rollback logging
            try:
                with client.transaction():
                    client.execute("SELECT 1 as test_value")
                    client.execute("INVALID SQL")  # This should cause rollback
            except Exception as e:
                # Expected to fail and rollback, verify it's a SQL error
                assert "syntax" in str(e).lower() or "error" in str(e).lower()
                # The rollback should have been logged by the client

        finally:
            client.disconnect()

    @pytest.mark.asyncio
    async def test_async_logger_integration(self, connection_params):
        """Test async logger integration"""
        host, port, user, password, database = connection_params

        # Create logger
        logger = create_default_logger(sql_log_mode="auto")

        # Create async client with logging
        client = AsyncClient(logger=logger)
        await client.connect(host=host, port=port, user=user, password=password, database=database)

        try:
            # Test async operations with logging
            result = await client.execute("SELECT 1 as test_value")
            assert result is not None
            assert result.rows[0][0] == 1

            result = await client.execute("SELECT VERSION() as version")
            assert result is not None

            # Test async transaction logging
            async with client.transaction():
                await client.execute("SELECT 1 as test_value")
                await client.execute("SELECT 2 as test_value")

        finally:
            await client.disconnect()

    def test_logger_configuration_options(self, connection_params):
        """Test various logger configuration options"""
        host, port, user, password, database = connection_params

        # Test different log levels
        for level in [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]:
            logger_instance = logging.getLogger(f"test_logger_{level}")
            logger = create_custom_logger(logger=logger_instance, sql_log_mode="auto")

            client = Client(logger=logger)
            client.connect(host=host, port=port, user=user, password=password, database=database)

            try:
                result = client.execute("SELECT 1 as test_value")
                assert result is not None
            finally:
                client.disconnect()

    def test_logger_with_different_formats(self, connection_params):
        """Test logger with different format strings"""
        host, port, user, password, database = connection_params

        # Test different format strings
        formats = [
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            '%(levelname)s:%(name)s:%(message)s',
            '%(asctime)s | %(levelname)s | %(message)s',
            '%(name)s - %(levelname)s - %(message)s',
        ]

        for i, format_string in enumerate(formats):
            logger_instance = logging.getLogger(f"format_test_{i}")
            logger = create_custom_logger(logger=logger_instance, sql_log_mode="auto")

            client = Client(logger=logger)
            client.connect(host=host, port=port, user=user, password=password, database=database)

            try:
                result = client.execute("SELECT 1 as test_value")
                assert result is not None
            finally:
                client.disconnect()

    def test_matrixone_logger_class(self, connection_params):
        """Test MatrixOneLogger class directly"""
        host, port, user, password, database = connection_params

        # Create MatrixOneLogger instance
        matrixone_logger = MatrixOneLogger(level=logging.INFO, sql_log_mode="auto")

        # Create client with MatrixOneLogger
        client = Client(logger=matrixone_logger)
        client.connect(host=host, port=port, user=user, password=password, database=database)

        try:
            # Test operations with MatrixOneLogger
            result = client.execute("SELECT 1 as test_value")
            assert result is not None

            # Test performance logging
            result = client.execute("SELECT VERSION() as version")
            assert result is not None

            # Test SQL logging
            result = client.execute("SHOW DATABASES")
            assert result is not None

        finally:
            client.disconnect()
