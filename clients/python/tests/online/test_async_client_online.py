"""
Online tests for Async Client functionality - tests actual database operations
"""

import pytest
import pytest_asyncio
import asyncio
import os
import sys
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone import AsyncClient
from matrixone.exceptions import ConnectionError, QueryError
from .test_config import online_config


class TestAsyncClientOnline:
    """Online tests for Async Client functionality"""

    @pytest_asyncio.fixture(scope="function")
    async def test_async_client(self):
        """Create and connect AsyncClient for testing"""
        host, port, user, password, database = online_config.get_connection_params()
        client = AsyncClient()
        await client.connect(host, port, user, password, database)
        try:
            yield client
        finally:
            try:
                await client.disconnect()
            except Exception as e:
                print(f"Warning: Failed to disconnect async client: {e}")

    @pytest_asyncio.fixture(scope="function")
    async def test_database(self, test_async_client):
        """Set up test database and table"""
        test_db = "test_async_client_db"
        test_table = "test_async_client_table"

        try:
            await test_async_client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
            await test_async_client.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {test_db}.{test_table} (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    value INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Insert test data
            await test_async_client.execute(f"INSERT INTO {test_db}.{test_table} VALUES (1, 'async_test1', 100, NOW())")
            await test_async_client.execute(f"INSERT INTO {test_db}.{test_table} VALUES (2, 'async_test2', 200, NOW())")
            await test_async_client.execute(f"INSERT INTO {test_db}.{test_table} VALUES (3, 'async_test3', 300, NOW())")

            yield test_db, test_table

        finally:
            # Clean up
            try:
                await test_async_client.execute(f"DROP DATABASE IF EXISTS {test_db}")
            except Exception as e:
                print(f"Cleanup failed: {e}")

    @pytest.mark.asyncio
    async def test_basic_async_connection_and_query(self, test_async_client):
        """Test basic async connection and query functionality"""
        # Test simple query
        result = await test_async_client.execute("SELECT 1 as test_value")
        rows = result.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 1

        # Test query with parameters
        result = await test_async_client.execute("SELECT ? as param_value", (42,))
        rows = result.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 42

    @pytest.mark.asyncio
    async def test_async_table_operations(self, test_async_client, test_database):
        """Test async table operations"""
        test_db, test_table = test_database

        # Test SELECT
        result = await test_async_client.execute(f"SELECT * FROM {test_db}.{test_table} ORDER BY id")
        rows = result.fetchall()
        assert len(rows) == 3
        assert rows[0][0] == 1  # id
        assert rows[0][1] == 'async_test1'  # name

        # Test INSERT
        await test_async_client.execute(f"INSERT INTO {test_db}.{test_table} VALUES (4, 'async_test4', 400, NOW())")

        # Verify INSERT
        result = await test_async_client.execute(f"SELECT COUNT(*) FROM {test_db}.{test_table}")
        count = result.fetchone()[0]
        assert count == 4

        # Test UPDATE
        await test_async_client.execute(f"UPDATE {test_db}.{test_table} SET value = 500 WHERE id = 4")

        # Verify UPDATE
        result = await test_async_client.execute(f"SELECT value FROM {test_db}.{test_table} WHERE id = 4")
        value = result.fetchone()[0]
        assert value == 500

        # Test DELETE
        await test_async_client.execute(f"DELETE FROM {test_db}.{test_table} WHERE id = 4")

        # Verify DELETE
        result = await test_async_client.execute(f"SELECT COUNT(*) FROM {test_db}.{test_table}")
        count = result.fetchone()[0]
        assert count == 3

    @pytest.mark.asyncio
    async def test_async_transaction_operations(self, test_async_client, test_database):
        """Test async transaction operations"""
        test_db, test_table = test_database

        async with test_async_client.transaction() as tx:
            # Insert within transaction
            await tx.execute(f"INSERT INTO {test_db}.{test_table} VALUES (5, 'async_test5', 500, NOW())")

            # Verify within transaction
            result = await tx.execute(f"SELECT COUNT(*) FROM {test_db}.{test_table}")
            count = result.fetchone()[0]
            assert count == 4

        # Verify transaction commit
        result = await test_async_client.execute(f"SELECT COUNT(*) FROM {test_db}.{test_table}")
        count = result.fetchone()[0]
        assert count == 4

        # Test transaction rollback
        try:
            async with test_async_client.transaction() as tx:
                await tx.execute(f"INSERT INTO {test_db}.{test_table} VALUES (6, 'async_test6', 600, NOW())")
                # Force rollback by raising exception
                raise Exception("Test rollback")
        except Exception:
            pass

        # Verify rollback
        result = await test_async_client.execute(f"SELECT COUNT(*) FROM {test_db}.{test_table}")
        count = result.fetchone()[0]
        assert count == 4  # Should still be 4, not 5

        # Clean up
        await test_async_client.execute(f"DELETE FROM {test_db}.{test_table} WHERE id = 5")


    @pytest.mark.asyncio
    async def test_async_error_handling(self, test_async_client):
        """Test async error handling"""
        # Test connection error handling
        with pytest.raises(QueryError):
            await test_async_client.execute("SELECT * FROM non_existent_table")

        # Test invalid SQL
        with pytest.raises(QueryError):
            await test_async_client.execute("INVALID SQL STATEMENT")

    @pytest.mark.asyncio
    async def test_async_result_set_operations(self, test_async_client, test_database):
        """Test async result set operations"""
        test_db, test_table = test_database

        result = await test_async_client.execute(f"SELECT * FROM {test_db}.{test_table} ORDER BY id")

        # Test fetchone
        first_row = result.fetchone()
        assert first_row is not None
        assert first_row[0] == 1

        # Test fetchmany
        next_rows = result.fetchmany(2)
        assert len(next_rows) == 2
        assert next_rows[0][0] == 2
        assert next_rows[1][0] == 3

        # Test fetchall (should return remaining rows)
        remaining_rows = result.fetchall()
        assert len(remaining_rows) == 0  # Should be empty

        # Test column names
        result = await test_async_client.execute(f"SELECT id, name, value FROM {test_db}.{test_table} LIMIT 1")
        columns = result.keys()
        expected_columns = ['id', 'name', 'value']
        assert list(columns) == expected_columns

    @pytest.mark.asyncio
    async def test_async_parameter_binding(self, test_async_client, test_database):
        """Test async parameter binding"""
        test_db, test_table = test_database

        # Test string parameters
        result = await test_async_client.execute(f"SELECT * FROM {test_db}.{test_table} WHERE name = ?", ('async_test1',))
        rows = result.fetchall()
        assert len(rows) == 1
        assert rows[0][1] == 'async_test1'

        # Test numeric parameters
        result = await test_async_client.execute(f"SELECT * FROM {test_db}.{test_table} WHERE value = ?", (200,))
        rows = result.fetchall()
        assert len(rows) == 1
        assert rows[0][2] == 200

        # Test multiple parameters
        result = await test_async_client.execute(
            f"SELECT * FROM {test_db}.{test_table} WHERE name = ? AND value > ?",
            ('async_test2', 150),
        )
        rows = result.fetchall()
        assert len(rows) == 1
        assert rows[0][1] == 'async_test2'
        assert rows[0][2] == 200

    @pytest.mark.asyncio
    async def test_async_concurrent_operations(self, test_async_client, test_database):
        """Test async concurrent operations"""
        test_db, test_table = test_database

        # Test concurrent queries
        tasks = [
            test_async_client.execute(f"SELECT COUNT(*) FROM {test_db}.{test_table}"),
            test_async_client.execute(f"SELECT MAX(value) FROM {test_db}.{test_table}"),
            test_async_client.execute(f"SELECT MIN(value) FROM {test_db}.{test_table}"),
        ]

        results = await asyncio.gather(*tasks)

        # All queries should complete successfully
        assert len(results) == 3
        assert results[0].fetchone()[0] == 3  # count
        assert results[1].fetchone()[0] == 300  # max value
        assert results[2].fetchone()[0] == 100  # min value

    @pytest.mark.asyncio
    async def test_async_connection_pooling(self, test_async_client, test_database):
        """Test async connection pooling and reuse"""
        test_db, test_table = test_database

        # Execute multiple queries to test connection reuse
        for i in range(5):
            result = await test_async_client.execute("SELECT 1 as test")
            rows = result.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == 1

        # Test concurrent operations
        tasks = []
        for i in range(3):
            task = test_async_client.execute(f"SELECT COUNT(*) FROM {test_db}.{test_table}")
            tasks.append(task)

        results = await asyncio.gather(*tasks)

        # All should return the same count
        for result in results:
            count = result.fetchone()[0]
            assert count == 3

    @pytest.mark.asyncio
    async def test_async_simple_query_basic_functionality(self, test_async_client, test_database):
        """Test basic async simple_query functionality."""
        test_db, test_table = test_database

        # Enable experimental fulltext index
        try:
            await test_async_client.execute("SET experimental_fulltext_index=1")
        except Exception as e:
            print(f"Fulltext index setup warning: {e}")

        # Create a fulltext index for testing (only on text columns)
        try:
            await test_async_client.execute(f"CREATE FULLTEXT INDEX ft_test ON {test_db}.{test_table}(name)")
        except Exception as e:
            print(f"Fulltext index creation warning: {e}")
            return  # Skip test if fulltext not supported

        # Test basic simple_query (only search text columns)
        results = await (
            test_async_client.fulltext_index.simple_query(f"{test_db}.{test_table}")
            .columns("name")
            .search("async")
            .execute()
        )

        assert isinstance(results.rows, list)
        assert len(results.rows) > 0

        # Should find test data (row[0] is id, row[1] is name)
        found_test = any("async" in str(row[1]) for row in results.rows)
        assert found_test

    @pytest.mark.asyncio
    async def test_async_simple_query_with_score(self, test_async_client, test_database):
        """Test async simple_query with scoring."""
        test_db, test_table = test_database

        # Enable experimental fulltext index
        try:
            await test_async_client.execute("SET experimental_fulltext_index=1")
        except Exception as e:
            print(f"Fulltext index setup warning: {e}")

        try:
            await test_async_client.execute(f"CREATE FULLTEXT INDEX ft_test ON {test_db}.{test_table}(name)")
        except Exception as e:
            print(f"Fulltext index creation warning: {e}")
            return  # Skip test if fulltext not supported

        # Test simple_query with score (only search text columns)
        results = await (
            test_async_client.fulltext_index.simple_query(f"{test_db}.{test_table}")
            .columns("name")
            .search("async")
            .with_score()
            .execute()
        )

        assert isinstance(results.rows, list)
        assert len(results.rows) > 0

        # Check that score column is included
        assert len(results.columns) > 2  # Original 2 columns + score
        score_column_index = len(results.columns) - 1

        # Verify score values are numeric
        for row in results.rows:
            score = row[score_column_index]
            assert isinstance(score, (int, float))
            assert score >= 0

    @pytest.mark.asyncio
    async def test_async_simple_query_boolean_mode(self, test_async_client, test_database):
        """Test async simple_query with boolean mode."""
        test_db, test_table = test_database

        # Enable experimental fulltext index
        try:
            await test_async_client.execute("SET experimental_fulltext_index=1")
        except Exception as e:
            print(f"Fulltext index setup warning: {e}")

        try:
            await test_async_client.execute(f"CREATE FULLTEXT INDEX ft_test ON {test_db}.{test_table}(name)")
        except Exception as e:
            print(f"Fulltext index creation warning: {e}")
            return  # Skip test if fulltext not supported

        # Test boolean mode (only search text columns)
        results = await (
            test_async_client.fulltext_index.simple_query(f"{test_db}.{test_table}")
            .columns("name")
            .must_have("async")
            .must_not_have("nonexistent")
            .execute()
        )

        assert isinstance(results.rows, list)
        # Should find async test data (row[0] is id, row[1] is name)
        for row in results.rows:
            assert "async" in str(row[1]).lower()

    @pytest.mark.asyncio
    async def test_async_simple_query_with_where(self, test_async_client, test_database):
        """Test async simple_query with WHERE conditions."""
        test_db, test_table = test_database

        # Enable experimental fulltext index
        try:
            await test_async_client.execute("SET experimental_fulltext_index=1")
        except Exception as e:
            print(f"Fulltext index setup warning: {e}")

        try:
            await test_async_client.execute(f"CREATE FULLTEXT INDEX ft_test ON {test_db}.{test_table}(name)")
        except Exception as e:
            print(f"Fulltext index creation warning: {e}")
            return  # Skip test if fulltext not supported

        # Test with WHERE condition (only search text columns)
        results = await (
            test_async_client.fulltext_index.simple_query(f"{test_db}.{test_table}")
            .columns("name")
            .search("test")
            .where("value > 150")
            .execute()
        )

        assert isinstance(results.rows, list)
        # All results should have value > 150
        for row in results.rows:
            assert row[1] > 150

    @pytest.mark.asyncio
    async def test_async_simple_query_ordering_and_limit(self, test_async_client, test_database):
        """Test async simple_query with ordering and limit."""
        test_db, test_table = test_database

        # Enable experimental fulltext index
        try:
            await test_async_client.execute("SET experimental_fulltext_index=1")
        except Exception as e:
            print(f"Fulltext index setup warning: {e}")

        try:
            await test_async_client.execute(f"CREATE FULLTEXT INDEX ft_test ON {test_db}.{test_table}(name)")
        except Exception as e:
            print(f"Fulltext index creation warning: {e}")
            return  # Skip test if fulltext not supported

        # Test with ordering and limit (only search text columns)
        results = await (
            test_async_client.fulltext_index.simple_query(f"{test_db}.{test_table}")
            .columns("name")
            .search("test")
            .order_by("value", desc=True)
            .limit(2)
            .execute()
        )

        assert isinstance(results.rows, list)
        assert len(results.rows) <= 2

        # Results should be ordered by value descending
        if len(results.rows) > 1:
            assert results.rows[0][1] >= results.rows[1][1]

    @pytest.mark.asyncio
    async def test_async_simple_query_explain(self, test_async_client, test_database):
        """Test async simple_query explain functionality."""
        test_db, test_table = test_database

        builder = test_async_client.fulltext_index.simple_query(f"{test_db}.{test_table}")
        builder.columns("name", "value").search("test").with_score().where("value > 100")

        sql = builder.explain()
        assert isinstance(sql, str)
        assert "SELECT" in sql.upper()
        assert "MATCH" in sql.upper()
        assert "AGAINST" in sql.upper()

    @pytest.mark.asyncio
    async def test_async_simple_query_method_chaining(self, test_async_client, test_database):
        """Test async simple_query method chaining."""
        test_db, test_table = test_database

        builder = test_async_client.fulltext_index.simple_query(f"{test_db}.{test_table}")

        # All these should return the builder instance
        result = (
            builder.columns("name", "value")
            .search("test")
            .with_score("score")
            .where("value > 100")
            .order_by("value", desc=True)
            .limit(10, 0)
        )

        assert result is builder  # Should be the same instance

    @pytest.mark.asyncio
    async def test_async_simple_query_error_handling(self, test_async_client):
        """Test async simple_query error handling."""
        # Test with non-existent table
        with pytest.raises(QueryError):
            await (
                test_async_client.fulltext_index.simple_query("nonexistent_table")
                .columns("name", "value")
                .search("test")
                .execute()
            )


if __name__ == '__main__':
    pytest.main([__file__])
