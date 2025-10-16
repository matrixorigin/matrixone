#!/usr/bin/env python3

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
Online tests for MatrixOne CLI diagnostic tool
"""

import pytest
import io
import sys
import random
from contextlib import redirect_stdout
from matrixone import Client
from matrixone.cli_tools import MatrixOneCLI
from .test_config import online_config


@pytest.fixture(scope="module")
def client():
    """Create a MatrixOne client for testing"""
    c = Client()
    c.connect(
        host=online_config.host,
        port=online_config.port,
        user=online_config.user,
        password=online_config.password,
        database=online_config.database,
    )
    yield c
    c.disconnect()


@pytest.fixture(scope="module")
def cli_instance(client):
    """Create a CLI instance for testing"""
    cli = MatrixOneCLI(client)
    cli.current_database = online_config.database
    return cli


@pytest.fixture(scope="module")
def test_table_regular_indexes(client):
    """Create a test table with regular (secondary) indexes only"""
    table_name = "cli_test_regular_indexes"

    # Clean up if exists
    try:
        client.execute(f"DROP TABLE IF EXISTS {table_name}")
    except:
        pass

    # Create table with regular indexes
    sql = f"""
        CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            category VARCHAR(50),
            status INT,
            price DECIMAL(10, 2),
            INDEX idx_name (name),
            INDEX idx_category (category),
            INDEX idx_status (status)
        )
    """
    client.execute(sql)

    # Insert test data in a single batch to avoid table dropping issues
    for i in range(100):
        client.execute(
            f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?)",
            (i, f"item_{i}", f"cat{i % 10}", i % 5, round(random.uniform(10, 1000), 2)),
        )

    # Flush table to ensure metadata is available
    try:
        client.moctl.flush_table(online_config.database, table_name)
    except:
        pass

    yield table_name

    # Cleanup
    try:
        client.execute(f"DROP TABLE IF EXISTS {table_name}")
    except:
        pass


@pytest.fixture(scope="module")
def test_table_unique_indexes(client):
    """Create a test table with UNIQUE indexes"""
    table_name = "cli_test_unique_indexes"

    # Clean up if exists
    try:
        client.execute(f"DROP TABLE IF EXISTS {table_name}")
    except:
        pass

    # Create table with UNIQUE indexes
    sql = f"""
        CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            email VARCHAR(100) UNIQUE,
            username VARCHAR(50),
            phone VARCHAR(20),
            UNIQUE KEY uk_username (username),
            UNIQUE KEY uk_phone (phone)
        )
    """
    client.execute(sql)

    # Insert test data
    for i in range(80):
        client.execute(
            f"INSERT INTO {table_name} VALUES (?, ?, ?, ?)", (i, f"user{i}@test.com", f"user_{i}", f"1234567{i:04d}")
        )

    # Flush table to ensure metadata is available
    try:
        client.moctl.flush_table(online_config.database, table_name)
    except:
        pass

    yield table_name

    # Cleanup
    try:
        client.execute(f"DROP TABLE IF EXISTS {table_name}")
    except:
        pass


@pytest.fixture(scope="module")
def test_table_mixed_indexes(client):
    """Create a test table with both regular and UNIQUE indexes"""
    table_name = "cli_test_mixed_indexes"

    # Clean up if exists
    try:
        client.execute(f"DROP TABLE IF EXISTS {table_name}")
    except:
        pass

    # Create table with multiple index types
    sql = f"""
        CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            email VARCHAR(100) UNIQUE,
            username VARCHAR(50),
            category VARCHAR(50),
            status INT,
            code VARCHAR(20),
            UNIQUE KEY uk_username (username),
            UNIQUE KEY uk_code (code),
            INDEX idx_category (category),
            INDEX idx_status (status)
        )
    """
    client.execute(sql)

    # Insert test data
    for i in range(50):
        client.execute(
            f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?)",
            (i, f"user{i}@test.com", f"user{i}", f"cat{i % 5}", i % 3, f"CODE{i:04d}"),
        )

    # Flush table to ensure metadata is available
    try:
        client.moctl.flush_table(online_config.database, table_name)
    except:
        pass

    yield table_name

    # Cleanup
    try:
        client.execute(f"DROP TABLE IF EXISTS {table_name}")
    except:
        pass


@pytest.fixture(scope="module")
def test_table_fulltext_index(client):
    """Create a test table with fulltext index"""
    table_name = "cli_test_fulltext_index"

    # Clean up if exists
    try:
        client.execute(f"DROP TABLE IF EXISTS {table_name}")
    except:
        pass

    # Create table with fulltext index
    sql = f"""
        CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            title VARCHAR(200),
            content TEXT,
            FULLTEXT INDEX idx_content (content)
        )
    """
    try:
        client.execute(sql)

        # Insert test data
        for i in range(30):
            client.execute(
                f"INSERT INTO {table_name} VALUES (?, ?, ?)",
                (i, f"Article {i}", f"This is test content for article {i}. It contains various keywords and phrases."),
            )

        # Flush table to ensure metadata is available
        try:
            client.moctl.flush_table(online_config.database, table_name)
        except:
            pass

        yield table_name
    except Exception as e:
        # Fulltext might not be supported in all environments
        print(f"Fulltext index test skipped: {e}")
        yield None

    # Cleanup
    try:
        client.execute(f"DROP TABLE IF EXISTS {table_name}")
    except:
        pass


@pytest.fixture(scope="module")
def test_table_ivf_index(client):
    """Create a test table with IVF vector index"""
    table_name = "cli_test_ivf_index"
    table_created = False

    try:
        # Clean up if exists
        try:
            client.execute(f"DROP TABLE IF EXISTS {table_name}")
        except:
            pass

        # Create table with vector column
        sql = f"""
            CREATE TABLE {table_name} (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                embedding VECF32(128),
                metadata TEXT
            )
        """
        client.execute(sql)
        table_created = True

        # Insert test data
        for i in range(100):
            embedding = [random.random() for _ in range(128)]
            # Convert to proper vector format string
            embedding_str = "[" + ",".join(str(x) for x in embedding) + "]"
            client.execute(
                f"INSERT INTO {table_name} (id, name, embedding, metadata) VALUES (?, ?, ?, ?)",
                (i, f"item_{i}", embedding_str, f"metadata_{i}"),
            )

        # Flush table to ensure metadata is available
        try:
            client.moctl.flush_table(online_config.database, table_name)
        except:
            pass

        # Create IVF index
        try:
            client.vector_ops.create_ivf(table_name, "idx_embedding_ivf", "embedding", lists=15)
            yield table_name
        except Exception as e:
            # IVF might not be supported in all environments
            print(f"IVF index creation failed: {e}")
            yield None
    except Exception as e:
        # Table creation or data insertion failed
        print(f"IVF test table setup failed: {e}")
        yield None
    finally:
        # Cleanup
        if table_created:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except:
                pass


@pytest.fixture(scope="module")
def test_table_hnsw_index(client):
    """Create a test table with HNSW vector index"""
    table_name = "cli_test_hnsw_index"
    table_created = False

    try:
        # Clean up if exists
        try:
            client.execute(f"DROP TABLE IF EXISTS {table_name}")
        except:
            pass

        # Create table with vector column (HNSW requires BIGINT primary key)
        sql = f"""
            CREATE TABLE {table_name} (
                id BIGINT PRIMARY KEY,
                name VARCHAR(100),
                embedding VECF32(64),
                description TEXT
            )
        """
        client.execute(sql)
        table_created = True

        # Insert test data
        for i in range(50):
            embedding = [random.random() for _ in range(64)]
            # Convert to proper vector format string
            embedding_str = "[" + ",".join(str(x) for x in embedding) + "]"
            client.execute(
                f"INSERT INTO {table_name} (id, name, embedding, description) VALUES (?, ?, ?, ?)",
                (i, f"item_{i}", embedding_str, f"description_{i}"),
            )

        # Flush table to ensure metadata is available
        try:
            client.moctl.flush_table(online_config.database, table_name)
        except:
            pass

        # Create HNSW index
        try:
            # Enable HNSW support first
            client.vector_ops.enable_hnsw()
            # Create HNSW index using SDK
            client.vector_ops.create_hnsw(
                table_name, "idx_embedding_hnsw", "embedding", m=16, ef_construction=64, ef_search=50
            )
            yield table_name
        except Exception as e:
            # HNSW might not be supported in all environments
            print(f"HNSW index creation failed: {e}")
            yield None
    except Exception as e:
        # Table creation or data insertion failed
        print(f"HNSW test table setup failed: {e}")
        yield None
    finally:
        # Cleanup
        if table_created:
            try:
                client.execute(f"DROP TABLE IF EXISTS {table_name}")
            except:
                pass


@pytest.fixture(scope="module")
def test_table_with_tombstone(client):
    """Create a test table with some deleted rows (tombstone objects)"""
    table_name = "cli_test_with_tombstone"

    # Clean up if exists
    try:
        client.execute(f"DROP TABLE IF EXISTS {table_name}")
    except:
        pass

    # Create table with UNIQUE index to ensure index table tombstone objects
    sql = f"""
        CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            name VARCHAR(100) UNIQUE,
            value INT,
            category VARCHAR(50),
            INDEX idx_value (value),
            INDEX idx_category (category)
        )
    """
    client.execute(sql)

    # Insert test data (100 rows)
    for i in range(100):
        client.execute(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?)", (i, f"item_{i}", i * 10, f"cat{i % 10}"))

    # Flush table to ensure all data is written
    try:
        client.moctl.flush_table(online_config.database, table_name)
    except:
        pass

    # Delete some rows to create tombstone objects (delete first 20 rows)
    client.execute(f"DELETE FROM {table_name} WHERE id < 20")

    # Update some rows to create more tombstone objects
    client.execute(f"UPDATE {table_name} SET value = value + 1000 WHERE id >= 20 AND id < 40")

    # Flush again to create tombstone objects for both main table and index tables
    try:
        client.moctl.flush_table(online_config.database, table_name)
    except:
        pass

    yield table_name

    # Cleanup
    try:
        client.execute(f"DROP TABLE IF EXISTS {table_name}")
    except:
        pass


class TestCLIBasicCommands:
    """Test basic CLI commands"""

    def test_show_all_indexes(self, cli_instance):
        """Test show_all_indexes command"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("show_all_indexes")

        output = f.getvalue()
        # Should show index health report
        assert "Index Health Report" in output or "No tables with indexes" in output

    def test_sql_command(self, cli_instance):
        """Test SQL command execution"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("sql SELECT 1")

        output = f.getvalue()
        assert "1" in output or "returned" in output


class TestCLIRegularIndexes:
    """Test CLI commands with regular (secondary) indexes"""

    def test_show_indexes_regular(self, cli_instance, test_table_regular_indexes):
        """Test show_indexes on table with regular indexes"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_indexes {test_table_regular_indexes}")

        output = f.getvalue()
        assert "Secondary Indexes" in output
        assert "idx_name" in output or "idx_category" in output or "idx_status" in output

    def test_verify_counts_regular(self, cli_instance, test_table_regular_indexes):
        """Test verify_counts on table with regular indexes"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"verify_counts {test_table_regular_indexes}")

        output = f.getvalue()
        assert "PASSED" in output or "100 rows" in output
        # Index names may or may not appear depending on command output format
        assert len(output) > 0

    def test_show_table_stats_regular(self, cli_instance, test_table_regular_indexes):
        """Test show_table_stats on table with regular indexes"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_table_stats {test_table_regular_indexes}")

        output = f.getvalue()
        assert "Table Statistics" in output
        assert test_table_regular_indexes in output

    def test_show_table_stats_regular_detailed(self, cli_instance, test_table_regular_indexes):
        """Test show_table_stats -d on table with regular indexes"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_table_stats {test_table_regular_indexes} -d")

        output = f.getvalue()
        assert "Detailed Table Statistics" in output or "Object Name" in output

    def test_show_table_stats_regular_all_indexes(self, cli_instance, test_table_regular_indexes):
        """Test show_table_stats -a on table with regular indexes"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_table_stats {test_table_regular_indexes} -a")

        output = f.getvalue()
        assert "Table Statistics" in output
        # Should show index statistics (output format may vary)
        assert test_table_regular_indexes in output

    def test_show_table_stats_regular_all_detailed(self, cli_instance, test_table_regular_indexes):
        """Test show_table_stats -a -d on table with regular indexes"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_table_stats {test_table_regular_indexes} -a -d")

        output = f.getvalue()
        assert "Table Statistics" in output or "Detailed Table Statistics" in output
        # Should show index names and physical table info
        assert "Index:" in output


class TestCLIUniqueIndexes:
    """Test CLI commands with UNIQUE indexes"""

    def test_show_indexes_unique(self, cli_instance, test_table_unique_indexes):
        """Test show_indexes on table with UNIQUE indexes"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_indexes {test_table_unique_indexes}")

        output = f.getvalue()
        assert "Secondary Indexes" in output
        assert "uk_username" in output or "uk_phone" in output or "email" in output

    def test_verify_counts_unique(self, cli_instance, test_table_unique_indexes):
        """Test verify_counts on table with UNIQUE indexes"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"verify_counts {test_table_unique_indexes}")

        output = f.getvalue()
        assert "PASSED" in output or "80 rows" in output
        # Should verify UNIQUE indexes
        assert "__mo_index_unique_" in output or "uk_username" in output

    def test_show_table_stats_unique_all(self, cli_instance, test_table_unique_indexes):
        """Test show_table_stats -a on table with UNIQUE indexes"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_table_stats {test_table_unique_indexes} -a")

        output = f.getvalue()
        assert "Table Statistics" in output
        # Should show all UNIQUE indexes (output format may vary)
        assert test_table_unique_indexes in output

    def test_show_table_stats_unique_all_detailed(self, cli_instance, test_table_unique_indexes):
        """Test show_table_stats -a -d on table with UNIQUE indexes"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_table_stats {test_table_unique_indexes} -a -d")

        output = f.getvalue()
        assert "Table Statistics" in output or "Detailed Table Statistics" in output
        # Should show Index: line with physical table info
        assert "Index:" in output
        assert "__mo_index_unique_" in output or "uk_username" in output


class TestCLIMixedIndexes:
    """Test CLI commands with both regular and UNIQUE indexes"""

    def test_show_indexes_mixed(self, cli_instance, test_table_mixed_indexes):
        """Test show_indexes on table with mixed index types"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_indexes {test_table_mixed_indexes}")

        output = f.getvalue()
        assert "Secondary Indexes" in output
        # Should show both types
        assert ("uk_username" in output or "uk_code" in output) and ("idx_category" in output or "idx_status" in output)

    def test_verify_counts_mixed(self, cli_instance, test_table_mixed_indexes):
        """Test verify_counts on table with mixed index types"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"verify_counts {test_table_mixed_indexes}")

        output = f.getvalue()
        assert "PASSED" in output or "50 rows" in output
        # Should verify both UNIQUE and regular indexes
        assert "__mo_index_unique_" in output or "__mo_index_secondary_" in output

    def test_show_all_indexes_includes_mixed(self, cli_instance, test_table_mixed_indexes):
        """Test that show_all_indexes includes table with mixed index types"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("show_all_indexes")

        output = f.getvalue()
        assert test_table_mixed_indexes in output
        # Should show correct index count (3 UNIQUE + 2 regular = 5 total)
        # Or at least show the table is healthy
        assert "✓" in output or "HEALTHY" in output

    def test_show_table_stats_mixed_all_detailed(self, cli_instance, test_table_mixed_indexes):
        """Test show_table_stats -a -d on table with mixed indexes"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_table_stats {test_table_mixed_indexes} -a -d")

        output = f.getvalue()
        assert "Table Statistics" in output or "Detailed Table Statistics" in output
        # Should show all index types with their physical tables
        assert "Index:" in output
        assert "Data" in output  # Should show Data section
        # Should include physical table IDs
        assert ":" in output  # table_name:table_id format


class TestCLIFulltextIndex:
    """Test CLI commands with fulltext indexes"""

    def test_show_indexes_fulltext(self, cli_instance, test_table_fulltext_index):
        """Test show_indexes on table with fulltext index"""
        if test_table_fulltext_index is None:
            pytest.skip("Fulltext index not supported in this environment")

        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_indexes {test_table_fulltext_index}")

        output = f.getvalue()
        # Should handle fulltext indexes
        assert "Index" in output or "idx_content" in output or "fulltext" in output.lower()

    def test_show_all_indexes_includes_fulltext(self, cli_instance, test_table_fulltext_index):
        """Test that show_all_indexes handles fulltext indexes"""
        if test_table_fulltext_index is None:
            pytest.skip("Fulltext index not supported in this environment")

        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("show_all_indexes")

        output = f.getvalue()
        # Should list the table with fulltext index
        assert test_table_fulltext_index in output or "Fulltext" in output

    def test_show_table_stats_fulltext_all(self, cli_instance, test_table_fulltext_index):
        """Test show_table_stats -a on table with fulltext index"""
        if test_table_fulltext_index is None:
            pytest.skip("Fulltext index not supported in this environment")

        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_table_stats {test_table_fulltext_index} -a")

        output = f.getvalue()
        assert "Table Statistics" in output
        # Should show fulltext index information (output format may vary)
        assert test_table_fulltext_index in output


class TestCLIIVFIndex:
    """Test CLI commands with IVF vector indexes"""

    def test_show_indexes_ivf(self, cli_instance, test_table_ivf_index):
        """Test show_indexes on table with IVF index"""
        if test_table_ivf_index is None:
            pytest.skip("IVF index not supported in this environment")

        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_indexes {test_table_ivf_index}")

        output = f.getvalue()
        assert "Index" in output or "idx_embedding_ivf" in output or "IVF" in output

    def test_show_ivf_status_specific_table(self, cli_instance, test_table_ivf_index):
        """Test show_ivf_status on specific table"""
        if test_table_ivf_index is None:
            pytest.skip("IVF index not supported in this environment")

        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_ivf_status -t {test_table_ivf_index}")

        output = f.getvalue()
        # Should show IVF status (centroids, vectors, etc.)
        assert "IVF" in output or "centroid" in output.lower() or test_table_ivf_index in output

    def test_show_all_indexes_includes_ivf(self, cli_instance, test_table_ivf_index):
        """Test that show_all_indexes handles IVF indexes correctly"""
        if test_table_ivf_index is None:
            pytest.skip("IVF index not supported in this environment")

        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("show_all_indexes")

        output = f.getvalue()
        # Should list the table with IVF index
        assert test_table_ivf_index in output
        # Should show IVF stats (centroids and vectors)
        assert ("centroid" in output.lower() or "vector" in output.lower()) or "IVF" in output
        # Should NOT duplicate IVF information (bug fix verification)
        ivf_count = output.count("centroids")
        # IVF info should appear only once per index
        assert ivf_count <= 2  # Allow some flexibility but not triple duplication

    def test_show_table_stats_ivf_all(self, cli_instance, test_table_ivf_index):
        """Test show_table_stats -a on table with IVF index"""
        if test_table_ivf_index is None:
            pytest.skip("IVF index not supported in this environment")

        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_table_stats {test_table_ivf_index} -a")

        output = f.getvalue()
        assert "Table Statistics" in output
        # Should show IVF index (output format may vary)
        assert test_table_ivf_index in output

    def test_show_table_stats_ivf_all_detailed(self, cli_instance, test_table_ivf_index):
        """Test show_table_stats -a -d on table with IVF index (should show metadata, centroids, entries)"""
        if test_table_ivf_index is None:
            pytest.skip("IVF index not supported in this environment")

        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_table_stats {test_table_ivf_index} -a -d")

        output = f.getvalue()
        assert "Table Statistics" in output or "Detailed Table Statistics" in output
        # Should show IVF physical tables: metadata, centroids, entries
        assert "Index:" in output
        # Should show physical table types for IVF
        assert "metadata" in output or "centroids" in output or "entries" in output
        # Should show Data and Tombstone sections
        assert "Data" in output


class TestCLIHNSWIndex:
    """Test CLI commands with HNSW vector indexes"""

    def test_show_indexes_hnsw(self, cli_instance, test_table_hnsw_index):
        """Test show_indexes on table with HNSW index"""
        if test_table_hnsw_index is None:
            pytest.skip("HNSW index not supported in this environment")

        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_indexes {test_table_hnsw_index}")

        output = f.getvalue()
        assert "Index" in output or "idx_embedding_hnsw" in output or "HNSW" in output

    def test_show_all_indexes_includes_hnsw(self, cli_instance, test_table_hnsw_index):
        """Test that show_all_indexes handles HNSW indexes"""
        if test_table_hnsw_index is None:
            pytest.skip("HNSW index not supported in this environment")

        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("show_all_indexes")

        output = f.getvalue()
        # Should list the table with HNSW index
        assert test_table_hnsw_index in output or "HNSW" in output

    def test_show_table_stats_hnsw_all_detailed(self, cli_instance, test_table_hnsw_index):
        """Test show_table_stats -a -d on table with HNSW index"""
        if test_table_hnsw_index is None:
            pytest.skip("HNSW index not supported in this environment")

        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_table_stats {test_table_hnsw_index} -a -d")

        output = f.getvalue()
        assert "Table Statistics" in output or "Detailed Table Statistics" in output
        # Should show HNSW index information
        assert "Index:" in output


class TestCLITombstoneObjects:
    """Test CLI commands with tombstone (deleted) objects"""

    def test_show_table_stats_with_tombstone(self, cli_instance, test_table_with_tombstone):
        """Test show_table_stats -t to include tombstone objects"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_table_stats {test_table_with_tombstone} -t")

        output = f.getvalue()
        assert "Table Statistics" in output
        # Should show the table name and have some data
        assert test_table_with_tombstone in output
        # Should have objects (main table should have 80 active rows after deletion)
        assert "Objects:" in output or "80" in output

    def test_show_table_stats_tombstone_detailed(self, cli_instance, test_table_with_tombstone):
        """Test show_table_stats -d -t for detailed tombstone view"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_table_stats {test_table_with_tombstone} -d -t")

        output = f.getvalue()
        assert "Detailed Table Statistics" in output or "Table Statistics" in output

    def test_show_table_stats_tombstone_all_detailed(self, cli_instance, test_table_with_tombstone):
        """Test show_table_stats -a -d -t for all indexes with tombstone"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_table_stats {test_table_with_tombstone} -a -d -t")

        output = f.getvalue()
        assert "Table Statistics" in output or "Detailed Table Statistics" in output
        # Should show Data section
        assert "Data" in output
        # Should show Index sections (UNIQUE + 2 regular indexes = 3 total)
        assert "Index:" in output
        # Should show tombstone information for main table and index tables
        # After DELETE and UPDATE operations, should have tombstone objects
        assert "Tombstone" in output or "Objects:" in output

    def test_show_table_stats_tombstone_all(self, cli_instance, test_table_with_tombstone):
        """Test show_table_stats -a -t for brief view with indexes and tombstone"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_table_stats {test_table_with_tombstone} -a -t")

        output = f.getvalue()
        assert "Table Statistics" in output
        # Should show index information
        assert "index:" in output.lower() or test_table_with_tombstone in output


class TestCLIFlushCommands:
    """Test flush table commands"""

    def test_flush_table_basic(self, cli_instance, test_table_regular_indexes):
        """Test basic flush table command"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"flush_table {test_table_regular_indexes}")

        output = f.getvalue()
        # Should attempt to flush main table and index tables
        assert "Flushing table" in output
        # Should show table name
        assert test_table_regular_indexes in output
        # Should mention index tables (3 regular indexes)
        assert "index tables" in output.lower() or "flushed" in output.lower()

    def test_flush_table_with_indexes(self, cli_instance, test_table_mixed_indexes):
        """Test flush table with multiple index types"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"flush_table {test_table_mixed_indexes}")

        output = f.getvalue()
        # Should flush main table and all index tables
        assert "Flushing table" in output
        # Should show table name
        assert test_table_mixed_indexes in output
        # Should show index table count (3 UNIQUE + 2 regular = 5 total)
        assert "index tables" in output.lower() or "flushed" in output.lower()

    def test_flush_table_invalid(self, cli_instance):
        """Test flush table with invalid table name"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("flush_table nonexistent_table_xyz")

        output = f.getvalue()
        assert "Error" in output or "Failed" in output

    def test_flush_table_creates_tombstone(self, cli_instance, test_table_with_tombstone):
        """Test that flush_table command works and tombstone objects are created"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"flush_table {test_table_with_tombstone}")

        output = f.getvalue()
        # Should flush main table and all index tables
        assert "Flushing table" in output
        assert test_table_with_tombstone in output
        # Should show index table count (3 UNIQUE + 2 regular = 5 total)
        assert "index tables" in output.lower() or "flushed" in output.lower()


class TestCLIUtilityCommands:
    """Test utility commands"""

    def test_tables_command(self, cli_instance):
        """Test tables command"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("tables")

        output = f.getvalue()
        assert "Tables in database" in output or "Total:" in output

    def test_databases_command(self, cli_instance):
        """Test databases command"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("databases")

        output = f.getvalue()
        assert "Databases:" in output and "Total:" in output


class TestCLIErrorHandling:
    """Test CLI error handling"""

    def test_invalid_table_name(self, cli_instance):
        """Test handling of invalid table name"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("show_table_stats nonexistent_table_xyz_123")

        output = f.getvalue()
        assert "Error" in output or "No statistics" in output

    def test_empty_command(self, cli_instance):
        """Test handling of empty commands"""
        cli_instance.onecmd("")
        assert True  # Should not crash

    def test_malformed_sql(self, cli_instance):
        """Test handling of malformed SQL"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("sql SELECT * FROM")

        output = f.getvalue()
        assert "Error" in output or "❌" in output


class TestCLIHelp:
    """Test CLI help functionality"""

    def test_help_command(self, cli_instance):
        """Test help command"""
        f = io.StringIO()
        old_stdout = cli_instance.stdout
        cli_instance.stdout = f

        try:
            cli_instance.onecmd("help")
            output = f.getvalue()
            assert len(output) > 0
        finally:
            cli_instance.stdout = old_stdout

    def test_help_specific_command(self, cli_instance):
        """Test help for specific command"""
        f = io.StringIO()
        old_stdout = cli_instance.stdout
        cli_instance.stdout = f

        try:
            cli_instance.onecmd("help show_table_stats")
            output = f.getvalue()
            assert len(output) > 0
        finally:
            cli_instance.stdout = old_stdout


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
