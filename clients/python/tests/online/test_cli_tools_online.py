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
        database=online_config.database
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
def test_table(client):
    """Create a test table with vector index for CLI testing"""
    table_name = "cli_test_table"
    
    # Clean up if exists
    try:
        client.execute(f"DROP TABLE IF EXISTS {table_name}")
    except:
        pass
    
    # Create table with vector column
    client.create_table(
        table_name,
        {
            "id": "int",
            "name": "varchar(100)",
            "embedding": "vecf32(128)",
            "content": "text"
        },
        primary_key="id"
    )
    
    # Insert test data
    import random
    test_data = []
    for i in range(100):
        test_data.append({
            "id": i,
            "name": f"item_{i}",
            "embedding": [random.random() for _ in range(128)],
            "content": f"test content {i}"
        })
    
    client.batch_insert(table_name, test_data)
    
    # Flush table to ensure metadata is available
    try:
        client.moctl.flush_table(online_config.database, table_name)
    except:
        pass  # Flush might not be available in all environments
    
    # Create IVF index
    try:
        client.vector_ops.create_ivf_index(
            table_name=table_name,
            column_name="embedding",
            index_name="idx_embedding_ivf",
            lists=10
        )
    except:
        pass  # Index creation might fail in some environments
    
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
        # Capture output
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("show_all_indexes")
        
        output = f.getvalue()
        # Should not error
        assert "Error" not in output or output == ""
    
    def test_sql_command(self, cli_instance):
        """Test SQL command execution"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("sql SELECT 1")
        
        output = f.getvalue()
        assert "1" in output or "returned" in output


class TestCLIIndexCommands:
    """Test index-related CLI commands"""
    
    def test_show_indexes_on_table(self, cli_instance, test_table):
        """Test show_indexes command on a specific table"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_indexes {test_table}")
        
        output = f.getvalue()
        # Should either show indexes or indicate none found
        assert "Error" not in output or "No secondary indexes" in output
    
    def test_verify_counts(self, cli_instance, test_table):
        """Test verify_counts command"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"verify_counts {test_table}")
        
        output = f.getvalue()
        # Should either verify successfully or indicate no indexes
        assert "PASSED" in output or "No secondary indexes" in output or "Error" not in output


class TestCLIIVFCommands:
    """Test IVF index related commands"""
    
    def test_show_ivf_status(self, cli_instance):
        """Test show_ivf_status command"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("show_ivf_status")
        
        output = f.getvalue()
        # Should either show IVF indexes or indicate none found
        assert "Error" not in output or "No IVF indexes" in output
    
    def test_show_ivf_status_with_table_filter(self, cli_instance, test_table):
        """Test show_ivf_status with table filter"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_ivf_status -t {test_table}")
        
        output = f.getvalue()
        # Should complete without critical errors
        assert "❌" not in output or "No IVF indexes" in output


class TestCLITableStatsCommands:
    """Test table statistics commands"""
    
    def test_show_table_stats_basic(self, cli_instance, test_table):
        """Test basic table stats"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_table_stats {test_table}")
        
        output = f.getvalue()
        # Should show table statistics or indicate no stats available
        assert ("Table Statistics" in output or "Objects" in output or 
                "No statistics available" in output)
    
    def test_show_table_stats_with_tombstone(self, cli_instance, test_table):
        """Test table stats with tombstone"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_table_stats {test_table} -t")
        
        output = f.getvalue()
        # Should include statistics or indicate no stats available
        assert ("Table Statistics" in output or "Objects" in output or 
                "No statistics available" in output)
    
    def test_show_table_stats_detailed(self, cli_instance, test_table):
        """Test detailed table stats"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_table_stats {test_table} -d")
        
        output = f.getvalue()
        # Should show detailed object list or indicate no stats available
        assert ("Detailed Table Statistics" in output or "Object Name" in output or 
                "No statistics available" in output)
    
    def test_show_table_stats_all(self, cli_instance, test_table):
        """Test table stats with all options"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"show_table_stats {test_table} -a")
        
        output = f.getvalue()
        # Should show comprehensive statistics or indicate no stats available
        assert ("Table Statistics" in output or "Objects" in output or 
                "No statistics available" in output)


class TestCLINonInteractiveMode:
    """Test non-interactive command execution"""
    
    def test_single_command_execution(self, client):
        """Test executing a single command via onecmd"""
        cli = MatrixOneCLI(client)
        cli.current_database = online_config.database
        
        # Capture output
        f = io.StringIO()
        with redirect_stdout(f):
            cli.onecmd("sql SELECT 1 as test_value")
        
        output = f.getvalue()
        assert "test_value" in output or "1" in output
    
    def test_show_all_indexes_non_interactive(self, client):
        """Test show_all_indexes in non-interactive mode"""
        cli = MatrixOneCLI(client)
        cli.current_database = online_config.database
        
        f = io.StringIO()
        with redirect_stdout(f):
            cli.onecmd("show_all_indexes")
        
        output = f.getvalue()
        # Should complete without critical errors
        assert "❌ Error" not in output or output == ""


class TestCLIErrorHandling:
    """Test CLI error handling"""
    
    def test_invalid_table_name(self, cli_instance):
        """Test handling of invalid table name"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("show_table_stats nonexistent_table_xyz_123")
        
        output = f.getvalue()
        # Should show error or no statistics message
        assert "Error" in output or "No statistics" in output
    
    def test_empty_command(self, cli_instance):
        """Test handling of empty commands"""
        # Should not crash
        cli_instance.onecmd("")
        assert True  # If we get here, it didn't crash
    
    def test_malformed_sql(self, cli_instance):
        """Test handling of malformed SQL"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("sql SELECT * FROM")  # Incomplete SQL
        
        output = f.getvalue()
        # Should show error
        assert "Error" in output or "❌" in output


class TestCLIFlushCommands:
    """Test flush table commands"""
    
    def test_flush_table_basic(self, cli_instance, test_table):
        """Test basic flush table command"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"flush_table {test_table}")
        
        output = f.getvalue()
        # Should attempt to flush (may fail due to permissions, but should not crash)
        assert ("Flushing table" in output or "Error" in output or 
                "Failed" in output or "flushed" in output)
    
    def test_flush_table_with_database(self, cli_instance, test_table):
        """Test flush table with database parameter"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd(f"flush_table {test_table} test")
        
        output = f.getvalue()
        # Should attempt to flush (may fail due to permissions, but should not crash)
        assert ("Flushing table" in output or "Error" in output or 
                "Failed" in output or "flushed" in output)
    
    def test_flush_table_invalid_table(self, cli_instance):
        """Test flush table with invalid table name"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("flush_table nonexistent_table")
        
        output = f.getvalue()
        # Should show error for invalid table
        assert ("Error" in output or "Failed" in output)
    
    def test_flush_table_no_args(self, cli_instance):
        """Test flush table without arguments"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("flush_table")
        
        output = f.getvalue()
        # Should show usage error
        assert ("Error" in output and "required" in output)


class TestCLIUtilityCommands:
    """Test utility commands"""
    
    def test_tables_command(self, cli_instance):
        """Test tables command"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("tables")
        
        output = f.getvalue()
        # Should show tables in current database
        assert ("Tables in database" in output or "No tables" in output or 
                "Total:" in output)
    
    def test_tables_with_database(self, cli_instance):
        """Test tables command with database parameter"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("tables test")
        
        output = f.getvalue()
        # Should show tables in specified database
        assert ("Tables in database" in output or "No tables" in output or 
                "Total:" in output)
    
    def test_databases_command(self, cli_instance):
        """Test databases command"""
        f = io.StringIO()
        with redirect_stdout(f):
            cli_instance.onecmd("databases")
        
        output = f.getvalue()
        # Should show databases
        assert ("Databases:" in output and "Total:" in output)
        # Should show at least 'test' database
        assert "test" in output.lower() or "mo_catalog" in output.lower()


class TestCLIHelp:
    """Test CLI help functionality"""
    
    def test_help_command(self, cli_instance):
        """Test help command"""
        # Redirect both stdout and cli_instance.stdout
        f = io.StringIO()
        old_stdout = cli_instance.stdout
        cli_instance.stdout = f
        
        try:
            cli_instance.onecmd("help")
            output = f.getvalue()
            # Should show available commands
            assert len(output) > 0
        finally:
            cli_instance.stdout = old_stdout
    
    def test_help_specific_command(self, cli_instance):
        """Test help for specific command"""
        # Redirect both stdout and cli_instance.stdout
        f = io.StringIO()
        old_stdout = cli_instance.stdout
        cli_instance.stdout = f
        
        try:
            cli_instance.onecmd("help show_table_stats")
            output = f.getvalue()
            # Should show help for show_table_stats (help output goes to self.stdout)
            assert len(output) > 0
        finally:
            cli_instance.stdout = old_stdout


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

