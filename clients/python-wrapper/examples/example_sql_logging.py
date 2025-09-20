#!/usr/bin/env python3
"""
MatrixOne SQL Logging Examples

This example demonstrates the enhanced SQL logging capabilities of MatrixOne Python SDK:
1. Basic SQL logging
2. Full SQL logging (no truncation)
3. Slow SQL logging with configurable threshold
4. Error SQL logging
5. Combined logging options

Usage:
    python example_sql_logging.py
"""

import sys
import os
import time

# Add the matrixone module to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone.client import Client
from matrixone.logger import create_default_logger
import logging


def demonstrate_basic_sql_logging():
    """Demonstrate basic SQL logging"""
    print("🔍 Basic SQL Logging Demo")
    print("=" * 50)
    
    # Create client with basic SQL logging enabled
    client = Client(
        enable_sql_logging=True
    )
    
    try:
        # Connect to MatrixOne
        client.connect(
            host='127.0.0.1',
            port=6001,
            user='root',
            password='111',
            database='test'
        )
        
        print("✅ Connected to MatrixOne")
        print("📝 Executing queries with basic SQL logging...")
        
        # Execute some queries
        client.execute("SELECT 1 as test_column")
        client.execute("SHOW DATABASES")
        client.execute("SELECT USER(), CURRENT_USER()")
        
        print("✅ Basic SQL logging completed")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        client.disconnect()


def demonstrate_full_sql_logging():
    """Demonstrate full SQL logging (no truncation)"""
    print("\n🔍 Full SQL Logging Demo")
    print("=" * 50)
    
    # Create client with full SQL logging enabled
    client = Client(
        enable_full_sql_logging=True
    )
    
    try:
        # Connect to MatrixOne
        client.connect(
            host='127.0.0.1',
            port=6001,
            user='root',
            password='111',
            database='test'
        )
        
        print("✅ Connected to MatrixOne")
        print("📝 Executing queries with full SQL logging (no truncation)...")
        
        # Execute a long query to demonstrate full logging
        long_query = """
        SELECT 
            table_name,
            table_schema,
            table_type,
            engine,
            table_rows,
            avg_row_length,
            data_length,
            max_data_length,
            index_length,
            data_free,
            auto_increment,
            create_time,
            update_time,
            check_time,
            table_collation,
            checksum,
            create_options,
            table_comment
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        ORDER BY table_schema, table_name
        """
        
        client.execute(long_query)
        
        print("✅ Full SQL logging completed")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        client.disconnect()


def demonstrate_slow_sql_logging():
    """Demonstrate slow SQL logging"""
    print("\n🔍 Slow SQL Logging Demo")
    print("=" * 50)
    
    # Create client with slow SQL logging enabled (threshold: 0.1 seconds)
    client = Client(
        enable_slow_sql_logging=True,
        slow_sql_threshold=0.1
    )
    
    try:
        # Connect to MatrixOne
        client.connect(
            host='127.0.0.1',
            port=6001,
            user='root',
            password='111',
            database='test'
        )
        
        print("✅ Connected to MatrixOne")
        print("📝 Executing queries with slow SQL logging (threshold: 0.1s)...")
        
        # Execute fast query (should not be logged)
        client.execute("SELECT 1")
        
        # Execute slower query (should be logged)
        client.execute("SELECT SLEEP(0.2)")
        
        # Execute another fast query
        client.execute("SHOW DATABASES")
        
        print("✅ Slow SQL logging completed")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        client.disconnect()


def demonstrate_error_sql_logging():
    """Demonstrate error SQL logging"""
    print("\n🔍 Error SQL Logging Demo")
    print("=" * 50)
    
    # Create client with error SQL logging enabled
    client = Client(
        enable_error_sql_logging=True
    )
    
    try:
        # Connect to MatrixOne
        client.connect(
            host='127.0.0.1',
            port=6001,
            user='root',
            password='111',
            database='test'
        )
        
        print("✅ Connected to MatrixOne")
        print("📝 Executing queries with error SQL logging...")
        
        # Execute a successful query (should not be logged)
        client.execute("SELECT 1")
        
        # Execute a query that will fail (should be logged)
        try:
            client.execute("SELECT * FROM non_existent_table")
        except Exception:
            pass  # Expected error
        
        # Execute another successful query
        client.execute("SHOW DATABASES")
        
        print("✅ Error SQL logging completed")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        client.disconnect()


def demonstrate_combined_logging():
    """Demonstrate combined logging options"""
    print("\n🔍 Combined SQL Logging Demo")
    print("=" * 50)
    
    # Create client with multiple logging options enabled
    client = Client(
        enable_sql_logging=True,
        enable_full_sql_logging=True,
        enable_slow_sql_logging=True,
        enable_error_sql_logging=True,
        slow_sql_threshold=0.05
    )
    
    try:
        # Connect to MatrixOne
        client.connect(
            host='127.0.0.1',
            port=6001,
            user='root',
            password='111',
            database='test'
        )
        
        print("✅ Connected to MatrixOne")
        print("📝 Executing queries with combined logging options...")
        
        # Fast query (basic SQL logging)
        client.execute("SELECT 1")
        
        # Slow query (basic + slow SQL logging)
        client.execute("SELECT SLEEP(0.1)")
        
        # Long query (basic + full SQL logging)
        long_query = """
        SELECT 
            table_name,
            table_schema,
            table_type,
            engine
        FROM information_schema.tables 
        WHERE table_schema = 'information_schema'
        """
        client.execute(long_query)
        
        # Error query (error SQL logging)
        try:
            client.execute("INVALID SQL SYNTAX")
        except Exception:
            pass  # Expected error
        
        print("✅ Combined SQL logging completed")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        client.disconnect()


def demonstrate_custom_logger():
    """Demonstrate using custom logger with SQL logging"""
    print("\n🔍 Custom Logger with SQL Logging Demo")
    print("=" * 50)
    
    # Create custom logger
    custom_logger = logging.getLogger('matrixone_custom')
    custom_logger.setLevel(logging.INFO)
    
    # Create console handler with custom format
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s|%(name)s|%(levelname)s|%(message)s')
    handler.setFormatter(formatter)
    custom_logger.addHandler(handler)
    
    # Create MatrixOne logger from custom logger
    from matrixone.logger import create_custom_logger
from matrixone.config import get_connection_params, print_config
    mo_logger = create_custom_logger(
        logger=custom_logger,
        enable_sql_logging=True,
        enable_full_sql_logging=True,
        enable_slow_sql_logging=True,
        slow_sql_threshold=0.05
    )
    
    # Create client with custom logger
    client = Client(
        logger=mo_logger
    )
    
    try:
        # Connect to MatrixOne
        client.connect(
            host='127.0.0.1',
            port=6001,
            user='root',
            password='111',
            database='test'
        )
        
        print("✅ Connected to MatrixOne with custom logger")
        print("📝 Executing queries with custom logger...")
        
        client.execute("SELECT 1")
        client.execute("SELECT SLEEP(0.1)")
        client.execute("SHOW DATABASES")
        
        print("✅ Custom logger SQL logging completed")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        client.disconnect()


def main():
    """Main function to run all SQL logging demonstrations"""
    print("🚀 MatrixOne SQL Logging Examples")
    print("=" * 60)
    print("This example demonstrates various SQL logging capabilities:")
    print("- Basic SQL logging (truncated queries)")
    print("- Full SQL logging (complete queries)")
    print("- Slow SQL logging (configurable threshold)")
    print("- Error SQL logging (failed queries only)")
    print("- Combined logging options")
    print("- Custom logger integration")
    print()
    
    try:
        # Run all demonstrations
        demonstrate_basic_sql_logging()
        demonstrate_full_sql_logging()
        demonstrate_slow_sql_logging()
        demonstrate_error_sql_logging()
        demonstrate_combined_logging()
        demonstrate_custom_logger()
        
        print("\n🎉 All SQL logging demonstrations completed!")
        print("\nKey features demonstrated:")
        print("- ✅ Basic SQL logging with query truncation")
        print("- ✅ Full SQL logging without truncation")
        print("- ✅ Slow SQL logging with configurable threshold")
        print("- ✅ Error SQL logging for failed queries")
        print("- ✅ Combined logging options")
        print("- ✅ Custom logger integration")
        print("- ✅ Different log types (SQL, FULL_SQL, SLOW_SQL, ERROR_SQL)")
        
    except KeyboardInterrupt:
        print("\n⚠️ Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")


if __name__ == "__main__":
    main()
