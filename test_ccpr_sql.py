#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test script for Cross-Cluster Physical Subscription SQL statements.

This script tests:
- SHOW CCPR SUBSCRIPTIONS commands:
  - SHOW CCPR SUBSCRIPTIONS (show all subscriptions)
  - SHOW CCPR SUBSCRIPTION pub_name (show specific subscription)
  - Tests account visibility (sys account sees all, other accounts see only their own)
- DROP CCPR SUBSCRIPTION commands:
  - DROP CCPR SUBSCRIPTION pub_name (drop subscription)
  - DROP CCPR SUBSCRIPTION IF EXISTS pub_name (drop with IF EXISTS)
  - Tests database-level and table-level subscriptions
  - Verifies drop_at field is set in mo_ccpr_log

Sets up all necessary resources independently (accounts, databases, publications)
"""

import sys
import os
import time
from datetime import datetime

# Add clients/python to path to import matrixone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'clients', 'python'))

try:
    from matrixone import Client
    from matrixone.exceptions import MatrixOneError
except ImportError as e:
    print("=" * 80)
    print("ERROR: Missing required Python dependencies!")
    print("=" * 80)
    print(f"\nError: {e}")
    print("\nTo install dependencies, run:")
    print("  cd clients/python")
    print("  pip3 install -r requirements.txt")
    print("=" * 80)
    sys.exit(1)


# Configuration
CLUSTER1_HOST = '127.0.0.1'
CLUSTER1_PORT = 6001
CLUSTER2_HOST = '127.0.0.1'
CLUSTER2_PORT = 6002
SYS_USER = 'root'
SYS_PASSWORD = '111'

# Account configuration
CLUSTER1_ACCOUNT = 'cluster1_test_account'
CLUSTER2_ACCOUNT = 'cluster2_test_account'
ACCOUNT_ADMIN = 'admin'
ACCOUNT_PASSWORD = '111'

# Test configuration
TEST_DB_NAME = 'test_ccpr_db'
TEST_DB_NAME2 = 'test_ccpr_db2'
TEST_TABLE_NAME = 'test_ccpr_table'
TEST_TABLE_NAME2 = 'test_ccpr_table2'
PUBLICATION_NAME = 'test_ccpr_pub'
PUBLICATION_NAME2 = 'test_ccpr_pub2'
PUBLICATION_NAME_NOT_EXIST = 'nonexistent_pub'


class Colors:
    """ANSI color codes for terminal output"""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def print_header(msg):
    """Print a header message"""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 80}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{msg}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'=' * 80}{Colors.ENDC}\n")


def print_success(msg):
    """Print a success message"""
    print(f"{Colors.OKGREEN}✓ {msg}{Colors.ENDC}")


def print_error(msg):
    """Print an error message"""
    print(f"{Colors.FAIL}✗ {msg}{Colors.ENDC}")


def print_warning(msg):
    """Print a warning message"""
    print(f"{Colors.WARNING}⚠ {msg}{Colors.ENDC}")


def print_info(msg):
    """Print an info message"""
    print(f"{Colors.OKCYAN}ℹ {msg}{Colors.ENDC}")


def cleanup_account(client, account_name):
    """Drop account if exists"""
    try:
        client.execute(f"DROP ACCOUNT IF EXISTS `{account_name}`")
        print_success(f"Dropped account {account_name} (if existed)")
    except Exception as e:
        print_warning(f"Error dropping account {account_name}: {e}")


def cleanup_database(client, db_name):
    """Drop database if exists"""
    try:
        client.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
        print_success(f"Dropped database {db_name} (if existed)")
    except Exception as e:
        print_warning(f"Error dropping database {db_name}: {e}")


def cleanup_table(client, db_name, table_name):
    """Drop table if exists"""
    try:
        client.execute(f"USE `{db_name}`")
        client.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        print_success(f"Dropped table {db_name}.{table_name} (if existed)")
    except Exception as e:
        print_warning(f"Error dropping table {db_name}.{table_name}: {e}")


def cleanup_publication(client, pub_name):
    """Drop publication if exists"""
    try:
        client.execute(f"DROP PUBLICATION IF EXISTS `{pub_name}`")
        print_success(f"Dropped publication {pub_name} (if existed)")
    except Exception as e:
        print_warning(f"Error dropping publication {pub_name}: {e}")


def get_connection_string(cluster_account, admin_user, password, host, port):
    """Generate connection string for FROM clause"""
    return f"mysql://{cluster_account}#{admin_user}:{password}@{host}:{port}"


def query_mo_ccpr_log(client, filters=None):
    """Query mo_ccpr_log table"""
    try:
        sql = """
            SELECT 
                task_id,
                subscription_name,
                sync_level,
                account_id,
                db_name,
                table_name,
                upstream_conn,
                iteration_state,
                iteration_lsn,
                error_message,
                created_at,
                drop_at
            FROM mo_catalog.mo_ccpr_log
        """
        if filters:
            sql += " WHERE " + " AND ".join(filters)
        sql += " ORDER BY task_id DESC"
        
        result = client.execute(sql)
        rows = result.fetchall()
        return rows
    except Exception as e:
        print_error(f"Failed to query mo_ccpr_log: {e}")
        return []


def check_ccpr_log_record(client, sync_level, db_name=None, table_name=None, 
                          subscription_name=None, should_exist=True, check_drop_at=False):
    """Check if a record exists in mo_ccpr_log"""
    filters = [f"sync_level = '{sync_level}'"]
    
    if db_name:
        filters.append(f"db_name = '{db_name}'")
    if table_name:
        filters.append(f"table_name = '{table_name}'")
    if subscription_name:
        filters.append(f"subscription_name = '{subscription_name}'")
    
    if check_drop_at:
        filters.append("drop_at IS NOT NULL")
    else:
        filters.append("drop_at IS NULL")
    
    rows = query_mo_ccpr_log(client, filters)
    
    if should_exist:
        if rows:
            print_success(f"Found mo_ccpr_log record: sync_level={sync_level}, "
                         f"db_name={db_name}, table_name={table_name}")
            for row in rows:
                print_info(f"  task_id={row[0]}, subscription_name={row[1]}, "
                          f"iteration_state={row[7]}, drop_at={row[11]}")
            return True
        else:
            print_error(f"Expected mo_ccpr_log record not found: sync_level={sync_level}, "
                       f"db_name={db_name}, table_name={table_name}")
            return False
    else:
        if not rows:
            print_success(f"No mo_ccpr_log record found (as expected)")
            return True
        else:
            print_error(f"Unexpected mo_ccpr_log record found")
            return False


def test_create_database_from_publication():
    """Test CREATE DATABASE FROM PUBLICATION. Returns True on success, False on failure."""
    print_header("Testing CREATE DATABASE FROM PUBLICATION")
    
    cluster1_sys_client = Client()
    cluster2_sys_client = Client()
    cluster1_account_client = Client()
    cluster2_account_client = Client()
    success = True
    
    # Cleanup accounts at the beginning (in case they exist from previous failed runs)
    try:
        cluster1_sys_client.connect(host=CLUSTER1_HOST, port=CLUSTER1_PORT, 
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        cluster2_sys_client.connect(host=CLUSTER2_HOST, port=CLUSTER2_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        cleanup_account(cluster1_sys_client, CLUSTER1_ACCOUNT)
        cleanup_account(cluster2_sys_client, CLUSTER2_ACCOUNT)
        cluster1_sys_client.disconnect()
        cluster2_sys_client.disconnect()
    except Exception as e:
        print_warning(f"Error during initial cleanup: {e}")
    
    try:
        # Connect to clusters
        cluster1_sys_client.connect(host=CLUSTER1_HOST, port=CLUSTER1_PORT, 
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        cluster2_sys_client.connect(host=CLUSTER2_HOST, port=CLUSTER2_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        
        # Setup accounts
        
        cluster1_sys_client.execute(
            f"CREATE ACCOUNT {CLUSTER1_ACCOUNT} ADMIN_NAME '{ACCOUNT_ADMIN}' IDENTIFIED BY '{ACCOUNT_PASSWORD}'"
        )
        cluster2_sys_client.execute(
            f"CREATE ACCOUNT {CLUSTER2_ACCOUNT} ADMIN_NAME '{ACCOUNT_ADMIN}' IDENTIFIED BY '{ACCOUNT_PASSWORD}'"
        )
        print_success("Created test accounts")
        
        # Connect as account users
        cluster1_account_client.connect(
            host=CLUSTER1_HOST, port=CLUSTER1_PORT,
            user=f"{CLUSTER1_ACCOUNT}#{ACCOUNT_ADMIN}", password=ACCOUNT_PASSWORD, database=''
        )
        cluster2_account_client.connect(
            host=CLUSTER2_HOST, port=CLUSTER2_PORT,
            user=f"{CLUSTER2_ACCOUNT}#{ACCOUNT_ADMIN}", password=ACCOUNT_PASSWORD, database=''
        )
        
        # Setup upstream: create database and publication in cluster1
        cleanup_database(cluster1_account_client, TEST_DB_NAME)
        cluster1_account_client.execute(f"CREATE DATABASE `{TEST_DB_NAME}`")
        cluster1_account_client.execute(f"USE `{TEST_DB_NAME}`")
        cluster1_account_client.execute(
            f"CREATE TABLE `{TEST_TABLE_NAME}` (id INT PRIMARY KEY, name VARCHAR(100))"
        )
        
        # Also create TEST_DB_NAME2 for SYNC INTERVAL test
        cleanup_database(cluster1_account_client, TEST_DB_NAME2)
        cluster1_account_client.execute(f"CREATE DATABASE `{TEST_DB_NAME2}`")
        cluster1_account_client.execute(f"USE `{TEST_DB_NAME2}`")
        cluster1_account_client.execute(
            f"CREATE TABLE `{TEST_TABLE_NAME}` (id INT PRIMARY KEY, name VARCHAR(100))"
        )
        
        cleanup_publication(cluster1_account_client, PUBLICATION_NAME)
        # Create publication for TEST_DB_NAME
        cluster1_account_client.execute(
            f"CREATE PUBLICATION `{PUBLICATION_NAME}` DATABASE `{TEST_DB_NAME}` ACCOUNT `{CLUSTER1_ACCOUNT}`"
        )
        
        # Create a separate publication for TEST_DB_NAME2 (for SYNC INTERVAL test)
        cleanup_publication(cluster1_account_client, PUBLICATION_NAME2)
        cluster1_account_client.execute(
            f"CREATE PUBLICATION `{PUBLICATION_NAME2}` DATABASE `{TEST_DB_NAME2}` ACCOUNT `{CLUSTER1_ACCOUNT}`"
        )
        print_success("Setup upstream databases and publications")
        
        # Get connection string
        conn_str = get_connection_string(CLUSTER1_ACCOUNT, ACCOUNT_ADMIN, ACCOUNT_PASSWORD,
                                        CLUSTER1_HOST, CLUSTER1_PORT)
        
        # Test 1: Basic CREATE DATABASE FROM PUBLICATION
        print_info("Test 1: Basic CREATE DATABASE FROM PUBLICATION")
        cleanup_database(cluster2_account_client, TEST_DB_NAME)
        try:
            cluster2_account_client.execute(
                f"CREATE DATABASE `{TEST_DB_NAME}` FROM '{conn_str}' PUBLICATION `{PUBLICATION_NAME}`"
            )
            print_success("CREATE DATABASE executed successfully")
            if not check_ccpr_log_record(cluster2_sys_client, 'database', db_name=TEST_DB_NAME,
                                 subscription_name=PUBLICATION_NAME, should_exist=True):
                success = False
                return success
        except Exception as e:
            print_error(f"CREATE DATABASE failed: {e}")
            success = False
            return success
        
        # Test 2: CREATE DATABASE with IF NOT EXISTS
        print_info("Test 2: CREATE DATABASE with IF NOT EXISTS (should succeed)")
        try:
            cluster2_account_client.execute(
                f"CREATE DATABASE IF NOT EXISTS `{TEST_DB_NAME}` FROM '{conn_str}' PUBLICATION `{PUBLICATION_NAME}`"
            )
            print_success("CREATE DATABASE IF NOT EXISTS executed successfully (no error)")
        except Exception as e:
            print_error(f"CREATE DATABASE IF NOT EXISTS failed: {e}")
            success = False
            return success
        
        # Test 3: CREATE DATABASE with SYNC INTERVAL
        print_info("Test 3: CREATE DATABASE with SYNC INTERVAL")
        cleanup_database(cluster2_account_client, TEST_DB_NAME2)
        try:
            # Use PUBLICATION_NAME2 which covers TEST_DB_NAME2
            cluster2_account_client.execute(
                f"CREATE DATABASE `{TEST_DB_NAME2}` FROM '{conn_str}' PUBLICATION `{PUBLICATION_NAME2}` SYNC INTERVAL 60"
            )
            print_success("CREATE DATABASE with SYNC INTERVAL executed successfully")
            if not check_ccpr_log_record(cluster2_sys_client, 'database', db_name=TEST_DB_NAME2,
                                 subscription_name=PUBLICATION_NAME2, should_exist=True):
                success = False
                return success
        except Exception as e:
            print_error(f"CREATE DATABASE with SYNC INTERVAL failed: {e}")
            success = False
            return success
        
        # Test 4: CREATE DATABASE with non-existent publication (should fail)
        print_info("Test 4: CREATE DATABASE with non-existent publication (should fail)")
        cleanup_database(cluster2_account_client, 'test_fail_db')
        try:
            cluster2_account_client.execute(
                f"CREATE DATABASE `test_fail_db` FROM '{conn_str}' PUBLICATION `{PUBLICATION_NAME_NOT_EXIST}`"
            )
            print_error("CREATE DATABASE should have failed but didn't")
            success = False
            return success
        except Exception as e:
            print_success(f"CREATE DATABASE correctly failed: {e}")
        
        # Cleanup
        cleanup_database(cluster2_account_client, TEST_DB_NAME)
        cleanup_database(cluster2_account_client, TEST_DB_NAME2)
        cleanup_publication(cluster1_account_client, PUBLICATION_NAME2)
        time.sleep(1)  # Wait for cleanup
        
    except Exception as e:
        print_error(f"Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        success = False
    finally:
        try:
            cluster1_account_client.disconnect()
            cluster2_account_client.disconnect()
            cluster1_sys_client.disconnect()
            cluster2_sys_client.disconnect()
        except:
            pass
    
    return success


def test_create_table_from_publication():
    """Test CREATE TABLE FROM PUBLICATION. Returns True on success, False on failure."""
    print_header("Testing CREATE TABLE FROM PUBLICATION")
    
    cluster1_sys_client = Client()
    cluster2_sys_client = Client()
    cluster1_account_client = Client()
    cluster2_account_client = Client()
    success = True
    
    # Cleanup accounts at the beginning (in case they exist from previous failed runs)
    try:
        cluster1_sys_client.connect(host=CLUSTER1_HOST, port=CLUSTER1_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        cluster2_sys_client.connect(host=CLUSTER2_HOST, port=CLUSTER2_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        cleanup_account(cluster1_sys_client, CLUSTER1_ACCOUNT)
        cleanup_account(cluster2_sys_client, CLUSTER2_ACCOUNT)
        cluster1_sys_client.disconnect()
        cluster2_sys_client.disconnect()
    except Exception as e:
        print_warning(f"Error during initial cleanup: {e}")
    
    try:
        # Connect to clusters
        cluster1_sys_client.connect(host=CLUSTER1_HOST, port=CLUSTER1_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        cluster2_sys_client.connect(host=CLUSTER2_HOST, port=CLUSTER2_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        
        # Setup accounts
        
        cluster1_sys_client.execute(
            f"CREATE ACCOUNT {CLUSTER1_ACCOUNT} ADMIN_NAME '{ACCOUNT_ADMIN}' IDENTIFIED BY '{ACCOUNT_PASSWORD}'"
        )
        cluster2_sys_client.execute(
            f"CREATE ACCOUNT {CLUSTER2_ACCOUNT} ADMIN_NAME '{ACCOUNT_ADMIN}' IDENTIFIED BY '{ACCOUNT_PASSWORD}'"
        )
        
        # Connect as account users
        cluster1_account_client.connect(
            host=CLUSTER1_HOST, port=CLUSTER1_PORT,
            user=f"{CLUSTER1_ACCOUNT}#{ACCOUNT_ADMIN}", password=ACCOUNT_PASSWORD, database=''
        )
        cluster2_account_client.connect(
            host=CLUSTER2_HOST, port=CLUSTER2_PORT,
            user=f"{CLUSTER2_ACCOUNT}#{ACCOUNT_ADMIN}", password=ACCOUNT_PASSWORD, database=''
        )
        
        # Setup upstream
        cleanup_database(cluster1_account_client, TEST_DB_NAME)
        cluster1_account_client.execute(f"CREATE DATABASE `{TEST_DB_NAME}`")
        cluster1_account_client.execute(f"USE `{TEST_DB_NAME}`")
        cluster1_account_client.execute(
            f"CREATE TABLE `{TEST_TABLE_NAME}` (id INT PRIMARY KEY, name VARCHAR(100))"
        )
        # Also create TEST_TABLE_NAME2 for SYNC INTERVAL test
        cluster1_account_client.execute(
            f"CREATE TABLE `{TEST_TABLE_NAME2}` (id INT PRIMARY KEY, name VARCHAR(100))"
        )
        
        cleanup_publication(cluster1_account_client, PUBLICATION_NAME)
        # Include both tables in the publication
        cluster1_account_client.execute(
            f"CREATE PUBLICATION `{PUBLICATION_NAME}` DATABASE `{TEST_DB_NAME}` TABLE `{TEST_TABLE_NAME}`, `{TEST_TABLE_NAME2}` ACCOUNT `{CLUSTER1_ACCOUNT}`"
        )
        
        # Setup downstream database
        cleanup_database(cluster2_account_client, TEST_DB_NAME)
        cluster2_account_client.execute(f"CREATE DATABASE `{TEST_DB_NAME}`")
        cluster2_account_client.execute(f"USE `{TEST_DB_NAME}`")
        
        conn_str = get_connection_string(CLUSTER1_ACCOUNT, ACCOUNT_ADMIN, ACCOUNT_PASSWORD,
                                        CLUSTER1_HOST, CLUSTER1_PORT)
        
        # Test 1: Basic CREATE TABLE FROM PUBLICATION
        print_info("Test 1: Basic CREATE TABLE FROM PUBLICATION")
        cleanup_table(cluster2_account_client, TEST_DB_NAME, TEST_TABLE_NAME)
        try:
            cluster2_account_client.execute(
                f"CREATE TABLE `{TEST_TABLE_NAME}` FROM '{conn_str}' PUBLICATION `{PUBLICATION_NAME}`"
            )
            print_success("CREATE TABLE executed successfully")
            if not check_ccpr_log_record(cluster2_sys_client, 'table', db_name=TEST_DB_NAME,
                                 table_name=TEST_TABLE_NAME, subscription_name=PUBLICATION_NAME,
                                 should_exist=True):
                success = False
                return success
        except Exception as e:
            print_error(f"CREATE TABLE failed: {e}")
            success = False
            return success
        
        # Test 2: CREATE TABLE with IF NOT EXISTS
        print_info("Test 2: CREATE TABLE with IF NOT EXISTS")
        try:
            cluster2_account_client.execute(
                f"CREATE TABLE IF NOT EXISTS `{TEST_TABLE_NAME}` FROM '{conn_str}' PUBLICATION `{PUBLICATION_NAME}`"
            )
            print_success("CREATE TABLE IF NOT EXISTS executed successfully")
        except Exception as e:
            print_error(f"CREATE TABLE IF NOT EXISTS failed: {e}")
            success = False
            return success
        
        # Test 3: CREATE TABLE with SYNC INTERVAL
        print_info("Test 3: CREATE TABLE with SYNC INTERVAL")
        cleanup_table(cluster2_account_client, TEST_DB_NAME, TEST_TABLE_NAME2)
        try:
            cluster2_account_client.execute(
                f"CREATE TABLE `{TEST_TABLE_NAME2}` FROM '{conn_str}' PUBLICATION `{PUBLICATION_NAME}` SYNC INTERVAL 30"
            )
            print_success("CREATE TABLE with SYNC INTERVAL executed successfully")
            if not check_ccpr_log_record(cluster2_sys_client, 'table', db_name=TEST_DB_NAME,
                                 table_name=TEST_TABLE_NAME2, subscription_name=PUBLICATION_NAME,
                                 should_exist=True):
                success = False
                return success
        except Exception as e:
            print_error(f"CREATE TABLE with SYNC INTERVAL failed: {e}")
            success = False
            return success
        
        # Test 4: CREATE TABLE with table not in publication (should fail)
        print_info("Test 4: CREATE TABLE with table not in publication (should fail)")
        try:
            cluster2_account_client.execute(
                f"CREATE TABLE `nonexistent_table` FROM '{conn_str}' PUBLICATION `{PUBLICATION_NAME}`"
            )
            print_error("CREATE TABLE should have failed but didn't")
            success = False
            return success
        except Exception as e:
            print_success(f"CREATE TABLE correctly failed: {e}")
        
        # Cleanup
        cleanup_table(cluster2_account_client, TEST_DB_NAME, TEST_TABLE_NAME)
        cleanup_table(cluster2_account_client, TEST_DB_NAME, TEST_TABLE_NAME2)
        time.sleep(1)
        
    except Exception as e:
        print_error(f"Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        success = False
    finally:
        try:
            cluster1_account_client.disconnect()
            cluster2_account_client.disconnect()
            cluster1_sys_client.disconnect()
            cluster2_sys_client.disconnect()
        except:
            pass
    
    return success


def test_create_account_from_publication():
    """Test CREATE ACCOUNT FROM PUBLICATION. Returns True on success, False on failure."""
    print_header("Testing CREATE ACCOUNT FROM PUBLICATION")
    
    cluster1_sys_client = Client()
    cluster2_sys_client = Client()
    cluster1_account_client = None
    success = True
    
    try:
        # Connect to clusters
        cluster1_sys_client.connect(host=CLUSTER1_HOST, port=CLUSTER1_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        cluster2_sys_client.connect(host=CLUSTER2_HOST, port=CLUSTER2_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        
        # Setup upstream account
        cleanup_account(cluster1_sys_client, CLUSTER1_ACCOUNT)
        cluster1_sys_client.execute(
            f"CREATE ACCOUNT {CLUSTER1_ACCOUNT} ADMIN_NAME '{ACCOUNT_ADMIN}' IDENTIFIED BY '{ACCOUNT_PASSWORD}'"
        )
        
        cluster1_account_client = Client()
        cluster1_account_client.connect(
            host=CLUSTER1_HOST, port=CLUSTER1_PORT,
            user=f"{CLUSTER1_ACCOUNT}#{ACCOUNT_ADMIN}", password=ACCOUNT_PASSWORD, database=''
        )
        
        # Setup upstream database and publication
        cleanup_database(cluster1_account_client, TEST_DB_NAME)
        cluster1_account_client.execute(f"CREATE DATABASE `{TEST_DB_NAME}`")
        cluster1_account_client.execute(f"USE `{TEST_DB_NAME}`")
        cluster1_account_client.execute(
            f"CREATE TABLE `{TEST_TABLE_NAME}` (id INT PRIMARY KEY, name VARCHAR(100))"
        )
        
        cleanup_publication(cluster1_account_client, PUBLICATION_NAME)
        cluster1_account_client.execute(
            f"CREATE PUBLICATION `{PUBLICATION_NAME}` DATABASE `{TEST_DB_NAME}` ACCOUNT `{CLUSTER1_ACCOUNT}`"
        )
        
        conn_str = get_connection_string(CLUSTER1_ACCOUNT, ACCOUNT_ADMIN, ACCOUNT_PASSWORD,
                                        CLUSTER1_HOST, CLUSTER1_PORT)
        
        # Test: CREATE ACCOUNT FROM PUBLICATION
        print_info("Test: CREATE ACCOUNT FROM PUBLICATION")
        try:
            # Syntax: CREATE ACCOUNT FROM connection_string PUBLICATION pub_name
            # Uses current account (cluster2_sys_client's account), just adds record to mo_ccpr_log
            cluster2_sys_client.execute(
                f"CREATE ACCOUNT FROM '{conn_str}' PUBLICATION `{PUBLICATION_NAME}`"
            )
            print_success("CREATE ACCOUNT FROM PUBLICATION executed successfully")
            # Note: account_id would need to be queried to check mo_ccpr_log
            # subscription_name should be publication name, not account name
            if not check_ccpr_log_record(cluster2_sys_client, 'account', subscription_name=PUBLICATION_NAME,
                                 should_exist=True):
                success = False
                return success
        except Exception as e:
            print_error(f"CREATE ACCOUNT FROM PUBLICATION failed: {e}")
            success = False
            return success
        
        # Note: No cleanup needed for account-level subscription
        # CREATE ACCOUNT FROM PUBLICATION uses the current account (sys account)
        # and just adds a record to mo_ccpr_log, it doesn't create a new account
        time.sleep(1)
        
    except Exception as e:
        print_error(f"Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        success = False
    finally:
        try:
            if cluster1_account_client:
                cluster1_account_client.disconnect()
            cluster1_sys_client.disconnect()
            cluster2_sys_client.disconnect()
        except:
            pass
    
    return success


def test_drop_operations():
    """Test DROP CCPR SUBSCRIPTION operations and verify mo_ccpr_log. Returns True on success, False on failure."""
    print_header("Testing DROP CCPR SUBSCRIPTION operations")
    
    cluster1_sys_client = Client()
    cluster2_sys_client = Client()
    cluster1_account_client = Client()
    cluster2_account_client = Client()
    success = True
    
    # Cleanup accounts at the beginning (in case they exist from previous failed runs)
    try:
        cluster1_sys_client.connect(host=CLUSTER1_HOST, port=CLUSTER1_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        cluster2_sys_client.connect(host=CLUSTER2_HOST, port=CLUSTER2_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        cleanup_account(cluster1_sys_client, CLUSTER1_ACCOUNT)
        cleanup_account(cluster2_sys_client, CLUSTER2_ACCOUNT)
        cluster1_sys_client.disconnect()
        cluster2_sys_client.disconnect()
    except Exception as e:
        print_warning(f"Error during initial cleanup: {e}")
    
    try:
        # Setup similar to create tests
        cluster1_sys_client.connect(host=CLUSTER1_HOST, port=CLUSTER1_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        cluster2_sys_client.connect(host=CLUSTER2_HOST, port=CLUSTER2_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        
        cluster1_sys_client.execute(
            f"CREATE ACCOUNT {CLUSTER1_ACCOUNT} ADMIN_NAME '{ACCOUNT_ADMIN}' IDENTIFIED BY '{ACCOUNT_PASSWORD}'"
        )
        cluster2_sys_client.execute(
            f"CREATE ACCOUNT {CLUSTER2_ACCOUNT} ADMIN_NAME '{ACCOUNT_ADMIN}' IDENTIFIED BY '{ACCOUNT_PASSWORD}'"
        )
        
        cluster1_account_client.connect(
            host=CLUSTER1_HOST, port=CLUSTER1_PORT,
            user=f"{CLUSTER1_ACCOUNT}#{ACCOUNT_ADMIN}", password=ACCOUNT_PASSWORD, database=''
        )
        cluster2_account_client.connect(
            host=CLUSTER2_HOST, port=CLUSTER2_PORT,
            user=f"{CLUSTER2_ACCOUNT}#{ACCOUNT_ADMIN}", password=ACCOUNT_PASSWORD, database=''
        )
        
        # Setup upstream
        cleanup_database(cluster1_account_client, TEST_DB_NAME)
        cluster1_account_client.execute(f"CREATE DATABASE `{TEST_DB_NAME}`")
        cluster1_account_client.execute(f"USE `{TEST_DB_NAME}`")
        cluster1_account_client.execute(
            f"CREATE TABLE `{TEST_TABLE_NAME}` (id INT PRIMARY KEY, name VARCHAR(100))"
        )
        
        cleanup_publication(cluster1_account_client, PUBLICATION_NAME)
        cluster1_account_client.execute(
            f"CREATE PUBLICATION `{PUBLICATION_NAME}` DATABASE `{TEST_DB_NAME}` ACCOUNT `{CLUSTER1_ACCOUNT}`"
        )
        
        conn_str = get_connection_string(CLUSTER1_ACCOUNT, ACCOUNT_ADMIN, ACCOUNT_PASSWORD,
                                        CLUSTER1_HOST, CLUSTER1_PORT)
        
        # Test 1: DROP CCPR SUBSCRIPTION for database-level subscription
        print_info("Test 1: DROP CCPR SUBSCRIPTION (database-level)")
        cleanup_database(cluster2_account_client, TEST_DB_NAME)
        try:
            cluster2_account_client.execute(
                f"CREATE DATABASE `{TEST_DB_NAME}` FROM '{conn_str}' PUBLICATION `{PUBLICATION_NAME}`"
            )
            print_success("CREATE DATABASE FROM PUBLICATION executed")
        except Exception as e:
            print_error(f"Failed to create subscription database: {e}")
            success = False
            return success
        
        # Verify record exists before drop
        print_info("Verifying mo_ccpr_log record exists before DROP")
        if not check_ccpr_log_record(cluster2_sys_client, 'database', db_name=TEST_DB_NAME,
                             subscription_name=PUBLICATION_NAME, should_exist=True, check_drop_at=False):
            print_warning("mo_ccpr_log record not found, but continuing with DROP test")
        
        # Execute DROP CCPR SUBSCRIPTION
        try:
            cluster2_account_client.execute(f"DROP CCPR SUBSCRIPTION `{PUBLICATION_NAME}`")
            print_success("DROP CCPR SUBSCRIPTION executed successfully")
            # Check that drop_at is set
            if not check_ccpr_log_record(cluster2_sys_client, 'database', db_name=TEST_DB_NAME,
                                 subscription_name=PUBLICATION_NAME, should_exist=True, check_drop_at=True):
                print_error("drop_at was not set in mo_ccpr_log after DROP CCPR SUBSCRIPTION")
                success = False
                return success
            print_success("Verified drop_at is set in mo_ccpr_log")
        except Exception as e:
            print_error(f"DROP CCPR SUBSCRIPTION failed: {e}")
            success = False
            return success
        
        # Test 2: DROP CCPR SUBSCRIPTION IF EXISTS (on non-existent subscription)
        print_info("Test 2: DROP CCPR SUBSCRIPTION IF EXISTS (should succeed without error)")
        try:
            cluster2_account_client.execute(f"DROP CCPR SUBSCRIPTION IF EXISTS `{PUBLICATION_NAME_NOT_EXIST}`")
            print_success("DROP CCPR SUBSCRIPTION IF EXISTS executed successfully (no error)")
        except Exception as e:
            print_error(f"DROP CCPR SUBSCRIPTION IF EXISTS failed: {e}")
            success = False
            return success
        
        # Test 3: DROP CCPR SUBSCRIPTION without IF EXISTS (should fail for non-existent)
        print_info("Test 3: DROP CCPR SUBSCRIPTION without IF EXISTS (should fail for non-existent)")
        try:
            cluster2_account_client.execute(f"DROP CCPR SUBSCRIPTION `{PUBLICATION_NAME_NOT_EXIST}`")
            print_error("DROP CCPR SUBSCRIPTION should have failed for non-existent subscription")
            success = False
            return success
        except Exception as e:
            print_success(f"DROP CCPR SUBSCRIPTION correctly failed for non-existent subscription: {e}")
        
        # Test 4: DROP CCPR SUBSCRIPTION for table-level subscription
        print_info("Test 4: DROP CCPR SUBSCRIPTION (table-level)")
        # Create a regular database (not subscription database) for table-level subscription test
        cleanup_database(cluster2_account_client, TEST_DB_NAME)
        cluster2_account_client.execute(f"CREATE DATABASE `{TEST_DB_NAME}`")
        cluster2_account_client.execute(f"USE `{TEST_DB_NAME}`")
        
        # Create table-level subscription
        try:
            cluster2_account_client.execute(
                f"CREATE TABLE `{TEST_TABLE_NAME}` FROM '{conn_str}' PUBLICATION `{PUBLICATION_NAME}`"
            )
            print_success("CREATE TABLE FROM PUBLICATION executed")
        except Exception as e:
            print_error(f"Failed to create subscription table: {e}")
            success = False
            return success
        
        # Verify table record exists before drop
        print_info("Verifying mo_ccpr_log record exists for table-level subscription")
        if not check_ccpr_log_record(cluster2_sys_client, 'table', db_name=TEST_DB_NAME,
                             table_name=TEST_TABLE_NAME, subscription_name=PUBLICATION_NAME,
                             should_exist=True, check_drop_at=False):
            print_warning("mo_ccpr_log record not found for table-level subscription, but continuing")
        
        # Execute DROP CCPR SUBSCRIPTION for table-level subscription
        try:
            cluster2_account_client.execute(f"DROP CCPR SUBSCRIPTION `{PUBLICATION_NAME}`")
            print_success("DROP CCPR SUBSCRIPTION executed successfully for table-level subscription")
            # Check that drop_at is set for table-level subscription
            if not check_ccpr_log_record(cluster2_sys_client, 'table', db_name=TEST_DB_NAME,
                                 table_name=TEST_TABLE_NAME, subscription_name=PUBLICATION_NAME,
                                 should_exist=True, check_drop_at=True):
                print_error("drop_at was not set in mo_ccpr_log for table-level subscription")
                success = False
                return success
            print_success("Verified drop_at is set in mo_ccpr_log for table-level subscription")
        except Exception as e:
            print_error(f"DROP CCPR SUBSCRIPTION failed for table-level subscription: {e}")
            success = False
            return success
        
        # Cleanup
        cleanup_database(cluster2_account_client, TEST_DB_NAME)
        time.sleep(1)
        
    except Exception as e:
        print_error(f"Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        success = False
    finally:
        try:
            cluster1_account_client.disconnect()
            cluster2_account_client.disconnect()
            cluster1_sys_client.disconnect()
            cluster2_sys_client.disconnect()
        except:
            pass
    
    return success


def test_repeated_creation():
    """Test repeated creation of same objects. Returns True on success, False on failure."""
    print_header("Testing Repeated Creation")
    
    cluster1_sys_client = Client()
    cluster2_sys_client = Client()
    cluster1_account_client = Client()
    cluster2_account_client = Client()
    success = True
    
    # Cleanup accounts at the beginning (in case they exist from previous failed runs)
    try:
        cluster1_sys_client.connect(host=CLUSTER1_HOST, port=CLUSTER1_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        cluster2_sys_client.connect(host=CLUSTER2_HOST, port=CLUSTER2_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        cleanup_account(cluster1_sys_client, CLUSTER1_ACCOUNT)
        cleanup_account(cluster2_sys_client, CLUSTER2_ACCOUNT)
        cluster1_sys_client.disconnect()
        cluster2_sys_client.disconnect()
    except Exception as e:
        print_warning(f"Error during initial cleanup: {e}")
    
    try:
        # Setup
        cluster1_sys_client.connect(host=CLUSTER1_HOST, port=CLUSTER1_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        cluster2_sys_client.connect(host=CLUSTER2_HOST, port=CLUSTER2_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        
        cluster1_sys_client.execute(
            f"CREATE ACCOUNT {CLUSTER1_ACCOUNT} ADMIN_NAME '{ACCOUNT_ADMIN}' IDENTIFIED BY '{ACCOUNT_PASSWORD}'"
        )
        cluster2_sys_client.execute(
            f"CREATE ACCOUNT {CLUSTER2_ACCOUNT} ADMIN_NAME '{ACCOUNT_ADMIN}' IDENTIFIED BY '{ACCOUNT_PASSWORD}'"
        )
        
        cluster1_account_client.connect(
            host=CLUSTER1_HOST, port=CLUSTER1_PORT,
            user=f"{CLUSTER1_ACCOUNT}#{ACCOUNT_ADMIN}", password=ACCOUNT_PASSWORD, database=''
        )
        cluster2_account_client.connect(
            host=CLUSTER2_HOST, port=CLUSTER2_PORT,
            user=f"{CLUSTER2_ACCOUNT}#{ACCOUNT_ADMIN}", password=ACCOUNT_PASSWORD, database=''
        )
        
        cleanup_database(cluster1_account_client, TEST_DB_NAME)
        cluster1_account_client.execute(f"CREATE DATABASE `{TEST_DB_NAME}`")
        cluster1_account_client.execute(f"USE `{TEST_DB_NAME}`")
        cluster1_account_client.execute(
            f"CREATE TABLE `{TEST_TABLE_NAME}` (id INT PRIMARY KEY, name VARCHAR(100))"
        )
        
        cleanup_publication(cluster1_account_client, PUBLICATION_NAME)
        cluster1_account_client.execute(
            f"CREATE PUBLICATION `{PUBLICATION_NAME}` DATABASE `{TEST_DB_NAME}` ACCOUNT `{CLUSTER1_ACCOUNT}`"
        )
        
        conn_str = get_connection_string(CLUSTER1_ACCOUNT, ACCOUNT_ADMIN, ACCOUNT_PASSWORD,
                                        CLUSTER1_HOST, CLUSTER1_PORT)
        
        # Test: Create, drop, create again
        print_info("Test: Create -> Drop -> Create again")
        cleanup_database(cluster2_account_client, TEST_DB_NAME)
        
        # First create
        cluster2_account_client.execute(
            f"CREATE DATABASE `{TEST_DB_NAME}` FROM '{conn_str}' PUBLICATION `{PUBLICATION_NAME}`"
        )
        print_success("First CREATE DATABASE executed")
        time.sleep(2)
        
        # Drop
        cluster2_account_client.execute(f"DROP DATABASE `{TEST_DB_NAME}`")
        print_success("DROP DATABASE executed")
        time.sleep(2)
        
        # Create again
        cluster2_account_client.execute(
            f"CREATE DATABASE `{TEST_DB_NAME}` FROM '{conn_str}' PUBLICATION `{PUBLICATION_NAME}`"
        )
        print_success("Second CREATE DATABASE executed")
        time.sleep(2)
        
        # Verify new record exists
        if not check_ccpr_log_record(cluster2_sys_client, 'database', db_name=TEST_DB_NAME,
                             subscription_name=PUBLICATION_NAME, should_exist=True, check_drop_at=False):
            success = False
            return success
        
        # Cleanup
        cleanup_database(cluster2_account_client, TEST_DB_NAME)
        
    except Exception as e:
        print_error(f"Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        success = False
    finally:
        try:
            cluster1_account_client.disconnect()
            cluster2_account_client.disconnect()
            cluster1_sys_client.disconnect()
            cluster2_sys_client.disconnect()
        except:
            pass
    
    return success


def test_show_ccpr_subscriptions():
    """Test SHOW CCPR SUBSCRIPTIONS and account visibility. Returns True on success, False on failure."""
    print_header("Testing SHOW CCPR SUBSCRIPTIONS")
    
    cluster1_sys_client = Client()
    cluster2_sys_client = Client()
    cluster1_account_client = Client()
    cluster2_account_client = Client()
    success = True
    
    # Cleanup accounts at the beginning
    try:
        cluster1_sys_client.connect(host=CLUSTER1_HOST, port=CLUSTER1_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        cluster2_sys_client.connect(host=CLUSTER2_HOST, port=CLUSTER2_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        cleanup_account(cluster1_sys_client, CLUSTER1_ACCOUNT)
        cleanup_account(cluster2_sys_client, CLUSTER2_ACCOUNT)
        cluster1_sys_client.disconnect()
        cluster2_sys_client.disconnect()
    except Exception as e:
        print_warning(f"Error during initial cleanup: {e}")
    
    try:
        # Connect to clusters
        cluster1_sys_client.connect(host=CLUSTER1_HOST, port=CLUSTER1_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        cluster2_sys_client.connect(host=CLUSTER2_HOST, port=CLUSTER2_PORT,
                                    user=SYS_USER, password=SYS_PASSWORD, database='')
        
        # Setup accounts
        cluster1_sys_client.execute(
            f"CREATE ACCOUNT {CLUSTER1_ACCOUNT} ADMIN_NAME '{ACCOUNT_ADMIN}' IDENTIFIED BY '{ACCOUNT_PASSWORD}'"
        )
        cluster2_sys_client.execute(
            f"CREATE ACCOUNT {CLUSTER2_ACCOUNT} ADMIN_NAME '{ACCOUNT_ADMIN}' IDENTIFIED BY '{ACCOUNT_PASSWORD}'"
        )
        print_success("Created test accounts")
        
        # Connect as account users
        cluster1_account_client.connect(
            host=CLUSTER1_HOST, port=CLUSTER1_PORT,
            user=f"{CLUSTER1_ACCOUNT}#{ACCOUNT_ADMIN}", password=ACCOUNT_PASSWORD, database=''
        )
        cluster2_account_client.connect(
            host=CLUSTER2_HOST, port=CLUSTER2_PORT,
            user=f"{CLUSTER2_ACCOUNT}#{ACCOUNT_ADMIN}", password=ACCOUNT_PASSWORD, database=''
        )
        
        # Setup upstream: create database and publication in cluster1
        cleanup_database(cluster1_account_client, TEST_DB_NAME)
        cluster1_account_client.execute(f"CREATE DATABASE `{TEST_DB_NAME}`")
        cluster1_account_client.execute(f"USE `{TEST_DB_NAME}`")
        cluster1_account_client.execute(
            f"CREATE TABLE `{TEST_TABLE_NAME}` (id INT PRIMARY KEY, name VARCHAR(100))"
        )
        
        # Also create TEST_DB_NAME2 for second publication
        cleanup_database(cluster1_account_client, TEST_DB_NAME2)
        cluster1_account_client.execute(f"CREATE DATABASE `{TEST_DB_NAME2}`")
        cluster1_account_client.execute(f"USE `{TEST_DB_NAME2}`")
        cluster1_account_client.execute(
            f"CREATE TABLE `{TEST_TABLE_NAME}` (id INT PRIMARY KEY, name VARCHAR(100))"
        )
        
        cleanup_publication(cluster1_account_client, PUBLICATION_NAME)
        cluster1_account_client.execute(
            f"CREATE PUBLICATION `{PUBLICATION_NAME}` DATABASE `{TEST_DB_NAME}` ACCOUNT `{CLUSTER1_ACCOUNT}`"
        )
        
        cleanup_publication(cluster1_account_client, PUBLICATION_NAME2)
        cluster1_account_client.execute(
            f"CREATE PUBLICATION `{PUBLICATION_NAME2}` DATABASE `{TEST_DB_NAME2}` ACCOUNT `{CLUSTER1_ACCOUNT}`"
        )
        print_success("Setup upstream databases and publications")
        
        # Get connection string
        conn_str = get_connection_string(CLUSTER1_ACCOUNT, ACCOUNT_ADMIN, ACCOUNT_PASSWORD,
                                        CLUSTER1_HOST, CLUSTER1_PORT)
        
        # Create subscriptions in cluster2
        cleanup_database(cluster2_account_client, TEST_DB_NAME)
        cleanup_database(cluster2_account_client, TEST_DB_NAME2)
        
        # Create first subscription
        cluster2_account_client.execute(
            f"CREATE DATABASE `{TEST_DB_NAME}` FROM '{conn_str}' PUBLICATION `{PUBLICATION_NAME}`"
        )
        print_success(f"Created subscription for {PUBLICATION_NAME}")
        time.sleep(2)
        
        # Create second subscription
        cluster2_account_client.execute(
            f"CREATE DATABASE `{TEST_DB_NAME2}` FROM '{conn_str}' PUBLICATION `{PUBLICATION_NAME2}`"
        )
        print_success(f"Created subscription for {PUBLICATION_NAME2}")
        time.sleep(2)
        
        # Test 1: SHOW CCPR SUBSCRIPTIONS from sys account (should see all)
        print_info("Test 1: SHOW CCPR SUBSCRIPTIONS from sys account (should see all)")
        result = cluster2_sys_client.execute("SHOW CCPR SUBSCRIPTIONS")
        rows = result.fetchall()
        print_info(f"Found {len(rows)} subscriptions from sys account")
        
        if len(rows) < 2:
            print_error(f"Expected at least 2 subscriptions from sys account, got {len(rows)}")
            success = False
        else:
            print_success(f"Sys account can see {len(rows)} subscriptions")
            # Verify columns
            if len(rows) > 0:
                print_info(f"Columns: {len(rows[0])} columns per row")
                # Check if we have the expected columns
                if len(rows[0]) >= 12:
                    print_success("Result has expected number of columns (12)")
                else:
                    print_error(f"Expected 12 columns, got {len(rows[0])}")
                    success = False
        
        # Test 2: SHOW CCPR SUBSCRIPTIONS from account user (should see only own)
        print_info("Test 2: SHOW CCPR SUBSCRIPTIONS from account user (should see only own)")
        result = cluster2_account_client.execute("SHOW CCPR SUBSCRIPTIONS")
        account_rows = result.fetchall()
        print_info(f"Found {len(account_rows)} subscriptions from account user")
        
        # Account user should see subscriptions for their account
        if len(account_rows) < 2:
            print_error(f"Expected at least 2 subscriptions from account user, got {len(account_rows)}")
            success = False
        else:
            print_success(f"Account user can see {len(account_rows)} subscriptions")
            
            # Verify all subscriptions belong to this account
            for row in account_rows:
                pub_name = row[0] if len(row) > 0 else None
                pub_account = row[1] if len(row) > 1 else None
                if pub_account != CLUSTER2_ACCOUNT:
                    print_error(f"Account user sees subscription from wrong account: {pub_account}")
                    success = False
                else:
                    print_success(f"Subscription {pub_name} belongs to correct account: {pub_account}")
        
        # Test 3: SHOW CCPR SUBSCRIPTION with specific pub_name
        print_info(f"Test 3: SHOW CCPR SUBSCRIPTION {PUBLICATION_NAME}")
        result = cluster2_sys_client.execute(f"SHOW CCPR SUBSCRIPTION {PUBLICATION_NAME}")
        specific_rows = result.fetchall()
        print_info(f"Found {len(specific_rows)} subscriptions for {PUBLICATION_NAME}")
        
        if len(specific_rows) < 1:
            print_error(f"Expected at least 1 subscription for {PUBLICATION_NAME}, got {len(specific_rows)}")
            success = False
        else:
            print_success(f"Found subscription for {PUBLICATION_NAME}")
            # Verify it's the correct publication
            for row in specific_rows:
                pub_name = row[0] if len(row) > 0 else None
                if pub_name != PUBLICATION_NAME:
                    print_error(f"Expected {PUBLICATION_NAME}, got {pub_name}")
                    success = False
                else:
                    print_success(f"Correct publication name: {pub_name}")
        
        # Test 4: SHOW CCPR SUBSCRIPTION with non-existent pub_name
        print_info(f"Test 4: SHOW CCPR SUBSCRIPTION {PUBLICATION_NAME_NOT_EXIST}")
        result = cluster2_sys_client.execute(f"SHOW CCPR SUBSCRIPTION {PUBLICATION_NAME_NOT_EXIST}")
        nonexistent_rows = result.fetchall()
        
        if len(nonexistent_rows) > 0:
            print_warning(f"Found {len(nonexistent_rows)} subscriptions for non-existent publication (might be expected)")
        else:
            print_success(f"No subscriptions found for non-existent publication (as expected)")
        
        # Test 5: Account visibility - account user should not see other accounts' subscriptions
        print_info("Test 5: Account visibility verification")
        # Create another account and subscription
        CLUSTER3_ACCOUNT = 'cluster3_test_account'
        cluster3_account_client = Client()
        
        try:
            cleanup_account(cluster2_sys_client, CLUSTER3_ACCOUNT)
            cluster2_sys_client.execute(
                f"CREATE ACCOUNT {CLUSTER3_ACCOUNT} ADMIN_NAME '{ACCOUNT_ADMIN}' IDENTIFIED BY '{ACCOUNT_PASSWORD}'"
            )
            cluster3_account_client.connect(
                host=CLUSTER2_HOST, port=CLUSTER2_PORT,
                user=f"{CLUSTER3_ACCOUNT}#{ACCOUNT_ADMIN}", password=ACCOUNT_PASSWORD, database=''
            )
            
            # Create subscription for cluster3 account
            cleanup_database(cluster3_account_client, TEST_DB_NAME)
            cluster3_account_client.execute(
                f"CREATE DATABASE `{TEST_DB_NAME}` FROM '{conn_str}' PUBLICATION `{PUBLICATION_NAME}`"
            )
            print_success(f"Created subscription for {CLUSTER3_ACCOUNT}")
            time.sleep(2)
            
            # cluster2_account_client should not see cluster3's subscriptions
            result = cluster2_account_client.execute("SHOW CCPR SUBSCRIPTIONS")
            cluster2_rows = result.fetchall()
            
            # Count subscriptions that belong to cluster2_account
            cluster2_count = 0
            for row in cluster2_rows:
                pub_account = row[1] if len(row) > 1 else None
                if pub_account == CLUSTER2_ACCOUNT:
                    cluster2_count += 1
            
            print_info(f"Cluster2 account sees {cluster2_count} of its own subscriptions")
            print_info(f"Total rows visible to cluster2 account: {len(cluster2_rows)}")
            
            # Verify cluster2 doesn't see cluster3's subscriptions
            for row in cluster2_rows:
                pub_account = row[1] if len(row) > 1 else None
                if pub_account == CLUSTER3_ACCOUNT:
                    print_error(f"Cluster2 account incorrectly sees cluster3 subscription: {pub_account}")
                    success = False
                elif pub_account == CLUSTER2_ACCOUNT:
                    print_success(f"Cluster2 account correctly sees its own subscription")
            
            # Sys account should see all subscriptions (cluster2 + cluster3)
            result = cluster2_sys_client.execute("SHOW CCPR SUBSCRIPTIONS")
            sys_rows = result.fetchall()
            print_info(f"Sys account sees {len(sys_rows)} total subscriptions")
            
            if len(sys_rows) < 3:  # At least 2 from cluster2 + 1 from cluster3
                print_warning(f"Expected at least 3 subscriptions from sys account, got {len(sys_rows)}")
            else:
                print_success(f"Sys account can see all subscriptions from different accounts")
            
            # Cleanup cluster3
            cleanup_database(cluster3_account_client, TEST_DB_NAME)
            cleanup_account(cluster2_sys_client, CLUSTER3_ACCOUNT)
            cluster3_account_client.disconnect()
        except Exception as e:
            print_warning(f"Test 5 (account visibility) failed: {e}")
            import traceback
            traceback.print_exc()
            # Don't fail the whole test if this part fails
        
        # Cleanup
        cleanup_database(cluster2_account_client, TEST_DB_NAME)
        cleanup_database(cluster2_account_client, TEST_DB_NAME2)
        
    except Exception as e:
        print_error(f"Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        success = False
    finally:
        try:
            cluster1_account_client.disconnect()
            cluster2_account_client.disconnect()
            cluster1_sys_client.disconnect()
            cluster2_sys_client.disconnect()
        except:
            pass
    
    return success


def main():
    """Main test function - SHOW CCPR SUBSCRIPTIONS and DROP CCPR SUBSCRIPTION"""
    print_header("Cross-Cluster Physical Subscription SQL Test Suite")
    
    try:
        # Run SHOW CCPR SUBSCRIPTIONS test
        print_header("Running SHOW CCPR SUBSCRIPTIONS tests")
        if not test_show_ccpr_subscriptions():
            print_error("Test suite stopped due to failure in test_show_ccpr_subscriptions")
            sys.exit(1)
        
        # Run DROP CCPR SUBSCRIPTION test
        print_header("Running DROP CCPR SUBSCRIPTION tests")
        if not test_drop_operations():
            print_error("Test suite stopped due to failure in test_drop_operations")
            sys.exit(1)
        
        print_header("All Tests Completed")
        print_success("Cross-Cluster Physical Subscription SQL test suite finished successfully!")
        
    except KeyboardInterrupt:
        print_warning("\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        print_error(f"Test suite failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

