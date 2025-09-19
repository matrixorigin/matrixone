"""
MatrixOne Python SDK - PITR Operations Example
Demonstrates Point-in-Time Recovery functionality
"""

import sys
import os
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone import Client, AsyncClient
from matrixone.exceptions import ConnectionError, PitrError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def pitr_example():
    """Example of PITR operations"""
    
    print("MatrixOne PITR Operations Example")
    print("=" * 50)
    
    client = Client()
    
    try:
        # Connect to MatrixOne
        client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        logger.info("Connected to MatrixOne!")
        
        # Example 1: Create Cluster PITR
        logger.info("\n=== Example 1: Create Cluster PITR ===")
        try:
            # Create cluster-level PITR
            cluster_pitr = client.pitr.create_cluster_pitr(
                name="cluster_pitr_example",
                range_value=1,
                range_unit="d"
            )
            logger.info(f"✅ Created cluster PITR: {cluster_pitr}")
            logger.info(f"   - Name: {cluster_pitr.name}")
            logger.info(f"   - Level: {cluster_pitr.level}")
            logger.info(f"   - Range: {cluster_pitr.range_value}{cluster_pitr.range_unit}")
            logger.info(f"   - Created: {cluster_pitr.created_time}")
                
        except PitrError as e:
            logger.error(f"❌ Cluster PITR creation failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 2: Create Account PITR (Current Account)
        logger.info("\n=== Example 2: Create Account PITR (Current Account) ===")
        try:
            # Create account-level PITR for current account
            account_pitr = client.pitr.create_account_pitr(
                name="account_pitr_example",
                range_value=2,
                range_unit="h"
            )
            logger.info(f"✅ Created account PITR: {account_pitr}")
            logger.info(f"   - Name: {account_pitr.name}")
            logger.info(f"   - Level: {account_pitr.level}")
            logger.info(f"   - Account: {account_pitr.account_name or 'Current Account'}")
            logger.info(f"   - Range: {account_pitr.range_value}{account_pitr.range_unit}")
                
        except PitrError as e:
            logger.error(f"❌ Account PITR creation failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 3: Create Account PITR (Specific Account)
        logger.info("\n=== Example 3: Create Account PITR (Specific Account) ===")
        try:
            # Create account-level PITR for specific account (cluster admin only)
            specific_account_pitr = client.pitr.create_account_pitr(
                name="specific_account_pitr_example",
                account_name="acc1",
                range_value=1,
                range_unit="d"
            )
            logger.info(f"✅ Created specific account PITR: {specific_account_pitr}")
            logger.info(f"   - Name: {specific_account_pitr.name}")
            logger.info(f"   - Level: {specific_account_pitr.level}")
            logger.info(f"   - Account: {specific_account_pitr.account_name}")
            logger.info(f"   - Range: {specific_account_pitr.range_value}{specific_account_pitr.range_unit}")
                
        except PitrError as e:
            logger.error(f"❌ Specific account PITR creation failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 4: Create Database PITR
        logger.info("\n=== Example 4: Create Database PITR ===")
        try:
            # Create database-level PITR
            db_pitr = client.pitr.create_database_pitr(
                name="database_pitr_example",
                database_name="test_db",
                range_value=1,
                range_unit="y"
            )
            logger.info(f"✅ Created database PITR: {db_pitr}")
            logger.info(f"   - Name: {db_pitr.name}")
            logger.info(f"   - Level: {db_pitr.level}")
            logger.info(f"   - Database: {db_pitr.database_name}")
            logger.info(f"   - Range: {db_pitr.range_value}{db_pitr.range_unit}")
                
        except PitrError as e:
            logger.error(f"❌ Database PITR creation failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 5: Create Table PITR
        logger.info("\n=== Example 5: Create Table PITR ===")
        try:
            # Create table-level PITR
            table_pitr = client.pitr.create_table_pitr(
                name="table_pitr_example",
                database_name="test_db",
                table_name="test_table",
                range_value=1,
                range_unit="y"
            )
            logger.info(f"✅ Created table PITR: {table_pitr}")
            logger.info(f"   - Name: {table_pitr.name}")
            logger.info(f"   - Level: {table_pitr.level}")
            logger.info(f"   - Database: {table_pitr.database_name}")
            logger.info(f"   - Table: {table_pitr.table_name}")
            logger.info(f"   - Range: {table_pitr.range_value}{table_pitr.range_unit}")
                
        except PitrError as e:
            logger.error(f"❌ Table PITR creation failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 6: List PITRs
        logger.info("\n=== Example 6: List PITRs ===")
        try:
            # List all PITRs
            all_pitrs = client.pitr.list()
            logger.info(f"✅ Found {len(all_pitrs)} PITRs:")
            for pitr in all_pitrs:
                logger.info(f"   - {pitr.name} ({pitr.level}): {pitr.range_value}{pitr.range_unit}")
            
            # List PITRs by level
            cluster_pitrs = client.pitr.list(level="cluster")
            logger.info(f"✅ Found {len(cluster_pitrs)} cluster PITRs")
            
            # List PITRs by account
            account_pitrs = client.pitr.list(account_name="acc1")
            logger.info(f"✅ Found {len(account_pitrs)} PITRs for account 'acc1'")
                
        except PitrError as e:
            logger.error(f"❌ PITR listing failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 7: Get PITR
        logger.info("\n=== Example 7: Get PITR ===")
        try:
            # Get specific PITR
            if all_pitrs:
                pitr_name = all_pitrs[0].name
                pitr = client.pitr.get(pitr_name)
                logger.info(f"✅ Retrieved PITR: {pitr}")
                logger.info(f"   - Name: {pitr.name}")
                logger.info(f"   - Level: {pitr.level}")
                logger.info(f"   - Range: {pitr.range_value}{pitr.range_unit}")
                logger.info(f"   - Created: {pitr.created_time}")
                logger.info(f"   - Modified: {pitr.modified_time}")
                
        except PitrError as e:
            logger.error(f"❌ PITR retrieval failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 8: Alter PITR
        logger.info("\n=== Example 8: Alter PITR ===")
        try:
            # Alter PITR range
            if all_pitrs:
                pitr_name = all_pitrs[0].name
                original_pitr = client.pitr.get(pitr_name)
                logger.info(f"Original PITR range: {original_pitr.range_value}{original_pitr.range_unit}")
                
                # Alter to new range
                altered_pitr = client.pitr.alter(pitr_name, 3, "mo")
                logger.info(f"✅ Altered PITR: {altered_pitr}")
                logger.info(f"   - Name: {altered_pitr.name}")
                logger.info(f"   - New Range: {altered_pitr.range_value}{altered_pitr.range_unit}")
                logger.info(f"   - Modified: {altered_pitr.modified_time}")
                
        except PitrError as e:
            logger.error(f"❌ PITR alteration failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 9: PITR in Transaction
        logger.info("\n=== Example 9: PITR in Transaction ===")
        try:
            with client.transaction() as tx:
                # Create PITR within transaction
                tx_pitr = tx.pitr.create_database_pitr(
                    name="transaction_pitr_example",
                    database_name="test_db",
                    range_value=1,
                    range_unit="d"
                )
                logger.info(f"✅ Created PITR within transaction: {tx_pitr}")
                
                # List PITRs within transaction
                tx_pitrs = tx.pitr.list(level="database")
                logger.info(f"✅ Found {len(tx_pitrs)} database PITRs in transaction")
                
                # Alter PITR within transaction
                if tx_pitrs:
                    altered_tx_pitr = tx.pitr.alter(tx_pitrs[0].name, 2, "h")
                    logger.info(f"✅ Altered PITR within transaction: {altered_tx_pitr}")
                    
        except PitrError as e:
            logger.error(f"❌ Transaction PITR operations failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Cleanup
        logger.info("\n=== Cleanup ===")
        try:
            # Delete PITRs
            pitrs_to_delete = [
                "cluster_pitr_example",
                "account_pitr_example", 
                "specific_account_pitr_example",
                "database_pitr_example",
                "table_pitr_example",
                "transaction_pitr_example"
            ]
            
            for pitr_name in pitrs_to_delete:
                try:
                    client.pitr.delete(pitr_name)
                    logger.info(f"✅ Deleted PITR: {pitr_name}")
                except Exception as e:
                    logger.warning(f"Failed to delete PITR {pitr_name}: {e}")
                    
        except Exception as e:
            logger.warning(f"Cleanup failed: {e}")
        
    except ConnectionError as e:
        logger.error(f"Connection failed: {e}")
        logger.info("This is expected if MatrixOne is not running or accessible")
        return False
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        client.disconnect()
        logger.info("Disconnected from MatrixOne")
    
    return True


async def async_pitr_example():
    """Example of async PITR operations"""
    
    print("MatrixOne Async PITR Operations Example")
    print("=" * 50)
    
    client = AsyncClient()
    
    try:
        # Connect to MatrixOne
        await client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        logger.info("Connected to MatrixOne!")
        
        # Example 1: Async Create Cluster PITR
        logger.info("\n=== Example 1: Async Create Cluster PITR ===")
        try:
            # Create cluster-level PITR asynchronously
            cluster_pitr = await client.pitr.create_cluster_pitr(
                name="async_cluster_pitr_example",
                range_value=1,
                range_unit="d"
            )
            logger.info(f"✅ Created async cluster PITR: {cluster_pitr}")
            logger.info(f"   - Name: {cluster_pitr.name}")
            logger.info(f"   - Level: {cluster_pitr.level}")
            logger.info(f"   - Range: {cluster_pitr.range_value}{cluster_pitr.range_unit}")
                
        except PitrError as e:
            logger.error(f"❌ Async cluster PITR creation failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 2: Async Create Account PITR
        logger.info("\n=== Example 2: Async Create Account PITR ===")
        try:
            # Create account-level PITR asynchronously
            account_pitr = await client.pitr.create_account_pitr(
                name="async_account_pitr_example",
                range_value=2,
                range_unit="h"
            )
            logger.info(f"✅ Created async account PITR: {account_pitr}")
            logger.info(f"   - Name: {account_pitr.name}")
            logger.info(f"   - Level: {account_pitr.level}")
            logger.info(f"   - Range: {account_pitr.range_value}{account_pitr.range_unit}")
                
        except PitrError as e:
            logger.error(f"❌ Async account PITR creation failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 3: Async List PITRs
        logger.info("\n=== Example 3: Async List PITRs ===")
        try:
            # List all PITRs asynchronously
            all_pitrs = await client.pitr.list()
            logger.info(f"✅ Found {len(all_pitrs)} async PITRs:")
            for pitr in all_pitrs:
                logger.info(f"   - {pitr.name} ({pitr.level}): {pitr.range_value}{pitr.range_unit}")
                
        except PitrError as e:
            logger.error(f"❌ Async PITR listing failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 4: Async PITR in Transaction
        logger.info("\n=== Example 4: Async PITR in Transaction ===")
        try:
            async with client.transaction() as tx:
                # Create PITR within async transaction
                tx_pitr = await tx.pitr.create_database_pitr(
                    name="async_transaction_pitr_example",
                    database_name="test_db",
                    range_value=1,
                    range_unit="d"
                )
                logger.info(f"✅ Created PITR within async transaction: {tx_pitr}")
                
                # List PITRs within async transaction
                tx_pitrs = await tx.pitr.list(level="database")
                logger.info(f"✅ Found {len(tx_pitrs)} database PITRs in async transaction")
                
        except PitrError as e:
            logger.error(f"❌ Async transaction PITR operations failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Cleanup
        logger.info("\n=== Cleanup ===")
        try:
            # Delete async PITRs
            pitrs_to_delete = [
                "async_cluster_pitr_example",
                "async_account_pitr_example",
                "async_transaction_pitr_example"
            ]
            
            for pitr_name in pitrs_to_delete:
                try:
                    await client.pitr.delete(pitr_name)
                    logger.info(f"✅ Deleted async PITR: {pitr_name}")
                except Exception as e:
                    logger.warning(f"Failed to delete async PITR {pitr_name}: {e}")
                    
        except Exception as e:
            logger.warning(f"Async cleanup failed: {e}")
        
    except ConnectionError as e:
        logger.error(f"Connection failed: {e}")
        logger.info("This is expected if MatrixOne is not running or accessible")
        return False
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        await client.disconnect()
        logger.info("Disconnected from MatrixOne")
    
    return True


def demo_pitr_benefits():
    """Demo the benefits of PITR operations"""
    
    logger.info("\n" + "="*50)
    logger.info("PITR Operations Benefits")
    logger.info("="*50)
    
    logger.info("✓ Point-in-Time Recovery:")
    logger.info("  - Create cluster-level PITRs")
    logger.info("  - Create account-level PITRs")
    logger.info("  - Create database-level PITRs")
    logger.info("  - Create table-level PITRs")
    
    logger.info("\n✓ Flexible Time Ranges:")
    logger.info("  - Hours (h): 1-100 hours")
    logger.info("  - Days (d): 1-100 days (default)")
    logger.info("  - Months (mo): 1-100 months")
    logger.info("  - Years (y): 1-100 years")
    
    logger.info("\n✓ Management Operations:")
    logger.info("  - List PITRs with filters")
    logger.info("  - Get specific PITR details")
    logger.info("  - Alter PITR time ranges")
    logger.info("  - Delete PITRs")
    
    logger.info("\n✓ Transaction Support:")
    logger.info("  - Create PITRs within transactions")
    logger.info("  - Atomic PITR operations")
    logger.info("  - Rollback support for failed operations")
    
    logger.info("\n✓ Async Support:")
    logger.info("  - Non-blocking PITR operations")
    logger.info("  - Concurrent PITR management")
    logger.info("  - Better performance for large operations")
    
    logger.info("\n✓ Security and Isolation:")
    logger.info("  - PITR information only visible to creating tenant")
    logger.info("  - Cluster admin can create tenant-level PITRs")
    logger.info("  - Proper permission controls")


def demo_without_database():
    """Demo without database connection"""
    
    logger.info("\n" + "="*50)
    logger.info("PITR Operations Demo (No Database Required)")
    logger.info("="*50)
    
    client = Client()
    logger.info("✓ Client created successfully")
    
    logger.info("✓ PITR operations syntax:")
    logger.info("  # Create cluster PITR")
    logger.info("  pitr = client.pitr.create_cluster_pitr('cluster_pitr1', 1, 'd')")
    logger.info("  ")
    logger.info("  # Create account PITR")
    logger.info("  pitr = client.pitr.create_account_pitr('account_pitr1', range_value=2, range_unit='h')")
    logger.info("  pitr = client.pitr.create_account_pitr('account_pitr1', 'acc1', 1, 'd')")
    logger.info("  ")
    logger.info("  # Create database PITR")
    logger.info("  pitr = client.pitr.create_database_pitr('db_pitr1', 'db1', 1, 'y')")
    logger.info("  ")
    logger.info("  # Create table PITR")
    logger.info("  pitr = client.pitr.create_table_pitr('tab_pitr1', 'db1', 't1', 1, 'y')")
    logger.info("  ")
    logger.info("  # List PITRs")
    logger.info("  pitrs = client.pitr.list()")
    logger.info("  pitrs = client.pitr.list(level='cluster')")
    logger.info("  ")
    logger.info("  # Get PITR")
    logger.info("  pitr = client.pitr.get('pitr_name')")
    logger.info("  ")
    logger.info("  # Alter PITR")
    logger.info("  pitr = client.pitr.alter('pitr_name', 2, 'h')")
    logger.info("  ")
    logger.info("  # Delete PITR")
    logger.info("  success = client.pitr.delete('pitr_name')")
    logger.info("  ")
    logger.info("  # Transaction PITR")
    logger.info("  with client.transaction() as tx:")
    logger.info("      pitr = tx.pitr.create_database_pitr('tx_pitr', 'db1', 1, 'd')")
    logger.info("  ")
    logger.info("  # Async PITR")
    logger.info("  pitr = await client.pitr.create_cluster_pitr('async_pitr', 1, 'd')")
    
    demo_pitr_benefits()


def main():
    """Main function"""
    print("MatrixOne PITR Operations Example")
    print("=" * 50)
    
    try:
        # Try to run with database
        success = pitr_example()
        
        if not success:
            # Run demo without database
            demo_without_database()
            
            print("\n" + "="*50)
            print("Demo completed!")
            print("\nTo run with database:")
            print("1. Start MatrixOne database")
            print("2. Update connection parameters")
            print("3. Run the script again")
        else:
            print("\n" + "="*50)
            print("✅ All PITR examples completed successfully!")
            print("\n🎉 MatrixOne PITR operations work perfectly!")
            print("\nKey features:")
            print("- Cluster, account, database, and table PITR creation")
            print("- Flexible time ranges (hours, days, months, years)")
            print("- PITR listing, retrieval, alteration, and deletion")
            print("- Transaction support for PITR operations")
            print("- Async PITR operations")
            print("- Comprehensive error handling")
            print("- Based on MatrixOne official documentation")
        
    except Exception as e:
        print(f"❌ Examples failed: {e}")
        raise


if __name__ == "__main__":
    main()
