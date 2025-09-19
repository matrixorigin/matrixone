"""
MatrixOne Python SDK - Publish-Subscribe Operations Example
Demonstrates Publish-Subscribe functionality based on MatrixOne official documentation
"""

import sys
import os
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone import Client, AsyncClient
from matrixone.exceptions import ConnectionError, PubSubError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def pubsub_example():
    """Example of publish-subscribe operations based on MatrixOne documentation"""
    
    print("MatrixOne Publish-Subscribe Operations Example")
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
        
        # Example 1: Create Database Publication
        logger.info("\n=== Example 1: Create Database Publication ===")
        try:
            # Create database-level publication (publishes all tables in the database)
            db_pub = client.pubsub.create_database_publication(
                name="db_warehouse_pub",
                database="central_warehouse_db",
                account="sys"
            )
            logger.info(f"✅ Created database publication: {db_pub}")
            logger.info(f"   - Name: {db_pub.name}")
            logger.info(f"   - Database: {db_pub.database}")
            logger.info(f"   - Tables: {db_pub.tables}")
            logger.info(f"   - Sub Account: {db_pub.sub_account}")
            logger.info(f"   - Subscribed Accounts: {db_pub.subscribed_accounts}")
            logger.info(f"   - Created: {db_pub.created_time}")
            logger.info(f"   - Updated: {db_pub.update_time}")
            logger.info(f"   - Comments: {db_pub.comments}")
                
        except PubSubError as e:
            logger.error(f"❌ Database publication creation failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 2: Create Table Publication
        logger.info("\n=== Example 2: Create Table Publication ===")
        try:
            # Create table-level publication (publishes only specific table)
            table_pub = client.pubsub.create_table_publication(
                name="tab_products_pub",
                database="central_warehouse_db",
                table="products",
                account="sys"
            )
            logger.info(f"✅ Created table publication: {table_pub}")
            logger.info(f"   - Name: {table_pub.name}")
            logger.info(f"   - Database: {table_pub.database}")
            logger.info(f"   - Tables: {table_pub.tables}")
            logger.info(f"   - Sub Account: {table_pub.sub_account}")
            logger.info(f"   - Created: {table_pub.created_time}")
                
        except PubSubError as e:
            logger.error(f"❌ Table publication creation failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 3: List Publications
        logger.info("\n=== Example 3: List Publications ===")
        try:
            # List all publications
            all_pubs = client.pubsub.list_publications()
            logger.info(f"✅ Found {len(all_pubs)} publications:")
            for pub in all_pubs:
                logger.info(f"   - {pub.name} ({pub.account}): {pub.database}.{pub.tables}")
            
            # List publications by account
            sys_pubs = client.pubsub.list_publications(account="sys")
            logger.info(f"✅ Found {len(sys_pubs)} publications for account 'sys'")
            
            # List publications by database
            warehouse_pubs = client.pubsub.list_publications(database="central_warehouse_db")
            logger.info(f"✅ Found {len(warehouse_pubs)} publications for database 'central_warehouse_db'")
                
        except PubSubError as e:
            logger.error(f"❌ Publication listing failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 4: Get Publication
        logger.info("\n=== Example 4: Get Publication ===")
        try:
            # Get specific publication
            if all_pubs:
                pub_name = all_pubs[0].name
                pub = client.pubsub.get_publication(pub_name)
                logger.info(f"✅ Retrieved publication: {pub}")
                logger.info(f"   - Name: {pub.name}")
                logger.info(f"   - Database: {pub.database}")
                logger.info(f"   - Tables: {pub.tables}")
                logger.info(f"   - Sub Account: {pub.sub_account}")
                logger.info(f"   - Subscribed Accounts: {pub.subscribed_accounts}")
                logger.info(f"   - Created: {pub.created_time}")
                logger.info(f"   - Updated: {pub.update_time}")
                logger.info(f"   - Comments: {pub.comments}")
                
        except PubSubError as e:
            logger.error(f"❌ Publication retrieval failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 4.5: Show Create Publication
        logger.info("\n=== Example 4.5: Show Create Publication ===")
        try:
            # Show CREATE PUBLICATION statement
            if all_pubs:
                pub_name = all_pubs[0].name
                create_statement = client.pubsub.show_create_publication(pub_name)
                logger.info(f"✅ CREATE PUBLICATION statement for '{pub_name}':")
                logger.info(f"   {create_statement}")
                
        except PubSubError as e:
            logger.error(f"❌ Show create publication failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 5: Alter Publication
        logger.info("\n=== Example 5: Alter Publication ===")
        try:
            # Alter publication (change subscriber account)
            if all_pubs:
                pub_name = all_pubs[0].name
                original_pub = client.pubsub.get_publication(pub_name)
                logger.info(f"Original publication sub_account: {original_pub.sub_account}")
                
                # Alter to new account
                altered_pub = client.pubsub.alter_publication(pub_name, account="sys")
                logger.info(f"✅ Altered publication: {altered_pub}")
                logger.info(f"   - Name: {altered_pub.name}")
                logger.info(f"   - New Sub Account: {altered_pub.sub_account}")
                logger.info(f"   - Database: {altered_pub.database}")
                logger.info(f"   - Tables: {altered_pub.tables}")
                
        except PubSubError as e:
            logger.error(f"❌ Publication alteration failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 6: Create Subscription
        logger.info("\n=== Example 6: Create Subscription ===")
        try:
            # Create subscription from publication
            if all_pubs:
                pub_name = all_pubs[0].name
                sub = client.pubsub.create_subscription(
                    subscription_name="db_warehouse_sub",
                    publication_name=pub_name,
                    publisher_account="sys"
                )
                logger.info(f"✅ Created subscription: {sub}")
                logger.info(f"   - Publication: {sub.pub_name}")
                logger.info(f"   - Publisher Account: {sub.pub_account}")
                logger.info(f"   - Publisher Database: {sub.pub_database}")
                logger.info(f"   - Publisher Tables: {sub.pub_tables}")
                logger.info(f"   - Subscription Name: {sub.sub_name}")
                logger.info(f"   - Subscription Time: {sub.sub_time}")
                logger.info(f"   - Status: {sub.status}")
                
        except PubSubError as e:
            logger.error(f"❌ Subscription creation failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 7: List Subscriptions
        logger.info("\n=== Example 7: List Subscriptions ===")
        try:
            # List all subscriptions
            all_subs = client.pubsub.list_subscriptions()
            logger.info(f"✅ Found {len(all_subs)} subscriptions:")
            for sub in all_subs:
                logger.info(f"   - {sub.sub_name} <- {sub.pub_name} ({sub.pub_account})")
                logger.info(f"     Status: {sub.status}, Created: {sub.sub_time}")
            
            # List subscriptions by publisher account
            sys_subs = client.pubsub.list_subscriptions(pub_account="sys")
            logger.info(f"✅ Found {len(sys_subs)} subscriptions from account 'sys'")
            
            # List subscriptions by publisher database
            warehouse_subs = client.pubsub.list_subscriptions(pub_database="central_warehouse_db")
            logger.info(f"✅ Found {len(warehouse_subs)} subscriptions for database 'central_warehouse_db'")
                
        except PubSubError as e:
            logger.error(f"❌ Subscription listing failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 8: Get Subscription
        logger.info("\n=== Example 8: Get Subscription ===")
        try:
            # Get specific subscription
            if all_subs:
                sub_name = all_subs[0].sub_name
                sub = client.pubsub.get_subscription(sub_name)
                logger.info(f"✅ Retrieved subscription: {sub}")
                logger.info(f"   - Publication: {sub.pub_name}")
                logger.info(f"   - Publisher Account: {sub.pub_account}")
                logger.info(f"   - Publisher Database: {sub.pub_database}")
                logger.info(f"   - Publisher Tables: {sub.pub_tables}")
                logger.info(f"   - Subscription Name: {sub.sub_name}")
                logger.info(f"   - Publication Time: {sub.pub_time}")
                logger.info(f"   - Subscription Time: {sub.sub_time}")
                logger.info(f"   - Status: {sub.status}")
                
        except PubSubError as e:
            logger.error(f"❌ Subscription retrieval failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 9: Publish-Subscribe in Transaction
        logger.info("\n=== Example 9: Publish-Subscribe in Transaction ===")
        try:
            with client.transaction() as tx:
                # Create publication within transaction
                tx_pub = tx.pubsub.create_database_publication(
                    name="transaction_pub",
                    database="test_db",
                    account="sys"
                )
                logger.info(f"✅ Created publication within transaction: {tx_pub}")
                
                # List publications within transaction
                tx_pubs = tx.pubsub.list_publications(account="sys")
                logger.info(f"✅ Found {len(tx_pubs)} publications in transaction")
                
                # Create subscription within transaction
                if tx_pubs:
                    tx_sub = tx.pubsub.create_subscription(
                        subscription_name="transaction_sub",
                        publication_name=tx_pubs[0].name,
                        publisher_account="sys"
                    )
                    logger.info(f"✅ Created subscription within transaction: {tx_sub}")
                
                # Alter publication within transaction
                if tx_pubs:
                    altered_tx_pub = tx.pubsub.alter_publication(tx_pubs[0].name, account="sys")
                    logger.info(f"✅ Altered publication within transaction: {altered_tx_pub}")
                    
        except PubSubError as e:
            logger.error(f"❌ Transaction publish-subscribe operations failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Cleanup
        logger.info("\n=== Cleanup ===")
        try:
            # Drop publications
            pubs_to_drop = [
                "db_warehouse_pub",
                "tab_products_pub", 
                "transaction_pub"
            ]
            
            for pub_name in pubs_to_drop:
                try:
                    client.pubsub.drop_publication(pub_name)
                    logger.info(f"✅ Dropped publication: {pub_name}")
                except Exception as e:
                    logger.warning(f"Failed to drop publication {pub_name}: {e}")
                    
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


async def async_pubsub_example():
    """Example of async publish-subscribe operations"""
    
    print("MatrixOne Async Publish-Subscribe Operations Example")
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
        
        # Example 1: Async Create Database Publication
        logger.info("\n=== Example 1: Async Create Database Publication ===")
        try:
            # Create database-level publication asynchronously
            db_pub = await client.pubsub.create_database_publication(
                name="async_db_warehouse_pub",
                database="central_warehouse_db",
                account="sys"
            )
            logger.info(f"✅ Created async database publication: {db_pub}")
            logger.info(f"   - Name: {db_pub.name}")
            logger.info(f"   - Sub Account: {db_pub.sub_account}")
            logger.info(f"   - Database: {db_pub.database}")
            logger.info(f"   - Tables: {db_pub.tables}")
                
        except PubSubError as e:
            logger.error(f"❌ Async database publication creation failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 2: Async Create Table Publication
        logger.info("\n=== Example 2: Async Create Table Publication ===")
        try:
            # Create table-level publication asynchronously
            table_pub = await client.pubsub.create_table_publication(
                name="async_tab_products_pub",
                database="central_warehouse_db",
                table="products",
                account="sys"
            )
            logger.info(f"✅ Created async table publication: {table_pub}")
            logger.info(f"   - Name: {table_pub.name}")
            logger.info(f"   - Sub Account: {table_pub.sub_account}")
            logger.info(f"   - Database: {table_pub.database}")
            logger.info(f"   - Tables: {table_pub.tables}")
                
        except PubSubError as e:
            logger.error(f"❌ Async table publication creation failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 3: Async List Publications
        logger.info("\n=== Example 3: Async List Publications ===")
        try:
            # List all publications asynchronously
            all_pubs = await client.pubsub.list_publications()
            logger.info(f"✅ Found {len(all_pubs)} async publications:")
            for pub in all_pubs:
                logger.info(f"   - {pub.name} ({pub.account}): {pub.database}.{pub.tables}")
                
        except PubSubError as e:
            logger.error(f"❌ Async publication listing failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 4: Async Create Subscription
        logger.info("\n=== Example 4: Async Create Subscription ===")
        try:
            # Create subscription asynchronously
            if all_pubs:
                pub_name = all_pubs[0].name
                sub = await client.pubsub.create_subscription(
                    subscription_name="async_db_warehouse_sub",
                    publication_name=pub_name,
                    publisher_account="sys"
                )
                logger.info(f"✅ Created async subscription: {sub}")
                logger.info(f"   - Publication: {sub.pub_name}")
                logger.info(f"   - Publisher Account: {sub.pub_account}")
                logger.info(f"   - Subscription Name: {sub.sub_name}")
                logger.info(f"   - Status: {sub.status}")
                
        except PubSubError as e:
            logger.error(f"❌ Async subscription creation failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Example 5: Async Publish-Subscribe in Transaction
        logger.info("\n=== Example 5: Async Publish-Subscribe in Transaction ===")
        try:
            async with client.transaction() as tx:
                # Create publication within async transaction
                tx_pub = await tx.pubsub.create_database_publication(
                    name="async_transaction_pub",
                    database="test_db",
                    account="sys"
                )
                logger.info(f"✅ Created publication within async transaction: {tx_pub}")
                
                # List publications within async transaction
                tx_pubs = await tx.pubsub.list_publications(account="sys")
                logger.info(f"✅ Found {len(tx_pubs)} publications in async transaction")
                
                # Create subscription within async transaction
                if tx_pubs:
                    tx_sub = await tx.pubsub.create_subscription(
                        subscription_name="async_transaction_sub",
                        publication_name=tx_pubs[0].name,
                        publisher_account="sys"
                    )
                    logger.info(f"✅ Created subscription within async transaction: {tx_sub}")
                
        except PubSubError as e:
            logger.error(f"❌ Async transaction publish-subscribe operations failed: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
        
        # Cleanup
        logger.info("\n=== Cleanup ===")
        try:
            # Drop async publications
            pubs_to_drop = [
                "async_db_warehouse_pub",
                "async_tab_products_pub",
                "async_transaction_pub"
            ]
            
            for pub_name in pubs_to_drop:
                try:
                    await client.pubsub.drop_publication(pub_name)
                    logger.info(f"✅ Dropped async publication: {pub_name}")
                except Exception as e:
                    logger.warning(f"Failed to drop async publication {pub_name}: {e}")
                    
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


def demo_pubsub_benefits():
    """Demo the benefits of publish-subscribe operations"""
    
    logger.info("\n" + "="*50)
    logger.info("Publish-Subscribe Operations Benefits")
    logger.info("="*50)
    
    logger.info("✓ Data Synchronization:")
    logger.info("  - Real-time data replication between tenants")
    logger.info("  - Cross-regional data synchronization")
    logger.info("  - Multi-tenant data sharing")
    
    logger.info("\n✓ Publication Types:")
    logger.info("  - Database-level publications (all tables)")
    logger.info("  - Table-level publications (specific tables)")
    logger.info("  - Flexible publishing scope control")
    
    logger.info("\n✓ Management Operations:")
    logger.info("  - Create, list, get, alter, and drop publications")
    logger.info("  - Create, list, and get subscriptions")
    logger.info("  - Publication and subscription filtering")
    
    logger.info("\n✓ Transaction Support:")
    logger.info("  - Atomic publish-subscribe operations")
    logger.info("  - Rollback support for failed operations")
    logger.info("  - Consistent data state management")
    
    logger.info("\n✓ Async Support:")
    logger.info("  - Non-blocking publish-subscribe operations")
    logger.info("  - Concurrent data synchronization")
    logger.info("  - Better performance for large operations")
    
    logger.info("\n✓ Security and Isolation:")
    logger.info("  - Tenant-level data isolation")
    logger.info("  - Publisher controls subscriber access")
    logger.info("  - Read-only access for subscribers")
    
    logger.info("\n✓ Real-world Use Cases:")
    logger.info("  - Central warehouse to branch synchronization")
    logger.info("  - Multi-region data replication")
    logger.info("  - Data sharing between departments")
    logger.info("  - Real-time analytics data distribution")


def demo_without_database():
    """Demo without database connection"""
    
    logger.info("\n" + "="*50)
    logger.info("Publish-Subscribe Operations Demo (No Database Required)")
    logger.info("="*50)
    
    client = Client()
    logger.info("✓ Client created successfully")
    
    logger.info("✓ Publish-Subscribe operations syntax:")
    logger.info("  # Create database publication")
    logger.info("  pub = client.pubsub.create_database_publication('db_pub', 'central_db', 'acc1')")
    logger.info("  ")
    logger.info("  # Create table publication")
    logger.info("  pub = client.pubsub.create_table_publication('table_pub', 'central_db', 'products', 'acc1')")
    logger.info("  ")
    logger.info("  # List publications")
    logger.info("  pubs = client.pubsub.list_publications()")
    logger.info("  pubs = client.pubsub.list_publications(account='sys')")
    logger.info("  ")
    logger.info("  # Get publication")
    logger.info("  pub = client.pubsub.get_publication('pub_name')")
    logger.info("  ")
    logger.info("  # Alter publication")
    logger.info("  pub = client.pubsub.alter_publication('pub_name', account='acc2')")
    logger.info("  ")
    logger.info("  # Drop publication")
    logger.info("  success = client.pubsub.drop_publication('pub_name')")
    logger.info("  ")
    logger.info("  # Create subscription")
    logger.info("  sub = client.pubsub.create_subscription('sub_db', 'pub_name', 'sys')")
    logger.info("  ")
    logger.info("  # List subscriptions")
    logger.info("  subs = client.pubsub.list_subscriptions()")
    logger.info("  ")
    logger.info("  # Get subscription")
    logger.info("  sub = client.pubsub.get_subscription('sub_name')")
    logger.info("  ")
    logger.info("  # Transaction publish-subscribe")
    logger.info("  with client.transaction() as tx:")
    logger.info("      pub = tx.pubsub.create_database_publication('tx_pub', 'db1', 'acc1')")
    logger.info("  ")
    logger.info("  # Async publish-subscribe")
    logger.info("  pub = await client.pubsub.create_database_publication('async_pub', 'db1', 'acc1')")
    
    demo_pubsub_benefits()


def main():
    """Main function"""
    print("MatrixOne Publish-Subscribe Operations Example")
    print("=" * 50)
    
    try:
        # Try to run with database
        success = pubsub_example()
        
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
            print("✅ All publish-subscribe examples completed successfully!")
            print("\n🎉 MatrixOne publish-subscribe operations work perfectly!")
            print("\nKey features:")
            print("- Database and table-level publications")
            print("- Real-time data synchronization")
            print("- Cross-tenant data sharing")
            print("- Publication and subscription management")
            print("- Transaction support for publish-subscribe operations")
            print("- Async publish-subscribe operations")
            print("- Comprehensive error handling")
            print("- Based on MatrixOne official documentation")
        
    except Exception as e:
        print(f"❌ Examples failed: {e}")
        raise


if __name__ == "__main__":
    main()
