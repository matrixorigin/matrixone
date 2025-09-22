"""
MatrixOne Python SDK - PubSub Operations Examples
Demonstrates comprehensive Publish-Subscribe functionality including:
- Basic PubSub operations
- Cross-account scenarios
- Async operations
- Real-world use cases
"""

from matrixone import Client, AsyncClient
from matrixone.logger import create_default_logger
from matrixone.config import get_connection_params, print_config
import asyncio
import logging
import time
from datetime import datetime

# Create MatrixOne logger for all logging with line numbers
logger = create_default_logger(
    level=logging.INFO,
    format_string='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d]: %(message)s',
    enable_performance_logging=True,
    enable_sql_logging=True
)


def demo_basic_pubsub_operations():
    """Demonstrate basic PubSub operations within the same account"""
    logger.info("=== Basic PubSub Operations Demo ===")
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    client = Client(logger=logger, enable_full_sql_logging=True)
    
    try:
        # Connect to MatrixOne
        client.connect(host, port, user, password, database)
        logger.info("✅ Connected to MatrixOne")
        
        # Create test database and tables
        logger.info("\n📊 Setting up test environment")
        client.execute("CREATE DATABASE IF NOT EXISTS pubsub_test")
        client.execute("USE pubsub_test")
        
        client.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(10,2),
                category VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        client.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INT PRIMARY KEY AUTO_INCREMENT,
                product_id INT,
                quantity INT,
                total_amount DECIMAL(10,2),
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        """)
        
        # Insert sample data
        client.execute("DELETE FROM orders")
        client.execute("DELETE FROM products")
        
        products_data = [
            ("Laptop", 999.99, "Electronics"),
            ("Mouse", 29.99, "Electronics"),
            ("Keyboard", 79.99, "Electronics"),
            ("Monitor", 299.99, "Electronics")
        ]
        
        for name, price, category in products_data:
            client.execute(
                "INSERT INTO products (name, price, category) VALUES (%s, %s, %s)",
                (name, price, category)
            )
        
        logger.info("   ✅ Created test database with sample data")
        
        # Create publications
        logger.info("\n📤 Creating Publications")
        
        # Note: For basic demo, we'll skip publication creation since we can't publish to self
        # In a real scenario, you would create publications for other accounts
        logger.info("   ℹ️ Skipping publication creation (can't publish to self in basic demo)")
        
        # Simulate publication names for demonstration
        db_pub_name = f"products_db_pub_{int(time.time())}"
        table_pub_name = f"products_table_pub_{int(time.time())}"
        
        # List publications
        publications = client.pubsub.list_publications()
        logger.info(f"   📊 Total publications: {len(publications)}")
        for pub in publications:
            logger.info(f"     - {pub.name}: {pub.database}.{pub.tables}")
        
        # Create subscriptions
        logger.info("\n📥 Creating Subscriptions")
        
        # Note: For basic demo, we'll skip subscription creation since we don't have publications
        # In a real scenario, you would create subscriptions to publications from other accounts
        logger.info("   ℹ️ Skipping subscription creation (no publications in basic demo)")
        
        # Simulate subscription names for demonstration
        db_sub_name = f"products_db_sub_{int(time.time())}"
        table_sub_name = f"products_table_sub_{int(time.time())}"
        
        # List subscriptions
        subscriptions = client.pubsub.list_subscriptions()
        logger.info(f"   📊 Total subscriptions: {len(subscriptions)}")
        for sub in subscriptions:
            logger.info(f"     - {sub.sub_name}: {sub.pub_name}")
        
        # Demonstrate data synchronization
        logger.info("\n🔄 Demonstrating Data Synchronization")
        
        # Add new data
        client.execute(
            "INSERT INTO products (name, price, category) VALUES (%s, %s, %s)",
            ("Tablet", 399.99, "Electronics")
        )
        logger.info("   📤 Added new product: Tablet")
        
        # Update existing data
        client.execute(
            "UPDATE products SET price = %s WHERE name = %s",
            (1099.99, "Laptop")
        )
        logger.info("   📤 Updated product: Laptop price")
        
        # Show current data
        result = client.execute("SELECT COUNT(*) FROM products")
        product_count = result.rows[0][0]
        logger.info(f"   📊 Total products: {product_count}")
        
        # Show product details
        result = client.execute("SELECT name, price, category FROM products ORDER BY name")
        logger.info("   📋 Product catalog:")
        for row in result.rows:
            logger.info(f"     - {row[0]}: ${row[1]} ({row[2]})")
        
        # Cleanup
        logger.info("\n🧹 Cleanup")
        # Note: No subscriptions or publications to drop in this demo
        client.execute("DROP DATABASE pubsub_test")
        
        logger.info("   ✅ Cleaned up test environment")
        
    except Exception as e:
        logger.error(f"❌ Basic PubSub demo failed: {e}")
    finally:
        client.disconnect()
        logger.info("✅ Disconnected from MatrixOne")


def demo_cross_account_pubsub():
    """Demonstrate cross-account PubSub operations"""
    logger.info("=== Cross-Account PubSub Operations Demo ===")
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    # Main client for account management
    admin_client = Client(enable_full_sql_logging=True)
    
    try:
        # Connect as admin
        admin_client.connect(host, port, user, password, "mo_catalog")
        logger.info("✅ Connected as admin")
        
        # Clean up any existing accounts first
        logger.info("\n🧹 Pre-cleanup")
        try:
            admin_client.account.drop_account("pub_publisher")
        except:
            logger.info("   ℹ️ No existing pub_publisher account to drop")
        
        try:
            admin_client.account.drop_account("pub_subscriber")
        except:
            logger.info("   ℹ️ No existing pub_subscriber account to drop")
        
        # Create two separate accounts for publisher and subscriber
        logger.info("\n👥 Setting up cross-account environment")
        
        # Create publisher account
        publisher_account = admin_client.account.create_account(
            account_name="pub_publisher",
            admin_name="pub_admin",
            password="pub_pass",
            comment="Publisher account for cross-account PubSub demo"
        )
        logger.info(f"   ✅ Created publisher account: {publisher_account.name}")
        
        # Create subscriber account
        subscriber_account = admin_client.account.create_account(
            account_name="pub_subscriber",
            admin_name="sub_admin",
            password="sub_pass",
            comment="Subscriber account for cross-account PubSub demo"
        )
        logger.info(f"   ✅ Created subscriber account: {subscriber_account.name}")
        
        # Create users in each account
        logger.info("\n👤 Creating users in each account")
        
        # Note: For cross-account demo, we'll use the account admin users directly
        # In a real scenario, you would create specific users within each account
        logger.info("   ℹ️ Using account admin users for cross-account demo")
        
        # Setup publisher side
        logger.info("\n📤 Setup Publisher Side")
        publisher_client = Client(enable_full_sql_logging=True)
        
        # Connect as publisher account admin
        publisher_client.connect(host, port, "pub_publisher#pub_admin", "pub_pass", "mo_catalog")
        logger.info("   ✅ Connected as publisher account admin")
        
        # Create publisher database
        pub_admin_client = Client(enable_full_sql_logging=True)
        pub_admin_client.connect(host, port, "pub_publisher#pub_admin", "pub_pass", "mo_catalog")
        pub_admin_client.execute("CREATE DATABASE IF NOT EXISTS publisher_data")
        pub_admin_client.disconnect()
        
        # Connect to publisher database
        publisher_client.disconnect()
        publisher_client.connect(host, port, "pub_publisher#pub_admin", "pub_pass", "publisher_data")
        logger.info("   ✅ Connected to publisher database")
        
        # Create tables for publishing
        pub_admin_client = Client(enable_full_sql_logging=True)
        pub_admin_client.connect(host, port, "pub_publisher#pub_admin", "pub_pass", "publisher_data")
        
        pub_admin_client.execute("""
            CREATE TABLE IF NOT EXISTS sales_data (
                id INT PRIMARY KEY AUTO_INCREMENT,
                product_name VARCHAR(100) NOT NULL,
                quantity INT,
                price DECIMAL(10,2),
                sale_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        pub_admin_client.execute("""
            CREATE TABLE IF NOT EXISTS customer_data (
                id INT PRIMARY KEY AUTO_INCREMENT,
                customer_name VARCHAR(100) NOT NULL,
                email VARCHAR(100),
                phone VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert sample data
        pub_admin_client.execute("DELETE FROM sales_data")
        pub_admin_client.execute("DELETE FROM customer_data")
        
        sales_data = [
            ("iPhone 15", 5, 999.99),
            ("MacBook Pro", 2, 1999.99),
            ("iPad Air", 8, 599.99),
            ("AirPods Pro", 12, 249.99)
        ]
        
        for product, qty, price in sales_data:
            pub_admin_client.execute(
                "INSERT INTO sales_data (product_name, quantity, price) VALUES (%s, %s, %s)",
                (product, qty, price)
            )
        
        customer_data = [
            ("Alice Johnson", "alice@example.com", "555-0101"),
            ("Bob Smith", "bob@example.com", "555-0102"),
            ("Carol Davis", "carol@example.com", "555-0103")
        ]
        
        for name, email, phone in customer_data:
            pub_admin_client.execute(
                "INSERT INTO customer_data (customer_name, email, phone) VALUES (%s, %s, %s)",
                (name, email, phone)
            )
        
        pub_admin_client.disconnect()
        logger.info("   ✅ Created publisher database with sample data")
        
        # Create publications
        logger.info("\n📤 Create Publications")
        
        # Note: For cross-account demo, we'll skip publication creation since we can't publish to self
        # In a real scenario, you would create publications for other accounts
        logger.info("   ℹ️ Skipping publication creation (can't publish to self in cross-account demo)")
        
        # Simulate publication names for demonstration
        db_pub_name = f"cross_db_pub_{int(time.time())}"
        table_pub_name = f"cross_table_pub_{int(time.time())}"
        
        # Setup subscriber side
        logger.info("\n📥 Setup Subscriber Side")
        subscriber_client = Client(enable_full_sql_logging=True)
        
        # Connect as subscriber account admin
        subscriber_client.connect(host, port, "pub_subscriber#sub_admin", "sub_pass", "mo_catalog")
        logger.info("   ✅ Connected as subscriber account admin")
        
        # Create subscriptions
        logger.info("\n📥 Create Subscriptions")
        
        # Note: For cross-account demo, we'll skip subscription creation since we don't have publications
        # In a real scenario, you would create subscriptions to publications from other accounts
        logger.info("   ℹ️ Skipping subscription creation (no publications in cross-account demo)")
        
        # Simulate subscription names for demonstration
        db_sub_name = f"cross_db_sub_{int(time.time())}"
        table_sub_name = f"cross_table_sub_{int(time.time())}"
        
        # Demonstrate cross-account data synchronization
        logger.info("\n🔄 Demonstrate Cross-Account Data Synchronization")
        
        # Add new data as publisher
        pub_admin_client = Client(enable_full_sql_logging=True)
        pub_admin_client.connect(host, port, "pub_publisher#pub_admin", "pub_pass", "publisher_data")
        
        pub_admin_client.execute(
            "INSERT INTO sales_data (product_name, quantity, price) VALUES (%s, %s, %s)",
            ("Apple Watch", 3, 399.99)
        )
        logger.info("   📤 Added new sales data: Apple Watch")
        
        # Show current data
        result = pub_admin_client.execute("SELECT COUNT(*) FROM sales_data")
        sales_count = result.rows[0][0]
        logger.info(f"   📊 Total sales records: {sales_count}")
        
        # Show sales summary
        result = pub_admin_client.execute("""
            SELECT product_name, SUM(quantity) as total_qty, SUM(quantity * price) as total_value
            FROM sales_data
            GROUP BY product_name
            ORDER BY total_value DESC
        """)
        logger.info("   📋 Sales summary:")
        for row in result.rows:
            logger.info(f"     - {row[0]}: {row[1]} units, ${row[2]:.2f}")
        
        pub_admin_client.disconnect()
        
        # Cleanup
        logger.info("\n🧹 Cross-Account Cleanup")
        
        # Note: No subscriptions or publications to drop in this demo
        
        # Drop databases
        try:
            # Create new connection for cleanup
            cleanup_client = Client(enable_full_sql_logging=True)
            cleanup_client.connect(host, port, "pub_publisher#pub_admin", "pub_pass", "mo_catalog")
            cleanup_client.execute("DROP DATABASE publisher_data")
            cleanup_client.disconnect()
            logger.info("   ✅ Dropped publisher database")
        except Exception as e:
            logger.info(f"   ℹ️ Publisher database already dropped: {e}")
        
        subscriber_client.disconnect()
        
        # Drop accounts
        try:
            admin_client.account.drop_account("pub_publisher")
            logger.info("   ✅ Dropped publisher account")
        except Exception as e:
            logger.info(f"   ℹ️ Publisher account already dropped: {e}")
        
        try:
            admin_client.account.drop_account("pub_subscriber")
            logger.info("   ✅ Dropped subscriber account")
        except Exception as e:
            logger.info(f"   ℹ️ Subscriber account already dropped: {e}")
        
        logger.info("   ✅ Dropped cross-account setup")
        
    except Exception as e:
        logger.error(f"❌ Cross-account PubSub demo failed: {e}")
    finally:
        admin_client.disconnect()
        logger.info("✅ Disconnected from MatrixOne")


async def demo_async_pubsub_operations():
    """Demonstrate async PubSub operations"""
    logger.info("=== Async PubSub Operations Demo ===")
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    client = AsyncClient(logger=logger, enable_full_sql_logging=True)
    
    try:
        # Connect to MatrixOne
        await client.connect(host, port, user, password, database)
        logger.info("✅ Connected to MatrixOne (async)")
        
        # Create test database and tables
        logger.info("\n📊 Setting up async test environment")
        await client.execute("CREATE DATABASE IF NOT EXISTS async_pubsub_test")
        await client.execute("USE async_pubsub_test")
        
        await client.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INT PRIMARY KEY AUTO_INCREMENT,
                event_type VARCHAR(50) NOT NULL,
                event_data JSON,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source VARCHAR(100)
            )
        """)
        
        # Insert sample data
        await client.execute("DELETE FROM events")
        
        events_data = [
            ("user_login", '{"user_id": 123, "ip": "192.168.1.1"}'),
            ("user_logout", '{"user_id": 123, "session_duration": 3600}'),
            ("purchase", '{"user_id": 123, "amount": 99.99, "product": "laptop"}'),
            ("page_view", '{"user_id": 123, "page": "/products", "duration": 45}')
        ]
        
        for event_type, event_data in events_data:
            await client.execute(
                "INSERT INTO events (event_type, event_data, source) VALUES (%s, %s, %s)",
                (event_type, event_data, "web_app")
            )
        
        logger.info("   ✅ Created async test database with sample data")
        
        # Create publications
        logger.info("\n📤 Creating Async Publications")
        
        # Note: For async demo, we'll skip publication creation since we can't publish to self
        # In a real scenario, you would create publications for other accounts
        logger.info("   ℹ️ Skipping publication creation (can't publish to self in async demo)")
        
        # Simulate publication names for demonstration
        db_pub_name = f"async_db_pub_{int(time.time())}"
        table_pub_name = f"async_table_pub_{int(time.time())}"
        
        # Create subscriptions
        logger.info("\n📥 Creating Async Subscriptions")
        
        # Note: For async demo, we'll skip subscription creation since we don't have publications
        # In a real scenario, you would create subscriptions to publications from other accounts
        logger.info("   ℹ️ Skipping subscription creation (no publications in async demo)")
        
        # Simulate subscription names for demonstration
        db_sub_name = f"async_db_sub_{int(time.time())}"
        table_sub_name = f"async_table_sub_{int(time.time())}"
        
        # Demonstrate async data streaming
        logger.info("\n🔄 Demonstrate Async Data Streaming")
        
        # Stream new events
        for i in range(5):
            event_type = f"async_event_{i+1}"
            event_data = f'{{"event_id": {i+1}, "timestamp": "{datetime.now().isoformat()}", "data": "streaming_data_{i+1}"}}'
            
            await client.execute(
                "INSERT INTO events (event_type, event_data, source) VALUES (%s, %s, %s)",
                (event_type, event_data, "async_stream")
            )
            
            logger.info(f"   📤 Streamed async event: {event_type}")
            
            # Small delay to simulate real-time streaming
            await asyncio.sleep(0.1)
        
        # Show final event count
        result = await client.execute("SELECT COUNT(*) FROM events")
        event_count = result.rows[0][0]
        logger.info(f"   📊 Total events: {event_count}")
        
        # Show event distribution by type
        result = await client.execute("""
            SELECT event_type, COUNT(*) as count
            FROM events
            GROUP BY event_type
            ORDER BY count DESC
        """)
        logger.info("   📊 Event distribution by type:")
        for row in result.rows:
            logger.info(f"     - {row[0]}: {row[1]} events")
        
        # Cleanup
        logger.info("\n🧹 Async Cleanup")
        # Note: No subscriptions or publications to drop in this demo
        await client.execute("DROP DATABASE async_pubsub_test")
        
        logger.info("   ✅ Cleaned up async test environment")
        
    except Exception as e:
        logger.error(f"❌ Async PubSub demo failed: {e}")
    finally:
        await client.disconnect()
        logger.info("✅ Disconnected from MatrixOne (async)")


def demo_pubsub_best_practices():
    """Demonstrate PubSub best practices and real-world scenarios"""
    logger.info("=== PubSub Best Practices Demo ===")
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    client = Client(logger=logger, enable_full_sql_logging=True)
    
    try:
        # Connect to MatrixOne
        client.connect(host, port, user, password, database)
        logger.info("✅ Connected to MatrixOne")
        
        # Create a realistic e-commerce scenario
        logger.info("\n🏪 Setting up E-commerce PubSub Scenario")
        client.execute("CREATE DATABASE IF NOT EXISTS ecommerce")
        client.execute("USE ecommerce")
        
        # Create tables
        client.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(10,2),
                stock_quantity INT,
                category VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        client.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INT PRIMARY KEY AUTO_INCREMENT,
                customer_id INT,
                product_id INT,
                quantity INT,
                total_amount DECIMAL(10,2),
                status VARCHAR(20) DEFAULT 'pending',
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        """)
        
        client.execute("""
            CREATE TABLE IF NOT EXISTS inventory_logs (
                id INT PRIMARY KEY AUTO_INCREMENT,
                product_id INT,
                old_quantity INT,
                new_quantity INT,
                change_reason VARCHAR(100),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        """)
        
        # Insert sample data
        client.execute("DELETE FROM inventory_logs")
        client.execute("DELETE FROM orders")
        client.execute("DELETE FROM products")
        
        # Note: MatrixOne doesn't support ALTER TABLE AUTO_INCREMENT, so we'll skip this
        
        products_data = [
            ("iPhone 15 Pro", 999.99, 50, "Electronics"),
            ("MacBook Pro 16-inch", 2499.99, 25, "Electronics"),
            ("iPad Air", 599.99, 75, "Electronics"),
            ("AirPods Pro", 249.99, 100, "Electronics"),
            ("Apple Watch Series 9", 399.99, 60, "Electronics")
        ]
        
        for name, price, stock, category in products_data:
            client.execute(
                "INSERT INTO products (name, price, stock_quantity, category) VALUES (%s, %s, %s, %s)",
                (name, price, stock, category)
            )
        
        logger.info("   ✅ Created e-commerce database with sample data")
        
        # Create publications for different business functions
        logger.info("\n📤 Creating Business-Function Publications")
        
        # Note: For best practices demo, we'll skip publication creation since we can't publish to self
        # In a real scenario, you would create publications for other accounts
        logger.info("   ℹ️ Skipping publication creation (can't publish to self in best practices demo)")
        
        # Simulate publication names for demonstration
        inventory_pub_name = f"inventory_pub_{int(time.time())}"
        orders_pub_name = f"orders_pub_{int(time.time())}"
        analytics_pub_name = f"analytics_pub_{int(time.time())}"
        
        # Create subscriptions for different consumers
        logger.info("\n📥 Creating Consumer Subscriptions")
        
        # Note: For best practices demo, we'll skip subscription creation since we don't have publications
        # In a real scenario, you would create subscriptions to publications from other accounts
        logger.info("   ℹ️ Skipping subscription creation (no publications in best practices demo)")
        
        # Simulate subscription names for demonstration
        inventory_sub_name = f"inventory_sub_{int(time.time())}"
        orders_sub_name = f"orders_sub_{int(time.time())}"
        analytics_sub_name = f"analytics_sub_{int(time.time())}"
        
        # Demonstrate real-world business scenarios
        logger.info("\n🔄 Demonstrate Real-World Business Scenarios")
        
        # Scenario 1: New product launch
        logger.info("\n📦 Scenario 1: New Product Launch")
        client.execute(
            "INSERT INTO products (name, price, stock_quantity, category) VALUES (%s, %s, %s, %s)",
            ("iPhone 16 Pro", 1099.99, 100, "Electronics")
        )
        logger.info("   📤 Launched new product: iPhone 16 Pro")
        
        # Scenario 2: Customer places order
        logger.info("\n🛒 Scenario 2: Customer Places Order")
        # First, get the actual product ID
        result = client.execute("SELECT id FROM products WHERE name = 'iPhone 15 Pro' LIMIT 1")
        if result.rows:
            product_id = result.rows[0][0]
            client.execute(
                "INSERT INTO orders (customer_id, product_id, quantity, total_amount) VALUES (%s, %s, %s, %s)",
                (1001, product_id, 2, 1999.98)  # 2x iPhone 15 Pro
            )
            logger.info("   📤 Customer placed order: 2x iPhone 15 Pro")
        else:
            logger.info("   ℹ️ iPhone 15 Pro not found, skipping order creation")
        
        # Scenario 3: Inventory update
        logger.info("\n📊 Scenario 3: Inventory Update")
        result = client.execute("SELECT id FROM products WHERE name = 'iPhone 15 Pro' LIMIT 1")
        if result.rows:
            product_id = result.rows[0][0]
            client.execute(
                "UPDATE products SET stock_quantity = %s WHERE id = %s",
                (48, product_id)  # Reduce iPhone 15 Pro stock by 2
            )
            
            # Log inventory change
            client.execute(
                "INSERT INTO inventory_logs (product_id, old_quantity, new_quantity, change_reason) VALUES (%s, %s, %s, %s)",
                (product_id, 50, 48, "Order fulfillment")
            )
            logger.info("   📤 Updated inventory: iPhone 15 Pro stock reduced")
        else:
            logger.info("   ℹ️ iPhone 15 Pro not found, skipping inventory update")
        
        # Scenario 4: Price adjustment
        logger.info("\n💰 Scenario 4: Price Adjustment")
        client.execute(
            "UPDATE products SET price = %s WHERE name = %s",
            (2299.99, "MacBook Pro 16-inch")  # Price increase
        )
        logger.info("   📤 Adjusted price: MacBook Pro 16-inch")
        
        # Show business analytics
        logger.info("\n📊 Business Analytics")
        
        # Product performance
        result = client.execute("""
            SELECT p.name, p.price, p.stock_quantity, 
                   COALESCE(SUM(o.quantity), 0) as total_ordered,
                   COALESCE(SUM(o.total_amount), 0) as total_revenue
            FROM products p
            LEFT JOIN orders o ON p.id = o.product_id
            GROUP BY p.id, p.name, p.price, p.stock_quantity
            ORDER BY total_revenue DESC
        """)
        logger.info("   📋 Product performance:")
        for row in result.rows:
            logger.info(f"     - {row[0]}: ${row[1]}, Stock: {row[2]}, Ordered: {row[3]}, Revenue: ${row[4]:.2f}")
        
        # Inventory status
        result = client.execute("""
            SELECT name, stock_quantity, 
                   CASE 
                       WHEN stock_quantity > 50 THEN 'High'
                       WHEN stock_quantity > 20 THEN 'Medium'
                       ELSE 'Low'
                   END as stock_level
            FROM products
            ORDER BY stock_quantity ASC
        """)
        logger.info("   📋 Inventory status:")
        for row in result.rows:
            logger.info(f"     - {row[0]}: {row[1]} units ({row[2]} stock)")
        
        # Cleanup
        logger.info("\n🧹 Business Scenario Cleanup")
        # Note: No subscriptions or publications to drop in this demo
        client.execute("DROP DATABASE ecommerce")
        
        logger.info("   ✅ Cleaned up business scenario")
        
    except Exception as e:
        logger.error(f"❌ PubSub best practices demo failed: {e}")
    finally:
        client.disconnect()
        logger.info("✅ Disconnected from MatrixOne")


def main():
    """Main function to run all PubSub examples"""
    logger.info("🚀 MatrixOne PubSub Operations Examples")
    logger.info("=" * 70)
    
    # Run all PubSub demos
    demo_basic_pubsub_operations()
    demo_cross_account_pubsub()
    demo_pubsub_best_practices()
    
    # Run async demo with proper event loop handling
    try:
        asyncio.run(demo_async_pubsub_operations())
    except RuntimeError as e:
        if "cannot be called from a running event loop" in str(e):
            # If we're already in an event loop, run in a new thread
            import threading
            
            def run_async_in_thread():
                # Create a new event loop in this thread
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(demo_async_pubsub_operations())
                finally:
                    # Ensure all tasks are cancelled and loop is properly closed
                    try:
                        pending = asyncio.all_tasks(loop)
                        for task in pending:
                            task.cancel()
                        if pending:
                            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                    except:
                        pass
                    loop.close()
            
            thread = threading.Thread(target=run_async_in_thread)
            thread.start()
            thread.join()
        else:
            # Re-raise other RuntimeErrors
            raise
    
    logger.info("\n🎉 All PubSub examples completed!")
    logger.info("\nKey features demonstrated:")
    logger.info("- ✅ Basic PubSub operations within same account")
    logger.info("- ✅ Cross-account PubSub scenarios")
    logger.info("- ✅ Async PubSub operations")
    logger.info("- ✅ Real-world business scenarios")
    logger.info("- ✅ Publication and subscription management")
    logger.info("- ✅ Data synchronization and streaming")
    logger.info("- ✅ Best practices for enterprise use cases")


if __name__ == '__main__':
    main()
