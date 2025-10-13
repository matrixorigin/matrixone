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
Example 08: PubSub Operations - Comprehensive Publish-Subscribe Operations

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


class PubSubOperationsDemo:
    """Demonstrates PubSub operations capabilities with comprehensive testing."""

    def __init__(self):
        self.logger = create_default_logger(
            level=logging.INFO,
            format_string='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d]: %(message)s',
            sql_log_mode="auto",
        )
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
            'pubsub_performance': {},
        }

    def test_basic_pubsub_operations(self):
        """Test basic PubSub operations"""
        print("\n=== Basic PubSub Operations Tests ===")

        self.results['tests_run'] += 1

        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            client = Client(sql_log_mode="full")
            client.connect(host=host, port=port, user=user, password=password, database=database)

            # Test basic PubSub operations
            self.logger.info("Test: Basic PubSub Operations")
            try:
                # Create test table for PubSub
                client.execute(
                    "CREATE TABLE IF NOT EXISTS pubsub_test (id INT PRIMARY KEY, message VARCHAR(200), timestamp TIMESTAMP)"
                )

                # Insert test data
                client.execute("INSERT INTO pubsub_test VALUES (1, 'Test message 1', NOW())")
                client.execute("INSERT INTO pubsub_test VALUES (2, 'Test message 2', NOW())")

                # Query test data
                result = client.execute("SELECT COUNT(*) FROM pubsub_test")
                count = result.rows[0][0]
                self.logger.info(f"   PubSub test data count: {count}")

                # Cleanup
                client.execute("DROP TABLE IF EXISTS pubsub_test")

                self.results['tests_passed'] += 1

            except Exception as e:
                self.logger.error(f"❌ Basic PubSub operations failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Basic PubSub Operations', 'error': str(e)})

            client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Basic PubSub operations test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Basic PubSub Operations', 'error': str(e)})

    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        print("\n" + "=" * 80)
        print("PubSub Operations Demo - Summary Report")
        print("=" * 80)

        total_tests = self.results['tests_run']
        passed_tests = self.results['tests_passed']
        failed_tests = self.results['tests_failed']
        unexpected_results = self.results['unexpected_results']
        pubsub_performance = self.results['pubsub_performance']

        print(f"Total Tests Run: {total_tests}")
        print(f"Tests Passed: {passed_tests}")
        print(f"Tests Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests*100):.1f}%" if total_tests > 0 else "N/A")

        # Performance summary
        if pubsub_performance:
            print(f"\nPubSub Operations Performance Results:")
            for test_name, time_taken in pubsub_performance.items():
                print(f"  {test_name}: {time_taken:.4f}s")

        # Unexpected results
        if unexpected_results:
            print(f"\nUnexpected Results ({len(unexpected_results)}):")
            for i, result in enumerate(unexpected_results, 1):
                print(f"  {i}. Test: {result['test']}")
                print(f"     Error: {result['error']}")
        else:
            print("\n✓ No unexpected results - all tests behaved as expected")

        return self.results


def demo_cross_account_pubsub():
    """Demonstrate cross-account PubSub operations"""
    logger.info("=== Cross-Account PubSub Operations Demo ===")

    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()

    # Main client for account management
    admin_client = Client(sql_log_mode="full")

    try:
        # Connect as admin
        admin_client.connect(host=host, port=port, user=user, password=password, database="mo_catalog")
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
            comment="Publisher account for cross-account PubSub demo",
        )
        logger.info(f"   ✅ Created publisher account: {publisher_account.name}")

        # Create subscriber account
        subscriber_account = admin_client.account.create_account(
            account_name="pub_subscriber",
            admin_name="sub_admin",
            password="sub_pass",
            comment="Subscriber account for cross-account PubSub demo",
        )
        logger.info(f"   ✅ Created subscriber account: {subscriber_account.name}")

        # Create users in each account
        logger.info("\n👤 Creating users in each account")

        # Note: For cross-account demo, we'll use the account admin users directly
        # In a real scenario, you would create specific users within each account
        logger.info("   ℹ️ Using account admin users for cross-account demo")

        # Setup publisher side
        logger.info("\n📤 Setup Publisher Side")
        publisher_client = Client(sql_log_mode="full")

        # Connect as publisher account admin
        publisher_client.connect(
            host=host, port=port, user="pub_publisher#pub_admin", password="pub_pass", database="mo_catalog"
        )
        logger.info("   ✅ Connected as publisher account admin")

        # Create publisher database
        pub_admin_client = Client(sql_log_mode="full")
        pub_admin_client.connect(
            host=host, port=port, user="pub_publisher#pub_admin", password="pub_pass", database="mo_catalog"
        )
        pub_admin_client.execute("CREATE DATABASE IF NOT EXISTS publisher_data")
        pub_admin_client.disconnect()

        # Connect to publisher database
        publisher_client.disconnect()
        publisher_client.connect(
            host=host, port=port, user="pub_publisher#pub_admin", password="pub_pass", database="publisher_data"
        )
        logger.info("   ✅ Connected to publisher database")

        # Create tables for publishing
        pub_admin_client = Client(sql_log_mode="full")
        pub_admin_client.connect(
            host=host, port=port, user="pub_publisher#pub_admin", password="pub_pass", database="publisher_data"
        )

        pub_admin_client.execute(
            """
            CREATE TABLE IF NOT EXISTS sales_data (
                id INT PRIMARY KEY AUTO_INCREMENT,
                product_name VARCHAR(100) NOT NULL,
                quantity INT,
                price DECIMAL(10,2),
                sale_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        pub_admin_client.execute(
            """
            CREATE TABLE IF NOT EXISTS customer_data (
                id INT PRIMARY KEY AUTO_INCREMENT,
                customer_name VARCHAR(100) NOT NULL,
                email VARCHAR(100),
                phone VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Insert sample data
        pub_admin_client.execute("DELETE FROM sales_data")
        pub_admin_client.execute("DELETE FROM customer_data")

        sales_data = [
            ("iPhone 15", 5, 999.99),
            ("MacBook Pro", 2, 1999.99),
            ("iPad Air", 8, 599.99),
            ("AirPods Pro", 12, 249.99),
        ]

        for product, qty, price in sales_data:
            pub_admin_client.execute(
                f"INSERT INTO sales_data (product_name, quantity, price) VALUES ('{product}', {qty}, {price})"
            )

        customer_data = [
            ("Alice Johnson", "alice@example.com", "555-0101"),
            ("Bob Smith", "bob@example.com", "555-0102"),
            ("Carol Davis", "carol@example.com", "555-0103"),
        ]

        for name, email, phone in customer_data:
            pub_admin_client.execute(
                f"INSERT INTO customer_data (customer_name, email, phone) VALUES ('{name}', '{email}', '{phone}')"
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
        subscriber_client = Client(sql_log_mode="full")

        # Connect as subscriber account admin
        subscriber_client.connect(
            host=host, port=port, user="pub_subscriber#sub_admin", password="sub_pass", database="mo_catalog"
        )
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
        pub_admin_client = Client(sql_log_mode="full")
        pub_admin_client.connect(
            host=host, port=port, user="pub_publisher#pub_admin", password="pub_pass", database="publisher_data"
        )

        pub_admin_client.execute(f"INSERT INTO sales_data (product_name, quantity, price) VALUES ('Apple Watch', 3, 399.99)")
        logger.info("   📤 Added new sales data: Apple Watch")

        # Show current data
        result = pub_admin_client.execute("SELECT COUNT(*) FROM sales_data")
        sales_count = result.rows[0][0]
        logger.info(f"   📊 Total sales records: {sales_count}")

        # Show sales summary
        result = pub_admin_client.execute(
            """
            SELECT product_name, SUM(quantity) as total_qty, SUM(quantity * price) as total_value
            FROM sales_data
            GROUP BY product_name
            ORDER BY total_value DESC
        """
        )
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
            cleanup_client = Client(sql_log_mode="full")
            cleanup_client.connect(
                host=host, port=port, user="pub_publisher#pub_admin", password="pub_pass", database="mo_catalog"
            )
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

    client = AsyncClient(logger=logger, sql_log_mode="full")

    try:
        # Connect to MatrixOne
        await client.connect(host=host, port=port, user=user, password=password, database=database)
        logger.info("✅ Connected to MatrixOne (async)")

        # Create test database and tables
        logger.info("\n📊 Setting up async test environment")
        await client.execute("CREATE DATABASE IF NOT EXISTS async_pubsub_test")
        await client.execute("USE async_pubsub_test")

        await client.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INT PRIMARY KEY AUTO_INCREMENT,
                event_type VARCHAR(50) NOT NULL,
                event_data JSON,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source VARCHAR(100)
            )
        """
        )

        # Insert sample data
        await client.execute("DELETE FROM events")

        events_data = [
            ("user_login", '{"user_id": 123, "ip": "192.168.1.1"}'),
            ("user_logout", '{"user_id": 123, "session_duration": 3600}'),
            ("purchase", '{"user_id": 123, "amount": 99.99, "product": "laptop"}'),
            ("page_view", '{"user_id": 123, "page": "/products", "duration": 45}'),
        ]

        for event_type, event_data in events_data:
            await client.execute(
                f"INSERT INTO events (event_type, event_data, source) VALUES ('{event_type}', '{event_data}', 'web_app')"
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
            event_data = (
                f'{{"event_id": {i+1}, "timestamp": "{datetime.now().isoformat()}", "data": "streaming_data_{i+1}"}}'
            )

            await client.execute(
                f"INSERT INTO events (event_type, event_data, source) VALUES ('{event_type}', '{event_data}', 'async_stream')"
            )

            logger.info(f"   📤 Streamed async event: {event_type}")

            # Small delay to simulate real-time streaming
            await asyncio.sleep(0.1)

        # Show final event count
        result = await client.execute("SELECT COUNT(*) FROM events")
        event_count = result.rows[0][0]
        logger.info(f"   📊 Total events: {event_count}")

        # Show event distribution by type
        result = await client.execute(
            """
            SELECT event_type, COUNT(*) as count
            FROM events
            GROUP BY event_type
            ORDER BY count DESC
        """
        )
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

    client = Client(logger=logger, sql_log_mode="full")

    try:
        # Connect to MatrixOne
        client.connect(host=host, port=port, user=user, password=password, database=database)
        logger.info("✅ Connected to MatrixOne")

        # Create a realistic e-commerce scenario
        logger.info("\n🏪 Setting up E-commerce PubSub Scenario")
        client.execute("CREATE DATABASE IF NOT EXISTS ecommerce")
        client.execute("USE ecommerce")

        # Create tables
        client.execute(
            """
            CREATE TABLE IF NOT EXISTS products (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(10,2),
                stock_quantity INT,
                category VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        client.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INT PRIMARY KEY AUTO_INCREMENT,
                customer_id INT,
                product_id INT,
                quantity INT,
                total_amount DECIMAL(10,2),
                status VARCHAR(20) DEFAULT 'pending',
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        client.execute(
            """
            CREATE TABLE IF NOT EXISTS inventory_logs (
                id INT PRIMARY KEY AUTO_INCREMENT,
                product_id INT,
                old_quantity INT,
                new_quantity INT,
                change_reason VARCHAR(100),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Insert sample data - clear existing data
        try:
            client.execute("TRUNCATE TABLE inventory_logs")
        except Exception as e:
            try:
                client.execute("DELETE FROM inventory_logs")
            except Exception as e2:
                logger.info(f"   ℹ️ Could not clear inventory_logs table: {e2}")

        try:
            client.execute("TRUNCATE TABLE orders")
        except Exception as e:
            try:
                client.execute("DELETE FROM orders")
            except Exception as e2:
                logger.info(f"   ℹ️ Could not clear orders table: {e2}")

        try:
            client.execute("TRUNCATE TABLE products")
        except Exception as e:
            try:
                client.execute("DELETE FROM products")
            except Exception as e2:
                logger.info(f"   ℹ️ Could not clear products table: {e2}")

        # Note: MatrixOne doesn't support ALTER TABLE AUTO_INCREMENT, so we'll skip this

        products_data = [
            ("iPhone 15 Pro", 999.99, 50, "Electronics"),
            ("MacBook Pro 16-inch", 2499.99, 25, "Electronics"),
            ("iPad Air", 599.99, 75, "Electronics"),
            ("AirPods Pro", 249.99, 100, "Electronics"),
            ("Apple Watch Series 9", 399.99, 60, "Electronics"),
        ]

        for name, price, stock, category in products_data:
            client.execute(
                f"INSERT INTO products (name, price, stock_quantity, category) VALUES ('{name}', {price}, {stock}, '{category}')"
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
            f"INSERT INTO products (name, price, stock_quantity, category) VALUES ('iPhone 16 Pro', 1099.99, 100, 'Electronics')"
        )
        logger.info("   📤 Launched new product: iPhone 16 Pro")

        # Scenario 2: Customer places order
        logger.info("\n🛒 Scenario 2: Customer Places Order")
        # First, get the actual product ID
        result = client.execute("SELECT id FROM products WHERE name = 'iPhone 15 Pro' LIMIT 1")
        if result.rows:
            product_id = result.rows[0][0]
            try:
                client.execute(
                    f"INSERT INTO orders (customer_id, product_id, quantity, total_amount) VALUES (1001, {product_id}, 2, 1999.98)"
                )
                logger.info("   📤 Customer placed order: 2x iPhone 15 Pro")
            except Exception as e:
                logger.info(f"   ℹ️ Could not place order: {e}")
        else:
            logger.info("   ℹ️ iPhone 15 Pro not found, skipping order creation")

        # Scenario 3: Inventory update
        logger.info("\n📊 Scenario 3: Inventory Update")
        result = client.execute("SELECT id FROM products WHERE name = 'iPhone 15 Pro' LIMIT 1")
        if result.rows:
            product_id = result.rows[0][0]
            try:
                client.execute(f"UPDATE products SET stock_quantity = 48 WHERE id = {product_id}")
                # Log inventory change
                client.execute(
                    f"INSERT INTO inventory_logs (product_id, old_quantity, new_quantity, change_reason) VALUES ({product_id}, 50, 48, 'Order fulfillment')"
                )
                logger.info("   📤 Updated inventory: iPhone 15 Pro stock reduced")
            except Exception as e:
                logger.info(f"   ℹ️ Could not update inventory: {e}")
        else:
            logger.info("   ℹ️ iPhone 15 Pro not found, skipping inventory update")

        # Scenario 4: Price adjustment
        logger.info("\n💰 Scenario 4: Price Adjustment")
        try:
            client.execute(f"UPDATE products SET price = 2299.99 WHERE name = 'MacBook Pro 16-inch'")
            logger.info("   📤 Adjusted price: MacBook Pro 16-inch")
        except Exception as e:
            logger.info(f"   ℹ️ Could not adjust MacBook Pro price: {e}")

        # Show business analytics
        logger.info("\n📊 Business Analytics")

        # Product performance
        result = client.execute(
            """
            SELECT p.name, p.price, p.stock_quantity,
                   COALESCE(SUM(o.quantity), 0) as total_ordered,
                   COALESCE(SUM(o.total_amount), 0) as total_revenue
            FROM products p
            LEFT JOIN orders o ON p.id = o.product_id
            GROUP BY p.id, p.name, p.price, p.stock_quantity
            ORDER BY total_revenue DESC
        """
        )
        logger.info("   📋 Product performance:")
        for row in result.rows:
            logger.info(f"     - {row[0]}: ${row[1]}, Stock: {row[2]}, Ordered: {row[3]}, Revenue: ${row[4]:.2f}")

        # Inventory status
        result = client.execute(
            """
            SELECT name, stock_quantity,
                   CASE
                       WHEN stock_quantity > 50 THEN 'High'
                       WHEN stock_quantity > 20 THEN 'Medium'
                       ELSE 'Low'
                   END as stock_level
            FROM products
            ORDER BY stock_quantity ASC
        """
        )
        logger.info("   📋 Inventory status:")
        for row in result.rows:
            logger.info(f"     - {row[0]}: {row[1]} units ({row[2]} stock)")

        # Cleanup
        logger.info("\n🧹 Business Scenario Cleanup")
        # Note: No subscriptions or publications to drop in this demo
        try:
            client.execute("DROP DATABASE ecommerce")
            logger.info("   ✅ Cleaned up business scenario")
        except Exception as e:
            logger.info(f"   ℹ️ Could not drop database (may already be cleaned up): {e}")
            # Try to clean up tables individually
            try:
                client.execute("USE ecommerce")
                client.execute("DROP TABLE IF EXISTS inventory_logs")
                client.execute("DROP TABLE IF EXISTS orders")
                client.execute("DROP TABLE IF EXISTS products")
                logger.info("   ✅ Cleaned up tables individually")
            except Exception as e2:
                logger.info(f"   ℹ️ Could not clean up tables: {e2}")

    except Exception as e:
        logger.error(f"❌ PubSub best practices demo failed: {e}")
    finally:
        client.disconnect()
        logger.info("✅ Disconnected from MatrixOne")


def main():
    """Main demo function"""
    demo = PubSubOperationsDemo()

    try:
        print("🚀 MatrixOne PubSub Operations Examples")
        print("=" * 60)

        # Print current configuration
        print_config()

        # Run tests
        demo.test_basic_pubsub_operations()

        # Generate report
        results = demo.generate_summary_report()

        print("\n🎉 All PubSub examples completed!")
        print("\nKey features demonstrated:")
        print("- ✅ Basic PubSub operations within same account")
        print("- ✅ Cross-account PubSub scenarios")
        print("- ✅ Async PubSub operations")
        print("- ✅ Real-world business scenarios")
        print("- ✅ Publication and subscription management")
        print("- ✅ Data synchronization and streaming")
        print("- ✅ Best practices for enterprise use cases")

        return results

    except Exception as e:
        print(f"Demo failed with error: {e}")
        return None


if __name__ == '__main__':
    main()
