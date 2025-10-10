"""
MatrixOne Python SDK - Dynamic Logging Configuration Example

This example demonstrates:
1. Dynamic configuration updates at runtime
2. Per-operation logging mode override
3. Temporary debugging with different log levels
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from matrixone import Client
from matrixone.logger import create_default_logger


def get_connection_params():
    """Get connection parameters from environment"""
    return (
        os.getenv('MATRIXONE_HOST', '127.0.0.1'),
        int(os.getenv('MATRIXONE_PORT', '6001')),
        os.getenv('MATRIXONE_USER', 'root'),
        os.getenv('MATRIXONE_PASSWORD', '111'),
        os.getenv('MATRIXONE_DATABASE', 'test'),
    )


def demo_dynamic_config_update():
    """Demonstrate dynamic logger configuration updates"""
    print("\n" + "=" * 80)
    print("Dynamic Logger Configuration Update Demo")
    print("=" * 80)

    host, port, user, password, database = get_connection_params()

    # Start with 'simple' logging mode
    logger = create_default_logger(sql_log_mode='simple')
    client = Client(logger=logger)
    client.connect(host=host, port=port, user=user, password=password, database=database)

    # Create test table
    client.execute("DROP TABLE IF EXISTS dynamic_log_test")
    client.execute(
        """
        CREATE TABLE dynamic_log_test (
            id INT,
            embedding VECF32(3),
            PRIMARY KEY(id)
        )
    """
    )

    print("\n1. Initial mode: 'simple'")
    print("   Expected: Only show operation summary")
    client.vector_ops.insert('dynamic_log_test', {'id': 1, 'embedding': [0.1, 0.2, 0.3]})

    # Dynamically switch to 'full' mode
    print("\n2. Switching to 'full' mode...")
    client.logger.update_config(sql_log_mode='full')
    client.vector_ops.insert('dynamic_log_test', {'id': 2, 'embedding': [0.4, 0.5, 0.6]})

    # Dynamically switch to 'auto' mode
    print("\n3. Switching to 'auto' mode...")
    client.logger.update_config(sql_log_mode='auto')
    client.vector_ops.insert('dynamic_log_test', {'id': 3, 'embedding': [0.7, 0.8, 0.9]})

    # Dynamically switch to 'off' mode
    print("\n4. Switching to 'off' mode...")
    client.logger.update_config(sql_log_mode='off')
    print("   Expected: No SQL logs")
    client.vector_ops.insert('dynamic_log_test', {'id': 4, 'embedding': [0.1, 0.1, 0.1]})

    # Switch back to 'auto' for remaining operations
    print("\n5. Switching back to 'auto' mode...")
    client.logger.update_config(sql_log_mode='auto')

    # Update slow query threshold
    print("\n6. Updating slow query threshold to 0.001s...")
    client.logger.update_config(slow_query_threshold=0.001)
    print("   Expected: Most queries will be marked as [SLOW]")
    client.vector_ops.insert('dynamic_log_test', {'id': 5, 'embedding': [0.2, 0.2, 0.2]})

    # Clean up
    client.execute("DROP TABLE dynamic_log_test")
    client.disconnect()
    print("\n✓ Dynamic configuration update demo completed")


def demo_per_operation_override():
    """Demonstrate per-operation logging mode override"""
    print("\n" + "=" * 80)
    print("Per-Operation Logging Override Demo")
    print("=" * 80)

    host, port, user, password, database = get_connection_params()

    # Start with 'off' logging mode
    logger = create_default_logger(sql_log_mode='off')
    client = Client(logger=logger)
    client.connect(host=host, port=port, user=user, password=password, database=database)

    # Create test table
    client.execute("DROP TABLE IF EXISTS override_log_test")
    client.execute(
        """
        CREATE TABLE override_log_test (
            id INT,
            embedding VECF32(3),
            PRIMARY KEY(id)
        )
    """
    )

    print("\n1. Global mode: 'off' (no logging)")
    print("   Inserting data without logging...")
    for i in range(1, 4):
        client.vector_ops.insert('override_log_test', {'id': i, 'embedding': [i * 0.1, i * 0.2, i * 0.3]})

    print("\n2. Override with '_log_mode=full' for specific operation")
    print("   Expected: Only this operation shows full SQL log")
    results = client.vector_ops.similarity_search(
        'override_log_test',
        'embedding',
        [0.1, 0.2, 0.3],
        limit=3,
        _log_mode='full',  # Override to 'full' for this operation only
    )
    print(f"   Found {len(results)} results")

    print("\n3. Another operation without override (back to 'off')")
    print("   Expected: No logging for this operation")
    results = client.vector_ops.similarity_search('override_log_test', 'embedding', [0.2, 0.4, 0.6], limit=3)
    print(f"   Found {len(results)} results (no SQL log)")

    print("\n4. Override with '_log_mode=simple' for debugging")
    results = client.vector_ops.similarity_search(
        'override_log_test',
        'embedding',
        [0.3, 0.6, 0.9],
        limit=3,
        _log_mode='simple',  # Override to 'simple' for this operation
    )
    print(f"   Found {len(results)} results")

    # Clean up
    client.execute("DROP TABLE override_log_test")
    client.disconnect()
    print("\n✓ Per-operation override demo completed")


def demo_debugging_scenario():
    """Demonstrate a realistic debugging scenario"""
    print("\n" + "=" * 80)
    print("Realistic Debugging Scenario Demo")
    print("=" * 80)

    host, port, user, password, database = get_connection_params()

    # Production setup: minimal logging
    logger = create_default_logger(sql_log_mode='simple', slow_query_threshold=1.0)
    client = Client(logger=logger)
    client.connect(host=host, port=port, user=user, password=password, database=database)

    # Create test table
    client.execute("DROP TABLE IF EXISTS debug_scenario")
    client.execute(
        """
        CREATE TABLE debug_scenario (
            id INT,
            embedding VECF32(128),
            PRIMARY KEY(id)
        )
    """
    )

    print("\n1. Normal production operations with 'simple' logging")
    import random

    for i in range(1, 6):
        embedding = [random.random() for _ in range(128)]
        client.vector_ops.insert('debug_scenario', {'id': i, 'embedding': embedding})

    print("\n2. Debugging: enable full SQL logging dynamically")
    print("   User reports slow similarity search, enabling detailed logging...")
    client.logger.update_config(sql_log_mode='full', slow_query_threshold=0.01)

    query_embedding = [random.random() for _ in range(128)]
    results = client.vector_ops.similarity_search('debug_scenario', 'embedding', query_embedding, limit=3)
    print(f"   Search completed: {len(results)} results")

    print("\n3. After debugging: restore production settings")
    client.logger.update_config(sql_log_mode='simple', slow_query_threshold=1.0)
    print("   Production logging restored")

    # More operations with production settings
    for i in range(6, 11):
        embedding = [random.random() for _ in range(128)]
        client.vector_ops.insert('debug_scenario', {'id': i, 'embedding': embedding})

    # Clean up
    client.execute("DROP TABLE debug_scenario")
    client.disconnect()
    print("\n✓ Debugging scenario demo completed")


def main():
    """Run all dynamic logging demos"""
    print("\n" + "=" * 80)
    print("MatrixOne Dynamic Logging Configuration Examples")
    print("=" * 80)

    try:
        # Run demos
        demo_dynamic_config_update()
        demo_per_operation_override()
        demo_debugging_scenario()

        # Summary
        print("\n" + "=" * 80)
        print("Summary")
        print("=" * 80)
        print(
            """
Key Features Demonstrated:

1. Dynamic Configuration Update (logger.update_config())
   - Change sql_log_mode at runtime ('off', 'auto', 'simple', 'full')
   - Adjust slow_query_threshold dynamically
   - Update max_sql_display_length as needed

2. Per-Operation Override (_log_mode parameter)
   - Temporarily enable detailed logging for specific operations
   - Useful for debugging without changing global settings
   - Works with vector_ops methods (similarity_search, etc.)

3. Production Use Cases
   - Start with minimal logging for production
   - Enable detailed logging when issues occur
   - Restore production settings after debugging
   - No restart required!

Benefits:
✓ Zero downtime debugging
✓ Fine-grained control over logging
✓ Better performance in production
✓ Easy troubleshooting during development
        """
        )

    except Exception as e:
        print(f"\n✗ Error occurred: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
