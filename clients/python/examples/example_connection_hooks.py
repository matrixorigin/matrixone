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
Example: Connection Hooks - Automatic Feature Enablement on Connection

This example demonstrates how to use connection hooks to automatically
enable features like IVF, HNSW, and fulltext search after connecting to MatrixOne:

1. Basic connection hooks with predefined actions
2. Custom callback functions
3. Mixed actions and custom callbacks
4. Asynchronous connection hooks
5. String-based action names
6. Practical application examples
7. Error handling and logging

Connection hooks allow you to automatically configure your MatrixOne client
whenever a new connection is established, eliminating the need for manual
feature enablement.
"""

import asyncio
import logging
from matrixone import Client, AsyncClient
from matrixone.connection_hooks import ConnectionAction, create_connection_hook
from matrixone.config import get_connection_kwargs, print_config
from matrixone.logger import create_default_logger


class ConnectionHooksDemo:
    """Demonstrates connection hooks capabilities with comprehensive testing."""

    def __init__(self):
        self.logger = create_default_logger(
            sql_log_mode="auto",
        )
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
            'connection_hooks_tested': [],
        }
        # Get connection parameters and filter to only supported ones
        all_params = get_connection_kwargs()
        self.connection_params = {
            'host': all_params['host'],
            'port': all_params['port'],
            'user': all_params['user'],
            'password': all_params['password'],
            'database': all_params['database'],
        }
        # Note: on_connect is available in all_params but we'll set it explicitly in examples

    def sync_connection_hooks_example(self):
        """Demonstrate connection hooks with synchronous client"""
        print("=== Synchronous Connection Hooks Example ===")

        # Example 1: Enable all features
        print("\n1. Enable all features:")
        self.results['tests_run'] += 1
        client = Client()
        try:
            client.connect(
                **self.connection_params,
                on_connect=[ConnectionAction.ENABLE_ALL],
            )
            print("✓ Connected with all features enabled")
            self.results['tests_passed'] += 1
            self.results['connection_hooks_tested'].append('ENABLE_ALL')
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"ENABLE_ALL failed: {e}")
        finally:
            if client.connected():
                client.disconnect()

        # Example 2: Enable only vector operations
        print("\n2. Enable only vector operations:")
        self.results['tests_run'] += 1
        client = Client()
        try:
            client.connect(
                **self.connection_params,
                on_connect=[ConnectionAction.ENABLE_VECTOR],
            )
            print("✓ Connected with vector operations enabled")
            self.results['tests_passed'] += 1
            self.results['connection_hooks_tested'].append('ENABLE_VECTOR')
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"ENABLE_VECTOR failed: {e}")
        finally:
            if client.connected():
                client.disconnect()

        # Example 3: Enable only fulltext search
        print("\n3. Enable only fulltext search:")
        self.results['tests_run'] += 1
        client = Client()
        try:
            client.connect(
                **self.connection_params,
                on_connect=[ConnectionAction.ENABLE_FULLTEXT],
            )
            print("✓ Connected with fulltext search enabled")
            self.results['tests_passed'] += 1
            self.results['connection_hooks_tested'].append('ENABLE_FULLTEXT')
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"ENABLE_FULLTEXT failed: {e}")
        finally:
            if client.connected():
                client.disconnect()

        # Example 4: Custom callback function
        print("\n4. Custom callback function:")
        self.results['tests_run'] += 1

        def my_callback(client):
            print(f"  Custom callback: Connected to {client._connection_params['host']}:{client._connection_params['port']}")
            print(f"  Database: {client._connection_params['database']}")

        client = Client()
        try:
            client.connect(**self.connection_params, on_connect=my_callback)
            print("✓ Connected with custom callback")
            self.results['tests_passed'] += 1
            self.results['connection_hooks_tested'].append('CUSTOM_CALLBACK')
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"CUSTOM_CALLBACK failed: {e}")
        finally:
            if client.connected():
                client.disconnect()

        # Example 5: Mixed actions and custom callback
        print("\n5. Mixed actions and custom callback:")
        self.results['tests_run'] += 1

        def setup_callback(client):
            print(f"  Setup callback: Setting up client for {client._connection_params['database']}")
            # You can add custom setup logic here

        client = Client()
        try:
            client.connect(
                **self.connection_params,
                on_connect=create_connection_hook(
                    actions=[ConnectionAction.ENABLE_FULLTEXT, ConnectionAction.ENABLE_IVF], custom_hook=setup_callback
                ),
            )
            print("✓ Connected with mixed actions and custom callback")
            self.results['tests_passed'] += 1
            self.results['connection_hooks_tested'].append('MIXED_ACTIONS')
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"MIXED_ACTIONS failed: {e}")
        finally:
            if client.connected():
                client.disconnect()

    async def async_connection_hooks_example(self):
        """Demonstrate connection hooks with asynchronous client"""
        print("\n=== Asynchronous Connection Hooks Example ===")

        # Example 1: Enable all features
        print("\n1. Enable all features:")
        self.results['tests_run'] += 1
        client = AsyncClient()
        try:
            await client.connect(
                **self.connection_params,
                on_connect=[ConnectionAction.ENABLE_ALL],
            )
            print("✓ Connected with all features enabled")
            self.results['tests_passed'] += 1
            self.results['connection_hooks_tested'].append('ASYNC_ENABLE_ALL')
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"ASYNC_ENABLE_ALL failed: {e}")
        finally:
            if client.connected():
                await client.disconnect()

        # Example 2: Custom async callback
        print("\n2. Custom async callback:")
        self.results['tests_run'] += 1

        async def async_callback(client):
            print(f"  Async callback: Connected to {client._connection_params['host']}:{client._connection_params['port']}")
            print(f"  Database: {client._connection_params['database']}")
            # Simulate some async setup
            await asyncio.sleep(0.1)
            print("  Async setup completed")

        client = AsyncClient()
        try:
            await client.connect(**self.connection_params, on_connect=async_callback)
            print("✓ Connected with async callback")
            self.results['tests_passed'] += 1
            self.results['connection_hooks_tested'].append('ASYNC_CUSTOM_CALLBACK')
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"ASYNC_CUSTOM_CALLBACK failed: {e}")
        finally:
            if client.connected():
                await client.disconnect()

        # Example 3: String action names
        print("\n3. Using string action names:")
        self.results['tests_run'] += 1
        client = AsyncClient()
        try:
            await client.connect(
                **self.connection_params,
                on_connect=["enable_fulltext", "enable_ivf"],
            )
            print("✓ Connected with string action names")
            self.results['tests_passed'] += 1
            self.results['connection_hooks_tested'].append('ASYNC_STRING_ACTIONS')
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"ASYNC_STRING_ACTIONS failed: {e}")
        finally:
            if client.connected():
                await client.disconnect()

    def practical_example(self):
        """Practical example showing how to use connection hooks in a real application"""
        print("\n=== Practical Application Example ===")

        # This is how you might use it in a real application
        def setup_vector_search_client():
            """Setup a client optimized for vector search operations"""
            client = Client()
            client.connect(
                **self.connection_params,
                on_connect=[ConnectionAction.ENABLE_VECTOR],
            )
            return client

        def setup_fulltext_search_client():
            """Setup a client optimized for fulltext search operations"""
            client = Client()
            client.connect(
                **self.connection_params,
                on_connect=[ConnectionAction.ENABLE_FULLTEXT],
            )
            return client

        def setup_analytics_client():
            """Setup a client for analytics with all features enabled"""

            def analytics_setup(client):
                print(f"  Analytics setup: Configuring client for analytics workload")
                # You could set session variables, create temporary tables, etc.

            client = Client()
            client.connect(
                **self.connection_params,
                on_connect=create_connection_hook(actions=[ConnectionAction.ENABLE_ALL], custom_hook=analytics_setup),
            )
            return client

        # Example usage
        try:
            print("\nSetting up vector search client...")
            self.results['tests_run'] += 1
            vector_client = setup_vector_search_client()
            print("✓ Vector search client ready")
            vector_client.disconnect()
            self.results['tests_passed'] += 1
            self.results['connection_hooks_tested'].append('VECTOR_SEARCH_CLIENT')

            print("\nSetting up fulltext search client...")
            self.results['tests_run'] += 1
            search_client = setup_fulltext_search_client()
            print("✓ Fulltext search client ready")
            search_client.disconnect()
            self.results['tests_passed'] += 1
            self.results['connection_hooks_tested'].append('FULLTEXT_SEARCH_CLIENT')

            print("\nSetting up analytics client...")
            self.results['tests_run'] += 1
            analytics_client = setup_analytics_client()
            print("✓ Analytics client ready")
            analytics_client.disconnect()
            self.results['tests_passed'] += 1
            self.results['connection_hooks_tested'].append('ANALYTICS_CLIENT')

        except Exception as e:
            print(f"✗ Setup failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"PRACTICAL_EXAMPLE failed: {e}")

    def config_management_example(self):
        """Demonstrate using on_connect with configuration management"""
        print("\n=== Configuration Management Example ===")

        # Example 1: Using get_connection_kwargs with on_connect
        print("\n1. Using get_connection_kwargs with on_connect:")
        self.results['tests_run'] += 1
        try:
            # Get config with on_connect parameter
            config_with_hooks = get_connection_kwargs(on_connect=[ConnectionAction.ENABLE_FULLTEXT])

            # Filter to only supported parameters for Client.connect()
            filtered_config = {
                'host': config_with_hooks['host'],
                'port': config_with_hooks['port'],
                'user': config_with_hooks['user'],
                'password': config_with_hooks['password'],
                'database': config_with_hooks['database'],
                'on_connect': config_with_hooks['on_connect'],
            }

            # Create client and connect using the filtered config
            client = Client()
            client.connect(**filtered_config)
            print("✓ Connected using config with on_connect")
            self.results['tests_passed'] += 1
            self.results['connection_hooks_tested'].append('CONFIG_MANAGEMENT')
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append(f"CONFIG_MANAGEMENT failed: {e}")
        finally:
            if client.connected():
                client.disconnect()

        # Example 2: Environment variable support
        print("\n2. Environment variable support:")
        print("   You can set MATRIXONE_ON_CONNECT environment variable")
        print("   Example: export MATRIXONE_ON_CONNECT='enable_all,enable_vector'")
        print("   This will automatically apply connection hooks to all connections")

    def print_summary(self):
        """Print comprehensive summary of all tests and results"""
        print("\n" + "=" * 60)
        print("CONNECTION HOOKS EXAMPLE SUMMARY")
        print("=" * 60)

        print(f"Total Tests Run: {self.results['tests_run']}")
        print(f"Tests Passed: {self.results['tests_passed']}")
        print(f"Tests Failed: {self.results['tests_failed']}")

        if self.results['tests_run'] > 0:
            success_rate = (self.results['tests_passed'] / self.results['tests_run']) * 100
            print(f"Success Rate: {success_rate:.1f}%")

        print(f"\nConnection Hooks Tested: {len(self.results['connection_hooks_tested'])}")
        for hook in self.results['connection_hooks_tested']:
            print(f"  ✓ {hook}")

        if self.results['unexpected_results']:
            print(f"\nUnexpected Results ({len(self.results['unexpected_results'])}):")
            for result in self.results['unexpected_results']:
                print(f"  ✗ {result}")

        print("\nConnection Parameters Used:")
        print_config()

        print("\n" + "=" * 60)
        print("Connection hooks provide automatic feature enablement")
        print("whenever a new connection is established to MatrixOne.")
        print("This eliminates the need for manual feature configuration")
        print("and ensures consistent setup across your application.")
        print("=" * 60)


def main():
    """Main function to run all examples"""
    print("MatrixOne Connection Hooks Examples")
    print("=" * 50)

    # Create demo instance
    demo = ConnectionHooksDemo()

    # Run synchronous examples
    demo.sync_connection_hooks_example()

    # Run asynchronous examples
    asyncio.run(demo.async_connection_hooks_example())

    # Run practical example
    demo.practical_example()

    # Run configuration management example
    demo.config_management_example()

    # Print comprehensive summary
    demo.print_summary()


if __name__ == "__main__":
    main()
