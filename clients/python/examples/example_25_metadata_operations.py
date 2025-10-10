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
Example 25: Metadata Operations - Table Metadata Analysis and Schema Management

This example demonstrates comprehensive metadata operations in MatrixOne using ORM:

1. ORM-based table creation with indexes
2. Basic metadata scanning with raw SQLAlchemy results
3. Structured metadata with fixed schema (13 predefined columns)
4. Column selection capabilities (enum and string-based)
5. Synchronous and asynchronous metadata operations
6. Transaction-based metadata operations
7. Table brief statistics (aggregated view)
8. Table detailed statistics (object-level view)
9. Index metadata scanning using moctl flush
10. Type-safe metadata row objects

The metadata module provides powerful table analysis capabilities through the
metadata_scan function and new statistics interfaces, allowing you to analyze
table statistics, column information, and data distribution with both raw and
structured data formats.
"""

import asyncio
import logging
from sqlalchemy import Column, Integer, String, DateTime, Index, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from matrixone import Client, AsyncClient
from matrixone.metadata import MetadataColumn, MetadataRow
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger


class MetadataOperationsDemo:
    """Demonstrates comprehensive metadata operations with ORM and structured examples."""

    def __init__(self):
        self.logger = create_default_logger(sql_log_mode="auto")
        self.connection_params = get_connection_params()
        self.Base = declarative_base()
        self.engine = None
        self.session = None
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
            'performance_results': {},
        }

    def setup_connection(self):
        """Setup database connection and ORM."""
        self.logger.info("MatrixOne Metadata Operations Demo")
        self.logger.info("=" * 50)

        # Print configuration
        print_config()

        # Create client and connect
        self.client = Client(logger=self.logger)
        host, port, user, password, database = self.connection_params
        self.client.connect(host=host, port=port, user=user, password=password, database=database)

        # Get SQLAlchemy engine and create session
        self.engine = self.client.get_sqlalchemy_engine()
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        self.logger.info("✓ Connected to MatrixOne with ORM support")

    def create_test_models(self):
        """Create test models with indexes using ORM."""
        self.logger.info("\n=== Creating Test Models with ORM ===")

        # Define User model with indexes
        class User(self.Base):
            __tablename__ = 'users'
            __table_args__ = (
                Index('idx_users_name', 'name'),
                Index('idx_users_email', 'email'),
                Index('idx_users_age', 'age'),
                {'schema': 'metadata_demo'},
            )

            id = Column(Integer, primary_key=True)
            name = Column(String(100), nullable=False)
            email = Column(String(100), nullable=False, unique=True)
            age = Column(Integer, nullable=False)
            created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'))

        # Create database and tables
        try:
            self.session.execute(text("CREATE DATABASE IF NOT EXISTS metadata_demo"))
            self.session.execute(text("USE metadata_demo"))
            self.Base.metadata.create_all(self.engine)
            self.session.commit()
            self.logger.info("✓ Created User model with indexes")
            return User
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Failed to create models: {e}")
            raise

    def insert_test_data(self, User):
        """Insert test data using ORM."""
        self.logger.info("\n=== Inserting Test Data with ORM ===")

        try:
            # Clear existing data
            self.session.query(User).delete()

            # Insert test data
            users = []
            for i in range(1, 101):
                user = User(id=i, name=f'User{i}', email=f'user{i}@example.com', age=20 + (i % 50))
                users.append(user)

            self.session.add_all(users)
            self.session.commit()
            self.logger.info(f"✓ Inserted {len(users)} users using ORM")

            # Single checkpoint after all data is ready - this is expensive, so do it only once
            self.client.moctl.increment_checkpoint()
            self.logger.info("✓ Checkpoint completed using moctl")

        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"Failed to insert data: {e}")
            raise

    def demonstrate_basic_metadata_scanning(self):
        """Demonstrate basic metadata scanning operations."""
        self.logger.info("\n=== Basic Metadata Scanning ===")

        try:
            # Raw SQLAlchemy Result (backward compatibility)
            self.logger.info("1. Raw SQLAlchemy Result (backward compatibility):")
            result = self.client.metadata.scan("metadata_demo", "users")
            rows = result.fetchall()

            self.logger.info(f"Found {len(rows)} metadata entries:")
            for i, row in enumerate(rows[:3]):  # Show first 3 rows
                self.logger.info(f"  Row {i+1}:")
                self.logger.info(f"    Column: {row._mapping['col_name']}")
                self.logger.info(f"    Rows: {row._mapping['rows_cnt']}")
                self.logger.info(f"    Size: {row._mapping['origin_size']}")
                self.logger.info(f"    Nulls: {row._mapping['null_cnt']}")

            # Get table brief statistics
            self.logger.info("\n2. Table Brief Statistics:")
            brief_stats = self.client.metadata.get_table_brief_stats("metadata_demo", "users")
            table_stats = brief_stats["users"]
            self.logger.info(f"  Total objects: {table_stats['total_objects']}")
            self.logger.info(f"  Total rows: {table_stats['row_cnt']}")
            self.logger.info(f"  Total nulls: {table_stats['null_cnt']}")
            self.logger.info(f"  Original size: {table_stats['original_size']}")
            self.logger.info(f"  Compressed size: {table_stats['compress_size']}")

            # Get table detailed statistics
            self.logger.info("\n3. Table Detailed Statistics:")
            detail_stats = self.client.metadata.get_table_detail_stats("metadata_demo", "users")
            table_details = detail_stats["users"]
            for detail in table_details:
                self.logger.info(f"  Object: {detail['object_name']}")
                self.logger.info(f"    Created: {detail['create_ts']}")
                self.logger.info(f"    Rows: {detail['row_cnt']}, Nulls: {detail['null_cnt']}")
                self.logger.info(f"    Size: {detail['original_size']} -> {detail['compress_size']}")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"Error in basic metadata scanning: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'basic_metadata_scanning', 'error': str(e)})
        finally:
            self.results['tests_run'] += 1

    def demonstrate_structured_metadata(self):
        """Demonstrate structured metadata with fixed schema."""
        self.logger.info("\n=== Structured Metadata with Fixed Schema ===")

        try:
            # Get all columns as structured MetadataRow objects
            self.logger.info("1. All columns as structured MetadataRow objects:")
            rows = self.client.metadata.scan("metadata_demo", "users", columns="*")

            self.logger.info(f"Found {len(rows)} structured metadata entries:")
            for i, row in enumerate(rows[:2]):  # Show first 2 rows
                self.logger.info(f"  Row {i+1}:")
                self.logger.info(f"    Column: {row.col_name}")
                self.logger.info(f"    Object: {row.object_name}")
                self.logger.info(f"    Hidden: {row.is_hidden}")
                self.logger.info(f"    Rows: {row.rows_cnt}")
                self.logger.info(f"    Nulls: {row.null_cnt}")
                self.logger.info(f"    Size: {row.origin_size}")
                self.logger.info(f"    Min: {row.min}")
                self.logger.info(f"    Max: {row.max}")
                self.logger.info(f"    Type: {type(row).__name__}")

            # Demonstrate type safety
            self.logger.info("\n2. Type Safety Demonstration:")
            for row in rows[:1]:
                self.logger.info(f"  rows_cnt is int: {isinstance(row.rows_cnt, int)}")
                self.logger.info(f"  is_hidden is bool: {isinstance(row.is_hidden, bool)}")
                self.logger.info(f"  col_name is str: {isinstance(row.col_name, str)}")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"Error in structured metadata: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'structured_metadata', 'error': str(e)})
        finally:
            self.results['tests_run'] += 1

    def demonstrate_column_selection(self):
        """Demonstrate column selection capabilities."""
        self.logger.info("\n=== Column Selection Capabilities ===")

        try:
            # Select specific columns using enum
            self.logger.info("1. Specific columns using MetadataColumn enum:")
            rows = self.client.metadata.scan(
                "metadata_demo",
                "users",
                columns=[MetadataColumn.COL_NAME, MetadataColumn.ROWS_CNT, MetadataColumn.ORIGIN_SIZE],
            )

            for row in rows:
                self.logger.info(f"  {row['col_name']}: {row['rows_cnt']} rows, {row['origin_size']} bytes")

            # Select specific columns using strings
            self.logger.info("\n2. Specific columns using string names:")
            rows = self.client.metadata.scan("metadata_demo", "users", columns=['col_name', 'null_cnt', 'compress_size'])

            for row in rows:
                self.logger.info(f"  {row['col_name']}: {row['null_cnt']} nulls, {row['compress_size']} compressed bytes")

            # Show all available columns
            self.logger.info("\n3. Available metadata columns:")
            for col in MetadataColumn:
                self.logger.info(f"  {col.name}: {col.value}")

            # Test distinct object name functionality
            self.logger.info("\n4. Distinct object name functionality:")
            rows = self.client.metadata.scan("metadata_demo", "users", distinct_object_name=True)
            self.logger.info(f"  Found {len(rows)} distinct object names")
            for row in rows:
                self.logger.info(f"    Object: {row._mapping['object_name']}")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"Error in column selection: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'column_selection', 'error': str(e)})
        finally:
            self.results['tests_run'] += 1

    def demonstrate_advanced_metadata_operations(self):
        """Demonstrate advanced metadata operations."""
        self.logger.info("\n=== Advanced Metadata Operations ===")

        try:
            # Table statistics analysis
            self.logger.info("1. Table Statistics Analysis:")
            brief_stats = self.client.metadata.get_table_brief_stats("metadata_demo", "users")
            table_stats = brief_stats["users"]
            self.logger.info(f"  Total rows: {table_stats['row_cnt']}")
            self.logger.info(f"  Total objects: {table_stats['total_objects']}")
            self.logger.info(f"  Original size: {table_stats['original_size']}")
            self.logger.info(f"  Compressed size: {table_stats['compress_size']}")

            # Detailed object analysis
            self.logger.info("\n2. Detailed Object Analysis:")
            detail_stats = self.client.metadata.get_table_detail_stats("metadata_demo", "users")
            table_details = detail_stats["users"]
            self.logger.info(f"  Number of objects: {len(table_details)}")
            for i, detail in enumerate(table_details[:3]):  # Show first 3 objects
                self.logger.info(f"  Object {i+1}: {detail['object_name']}")
                self.logger.info(f"    Rows: {detail['row_cnt']}, Size: {detail['original_size']}")

            # Index metadata scanning
            self.logger.info("\n3. Index Metadata:")
            try:
                # Scan index metadata (using existing checkpoint)
                result = self.client.metadata.scan("metadata_demo", "users", indexname="idx_users_name")
                rows = result.fetchall()
                self.logger.info(f"  Index metadata entries: {len(rows)}")

                if rows:
                    for row in rows[:2]:  # Show first 2 index entries
                        self.logger.info(f"    Index column: {row._mapping.get('col_name', 'N/A')}")
                        self.logger.info(f"    Index rows: {row._mapping.get('rows_cnt', 'N/A')}")

            except Exception as e:
                self.logger.info(f"  Index metadata scan: {e}")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"Error in advanced metadata operations: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'advanced_metadata_operations', 'error': str(e)})
        finally:
            self.results['tests_run'] += 1

    def demonstrate_transaction_metadata(self):
        """Demonstrate transaction-based metadata operations."""
        self.logger.info("\n=== Transaction-Based Metadata Operations ===")

        try:
            with self.client.transaction() as tx:
                # Metadata operations within transaction
                self.logger.info("1. Metadata scan within transaction:")
                result = tx.metadata.scan("metadata_demo", "users", columns="*")

                self.logger.info(f"  Found {len(result)} metadata entries in transaction")
                for row in result[:2]:
                    self.logger.info(f"    {row.col_name}: {row.rows_cnt} rows")

                # Get table statistics within transaction
                self.logger.info("\n2. Table statistics within transaction:")
                brief_stats = tx.metadata.get_table_brief_stats("metadata_demo", "users")
                table_stats = brief_stats["users"]
                self.logger.info(f"    Total rows: {table_stats['row_cnt']}")
                self.logger.info(f"    Total objects: {table_stats['total_objects']}")
                self.logger.info(f"    Size: {table_stats['original_size']}")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"Error in transaction metadata: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'transaction_metadata', 'error': str(e)})
        finally:
            self.results['tests_run'] += 1

    async def demonstrate_async_metadata_operations(self):
        """Demonstrate asynchronous metadata operations."""
        self.logger.info("\n=== Asynchronous Metadata Operations ===")

        client = AsyncClient()
        host, port, user, password, database = self.connection_params
        await client.connect(host=host, port=port, user=user, password=password, database=database)

        try:
            # Async metadata scan with raw results
            self.logger.info("1. Async metadata scan (raw results):")
            result = await client.metadata.scan("metadata_demo", "users")
            rows = result.fetchall()

            self.logger.info(f"  Found {len(rows)} metadata entries")
            for row in rows[:2]:
                self.logger.info(f"    {row._mapping['col_name']}: {row._mapping['rows_cnt']} rows")

            # Async metadata scan with structured results
            self.logger.info("\n2. Async metadata scan (structured results):")
            rows = await client.metadata.scan("metadata_demo", "users", columns="*")

            self.logger.info(f"  Found {len(rows)} structured metadata entries")
            for row in rows[:2]:
                self.logger.info(f"    {row.col_name}: {row.rows_cnt} rows, {row.origin_size} bytes")

            # Async column selection
            self.logger.info("\n3. Async column selection:")
            rows = await client.metadata.scan(
                "metadata_demo", "users", columns=[MetadataColumn.COL_NAME, MetadataColumn.ROWS_CNT]
            )

            for row in rows:
                self.logger.info(f"    {row['col_name']}: {row['rows_cnt']} rows")

            # Async advanced operations
            self.logger.info("\n4. Async advanced operations:")
            brief_stats = await client.metadata.get_table_brief_stats("metadata_demo", "users")
            table_stats = brief_stats["users"]

            self.logger.info(f"    Total rows: {table_stats['row_cnt']}")
            self.logger.info(f"    Total objects: {table_stats['total_objects']}")
            self.logger.info(f"    Original size: {table_stats['original_size']}")
            self.logger.info(f"    Compressed size: {table_stats['compress_size']}")

            # Async distinct object name functionality
            self.logger.info("\n5. Async distinct object name functionality:")
            rows = await client.metadata.scan("metadata_demo", "users", distinct_object_name=True)
            self.logger.info(f"    Found {len(rows)} distinct object names")
            for row in rows:
                self.logger.info(f"      Object: {row._mapping['object_name']}")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"Error in async metadata operations: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'async_metadata_operations', 'error': str(e)})
        finally:
            self.results['tests_run'] += 1
            await client.disconnect()

    def demonstrate_metadata_row_features(self):
        """Demonstrate MetadataRow dataclass features."""
        self.logger.info("\n=== MetadataRow Dataclass Features ===")

        try:
            # Create a mock SQLAlchemy row for demonstration
            from unittest.mock import Mock

            mock_row = Mock()
            mock_row._mapping = {
                'col_name': 'user_id',
                'object_name': 'users_table',
                'is_hidden': 'false',
                'obj_loc': 's3://bucket/users',
                'create_ts': '2024-01-01 10:00:00',
                'delete_ts': 'NULL',
                'rows_cnt': 50000,
                'null_cnt': 0,
                'compress_size': 2500000,
                'origin_size': 5000000,
                'min': '1',
                'max': '50000',
                'sum': '1250025000',
            }

            # Convert to MetadataRow
            metadata_row = MetadataRow.from_sqlalchemy_row(mock_row)

            self.logger.info("MetadataRow object created from SQLAlchemy row:")
            self.logger.info(f"  Column Name: {metadata_row.col_name}")
            self.logger.info(f"  Object Name: {metadata_row.object_name}")
            self.logger.info(f"  Is Hidden: {metadata_row.is_hidden}")
            self.logger.info(f"  Row Count: {metadata_row.rows_cnt}")
            self.logger.info(f"  Null Count: {metadata_row.null_cnt}")
            self.logger.info(f"  Origin Size: {metadata_row.origin_size}")
            self.logger.info(f"  Min Value: {metadata_row.min}")
            self.logger.info(f"  Max Value: {metadata_row.max}")

            # Demonstrate type safety
            self.logger.info(f"\nType Safety:")
            self.logger.info(f"  rows_cnt is int: {isinstance(metadata_row.rows_cnt, int)}")
            self.logger.info(f"  is_hidden is bool: {isinstance(metadata_row.is_hidden, bool)}")
            self.logger.info(f"  col_name is str: {isinstance(metadata_row.col_name, str)}")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"Error in metadata row features: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'metadata_row_features', 'error': str(e)})
        finally:
            self.results['tests_run'] += 1

    def cleanup(self):
        """Clean up resources."""
        try:
            if self.session:
                self.session.close()
            if self.client:
                self.client.disconnect()
            self.logger.info("✓ Cleaned up resources")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

    def generate_summary_report(self):
        """Generate a comprehensive summary report."""
        self.logger.info("\n" + "=" * 80)
        self.logger.info("METADATA OPERATIONS DEMO - SUMMARY REPORT")
        self.logger.info("=" * 80)

        # Test Results Summary
        total_tests = self.results['tests_run']
        passed_tests = self.results['tests_passed']
        failed_tests = self.results['tests_failed']
        success_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 0

        self.logger.info(f"\n📊 Test Results Summary:")
        self.logger.info(f"  Total Tests Run: {total_tests}")
        self.logger.info(f"  Tests Passed: {passed_tests}")
        self.logger.info(f"  Tests Failed: {failed_tests}")
        self.logger.info(f"  Success Rate: {success_rate:.1f}%")

        # Unexpected Results
        unexpected_results = self.results['unexpected_results']
        if unexpected_results:
            self.logger.info(f"\n⚠️  Unexpected Results ({len(unexpected_results)}):")
            for i, result in enumerate(unexpected_results, 1):
                self.logger.info(f"  {i}. Test: {result['test']}")
                self.logger.info(f"     Error: {result['error']}")
        else:
            self.logger.info("\n✅ No unexpected results - all tests behaved as expected")

        # Key Features Demonstrated
        self.logger.info(f"\n🎯 Key Features Demonstrated:")
        self.logger.info(f"  ✅ ORM-based table creation with indexes")
        self.logger.info(f"  ✅ Basic metadata scanning (raw SQLAlchemy results)")
        self.logger.info(f"  ✅ Structured metadata with fixed schema")
        self.logger.info(f"  ✅ Column selection capabilities (enum and string-based)")
        self.logger.info(f"  ✅ Synchronous and asynchronous operations")
        self.logger.info(f"  ✅ Transaction-based metadata operations")
        self.logger.info(f"  ✅ Table brief and detailed statistics")
        self.logger.info(f"  ✅ Index metadata scanning with moctl flush")
        self.logger.info(f"  ✅ Type-safe metadata row objects")

        # Performance Insights
        self.logger.info(f"\n⚡ Performance Insights:")
        self.logger.info(f"  • ORM provides clean table definition with automatic index creation")
        self.logger.info(f"  • moctl.flush_table() ensures metadata availability")
        self.logger.info(f"  • Structured MetadataRow objects provide type safety")
        self.logger.info(f"  • Column selection reduces data transfer overhead")
        self.logger.info(f"  • Async operations enable concurrent metadata analysis")

        # Best Practices
        self.logger.info(f"\n💡 Best Practices Demonstrated:")
        self.logger.info(f"  • Use ORM for table creation instead of raw SQL")
        self.logger.info(f"  • Define indexes in table model for automatic creation")
        self.logger.info(f"  • Use moctl.flush_table() after data changes")
        self.logger.info(f"  • Leverage structured metadata for type safety")
        self.logger.info(f"  • Use column selection to optimize performance")
        self.logger.info(f"  • Implement proper error handling and cleanup")

        self.logger.info(f"\n🎉 Metadata Operations Demo Completed Successfully!")
        self.logger.info("=" * 80)

        return self.results

    def run_demo(self):
        """Run the complete metadata operations demonstration."""
        try:
            # Setup
            self.setup_connection()
            User = self.create_test_models()
            self.insert_test_data(User)

            # Run all demonstrations
            self.demonstrate_basic_metadata_scanning()
            self.demonstrate_structured_metadata()
            self.demonstrate_column_selection()
            self.demonstrate_advanced_metadata_operations()
            self.demonstrate_transaction_metadata()
            self.demonstrate_metadata_row_features()

            # Run async demonstration
            asyncio.run(self.demonstrate_async_metadata_operations())

            # Generate report
            results = self.generate_summary_report()

            return results

        except Exception as e:
            self.logger.error(f"Demo failed: {e}")
            raise
        finally:
            self.cleanup()


def main():
    """Main function to run the metadata operations demo."""
    demo = MetadataOperationsDemo()
    return demo.run_demo()


if __name__ == "__main__":
    main()
