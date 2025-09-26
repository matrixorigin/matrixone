#!/usr/bin/env python3
"""
Example 09: Logger Integration - Comprehensive Logging Operations

This example demonstrates how to use the MatrixOne Python SDK with custom logging:
1. Default logger configuration
2. Custom logger integration
3. Performance logging
4. SQL query logging
5. Structured logging
6. Error tracking

This example shows how to integrate MatrixOne SDK logging with your business application.
"""

import logging
import asyncio
from matrixone import Client, AsyncClient
from matrixone.logger import MatrixOneLogger, create_default_logger, create_custom_logger
from matrixone.config import get_connection_params, print_config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)


class LoggerIntegrationDemo:
    """Demonstrates logger integration capabilities with comprehensive testing."""
    
    def __init__(self):
        self.logger = create_default_logger(
            enable_performance_logging=True,
            enable_sql_logging=True
        )
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
            'logger_performance': {}
        }

    def test_default_logger(self):
        """Test default logger configuration"""
        print("\n=== Default Logger Tests ===")
        
        self.results['tests_run'] += 1
        
        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()
            
            # Create client with default logger
            client = Client(
                enable_performance_logging=True,
                enable_sql_logging=True,
                enable_full_sql_logging=True
            )
            
            # Test default logger
            self.logger.info("Test: Default Logger Configuration")
            try:
                # Connect to MatrixOne
                client.connect(host, port, user, password, database)
                
                # Execute some queries to see logging in action
                client.execute("SELECT 1 as test_value")
                client.execute("SELECT VERSION() as version")
                client.execute("SHOW DATABASES")
                
                self.logger.info("✅ Default logger test completed")
                self.results['tests_passed'] += 1
                
            except Exception as e:
                self.logger.error(f"❌ Default logger test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({
                    'test': 'Default Logger Configuration',
                    'error': str(e)
                })
            
            client.disconnect()
            
        except Exception as e:
            self.logger.error(f"❌ Default logger test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({
                'test': 'Default Logger',
                'error': str(e)
            })

    def test_custom_logger(self):
        """Test custom logger integration"""
        print("\n=== Custom Logger Tests ===")
        
        self.results['tests_run'] += 1
        
        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()
            
            # Create custom logger
            import logging
            custom_logger_instance = logging.getLogger("custom_matrixone_logger")
            custom_logger_instance.setLevel(logging.INFO)
            
            # Create console handler with custom format
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s [CUSTOM] %(levelname)s: %(message)s')
            console_handler.setFormatter(formatter)
            custom_logger_instance.addHandler(console_handler)
            custom_logger_instance.propagate = False
            
            custom_logger = create_custom_logger(
                logger=custom_logger_instance,
                enable_performance_logging=True,
                enable_sql_logging=True
            )
            
            # Test custom logger
            self.logger.info("Test: Custom Logger Integration")
            try:
                # Create client with custom logger
                client = Client(logger=custom_logger)
                client.connect(host, port, user, password, database)
                
                # Execute queries with custom logging
                client.execute("SELECT 1 as custom_test")
                client.execute('SELECT USER() AS "current_user"')
                
                self.logger.info("✅ Custom logger test completed")
                self.results['tests_passed'] += 1
                
            except Exception as e:
                self.logger.error(f"❌ Custom logger test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({
                    'test': 'Custom Logger Integration',
                    'error': str(e)
                })
            
            client.disconnect()
            
        except Exception as e:
            self.logger.error(f"❌ Custom logger test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({
                'test': 'Custom Logger',
                'error': str(e)
            })

    def test_structured_logging(self):
        """Test structured logging"""
        print("\n=== Structured Logging Tests ===")
        
        self.results['tests_run'] += 1
        
        try:
            # Test structured logging
            self.logger.info("Test: Structured Logging")
            try:
                # Create structured logger
                import logging
                structured_logger_instance = logging.getLogger("structured_logger")
                structured_logger_instance.setLevel(logging.INFO)
                
                # Create console handler with custom format
                console_handler = logging.StreamHandler()
                console_handler.setLevel(logging.INFO)
                formatter = logging.Formatter('%(asctime)s [STRUCTURED] %(levelname)s: %(message)s')
                console_handler.setFormatter(formatter)
                structured_logger_instance.addHandler(console_handler)
                structured_logger_instance.propagate = False
                
                structured_logger = create_custom_logger(
                    logger=structured_logger_instance,
                    enable_performance_logging=True,
                    enable_sql_logging=True
                )
                
                # Test structured logging with context
                structured_logger.info("Testing structured logging with context")
                structured_logger.info("Database operation started", extra={
                    'operation': 'SELECT',
                    'table': 'test_table',
                    'user': 'test_user'
                })
                
                self.logger.info("✅ Structured logging test completed")
                self.results['tests_passed'] += 1
                
            except Exception as e:
                self.logger.error(f"❌ Structured logging test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({
                    'test': 'Structured Logging',
                    'error': str(e)
                })
            
        except Exception as e:
            self.logger.error(f"❌ Structured logging test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({
                'test': 'Structured Logging',
                'error': str(e)
            })

    def test_logger_levels(self):
        """Test different logger levels"""
        print("\n=== Logger Levels Tests ===")
        
        self.results['tests_run'] += 1
        
        try:
            # Test different logger levels
            self.logger.info("Test: Logger Levels")
            try:
                # Create logger with different levels
                import logging
                
                # Debug logger
                debug_logger_instance = logging.getLogger("debug_logger")
                debug_logger_instance.setLevel(logging.DEBUG)
                debug_handler = logging.StreamHandler()
                debug_handler.setLevel(logging.DEBUG)
                debug_formatter = logging.Formatter('%(asctime)s [DEBUG] %(levelname)s: %(message)s')
                debug_handler.setFormatter(debug_formatter)
                debug_logger_instance.addHandler(debug_handler)
                debug_logger_instance.propagate = False
                debug_logger = create_custom_logger(logger=debug_logger_instance)
                
                # Info logger
                info_logger_instance = logging.getLogger("info_logger")
                info_logger_instance.setLevel(logging.INFO)
                info_handler = logging.StreamHandler()
                info_handler.setLevel(logging.INFO)
                info_formatter = logging.Formatter('%(asctime)s [INFO] %(levelname)s: %(message)s')
                info_handler.setFormatter(info_formatter)
                info_logger_instance.addHandler(info_handler)
                info_logger_instance.propagate = False
                info_logger = create_custom_logger(logger=info_logger_instance)
                
                # Warning logger
                warning_logger_instance = logging.getLogger("warning_logger")
                warning_logger_instance.setLevel(logging.WARNING)
                warning_handler = logging.StreamHandler()
                warning_handler.setLevel(logging.WARNING)
                warning_formatter = logging.Formatter('%(asctime)s [WARNING] %(levelname)s: %(message)s')
                warning_handler.setFormatter(warning_formatter)
                warning_logger_instance.addHandler(warning_handler)
                warning_logger_instance.propagate = False
                warning_logger = create_custom_logger(logger=warning_logger_instance)
                
                # Test different levels
                debug_logger.debug("Debug message")
                info_logger.info("Info message")
                warning_logger.warning("Warning message")
                
                self.logger.info("✅ Logger levels test completed")
                self.results['tests_passed'] += 1
                
            except Exception as e:
                self.logger.error(f"❌ Logger levels test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({
                    'test': 'Logger Levels',
                    'error': str(e)
                })
            
        except Exception as e:
            self.logger.error(f"❌ Logger levels test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({
                'test': 'Logger Levels',
                'error': str(e)
            })

    def test_performance_logging(self):
        """Test performance logging"""
        print("\n=== Performance Logging Tests ===")
        
        self.results['tests_run'] += 1
        
        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()
            
            # Test performance logging
            self.logger.info("Test: Performance Logging")
            try:
                # Create client with performance logging
                client = Client(
                    enable_performance_logging=True,
                    enable_sql_logging=True
                )
                client.connect(host, port, user, password, database)
                
                # Execute queries to test performance logging
                client.execute("SELECT 1 as performance_test")
                client.execute("SELECT COUNT(*) FROM mo_catalog.mo_database")
                
                self.logger.info("✅ Performance logging test completed")
                self.results['tests_passed'] += 1
                
            except Exception as e:
                self.logger.error(f"❌ Performance logging test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({
                    'test': 'Performance Logging',
                    'error': str(e)
                })
            
            client.disconnect()
            
        except Exception as e:
            self.logger.error(f"❌ Performance logging test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({
                'test': 'Performance Logging',
                'error': str(e)
            })

    async def test_async_logger(self):
        """Test async logger"""
        print("\n=== Async Logger Tests ===")
        
        self.results['tests_run'] += 1
        
        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()
            
            # Test async logger
            self.logger.info("Test: Async Logger")
            try:
                # Create async client with logging
                client = AsyncClient(
                    enable_performance_logging=True,
                    enable_sql_logging=True
                )
                await client.connect(host, port, user, password, database)
                
                # Execute async queries
                await client.execute("SELECT 1 as async_test")
                await client.execute("SELECT USER() as async_user")
                
                self.logger.info("✅ Async logger test completed")
                self.results['tests_passed'] += 1
                
            except Exception as e:
                self.logger.error(f"❌ Async logger test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({
                    'test': 'Async Logger',
                    'error': str(e)
                })
            
            await client.disconnect()
            
        except Exception as e:
            self.logger.error(f"❌ Async logger test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({
                'test': 'Async Logger',
                'error': str(e)
            })

    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        print("\n" + "=" * 80)
        print("Logger Integration Demo - Summary Report")
        print("=" * 80)
        
        total_tests = self.results['tests_run']
        passed_tests = self.results['tests_passed']
        failed_tests = self.results['tests_failed']
        unexpected_results = self.results['unexpected_results']
        logger_performance = self.results['logger_performance']
        
        print(f"Total Tests Run: {total_tests}")
        print(f"Tests Passed: {passed_tests}")
        print(f"Tests Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests*100):.1f}%" if total_tests > 0 else "N/A")
        
        # Performance summary
        if logger_performance:
            print(f"\nLogger Integration Performance Results:")
            for test_name, time_taken in logger_performance.items():
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


def main():
    """Main demo function"""
    demo = LoggerIntegrationDemo()
    
    try:
        print("🚀 MatrixOne Logger Integration Examples")
        print("=" * 60)
        
        # Run tests
        demo.test_default_logger()
        demo.test_custom_logger()
        demo.test_structured_logging()
        demo.test_logger_levels()
        demo.test_performance_logging()
        
        # Run async tests
        asyncio.run(demo.test_async_logger())
        
        # Generate report
        results = demo.generate_summary_report()
        
        print("\n🎉 All logger integration examples completed!")
        print("\nKey features demonstrated:")
        print("- ✅ Default logger configuration")
        print("- ✅ Custom logger integration")
        print("- ✅ Structured logging with context")
        print("- ✅ Async client logging")
        print("- ✅ Different logger levels")
        print("- ✅ Business application integration")
        print("- ✅ Performance monitoring")
        print("- ✅ SQL query logging")
        print("- ✅ Error tracking and reporting")
        
        return results
        
    except Exception as e:
        print(f"Demo failed with error: {e}")
        return None


if __name__ == '__main__':
    main()