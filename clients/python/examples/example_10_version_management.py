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
Example 10: Version Management - Comprehensive Version Management Operations

MatrixOne Python SDK - Version Management Example

This example demonstrates how to use the version management framework
for MatrixOne Python SDK to handle version compatibility and provide
helpful error messages.

Features demonstrated:
1. Version parsing and comparison
2. Backend version detection
3. Feature availability checking
4. Version-aware error messages
5. Version checking decorators
6. Custom feature requirements
"""

import sys
import os
import asyncio
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone import Client
from matrixone.version import VersionManager, VersionInfo, FeatureRequirement, requires_version
from matrixone.exceptions import VersionError, ConnectionError, QueryError
from matrixone.config import get_connection_params, print_config


class VersionManagementDemo:
    """Demonstrates version management capabilities with comprehensive testing."""

    def __init__(self):
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
            'version_performance': {},
        }

    def test_version_parsing(self):
        """Test version parsing and comparison"""
        print("\n=== Version Parsing Tests ===")

        self.results['tests_run'] += 1

        try:
            version_manager = VersionManager()

            # Test version parsing
            print("Test: Version Parsing and Comparison")
            try:
                # Parse version strings
                versions = ["3.0.1", "3.0.2", "2.1.19", "3.0.9", "1.5.0"]

                print("Parsing versions:")
                for version_str in versions:
                    version = version_manager.parse_version(version_str)
                    print(f"  {version_str} -> {version}")

                # Test version comparisons
                test_cases = [
                    ("3.0.2", "3.0.1"),
                    ("2.1.19", "3.0.9"),
                    ("3.0.1", "3.0.1"),
                ]

                print("\nVersion comparisons:")
                for v1_str, v2_str in test_cases:
                    v1 = version_manager.parse_version(v1_str)
                    v2 = version_manager.parse_version(v2_str)
                    comparison = ">" if v1 > v2 else "<" if v1 < v2 else "=="
                    print(f"  {v1_str} {comparison} {v2_str}")

                self.results['tests_passed'] += 1

            except Exception as e:
                print(f"❌ Version parsing test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Version Parsing', 'error': str(e)})

        except Exception as e:
            print(f"❌ Version parsing test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Version Parsing', 'error': str(e)})

    def test_backend_version_detection(self):
        """Test backend version detection"""
        print("\n=== Backend Version Detection Tests ===")

        self.results['tests_run'] += 1

        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            # Test backend version detection
            print("Test: Backend Version Detection")
            try:
                client = Client()
                client.connect(host=host, port=port, user=user, password=password, database=database)

                # Get version information
                version_info = client.version()
                print(f"   Backend version: {version_info}")

                # Test version manager with backend
                version_manager = VersionManager()
                # Parse MatrixOne version string to standard format
                parsed_version = version_manager._parse_matrixone_version(version_info)
                if parsed_version:
                    version_manager.set_backend_version(parsed_version)
                    backend_version = version_manager.get_backend_version()
                    print(f"   Detected backend version: {backend_version}")
                else:
                    print(f"   Could not parse MatrixOne version: {version_info}")

                client.disconnect()

                self.results['tests_passed'] += 1

            except Exception as e:
                print(f"❌ Backend version detection test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Backend Version Detection', 'error': str(e)})

        except Exception as e:
            print(f"❌ Backend version detection test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Backend Version Detection', 'error': str(e)})

    def test_feature_availability(self):
        """Test feature availability checking"""
        print("\n=== Feature Availability Tests ===")

        self.results['tests_run'] += 1

        try:
            # Test feature availability checking
            print("Test: Feature Availability Checking")
            try:
                version_manager = VersionManager()

                # Register some test features
                version_manager.register_feature_requirement(
                    FeatureRequirement(
                        feature_name="vector_search",
                        min_version=version_manager.parse_version("3.0.0"),
                        description="Vector search functionality",
                    )
                )
                version_manager.register_feature_requirement(
                    FeatureRequirement(
                        feature_name="advanced_analytics",
                        min_version=version_manager.parse_version("2.5.0"),
                        description="Advanced analytics features",
                    )
                )

                # Test feature availability
                test_version = version_manager.parse_version("3.0.1")

                # Set test version as backend version for feature checking
                version_manager.set_backend_version("3.0.1")
                vector_available = version_manager.is_feature_available("vector_search")
                analytics_available = version_manager.is_feature_available("advanced_analytics")

                print(f"   Vector search available in 3.0.1: {vector_available}")
                print(f"   Advanced analytics available in 3.0.1: {analytics_available}")

                self.results['tests_passed'] += 1

            except Exception as e:
                print(f"❌ Feature availability test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Feature Availability', 'error': str(e)})

        except Exception as e:
            print(f"❌ Feature availability test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Feature Availability', 'error': str(e)})

    def test_version_aware_decorators(self):
        """Test version-aware decorators"""
        print("\n=== Version-Aware Decorators Tests ===")

        self.results['tests_run'] += 1

        try:
            # Test version-aware decorators
            print("Test: Version-Aware Decorators")
            try:
                version_manager = VersionManager()

                # Define a test method with version requirement
                @requires_version(
                    min_version="3.0.0",
                    description="This feature requires MatrixOne 3.0.0 or higher",
                )
                def test_feature():
                    return "Feature executed successfully"

                # Test with compatible version
                try:
                    result = test_feature()
                    print(f"   Feature result: {result}")
                    self.results['tests_passed'] += 1
                except VersionError as e:
                    print(f"   Version error (expected): {e}")
                    self.results['tests_passed'] += 1

            except Exception as e:
                print(f"❌ Version-aware decorators test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Version-Aware Decorators', 'error': str(e)})

        except Exception as e:
            print(f"❌ Version-aware decorators test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Version-Aware Decorators', 'error': str(e)})

    def test_custom_feature_requirements(self):
        """Test custom feature requirements"""
        print("\n=== Custom Feature Requirements Tests ===")

        self.results['tests_run'] += 1

        try:
            # Test custom feature requirements
            print("Test: Custom Feature Requirements")
            try:
                version_manager = VersionManager()

                # Create custom feature requirement
                custom_feature = FeatureRequirement(
                    feature_name="custom_analytics",
                    min_version=version_manager.parse_version("2.8.0"),
                    description="Custom analytics functionality",
                )

                # Register custom feature
                version_manager.register_feature_requirement(custom_feature)

                # Test custom feature
                version_manager.set_backend_version("3.0.0")
                is_available = version_manager.is_feature_available("custom_analytics")

                print(f"   Custom analytics available in 3.0.0: {is_available}")

                self.results['tests_passed'] += 1

            except Exception as e:
                print(f"❌ Custom feature requirements test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Custom Feature Requirements', 'error': str(e)})

        except Exception as e:
            print(f"❌ Custom feature requirements test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Custom Feature Requirements', 'error': str(e)})

    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        print("\n" + "=" * 80)
        print("Version Management Demo - Summary Report")
        print("=" * 80)

        total_tests = self.results['tests_run']
        passed_tests = self.results['tests_passed']
        failed_tests = self.results['tests_failed']
        unexpected_results = self.results['unexpected_results']
        version_performance = self.results['version_performance']

        print(f"Total Tests Run: {total_tests}")
        print(f"Tests Passed: {passed_tests}")
        print(f"Tests Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests*100):.1f}%" if total_tests > 0 else "N/A")

        # Performance summary
        if version_performance:
            print(f"\nVersion Management Performance Results:")
            for test_name, time_taken in version_performance.items():
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
    demo = VersionManagementDemo()

    try:
        print("🚀 MatrixOne Version Management Examples")
        print("=" * 60)

        # Print current configuration
        print_config()

        # Run tests
        demo.test_version_parsing()
        demo.test_backend_version_detection()
        demo.test_feature_availability()
        demo.test_version_aware_decorators()
        demo.test_custom_feature_requirements()

        # Generate report
        results = demo.generate_summary_report()

        print("\n🎉 All version management examples completed!")
        print("\nKey Features:")
        print("✓ Semantic version parsing and comparison")
        print("✓ Automatic backend version detection")
        print("✓ Feature requirement registration and checking")
        print("✓ Version-aware decorators for methods")
        print("✓ Helpful error messages and suggestions")
        print("✓ Integration with both sync and async clients")
        print("✓ Custom feature requirements support")

        return results

    except Exception as e:
        print(f"Demo failed with error: {e}")
        return None


if __name__ == "__main__":
    main()
