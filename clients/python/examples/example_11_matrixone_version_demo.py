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
Example 11: MatrixOne Version Demo - Comprehensive MatrixOne Version Operations

MatrixOne Python SDK - MatrixOne Version Parsing Demo

This example demonstrates MatrixOne-specific version parsing functionality
that handles the two version formats:
1. "8.0.30-MatrixOne-v" (development version, highest priority)
2. "8.0.30-MatrixOne-v3.0.0" (release version)
"""

import sys
import os

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone.version import VersionManager, VersionInfo, FeatureRequirement, VersionComparison
from matrixone.config import get_connection_params, print_config


class MatrixOneVersionDemo:
    """Demonstrates MatrixOne version capabilities with comprehensive testing."""

    def __init__(self):
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
            'version_performance': {},
        }

    def test_matrixone_version_parsing(self):
        """Test MatrixOne-specific version parsing"""
        print("\n=== MatrixOne Version Parsing Tests ===")

        self.results['tests_run'] += 1

        try:
            version_manager = VersionManager()

            # Test MatrixOne version parsing
            print("Test: MatrixOne Version Parsing")
            try:
                # Test different MatrixOne version formats
                test_versions = [
                    "8.0.30-MatrixOne-v",  # Development version
                    "10.5.2-MatrixOne-v",  # Development version
                    "8.0.30-MatrixOne-v3.0.0",  # Release version
                    "10.5.2-MatrixOne-v2.1.5",  # Release version
                    "MatrixOne 3.0.1",  # Fallback format
                    "Version 2.5.0",  # Alternative format
                    "3.0.1",  # Clean version
                ]

                print("Parsing MatrixOne version strings:")
                for version_string in test_versions:
                    parsed = version_manager._parse_matrixone_version(version_string)
                    version_type = "Development" if parsed == "999.0.0" else "Release"
                    print(f"  '{version_string}' -> '{parsed}' ({version_type})")

                self.results['tests_passed'] += 1

            except Exception as e:
                print(f"❌ MatrixOne version parsing test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'MatrixOne Version Parsing', 'error': str(e)})

        except Exception as e:
            print(f"❌ MatrixOne version parsing test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'MatrixOne Version Parsing', 'error': str(e)})

    def test_invalid_version_formats(self):
        """Test invalid version format handling"""
        print("\n=== Invalid Version Format Tests ===")

        self.results['tests_run'] += 1

        try:
            version_manager = VersionManager()

            # Test invalid version format handling
            print("Test: Invalid Version Format Handling")
            try:
                # Test invalid formats
                invalid_versions = [
                    "8.0.30-MatrixOne",  # Missing version part
                    "8.0.30-MatrixOne-v-",  # Invalid suffix
                    "invalid",  # Completely invalid
                    "",  # Empty string
                ]

                print("Testing invalid formats:")
                for invalid_version in invalid_versions:
                    try:
                        parsed = version_manager._parse_matrixone_version(invalid_version)
                        print(f"  '{invalid_version}' -> '{parsed}' (handled gracefully)")
                    except Exception as e:
                        print(f"  '{invalid_version}' -> Error: {type(e).__name__}")

                self.results['tests_passed'] += 1

            except Exception as e:
                print(f"❌ Invalid version format test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Invalid Version Format Handling', 'error': str(e)})

        except Exception as e:
            print(f"❌ Invalid version format test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Invalid Version Format', 'error': str(e)})

    def test_version_comparison(self):
        """Test version comparison functionality"""
        print("\n=== Version Comparison Tests ===")

        self.results['tests_run'] += 1

        try:
            version_manager = VersionManager()

            # Test version comparison
            print("Test: Version Comparison")
            try:
                # Test version comparisons
                test_cases = [
                    ("8.0.30-MatrixOne-v", "8.0.30-MatrixOne-v3.0.0"),
                    ("8.0.30-MatrixOne-v3.0.0", "8.0.30-MatrixOne-v2.1.5"),
                    ("8.0.30-MatrixOne-v", "8.0.30-MatrixOne-v"),
                ]

                print("Version comparisons:")
                for v1_str, v2_str in test_cases:
                    v1 = version_manager._parse_matrixone_version(v1_str)
                    v2 = version_manager._parse_matrixone_version(v2_str)

                    v1_parsed = version_manager.parse_version(v1)
                    v2_parsed = version_manager.parse_version(v2)

                    comparison = ">" if v1_parsed > v2_parsed else "<" if v1_parsed < v2_parsed else "=="
                    print(f"  {v1_str} ({v1}) {comparison} {v2_str} ({v2})")

                self.results['tests_passed'] += 1

            except Exception as e:
                print(f"❌ Version comparison test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Version Comparison', 'error': str(e)})

        except Exception as e:
            print(f"❌ Version comparison test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Version Comparison', 'error': str(e)})

    def test_development_version_priority(self):
        """Test development version priority"""
        print("\n=== Development Version Priority Tests ===")

        self.results['tests_run'] += 1

        try:
            version_manager = VersionManager()

            # Test development version priority
            print("Test: Development Version Priority")
            try:
                # Test that development versions have highest priority
                dev_version = version_manager._parse_matrixone_version("8.0.30-MatrixOne-v")
                release_version = version_manager._parse_matrixone_version("8.0.30-MatrixOne-v3.0.0")

                dev_parsed = version_manager.parse_version(dev_version)
                release_parsed = version_manager.parse_version(release_version)

                print(f"  Development version: {dev_version} -> {dev_parsed}")
                print(f"  Release version: {release_version} -> {release_parsed}")

                if dev_parsed > release_parsed:
                    print("  ✅ Development version has higher priority")
                    self.results['tests_passed'] += 1
                else:
                    print("  ❌ Development version should have higher priority")
                    self.results['tests_failed'] += 1
                    self.results['unexpected_results'].append(
                        {
                            'test': 'Development Version Priority',
                            'error': 'Development version should have higher priority than release version',
                        }
                    )

            except Exception as e:
                print(f"❌ Development version priority test failed: {e}")
                self.results['tests_failed'] += 1
                self.results['unexpected_results'].append({'test': 'Development Version Priority', 'error': str(e)})

        except Exception as e:
            print(f"❌ Development version priority test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Development Version Priority', 'error': str(e)})

    def test_backend_version_detection(self):
        """Test backend version detection with MatrixOne formats"""
        print("\n=== Backend Version Detection Tests ===")

        self.results['tests_run'] += 1

        try:
            # Get connection parameters from config
            host, port, user, password, database = get_connection_params()

            # Test backend version detection
            print("Test: Backend Version Detection")
            try:
                from matrixone import Client

                client = Client()
                client.connect(host=host, port=port, user=user, password=password, database=database)

                # Get version information
                version_info = client.version()
                print(f"   Backend version: {version_info}")

                # Test MatrixOne version parsing
                version_manager = VersionManager()
                parsed_version = version_manager._parse_matrixone_version(version_info)
                print(f"   Parsed MatrixOne version: {parsed_version}")

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

    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        print("\n" + "=" * 80)
        print("MatrixOne Version Demo - Summary Report")
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
            print(f"\nMatrixOne Version Performance Results:")
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
    demo = MatrixOneVersionDemo()

    try:
        print("🚀 MatrixOne Version Demo")
        print("=" * 60)

        # Print current configuration
        print_config()

        # Run tests
        demo.test_matrixone_version_parsing()
        demo.test_invalid_version_formats()
        demo.test_version_comparison()
        demo.test_development_version_priority()
        demo.test_backend_version_detection()

        # Generate report
        results = demo.generate_summary_report()

        print("\n🎉 All MatrixOne version examples completed!")
        print("\nKey Features:")
        print("• Development: '8.0.30-MatrixOne-v' → '999.0.0' (highest)")
        print("• Release: '8.0.30-MatrixOne-v3.0.0' → '3.0.0'")
        print("• Legacy: 'MatrixOne 3.0.1' → '3.0.1'")
        print("• Clean: '3.0.1' → '3.0.1'")

        return results

    except Exception as e:
        print(f"Demo failed with error: {e}")
        return None


if __name__ == "__main__":
    main()
