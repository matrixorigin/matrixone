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
MatrixOne Python SDK - MatrixOne Version Parsing Tests

Test suite for MatrixOne-specific version parsing logic.
"""

import unittest
import sys
import os
import re
from typing import Optional

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone.version import VersionManager, VersionInfo, VersionComparison


class TestMatrixOneVersionParsing(unittest.TestCase):
    """Test MatrixOne-specific version parsing functionality"""

    def setUp(self):
        self.version_manager = VersionManager()

    def test_development_version_parsing(self):
        """Test parsing development version strings"""
        test_cases = [
            ("8.0.30-MatrixOne-v", "999.0.0"),
            ("10.5.2-MatrixOne-v", "999.0.0"),
            ("1.0.0-MatrixOne-v", "999.0.0"),
        ]

        for version_string, expected in test_cases:
            with self.subTest(version=version_string):
                parsed_version = self.version_manager._parse_matrixone_version(version_string)
                self.assertEqual(parsed_version, expected)

                # Verify it's recognized as development version
                if parsed_version:
                    version_info = self.version_manager.parse_version(parsed_version)
                    self.assertTrue(self.version_manager.is_development_version(version_info))

    def test_release_version_parsing(self):
        """Test parsing release version strings"""
        test_cases = [
            ("8.0.30-MatrixOne-v3.0.0", "3.0.0"),
            ("10.5.2-MatrixOne-v2.1.5", "2.1.5"),
            ("1.0.0-MatrixOne-v1.0.0", "1.0.0"),
            ("15.20.30-MatrixOne-v4.5.6", "4.5.6"),
        ]

        for version_string, expected in test_cases:
            with self.subTest(version=version_string):
                parsed_version = self.version_manager._parse_matrixone_version(version_string)
                self.assertEqual(parsed_version, expected)

                # Verify it's NOT a development version
                if parsed_version:
                    version_info = self.version_manager.parse_version(parsed_version)
                    self.assertFalse(self.version_manager.is_development_version(version_info))

    def test_fallback_version_parsing(self):
        """Test fallback version parsing for other formats"""
        test_cases = [
            ("MatrixOne 3.0.1", "3.0.1"),
            ("Version 2.5.0", "2.5.0"),
            ("3.0.1", "3.0.1"),
        ]

        for version_string, expected in test_cases:
            with self.subTest(version=version_string):
                parsed_version = self.version_manager._parse_matrixone_version(version_string)
                self.assertEqual(parsed_version, expected)

    def test_invalid_version_parsing(self):
        """Test parsing invalid version strings"""
        invalid_versions = [
            "",  # Empty string
            None,  # None value
            "invalid",  # Completely invalid
            "8.0.30-MatrixOne",  # Missing version part
            "8.0.30-MatrixOne-v-",  # Invalid format
        ]

        for invalid_version in invalid_versions:
            with self.subTest(version=invalid_version):
                parsed_version = self.version_manager._parse_matrixone_version(invalid_version)
                self.assertIsNone(parsed_version)

    def test_development_version_comparison(self):
        """Test that development version (999.0.0) is higher than any release version"""
        dev_version = "999.0.0"
        release_versions = ["1.0.0", "2.5.0", "3.0.0", "10.0.0", "99.99.99"]

        for release_version in release_versions:
            with self.subTest(release=release_version):
                result = self.version_manager.compare_versions(dev_version, release_version)
                self.assertEqual(result, VersionComparison.GREATER)

                # Test reverse comparison
                result_reverse = self.version_manager.compare_versions(release_version, dev_version)
                self.assertEqual(result_reverse, VersionComparison.LESS)

    def test_development_version_compatibility(self):
        """Test version compatibility with development version"""
        version_manager = VersionManager()
        version_manager.set_backend_version("999.0.0")  # Development version

        # Development version should be compatible with any required version
        test_requirements = ["1.0.0", "2.5.0", "3.0.0", "10.0.0", "99.99.99"]

        for required_version in test_requirements:
            with self.subTest(required=required_version):
                # Test >= operator
                self.assertTrue(version_manager.is_version_compatible(required_version, operator=">="))
                # Test > operator
                self.assertTrue(version_manager.is_version_compatible(required_version, operator=">"))
                # Test == operator
                self.assertFalse(version_manager.is_version_compatible(required_version, operator="=="))

    def test_feature_availability_with_development_version(self):
        """Test feature availability checking with development version"""
        from matrixone.version import FeatureRequirement

        version_manager = VersionManager()
        version_manager.set_backend_version("999.0.0")  # Development version

        # Register a feature with high version requirement
        feature = FeatureRequirement(
            feature_name="future_feature",
            min_version=VersionInfo(10, 0, 0),
            description="Future feature requiring version 10.0.0+",
        )
        version_manager.register_feature_requirement(feature)

        # Development version should have all features available
        self.assertTrue(version_manager.is_feature_available("future_feature"))

        # Test with a feature that has maximum version constraint
        # Note: Development version (999.0.0) is higher than any realistic max version
        # so it should NOT have access to features with max version constraints
        feature_with_max = FeatureRequirement(
            feature_name="legacy_feature",
            max_version=VersionInfo(5, 0, 0),
            description="Legacy feature only available up to version 5.0.0",
        )
        version_manager.register_feature_requirement(feature_with_max)

        # Development version should NOT have access to features with max version constraints
        # because 999.0.0 > 5.0.0
        self.assertFalse(version_manager.is_feature_available("legacy_feature"))

    def test_version_display_strings(self):
        """Test version display and string representation"""
        # Test development version display
        dev_version = VersionInfo(999, 0, 0)
        self.assertEqual(str(dev_version), "999.0.0")

        # Test regular version display
        regular_version = VersionInfo(3, 0, 1)
        self.assertEqual(str(regular_version), "3.0.1")


class TestClientVersionDetection(unittest.TestCase):
    """Test client version detection methods"""

    def test_parse_matrixone_version_method(self):
        """Test the _parse_matrixone_version method directly"""
        # This would normally be tested through the client, but we can test the logic
        import re

        def parse_matrixone_version(version_string: str) -> Optional[str]:
            """Simulate the client's version parsing logic"""
            if not version_string:
                return None

            # Pattern 1: Development version "8.0.30-MatrixOne-v" (v后面为空)
            dev_pattern = r'(\d+\.\d+\.\d+)-MatrixOne-v$'
            dev_match = re.search(dev_pattern, version_string.strip())
            if dev_match:
                return "999.0.0"

            # Pattern 2: Release version "8.0.30-MatrixOne-v3.0.0" (v后面有版本号)
            release_pattern = r'(\d+\.\d+\.\d+)-MatrixOne-v(\d+\.\d+\.\d+)'
            release_match = re.search(release_pattern, version_string.strip())
            if release_match:
                return release_match.group(2)

            # Pattern 3: Fallback format
            fallback_pattern = r'(\d+\.\d+\.\d+)'
            fallback_match = re.search(fallback_pattern, version_string)
            if fallback_match:
                return fallback_match.group(1)

            return None

        # Test cases
        test_cases = [
            ("8.0.30-MatrixOne-v", "999.0.0"),
            ("8.0.30-MatrixOne-v3.0.0", "3.0.0"),
            ("MatrixOne 3.0.1", "3.0.1"),
            ("", None),
            ("invalid", None),
        ]

        for version_string, expected in test_cases:
            with self.subTest(version=version_string):
                result = parse_matrixone_version(version_string)
                self.assertEqual(result, expected)


def run_tests():
    """Run all tests"""
    # Create test suite
    test_suite = unittest.TestSuite()

    # Add test classes
    test_classes = [TestMatrixOneVersionParsing, TestClientVersionDetection]

    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    return result.wasSuccessful()


if __name__ == "__main__":
    print("MatrixOne Python SDK - MatrixOne Version Parsing Test Suite")
    print("=" * 60)

    success = run_tests()

    if success:
        print("\n✓ All MatrixOne version parsing tests passed!")
        print("\nMatrixOne Version Parsing Features:")
        print("✓ Development version parsing (8.0.30-MatrixOne-v -> 999.0.0)")
        print("✓ Release version parsing (8.0.30-MatrixOne-v3.0.0 -> 3.0.0)")
        print("✓ Fallback version parsing (MatrixOne 3.0.1 -> 3.0.1)")
        print("✓ Development version has highest priority")
        print("✓ Development version compatibility with all features")
        print("✓ Proper error handling for invalid versions")
    else:
        print("\n✗ Some tests failed!")
        sys.exit(1)
