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
MatrixOne Python SDK - Version Management Tests

Test suite for the version management framework.
"""

import unittest
import sys
import os

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone.version import (
    VersionManager,
    VersionInfo,
    FeatureRequirement,
    VersionComparison,
    requires_version,
    VersionError,
)
from matrixone.exceptions import VersionError as MatrixOneVersionError


class TestVersionParsing(unittest.TestCase):
    """Test version parsing functionality"""

    def setUp(self):
        self.version_manager = VersionManager()

    def test_valid_version_parsing(self):
        """Test parsing valid version strings"""
        test_cases = [
            ("3.0.1", VersionInfo(3, 0, 1)),
            ("1.0.0", VersionInfo(1, 0, 0)),
            ("10.25.100", VersionInfo(10, 25, 100)),
            ("0.0.1", VersionInfo(0, 0, 1)),
        ]

        for version_str, expected in test_cases:
            with self.subTest(version=version_str):
                result = self.version_manager.parse_version(version_str)
                self.assertEqual(result.major, expected.major)
                self.assertEqual(result.minor, expected.minor)
                self.assertEqual(result.patch, expected.patch)

    def test_invalid_version_parsing(self):
        """Test parsing invalid version strings"""
        invalid_versions = [
            "3.0",  # Missing patch
            "3.0.1.2",  # Too many components
            "3.0.a",  # Non-numeric component
            "3.0.1-beta",  # Pre-release suffix
            "",  # Empty string
            "invalid",  # Completely invalid
        ]

        for invalid_version in invalid_versions:
            with self.subTest(version=invalid_version):
                with self.assertRaises(ValueError):
                    self.version_manager.parse_version(invalid_version)

    def test_version_string_representation(self):
        """Test version string representation"""
        version = VersionInfo(3, 0, 1)
        self.assertEqual(str(version), "3.0.1")


class TestVersionComparison(unittest.TestCase):
    """Test version comparison functionality"""

    def setUp(self):
        self.version_manager = VersionManager()

    def test_version_comparisons(self):
        """Test various version comparisons"""
        test_cases = [
            # (version1, version2, expected_result)
            ("3.0.2", "3.0.1", VersionComparison.GREATER),
            ("3.0.1", "3.0.2", VersionComparison.LESS),
            ("3.0.1", "3.0.1", VersionComparison.EQUAL),
            ("2.1.19", "3.0.9", VersionComparison.LESS),
            ("3.0.9", "2.1.19", VersionComparison.GREATER),
            ("1.0.0", "2.0.0", VersionComparison.LESS),
            ("2.0.0", "1.9.9", VersionComparison.GREATER),
        ]

        for v1, v2, expected in test_cases:
            with self.subTest(v1=v1, v2=v2):
                result = self.version_manager.compare_versions(v1, v2)
                self.assertEqual(result, expected)

    def test_version_compatibility_checks(self):
        """Test version compatibility checking"""
        self.version_manager.set_backend_version("3.0.1")

        # Test >= operator
        self.assertTrue(self.version_manager.is_version_compatible("3.0.0", operator=">="))
        self.assertTrue(self.version_manager.is_version_compatible("3.0.1", operator=">="))
        self.assertFalse(self.version_manager.is_version_compatible("3.0.2", operator=">="))

        # Test > operator
        self.assertTrue(self.version_manager.is_version_compatible("3.0.0", operator=">"))
        self.assertFalse(self.version_manager.is_version_compatible("3.0.1", operator=">"))
        self.assertFalse(self.version_manager.is_version_compatible("3.0.2", operator=">"))

        # Test <= operator
        self.assertFalse(self.version_manager.is_version_compatible("3.0.0", operator="<="))
        self.assertTrue(self.version_manager.is_version_compatible("3.0.1", operator="<="))
        self.assertTrue(self.version_manager.is_version_compatible("3.0.2", operator="<="))

        # Test < operator
        self.assertFalse(self.version_manager.is_version_compatible("3.0.0", operator="<"))
        self.assertFalse(self.version_manager.is_version_compatible("3.0.1", operator="<"))
        self.assertTrue(self.version_manager.is_version_compatible("3.0.2", operator="<"))

        # Test == operator
        self.assertFalse(self.version_manager.is_version_compatible("3.0.0", operator="=="))
        self.assertTrue(self.version_manager.is_version_compatible("3.0.1", operator="=="))
        self.assertFalse(self.version_manager.is_version_compatible("3.0.2", operator="=="))

        # Test != operator
        self.assertTrue(self.version_manager.is_version_compatible("3.0.0", operator="!="))
        self.assertFalse(self.version_manager.is_version_compatible("3.0.1", operator="!="))
        self.assertTrue(self.version_manager.is_version_compatible("3.0.2", operator="!="))


class TestFeatureRequirements(unittest.TestCase):
    """Test feature requirement functionality"""

    def setUp(self):
        self.version_manager = VersionManager()

    def test_feature_registration_and_availability(self):
        """Test feature registration and availability checking"""
        # Register a feature requirement
        feature = FeatureRequirement(
            feature_name="test_feature",
            min_version=VersionInfo(3, 0, 0),
            max_version=VersionInfo(3, 5, 0),
            description="Test feature",
            alternative="Use alternative_feature instead",
        )
        self.version_manager.register_feature_requirement(feature)

        # Test with compatible version
        self.version_manager.set_backend_version("3.1.0")
        self.assertTrue(self.version_manager.is_feature_available("test_feature"))

        # Test with version too low
        self.version_manager.set_backend_version("2.9.0")
        self.assertFalse(self.version_manager.is_feature_available("test_feature"))

        # Test with version too high
        self.version_manager.set_backend_version("3.6.0")
        self.assertFalse(self.version_manager.is_feature_available("test_feature"))

    def test_feature_info_retrieval(self):
        """Test feature information retrieval"""
        feature = FeatureRequirement(
            feature_name="info_test",
            min_version=VersionInfo(2, 0, 0),
            description="Information test feature",
            alternative="Alternative approach",
        )
        self.version_manager.register_feature_requirement(feature)

        retrieved = self.version_manager.get_feature_info("info_test")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.feature_name, "info_test")
        self.assertEqual(retrieved.description, "Information test feature")
        self.assertEqual(retrieved.alternative, "Alternative approach")

        # Test non-existent feature
        self.assertIsNone(self.version_manager.get_feature_info("non_existent"))

    def test_version_hints(self):
        """Test version hint generation"""
        self.version_manager.register_feature_requirement(
            FeatureRequirement(
                feature_name="hint_test",
                min_version=VersionInfo(3, 0, 0),
                description="Hint test feature",
                alternative="Use hint_alternative",
            )
        )

        # Test with version too low
        self.version_manager.set_backend_version("2.5.0")
        hint = self.version_manager.get_version_hint("hint_test", "Test context")

        self.assertIn("3.0.0", hint)
        self.assertIn("2.5.0", hint)
        self.assertIn("hint_alternative", hint)
        self.assertIn("Test context", hint)


class TestVersionDecorator(unittest.TestCase):
    """Test version checking decorator"""

    def test_successful_version_check(self):
        """Test successful version check"""
        # Clear any existing feature requirements to avoid interference
        from matrixone.version import _version_manager

        _version_manager._feature_requirements.clear()

        class TestClass:
            def __init__(self):
                # Set global version manager
                _version_manager.set_backend_version("3.0.0")

            @requires_version(min_version="2.0.0", feature_name="test_feature", description="Test feature")
            def test_method(self):
                return "success"

        obj = TestClass()
        result = obj.test_method()
        self.assertEqual(result, "success")

    def test_failed_version_check(self):
        """Test failed version check"""

        class TestClass:
            def __init__(self):
                # Set global version manager
                from matrixone.version import _version_manager

                _version_manager.set_backend_version("1.0.0")

            @requires_version(
                min_version="2.0.0",
                feature_name="test_feature",
                description="Test feature",
                alternative="Use alternative method",
            )
            def test_method(self):
                return "success"

        obj = TestClass()
        with self.assertRaises(VersionError):
            obj.test_method()

    def test_version_range_check(self):
        """Test version range checking"""

        class TestClass:
            def __init__(self, version):
                # Set global version manager
                from matrixone.version import _version_manager

                _version_manager.set_backend_version(version)

            @requires_version(
                min_version="2.0.0",
                max_version="2.9.9",
                feature_name="legacy_feature",
                description="Legacy feature",
            )
            def legacy_method(self):
                return "legacy success"

        # Test within range
        obj = TestClass("2.5.0")
        result = obj.legacy_method()
        self.assertEqual(result, "legacy success")

        # Test below range
        obj = TestClass("1.9.0")
        with self.assertRaises(VersionError):
            obj.legacy_method()

        # Test above range
        obj = TestClass("3.0.0")
        with self.assertRaises(VersionError):
            obj.legacy_method()


class TestIntegration(unittest.TestCase):
    """Test integration with MatrixOne client"""

    def test_client_version_management_api(self):
        """Test client version management API"""
        from matrixone import Client

        # Create client (without connecting)
        client = Client()

        # Test manual version setting
        client.set_backend_version("3.0.1")
        self.assertEqual(client.get_backend_version(), "3.0.1")

        # Test version compatibility check
        self.assertTrue(client.check_version_compatibility("3.0.0", ">="))
        self.assertFalse(client.check_version_compatibility("3.0.2", ">="))

        # Test feature availability (should work since we have default features)
        self.assertTrue(client.is_feature_available("snapshot_creation"))

        # Test feature info retrieval (feature may not exist if not registered)
        info = client.get_feature_info("snapshot_creation")
        if info is not None:
            self.assertEqual(info['feature_name'], "snapshot_creation")
        else:
            # Feature not registered, which is acceptable
            self.assertIsNone(info)


class TestErrorHandling(unittest.TestCase):
    """Test error handling"""

    def test_version_error_inheritance(self):
        """Test that VersionError inherits from MatrixOneError"""
        from matrixone.exceptions import MatrixOneError

        self.assertTrue(issubclass(VersionError, MatrixOneError))
        self.assertTrue(issubclass(MatrixOneVersionError, MatrixOneError))


def run_tests():
    """Run all tests"""
    # Create test suite
    test_suite = unittest.TestSuite()

    # Add test classes
    test_classes = [
        TestVersionParsing,
        TestVersionComparison,
        TestFeatureRequirements,
        TestVersionDecorator,
        TestIntegration,
        TestErrorHandling,
    ]

    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    return result.wasSuccessful()


if __name__ == "__main__":
    print("MatrixOne Python SDK - Version Management Test Suite")
    print("=" * 60)

    success = run_tests()

    if success:
        print("\n✓ All tests passed!")
    else:
        print("\n✗ Some tests failed!")
        sys.exit(1)
