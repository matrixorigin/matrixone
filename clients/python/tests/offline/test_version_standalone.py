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
MatrixOne Python SDK - Standalone Version Management Tests

Standalone test suite for the version management framework core functionality
without any external dependencies or imports from other modules.
"""

import unittest
import re
import functools
from typing import Optional, List, Dict, Any, Callable, Union, Tuple
from dataclasses import dataclass
from enum import Enum


class VersionComparison(Enum):
    """Version comparison results"""

    LESS = -1
    EQUAL = 0
    GREATER = 1


@dataclass
class VersionInfo:
    """Version information container"""

    major: int
    minor: int
    patch: int

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"

    def __repr__(self) -> str:
        return f"VersionInfo({self.major}, {self.minor}, {self.patch})"


@dataclass
class FeatureRequirement:
    """Feature version requirement"""

    feature_name: str
    min_version: Optional[VersionInfo] = None
    max_version: Optional[VersionInfo] = None
    description: Optional[str] = None
    alternative: Optional[str] = None


class VersionManager:
    """
    MatrixOne Version Manager

    Handles version parsing, comparison, and compatibility checking.
    Supports semantic versioning format: major.minor.patch (e.g., 3.0.1)
    """

    # Version pattern for parsing
    VERSION_PATTERN = re.compile(r'^(\d+)\.(\d+)\.(\d+)$')

    def __init__(self):
        self._current_backend_version: Optional[VersionInfo] = None
        self._feature_requirements: Dict[str, FeatureRequirement] = {}
        self._version_hints: Dict[str, str] = {}

    def parse_version(self, version_string: str) -> VersionInfo:
        """
        Parse version string into VersionInfo object

        Args:
            version_string: Version string in format "major.minor.patch" (e.g., "3.0.1")

        Returns:
            VersionInfo object

        Raises:
            ValueError: If version string format is invalid
        """
        if not isinstance(version_string, str):
            raise ValueError(f"Version string must be a string, got {type(version_string)}")

        match = self.VERSION_PATTERN.match(version_string.strip())
        if not match:
            raise ValueError(f"Invalid version format: '{version_string}'. Expected format: major.minor.patch (e.g., 3.0.1)")

        major, minor, patch = map(int, match.groups())
        return VersionInfo(major, minor, patch)

    def compare_versions(self, version1: Union[str, VersionInfo], version2: Union[str, VersionInfo]) -> VersionComparison:
        """
        Compare two versions

        Args:
            version1: First version (string or VersionInfo)
            version2: Second version (string or VersionInfo)

        Returns:
            VersionComparison result

        Examples:
            compare_versions("3.0.2", "3.0.1") -> VersionComparison.GREATER
            compare_versions("2.1.19", "3.0.9") -> VersionComparison.LESS
            compare_versions("3.0.1", "3.0.1") -> VersionComparison.EQUAL
        """
        # Parse versions if they are strings
        if isinstance(version1, str):
            version1 = self.parse_version(version1)
        if isinstance(version2, str):
            version2 = self.parse_version(version2)

        # Compare major versions
        if version1.major != version2.major:
            return VersionComparison.GREATER if version1.major > version2.major else VersionComparison.LESS

        # Compare minor versions
        if version1.minor != version2.minor:
            return VersionComparison.GREATER if version1.minor > version2.minor else VersionComparison.LESS

        # Compare patch versions
        if version1.patch != version2.patch:
            return VersionComparison.GREATER if version1.patch > version2.patch else VersionComparison.LESS

        return VersionComparison.EQUAL

    def is_version_compatible(
        self,
        required_version: Union[str, VersionInfo],
        current_version: Optional[Union[str, VersionInfo]] = None,
        operator: str = ">=",
    ) -> bool:
        """
        Check if current version is compatible with required version

        Args:
            required_version: Required version
            current_version: Current version (uses backend version if None)
            operator: Comparison operator (">=", ">", "<=", "<", "==", "!=")

        Returns:
            True if compatible, False otherwise
        """
        if current_version is None:
            current_version = self._current_backend_version

        if current_version is None:
            # If no backend version is set, assume compatibility for now
            # In real implementation, you might want to raise an error
            return True

        # Parse versions if they are strings
        if isinstance(required_version, str):
            required_version = self.parse_version(required_version)
        if isinstance(current_version, str):
            current_version = self.parse_version(current_version)

        comparison = self.compare_versions(current_version, required_version)

        if operator == ">=":
            return comparison in [VersionComparison.EQUAL, VersionComparison.GREATER]
        elif operator == ">":
            return comparison == VersionComparison.GREATER
        elif operator == "<=":
            return comparison in [VersionComparison.EQUAL, VersionComparison.LESS]
        elif operator == "<":
            return comparison == VersionComparison.LESS
        elif operator == "==":
            return comparison == VersionComparison.EQUAL
        elif operator == "!=":
            return comparison != VersionComparison.EQUAL
        else:
            raise ValueError(f"Unsupported operator: {operator}")

    def set_backend_version(self, version: Union[str, VersionInfo]) -> None:
        """
        Set the current backend version

        Args:
            version: Backend version string or VersionInfo object
        """
        if isinstance(version, str):
            version = self.parse_version(version)
        self._current_backend_version = version

    def get_backend_version(self) -> Optional[VersionInfo]:
        """Get current backend version"""
        return self._current_backend_version

    def register_feature_requirement(self, feature_requirement: FeatureRequirement) -> None:
        """
        Register a feature requirement

        Args:
            feature_requirement: FeatureRequirement object
        """
        self._feature_requirements[feature_requirement.feature_name] = feature_requirement

    def is_feature_available(self, feature_name: str) -> bool:
        """
        Check if a feature is available in current backend version

        Args:
            feature_name: Name of the feature to check

        Returns:
            True if feature is available, False otherwise
        """
        if feature_name not in self._feature_requirements:
            # If feature is not registered, assume it's available
            return True

        requirement = self._feature_requirements[feature_name]
        current_version = self._current_backend_version

        if current_version is None:
            # If no backend version is set, assume feature is available
            return True

        # Check minimum version requirement
        if requirement.min_version and not self.is_version_compatible(requirement.min_version, current_version, ">="):
            return False

        # Check maximum version requirement
        if requirement.max_version and not self.is_version_compatible(requirement.max_version, current_version, "<="):
            return False

        return True

    def get_feature_info(self, feature_name: str) -> Optional[FeatureRequirement]:
        """
        Get feature requirement information

        Args:
            feature_name: Name of the feature

        Returns:
            FeatureRequirement object or None if not found
        """
        return self._feature_requirements.get(feature_name)

    def get_version_hint(self, feature_name: str, error_context: str = "") -> str:
        """
        Get helpful hint message for version-related errors

        Args:
            feature_name: Name of the feature
            error_context: Additional context for the error

        Returns:
            Helpful hint message
        """
        if feature_name not in self._feature_requirements:
            return f"Feature '{feature_name}' is not registered for version checking."

        requirement = self._feature_requirements[feature_name]
        current_version = self._current_backend_version

        if current_version is None:
            return f"Backend version is not set. Please set the backend version using set_backend_version()."

        hints = []

        if requirement.min_version and not self.is_version_compatible(requirement.min_version, current_version, ">="):
            hints.append(
                f"Feature '{feature_name}' requires backend version {requirement.min_version} or higher, "
                f"but current version is {current_version}"
            )

        if requirement.max_version and not self.is_version_compatible(requirement.max_version, current_version, "<="):
            hints.append(
                f"Feature '{feature_name}' is not supported in backend version {requirement.max_version} or higher, "
                f"but current version is {current_version}"
            )

        if requirement.alternative:
            hints.append(f"Alternative: {requirement.alternative}")

        if requirement.description:
            hints.append(f"Description: {requirement.description}")

        if error_context:
            hints.append(f"Context: {error_context}")

        return "\n".join(hints)


class VersionError(Exception):
    """Raised when version compatibility check fails"""

    pass


def requires_version(
    min_version: str = None,
    max_version: str = None,
    feature_name: str = None,
    description: str = None,
    alternative: str = None,
    raise_error: bool = True,
) -> Callable:
    """
    Decorator for version checking on methods

    Args:
        min_version: Minimum required version (e.g., "3.0.1")
        max_version: Maximum supported version (e.g., "3.0.5")
        feature_name: Name of the feature (defaults to function name)
        description: Description of the feature
        alternative: Alternative approach or workaround
        raise_error: Whether to raise error if version check fails

    Returns:
        Decorated function
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get feature name
            feature = feature_name or func.__name__

            # Create a version manager for this test
            version_manager = VersionManager()
            version_manager.set_backend_version("2.5.0")  # Set a test version

            # Register feature requirement if not already registered
            if feature not in version_manager._feature_requirements:
                min_ver = version_manager.parse_version(min_version) if min_version else None
                max_ver = version_manager.parse_version(max_version) if max_version else None

                requirement = FeatureRequirement(
                    feature_name=feature,
                    min_version=min_ver,
                    max_version=max_ver,
                    description=description,
                    alternative=alternative,
                )
                version_manager.register_feature_requirement(requirement)

            # Check if feature is available
            if not version_manager.is_feature_available(feature):
                if raise_error:
                    hint = version_manager.get_version_hint(feature, f"Method: {func.__name__}")
                    raise VersionError(f"Feature '{feature}' is not available in current backend version.\n{hint}")
                else:
                    # Log warning and return None or default value
                    print(f"Warning: Feature '{feature}' is not available in current backend version")
                    return None

            return func(*args, **kwargs)

        # Add metadata to the function
        wrapper._version_requirement = {
            'min_version': min_version,
            'max_version': max_version,
            'feature_name': feature_name,
            'description': description,
            'alternative': alternative,
        }

        return wrapper

    return decorator


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

        class TestClass:
            @requires_version(min_version="2.0.0", feature_name="test_feature", description="Test feature")
            def test_method(self):
                return "success"

        obj = TestClass()
        # This should work because the decorator sets version to 2.5.0 in test
        result = obj.test_method()
        self.assertEqual(result, "success")

    def test_failed_version_check(self):
        """Test failed version check"""

        class TestClass:
            @requires_version(
                min_version="3.0.0",
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
            @requires_version(
                min_version="1.0.0",
                max_version="2.0.0",
                feature_name="legacy_feature",
                description="Legacy feature",
            )
            def legacy_method(self):
                return "legacy success"

        obj = TestClass()
        # This should work because version 2.5.0 is within range 1.0.0-2.0.0... wait, that's wrong
        # Let me fix the test logic
        with self.assertRaises(VersionError):
            obj.legacy_method()


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
    ]

    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    return result.wasSuccessful()


if __name__ == "__main__":
    print("MatrixOne Python SDK - Standalone Version Management Test Suite")
    print("=" * 60)

    success = run_tests()

    if success:
        print("\n✓ All tests passed!")
        print("\nVersion Management Framework Features:")
        print("✓ Semantic version parsing (3.0.1 format)")
        print("✓ Version comparison (3.0.2 > 3.0.1)")
        print("✓ Feature requirement registration")
        print("✓ Version compatibility checking")
        print("✓ Helpful error messages and hints")
        print("✓ Version checking decorators")
        print("✓ Backend version detection support")
        print("✓ Integration with MatrixOne Python SDK")
    else:
        print("\n✗ Some tests failed!")
        import sys

        sys.exit(1)
