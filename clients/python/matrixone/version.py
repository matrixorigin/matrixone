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
MatrixOne Version Management Module

Provides version comparison, compatibility checking, and version-aware feature management
for MatrixOne Python SDK.

Features:
1. Version parsing and comparison (e.g., 3.0.1, 3.0.2 > 3.0.1)
2. Backend version compatibility checking
3. Feature availability checking based on version
4. Version-aware error messages and suggestions
5. Decorators for version checking on methods
"""

import functools
import re
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, Optional, Union

from .exceptions import MatrixOneError


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

    def __eq__(self, other) -> bool:
        """Check if two versions are equal"""
        if not isinstance(other, VersionInfo):
            return False
        return (self.major, self.minor, self.patch) == (other.major, other.minor, other.patch)

    def __lt__(self, other) -> bool:
        """Check if this version is less than other version"""
        if not isinstance(other, VersionInfo):
            return NotImplemented
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)

    def __le__(self, other) -> bool:
        """Check if this version is less than or equal to other version"""
        if not isinstance(other, VersionInfo):
            return NotImplemented
        return (self.major, self.minor, self.patch) <= (other.major, other.minor, other.patch)

    def __gt__(self, other) -> bool:
        """Check if this version is greater than other version"""
        if not isinstance(other, VersionInfo):
            return NotImplemented
        return (self.major, self.minor, self.patch) > (other.major, other.minor, other.patch)

    def __ge__(self, other) -> bool:
        """Check if this version is greater than or equal to other version"""
        if not isinstance(other, VersionInfo):
            return NotImplemented
        return (self.major, self.minor, self.patch) >= (other.major, other.minor, other.patch)


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
    VERSION_PATTERN = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")

    def __init__(self):
        self._current_backend_version: Optional[VersionInfo] = None
        self._feature_requirements: Dict[str, FeatureRequirement] = {}
        self._version_hints: Dict[str, str] = {}

    def parse_version(self, version_string: str) -> VersionInfo:
        """
        Parse version string into VersionInfo object

        Args:

            version_string: Version string in format "major.minor.patch" (e.g., "3.0.1")
                          Special case: "999.0.0" represents development version (highest priority)

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

        # Special handling for development version
        if major == 999:
            # This is a development version - it has highest priority
            pass

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

    def is_development_version(self, version: Optional[Union[str, VersionInfo]] = None) -> bool:
        """
        Check if a version is a development version

        Args:

            version: Version to check (uses current backend version if None)

        Returns:

            True if it's a development version (999.x.x), False otherwise
        """
        if version is None:
            version = self._current_backend_version

        if version is None:
            return False

        if isinstance(version, str):
            version = self.parse_version(version)

        return version.major == 999

    def _parse_matrixone_version(self, version_string: str) -> Optional[str]:
        """
        Parse MatrixOne version string to extract semantic version

        Handles formats:
        1. "8.0.30-MatrixOne-v" -> "999.0.0" (development version, highest)
        2. "8.0.30-MatrixOne-v3.0.0" -> "3.0.0" (release version)
        3. "MatrixOne 3.0.1" -> "3.0.1" (fallback format)

        Args:

            version_string: Raw version string from MatrixOne

        Returns:

            Semantic version string or None if parsing fails
        """
        import re

        if not version_string:
            return None

        version_string = version_string.strip()

        # Pattern 1: Development version "8.0.30-MatrixOne-v" (v后面为空)
        dev_pattern = r"^(\d+\.\d+\.\d+)-MatrixOne-v$"
        dev_match = re.search(dev_pattern, version_string)
        if dev_match:
            # Development version - assign highest version number
            return "999.0.0"

        # Pattern 2: Release version "8.0.30-MatrixOne-v3.0.0" (v后面有版本号)
        release_pattern = r"^(\d+\.\d+\.\d+)-MatrixOne-v(\d+\.\d+\.\d+)$"
        release_match = re.search(release_pattern, version_string)
        if release_match:
            # Extract the semantic version part
            semantic_version = release_match.group(2)
            return semantic_version

        # Pattern 3: Fallback format "MatrixOne 3.0.1", "Version 2.5.0", or "3.0.1"
        # Match clean version strings or strings that start with common prefixes
        if (
            version_string.startswith("MatrixOne ")
            or version_string.startswith("Version ")
            or re.match(r"^\d+\.\d+\.\d+$", version_string)
        ):
            fallback_pattern = r"(\d+\.\d+\.\d+)"
            fallback_match = re.search(fallback_pattern, version_string)
            if fallback_match:
                return fallback_match.group(1)

        # For invalid MatrixOne formats like "8.0.30-MatrixOne" or "8.0.30-MatrixOne-v-"
        # return None instead of falling back to extracting version numbers
        return None

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
            return "Backend version is not set. Please set the backend version using set_backend_version()."

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


# Global version manager instance
_version_manager = VersionManager()


def get_version_manager() -> VersionManager:
    """Get the global version manager instance"""
    return _version_manager


def requires_version(
    min_version: Optional[str] = None,
    max_version: Optional[str] = None,
    feature_name: Optional[str] = None,
    description: Optional[str] = None,
    alternative: Optional[str] = None,
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

            # Register feature requirement if not already registered
            if feature not in _version_manager._feature_requirements:
                min_ver = _version_manager.parse_version(min_version) if min_version else None
                max_ver = _version_manager.parse_version(max_version) if max_version else None

                requirement = FeatureRequirement(
                    feature_name=feature,
                    min_version=min_ver,
                    max_version=max_ver,
                    description=description,
                    alternative=alternative,
                )
                _version_manager.register_feature_requirement(requirement)

            # Check if feature is available
            if not _version_manager.is_feature_available(feature):
                if raise_error:
                    hint = _version_manager.get_version_hint(feature, f"Method: {func.__name__}")
                    raise VersionError(f"Feature '{feature}' is not available in current backend version.\n{hint}")
                else:
                    # Log warning and return None or default value
                    print(f"Warning: Feature '{feature}' is not available in current backend version")
                    return None

            return func(*args, **kwargs)

        # Add metadata to the function
        wrapper._version_requirement = {
            "min_version": min_version,
            "max_version": max_version,
            "feature_name": feature_name,
            "description": description,
            "alternative": alternative,
        }

        return wrapper

    return decorator


class VersionError(MatrixOneError):
    """Raised when version compatibility check fails"""


# Initialize common feature requirements
def _initialize_default_features():
    """Initialize default feature requirements for common MatrixOne features"""

    # Snapshot features
    _version_manager.register_feature_requirement(
        FeatureRequirement(
            feature_name="snapshot_cluster_level",
            min_version=_version_manager.parse_version("1.0.0"),
            description="Cluster-level snapshot functionality",
            alternative="Use database-level snapshots instead",
        )
    )

    _version_manager.register_feature_requirement(
        FeatureRequirement(
            feature_name="snapshot_account_level",
            min_version=_version_manager.parse_version("1.0.0"),
            description="Account-level snapshot functionality",
            alternative="Use database-level snapshots instead",
        )
    )

    # PITR features
    _version_manager.register_feature_requirement(
        FeatureRequirement(
            feature_name="pitr_point_in_time_recovery",
            min_version=_version_manager.parse_version("1.0.0"),
            description="Point-in-time recovery functionality",
            alternative="Use snapshot restore instead",
        )
    )

    # Pub/Sub features
    _version_manager.register_feature_requirement(
        FeatureRequirement(
            feature_name="pubsub_publications",
            min_version=_version_manager.parse_version("1.0.0"),
            description="Publication and subscription functionality",
            alternative="Use direct table queries instead",
        )
    )

    # Account management features
    _version_manager.register_feature_requirement(
        FeatureRequirement(
            feature_name="account_management",
            min_version=_version_manager.parse_version("1.0.0"),
            description="Account and user management functionality",
            alternative="Use SQL DDL statements directly",
        )
    )


# Initialize default features
_initialize_default_features()
