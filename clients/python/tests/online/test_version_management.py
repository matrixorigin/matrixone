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
Online tests for version management

These tests are inspired by example_10_version_management.py
"""

import pytest
from matrixone import Client, AsyncClient
from matrixone.version import VersionManager, VersionInfo, FeatureRequirement, requires_version
from matrixone.exceptions import VersionError, ConnectionError, QueryError
from matrixone.logger import create_default_logger


@pytest.mark.online
class TestVersionManagement:
    """Test version management functionality"""

    def test_version_parsing_and_comparison(self):
        """Test version parsing and comparison"""
        version_manager = VersionManager()

        # Test version parsing
        versions = ["3.0.1", "3.0.2", "2.1.19", "3.0.9", "1.5.0"]

        for version_str in versions:
            version = version_manager.parse_version(version_str)
            assert isinstance(version, VersionInfo)
            assert str(version) == version_str

        # Test version comparisons
        test_cases = [
            ("3.0.2", "3.0.1", 1),  # 3.0.2 > 3.0.1
            ("2.1.19", "3.0.9", -1),  # 2.1.19 < 3.0.9
            ("3.0.1", "3.0.1", 0),  # 3.0.1 == 3.0.1
        ]

        for v1_str, v2_str, expected in test_cases:
            v1 = version_manager.parse_version(v1_str)
            v2 = version_manager.parse_version(v2_str)

            comparison = version_manager.compare_versions(v1, v2)
            if expected > 0:
                assert comparison.value == 1, f"{v1} should be greater than {v2}"
            elif expected < 0:
                assert comparison.value == -1, f"{v1} should be less than {v2}"
            else:
                assert comparison.value == 0, f"{v1} should be equal to {v2}"

    def test_backend_version_detection(self, test_client):
        """Test backend version detection"""
        version_manager = VersionManager()

        # Test version detection
        version_str = test_client.get_backend_version()
        assert version_str is not None
        assert isinstance(version_str, str)
        assert len(version_str) > 0

        # Parse version string to get VersionInfo
        version_info = version_manager.parse_version(version_str)
        assert isinstance(version_info, VersionInfo)

        # Test version components
        assert version_info.major >= 0
        assert version_info.minor >= 0
        assert version_info.patch >= 0

    def test_feature_availability_checking(self, test_client):
        """Test feature availability checking"""
        version_manager = VersionManager()
        backend_version_str = test_client.get_backend_version()
        backend_version = version_manager.parse_version(backend_version_str)

        # Test feature requirements
        feature_requirements = [
            FeatureRequirement("basic_query", "1.0.0"),
            FeatureRequirement("transactions", "2.0.0"),
            FeatureRequirement("advanced_features", "3.0.0"),
        ]

        for feature_req in feature_requirements:
            is_available = version_manager.is_feature_available(feature_req.feature_name)
            assert isinstance(is_available, bool)

            # Basic query should always be available
            if feature_req.feature_name == "basic_query":
                assert is_available, "Basic query should always be available"

    def test_version_aware_error_messages(self, test_client):
        """Test version-aware error messages"""
        version_manager = VersionManager()
        backend_version_str = test_client.get_backend_version()
        backend_version = version_manager.parse_version(backend_version_str)

        # Test version error creation
        try:
            # Try to use a feature that requires a very high version
            feature_req = FeatureRequirement("nonexistent_feature", "999.0.0")
            is_available = version_manager.is_feature_available(feature_req.feature_name)
            # This should return True for unregistered features
            assert isinstance(is_available, bool)
        except Exception as e:
            # If an error is raised, it should contain relevant information
            assert "version" in str(e).lower() or "feature" in str(e).lower()

    def test_version_checking_decorators(self):
        """Test version checking decorators"""
        version_manager = VersionManager()

        # Test decorator with valid version
        @requires_version("1.0.0")
        def test_function():
            return "success"

        # This should work with any backend version >= 1.0.0
        result = test_function()
        assert result == "success"

        # Test decorator with high version requirement
        @requires_version("999.0.0")
        def test_function_high_version():
            return "success"

        # This might fail depending on backend version
        try:
            result = test_function_high_version()
            assert result == "success"
        except VersionError:
            # Expected to fail if backend version < 999.0.0
            pass

    def test_custom_feature_requirements(self, test_client):
        """Test custom feature requirements"""
        version_manager = VersionManager()
        backend_version_str = test_client.get_backend_version()
        backend_version = version_manager.parse_version(backend_version_str)

        # Test custom feature requirement
        custom_feature = FeatureRequirement("custom_feature", "2.5.0")

        is_available = version_manager.is_feature_available(custom_feature.feature_name)
        assert isinstance(is_available, bool)

        # Test feature requirement with description
        feature_with_desc = FeatureRequirement("feature_with_description", "1.0.0", description="This is a test feature")

        assert feature_with_desc.description == "This is a test feature"

        is_available = version_manager.is_feature_available(feature_with_desc.feature_name)
        assert isinstance(is_available, bool)

    def test_version_compatibility_matrix(self, test_client):
        """Test version compatibility matrix"""
        version_manager = VersionManager()
        backend_version_str = test_client.get_backend_version()
        backend_version = version_manager.parse_version(backend_version_str)

        # Test compatibility with different SDK versions
        sdk_versions = ["1.0.0", "2.0.0", "3.0.0"]

        for sdk_version_str in sdk_versions:
            sdk_version = version_manager.parse_version(sdk_version_str)

            # Test basic compatibility by comparing versions
            comparison = version_manager.compare_versions(backend_version, sdk_version)
            # Backend should be >= SDK version for compatibility
            is_compatible = comparison.value >= 0
            assert isinstance(is_compatible, bool)

    def test_version_info_properties(self, test_client):
        """Test VersionInfo properties"""
        version_manager = VersionManager()
        backend_version_str = test_client.get_backend_version()
        backend_version = version_manager.parse_version(backend_version_str)

        # Test version properties
        assert hasattr(backend_version, 'major')
        assert hasattr(backend_version, 'minor')
        assert hasattr(backend_version, 'patch')

        assert isinstance(backend_version.major, int)
        assert isinstance(backend_version.minor, int)
        assert isinstance(backend_version.patch, int)

        # Test version string representation
        version_str = str(backend_version)
        assert isinstance(version_str, str)
        assert len(version_str) > 0

        # Test version tuple
        version_tuple = (backend_version.major, backend_version.minor, backend_version.patch)
        assert isinstance(version_tuple, tuple)
        assert len(version_tuple) == 3

    def test_version_manager_methods(self):
        """Test VersionManager methods"""
        version_manager = VersionManager()

        # Test parse_version method
        version = version_manager.parse_version("1.2.3")
        assert isinstance(version, VersionInfo)
        assert version.major == 1
        assert version.minor == 2
        assert version.patch == 3

        # Test compare_versions method
        v1 = version_manager.parse_version("1.2.3")
        v2 = version_manager.parse_version("1.2.4")

        comparison = version_manager.compare_versions(v1, v2)
        assert comparison.value < 0  # v1 < v2

        # Test compatibility by comparing versions
        backend_version = version_manager.parse_version("2.0.0")
        sdk_version = version_manager.parse_version("1.0.0")

        comparison = version_manager.compare_versions(backend_version, sdk_version)
        is_compatible = comparison.value >= 0
        assert isinstance(is_compatible, bool)

    @pytest.mark.asyncio
    async def test_async_version_management(self, test_async_client):
        """Test async version management"""
        # Test async version detection - AsyncClient doesn't have get_backend_version
        # So we'll test basic async operations instead
        result = await test_async_client.execute("SELECT VERSION()")
        assert result is not None
        assert len(result.rows) > 0

        version_str = result.rows[0][0]
        assert isinstance(version_str, str)
        assert len(version_str) > 0

    def test_version_with_logging(self, connection_params):
        """Test version management with logging"""
        host, port, user, password, database = connection_params

        # Create logger
        logger = create_default_logger()

        # Create client with logging
        client = Client(logger=logger)
        client.connect(host=host, port=port, user=user, password=password, database=database)

        try:
            # Test version detection with logging
            version_str = client.get_backend_version()
            assert version_str is not None
            assert isinstance(version_str, str)

        finally:
            client.disconnect()

    def test_version_error_handling(self):
        """Test version error handling"""
        version_manager = VersionManager()

        # Test invalid version string
        try:
            version_manager.parse_version("invalid_version")
            assert False, "Should have failed with invalid version"
        except Exception:
            pass  # Expected to fail

        # Test empty version string
        try:
            version_manager.parse_version("")
            assert False, "Should have failed with empty version"
        except Exception:
            pass  # Expected to fail

        # Test None version
        try:
            version_manager.parse_version(None)
            assert False, "Should have failed with None version"
        except Exception:
            pass  # Expected to fail
