#!/usr/bin/env python3
"""
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


def demonstrate_version_parsing():
    """Demonstrate version parsing and comparison"""
    print("=== Version Parsing and Comparison ===")
    
    version_manager = VersionManager()
    
    # Parse version strings
    versions = ["3.0.1", "3.0.2", "2.1.19", "3.0.9", "1.5.0"]
    
    print("Parsing versions:")
    for version_str in versions:
        version = version_manager.parse_version(version_str)
        print(f"  {version_str} -> {version}")
    
    print("\nVersion comparisons:")
    test_cases = [
        ("3.0.2", "3.0.1"),
        ("2.1.19", "3.0.9"),
        ("3.0.1", "3.0.1"),
        ("1.5.0", "2.0.0"),
        ("3.0.0", "2.9.9")
    ]
    
    for v1, v2 in test_cases:
        result = version_manager.compare_versions(v1, v2)
        comparison_text = {
            -1: "less than",
            0: "equal to", 
            1: "greater than"
        }[result.value]
        print(f"  {v1} is {comparison_text} {v2}")


def demonstrate_feature_requirements():
    """Demonstrate feature requirement registration and checking"""
    print("\n=== Feature Requirements ===")
    
    version_manager = VersionManager()
    
    # Register some custom feature requirements
    features = [
        FeatureRequirement(
            feature_name="advanced_analytics",
            min_version=VersionInfo(3, 0, 0),
            description="Advanced analytics features including window functions",
            alternative="Use basic aggregation functions instead"
        ),
        FeatureRequirement(
            feature_name="streaming_ingest",
            min_version=VersionInfo(2, 5, 0),
            max_version=VersionInfo(2, 9, 9),
            description="Streaming data ingestion capability",
            alternative="Use batch loading instead"
        ),
        FeatureRequirement(
            feature_name="multi_tenant",
            min_version=VersionInfo(3, 1, 0),
            description="Multi-tenant database support",
            alternative="Use single-tenant setup"
        )
    ]
    
    for feature in features:
        version_manager.register_feature_requirement(feature)
    
    # Test feature availability with different versions
    test_versions = ["2.4.0", "2.8.0", "3.0.0", "3.1.0"]
    
    for version in test_versions:
        print(f"\nBackend version {version}:")
        version_manager.set_backend_version(version)
        
        for feature_name in ["advanced_analytics", "streaming_ingest", "multi_tenant"]:
            available = version_manager.is_feature_available(feature_name)
            status = "✓ Available" if available else "✗ Not available"
            print(f"  {feature_name}: {status}")
            
            if not available:
                hint = version_manager.get_version_hint(feature_name)
                print(f"    Hint: {hint}")


def demonstrate_version_checking_decorator():
    """Demonstrate version checking decorator"""
    print("\n=== Version Checking Decorator ===")
    
    class MockClient:
        def __init__(self, version):
            self._version_manager = VersionManager()
            self._version_manager.set_backend_version(version)
        
        @requires_version(
            min_version="3.0.0",
            feature_name="premium_feature",
            description="Premium feature that requires version 3.0.0+",
            alternative="Use basic_feature() instead"
        )
        def premium_feature(self):
            return "Premium feature executed successfully!"
        
        @requires_version(
            min_version="2.0.0",
            max_version="2.9.9",
            feature_name="legacy_feature",
            description="Legacy feature only available in 2.x versions",
            alternative="Use new_feature() instead"
        )
        def legacy_feature(self):
            return "Legacy feature executed successfully!"
        
        def basic_feature(self):
            return "Basic feature executed successfully!"
    
    # Test with different versions
    test_versions = ["1.9.0", "2.5.0", "3.0.0", "3.1.0"]
    
    for version in test_versions:
        print(f"\nTesting with backend version {version}:")
        client = MockClient(version)
        
        # Test premium feature
        try:
            result = client.premium_feature()
            print(f"  premium_feature: {result}")
        except VersionError as e:
            print(f"  premium_feature: {e}")
        
        # Test legacy feature
        try:
            result = client.legacy_feature()
            print(f"  legacy_feature: {result}")
        except VersionError as e:
            print(f"  legacy_feature: {e}")
        
        # Basic feature (no version requirement)
        result = client.basic_feature()
        print(f"  basic_feature: {result}")


def demonstrate_real_client_integration():
    """Demonstrate real client integration with version management"""
    print("\n=== Real Client Integration ===")
    
    # This would normally connect to a real MatrixOne instance
    # For demo purposes, we'll show the API usage
    
    print("Client version management API:")
    print("""
    # Create client
    client = Client()
    
    # Connect to MatrixOne (version will be auto-detected)
    client.connect('localhost', 6001, 'root', 'password', 'test')
    
    # Get detected version
    version = client.get_backend_version()
    print(f"Backend version: {version}")
    
    # Check feature availability
    if client.is_feature_available('snapshot_creation'):
        print("Snapshot creation is available")
    else:
        hint = client.get_version_hint('snapshot_creation')
        print(f"Snapshot creation not available: {hint}")
    
    # Check version compatibility
    if client.check_version_compatibility('3.0.0', '>='):
        print("Backend supports version 3.0.0+ features")
    
    # Get feature information
    info = client.get_feature_info('pitr_cluster_level')
    if info:
        print(f"PITR cluster level feature: {info}")
    
    # Use version-aware methods (these will automatically check versions)
    try:
        snapshot = client.snapshots.create('my_snapshot', 'cluster')
        print(f"Created snapshot: {snapshot}")
    except VersionError as e:
        print(f"Snapshot creation failed: {e}")
    
    client.disconnect()
    """)


def demonstrate_error_scenarios():
    """Demonstrate various error scenarios and helpful messages"""
    print("\n=== Error Scenarios and Helpful Messages ===")
    
    version_manager = VersionManager()
    
    # Scenario 1: Feature not available due to low version
    print("Scenario 1: Feature requiring higher version")
    version_manager.set_backend_version("2.0.0")
    version_manager.register_feature_requirement(
        FeatureRequirement(
            feature_name="new_api",
            min_version=VersionInfo(3, 0, 0),
            description="New API with improved performance",
            alternative="Use legacy_api() method instead"
        )
    )
    
    try:
        # This would normally be called via decorator
        if not version_manager.is_feature_available("new_api"):
            hint = version_manager.get_version_hint("new_api", "Calling new_api() method")
            raise VersionError(f"Feature 'new_api' is not available.\n{hint}")
    except VersionError as e:
        print(f"Error: {e}")
    
    # Scenario 2: Feature not available due to high version (deprecated)
    print("\nScenario 2: Deprecated feature in newer version")
    version_manager.set_backend_version("3.5.0")
    version_manager.register_feature_requirement(
        FeatureRequirement(
            feature_name="old_api",
            max_version=VersionInfo(3, 0, 9),
            description="Legacy API that was deprecated in 3.1.0",
            alternative="Use modern_api() method instead"
        )
    )
    
    try:
        if not version_manager.is_feature_available("old_api"):
            hint = version_manager.get_version_hint("old_api", "Calling old_api() method")
            raise VersionError(f"Feature 'old_api' is not available.\n{hint}")
    except VersionError as e:
        print(f"Error: {e}")
    
    # Scenario 3: Version format error
    print("\nScenario 3: Invalid version format")
    try:
        version_manager.parse_version("invalid.version")
    except ValueError as e:
        print(f"Version parsing error: {e}")


def demonstrate_async_client_version_management():
    """Demonstrate async client version management"""
    print("\n=== Async Client Version Management ===")
    
    async def async_demo():
        print("Async client version management API:")
        print("""
        # Create async client
        client = AsyncClient()
        
        # Connect to MatrixOne (version will be auto-detected)
        await client.connect('localhost', 6001, 'root', 'password', 'test')
        
        # Get detected version
        version = client.get_backend_version()
        print(f"Backend version: {version}")
        
        # Check feature availability
        if client.is_feature_available('snapshot_creation'):
            print("Snapshot creation is available")
        
        # Use version-aware async methods
        try:
            snapshot = await client.snapshots.create('my_snapshot', 'cluster')
            print(f"Created snapshot: {snapshot}")
        except VersionError as e:
            print(f"Snapshot creation failed: {e}")
        
        await client.disconnect()
        """)
    
    # Run async demo
    asyncio.run(async_demo())


def main():
    """Main demonstration function"""
    print("MatrixOne Python SDK - Version Management Framework Demo")
    print("=" * 60)
    
    try:
        # Run all demonstrations
        demonstrate_version_parsing()
        demonstrate_feature_requirements()
        demonstrate_version_checking_decorator()
        demonstrate_real_client_integration()
        demonstrate_error_scenarios()
        demonstrate_async_client_version_management()
        
        print("\n" + "=" * 60)
        print("Version Management Framework Demo Completed Successfully!")
        print("\nKey Features:")
        print("✓ Semantic version parsing and comparison")
        print("✓ Automatic backend version detection")
        print("✓ Feature requirement registration and checking")
        print("✓ Version-aware decorators for methods")
        print("✓ Helpful error messages and suggestions")
        print("✓ Integration with both sync and async clients")
        print("✓ Custom feature requirements support")
        
    except Exception as e:
        print(f"\nDemo failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
