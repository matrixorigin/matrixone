#!/usr/bin/env python3
"""
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


def demonstrate_matrixone_version_parsing():
    """Demonstrate MatrixOne-specific version parsing"""
    print("=== MatrixOne Version Parsing Demo ===")
    
    version_manager = VersionManager()
    
    # Test different MatrixOne version formats
    test_versions = [
        "8.0.30-MatrixOne-v",           # Development version
        "10.5.2-MatrixOne-v",           # Development version
        "8.0.30-MatrixOne-v3.0.0",      # Release version
        "10.5.2-MatrixOne-v2.1.5",      # Release version
        "MatrixOne 3.0.1",              # Fallback format
        "Version 2.5.0",                # Alternative format
        "3.0.1",                        # Clean version
    ]
    
    print("Parsing MatrixOne version strings:")
    for version_string in test_versions:
        parsed = version_manager._parse_matrixone_version(version_string)
        version_type = "Development" if parsed == "999.0.0" else "Release"
        print(f"  '{version_string}' -> '{parsed}' ({version_type})")
    
    print("\nTesting invalid formats:")
    invalid_versions = [
        "8.0.30-MatrixOne",             # Missing version part
        "8.0.30-MatrixOne-v-",          # Invalid suffix
        "invalid",                       # Completely invalid
        "",                             # Empty string
    ]
    
    for invalid_version in invalid_versions:
        parsed = version_manager._parse_matrixone_version(invalid_version)
        print(f"  '{invalid_version}' -> {parsed}")


def demonstrate_development_version_priority():
    """Demonstrate that development version has highest priority"""
    print("\n=== Development Version Priority ===")
    
    version_manager = VersionManager()
    
    # Set development version
    version_manager.set_backend_version("999.0.0")
    dev_version = version_manager.get_backend_version()
    
    print(f"Current backend version: {dev_version}")
    print(f"Is development version: {version_manager.is_development_version()}")
    
    # Compare with various release versions
    release_versions = ["1.0.0", "2.5.0", "3.0.0", "10.0.0", "99.99.99"]
    
    print("\nDevelopment version vs release versions:")
    for release_version in release_versions:
        comparison = version_manager.compare_versions(dev_version, release_version)
        comparison_text = {
            VersionComparison.GREATER: "greater than",
            VersionComparison.EQUAL: "equal to",
            VersionComparison.LESS: "less than"
        }[comparison]
        print(f"  {dev_version} is {comparison_text} {release_version}")


def demonstrate_feature_compatibility():
    """Demonstrate feature compatibility with development version"""
    print("\n=== Feature Compatibility ===")
    
    version_manager = VersionManager()
    
    # Register various features with different version requirements
    features = [
        FeatureRequirement(
            feature_name="basic_feature",
            min_version=VersionInfo(1, 0, 0),
            description="Basic feature available since version 1.0.0"
        ),
        FeatureRequirement(
            feature_name="advanced_feature",
            min_version=VersionInfo(3, 0, 0),
            description="Advanced feature requiring version 3.0.0+"
        ),
        FeatureRequirement(
            feature_name="future_feature",
            min_version=VersionInfo(10, 0, 0),
            description="Future feature requiring version 10.0.0+"
        ),
        FeatureRequirement(
            feature_name="legacy_feature",
            max_version=VersionInfo(2, 9, 9),
            description="Legacy feature only available up to version 2.9.9"
        ),
    ]
    
    for feature in features:
        version_manager.register_feature_requirement(feature)
    
    # Test with development version
    version_manager.set_backend_version("999.0.0")
    print("Features available in development version (999.0.0):")
    
    for feature_name in ["basic_feature", "advanced_feature", "future_feature", "legacy_feature"]:
        available = version_manager.is_feature_available(feature_name)
        status = "✓ Available" if available else "✗ Not available"
        print(f"  {feature_name}: {status}")
        
        if not available:
            hint = version_manager.get_version_hint(feature_name)
            print(f"    Hint: {hint}")
    
    # Test with a release version
    print("\nFeatures available in release version (3.0.0):")
    version_manager.set_backend_version("3.0.0")
    
    for feature_name in ["basic_feature", "advanced_feature", "future_feature", "legacy_feature"]:
        available = version_manager.is_feature_available(feature_name)
        status = "✓ Available" if available else "✗ Not available"
        print(f"  {feature_name}: {status}")
        
        if not available:
            hint = version_manager.get_version_hint(feature_name)
            print(f"    Hint: {hint}")


def demonstrate_version_detection_simulation():
    """Simulate version detection from MatrixOne backend"""
    print("\n=== Version Detection Simulation ===")
    
    version_manager = VersionManager()
    
    # Simulate different backend responses
    backend_responses = [
        ("8.0.30-MatrixOne-v", "Development build"),
        ("8.0.30-MatrixOne-v3.0.0", "Release version 3.0.0"),
        ("10.5.2-MatrixOne-v2.1.5", "Release version 2.1.5"),
        ("MatrixOne 1.5.0", "Legacy format"),
    ]
    
    for version_string, description in backend_responses:
        print(f"\nBackend response: '{version_string}' ({description})")
        
        # Parse the version
        parsed_version = version_manager._parse_matrixone_version(version_string)
        
        if parsed_version:
            version_manager.set_backend_version(parsed_version)
            current_version = version_manager.get_backend_version()
            
            print(f"  Parsed version: {current_version}")
            print(f"  Is development: {version_manager.is_development_version()}")
            
            # Test compatibility with some common requirements
            requirements = ["2.0.0", "3.0.0", "5.0.0"]
            print("  Compatibility checks:")
            for req in requirements:
                compatible = version_manager.is_version_compatible(req, operator=">=")
                status = "✓ Compatible" if compatible else "✗ Not compatible"
                print(f"    >= {req}: {status}")
        else:
            print("  Failed to parse version")


def demonstrate_error_scenarios():
    """Demonstrate error scenarios and helpful messages"""
    print("\n=== Error Scenarios ===")
    
    version_manager = VersionManager()
    
    # Register a feature with version requirements
    feature = FeatureRequirement(
        feature_name="matrixone_v3_feature",
        min_version=VersionInfo(3, 0, 0),
        description="Feature available in MatrixOne 3.0.0+",
        alternative="Use legacy methods or upgrade MatrixOne"
    )
    version_manager.register_feature_requirement(feature)
    
    # Test with an older version
    version_manager.set_backend_version("2.5.0")
    
    print("Testing with MatrixOne 2.5.0:")
    if not version_manager.is_feature_available("matrixone_v3_feature"):
        hint = version_manager.get_version_hint("matrixone_v3_feature", "Trying to use v3 feature")
        print(f"Error: {hint}")
    
    # Test with development version
    version_manager.set_backend_version("999.0.0")
    
    print("\nTesting with development version:")
    available = version_manager.is_feature_available("matrixone_v3_feature")
    print(f"Feature available: {available}")
    if available:
        print("✓ Development version has access to all features!")


def main():
    """Main demonstration function"""
    print("MatrixOne Python SDK - MatrixOne Version Parsing Demo")
    print("=" * 60)
    
    try:
        # Run all demonstrations
        demonstrate_matrixone_version_parsing()
        demonstrate_development_version_priority()
        demonstrate_feature_compatibility()
        demonstrate_version_detection_simulation()
        demonstrate_error_scenarios()
        
        print("\n" + "=" * 60)
        print("MatrixOne Version Parsing Demo Completed Successfully!")
        print("\nKey Features:")
        print("✓ Development version parsing (8.0.30-MatrixOne-v -> 999.0.0)")
        print("✓ Release version parsing (8.0.30-MatrixOne-v3.0.0 -> 3.0.0)")
        print("✓ Fallback format support (MatrixOne 3.0.1, Version 2.5.0)")
        print("✓ Development version has highest priority (999.0.0)")
        print("✓ Proper error handling for invalid formats")
        print("✓ Feature compatibility checking")
        print("✓ Integration with MatrixOne Python SDK")
        
        print("\nVersion Format Support:")
        print("• Development: '8.0.30-MatrixOne-v' → '999.0.0' (highest)")
        print("• Release: '8.0.30-MatrixOne-v3.0.0' → '3.0.0'")
        print("• Legacy: 'MatrixOne 3.0.1' → '3.0.1'")
        print("• Clean: '3.0.1' → '3.0.1'")
        
    except Exception as e:
        print(f"\nDemo failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
