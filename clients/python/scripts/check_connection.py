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
Connection check script for MatrixOne database.
This script verifies if a MatrixOne database connection can be established
before running online tests.
"""

import sys
import os
import time

# Add the parent directory to the path to import matrixone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from matrixone import Client
from matrixone.exceptions import ConnectionError

def get_connection_params():
    """Get connection parameters from environment variables or use defaults."""
    return {
        'host': os.getenv('MATRIXONE_HOST', 'localhost'),
        'port': int(os.getenv('MATRIXONE_PORT', '6001')),
        'user': os.getenv('MATRIXONE_USER', 'root'),
        'password': os.getenv('MATRIXONE_PASSWORD', '111'),
        'database': os.getenv('MATRIXONE_DATABASE', 'test')
    }

def check_connection():
    """Check if MatrixOne database connection is available."""
    print("🔍 Checking MatrixOne database connection...")
    
    params = get_connection_params()
    print(f"   Host: {params['host']}:{params['port']}")
    print(f"   User: {params['user']}")
    print(f"   Database: {params['database']}")
    
    try:
        # Create client and attempt connection
        client = Client()
        client.connect(
            host=params['host'],
            port=params['port'],
            user=params['user'],
            password=params['password'],
            database=params['database']
        )
        
        # Test basic connection with a simple query
        result = client.execute("SELECT 1 as test")
        client.disconnect()
        
        print("✅ MatrixOne database connection successful!")
        print("   Online tests can proceed.")
        return True
        
    except ConnectionError as e:
        print("❌ MatrixOne database connection failed!")
        print(f"   Error: {e}")
        print("   Please ensure MatrixOne database is running and accessible.")
        return False
        
    except Exception as e:
        print("❌ Unexpected error during connection check!")
        print(f"   Error: {e}")
        print("   Please check your MatrixOne database configuration.")
        return False

def main():
    """Main function."""
    if check_connection():
        sys.exit(0)
    else:
        print("\n💡 To run online tests:")
        print("   1. Start MatrixOne database")
        print("   2. Set environment variables if needed:")
        print("      export MATRIXONE_HOST=localhost")
        print("      export MATRIXONE_PORT=6001")
        print("      export MATRIXONE_USER=root")
        print("      export MATRIXONE_PASSWORD=111")
        print("      export MATRIXONE_DATABASE=test")
        print("   3. Run: make test-online")
        sys.exit(1)

if __name__ == "__main__":
    main()
