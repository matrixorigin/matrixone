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
Basic test for MatrixOne Python SDK
"""

from matrixone import Client, ConnectionError, QueryError


def test_basic_connection():
    """Test basic connection functionality"""
    client = Client()

    try:
        # Connect to MatrixOne
        client.connect(host="localhost", port=6001, user="root", password="111", database="test")

        print("✓ Connected to MatrixOne successfully")

        # Test basic query
        result = client.execute("SELECT 1 as test_value")
        print(f"✓ Query executed successfully: {result.fetchone()}")

        # Test SQLAlchemy engine
        engine = client.get_sqlalchemy_engine()
        print("✓ SQLAlchemy engine created successfully")

        # Test transaction
        with client.transaction() as tx:
            result = tx.execute("SELECT 2 as transaction_value")
            print(f"✓ Transaction executed successfully: {result.fetchone()}")

        print("✓ All basic tests passed!")

    except Exception as e:
        print(f"✗ Test failed: {e}")
    finally:
        client.disconnect()


if __name__ == "__main__":
    test_basic_connection()
