"""
Basic test for MatrixOne Python SDK
"""

from matrixone import Client, ConnectionError, QueryError


def test_basic_connection():
    """Test basic connection functionality"""
    client = Client()
    
    try:
        # Connect to MatrixOne
        client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
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
