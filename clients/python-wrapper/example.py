"""
MatrixOne Python SDK - Basic Example
"""

from matrixone import Client
from sqlalchemy import text


def basic_example():
    """Basic usage example"""
    
    # Create client
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
        
        print("Connected to MatrixOne!")
        
        # Execute simple query
        result = client.execute("SELECT VERSION() as version")
        print(f"MatrixOne version: {result.fetchone()}")
        
        # Create a test table
        client.execute("""
            CREATE TABLE IF NOT EXISTS test_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("Test table created!")
        
        # Insert some data
        with client.transaction() as tx:
            tx.execute(
                "INSERT INTO test_users (name, email) VALUES (%s, %s)",
                ("John Doe", "john@example.com")
            )
            tx.execute(
                "INSERT INTO test_users (name, email) VALUES (%s, %s)",
                ("Jane Smith", "jane@example.com")
            )
        print("Data inserted!")
        
        # Query data
        result = client.execute("SELECT * FROM test_users")
        print("Users:")
        for row in result.fetchall():
            print(f"  ID: {row[0]}, Name: {row[1]}, Email: {row[2]}")
        
        # Use SQLAlchemy
        engine = client.get_sqlalchemy_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM test_users"))
            count = result.scalar()
            print(f"Total users: {count}")
        
        # Clean up
        client.execute("DROP TABLE test_users")
        print("Test table dropped!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.disconnect()
        print("Disconnected from MatrixOne")


if __name__ == "__main__":
    basic_example()
