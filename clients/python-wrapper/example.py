"""
MatrixOne Python SDK - Basic Example
"""

from matrixone import Client
from matrixone.logger import create_default_logger
from sqlalchemy import text


def basic_example():
    """Basic usage example"""
    
    # Create MatrixOne logger for all logging
    logger = create_default_logger(
        enable_performance_logging=True,
        enable_sql_logging=True
    )
    
    # Create client with logger
    client = Client(logger=logger)
    
    try:
        # Connect to MatrixOne
        client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        logger.info("Connected to MatrixOne!")
        
        # Execute simple query
        result = client.execute("SELECT VERSION() as version")
        logger.info(f"MatrixOne version: {result.fetchone()}")
        
        # Create a test table
        client.execute("""
            CREATE TABLE IF NOT EXISTS test_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logger.info("Test table created!")
        
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
        logger.info("Data inserted!")
        
        # Query data
        result = client.execute("SELECT * FROM test_users")
        logger.info("Users:")
        for row in result.fetchall():
            logger.info(f"  ID: {row[0]}, Name: {row[1]}, Email: {row[2]}")
        
        # Use SQLAlchemy
        engine = client.get_sqlalchemy_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM test_users"))
            count = result.scalar()
            logger.info(f"Total users: {count}")
        
        # Clean up
        client.execute("DROP TABLE test_users")
        logger.info("Test table dropped!")
        
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        client.disconnect()
        logger.info("Disconnected from MatrixOne")


if __name__ == "__main__":
    basic_example()
