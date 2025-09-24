"""
Online tests for ORM functionality - tests actual database operations
"""

import unittest
import os
import sys
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone import Client
from matrixone.orm import Model, Column
from matrixone.exceptions import QueryError


class User(Model):
    """Test user model"""
    __tablename__ = "test_users"
    
    id = Column("id", "INT", primary_key=True)
    name = Column("name", "VARCHAR(100)")
    email = Column("email", "VARCHAR(100)")
    age = Column("age", "INT")


class Product(Model):
    """Test product model"""
    __tablename__ = "test_products"
    
    id = Column("id", "INT", primary_key=True)
    name = Column("name", "VARCHAR(100)")
    price = Column("price", "DECIMAL(10,2)")
    category = Column("category", "VARCHAR(50)")


class TestORMOnline(unittest.TestCase):
    """Online tests for ORM functionality"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test database connection"""
        cls.client = Client(
            host=os.getenv('MATRIXONE_HOST', '127.0.0.1'),
            port=int(os.getenv('MATRIXONE_PORT', '6001')),
            user=os.getenv('MATRIXONE_USER', 'root'),
            password=os.getenv('MATRIXONE_PASSWORD', '111'),
            database=os.getenv('MATRIXONE_DATABASE', 'test')
        )
        
        # Create test database and tables
        cls.test_db = "test_orm_db"
        
        try:
            cls.client.execute(f"CREATE DATABASE IF NOT EXISTS {cls.test_db}")
            cls.client.execute(f"USE {cls.test_db}")
            
            # Create test tables
            cls.client.execute("""
                CREATE TABLE IF NOT EXISTS test_users (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    age INT
                )
            """)
            
            cls.client.execute("""
                CREATE TABLE IF NOT EXISTS test_products (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    price DECIMAL(10,2),
                    category VARCHAR(50)
                )
            """)
            
            # Insert test data
            cls.client.execute("""
                INSERT INTO test_users VALUES 
                (1, 'John Doe', 'john@example.com', 30),
                (2, 'Jane Smith', 'jane@example.com', 25),
                (3, 'Bob Johnson', 'bob@example.com', 35)
            """)
            
            cls.client.execute("""
                INSERT INTO test_products VALUES 
                (1, 'Laptop', 999.99, 'Electronics'),
                (2, 'Book', 19.99, 'Education'),
                (3, 'Phone', 699.99, 'Electronics')
            """)
            
        except Exception as e:
            print(f"Setup failed: {e}")
            raise
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test database"""
        try:
            cls.client.execute(f"DROP DATABASE IF EXISTS {cls.test_db}")
        except Exception as e:
            print(f"Cleanup failed: {e}")
    
    def test_model_creation_and_attributes(self):
        """Test model creation and attribute access"""
        user = User(id=1, name="Test User", email="test@example.com", age=30)
        
        self.assertEqual(user.id, 1)
        self.assertEqual(user.name, "Test User")
        self.assertEqual(user.email, "test@example.com")
        self.assertEqual(user.age, 30)
        
        # Test to_dict method
        user_dict = user.to_dict()
        expected_dict = {
            'id': 1,
            'name': 'Test User',
            'email': 'test@example.com',
            'age': 30
        }
        self.assertEqual(user_dict, expected_dict)
    
    def test_model_table_creation(self):
        """Test creating tables from models"""
        # This test verifies that the model metadata is correctly set up
        self.assertEqual(User._table_name, "test_users")
        self.assertEqual(Product._table_name, "test_products")
        
        # Check that columns are properly defined
        self.assertIn('id', User._columns)
        self.assertIn('name', User._columns)
        self.assertIn('email', User._columns)
        self.assertIn('age', User._columns)
        
        # Check column properties
        id_column = User._columns['id']
        self.assertEqual(id_column.name, "id")
        self.assertEqual(id_column.type, "INT")
        self.assertTrue(id_column.primary_key)
    
    def test_orm_query_basic_operations(self):
        """Test basic ORM query operations"""
        # Test all() method
        users = self.client.query(User).all()
        self.assertEqual(len(users), 3)
        
        # Test first() method
        first_user = self.client.query(User).first()
        self.assertIsNotNone(first_user)
        self.assertIsInstance(first_user, User)
        
        # Test filter_by() method
        john = self.client.query(User).filter_by(name="John Doe").first()
        self.assertIsNotNone(john)
        self.assertEqual(john.name, "John Doe")
        self.assertEqual(john.email, "john@example.com")
    
    def test_orm_query_filtering(self):
        """Test ORM query filtering"""
        # Test filter_by with multiple conditions
        electronics = self.client.query(Product).filter_by(category="Electronics").all()
        self.assertEqual(len(electronics), 2)
        
        # Test filter() method with conditions
        expensive_products = self.client.query(Product).filter("price > ?", 500).all()
        self.assertEqual(len(expensive_products), 2)
        
        # Test combined filtering
        expensive_electronics = (self.client.query(Product)
                               .filter_by(category="Electronics")
                               .filter("price > ?", 500)
                               .all())
        self.assertEqual(len(expensive_electronics), 2)
    
    def test_orm_query_ordering_and_limiting(self):
        """Test ORM query ordering and limiting"""
        # Test order_by
        users_by_age = self.client.query(User).order_by("age DESC").all()
        self.assertEqual(len(users_by_age), 3)
        # Should be ordered by age descending
        self.assertGreaterEqual(users_by_age[0].age, users_by_age[1].age)
        
        # Test limit
        limited_users = self.client.query(User).limit(2).all()
        self.assertEqual(len(limited_users), 2)
        
        # Test combined ordering and limiting
        top_products = (self.client.query(Product)
                       .order_by("price DESC")
                       .limit(2)
                       .all())
        self.assertEqual(len(top_products), 2)
        self.assertGreaterEqual(top_products[0].price, top_products[1].price)
    
    def test_orm_query_count(self):
        """Test ORM query count functionality"""
        # Test count() method
        total_users = self.client.query(User).count()
        self.assertEqual(total_users, 3)
        
        # Test count with filtering
        electronics_count = self.client.query(Product).filter_by(category="Electronics").count()
        self.assertEqual(electronics_count, 2)
        
        # Test count with complex filtering
        expensive_count = self.client.query(Product).filter("price > ?", 100).count()
        self.assertEqual(expensive_count, 2)
    
    def test_orm_query_parameter_handling(self):
        """Test ORM query parameter handling"""
        # Test string parameters
        user = self.client.query(User).filter("name = ?", "Jane Smith").first()
        self.assertIsNotNone(user)
        self.assertEqual(user.name, "Jane Smith")
        
        # Test numeric parameters
        user = self.client.query(User).filter("age = ?", 30).first()
        self.assertIsNotNone(user)
        self.assertEqual(user.age, 30)
        
        # Test multiple parameters
        user = (self.client.query(User)
               .filter("name = ? AND age > ?", "Bob Johnson", 30)
               .first())
        self.assertIsNotNone(user)
        self.assertEqual(user.name, "Bob Johnson")
        self.assertGreater(user.age, 30)
    
    def test_orm_query_error_handling(self):
        """Test ORM query error handling"""
        # Test query with invalid table
        with self.assertRaises(QueryError):
            # This should raise an error for non-existent table
            self.client.query(User).filter("invalid_column = ?", "value").all()
    
    def test_orm_model_serialization(self):
        """Test model serialization methods"""
        user = User(id=1, name="Test User", email="test@example.com", age=30)
        
        # Test to_dict
        user_dict = user.to_dict()
        self.assertIsInstance(user_dict, dict)
        self.assertEqual(user_dict['id'], 1)
        self.assertEqual(user_dict['name'], "Test User")
        
        # Test from_dict
        new_user = User.from_dict(user_dict)
        self.assertEqual(new_user.id, user.id)
        self.assertEqual(new_user.name, user.name)
        self.assertEqual(new_user.email, user.email)
        self.assertEqual(new_user.age, user.age)


if __name__ == '__main__':
    unittest.main()
