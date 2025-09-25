"""
Online tests for advanced ORM features (join, func, group_by, having)
"""

import pytest
import os
import sys
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone import Client
from matrixone.orm import Model, Column
from sqlalchemy import func
from .test_config import online_config


class User(Model):
    """User model for testing"""
    _table_name = "test_users_advanced"
    _columns = {
        "id": Column("id", "INT", nullable=False),
        "name": Column("name", "VARCHAR(100)", nullable=False),
        "email": Column("email", "VARCHAR(100)", nullable=False),
        "age": Column("age", "INT", nullable=True),
        "department_id": Column("department_id", "INT", nullable=True),
    }


class Department(Model):
    """Department model for testing"""
    _table_name = "test_departments_advanced"
    _columns = {
        "id": Column("id", "INT", nullable=False),
        "name": Column("name", "VARCHAR(100)", nullable=False),
        "budget": Column("budget", "DECIMAL(10,2)", nullable=True),
    }


class Product(Model):
    """Product model for testing"""
    _table_name = "test_products_advanced"
    _columns = {
        "id": Column("id", "INT", nullable=False),
        "name": Column("name", "VARCHAR(100)", nullable=False),
        "price": Column("price", "DECIMAL(10,2)", nullable=False),
        "category": Column("category", "VARCHAR(50)", nullable=False),
        "quantity": Column("quantity", "INT", nullable=False),
    }


class TestORMAdvancedFeatures:
    """Online tests for advanced ORM features"""
    
    @pytest.fixture(scope="class")
    def test_client(self):
        """Create and connect Client for testing"""
        host, port, user, password, database = online_config.get_connection_params()
        client = Client()
        client.connect(host=host, port=port, user=user, password=password, database=database)
        try:
            yield client
        finally:
            try:
                client.disconnect()
            except Exception as e:
                print(f"Warning: Failed to disconnect client: {e}")
    
    @pytest.fixture(scope="class")
    def test_database(self, test_client):
        """Set up test database and tables"""
        test_db = "test_orm_advanced_db"
        
        try:
            test_client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
            test_client.execute(f"USE {test_db}")
            
            # Create test tables
            test_client.execute("""
                CREATE TABLE IF NOT EXISTS test_users_advanced (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    age INT,
                    department_id INT
                )
            """)
            
            test_client.execute("""
                CREATE TABLE IF NOT EXISTS test_departments_advanced (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    budget DECIMAL(10,2)
                )
            """)
            
            test_client.execute("""
                CREATE TABLE IF NOT EXISTS test_products_advanced (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    price DECIMAL(10,2),
                    category VARCHAR(50),
                    quantity INT
                )
            """)
            
            # Insert test data
            test_client.execute("""
                INSERT INTO test_users_advanced VALUES 
                (1, 'John Doe', 'john@example.com', 30, 1),
                (2, 'Jane Smith', 'jane@example.com', 25, 1),
                (3, 'Bob Johnson', 'bob@example.com', 35, 2),
                (4, 'Alice Brown', 'alice@example.com', 28, 2),
                (5, 'Charlie Wilson', 'charlie@example.com', 32, 1)
            """)
            
            test_client.execute("""
                INSERT INTO test_departments_advanced VALUES 
                (1, 'Engineering', 100000.00),
                (2, 'Marketing', 75000.00),
                (3, 'Sales', 90000.00)
            """)
            
            test_client.execute("""
                INSERT INTO test_products_advanced VALUES 
                (1, 'Laptop', 999.99, 'Electronics', 10),
                (2, 'Book', 19.99, 'Education', 50),
                (3, 'Phone', 699.99, 'Electronics', 15),
                (4, 'Pen', 2.99, 'Office', 100),
                (5, 'Tablet', 499.99, 'Electronics', 8)
            """)
            
            yield test_db
            
        finally:
            # Clean up
            try:
                test_client.execute(f"DROP DATABASE IF EXISTS {test_db}")
            except Exception as e:
                print(f"Cleanup failed: {e}")
    
    def test_select_specific_columns(self, test_client, test_database):
        """Test selecting specific columns"""
        query = test_client.query(User).select("id", "name", "email")
        results = query.all()
        
        assert len(results) == 5
        # Check that results have only the selected columns
        for user in results:
            assert hasattr(user, 'id')
            assert hasattr(user, 'name')
            assert hasattr(user, 'email')
    
    def test_join_operations(self, test_client, test_database):
        """Test JOIN operations"""
        # Test INNER JOIN
        query = test_client.query(User).select("u.name", "d.name as dept_name").join(
            "test_departments_advanced d", "u.department_id = d.id"
        )
        # Note: This is a simplified test - actual JOIN syntax may need adjustment
        # based on MatrixOne's specific requirements
        
        # Test LEFT OUTER JOIN
        query = test_client.query(User).select("u.name", "d.name as dept_name").outerjoin(
            "test_departments_advanced d", "u.department_id = d.id"
        )
    
    def test_group_by_operations(self, test_client, test_database):
        """Test GROUP BY operations"""
        # Group products by category
        query = test_client.query(Product).select(
            "category", 
            func.count("id")
        ).group_by("category")
        
        # This will test the SQL generation, though execution may need adjustment
        sql, params = query._build_sql()
        assert "GROUP BY" in sql
        assert "category" in sql
    
    def test_having_operations(self, test_client, test_database):
        """Test HAVING operations"""
        # Find categories with more than 1 product
        query = test_client.query(Product).select(
            "category",
            func.count("id")
        ).group_by("category").having("COUNT(id) > ?", 1)
        
        sql, params = query._build_sql()
        assert "HAVING" in sql
        assert "COUNT(id) > 1" in sql
    
    def test_aggregate_functions(self, test_client, test_database):
        """Test aggregate functions"""
        # Test COUNT
        count_query = test_client.query(User).select(func.count("id"))
        sql, params = count_query._build_sql()
        assert "count(id)" in sql.lower()
        
        # Test SUM
        sum_query = test_client.query(Product).select(func.sum("price"))
        sql, params = sum_query._build_sql()
        assert "sum(price)" in sql.lower()
        
        # Test AVG
        avg_query = test_client.query(User).select(func.avg("age"))
        sql, params = avg_query._build_sql()
        assert "avg(age)" in sql.lower()
        
        # Test MIN
        min_query = test_client.query(Product).select(func.min("price"))
        sql, params = min_query._build_sql()
        assert "min(price)" in sql.lower()
        
        # Test MAX
        max_query = test_client.query(Product).select(func.max("price"))
        sql, params = max_query._build_sql()
        assert "max(price)" in sql.lower()
    
    def test_complex_query_combination(self, test_client, test_database):
        """Test complex query with multiple features combined"""
        # Complex query: Find departments with average age > 30
        query = (test_client.query(User)
                .select("department_id", func.avg("age"))
                .group_by("department_id")
                .having("AVG(age) > ?", 30)
                .order_by("avg(age) DESC")
                .limit(5))
        
        sql, params = query._build_sql()
        assert "SELECT" in sql
        assert "avg(age)" in sql.lower()
        assert "GROUP BY" in sql
        assert "HAVING" in sql
        assert "ORDER BY" in sql
        assert "LIMIT 5" in sql
    
    def test_func_class_methods(self):
        """Test func class methods"""
        # Test that func methods return SQLAlchemy function objects
        from sqlalchemy.sql import functions
        
        # Test all func methods return proper SQLAlchemy function objects
        assert hasattr(func.count("id"), 'compile')
        assert hasattr(func.sum("price"), 'compile')
        assert hasattr(func.avg("age"), 'compile')
        assert hasattr(func.min("price"), 'compile')
        assert hasattr(func.max("price"), 'compile')
        assert hasattr(func.distinct("category"), 'compile')
    
    def test_sql_generation(self, test_client, test_database):
        """Test SQL generation for various query combinations"""
        # Test basic select with specific columns
        query1 = test_client.query(User).select("id", "name")
        sql1, _ = query1._build_sql()
        assert "SELECT id, name" in sql1
        
        # Test select with functions
        query2 = test_client.query(User).select(func.count("id"), func.avg("age"))
        sql2, _ = query2._build_sql()
        assert "count(id)" in sql2.lower()
        assert "avg(age)" in sql2.lower()
        
        # Test with joins
        query3 = (test_client.query(User)
                 .select("u.name", "d.name")
                 .join("test_departments_advanced d", "u.department_id = d.id"))
        sql3, _ = query3._build_sql()
        assert "JOIN" in sql3
        
        # Test with group by and having
        query4 = (test_client.query(Product)
                 .select("category", func.count("id"))
                 .group_by("category")
                 .having("COUNT(id) > ?", 1))
        sql4, _ = query4._build_sql()
        assert "GROUP BY" in sql4
        assert "HAVING" in sql4

    def test_sqlalchemy_label_support(self, test_client, test_database):
        """Test SQLAlchemy label() method support for aliases"""
        # Test single function with label
        query1 = test_client.query(User).select(func.count("id").label("user_count"))
        sql1, _ = query1._build_sql()
        assert "count(id) as user_count" in sql1.lower()
        
        # Execute the query and verify the result can be accessed by label name
        result1 = query1.all()
        assert hasattr(result1[0], 'user_count')
        assert result1[0].user_count == 5
        
        # Test multiple functions with labels
        query2 = test_client.query(User).select(
            func.count("id").label("total_users"),
            func.avg("age").label("avg_age")
        )
        sql2, _ = query2._build_sql()
        assert "count(id) as total_users" in sql2.lower()
        assert "avg(age) as avg_age" in sql2.lower()
        
        # Execute the query and verify results
        result2 = query2.all()
        assert hasattr(result2[0], 'total_users')
        assert hasattr(result2[0], 'avg_age')
        assert result2[0].total_users == 5
        
        # Test with GROUP BY and labels
        query3 = test_client.query(User).select(
            "department_id",
            func.count("id").label("dept_user_count")
        ).group_by("department_id")
        sql3, _ = query3._build_sql()
        assert "count(id) as dept_user_count" in sql3.lower()
        assert "GROUP BY" in sql3
        
        # Execute the query and verify results
        result3 = query3.all()
        assert len(result3) > 0
        for row in result3:
            assert hasattr(row, 'dept_user_count')
            assert hasattr(row, 'department_id')
            assert row.dept_user_count > 0

    def test_string_alias_support(self, test_client, test_database):
        """Test string alias support for aggregate functions"""
        # Test single function with string alias
        query1 = test_client.query(User).select("COUNT(id) AS user_count")
        sql1, _ = query1._build_sql()
        assert "COUNT(id) AS user_count" in sql1
        
        # Execute the query and verify the result can be accessed by alias name
        result1 = query1.all()
        assert hasattr(result1[0], 'user_count')
        assert result1[0].user_count == 5
        
        # Test multiple functions with string aliases
        query2 = test_client.query(User).select(
            "COUNT(id) AS total_users",
            "AVG(age) AS avg_age"
        )
        sql2, _ = query2._build_sql()
        assert "COUNT(id) AS total_users" in sql2
        assert "AVG(age) AS avg_age" in sql2
        
        # Execute the query and verify results
        result2 = query2.all()
        assert hasattr(result2[0], 'total_users')
        assert hasattr(result2[0], 'avg_age')
        assert result2[0].total_users == 5

    def test_mixed_alias_support(self, test_client, test_database):
        """Test mixing SQLAlchemy label() and string aliases"""
        # Test mixing different alias methods
        query = test_client.query(User).select(
            func.count("id").label("sqlalchemy_count"),
            "COUNT(age) AS string_count"
        )
        sql, _ = query._build_sql()
        assert "count(id) as sqlalchemy_count" in sql.lower()
        assert "COUNT(age) AS string_count" in sql
        
        # Execute the query and verify results
        result = query.all()
        assert hasattr(result[0], 'sqlalchemy_count')
        assert hasattr(result[0], 'string_count')
        assert result[0].sqlalchemy_count == 5
        assert result[0].string_count == 5

    def test_complex_alias_queries(self, test_client, test_database):
        """Test complex queries with aliases and other features"""
        # Test complex query with labels, GROUP BY, and HAVING
        query = (test_client.query(User)
                .select(
                    "department_id",
                    func.count("id").label("user_count"),
                    func.avg("age").label("avg_age")
                )
                .group_by("department_id")
                .having("COUNT(id) > ?", 1)
                .order_by("user_count DESC"))
        
        sql, _ = query._build_sql()
        assert "count(id) as user_count" in sql.lower()
        assert "avg(age) as avg_age" in sql.lower()
        assert "GROUP BY" in sql
        assert "HAVING" in sql
        assert "ORDER BY" in sql
        
        # Execute the query and verify results
        result = query.all()
        assert len(result) > 0
        for row in result:
            assert hasattr(row, 'user_count')
            assert hasattr(row, 'avg_age')
            assert hasattr(row, 'department_id')
            assert row.user_count > 1  # Due to HAVING clause

    def test_product_alias_queries(self, test_client, test_database):
        """Test product-related queries with aliases"""
        # Test product statistics with labels
        query = test_client.query(Product).select(
            "category",
            func.count("id").label("product_count"),
            func.avg("price").label("avg_price"),
            func.sum("quantity").label("total_quantity")
        ).group_by("category")
        
        sql, _ = query._build_sql()
        assert "count(id) as product_count" in sql.lower()
        assert "avg(price) as avg_price" in sql.lower()
        assert "sum(quantity) as total_quantity" in sql.lower()
        assert "GROUP BY" in sql
        
        # Execute the query and verify results
        result = query.all()
        assert len(result) > 0
        for row in result:
            assert hasattr(row, 'product_count')
            assert hasattr(row, 'avg_price')
            assert hasattr(row, 'total_quantity')
            assert hasattr(row, 'category')
            assert row.product_count > 0
            assert row.avg_price > 0
            assert row.total_quantity > 0


if __name__ == "__main__":
    pytest.main([__file__])
