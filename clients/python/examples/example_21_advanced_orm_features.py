#!/usr/bin/env python3
"""
Example 21: Advanced ORM Features
Demonstrates join, SQLAlchemy func, group_by, having, and other advanced SQLAlchemy-style features

Note: This example uses SQLAlchemy's func module for aggregate functions like COUNT, SUM, AVG, etc.
This provides better integration and type safety compared to custom implementations.
"""

import asyncio
import logging
import sys
import os
from datetime import datetime

# Add the matrixone module to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from matrixone import Client, AsyncClient, SnapshotLevel
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger
from sqlalchemy import func, Column, Integer, String, DECIMAL, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# Create MatrixOne logger for all logging
logger = create_default_logger(
    enable_performance_logging=True,
    enable_sql_logging=True
)


# Define models for the example
class User(Base):
    """User model"""
    __tablename__ = "demo_users_advanced"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100), nullable=False)
    age = Column(Integer, nullable=True)
    department_id = Column(Integer, nullable=True)
    salary = Column(DECIMAL(10, 2), nullable=True)


class Department(Base):
    """Department model"""
    __tablename__ = "demo_departments_advanced"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    budget = Column(DECIMAL(10, 2), nullable=True)
    location = Column(String(100), nullable=True)


class Product(Base):
    """Product model"""
    __tablename__ = "demo_products_advanced"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    price = Column(DECIMAL(10, 2), nullable=False)
    category = Column(String(50), nullable=False)
    quantity = Column(Integer, nullable=False)
    supplier_id = Column(Integer, nullable=True)


def demo_sync_advanced_orm():
    """Demonstrate advanced ORM features synchronously"""
    logger.info("🚀 Starting sync advanced ORM features demo")
    
    # Print current configuration
    print_config()
    
    # Get connection parameters from config (supports environment variables)
    host, port, user, password, database = get_connection_params()
    
    try:
        client = Client()
        client.connect(host, port, user, password, database)
        logger.info("✅ Connected to MatrixOne")
        
        # Set up demo database and tables
        demo_db = "demo_advanced_orm"
        client.execute(f"CREATE DATABASE IF NOT EXISTS {demo_db}")
        client.execute(f"USE {demo_db}")
        
        # Drop and recreate tables to ensure correct data types
        client.execute("DROP TABLE IF EXISTS demo_users_advanced")
        client.execute("""
            CREATE TABLE demo_users_advanced (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                age INT,
                department_id INT,
                salary DECIMAL(10,2)
            )
        """)
        
        client.execute("DROP TABLE IF EXISTS demo_departments_advanced")
        client.execute("""
            CREATE TABLE demo_departments_advanced (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                budget DECIMAL(10,2),
                location VARCHAR(100)
            )
        """)
        
        client.execute("DROP TABLE IF EXISTS demo_products_advanced")
        client.execute("""
            CREATE TABLE demo_products_advanced (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                price DECIMAL(10,2),
                category VARCHAR(50),
                quantity INT,
                supplier_id INT
            )
        """)
        
        # Insert sample data
        client.execute("""
            INSERT INTO demo_users_advanced VALUES 
            (1, 'John Doe', 'john@example.com', 30, 1, 75000.00),
            (2, 'Jane Smith', 'jane@example.com', 25, 1, 80000.00),
            (3, 'Bob Johnson', 'bob@example.com', 35, 2, 65000.00),
            (4, 'Alice Brown', 'alice@example.com', 28, 2, 70000.00),
            (5, 'Charlie Wilson', 'charlie@example.com', 32, 1, 85000.00),
            (6, 'Diana Prince', 'diana@example.com', 29, 3, 90000.00)
        """)
        
        client.execute("""
            INSERT INTO demo_departments_advanced VALUES 
            (1, 'Engineering', 500000.00, 'San Francisco'),
            (2, 'Marketing', 300000.00, 'New York'),
            (3, 'Sales', 400000.00, 'Chicago')
        """)
        
        client.execute("""
            INSERT INTO demo_products_advanced VALUES 
            (1, 'Laptop', 999.99, 'Electronics', 10, 1),
            (2, 'Book', 19.99, 'Education', 50, 2),
            (3, 'Phone', 699.99, 'Electronics', 15, 1),
            (4, 'Pen', 2.99, 'Office', 100, 3),
            (5, 'Tablet', 499.99, 'Electronics', 8, 1),
            (6, 'Notebook', 5.99, 'Office', 200, 3)
        """)
        
        logger.info("📊 Demo data inserted successfully")
        
        # 1. Basic SELECT with specific columns
        logger.info("1. Basic SELECT with specific columns")
        users = client.query(User).select("id", "name", "email").all()
        logger.info(f"Found {len(users)} users")
        for user in users[:3]:  # Show first 3
            logger.info(f"  - {user.name} ({user.email})")
        
        # 2. Using aggregate functions
        logger.info("2. Using aggregate functions")
        
        # Count total users
        count_query = client.query(User).select(func.count("id"))
        count_result = count_query.all()
        total_users = count_result[0].count if count_result else 0
        logger.info(f"Total users: {total_users}")
        
        # Average salary
        avg_query = client.query(User).select(func.avg("salary"))
        avg_result = avg_query.all()
        if avg_result and avg_result[0].avg:
            avg_salary = avg_result[0].avg
            logger.info(f"Average salary: ${avg_salary:.2f}")
        else:
            logger.info("No salary data")
        
        # 3. GROUP BY operations
        logger.info("3. GROUP BY operations")
        
        # Group users by department
        dept_query = (client.query(User)
                     .select("department_id", func.count("id"))
                     .group_by("department_id"))
        dept_results = dept_query.all()
        logger.info("Users per department:")
        for result in dept_results:
            logger.info(f"  Department {result.department_id}: {result.count} users")
        
        # 4. HAVING clause
        logger.info("4. HAVING clause")
        
        # Find departments with more than 1 user
        having_query = (client.query(User)
                       .select("department_id", func.count("id"))
                       .group_by("department_id")
                       .having("COUNT(id) > ?", 1))
        having_results = having_query.all()
        logger.info("Departments with more than 1 user:")
        for result in having_results:
            logger.info(f"  Department {result.department_id}: {result.count} users")
        
        # 5. Complex queries with multiple features
        logger.info("5. Complex queries with multiple features")
        
        # Find high-earning users by department
        complex_query = (client.query(User)
                        .select("department_id", 
                               func.avg("salary"),
                               func.count("id"))
                        .group_by("department_id")
                        .having("AVG(salary) > ?", 70000)
                        .order_by("avg(salary) DESC"))
        complex_results = complex_query.all()
        logger.info("High-earning departments (avg salary > $70k):")
        for result in complex_results:
            logger.info(f"  Department {result.department_id}: ${result.avg:.2f} avg salary, {result.count} users")
        
        # 6. Product analysis
        logger.info("6. Product analysis")
        
        # Products by category with statistics
        product_query = (client.query(Product)
                        .select("category",
                               func.count("id"),
                               func.avg("price"),
                               func.sum("quantity"))
                        .group_by("category")
                        .order_by("avg(price) DESC"))
        product_results = product_query.all()
        logger.info("Products by category:")
        for result in product_results:
            logger.info(f"  {result.category}: {result.count} products, ${result.avg:.2f} avg price, {result.sum} total quantity")
        
        # 7. Using DISTINCT
        logger.info("7. Using DISTINCT")
        
        # Get unique categories
        distinct_query = client.query(Product).select(func.distinct("category"))
        distinct_results = distinct_query.all()
        logger.info("Unique product categories:")
        for result in distinct_results:
            logger.info(f"  - {result.DISTINCT_category}")
        
        # 8. MIN/MAX functions
        logger.info("8. MIN/MAX functions")
        
        # Price range for products
        price_range_query = (client.query(Product)
                            .select(func.min("price"),
                                   func.max("price"),
                                   func.avg("price")))
        price_range_result = price_range_query.all()
        if price_range_result:
            result = price_range_result[0]
            logger.info(f"Product price range: ${result.min:.2f} - ${result.max:.2f} (avg: ${result.avg:.2f})")
        
        logger.info("✅ Advanced ORM features demo completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Demo failed: {e}")
        raise
    finally:
        try:
            # Clean up
            client.execute(f"DROP DATABASE IF EXISTS {demo_db}")
            client.disconnect()
        except Exception as e:
            logger.warning(f"Cleanup failed: {e}")


async def demo_async_advanced_orm():
    """Demonstrate advanced ORM features asynchronously"""
    logger.info("🚀 Starting async advanced ORM features demo")
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    try:
        client = AsyncClient()
        await client.connect(host, port, user, password, database)
        logger.info("✅ Connected to MatrixOne (async)")
        
        # Set up demo database and tables
        demo_db = "demo_async_advanced_orm"
        await client.execute(f"CREATE DATABASE IF NOT EXISTS {demo_db}")
        await client.execute(f"USE {demo_db}")
        
        # Drop and recreate tables (same as sync version)
        await client.execute("DROP TABLE IF EXISTS demo_users_advanced")
        await client.execute("""
            CREATE TABLE demo_users_advanced (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                age INT,
                department_id INT,
                salary DECIMAL(10,2)
            )
        """)
        
        await client.execute("DROP TABLE IF EXISTS demo_products_advanced")
        await client.execute("""
            CREATE TABLE demo_products_advanced (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                price DECIMAL(10,2),
                category VARCHAR(50),
                quantity INT
            )
        """)
        
        # Insert sample data
        await client.execute("""
            INSERT INTO demo_users_advanced VALUES 
            (1, 'Async John', 'async.john@example.com', 30, 1, 75000.00),
            (2, 'Async Jane', 'async.jane@example.com', 25, 1, 80000.00),
            (3, 'Async Bob', 'async.bob@example.com', 35, 2, 65000.00)
        """)
        
        await client.execute("""
            INSERT INTO demo_products_advanced VALUES 
            (1, 'Async Laptop', 999.99, 'Electronics', 10),
            (2, 'Async Book', 19.99, 'Education', 50),
            (3, 'Async Phone', 699.99, 'Electronics', 15)
        """)
        
        logger.info("📊 Async demo data inserted successfully")
        
        # Test async aggregate functions
        logger.info("Testing async aggregate functions")
        
        # Count users
        count_query = client.query(User).select(func.count("id"))
        count_result = await count_query.all()
        total_users = count_result[0].count if count_result else 0
        logger.info(f"Total async users: {total_users}")
        
        # Average salary
        avg_query = client.query(User).select(func.avg("salary"))
        avg_result = await avg_query.all()
        if avg_result and avg_result[0].avg:
            avg_salary = avg_result[0].avg
            logger.info(f"Average async salary: ${avg_salary:.2f}")
        else:
            logger.info("No async salary data")
        
        # Group by department
        dept_query = (client.query(User)
                     .select("department_id", func.count("id"))
                     .group_by("department_id"))
        dept_results = await dept_query.all()
        logger.info("Async users per department:")
        for result in dept_results:
            logger.info(f"  Department {result.department_id}: {result.count} users")
        
        logger.info("✅ Async advanced ORM features demo completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Async demo failed: {e}")
        raise
    finally:
        try:
            # Clean up
            await client.execute(f"DROP DATABASE IF EXISTS {demo_db}")
            await client.disconnect()
        except Exception as e:
            logger.warning(f"Async cleanup failed: {e}")


def main():
    """Main function to run all demos"""
    logger.info("🎯 MatrixOne Advanced ORM Features Demo")
    logger.info("=" * 50)
    
    try:
        # Run sync demo
        demo_sync_advanced_orm()
        
        logger.info("\n" + "=" * 50)
        
        # Run async demo
        asyncio.run(demo_async_advanced_orm())
        
        logger.info("\n🎉 All advanced ORM demos completed successfully!")
        
    except Exception as e:
        logger.error(f"💥 Demo failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
