#!/usr/bin/env python3
"""
Example: Table Aliases Support
Demonstrates various table alias scenarios in MatrixOne ORM
"""

import sys
import os

# Add the matrixone module to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from matrixone import Client
from matrixone.orm import Column, Model
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger
from sqlalchemy import func

# Create MatrixOne logger
logger = create_default_logger(
    enable_performance_logging=True,
    enable_sql_logging=True
)


# Define models for the example
class User(Model):
    """User model"""
    _table_name = "demo_users_alias"
    _columns = {
        "id": Column("id", "INT", nullable=False),
        "name": Column("name", "VARCHAR(100)", nullable=False),
        "email": Column("email", "VARCHAR(100)", nullable=False),
        "salary": Column("salary", "DECIMAL(10,2)", nullable=True),
        "department_id": Column("department_id", "INT", nullable=True),
        "manager_id": Column("manager_id", "INT", nullable=True),
    }


class Department(Model):
    """Department model"""
    _table_name = "demo_departments_alias"
    _columns = {
        "id": Column("id", "INT", nullable=False),
        "name": Column("name", "VARCHAR(100)", nullable=False),
        "budget": Column("budget", "DECIMAL(10,2)", nullable=True),
        "manager_id": Column("manager_id", "INT", nullable=True),
    }


def demo_table_aliases():
    """Demonstrate table alias functionality"""
    logger.info("🚀 Starting table aliases demo")
    
    # Print current configuration
    print_config()
    
    # Get connection parameters
    host, port, user, password, database = get_connection_params()
    
    try:
        client = Client()
        client.connect(host, port, user, password, database)
        logger.info("✅ Connected to MatrixOne")
        
        # Set up demo database and tables
        demo_db = "demo_table_aliases"
        client.execute(f"CREATE DATABASE IF NOT EXISTS {demo_db}")
        client.execute(f"USE {demo_db}")
        
        # Drop and recreate tables
        client.execute("DROP TABLE IF EXISTS demo_users_alias")
        client.execute("""
            CREATE TABLE demo_users_alias (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                salary DECIMAL(10,2),
                department_id INT,
                manager_id INT
            )
        """)
        
        client.execute("DROP TABLE IF EXISTS demo_departments_alias")
        client.execute("""
            CREATE TABLE demo_departments_alias (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                budget DECIMAL(10,2),
                manager_id INT
            )
        """)
        
        # Insert sample data
        client.execute("""
            INSERT INTO demo_users_alias VALUES 
            (1, 'John Doe', 'john@example.com', 75000.00, 1, 3),
            (2, 'Jane Smith', 'jane@example.com', 80000.00, 1, 3),
            (3, 'Bob Johnson', 'bob@example.com', 95000.00, 2, NULL),
            (4, 'Alice Brown', 'alice@example.com', 70000.00, 2, 3),
            (5, 'Charlie Wilson', 'charlie@example.com', 85000.00, 1, 3)
        """)
        
        client.execute("""
            INSERT INTO demo_departments_alias VALUES 
            (1, 'Engineering', 500000.00, 3),
            (2, 'Marketing', 300000.00, 3),
            (3, 'Sales', 400000.00, NULL)
        """)
        
        logger.info("📊 Demo data inserted successfully")
        
        # 1. Basic table alias usage
        logger.info("1. Basic table alias usage")
        query1 = client.query(User).alias('u').select('u.name', 'u.email')
        sql1, _ = query1._build_sql()
        logger.info(f"Generated SQL: {sql1}")
        
        results1 = query1.all()
        logger.info(f"Found {len(results1)} users")
        for user in results1[:3]:
            logger.info(f"  - {user.name} ({user.email})")
        
        # 2. JOIN with table aliases
        logger.info("2. JOIN with table aliases")
        query2 = client.query(User).alias('u').select('u.name', 'd.name').join(
            'demo_departments_alias d', 'u.department_id = d.id'
        )
        sql2, _ = query2._build_sql()
        logger.info(f"Generated SQL: {sql2}")
        
        results2 = query2.all()
        logger.info("Users with their departments:")
        for result in results2:
            logger.info(f"  - {result.name} works in {result.name}")
        
        # 3. Self-join (manager-employee relationship)
        logger.info("3. Self-join with aliases (manager-employee)")
        query3 = client.query(User).alias('e').select('e.name', 'm.name').join(
            'demo_users_alias m', 'e.manager_id = m.id'
        )
        sql3, _ = query3._build_sql()
        logger.info(f"Generated SQL: {sql3}")
        
        results3 = query3.all()
        logger.info("Employee-manager relationships:")
        for result in results3:
            logger.info(f"  - {result.name} reports to {result.name}")
        
        # 4. Complex WHERE with aliases
        logger.info("4. Complex WHERE with aliases")
        query4 = client.query(User).alias('u').select('u.name', 'u.salary').join(
            'demo_departments_alias d', 'u.department_id = d.id'
        ).filter('u.salary > 70000').filter('d.budget > 400000')
        sql4, _ = query4._build_sql()
        logger.info(f"Generated SQL: {sql4}")
        
        results4 = query4.all()
        logger.info("High-earning users in high-budget departments:")
        for result in results4:
            logger.info(f"  - {result.name}: ${result.salary}")
        
        # 5. Aggregate functions with aliases
        logger.info("5. Aggregate functions with aliases")
        query5 = client.query(User).alias('u').select(
            'u.department_id',
            func.count('u.id').label('user_count'),
            func.avg('u.salary').label('avg_salary')
        ).group_by('u.department_id')
        sql5, _ = query5._build_sql()
        logger.info(f"Generated SQL: {sql5}")
        
        results5 = query5.all()
        logger.info("Department statistics:")
        for result in results5:
            logger.info(f"  Department {result.department_id}: {result.user_count} users, avg salary ${result.avg_salary:.2f}")
        
        # 6. Subquery examples
        logger.info("6. Subquery examples")
        
        # Create a subquery for average salary
        avg_salary_query = client.query(User).select(func.avg('salary'))
        avg_subquery = avg_salary_query.subquery('avg_sal')
        logger.info(f"Average salary subquery: {avg_subquery}")
        
        # Create department stats subquery
        dept_stats_query = client.query(User).select(
            'department_id',
            func.count('id'),
            func.avg('salary')
        ).group_by('department_id')
        dept_subquery = dept_stats_query.subquery('dept_stats')
        logger.info(f"Department stats subquery: {dept_subquery}")
        
        # 7. Limitations and workarounds
        logger.info("7. Current limitations and workarounds")
        logger.info("Current ORM limitations:")
        logger.info("  - Subqueries in WHERE clauses require manual SQL construction")
        logger.info("  - Complex nested queries need raw SQL")
        logger.info("  - EXISTS clauses not directly supported")
        
        logger.info("Workarounds:")
        logger.info("  - Use raw SQL for complex subqueries")
        logger.info("  - Build subqueries separately and combine manually")
        logger.info("  - Use query.subquery() method to generate subquery strings")
        
        # Example of manual complex query construction
        logger.info("Example of complex query (manual SQL):")
        complex_sql = """
        SELECT u.name, u.salary, d.name as dept_name
        FROM demo_users_alias u
        JOIN demo_departments_alias d ON u.department_id = d.id
        WHERE u.salary > (SELECT AVG(salary) FROM demo_users_alias)
        ORDER BY u.salary DESC
        """
        logger.info(f"Complex SQL: {complex_sql.strip()}")
        
        # Execute the complex query manually
        complex_result = client.execute(complex_sql)
        logger.info("High-earning users (above average):")
        for row in complex_result.rows[:3]:
            logger.info(f"  - {row[0]}: ${row[1]} ({row[2]})")
        
        logger.info("✅ Table aliases demo completed successfully")
        
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


def main():
    """Main function"""
    logger.info("🎯 MatrixOne Table Aliases Demo")
    logger.info("=" * 50)
    
    try:
        demo_table_aliases()
        logger.info("\n🎉 Table aliases demo completed successfully!")
        
    except Exception as e:
        logger.error(f"💥 Demo failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
