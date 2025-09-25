#!/usr/bin/env python3
"""
MatrixOne CTE (Common Table Expression) Usage Examples

This example demonstrates how to use the new CTEQuery builder in MatrixOne Python client.
CTEs allow you to define temporary named result sets that exist within the scope of a single SQL statement.
"""

import sys
import os

# Add the parent directory to the path so we can import matrixone
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from matrixone import Client
from matrixone.orm import Model, Column
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger
from sqlalchemy import func


# Define sample models
class User(Model):
    """User model for CTE demo"""
    _table_name = "test_users_cte"
    _columns = {
        "id": Column("id", "INT", nullable=False),
        "name": Column("name", "VARCHAR(100)", nullable=False),
        "department_id": Column("department_id", "INT", nullable=True),
        "salary": Column("salary", "DECIMAL(10,2)", nullable=True),
        "email": Column("email", "VARCHAR(100)", nullable=True),
    }


class Department(Model):
    """Department model for CTE demo"""
    _table_name = "test_departments_cte"
    _columns = {
        "id": Column("id", "INT", nullable=False),
        "name": Column("name", "VARCHAR(100)", nullable=False),
        "budget": Column("budget", "DECIMAL(12,2)", nullable=True),
    }


class Order(Model):
    """Order model for CTE demo"""
    _table_name = "test_orders_cte"
    _columns = {
        "id": Column("id", "INT", nullable=False),
        "user_id": Column("user_id", "INT", nullable=True),
        "amount": Column("amount", "DECIMAL(10,2)", nullable=True),
        "order_date": Column("order_date", "DATE", nullable=True),
    }


def main():
    """Main function demonstrating CTE usage"""
    
    # Create logger
    logger = create_default_logger(
        enable_performance_logging=True,
        enable_sql_logging=True
    )
    
    logger.info("🎯 MatrixOne CTE (Common Table Expression) Usage Demo")
    logger.info("=" * 60)
    
    # Print current configuration
    print_config()
    
    # Get connection parameters from config (supports environment variables)
    host, port, user, password, database = get_connection_params()
    
    # Connect to MatrixOne
    logger.info("Connecting to MatrixOne...")
    client = Client()
    client.connect(host, port, user, password, database)
    logger.info("✅ Connected to MatrixOne")
    
    try:
        # Set up demo database and tables
        demo_db = "demo_cte"
        client.execute(f"CREATE DATABASE IF NOT EXISTS {demo_db}")
        client.execute(f"USE {demo_db}")
        
        # Create tables
        logger.info("Creating tables...")
        client.execute("DROP TABLE IF EXISTS test_users_cte")
        client.execute("""
            CREATE TABLE test_users_cte (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                department_id INT,
                salary DECIMAL(10,2),
                email VARCHAR(100)
            )
        """)
        
        client.execute("DROP TABLE IF EXISTS test_departments_cte")
        client.execute("""
            CREATE TABLE test_departments_cte (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                budget DECIMAL(12,2)
            )
        """)
        
        client.execute("DROP TABLE IF EXISTS test_orders_cte")
        client.execute("""
            CREATE TABLE test_orders_cte (
                id INT PRIMARY KEY,
                user_id INT,
                amount DECIMAL(10,2),
                order_date DATE
            )
        """)
        
        # Insert sample data
        logger.info("Inserting sample data...")
        client.execute("""
            INSERT INTO test_departments_cte (id, name, budget) VALUES 
            (1, 'Engineering', 100000),
            (2, 'Marketing', 50000),
            (3, 'Sales', 80000),
            (4, 'HR', 30000)
        """)
        
        client.execute("""
            INSERT INTO test_users_cte (id, name, department_id, salary, email) VALUES 
            (1, 'John Doe', 1, 75000, 'john@example.com'),
            (2, 'Jane Smith', 1, 80000, 'jane@example.com'),
            (3, 'Bob Johnson', 2, 60000, 'bob@example.com'),
            (4, 'Alice Brown', 3, 70000, 'alice@example.com'),
            (5, 'Charlie Wilson', 4, 55000, 'charlie@example.com'),
            (6, 'David Lee', 1, 85000, 'david@example.com'),
            (7, 'Eva Chen', 3, 65000, 'eva@example.com')
        """)
        
        client.execute("""
            INSERT INTO test_orders_cte (id, user_id, amount, order_date) VALUES 
            (1, 1, 1000, '2024-01-15'),
            (2, 1, 1500, '2024-02-20'),
            (3, 2, 2000, '2024-01-25'),
            (4, 3, 800, '2024-02-10'),
            (5, 4, 1200, '2024-01-30'),
            (6, 5, 900, '2024-02-15'),
            (7, 6, 1800, '2024-01-20'),
            (8, 7, 1100, '2024-02-05')
        """)
        
        # Example 1: Basic CTE - Department Statistics
        logger.info("\n=== Example 1: Basic CTE - Department Statistics ===")
        
        # Create a CTE for department statistics
        dept_stats = client.query(User).select(
            'department_id', 
            func.count('id').label('user_count'),
            func.avg('salary').label('avg_salary'),
            func.max('salary').label('max_salary'),
            func.min('salary').label('min_salary')
        ).group_by('department_id')
        
        # Use CTE to find departments with high average salary
        cte_query = client.cte_query()
        results = (cte_query
                  .with_cte('dept_stats', dept_stats)
                  .select_from('dept_stats.department_id', 'dept_stats.user_count', 
                              'dept_stats.avg_salary', 'dept_stats.max_salary', 'dept_stats.min_salary')
                  .from_table('dept_stats')
                  .where('dept_stats.avg_salary > ?', 65000)
                  .order_by('dept_stats.avg_salary DESC')
                  .execute())
        
        logger.info("Departments with average salary > 65000:")
        for row in results:
            logger.info(f"  Dept {row.department_id}: {row.user_count} users, "
                       f"avg: ${row.avg_salary:.2f}, max: ${row.max_salary:.2f}, min: ${row.min_salary:.2f}")
        
        # Example 2: CTE with JOINs
        logger.info("\n=== Example 2: CTE with JOINs ===")
        
        # Create CTE for high-salary users
        high_salary_users = client.query(User).select('id', 'name', 'department_id', 'salary').filter('salary > ?', 70000)
        
        # Use CTE with JOIN to get department names
        cte_query2 = client.cte_query()
        results2 = (cte_query2
                   .with_cte('high_salary_users', high_salary_users)
                   .select_from('high_salary_users.name', 'high_salary_users.salary', 'd.name')
                   .from_table('high_salary_users')
                   .inner_join('test_departments_cte d', 'high_salary_users.department_id = d.id')
                   .order_by('high_salary_users.salary DESC')
                   .execute())
        
        logger.info("High-salary users with department names:")
        for row in results2:
            logger.info(f"  {row.name}: ${row.salary:.2f} in {row.name_2}")
        
        # Example 3: Multiple CTEs
        logger.info("\n=== Example 3: Multiple CTEs ===")
        
        # Create first CTE: department statistics
        dept_stats2 = client.query(User).select(
            'department_id', 
            func.count('id').label('user_count'),
            func.avg('salary').label('avg_salary')
        ).group_by('department_id')
        
        # Create second CTE: high-budget departments
        high_budget_depts = client.query(Department).select('id', 'name', 'budget').filter('budget > ?', 60000)
        
        # Use multiple CTEs
        cte_query3 = client.cte_query()
        results3 = (cte_query3
                   .with_cte('dept_stats', dept_stats2)
                   .with_cte('high_budget_depts', high_budget_depts)
                   .select_from('ds.department_id', 'ds.user_count', 'ds.avg_salary', 'hbd.name', 'hbd.budget')
                   .from_table('dept_stats ds')
                   .inner_join('high_budget_depts hbd', 'ds.department_id = hbd.id')
                   .order_by('ds.avg_salary DESC')
                   .execute())
        
        logger.info("High-budget departments with statistics:")
        for row in results3:
            logger.info(f"  {row.name}: {row.user_count} users, avg salary: ${row.avg_salary:.2f}, budget: ${row.budget:.2f}")
        
        # Example 4: CTE with Complex Aggregations
        logger.info("\n=== Example 4: CTE with Complex Aggregations ===")
        
        # Create CTE for user order statistics
        user_order_stats = client.query(Order).select(
            'user_id',
            func.count('id').label('order_count'),
            func.sum('amount').label('total_amount'),
            func.avg('amount').label('avg_order_amount')
        ).group_by('user_id')
        
        # Use CTE to find top customers
        cte_query4 = client.cte_query()
        results4 = (cte_query4
                   .with_cte('user_order_stats', user_order_stats)
                   .select_from('u.name', 'uos.order_count', 'uos.total_amount', 'uos.avg_order_amount')
                   .from_table('user_order_stats uos')
                   .inner_join('test_users_cte u', 'uos.user_id = u.id')
                   .where('uos.total_amount > ?', 2000)
                   .order_by('uos.total_amount DESC')
                   .execute())
        
        logger.info("Top customers (total amount > $2000):")
        for row in results4:
            logger.info(f"  {row.name}: {row.order_count} orders, "
                       f"total: ${row.total_amount:.2f}, avg: ${row.avg_order_amount:.2f}")
        
        # Example 5: CTE with Raw SQL
        logger.info("\n=== Example 5: CTE with Raw SQL ===")
        
        # Use raw SQL for CTE
        cte_query5 = client.cte_query()
        results5 = (cte_query5
                   .with_cte('salary_ranges', """
                       SELECT 
                           CASE 
                               WHEN salary < 60000 THEN 'Low'
                               WHEN salary < 80000 THEN 'Medium'
                               ELSE 'High'
                           END as salary_range,
                           COUNT(*) as count
                       FROM test_users_cte
                       GROUP BY 
                           CASE 
                               WHEN salary < 60000 THEN 'Low'
                               WHEN salary < 80000 THEN 'Medium'
                               ELSE 'High'
                           END
                   """)
                   .select_from('salary_range', 'count')
                   .from_table('salary_ranges')
                   .order_by('count DESC')
                   .execute())
        
        logger.info("Salary distribution:")
        for row in results5:
            logger.info(f"  {row.salary_range}: {row.count} users")
        
        logger.info("\n🎉 CTE Examples Completed Successfully!")
        
    except Exception as e:
        logger.error(f"💥 Demo failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        # Clean up
        logger.info("\nCleaning up...")
        try:
            client.execute("DROP TABLE IF EXISTS test_orders_cte")
            client.execute("DROP TABLE IF EXISTS test_users_cte")
            client.execute("DROP TABLE IF EXISTS test_departments_cte")
            client.close()
        except:
            pass  # Ignore cleanup errors
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
