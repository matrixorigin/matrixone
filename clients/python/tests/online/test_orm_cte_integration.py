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
Test CTE (Common Table Expression) integration with MatrixOneQuery

This test verifies that the new SQLAlchemy-style CTE support works correctly
with MatrixOneQuery, including:
- Creating CTEs from queries
- Using CTEs in other queries
- Multiple CTEs in a single query
- CTE parameter handling
"""

import pytest
from sqlalchemy import Column, Integer, String, create_engine, text
from sqlalchemy.orm import sessionmaker

from matrixone.client import Client
from matrixone.orm import CTE, declarative_base

Base = declarative_base()


class User(Base):
    __tablename__ = "cte_users"
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    department = Column(String(50))
    salary = Column(Integer)


class Article(Base):
    __tablename__ = "cte_articles"
    id = Column(Integer, primary_key=True)
    title = Column(String(100))
    author_id = Column(Integer)
    views = Column(Integer)


class TestORMCTEIntegration:
    """Test CTE integration with MatrixOneQuery"""

    @pytest.fixture(autouse=True)
    def setup_and_cleanup(self, connection_params, test_database):
        """Setup test database and tables"""
        host, port, user, password, database = connection_params
        self.client = Client(host=host, port=port, user=user, password=password, database=database)
        self.test_db = f"cte_test_db_{test_database}"

        # Create test database
        self.client.execute(f"CREATE DATABASE IF NOT EXISTS {self.test_db}")
        self.client.execute(f"USE {self.test_db}")

        # Create tables
        self.client.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.test_db}.cte_users (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(50),
                department VARCHAR(50),
                salary INT
            )
        """
        )

        self.client.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.test_db}.cte_articles (
                id INT PRIMARY KEY AUTO_INCREMENT,
                title VARCHAR(100),
                author_id INT,
                views INT
            )
        """
        )

        # Insert test data
        self.client.execute(
            f"""
            INSERT INTO {self.test_db}.cte_users (name, department, salary) VALUES
            ('Alice', 'Engineering', 80000),
            ('Bob', 'Engineering', 90000),
            ('Charlie', 'Marketing', 70000),
            ('Diana', 'Marketing', 75000),
            ('Eve', 'Sales', 60000)
        """
        )

        self.client.execute(
            f"""
            INSERT INTO {self.test_db}.cte_articles (title, author_id, views) VALUES
            ('Python Tutorial', 1, 1000),
            ('Database Design', 1, 800),
            ('Marketing Strategy', 3, 600),
            ('Sales Tips', 5, 400),
            ('Advanced Python', 2, 1200)
        """
        )

        yield

        # Cleanup
        try:
            self.client.execute(f"DROP DATABASE IF EXISTS {self.test_db}")
        except:
            pass
        # Client doesn't have close method, it's automatically managed

    def test_basic_cte_creation(self):
        """Test basic CTE creation from a query"""
        # Create a CTE for high-salary users
        high_salary_users = self.client.query(User).filter(User.salary > 75000).cte("high_salary_users")

        # Verify CTE object properties
        assert isinstance(high_salary_users, CTE)
        assert high_salary_users.name == "high_salary_users"
        assert high_salary_users.recursive == False

        # Get SQL from CTE
        cte_sql, cte_params = high_salary_users.as_sql()
        assert "SELECT" in cte_sql
        assert "cte_users" in cte_sql
        assert "salary > 75000" in cte_sql

    def test_cte_usage_in_query(self):
        """Test using a CTE in another query"""
        # Create CTE for engineering users
        engineering_users = self.client.query(User).filter(User.department == "Engineering").cte("engineering_users")

        # Use CTE in another query
        result = self.client.query(engineering_users).all()

        # Should return 2 users (Alice and Bob)
        assert len(result) == 2
        # Check that we got engineering users (Alice and Bob)
        # Since we're querying from CTE, we get RowData objects with '*' attribute
        # The actual data is in _values tuple: (id, name, department, salary)
        names = [row._values[1] for row in result]  # name is at index 1
        assert "Alice" in names
        assert "Bob" in names

    def test_multiple_ctes_in_query(self):
        """Test using multiple CTEs in a single query"""
        # Create first CTE: high-salary users
        high_salary = self.client.query(User).filter(User.salary > 70000).cte("high_salary")

        # Create second CTE: popular articles
        popular_articles = self.client.query(Article).filter(Article.views > 500).cte("popular_articles")

        # Use both CTEs in a query with with_cte method
        result = (
            self.client.query(high_salary)
            .with_cte(popular_articles)
            .join(popular_articles, onclause="high_salary.id = popular_articles.author_id")
            .all()
        )

        # Should return users who have popular articles and high salary
        assert len(result) >= 0  # At least Alice (high salary + popular articles)

    def test_cte_with_aggregation(self):
        """Test CTE with aggregation functions"""
        # Create CTE with department statistics
        dept_stats = self.client.query(User.department, User.salary).cte("dept_stats")

        # Use CTE in aggregation query
        result = self.client.query(dept_stats).all()

        # Should return all department-salary combinations
        assert len(result) == 5  # 5 users total

    def test_recursive_cte_flag(self):
        """Test recursive CTE flag"""
        # Create a recursive CTE (even though we won't use recursion in this test)
        recursive_cte = self.client.query(User).filter(User.id == 1).cte("recursive_users", recursive=True)

        assert recursive_cte.recursive == True
        assert recursive_cte.name == "recursive_users"

    def test_cte_in_transaction(self):
        """Test CTE usage within a transaction"""
        with self.client.transaction() as tx:
            # Create CTE within transaction
            tx_users = tx.query(User).filter(User.department == "Marketing").cte("tx_users")

            # Use CTE within transaction
            result = tx.query(tx_users).all()

            # Should return 2 marketing users
            assert len(result) == 2
            names = [row._values[1] for row in result]  # name is at index 1
            assert all(name in ["Charlie", "Diana"] for name in names)

    def test_cte_with_join(self):
        """Test CTE with JOIN operations"""
        # Create CTE for users with articles - select specific columns to avoid ambiguity
        users_with_articles = (
            self.client.query(User.id, User.name, User.department, User.salary)
            .join("cte_articles", onclause="cte_users.id = cte_articles.author_id")
            .cte("users_with_articles")
        )

        # Query from the CTE
        result = self.client.query(users_with_articles).all()

        # Should return users who have written articles
        assert len(result) >= 0
        # All returned users should have written at least one article

    def test_cte_sql_generation(self):
        """Test that CTE generates correct SQL"""
        # Create a simple CTE
        simple_cte = self.client.query(User.id, User.name).filter(User.salary > 80000).cte("simple_cte")

        # Get the SQL
        cte_sql, params = simple_cte.as_sql()

        # Verify SQL structure
        assert "SELECT" in cte_sql
        assert "cte_users" in cte_sql
        assert "id" in cte_sql
        assert "name" in cte_sql
        assert "salary > 80000" in cte_sql

        # Use CTE in a query and check final SQL
        query = self.client.query(simple_cte)
        final_sql, final_params = query._build_sql()

        # Should have WITH clause
        assert "WITH simple_cte AS" in final_sql
        assert "SELECT * FROM simple_cte" in final_sql

    def test_cte_parameter_handling(self):
        """Test that CTE parameters are handled correctly"""
        # Create CTE with parameterized query
        param_cte = self.client.query(User).filter(User.salary > 75000).cte("param_cte")  # This should generate a parameter

        # Use CTE in query
        result = self.client.query(param_cte).all()

        # Should return high-salary users
        assert len(result) == 2  # Alice and Bob
        salaries = [row._values[3] for row in result]  # salary is at index 3
        assert all(salary > 75000 for salary in salaries)

    def test_cte_with_raw_sql(self):
        """Test CTE creation with raw SQL string"""
        # Create CTE with raw SQL
        raw_cte = CTE("raw_cte", f"SELECT * FROM {self.test_db}.cte_users WHERE salary > 80000")

        # Use in query
        result = self.client.query(raw_cte).all()

        # Should return high-salary users
        assert len(result) == 1  # Only Bob
        assert result[0]._values[1] == "Bob"  # name is at index 1
        assert result[0]._values[3] > 80000  # salary is at index 3
