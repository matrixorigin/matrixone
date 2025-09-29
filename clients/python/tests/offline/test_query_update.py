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
Offline tests for query update functionality
"""

import pytest
import os
import sys
from unittest.mock import Mock, MagicMock

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone.orm import BaseMatrixOneQuery, MatrixOneQuery
from sqlalchemy import func


class MockUser:
    """Mock SQLAlchemy model for testing"""

    __tablename__ = "users"
    __name__ = "MockUser"  # Add __name__ attribute

    def __init__(self):
        self.id = Mock()
        self.name = Mock()
        self.email = Mock()
        self.age = Mock()
        self.login_count = Mock()
        self.last_login = Mock()
        self.status = Mock()


class TestQueryUpdate:
    """Test query update functionality"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_client.execute.return_value = Mock()
        self.mock_client.execute.return_value.rows = []

        # Create mock user model
        self.user_model = MockUser()

        # Create query instances
        self.base_query = BaseMatrixOneQuery(self.user_model, self.mock_client)
        self.matrixone_query = MatrixOneQuery(self.user_model, self.mock_client)

    def test_simple_update(self):
        """Test simple update with key-value pairs"""
        query = self.base_query.update(name="New Name", email="new@example.com")

        # Check that query type is set to UPDATE
        assert query._query_type == "UPDATE"

        # Check that update columns are set correctly
        assert "name = ?" in query._update_set_columns
        assert "email = ?" in query._update_set_columns

        # Check that update values are set correctly
        assert "New Name" in query._update_set_values
        assert "new@example.com" in query._update_set_values

    def test_update_with_filter(self):
        """Test update with filter conditions"""
        query = self.base_query.update(status="inactive").filter("age > ?", 65)

        # Check that query type is set to UPDATE
        assert query._query_type == "UPDATE"

        # Check that update columns are set
        assert "status = ?" in query._update_set_columns
        assert "inactive" in query._update_set_values

        # Check that filter conditions are set
        assert "age > 65" in query._where_conditions

    def test_update_with_sqlalchemy_expressions(self):
        """Test update with SQLAlchemy expressions"""
        # Mock SQLAlchemy expression
        mock_expression = Mock()
        mock_expression.compile.return_value = Mock()
        mock_expression.compile.return_value.__str__ = Mock(return_value="NOW()")

        query = self.base_query.update(last_login=mock_expression)

        # Check that query type is set to UPDATE
        assert query._query_type == "UPDATE"

        # Check that SQLAlchemy expression is handled correctly
        assert "last_login = NOW()" in query._update_set_columns

    def test_update_with_complex_expressions(self):
        """Test update with complex SQLAlchemy expressions"""
        # Mock complex expression
        mock_expression = Mock()
        mock_expression.compile.return_value = Mock()
        mock_expression.compile.return_value.__str__ = Mock(return_value="login_count + 1")

        query = self.base_query.update(login_count=mock_expression)

        # Check that query type is set to UPDATE
        assert query._query_type == "UPDATE"

        # Check that complex expression is handled correctly
        assert "login_count = login_count + 1" in query._update_set_columns

    def test_update_build_sql_simple(self):
        """Test building UPDATE SQL with simple values"""
        query = self.base_query.update(name="New Name", email="new@example.com").filter("id = ?", 1)

        sql, params = query._build_update_sql()

        # Check SQL structure
        assert "UPDATE mockuser SET" in sql
        assert "name = ?" in sql
        assert "email = ?" in sql
        assert "WHERE id = 1" in sql

        # Check parameters
        assert "New Name" in params
        assert "new@example.com" in params

    def test_update_build_sql_with_expressions(self):
        """Test building UPDATE SQL with SQLAlchemy expressions"""
        # Mock SQLAlchemy expression
        mock_expression = Mock()
        mock_compiled = Mock()
        mock_compiled.__str__ = Mock(return_value="NOW()")
        mock_expression.compile.return_value = mock_compiled

        query = self.base_query.update(last_login=mock_expression, status="active").filter("id = ?", 1)

        sql, params = query._build_update_sql()

        # Check SQL structure
        assert "UPDATE mockuser SET" in sql
        assert "last_login = NOW()" in sql
        assert "status = ?" in sql
        assert "WHERE id = 1" in sql

        # Check parameters
        assert "active" in params

    def test_update_execute(self):
        """Test executing UPDATE query"""
        query = self.base_query.update(name="New Name").filter("id = ?", 1)

        # Mock the _execute method
        query._execute = Mock(return_value=Mock())

        # Use MatrixOneQuery which has execute method
        matrixone_query = MatrixOneQuery(self.user_model, self.mock_client)
        matrixone_query._query_type = "UPDATE"
        matrixone_query._update_set_columns = ["name = ?"]
        matrixone_query._update_set_values = ["New Name"]
        matrixone_query._where_conditions = ["id = 1"]
        matrixone_query._where_params = []
        matrixone_query._table_name = "mockuser"
        matrixone_query._execute = Mock(return_value=Mock())

        result = matrixone_query.execute()

        # Check that _execute was called
        matrixone_query._execute.assert_called_once()

        # Check that _build_update_sql was called
        sql, params = matrixone_query._execute.call_args[0]
        assert "UPDATE mockuser SET" in sql
        assert "name = ?" in sql
        assert "WHERE id = 1" in sql

    def test_update_no_set_clauses_error(self):
        """Test that ValueError is raised when no SET clauses are provided"""
        query = self.base_query.filter("id = ?", 1)

        with pytest.raises(ValueError, match="No SET clauses provided for UPDATE"):
            query._build_update_sql()

    def test_update_with_multiple_filters(self):
        """Test update with multiple filter conditions"""
        query = self.base_query.update(status="inactive").filter("age > ?", 65).filter("last_login < ?", "2023-01-01")

        # Check that query type is set to UPDATE
        assert query._query_type == "UPDATE"

        # Check that update columns are set
        assert "status = ?" in query._update_set_columns
        assert "inactive" in query._update_set_values

        # Check that multiple filter conditions are set
        assert "age > 65" in query._where_conditions
        assert "last_login < '2023-01-01'" in query._where_conditions

    def test_update_with_sqlalchemy_quoted_columns(self):
        """Test update with SQLAlchemy quoted column names"""
        # Mock SQLAlchemy expression with quoted columns
        mock_expression = Mock()
        mock_expression.compile.return_value = Mock()
        mock_expression.compile.return_value.__str__ = Mock(return_value="COUNT('id')")

        query = self.base_query.update(user_count=mock_expression)

        # Check that quoted columns are fixed
        assert "user_count = COUNT(id)" in query._update_set_columns

    def test_matrixone_query_update(self):
        """Test MatrixOneQuery update functionality"""
        query = self.matrixone_query.update(name="New Name", email="new@example.com").filter("id = ?", 1)

        # Check that query type is set to UPDATE
        assert query._query_type == "UPDATE"

        # Check that update columns are set correctly
        assert "name = ?" in query._update_set_columns
        assert "email = ?" in query._update_set_columns

        # Check that update values are set correctly
        assert "New Name" in query._update_set_values
        assert "new@example.com" in query._update_set_values

    def test_update_method_chaining(self):
        """Test that update method returns self for chaining"""
        query = self.base_query.update(name="New Name")

        # Check that the same instance is returned
        assert query is self.base_query

        # Check that chaining works
        chained_query = query.filter("id = ?", 1).filter("status = ?", "active")

        assert chained_query is self.base_query
        assert query._query_type == "UPDATE"
        assert "id = 1" in query._where_conditions
        assert "status = 'active'" in query._where_conditions

    def test_update_with_none_values(self):
        """Test update with None values"""
        query = self.base_query.update(name=None, email="new@example.com")

        # Check that None values are handled correctly
        assert "name = ?" in query._update_set_columns
        assert "email = ?" in query._update_set_columns
        assert None in query._update_set_values
        assert "new@example.com" in query._update_set_values

    def test_update_with_numeric_values(self):
        """Test update with numeric values"""
        query = self.base_query.update(age=25, salary=50000.50)

        # Check that numeric values are handled correctly
        assert "age = ?" in query._update_set_columns
        assert "salary = ?" in query._update_set_columns
        assert 25 in query._update_set_values
        assert 50000.50 in query._update_set_values

    def test_update_with_boolean_values(self):
        """Test update with boolean values"""
        query = self.base_query.update(active=True, verified=False)

        # Check that boolean values are handled correctly
        assert "active = ?" in query._update_set_columns
        assert "verified = ?" in query._update_set_columns
        assert True in query._update_set_values
        assert False in query._update_set_values


if __name__ == "__main__":
    pytest.main([__file__])
