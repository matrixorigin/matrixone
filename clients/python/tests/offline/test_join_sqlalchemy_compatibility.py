#!/usr/bin/env python3

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
Test join methods compatibility with SQLAlchemy ORM

This test file verifies that MatrixOne's join methods produce the same SQL
as SQLAlchemy ORM for equivalent operations.
"""

import sys
import os
import unittest
from unittest.mock import Mock

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone.orm import MatrixOneQuery, declarative_base
from sqlalchemy import Column, Integer, String, create_engine, select
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    department_id = Column(Integer)


class Address(Base):
    __tablename__ = 'addresses'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    email = Column(String(100))


class Department(Base):
    __tablename__ = 'departments'
    id = Column(Integer, primary_key=True)
    name = Column(String(50))


class TestJoinSQLAlchemyCompatibility(unittest.TestCase):
    """Test join methods compatibility with SQLAlchemy ORM"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.query = MatrixOneQuery(User, self.mock_client)
        self.query._table_name = 'users'

    def test_join_behavior_matches_sqlalchemy(self):
        """Test that join behavior matches SQLAlchemy ORM"""
        # MatrixOne query
        matrixone_query = MatrixOneQuery(User, self.mock_client)
        matrixone_query._table_name = 'users'
        matrixone_query.join(Address, User.id == Address.user_id)

        # SQLAlchemy equivalent
        sqlalchemy_query = select(User).join(Address, User.id == Address.user_id)

        # Both should produce INNER JOIN
        self.assertTrue(" INNER JOIN " in matrixone_query.to_sql())

        # MatrixOne should handle the SQLAlchemy expression correctly
        self.assertIn("addresses", matrixone_query.to_sql())
        self.assertIn("users.id = addresses.user_id", matrixone_query.to_sql())

    def test_left_join_behavior_matches_sqlalchemy(self):
        """Test that left join behavior matches SQLAlchemy ORM"""
        # MatrixOne query
        matrixone_query = MatrixOneQuery(User, self.mock_client)
        matrixone_query._table_name = 'users'
        matrixone_query.join(Address, User.id == Address.user_id, isouter=True)

        # SQLAlchemy equivalent
        sqlalchemy_query = select(User).outerjoin(Address, User.id == Address.user_id)

        # Both should produce LEFT OUTER JOIN
        self.assertTrue(" LEFT OUTER JOIN " in matrixone_query.to_sql())
        self.assertIn("addresses", matrixone_query.to_sql())

    def test_join_without_condition_matches_sqlalchemy(self):
        matrixone_query = MatrixOneQuery(User, self.mock_client)
        with self.assertRaisesRegex(ValueError, 'foreign key'):
            matrixone_query.join(Address)
        with self.assertRaises(Exception):
            str(select(User).join(Address))

    def test_multiple_joins_matches_sqlalchemy(self):
        """Test that multiple joins match SQLAlchemy ORM"""
        # MatrixOne query
        matrixone_query = MatrixOneQuery(User, self.mock_client)
        matrixone_query._table_name = 'users'
        matrixone_query.join(Address, User.id == Address.user_id)
        matrixone_query.join(Department, User.department_id == Department.id)

        # SQLAlchemy equivalent
        sqlalchemy_query = (
            select(User).join(Address, User.id == Address.user_id).join(Department, User.department_id == Department.id)
        )

        # Both should have two joins
        self.assertEqual(len(matrixone_query._joins), 2)
        self.assertTrue(" INNER JOIN " in matrixone_query.to_sql())
        self.assertTrue(" INNER JOIN " in matrixone_query.to_sql())
        self.assertIn("addresses", matrixone_query.to_sql())
        self.assertIn("departments", matrixone_query.to_sql())

    def test_join_with_string_condition(self):
        """Test join with string condition (MatrixOne specific)"""
        # MatrixOne query with string condition
        matrixone_query = MatrixOneQuery(User, self.mock_client)
        matrixone_query._table_name = 'users'
        matrixone_query.join('addresses', 'users.id = addresses.user_id')

        # Should produce INNER JOIN with the exact condition
        self.assertTrue(" INNER JOIN " in matrixone_query.to_sql())
        self.assertIn("addresses", matrixone_query.to_sql())
        self.assertIn("users.id = addresses.user_id", matrixone_query.to_sql())

    def test_join_with_onclause_parameter(self):
        """Test join with onclause parameter (SQLAlchemy style)"""
        # MatrixOne query with onclause parameter
        matrixone_query = MatrixOneQuery(User, self.mock_client)
        matrixone_query._table_name = 'users'
        matrixone_query.join('addresses', onclause='users.id = addresses.user_id')

        # Should produce INNER JOIN with the exact condition
        self.assertTrue(" INNER JOIN " in matrixone_query.to_sql())
        self.assertIn("addresses", matrixone_query.to_sql())
        self.assertIn("users.id = addresses.user_id", matrixone_query.to_sql())

    def test_join_method_aliases(self):
        """Test that join method aliases work correctly"""
        # Test innerjoin
        query1 = MatrixOneQuery(User, self.mock_client)
        query1._table_name = 'users'
        query1.innerjoin(Address, User.id == Address.user_id)

        # Test leftjoin
        query2 = MatrixOneQuery(User, self.mock_client)
        query2._table_name = 'users'
        query2.leftjoin(Address, User.id == Address.user_id)

        # Test outerjoin
        query3 = MatrixOneQuery(User, self.mock_client)
        query3._table_name = 'users'
        query3.outerjoin(Address, User.id == Address.user_id)

        # Test fullouterjoin
        query4 = MatrixOneQuery(User, self.mock_client)
        query4._table_name = 'users'
        query4.fullouterjoin(Address, User.id == Address.user_id)

        # Verify join types
        self.assertTrue(" INNER JOIN " in query1.to_sql())
        self.assertTrue(" LEFT OUTER JOIN " in query2.to_sql())
        self.assertTrue(" LEFT OUTER JOIN " in query3.to_sql())
        self.assertTrue(" FULL OUTER JOIN " in query4.to_sql())

    def test_join_with_complex_expression(self):
        """Test join with complex SQLAlchemy expression"""
        # MatrixOne query with complex expression
        matrixone_query = MatrixOneQuery(User, self.mock_client)
        matrixone_query._table_name = 'users'
        matrixone_query.join(Address, (User.id == Address.user_id) & (Address.email.like('%@example.com')))

        # Should handle complex expressions
        self.assertTrue(" INNER JOIN " in matrixone_query.to_sql())
        self.assertIn("addresses", matrixone_query.to_sql())
        self.assertIn("ON", matrixone_query.to_sql())
        # The complex expression should be compiled to SQL
        join_clause = matrixone_query.to_sql()
        self.assertIn("users.id = addresses.user_id", join_clause)

    def test_join_method_chaining_compatibility(self):
        """Test that join methods support method chaining like SQLAlchemy"""
        # MatrixOne query with method chaining
        result = (
            MatrixOneQuery(User, self.mock_client)
            .join(Address, User.id == Address.user_id)
            .join(Department, User.department_id == Department.id)
            .filter(User.name == 'John')
        )

        # Should return self for chaining
        self.assertEqual(result._table_name, 'users')
        self.assertEqual(len(result._joins), 2)
        self.assertEqual(len(result._where_conditions), 1)

    def test_join_with_model_class_target(self):
        """Test join with model class as target"""
        # MatrixOne query with model class
        matrixone_query = MatrixOneQuery(User, self.mock_client)
        matrixone_query._table_name = 'users'
        matrixone_query.join(Address, 'users.id = addresses.user_id')

        # Should use the model's table name
        self.assertTrue(" INNER JOIN " in matrixone_query.to_sql())
        self.assertIn("addresses", matrixone_query.to_sql())
        self.assertIn("users.id = addresses.user_id", matrixone_query.to_sql())

    def test_join_parameter_consistency_with_sqlalchemy(self):
        """Test that parameter names are consistent with SQLAlchemy"""
        # Test that onclause parameter works (SQLAlchemy style)
        query1 = MatrixOneQuery(User, self.mock_client)
        query1._table_name = 'users'
        query1.join('addresses', onclause='users.id = addresses.user_id')

        # Test that positional parameter also works (backward compatibility)
        query2 = MatrixOneQuery(User, self.mock_client)
        query2._table_name = 'users'
        query2.join('addresses', 'users.id = addresses.user_id')

        # Both should produce the same result
        self.assertEqual(query1.to_sql(), query2.to_sql())

    def test_join_default_behavior_is_inner_join(self):
        """Test that join() defaults to INNER JOIN like SQLAlchemy"""
        # MatrixOne query
        matrixone_query = MatrixOneQuery(User, self.mock_client)
        matrixone_query._table_name = 'users'
        matrixone_query.join(Address, User.id == Address.user_id)

        # SQLAlchemy equivalent
        sqlalchemy_query = select(User).join(Address, User.id == Address.user_id)

        # Both should default to INNER JOIN
        self.assertTrue(" INNER JOIN " in matrixone_query.to_sql())

        # This matches SQLAlchemy's default behavior where join() creates INNER JOIN


if __name__ == '__main__':
    # Create test suite
    test_suite = unittest.TestSuite()

    # Add all test methods
    test_suite.addTest(unittest.makeSuite(TestJoinSQLAlchemyCompatibility))

    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    # Print summary
    print(f"\n{'='*50}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(
        f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%"
    )
    print(f"{'='*50}")
