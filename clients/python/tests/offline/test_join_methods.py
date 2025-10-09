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
Test join methods alignment with SQLAlchemy ORM

This test file verifies that MatrixOne's join methods behave exactly like SQLAlchemy ORM.
"""

import sys
import os
import unittest
from unittest.mock import Mock

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone.orm import BaseMatrixOneQuery, MatrixOneQuery, declarative_base
from sqlalchemy import Column, Integer, String, create_engine

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


class TestJoinMethods(unittest.TestCase):
    """Test join methods alignment with SQLAlchemy ORM"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.query = MatrixOneQuery(User, self.mock_client)
        self.query._table_name = 'users'

    def test_join_default_inner_join(self):
        """Test that join() defaults to INNER JOIN"""
        self.query.join(Address, User.id == Address.user_id)

        self.assertEqual(len(self.query._joins), 1)
        self.assertTrue(self.query._joins[0].startswith("INNER JOIN"))
        self.assertIn("addresses", self.query._joins[0])
        self.assertIn("id = user_id", self.query._joins[0])

    def test_join_with_string_condition(self):
        """Test join with string condition"""
        self.query.join('addresses', 'users.id = addresses.user_id')

        self.assertEqual(len(self.query._joins), 1)
        self.assertTrue(self.query._joins[0].startswith("INNER JOIN"))
        self.assertIn("addresses", self.query._joins[0])
        self.assertIn("users.id = addresses.user_id", self.query._joins[0])

    def test_join_with_onclause_parameter(self):
        """Test join with onclause parameter"""
        self.query.join('addresses', onclause='users.id = addresses.user_id')

        self.assertEqual(len(self.query._joins), 1)
        self.assertTrue(self.query._joins[0].startswith("INNER JOIN"))
        self.assertIn("addresses", self.query._joins[0])
        self.assertIn("users.id = addresses.user_id", self.query._joins[0])

    def test_join_with_sqlalchemy_expression(self):
        """Test join with SQLAlchemy expression"""
        self.query.join(Address, User.id == Address.user_id)

        self.assertEqual(len(self.query._joins), 1)
        self.assertTrue(self.query._joins[0].startswith("INNER JOIN"))
        self.assertIn("addresses", self.query._joins[0])
        # SQLAlchemy expressions get compiled and table prefixes are removed
        self.assertIn("id = user_id", self.query._joins[0])

    def test_join_with_model_class_target(self):
        """Test join with model class as target"""
        self.query.join(Address, 'users.id = addresses.user_id')

        self.assertEqual(len(self.query._joins), 1)
        self.assertTrue(self.query._joins[0].startswith("INNER JOIN"))
        self.assertIn("addresses", self.query._joins[0])

    def test_join_without_onclause(self):
        """Test join without onclause (should still work)"""
        self.query.join(Address)

        self.assertEqual(len(self.query._joins), 1)
        self.assertTrue(self.query._joins[0].startswith("INNER JOIN"))
        self.assertIn("addresses", self.query._joins[0])
        self.assertNotIn("ON", self.query._joins[0])

    def test_join_with_isouter_true(self):
        """Test join with isouter=True creates LEFT OUTER JOIN"""
        self.query.join(Address, User.id == Address.user_id, isouter=True)

        self.assertEqual(len(self.query._joins), 1)
        self.assertTrue(self.query._joins[0].startswith("LEFT OUTER JOIN"))
        self.assertIn("addresses", self.query._joins[0])

    def test_join_with_full_true(self):
        """Test join with full=True creates FULL OUTER JOIN"""
        self.query.join(Address, User.id == Address.user_id, full=True)

        self.assertEqual(len(self.query._joins), 1)
        self.assertTrue(self.query._joins[0].startswith("FULL OUTER JOIN"))
        self.assertIn("addresses", self.query._joins[0])

    def test_innerjoin_method(self):
        """Test innerjoin method"""
        self.query.innerjoin(Address, User.id == Address.user_id)

        self.assertEqual(len(self.query._joins), 1)
        self.assertTrue(self.query._joins[0].startswith("INNER JOIN"))
        self.assertIn("addresses", self.query._joins[0])

    def test_leftjoin_method(self):
        """Test leftjoin method"""
        self.query.leftjoin(Address, User.id == Address.user_id)

        self.assertEqual(len(self.query._joins), 1)
        self.assertTrue(self.query._joins[0].startswith("LEFT OUTER JOIN"))
        self.assertIn("addresses", self.query._joins[0])

    def test_outerjoin_method(self):
        """Test outerjoin method (should be same as leftjoin)"""
        self.query.outerjoin(Address, User.id == Address.user_id)

        self.assertEqual(len(self.query._joins), 1)
        self.assertTrue(self.query._joins[0].startswith("LEFT OUTER JOIN"))
        self.assertIn("addresses", self.query._joins[0])

    def test_fullouterjoin_method(self):
        """Test fullouterjoin method"""
        self.query.fullouterjoin(Address, User.id == Address.user_id)

        self.assertEqual(len(self.query._joins), 1)
        self.assertTrue(self.query._joins[0].startswith("FULL OUTER JOIN"))
        self.assertIn("addresses", self.query._joins[0])

    def test_multiple_joins(self):
        """Test multiple joins in one query"""
        self.query.join(Address, User.id == Address.user_id)
        self.query.join(Department, User.department_id == Department.id)

        self.assertEqual(len(self.query._joins), 2)
        self.assertTrue(self.query._joins[0].startswith("INNER JOIN"))
        self.assertTrue(self.query._joins[1].startswith("INNER JOIN"))
        self.assertIn("addresses", self.query._joins[0])
        self.assertIn("departments", self.query._joins[1])

    def test_join_method_chaining(self):
        """Test that join methods support method chaining"""
        result = (
            self.query.join(Address, User.id == Address.user_id)
            .join(Department, User.department_id == Department.id)
            .filter(User.name == 'John')
        )

        self.assertEqual(result, self.query)  # Should return self for chaining
        self.assertEqual(len(self.query._joins), 2)
        self.assertEqual(len(self.query._where_conditions), 1)

    def test_join_with_cte(self):
        """Test join with CTE object"""
        # Create a CTE
        cte = self.query.filter(User.department_id == 1).cte("engineering_users")

        # Create new query and join with CTE
        new_query = MatrixOneQuery(User, self.mock_client)
        new_query._table_name = 'users'
        new_query.join(cte, 'users.id = engineering_users.id')

        self.assertEqual(len(new_query._joins), 1)
        self.assertTrue(new_query._joins[0].startswith("INNER JOIN"))
        self.assertIn("engineering_users", new_query._joins[0])

    def test_join_with_string_table_name(self):
        """Test join with string table name"""
        self.query.join('custom_table', 'users.id = custom_table.user_id')

        self.assertEqual(len(self.query._joins), 1)
        self.assertTrue(self.query._joins[0].startswith("INNER JOIN"))
        self.assertIn("custom_table", self.query._joins[0])
        self.assertIn("users.id = custom_table.user_id", self.query._joins[0])

    def test_join_sqlalchemy_expression_compilation(self):
        """Test that SQLAlchemy expressions are properly compiled"""
        # Test with complex expression
        self.query.join(Address, (User.id == Address.user_id) & (Address.email.like('%@example.com')))

        self.assertEqual(len(self.query._joins), 1)
        join_clause = self.query._joins[0]
        self.assertTrue(join_clause.startswith("INNER JOIN"))
        self.assertIn("addresses", join_clause)
        # The complex expression should be compiled to SQL
        self.assertIn("ON", join_clause)

    def test_join_parameter_consistency(self):
        """Test that all join methods use consistent parameter names"""
        # Test that onclause parameter works consistently across all methods
        methods_to_test = [
            ('join', lambda q: q.join('addresses', onclause='users.id = addresses.user_id')),
            (
                'innerjoin',
                lambda q: q.innerjoin('addresses', onclause='users.id = addresses.user_id'),
            ),
            (
                'leftjoin',
                lambda q: q.leftjoin('addresses', onclause='users.id = addresses.user_id'),
            ),
            (
                'outerjoin',
                lambda q: q.outerjoin('addresses', onclause='users.id = addresses.user_id'),
            ),
            (
                'fullouterjoin',
                lambda q: q.fullouterjoin('addresses', onclause='users.id = addresses.user_id'),
            ),
        ]

        for method_name, method_call in methods_to_test:
            with self.subTest(method=method_name):
                query = MatrixOneQuery(User, self.mock_client)
                query._table_name = 'users'

                result = method_call(query)

                # Should return self for chaining
                self.assertEqual(result, query)
                # Should have one join
                self.assertEqual(len(query._joins), 1)
                # Should contain the condition
                self.assertIn("users.id = addresses.user_id", query._joins[0])


if __name__ == '__main__':
    # Create test suite
    test_suite = unittest.TestSuite()

    # Add all test methods
    test_suite.addTest(unittest.makeSuite(TestJoinMethods))

    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    # Print summary
    print(f"\n{'='*50}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    print(f"{'='*50}")
