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
Test cases for SQLAlchemy-style query methods (all, one, first, scalar, one_or_none)
"""

import pytest
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.exc import NoResultFound, MultipleResultsFound

from matrixone import Client
from matrixone.orm import declarative_base
from .test_config import online_config

Base = declarative_base()


class QueryTestUser(Base):
    __tablename__ = 'test_query_users'
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    age = Column(Integer)


class TestQueryMethods:
    """Test SQLAlchemy-style query methods"""

    @pytest.fixture(autouse=True)
    def setup_and_cleanup(self):
        """Setup test data and cleanup after tests"""
        # Setup
        self.client = Client()
        host, port, user, password, database = online_config.get_connection_params()
        self.client.connect(host=host, port=port, user=user, password=password, database=database)

        # Create table (with checkfirst to avoid errors if table exists)
        try:
            self.client.create_table(QueryTestUser)
        except Exception:
            # Table might already exist, try to drop and recreate
            try:
                self.client.drop_table(QueryTestUser)
                self.client.create_table(QueryTestUser)
            except Exception:
                pass  # Continue with existing table

        # Insert test data
        users_data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
        ]
        for user_data in users_data:
            self.client.insert(QueryTestUser.__tablename__, user_data)

        yield

        # Cleanup
        try:
            self.client.drop_table(QueryTestUser)
        except:
            pass
        self.client.disconnect()

    def test_all_method(self):
        """Test all() method returns all results"""
        results = self.client.query(QueryTestUser).all()
        assert len(results) == 3
        assert all(isinstance(user, QueryTestUser) for user in results)
        assert results[0].name == 'Alice'
        assert results[1].name == 'Bob'
        assert results[2].name == 'Charlie'

    def test_first_method(self):
        """Test first() method returns first result or None"""
        # Test with results
        result = self.client.query(QueryTestUser).first()
        assert isinstance(result, QueryTestUser)
        assert result.name == 'Alice'

        # Test with no results
        result = self.client.query(QueryTestUser).filter(QueryTestUser.id == 999).first()
        assert result is None

    def test_one_method_success(self):
        """Test one() method with exactly one result"""
        result = self.client.query(QueryTestUser).filter(QueryTestUser.id == 1).one()
        assert isinstance(result, QueryTestUser)
        assert result.name == 'Alice'

    def test_one_method_no_results(self):
        """Test one() method with no results raises NoResultFound"""
        with pytest.raises(NoResultFound):
            self.client.query(QueryTestUser).filter(QueryTestUser.id == 999).one()

    def test_one_method_multiple_results(self):
        """Test one() method with multiple results raises MultipleResultsFound"""
        with pytest.raises(MultipleResultsFound):
            self.client.query(QueryTestUser).filter(QueryTestUser.age > 20).one()

    def test_one_or_none_method_success(self):
        """Test one_or_none() method with exactly one result"""
        result = self.client.query(QueryTestUser).filter(QueryTestUser.id == 1).one_or_none()
        assert isinstance(result, QueryTestUser)
        assert result.name == 'Alice'

    def test_one_or_none_method_no_results(self):
        """Test one_or_none() method with no results returns None"""
        result = self.client.query(QueryTestUser).filter(QueryTestUser.id == 999).one_or_none()
        assert result is None

    def test_one_or_none_method_multiple_results(self):
        """Test one_or_none() method with multiple results raises MultipleResultsFound"""
        with pytest.raises(MultipleResultsFound):
            self.client.query(QueryTestUser).filter(QueryTestUser.age > 20).one_or_none()

    def test_scalar_method_with_model(self):
        """Test scalar() method returns first column value from model"""
        # Test with primary key (first column)
        result = self.client.query(QueryTestUser).filter(QueryTestUser.id == 1).scalar()
        assert result == 1

        # Test with no results
        result = self.client.query(QueryTestUser).filter(QueryTestUser.id == 999).scalar()
        assert result is None

    def test_scalar_method_with_select_columns(self):
        """Test scalar() method with custom select columns"""
        # Select specific column
        result = self.client.query(QueryTestUser).select(QueryTestUser.name).filter(QueryTestUser.id == 1).scalar()
        assert result == 'Alice'

        # Select count - use count() method directly, not with select
        result = self.client.query(QueryTestUser).count()
        assert result == 3

    def test_scalar_method_with_aggregate(self):
        """Test scalar() method with aggregate functions"""
        from sqlalchemy import func

        # Count all users
        result = self.client.query(QueryTestUser).select(func.count(QueryTestUser.id)).scalar()
        assert result == 3

        # Average age
        result = self.client.query(QueryTestUser).select(func.avg(QueryTestUser.age)).scalar()
        assert result == 30.0  # (25 + 30 + 35) / 3

    def test_methods_with_filters(self):
        """Test query methods work with various filters"""
        # Test with multiple filters - use AND to combine conditions
        from sqlalchemy import and_

        result = self.client.query(QueryTestUser).filter(and_(QueryTestUser.age > 25, QueryTestUser.name.like('B%'))).one()
        assert result.name == 'Bob'
        assert result.age == 30

    def test_methods_with_ordering(self):
        """Test query methods work with ordering"""
        # Test first() with ordering
        result = self.client.query(QueryTestUser).order_by(QueryTestUser.age.desc()).first()
        assert result.name == 'Charlie'
        assert result.age == 35

        # Test scalar() with ordering
        result = self.client.query(QueryTestUser).select(QueryTestUser.name).order_by(QueryTestUser.age.desc()).scalar()
        assert result == 'Charlie'

    def test_methods_with_limit(self):
        """Test query methods work with limit"""
        # Test all() with limit
        results = self.client.query(QueryTestUser).limit(2).all()
        assert len(results) == 2

        # Test first() with limit (should still return only one)
        result = self.client.query(QueryTestUser).limit(2).first()
        assert isinstance(result, QueryTestUser)

    def test_count_method(self):
        """Test count() method"""
        # Count all
        count = self.client.query(QueryTestUser).count()
        assert count == 3

        # Count with filter
        count = self.client.query(QueryTestUser).filter(QueryTestUser.age > 25).count()
        assert count == 2

    def test_methods_consistency(self):
        """Test that methods are consistent with each other"""
        # Test that first() and all()[0] return same result
        first_result = self.client.query(QueryTestUser).order_by(QueryTestUser.id).first()
        all_results = self.client.query(QueryTestUser).order_by(QueryTestUser.id).all()
        assert first_result.id == all_results[0].id

        # Test that one() and one_or_none() return same result when exactly one exists
        one_result = self.client.query(QueryTestUser).filter(QueryTestUser.id == 1).one()
        one_or_none_result = self.client.query(QueryTestUser).filter(QueryTestUser.id == 1).one_or_none()
        assert one_result.id == one_or_none_result.id

    def test_error_messages(self):
        """Test that error messages are informative"""
        # Test NoResultFound message
        with pytest.raises(NoResultFound, match="No row was found for one\\(\\)"):
            self.client.query(QueryTestUser).filter(QueryTestUser.id == 999).one()

        # Test MultipleResultsFound message
        with pytest.raises(MultipleResultsFound, match="Multiple rows were found for one_or_none\\(\\)"):
            self.client.query(QueryTestUser).filter(QueryTestUser.age > 20).one_or_none()


if __name__ == '__main__':
    pytest.main([__file__])
