"""
Online tests for large vector dimensions.
Tests the ability to handle vectors with very large dimensions (65535).
"""

import pytest
import math
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base
from matrixone import Client
from matrixone.sqlalchemy_ext import Vectorf64, Vectorf32
from .test_config import online_config


@pytest.mark.online
class TestLargeVectorDimensions:
    """Test large vector dimensions (65535)."""

    @pytest.fixture(scope="class")
    def client(self):
        """Create MatrixOne client for testing."""
        client = Client()
        host, port, user, password, database = online_config.get_connection_params()
        client.connect(host=host, port=port, user=user, password=password, database=database)
        return client

    @pytest.fixture(scope="class")
    def engine(self, client):
        """Get SQLAlchemy engine from client."""
        return client.get_sqlalchemy_engine()

    @pytest.fixture(scope="function")
    def Base(self):
        """Create declarative base for ORM models."""
        return declarative_base()

    @pytest.fixture(scope="class")
    def Session(self, engine):
        """Create session maker for ORM operations."""
        return sessionmaker(bind=engine)

    def test_65535_dimension_vecf64_batch_insertion(self, engine, Base, Session):
        """Test batch inserting 65535-dimensional vecf64 vectors (10 rows) using ORM."""

        class LargeVectorf64Model(Base):
            __tablename__ = 'test_large_vecf64_batch_orm_f64'

            id = Column(Integer, primary_key=True)
            vector = Column(Vectorf64(dimension=65535))
            batch_id = Column(Integer)

            @classmethod
            def create(cls, engine):
                Base.metadata.create_all(engine, tables=[cls.__table__])
                print(f"Created table: {cls.__tablename__}")

            @classmethod
            def drop(cls, engine, checkfirst=False):
                from sqlalchemy import text

                with engine.begin() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {cls.__tablename__}"))
                print(f"Dropped table: {cls.__tablename__}")

        LargeVectorf64Model.drop(engine, checkfirst=True)
        LargeVectorf64Model.create(engine)

        # Batch insert 10 rows using ORM
        batch_size = 10
        session = Session()

        print(f"Starting batch insertion of {batch_size} rows using ORM...")

        try:
            for batch_id in range(batch_size):
                # Generate unique vector data for each row
                vector_data = []
                for i in range(65535):
                    # Use batch_id to make each vector unique: sin((i + batch_id*1000)/1000)
                    value = math.sin((i + batch_id * 1000) / 1000.0)
                    vector_data.append(value)

                # Create model instance
                model_instance = LargeVectorf64Model(vector=vector_data, batch_id=batch_id + 1)

                session.add(model_instance)
                print(f"Added row {batch_id + 1} to session")

            # Commit all changes at once
            session.commit()
            print(f"Committed {batch_size} rows to database")

            # Verify count using ORM
            count = session.query(LargeVectorf64Model).count()
            print(f"Total rows in table: {count}")
            assert count == batch_size, f"Expected {batch_size} rows, got {count}"

            # Verify each row individually using ORM
            for batch_id in range(1, batch_size + 1):
                model = session.query(LargeVectorf64Model).filter_by(batch_id=batch_id).first()

                assert model is not None, f"No data found for batch_id {batch_id}"
                assert model.id == batch_id, f"ID mismatch for batch_id {batch_id}: expected {batch_id}, got {model.id}"
                assert model.batch_id == batch_id, f"batch_id mismatch: expected {batch_id}, got {model.batch_id}"

                # Verify vector data matches expected pattern
                assert isinstance(model.vector, list), f"Expected list, got {type(model.vector)}"
                assert (
                    len(model.vector) == 65535
                ), f"Expected 65535 dimensions for batch_id {batch_id}, got {len(model.vector)}"

                # Check first few values match expected pattern
                for i in range(10):
                    expected = math.sin((i + (batch_id - 1) * 1000) / 1000.0)
                    actual = model.vector[i]
                    assert (
                        abs(expected - actual) < 1e-6
                    ), f"Value mismatch at index {i} for batch_id {batch_id}: expected {expected}, got {actual}"

                print(f"✅ Verified row {batch_id} with batch_id {batch_id}")

            # Test batch count query using ORM
            batch_counts = (
                session.query(
                    LargeVectorf64Model.batch_id,
                    session.query(LargeVectorf64Model)
                    .filter(LargeVectorf64Model.batch_id == LargeVectorf64Model.batch_id)
                    .count(),
                )
                .group_by(LargeVectorf64Model.batch_id)
                .all()
            )

            # Alternative simpler approach
            all_models = session.query(LargeVectorf64Model).order_by(LargeVectorf64Model.batch_id).all()

            assert len(all_models) == batch_size, f"Expected {batch_size} models, got {len(all_models)}"
            for i, model in enumerate(all_models):
                expected_batch_id = i + 1
                assert (
                    model.batch_id == expected_batch_id
                ), f"Batch ID mismatch at position {i}: expected {expected_batch_id}, got {model.batch_id}"

            print(f"✅ Batch insertion of {batch_size} rows successful!")
            print(f"✅ Count verification passed: {count} rows total")
            print(f"✅ Individual row verification passed for all {batch_size} rows")

        finally:
            session.close()
            # Clean up table
            Base.metadata.drop_all(engine)

    def test_65535_dimension_vecf32_batch_insertion(self, engine, Base, Session):
        """Test batch inserting 65535-dimensional vecf32 vectors (10 rows) using ORM."""

        class LargeVectorf32Model(Base):
            __tablename__ = 'test_large_vecf32_batch_orm_f32'

            id = Column(Integer, primary_key=True)
            vector = Column(Vectorf32(dimension=65535))
            batch_id = Column(Integer)

            @classmethod
            def create(cls, engine):
                Base.metadata.create_all(engine, tables=[cls.__table__])
                print(f"Created table: {cls.__tablename__}")

            @classmethod
            def drop(cls, engine, checkfirst=False):
                from sqlalchemy import text

                with engine.begin() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {cls.__tablename__}"))
                print(f"Dropped table: {cls.__tablename__}")

        LargeVectorf32Model.drop(engine, checkfirst=True)
        LargeVectorf32Model.create(engine)

        # Batch insert 10 rows using ORM
        batch_size = 10
        session = Session()

        print(f"Starting vecf32 batch insertion of {batch_size} rows using ORM...")

        try:
            for batch_id in range(batch_size):
                # Generate unique vector data for each row
                vector_data = []
                for i in range(65535):
                    # Use batch_id to make each vector unique: cos((i + batch_id*1000)/1000)
                    value = math.cos((i + batch_id * 1000) / 1000.0)
                    vector_data.append(value)

                # Create model instance
                model_instance = LargeVectorf32Model(vector=vector_data, batch_id=batch_id + 1)

                session.add(model_instance)
                print(f"Added vecf32 row {batch_id + 1} to session")

            # Commit all changes at once
            session.commit()
            print(f"Committed {batch_size} vecf32 rows to database")

            # Verify count using ORM
            count = session.query(LargeVectorf32Model).count()
            print(f"Total vecf32 rows in table: {count}")
            assert count == batch_size, f"Expected {batch_size} rows, got {count}"

            # Verify each row individually using ORM
            for batch_id in range(1, batch_size + 1):
                model = session.query(LargeVectorf32Model).filter_by(batch_id=batch_id).first()

                assert model is not None, f"No data found for batch_id {batch_id}"
                assert model.id == batch_id, f"ID mismatch for batch_id {batch_id}: expected {batch_id}, got {model.id}"
                assert model.batch_id == batch_id, f"batch_id mismatch: expected {batch_id}, got {model.batch_id}"

                # Verify vector data matches expected pattern
                assert isinstance(model.vector, list), f"Expected list, got {type(model.vector)}"
                assert (
                    len(model.vector) == 65535
                ), f"Expected 65535 dimensions for batch_id {batch_id}, got {len(model.vector)}"

                # Check first few values match expected pattern
                for i in range(10):
                    expected = math.cos((i + (batch_id - 1) * 1000) / 1000.0)
                    actual = model.vector[i]
                    assert (
                        abs(expected - actual) < 1e-6
                    ), f"Value mismatch at index {i} for batch_id {batch_id}: expected {expected}, got {actual}"

                print(f"✅ Verified vecf32 row {batch_id} with batch_id {batch_id}")

            # Test batch count query using ORM
            all_models = session.query(LargeVectorf32Model).order_by(LargeVectorf32Model.batch_id).all()

            assert len(all_models) == batch_size, f"Expected {batch_size} models, got {len(all_models)}"
            for i, model in enumerate(all_models):
                expected_batch_id = i + 1
                assert (
                    model.batch_id == expected_batch_id
                ), f"Batch ID mismatch at position {i}: expected {expected_batch_id}, got {model.batch_id}"

            print(f"✅ vecf32 Batch insertion of {batch_size} rows successful!")
            print(f"✅ vecf32 Count verification passed: {count} rows total")
            print(f"✅ vecf32 Individual row verification passed for all {batch_size} rows")

        finally:
            session.close()
            # Clean up table
            Base.metadata.drop_all(engine)

    def test_vector_string_length_limits(self, client):
        """Test the actual string length limits for large vectors."""
        # Test different dimensions to see where limits might be
        test_dimensions = [1000, 10000, 65535]

        for dim in test_dimensions:
            print(f"\n--- Testing dimension {dim} ---")

            # Generate vector data
            vector_data = [math.sin(i / 1000.0) for i in range(dim)]

            # Convert to MatrixOne vector format
            vector_string = "[" + ",".join(map(str, vector_data)) + "]"

            print(f"Dimension {dim}: String length = {len(vector_string)}")
            print(f"Average chars per dimension: {len(vector_string) / dim:.2f}")

            # Check if string is within reasonable limits
            if len(vector_string) > 1000000:  # 1MB
                print(f"⚠️  Warning: String length {len(vector_string)} is very large for dimension {dim}")
            else:
                print(f"✅ String length {len(vector_string)} is reasonable for dimension {dim}")
