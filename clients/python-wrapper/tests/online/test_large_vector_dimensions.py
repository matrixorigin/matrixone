"""
Online tests for large vector dimensions.
Tests the ability to handle vectors with very large dimensions (65535).
"""

import pytest
import math
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer
from sqlalchemy.schema import CreateTable
from matrixone.sqlalchemy_ext import Vectorf64, MatrixOneDialect


@pytest.mark.online
class TestLargeVectorDimensions:
    """Test large vector dimensions (65535)."""

    @pytest.fixture(scope="class")
    def engine(self):
        """Create engine for testing."""
        connection_string = "mysql+pymysql://root:111@localhost:6001/test"
        engine = create_engine(connection_string)
        # Replace the dialect with our MatrixOne dialect but preserve dbapi
        original_dbapi = engine.dialect.dbapi
        engine.dialect = MatrixOneDialect()
        engine.dialect.dbapi = original_dbapi
        return engine

    def test_65535_dimension_vecf64_batch_insertion(self, engine):
        """Test batch inserting 65535-dimensional vecf64 vectors (10 rows)."""
        # Create table with 65535-dimensional vecf64 column
        metadata = MetaData()
        table = Table(
            'test_large_vecf64_batch',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('vector', Vectorf64(dimension=65535)),
            Column('batch_id', Integer)
        )
        
        # Create table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_large_vecf64_batch"))
            
            # Generate CREATE TABLE SQL
            create_sql = str(CreateTable(table).compile(
                dialect=MatrixOneDialect(), 
                compile_kwargs={"literal_binds": True}
            ))
            print(f"Creating table with SQL: {create_sql}")
            conn.execute(text(create_sql))
            
            # Batch insert 10 rows
            batch_size = 10
            vf64 = Vectorf64(dimension=65535)
            processor = vf64.process_bind_param
            
            print(f"Starting batch insertion of {batch_size} rows...")
            
            for batch_id in range(batch_size):
                # Generate unique vector data for each row
                vector_data = []
                for i in range(65535):
                    # Use batch_id to make each vector unique: sin((i + batch_id*1000)/1000)
                    value = math.sin((i + batch_id * 1000) / 1000.0)
                    vector_data.append(value)
                
                # Convert to string
                vector_string = processor(vector_data, engine.dialect)
                
                # Insert the vector data
                insert_sql = text("INSERT INTO test_large_vecf64_batch (vector, batch_id) VALUES (:vector, :batch_id)")
                result = conn.execute(insert_sql, {"vector": vector_string, "batch_id": batch_id + 1})
                
                print(f"Inserted row {batch_id + 1}, affected rows: {result.rowcount}")
            
            # Verify count
            count_sql = text("SELECT COUNT(*) FROM test_large_vecf64_batch")
            result = conn.execute(count_sql)
            count = result.fetchone()[0]
            print(f"Total rows in table: {count}")
            assert count == batch_size, f"Expected {batch_size} rows, got {count}"
            
            # Verify each row individually
            for batch_id in range(1, batch_size + 1):
                select_sql = text("SELECT id, vector, batch_id FROM test_large_vecf64_batch WHERE batch_id = :batch_id")
                result = conn.execute(select_sql, {"batch_id": batch_id})
                row = result.fetchone()
                
                assert row is not None, f"No data found for batch_id {batch_id}"
                assert row[0] == batch_id, f"ID mismatch for batch_id {batch_id}: expected {batch_id}, got {row[0]}"
                assert row[2] == batch_id, f"batch_id mismatch: expected {batch_id}, got {row[2]}"
                
                # Verify vector data matches expected pattern
                returned_vector_str = row[1]
                clean_str = returned_vector_str.strip("[]")
                returned_values = [float(x.strip()) for x in clean_str.split(",")]
                
                assert len(returned_values) == 65535, f"Expected 65535 dimensions for batch_id {batch_id}, got {len(returned_values)}"
                
                # Check first few values match expected pattern
                for i in range(10):
                    expected = math.sin((i + (batch_id - 1) * 1000) / 1000.0)
                    actual = returned_values[i]
                    assert abs(expected - actual) < 1e-6, f"Value mismatch at index {i} for batch_id {batch_id}: expected {expected}, got {actual}"
                
                print(f"✅ Verified row {batch_id} with batch_id {batch_id}")
            
            # Test batch count query
            batch_count_sql = text("SELECT batch_id, COUNT(*) as count FROM test_large_vecf64_batch GROUP BY batch_id ORDER BY batch_id")
            result = conn.execute(batch_count_sql)
            rows = result.fetchall()
            
            assert len(rows) == batch_size, f"Expected {batch_size} batch groups, got {len(rows)}"
            for i, (batch_id, count) in enumerate(rows):
                assert batch_id == i + 1, f"Batch ID mismatch at position {i}: expected {i + 1}, got {batch_id}"
                assert count == 1, f"Expected 1 row per batch, got {count} for batch_id {batch_id}"
            
            print(f"✅ Batch insertion of {batch_size} rows successful!")
            print(f"✅ Count verification passed: {count} rows total")
            print(f"✅ Individual row verification passed for all {batch_size} rows")

    def test_65535_dimension_vecf32_batch_insertion(self, engine):
        """Test batch inserting 65535-dimensional vecf32 vectors (10 rows)."""
        # Create table with 65535-dimensional vecf32 column
        from matrixone.sqlalchemy_ext import Vectorf32
        
        metadata = MetaData()
        table = Table(
            'test_large_vecf32_batch',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('vector', Vectorf32(dimension=65535)),
            Column('batch_id', Integer)
        )
        
        # Create table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_large_vecf32_batch"))
            
            # Generate CREATE TABLE SQL
            create_sql = str(CreateTable(table).compile(
                dialect=MatrixOneDialect(), 
                compile_kwargs={"literal_binds": True}
            ))
            print(f"Creating table with SQL: {create_sql}")
            conn.execute(text(create_sql))
            
            # Batch insert 10 rows
            batch_size = 10
            vf32 = Vectorf32(dimension=65535)
            processor = vf32.process_bind_param
            
            print(f"Starting vecf32 batch insertion of {batch_size} rows...")
            
            for batch_id in range(batch_size):
                # Generate unique vector data for each row
                vector_data = []
                for i in range(65535):
                    # Use batch_id to make each vector unique: cos((i + batch_id*1000)/1000)
                    value = math.cos((i + batch_id * 1000) / 1000.0)
                    vector_data.append(value)
                
                # Convert to string
                vector_string = processor(vector_data, engine.dialect)
                
                # Insert the vector data
                insert_sql = text("INSERT INTO test_large_vecf32_batch (vector, batch_id) VALUES (:vector, :batch_id)")
                result = conn.execute(insert_sql, {"vector": vector_string, "batch_id": batch_id + 1})
                
                print(f"Inserted vecf32 row {batch_id + 1}, affected rows: {result.rowcount}")
            
            # Verify count
            count_sql = text("SELECT COUNT(*) FROM test_large_vecf32_batch")
            result = conn.execute(count_sql)
            count = result.fetchone()[0]
            print(f"Total vecf32 rows in table: {count}")
            assert count == batch_size, f"Expected {batch_size} rows, got {count}"
            
            # Verify each row individually
            for batch_id in range(1, batch_size + 1):
                select_sql = text("SELECT id, vector, batch_id FROM test_large_vecf32_batch WHERE batch_id = :batch_id")
                result = conn.execute(select_sql, {"batch_id": batch_id})
                row = result.fetchone()
                
                assert row is not None, f"No data found for batch_id {batch_id}"
                assert row[0] == batch_id, f"ID mismatch for batch_id {batch_id}: expected {batch_id}, got {row[0]}"
                assert row[2] == batch_id, f"batch_id mismatch: expected {batch_id}, got {row[2]}"
                
                # Verify vector data matches expected pattern
                returned_vector_str = row[1]
                clean_str = returned_vector_str.strip("[]")
                returned_values = [float(x.strip()) for x in clean_str.split(",")]
                
                assert len(returned_values) == 65535, f"Expected 65535 dimensions for batch_id {batch_id}, got {len(returned_values)}"
                
                # Check first few values match expected pattern
                for i in range(10):
                    expected = math.cos((i + (batch_id - 1) * 1000) / 1000.0)
                    actual = returned_values[i]
                    assert abs(expected - actual) < 1e-6, f"Value mismatch at index {i} for batch_id {batch_id}: expected {expected}, got {actual}"
                
                print(f"✅ Verified vecf32 row {batch_id} with batch_id {batch_id}")
            
            # Test batch count query
            batch_count_sql = text("SELECT batch_id, COUNT(*) as count FROM test_large_vecf32_batch GROUP BY batch_id ORDER BY batch_id")
            result = conn.execute(batch_count_sql)
            rows = result.fetchall()
            
            assert len(rows) == batch_size, f"Expected {batch_size} batch groups, got {len(rows)}"
            for i, (batch_id, count) in enumerate(rows):
                assert batch_id == i + 1, f"Batch ID mismatch at position {i}: expected {i + 1}, got {batch_id}"
                assert count == 1, f"Expected 1 row per batch, got {count} for batch_id {batch_id}"
            
            print(f"✅ vecf32 Batch insertion of {batch_size} rows successful!")
            print(f"✅ vecf32 Count verification passed: {count} rows total")
            print(f"✅ vecf32 Individual row verification passed for all {batch_size} rows")

    def test_vector_string_length_limits(self, engine):
        """Test the actual string length limits for large vectors."""
        # Test different dimensions to see where limits might be
        test_dimensions = [1000, 10000, 65535]
        
        for dim in test_dimensions:
            print(f"\n--- Testing dimension {dim} ---")
            
            # Generate vector data
            vector_data = [math.sin(i / 1000.0) for i in range(dim)]
            
            # Convert to string
            vf64 = Vectorf64(dimension=dim)
            processor = vf64.process_bind_param
            vector_string = processor(vector_data, engine.dialect)
            
            print(f"Dimension {dim}: String length = {len(vector_string)}")
            print(f"Average chars per dimension: {len(vector_string) / dim:.2f}")
            
            # Check if string is within reasonable limits
            if len(vector_string) > 1000000:  # 1MB
                print(f"⚠️  Warning: String length {len(vector_string)} is very large for dimension {dim}")
            else:
                print(f"✅ String length {len(vector_string)} is reasonable for dimension {dim}")
