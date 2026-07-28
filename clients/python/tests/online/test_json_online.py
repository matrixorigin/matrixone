"""
Online tests for all JSON functionality - verify execution results.

Unified test file covering:
- JSON functions (json_extract, json_set, json_insert, json_replace)
- SQLAlchemy standard syntax (column['key'], .astext, .cast())
- Boolean handling
- Complex queries

Uses a single test table to reduce overhead.
"""

import json
import pytest
from sqlalchemy import Column, Integer, String, update, Numeric
from matrixone import Client
from matrixone.orm import declarative_base
from matrixone.sqlalchemy_ext import (
    JSON,
    json_extract,
    json_extract_float64,
    json_extract_string,
    json_insert,
    json_replace,
    json_set,
)

Base = declarative_base()


class Product(Base):
    """Unified test model for all JSON tests."""

    __tablename__ = 'test_json_all_online'
    product_id = Column(Integer, primary_key=True)
    product_name = Column(String(200))
    specifications = Column(JSON)


@pytest.fixture(scope='module')
def client():
    """Create and connect client for tests."""
    c = Client()
    c.connect(database='test')
    yield c
    c.disconnect()


@pytest.fixture(scope='module')
def test_table(client):
    """Create test table once for all tests."""
    try:
        client.drop_table(Product)
    except:
        pass

    client.create_table(Product)

    # Insert comprehensive test data
    test_data = [
        {
            'product_id': 1,
            'product_name': 'Laptop',
            'specifications': {
                'brand': 'Dell',
                'model': 'XPS 15',
                'category': 'Electronics',
                'price': 1299.99,
                'ram': 16,
                'storage': '512GB SSD',
                'active': True,
                'verified': True,
                'features': ['touchscreen', 'backlit keyboard'],
                'hardware': {'processor': 'Intel i7', 'graphics': 'NVIDIA'},
            },
        },
        {
            'product_id': 2,
            'product_name': 'Mouse',
            'specifications': {
                'brand': 'Logitech',
                'model': 'MX Master 3',
                'category': 'Accessories',
                'price': 99.99,
                'color': 'black',
                'wireless': True,
                'active': True,
                'verified': False,
            },
        },
        {
            'product_id': 3,
            'product_name': 'Monitor',
            'specifications': {
                'brand': 'LG',
                'model': '27UP850',
                'category': 'Displays',
                'price': 599.99,
                'size': 27,
                'resolution': '4K',
                'active': False,
                'verified': True,
            },
        },
    ]

    client.batch_insert(Product, test_data)

    yield

    # Cleanup
    try:
        client.drop_table(Product)
    except:
        pass


class TestJSONExtract:
    """Test json_extract function execution."""

    def test_extract_single_path(self, client, test_table):
        """Test extracting single JSON field."""
        results = client.query(
            Product.product_name, json_extract(Product.specifications, '$.brand').label('brand')
        ).execute()

        rows = results.rows
        assert len(rows) == 3
        assert '"Dell"' in rows[0][1]
        assert '"Logitech"' in rows[1][1]

    def test_extract_nested_path(self, client, test_table):
        """Test extracting nested JSON field."""
        results = (
            client.query(Product.product_name, json_extract(Product.specifications, '$.hardware.processor').label('cpu'))
            .filter(Product.product_id == 1)
            .execute()
        )

        assert len(results.rows) == 1
        assert '"Intel i7"' in results.rows[0][1]

    def test_extract_with_filter(self, client, test_table):
        """Test json_extract in WHERE clause."""
        results = client.query(Product).filter(json_extract(Product.specifications, '$.brand') == '"Dell"').execute()

        assert len(results.rows) == 1
        assert results.rows[0][1] == 'Laptop'

    def test_extract_with_order_by(self, client, test_table):
        """Test ordering by extracted JSON field."""
        results = (
            client.query(Product.product_name, json_extract_float64(Product.specifications, '$.price').label('price'))
            .order_by(json_extract_float64(Product.specifications, '$.price').desc())
            .execute()
        )

        rows = results.rows
        # Should be ordered: Laptop, Monitor, Mouse
        assert rows[0][0] == 'Laptop'
        assert rows[1][0] == 'Monitor'
        assert rows[2][0] == 'Mouse'


class TestJSONExtractTyped:
    """Test typed extraction functions."""

    def test_extract_string(self, client, test_table):
        """Test json_extract_string."""
        results = (
            client.query(Product.product_name, json_extract_string(Product.specifications, '$.brand').label('brand'))
            .filter(Product.product_id == 1)
            .execute()
        )

        assert results.rows[0][1] == 'Dell'  # No quotes

    def test_extract_float64(self, client, test_table):
        """Test json_extract_float64."""
        results = (
            client.query(Product.product_name, json_extract_float64(Product.specifications, '$.price').label('price'))
            .filter(Product.product_id == 1)
            .execute()
        )

        assert results.rows[0][1] == 1299.99  # Numeric


class TestJSONModification:
    """Test json_set/insert/replace execution."""

    def test_json_set_update_and_insert(self, client, test_table):
        """Test json_set updating existing and inserting new field."""
        stmt = (
            update(Product)
            .values(specifications=json_set(Product.specifications, '$.ram', 32, '$.warranty', '3 years'))
            .where(Product.product_id == 1)
        )

        client.execute(stmt)

        result = client.query(Product).filter(Product.product_id == 1).first()
        specs = json.loads(result.specifications) if isinstance(result.specifications, str) else result.specifications
        assert specs['ram'] == 32
        assert specs['warranty'] == '3 years'

    def test_json_insert_no_overwrite(self, client, test_table):
        """Test json_insert doesn't overwrite existing."""
        stmt = (
            update(Product)
            .values(
                specifications=json_insert(Product.specifications, '$.brand', 'ShouldNotChange', '$.warranty', '2 years')
            )
            .where(Product.product_id == 2)
        )

        client.execute(stmt)

        result = client.query(Product).filter(Product.product_id == 2).first()
        specs = json.loads(result.specifications) if isinstance(result.specifications, str) else result.specifications
        assert specs['brand'] == 'Logitech'  # Not changed
        assert specs['warranty'] == '2 years'  # Added

    def test_json_replace_only_existing(self, client, test_table):
        """Test json_replace only updates existing fields."""
        stmt = (
            update(Product)
            .values(specifications=json_replace(Product.specifications, '$.price', 499.99, '$.discount', 10))
            .where(Product.product_id == 3)
        )

        client.execute(stmt)

        result = client.query(Product).filter(Product.product_id == 3).first()
        specs = json.loads(result.specifications) if isinstance(result.specifications, str) else result.specifications
        assert specs['price'] == '499.99'  # Updated
        assert 'discount' not in specs  # Not added


class TestStandardSyntax:
    """Test SQLAlchemy standard JSON syntax."""

    def test_dict_access(self, client, test_table):
        """Test column['key'] dictionary access."""
        results = client.query(Product.product_name, Product.specifications['brand'].label('brand')).execute()

        assert len(results.rows) == 3
        assert '"Dell"' in results.rows[0][1]

    def test_nested_dict_access(self, client, test_table):
        """Test column['key1']['key2'] nested access."""
        results = (
            client.query(Product.product_name, Product.specifications['hardware']['processor'].label('cpu'))
            .filter(Product.product_id == 1)
            .execute()
        )

        assert results.rows[0][1] == '"Intel i7"'

    def test_astext_removes_quotes(self, client, test_table):
        """Test .astext removes JSON quotes."""
        results = client.query(Product.product_name, Product.specifications['brand'].astext.label('brand')).execute()

        # No quotes
        assert results.rows[0][1] == 'Dell'
        assert results.rows[1][1] == 'Logitech'

    def test_cast_to_numeric(self, client, test_table):
        """Test .cast(Numeric) for numeric conversion."""
        results = client.query(Product.product_name, Product.specifications['price'].cast(Numeric).label('price')).execute()

        # Returns numeric values
        assert results.rows[0][1] == 1299.99
        assert results.rows[1][1] == 99.99

    def test_filter_with_dict_syntax(self, client, test_table):
        """Test WHERE with column['key']."""
        results = client.query(Product).filter(Product.specifications['brand'] == 'Dell').execute()  # Auto-quotes

        assert len(results.rows) == 1
        assert results.rows[0][1] == 'Laptop'

    def test_filter_with_astext(self, client, test_table):
        """Test WHERE with .astext."""
        results = client.query(Product).filter(Product.specifications['category'].astext == 'Electronics').execute()

        assert len(results.rows) == 1
        assert results.rows[0][1] == 'Laptop'

    def test_numeric_filter_with_cast(self, client, test_table):
        """Test numeric filtering with .cast()."""
        # Use a fresh query to avoid data modification from previous tests
        results = (
            client.query(Product.product_name, Product.specifications['price'].cast(Numeric).label('price'))
            .filter(Product.specifications['price'].cast(Numeric) > 100)
            .execute()
        )

        # Should include products with price > 100
        assert len(results.rows) >= 1
        # Verify all returned prices are > 100
        for row in results.rows:
            assert row[1] > 100


class TestBooleanHandling:
    """Test boolean value handling."""

    def test_python_true_comparison(self, client, test_table):
        """Test filtering with Python True."""
        results = client.query(Product).filter(Product.specifications['active'] == True).execute()

        # Laptop and Mouse are active
        assert len(results.rows) == 2
        names = [row[1] for row in results.rows]
        assert 'Laptop' in names
        assert 'Mouse' in names

    def test_python_false_comparison(self, client, test_table):
        """Test filtering with Python False."""
        results = client.query(Product).filter(Product.specifications['active'] == False).execute()

        # Only Monitor is inactive
        assert len(results.rows) == 1
        assert results.rows[0][1] == 'Monitor'

    def test_string_boolean_comparison(self, client, test_table):
        """Test filtering with string 'true'/'false'."""
        results = client.query(Product).filter(Product.specifications['verified'] == 'true').execute()

        # Laptop and Monitor are verified
        assert len(results.rows) == 2

    def test_asbool_property(self, client, test_table):
        """Test .asbool property."""
        results = (
            client.query(Product.product_name, Product.specifications['active'].asbool.label('is_active'))
            .order_by(Product.product_id)
            .execute()
        )

        # Verify exact boolean results
        assert len(results.rows) == 3
        # Laptop: active=true -> should be true
        assert results.rows[0][0] == 'Laptop'
        assert results.rows[0][1] in [True, 'true', 1, '1']
        # Mouse: active=true -> should be true
        assert results.rows[1][0] == 'Mouse'
        assert results.rows[1][1] in [True, 'true', 1, '1']
        # Monitor: active=false -> should be false
        assert results.rows[2][0] == 'Monitor'
        assert results.rows[2][1] in [False, 'false', 0, '0']

    def test_boolean_inequality(self, client, test_table):
        """Test != with boolean."""
        results = client.query(Product).filter(Product.specifications['active'] != False).execute()

        # Should return active products
        assert len(results.rows) == 2


class TestComplexScenarios:
    """Test complex real-world scenarios."""

    def test_combined_filters(self, client, test_table):
        """Test multiple filters with different types."""
        results = (
            client.query(
                Product.product_name,
                Product.specifications['brand'].astext.label('brand'),
                Product.specifications['price'].cast(Numeric).label('price'),
            )
            .filter(Product.specifications['active'] == True)
            .filter(Product.specifications['price'].cast(Numeric) > 100)
            .order_by(Product.specifications['price'].cast(Numeric).desc())
            .execute()
        )

        rows = results.rows
        # Should be: Laptop (active, price > 100)
        assert len(rows) == 1
        assert rows[0][0] == 'Laptop'
        assert rows[0][1] == 'Dell'
        assert rows[0][2] == 1299.99

    def test_multiple_json_extracts(self, client, test_table):
        """Test multiple JSON field extractions."""
        results = (
            client.query(
                Product.product_name,
                Product.specifications['brand'].astext.label('brand'),
                Product.specifications['category'].astext.label('category'),
                Product.specifications['price'].cast(Numeric).label('price'),
                Product.specifications['active'].label('active'),
            )
            .filter(Product.product_id == 1)
            .execute()
        )

        row = results.rows[0]
        assert row[0] == 'Laptop'
        assert row[1] == 'Dell'
        assert row[2] == 'Electronics'
        assert row[3] == 1299.99
        assert 'true' in str(row[4])


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_null_json_column(self, client, test_table):
        """Test JSON functions with NULL column."""
        client.insert(Product, {'product_id': 99, 'product_name': 'Test NULL', 'specifications': None})

        results = (
            client.query(Product.product_name, json_extract(Product.specifications, '$.field').label('value'))
            .filter(Product.product_id == 99)
            .execute()
        )

        assert len(results.rows) == 1
        assert results.rows[0][1] is None

    def test_nonexistent_path(self, client, test_table):
        """Test extracting non-existent JSON path."""
        results = (
            client.query(Product.product_name, json_extract(Product.specifications, '$.nonexistent').label('value'))
            .filter(Product.product_id == 1)
            .execute()
        )

        assert results.rows[0][1] is None

    def test_empty_json_object(self, client, test_table):
        """Test with empty JSON object."""
        client.insert(Product, {'product_id': 98, 'product_name': 'Empty', 'specifications': {}})

        results = (
            client.query(Product.product_name, json_extract(Product.specifications, '$.field').label('value'))
            .filter(Product.product_id == 98)
            .execute()
        )

        assert results.rows[0][1] is None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
