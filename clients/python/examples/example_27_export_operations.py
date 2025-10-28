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
Example: Data Export Operations (Pandas-Style Interface)

This example demonstrates how to export query results to files and stages
using pandas-style to_csv() and to_jsonl() methods.

Key Features:
- Pandas-style interface: to_csv() and to_jsonl()
- Support for raw SQL, SQLAlchemy select(), and MatrixOne queries
- Export to local files or external stages
- Customizable CSV options (sep, quotechar, lineterminator)

Requirements:
- MatrixOne server running on localhost:6001
- Database 'test' with appropriate permissions
"""

from matrixone import Client
from matrixone.orm import declarative_base
from sqlalchemy import Column, Integer, String, DECIMAL, select, func
import tempfile
import os

# Create Base for ORM
Base = declarative_base()


# Define models
class Product(Base):
    __tablename__ = 'export_products'

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    category = Column(String(50))
    price = Column(DECIMAL(10, 2))
    stock = Column(Integer)


class Sale(Base):
    __tablename__ = 'export_sales'

    id = Column(Integer, primary_key=True)
    product_id = Column(Integer)
    quantity = Column(Integer)
    total = Column(DECIMAL(10, 2))


def setup_test_data(client: Client):
    """Create tables and populate with test data"""
    print("Setting up test data...")

    # Drop and create tables
    client.drop_table('export_products')
    client.drop_table('export_sales')
    client.create_table(Product)
    client.create_table(Sale)

    # Insert product data
    products = [
        {'id': 1, 'name': 'Laptop', 'category': 'Electronics', 'price': 999.99, 'stock': 50},
        {'id': 2, 'name': 'Mouse', 'category': 'Electronics', 'price': 29.99, 'stock': 200},
        {'id': 3, 'name': 'Desk', 'category': 'Furniture', 'price': 299.99, 'stock': 30},
        {'id': 4, 'name': 'Chair', 'category': 'Furniture', 'price': 199.99, 'stock': 45},
        {'id': 5, 'name': 'Monitor', 'category': 'Electronics', 'price': 399.99, 'stock': 75},
    ]

    for product in products:
        client.insert(Product, product)

    # Insert sales data
    sales = [
        {'id': 1, 'product_id': 1, 'quantity': 2, 'total': 1999.98},
        {'id': 2, 'product_id': 2, 'quantity': 5, 'total': 149.95},
        {'id': 3, 'product_id': 3, 'quantity': 1, 'total': 299.99},
        {'id': 4, 'product_id': 1, 'quantity': 1, 'total': 999.99},
        {'id': 5, 'product_id': 4, 'quantity': 3, 'total': 599.97},
    ]

    for sale in sales:
        client.insert(Sale, sale)

    print(f"✓ Created {len(products)} products and {len(sales)} sales")


def example_1_basic_csv_export(client: Client, tmpdir: str):
    """Example 1: Basic CSV export with defaults"""
    print("\n" + "=" * 60)
    print("Example 1: Basic CSV Export")
    print("=" * 60)

    export_file = os.path.join(tmpdir, 'products.csv')

    # Export all products to CSV (pandas-style)
    print(f"\nExporting products to {export_file}")
    client.export.to_csv(export_file, "SELECT * FROM export_products")

    print("✓ Export completed")
    print(f"  File: {export_file}")

    # Show file contents
    if os.path.exists(export_file):
        with open(export_file, 'r') as f:
            print("\nFirst 3 lines:")
            for i, line in enumerate(f):
                if i < 3:
                    print(f"  {line.rstrip()}")


def example_2_custom_separator(client: Client, tmpdir: str):
    """Example 2: TSV export with tab separator"""
    print("\n" + "=" * 60)
    print("Example 2: TSV Export (Tab-Separated Values)")
    print("=" * 60)

    export_file = os.path.join(tmpdir, 'products.tsv')

    # Export with tab separator (pandas-style)
    print(f"\nExporting to TSV: {export_file}")
    client.export.to_csv(export_file, "SELECT name, category, price FROM export_products", sep='\t')  # Tab separator

    print("✓ TSV export completed")

    # Show file contents
    if os.path.exists(export_file):
        with open(export_file, 'r') as f:
            print("\nFirst 3 lines:")
            for i, line in enumerate(f):
                if i < 3:
                    print(f"  {line.rstrip()}")


def example_3_pipe_delimiter(client: Client, tmpdir: str):
    """Example 3: Pipe-delimited export"""
    print("\n" + "=" * 60)
    print("Example 3: Pipe-Delimited Export")
    print("=" * 60)

    export_file = os.path.join(tmpdir, 'products_pipe.csv')

    # Export with pipe delimiter (pandas-style)
    print(f"\nExporting with pipe delimiter: {export_file}")
    client.export.to_csv(export_file, "SELECT * FROM export_products", sep='|')  # Pipe separator

    print("✓ Export completed")

    # Show file contents
    if os.path.exists(export_file):
        with open(export_file, 'r') as f:
            print("\nFirst 3 lines:")
            for i, line in enumerate(f):
                if i < 3:
                    print(f"  {line.rstrip()}")


def example_4_sqlalchemy_select(client: Client, tmpdir: str):
    """Example 4: Export with SQLAlchemy select()"""
    print("\n" + "=" * 60)
    print("Example 4: Export with SQLAlchemy select()")
    print("=" * 60)

    export_file = os.path.join(tmpdir, 'electronics.csv')

    # Use SQLAlchemy select() statement
    stmt = select(Product).where(Product.category == 'Electronics')

    print(f"\nExporting electronics products to {export_file}")
    client.export.to_csv(export_file, stmt, sep=',')  # SQLAlchemy statement

    print("✓ Export completed")
    print(f"  Query: {stmt}")


def example_5_matrixone_query(client: Client, tmpdir: str):
    """Example 5: Export with MatrixOne query builder"""
    print("\n" + "=" * 60)
    print("Example 5: Export with MatrixOne Query Builder")
    print("=" * 60)

    export_file = os.path.join(tmpdir, 'expensive_products.csv')

    # Use MatrixOne query builder
    query = client.query(Product).filter(Product.price > 200)

    print(f"\nExporting expensive products to {export_file}")
    client.export.to_csv(export_file, query)  # MatrixOne query

    print("✓ Export completed")
    print(f"  Filter: price > 200")


def example_6_aggregated_query(client: Client, tmpdir: str):
    """Example 6: Export aggregated data"""
    print("\n" + "=" * 60)
    print("Example 6: Export Aggregated Query Results")
    print("=" * 60)

    export_file = os.path.join(tmpdir, 'sales_by_product.csv')

    # Aggregated query with SQLAlchemy
    stmt = select(
        Sale.product_id, func.sum(Sale.quantity).label('total_quantity'), func.sum(Sale.total).label('total_revenue')
    ).group_by(Sale.product_id)

    print(f"\nExporting sales summary to {export_file}")
    client.export.to_csv(export_file, stmt)

    print("✓ Export completed")


def example_7_jsonl_export(client: Client, tmpdir: str):
    """Example 7: JSONL export"""
    print("\n" + "=" * 60)
    print("Example 7: JSONL Export (JSON Lines)")
    print("=" * 60)

    export_file = os.path.join(tmpdir, 'products.jsonl')

    # Export to JSONL format (pandas-style)
    print(f"\nExporting products to JSONL: {export_file}")
    client.export.to_jsonl(export_file, "SELECT * FROM export_products")

    print("✓ JSONL export completed")

    # Show file contents
    if os.path.exists(export_file):
        with open(export_file, 'r') as f:
            print("\nFirst 2 JSON objects:")
            for i, line in enumerate(f):
                if i < 2:
                    print(f"  {line.rstrip()}")


def example_8_export_to_stage(client: Client, tmpdir: str):
    """Example 8: Export to external stage"""
    print("\n" + "=" * 60)
    print("Example 8: Export to External Stage")
    print("=" * 60)

    # Create a stage
    stage_name = 'example_export_stage'
    stage_url = f"file://{tmpdir}/stage/"

    print(f"\nCreating stage: {stage_name}")
    client.stage.create(stage_name, stage_url, if_not_exists=True)

    try:
        # Export to stage using stage:// protocol (pandas-style)
        print(f"\nExporting to stage://{stage_name}/backup.csv")
        client.export.to_csv(f'stage://{stage_name}/backup.csv', "SELECT * FROM export_products")

        print("✓ Export to stage completed")

        # Export JSONL to stage
        print(f"\nExporting to stage://{stage_name}/backup.jsonl")
        client.export.to_jsonl(f'stage://{stage_name}/backup.jsonl', "SELECT * FROM export_products")

        print("✓ JSONL export to stage completed")

    finally:
        # Clean up stage
        client.stage.drop(stage_name, if_exists=True)
        print(f"\n✓ Dropped stage: {stage_name}")


def example_9_export_with_session(client: Client, tmpdir: str):
    """Example 9: Export using session"""
    print("\n" + "=" * 60)
    print("Example 9: Export with Transaction Session")
    print("=" * 60)

    export_file = os.path.join(tmpdir, 'session_export.csv')

    # Export using session (pandas-style)
    print(f"\nExporting via session to {export_file}")
    with client.session() as session:
        stmt = select(Product).where(Product.stock > 50)
        result = session.export.to_csv(export_file, stmt)
        print("✓ Export completed within session")


def example_10_special_characters(client: Client, tmpdir: str):
    """Example 10: Export data with special characters"""
    print("\n" + "=" * 60)
    print("Example 10: Export with Special Characters")
    print("=" * 60)

    # Insert product with special characters
    client.execute(
        "INSERT INTO export_products (id, name, category, price, stock) "
        "VALUES (10, 'Product \"Special\" Name', 'Test,Category', 99.99, 10)"
    )

    export_file = os.path.join(tmpdir, 'special_chars.csv')

    # Export with quotechar to handle special characters (pandas-style)
    print(f"\nExporting with quotechar to {export_file}")
    client.export.to_csv(export_file, "SELECT * FROM export_products WHERE id = 10", quotechar='"')

    print("✓ Export with quotechar completed")

    # Show file contents
    if os.path.exists(export_file):
        with open(export_file, 'r') as f:
            print("\nFile contents:")
            print(f"  {f.read().rstrip()}")

    # Clean up
    client.execute("DELETE FROM export_products WHERE id = 10")


def main():
    """Main function to run all examples"""
    print("\n" + "=" * 60)
    print("MatrixOne Export Operations - Pandas-Style Interface")
    print("=" * 60)

    # Create client and connect
    client = Client()
    client.connect(host='127.0.0.1', port=6001, user='root', password='111', database='test')

    # Create temporary directory for exports
    tmpdir = tempfile.mkdtemp(prefix='mo_export_')
    print(f"\nTemporary directory: {tmpdir}")

    try:
        # Setup test data
        setup_test_data(client)

        # Run examples
        example_1_basic_csv_export(client, tmpdir)
        example_2_custom_separator(client, tmpdir)
        example_3_pipe_delimiter(client, tmpdir)
        example_4_sqlalchemy_select(client, tmpdir)
        example_5_matrixone_query(client, tmpdir)
        example_6_aggregated_query(client, tmpdir)
        example_7_jsonl_export(client, tmpdir)
        example_8_export_to_stage(client, tmpdir)
        example_9_export_with_session(client, tmpdir)
        example_10_special_characters(client, tmpdir)

        print("\n" + "=" * 60)
        print("All examples completed successfully!")
        print("=" * 60)
        print(f"\nExported files are in: {tmpdir}")

    finally:
        # Clean up
        client.drop_table('export_products')
        client.drop_table('export_sales')
        client.disconnect()

        # Note: tmpdir is not deleted so you can inspect the exported files
        print(f"\nNote: Temporary files are kept for inspection: {tmpdir}")


if __name__ == '__main__':
    main()
