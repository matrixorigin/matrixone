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
Example 27: Export Operations - Data Export to Files and Stages

This example demonstrates comprehensive export operations:
1. Export query results to local files (SELECT ... INTO OUTFILE)
2. Export to external stages (S3, local filesystem)
3. ORM-style chainable export methods
4. Various export formats (CSV, JSONLINE, TSV)
5. Custom delimiters and field enclosures
6. Export with aggregations and transformations
7. Stage-based export convenience methods

This example showcases the ExportManager functionality for efficient
data export from MatrixOne to external storage.
"""

import os
import tempfile
import shutil
from matrixone import Client, ExportFormat
from matrixone.logger import create_default_logger
from matrixone.config import get_connection_params, print_config
from matrixone.orm import declarative_base
from sqlalchemy import Column, Integer, String, DECIMAL, func

# Create Base for model definitions
Base = declarative_base()

# Create tmpfiles directory if it doesn't exist
TMPFILES_DIR = os.path.join(os.path.dirname(__file__), '..', 'tmpfiles')
os.makedirs(TMPFILES_DIR, exist_ok=True)


# Define table model
class Sale(Base):
    __tablename__ = 'sales'
    id = Column(Integer, primary_key=True)
    product = Column(String(100))
    category = Column(String(50))
    amount = Column(DECIMAL(10, 2))
    quantity = Column(Integer)


class ExportOperationsDemo:
    """
    Comprehensive demonstration of MatrixOne export operations.

    Export operations allow you to write query results to external files or stages,
    enabling data backup, data sharing, and integration with other systems.
    """

    def __init__(self):
        self.logger = create_default_logger()
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'stages_created': [],
            'tables_created': [],
            'files_exported': [],
        }
        self.stage_name = 'demo_export_stage'
        self.tmpdir = None

    def test_orm_export_to_stage(self, client):
        """Test ORM-style export to stage (RECOMMENDED)"""
        print("\n=== ORM-Style Export to Stage (Recommended) ===")
        self.results['tests_run'] += 1

        try:
            # Export filtered data using ORM
            result = (
                client.query(Sale).filter(Sale.category == 'Electronics').export_to_stage(self.stage_name, 'electronics.csv')
            )

            self.logger.info("✅ Exported filtered electronics sales")
            self.results['files_exported'].append('electronics.csv')
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ ORM export test failed: {e}")
            self.results['tests_failed'] += 1

    def test_orm_export_aggregated(self, client):
        """Test exporting aggregated query results"""
        print("\n=== Export Aggregated Data ===")
        self.results['tests_run'] += 1

        try:
            # Export aggregated results
            result = (
                client.query(Sale.category, func.sum(Sale.amount).label('total'))
                .group_by(Sale.category)
                .export_to_stage(self.stage_name, 'category_totals.csv')
            )

            self.logger.info("✅ Exported category totals")
            self.results['files_exported'].append('category_totals.csv')
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Aggregated export test failed: {e}")
            self.results['tests_failed'] += 1

    def test_low_level_export_to_stage(self, client):
        """Test low-level export API"""
        print("\n=== Low-Level Export API ===")
        self.results['tests_run'] += 1

        try:
            # Use client.export.to_stage() directly
            result = client.export.to_stage(
                query="SELECT * FROM sales WHERE category = 'Furniture'",
                stage_name=self.stage_name,
                filename='furniture.csv',
            )

            self.logger.info("✅ Exported using low-level API")
            self.results['files_exported'].append('furniture.csv')
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Low-level export test failed: {e}")
            self.results['tests_failed'] += 1

    def test_stage_export_convenience(self, client):
        """Test stage.export_to() convenience method"""
        print("\n=== Stage Export Convenience Method ===")
        self.results['tests_run'] += 1

        try:
            # Get stage object and use its export method
            stage = client.stage.get(self.stage_name)
            result = stage.export_to(
                query="SELECT product, SUM(quantity) as total_qty FROM sales GROUP BY product", filename='product_totals.csv'
            )

            self.logger.info("✅ Exported using stage convenience method")
            self.results['files_exported'].append('product_totals.csv')
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Stage convenience export test failed: {e}")
            self.results['tests_failed'] += 1

    def test_export_formats(self, client):
        """Test different export formats"""
        print("\n=== Export Formats ===")

        # Test 1: CSV with field enclosure
        self.results['tests_run'] += 1
        try:
            result = client.query(Sale).export_to_stage(
                self.stage_name, 'sales_enclosed.csv', format='csv', fields_enclosed_by='"'
            )

            self.logger.info("✅ Exported CSV with field enclosure")
            self.results['files_exported'].append('sales_enclosed.csv')
            self.results['tests_passed'] += 1
        except Exception as e:
            self.logger.error(f"❌ CSV field enclosure test failed: {e}")
            self.results['tests_failed'] += 1

        # Test 2: JSONLINE format
        self.results['tests_run'] += 1
        try:
            result = client.query(Sale).export_to_stage(self.stage_name, 'sales.jsonl', format='jsonline')

            self.logger.info("✅ Exported as JSONLINE")
            self.results['files_exported'].append('sales.jsonl')
            self.results['tests_passed'] += 1
        except Exception as e:
            self.logger.error(f"❌ JSONLINE export test failed: {e}")
            self.results['tests_failed'] += 1

        # Test 3: TSV format (tab-separated)
        self.results['tests_run'] += 1
        try:
            result = client.query(Sale).export_to_stage(
                self.stage_name, 'sales.tsv', format='csv', fields_terminated_by='\t'
            )

            self.logger.info("✅ Exported as TSV")
            self.results['files_exported'].append('sales.tsv')
            self.results['tests_passed'] += 1
        except Exception as e:
            self.logger.error(f"❌ TSV export test failed: {e}")
            self.results['tests_failed'] += 1

    def test_practical_use_cases(self, client):
        """Test practical real-world use cases"""
        print("\n=== Practical Use Cases ===")

        # Use case 1: Daily sales report
        self.results['tests_run'] += 1
        try:
            result = (
                client.query(
                    Sale.category,
                    func.count(Sale.id).label('num_sales'),
                    func.sum(Sale.amount).label('total_revenue'),
                    func.avg(Sale.amount).label('avg_sale'),
                )
                .group_by(Sale.category)
                .export_to_stage(self.stage_name, 'daily_report.csv')
            )

            self.logger.info("✅ Exported daily sales report")
            self.results['files_exported'].append('daily_report.csv')
            self.results['tests_passed'] += 1
        except Exception as e:
            self.logger.error(f"❌ Daily report test failed: {e}")
            self.results['tests_failed'] += 1

        # Use case 2: Data backup
        self.results['tests_run'] += 1
        try:
            result = client.query(Sale).order_by(Sale.id).export_to_stage(self.stage_name, 'sales_backup.csv')

            self.logger.info("✅ Exported full data backup")
            self.results['files_exported'].append('sales_backup.csv')
            self.results['tests_passed'] += 1
        except Exception as e:
            self.logger.error(f"❌ Data backup test failed: {e}")
            self.results['tests_failed'] += 1

        # Use case 3: Partner data sharing (selected columns)
        self.results['tests_run'] += 1
        try:
            result = (
                client.query(Sale.product, Sale.category, Sale.quantity)
                .filter(Sale.category == 'Electronics')
                .export_to_stage(self.stage_name, 'partner_data.csv')
            )

            self.logger.info("✅ Exported partner data (selected columns)")
            self.results['files_exported'].append('partner_data.csv')
            self.results['tests_passed'] += 1
        except Exception as e:
            self.logger.error(f"❌ Partner data test failed: {e}")
            self.results['tests_failed'] += 1

    def setup(self, client):
        """Setup demo environment"""
        print("=== Setting Up Demo Environment ===\n")

        # Create table
        client.drop_table('sales')
        client.create_table(Sale)
        self.results['tables_created'].append('sales')
        self.logger.info("✅ Created sales table")

        # Insert sample data using ORM
        sales_data = [
            {'id': 1, 'product': 'Laptop', 'category': 'Electronics', 'amount': 1200.00, 'quantity': 5},
            {'id': 2, 'product': 'Mouse', 'category': 'Electronics', 'amount': 25.99, 'quantity': 50},
            {'id': 3, 'product': 'Desk', 'category': 'Furniture', 'amount': 350.00, 'quantity': 10},
            {'id': 4, 'product': 'Chair', 'category': 'Furniture', 'amount': 150.00, 'quantity': 20},
            {'id': 5, 'product': 'Monitor', 'category': 'Electronics', 'amount': 300.00, 'quantity': 15},
            {'id': 6, 'product': 'Bookshelf', 'category': 'Furniture', 'amount': 120.00, 'quantity': 8},
        ]

        for sale_data in sales_data:
            client.insert(Sale, sale_data)
        self.logger.info(f"✅ Inserted {len(sales_data)} sales records")

        # Create local stage
        self.tmpdir = tempfile.mkdtemp(dir=TMPFILES_DIR)
        stage = client.stage.create(self.stage_name, f"file://{self.tmpdir}/", comment='Demo export stage')
        self.results['stages_created'].append(self.stage_name)
        self.logger.info(f"✅ Created stage at {self.tmpdir}")

    def cleanup(self, client):
        """Cleanup demo resources"""
        print("\n=== Cleaning Up ===")

        try:
            # Drop stage
            if self.stage_name in self.results['stages_created']:
                client.stage.drop(self.stage_name, if_exists=True)
                self.logger.info(f"✅ Dropped stage '{self.stage_name}'")

            # Drop tables
            for table in self.results['tables_created']:
                client.drop_table(table)
                self.logger.info(f"✅ Dropped table '{table}'")

            # Remove temp directory
            if self.tmpdir and os.path.exists(self.tmpdir):
                shutil.rmtree(self.tmpdir, ignore_errors=True)
                self.logger.info(f"✅ Removed temporary directory")

        except Exception as e:
            self.logger.warning(f"⚠️ Cleanup warning: {e}")

    def print_summary(self):
        """Print test results summary"""
        print("\n" + "=" * 80)
        print("Export Operations Demo Summary")
        print("=" * 80)

        print(f"\nTests Run: {self.results['tests_run']}")
        print(f"Tests Passed: {self.results['tests_passed']}")
        print(f"Tests Failed: {self.results['tests_failed']}")

        print(f"\nStages Created: {len(self.results['stages_created'])}")
        print(f"Tables Created: {len(self.results['tables_created'])}")
        print(f"Files Exported: {len(self.results['files_exported'])}")

        if self.results['files_exported']:
            print("\nExported Files:")
            for filename in sorted(self.results['files_exported']):
                print(f"  - {filename}")

        print("\nKey Takeaways:")
        print("1. ✓ Use query().export_to_stage() for ORM-style exports (RECOMMENDED)")
        print("2. ✓ Export supports filtering, aggregations, and transformations")
        print("3. ✓ Multiple formats: CSV, JSONLINE, TSV")
        print("4. ✓ Stage-based export is more flexible than server filesystem")
        print("5. ✓ Note: MatrixOne doesn't support fields_terminated_by and fields_enclosed_by simultaneously")
        print("=" * 80)

    def run(self):
        """Run all demo tests"""
        print("=" * 80)
        print("MatrixOne Export Operations Demo")
        print("=" * 80)

        # Create client
        client = Client()
        try:
            client.connect(host='127.0.0.1', port=6001, user='root', password='111', database='test')
            self.logger.info("✅ Connected to MatrixOne")

            # Setup
            self.setup(client)

            # Run tests
            self.test_orm_export_to_stage(client)
            self.test_orm_export_aggregated(client)
            self.test_low_level_export_to_stage(client)
            self.test_stage_export_convenience(client)
            self.test_export_formats(client)
            self.test_practical_use_cases(client)

            # Print summary
            self.print_summary()

        finally:
            self.cleanup(client)
            client.disconnect()
            self.logger.info("✅ Disconnected from MatrixOne")


def main():
    """Main entry point"""
    demo = ExportOperationsDemo()
    demo.run()


if __name__ == '__main__':
    main()
