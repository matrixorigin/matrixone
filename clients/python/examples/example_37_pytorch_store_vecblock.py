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
Example 36: PyTorch Store - Store Tensors as vecf8 / vecf4 Cells

MatrixOne PyTorch Integration Example

This example demonstrates how to write PyTorch embeddings into MatrixOne vecf8 (MXFP8)
and vecf4 (NVFP4) columns:
- Quantizing embeddings in PyTorch with vecblock.quantize()
- Inserting the cells as they are (CAST(CAST(x'..' AS BLOB) AS vecf8(N))), so MatrixOne
  stores them without quantizing again, and reading them back byte-identical
- vecblock.quantize() following MatrixOne's encoder: the same cells as CAST(float AS vecf8(N))
- The quantization error of each format

Requires PyTorch 2.9 or later (pip install 'matrixone-python-sdk[torch]').
"""

import sys

try:
    import torch
except ImportError:
    print("This example needs PyTorch (pip install 'matrixone-python-sdk[torch]'); skipped.")
    sys.exit(0)

from matrixone import Client
from matrixone import vecblock
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger

TABLE = "pt_store_docs"
DIM = 96
ROWS = 500


class PyTorchStoreVecBlockDemo:
    """Demonstrates storing PyTorch tensors as vecf8 / vecf4 cells."""

    def __init__(self):
        self.logger = create_default_logger(sql_log_mode="auto")
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
        }
        # embeddings produced by a model, here random
        self.emb = torch.randn(ROWS, DIM, generator=torch.Generator().manual_seed(28)) * 0.05

    def connect(self):
        """Connect a MatrixOne Client with the configured connection parameters."""
        host, port, user, password, database = get_connection_params()
        client = Client(logger=self.logger, sql_log_mode="auto")
        client.connect(host=host, port=port, user=user, password=password, database=database)
        return client

    def setup_table(self, client):
        """Create a table with vecf8, vecf4 and vecf32 columns."""
        client.execute(f"DROP TABLE IF EXISTS {TABLE}")
        client.execute(
            f"CREATE TABLE {TABLE} (id INT PRIMARY KEY, v8 vecf8({DIM}), v4 vecf4({DIM}), f32 vecf32({DIM}))"
        )

    def insert_cells(self, client, column, cells):
        """Insert cells in batches; vecblock.cell_sql() makes each a value of the column type."""
        for start in range(0, len(cells), 100):
            values = ",".join(
                f"({i}, {vecblock.cell_sql(cells[i])})" for i in range(start, min(start + 100, len(cells)))
            )
            client.execute(
                f"INSERT INTO {TABLE} (id, {column}) VALUES {values} "
                f"ON DUPLICATE KEY UPDATE {column} = VALUES({column})"
            )

    def test_store_cells(self):
        """Test storing quantized cells and reading them back byte-identical"""
        print("\n=== Store Cells Tests ===")

        self.results['tests_run'] += 1

        try:
            client = self.connect()
            try:
                self.setup_table(client)

                cells8 = vecblock.quantize(self.emb, "vecf8").to_cells()
                cells4 = vecblock.quantize(self.emb, "vecf4").to_cells()
                self.insert_cells(client, "v8", cells8)
                self.insert_cells(client, "v4", cells4)
                self.logger.info(
                    f"📊 Stored {ROWS} vecf8({DIM}) cells ({len(cells8[0])} bytes each) "
                    f"and vecf4({DIM}) cells ({len(cells4[0])} bytes each)"
                )

                rows = client.execute(
                    f"SELECT vecblock_binary(v8), vecblock_binary(v4) FROM {TABLE} ORDER BY id"
                ).fetchall()
                assert all(bytes(r[0]) == c for r, c in zip(rows, cells8)), "vecf8 cells differ after read-back"
                assert all(bytes(r[1]) == c for r, c in zip(rows, cells4)), "vecf4 cells differ after read-back"
                self.logger.info("✅ vecf8 and vecf4 cells read back byte-identical")

                self.results['tests_passed'] += 1
            finally:
                client.execute(f"DROP TABLE IF EXISTS {TABLE}")
                client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Store cells test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Store Cells', 'error': str(e)})

    def test_quantize_equals_matrixone(self):
        """Test vecblock.quantize() against MatrixOne's CAST(float AS vecf8(N) / vecf4(N))"""
        print("\n=== Quantize Equals MatrixOne Tests ===")

        self.results['tests_run'] += 1

        try:
            client = self.connect()
            try:
                self.setup_table(client)

                for start in range(0, ROWS, 100):
                    values = []
                    for i in range(start, min(start + 100, ROWS)):
                        text = "[" + ",".join(repr(float(x)) for x in self.emb[i]) + "]"
                        values.append(f"({i}, '{text}')")
                    client.execute(f"INSERT INTO {TABLE} (id, f32) VALUES " + ",".join(values))
                rows = client.execute(
                    f"SELECT vecblock_binary(CAST(f32 AS vecf8({DIM}))), vecblock_binary(CAST(f32 AS vecf4({DIM}))) "
                    f"FROM {TABLE} ORDER BY id"
                ).fetchall()

                for name, col in [("vecf8", 0), ("vecf4", 1)]:
                    cells = vecblock.quantize(self.emb, name).to_cells()
                    assert all(bytes(r[col]) == c for r, c in zip(rows, cells)), f"{name} cells differ from CAST"
                    self.logger.info(f"✅ vecblock.quantize equals CAST(... AS {name}) for {ROWS} rows")

                self.results['tests_passed'] += 1
            finally:
                client.execute(f"DROP TABLE IF EXISTS {TABLE}")
                client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Quantize test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Quantize Equals MatrixOne', 'error': str(e)})

    def test_quantization_error(self):
        """Test the quantization error of each format"""
        print("\n=== Quantization Error Tests ===")

        self.results['tests_run'] += 1

        try:
            for name in ("vecf8", "vecf4"):
                dec = vecblock.quantize(self.emb, name).to_float()
                rel = ((dec - self.emb).norm(dim=1) / self.emb.norm(dim=1)).mean().item()
                cos = torch.nn.functional.cosine_similarity(dec, self.emb).mean().item()
                self.logger.info(f"📊 {name}: mean relative L2 error {rel:.4f}, mean cosine to the original {cos:.5f}")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Quantization error test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Quantization Error', 'error': str(e)})

    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        print("\n" + "=" * 80)
        print("PyTorch Store vecf8 / vecf4 Demo - Summary Report")
        print("=" * 80)

        total_tests = self.results['tests_run']
        passed_tests = self.results['tests_passed']
        failed_tests = self.results['tests_failed']
        unexpected_results = self.results['unexpected_results']

        print(f"Total Tests Run: {total_tests}")
        print(f"Tests Passed: {passed_tests}")
        print(f"Tests Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests / total_tests * 100):.1f}%" if total_tests > 0 else "N/A")

        if unexpected_results:
            print(f"\nUnexpected Results ({len(unexpected_results)}):")
            for i, result in enumerate(unexpected_results, 1):
                print(f"  {i}. Test: {result['test']}")
                print(f"     Error: {result['error']}")
        else:
            print("\n✓ No unexpected results - all tests behaved as expected")

        return self.results


def main():
    """Main demo function"""
    demo = PyTorchStoreVecBlockDemo()

    try:
        print("🎯 MatrixOne PyTorch Store vecf8 / vecf4 Demo")
        print("=" * 60)

        # Print current configuration
        print_config()

        # Run tests
        demo.test_store_cells()
        demo.test_quantize_equals_matrixone()
        demo.test_quantization_error()

        # Generate report
        results = demo.generate_summary_report()

        print("\n🎉 All PyTorch store demos completed!")
        print("\nKey features demonstrated:")
        print("- ✅ vecblock.quantize() turns tensors into vecf8 / vecf4 cells")
        print("- ✅ vecblock.cell_sql() inserts the cells without a second quantization")
        print("- ✅ Cells read back byte-identical with vecblock_binary()")
        print("- ✅ vecblock.quantize() produces the same cells as MatrixOne's CAST")

        return results

    except Exception as e:
        print(f"Demo failed with error: {e}")
        return None


if __name__ == "__main__":
    main()
