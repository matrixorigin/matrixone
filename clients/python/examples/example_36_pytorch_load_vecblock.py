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
Example 35: PyTorch Load - Load vecf8 / vecf4 Columns into Tensors

MatrixOne PyTorch Integration Example

This example demonstrates how to read MatrixOne vecf8 (MXFP8) and vecf4 (NVFP4)
columns into PyTorch:
- vecblock_binary() returns the stored cells, and matrixone.vecblock splits them into
  FP8 / FP4 element tensors, block-scale tensors and a global scale per row
- The float32 decoding in PyTorch equals MatrixOne's CAST(v AS vecf32(N))
- An IterableDataset streams a table into a DataLoader, as a training loop reads it

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

TABLE = "pt_load_docs"
DIM = 64
ROWS = 1000


class VecBlockDataset(torch.utils.data.IterableDataset):
    """Streams (ids, float32 vectors) from a table in id order, `batch_rows` rows at a time."""

    def __init__(self, client, table, id_col, vec_col, batch_rows=256):
        self.client, self.table, self.id_col, self.vec_col, self.batch_rows = client, table, id_col, vec_col, batch_rows

    def __iter__(self):
        last = None
        while True:
            where = f"WHERE {self.id_col} > {last}" if last is not None else ""
            rows = self.client.execute(
                f"SELECT {self.id_col}, vecblock_binary({self.vec_col}) FROM {self.table} {where} "
                f"ORDER BY {self.id_col} LIMIT {self.batch_rows}"
            ).fetchall()
            if not rows:
                return
            last = rows[-1][0]
            yield torch.tensor([r[0] for r in rows]), vecblock.from_cells([r[1] for r in rows]).to_float()


class PyTorchLoadVecBlockDemo:
    """Demonstrates loading vecf8 / vecf4 columns into PyTorch tensors."""

    def __init__(self):
        self.logger = create_default_logger(sql_log_mode="auto")
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
        }

    def connect(self):
        """Connect a MatrixOne Client with the configured connection parameters."""
        host, port, user, password, database = get_connection_params()
        client = Client(logger=self.logger, sql_log_mode="auto")
        client.connect(host=host, port=port, user=user, password=password, database=database)
        return client

    def setup_table(self, client):
        """Create a table with vecf8 and vecf4 columns; MatrixOne quantizes the float values."""
        client.execute(f"DROP TABLE IF EXISTS {TABLE}")
        client.execute(f"CREATE TABLE {TABLE} (id INT PRIMARY KEY, v8 vecf8({DIM}), v4 vecf4({DIM}))")
        data = torch.randn(ROWS, DIM, generator=torch.Generator().manual_seed(27))
        for start in range(0, ROWS, 200):
            values = []
            for i in range(start, min(start + 200, ROWS)):
                text = "[" + ",".join(repr(float(x)) for x in data[i]) + "]"
                values.append(f"({i}, '{text}', '{text}')")
            client.execute(f"INSERT INTO {TABLE} VALUES " + ",".join(values))
        self.logger.info(f"📊 Inserted {ROWS} rows of vecf8({DIM}) and vecf4({DIM})")

    def test_load_cells(self):
        """Test splitting stored cells into FP8 / FP4 tensors"""
        print("\n=== Load Cells into Tensors Tests ===")

        self.results['tests_run'] += 1

        try:
            client = self.connect()
            try:
                self.setup_table(client)

                rows = client.execute(
                    f"SELECT id, vecblock_binary(v8), vecblock_binary(v4) FROM {TABLE} ORDER BY id"
                ).fetchall()
                v8 = vecblock.from_cells([r[1] for r in rows])
                v4 = vecblock.from_cells([r[2] for r in rows])
                for name, batch in [("vecf8", v8), ("vecf4", v4)]:
                    self.logger.info(
                        f"📊 {name}: elements {tuple(batch.element_tensor().shape)} {batch.element_tensor().dtype}, "
                        f"scales {tuple(batch.scale_tensor().shape)} {batch.scale_tensor().dtype}, "
                        f"global {tuple(batch.global_scale.shape)} {batch.global_scale.dtype}"
                    )
                assert v8.element_tensor().dtype == torch.float8_e4m3fn
                assert v8.scale_tensor().dtype == torch.float8_e8m0fnu
                assert v4.element_tensor().dtype == torch.float4_e2m1fn_x2
                assert v4.scale_tensor().dtype == torch.float8_e4m3fn
                self.logger.info("✅ Cells split into FP8 / FP4 tensors")

                self.results['tests_passed'] += 1
            finally:
                client.execute(f"DROP TABLE IF EXISTS {TABLE}")
                client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Load cells test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Load Cells into Tensors', 'error': str(e)})

    def test_decode_equals_matrixone(self):
        """Test the float32 decoding in PyTorch against CAST(v AS vecf32(N))"""
        print("\n=== Decode Equals MatrixOne Tests ===")

        self.results['tests_run'] += 1

        try:
            client = self.connect()
            try:
                self.setup_table(client)

                rows = client.execute(
                    f"SELECT vecblock_binary(v8), vecblock_binary(v4), "
                    f"CAST(v8 AS vecf32({DIM})), CAST(v4 AS vecf32({DIM})) FROM {TABLE} ORDER BY id"
                ).fetchall()

                def parse(text):
                    return [float(x) for x in text.strip("[]").split(",")]

                for name, cell_col, f32_col in [("vecf8", 0, 2), ("vecf4", 1, 3)]:
                    decoded = vecblock.from_cells([r[cell_col] for r in rows]).to_float()
                    expected = torch.tensor([parse(r[f32_col]) for r in rows], dtype=torch.float32)
                    assert torch.equal(decoded, expected), f"{name} decode differs from MatrixOne"
                    self.logger.info(f"✅ {name} decode equals MatrixOne for {len(rows)} rows")

                self.results['tests_passed'] += 1
            finally:
                client.execute(f"DROP TABLE IF EXISTS {TABLE}")
                client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ Decode test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Decode Equals MatrixOne', 'error': str(e)})

    def test_stream_dataloader(self):
        """Test streaming a table through a DataLoader"""
        print("\n=== Stream with DataLoader Tests ===")

        self.results['tests_run'] += 1

        try:
            client = self.connect()
            try:
                self.setup_table(client)

                loader = torch.utils.data.DataLoader(VecBlockDataset(client, TABLE, "id", "v8"), batch_size=None)
                batches = rows = 0
                total = torch.zeros(DIM)
                for ids, vectors in loader:
                    batches += 1
                    rows += len(ids)
                    total += vectors.sum(dim=0)
                assert rows == ROWS, f"streamed {rows} rows, expected {ROWS}"
                mean = [round(x, 4) for x in (total / rows)[:4].tolist()]
                self.logger.info(f"📊 Streamed {rows} rows in {batches} batches; mean of the first 4 dims {mean}")

                self.results['tests_passed'] += 1
            finally:
                client.execute(f"DROP TABLE IF EXISTS {TABLE}")
                client.disconnect()

        except Exception as e:
            self.logger.error(f"❌ DataLoader stream test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Stream with DataLoader', 'error': str(e)})

    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        print("\n" + "=" * 80)
        print("PyTorch Load vecf8 / vecf4 Demo - Summary Report")
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
    demo = PyTorchLoadVecBlockDemo()

    try:
        print("🎯 MatrixOne PyTorch Load vecf8 / vecf4 Demo")
        print("=" * 60)

        # Print current configuration
        print_config()

        # Run tests
        demo.test_load_cells()
        demo.test_decode_equals_matrixone()
        demo.test_stream_dataloader()

        # Generate report
        results = demo.generate_summary_report()

        print("\n🎉 All PyTorch load demos completed!")
        print("\nKey features demonstrated:")
        print("- ✅ vecblock_binary() cells split into FP8 / FP4 tensors with vecblock.from_cells()")
        print("- ✅ Float32 decoding identical to MatrixOne's CAST(v AS vecf32(N))")
        print("- ✅ IterableDataset streaming a table into a DataLoader")

        return results

    except Exception as e:
        print(f"Demo failed with error: {e}")
        return None


if __name__ == "__main__":
    main()
