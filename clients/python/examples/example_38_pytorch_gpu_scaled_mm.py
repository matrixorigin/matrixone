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
Example 37: PyTorch GPU - Score vecf8 / vecf4 Cells with torch._scaled_mm

MatrixOne PyTorch Integration Example

This example demonstrates how to multiply MatrixOne cells on the GPU in PyTorch:
- PyTorch's block-scaled GEMM (MXFP8 / NVFP4 tensor cores) takes the cell bytes as its
  operands, with no decoding
- The same top-k from vector_matmul in MatrixOne, with the queries passed as cells in a user
  variable ('{"query_format":"vecblock"}')
- With MatrixOne scoring on the GPU (gpu_mode = 1), the distances are bit-identical: both run
  cuBLASLt's block-scaled GEMM

Requires PyTorch 2.9 or later with a CUDA GPU that has block-scaled tensor cores (Blackwell).
"""

import json
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

TABLE = "pt_gpu_docs"
DIM = 256
ROWS = 4096
QUERIES = 64
K = 10


def gpu_ready():
    """Whether torch._scaled_mm runs block-scaled GEMMs on this machine."""
    if not torch.cuda.is_available():
        print("No CUDA device; skipped.")
        return False
    try:
        x = vecblock.quantize(torch.ones(128, 32), "vecf8").to("cuda")
        vecblock.scaled_mm(x, x)
    except Exception as e:  # an older GPU or PyTorch without block-scaled support
        print(f"torch._scaled_mm has no block-scaled GEMM here ({type(e).__name__}); skipped.")
        return False
    return True


class PyTorchGpuScaledMmDemo:
    """Demonstrates scoring vecf8 / vecf4 cells with torch._scaled_mm against vector_matmul."""

    def __init__(self):
        self.logger = create_default_logger(sql_log_mode="auto")
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
        }
        gen = torch.Generator().manual_seed(29)
        self.docs = torch.randn(ROWS, DIM, generator=gen)
        self.queries = torch.randn(QUERIES, DIM, generator=gen)

    def connect(self):
        """Connect a MatrixOne Client with the configured connection parameters."""
        host, port, user, password, database = get_connection_params()
        client = Client(logger=self.logger, sql_log_mode="auto")
        client.connect(host=host, port=port, user=user, password=password, database=database)
        return client

    def setup_table(self, client, fmt):
        """Create a table with one column of the format and insert the quantized documents."""
        client.execute(f"DROP TABLE IF EXISTS {TABLE}")
        client.execute(f"CREATE TABLE {TABLE} (id INT PRIMARY KEY, v {fmt}({DIM}))")
        cells = vecblock.quantize(self.docs, fmt).to_cells()
        for start in range(0, ROWS, 200):
            values = ",".join(f"({i}, {vecblock.cell_sql(cells[i])})" for i in range(start, min(start + 200, ROWS)))
            client.execute(f"INSERT INTO {TABLE} VALUES {values}")
        self.logger.info(f"📊 Inserted {ROWS} rows of {fmt}({DIM})")

    def compare_with_vector_matmul(self, fmt):
        """Score the stored cells with torch._scaled_mm and with vector_matmul, and compare."""
        client = self.connect()
        try:
            self.setup_table(client, fmt)

            # PyTorch: the stored cells and the query cells, multiplied on the GPU
            rows = client.execute(f"SELECT id, vecblock_binary(v) FROM {TABLE} ORDER BY id").fetchall()
            ids = [str(r[0]) for r in rows]
            docs_gpu = vecblock.from_cells([r[1] for r in rows]).to("cuda")
            q = vecblock.quantize(self.queries, fmt)
            scores = vecblock.scaled_mm(docs_gpu, q.to("cuda")).cpu()  # [ROWS, QUERIES], the dot products

            # MatrixOne: vector_matmul over the same rows, the query cells in a user variable
            with client.session() as session:
                session.execute("SET gpu_mode = 1")
                session.execute("SET @q = " + vecblock.blob_literal(b"".join(q.to_cells())))
                text = session.execute(
                    f"SELECT vector_matmul({K}, id, v, CAST(@q AS BLOB), "
                    f"'{{\"query_format\":\"vecblock\"}}') FROM {TABLE}"
                ).fetchone()[0]
            hits = json.loads(text)  # per query: [[id, distance], ...], distance = -dot

            same_ids = bit_exact = 0
            for j in range(QUERIES):
                order = sorted(range(ROWS), key=lambda r: (-scores[r, j].item(), ids[r]))[:K]
                same_ids += [ids[r] for r in order] == [h[0] for h in hits[j]]
                bit_exact += all(torch.tensor(-h[1], dtype=torch.float32) == scores[int(h[0]), j] for h in hits[j])
            self.logger.info(
                f"📊 {fmt}: top-{K} ids equal for {same_ids}/{QUERIES} queries, "
                f"distances bit-identical for {bit_exact}/{QUERIES}"
            )
            assert same_ids == QUERIES, f"{fmt}: top-{K} ids differ for {QUERIES - same_ids} queries"
            assert bit_exact == QUERIES, f"{fmt}: distances differ for {QUERIES - bit_exact} queries"
            self.logger.info(f"✅ {fmt}: torch._scaled_mm equals vector_matmul")
        finally:
            client.execute(f"DROP TABLE IF EXISTS {TABLE}")
            client.disconnect()

    def test_vecf8_scaled_mm(self):
        """Test MXFP8 torch._scaled_mm against vector_matmul"""
        print("\n=== vecf8 torch._scaled_mm Tests ===")

        self.results['tests_run'] += 1

        try:
            self.compare_with_vector_matmul("vecf8")
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ vecf8 scaled_mm test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'vecf8 torch._scaled_mm', 'error': str(e)})

    def test_vecf4_scaled_mm(self):
        """Test NVFP4 torch._scaled_mm against vector_matmul"""
        print("\n=== vecf4 torch._scaled_mm Tests ===")

        self.results['tests_run'] += 1

        try:
            self.compare_with_vector_matmul("vecf4")
            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ vecf4 scaled_mm test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'vecf4 torch._scaled_mm', 'error': str(e)})

    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        print("\n" + "=" * 80)
        print("PyTorch GPU torch._scaled_mm Demo - Summary Report")
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
    try:
        print("🎯 MatrixOne PyTorch GPU torch._scaled_mm Demo")
        print("=" * 60)

        if not gpu_ready():
            return None

        demo = PyTorchGpuScaledMmDemo()

        # Print current configuration
        print_config()

        # Run tests
        demo.test_vecf8_scaled_mm()
        demo.test_vecf4_scaled_mm()

        # Generate report
        results = demo.generate_summary_report()

        print("\n🎉 All PyTorch GPU demos completed!")
        print("\nKey features demonstrated:")
        print("- ✅ vecf8 / vecf4 cells as torch._scaled_mm operands, without decoding")
        print("- ✅ vector_matmul with the queries as cells in a user variable")
        print("- ✅ Top-k ids and distances bit-identical between PyTorch and MatrixOne")

        return results

    except Exception as e:
        print(f"Demo failed with error: {e}")
        return None


if __name__ == "__main__":
    main()
