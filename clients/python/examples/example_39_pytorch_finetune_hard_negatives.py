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
Example 38: PyTorch Fine-Tuning - Hard Negatives Mined in MatrixOne

MatrixOne PyTorch Integration Example

This example demonstrates a retrieval fine-tuning loop around MatrixOne:
- Raw document embeddings stored as vecf8 and loaded losslessly into PyTorch
- Each epoch, the projected document embeddings are written back as vecf8 cells
- Hard negatives for every training batch are mined in MatrixOne with vector_matmul,
  the batch's queries passed as vecf8 cells in a user variable
- Recall@1 of held-out queries, measured in MatrixOne, before and after each epoch

The data is synthetic: the stored raw embeddings are a distorted view of the space the queries
come from, and a linear projection learns to undo the distortion.

Requires PyTorch 2.9 or later (pip install 'matrixone-python-sdk[torch]'). Runs on CPU; MatrixOne uses its GPU engine
for vector_matmul when gpu_mode is on and a device is available.
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

TABLE = "pt_ft_docs"
DIM = 64
DOCS = 2000
TRAIN = 1500  # documents 0..TRAIN-1 have training queries; the rest are held out
BATCH = 250
NEGATIVES = 8
EPOCHS = 5
TEMPERATURE = 0.05


class PyTorchFineTuneDemo:
    """Demonstrates fine-tuning with hard negatives mined in MatrixOne by vector_matmul."""

    def __init__(self):
        self.logger = create_default_logger(sql_log_mode="auto")
        self.results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'unexpected_results': [],
            'recall_at_1': [],
        }
        self.client = None
        self.stored_raw = None
        self.queries = None
        self.held_out = list(range(TRAIN, DOCS))
        self.gen = torch.Generator().manual_seed(30)

    def connect(self):
        """Connect a MatrixOne Client with the configured connection parameters."""
        host, port, user, password, database = get_connection_params()
        self.client = Client(logger=self.logger, sql_log_mode="auto")
        self.client.connect(host=host, port=port, user=user, password=password, database=database)

    def cleanup(self):
        """Drop the table and disconnect."""
        if self.client is not None:
            self.client.execute(f"DROP TABLE IF EXISTS {TABLE}")
            self.client.disconnect()

    def write_column(self, column, emb):
        """Write a [DOCS, DIM] tensor into a vecf8 column as cells, 200 rows per statement."""
        cells = vecblock.quantize(emb.detach(), "vecf8").to_cells()
        for start in range(0, DOCS, 200):
            values = ",".join(f"({i}, {vecblock.cell_sql(cells[i])})" for i in range(start, min(start + 200, DOCS)))
            self.client.execute(
                f"INSERT INTO {TABLE} (id, {column}) VALUES {values} "
                f"ON DUPLICATE KEY UPDATE {column} = VALUES({column})"
            )

    def top_ids(self, queries, k):
        """The ids of the k nearest documents (inner product on the emb column) of each query."""
        with self.client.session() as session:
            session.execute(
                "SET @q = " + vecblock.blob_literal(b"".join(vecblock.quantize(queries, "vecf8").to_cells()))
            )
            text = session.execute(
                f"SELECT vector_matmul({k}, id, emb, CAST(@q AS BLOB), '{{\"query_format\":\"vecblock\"}}') "
                f"FROM {TABLE}"
            ).fetchone()[0]
        return [[int(h[0]) for h in hits] for hits in json.loads(text)]

    def recall_at_1(self):
        """Recall@1 of the held-out queries, measured in MatrixOne."""
        hits = self.top_ids(self.queries[self.held_out], 1)
        return sum(h[0] == t for h, t in zip(hits, self.held_out)) / len(self.held_out)

    def test_prepare_training_data(self):
        """Test storing the raw embeddings and loading them back as the training input"""
        print("\n=== Prepare Training Data Tests ===")

        self.results['tests_run'] += 1

        try:
            self.connect()
            self.client.execute(f"DROP TABLE IF EXISTS {TABLE}")
            self.client.execute(f"CREATE TABLE {TABLE} (id INT PRIMARY KEY, raw vecf8({DIM}), emb vecf8({DIM}))")

            # synthetic data: queries live in the latent space, raw document embeddings are distorted
            latent = torch.nn.functional.normalize(torch.randn(DOCS, DIM, generator=self.gen), dim=1)
            distortion = torch.randn(DIM, DIM, generator=self.gen) / DIM**0.5  # mixes every dimension
            raw = latent @ distortion.T + 0.02 * torch.randn(DOCS, DIM, generator=self.gen)
            self.queries = latent + 0.05 * torch.randn(DOCS, DIM, generator=self.gen)
            self.write_column("raw", raw)

            # the training input is exactly what MatrixOne stores
            rows = self.client.execute(f"SELECT vecblock_binary(raw) FROM {TABLE} ORDER BY id").fetchall()
            self.stored_raw = vecblock.from_cells([r[0] for r in rows]).to_float()
            assert tuple(self.stored_raw.shape) == (DOCS, DIM)
            self.logger.info(f"✅ Stored and loaded {DOCS} raw vecf8({DIM}) embeddings")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Prepare training data test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Prepare Training Data', 'error': str(e)})

    def test_finetune_with_hard_negatives(self):
        """Test fine-tuning a projection with hard negatives mined by vector_matmul"""
        print("\n=== Fine-Tune with Hard Negatives Tests ===")

        self.results['tests_run'] += 1

        try:
            head = torch.nn.Linear(DIM, DIM, bias=False)
            torch.nn.init.eye_(head.weight)
            optimizer = torch.optim.Adam(head.parameters(), lr=0.02)

            self.write_column("emb", head(self.stored_raw))
            baseline = self.recall_at_1()
            self.results['recall_at_1'].append(baseline)
            self.logger.info(f"📊 epoch 0: recall@1 on {len(self.held_out)} held-out queries {baseline:.3f}")

            for epoch in range(1, EPOCHS + 1):
                order = torch.randperm(TRAIN, generator=self.gen).tolist()
                total = 0.0
                for start in range(0, TRAIN, BATCH):
                    batch = order[start : start + BATCH]
                    # hard negatives: the nearest documents under the current embeddings, minus the positive
                    mined = self.top_ids(self.queries[batch], NEGATIVES + 1)
                    negatives = [[d for d in ids if d != pos][:NEGATIVES] for ids, pos in zip(mined, batch)]
                    candidates = torch.tensor([[pos] + neg for pos, neg in zip(batch, negatives)])
                    docs = head(self.stored_raw[candidates])  # [B, 1+N, DIM]
                    logits = torch.einsum("bd,bnd->bn", self.queries[batch], docs) / TEMPERATURE
                    loss = torch.nn.functional.cross_entropy(logits, torch.zeros(len(batch), dtype=torch.long))
                    optimizer.zero_grad()
                    loss.backward()
                    optimizer.step()
                    total += loss.item() * len(batch)
                # write the re-embedded documents back, then measure in MatrixOne
                self.write_column("emb", head(self.stored_raw))
                recall = self.recall_at_1()
                self.results['recall_at_1'].append(recall)
                self.logger.info(
                    f"📊 epoch {epoch}: loss {total / TRAIN:.4f}, recall@1 on held-out queries {recall:.3f}"
                )

            final = self.results['recall_at_1'][-1]
            assert final > baseline, f"recall@1 did not improve ({baseline:.3f} -> {final:.3f})"
            self.logger.info(f"✅ recall@1 improved from {baseline:.3f} to {final:.3f}")

            self.results['tests_passed'] += 1

        except Exception as e:
            self.logger.error(f"❌ Fine-tune test failed: {e}")
            self.results['tests_failed'] += 1
            self.results['unexpected_results'].append({'test': 'Fine-Tune with Hard Negatives', 'error': str(e)})

    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        print("\n" + "=" * 80)
        print("PyTorch Fine-Tuning with Hard Negatives Demo - Summary Report")
        print("=" * 80)

        total_tests = self.results['tests_run']
        passed_tests = self.results['tests_passed']
        failed_tests = self.results['tests_failed']
        unexpected_results = self.results['unexpected_results']
        recall_at_1 = self.results['recall_at_1']

        print(f"Total Tests Run: {total_tests}")
        print(f"Tests Passed: {passed_tests}")
        print(f"Tests Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests / total_tests * 100):.1f}%" if total_tests > 0 else "N/A")

        if recall_at_1:
            print("\nHeld-out Recall@1 by Epoch:")
            for epoch, recall in enumerate(recall_at_1):
                print(f"  epoch {epoch}: {recall:.3f}")

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
    demo = PyTorchFineTuneDemo()

    try:
        print("🎯 MatrixOne PyTorch Fine-Tuning with Hard Negatives Demo")
        print("=" * 60)

        # Print current configuration
        print_config()

        # Run tests
        demo.test_prepare_training_data()
        if demo.stored_raw is not None:
            demo.test_finetune_with_hard_negatives()

        # Generate report
        results = demo.generate_summary_report()

        print("\n🎉 All PyTorch fine-tuning demos completed!")
        print("\nKey features demonstrated:")
        print("- ✅ Training input loaded losslessly from vecf8 cells")
        print("- ✅ Hard negatives mined in MatrixOne with vector_matmul")
        print("- ✅ Re-embedded documents written back as vecf8 cells each epoch")
        print("- ✅ Retrieval quality measured in MatrixOne during training")

        return results

    except Exception as e:
        print(f"Demo failed with error: {e}")
        return None

    finally:
        demo.cleanup()


if __name__ == "__main__":
    main()
