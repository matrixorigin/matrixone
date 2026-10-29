# ISCP progress for partitioned table catch-up

## Problem

Issue #26973 describes an unbounded dirty-source iteration: ISCP reads a whole
`[fromTS, toTS]` range and advances its watermark only after the sink commits.
The existing 30-minute timestamp-span limit does not bound its event count.
This patch removes one concrete source of repeated work during catch-up and
adds a soft row target for tail index jobs. It does not implement a durable
within-timestamp cursor or close #26973. See
[the row-budget design](20261029-iscp-row-budget-range.md) for the range
selection contract and its limits.

The combined change reader for partitioned tables legitimately returns
`Tail_wip` for a run of pure inserts or deletes, followed by `Tail_done`.
ISCP currently rejects every `Tail_wip`. A partitioned-table catch-up with
this shape retries the same range and cannot advance its watermark.

## Change and invariants

The ISCP change-reader owns returned batches from the first `Tail_wip` until
`Tail_done`, then publishes the whole group once. The existing `AtomicBatch`
keeps row order by commit timestamp and primary key. Consumers continue to
apply deletes before inserts at the same timestamp, and commit their sink and
watermark at the existing iteration boundary. An incomplete group on EOF,
error, malformed hint or cancellation is discarded and cannot advance a
watermark.

Tail grouping alone does not add a sink transaction within an iteration.
Row-budgeted catch-up can create more iterations and therefore more HNSW model
saves, cuVS flushes, and fulltext2 spool persists. Those costs need
measurement. A dense source commit at one timestamp remains unbounded under
the first-commit exception. Solving that part of #26973 requires a
restart-stable source cursor or another durable bounded-work protocol.

Index sync jobs select ranges from their own watermarks. In-step index jobs
with the same row target share one read; lagging jobs catch up separately.
Index jobs never share with normal consumers. The scheduler caps every
catch-up range at a 30-minute timestamp span before dispatch or clean-table
watermark advance. Initial snapshots use the worker's snapshot timestamp and
are not bounded by this catch-up window.

## Validation

Run focused tests for multi-batch insert and delete `Tail_wip` groups,
incomplete groups and cleanup, cancellation, snapshot/tail separation, and
row-budget range selection. Run the ISCP package tests for neighboring
behavior and measure sink persistence costs before rollout.
