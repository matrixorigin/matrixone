# ISCP progress for partitioned table catch-up

## Problem

Issue #26973 describes an unbounded dirty-source iteration: ISCP reads a whole
`[fromTS, toTS]` range and advances its watermark only after the sink commits.
The existing 30-minute timestamp-span limit does not bound its event count.
This patch removes one concrete source of repeated work during catch-up. It
does not implement a durable within-timestamp cursor or close #26973.

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

This does not add sink transactions or repeat HNSW model saves, cuVS flushes,
or fulltext2 spool persists during a successful iteration. A dense source
commit at one timestamp and a dirty 30-minute range remain unbounded. Solving
that part of #26973 requires a restart-stable source cursor or another durable
bounded-work protocol.

Index sync jobs now select their own ranges from their own watermarks and run
in separate iterations, including when two index jobs or a normal consumer
have identical ranges. The scheduler caps each non-snapshot range at the
existing 30-minute timestamp span before dispatch or clean-table watermark
advance. Initial snapshots use the worker's snapshot timestamp and are not
bounded by this catch-up window.

## Validation

Run focused tests on the designated remote host for multi-batch insert and
delete `Tail_wip` groups, incomplete groups and cleanup, cancellation, and
snapshot/tail separation. Run the ISCP package tests for neighboring behavior.
No local builds or tests.
