# Fused IVF exact-membership Top-K reads

Status: approved for implementation

Related issues: matrixorigin/matrixone#24097, matrixorigin/matrixone#27854

## Problem and invariant

The persisted IVF PRE path currently opens the same entries block separately to
decode the centroid/source-key filter columns, score the surviving embedding
chunks, and materialize winner columns.  The source key can also be decoded
again even though membership already read it.

Required membership must remain exact and must run before Top-K.  Deleted or
invisible rows must never enter the distance heap, and the optimization must
not replace sparse chunk reads with whole decoded embedding materialization.

## Design

An exact membership filter marks its storage filter as eligible for a fused
persisted Top-K operation.  The operation loads object metadata once, pins the
centroid/source-key columns while it computes sorted physical survivors,
applies transaction tombstones, scores only embedding chunks intersecting
those survivors through the existing distance implementation, and copies only
block-local Top-K winner columns.  Output columns already present in the filter
set are copied from the pinned filter entry instead of read again.

The fast path is limited to non-appendable persisted blocks, exact membership,
and supported ascending vector Top-K.  In-memory/appendable blocks, approximate
filters, ordered or descending limits, and unsupported layouts retain the
existing implementation.  There is no SQL, catalog, wire, or on-disk change.

## Ownership and failures

The block operation owns every IOVector and borrowed decoded view until it
returns; output vectors own only copied winner data.  The membership filter and
reader-wide distance heap remain reader-owned.  All error and cancellation
paths release filter, vector-chunk, metadata, tombstone, and output-prefix
resources exactly once.  Empty membership results perform no vector scoring.

## Validation

Unit tests compare the fused and legacy results for empty, sparse, and dense
domains; centroid-prefix intersection; transaction tombstones; NULL and INCLUDE
columns; chunked and legacy vectors; cache states; invalid contracts; errors;
and cancellation.  Deterministic I/O assertions prove one metadata load, no
source-key reread, and no vector-chunk reads without survivors.  Benchmarks
report selective and dense cases without adding wall-clock assertions to UT or
BVT.
