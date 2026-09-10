# IVF execution observability

Status: approved for implementation

Related issue: matrixorigin/matrixone#24097

## Problem and invariant

`EXPLAIN ANALYZE` reports IVF search rounds, but it does not show how much
hidden-table or vector work each search performed.  Operators therefore cannot
distinguish routing, storage filtering, vector decoding, and Top-K work without
profiling a CN.

Standalone vector scans must expose a bounded, execution-owned summary of that
work.  Ordinary queries and correlated APPLY must not allocate or retain the
summary, and diagnostics must survive physical-plan JSON transport without new
wire fields.

## Design

Each standalone IVF reader owns one local summary.  Hidden-relation scans add
reader, block, row, and elapsed-time totals by table role.  Storage Top-K adds
storage-filter input/output rows, vector rows admitted for scoring, consumed
vector chunks and cache hits, compressed/decoded bytes, and block-local winner
rows.  Counters are plain integers because a reader is called serially; DOP
uses independent summaries and the EXPLAIN renderer aggregates them.

At completion the reader encodes one versioned `plan.Query.Headings` value and
hands it to the existing `ExplainDiagnosticReader` drain.  The carrier contains
no expression oneofs and is bounded by physical reader count.  Correlated APPLY
keeps collection disabled, as it does for search-round diagnostics.

## Ownership and failure behavior

The plan reader owns the summary until `TakeExplainDiagnostics` transfers the
encoded value.  Close discards undrained state.  Error and cancellation paths
do not publish a partial summary.  ObjectIO borrows an optional pointer only for
the synchronous Top-K call and never retains it beyond the reader generation.
Malformed or future diagnostic versions remain ordinary background queries.

No SQL, catalog, persistent, or client protocol changes are introduced.  The
feature is enabled only by the existing standalone EXPLAIN collection flag and
therefore needs no rollout switch or mixed-version migration.

## Validation

Unit tests cover encoding and remote JSON round trips, malformed carriers,
bounded DOP aggregation, one-time draining and close, disabled collection,
storage-filter and Top-K counters, cache/chunk accounting, and stable text
rendering.  Existing vector EXPLAIN tests remain the public consumer check.
