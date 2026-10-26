# Bounded ISCP progress (design proposal)

- Status: **Draft; design review required before implementation**
- Revision: 1
- Tracking issues: [#26973](https://github.com/matrixorigin/matrixone/issues/26973), [#30853](https://github.com/matrixorigin/matrixone/issues/30853)
- Scope: ISCP source collection, progress persistence, and every index consumer

## Problem

An ISCP iteration currently selects a timestamp interval, calls `CollectChanges`
for that interval, and advances each job's watermark only when its consumer has
persisted the entire result. The scheduler caps the *timestamp span* at 30
minutes, but neither that cap nor the 10-second scheduler tick bounds the
number or width of changes in the interval. A busy table can make one sink
transaction and its retry replay very large. FULLTEXT2 search remains stale
until the corresponding consumer commits. Shortening the timestamp interval
alone cannot bound a burst committed at one timestamp.

The current `engine.ChangesHandle` exposes `Next` and `Close`, without a
durable cursor. The combined tail handle gathers all rows at one commit
timestamp before returning a batch. The initial snapshot follows a separate
checkpoint path. These are implementation facts, not a proposed contract.

## Goals and invariants

1. Each independently committed ISCP work unit has an explicit, enforced
   resource budget. A failed unit retries at most that unit; an earlier
   committed unit is never replayed into a sink.
2. A job's durable progress and its derived-index effects commit atomically.
   A Running status or a planned range is not progress.
3. A public watermark denotes the last **complete** source timestamp. It never
   claims coverage of only some rows at a timestamp.
4. Split and unsplit processing produce equivalent final derived state with
   last-writer-wins ordering, including repeated primary keys, multiple
   partitions, and restart/recovery paths. A retry neither skips nor duplicates
   a visible effect. Current `CollectChanges` coalesces some intermediate
   operations; design review must choose whether bounded units split that
   coalesced stream or consume raw chronological changes.
5. New and old CNs cannot concurrently write incompatible cursor formats for
   the same job generation. Existing completed jobs retain their watermarks.
6. Snapshot initialization, tail catch-up, cancellation, and job replacement
   each have a durable restart rule.

This proposal preserves the eventual-consistency search contract in #30853.
It does not make a `MATCH` query wait for ISCP or add a base-table fallback.

## Proposed protocol boundary

### Source reader

Add an opt-in, budget-aware change stream in the engine. Its request fixes the
source table and schema generation, read mode (snapshot or tail), upper source
timestamp, durable resume cursor, and positive work budgets. It returns one
complete unit and its **next cursor before ISCP sends the unit to consumers**.
The reader must account for inserts and deletes, variable-width values, decode
buffers, and spill. Existing `CollectChanges` callers retain their behavior.

For tail data, order committed changes by timestamp and a stable logical key.
All operations required for one `(timestamp, primary key)` group stay together,
with delete before insert. A cursor records the last completed timestamp plus,
while inside a timestamp, the last completed logical key. It must remain valid
after object compaction; a physical block or row offset is unsuitable. The
reader must not first materialize all rows at a timestamp and only then apply
the budget. A source view that cannot be reconstructed after restart must be
retained or the job must fail closed, without advancing progress.

For the initial snapshot, fix one source snapshot timestamp and enumerate
logical rows in stable primary-key order with its own cursor. The catalog
watermark remains empty until the snapshot and its initialization effects
finish. Tail processing then starts after that snapshot timestamp. Retention
and schema mapping for a resumed snapshot must be defined before this path is
implemented.

### Durable progress and sink commit

Persist a versioned cursor per job generation alongside existing status and
watermark. Each consumer transaction writes one unit's derived-index effects
and the next cursor with a compare-and-swap on job ID, previous LSN, previous
watermark, and previous cursor. An in-flight timestamp remains invisible to
coverage readers; after its final unit commits, the same transaction advances
the public watermark to that timestamp and clears the in-flight cursor.
An uncertain commit is resolved by rereading durable progress, not by retrying
from an in-memory planned cursor. A stale generation or schema mismatch is
fenced. Shared source fanout is permitted only for jobs with identical durable
source positions; each job owns its sink transaction and progress separately.

The ordinary ISCP tail writers already couple final effects and watermark in
one transaction. They need a per-unit checkpoint method before partial
timestamps can be used. FULLTEXT2 must persist its tail frames and cursor
together, preserve frame ordering, and expose a build timestamp no later than
the last fully applied source timestamp. HNSW currently saves a dirty whole
model at the end of an iteration; cuVS writers retain pending changes across
transactions. Both require consumer-specific bounded persistence designs before
the shared source protocol can enforce a bound for **all** consumers.

## Budget and oversized-unit decision (blocking)

A source-event count alone does not bound transaction bytes or work: a single
TEXT value can be very large, and one HNSW save can rewrite a large existing
model. Conversely, a byte cap that rejects one oversized logical row would
leave the job unable to progress. The implementation needs a precise answer to
all three questions:

1. Which measured costs are bounded per commit: source rows, source bytes,
   decoded bytes, spill bytes, sink output bytes, sink transaction bytes, and/or
   execution time? What are the default and maximum values?
2. How is one logical row or `(timestamp, primary key)` operation group that
   exceeds the budget streamed and checkpointed without a partial visible row?
3. How are HNSW full-model persistence and cuVS pending records made bounded
   under the same rule? An exception for them must be explicit and must not be
   described as a strict all-consumer bound.

The design review must settle these before a production implementation is
approved. Silently admitting an unlimited exceptional unit repeats #26973's
failure mode; repeatedly rejecting it creates permanent catch-up starvation.

## Alternatives considered

| Approach | Benefit | Why it does not close #26973 |
| --- | --- | --- |
| Reduce the 30-minute timestamp cap | Smaller normal backlog chunks | One timestamp can contain an arbitrary burst; snapshot bypasses the cap. |
| Poll more often than every 10 seconds | Reduces idle detection delay | Raises background load and does not bound a catch-up iteration. |
| Stop after N `ChangesHandle.Next` batches | Small ISCP-only diff | The handle can already have materialized a large same-timestamp batch; there is no durable mid-timestamp resume cursor. |
| Bound only in-memory replay with `ChangeRangeLimit` | Controls one replay memory path | It spills excess rows and does not bound total work or the sink transaction. |

## Validation and delivery gates

- Demonstrate a dense interval and one oversized timestamp, with measured
  source/decode/spill/sink usage below the accepted per-unit budgets and
  monotonic committed progress.
- Compare split and unsplit final index contents for insert, update, delete,
  repeated key, cross-partition, and logical-timestamp boundary cases.
- Inject failure before and after effect/cursor commit, during fanout, after
  Running status, and after an ambiguous commit; restart from catalog state.
- Exercise initial snapshot continuation, schema change, compaction/GC,
  job replacement, and a rolling mixed-version deployment.
- Run a FULLTEXT2 catch-up workload and report commit-to-index visibility lag,
  transaction duration, frame count, and query load cost. Lower lag is a
  measured outcome, not implied by smaller units alone.

No implementation PR should claim to fix #26973 until the blocking budget and
consumer-persistence decisions are resolved and the corresponding tests pass.
