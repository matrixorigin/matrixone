# Hash aggregation throughput with atomic publication

Status: design review candidate, revision 1.
Work item: user-requested systematic exact aggregation optimization, prompted by
the full ClickBench Q5 comparison. Delivery is one implementation branch/PR;
the PR link is assigned when published.
Base: `e9dc8c420818732d6a5691bef1b7ebf6e4d8a87f` (verified remote main).

## Problem and evidence

Q5 is `SELECT COUNT(DISTINCT UserID) FROM hits`. The official projection contains
99,997,497 signed, non-null 64-bit keys and 17,630,976 distinct values. At
`66c9e762`, four pinned cores, three warmups and ten sequential measured runs,
MO takes a median 2.707 seconds and 8.98 process CPU seconds; DuckDB
`cc9648d736fd7e4fb2fc813cba6f6d34932c3386` takes 0.634 seconds and 2.42 CPU
seconds. Neither spills or swaps. The newer base leaves the hash/group code
unchanged, but its complete binary is rebuilt and remeasured for comparison.

The earlier 50M non-null CPU profile attributes 40.38% flat CPU to
Int64HashMap.PlanInsertBatch. Samples concentrate on loading existing cells,
which motivates, but does not itself prove, a memory-latency hypothesis.
Group key publication, encoding, allocation preflight and resizing are secondary
costs. Local aggregation reduces the 100M input to 20,297,379 rows before final
hash partitioning; removing that stage blindly would discard useful reduction.

Evidence lives in the retained `clickbench-q5-100m-20261028` and
`clickbench-q5-50m-20261027` benchmark directories. Performance measurements are
separate from functional UT/BVT fixtures.

## Contract and scope

The work spans hashtable, hashmap encoding and group-key publication. It earns
the design gate because it materially changes a hot execution path across
package boundaries. The overall scope is one optimization of that existing
contract, not a new planner, aggregate family or resource controller.

For every input batch, preserve exact equality, first-occurrence group IDs,
NULL/non-matching semantics, output values, insertion flags and cardinality.
Preview remains immutable. Every fallible allocation precedes publication;
capacity rejection permits retry at the same input offset without lost or
duplicated rows. Commit rejects stale owner/epoch/table versions before mutation.
All existing collision handling and rollback checks remain authoritative.

No persisted, wire, SQL, configuration or hash-code format changes are planned.
Mixed-version boundaries retain the existing key grammar. No new worker, cache,
lock, channel, cross-batch borrowed pointer or unbounded retained state is added.
Empty input, empty/all-NULL groups, signed extrema, zero, duplicate values,
collisions, growth, failed admission, reset and reuse remain valid.

## Selected approach

1. Batch independent first-cell loads during immutable hash insertion planning.
   The current scalar probe waits on each key before starting the next key.
   For sufficiently large tables and bounded batches, load first cells into
   call-local fixed scratch before resolving collisions and tentative inserts
   in input order. The table is immutable during preview, so these cell copies
   remain valid. A copied empty cell is still checked against earlier planned
   inserts; collision chains beyond it use the live, unchanged table. Keep
   scalar handling for small tables/batches. Apply only to representations with
   measured benefit; integer and string-state plans have separate cost evidence.
2. Give one full-width, non-null integer vector a direct key path. The eight-byte
   integer representation is already the H8 grammar. Either copy it in one
   operation or borrow its storage for synchronous hashing, whichever wins.
   Constants, NULL markers, floats, grouping sentinels, composites, CHAR padding
   and legacy codecs retain the existing implementation. Borrowed pointers must
   never survive the call; plans own hashes and mappings, never source pointers.
3. Avoid redundant group-key work when the exact insertion plan proves there
   are no new groups, and carry already-proven selection counts across the
   publication boundary where useful. Existing-row aggregate updates and string
   source provenance merging still run; duplicate-only input is not permission
   to skip them. Keep every fallible metadata preflight before hash commit.

These are hypotheses to evaluate independently and together, not promises to
retain three patches regardless of their measured value. Compiler escape
analysis and benchmark allocation counts must confirm bounded scratch remains
allocation-free in the steady state. Any material algorithm change beyond these
steps requires a revised design review before implementation.

## Alternatives and precedent

The status quo provides strong admission/retry safety but serializes random
accesses and repeats generic encoding/publication work. Removing preflight or
reserving a worst-case batch without exact duplicate detection can reject valid
duplicate-only input under a tight budget and is rejected.

DuckDB's inspected source revision uses radix-partitioned aggregation, adaptive
local tables, and batch probing (src/execution/aggregate_hashtable.cpp and
radix_partitioned_hashtable.cpp). That is useful algorithmic precedent, not a
drop-in contract: MO has distributed partials and exact allocation-admission
rules. Replacing MO's table ownership with a partitioned aggregate subsystem
adds transitions, spill owners and compatibility decisions; first measure the
common primitive improvements against that larger alternative.

Fusing final DISTINCT and COUNT could avoid materializing 17.6M output rows,
but it requires an explicit partition-disjointness and NULL contract through
compile and remote execution. It is a separate possible follow-up if profiles
after this work justify it. It cannot compensate for a slow per-key probe alone.
Similarly, preallocation or changing the load factor trades resident/peak memory
for CPU and needs a separate capacity argument rather than a Q5-only constant.

## Ownership and failure closure

Call-local snapshot scratch is bounded by a compile-time batch cap and dies on
all returns. It reads only validated table indices; skipped rows do not become
insertions. The same planning epoch owns hashes, slots and flags. Resize
invalidates the table version and requires the existing complete replan.

The vector retains ownership of its key storage. Source vectors stay alive for
the synchronous encode/hash operation, and no mutation is permitted. Iterator
scratch reset remains correct when switching between optimized and generic
input or between Find, Insert, Preview and commit.

Group continues to own key batches and aggregate state. On allocation failure,
preflight cancellation, resident spill, retry and final cleanup keep their
existing owners. String provenance can change even for an existing group, so its
preflight/apply/finalize path is mandatory. No new asynchronous transition exists;
cancellation and pipeline terminal behavior are unchanged.

There are no authorization or tenant-boundary changes. Bounded scratch adds no
input-amplified retained allocation. Rollback is reverting local algorithms;
stored data and remote peers require no migration or feature gate.

## Validation and acceptance

| Closure | Risk | Cheapest discriminating proof |
| --- | --- | --- |
| Integer/string probe planning | R2 | Independent sequential-map oracle; collision/wraparound, skipped rows, empty/mixed/duplicate batches, stale plans, growth/retry; owning hashtable suite |
| Integer vector path | R2 | Generic-codec parity and transitions across integer/constant/NULL/composite/float inputs; owning hashmap suite |
| Group publication | R2 | Existing allocation-failure/provenance/spill/reuse oracles plus any uncovered duplicate-only transition; owning group suite |
| Public composition | R2 | Small SQL parity cases for DISTINCT/GROUP BY/count, NULL, strings, skew, empty input; existing forced-spill tests and service parity |
| Performance | R2 | Reproducible microbenchmarks and pinned-core SQL baseline/candidate ABBA runs with exact data/result checks |

Search and extend existing cases before adding fixtures. Tests use the smallest
cardinality that reaches the changed branch, not 100M rows. Performance data is
generated deterministically outside UT and reports allocations, RSS, CPU,
wall time, spill and process swap. Cover both low and high cardinality, clustered
duplicates and shuffled keys, hits/misses, nullable/non-null and one/four workers.
No timing assertion belongs in a unit test.

Target a material improvement (initial acceptance target: at least 25% lower
100M Q5 wall time) with exact results, no spill/swap and no material resident
memory increase. Investigate repeatable control-workload regressions above 5%;
retain a scalar/generic guard or drop the offending optimization. The target is
not a guaranteed DuckDB parity claim. Record actual gains and residual hotspots.
Use final-scope package checks, public-path evidence and mo-self-review; tests
need reruns only when their relevant implementation or validation inputs change.

## Design review record

Review is a distinct phase before production edits. Record the reviewed commit,
invariant/ownership findings and PASS/REQUEST_CHANGES in the work evidence, then
update this status. No production implementation is approved by this draft.
