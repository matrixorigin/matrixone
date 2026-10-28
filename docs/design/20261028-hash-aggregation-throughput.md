# Hash aggregation throughput with atomic publication

Status: revision 2 passed design self-review before production implementation.
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

1. Keep insertion planning sequential, hoist immutable segment address masks,
   and advance directly when an existing or tentative mapping is resolved.
   This removes repeated address computation and the redundant found-state
   transition without adding another full batch scan, cell copies, table state
   or special treatment for consecutive values. Integer and string-state
   equality checks have separate cost evidence; only the integer rewrite is
   retained because the string candidate regressed low-cardinality controls.
2. Give one full-width, non-null fixed vector a direct copy path. Its eight-byte
   representation is already the H8 grammar for integers, dates and decimals.
   Copy into iterator-owned scratch. Constants, NULL markers, floats, composites,
   CHAR padding and legacy codecs retain the existing implementation. No plan
   retains a pointer to input-vector storage.
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

The existing call-local tentative-slot overlay remains bounded and dies on
all returns. Planning reads only validated table indices; skipped rows do not
become insertions. The same planning epoch owns hashes, slots and flags. Resize
invalidates the table version and requires the existing complete replan.

The vector retains ownership of its key storage. Source vectors stay alive for
the synchronous encoding operation, which copies their bytes without mutation. Iterator
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
The user explicitly requires a general optimization. Make clustered keys and a
random permutation of the exact same multiset a mandatory comparison; an
adjacent-repeat shortcut cannot be the main source of the claimed gain. Include
all-unique keys, hot working sets and full-table random working sets. Use fixed
prehashed keys for primitive comparisons so process-local random hash seeds do
not confound table layout. Report string/composite/NULL controls separately.
No timing assertion belongs in a unit test.

Target a material improvement (initial acceptance target: at least 25% lower
100M Q5 wall time) with exact results, no spill/swap and no material resident
memory increase. Investigate repeatable control-workload regressions above 5%;
retain a scalar/generic guard or drop the offending optimization. The target is
not a guaranteed DuckDB parity claim. Record actual gains and residual hotspots.
Use final-scope package checks, public-path evidence and mo-self-review; tests
need reruns only when their relevant implementation or validation inputs change.

## Design review record

Reviewed design source: commit `456c0b1844` (design-only). Decision: PASS in a
distinct self-review phase before production edits; final implementation review
remains required. No independent or external approval is claimed.

Decisions: snapshotting is restricted to an immutable preview, with a bounded
batch and scalar handling outside its measured domain. Sequential tentative
inserts remain authoritative even when a snapshot cell was empty. Key borrowing
is synchronous and integer-only; float/NULL/CHAR semantics stay generic. A
duplicate-only group shortcut must preserve existing-row provenance and
aggregate preflight/update. Existing ownership/version/admission boundaries are
unchanged. No unresolved design blocker remains; measured regression or an
escaping scratch allocation rejects the corresponding implementation.

Revision 2 design self-review: the first-cell experiment may publish confirmed
existing matches to caller-owned output scratch before sequential resolution of
unmatched rows. It does not publish table state, alter group IDs or bypass the
planned-slot overlay. The additional same-multiset/order controls are mandatory.
The full cell-copy experiment improves random accesses but regresses skew/hot
controls; it is not accepted for delivery. No new owner, resource bound or
compatibility change is introduced by the alternative. Decision: PASS before
implementing the output-scratch variant; implementation performance remains open.

Experiment update: neither first-cell copying nor a separate confirmed-hit pass
meets the hot-cache control criterion. Both are rejected. The candidate instead
keeps a single sequential probe pass, hoists immutable segment addressing, and
advances immediately when a mapping is resolved. The planned-slot overlay and
all validation/commit boundaries are preserved for both integer and string
states. This is a simplification of the existing algorithm, with no new state or
layout. Design self-review: PASS; numeric acceptance remains subject to SQL A/B.

String probe candidate rejected after fixed-layout benchmarks: the larger
hash-state representation incurred low-cardinality regressions above 5% despite
better large-table misses. The string production algorithm stays at baseline;
its benchmark remains a control. Integer probes passed an additional ABBA
comparison after an initially noisy 1M-hit result. No adjacent-repeat shortcut,
cardinality-specific encoding or query-pattern guard is retained.

## Revision 3: bounded local reduction experiment

The first implementation passes the owning suites and a shared-host SQL ABBA
comparison: Q5 improves from 2.5202 to 2.3395 seconds (7.2%), below the 25% target.
The candidate profile still attributes 43.18% flat CPU to insertion planning
and 12.89% cumulative CPU to in-place rehashing. Hardware cache/TLB counters are
unavailable; locality remains a hypothesis, not a measured cache-miss claim.

Experiment: bound the resident working set of an existing pure-key local Group
whose downstream Group completes exactly the same keys after a local shuffle.
Reuse its established byte threshold and Build -> Eval -> EvalReset path. Start
with a 2 MiB working-set hypothesis, then compare several sizes; no query, key
name, adjacency, or cardinality-specific branch is allowed. Keep a smaller
explicit threshold. This is a scheduling target checked after an input batch,
not a replacement for hard allocation admission. The final Group retains its
session threshold. No disk spill is introduced by a normal partial flush.

The compiler must prove the existing pure-key pair contract and a single-CN
exchange with local sources. Remote exchange retains full local reduction to
avoid unmeasured network amplification. Ordered/grouping-set plans and
coordinator relocation remain outside this experiment. Reuse placement checks
with local-aggregation omission instead of duplicating their logic. The chosen
threshold uses the existing serialized SpillMem field; no protocol or storage
change, new resource owner, worker, or operator state is introduced. The explicit
local threshold replaces automatic soft spill scheduling for that operator;
the query/CN allocation ledger still owns hard admission and capacity recovery.

The union of emitted local key batches must equal the local input's distinct
keys; duplicate keys between batches are permitted only because the downstream
Group merges them. Group owns each returned batch until the next Call. EvalReset
frees the consumed hash, key batches and aggregate state before reading another
input; EOF and cancellation retain the existing output/Free cleanup edges.
Preflight and capacity-retry behavior are unchanged, including rare hard-budget
recovery paths. Source provenance must merge across flushes exactly as across
workers. There are no new waits; existing shuffle backpressure bounds queued
batches. The working-set target may be exceeded by one input batch and transient
preflight reservations, both still subject to the existing hard budget.

Alternatives are the current unbounded local reduction, planner omission (can
increase exchange traffic and requires a cost estimate), and a new partitioned
aggregate subsystem (more owners and transitions). Bounded partial output is
the smallest experiment that addresses the measured growing-table costs while
preserving existing publication and distributed completion contracts.

Require a full shuffled permutation of the official Q5 multiset, in addition to
synthetic low/high-cardinality, unique, skew, NULL, string and composite controls.
Existing 8M-key SQL plans can omit local aggregation, so verify plans actually
exercise the changed stage. Check intermediate row amplification, wall/CPU,
allocation/RSS, exact output, spill and swap. Reject the experiment if its gain
depends on clustered order or repeats a control regression above 5%. Extend
existing compile placement tests and test partial output -> reset -> resume/EOF,
cross-flush duplicates/provenance, cancellation and final allocation release.

Revision 3 design review is pending; production implementation of this
experiment must follow a distinct review phase.
