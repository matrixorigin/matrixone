- Status: draft (pending independent approval)
- Start Date: 2026-10-16
- Revision: 8
- Implementation PR: https://github.com/matrixorigin/matrixone/pull/30184
- Issue: https://github.com/matrixorigin/matrixone/issues/30155
- Original implementation baseline: `fae17ae98d93f2b3e5e251dbc5649d643513af03`
- Rebase baseline: official main `47b9690d7caec56b589fdefaf0684177412cffb2`
- Design feedback: https://github.com/matrixorigin/matrixone/pull/30184#pullrequestreview-5541720784

# Signed-zero set membership and scalar reaggregation

## Decision and scope

This revision describes the complete proposed execution and compatibility
contract, including the scalar fallback added after the initial membership fix.
Global occurrence numbering, cross-operator reaggregation and execution/persisted
expression admission trigger the design-first gate. The earlier ordinary-fix
exemption does not cover this expansion. Revision 3 supplied the missing design;
revision 4 retains that architecture and mechanically advances set-key admission
after official main allocated version 116;
revision 5 adds the local capacity protocol, receipt and composed lifecycle tests;
revision 6 extends accounted resource, input-distribution and composed fault evidence;
revision 7 defines the intended deployment qualification/rollout contract and
executes its sparse and concurrent boundary cases with enforced benchmark budgets.
revision 8 reconciles upstream function allocation and refreshes evidence after
the official-main rebase. These revisions do not assert repository approval or advance implementation approval.

The proposal reuses existing WINDOW, JOIN, GROUP and SINGLE owners. It adds no
wire field, worker, queue, persisted occurrence identity or recovery protocol.
Its principal tradeoff is global outer materialization and potentially quadratic
candidate work. The capacity envelope and independent design acceptance remain
explicit pending items below. Publishing this documentation repair is distinct
from accepting that tradeoff for rollout.

## Membership contract

Issue #30155 reports UNION/INTERSECT/EXCEPT row counts of 1/1/0 for opposite
signed zeros in MatrixOne, versus 2/0/1 in MySQL 8.0.46; FLOAT and DOUBLE both
reproduce it. Scalar equality remains true and UNION ALL retains both rows on
both servers. This proposal follows that observed MySQL compatibility contract,
not a claim that IEEE scalar equality itself distinguishes signed zeros. The
goal is matching set membership while preserving unrelated equality domains.

FLOAT and DOUBLE set operations compare their physical membership keys while
returning the original visible values. Positive and negative zero remain distinct;
NULL, NaN normalization, ALL multiplicities, and the shared GROUP BY/DISTINCT
contracts retain their existing owners. UNION ALL needs no membership key.
The internal `set_operation_float_key` registry identity is 600, with overloads
0 (FLOAT) and 1 (DOUBLE). No user-visible column is added.

There are three planning routes; their admission proofs must remain separate:

1. The existing equality-correlated route retains correlation partitioning.
2. Identical, nonprepared, effect-free UNION arms with an aggregate or literal
   LIMIT may reuse one evaluation, preserving that arm's semantic boundaries.
3. An admitted nonidentity-correlated scalar UNION uses the occurrence-based
   fallback described below. It must not substitute hidden inner correlation
   columns for visible membership, or equal outer values for outer occurrences.

For route 2, an ungrouped aggregate or literal LIMIT of zero or one needs no
DISTINCT. This preserves its signed-zero representative and leaves aggregate
empty-input rows, COUNT zero reconstruction, and per-correlation pagination with
the existing single-arm decorrelation owner.

Grouped or multirow boundary arms reuse one evaluation followed by set-specific
DISTINCT. A fresh limit-free forwarding projection keeps the arm's original
GROUP BY, ORDER BY, LIMIT and OFFSET below duplicate elimination. The existing
DISTINCT-to-AGG owner hashes hidden physical membership keys and returns original
visible values. Correlated pull-up appends raw identity keys to that physical
key list, so distinct outer partitions cannot merge; floating identities retain
scalar equality. Integer-only arms use the same boundary-preserving projection
with ordinary DISTINCT, whose grouping includes those identities. The final
SINGLE owner still rejects multiple members.

An unordered LIMIT retains the established single-evaluation choice without
promising deterministic input ordering. Ordinary multirow arms, volatile
expressions, parameters and extra producer steps keep the existing typed set
route. Existing non-equality grouped aggregate and pagination rejection boundaries
remain in force. Boundary-arm reuse is not permission to broaden route 3.

## Scalar fallback admission and execution

The selector in [correlated_scalar_set.go](../../pkg/sql/plan/correlated_scalar_set.go)
requires one visible scalar output and a UNION root without root LIMIT/OFFSET,
aggregate nodes or volatile expressions. Supported PROJECT/SORT wrappers have
no LIMIT/OFFSET; PROJECT must forward the single child output with the same type.
A correlated filter not recognized as an equality identity selects reaggregation.
This selector is not proof that arbitrary nested pagination is admissible:
existing decorrelation checks still apply. Unsupported outer-output collection,
subquery shape or residual deep correlation fails closed with NYI 20102 rather
than inventing an occurrence identity.

```text
current outer stream
  -> global unpartitioned ROW_NUMBER (occurrence ID)
  -> LEFT JOIN (inner candidate bag + true presence marker)
  -> AGG membership by (occurrence ID, marker, physical visible key)
  -> AGG per occurrence: count(marker), any_value(visible value)
  -> SINGLE against constants {0, 1}
  -> scalar visible result
```

Numbering happens on the current outer stream, after its derived projection,
DISTINCT, UNION ALL, JOIN or earlier scalar rewrite. Duplicate values and
join-expanded occurrences receive different IDs. A base-table Row_ID or grouping
by visible outer values would incorrectly merge such occurrences. The outer
producer is evaluated once; numbering order need not be stable across executions.
The compiler's `compileWin` creates a merge scope, so the unpartitioned numbering
is global within this execution, rather than independently restarting per worker.

The LEFT JOIN evaluates correlated predicates against candidate rows. A true
marker identifies a real match even when the visible member is NULL; an unmatched
outer occurrence has a NULL marker. The first logical AGG carries outer columns,
ID, marker and original visible value, but its `GroupByHashKey` selects only ID,
marker and the visible physical membership key. Hidden inner correlation slots
are not members. The second logical AGG carries outer columns and ID, hashes only
ID, and computes COUNT(marker) and ANY_VALUE(original visible value). Planner
aliases preserve later references through both stages.

An unmatched occurrence therefore produces count 0 and scalar NULL; one real NULL
produces count 1 and scalar NULL. Repeated equal members produce count 1, while
opposite zero signs produce count 2. The final SINGLE join matches constant 0
for counts 0/1 and both constants for counts above 1, delegating MySQL error
1242/21000 to the existing cardinality owner. ANY_VALUE supplies a representative
only when the scalar cardinality permits one; it does not choose a successful
answer from multiple distinct members. The constant relation contains two rows,
independent of candidate cardinality.

## Resource ownership and terminal paths

These are delegated existing execution contracts, not a claim that this RFC
revision adds or independently retests cleanup. The planner must not release
executor-owned buffers, and carried/borrowed vectors follow their operator's
batch lifetime.

| Resource or state | Effective owner and lifetime | Success, error, cancel and reuse contract |
| --- | --- | --- |
| Tags, marker/key expressions, outer-output aliases | QueryBuilder/query plan | Planning failure discards the plan; aliases and occurrence IDs are not catalog state. A new execution creates fresh IDs. |
| Global merge inputs, buffered outer batch, window vectors/executors/results | `compileWin` scope and [Window](../../pkg/sql/colexec/window/types.go) | Unpartitioned `receiveAll` copies input using `AppendWithCopy`. Returned batches live until the next Call/Reset. Reset clears generation state and releases accounted backing; Free releases terminal batch/vector/executor storage, including error exits. |
| Candidate build state, condition executors, match tracking and output batches | Join compiler's selected build/probe owners, including [LoopJoin](../../pkg/sql/colexec/loopjoin/types.go) for general predicates | Existing build/message ownership governs transfer and cancellation. Reset discards generation-bound state; Free releases terminal storage. The planner's SpillMem field is not a separate cleanup or spill owner. |
| Membership/outer group keys, representatives, aggregate states, spill/reload state | [Group and MergeGroup](../../pkg/sql/colexec/group/types2.go) | Their Reset/Free owners release or reset hash/aggregate state, projections, spill files/reservations and reload staging under existing contracts. Each logical AGG may compile to multiple physical owners. |
| Scalar cardinality check and two-row constant input | Existing SINGLE join and value-scan owners | Zero/one-member success and multiple-member 1242 share ordinary pipeline cleanup; no special retry or retained scalar state is added. |
| Partial Prepare, upstream waits, deadline/cancellation and scope shutdown | Process/scope lifecycle and each initialized operator | Errors propagate to scope teardown; cancellation must release build/message waits and terminate upstream work. Partial initialization must remain safe for cleanup. Reset/re-execution must not retain row-number counters, buffered rows or failed-generation execution resources. Immutable plan tags/aliases may be reused; occurrence identity is scoped to the new execution. |

Restart discards all execution-local numbering and aggregation state. It neither
repairs nor removes persisted expressions. Required forced-budget and interrupted
execution acceptance is listed separately below; ordinary successful SQL tests
do not establish all of these terminal-path properties for the composed plan.

## Global merge and two-aggregation capacity model

For one fallback invocation, let `O` be outer occurrences, `I` the total candidate
rows retained across inner bags, `M` qualifying occurrence/candidate pairs, `Z`
unmatched outer occurrences, and `U` distinct matched visible memberships summed
across outer occurrences. Equal members in different occurrences count separately.

| Stage | Logical cardinality or work |
| --- | --- |
| Global numbering | `O` rows, copying the carried outer columns and adding one ID |
| Candidate matching | At most `O * I` pairs examined for a general range predicate, even when few qualify; LEFT JOIN emits `M + Z` |
| First logical AGG | Consumes `M + Z`, emits `U + Z`, with `U <= M` |
| Second logical AGG | Consumes `U + Z`, emits `O` |
| SINGLE | Two-row constant relation; at most two matches per occurrence before the cardinality error |

This is two **logical** AGGs, not necessarily two physical Group operators.
Partial/merge grouping, CN placement and concurrency change replication and
lifetimes. Deduplication after matching does not remove candidate-predicate work.
Empty outer input gives no scalar rows; an empty candidate bag gives `Z = O`.

Let `W_o` be carried outer-row width, `W_i` candidate width, and `W_k` membership
key/representative width. Resident pressure includes full outer buffering
proportional to `O * W_o`, retained inner/build payload up to `I * W_i`,
first-stage group payload up to `(U + Z) * (W_o + W_k)` carrying outer columns
as well as keys/representatives, and second-stage payload proportional to
`O * W_o` plus aggregate state. Hash metadata, aggregate states, transport
batches, allocator overhead and spill/reload staging add to these terms. This is
a component model, not an exact additive peak: overlapping scopes, parallelism
and early release determine the actual peak. Wide outer rows matter even though
the float key itself is only UINT64.

The source sets WINDOW SpillMem from `sort_spill_mem`, JOIN SpillMem from
`join_spill_mem`, and AGG SpillMem from `agg_spill_mem`. None is a query-wide
memory cap. In particular, unpartitioned ROW_NUMBER uses `receiveAll`; window
sort spilling is conditional on ordering work and does **not** establish spilling
or a bound for its entire outer buffer. A general range LoopJoin has no universal
spill guarantee merely because the node carries SpillMem. Eligible hash/shuffle
joins have their own contracts. Group spilling reduces eligible group-state
pressure but retains owner-specific reservations and reload costs. Existing
execution budgets may reject allocations; they do not prove a universal bound
on all memory retained by this composition. No new row cap, feature flag or
automatic admission limit is introduced here.

### Capacity acceptance protocol

Before accepting this design for rollout, planner/execution reviewers must record
a named deployment profile and supported envelope: outer/candidate sizes, widths,
selectivity, distinctness/skew, concurrency, CN placement and numerical latency,
peak-memory and spill/disk budgets. Revisions 5 and 6 record measured local proposals below;
an independently accepted production envelope remains outstanding.

Validation must inspect the actual compiled plan while independently varying
`O`, `I`, `M/(O*I)`, `U/M`, outer width, skew and concurrency. Include both a tiny
result after many predicate comparisons and high distinct membership cardinality;
counting result rows alone misses the amplification. Record first-result and
completion latency, peak accounted and resident memory, merge/network volume,
spill bytes/files/reloads, budget rejection behavior and cleanup after cancel.
For repeated latency measurements, record cold samples separately; run one
excluded warm-up followed by three measured rounds on identical inputs/settings.
Do not treat the warm-up or cold run as one of the three measured rounds.

Use deterministic small forced-budget/fault cases for partial Prepare, spill
recovery error, cancellation during merge/build/group, Reset and subsequent
successful reuse. Verify terminal cleanup with the owning allocation/reservation
and file oracles, rather than sleeps or a large stress fixture. The evidence must
identify source SHA, commands, settings, topology, inputs and terminal outcomes.
If the envelope or evidence is missing, design/capacity acceptance remains pending;
this documentation delivery does not waive it or require an unrelated runtime fix.

### Revision 5 local qualification proposal and receipt

Revision 5 supplies an executable capacity harness and a measured **local
qualification proposal**, not an independently accepted production envelope.
Targets were fixed before measurement: Apple M4 / 16 GiB, Darwin arm64,
Go 1.27.1, CPU/CGo, one CN; `O <= 64`, total retained candidates across both
arms `I <= 256`, carried outer payload `W_o <= 256` bytes excluding metadata,
and `C <= 2`. Proposed limits are first-invocation completion <= 10 seconds,
maximum warm completion <= 5 seconds, whole-process lifetime RSS high-water
<= 4 GiB, maximum reported operator MemorySize <= 256 MiB, and sum of reported
operator spill bytes per analyzed query <= 64 MiB. These limits are measurement
acceptance targets. They do not install admission limits, qualify larger inputs,
or authorize production rollout.

The opt-in `BenchmarkScalarSetFallbackCapacity` in
`pkg/tests/sqlintegration/scalar_set_capacity_bench_test.go` uses the existing
canonical single-CN fixture. Its SQL carries `o.pad` through the result and
retains a hidden integer correlation column with `i.id > o.id`. Both ordinary
multirow arms return DOUBLE signed zero. Thus candidate cardinality is retained
before matching; selecting constant zero does not turn this into the selective
set-filter path. Each successful analyzed plan contains global `row_number`,
LEFT JOIN, two aggregates, COUNT/ANY_VALUE and the SINGLE cardinality check.

The [machine-readable receipt](validation/20261016_scalar_set_capacity.json)
records source and native hashes, settings, all samples and complete logical
EXPLAIN ANALYZE output. Reproduce with:

```sh
MO_SCALAR_SET_CAPACITY_REPORT="$TMPDIR/scalar-set-capacity.json" \
  .agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=240s \
  -run '^$' -bench '^BenchmarkScalarSetFallbackCapacity$' -benchtime=1x \
  ./pkg/tests/sqlintegration
```

Each profile runs a separately labeled first invocation, one excluded warm-up,
then three measured rounds on identical inputs/settings. Two-client profiles use
separate sessions and a start barrier; measured client intervals overlap. Table
entries are maximum milliseconds across clients and measured rounds. The first
invocation follows data loading and is **not** cold OS/storage-cache evidence.

| Profile | O / total I / payload bytes / C | First invocation completion ms | Warm first row ms | Warm completion ms |
| --- | --- | --- | --- | --- |
| baseline | 8 / 16 / 0 / 1 | 1.514 | 0.761 | 0.764 |
| outer | 64 / 16 / 0 / 1 | 1.483 | 1.240 | 1.254 |
| candidates | 8 / 256 / 0 / 1 | 1.710 | 1.297 | 1.301 |
| sparse | 8 / 16 / 0 / 1 | 1.305 | 0.889 | 0.892 |
| distinct-cardinality-error | 8 / 16 / 0 / 1 | 1.242 | unavailable | 0.807 |
| width | 8 / 16 / 256 / 1 | 1.125 | 0.873 | 0.879 |
| concurrency | 8 / 16 / 0 / 2 | 1.758 | 1.151 | 1.154 |
| combined | 64 / 256 / 256 / 2 | 6.559 | 6.301 | 6.327 |
| forced-spill | 64 / 256 / 0 / 1 | 70.672 | 64.593 | 64.611 |

Baseline/dense inputs yield `M = O*I` but one distinct membership per outer
occurrence. Sparse input yields one qualifying outer occurrence and seven NULL
results despite comparison work. The all-distinct profile returns expected 1242,
including during EXPLAIN ANALYZE; first-row and runtime analyze quantities are
unavailable for it, not zero. A separate profiling execution sets
`agg_spill_mem=1` and observes spill at both aggregate stages. Normal profiles
explicitly use `agg_spill_mem=0`, `max_dop=1`; resolved join/sort settings are in
the receipt. The combined timed profile overlaps two queries, while its EXPLAIN
ANALYZE is a **separate serial execution**, not concurrent memory evidence.

All available quantities meet the proposed local numerical targets. Reported
MemorySize is an operator statistic, not a query peak; human-readable values
and their byte conversions are rounded. RSS is the entire process's lifetime
high-water, including startup and preceding profiles, not incremental or
per-query resident memory. Shared-host scheduling is uncontrolled. The receipt
does not measure capacity-scale query-accounted peak, merge/network volume,
spill file/reload counts, independent skew variation, the high-distinctness
boundary of the envelope, or multi-CN capacity. These remain explicit protocol
items; the local experiment does not close the full rollout acceptance gate.

### Revision 5 deterministic failure and reuse evidence

`TestScalarSetFallbackResourceLifecycle` compiles the actual fallback from two
outer occurrences and two literal inner members per arm, with no storage/network
fixture. It asserts WINDOW/LEFT JOIN/two logical AGGs, signed-zero/NULL output
and exact outer identities. Private generations share one 64 MiB aggregate
budget, so a later normal generation also proves the budget/registry can admit
work after the preceding failures.

The 4 KiB generation rejects a group allocation with typed
`ErrAllocationAccountCapacity` (`used=424 requested=32768 limit=4096`) after
partial initialization and produces no output. This account-level rejection
precedes the shared controller; the controller's RejectCount remains zero and
is not presented as rejection evidence. Normal execution and cancellation from
the output callback both exercise real cached TP `Compile.Reset` with a fresh
top context and then return the exact original rows. Cancellation is injected
after fallback materialization, not during merge/build/group. The normal
accounted generation peak in this tiny fixture is approximately 633 kB; that
is not an estimate of the capacity benchmark's query peak.

After Run, reservations and live allocation metadata must already be zero,
before fixture destruction. Prepared operators are explicitly freed before
Compile.Release; then MPool bytes, memory-growth participants, spill disk and FD
reservations must be zero before generation Close. The final fresh execution
uses the same aggregate budget and registry. The race-enabled test passes.

Existing owner tests provide additional, separate evidence:

- Group: `TestAccountedGroupSpillResourceAdmissionCleans`,
  `TestAccountedGroupCancellationResetAndReuse`,
  `TestAccountedGroupShortSpillWriteCleans`, and
  `TestAccountedGroupCorruptSpillRecordCleans` cover admission, cancellation,
  partial spill writes and corrupted spill reload cleanup.
- WINDOW: `TestWindowCallHonorsPreCancellation` and
  `TestWindowResetReleasesInheritedAccountedBuffers` cover its cancellation
  boundary and inherited-buffer Reset ownership.
- LoopJoin: `TestLoopJoinResetReleasesGenerationJoinBatch` and
  `TestResetRebuildsExpressionForNextAllocationGeneration` cover generation
  ownership and expression recreation.

At revision 5, owner tests did not establish composed in-flight merge/build/group
cancellation or composed corrupt-spill recovery. Revision 6 below adds specific
compiled input/reload boundaries; it does not turn them into arbitrary mid-call
cancellation proof. No production fault hook or unrelated executor change is
introduced to hide those gaps. Independent acceptance must name this exact
revision and the agreed supported envelope; this author-supplied evidence is
not that approval.

### Revision 6 extended capacity and composed failure receipt

Revision 6 keeps the same proposed single-CN envelope and numerical latency,
operator-memory, spill and process-RSS targets. It additionally requires the
internal execution allocation-account generation peak to be <= 256 MiB. This
remains a local qualification proposal awaiting independent acceptance, not a
new admission limit or a claim about arbitrary deployments.

The [revision 6 receipt](validation/20261016_scalar_set_capacity_r6.json) retains
12 profiles and 75 public SQL samples, using the same first-invocation / excluded
warm-up / three measured rounds protocol. High distinct membership is now
measured at the envelope boundary. Balanced and skewed profiles hold `O=64`,
total `I=256`, payload width 256 and two distinct values fixed: the two values
occur 64:64 versus 127:1 times per arm. Both correctly return cardinality error
1242, measuring work before the existing SINGLE rejection.

| Profile | O / total I / payload bytes / C | Warm maximum completion ms | Internal account generation peak bytes | Physical merge input bytes sum |
| --- | --- | --- | --- | --- |
| baseline | 8/16/0/1 | 2.672 | 1,366,648 | 912 |
| outer | 64/16/0/1 | 4.832 | 1,450,312 | 5840 |
| candidates | 8/256/0/1 | 3.734 | 1,545,376 | 3792 |
| sparse | 8/16/0/1 | 10.843 | 1,355,528 | 912 |
| distinct-cardinality-error | 8/16/0/1 | 6.592 | 1,365,624 | unavailable |
| width | 8/16/256/1 | 3.627 | 1,417,848 | 5008 |
| concurrency | 8/16/0/2 | 3.739 | 1,366,648 | 912 |
| combined | 64/256/256/2 | 14.417 | 4,820,544 | 41488 |
| high-distinctness-boundary | 64/256/256/2 | 34.590 | 8,505,176 | unavailable |
| balanced-two-values | 64/256/256/1 | 12.245 | 4,820,544 | unavailable |
| skewed-two-values | 64/256/256/1 | 20.099 | 4,820,544 | unavailable |
| forced-spill | 64/256/0/1 | 77.766 | 2,107,648 | 8720 |

All available quantities meet the proposal's numerical targets. Every internal
root reports one valid allocation generation, no failed generation and zero
terminal account bytes. The largest measured account peak is 8,505,176 bytes;
the largest process lifetime RSS high-water is 397,246,464 bytes. These are
separate observations with different coverage, not additive query memory.

Public `EXPLAIN ANALYZE FORMAT JSON` and `EXPLAIN PHYPLAN FORMAT JSON` are
explicitly unsupported by this source. The harness therefore retains ordinary
logical analysis plus full **physical-plan text** from a separate serial
`EXPLAIN PHYPLAN ANALYZE` execution for every successful profile. That text
includes exact integer operator input/output counters, network bytes and Group
spill/reload ExtraStats. The combined profile records 41,488 bytes of summed
merge inputs; repeated traversal at different merge stages is counted repeatedly.
Observed network bytes are zero on this one-CN topology; this is no evidence
about remote transport. Forced spill records 251 bucket creations, 313 write
calls, 251 reloaded buckets, 375 reloaded records and 384 reloaded rows across
the two physical Groups, with 72,492 reported spill bytes. Bucket creation,
reload count and write calls are distinct quantities, not interchangeable.

A separate internal-executor execution attaches a fresh `resource.Root` per
worker and retains its terminal summary, including expected 1242 failures that
cannot return an analyzed plan. It uses the public profile's resolved spill/DOP
settings and database. Internal execution excludes MySQL protocol work and is
not included in public latency samples. Two-worker internal profiles start via
a barrier; their actual overlap is not measured. Sum of generation peaks is an
upper bound, not a measured simultaneous peak. `Allocation.MaxGenerationPeak`
covers accounted execution owners only. Internal `Memory` domain fields remain
zero despite complete quality / zero missing-domain counters, so this receipt
does not claim a complete allocator-domain or per-query resident peak. RSS
remains the conservative whole-process lifetime high-water observation. Physical
merge/spill metrics for cardinality-error profiles remain unavailable; the
internal account peaks for those failures are measured and retained.

The composed compile fixture now uses an owned local filesystem decorator and
eight deterministic modes. In addition to revision 5's budget/post-output cancel
controls, it validates:

- WINDOW input cancellation after one nonempty batch has been delivered, on the
  next child call. The actual unpartitioned materialization and its merge edge
  have started. The test decorates only an unaccounted child edge and requires
  TP compilation, preserving every allocation owner's discovery/cleanup methods.
- GROUP cancellation at its synchronous post-build, post-writer-close file reopen
  boundary, before reload. The existing file service supplies this seam.
- Corrupt spill recovery at that same reopen boundary: truncate the reopened
  file to one byte before returning it to GROUP. The real compiled fallback
  returns typed `io.ErrUnexpectedEOF`; no truncation thread races a buffered
  writer and no product fault hook is added.
- Normal forced-spill control and subsequent TP Reset recovery with the fault
  disabled. A fresh generation then executes successfully on the shared budget.

Before Reset **and** after recovery, the test checks zero memory/spill-disk/FD
reservations, zero live allocation metadata, successful removal of every named
spill file, and `os.ErrClosed` for every created/reopened handle. Prepared
operators are finally freed before Compile.Release, followed by zero MPool bytes
and memory-growth participants before ledger Close. First cancellation/corrupt
attempts each create two files, reopen one and remove both; the positive spill
control creates/reopens/removes twelve files per execution. These tiny-fixture
filesystem counts must not be substituted for capacity-scale bucket counters.
The exact counts and terminal generation observations are in the receipt.

This closes deterministic composed materialization-input, post-group-build /
pre-reload, corrupt-reload, cleanup and subsequent-reuse evidence for this local
profile. It does not inject cancellation in the middle of one batch's comparison
or aggregation call, establish full allocator attribution, or qualify distributed
placement/capacity. Independent acceptance of this exact revision and supported
production envelope remains external; the author does not infer it from tests.

Revision 6 was subsequently rebased onto official main
`7c61f67e5dc46b0b8e591fe957219308b078bfa4`. Main allocated 117 to
collation-aware LEAST/GREATEST; set keys now use 118 across all admission layers.
The independent division floor remains 116, and real bound collation-only and
combined-expression controls verify 117 versus 118. Capacity and lifecycle
receipts above were refreshed after the container/spill dependency changes;
revision 5's 116/117 records remain historical. Full planner, compile, colexec
and protobuf-plan package checks and updated static checks pass on this source.

### Revision 7 intended deployment and rollout contract

This revision selects the first alternative in
[review 5549738840](https://github.com/matrixorigin/matrixone/pull/30184#pullrequestreview-5549738840):
define an intended deployment envelope and validate its boundary, sparse and
concurrent workloads against explicit budgets. It does **not** select a new
runtime admission mechanism. A default-off planner switch would reject the
previously supported correlated scalar queries restored by this PR; silently
returning to the old decorrelation route could instead change membership.

The initial intended qualification profile is **S1**, a dedicated single-CN CPU
instance on Apple M4 / 16 GiB, Darwin arm64 / Go 1.27.1 with the recorded native
libraries and MORPC 118 implementation. Its workload is the benchmark's actual
nonidentity-correlated scalar UNION template and its duplicate/NULL/error
controls. Each query carries at most 64
outer occurrences, 256 total retained candidates across both arms and a
256-byte outer VARCHAR payload (plus the benchmark's integer identity and
execution metadata). At most two such clients execute concurrently; `max_dop=1`
is required for every client and the internal executor. These are intended
workload/deployment **preconditions**, not optimizer cardinality estimates or
newly enforced server limits. They do not qualify arbitrary SQL with the same
row counts, other hardware/OS, other user workloads sharing the host, AP
parallelism or distributed CN placement. In particular, Darwin results are not
Linux production evidence.

S1 uses fixed acceptance criteria established before this execution:

| Quantity | Acceptance criterion | Observation boundary |
| --- | --- | --- |
| First invocation completion | <= 10 s per client | Ordinary SQL, after fixture/data loading; not OS/storage cold |
| Measured warm completion or expected 1242 error | <= 5 s per client in every measured round | One excluded warm-up, then three measured rounds; first invocation separate |
| Process lifetime resident high-water | <= 4 GiB | Whole process including fixture/prior profiles, not a query RSS peak |
| Accounted allocation generation peak | <= 256 MiB per internal query | Fresh terminal resource root, also on expected 1242 errors |
| Sum of internal generation peaks | <= C * 256 MiB (512 MiB at C=2) | Conservative sum bound; internal execution overlap is not measured |
| Largest reported operator memory | <= 256 MiB | Separate successful serial physical EXPLAIN execution |
| Sum of reported operator spill bytes | <= 64 MiB per analyzed query | Separate serial physical EXPLAIN; not simultaneous two-client disk occupancy |

The benchmark now asserts these criteria, requires nonempty physical memory
and spill counters, and preserves its public SQL result/cardinality and
concurrent-interval overlap oracles. No timing threshold is added to ordinary
UT/BVT. Physical runtime payload remains unavailable on cardinality errors;
accounted roots cover those failures, but no missing metric is replaced with
zero or an implied whole-query bound.

Revision 7 extends the existing orthogonal profiles with sparse-boundary C=1
and C=2 at O=64/I=256/payload=256, and forced-spill C=2 at that same boundary.
The sparse input has one matching outer occurrence and 63 unmatched occurrences:
only the final inner ID matches, with one candidate from each identical arm.
Thus the input model has M=2, Z=63 and one distinct matched membership, while a
general range join can inspect O*I=16,384 pairs. The public oracle checks all 64
outer IDs/payloads, one negative-zero result and 63 NULL results. This comparison
bound is derived from the fixture, not claimed as a measured comparison counter.
The exact plan/physical counters, all public samples and terminal roots are in
the [revision 7 receipt](validation/20261017_scalar_set_capacity_r7.json).

The fresh run passes all 15 profiles and 100 public samples (package 8.445 s).
Boundary results, with one excluded warm-up and three measured rounds each:

| Boundary workload | O / total I / payload / C | Maximum warm completion ms | Maximum internal generation peak bytes | Sum of internal peaks bound bytes | Separate serial spill bytes |
| --- | --- | --- | --- | --- | --- |
| sparse-boundary | 64/256/256/1 | 1.566 | 1,481,312 | 1,481,312 | 0 |
| sparse-boundary-concurrent | 64/256/256/2 | 3.216 | 1,481,312 | 2,962,624 | 0 |
| forced-spill-boundary-concurrent | 64/256/256/2 | 147.595 | 4,820,544 | 9,641,088 | 170,796 |

Both sparse physical plans independently report 65 LEFT JOIN output rows and
64 final rows, alongside the public result oracle. The concurrent forced-spill
profile's separate serial analysis records 251 created/reloaded buckets, 313
write calls, 375 reload records and 384 reload rows. These do not measure total
simultaneous two-client spill. Across the full matrix, first-invocation maximum
completion is 112.065 ms, maximum warm completion is 147.595 ms, largest internal
generation peak is 8,505,176 bytes and process lifetime RSS high-water is
434,323,456 bytes. All applicable recorded quantities meet S1's fixed targets;
unavailable error-profile physical metrics remain outside that statement.

Rollout is a separate, traceable decision for the exact revision:

1. **Qualification delivery (current):** publish this design, executable matrix
   and receipt. Independent planner/execution acceptance of S1 remains pending;
   do not describe the change as generally production-ready on the basis of CI.
   The planner still automatically selects the route outside S1; documentation
   does not enforce an admission limit and this revision does not claim otherwise.
2. **First controlled deployment, only after independent acceptance:** use a
   dedicated S1 instance and the bounded workload family above. The deployment
   owner must establish input/payload bounds and a two-client admission limit
   before submission, pin `max_dop=1`, and retain physical plan/resource/error
   observations. A shared instance accepting arbitrary client SQL is outside S1.
   Stop the workload on any exceeded budget, incomplete cleanup or unexpected
   cardinality/result; cancellation and same-instance recovery use the existing
   owners demonstrated by the composed lifecycle receipt.
3. **Expansion:** larger inputs, wider rows, higher concurrency, Linux/other
   hardware, additional CNs or DOP require a versioned profile, the same warm-up/
   three-round and unhappy-path protocol on that actual deployment, and separate
   acceptance before widening support. An unchanged CI result is not evidence
   for those dimensions.
4. **Rollback:** stop admitting this workload and cancel/drain its sessions;
   withdraw the candidate deployment if its resource/cleanup targets fail.
   Follow the existing MORPC predecessor/persisted-plan rollback contract. No
   dynamic session flag, cache invalidation mechanism or implicit optimizer
   fallback is introduced by this document.

This completes the objective S1 qualification work requested by the review;
independent acceptance of that exact contract is an external review state, not
an author-issued approval. General production capacity and wider rollout remain
unqualified. Existing revision-6 allocator-attribution, separate serial EXPLAIN,
internal overlap and distributed-capacity limitations continue to apply.


### Revision 8 rebase and registry reconciliation

Rebase onto official main `47b9690d7caec56b589fdefaf0684177412cffb2`
preserves its SQL file-access authorization, catalog/collation admission, shared
fixture ownership and WINDOW terminal-budget normalization. Main has assigned
function ID 599 to `CHECK_ASYNC_DATALINK_SOURCE`; this PR therefore appends
`SET_OPERATION_FLOAT_KEY = 600` and moves the registry sentinel to 601. Encoded
feature round-trip tests recognize both 600 overloads and explicitly reject
upstream 599 as a set-key capability. Both builtin registrations remain intact.

Official `MORPCLatestVersion` is still 117. Set keys retain floor 118 throughout
placement, send, receive-before-decode, executor/Prepare and persisted admission;
predecessor 117 rejects them, while collation 117 and division diagnostics 116
retain their independent floors. This changes the unmerged function allocation,
not those protocol contracts. Earlier receipts accurately record historical
function 599 and source hashes and are retained unchanged.

The [revision 8 receipt](validation/20261017_scalar_set_capacity_r8.json) refreshes
the same S1 15-profile/100-sample matrix after the rebase. Source fingerprints,
settings, samples and physical/internal measurement boundaries are retained.
Revision 7's intended deployment preconditions, budget targets, phased rollout
and pending independent acceptance remain unchanged. New measurements do not
qualify a wider deployment or claim that runtime admission enforces S1.

Fresh validation on this rebased source passes:

- Complete protobuf-plan, function, planner, compile and colexec packages:
  0.799 s / 8.494 s / 9.013 s / 11.380 s / 1.377 s, including the encoded 600
  and upstream 599 control and all existing protocol-boundary tests.
- Public signed-zero/correlation/prepare suite with race/short/matrixone_test:
  117 named PASS events, package 13.201 s; service/admission/port leases close.
- Eight composed lifecycle modes under the same race/tags mode: package 2.494 s,
  retaining first/final reservations, live metadata, removed-file/closed-handle,
  Reset and same-budget recovery oracles after WINDOW error normalization.
- Fifteen capacity profiles / 100 public samples: package 10.101 s. Sparse
  boundary C=2 warm maximum is 9.254 ms; forced-spill boundary C=2 is 137.702 ms.
  Across the matrix, first-invocation maximum completion is 156.012 ms, warm
  maximum is 151.647 ms, internal generation peak is 8,505,176 bytes and process
  lifetime RSS high-water is 435,421,184 bytes. All applicable fixed S1 budgets
  pass; the same unavailable/serial/allocator/overlap limitations apply.
- Vet for the seven affected packages and incremental lint pass (0 issues).

These are refreshed source-bound receipts, not retrospective changes to the
historical revision-7 run. Independent S1 acceptance remains pending.

## Alternatives

| Choice | Benefits | Costs and reason for the current proposal |
| --- | --- | --- |
| Keep nonidentity scalar UNION unsupported | Smallest operational and compatibility footprint; no new global materialization | Leaves the admitted correlated workload unsupported. This remains the conservative alternative if the proposed capacity envelope is unacceptable. |
| Per-outer correlated execution with local membership and early detection of a second distinct member | Simple occurrence identity and local per-occurrence membership state | Repeats scans/RPCs per outer occurrence; requires executor integration and care with evaluation/volatile semantics. It is not a planner-only substitution with proven lower total cost. |
| Distributed `(producer, sequence)` IDs and partitioned aggregation | Avoids global numbering/outer merge bottleneck | Needs uniqueness across producer generations, retries and distributed boundaries; expands the identity and recovery contract beyond this proposal. |
| Proposed WINDOW/LEFT JOIN/two logical AGGs/SINGLE composition | Reuses existing SQL semantics, cardinality checks and cleanup owners; directly testable through public SQL | Materializes outer input and can examine `O * I` candidates. Chosen as a proposal for existing-owner reuse, not as a measured performance improvement; acceptance of its envelope is outstanding. |

Within membership implementation, ordinary DISTINCT cannot distinguish signed
zeros, unconditional multirow-arm reuse loses membership, and unchanged protocol
advertisement exposes unknown-function execution on old workers. A set-specific
physical key plus explicit capability admission avoids changing shared scalar or
GROUP BY equality and avoids a new set executor/wire schema.

## Compatibility, rollout and rollback

Official main at this revision ends at MORPC 117. This capability uses MORPC
**118**. The feature walker recognizes encoded function identity 600 without
depending on its display name. It sees hidden physical equality lists, grouping
keys and serialized SetOp.KeyExprs as well as ordinary expressions. The scalar
composition adds no separate function identity or wire field. Official main
retains SELECT division diagnostics at 116, conditional on execution/session
policy. Set keys require 118 regardless of that policy; a combined expression
requires 118. Division diagnostics remain outside persisted-expression admission. Official main
adds collation-aware LEAST/GREATEST at 117; that independent feature remains
admitted at 117, and a combined expression with a set key requires 118.

Placement requires workers advertising 118 or newer; missing or old capabilities
fall back to one CN on the capable coordinator. Sending rechecks actual pipeline
destinations, including changes after placement. The receiver rejects a pipeline
containing these expressions before operator decoding when its local floor is
missing or below 118. Operator expression construction checks the same floor
before allocating an executor. Persisted-expression read and authoring admission
also require 118. These checks add no per-row work or new resource owner.

Prepared specialization regenerates membership keys from the resolved output
domain before capability detection: integer -> float -> integer must acquire and
then release the 118 requirement. A prepare-time ANY type is not capability proof.
A new binary configured at immediate predecessor floor 117 rejects these
expressions before execution. An actual old binary lacks the new guard: new
senders must exclude it, and durable authoring must not publish an expression
under a floor below 118. Lowering a new binary's floor does not rewrite existing
catalog expressions or teach an old binary identity 600.

Before downgrading/restoring a backup onto old binaries, inventory affected
persisted expressions and remove or rewrite them to an old-supported definition;
drain/recompile affected plans. Otherwise retain capable binaries. Restart alone
is not a migration. Rollout retains existing authorization/tenant boundaries;
candidate amplification is an availability risk to evaluate under existing
execution budgets, not evidence of a new privilege bypass. Use existing plan and
operator analysis to inspect global merge, row counts, memory and spill work;
this proposal introduces no new metric or log-cardinality scheme.

There is no reservation for unmerged PRs. Before delivery/rebase, compare fresh
official-main latest and the exact PR head; a rebase advances this single
capability to latest + 1 and updates sender/receiver/Prepare/persistence gates,
this document and immediate-predecessor rollback tests together. Other open PRs
using the same candidate do not reserve it.

## Evidence and acceptance record

The baseline below records historical correctness evidence. Revision 3 was
documentation-only; revision 4 rebases onto the stated official main and changes
the set-key protocol floor to 117. Historical 115/116 receipts are not rewritten
as new 116/117 evidence. Fresh boundary and composition checks are recorded
separately; unrelated correctness evidence remains reusable only where its
semantic inputs are unchanged. Successful tests are not independent design
acceptance or a capacity benchmark.

| Invariant or risk | Evidence and revision | Result and limitation | Pending acceptance |
| --- | --- | --- | --- |
| Signed-zero membership, empty/real NULL, outer identity and scalar cardinality | Public `TestSetOperationSignedZero` at `fae17ae98d93f2b3e5e251dbc5649d643513af03`; preceding repair receipts report 117 named pass events (6.36s root, 8.438s package) | Pass includes grouped MAX, LIMIT 2, OFFSET, COUNT, repeated members, opposite-zero/finite-member 1242, and correlation partitions. One-CN correctness, not capacity. | Independent reconciliation/implementation review |
| Boundary routing, physical/correlation keys, pagination and SINGLE, volatile/prepared controls | Complete planner and compile package tests from that same repair; `-count=1` through repository CGo wrapper, 35.715s and 14.010s | Passed; gofmt/vet and incremental lint also passed (0 lint issues). Revision 3 ran no product tests; this is a historical receipt. | Independent implementation review |
| Historical encoded 599 overloads, hidden key traversal, prepare regeneration and 115/116 rejection | Retained compatibility tests described in the PR at the source baseline | Registry/protobuf, compiled grouping stages, old/missing/new/changed destinations, balanced releases, receive-before-decode, executor/persisted floors and integer/float/integer controls. Does not prove a live mixed-version upgrade or old-binary backup restore. | Execution/compatibility acceptance |
| Historical CI behavior | [Run 38826312235](https://github.com/matrixorigin/matrixone/actions/runs/38826312235), head `fae17ae98d93f2b3e5e251dbc5649d643513af03` | Required CI, Ubuntu UT, SCA, coverage and proxy/pessimistic multi-CN BVT succeeded; upgrade checks skipped. | No inference of upgrade or capacity acceptance |
| Selective typed-key filter work | Earlier unchanged-path receipt: 4,096 rows, one scan row per arm, two aggregation input rows, reported memory 262,270 bytes | A selective set-operation observation only; does not measure occurrence-based scalar fallback latency, concurrency or resident peak. | Separate from fallback acceptance |
| Rebased capability allocation | Set-key placement/send/receive/Prepare/persisted tests at 116/117, plus division-only and combined-feature controls | Focused compile/colexec/planner boundary checks pass (3.073s/2.088s/1.517s); full planner, compile, colexec, function and protobuf-plan packages also pass. Fresh receipts are recorded in the implementation PR; historical 115/116 receipts above retain their original provenance. | Independent implementation review |
| Rebased function 600 overloads, upstream 599 negative control and predecessor 117 rejection | Revision 8 on official main `47b9690d7caec56b589fdefaf0684177412cffb2`; complete protobuf-plan/function/planner/compile/colexec package suites passed | Fresh registry/encoded-feature and existing placement/send/receive/Prepare/executor/persisted tests; independent collation 117/division 116 controls retained. Not a live mixed-version cluster. | Execution/compatibility acceptance |
| Global merge and reaggregation capacity | Revision 6 benchmark and linked receipt above | Twelve actual fallback profiles and separate terminal internal roots; local targets met, including high-U/skew, measured account peaks, merge and spill/reload counters. Full allocator attribution and remote qualification are not claimed. | Planner/execution reviewers: supported production envelope and remaining protocol acceptance |
| Composed partial failure/cancel/reset/spill recovery | Revision 6 composed compile test plus named owner fault tests above | Typed 4 KiB rejection, WINDOW input / pre-reload cancellation, synchronous corrupt reload, first/final attempt file/handle/ledger cleanup and TP Reset/same-budget recovery. Mid-single-batch cancellation is not injected. | Execution reviewers: remaining composed boundaries |
| Design gate | [Review 5541720784](https://github.com/matrixorigin/matrixone/pull/30184#pullrequestreview-5541720784) requested ownership, capacity and alternatives | This revision supplies those artifacts; the reviewer deferred full implementation review. No independent approval is recorded. | Independent reviewer approval of this exact revision |

Record any later acceptance with the immutable revision/commit link, reviewer,
decision, supported capacity envelope and evidence links. Advance status to
`in progress` only after independent approval; the author does not self-approve
or infer acceptance from CI. Until then, revision 6 remains a reviewable draft.
