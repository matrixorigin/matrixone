- Status: in progress
- Start Date: 2026-10-16
- Revision: 31
- Implementation PR: https://github.com/matrixorigin/matrixone/pull/30184
- Issue: https://github.com/matrixorigin/matrixone/issues/30155
- Original implementation baseline: `fae17ae98d93f2b3e5e251dbc5649d643513af03`
- Revision 31 integration baseline: official main `67c91c5bdf6c669f63331778634215f7aa3e8f58`
- Revision 30 integration baseline: official main `d4bda99eee3dd0964e8e1ec1f66d5e3e0fb19e14`
- Revision 29 integration baseline: official main `a4de4ed1381780d02fa7f440ce7c4f9e01269674`
- Revision 28 integration baseline: official main `fdcb37a5212a4bbc82fd958e6f54f195081bb56b`
- Revision 27 integration baseline: official main `32591dcf3757a0ae4c2dec52cfd8d98761bd3454`
- Revision 26 integration baseline: official main `7038c8520e5fc1ba456914b3c389ccedb69e22f7`
- Revision 25 integration baseline: official main `a648b13e53cf0a07ff19211203a69e37b2d1dd93`
- Revision 24 integration baseline: official main `63a964a82fbaae4759cfabb45655e0185bc0979c`
- Revision 23 integration baseline: official main `ef07d45cf6bc258df8c2ac60b1ea9d2a38045b1a`
- Revision 22 integration baseline: official main `21966565287edf9f7d7f519af60d269711608945`
- Revision 21 integration baseline: official main `743e749a1e33d6c845bb73999e8c82d297a2b657`
- Revision 20 integration baseline: official main `743e749a1e33d6c845bb73999e8c82d297a2b657`
- Revision 19 integration baseline: official main `3829ee49c7c118f2f3b0ebf7ed3acbd52483d870`
- Revision 18 integration baseline: official main `983ab88913462ebba9441d9626cad64e206f0884`
- Revision 17 integration baseline: official main `7c146f4adbf068dd0c06b377157cfb7d7698b1eb`
- Revision 16 integration baseline: official main `d82e828b0a4df9ce635fe573c1e90c5cde978bf9`
- Revision 15 integration baseline: official main `873282742714436bff407dfcdbbe879147162bd3`
- Revision 14 integration baseline: official main `57fb9530859d92791100098d312ea8cacc671910`
- Revision 11 measurement baseline: official main `b111224b0c624773389b390cd3cf1223126b6ee2`
- Design feedback: https://github.com/matrixorigin/matrixone/pull/30184#pullrequestreview-5541720784
- Independently accepted revision: 13 at `40eb8f3d0dd45085649d1f7a0345c1ea419a8542`
- Design acceptance: https://github.com/matrixorigin/matrixone/pull/30184#pullrequestreview-5567698353
- Implementation approval: https://github.com/matrixorigin/matrixone/pull/30184#pullrequestreview-5566840311
- Protocol allocation correction: https://github.com/matrixorigin/matrixone/pull/30184#pullrequestreview-5567799063

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
the official-main rebase. Revision 9 preserves newly merged diagnostic/folding
semantics, resolves the shared executor test append conflict and refreshes the
same validation protocol. Revision 10 preserves upstream table charset and
grouping-query-block semantics, advances the set-key allocation, and retains
previous capacity receipts as historical evidence. Those historical revisions
did not assert repository approval or advance implementation approval.

Revision 11 addresses the automatic-activation capacity gap identified in
review 5558405070. It replaces full outer materialization in the explicitly
marked occurrence stage with streaming numbering and introduces enforced
attempt-wide memory/work admission plus CN concurrency admission. It adds typed
plan/pipeline markers; ordinary WINDOW and equality/set semantics keep their
existing paths. Revisions 5–10 below are immutable historical receipts, not
qualification of this revision. Independent review 5559536863 accepts revision 11
at the exact commit recorded above; review 5559558038 separately approves that
implementation. The acceptance record below preserves their distinct scope and
limitations. This documentation update records those decisions without changing
the accepted architecture or qualification protocol.

Revision 12 integrated official parameterized subquery programs and ordinary
analytic allocation/continuation owners. Its query-wide rejection of mixed
scalar fallback and dependent programs caused the compatibility regression in
review 5566330907. Revision 13 removes that rejection by binding dynamic native
operators to the existing statement account and scalar-set lease, including
nested invocations. Independent review 5567698353 subsequently accepted revision
13 at `40eb8f3d0dd45085649d1f7a0345c1ea419a8542`. Revision 14 integrates the
new official protocol allocation and retains that reviewed architecture. Revision 15
preserves the released DECIMAL DIV capability while advancing set-key admission
to 122; it retains the same execution and ownership design.
Revision 16 retained temporal 122 and advanced set keys to 123. Revision 17
preserves upstream nonblocking row-lock admission at 123 and advances only
set-key/scalar-marker admission to 124. The FUNCTION_SCAN outer-output correction
from `24c7a0246731271b7dee6e567dbadab4e6fadfb7` remains included; function outputs
and aliases stop at their own schema, without exposing argument children.

Revision 18 repairs scalar UNION fallback after CROSS APPLY/OUTER APPLY of
UNNEST or JSON_TABLE. The scalar-set collector exposes left and function-result
columns, preserving OUTER APPLY right-side null extension without traversing
function argument children. It numbers occurrences after APPLY expansion, so
identical expanded rows remain separate scalar invocations. Admission requires
two children and a right FUNCTION_SCAN; the older correlated-aggregate collector
and unsupported source shapes retain their existing rejection. This is an
outer-output compatibility correction, with no change to the accepted execution,
resource or wire contracts. The integration preserves upstream result-column
provenance field 23 and keeps set-key/scalar-marker version 124 above official 123.


Revision 20 preserves official TIMESTAMPADD_SECOND_INTERVAL at function 601 and
MORPC 124, and allocates set-operation float keys at function 602 with sentinel
603 and MORPC 125. Scalar markers share the set-key capability. Literal-ID
feature recognition, native executor admission, persisted/CTAS requirements and
placement/send/receive/actual-stream requirements follow these distinct owners.
The earlier unpublished set-key identity 601 and its serialized receipts remain
historical evidence only; they are neither remapped nor interpreted as 602.
Fresh 602 wire fixtures and actual official-124 controls establish this boundary.
The [revision-20 receipt](validation/20261020_set_key_protocol125_rebase.json)
records the integrated source, prepared composition, predecessor rejection and
scoped evidence reuse. No new capacity or live mixed-binary claim is made.

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
The internal `set_operation_float_key` registry identity is 605, with overloads
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
The explicit `scalar_set_reaggregation` marker selects a streaming identity
owner. Compilation confines the whole marked query to the coordinator at DOP 1;
there is one sequence per marked stage in an attempt, with checked UINT64
increment and a fresh sequence on Reset. It never invokes ordinary WINDOW's
`receiveAll` path. The marker is preserved through plan copying and physical
operator construction.

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

The planner does not release executor-owned buffers. Marked identity and
candidate stages join the existing execution-account lifecycle, with the new
attempt lease below; ordinary operators retain their existing cleanup owners.

| Resource or state | Effective owner and lifetime | Success, error, cancel and reuse contract |
| --- | --- | --- |
| Tags, marker/key expressions, outer-output aliases | QueryBuilder/query plan | Planning failure discards the plan; aliases and occurrence IDs are not catalog state. A new execution creates fresh IDs. |
| Streamed outer batch, occurrence vector and output views | Explicitly marked [Window](../../pkg/sql/colexec/window/scalar_identity.go) | Retains one child batch and emits at most DefaultBatchSize rows per Call. Reserves its full retained backing before publication, including offset prefixes, buffer leases and NULL/grouping bitmap capacity. Measurement does not materialize borrowed validity. Previous output views are released before advancing the child. Reset/Free release the reservation and output; Reset restarts the counter. |
| Candidate build state, condition executors, match tracking and output batches | Coordinator-local [LoopJoin](../../pkg/sql/colexec/loopjoin/types.go) for every marked candidate join | Hash/shuffle dispatch is disabled for this marker. All marked joins share one attempt work lease and charge actual predicate-window pairs before evaluation, including false matches. Cached condition results are not charged again after output yields. Existing build/message ownership and Reset/Free drain their allocations. |
| Membership/outer group keys, representatives, aggregate states, spill/reload state | [Group and MergeGroup](../../pkg/sql/colexec/group/types2.go) | Their Reset/Free owners release or reset hash/aggregate state, projections, spill files/reservations and reload staging under existing contracts. Each logical AGG may compile to multiple physical owners. |
| Scalar cardinality check and two-row constant input | Existing SINGLE join and value-scan owners | Zero/one-member success and multiple-member 1242 share ordinary pipeline cleanup; no special retry or retained scalar state is added. |
| Partial Prepare, upstream waits, deadline/cancellation and scope shutdown | Process/scope lifecycle and each initialized operator | Errors propagate to scope teardown; cancellation must release build/message waits and terminate upstream work. Partial initialization must remain safe for cleanup. Reset/re-execution must not retain row-number counters, buffered rows or failed-generation execution resources. Immutable plan tags/aliases may be reused; occurrence identity is scoped to the new execution. |

A marked attempt admits one CN permit before execution. The same CN execution
budget admits at most **2** marked attempts concurrently. It opens an allocation
account capped at **256 MiB**, or the smaller inherited limit. Retained borrowed
input is conservatively reserved against that same account even when upstream
physical provenance is unknown; an allocation selection alone never proves that
the retained backing is charged. Shared backing may be charged twice. The official owner
catalog reserves Dependent ID 14 and Window ID 15. Marked streaming identity and
ordinary analytic windows share the Window owner class, with their respective
allocation bindings and cleanup paths.

All candidate stages in an attempt share **16,777,216 candidate pairs**. This is
a monotonic checked counter, not a per-join or per-output-batch allowance. Work
and concurrency rejection use `ErrScalarSetResourceBudgetExceeded` (20833), a
separate terminal error from OOM, spill recovery and transaction retry. Neither
output yields nor a failed allocation refunds work. Reset starts a new lease.
The CN permit is returned only after all scopes/owners drain and the exact
account has a matching, sealed, valid zero-live terminal snapshot. Missing or
panicking terminal proof retains the permit (fail closed); exporter failure
after proven drainage does not. Partial initialization follows the same rule.

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
| Global numbering | `O` streamed rows, one borrowed child batch plus output views and a bounded identity vector |
| Candidate matching | At most `O * I` pairs examined for a general range predicate, even when few qualify; LEFT JOIN emits `M + Z` |
| First logical AGG | Consumes `M + Z`, emits `U + Z`, with `U <= M` |
| Second logical AGG | Consumes `U + Z`, emits `O` |
| SINGLE | Two-row constant relation; at most two matches per occurrence before the cardinality error |

This is two **logical** AGGs, not necessarily two physical Group operators.
Partial/merge grouping, CN placement and concurrency change replication and
lifetimes. Deduplication after matching does not remove candidate-predicate work.
Empty outer input gives no scalar rows; an empty candidate bag gives `Z = O`.

Let `W_o` be carried outer-row width, `W_i` candidate width, and `W_k` membership
key/representative width. Resident pressure includes one retained outer child batch and the current identity/output views, retained inner/build payload up to `I * W_i`,
first-stage group payload up to `(U + Z) * (W_o + W_k)` carrying outer columns
as well as keys/representatives, and second-stage payload proportional to
`O * W_o` plus aggregate state. Hash metadata, aggregate states, transport
batches, allocator overhead and spill/reload staging add to these terms. This is
a component model, not an exact additive peak: overlapping scopes, parallelism
and early release determine the actual peak. Wide outer rows matter even though
the float key itself is only UINT64.

The marked identity stage does not use `sort_spill_mem` as a buffer bound and
never invokes ordinary WINDOW materialization. JOIN and AGG still use their
existing spill owners; assigning SpillMem alone does not imply universal JOIN
spilling. Group spilling retains existing recovery reservations and reload costs.
The enforced 256 MiB account covers this composition's participating allocation
owners and retained input reservation; it is not a process-RSS or arbitrary
upstream-producer bound. Ordinary upstream SQL can allocate before delivering a
batch to the marked stage. Existing CN budgets still govern those producers.
The memory admission occurs before that stage retains/publishes the batch;
candidate admission occurs before evaluating a predicate window. Thus arbitrary
client shapes cannot receive an unlimited fallback account or predicate counter,
while a large or wide query may receive a resource error instead of a result.
The budget is an availability guard, not a claim that every admitted SQL shape
meets one latency SLA. The expanded qualification receipt below measures the
specific automatic route, including sparse work and over-limit termination.

### Capacity acceptance protocol

The independently accepted revision-11 contract permits automatic selection under
the enforced local execution boundaries: coordinator DOP 1, at most two marked
attempts per CN, a 256 MiB participating account or smaller inherited cap, and
16,777,216 candidate pairs shared across all marked stages in one attempt.
Excess work or concurrency terminates with 20833. This replaces the historical
S1-only deployment preconditions; it does not accept a universal latency/RSS
guarantee or distributed scalar execution. The source-bound revision-11 receipt
below records the measured sizes, widths, selectivity, distinctness/skew,
concurrency and numerical latency, memory and spill targets for its 21 profiles.

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
Independent review 5559536863 accepts this bounded contract and its current
source-bound evidence. A wider execution scope or stronger capacity guarantee
requires its own evidence and acceptance; the historical receipts do not grant it.

### Historical revisions 5–10

The following receipts describe their exact historical code. Their materialization
and S1-only rollout statements are superseded by revision 11 above and below.

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

### Revision 9 diagnostic-preserving rebase

Official main `aa8d4278bdb940fe2e2b50a02fc2d9c5c362c3b7` adds explicit-cast
and integer-argument diagnostic/folding behavior in shared planner/function and
executor consumers. The rebase preserves those upstream changes, including
row-scoped effects, scalar-value publication and prepared reuse. Its sole manual
conflict combines the complete upstream diagnostic test functions and the
set-key executor protocol test at the same file append point. Sixteen topic
patches replay identically; the remaining patch differs only in that append context.
No new route, wire field, ownership boundary or capacity envelope is introduced.

Main still advertises MORPC 117, so this feature retains 118 across placement,
actual-destination sending, receive-before-decode, executor/Prepare and persisted
admission. Function IDs remain upstream datalink 599, set key 600 and sentinel
601. The immediate predecessor 117 still rejects set keys; collation 117 and
division 116 controls remain independent.

The [revision 9 receipt](validation/20261017_scalar_set_capacity_r9.json) records
fresh evidence after the shared execution/folding changes. Earlier receipts,
including revision 8, remain unchanged historical measurements. The same S1
workload/deployment preconditions, fixed budgets, first-invocation separation,
one excluded warm-up and three measured rounds apply. Module-file fingerprints
are retained alongside the 17 production and two measurement input hashes.

- Complete selected owning-package suites with short/matrixone_test/count=1
  pass: protobuf-plan/function/planner/compile/colexec 0.968 / 9.681 / 8.337 / 11.324 / 1.455 s.
  These include both preserved append groups and their diagnostic/protocol
  controls; they are not full-repository or full-suite race results.
- Public signed-zero/correlation/prepared SQL suite passes under race with
  117 named PASS events, package 9.194 s.
  Fixture services, admission and port leases close.
- Eight composed lifecycle modes pass under race, package
  2.540 s, preserving first/final reservations,
  live metadata, spill-file removal, handle closure, Reset and same-budget
  recovery checks before ledger Close.
- Fifteen capacity profiles / 100 public samples pass, package
  8.704 s. First-invocation maximum is
  62.066 ms; measured warm maximum 64.317 ms; sparse C2 warm maximum
  1.981 ms; forced-spill boundary C2 64.317 ms. Maximum accounted
  generation peak is 8,505,176 bytes and process lifetime RSS high-water is
  437,125,120 bytes. All applicable fixed S1 targets pass. The receipt retains the
  serial EXPLAIN, per-root accounting, overlap and unavailable-metric limits.
- Seven-package vet and incremental lint pass (0 issues).

These results refresh the same bounded qualification, not general production,
distributed or whole-allocator capacity. Independent acceptance of the exact
RFC/S1 deployment contract remains pending; this scoped rebase does not supply
or waive that repository decision.

### Revision 10 table-collation and grouping integration

This revision rebases onto official main
`b111224b0c624773389b390cd3cf1223126b6ee2`, including transactional
charset defaults/table conversion and grouping provenance isolation across
query blocks. The main implementation remains authoritative for both features.
Grouping output normalization and active-key predicate simplification are
preserved alongside set-key membership and integer/bool predicate pushdown.
The subsequent official-main fixture rollback, HTTP-trace ownership and
HashBuild terminal-classification fixes are also retained unchanged; all 19
topic patches replay identically onto this final baseline.

Main has allocated MORPC 118 and function 600 to table collation. Set keys
therefore use MORPC **119**, encoded function **601**, and sentinel **602**.
Placement, actual-destination sends, receive-before-decode, Prepare/executor,
and persisted read/authoring checks use 119. Immediate predecessor 118 rejects
set keys while table collation retains its released 118 admission. Independent
division 116 and LEAST/GREATEST 117 controls retain their own floors.

Revision 9 and older capacity receipts remain immutable measurements of their
recorded source and environment. This integration does not claim new capacity
qualification on the changed grouping/charset/pipeline dependencies. The S1 deployment
preconditions, numerical budgets and pending independent acceptance remain
unchanged. Fresh functional/protocol validation on this source passes the
complete protobuf-plan, function, planner, compile and colexec package suites
with `-short -tags matrixone_test -count=1`. Scoped vet and incremental lint
also pass (0 issues). Public `TestSetOperationSignedZero` and
`TestGroupingSetQueryBlockIsolation` pass together with race enabled (146 named
PASS events, 9.853 s package). Complete process, HashBuild and compile race
suites pass (3.314 s, 3.671 s and 12.461 s), including all eight composed
scalar fallback resource/recovery modes. The new table-only/key-only/combined protocol
controls exercise feature identity, persisted floors, actual-destination sends
and rejection before decode at the 118/119 boundary.

The prior head's Ubuntu UT failure in run 39032714230 attempt 1 is a
`context deadline exceeded` assertion in the deferred database cleanup of
`TestIssue29876CrossDatabaseCorrelation`. Official main corrects the demonstrated
workload/teardown budget mismatch using its existing two-minute SQL operation
policy and verifies resource absence with independent bounded contexts. The
updated exact case and deterministic cleanup lifecycle controls pass locally
with race enabled (13.738 s package). This does not establish the historical
timing cause, production DROP latency or a flaky/non-PR classification. Fresh
CI remains pending.
This documentation does not grant independent design or capacity approval.


### Revision 11 bounded automatic execution and measurement receipt

The historical [source-bound receipt](validation/20261018_scalar_set_capacity_r11.json)
records the revision-11 implementation and benchmark on Darwin/arm64 CPU, Go 1.27.1,
with the repository CGo wrapper. The full public signed-zero regression and
manual benchmark completed successfully in 19.370 seconds. Official main was
freshly verified at `d304047724bea6104c5c2be496107928034b0778`; its new HAKeeper
and isolated-test commits do not change the implementation base or protocol 118.
The measured capability was 119. Source fingerprints identify that exact
production/test closure; this receipt is retained unchanged and does not qualify
the changed revision-12 inputs.

Each of 21 profiles executed one separately labelled first invocation, one
excluded warm-up and three measured rounds on the same input/settings. Concurrent
profiles use two independent connections and assert overlapping query intervals.
There are 135 public SQL samples. First invocation does not mean a cold OS/storage
cache. Separately executed physical EXPLAIN and internal terminal summaries are
stored independently; errors 1242 and 20833 cannot return physical EXPLAIN output.
I is the total retained candidates across both arms, W the carried VARCHAR width.

| Profile | O/I/W/C | First maximum ms | Measured maximum ms | Internal account maximum bytes | Expected error |
| --- | --- | --- | --- | --- | --- |
| baseline | 8/16/0/1 | 1.109 | 0.786 | 1,366,648 | 0 |
| outer | 64/16/0/1 | 1.489 | 1.200 | 1,450,312 | 0 |
| candidates | 8/256/0/1 | 1.610 | 1.322 | 1,545,376 | 0 |
| sparse | 8/16/0/1 | 1.171 | 0.792 | 1,355,528 | 0 |
| sparse-boundary | 64/256/256/1 | 1.956 | 1.669 | 1,481,312 | 0 |
| sparse-boundary-concurrent | 64/256/256/2 | 3.346 | 2.089 | 1,481,312 | 0 |
| distinct-cardinality-error | 8/16/0/1 | 1.137 | 1.076 | 1,365,624 | 1242 |
| width | 8/16/256/1 | 1.147 | 0.828 | 1,417,848 | 0 |
| concurrency | 8/16/0/2 | 1.656 | 1.226 | 1,366,648 | 0 |
| combined | 64/256/256/2 | 6.552 | 6.099 | 4,840,768 | 0 |
| high-distinctness-boundary | 64/256/256/2 | 19.266 | 19.379 | 8,525,400 | 1242 |
| balanced-two-values | 64/256/256/1 | 5.640 | 8.287 | 4,840,768 | 1242 |
| skewed-two-values | 64/256/256/1 | 5.695 | 6.531 | 4,840,768 | 1242 |
| forced-spill | 64/256/0/1 | 60.639 | 59.177 | 2,107,648 | 0 |
| forced-spill-boundary-concurrent | 64/256/256/2 | 60.429 | 68.460 | 4,840,768 | 0 |
| streamed-outer | 16384/16/0/1 | 49.792 | 54.582 | 6,509,768 | 0 |
| larger-candidates | 128/8192/0/1 | 29.098 | 32.611 | 1,537,944 | 0 |
| wide-concurrent | 128/512/4096/2 | 46.015 | 51.160 | 63,507,040 | 0 |
| larger-dense | 512/2048/0/1 | 199.070 | 198.149 | 2,195,168 | 0 |
| work-boundary-sparse | 4096/4096/0/1 | 489.827 | 702.131 | 2,609,184 | 0 |
| work-over-limit | 4097/4096/0/1 | 486.563 | 490.328 | 732,704 | 20833 |

All fixed criteria pass: first invocation <= 10 seconds, measured completion
<= 5 seconds, participating generation peak <= 256 MiB, concurrent conservative
sum <= C * 256 MiB, process-lifetime RSS high-water <= 4 GiB, and existing
physical operator/spill limits. Maximum observed participating account peak was
63,507,040 bytes (wide C=2); maximum process-lifetime RSS high-water was
658,096,128 bytes. RSS and reported operator MemorySize are separate quantities,
not substitutes for the allocation account. Successful physical plans retain the
marked local identity/LoopJoin/two-aggregation path. The exact-work sparse case
has 4,096 outer rows and 4,096 candidates; 4,097 outer rows rejects at the shared
16,777,216-pair limit with 20833. All internal summaries, including expected
errors, have valid generations, zero failed generations, zero live terminal bytes
and complete memory-domain quality.

The deterministic layer independently covers one full source batch plus streamed
chunks, offset/nested borrowed backing, zero-charge borrowed validity and ordinary
bitmap capacity, counter overflow, memory rejection, false predicates and output
yields. The composed mixed equality/range fixture preserves marked LEFT joins
through costing, shares one permit/work lease across two fallback stages, and
rejects their aggregate work after pre-consuming all but 12 pairs: each stage
needs 8, together 16. Real Compile.Reset then acquires a different zero-work lease,
returns the expected signed-zero/NULL results, and finishes with 16 consumed pairs;
the old lease is closed. Missing terminal proof, owner cleanup panic, partial
initialization and exporter panic controls distinguish permit retention from safe
release. Existing composed cancellation, corrupt reload and forced spill recovery
remain covered. Multi-CN scalar execution and universal upstream/RSS/latency bounds
are not asserted. Independent design acceptance and implementation approval are
recorded below. The receipt's `Execution.IndependentApproval` field retains the
pending state at measurement time; the immutable receipt is not rewritten to
represent the subsequent review decisions.


### Historical revision 12 dependent-program integration and qualification

Official main `f11dd8c5d700f215922835f314ea18d32471f482` adds parameterized
subquery programs at MORPC 119, ordinary analytic allocation and suspendable
LoopJoin predicates. This integration preserves those owners, their continuation
cursors and source admission, while retaining the marked fallback's streaming
identity and shared resource leases. Window owner ID 15 is the official catalog
entry; no second Window owner is introduced. Protobuf regeneration preserves
both dependent/slot/correlation descriptors and the scalar marker 95.

Both ordinary and dependent native function construction now recheck the
set-key 120 floor before allocation. Dependent compilation checks expression
features too; a retained template prepared at 120 rejects key execution after
rollback to 119. Dependent-only queries retain 119. Mixed dependent-program and
marked scalar fallback queries reject before source work, because the dependent
kernel factories do not yet carry that fallback lease/placement contract.
Public controls exercise each route separately and prove the mixed rejection.
The formerly unsupported missing-identity CTE UNION ALL now executes through the
upstream dependent owner; its public regression asserts the result row 2.

The refreshed [source-bound receipt](validation/20261019_scalar_set_capacity_rebase.json)
records the revision-12 source, Darwin/arm64 CPU, Go 1.27.1 and the repository
CGo wrapper. The public suite and benchmark passed in 22.050 s. All 21 profiles and
135 samples execute the same separate first invocation, excluded warm-up and
three measured rounds, with actual overlap for C2 profiles. Maximum first
completion was 689.871 ms; maximum measured completion was 796.713 ms. Participating
account peak was 63,507,040 bytes; process-lifetime RSS high-water was 686,178,304 bytes.
Exact 2^24-pair input succeeds; excess work returns 20833. Every internal account
summary has a valid generation and zero live terminal bytes. These measurements
retain the stated local scope and are not universal latency, upstream-memory or
process-RSS bounds. The revision 11 receipt remains immutable historical evidence.

Owning planner/compile/colexec/dependent, Window/LoopJoin/process/mpool and protobuf
checks passed. Focused race checks include retained protocol rollback, all nine
composed fallback failure/reuse modes, continuation/scan caches and Window
retention/account cleanup. The composed fixture explicitly installs and restores
its rollout floor, independently of other protocol tests. Vet and incremental
lint passed for the 17-package closure; the changed compile/colexec/public-test
closure was checked again after the protocol correction. Plan/pipeline protobuf
regeneration matches the committed generated sources, including postprocessors.
The independent acceptance/approval links below remain decisions on revision 11;
this integration does not rewrite their reviewed revision.

### Revision 13 mixed-program compatibility and ownership

The public regression composes a finite scalar UNION with a separate two-level
correlated scalar expression. For `t(id,d) = (1,1.25)`, both the single-arm
control and UNION composition return `(1.25,2)`. Signed-zero members, repeated
outer occurrences, nested fallback regions and prepared reexecution retain their
existing results. The former mixed-query rejection is removed; the admission
limits remain enforced.

The statement allocation attempt remains the sole lifetime owner. Its
`dependent.Operator` borrows the account and scalar-set lease and passes them
to each fresh `Execution`. All dynamic native WINDOW and LEFT join factories
retain their marker. Each marked LoopJoin binds that exact lease before Prepare
and source demand; existing native allocation hooks receive the same account.
Nested invocations and recursive generations share the current Execution;
Reset creates fresh execution state and the next statement attempt obtains a
fresh counter/permit only after the previous valid zero-live terminal proof.

Pure marked predicates use native bounded condition windows. A marked join is
always a LEFT LoopJoin, even when an equality predicate could select a hash
join. An absent condition becomes explicit TRUE so candidate work cannot bypass
admission. Dependent or guarded predicates use the existing pair continuation;
one candidate is charged after cancellation checking and before its frame is
created. Suspension and resumption retain that frame without another charge;
consuming it and advancing charges the next candidate, including false matches.
Streaming identity propagates `ExecYield` without marking EOF or advancing the
counter, so demanded input can resume across multiple batches.

Private kernel initialization owns rollback before publication. Binding or
Prepare panics become invariant failures, and Reset, Free, account clearing and
Release are independently protected so one cleanup panic cannot strand later
owners. Damaged objects do not return to reuse pools. Sticky cleanup uncertainty
reaches the dependent operator's account clearing and invalidates terminal
proof, retaining the permit fail-closed. A fully drained initialization failure
can release normally. Existing generation reservations continue to own dependent
metadata; native payloads are not charged twice.

The [revision-13 source-bound receipt](validation/20261019_scalar_set_capacity_mixed.json)
records the unchanged 21 ordinary profiles and five mixed profiles: baseline,
C2 overlap, 16,384 streamed outer rows, exact 16,777,216 candidate pairs, and
20833 over-limit termination. All 26 profiles and 165 public samples passed the
separate first-invocation, excluded warm-up and three measured rounds protocol.
On Darwin/arm64 CPU with Go 1.27.1 the combined public/benchmark package completed
in 98.869 s. Maximum first completion was 3,632.785 ms; maximum measured completion
was 3,613.832 ms, within the existing 10 s/5 s acceptance budgets. Maximum
participating account peak was 63,507,040 bytes; process-lifetime RSS high-water
was 584,351,744 bytes. Every internal account summary has valid generations and
zero live terminal bytes. These are local qualification measurements; they do
not assert universal latency, arbitrary upstream allocation or process-RSS bounds.

Mixed forced-spill correctness is proved by the actual dynamic lifecycle test:
both intended AGGs have positive native spill-row and reload-record counters
before retirement, results survive reload, files/disk/FD reservations drain,
and the account and permit are reusable across prepared attempts. An initial
capacity experiment failed a new EXPLAIN annotation oracle because dynamic
native statistics are not exported by the upstream dependent scope traversal;
native diagnostics confirmed actual spill/reload. That failed experiment is not
a passing qualification receipt. Mixed forced-spill latency and bytes remain
unqualified; all existing ordinary forced-spill capacity oracles are retained.
No production analyzer instrumentation is introduced by this compatibility fix.

Owning dependent/Window/LoopJoin/compile package suites pass, as do focused
continuation charge, initialization/cleanup panic, nested sticky-error and
mixed forced-spill tests. Final vet and incremental lint pass for the five
changed packages. Full dependent, Window, LoopJoin, compile and colexec package suites also pass
under `-race`. Delivery review is recorded with the final evidence.
The receipt binds 86 source/build/test inputs and the exact source patch against
`ca6cbe14a44935f6e0b7e854803e5e157bc93a9a`. Historical revision-11/12 receipts retain
their source provenance and approval scope. Independent reviews 5567698353 and
5566840311 subsequently accepted the revision-13 design and implementation at
`40eb8f3d0dd45085649d1f7a0345c1ea419a8542`.

### Historical revision 14 official protocol allocation and integration

Official main `57fb9530859d92791100098d312ea8cacc671910` allocates MORPC 120 to
current-session KILL authorization and proxy disconnect cleanup. This PR's
single set-key/scalar-fallback capability moves to 121, with immediate predecessor
120 rejection in placement, destination, receiver, Prepare, retained dependent
and persisted-expression tests. Dependent-only 119, table collation 118,
LEAST/GREATEST 117 and division 116 retain their released floors. Function 601,
marker fields 95/65 and allocation owners remain unchanged.

The [official-120 implementation probe](validation/20261019_set_key_protocol121_main120.json)
compiles all official Go sources using a complete temporary overlay, including
its generated protobufs. Candidate-bound FLOAT/DOUBLE pipelines unmarshal and
pass admission there, but function 601 is absent and native construction fails
with unknown overload; ordinary ABS controls construct successfully. The real
official query-service handler returns 120. Replaying that serialized response
through candidate placement and actual-destination encoding chooses the
coordinator and rejects sending either key with a nil pipeline payload. This is
isolated native execution plus response replay, not a live mixed-binary cluster.

The rebase retains upstream `bindingRecurQueryBlock()` checks in GROUP BY and
HAVING: aggregates in a scalar query block inside a recursive CTE remain legal,
while direct recursive-member aggregation remains rejected. Scalar comparison
still returns true for `-0 = +0`; the set-specific key preserves the separate
members recorded for MySQL in issue 30155. Correlation and resource preservation
closures remain as accepted in revision 13.

The revision-14 [source-bound qualification](validation/20261019_scalar_set_capacity_protocol121.json)
passes all 26 profiles and 165 public samples at source snapshot
`15230a9f349b391813383b526de781f449c7a4d7` in 54.930 s. The maximum first-invocation
and measured completion are 2,206.798/2,167.900 ms, within the unchanged 10 s/5 s
limits. Maximum participating-account peak is 63,507,040 bytes; process-lifetime
RSS high-water is 705,347,584 bytes. All internal terminal summaries are valid
with zero live accounted bytes. The receipt records 95 source/build/test hashes.
Public timing, EXPLAIN and internal accounting remain separate executions; this
is local single-CN Darwin/arm64 qualification, not a universal latency claim.

Two earlier full attempts failed the unchanged 5 s measured-completion gate for `mixed-streamed-outer`
(6.143 s and 5.282 s). After the first failure, a separate diagnostic selection
of ordinary and mixed streamed profiles preserved the first invocation, one
excluded warm-up and three measured rounds; the mixed measured rounds were
2.161/2.242/2.162 s. This selected run is not a complete qualification. The second
full attempt's process record shows independently owned compiler jobs overlapping
the mixed measurements. Shared-host contention is observed, but has not been
proved to be the sole cause. No threshold, data size, sample or oracle was relaxed.
The opt-in benchmark now logs samples before acceptance assertions and supports
an exact, fail-closed diagnostic selector; an unset selector still runs all 26
profiles. A selected report is explicitly marked incomplete. The subsequent complete qualification ran only after those competing jobs ended
and a recorded 30-second window contained no compiler/test/lint process above
10% CPU. That controlled pass does not establish the sole cause of earlier
failures. The earlier failed runs and selected/profile diagnostics remain
separately identified in the receipt; they are not counted as qualification.

Historical receipts retain their measured source and approval scope. Revision-13 design acceptance and implementation evidence apply to their exact
head. Review 5567799063 supersedes one approval for the official-120 capability
collision; the 121 gates and predecessor rejection above address that specific
counterexample. This mechanical integration does not rewrite historical approval
or capacity evidence.

### Historical revision 15 DECIMAL DIV integration and evidence validity

Official main `873282742714436bff407dfcdbbe879147162bd3` assigns MORPC 121 to
the exact unsigned DECIMAL DIV result domain (existing function 14, overload 1).
This PR's single set-key/scalar-fallback capability advances to 122. Function
601, marker fields 95/65 and all allocation/lifecycle owners stay unchanged.
Every set-specific placement, destination, receiver, executor/retained-template
and persisted guard advances together; immediate predecessor 121 rejects the
capability. DIV's 120-reject/121-accept and legacy-overload controls retain their
released contracts. Typed and public composition tests cover independent DIV,
DIV with set membership, and DIV with a coordinator-only scalar marker.

The revision-14 capacity receipt remains immutable and records its original
source and measurements. Its 26 profiles contain no DECIMAL DIV, CTAS or view
operations; the mixed expression uses integer addition. The new numeric resolver
branch is DIV-specific, and the metadata hook returns immediately for non-DIV
functions. No dependent, WINDOW, LoopJoin, account/work/permit owner, membership
kernel, native dependency, module, build input or benchmark dataset changed.
The validity ledger distinguishes matching fingerprints from mechanical guards,
upstream DIV-only changes and refreshed tests. Reuse concerns the same fallback
workload and participating-account envelope; the old package time and process-RSS
sample remain historical. This revision does not claim a new exact-head capacity
measurement. Public DIV/view/prepared integration is separately validated.

The [revision-15 source-bound receipt](validation/20261019_set_key_protocol122_rebase.json)
records the complete official-121 source overlay, candidate FLOAT/DOUBLE wire
fixtures, native ABS/unsigned-DIV construction controls, and real query-service
response replay. Official 121 lacks function 601; candidate placement excludes
it and actual destination encoding rejects before producing pipeline bytes.
This proves native implementation and response-replay boundaries, not a live
mixed-binary cluster. Refreshed five-package normal tests, the public signed-zero
and upstream DIV suites, and incremental 18-package vet/lint all pass. The new
protocol-composition test uses explicit sender/receiver floors and releases
successful decoded scopes; its earlier fixture failures remain recorded as
failures in the receipt.

### Revision 16 temporal, stream and CTAS integration

Official main `d82e828b0a4df9ce635fe573c1e90c5cde978bf9` assigns MORPC 122
to unary TIMESTAMP and dynamic TIME result contracts. The single set-key/scalar
fallback capability moves to 123; function 601 and marker fields 95/65 are unchanged.
Independent temporal 122, unsigned DIV 121, KILL 120 and dependent 119 remain intact.

The upstream transport now confirms capability on the actual pipeline stream,
after the separate worker query. Set keys contribute 123 to that existing maximum,
including when combined with temporal expressions. Thus a replacement worker at
122 cannot pass a temporal-only handshake and receive an unknown function 601.
The existing stream owner handles old/malformed/closed responses; marked scalar
scopes remain forbidden remotely at every version.

Retained CTAS population uses upstream CreateTable field 19 to keep its execution
requirement after query release and constant folding. Finalized population plans
with set keys or scalar markers require at least 123, even if temporal observation
already recorded 122. Cached Compile/Run reject rollback before DDL work. This is
an execution requirement, not a catalog format change; temporal-only CTAS stays at 122.
The merged schema retains both upstream field 19 and this PR's Node 95, with
reproducible generation and descriptor tests.

Historical capacity and unchanged-owner race evidence remain scoped to their
original source. The 26 benchmark profiles contain no temporal functions, CTAS,
views or OR predicates. Their mixed expression is integer addition. The new OR
optimization guard and temporal-only binder/kernel paths are not reached; the
current-capability Compile/Run guard returns before walking non-CTAS plans.
Fallback allocation, account/work/permit owners, membership kernels, native/module
inputs and benchmark data are unchanged. Upstream checkpoint cleanup is separate
from aggregate spill ownership and is not claimed as covered by historical race
or capacity measurements. No new exact-head latency, RSS or total package-duration
claim is made. Refreshed protocol/composition, public SQL and static evidence is
recorded separately from those immutable receipts.

The [revision-16 source-bound receipt](validation/20261019_set_key_protocol123_rebase.json)
records fresh five-package normal, public SQL and incremental 18-package static
passes, the official-122 native construction/real-handler response replay, and
unchanged-owner capacity/race validity. Public CTAS retains both signed-zero rows
and DATETIME(6); repeated prepared temporal executions retain values and membership.
Earlier invalid test fixtures remain recorded as failures, without changing
production behavior to accommodate them. Generated merged bindings reproduce
byte-for-byte with the established protoc/gogofast and owner postprocessors.

The [revision-17 source-bound receipt](validation/20261020_set_key_protocol124_rebase.json)
records the official-123 integration, merged row-lock/scalar descriptors,
immediate-predecessor 123 rejection and actual official-123 native/response replay.
The full signed-zero suite retains the direct FUNCTION_SCAN regression. Historical
capacity and race evidence remains limited to its original unchanged workloads;
this rebase asserts no fresh capacity or function-source measurements. Run
39294206106's two UT deadline failures did not reproduce in three CI-mode runs
on either original head or official main. Complete unrelated-flake attribution is
not established, so no timeout workaround or old-run rerun is claimed. New CI on
the delivered rebase remains pending.


The [revision-18 source-bound receipt](validation/20261020_scalar_apply_outer_rebase.json)
records the APPLY counterexample and controls, public UNNEST/JSON_TABLE coverage,
right-output and composed scalar correlation, aliases, identical occurrences,
empty input, null extension, real NULL, cardinality and prepared reuse. Owning
planner/compile/protobuf suites, metadata consumers and generated bindings are
validated on the new integration baseline. Historical capacity, race and native
123 evidence retain their recorded source and workload limits; no APPLY capacity
qualification or new live mixed-binary execution is claimed.


The [revision-19 maintenance receipt](validation/20261020_coverage_rebase.json)
records a patch-identical replay onto the newer official storage/Iceberg baseline.
Official MORPC remains 123 and set-key/scalar-marker admission remains 124;
protobuf and runtime-owner inputs are unchanged. The coverage cancellation
fixture's exit-90 failure in run 39371079437 remains unclassified: a focused local
coverage-mode pass does not establish Linux CI equivalence or unrelated flakiness.
The public signed-zero root is checked against the new storage baseline. This
maintenance rebase changes no execution or capacity contract; earlier receipts
retain their original sources, workloads and limits. New CI is left to the
subsequent run triggered by delivery.


### Revision 21 external outer streams and passthrough demand

Review 5582986949 demonstrated that a direct external CSV outer scan was
incorrectly rejected with 20102, while single-arm and projected-source controls
worked. EXTERNAL_SCAN now owns its schema at the same output boundary as a table
or function scan. A global occurrence number is assigned after the external
producer, so identical records retain independent scalar results and empty/NULL
and multiple-member semantics still use the existing SINGLE owner.

Collecting an outer schema is not permission to evaluate every CSV column.
Synthetic reaggregation passthrough slots are pruned using final consumer demand
before their child is remapped. Every explicit hash key, direct group reference,
and demanded logical alias is retained. Only unused non-key slots recorded in
scalarReaggAliases become typed NULL placeholders, preserving group ordinals.
An empty explicit hash-key list still means all groups are keys and disables
this pruning. Ordinary aggregates retain their original grouping semantics.
Explicit external diagnostic columns continue to request whole-record conversion;
unused invalid fields do not become conversion obligations. Needed invalid
projection, correlation and ordering fields still produce conversion errors.

The [revision-21 receipt](validation/20261020_scalar_external_outer.json) records
baseline/admission-only counterexamples and final typed/public/consumer evidence.
The bounded local runtime owners, candidate-pair/memory/CN limits and function602
at MORPC125 are unchanged. Official main was checked at
`0fb05134c76148fe7fd42a6bd54d37038df9510e` (latest124); its unrelated XFS join-spill
allocation change is not a dependency of this correction and no rebase is made.
Historical capacity and race receipts retain their measured source and workload;
column-liveness changes can alter fallback input width, so they are not presented
as current-head performance or external-source capacity qualification. No new
runtime owner, per-row admission step or distributed fallback is introduced.


### Revision 22 released diagnostic fields and marker reassignment

Official main `21966565287edf9f7d7f519af60d269711608945` adds logical Unicode
UNIQUE diagnostic expressions at `Node.dedup_key_exprs=95`. This revision
preserves that released field and moves the unpublished scalar reaggregation
marker to `Node.scalar_set_reaggregation=96`. Instruction marker65, function602
and its MORPC125 admission floor remain unchanged: official latest is still124.
The previous Node95 scalar payloads and their receipts retain historical scope;
they are not reinterpreted as diagnostics or used to prove current marker wire
compatibility. This PR has not released that prior candidate schema.

Merged bindings retain PreInsertUkCtx diagnostic field19, pipeline DedupJoin30,
RightDedupJoin18 and HashBuild20, together with all existing metadata, locking,
dependent-expression and scalar marker fields. Repository postprocessors retain
traversal of the new expression containers. Diagnostic expressions preserve
reference counts, remapping, deep-copy independence and send/receive transport.
Fresh descriptor, combined wire, expression-feature and deep-copy tests cover
these intersections; owning suites and the public Unicode UNIQUE diagnostic case
exercise the upstream contract alongside the complete signed-zero public suite.
A fresh candidate marker96 export is decoded by the full official219 protobuf
package: the unknown marker does not populate released diagnostic field95.
Version124 still cannot execute function602; current125/124 rollback, placement,
Prepare and remote-marker rejection tests retain that boundary.

The [revision-22 source-bound receipt](validation/20261020_set_key_node96_rebase.json)
separates current validation from reused evidence. Ordinary function602 native
fixtures contain only projection expressions; official function/evaluator/native
inputs are unchanged from the actual124 predecessor proof, so that narrower proof
is retained. Old scalar-marker wire evidence is superseded by the fresh96 proof.
The upstream XFS append correction belongs to hashbuild/spillutil; scalar
WINDOW/loopjoin/group code does not call that helper. Their existing bounds and
ownership design remain intact, while fresh spill/hashbuild and diagnostic tests
cover the merged base. Earlier capacity and race receipts keep their original
source, workloads and limits; this rebase claims no new-head or XFS spill latency,
RSS, external-source capacity or mixed-binary cluster qualification.

### Revision 23 CI maintenance integration

This revision replays all 36 PR commits, with identical patches, onto official
main `ef07d45cf6bc258df8c2ac60b1ea9d2a38045b1a`. It preserves the new shared-CTE
publication owner: an explicit child-local PROJECT narrows the publication
schema after evaluation, retaining errors from evaluation-only child outputs.
Only registered shared CTEs use this path; DML and recursive sink layouts retain
their existing owners. The scalar fallback's synthetic passthrough demand
pruning remains scoped to its registered columns. Upstream ALTER column
replacement CHECK/index binding, account-scoped transaction test configuration,
PITR publication projection and CDC prerequisite fixes are also retained.
CDC's classification of lock-table binding error 20702 as retryable is preserved,
including cancellation precedence and the existing admission/retry limits.
Official latest remains 124, so function602/MORPC125 and marker fields
Node96/Instruction65 do not advance.

Run [39422414433](https://github.com/matrixorigin/matrixone/actions/runs/39422414433)
attempt 1 at `a5f4d82b00f23bab1397d916a469444f98a7bbd6` failed
`TestCheckConstraintNamespace/repeatable_read_preserves_check_authoring`:
`ALTER si_invalid ADD CONSTRAINT si_reusable CHECK(x>0)` expected CHECK error
3819 but received 20619, `catalog namespace snapshot is stale`. CI Required
propagated that UT failure. The exact root passes three separate invocations on
the original head and three on the complete official `a59438e9` Go-source
overlay, using CI's race, short, tags and CGO mode on Darwin/arm64. The later
CDC-only `ef07d45c` change is checked separately with the final integration.
These are local
non-reproductions, not Linux/x64 parity or proof of an unrelated flake. The PR
touches the shared planner/compile dependency closure; closed issue #30621 is
historical mechanism context, not an open-defect shortcut. No CHECK assertion,
retry, deadline or product guard is changed, and no issue or old-run rerun is
part of this delivery. New-head CI remains pending after the maintenance push.

The [revision-23 receipt](validation/20261020_ci_maintenance_rebase.json) records
the exact failure, source controls, integration map, current owning/public/CHECK
validation and incremental static checks. Existing ignored native artifacts are
read-only local test inputs. Upstream's ARM `-march=armv8-a` build-recipe change
is preserved but is not validated by reusing those artifacts. Previous capacity,
race, protobuf-generation and native-wire receipts retain their recorded scope;
unchanged protocol inputs permit reuse of the function602 and marker96 proofs.
No fresh capacity benchmark, portable ARM build or live mixed-version cluster
qualification is claimed.

### Revision 24 released LOAD capability and prepared binding integration

Official main `63a964a82fbaae4759cfabb45655e0185bc0979c` allocates the next
function602/MORPC125 capability to LOAD IGNORE foreign-key admission. This
revision preserves that released meaning and advances only this PR's set key
to function603 and its shared scalar-marker capability to MORPC126. Literal-ID
feature recognition distinguishes602 LOAD from603 FLOAT/DOUBLE set keys without
using display names. Registry sentinel604 and all relevant planner, persisted,
placement, actual destination, send, receive, native construction/Prepare and
retained CTAS requirements follow these independent owners. Node96 and
Instruction65 are unchanged; no protobuf allocation or generated schema changes.

The integration also preserves upstream FIELD binary domains and prepared
CASE/FIELD type history, query-block HAVING ownership, compiler plan-builder
cleanup, VARBINARY-family/UUID shuffle keys, compressed-input truncation rejection,
case-insensitive charset introducers and raw BIT dump output. Fresh owning and
public tests cover their intersections with set-key specialization, shared
resource lifecycles and external outer streams. Native implementation inputs
are unchanged; the upstream CGo link-order recipe correction is preserved, while
existing native libraries remain read-only test inputs rather than evidence of
a fresh native rebuild.

The [revision-24 receipt](validation/20261021_set_key_protocol126_rebase.json)
records fresh603 wire and actual official125 native/response controls, current
validation and semantic evidence reuse. Official125 implements LOAD602 and
TIMESTAMPADD601 but not set-key603. Its real protocol response must make the
candidate choose a capable coordinator and refuse an incompatible actual remote
destination. Historical602/125 native fixtures and capacity/race receipts keep
their original source and workload scope; they do not prove603 compatibility or
new-head performance. The unchanged Node96 unknown-field proof remains reusable.
No live mixed-version cluster, fresh capacity benchmark or Linux performance
qualification is claimed. The accepted bounded local architecture is unchanged.

### Revision 25 derived-view planning integration

Official main `a648b13e53cf0a07ff19211203a69e37b2d1dd93` adds opt-in
SELECT/CTAS/PREPARE planning through the read-only View contract. Ordinary SQL
entry points keep their existing routing. The mechanical rebase preserves its
CTE declaration scope and case-sensitive projection keys, snapshot propagation
and restoration, publisher metadata ownership, and real executable source trees.
It also preserves this PR's common-type set keys, shared-CTE column demand,
scalar occurrence boundaries and retained CTAS requirement126. Function603 and
MORPC126 remain the next available allocations after released LOAD602/125.

The [revision-25 integration receipt](validation/20261021_derived_view_integration.json)
records final source fingerprints and focused planner, compiler, frontend and
public SQL validation. The revision-24 receipt is immutable: its actual official
native/response execution baseline remains63a, whose protocol implementation is
unchanged in a648. It is reused for that compatibility contract, not relabeled
as an a648 execution. Earlier unaffected functional and capacity evidence retains
its explicitly recorded scope. This integration adds no architecture or resource
budget changes and claims no new capacity or live mixed-version qualification.

### Revision 26 released VIEWS metadata and protocol composition

Official main `7038c8520e5fc1ba456914b3c389ccedb69e22f7` allocates VIEWS metadata
functions603/604 and MORPC126. This revision preserves those released identities
and advances this PR's single set-key/scalar-marker capability to function605,
sentinel606 and MORPC127. Node96 and Instruction65 remain unchanged. Literal-ID
recognition, native executor admission, persisted/CTAS/Prepare requirements,
placement, destination, sender, receiver and actual-stream gates compose each
feature independently. VIEWS-only streams require126; adding a set key or marker
requires127. Marked scopes retain their coordinator-only execution contract.

The rebase preserves upstream stable view headings and expanded-star regeneration,
ENUM/SET assignment domains, DUMP create-only publication, point-read bounds,
vector dispatch and ISCP lease-stop ownership. The new VIEWS/set-key matrix checks
both metadata-only126 and composed127 requirements with missing, old and current
worker capabilities. No resource or execution architecture changes are introduced.

The [revision-26 receipt](validation/20261021_set_key_protocol127_rebase.json)
records current source fingerprints, owning/public regression checks and fresh605
wire tests against actual official7038 source. Its real126 protocol response is
consumed by candidate placement and destination checks. Earlier unpublished603
set-key fixtures are historical only, never aliases for released VIEWS603/604.
Previous capacity, race and marker unknown-field receipts retain their recorded
source and workload limits; no new capacity benchmark, live mixed-version cluster
or portable native rebuild qualification is claimed.

### Revision 27 Top layout and VIEWS fixture integration

Official main `32591dcf3757a0ae4c2dec52cfd8d98761bd3454` fixes Top ordering-slot
layout when a column expression returns an executor-owned typed-NULL vector.
The logical column position now comes from the plan rather than pointer identity;
computed sort keys retain their separate appended slots. The integration preserves
this official change across resident, spill, reset and partial-error paths alongside
this PR's hidden physical keys, visible output width and scalar occurrence owners.
Function605/MORPC127 and released VIEWS603/604/126 remain unchanged.

The failed PROXY BVT in run39590665469 attempt1 is a deterministic metadata
fixture mismatch, not a claimed unrelated flake. Its sole final failure compares
the explicit-column UNION view's frozen parser-derived definition with a string
that depended on the old early identical-arm collapse. That rewrite injected a
redundant DISTINCT into the left AST and left the right arm unbound. Retaining
both arms correctly binds both column lists without adding that DISTINCT. The
fixture and its executable replay now assert that canonical form; the exact
string predicate, public output columns and row comparisons remain in force.
The existing planner metadata/replay case gains a precise independent assertion.
No production rewrite is restored merely to reproduce the old metadata spelling.

Coverage job119214095258 reports incomplete producer prerequisites and computes
no coverage verdict; CI Required job119214178371 propagates the PROXY failure.
The [revision-27 receipt](validation/20261021_top_view_bvt_integration.json) records
complete failure logs and BVT terminal diagnostics, the rebase and current
validation. Historical revision26 protocol-native and earlier capacity/race/marker
receipts remain immutable and are reused only for unchanged semantic owners.
No old-run rerun, new capacity result or live mixed-version qualification is claimed.

### Revision 28 application-role regression integration

Official main `fdcb37a5212a4bbc82fd958e6f54f195081bb56b` adds least-privilege
application-role documentation and its real-client regression. It changes no
production, protocol, module, native or build implementation. The mechanical
rebase preserves all 41 PR patches, function605/MORPC127 and the exact revision27
VIEWS fixture correction. Historical receipts remain immutable.

The failed shared-build job119233193860 in run39603323077 attempt1 compiles the
Java sources and reports 24 tests with zero failures, then fails in Maven shade
3.6.0 while resolving the commons-compress1.26.2 and commons-io2.16.1 artifact
descriptors. The log establishes dependency-resolution failure, without exposing
a specific transport or cache cause. No product workaround or transient recovery
is inferred. Because official main advanced, this delivery rebases first and
leaves the new build to push-triggered CI; it does not rerun the old workflow.

The [revision-28 receipt](validation/20261021_application_role_integration.json)
records the upstream delta, fresh exact application-role regression, bounded
static checks and unchanged semantic-owner evidence reused from revision27.
No new signed-zero, Top, capacity, native or live mixed-version execution is
claimed by this documentation/test-only integration.

### Revision 29 procedure, index-maintenance and duplicate-diagnostic integration

Official main `a4de4ed1381780d02fa7f440ce7c4f9e01269674` adds per-routine
stored-procedure recursion limits, preserves trailing deletes for every index
synchronization range, counts retained DELETE records toward the cuVS rebuild
cadence/cap, and completes Unicode UNIQUE diagnostics. The rebase preserves all
42 existing PR patches. Function605/MORPC127 and the released predecessor126
identities remain unchanged; the new procedure error20482 does not collide with
this PR's terminal capacity error20833.

The integration checks the independent equality domains explicitly: UNIQUE
conflicts use their existing hashmap identity and logical diagnostic rendering,
including fixed-width signed zero and NULL-leading rows; set operations continue
using their separate physical keys. Diagnostic iterators and temporary expression
results retain scoped cleanup, while scalar attempt accounts/permits and hidden
key width are unchanged. Procedure invocation frames are immutable context-local
CALL ancestry; inherited session limits do not introduce statement-global scalar
identity or capacity owners. ISCP's index mode retains trailing tombstones in all
ranges, with DELETE-only tail records included in the existing CPU rebuild gate;
non-index seeded consumers keep their durable-watermark window.

The [revision-29 receipt](validation/20261021_procedure_index_unique_integration.json)
records fresh owning/race checks, real engine collection, public Unicode UNIQUE
and signed-zero regressions, and normal comparison of the full procedure BVT
file twice on one instance. Historical protocol/native, Top and capacity evidence
is retained only for its unchanged semantic owners. CPU tail-count tests do not
qualify live GPU index reclamation; no new capacity or mixed-version cluster
result is claimed.

Run39606308050 attempt1's size-label job119244157111 failed before receiving PR
metadata because its GitHub API TLS connection disconnected. Complete diagnostic
evidence is already recorded in issue27127 comment6317361242. This is direct
transport evidence, without identifying which proxy/upstream component closed
the connection. As main advanced while that workflow was still active, this
revision rebases and pushes first rather than rerunning the old workflow. It
does not duplicate the issue comment or change reviewer requests/PR metadata.

### Revision 30 JSONLine binary and MongoDB harness integration

The publication CAS observed official main advance to
`d4bda99eee3dd0964e8e1ec1f66d5e3e0fb19e14` before any push. This revision
rebases the 43 existing patches again without rewriting historical receipts.
The new production boundary is JSONLine binary export/load: OUTFILE emits an
explicit single-key `$binary` base64 object for BINARY/VARBINARY/BLOB, and LOAD
decodes only that wrapper. Bare strings keep their original bytes; NULL is
determined from the original JSON node, so a wrapped payload decoding to the
bytes of `\N` remains a value. Projection, strict-error handling and original
non-binary values retain their separate contracts.

The MongoDB delta changes tests and the local E2E runner's oracles, not connector
production code. Exact source/materialized/join/aggregate rows replace weaker
counts; rejected writes and duplicate-target ingestion must preserve state.
The blocked-GetMore cancellation test now observes entry before canceling and
requires the worker to return. CPU/mock and race evidence is recorded separately
from real MongoDB E2E, which this integration does not claim to execute.

The [revision-30 receipt](validation/20261021_jsonline_mongodb_integration.json)
records refreshed external/export/MongoDB checks and a public OUTFILE-to-LOAD
round trip with binary zero/non-UTF8 bytes, empty value, sentinel bytes, SQL NULL,
bare-string passthrough and invalid-wrapper rejection. Prior revision29 procedure,
deduplication, ISCP, CPU tail-count and public evidence is reused only where the
new JSONLine/harness delta leaves its semantic owner unchanged. The crossed
external pruning/LOAD paths are refreshed instead of reusing stale reader proof.
Function605/MORPC127, Node96/Instruction65 and scalar capacity owners are unchanged;
no fresh native, GPU, capacity or live mixed-version qualification is claimed.

### Revision 31 Kafka projection and Arrow LOAD admission integration

The publication CAS stopped before any push when official main advanced through
Kafka JSONLine projection to `67c91c5bdf6c669f63331778634215f7aa3e8f58`.
All 45 existing patches replay without conflicts or changed patches. Kafka JSON
objects are validated against the declared data width, independently of projected
fields; metadata-only projections retain synthetic columns in both strict modes.
Short objects fail before the pending-record state is published. Non-Kafka JSON
width checks and the revision-30 binary-wrapper contract remain separate.

Arrow LOAD now checks the target INSERT privilege before planning can read an
external source. Direct, prepared and explained LOAD share that pre-plan check;
post-plan authorization remains in force. Scoped statement binding restores the
session and compiler databases independently. This crosses the PR's prepared
specialization and external projection paths, so those paths are refreshed.

The [revision-31 receipt](validation/20261021_kafka_arrow_admission_integration.json)
records the complete external owner suite, twelve frontend authorization/binding
roots and five-package format/vet/lint checks. Public SQL tests use an in-process Kafka test broker over real sockets, repeat the binary OUTFILE/LOAD and pruning/whitespace probes, and run
prepared LOAD database binding both initially and again in the same instance.
A real local MinIO test proves denied direct/prepared LOAD performs zero source
requests and no additional target writes; the seed remains. Granting INSERT then
permits source reads and two total target rows (the seed plus one loaded row). Its certificate directory is isolated under the task temporary
directory. No skipped root is accepted as this proof.

Revision-29 procedure/dedup/ISCP and revision-30 exporter/MongoDB evidence remains
immutable and reusable only for unchanged owners. The earlier interrupted Kafka
external command had no verified terminal result and supplies no pass evidence;
this revision
runs that complete suite to a terminal result. Function605/MORPC128 and the
scalar capacity owners are unchanged. No new capacity, native-wire, GPU, live
MongoDB or mixed-version-cluster qualification is claimed.

## Alternatives

| Choice | Benefits | Costs and reason for the current proposal |
| --- | --- | --- |
| Keep nonidentity scalar UNION unsupported | Smallest operational and compatibility footprint; no new global materialization | Leaves the admitted correlated workload unsupported. This remains the conservative alternative if the proposed capacity envelope is unacceptable. |
| Per-outer correlated execution with local membership and early detection of a second distinct member | Simple occurrence identity and local per-occurrence membership state | Repeats scans/RPCs per outer occurrence; requires executor integration and care with evaluation/volatile semantics. It is not a planner-only substitution with proven lower total cost. |
| Distributed `(producer, sequence)` IDs and partitioned aggregation | Avoids global numbering/outer merge bottleneck | Needs uniqueness across producer generations, retries and distributed boundaries; expands the identity and recovery contract beyond this proposal. |
| Marked streaming WINDOW/LEFT JOIN/two logical AGGs/SINGLE composition | Preserves occurrence and cardinality semantics using existing operators, with explicit local admission | Can examine `O * I` candidate pairs, bounded by one shared work lease; participating retained memory and CN concurrency have finite admission. Broader distributed execution remains outside this revision. |

Within membership implementation, ordinary DISTINCT cannot distinguish signed
zeros, unconditional multirow-arm reuse loses membership, and unchanged protocol
advertisement exposes unknown-function execution on old workers. A set-specific
physical key plus explicit capability admission avoids changing shared scalar or
GROUP BY equality and avoids a new set executor; the bounded scalar composition has explicit typed marker fields.

## Compatibility, rollout and rollback

The accepted rollout scope is the automatically selected, resource-bounded local
route described above. Historical S1 workload restrictions are not current
activation gates. Retain the terminal 20833 behavior, participating-account limits
and coordinator-only scope when deploying; broader distributed execution and
universal latency or process-RSS guarantees remain unqualified.

Official main at this revision allocates MORPC127 to IndexSearchScan internal
search knobs, while function602/MORPC125 remains LOAD IGNORE foreign-key
rejection diagnostics. VIEWS metadata retains functions603/604 and floor126.
TIMESTAMPADD SECOND retains function601 and floor124. This PR's single
unpublished set-key/scalar-marker capability uses function605, sentinel606 and
MORPC **128**. The feature walker recognizes encoded function identity 605 without
depending on its display name. It sees hidden physical equality lists, grouping
keys and serialized SetOp.KeyExprs as well as ordinary expressions. The scalar composition adds no separate function identity. It adds
`plan.Node.scalar_set_reaggregation` field 96 and
`pipeline.Instruction.scalar_set_reaggregation` field 65. Marker-only integer
fallbacks require 128 too. Senders and receivers reject marked remote scopes at
all versions, before operator decoding, because a remote attempt cannot share
the coordinator's work/account lease. Local scope validation rejects remote
nodes, DOP above one, and marker placement outside WINDOW/LEFT JOIN. Official main
retains SELECT division diagnostics at 116, conditional on execution/session
policy. Set keys require 128 regardless of that policy; a combined expression
requires 128. Division diagnostics remain outside persisted-expression admission. Official main
adds collation-aware LEAST/GREATEST at 117; that independent feature remains
admitted at 117, and a combined expression with a set key requires 128.
The released table-collation function retains identity 600 and floor 118;
table-only expressions remain admitted at 118, while a combined expression
with a set key requires 128.

LOAD IGNORE alone retains its released function602 and floor125. It is recognized
independently of set keys: LOAD plus a set key or scalar marker requires128.
The released LOAD binding/native, placement/send/destination/receive and retained
plan rollback checks remain in force; temporal CTAS checks keep their125 fast
path only after the retained requirement has been checked. Set-key/marker
admission has its independent128 owner. Earlier unpublished set-key602/603 payloads
are historical artifacts, not aliases to the released LOAD602, VIEWS603/604 or
new set-key605 identities; this revision neither remaps nor replays them.

TIMESTAMPADD SECOND normalization alone retains function 601 and floor 124.
A combination with set keys or scalar markers requires 128; the actual-stream
handshake carries the maximum of these independently detected requirements.

Unsigned DECIMAL DIV alone retains its released floor 121. Combining DIV with a
set key or local scalar marker requires 128; a marker still prohibits remote
execution at every version. Historical DIV overload zero keeps its prior
compatibility contract.

Placement requires workers advertising 128 or newer; missing or old capabilities
fall back to one CN on the capable coordinator. Sending rechecks actual pipeline
destinations, including changes after placement. The receiver rejects a pipeline
containing these expressions before operator decoding when its local floor is
missing or below 128. Operator expression construction checks the same floor
before allocating an executor. Persisted-expression read and authoring admission
also require 128. Membership feature checks add no per-row work; the marked scalar stages have the explicit resource owners and predicate admission described above.

Prepared specialization regenerates membership keys from the resolved output
domain before capability detection: integer -> float -> integer must acquire and
then release the 128 requirement. A prepare-time ANY type is not capability proof.
A new binary configured at immediate predecessor floor 127 rejects these
expressions before execution. An actual old binary lacks the new guard: new
senders must exclude it, and durable authoring must not publish an expression
under a floor below 128. Lowering a new binary's floor does not rewrite existing
catalog expressions or teach an old binary identity 605.

VIEWS metadata alone requires126, including its actual-stream handshake. Combining
VIEWS metadata with a set key or scalar marker requires128. Literal IDs603/604
remain VIEWS functions; they are never recognized as set keys. Fresh605 FLOAT/DOUBLE
wire fixtures are checked against actual official-main127 source, where the key
is absent but both VIEWS functions, LOAD602 and TIMESTAMPADD601 remain available.

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

Dependent-only queries retain their independently released protocol 119.
Mixed dependent/scalar-set queries require the set-key floor 128 as well as
coordinator-local DOP 1 placement. The final scope graph, including detached
producer scopes, is validated before any execution. The statement attempt
acquires one scalar-set CN permit and one bounded participating account; all
main and nested dependent regions borrow them. No dynamic invocation acquires a
second permit, resets the shared candidate counter, or raises the memory limit.
Distributed scalar fallback remains outside the qualified scope.

## Evidence and acceptance record

The baseline below records historical correctness evidence. Revision 3 was
documentation-only; revision 4 rebases onto the stated official main and changes
the set-key protocol floor to 117. Historical 115/116 receipts are not rewritten
as new 116/117 evidence. Fresh boundary and composition checks are recorded
separately; unrelated correctness evidence remains reusable only where its
semantic inputs are unchanged. Successful tests are not independent design
acceptance or a capacity benchmark.

The following table preserves the review context of those historical revisions.
Its open review items describe their state at the time, superseded by the
revision-11 decisions and current evidence recorded after the table.

| Invariant or risk | Evidence and revision | Result and limitation | Historical review state (superseded) |
| --- | --- | --- | --- |
| Signed-zero membership, empty/real NULL, outer identity and scalar cardinality | Public `TestSetOperationSignedZero` at `fae17ae98d93f2b3e5e251dbc5649d643513af03`; preceding repair receipts report 117 named pass events (6.36s root, 8.438s package) | Pass includes grouped MAX, LIMIT 2, OFFSET, COUNT, repeated members, opposite-zero/finite-member 1242, and correlation partitions. One-CN correctness, not capacity. | Independent reconciliation/implementation review |
| Boundary routing, physical/correlation keys, pagination and SINGLE, volatile/prepared controls | Complete planner and compile package tests from that same repair; `-count=1` through repository CGo wrapper, 35.715s and 14.010s | Passed; gofmt/vet and incremental lint also passed (0 lint issues). Revision 3 ran no product tests; this is a historical receipt. | Independent implementation review |
| Historical encoded 599 overloads, hidden key traversal, prepare regeneration and 115/116 rejection | Retained compatibility tests described in the PR at the source baseline | Registry/protobuf, compiled grouping stages, old/missing/new/changed destinations, balanced releases, receive-before-decode, executor/persisted floors and integer/float/integer controls. Does not prove a live mixed-version upgrade or old-binary backup restore. | Execution/compatibility acceptance |
| Historical CI behavior | [Run 38826312235](https://github.com/matrixorigin/matrixone/actions/runs/38826312235), head `fae17ae98d93f2b3e5e251dbc5649d643513af03` | Required CI, Ubuntu UT, SCA, coverage and proxy/pessimistic multi-CN BVT succeeded; upgrade checks skipped. | No inference of upgrade or capacity acceptance |
| Selective typed-key filter work | Earlier unchanged-path receipt: 4,096 rows, one scan row per arm, two aggregation input rows, reported memory 262,270 bytes | A selective set-operation observation only; does not measure occurrence-based scalar fallback latency, concurrency or resident peak. | Separate from fallback acceptance |
| Rebased capability allocation | Set-key placement/send/receive/Prepare/persisted tests at 116/117, plus division-only and combined-feature controls | Focused compile/colexec/planner boundary checks pass (3.073s/2.088s/1.517s); full planner, compile, colexec, function and protobuf-plan packages also pass. Fresh receipts are recorded in the implementation PR; historical 115/116 receipts above retain their original provenance. | Independent implementation review |
| Rebased function 600 overloads, upstream 599 negative control and predecessor 117 rejection | Revision 9 on official main `aa8d4278bdb940fe2e2b50a02fc2d9c5c362c3b7`; complete protobuf-plan/function/planner/compile/colexec package suites passed | Fresh registry/encoded-feature and existing placement/send/receive/Prepare/executor/persisted tests; independent collation 117/division 116 controls retained. Not a live mixed-version cluster. | Execution/compatibility acceptance |
| Global merge and reaggregation capacity | Revision 6 benchmark and linked receipt above | Twelve actual fallback profiles and separate terminal internal roots; local targets met, including high-U/skew, measured account peaks, merge and spill/reload counters. Full allocator attribution and remote qualification are not claimed. | Planner/execution reviewers: supported production envelope and remaining protocol acceptance |
| Composed partial failure/cancel/reset/spill recovery | Revision 6 composed compile test plus named owner fault tests above | Typed 4 KiB rejection, WINDOW input / pre-reload cancellation, synchronous corrupt reload, first/final attempt file/handle/ledger cleanup and TP Reset/same-budget recovery. Mid-single-batch cancellation is not injected. | Execution reviewers: remaining composed boundaries |
| Historical design gate | [Review 5541720784](https://github.com/matrixorigin/matrixone/pull/30184#pullrequestreview-5541720784) requested ownership, capacity and alternatives | The earlier artifacts were supplied while full implementation review was deferred; independent approval had not been recorded at that stage. | Superseded by the independent revision-11 decisions below |

### Revision 11 independent acceptance

Both decisions below concern revision 11 at
[`e7dbe6a2cdd57d0c53221bafe35bae1c1f6b29e8`](https://github.com/matrixorigin/matrixone/commit/e7dbe6a2cdd57d0c53221bafe35bae1c1f6b29e8).
The reviewer is XuPeng-SH; the times are UTC. Design acceptance and implementation
approval were recorded as separate phases.

| Phase | Independent record | Decision and scope |
| --- | --- | --- |
| Design, 2026-10-17 19:03:36 UTC | [Review 5559536863](https://github.com/matrixorigin/matrixone/pull/30184#pullrequestreview-5559536863) | Accept revision 11's bounded local contract and advance the RFC to `in progress`: streaming occurrence IDs, shared memory/work admission, CN concurrency admission and fail-closed terminal proof. This decision did not approve implementation. |
| Implementation, 2026-10-17 19:07:25 UTC | [Review 5559558038](https://github.com/matrixorigin/matrixone/pull/30184#pullrequestreview-5559558038) | Approve the reviewed implementation after the separate design acceptance. The reviewer reports no remaining blocking finding within that review and requests this documentation synchronization. This is not a statement that every PR reviewer has approved. |

The implementation review independently reconciled all 63 delivered-source
fingerprints in the revision-11 receipt and reused its 21 profiles / 135 samples,
including exact/excess work boundaries. It also records passing public
signed-zero SQL, focused compile/process checks, targeted race checks and four
independent public-entry counterexamples. These are the reviewer's recorded
checks, not new measurements from this documentation update.

The accepted capacity scope is the enforced participating-account, shared-work
and concurrency envelope stated above, with terminal rejection at its limits.
The measured profiles do not establish a universal latency/RSS bound or qualify
distributed scalar execution. Historical revisions 5–10 and every receipt retain
their original source and measurement provenance. The RFC status now reflects
the independent revision-11 decision; no architecture or runtime contract is
changed by this synchronization.
