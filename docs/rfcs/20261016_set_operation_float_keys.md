- Status: draft (pending independent approval)
- Start Date: 2026-10-16
- Revision: 4
- Implementation PR: https://github.com/matrixorigin/matrixone/pull/30184
- Issue: https://github.com/matrixorigin/matrixone/issues/30155
- Original implementation baseline: `fae17ae98d93f2b3e5e251dbc5649d643513af03`
- Rebase baseline: official main `b24d1b311555e0b6f3d56eb37f9a2e9617e9a5b7`
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
it does not assert repository approval or advance implementation approval.

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
The internal `set_operation_float_key` registry identity is 599, with overloads
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
peak-memory and spill/disk budgets. No measured envelope is currently asserted.

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

Official main at this revision ends at MORPC 116. This capability uses MORPC
**117**. The feature walker recognizes encoded function identity 599 without
depending on its display name. It sees hidden physical equality lists, grouping
keys and serialized SetOp.KeyExprs as well as ordinary expressions. The scalar
composition adds no separate function identity or wire field. Official main
retains SELECT division diagnostics at 116, conditional on execution/session
policy. Set keys require 117 regardless of that policy; a combined expression
requires 117. Division diagnostics remain outside persisted-expression admission.

Placement requires workers advertising 117 or newer; missing or old capabilities
fall back to one CN on the capable coordinator. Sending rechecks actual pipeline
destinations, including changes after placement. The receiver rejects a pipeline
containing these expressions before operator decoding when its local floor is
missing or below 117. Operator expression construction checks the same floor
before allocating an executor. Persisted-expression read and authoring admission
also require 117. These checks add no per-row work or new resource owner.

Prepared specialization regenerates membership keys from the resolved output
domain before capability detection: integer -> float -> integer must acquire and
then release the 117 requirement. A prepare-time ANY type is not capability proof.
A new binary configured at immediate predecessor floor 116 rejects these
expressions before execution. An actual old binary lacks the new guard: new
senders must exclude it, and durable authoring must not publish an expression
under a floor below 117. Lowering a new binary's floor does not rewrite existing
catalog expressions or teach an old binary identity 599.

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
| Encoded 599 overloads, hidden key traversal, prepare regeneration and 115/116 rejection | Retained compatibility tests described in the PR at the source baseline | Registry/protobuf, compiled grouping stages, old/missing/new/changed destinations, balanced releases, receive-before-decode, executor/persisted floors and integer/float/integer controls. Does not prove a live mixed-version upgrade or old-binary backup restore. | Execution/compatibility acceptance |
| Historical CI behavior | [Run 38826312235](https://github.com/matrixorigin/matrixone/actions/runs/38826312235), head `fae17ae98d93f2b3e5e251dbc5649d643513af03` | Required CI, Ubuntu UT, SCA, coverage and proxy/pessimistic multi-CN BVT succeeded; upgrade checks skipped. | No inference of upgrade or capacity acceptance |
| Selective typed-key filter work | Earlier unchanged-path receipt: 4,096 rows, one scan row per arm, two aggregation input rows, reported memory 262,270 bytes | A selective set-operation observation only; does not measure occurrence-based scalar fallback latency, concurrency or resident peak. | Separate from fallback acceptance |
| Rebased capability allocation | Set-key placement/send/receive/Prepare/persisted tests at 116/117, plus division-only and combined-feature controls | Focused compile/colexec/planner boundary checks pass (3.073s/2.088s/1.517s); full planner, compile, colexec, function and protobuf-plan packages also pass. Fresh receipts are recorded in the implementation PR; historical 115/116 receipts above retain their original provenance. | Independent implementation review |
| Global merge and reaggregation capacity | Source model and protocol above | Full outer buffering and candidate multiplication are explicit. No supported numerical envelope or completed fallback benchmark is claimed. | Planner/execution reviewers: envelope, measured evidence and decision |
| Composed partial failure/cancel/reset/spill recovery | Existing cleanup owners mapped above | Delegation is documented; successful SQL/CI is not proof of every forced-budget failure path. | Execution reviewers: deterministic failure-path acceptance |
| Design gate | [Review 5541720784](https://github.com/matrixorigin/matrixone/pull/30184#pullrequestreview-5541720784) requested ownership, capacity and alternatives | This revision supplies those artifacts; the reviewer deferred full implementation review. No independent approval is recorded. | Independent reviewer approval of this exact revision |

Record any later acceptance with the immutable revision/commit link, reviewer,
decision, supported capacity envelope and evidence links. Advance status to
`in progress` only after independent approval; the author does not self-approve
or infer acceptance from CI. Until then, revision 4 remains a reviewable draft.
