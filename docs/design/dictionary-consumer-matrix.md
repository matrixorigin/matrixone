# Dictionary consumer migration matrix

Owner: #32385 / #32386, parent #32379. Prerequisite: #32405.
Approved combined design: `20261110-dictionary-dependent-prs.md`.
This matrix records implementation progress, not a capability declaration.
No production capability or producer is enabled until its closure passes.

| Closure | Current state | Required closure before admission |
|---|---|---|
| Function parameter wrappers | Implemented; vector owning-package tests pass | Fixed/varlen logical reads, conversion, cached-shape changes, raw-slice rejection |
| Function overloads/executors | Primitive comparisons/string IN and integer arithmetic opt in by runtime shape; other calls normalize | SIMD/SQL acceptance pending; fused decimals normalize referenced operands only |
| Vector/batch gather | Typed bulk gathers and selected preflight implemented; shared mappings/snapshots tested | Final selected pressure/cancellation and static review |
| Hash preparation/probe | Selected key paths and canonical spill hash implemented/tested | Preserve unsupported type/mode boundaries |
| HashBuild | Direct-column, non-dedup mode admitted and endpoint tested | Finish selected fault/pressure controls before producer rollout; computed keys and dedup stay flat |
| HashJoin | Residual-free INNER/probe-side SEMI/ANTI admitted and endpoint tested; results stay flat | Other modes retain flat ingress; markers normalize through producer state |
| Aggregates/Group | Basic non-DISTINCT aggregates admitted; decimal selected scalar/dense/general/bulk tested | DISTINCT, ordered/rollup/window and unknown aggregate contracts stay flat |
| Shuffle | Owned synchronous complete-key input admitted/tested; output partitions remain flat | Legacy hash/range/async input keeps normalization; dictionary partitions remain #32387 |
| Filter/Projection | Selected filters and forwarding projection enabled under account/capability admission | Pure forwarders propagate destination permission; unsupported modes stay flat |
| Reader/TableScan | Selection callback and retained output enabled; persisted tombstone/late-row test passes | Exact/Top-K/workspace fallbacks retain old scheduling; SQL/performance acceptance pending |
| Sort/window/frontend/spill/network | Explicit normalization | Keep existing formats and unknown-consumer default from #32405 |

## Contract and evidence

Values use selected physical indices; null/grouping/provenance use logical indices.
No raw getter or wrapper secretly allocates/materializes. Borrowed parameter
wrappers hold no independent lease and are only valid while the source is live.
The allocation owner, cancellation/reset/free closure and actual dependent test
must be recorded for every enabled consumer.

Performance uses one process/configuration, one load/open, six full suites,
first cold suite excluded, profiling/accounting over the five warm suites.
All Q1-Q22 and Total are reported. No benchmark-time result verification.
Final combined acceptance additionally requires attributable Q12 gain at SF10
and SF100 memory profiles, no unexplained all-query/Total regression, and no
SF100 spill. Results are appended as issue/PR comments.

### First implementation evidence

2026-11-10: scalar Go 1.27.1 / GOAMD64=v3, controlled CGo wrapper,
`go test -count=1 -timeout=120s ./pkg/container/vector` passed (0.357s).
New cases cover fixed/varlen selections, repetition, NULLs, constant/all-NULL
backing, input identity, cached representation transitions and raw-slice rejection.
No SQL producer is enabled; this is consumer infrastructure evidence only.
Matching native source and original jemalloc build flags were verified against
`dictionary-boundaries` before reusing its native artifacts for the test.

### Consumer implementation checkpoint

Expression compatibility storage now belongs to FunctionExpressionExecutor,
including normal/masked/folding entry points and Reset/Free. Unknown overloads
normalize; no overload has been opted in yet. Accounted input cannot silently
be copied into an unaccounted destination. Real lower-expression tests cover
scratch reuse, masking, source immutability and reset. Owning colexec tests passed
(0.181s) before the subsequent gather-all guard; rerun the affected closure when
operator admission is complete, rather than claiming final qualification now.

Vector UnionOne/UnionMulti/Union/UnionInt32/Copy and bulk UnionBatch now use
logical metadata with selected physical payload. GetUnionAllFunction routes
selected inputs through the direct bulk gather. Bulk flags and NULLs are covered
by focused tests; operator-owned preflight and alias failure controls remain.

Hash-map selected integer, composite CHAR/string and float probes are covered.
The float control explicitly checks -0/+0 equality and NaN nonmatching. The
hashmap owning package passed (0.022s); the subsequent NaN control passed (0.017s).
The nullable H8 test uses i32 (domain + value fits eight bytes); nonnullable H8
uses i64. SQL's wider nullable keys belong to the string hash map.

At that first checkpoint no operator admission or dictionary originator was enabled. No benchmark or
production-readiness claim is made by this checkpoint. Remaining work follows
the matrix above, including explicit spill/transport normalization and producer
admission before #32386 can qualify.

Latest checkpoint: all three owning packages passed together (vector 0.466s,
colexec 0.172s, hashmap 0.023s). The final fixed-byte read-view change then
passed the focused dictionary controls in all three packages (0.014s, 0.051s,
0.015s). Fixed and varlen hash payloads now resolve backing/mapping once per
batch; no type-erased getter or per-row dictionary resolution is needed in
those selected encoding loops. Fast production-convention checks passed;
full pre-push SCA and final performance acceptance have not been run.

Next implementation closure: close CopyIntoBatches selected ingress and
HashBuild recovery projections (which still call physical GetArea), then
selected spill serialization and real build/probe tests before admission.
Shuffle routing, aggregate direct reads, specialized expression direct support,
producer eligibility and the separate reader selection callback remain pending.

### HashBuild ingress checkpoint

Direct-column non-dedup HashBuild now declares dictionary read capability.
Computed keys/dedup retain the flat compatibility edge. A real MockOperator ->
HashBuild -> JoinMap test proves three selected rows, two groups, expected probes,
unchanged dictionary ingress and final memory cleanup. Accounted retained-copy
integration also passes through colexec.Batches with flat destination ownership.

Spill-recovery projection now sums selected varlen rows, including duplicates,
without calling physical GetArea on a dictionary. Shared spill routing uses
logical dictionary keys and canonical floating bytes. Spill partitions already
use UnionInt32 into owned flat scratch before existing serialization; no wire
format changes are introduced.

Focused ingress/recovery/retention checks passed. Owning HashBuild (0.267s) and
keycodec (0.041s) package checks passed. No dictionary producer is enabled yet;
operator consumer migration, selected pressure controls, reader/filter producers
and Q12 performance qualification remain required for the combined PR.

### Rebased combined implementation — 2026-11-10

Prerequisite #32405 merged in main 00da05219f. The unpublished branch was
rebased without inherited foundation commits. New selector allocations join
Compile's existing account attempt (append-only owner selection=16). Managed
reset retires prepared row storage before account terminal; external native
scan selections retain their existing provenance.

Real scan -> filter -> projection -> Group execution with automatic account
attachment passes and releases account usage on terminal cleanup. Persisted
reader selection preserves tombstones/original physical late-row mapping and
never calls Apply again after Select. Compact CHAR grouping remains active
on selected keys. SUM/AVG decimal controls cover widths 64/128/256, dense and
general groups, bulk/reuse, repeated/reordered/NULL input. HashJoin selected
INNER/SEMI/ANTI endpoints preserve logical duplicates and flat owned output.

Affected scalar owning packages passed in one closure: vector, batch, hashmap,
keycodec, mpool, VM, colexec, filter, projection, shuffle, hashbuild, hashjoin,
aggexec, group, table_scan, blockio and function. SIMD focused controls plus
the compiler account-lifecycle endpoint also passed. No timing is claimed.
Pending: public SQL expected-result generation/comparison, targeted race and
final self/static review, pinned builds and the six-suite SF10/SF100 comparison.
Q1 is a secondary scan-copy target; Q12 remains the primary required gain.
