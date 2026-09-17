# PR #28523: String Math Numeric Coercion and Prepared-Parameter Roles

- Status: Draft / awaiting maintainer approval
- Design revision: 7
- Issue: [#28487](https://github.com/matrixorigin/matrixone/issues/28487)
- Implementation PR: [#28523](https://github.com/matrixorigin/matrixone/pull/28523)
- Current upstream main base: `24e66eba121c781c29998ff2611db11335e3028c`
- Current main tree: `b717729c7e1b0cc33d808f271f3211e8d6028f37`
- Current source inputs: original PR head `d27667a4936e813fb612de6cd245be03a2d6f101`
  (tree `d02aa9a8b2c219b7767b9793c700287ecfc24c0b`) and current main tree
  `b717729c7e1b0cc33d808f271f3211e8d6028f37`. The original 16 PR commits
  plus one test/documentation follow-up were replayed onto main
  `24e66eba121c781c29998ff2611db11335e3028c` in a clean detached worktree;
  all 17 commits applied without conflicts. The source/test snapshot before
  this design-record update, with the design file at its original PR contents,
  is `1e363305ec142410f5f9e95c803d8e0db18f5127`.
- The initial rebase had two shared paths with main since the historical
  base, `pkg/sql/plan/base_binder.go` and `pkg/sql/plan/utils.go`; both applied
  cleanly. The `4c31142` to `a3ede72` delta added 18 paths and the subsequent
  `a3ede72` to `5453d72` delta added 14 paths; neither overlaps the current PR
  file list.
- Upstream changes since the historical `4ff27bb9b35c43c1b0961bb9a01bf8fc0b6a2171`
  base are `25794ebdf4814f3f2e17b90416432a7405da5ea2` (`fix(plan): honor
  NO_UNSIGNED_SUBTRACTION`), `0370bb4d6b118da29e164c54aa34ee010ed897bc`
  (`test: avoid DWARF linking for temporary race binaries`),
  `7f9505e7b8eb4ba45264114a76e3145f6fa26c7a` (`fix(fulltext2): retain
  non-null columns in multi-column indexes`),
  `5bf6d24157959febb839615462c30eafb5046fc9` (`feat: support prepared and
  sortable discrete percentiles`), and `4c31142f4dbd0c46e9674b365826acc015b35ac2`
  (HAKeeper catalog metadata design checkpoint), `e66c9da813ccbb8b9d1eedbdc40b619b7ee5dff3`
  (`test(iscp): prepare sink fixtures and avoid redundant target DDL`), and
  `a3ede72bef53bcd72a10b7bba17002f19f8132cf`
  (`fix: make aggregate states version compatible`), `db3915689fc9bccfd58a1a05b38b6e5329d43190`
  (`fix(snapshot): reject RESTORE TABLE of referenced tables`), and
  `5453d72b7372a9f77263d1dd4c982e616446d4cb`
  (`fix(fulltext2): avoid boxing loaded UUID membership probes`), and
  `24e66eba121c781c29998ff2611db11335e3028c`
  (`fix: release embedded cluster ownership after terminal cleanup`).
- Historical code/test baseline: `d56711fa5b429e5e6e52f64f603d2e853478edca`
  on base `4ff27bb9b35c43c1b0961bb9a01bf8fc0b6a2171`; its 14-commit rebase,
  validation, and gofmt-only correction remain historical evidence below.
- Prior independent design review: GPT-6 Astra, medium reasoning, reviewed the
  historical `d56711fa5b429e5e6e52f64f603d2e853478edca` candidate against
  base `4ff27bb9b35c43c1b0961bb9a01bf8fc0b6a2171`; this is historical review
  evidence. Maintainer approval remains pending.
- Review trigger: review `5199052257` identified a major-refactor/compatibility design gate; review `5214666396` and comment `5687377735` require incomplete numeric strings to be mode-gated

This document is the stable design revision requested before implementation
approval. It must not be read as a maintainer approval until the approval
record at the end is filled by an authorized reviewer.

## 1. Problem, goals, and non-goals

String inputs to `ABS`, `SIGN`, `CEIL`/`CEILING`, `FLOOR`, `MOD`, `ROUND`, and
`TRUNCATE` previously took different conversion paths depending on whether the
source was a literal, column, or prepared parameter. The reachable failures
included lost fractional prefixes, discarded binary-literal provenance,
missing coercion warnings, unsafe zonemap pruning, historical native-mode
divergence, and prepared `ROUND`/`TRUNCATE` precision being converted through a
permissive `DOUBLE` source.

The goals are:

1. Equivalent character sources use one numeric-prefix contract for the same
   type family, compatibility mode, binary provenance, and expression role.
2. Native integer, floating-point, and DECIMAL sources retain their native
   overload and precision semantics.
3. Prepared value arguments and control arguments have separate contracts, and
   execute-time specialization never reuses a previous value or source type.
4. Existing serialized function identities remain safe across old plans and
   unknown identities fail as errors rather than panics.
5. String conversions cannot enable unsound function-wide zonemap pruning.

Non-goals:

- implementing every MySQL mathematical-function compatibility difference;
- making MySQL and MatrixOne native modes identical;
- changing the existing strict string-to-`INT64` precision contract into a new
  general integer-prefix feature;
- adding a new RPC/catalog schema, rollout protocol, or long-lived cache; and
- claiming that an old MatrixOne node has every corrected semantic of a new
  node merely because an overload identity remains decodable.

## 2. Frozen semantic invariants

### 2.1 Literal, column, and prepared sources

For the same logical source type, binary-source marker, compatibility mode, and
argument role, literals, columns, and SQL `EXECUTE` must observe the same
conversion result, warning behavior, and NULL propagation. A textual value
that looks numeric does not force the result metadata to be `DOUBLE`:

- character sources use the string numeric-conversion path;
- native integer, DECIMAL, and floating-point sources participate in native
  overload selection;
- COM_STMT source types follow protocol metadata and existing reprepare rules;
- explicit `CAST` remains an explicit semantic boundary; and
- SQL `NULL` remains NULL, including for masked or unevaluated rows.

Prepared plan specialization must deep-copy expressions before changing source
nodes. The prepared base plan keeps `ParamRef` provenance so a later execution
obtains its current value and source type rather than inheriting the prior
execution's expression.

### 2.2 Value and control roles

| Function | Value arguments | Control arguments |
| --- | --- | --- |
| ABS, SIGN, CEIL/CEILING, FLOOR | arg0 | none |
| ROUND, TRUNCATE | arg0 | arg1: precision, INT64 |
| MOD | arg0 and arg1 | none |

Only value arguments may use the permissive string-to-`DOUBLE` source. The
`ROUND`/`TRUNCATE` precision argument remains an `INT64` control value:

- a string precision is not first converted to `DOUBLE` and then rounded;
- native BOOL, integer, DECIMAL, and FLOAT sources are materialized from their
  actual source type and then converted to the existing INT64 target;
- explicit `CAST(? AS SIGNED/DOUBLE)` keeps the existing CAST contract;
- a marker inside `ROUND(x, ABS(?))` belongs to the inner ABS value role before
  its result becomes the outer precision input; and
- an expression such as `ROUND(x, ? + 0)` follows its own arithmetic contract
  and is not treated as a bare precision marker.

### 2.3 Compatibility mode, binary provenance, and warnings

The latest review feedback (review `5214666396`, comment `5687377735`) makes
the mode boundary explicit: a character value with a suffix such as
`'1.5tail'` must not be converted permissively unless MySQL compatibility is
selected. The existing `MATRIXONE_NATIVE` SQL-mode bit is the selector:

| Effective process mode | `'1.5tail'` and other incomplete tokens |
| --- | --- |
| MySQL compatibility (the bit is absent) | Consume the decimal prefix and emit the existing truncation warning. A wholly non-numeric value becomes zero with a warning; an empty string remains zero without a warning. |
| MatrixOne native (`MATRIXONE_NATIVE` is present) | Reject the incomplete, non-numeric, or empty token with the native conversion error and emit no MySQL truncation warning. |

The default SQL mode currently omits `MATRIXONE_NATIVE`, so the default session
continues to use the established MySQL-compatible contract. This is a
compatibility-gated behavior, not a global default change; callers requiring
the stricter rule must select `MATRIXONE_NATIVE`. NULL and masked rows do not
create extra conversion warnings in either mode.

Historical serialized CEIL/FLOOR VARCHAR overloads must derive this process
compatibility mode as well. They must not hardcode MySQL mode or emit
MySQL-only warnings in native mode. Every direct string executor and the
prepared/cast path is required to make the same mode decision at execution
time.

HEX/BIT source provenance remains observable: `X'31'` is interpreted by its
binary value (49), not as character text (`'1'`). Ordinary binary string types
and HEX/BIT literal provenance are distinct. Rebinding, deep-copy, and
parameter-restoration paths must preserve the marker that affects this result.

### 2.4 Overload and mixed-version safety

The implementation reuses existing numeric overload identities and existing
warning-aware/binary-aware casts instead of introducing new serialized string
overload IDs. `validFunctionOverloadID` validates both function and overload
indices, including negative and out-of-range values. This bounds check is a
panic-safety rule, not a claim that all old and new nodes have identical
semantics. No new protobuf or catalog format is introduced.

## 3. Ownership and execution design

The execution chain is:

```text
prepare role/source classification
  -> execute source identification
  -> separate value/control binding
  -> existing overload/cast execution
  -> specialization and cache restoration
```

- The binder owns provisional prepare-time types and fallback metadata.
- `ResetParamRefRule` owns the current execution's source expression and
  rebind state.
- Value-source and generic numeric-source metadata remain separate; one
  occurrence of a parameter cannot change another occurrence's role.
- The cached base plan retains parameter provenance, and a failed execution
  cannot publish its partially specialized expression into the next execution.
- Existing aggregate/configuration metadata and explicit-cast semantics remain
  owned by their existing paths.
- No new goroutine, background resource, external I/O, or unbounded retry is
  introduced.

## 4. Zonemap correctness and performance

String-to-number conversion is not monotonic under string ordering. For
`'10'`, `'2'`, and `'30'`, string endpoints cannot prove the numeric interval
contains or excludes `2`. Therefore CEIL/FLOOR/ROUND string paths do not expose
a function-wide zonemap flag. The revision-5 decision proposal is to accept the
loss of those numeric pruning opportunities for this PR because correctness is
observable and the old flag could prune matching rows. A future
overload-aware monotonicity proof may restore safe pruning independently; no
such proof is part of this change. Maintainer acceptance of this trade-off
remains pending.

Execute-time source discovery scans the plan for each parameter. With `P`
parameters, `N` expression nodes, and nesting depth `D`, the expected cost is
at least `O(P*N)` and can approach `O(P*N*D)` (or `O(P*N^2)` for repeated deep
subtree checks). Source arrays are `O(P)`, while traversal state follows
expression depth. The implementation does not claim zero specialization cost.

Historical revision-4 measurement (Apple M1, macOS arm64, Go 1.27.0; one CPU;
`-benchtime=1s -count=5`) used reproducible benchmark commands. It predates the
revision-6 candidate and is not a measurement of the rebased implementation:

```text
go test -mod=readonly -run '^$' -bench '^BenchmarkPreparedStringMath(Eligibility|Specialization)$' -benchmem -benchtime=1s -count=5 -cpu=1 ./pkg/sql/plan
go test -mod=readonly -run '^$' -bench '^BenchmarkPreparedStringMathRoleDiscovery$' -benchmem -benchtime=1s -count=5 -cpu=1 ./pkg/sql/plan
```

The eligibility benchmark builds plans with eligible decimal peers, text-only
no-match trees, parameter counts `P={1,8,32,128}`, nesting depths
`D={0,1,8,32,64}`, and noise nodes `N={0,32,128}`. Its `SourceDiscovery`
numbers are an eligibility-helper baseline, not the production role-discovery
loop. The role benchmark calls `preparedParamUsesStringMathFunction` for every
parameter position, and asserts value/control ownership for ABS, ROUND,
nearest-role nesting, text-only no-match, and a mixed `P=5/N=128` plan. Values
below are medians of the five samples; `B/op` and `allocs/op` are stable across
the samples.

| Path/case | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| eligibility baseline, P1/D0 | 3,557 | 0 | 0 |
| eligibility baseline, P8/D0 | 3,134 | 0 | 0 |
| eligibility baseline, P32/D0 | 5,058 | 2,120 | 7 |
| eligibility baseline, P128/D0 | 10,944 | 9,352 | 11 |
| eligibility baseline, P1/D8 | 3,463 | 0 | 0 |
| eligibility baseline, P1/D64 | 6,748 | 0 | 0 |
| eligibility baseline, P1/D8/N128 | 4,548 | 0 | 0 |
| eligibility baseline, no-match P8/D8/N128 | 5,931 | 0 | 0 |
| role discovery, ABS value P1 | 2,902 | 0 | 0 |
| role discovery, ROUND control P1 | 2,916 | 0 | 0 |
| role discovery, ABS(ROUND precision) P1 | 2,921 | 0 | 0 |
| role discovery, ROUND(ABS value) P1 | 2,977 | 0 | 0 |
| role discovery, CONCAT no-match P1 | 2,836 | 0 | 0 |
| role discovery, deep no-match P8/D8/N128 | 48,736 | 0 | 0 |
| role discovery, mixed roles P5/N128 | 22,908 | 0 | 0 |
| generic specialization baseline, P1/D0 | 37,956 | 18,120 | 197 |
| generic specialization baseline, P8/D0 | 236,519 | 109,560 | 1,253 |
| generic specialization baseline, P32/D0 | 999,358 | 443,481 | 4,887 |
| generic specialization baseline, P128/D0 | 5,090,430 | 1,766,494 | 19,148 |
| generic specialization baseline, P1/D8 | 113,248 | 123,120 | 1,331 |
| generic specialization baseline, P1/D64 | 1,899,876 | 3,482,907 | 38,253 |
| generic specialization baseline, P1/D8/N128 | 191,706 | 227,472 | 2,385 |

The deep no-match role case is intentionally included in the benchmark; its
five-sample median is recorded above and all eight expected `false` results are
asserted before timing. The generic specialization rows include the existing
comparison/common-type plan shapes; they are a rebinding baseline, not a claim
that every string-math executor has identical cost.

The specialization rows include `DeepCopyPlan` and the complete rebinding
walk, so they are end-to-end execute-copy costs rather than scan-only costs.
The decision proposal is to retain this bounded scan and defer a one-pass
occurrence-role table until a separate change can specify cache invalidation
and prove equivalent ownership. The benchmark measures one scan per call; it
does not invent a cache-hit rate. Existing cache/reuse tests cover type-change,
error-to-success, NULL, and restoration transitions, while production hit/miss
telemetry is not exposed by this path. Consequently no cache-hit acceptance
claim is made here; the scan-cost and optimization trade-off still require an
authorized maintainer decision.

## 5. Alternatives considered

| Alternative | Decision |
| --- | --- |
| Keep the old paths | Rejected; the reproduced semantic and safety failures remain. |
| Add separate string overloads for every type | Rejected; it expands serialized IDs, rollout gates, and duplicate warning/binary logic. |
| Convert every argument to DOUBLE | Rejected; it breaks precision controls, exact integers, DECIMAL, and explicit type contracts. |
| Reuse existing overloads with role-aware sources | Selected; it preserves identities and centralizes mature conversion behavior. |
| Build one occurrence-role table per plan | Deferred; it may reduce repeated scans but needs explicit cache-invalidation rules and measurements. |

## 6. Validation and counterexample matrix

| Risk | Required evidence |
| --- | --- |
| String prefix and type families | planner/function tests plus literal, VARCHAR/CHAR/TEXT column, and SQL EXECUTE paths |
| Binary provenance | direct/prepared HEX/BIT controls and result assertions |
| Warnings and native mode | warning-session tests plus direct ABS/SIGN/CEIL/FLOOR/ROUND/TRUNCATE and historical CEIL/FLOOR overload-12 tests in both modes; incomplete tokens must fail only under `MATRIXONE_NATIVE` |
| Precision role isolation | direct/prepared ROUND/TRUNCATE, nested expressions, explicit casts, INT64 plan assertions |
| Native precision sources | BOOL, integer, DECIMAL, and FLOAT SQL sources; result and plan metadata comparison |
| Cache/reuse | parameter type changes, repeated executions, error -> success -> NULL -> success, and source restoration |
| Unknown/old identities | negative/unknown/out-of-range function and overload IDs; old identity lookup without panic |
| Zonemap equivalence | string blocks containing endpoint traps; matching rows must not be pruned |
| Distributed/compatibility | Compose and standalone BVT, explicit MySQL/native SQL-mode BVT controls for literal/column/prepared paths, cross-CN index controls, and serialization-boundary review |
| Resource/state safety | race tests, cancellation/error return paths, and no stale expression reuse after failure |
| Performance | parameterized plan-size/depth measurements before any scan optimization |

Historical rebase integration history (superseded by the current source snapshot above):

- Rebased onto the verified main commit `8e8e1998ef02b1f6233ddb1c2d3208f3b70d2d83`,
  a descendant of the earlier `049dad215cf69bad4e8bd9911169aabe7493d691` base.
  The rebase conflicts in `pkg/sql/plan/utils.go` and
  `pkg/sql/plan/visit_plan_rule.go` were resolved locally.
- Rebased again onto current main `370c310a994de258ee01e87c31062590a7022f34`;
  all ten commits replayed without further conflicts.
- The intermediate rebase onto main `a52a665ec1e6670d1ee4d84831f24a1b83cd4f0b`
  (ordered-percentile spill fix) replayed twelve commits without conflicts; it
  is historical, not the current base.
- Final rebase onto main `4ff27bb9b35c43c1b0961bb9a01bf8fc0b6a2171` replayed 14
  commits cleanly; current HEAD is `d56711fa5b429e5e6e52f64f603d2e853478edca`.
- The upstream two-view `rebindPreparedNumericExprWithBound(expr, bound,
  positions)` remains the rebinding foundation, including recursive bound-child
  propagation, scalar-subquery refresh, explicit-cast metadata, unsupported and
  NULL bound preservation, and arity-aware ABS/SIGN/ELT handling.
- The integration keeps the PR's separate SQL-mode-aware string-math source
  scoped to the nearest value-role occurrence. ROUND/TRUNCATE precision
  occurrences retain their already-materialized INT64 cast even when they share
  a ParamRef position with a value occurrence; deferred ABS/SIGN value callers
  explicitly supply the value role, while ELT's index does not consume that
  source. Explicit casts and binary provenance are preserved.
- Generic runtime string handling retains the rebased main behavior: complete
  numeric text with a string runtime type is refined using its full numeric
  type, while unsupported/incomplete text does not invent a numeric source and
  keeps the already-materialized bound occurrence. The dedicated permissive
  SQL-mode-aware source remains limited to eligible string-math value roles.
- A user-authored explicit CAST is a hard source-conversion boundary during
  role-aware fallback rebinding. The surrounding value role cannot replace its
  string operand with the permissive DOUBLE source; the current EXECUTE's
  materialized source and explicit cast are retained.
- Regression coverage adds unsupported/NULL-bound preservation, nested
  ABS/ROUND/TRUNCATE role ownership, shared-ParamRef channel isolation,
  same-template MySQL/native mode flips, and DECIMAL scale assertions for
  precision-zero ROUND/TRUNCATE results. Revision 7 additionally covers
  `ELT(?, ...)` with `RuntimeType=TEXT` for complete numeric text and `foo`,
  unsupported-text bound identity, and direct-versus-prepared
  `ROUND`/`TRUNCATE(CAST(? AS SIGNED), ?)` plan/source/result equivalence.

Initial rebase integration evidence (main `4c31142f4dbd0c46e9674b365826acc015b35ac2`):

- Upstream compare from historical base `4ff27bb9b35c43c1b0961bb9a01bf8fc0b6a2171`
  to main `4c31142f4dbd0c46e9674b365826acc015b35ac2` contains five commits and
  52 changed paths.
- The GitHub PR files API at this checkpoint listed 24 PR paths. The two shared paths are
  `pkg/sql/plan/base_binder.go` and `pkg/sql/plan/utils.go`; all 16 original PR
  commit patches replayed in order onto the current main without conflicts.
  The rebased code/test tree is `0de7f80912eadd97754fb371a33d03ca7843e619`
  before this metadata-only design-record update. Review, CI, and BVT records
  are tracked separately in the PR/evidence ledger.
- Maintainer design approval remains pending.

Main integration through `a3ede72` (intermediate checkpoint):

- The new main delta is `e66c9da813ccbb8b9d1eedbdc40b619b7ee5dff3`
  (`test(iscp): prepare sink fixtures and avoid redundant target DDL`) followed
  by `a3ede72bef53bcd72a10b7bba17002f19f8132cf`
  (`fix: make aggregate states version compatible`). The 18 changed paths are:
  `pkg/iscp/mock_consumer.go`, `pkg/iscp/mock_consumer_test.go`;
  `pkg/sql/colexec/aggexec/aggState.go`, `aggexec_compat_test.go`,
  `capacity_preflight.go`, `count2_review_test.go`, `distinct_spill.go`,
  `distinct_spill_test.go`, `types.go`;
  `pkg/sql/colexec/group/exec2.go`, `group_test.go`, `helper.go`,
  `mergeGroup.go`, `types2.go`; `pkg/sql/compile/compile.go`,
  `pkg/sql/compile/compile_test.go`; and
  `pkg/vm/engine/test/cdc_testutil.go`,
  `pkg/vm/engine/test/change_handle_test.go`.
- The fetched PR-file list and new-main path list have zero overlap. Both
  commit diffs applied cleanly in order. The clean latest-main rebase and
  validation below supersede this intermediate checkpoint.
- The latest role/materialization test and local coverage evidence is recorded
  below for this checkpoint. It is not the repository's merged changed-line
  coverage result; no CI pass is claimed.

Intermediate main integration evidence (`5453d72`):

- The subsequent main delta is `db3915689fc9bccfd58a1a05b38b6e5329d43190`
  (`fix(snapshot): reject RESTORE TABLE of referenced tables`) followed by
  `5453d72b7372a9f77263d1dd4c982e616446d4cb`
  (`fix(fulltext2): avoid boxing loaded UUID membership probes`), producing
  main tree `9d637166adc3c93cb5aaaf696004b3f74eb9a6eb`. Its 14 paths are:
  `pkg/frontend/snapshot.go`, `pkg/frontend/snapshot_test.go`;
  `pkg/fulltext2/coverage_query_membership_test.go`, `membership.go`,
  `uuid_membership_bench_test.go`, `uuid_membership_test.go`;
  `test/distributed/cases/fulltext2/fulltext2_membership.result` and `.sql`;
  and the six `test/distributed/cases/snapshot` FK-restore `.sql` / `.result`
  files (`restore_fk_restore_master_table`, `restore_fk_table`, and
  `restore_table_referenced_by_fk`).
- REST path comparison confirmed zero overlap with the 24 current PR files.
  Both commit diffs applied cleanly in order. A clean detached worktree then
  replayed all 16 PR commits directly onto `5453d72` without conflicts; the
  source/test snapshot before this design-record update is
  `ad88c4dcaf67ff148c5a274f52180cc4f4d7bbaf`.

Latest-main integration evidence (`24e66eb`):

- Main advanced from `5453d72` to `24e66eba121c781c29998ff2611db11335e3028c`
  (tree `b717729c7e1b0cc33d808f271f3211e8d6028f37`). The 11 changed paths
  are `pkg/cnservice/{server.go,server_test.go,sirius_runtime.go,types.go}`,
  `pkg/embed/{cluster.go,cluster_test.go,operator.go,testing.go,testing_test.go}`,
  and `pkg/frontend/{server.go,server_test.go}`.
- The upstream compare has zero overlap with the 24 PR paths. Rebase
  `git rebase --onto 24e66eba121c781c29998ff2611db11335e3028c 5453d72b7372a9f77263d1dd4c982e616446d4cb`
  replayed the 16 PR commits and the test/documentation follow-up without
  conflicts.

The pre-rebase head `85635cc` passed required CI run `34813866468` (SCA,
Ubuntu UT, coverage, build, Compose/Standalone BVT, and CI Required); it is
historical evidence. The previously
recorded full package, frontend, race, vet, build, and protocol-smoke results
were collected against base `66b1672403e9b9efb1971d00b7ea87572891fe2a`; those
results are historical as well.

Historical evidence on the intermediate base `a52a665ec1e6670d1ee4d84831f24a1b83cd4f0b`
and candidate `ef4424cc0b4206d804537432b5599e5c5d0f9881` remains in the record:
its focused selection passed and its full CGo-wrapped `pkg/sql/plan` and
`pkg/sql/plan/function` packages passed in 6.940s and 15.803s. These results do
not describe the current source inputs above; they remain historical evidence.

Historical post-rebase evidence applies to the code/test baseline
`d56711fa5b429e5e6e52f64f603d2e853478edca`, based on main
`4ff27bb9b35c43c1b0961bb9a01bf8fc0b6a2171` (14 commits replayed cleanly),
plus the gofmt-only indentation correction to
`pkg/sql/plan/visit_plan_rule_test.go`. The full plan package and the integration
regression below were rerun after that correction.

The following exact CGo-wrapper selection was first listed and then executed.
`-list` returned seven planner tests and three function tests (non-empty
selection); focused execution passed with exit code 0:

```text
.agents/skills/mo-dev/scripts/mo-cgo-test -list '^(TestPreparedEltRebindsRuntimeNumericDomain|TestPreparedEltRebindsNumericTextWithStringRuntimeMetadata|TestPreparedSignRebindsRuntimeNumericDomain|TestPreparedMathStringValueAndPrecisionRoles|TestPreparedNumericRebindPreservesUnsupportedAndNullBoundOccurrences|TestPreparedMathStringParametersRebindToNumericOverloads|TestPreparedNestedMathStringParameterRebindsToNumericOverload|TestDirectMathStringExecutorsHonorNativeMode|TestMathStringExecutorsPreserveBinaryLiteralProvenance|TestMathStringExecutorsEmitNumericCoercionWarnings)$' ./pkg/sql/plan ./pkg/sql/plan/function
.agents/skills/mo-dev/scripts/mo-cgo-test -v -count=1 -timeout=180s -run '^(TestPreparedEltRebindsRuntimeNumericDomain|TestPreparedEltRebindsNumericTextWithStringRuntimeMetadata|TestPreparedSignRebindsRuntimeNumericDomain|TestPreparedMathStringValueAndPrecisionRoles|TestPreparedNumericRebindPreservesUnsupportedAndNullBoundOccurrences|TestPreparedMathStringParametersRebindToNumericOverloads|TestPreparedNestedMathStringParameterRebindsToNumericOverload|TestDirectMathStringExecutorsHonorNativeMode|TestMathStringExecutorsPreserveBinaryLiteralProvenance|TestMathStringExecutorsEmitNumericCoercionWarnings)$' ./pkg/sql/plan ./pkg/sql/plan/function
```

The focused run passed the upstream ELT 10-case rebind regression plus the new
`RuntimeType=TEXT` complete/unsupported text cases; ABS/SIGN runtime-domain
rebinding; unsupported and NULL bound preservation; nested value/control role
tests including `ABS(ROUND(1, ?))`, `ROUND(1, ABS(?))`, and the TRUNCATE
analogues; shared-ParamRef source-type/error isolation; explicit CAST source
preservation with direct/prepared ROUND and TRUNCATE results; a prepared
template MySQL/native/MySQL mode flip; binary-literal provenance; and direct
native-mode/warning executor cases.

Full CGo-wrapped package tests passed with exit code 0 on the post-rebase
candidate:

| Package | Result |
| --- | --- |
| `./pkg/sql/plan` | passed, 10.137s |
| `./pkg/sql/plan/function` | passed, 21.960s |
| `./pkg/frontend` | passed, 25.729s |

After the gofmt correction to `pkg/sql/plan/visit_plan_rule_test.go`,
`./pkg/sql/plan` passed again with the CGo wrapper in 6.219s. CGo-aware
`go vet` passed for `./pkg/frontend`, `./pkg/sql/plan`,
`./pkg/sql/plan/function`, and `./pkg/tests/issues`. The real-protocol issue
regression also passed after the correction:

```text
.agents/skills/mo-dev/scripts/mo-cgo-test -v -count=1 -timeout=600s -run '^TestIssue27294PreparedNumericOverloads$' ./pkg/tests/issues
--- PASS: TestIssue27294PreparedNumericOverloads
PASS
```

`gofmt` and `git diff --check` passed.

Previous local validation applies to the source-tree integration snapshot on
main `0370bb4d6b118da29e164c54aa34ee010ed897bc`. The candidate's native input
trees were checked against the cached artifact provenance before the CGo
wrapper was run. All commands used Go 1.26.4 darwin/arm64; these results do not
cover the three later upstream commits now in the rebase base:

```text
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=600s ./pkg/sql/parsers/dialect/mysql
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=600s ./pkg/sql/plan/function
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=600s ./pkg/sql/plan
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=600s ./pkg/frontend
```

| Package | Result |
| --- | --- |
| `./pkg/sql/parsers/dialect/mysql` | passed, 1.358s |
| `./pkg/sql/plan/function` | passed, 17.835s |
| `./pkg/sql/plan` | passed, 7.680s |
| `./pkg/frontend` | passed, 26.666s |

The absolute Go 1.26.4 `gofmt -d` check on all candidate Go files produced no
output; `git diff --check` passed. The four package results are local evidence
only. Commit replay, review, CI, and BVT records are tracked in the PR/evidence
ledger; maintainer approval remains pending.

Previous post-rebase validation on the `4c31142` checkpoint applies to the rebased code/test tree
`0de7f80912eadd97754fb371a33d03ca7843e619` on main
`4c31142f4dbd0c46e9674b365826acc015b35ac2`. All commands used Go 1.26.4
darwin/arm64 and the CGo wrapper:

```text
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=600s ./pkg/sql/parsers/dialect/mysql
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=600s ./pkg/sql/plan/function
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=600s ./pkg/sql/plan
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=600s ./pkg/frontend
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -v -count=1 -timeout=600s -run '^TestIssue27294PreparedNumericOverloads$' ./pkg/tests/issues
```

| Package/regression | Result |
| --- | --- |
| `./pkg/sql/parsers/dialect/mysql` | passed, 1.203s |
| `./pkg/sql/plan/function` | passed, 16.386s |
| `./pkg/sql/plan` | passed, 7.148s |
| `./pkg/frontend` | passed, 23.929s |
| `TestIssue27294PreparedNumericOverloads` | passed, 15.207s |

These are local results, not CI or distributed BVT results. Maintainer design
approval remains pending.

Validation on the `a3ede72` intermediate checkpoint applies to candidate source/test tree
`bf9de5ae36d64211c8ccddfef6f9d8afe22b2903` on main
`a3ede72bef53bcd72a10b7bba17002f19f8132cf` (tree
`f6b770d78e64c7064364cacaf603f7b27c41fadc`). Commands used Go 1.26.4
darwin/arm64 and the CGo wrapper.

The focused prepared-role/materialization selection passed after the new main
delta; both tests and all ten named subtests ran:

```text
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -v -count=1 -timeout=180s -run '^(TestPreparedStringMathRoleDiscoveryAcrossExpressionContainers|TestPreparedPrecisionFallbackMaterializesOnlyMatchingParam)$' ./pkg/sql/plan
```

The current CGo coverage profile also passed:

```text
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -coverpkg=./pkg/sql/plan/... -coverprofile=/tmp/mo28523-current-coverage.5xiLGz/coverage.out -count=1 -timeout=180s ./pkg/sql/plan ./pkg/sql/plan/function
```

The profile reported `pkg/sql/plan` 51.8% and `pkg/sql/plan/function` 29.0%
of statements in `./pkg/sql/plan/...`. `exprContainsPreparedPosition`,
`preparedParamUsesStringMathFunction`, `preparedExprUsesStringMathValueArg`,
`preparedExprContainsStringMathFunction`, and `materializePreparedParam` are
100%; `rebindPreparedNumericExprWithRole` is 74.5%. These are local
package/profile results, not the repository's PR-wide merged changed-line
coverage gate.

The requested owning and dependent package checks all passed with exit code 0:

```text
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -p=1 -count=1 -timeout=600s ./pkg/sql/plan ./pkg/sql/plan/function ./pkg/sql/compile ./pkg/sql/colexec/aggexec ./pkg/iscp
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -p=1 -count=1 -timeout=600s ./pkg/frontend ./pkg/sql/parsers/dialect/mysql
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -v -count=1 -timeout=600s -run '^TestIssue27294PreparedNumericOverloads$' ./pkg/tests/issues
```

| Package/regression | Result |
| --- | --- |
| `./pkg/sql/plan` | passed, 6.950s |
| `./pkg/sql/plan/function` | passed, 16.980s |
| `./pkg/sql/compile` | passed, 6.596s |
| `./pkg/sql/colexec/aggexec` | passed, 3.537s |
| `./pkg/iscp` | passed, 1.530s |
| `./pkg/frontend` | passed, 26.331s |
| `./pkg/sql/parsers/dialect/mysql` | passed, 0.975s |
| `TestIssue27294PreparedNumericOverloads` | passed, 11.54s test / 13.794s package |

`gofmt -d` on candidate Go files produced no output; `git diff --check` and
`git diff --cached --check` passed. Link commands emitted duplicate-rpath and
duplicate-library warnings, but all test commands exited 0. No distributed
BVT or current CI run is claimed; maintainer design approval remains pending.

Intermediate-base diagnostic validation on `5453d72` applies to the staged
candidate worktree snapshot on main tree
`9d637166adc3c93cb5aaaf696004b3f74eb9a6eb`. The two newly affected packages
and the prepared end-to-end regression were rerun after this main delta:

```text
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -p=1 -count=1 -timeout=600s ./pkg/frontend ./pkg/fulltext2
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -v -count=1 -timeout=600s -run '^TestIssue27294PreparedNumericOverloads$' ./pkg/tests/issues
```

| Package/regression | Result |
| --- | --- |
| `./pkg/frontend` | passed, 27.363s |
| `./pkg/fulltext2` | passed, 2.086s |
| `TestIssue27294PreparedNumericOverloads` | passed, 11.54s test / 13.668s package |

These frontend/fulltext2/issue checks were local diagnostics on a staged
candidate snapshot, not exact-PR-head validation. The clean exact-rebase
results below supersede that snapshot evidence. Neither section claims CI,
distributed BVT, or a passing merged changed-line coverage gate.

Exact clean-rebase validation on source/test snapshot
`ad88c4dcaf67ff148c5a274f52180cc4f4d7bbaf`, based on main
`5453d72b7372a9f77263d1dd4c982e616446d4cb` (tree
`9d637166adc3c93cb5aaaf696004b3f74eb9a6eb`), passed:

| Package/regression | Result |
| --- | --- |
| `./pkg/sql/plan` | passed, 10.256s |
| `./pkg/sql/plan/function` | passed, 16.194s |
| `./pkg/sql/compile` | passed, 6.509s |
| `./pkg/sql/colexec/aggexec` | passed, 3.119s |
| `./pkg/iscp` | passed, 1.580s |
| `./pkg/frontend` | passed, 26.995s |
| `./pkg/sql/parsers/dialect/mysql` | passed, 0.776s |
| `./pkg/fulltext2` | passed, 2.053s |
| `TestIssue27294PreparedNumericOverloads` | passed, 11.52s test / 13.592s package |

The exact commands were:

```text
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -p=1 -count=1 -timeout=600s ./pkg/sql/plan ./pkg/sql/plan/function ./pkg/sql/compile ./pkg/sql/colexec/aggexec ./pkg/iscp
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -p=1 -count=1 -timeout=600s ./pkg/frontend ./pkg/sql/parsers/dialect/mysql
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -p=1 -count=1 -timeout=600s ./pkg/fulltext2
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -v -count=1 -timeout=600s -run '^TestIssue27294PreparedNumericOverloads$' ./pkg/tests/issues
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -coverpkg=./pkg/sql/plan/... -coverprofile=/private/tmp/mo28523-exact-coverage.out -count=1 -timeout=180s ./pkg/sql/plan ./pkg/sql/plan/function
```

The plan package run includes the new role-discovery and matching-parameter
materialization counterexamples. All checks used the repository CGo wrapper
with Go 1.26.4 on darwin/arm64; `gofmt -d` and `git diff --check` passed.
Duplicate-rpath and duplicate-library linker warnings were non-fatal. These
are local UT results only; no distributed BVT, current CI pass, or PR-wide
merged changed-line coverage pass is claimed.

The local plan-subtree coverage profile reported `pkg/sql/plan` 51.8% and
`pkg/sql/plan/function` 29.0% of statements. The four prepared-role walkers
and `materializePreparedParam` were 100%; `rebindPreparedNumericExprWithRole`
was 74.5%. These package/profile figures are not the repository's merged
changed-line coverage gate.

Latest-base validation on main `24e66eba121c781c29998ff2611db11335e3028c`
(tree `b717729c7e1b0cc33d808f271f3211e8d6028f37`), on the clean rebased
candidate, passed:

| Package/regression | Result |
| --- | --- |
| `./pkg/cnservice` | passed, 10.369s |
| `./pkg/embed` | passed, 41.791s |
| `./pkg/frontend` | passed, 25.746s |
| `./pkg/sql/plan` | passed, 6.440s |
| `./pkg/sql/plan/function` | passed, 16.027s |
| `TestIssue27294PreparedNumericOverloads` | passed, 10.53s test / 12.625s package |

The latest-base tests were run with these commands:

```text
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -p=1 -count=1 -timeout=600s ./pkg/cnservice ./pkg/embed ./pkg/frontend
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -v -count=1 -timeout=600s -run '^TestIssue27294PreparedNumericOverloads$' ./pkg/tests/issues
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH ./.agents/skills/mo-dev/scripts/mo-cgo-test -p=1 -count=1 -timeout=600s ./pkg/sql/plan ./pkg/sql/plan/function
```

The main delta touches frontend and embedded-cluster lifecycle code but not
the PR's 24 paths. The updated frontend and issue regression were rerun on
this base; planner tests were also repeated after the rebase. These are local
CGo UT results, not current CI or distributed BVT results. The PR-wide merged
changed-line coverage gate remains unclaimed.

Historical tooling/BVT record on the earlier `4ff27bb9b35c43c1b0961bb9a01bf8fc0b6a2171`
baseline: local incremental `golangci-lint` did not pass. With the CGo include/link
environment configured, local golangci-lint v2.6.2 still rejected Go 1.27
export-data version 4 as newer than its maximum supported version 2. This is a
local linter/toolchain incompatibility, not a product-code pass. Local
distributed BVT was not run on that baseline. This linter/BVT record is
historical; current CI/BVT records are tracked in the PR/evidence ledger. The
historical results on base `66b167...` above remain historical only.

Known limitations: strict string precision behavior is intentionally not a
claim of full MySQL integer-prefix compatibility; upgrade/downgrade topology
and unrelated historical behavior such as FLOOR(NULL) are outside this scope.

## 7. Approval record

```text
Design path: docs/design/pr28523-string-math-coercion.md
Design revision: 7
Candidate source inputs: PR d27667a4936e813fb612de6cd245be03a2d6f101 (tree d02aa9a8b2c219b7767b9793c700287ecfc24c0b), rebased as 16 commits plus one test/documentation follow-up onto main 24e66eba121c781c29998ff2611db11335e3028c (tree b717729c7e1b0cc33d808f271f3211e8d6028f37); exact clean-rebase source/test validation is recorded above
Integration base: 24e66eba121c781c29998ff2611db11335e3028c (tree b717729c7e1b0cc33d808f271f3211e8d6028f37)
Scope/trigger: PR reviews 5199052257, 5214666396 and comment 5687377735; >500 production lines and planner/plan compatibility boundary
Reviewer identity and role: historical GPT-6 Astra review of d56711fa5b429e5e6e52f64f603d2e853478edca against base 4ff27bb9b35c43c1b0961bb9a01bf8fc0b6a2171; any exact-head review decision is tracked separately from maintainer design approval
Review timestamp: historical revision-7 review was recorded 2026-09-17; see the PR/evidence ledger for later exact-head reviews
Decision: DRAFT / AWAITING MAINTAINER APPROVAL
Validation evidence: exact clean-rebase CGo runs passed for plan, plan/function, compile, aggexec, iscp, frontend, MySQL parser, fulltext2, cnservice, embed, and TestIssue27294PreparedNumericOverloads; these are local results, not CI/BVT, the PR-wide merged changed-line coverage gate is not claimed, and maintainer design approval remains pending
Decisions proposed for maintainer acceptance: retain strict INT64 precision controls (no general integer-prefix widening); prefer correctness over function-wide zonemap pruning; retain the bounded scan and defer one-pass role collection pending scan-cost acceptance
Evidence links: [PR #28523](https://github.com/matrixorigin/matrixone/pull/28523); the PR/evidence ledger is the record for commit replay, review, CI, and BVT; historical CI run 34813866468 and the older local linter/BVT and d56711fa evidence are historical; current local CGo tests and formatting checks are recorded above
Implementation deviations requiring follow-up: MOD native arithmetic widening regression fixed in 8fc4d5250; strict INT64 precision acceptance, zonemap-pruning decision, and scan-cost acceptance remain pending
Approval link: pending maintainer review
```

The Astra review, implementation-agent self-review, and maintainer approval
must remain separate records. This document is not self-certified as approved.
