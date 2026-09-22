# PR #28523: String Math Numeric Coercion and Prepared-Parameter Roles

- Status: APPROVED / authorized maintainer sign-off recorded
- Design revision: 18
- Issue: [#28487](https://github.com/matrixorigin/matrixone/issues/28487)
- Implementation PR: [#28523](https://github.com/matrixorigin/matrixone/pull/28523)
- Revision-17 implementation/test source head: `ef9331788241c1b5e8d3707320dc6be79c23fc41`
  (tree `78653ab5d771f1bce48bdf9b248b3cc7af362f23`), with the local equivalent
  test commit `b0ee3f3a1726b4604940fc01ee7ea756afd2a984`. The versioned design
  document is published in a docs-only commit on top of that source head. The PR base is
  `d8ddce92b1c5c172111b50aefe6b6b200b2589cb` (tree
  `a270c3c5bd225762cb735afb9ab22d3fd9d78674`).
- Revision 17 is a publication/test-oracle update on top of revision 16: it
  records the current PR head and makes the legacy DATE comparison regression
  test explicitly opt into `MYSQL_NUMERIC_COMPATIBILITY` while preserving the
  strict default contract. No production conversion behavior is changed.
- Candidate integration base: `d97251938429be942fa0206b18df8cb5d630d356`
  (tree `96ae4531ac478532bc7d2629b30590913449032`), confirmed as current
  `main` and integrated by a clean rebase. The candidate's 31 commits replayed
  onto this base. Since the prior base `480b917`, main added the approved
  integer-parameter binding contract (`d9609d7`) and the fileservice fix
  (`d972519`); the former overlaps `pkg/sql/plan/utils.go` and
  `pkg/sql/plan/visit_plan_rule.go`, so the rebase explicitly preserved both
  the integer-source contract and this PR's prepared string-math handling.
  The exact post-rebase implementation candidate is
  `845a50360ac97e5f5b35dfd398eb7625f44b0f3d` (tree
  `79c397550626ccb4f2a5808b8fc5b1ce235b7821`), including the scoped
  compatibility-mode regression-fixture repair.
- Published PR source head before the local SCA repair:
  `7535f9f4023cd6c7d4e9485409477beaa4dbf61b` (tree
  `7b2b0c8236b1e5111b2e970137596a762439dcfd`).
- Prior published PR source head (before revision 12):
  `d27667a4936e813fb612de6cd245be03a2d6f101`
  (tree `d02aa9a8b2c219b7767b9793c700287ecfc24c0b`). The revision-8
  candidate, previously based on main `24e66eba121c781c29998ff2611db11335e3028c`
  (tree `b717729c7e1b0cc33d808f271f3211e8d6028f37`), was rebased onto
  `bc90a61230f02032b06351d0cfb317ae13a08258` (tree
  `4867dffe812b9c43bd4f1672c2d337da975d734d`) in revision 9. That
  historical rebase replayed 19 commits without conflicts. The earlier
  code/test snapshot is `e0bc0aabc16217dbb1bef9df655d83e04da59d26` (tree
  `8f419516495fb55333aa71c9e3b50f5b5e75342c`). The current post-rebase
  implementation/test snapshot is `e400dfef43c089a447af2aeeff646f882be509dd`
  (tree `5c368e3efd17b153080b00dd1293fc6041aed834`) was tested on base
  `89d28d5`. Revision 12's rebased implementation/test snapshot is
  `19d0047c71de2088f7383ed1dad6804bf5c01843` (tree
  `9229cf5dd59fb166183528f903b94751583b48a4`) on base `6690f1a`; all focused
  package, race, COM_STMT, and SQL-mode tests were rerun successfully there.
  Revision 12 records the latest-main integration evidence and proposed
  interpretation of Fengtt's compatibility note. Revision 13 records the
  earlier SCA repair and main rebase below. Revision 14 records the
  user-selected positive SQL-mode contract. Revision 15 records the exact
  current-main rebase and local validation; revision 16 records the scoped
  compatibility fixtures and exact-head build/BVT validation; revision 17
  records the current published head and the strict-default test-oracle repair.
  Implementation proceeds under the authorized maintainer design approval
  recorded in revision 18.
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
  (`fix(fulltext2): avoid boxing loaded UUID membership probes`),
  `24e66eba121c781c29998ff2611db11335e3028c`
  (`fix: release embedded cluster ownership after terminal cleanup`),
  `2243a0b636e18ebae82400246893e796dd4d6a6c` (`fix(window): spill internal sorting when sort_spill_mem is
  reached (#28877)`), and `bc90a61230f02032b06351d0cfb317ae13a08258`
  (`test: contain shared CN state failures and attribute race UT stalls
  (#29043)`), followed by the candidate's current integration base
  `89d28d5f3858f9d0164701c02e98b9eff2ce210e` (`Reduce idle startup
  allocations in embedded race UT (#29067)`).
- Historical code/test baseline: `d56711fa5b429e5e6e52f64f603d2e853478edca`
  on base `4ff27bb9b35c43c1b0961bb9a01bf8fc0b6a2171`; its 14-commit rebase,
  validation, and gofmt-only correction remain historical evidence below.
- Prior independent design review: GPT-6 Astra, medium reasoning, reviewed the
  historical `d56711fa5b429e5e6e52f64f603d2e853478edca` candidate against
  base `4ff27bb9b35c43c1b0961bb9a01bf8fc0b6a2171`; this is historical review
  evidence. Revision 18 records the authorized maintainer approval for the
  frozen design decisions.
- Review trigger: review `5199052257` identified a major-refactor/compatibility design gate; review `5214666396` and comment `5687377735` require incomplete numeric strings to be mode-gated

This document is the approved stable design revision for the implementation.
The authorized maintainer approval is recorded in the approval record at the
end of this document.

Revision 17 supersedes the revision-16 candidate mapping described in the
historical feedback records below. The user selected the explicit, opt-in
`MYSQL_NUMERIC_COMPATIBILITY` contract documented here and asked that
implementation proceed under the approved design. The revision-17 test-only update keeps
the compatibility behavior explicit in the regression fixture; it does not
weaken strict default conversion.

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

### 2.3 Numeric occurrence ownership boundaries

Numeric conversion is owned by a particular argument occurrence, not by the
parameter position or by any ancestor that happens to return a number. A
prepared parameter may use the permissive string-to-`DOUBLE` source only when
the complete expression path from a string-math value argument to that
occurrence proves the argument belongs to the same numeric result domain.
Numeric return type alone is not such proof: `LENGTH` returns a number but
consumes a string, and `CONCAT`/`REPLACE` preserve text semantics.

| Expression edge | Source discovery | Execute-time rebinding |
| --- | --- | --- |
| Known numeric function/value argument | Carry the nearest value/control role selected by the shared role classifier | Rebind only the occurrence whose argument contract proves ownership |
| Unknown or string-domain function argument | Reset inherited role, but continue looking for an independently nested string-math owner | Preserve the already-bound child subtree; `role=None` alone is not sufficient because a full numeric-looking text such as `"01"` can still be converted |
| Implicit prepared cast | Carry role through its non-explicit source envelope | Rebind through the provisional cast |
| SQL-authored explicit `CAST` | Stop inherited source discovery | Preserve the current bound CAST/source subtree |
| Scalar-subquery child | Carry a role to its single scalar result; function edges inside still enforce their own contracts | Preserve the subquery wrapper while refreshing only an independently owned child |
| List member | Reset inherited role; search each member for its own nested owner | Under inherited string-math Value/Control role, preserve the bound list; direct role-none integer/decimal fallback retains existing per-item rebinding; `ApplyExpr` has already visited members |
| Window value (`WindowFunc`) | Carry the scalar result role | Preserve the bound window wrapper while independently visited owners specialize |
| Window partition/order/frame | Reset inherited role because these are controls, not the scalar window result | Preserve the bound control subtree |

The numeric-position collectors intentionally remain conservative eligibility
supersets; occurrence rebinding is the authority that prevents a candidate
position from crossing a domain boundary. The shared semantic classifier is
used by source discovery and rebinding, while a no-inherited-role discovery
fast path establishes roles only at string-math functions and avoids
classifying every ordinary argument. Consequently `ABS(LENGTH(?))` must not
apply the outer ABS numeric-prefix conversion to the parameter, whereas
`LENGTH(ABS(?))` still discovers and specializes the inner ABS independently.
List and window control containers likewise must not inherit a scalar owner's
role, but nested owners inside them remain discoverable through `ApplyExpr`.

### 2.4 Compatibility mode, binary provenance, and warnings

Fengtt's review `5214666396` and follow-up `5687377735` state: “1.5tail convert
to number should fail. We are not as stupid as mysql folks. At minimum this
should be protected by mysql compatibility flag.” The user selected that
intent as an explicit opt-in SQL mode:

| Effective mode | `'1.5tail'` and other incomplete tokens |
| --- | --- |
| Default, empty, unset, or nil/old process snapshot | Strict: reject incomplete, non-numeric, empty, whitespace-only, and malformed-exponent tokens; emit no MySQL truncation warning. |
| `MYSQL_NUMERIC_COMPATIBILITY` | Preserve the existing MySQL prefix parser, zero result for wholly non-numeric text, empty string as zero without warning, and existing truncation warning count/code for discarded non-empty text. |
| `MATRIXONE_NATIVE` | Strict. It wins when both mode tokens are present. |

Add `MYSQL_NUMERIC_COMPATIBILITY` as a positive SQL mode accepted by the
frontend and SQL-mode membership helper, but do not add it to the existing
default mode. The protocol field is explicit and false by default; an absent
field from an older process payload fails closed as strict. Reorder the
`SQLCompatibilityMode` enum so its zero/uninitialized value is strict, and
require an explicit MySQL mode selection for prefix parsing. Keep the existing
parsers, warnings, binary-literal provenance, overflow handling, NULL/masked
row behavior, and precision argument typing unchanged.

The similarly named account/database `MYSQL_COMPATIBILITY_MODE` setting maps
to `version_compatibility`; it is not the numeric-conversion selector and must
not be reused for this contract. The design approval covers this compatibility
contract. NULL and masked rows do not create additional conversion warnings
in either mode.

Historical serialized CEIL/FLOOR VARCHAR overloads must derive this process
compatibility mode as well. They must not hardcode MySQL mode or emit
MySQL-only warnings in native mode. Every direct string executor and the
prepared/cast path is required to make the same mode decision at execution
time.

HEX/BIT source provenance remains observable: `X'31'` is interpreted by its
binary value (49), not as character text (`'1'`). Ordinary binary string types
and HEX/BIT literal provenance are distinct. Rebinding, deep-copy, and
parameter-restoration paths must preserve the marker that affects this result.

When a flow-control expression produces a heterogeneous string vector, its
row-level provenance sidecar is authoritative for the implicit string-to-
`DOUBLE` cast: a row selected from `X'31'` remains 49 while a row selected from
`'1'` remains 1. The cast consults `GetIsBinaryStringAt(row)` only when that
sidecar is active; the scalar `GetIsBin()` marker continues to identify direct
HEX/BIT literals, and ordinary static `BINARY`/`VARBINARY` values keep their
text-value conversion contract. `CASE`, `IF`, and `COALESCE` regressions cover
binary, text, and NULL rows together.

### 2.5 Overload and mixed-version safety

The implementation reuses existing numeric overload identities and existing
warning-aware/binary-aware casts instead of introducing new serialized string
overload IDs. `validFunctionOverloadID` validates both function and overload
indices, including negative and out-of-range values. This bounds check is a
panic-safety rule, not a claim that all old and new nodes have identical
semantics. Catalog format and function/overload identifiers are unchanged.
SessionInfo adds additive protobuf fields 18 and 19 to carry the explicit
numeric-compatibility mode and the sender contract marker across process
boundaries. A zero/absent contract marker identifies a legacy payload; it does
not infer a MySQL opt-in, and mode-sensitive remote expressions fail closed
until the v88 contract is known. New strict senders also fence pre-v88 workers
at placement and send time. Explicit MySQL compatibility remains the only
permissive opt-in.

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
such proof is part of this change. This correctness-over-pruning trade-off is
approved for this implementation.

Execute-time source discovery scans the plan for each parameter. With `P`
parameters and `N` expression nodes, the current traversal is `O(P*N)`; it no
longer performs a separate descendant-position scan at every function edge.
Source arrays are `O(P)`, while traversal state follows expression depth. The
implementation does not claim zero specialization cost.

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

Revision-9 post-rebase candidate source-discovery measurement (2026-09-17,
Darwin/arm64, Apple M1; the wrapper's default benchmark duration and CPU
count; three samples) used:

```text
.agents/skills/mo-dev/scripts/mo-cgo-test -run '^$' -bench '^BenchmarkPreparedStringMathRoleDiscovery$' -benchmem -count=3 ./pkg/sql/plan
```

The command exited 0 in 40.961s. The Go runtime reported that sonic/ast was
outside its supported Go version range and fell back to `encoding/json`.
All samples reported `0 B/op` and `0 allocs/op`; the raw `ns/op` samples were:

| Path/case | Sample 1 | Sample 2 | Sample 3 |
| --- | ---: | ---: | ---: |
| ABS value P1 | 2,702 | 2,685 | 2,688 |
| ROUND control P1 | 2,742 | 2,736 | 2,721 |
| ABS(ROUND precision) P1 | 2,780 | 2,777 | 2,778 |
| ROUND(ABS value) P1 | 2,773 | 2,777 | 2,786 |
| CONCAT no-match P1 | 2,708 | 2,684 | 2,707 |
| ABS does not own LENGTH argument P1 | 2,868 | 2,730 | 2,814 |
| nested ABS owner through LENGTH P1 | 2,710 | 2,718 | 2,720 |
| ROUND does not own CONCAT argument P1 | 2,793 | 2,793 | 2,808 |
| deep no-match P8/D8/N128 | 50,759 | 50,524 | 50,925 |
| mixed roles P5/N128 | 23,773 | 23,424 | 23,408 |

The no-match deep case is about 13.5% slower than the immediately preceding
per-edge descendant-scan diagnostic (44,734/44,629/45,062 ns/op); that earlier
command failed overall because three benchmark fixtures queried position 0
while their markers were at positions 13–15, although the no-match case itself
passed. Revision 8 corrected those benchmark positions and added the
no-inherited-role fast path. This post-rebase run is the valid
all-cases-passing candidate measurement; the no-match delta remains visible
for review rather than being presented as an improvement. No one-pass role
cache or cross-execution cache is introduced.

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
| Warnings and mode precedence | warning-session tests plus direct ABS/SIGN/CEIL/FLOOR/ROUND/TRUNCATE and historical CEIL/FLOOR overload-12 tests in strict-default, explicit MySQL-compatible, and both-flags/native-wins cases |
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
- Revision 18 approval supersedes this historical checkpoint.

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
ledger; revision 18 records the authorized maintainer approval.

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
approval is recorded in revision 18.

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
BVT or current CI run is claimed; the design approval is recorded in revision 18.

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

## Current owner-boundary implementation evidence (revision 9)

This evidence applies to the isolated post-rebase candidate code/test commit
`e0bc0aabc16217dbb1bef9df655d83e04da59d26` (tree
`8f419516495fb55333aa71c9e3b50f5b5e75342c`) on integration base
`bc90a61230f02032b06351d0cfb317ae13a08258` (main tree
`4867dffe812b9c43bd4f1672c2d337da975d734d`). This is local implementation
and validation evidence, not current PR CI; the design approval is recorded in
revision 18.

Before the production change, the focused planner tests failed with concrete
ownership violations:

```text
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 ./pkg/sql/plan -run '^(TestPreparedStringMathRoleDiscoveryAcrossExpressionContainers|TestPreparedNumericRebindingStopsAtNonNumericFunctionArguments)$'
```

`outer-math-does-not-own-through-length` reported position 13 wanted false,
got true; `outer-math-does-not-own-through-concat` reported position 15 wanted
false, got true. The rebinding counterexample also showed both a permissive
value role and `role=None` rewriting the already-bound CONCAT marker instead
of preserving its text wrapper.

The public COM_STMT fixture also failed before the fix:

```text
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 ./pkg/tests/issues -run '^TestIssue27294PreparedNumericOverloads$'
```

The prepared query `ABS(LENGTH(?))` with `"abc"` returned 1 instead of 3;
`ABS(REPLACE('11','1',?))` with `"01"` returned 11 instead of 101; and
`ABS(CAST(CONCAT('1', ?) AS CHAR))` returned 1011 instead of 101. The ordinary
literal SQL oracle `select abs(cast(concat('1', '01') as char))` returned 101.
`ROUND(CONCAT('1', ?), ?)` with `"01", 0` returned the expected 101 before
and after the change; it is retained as a passing control, not claimed as a
reproduced defect.

The fix applies the same argument-ownership classifier to source discovery
and execute-time rebinding. Unproven function edges reset discovery role and
preserve the already-bound subtree at rebinding; list members and window
controls do not inherit scalar result roles, while `ApplyExpr` independently
visits nested owners. Planner-bound implicit casts, scalar-subquery result
edges, explicit CAST boundaries, list-role isolation, and preservation of
window partition/order/frame markers under an inherited value role are covered
by the plan tests. The bounded verification commands passed:

```text
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 ./pkg/sql/plan -run '^(TestPreparedStringMathRoleDiscoveryAcrossExpressionContainers|TestPreparedNumericRebindingStopsAtNonNumericFunctionArguments)$'
.agents/skills/mo-dev/scripts/mo-cgo-test -race -count=1 -run '^(TestPreparedStringMathRoleDiscoveryAcrossExpressionContainers|TestPreparedNumericRebindingStopsAtNonNumericFunctionArguments)$' ./pkg/sql/plan
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=600s ./pkg/sql/plan
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=600s ./pkg/sql/plan/function
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 ./pkg/tests/issues -run '^TestIssue27294PreparedNumericOverloads$'
go vet -mod=readonly ./pkg/sql/plan ./pkg/sql/plan/function ./pkg/tests/issues
gofmt -d pkg/sql/plan/prepared_string_math_measure_test.go pkg/sql/plan/utils.go pkg/sql/plan/visit_plan_rule.go pkg/tests/issues/issue_27294_test.go
git diff --check origin/main...HEAD
```

After rebase, the focused owner-boundary race command exited 0 (`pkg/sql/plan`,
3.073s), full `pkg/sql/plan` exited 0 (6.039s), full
`pkg/sql/plan/function` exited 0 (16.849s), and the public COM_STMT regression
exited 0 (`pkg/tests/issues`, 12.790s). CGo-aware `go vet` for those three
packages exited 0 with `GOWORK=off` and the candidate `cgo/` plus
`thirdparties/install/{include,lib}` flags. Go 1.26.4 `gofmt -d` produced no
output and `git diff --check origin/main...HEAD` passed. Public assertions include the
three semantic regressions and literal oracle above, MySQL/native `"1.5tail"`
behavior on the same prepared template, nested `LENGTH(ABS(?))`, precision
control ownership, explicit CAST preservation, and the passing ROUND/CONCAT
control.

The corrected post-rebase role-discovery benchmark exited 0 in 40.961s and all
ten cases passed; the deep no-match P8/D8/N128 case measured
50,759/50,524/50,925 ns/op with zero allocations. The previous per-edge
descendant-scan diagnostic measured 44,734/44,629/45,062 ns/op but failed
overall because three benchmark fixtures queried position 0 while their
markers were at 13–15. The current run is valid and its modest no-match timing
delta is reported above. This is a narrow source-discovery diagnostic; it
does not establish the PR-wide merged changed-line coverage gate.

At the time of the revision-9 record, the published PR head was
`d27667a4936e813fb612de6cd245be03a2d6f101`, so no GitHub CI had run on that
local candidate. On that old head, CI run
`35197284882` passed SCA, shared build, UT coverage producer, and selected
Compose BVT jobs, but Ubuntu UT failed after 2h03m47s with failures/timeouts in
`pkg/tests/issues`, `pkg/tests/dml`, and `pkg/tests/sqlintegration`; the
Coverage merge job failed because a required producer artifact was missing,
so no coverage threshold verdict was produced. These failures have not been
reproduced on a clean base and are not attributed to this local delta.
Distributed BVT and the PR-wide changed-line coverage gate are not claimed.
Revision 18 approval supersedes this historical checkpoint.

## Current post-rebase feedback-specific verification (revision 12)

The fresh-session COM_STMT counterexample now reads the actual server-default
`@@sql_mode` on a new connection. It was
`ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ENABLE_BOOL_SUMAVG`
and did not contain `MATRIXONE_NATIVE`. In that session, prepared
`ABS(?)` with the string `1.5tail` returned `1.5`; `SHOW WARNINGS` returned
warning 1292 (`Truncated incorrect DOUBLE value`). This confirms the current
candidate's approved revision-18 compatibility mapping. The same public
COM_STMT test switches the session to
`STRICT_TRANS_TABLES,MATRIXONE_NATIVE` and asserts the value conversion fails.

To close the previously untested argument-position case, native-mode
`MOD(2, '1.5tail')` now has both a BVT oracle and a local SQL execution
assertion expecting `invalid input: "1.5tail" is invalid numeric string`.
The BVT fixture change was not run against a local distributed service; no
shared mutable service was used. No local distributed-BVT result is claimed;
use CI on the published head as the authoritative validation for that fixture.

These commands were rerun after the clean rebase onto
`6690f1af97977ad5671235e9204296cd2d504ad5` with Go 1.26.4 and the repository
CGo wrapper:

```text
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH GOWORK=off ./.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=600s ./pkg/sql/plan
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH GOWORK=off ./.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=600s ./pkg/sql/plan/function
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH GOWORK=off ./.agents/skills/mo-dev/scripts/mo-cgo-test -race -count=1 -timeout=600s ./pkg/sql/plan -run '^(TestPreparedStringMathRoleDiscoveryAcrossExpressionContainers|TestPreparedNumericRebindingStopsAtNonNumericFunctionArguments)$'
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH GOWORK=off ./.agents/skills/mo-dev/scripts/mo-cgo-test -v -count=1 -timeout=600s ./pkg/tests/issues -run '^TestIssue27294PreparedNumericOverloads$'
PATH=/Users/ljy/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.darwin-arm64/bin:$PATH GOWORK=off ./.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=600s ./pkg/frontend -run '^TestSetSessionSQLModeMatrixOneNativeLiteralAndModeFlip$'
git diff --check origin/main...HEAD
```

The latest-base results were: `pkg/sql/plan` passed in 6.489s,
`pkg/sql/plan/function` in 17.060s, the focused race tests in 3.099s,
`TestIssue27294PreparedNumericOverloads` in 13.817s, and the frontend mode
test in 2.013s. `git diff --check origin/main...HEAD` also passed before this
documentation-only revision update and is rerun for the final head below.
Local distributed BVT, current GitHub CI, and the PR-wide
changed-line coverage gate are not claimed. The previous CI run
`35197284882` is historical evidence on the old published head, not this
post-rebase candidate.

## Historical SCA repair and latest-main verification (revision 13)

CI run `35242989945` on published head `7535f9f4023cd6c7d4e9485409477beaa4dbf61b`
reported two PR-caused SCA findings: Go 1.26.4 gofmt rejected indentation in
`pkg/sql/plan/visit_plan_rule_test.go`, while `sqlclosecheck` required the
`nestedLength` prepared statement in `pkg/tests/issues/issue_27294_test.go` to
use deferred cleanup. Go 1.26.4 reproduced the formatter-only hunk; formatting
with that toolchain now reports no diff. The prepared statement's unchanged
Prepare/query/assert sequence is scoped in an immediately invoked closure with
deferred Close, so assertion failure also closes it before the subsequent
`MATRIXONE_NATIVE` mode switch. No SQL, result, or compatibility expectation
changed.

The candidate was rebased onto current main
`780ef933f2479aa1679faf13d08850ce6c8f7c2f` (tree
`2a9c8a48c882411b3b1b44f29f5137378bd88a7c`): 26 commits replayed cleanly.
The only upstream change since base `6690f1af` is the unrelated test-only
`pkg/vm/engine/test/change_handle_test.go` edit; it does not overlap the PR's
24 changed paths. On the exact rebased implementation/test snapshot
`7c88f8473f6ff723ef1da2d768932c975cc1b991` (tree
`6bcec702c1dcc627395f76f6bc46b5882bdd01e4`), Go 1.26.4, the repository CGo
wrapper, incremental configured golangci-lint for the four changed packages
(`0 issues`), `go vet`, targeted `sqlclosecheck`, gofmt, and diff checks passed. The
COM_STMT regression passed (test 9.65s; package 11.818s); full
`pkg/sql/plan`, `pkg/sql/plan/function`, and `pkg/frontend` passed in 6.433s,
17.040s, and 27.195s respectively.

The same CI run's successful Compose(PROXY) BVT job `105278780404` selected
`test/distributed/cases/function/func_math_string_numeric.test`. The successful
Standalone multi-CN job `105278780515` did not show this case in its selection
log. This is reusable evidence for the unchanged SQL/BVT behavior, not a CI run
on the new local head; no local BVT or post-fix remote SCA/CI run is claimed.
Revision 18 approval applies to this historical validation record.

## Current strict-default implementation and latest-main verification (revision 16)

The strict-by-default contract selected by the user is implemented: default,
empty, unset, and legacy/nil process state are strict; only the explicit
`MYSQL_NUMERIC_COMPATIBILITY` SQL mode enables MySQL numeric-prefix parsing;
`MATRIXONE_NATIVE` remains strict and takes precedence if both are set. The
separate account/database `MYSQL_COMPATIBILITY_MODE` setting is not reused.
Session/process propagation, protobuf transport, prepared-plan/cache mode
transitions, direct and prepared conversion paths, warnings, and the
function-role boundaries are covered by the implementation and tests above.

The pre-rebase implementation/test snapshot `ee613d767e4daa269ad0f3cf7c3599118de5b076`
(tree `8cc85f221c73b86f46cdc32e4627755065ae3058`) was rebased onto current
main `d97251938429be942fa0206b18df8cb5d630d356` (tree
`96ae4531ac478532bc7d2629b30590913449032`). The two overlapping planner files
were merged so main's integer-parameter contract and this PR's string-math
role handling both remain present. The implementation candidate before this
documentation-only revision is
`845a50360ac97e5f5b35dfd398eb7625f44b0f3d` (tree
`79c397550626ccb4f2a5808b8fc5b1ce235b7821`), 32 commits ahead of this base.

On the exact rebased source, Go 1.26.4 and the repository CGo test wrapper
passed the parser, process, math-function, frontend, and planner packages
with `-vet=off`; the same CGo-aware `go vet` command passed separately for all
six affected packages.
The two real embedded-cluster prepared-statement regressions
`TestIssue27294PreparedNumericOverloads` and
`TestIssue28523NumericCompatibilityOverBinaryPreparedStatement` passed
together. The focused prepared-owner planner race tests passed. CGo-aware
`go vet` passed for the parser, process, math-function, planner, frontend, and
issue-test packages. The full `pkg/tests/issues` package also passed after the
fixture repair; the two PR-specific prepared-statement regressions were rerun
in isolation. Exact durations and commands are retained in the local
issue-to-PR evidence ledger.

The latest local distributed BVT comparisons passed:
`test/distributed/cases/function/func_string_format.test` at 276/276 and
`test/distributed/cases/function/func_math_string_numeric.test` at 118/118.
Both used `mo-tester -m run -n -g`; `-n` ignores result-set metadata. The
FORMAT and issue fixtures enable `MYSQL_NUMERIC_COMPATIBILITY` only around
legacy numeric-prefix assertions, save/restore the prior SQL mode, and leave
strict-default error cases intact. The result values were not changed.
Therefore these passes establish SQL/result behavior, not result metadata.
The exact candidate was rebuilt with Go 1.26.4, CGo native libraries, and
`make build-with-prebuilt-native`; each BVT was run twice against that binary:
`func_string_format.test` 276/276 and `func_math_string_numeric.test` 118/118
on both runs. An additional normal comparison without `-n` also passed for
both cases at 276/276 and 118/118, so the checked-in result metadata was
validated rather than only ignored. The local service was stopped cleanly
afterward. No remote CI, PR-wide coverage result, or multi-CN runtime claim is
made. The BVT evidence is supplemental to the exact-source Go and COM_STMT
validation, not a substitute for it.

The implementation is complete under the approved design. The authorized
maintainer approval is recorded in the approval record below.

## Current published review artifact (revision 18)

The implementation/test source head for this review artifact is
`ef9331788241c1b5e8d3707320dc6be79c23fc41` (tree
`78653ab5d771f1bce48bdf9b248b3cc7af362f23`), based on
`d8ddce92b1c5c172111b50aefe6b6b200b2589cb` (tree
`a270c3c5bd225762cb735afb9ab22d3fd9d78674`). The revision-17 design document
is added in a docs-only commit on top of this source head. The only source/test change after the
revision-16 implementation snapshot is the frontend regression-test oracle:
`TestCOMStmtInetNtoaDomainHintDoesNotLeakIntoComparison` now explicitly sets
`MYSQL_NUMERIC_COMPATIBILITY` and refreshes statement-scoped process state
before asserting the legacy text-prefix comparison. The strict default path is
still covered by the existing conversion tests and remains unchanged.

Validation on the revision-18 local equivalent passed the focused test, all
`TestCOMStmtInetNtoa*` tests, and the full `pkg/frontend` package through the
repository CGo wrapper; `gofmt` and `git diff --check` also passed. The
corresponding ALL CI run is pending. Revision 18 freezes the design decisions
below, and the authorized-maintainer sign-off is recorded in the approval
record.

## 7. Approval record

```text
Design path: docs/design/pr28523-string-math-coercion.md
Design revision: 18
Candidate source inputs: published revision-17 head ef9331788241c1b5e8d3707320dc6be79c23fc41 (tree 78653ab5d771f1bce48bdf9b248b3cc7af362f23); local equivalent b0ee3f3a1726b4604940fc01ee7ea756afd2a984; revision-16 implementation/test candidate 845a50360ac97e5f5b35dfd398eb7625f44b0f3d (tree 79c397550626ccb4f2a5808b8fc5b1ce235b7821).
Integration base: d8ddce92b1c5c172111b50aefe6b6b200b2589cb (tree a270c3c5bd225762cb735afb9ab22d3fd9d78674)
Scope/trigger: PR reviews 5199052257, 5214666396 and comment 5687377735; >500 production lines and planner/plan compatibility boundary
Reviewer identity and role: historical GPT-6 Astra review of d56711fa5b429e5e6e52f64f603d2e853478edca against base 4ff27bb9b35c43c1b0961bb9a01bf8fc0b6a2171; any exact-head review decision is tracked separately from maintainer design approval
Review timestamp: exact final-candidate Astra Medium review is tracked separately; authorized maintainer design approval is recorded by the linked exact-head review below
Decision state: DESIGN DECISIONS APPROVED / AUTHORIZED MAINTAINER SIGN-OFF RECORDED
Validation evidence: historical revisions 9-16 remain recorded above. Revision 18 records the published head, strict-default frontend test-oracle repair, focused/all `TestCOMStmtInetNtoa*` tests, full `pkg/frontend`, gofmt, and diff-check. No remote CI or PR-wide coverage pass is claimed until the current run completes.
Frozen design decisions for this implementation snapshot:
1. Strict INT64 precision: retain strict integer-domain controls and do not widen arbitrary integer inputs through general prefix parsing. Exact integer, unsigned, BOOL, DECIMAL, and FLOAT sources retain their existing numeric domains; only eligible string-math value roles use the compatibility source.
2. Zonemap correctness: do not advertise function-wide CEIL/FLOOR/ROUND zonemap pruning for string-derived numeric expressions. Correctness wins over that optimization; a later change may add overload-specific pruning only with an equivalence proof and endpoint-trap tests.
3. Bounded scan cost: retain the bounded per-parameter source/owner scan. The recorded deep no-match and mixed-role measurements are the selected evidence for this scope; a one-pass occurrence-role table or cross-execution cache is deferred until cache invalidation, ownership equivalence, and new measurements are specified separately.
4. Argument ownership: retain the revision-9 ownership boundaries and no-inherited-role fast path. Only eligible string-math value occurrences are rebound; control/precision arguments stay in their declared domains; nested/non-owning functions do not inherit a string role.
5. Compatibility contract: default, empty, unset, and legacy/nil process state are strict; only explicit MYSQL_NUMERIC_COMPATIBILITY enables MySQL numeric-prefix conversion; MATRIXONE_NATIVE is always strict and wins if both flags are present; the token is appended without shifting existing SQL-mode positions and is transported through protobuf fields 18/19 with legacy/missing payloads failing closed for mode-sensitive remote expressions.
These decisions are frozen for the implementation snapshot and are distinct from the GitHub approval action. They are not changed by the two non-blocking implementation observations in the latest automated review.
Evidence links: [PR #28523](https://github.com/matrixorigin/matrixone/pull/28523); [historical-head CI run 35197284882](https://github.com/matrixorigin/matrixone/actions/runs/35197284882); the PR/evidence ledger is the record for commit replay, review, CI, and BVT; current local post-rebase evidence is recorded above
Implementation deviations requiring follow-up: MOD native arithmetic widening regression fixed in 8fc4d5250. The remaining follow-up is optimization-only: safe overload-specific zonemap pruning and a one-pass role table may be evaluated in a separate change; they are not required by this frozen contract.
Approval link: [fengttt APPROVED review for exact implementation head `a1b63763eccdc864d494728827190de73c293dca`](https://github.com/matrixorigin/matrixone/pull/28523#pullrequestreview-5261973268) (submitted 2026-09-20T22:40:07Z)

Approval scope note: this link records the authorized maintainer's GitHub approval for the exact implementation head and is the recorded sign-off for revision 18 and its frozen decisions.
```

The Astra review, implementation-agent self-review, and maintainer approval
remain separate records. The maintainer approval is recorded by the linked
GitHub review above.
