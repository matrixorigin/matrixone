# Decimal division: exception and regression cases

Scope: issue #28594 / PR #29241, reviewed production head
`3903e9cbf5711315b58818d389af86d72ac6430d`, base
`71c415705c909e524abd21dd0cf3e31b1c18e941` (2026-09-25).
This is a contract inventory for the changed paths, not a claim that every
possible input or failure has been enumerated. Update the evidence revision
when a relevant implementation, fixture, toolchain or execution mode changes.

## Contract and evidence

Newly authored exact division uses the current `div_precision_increment`.
Execution honors the bound result type and scale. Copying an unchanged stored
expression preserves its bound semantics; changing an operand type or explicitly
authoring a replacement expression requires a new binding. Errors must not
publish a partial table, row or prepared-plan generation.

Statuses below:

- **PASS**: observed success in the stated UT/BVT or live probe. The scope of
  each oracle matters: a mock protocol test is not a rolling-upgrade test.
- **FAIL**: observed violation of the stated expected behavior. Multiple cases
  can describe one defect.
- **EXISTS**: a relevant existing regression was located, but a dedicated result
  for the stated combination has not been established in this review.
- **GAP**: a concrete case to execute/add; no product defect is asserted.

Evidence reused from the preceding review: full plan and compile packages on
the reviewed production source passed (19.400s / 10.246s); the strict
`issue_28594_div_precision.test` BVT passed 117/117 statements twice. Function
and protobuf package evidence at `9d0519ba54d7682e62feca7824d87b2477200efd`
remains applicable because those production/test closures are unchanged at the
reviewed head. Focused frontend setting/cache tests passed. Broad generated
column, CHECK and LIKE BVT runs ignored metadata; they do not establish strict
metadata coverage. This inventory does not assert that all CI jobs are green.

There are 74 numbered entries: 45 existing coverage/evidence entries, 19 new
executable CTAS cases, and 10 explicit remaining gaps. These are case counts,
not defect counts. The first 17 probes ran twice with identical results; the
expanded 19-case probe ran on the same reviewed production binary and left no
probe databases behind. It is manual and adds no automatic CI runtime.

Locators below are relative to the repository root. A named Go test identifies
the semantic oracle; reuse it rather than duplicating the same cases in a new
fixture. `BVT` means
[`issue_28594_div_precision.test`](../../distributed/cases/dtype/issue_28594_div_precision.test).

## Numeric and representation boundaries

Most UT locators in this section are in
[`pkg/sql/plan/function`](../../../pkg/sql/plan/function).

| ID | Case / independent expected behavior | Test locator | Status |
| --- | --- | --- | --- |
| N01 | DECIMAL(10,2) 1/3 at increments 0,4,10,30: scales 2,6,12,30; values and result metadata agree | `decimal_div_precision_test.go`: `TestDecimalDivisionTypeUsesPrecisionIncrement`, `TestDecimalDivisionExecutionUsesBoundResultScale`; BVT | PASS |
| N02 | Required precision crosses 38: widen physical representation to Decimal256; ordinary smaller domains remain Decimal128 | Same type test; `TestDecimal256DivisionExecutionUsesBoundResultScale` | PASS |
| N03 | Precision/scale cap at 65/30; input scale above 30 still computes a correctly rounded result | `TestDecimalDivisionScaleCapBelowInputScale`; plan `TestDivisionSQLBoundaries` | PASS |
| N04 | A wide numerator overflows an intermediate scaling operation although the final quotient fits: return the quotient | `TestDecimal256DivisionAvoidsScaledNumeratorOverflow`, `TestD256DivViaD128AvoidsIntermediateScaleOverflow` | PASS |
| N05 | Decimal256 shift boundary and internal coefficient headroom: no false overflow or wraparound | `TestDecimal256DivisionShiftBoundaries`, `TestDecimal256DivisionInternalHeadroom` | PASS |
| N06 | Scale reduction rounds once, including negative values and wide fallback | `TestDecimalDivisionNegativeScaleRoundsOnce`, `TestDecimalDivisionNegativeScaleWideFallback` | PASS |
| N07 | Negative power-of-two divisor at the 128-bit fast-path boundary retains its sign | `arith_decimal_fast_test.go`: `TestD256DivViaD128NegativePowerOfTwoDivisor` | PASS |
| N08 | Rounding at MaxUint64 does not wrap to zero | `arith_decimal_fast_test.go`: `TestDecimalDivisionMaxUint64Rounding` | PASS |
| N09 | A representable coefficient exceeding the declared precision is rejected; CTAS leaves no target table | `TestDecimal256DivisionDeclaredPrecision`; BVT `rejected_result` | PASS |
| N10 | Zero divisor, NULL dividend and masked rows obey SQL mode and do not evaluate a masked error | `arithmetic_div_zero_test.go`; `arithmetic_selectlist_reuse_test.go`; function package UT | PASS |
| N11 | Constant/vector, vector/constant and vector/vector kernels produce the same arithmetic contract | `arith_decimal_fast_test.go`: `TestD64Div`, `TestD128Div`, `TestD256Div`; function package UT | PASS |
| N12 | Direct execution result has canonical OID/Size and survives vector serialization | `decimal_division_layout_test.go`: `TestDirectDecimalDivisionVectorRoundTrip` | PASS |
| N13 | FLOAT division remains FLOAT when the exact-division setting changes | `TestDecimalDivisionTypeUsesPrecisionIncrement` FLOAT control | PASS |
| N14 | Large signed/unsigned and inferred decimal parameters retain their integer capacity | `numeric_resolver_test.go`: `TestInferNumericParameterTypeDecimalIncludesIntegerCapacity`; plan `TestPreparedDivisionSpecializationUsesPrecisionIncrementContext` | PASS |
| N15 | Nested prepared division matches direct values/errors for decimal(38,*) and decimal(50,*) inputs at increments 4/30 | Live direct/prepared review probes; result-type equivalence still tracked as G02 | PASS (values/errors) |
| T01 | DATE/YEAR packed numeric widths remain 8/4 digits before precision increment | `TestTemporalDivisionUsesPackedPrecision`; plan `TestDivisionSQLBoundaries`; BVT | PASS |
| T02 | DATETIME/TIMESTAMP(6) preserve six fractional digits and the full packed integer part | Same tests; `func_cast_temporal_numeric_test.go` | PASS |
| T03 | TIME(3/6), including negative maximum supported TIME, does not truncate into Decimal64 | Same tests; plan `TestDivisionSQLBoundaries` positive/negative maximum | PASS |
| T04 | Temporal operand on the right uses the left operand's scale rule and a lossless temporal cast | `TestTemporalDivisionUsesPackedPrecision` reversed control; BVT | PASS |
| T05 | Timestamp numeric conversion respects the session timezone | `func_cast_temporal_numeric_test.go`: `TestTimestampNumericCastUsesSessionTimeZone` | PASS |

## State, persistence and failure boundaries

| ID | Case / expected behavior | Test locator | Status |
| --- | --- | --- | --- |
| S01 | Changing the setting invalidates cached plans; assigning the same value preserves them | frontend `plan_cache_test.go`: `TestSessionDivPrecisionIncrementChangeClearsPlanCache` | PASS |
| S02 | EXECUTE after a setting change rebuilds the prepared generation and publishes matching metadata | frontend `computation_wrapper_test.go`: `TestInitExecuteStmtParamRebuildsPreparedPlanWhenDivPrecisionIncrementChanges`; BVT | PASS |
| S03 | Prepared parameter specialization receives the same setting as ordinary binding | plan `TestPreparedDivisionSpecializationUsesPrecisionIncrementContext` | PASS |
| S04 | Failed execution/rebuild invalidates a stale cache generation without prematurely freeing its borrowed AST | frontend `TestTxnComputationWrapperRunLazilyInvalidatesFailedRebuild` | EXISTS; precision-change combination G03 |
| S05 | Column-metadata refresh failure preserves the old prepared plan and metadata together | frontend `TestInitExecuteStmtParamKeepsOldStateWhenColumnMetadataRefreshFails` | EXISTS; precision-change combination G03 |
| D01 | CREATE DEFAULT, generated expression and CHECK use increments 0,4,10,30 consistently | plan `TestDDLDivisionBindersUseSessionPrecision`; BVT nondefault CREATE/INSERT/UPDATE | PASS |
| D02 | CHECK admits 1/3 above 0.3333333 under increment 10; rejects 0/3 and leaves accepted-row count unchanged | BVT `ddl_precision`, `ddl_reorder` | PASS |
| D03 | COPY ALTER adding a FIRST column preserves existing generated/default/CHECK semantics at increment 4 | plan `TestPersistedDDLReplayRemapsExistingExpressions`; BVT `ddl_replay` | PASS |
| D04 | Adding a new generated expression uses the current setting while existing expressions retain theirs | BVT `ddl_replay.q` versus newly added `r` | PASS |
| D05 | LIKE preserves inherited bound defaults/generated expressions despite a changed session setting | BVT `ddl_replay_like` | PASS |
| D06 | Changing a referenced decimal width/scale requires rebinding under the current setting | replay UT type mismatch; BVT `ddl_rebind`; C09 | PASS |
| D07 | Hidden columns plus user column reorder use visible reference positions, not raw catalog positions | replay UT; BVT `ddl_reorder` | PASS |
| D08 | Replay does not mutate its source or target catalog expressions; wrong table identity or changed expression is not reused | `ddl_expression_replay_test.go`: `TestPersistedDDLReplayRemapsExistingExpressions` | PASS |
| D09 | Enum-value order or AutoIncr change invalidates replay compatibility | Same replay UT (internal compatibility contract) | PASS |
| D10 | Making an operand nullable preserves numeric semantics and evaluates COALESCE / IS NULL correctly | Live review probe: NULL/3 -> 0.333333333333 and `is null` -> true | PASS (live); permanent regression G04 |
| D11 | Changing generated target DECIMAL(30,12) to DECIMAL(30,6) produces a correctly rebound six-place result | Live review probe | PASS (live); permanent regression G04 |
| D12 | Missing CTAS dependency, invalid dependency graph or incompatible override is rejected during binding | plan `expression_default_test.go`: `TestCTASDefaultRebindAndReplay`, `TestExpressionDefaultRejectsInvalidDependencyGraph`, `TestRemapCTASSourceDefaultsUsesSourceAndOutputCoordinates` | PASS (plan package); catalog rollback G05 |
| P01 | New division cannot be placed/sent to protocol 96; protocol 97 accepts and carries the bound scale | compile `TestDecimalDivisionProtocolPlacementSendAndReceive` | PASS (mock RPC) |
| P02 | Destination changes to an old worker after placement: send-time validation still rejects | Same test | PASS (mock RPC) |
| P03 | Unknown worker capability falls back to a compatible execution path | compile `TestDecimalDivisionProtocolUnknownWorkerFallsBack` | PASS (mock RPC) |
| P04 | Old-to-new plans retain the legacy tagged scale and are accepted | function `TestLegacyDecimalDivisionPlanUsesTaggedScale`; compile placement/send/receive test | PASS (synthetic legacy plan) |
| P05 | Persisted division requires protocol 97, including expressions folded away during view binding | plan `TestPersistedDecimalDivisionRequiresV97`, `TestPersistedDecimalDivisionViewAdmissionBeforeFold`; DDL binder test | PASS |
| R01 | Scale-cap kernels avoid per-row heap allocation | `BenchmarkDecimalDivisionScaleCap`, `BenchmarkDecimal256DivisionScaleCap`: measured 0 allocs; D128 ~10.7–10.8us, D256 ~20–21us for benchmark batch | PASS (measured workload only) |
| R02 | Temporal execution keeps bounded result storage and avoids unnecessary widening | Prior 1,024-row probe: retained 32,784 bytes versus 81,952 before correction; 373–393us versus 394–402us | PASS (local comparison); reusable benchmark G08 |
| R03 | Capability responses have one release owner and every probe wait has cancellation/deadline termination | Release count asserted by protocol UT; code trace confirms shared 5s deadline per probe | PASS (release assertion); cancellation/fanout G07/G09 |

## CTAS cases added by this inventory

Run [`ctas_precision.sql`](ctas_precision.sql) through
[`run_ctas_precision.py`](run_ctas_precision.py) against an already running
isolated MatrixOne service. It uses the standard Python library and the mysql
CLI; no database driver, service startup, sleep or CI fixture is added.

```sh
# Configure credentials with a mysql option file or MYSQL_PWD.
python3 test/manual/issue28594/run_ctas_precision.py \
  --defaults-extra-file=/path/to/test-client.cnf -h127.0.0.1 -P6001
```

The runner creates a unique database, drops it in `finally`, checks that all
19 result IDs appear exactly once, compares both the SQL predicate and the
formatted value, and exits nonzero on failure. It does not convert a known
failure into a pass. Add client-specific options such as `--skip-ssl` when
required by the installed client. An SQL error aborts the probe and reports an
execution error rather than claiming partial success.

Oracles are literal decimal values obtained by rounding 1/3, 2/3 and 4/3 once
to the source or intentionally rebound scale. Each SQL comment identifies the
independent semantic dimension. Source precision is 10 and target precision 4
unless specified. The pre-fix `3903e9cbf5` production binary yields
**9 PASS / 10 FAIL**:

| ID | Dimension | Expected behavior | Observed status |
| --- | --- | --- | --- |
| C01 | Empty source | Future insert retains 12-place source division | FAIL |
| C02 | Populated source, copied row | Existing materialized value remains unchanged | PASS |
| C03 | Populated source, future insert | New row agrees with inserting into the source | FAIL |
| C04 | Constant default control | Inherited constant retains source precision | PASS |
| C05 | Aliased operands | Reference names change; arithmetic does not | FAIL |
| C06 | Swapped aliases | Both names are remapped simultaneously without reversing the quotient | FAIL |
| C07 | Reordered SELECT operands | Source positions map to output positions without rebinding arithmetic | FAIL |
| C08 | Prepended explicit target column | Output positions map to final target positions without rebinding arithmetic | FAIL |
| C09 | Operand scale changed from 2 to 3 | Rebind at current increment 4; seven fractional places | PASS |
| C10 | Explicit target DEFAULT | Newly authored expression binds at current increment 4 | PASS |
| C11 | Only operand nullability changed | Numeric domain unchanged; inherited precision retained | FAIL |
| C12 | CTAS of CTAS | Repetition does not change inherited precision | FAIL |
| C13 | Same source/target setting | Control: inherited and rebound values coincide | PASS |
| C14 | Reverse setting change, 4 -> 10 | Do not add precision to a stored six-place source division | FAIL |
| C15 | NULL operand | Result stays NULL | PASS |
| C16 | COPY ALTER control | Existing row-reference default retains source precision | PASS |
| C17 | Source reused after all copies | CTAS never mutates the source's bound default | PASS |
| C18 | Non-null operand becomes nullable | New non-NULL row retains the inherited 12-place precision | FAIL |
| C19 | Newly nullable operand is NULL | Inherited default evaluates to NULL | PASS |

The ten failures are one confirmed defect: `finalizeCTASDefaults` reparses
every inherited default containing a local column reference under the current
session setting. A typical expected value `0.666666666667` becomes
`0.666667000000`. Constant defaults and existing copied values bypass this
rebind, which is why checking only copied rows misses the defect.
See [the submitted review](https://github.com/matrixorigin/matrixone/pull/29241#pullrequestreview-5320419213).
[`observed-3903e9.tsv`](observed-3903e9.tsv) records the actual results; it is
evidence, not a golden consumed by the runner.

The repair retains the copied binding when the final operand and assignment
types have the same value semantics. It rebinds actual type overrides and
keeps explicitly authored target defaults. Type `Table` is catalog lineage,
so it does not trigger rebinding; this difference between mock types and real
catalog types caused an initial false green in the planner UT. The test now
injects the lineage marker and proved a pre-fix failure. The repair also
updates nullable type annotations, checks the final reference names/positions,
revalidates dependencies, and retains protocol authoring admission for the
new table.

On the final rebuilt service, the expanded probe passed **19/19 twice** with
identical output and no database residue. The distinct inheritance, alias,
nullability change, type-override and authored-default contracts are now in the
planner UT and the strict `issue_28594_div_precision.test` BVT. That BVT passed
**140/140 twice**
on one service with zero failures, ignored or abnormal statements. The existing
`expression_default_column_reference.sql` BVT passed **107/107** with its
historical metadata comparison disabled; it still checked result values and
errors. These local results do not replace pending CI or the separate topology
and restart gaps below.

## Remaining cases to close

These are explicit coverage gaps, not additional confirmed bugs. Use pairwise
coverage first; add higher-order combinations when a reachable interaction
requires them. No test may turn an error into success by retrying it.

| ID | Priority | Case and oracle | Smallest useful layer | Status |
| --- | --- | --- | --- | --- |
| G01 | High | Two sessions use increments 0 and 30; alternate identical SQL and prepared execution, then reconnect; values/metadata remain session-local | Frontend or SQL integration | GAP |
| G02 | High | Text versus binary prepared nested division: integer/decimal/float/NULL parameter transitions; compare values, physical type and public result metadata with the documented direct-query contract | Frontend metadata plus SQL protocol | GAP; observed metadata difference is not yet classified as a defect |
| G03 | High | Change precision, inject compile or column-metadata publication failure, then reuse the same prepared statement; no mixed-generation metadata/value or double cleanup | Existing frontend fixture with error injection | GAP |
| G04 | Medium | Persist live D10/D11 controls and expression-default/ON UPDATE precision transitions as permanent tests; ensure stable semantics after a second ALTER | Planner UT plus minimal SQL result | GAP |
| G05 | High | CTAS missing dependency and COPY ALTER evaluation failure: original schema/rows unchanged, no replacement table published; next CREATE in the same session uses current precision | SQL transaction/catalog oracle | GAP |
| G06 | High | Restart after nondefault DEFAULT/generated/CHECK authoring; new session inserts agree with stored semantics | Minimal persistent-service integration | GAP |
| G07 | Medium | Capability RPC cancellation, deadline, empty/error response: bounded completion, response released once, subsequent query succeeds | Existing compile mock client with explicit synchronization | GAP beyond current unknown-response control |
| G08 | Medium | Preserve a reusable temporal and ordinary-decimal benchmark; compare CPU, allocs and retained bytes at equal batch sizes, including a wide fallback | Function benchmark | GAP; local measurements above are not universal nonregression evidence |
| G09 | Medium | Many destinations with slow/old/unknown workers: measure planning/sending latency and bounded memory; verify no per-row probes | Controlled multi-CN performance test | GAP |
| G10 | High | Real protocol-96/97 binaries in both sender directions, then a worker upgrade between placement and send; correct result or explicit rejection, never wrong-scale data | Two-version integration topology | GAP; mock/wire tests do not close this |

## Maintenance rules

For each newly found exception, record the smallest reachable SQL/state, type
and precision setting, independent expected value/error, source revision,
observed result, test locator and root-cause group. Preserve a failing witness
before fixing it. A failure and its nearest successful control belong together.
Do not count case variants as separate defects or copy every permutation into
BVT. Do not label a pending, unselected or metadata-disabled run as a strict
pass. Keep manual failures outside automatic CI selection until their fixes and
regressions are delivered together.
