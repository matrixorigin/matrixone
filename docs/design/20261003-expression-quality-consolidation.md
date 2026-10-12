# Expression quality consolidation — revision 2

Continuation of #29249 and PR #29574; baseline9078a8bd, temporal closure commit95f58ac1d37. Broader audit covers342 recently changed production files. Candidate counts are discovery evidence, never deletion authority.

## Existing responsibilities and planned changes

TIMESTAMPADD DATE currently multiplies constant/dynamic units, output kind and initial wrapper type into eight row loops. Keep constant-unit parsing and dynamic metadata discovery exactly as supported today; resolve vector type once, then dispatch by fixed/dynamic unit and DATE/DATETIME output into four thin loops through doCalendarInterval. Share NULL publication and arithmetic-error handling; keep batch-invariant mode/output decisions outside the hot loops. Preserve constantNULL/invalid-unit error timing, selected-row rules, dynamic per-row NULLs, warning counts, precision and result-wrapper reuse. Check SetTypeAndFixData errors before interpreting backing memory: denied DATE-to-DATETIME growth leaves the original vector intact. A denied-growth oracle must prove error return without writes or leaked allocation. Preserve dynamic unit discovery before date/count NULL suppression. No public metadata policy change, no retirement of dynamic units merely because ordinary SQL grammar rejects them. Avoid caching parsed dynamic units in a new per-batch array; memory cost would add state for an uncommon internal path.

Expression constants must validate literal source metadata once before allocating, then materialize through existing allocation-aware helpers and apply source metadata once. NULL and non-NULL branches currently duplicate validation/application and have different cleanup. Keep IsBin/runtime-domain application exclusive to non-NULL values as today; share only source validation/application and failure cleanup. Preserve string domain, charset, binary subtype and prepared source identity; preserve allocation-account selection. Do not combine SQL semantic type and transport type.

Zonemap pruning owns temporary vectors passed to EvaluateFilterByZoneMap; verify ownership and panic paths before changing cleanup. Consolidate test fixtures and membership/endpoint cases using existing binders and real execution oracles; do not replace actual residual evaluation with assertions on helper internals. Preserve per-iteration leak checks, varying denominators, NULL/unknown proofs, signed bounds, scale and DST controls.

For unused function/state candidates, trace every reference including function values, registration, interfaces, generated/remote callers and tests before removal. Remove exclusively retired tests together. Any independent public contract or required recovery path blocks removal until separately designed.

## Validation

Record named coverage mappings and pre/post checks for each retired capability. Run full changed owning packages with native CGo; mutations must detect metadata, mask/NULL/diagnostic and arithmetic failures after fixture consolidation. Race checks only where lifecycle/shared state changes. Report implementation/test/docs deltas separately and preserve initial closure evidence. Final reviewer must assess entire expanded PR; no performance claim without measurement.

## Performance refinement

Same-process baseline/head experiments reproduced a small CPU regression from retaining batch-mode and output-kind decisions in the common loop. Select the four combinations once per batch without restoring initial-wrapper branches, a callback per row, generic method dictionaries, or a parsed-unit cache. Keep the existing unit-admission and vector-conversion owners. The normal NULL helper must inline; the shared error helper runs only after arithmetic failure. Validate baseline recovery and unchanged allocations together with metadata, selection, diagnostics, caller reuse and denied growth. The benchmark represents expression CPU, not an overall query-performance claim.

## Stage coverage and fixture ownership audit

Refs #29249 remains an ongoing task. This stage covers the complete PR range from `9078a8bd` through `e37f9795` plus the local fixture correction; it preserves the 72-line retirement in `e37f9795`. It does not claim that all 342 recently changed files or all repository tests have been audited.

The binder owns SQL result metadata; registered adapters own masks, NULL and per-evaluated-row diagnostics; checked calendar/time helpers own arithmetic. Constant construction owns allocations until successful executor transfer. Schema admission, index prefix matching, scoped authorization and the local Iceberg coordinator keep their existing owners. No new production state, scheduling, fixture framework or compatibility branch is introduced.

### Removed top-level tests to retained contracts

Every removed top-level test in the 14 changed test files is listed below. A retired implementation-only capability is explicitly marked as retired rather than represented as equivalent live coverage. Changed subcases and forwarding calls are mapped separately afterward.

| Removed test | Retained test or retirement | Independent value |
| --- | --- | --- |
| `TestDateStringAddOverflow` | TestDateStringInterval | upper year; NULL and warning identity where applicable |
| `TestDateStringAddNegativeYearOverflow` | TestDateStringInterval | lower year; NULL and warning identity where applicable |
| `TestDateStringAddOverflowNegativeMonth` | TestDateStringInterval | lower month; NULL and warning identity where applicable |
| `TestDateStringAddOverflowNegativeQuarter` | TestDateStringInterval | lower quarter; NULL and warning identity where applicable |
| `TestDateStringAddMicrosecondPrecision` | TestDateStringInterval | add microsecond; NULL and warning identity where applicable |
| `TestDateStringAddNonMicrosecondInterval` | TestDateStringInterval | fraction second/minute/hour/day; NULL and warning identity where applicable |
| `TestDateStringAddPadsFractionalSeconds` | TestDateStringInterval | pad four/three/one digits; six digits rollover; NULL and warning identity where applicable |
| `TestDateStringAddReturnTypeCompatibility` | TestDateStringInterval | varchar, char and text return OIDs plus exact formatted values; NULL and warning identity where applicable |
| `TestDateStringAddDateFormatOutput` | TestDateStringInterval | add day/month/year/week/quarter/second/minute/hour; NULL and warning identity where applicable |
| `TestDateStringSubMicrosecondPrecision` | TestDateStringInterval/sub microsecond | Exact subtraction and six fractional digits |
| `TestDateStringSubDateFormatOutput` | TestDateStringInterval/sub day/month/year/week/quarter/second/minute/hour | Exact date-only versus clock formatting |
| `TestDateStringAddVeryLargeInterval` | TestDateStringIntervalCountOverflow | Both signs of near-MaxInt64 SECOND/MINUTE/HOUR; row-local NULL |
| `TestDateStringAddInvalidInterval` | TestDateStringIntervalCountOverflow | MaxInt64 sentinel for YEAR_MONTH/DAY/WEEK/SECOND. Old intervalStr labels were never passed to a parser; actual parser syntax/overflow is independently owned by TestNormalizeIntervalDistinguishesOverflowFromInvalidSyntax in pkg/container/types. |
| `TestDoDatetimeAddComprehensive` | TestCalendarIntervalArithmetic | All 22 named cells retained: exact units/composite YEAR_MONTH, overflow marker, upper/lower calendar bounds and large signed counts; exact error sentinel |
| `TestDoDateStringAddComprehensive` | TestCalendarIntervalArithmetic; TestDateStringIntervalParsing | Shared arithmetic cells plus independently rejected duration/malformed strings and exact valid pre-epoch result; do not duplicate the arithmetic fixture for string parsing |
| `TestIsDateOverflowMaxError` | TestIsDatetimeOverflowMaxError | Retired duplicate date sentinel; retained canonical sentinel and unrelated-error negative control |
| `TestTimestampAddDateWithConstantDateUnitAndDateResultType` | TestTimestampAddDateMetadataAndWrapperReuse | Corresponding constant/dynamic DATE/clock/NULL-unit row under each initial wrapper; exact values, NULL bits, result OID and scale; one wrapper reused sequentially |
| `TestTimestampAddDateWithConstantDateUnitAndDatetimeResultType` | TestTimestampAddDateMetadataAndWrapperReuse | Corresponding constant/dynamic DATE/clock/NULL-unit row under each initial wrapper; exact values, NULL bits, result OID and scale; one wrapper reused sequentially |
| `TestTimestampAddDateWithConstantTimeUnitAndDatetimeResultType` | TestTimestampAddDateMetadataAndWrapperReuse | Corresponding constant/dynamic DATE/clock/NULL-unit row under each initial wrapper; exact values, NULL bits, result OID and scale; one wrapper reused sequentially |
| `TestTimestampAddDateNonConstantTimeUnitWithDateResultType` | TestTimestampAddDateMetadataAndWrapperReuse | Corresponding constant/dynamic DATE/clock/NULL-unit row under each initial wrapper; exact values, NULL bits, result OID and scale; one wrapper reused sequentially |
| `TestTimestampAddDateNonConstantTimeUnitWithDatetimeResultType` | TestTimestampAddDateMetadataAndWrapperReuse | Corresponding constant/dynamic DATE/clock/NULL-unit row under each initial wrapper; exact values, NULL bits, result OID and scale; one wrapper reused sequentially |
| `TestTimestampAddDateNonConstantDateUnitWithDateResultType` | TestTimestampAddDateMetadataAndWrapperReuse | Corresponding constant/dynamic DATE/clock/NULL-unit row under each initial wrapper; exact values, NULL bits, result OID and scale; one wrapper reused sequentially |
| `TestTimestampAddDateNonConstantDateUnitWithDatetimeResultType` | TestTimestampAddDateMetadataAndWrapperReuse | Corresponding constant/dynamic DATE/clock/NULL-unit row under each initial wrapper; exact values, NULL bits, result OID and scale; one wrapper reused sequentially |
| `TestTimestampAddDateNonConstantUnitWithNullUnit` | TestTimestampAddDateMetadataAndWrapperReuse | Corresponding constant/dynamic DATE/clock/NULL-unit row under each initial wrapper; exact values, NULL bits, result OID and scale; one wrapper reused sequentially |
| `TestDoDatetimeAddWithDefaultCaseInSwitch` | TestCalendarIntervalArithmetic/Normal add 1 day | Exact next-day value replaces nonzero assertion; no claim of reaching an obsolete default branch |
| `TestDoDatetimeAddWithNumsZero` | TestCalendarIntervalArithmetic/Normal add 1 day/week/hour/minute/second/microsecond | Exact results replace six repeated nonzero assertions about a removed implementation branch |
| `TestDoTimestampAddWithAddIntervalFailure` | TestTemporalMicrosecondBoundaryOverflowIsNull; TestTimestampAddTimestampWithMaxInt64Interval; Test_doTimestampSub_Edge | Required overflow errors and NULL/valid-neighbor outcomes replace optional if-error assertions; retained UTC/timezone lower-bound cases remain distinct |
| `TestDataBranchSchemaEquivalentRequiresCompleteLogicalTypes` | TestCheckSchemaCompatibility_RejectsDifferentTypeAttributes; TestCheckSchemaCompatibility_Identical | Five named attribute negatives (width, scale, enum, type nullability, auto increment), each with an equal-schema control, now call real schema admission |
| `TestTryMatchMoreLeadingFiltersRequiresContiguousPrefix` | TestRegularIndexOnlyMatchRequiresContiguousPrefix | Missing second/third parts and complete prefix assert exact active-owner filter positions |
| `TestBindTimestampAddReturnType` | TestBindTimestampAddFSPByUnit | DATE calendar/clock units, DATETIME/TIMESTAMP FSP, CHAR and unknown-unit fallback; assert bound OID/scale/width and actual result-column metadata |
| `TestEvaluateFilterByZoneMapNullableInListIsConservative` | TestNullableMembershipPruningAndResidual/list match | Exact prune decision plus actual folded residual true/false/NULL values; pre-fallback allocation baseline. Added vector-miss cell covers the distinct wire-vector representation. |
| `TestEvaluateFilterByZoneMapNullableInVecIsConservative` | TestNullableMembershipPruningAndResidual/vector match | Exact prune decision plus actual folded residual true/false/NULL values; pre-fallback allocation baseline. Added vector-miss cell covers the distinct wire-vector representation. |
| `TestEvaluateFilterByZoneMapNullableInListWithoutMatchPrunes` | TestNullableMembershipPruningAndResidual/list miss | Exact prune decision plus actual folded residual true/false/NULL values; pre-fallback allocation baseline. Added vector-miss cell covers the distinct wire-vector representation. |
| `TestEvaluateFilterByZoneMapNullableNotInListPrunes` | TestNullableMembershipPruningAndResidual/not in miss | Exact prune decision plus actual folded residual true/false/NULL values; pre-fallback allocation baseline. Added vector-miss cell covers the distinct wire-vector representation. |
| `TestFoldedNullableInExprKeepsMatchAndNullsMiss` | TestNullableMembershipPruningAndResidual/list match; vector match | Exact prune decision plus actual folded residual true/false/NULL values; pre-fallback allocation baseline. Added vector-miss cell covers the distinct wire-vector representation. |
| `TestFoldedNullableNotInExprNullsMiss` | TestNullableMembershipPruningAndResidual/not in match; not in miss | Exact prune decision plus actual folded residual true/false/NULL values; pre-fallback allocation baseline. Added vector-miss cell covers the distinct wire-vector representation. |
| `TestEvaluateFilterByZoneMapNotEqualBareNullPrunes` | TestEvaluateFilterByZoneMapNullComparisonsPrune/!= and <> | Same bare-NULL comparisons already belong to the retained seven-operator matrix |
| `TestCompileExternScanIcebergFileFanout` | TestCompileExternScanIcebergCoordinator | One local scope/address/Mcpu; original input immutability; runtime data/delete tasks, columns, snapshot and hidden-column wiring |
| `TestSplitIcebergDataFileShardsBalancesFiles` | Retired with unused splitter | Base already emitted one local scope; shard load balancing was not a live execution contract |
| `TestIcebergRemoteFanoutPolicyBlocksObjectRefEvenWithRemoteSigning` | Retired with unused remote-policy helper | No remote consumer retained. Live access/local credential behavior remains in TestCompileIcebergScanPassesAccessContextToPlanner and TestCompileIcebergScanKeepsCredentialScopedTasksOnCurrentCN; these do not simulate retired remote authorization. |
| `TestIcebergRemoteFanoutPolicyBlocksObjectRefWhenRemoteSigningDisabled` | Retired with unused remote-policy helper | No remote consumer retained. Live access/local credential behavior remains in TestCompileIcebergScanPassesAccessContextToPlanner and TestCompileIcebergScanKeepsCredentialScopedTasksOnCurrentCN; these do not simulate retired remote authorization. |
| `TestIcebergRemoteFanoutPolicyBlocksObjectRefEvenWithProtectedCNToCN` | Retired with unused remote-policy helper | No remote consumer retained. Live access/local credential behavior remains in TestCompileIcebergScanPassesAccessContextToPlanner and TestCompileIcebergScanKeepsCredentialScopedTasksOnCurrentCN; these do not simulate retired remote authorization. |
| `TestIcebergRemoteFanoutPolicyBlocksCredentialScopeEvenWithRemoteSigning` | Retired with unused remote-policy helper | No remote consumer retained. Live access/local credential behavior remains in TestCompileIcebergScanPassesAccessContextToPlanner and TestCompileIcebergScanKeepsCredentialScopedTasksOnCurrentCN; these do not simulate retired remote authorization. |

### Forwarding calls and changed subcases

| Retired path | Active owner / retained tests | Contract |
| --- | --- | --- |
| `buildExecuteUserParams` forwarding | `buildExecuteUserParamsWithMemberOfPositions`; `TestInitExecuteStmtParamFreesParamsOnResolveError`, `TestBuildExecuteUserParamsPreservesBoundConcreteTypes`, `TestBuildExecuteUserParamsRejectsBoundTypeKindMismatch`, `TestBuildExecuteUserParamsHonorsStoredProcedureScope`, `TestBuildExecuteUserParamsPreservesExplicitTextOverride` | Resolution failure releases parameters; concrete types/kinds, procedure/session scope and text override remain independent |
| `currentTxnSnapshotTS` session forwarding | `currentTxnSnapshotTSForProcess`; `TestCurrentTxnSnapshotTS`, `TestInitExecuteStmtParamUsesTxnSnapshotAfterRebuild` | Actual process snapshot, including rebuilt execution |
| `initExecuteStmtParamWithResolver` and `createPrepareStmt` forwarding | Existing `InSession` owners; `TestInitExecuteStmtParamReusesStableSubscriptionSelect`, `TestCreatePrepareStmtRestoresCurrentExecCtx` | Active session/compiler context and stable compile reuse; benchmark continues to call the real owner |
| Old role SQL wrappers and unscoped object-WGO helper | `Test_determineDML`, `TestGetRoleSetThatPrivilegeGrantedToWGOScopedCoverageEdges` | Explicit object/type/level scoping; retained ownership/fallback positives and exec/get-result/row-decode negatives. Removed object-only mock subcases belong to a retired helper, not a new authorization policy. |
| Flat-ring geometry helper chain | `TestGeometryDistanceHelpersRejectMalformedSlices` calls active line/polygon geometry owners; retained SQL distance/holes/SRID/mask cases | Malformed inputs and holes-aware geometry remain distinct; no claim that obsolete flat-ring behavior remains supported |

### Independent QA and fixture failure boundaries

The retained public SQL BVT case and actual binder/executor reuse cover real consumers. External renamed baseline-function comparisons are supplementary differential evidence, not simulated consumers or substitutes for literal oracles. They are not shipped as a default Cartesian test matrix.

`TestTimestampAddDateMetadataAndWrapperReuse` additionally distinguishes a masked MICROSECOND from a NULL-date MICROSECOND, validates late units even after precision reaches six, and checks malformed-unit admission before an earlier row's overflow warnings or result-type changes. `TestTimestampAddDateDeniedTypeGrowth` proves the account-capacity error before backing access and keeps the original DATE vector/length/data/account usage. `TestTimestampAddDateWarningsPerSelectedRow` covers four actual loop modes and exactly two evaluated warnings, not one scalar callback. `TestIntegerDateIntervalAdmission` keeps integer-date conversion, NULL/masks, overflow diagnostics and empty-batch clock-unit rejection at its own adapter. `TestCalendarIntervalDiagnosticsAndSelection` retains independent direction, mask/NULL ordering and warning suppression.

Fixture cleanup is registered immediately after each acquisition. Raw vectors are protected before Append; plain constant constructors return an owned vector even on allocation error, so their cleanup precedes the error assertion. Allocation-aware constructors instead release on failure. Returning helper fixtures use `t.Cleanup` so inputs outlive the helper. Process cleanup is registered first; result/input cleanup and account Seal/Finalize run before it. Account finalization is registered before selection/result setup and both teardown operations execute before nonfatal reporting. Payload controls use a lexical defer and are released before the constructor-under-test; unexpected executors also have cleanup. Exact account-use, metadata and CurrNB assertions remain before fallback cleanup, which cannot hide a constructor leak. Dynamic-unit fixtures construct the selected representation directly, eliminating the temporary constant-vector allocation/free.

No sleeps, retries, random scheduling, bigger data, global-helper changes, added production hooks, or new cluster fixtures are needed. Existing sequential subtests share a process only under identical configuration and reset warning state; caller-owned result-wrapper reuse remains explicit.

### Evidence, cost and limits

The complete function/colexec packages and the nine affected named tests are validated normally; the affected named tests also run with race. Incremental configured static checks cover both owning packages. Other production/consumer inputs are unchanged, so the accepted binder/executor, 56-statement SQL replay, arithmetic differential and expression benchmarks remain applicable. Historical full race/SCA/two multi-CN BVT evidence at `53fd9e19` supports unchanged closures; pending checks at later heads are not passing evidence. Dedicated Iceberg E2E was skipped.

Cost observations from the completed SQL replay separate the mechanisms: cluster construction 100.193ms; admission wait 78.88us; service start 11.570s; scenario body 2.034s; test total 13.71s; package total 14.331s. Prior full function package 14.982s included external QA; current-stage owning-package times are reported separately in the PR. Those observations have no same-condition original-test-suite control and therefore establish no CI/test-runtime saving. Whole-CI CPU/memory remain unmeasured. Source/fixture reduction is recorded separately from measured expression CPU; seven-row SECOND and masked/NULL limits remain disclosed in the PR.

Original author mutation reports detected direction, mask, metadata, denied-growth and constructor-cleanup changes. The outer zonemap scratch-cleanup mutation survived its selected probes, so those probes do not establish sensitivity to every outer cleanup leak. Do not count the surviving mutation as effective coverage or weaken the pre-fallback allocation oracle. This limitation, skipped Iceberg E2E and unmeasured system-wide cost remain explicit follow-up boundaries of #29249.

### Follow-up: outer zonemap scratch-cleanup sensitivity

The original scratch-cleanup mutation survived because `CurrNB()` observes native bytes, while operand vectors allocated by `ZMToVector` can own Go-heap backing. Extend the existing `TestEvaluateFilterByZoneMapRoundOverflowCleanup` and `TestEvaluateFilterByZoneMapScalarOverflowCleanup` with a pre-fallback `OnHeapCurrNB()` baseline and explicit nil scratch-slot assertions. Their existing success, overflow/unknown-result and reuse sequences supply the scenarios; no new test, fixture or product behavior is introduced. Fallback cleanup remains solely a safety net after body assertions.

A task-private Go overlay removing only the outer cleanup still passes the old scalar-overflow and materialized-tuple probes. With the strengthened assertions it fails the retained round and signed/unsigned scalar scenarios on live Go-heap bytes (32 bytes for scalar operands). This closes the previously recorded sensitivity gap for that exact mutation; it does not establish arbitrary leak coverage or a suite-cost improvement.

A second overlay that frees vectors but retains scratch pointers is rejected independently by the nil-slot assertions after the heap-ownership baseline passes. The normal complete colexec package passes (0.150s); both existing cleanup tests pass with race (1.147s), count1. The two mutants fail in the test body before fallback teardown.

### Follow-up: DATE_FORMAT fixture compression and measured cost

`DateFormat` remains the production owner. The six `initFormatTestCase1`–`initFormatTestCase6` fixtures repeated 24 literal date/result pairs over 600 cases and 4,915,200 rows. A single DateFormat-specific adapter now serves UT `(1 case, 4 rows)` and the unchanged benchmark population `(100 cases, 8192 rows)`. Independent input/result backing arrays remain separate. No production change, cluster fixture, timer or generic test framework is introduced.

| Retired fixture / scenario | Retained exact scenario | Prior distinct date/result rows retained |
| --- | --- | --- |
| `initFormatTestCase1` and duplicate `TestFormat` / `initFormatTestCase` | `TestDateFormat/all_tokens` | all four; year 0001, week/year fields and five-digit fraction padding retained |
| `initFormatTestCase2` | `TestDateFormat/comma_datetime` | all four, including year 2021 |
| `initFormatTestCase3` | `TestDateFormat/dash_date` | all four |
| `initFormatTestCase4` | `TestDateFormat/slash_date` | all four |
| `initFormatTestCase5` | `TestDateFormat/dash_datetime` | all four |
| `initFormatTestCase6` | `TestDateFormat/slash_datetime` | all four |

All 24 original datetime/expected string tuples and format literals were compared against `c161ffc580` and preserved byte for byte. `TestDateFormatUsesPerRowFormat` and `TestDateFormatZeroDatetimeMatchesMySQL` retain their existing selection, NULL and zero-date oracles, with cleanup-only changes. Every case releases its result wrapper and parameters before its process; native and Go-heap ownership are checked after case cleanup. Six benchmark names and assignments remain, including the existing per-case `BenchMarkRun()` then `Run()` sequence; benchmark iteration/timing policy is unchanged.

Controlled Go 1.26.4/Linux amd64, GOMAXPROCS=2/GOMEMLIMIT=2GiB: frozen pre-compression `c161ffc580` and candidate binaries each ran the complete function package three times in alternating serial order. Compilation was separate and excluded from runtime measurements. All six runs terminated PASS; current focused normal and race selection also passed, including all six format scenarios and both unchanged edge suites.

| Complete function package, three samples | Before compression | After compression | Median change |
| --- | --- | --- | --- |
| Wall time | 13.573–13.697s; median 13.625s | 11.825–11.850s; median 11.846s | −13.1% |
| Process CPU, user + system | 4.428–4.643s; median 4.431s | 2.407–2.620s; median 2.618s | −40.9% |
| Peak process RSS | 478.9–491.4 MiB; median 487.2 MiB | 200.4–215.7 MiB; median 208.5 MiB | −57.2% |

Wall time uses a monotonic process interval; CPU and peak RSS use the terminated child's `wait4` resource usage. This measures the owning package on the shared host, not whole-CI or production-query performance. The previous baseline-to-pre-compression measurements established no package gain; these reductions come from eliminating repeated correctness work and releasing fixture resources. Source-derived UT input/result/bool backing-array population falls from 121.875 MiB to 624 bytes, excluding vectors, strings and allocator overhead. Lock-cleanup waits remain a separate lifecycle investigation.


## Lock-fixture checkpoint review (2026-10-03)

Refs #29249; this checkpoint does not close the continuous quality audit.
Actual read-only reviewer: `gpt-6.1-sol`, reasoning effort `xhigh`.
Decision: **APPROVE for the explicitly requested non-final checkpoint commit/push**;
**overall PR readiness remains blocked by the failed full-function race gate**.

Reviewed source delta against `0fa3777831f72beb466cb03481490fa0b225947a`:
`func_unary_test.go`, +9/-3, SHA-256
`b5749a179a9f450536817ce78bee9588df60cfad645e8be9ff7a80e0902915a6`.
The shared fixture releases its two atomic simulated unlock blockers after the
callback, then invokes the existing reset owner. Scenario assertions, deadlines,
production code and dependencies are unchanged. Reset still invalidates the
generation, joins the retained worker, and replaces detached queues. Cleanup
also executes on same-goroutine `FailNow` and panic. Current consumers do not
replace the captured services; caller-owned channels are untouched.

| Evidence | Terminal result |
|---|---|
| Related lock tests, normal | 74/74 PASS; package PASS |
| Related lock cases within full race run | 74/74 PASS; enclosing package FAIL |
| Two directly benefited tests, individually under race | 100 repetitions each PASS |
| Full function normal measurements | Six runs PASS; three alternating serial pairs |
| Full function race | FAIL: admitted regexp control returned `regexp match timed out` |
| Verified clean baseline, full function race | PASS in 17.063s |

Compiled binaries were measured serially with the same pinned dependency,
`GOMAXPROCS=2` and `GOMEMLIMIT=2GiB`; build time was excluded. On the shared host,
median wall time was **11.750s -> 9.869s (-16.0%)**, CPU **2.081s -> 2.451s
(+17.8%)**, and peak RSS **209.7MiB -> 199.9MiB (-4.7%)**. This establishes no
CPU reduction or whole-CI improvement.

Independent deterministic probes expose idle and concurrent deadline defects in
regexp2; canonical complete clock controls pass the corresponding failing
counterexamples. They do **not** establish causality for the complete MatrixOne
race failure or prove absence of a fixture-induced regression. The dependency
fix, compatible artifact provenance, and final race closure remain follow-up work.
The proposed v1.11.5 upgrade was withdrawn after the concurrent counterexample.

Existing broader fixture limits remain: an assertion holding a mutex can prevent
reset, and detached receivers/backlog retries lack complete generation teardown.
This change releases simulated blockers; it does not claim universal teardown.
Unchanged closures reuse the preceding 27-file reviews and validation evidence.

Configured incremental golangci-lint: **PASS, 0 issues**, terminal exit 0. The
first attempt was interrupted while trimming the shared lint cache; a task-local
cache completed the same checks in 68s. MO-specific lint has no incremental
finding: its two unsafe diagnostics exactly match prior accepted evidence in
unchanged files. Formatting and patch checks pass. No new BVT is required for
this fixture-only change; existing production/public-path evidence is unchanged.

## Decimal256 scalar cast oracle consolidation (Refs #29249)

The dispatcher test now owns 21 original numeric/string destination routes and
five rejection cases. Eight former NULL-only evaluations share the corresponding
positive evaluations, with exact NULL publication assertions. Seven helper smoke
calls are absorbed into these real dispatch routes; a separate `(76,0) → (65,0)`
case preserves the helper parsing path, distinct from `(76,0) → (76,0)` copying.
Thus 41 evaluations become 27; no measured speedup is claimed for this closure.

Independent typed coefficient/value literals replace ignored errors and unchecked
results. Binary padding is checked byte for byte. Successful results retain full
destination metadata and source metadata; rejected casts assert error classes,
diagnostics and no NULL publication; binary width rejection also checks empty
variable output. Fixed result length is preallocated, so it is not an error
publication oracle. Both pool accounts must return to baseline after
each child, including early assertion failure. Existing FunctionTestCase owns
construction, result comparison and cleanup; one existing Process constructor
supplies context and memory without unnecessary file or SQL services.

Public CAST scale normalization, fractional assignment, precision rejection,
rounding, YEAR SQL modes and Datalink validation remain in their existing tests.
The bare numeric Datalink route is not a public validation claim. The current YEAR
route was absent from the old matrix and is outside this bounded consolidation.
Production code, shared framework and dependencies are unchanged; BVT is not
applicable to this test-only scalar fixture closure. Focused normal/race runs
pass all 27 children; 11 related public CAST tests pass. Vet and configured
incremental lint pass; two unchanged baseline MO lint diagnostics remain.
Six task-private producer mutations survive the old tests and are rejected by
the strengthened assertions, with both accounts restored. A forced FailNow
restores both accounts; omitting cleanup retains 32 heap bytes and is detected.
The initial diagnostic classifier incorrectly demanded simultaneous native and
heap leakage; the existing runtime logs were reclassified without rerunning.
Full function race remains failed under #29592 and unwaived; #29593 and #29594
remain unresolved. This local checkpoint cannot clear those gates; #29249 stays
open.

## Decimal multiplication/modulo kernel consolidation (Refs #29249)

Baseline: `83b47281b875a549eb21460282c920273109ffe4`. One connected test file
consolidates 15 overlapping wrappers into existing kernel owners. Production,
shared framework, dependencies and public BVT are unchanged. There are 101 kernel
calls and 205 logical positions, formerly 110 and 8,366, excluding unchanged
exceptional owners. Single-purpose batches stay direct; tables contain only
varying policy fields. `d256MulRef` remains the existing benchmark baseline.

| Retired responsibilities | Retained owners and independent witnesses |
| --- | --- |
| D128 multiplication, scales, constants and NULLs | `TestD128Mul`: int64/inline routes, both broadcast orientations, four separate signed-admission boundary identities, rounding and typed overflow |
| D256 multiplication tiers and large operands | `TestD256Mul`: actual int32/int64/generic routes, MaxInt64 squared, full-width carry, NULL-first mixed batches and scaled generic overflow suppression |
| Misnamed high-scale/int64 smokes | Reduction 8 is `TestD256Mul_Int32ScaleDown`; the former int64 fixture actually sampled int32. Actual high-scale, declared-width and raw-overflow recovery owners remain in `arith_decimal_wide_test.go` |
| Modulo helper argument matrices | `TestD256Mod`: caller-derived admission/length/bitmap, both scale directions/chunks, narrowing fallback, dividend sign and full-width remainders |
| Modulo zero/NULL policies | `TestD256Mod_DivByZeroPaths`: strict typed errors, permissive NULL publication followed by a live row, pre-existing NULL followed by a live row, all-NULL vector-divisor control |

All 105 former named children have concrete retained destinations in the local
review evidence. Original large-modulo operands, carry and alignment-overflow
owners remain. The original multi-step helper rounding oracles remain, with four
caller batches added. Inline adjustments retain their operands but use reachable
scales `(8,8)` and `(12,12)`. NULL payloads are undefined: compare every live
coefficient and the complete bitmap. Public wholly-NULL/constant-zero bypass
remains in its existing public owner. No scratch-result publication is assumed
on raw errors.

Final focused normal and race runs pass 18 owners and 125 children, zero skips.
The owning function package passed 2,096 ordinary tests and its fuzz seeds; the
last three added NULL positions change only two pure test bodies, covered by the
final focused runs. Unchanged owners reuse that complete-package evidence.
Vet and configured incremental lint pass (0 incremental issues); MO lint retains
two byte-identical baseline unsafe-import diagnostics. BVT is not applicable.
Ten task-private producer variants are rejected by 26 expected runtime assertion
failures, including NULL early returns, rounded/high-limb values, fallback,
whole-batch admission and zero policy/class. Formal production is untouched.

Three alternating pairs use the same diagnostic binary and untouched production
kernels. Summed synchronous test-body medians: wall 4.731ms -> 1.391ms (-70.6%),
process CPU 5.153ms -> 1.734ms (-66.3%), Go TotalAlloc 1,330,960 -> 326,272 bytes
(-75.5%), allocations 14,273 -> 4,181 (-70.7%). CPU includes background threads;
Go allocation excludes native memory and RSS. These exclude initialization,
fixtures, build/link and queue time; they are not whole-CI or machine gains. An
initial broad selector accidentally included eight unmeasured wide tests; that
pair is uncredited. Six corrected exact-scope runs reuse the same built binary.
These focused results do not clear the failed/unwaived full function race gate
under #29592. This checkpoint remains local; #29249 is an ongoing task.

### Decimal Add/Sub consolidation — local work in progress

Starting at `a7033da65d`, the six existing batch-kernel owners use independent
literal coefficients, complete NULL bitmaps and required error codes. Alias
bindings, helper indices, singleton decimal-minus-integer Format oracles and
wrapper error translation retain separate owners. The six scalar Decimal64
method cases move intact into the existing types owner. Thirteen duplicate
batch-test owners, two mixed-owner children and their exclusive reference/data
helpers retire. Shared helper and benchmark consumers remain.

NULL witnesses preserve the intended scaling admission: fast-path masked
coefficients stay within the prescan domain; separate checked-scaling cases
verify suppressed overflow and continuation. Scaling helper tests now compare
full signed coefficients. The error-mapping test registers cleanup immediately
after each allocation and checks both memory accounts after release.

The canonical tables contain 143 calls and 381 logical positions, excluding
retained/helper owners. Cost measurement below covers the mapped fast-file owners.
The normal candidate executes 139 passing cases and four separately failing
scale-39 vector-upscaling cases. The latter are correct-result expectations,
not expected-panic tests. Actual SQL parsing, planning and expression execution
also reproduce this defect; this is not full-server BVT evidence.

The promoted sources pass the types package and the function package excluding
those four independently recorded failures. Focused race passes 14 owners and
192 subtests, with the same four known failures excluded and recorded separately.
Scoped vet and incremental lint pass (0 new lint issues); molint exits zero with
two byte-identical baseline unsafe-import diagnostics. Comment cleanup changes
no executable text. Three private producer variants (zeroed scaling result,
wrong error class, first-row-only admission) survive the selected old tests;
the strengthened tests reject all three with 14 runtime assertion failures.
Both normal control groups pass for every variant. This evidence covers these
three counterexamples, not exhaustive correctness. Final gpt-6.1-sol xhigh
read-only review found no additional defects or checkpoint blockers and accepted
only an explicitly known-red local checkpoint. The scale-39 production defect
and full-function race gate #29592 are unresolved. This work is local and is
neither a green delivery nor completion of #29249.

Three alternating same-binary pairs measure 23 original versus 10 current
fast-file owners, using untouched production arithmetic. Test-body median wall
8.360ms -> 2.189ms, process CPU 8.856ms -> 2.679ms, Go TotalAlloc 1,217,176 ->
516,360 bytes and allocations 29,157 -> 6,512. The mapped scope includes unchanged
mixed NULL/downscale siblings on both sides; moved types rows, wrapper cleanup
and unchanged alias/Format owners are outside this measurement. Four new known-red
scale-39 literals are omitted only from the diagnostic snapshot; their formal
regressions remain unchanged and failed. This is a passing-closure cost comparison,
not evidence that all new tests pass. CPU includes background threads; allocation
excludes native memory/RSS; build/link/init/queue are outside the measured bodies.
Root-body measurements include child test-harness work and verbose output.
An initial unequal-filter run and a subsequent equal-filter run are uncredited:
Go 1.26.4 testdeps caches one regex, so alternating run/skip patterns recompiles
regexes and distorts test-body cost. The final comparison has no skip filter.

### Decimal64 multiplication — validated local checkpoint

`d64Mul` owns scale/shape dispatch through `d64MulScaled` and `d64MulInline`.
Six test owners consolidate into the existing `TestD64Mul`: 20 old scenarios map
to 24 cases, with 3,712 rows reduced to 78. Five duplicate owners and orphan
comments retire. All 164 other function bodies, including benchmarks, remain
identical. Test delta: -143 lines/-2,715 bytes; production delta: zero.

The old `LargeValues_SlowPath` actually stayed inside signed-int32 admission.
Independent literal coefficients now cover real wide scaling, both admission
operands, late-wide-row rejection, signed endpoints/high limbs, half rounding,
NULL-prefix continuation, scalar broadcasting and scale-policy boundaries.
Review found that wide fast-path quotients must be preserved separately in VV,
SV and VS loops; each now has positive and negative results exceeding int32.
A left-int32/right-wide case independently verifies the right admission gate.
No artificial overflow-error matrix is added: signed D64 products fit D128.

All 24 controls pass. Six private wrong implementations are rejected by 12 new
runtime assertions; two original broadcast tests also reject narrowed quotients,
proving those obligations were retained. Selected old NoError-only groups miss
NULL/rounding faults; all six old owners miss the admission faults. Both controls
pass for every variant. This is scoped sensitivity evidence, not exhaustive QA.

Three alternating same-binary pairs use untouched kernels: root-body medians
wall 1.562ms -> 0.355ms, process CPU 1.665ms -> 0.431ms, Go TotalAlloc 276,312 ->
68,512 bytes and allocations 4,761 -> 954. Child harness/verbose output are
included; build/link/init/queue and native memory/RSS are excluded. These are
mapped test costs, not whole-CI or query gains. Current go vet and incremental
lint pass; molint exits zero with the same two recorded unsafe-import diagnostics.
The gpt-6.1-sol xhigh follow-up review closes both findings without additional
concrete defects. Current focused race exits zero with all 24 cases passing and no skipped cases.
The earlier superseded race build was cancelled after the review findings, not
counted as passed. Four scale-39 failures and full-function
race #29592 remain unchanged and unwaived; #29249 remains unfinished.

### Decimal division — proposed next closure, review changes open

Production `divFn` reads the bound result scale and uses the three AtScale
adapters/kernels. The old three factories, three default-scale entry points and
`legacyDecimalDivisionScale` have only test/benchmark consumers. The proposed
retirement migrates 137 call sites at their original numerical scales before
removing those seven symbols. This preserves workload semantics; the frozen
raw scales do not define current SQL type inference. A focused control checkpoint
separates migration mistakes from later fixture/oracle replacement.

Then consolidate batch contracts into existing `TestD64Div`, `TestD128Div` and
`TestD256Div` typed tables. Map every old scenario by physical shape, scale
adjustment, admission operand, actual fallback, signed quotient range, rounding,
NULL and typed error before deletion. Preserve strong Format/cross-width/integer
oracles, mixed non-division children and benchmark workloads. Expected limbs
come from independent integer arithmetic with one final half-away-from-zero
rounding; production helpers must not calculate expected values.

Names are not branch evidence: D64 inline scaling cannot overflow because
`2^63 * 10^19 < 2^127`; its six overflow-fallback sites are retirement candidates
requiring explicit review. Two D128 fallback owners also use numerators too small
to reach their claimed inline rejection. Preserve their useful value/NULL domains
and add genuine intermediate-overflow and final-overflow witnesses. Keep the
D128 fallback and D64 out-of-inline scale paths, which remain reachable.

Existing adapters own all-masked admission, public error translation and result
precision; existing direct-call/executor owners publish constant results. Reuse
these consumer tests and strengthen only missing metadata/error assertions.
Raw singleton cases do not establish arbitrary logical-length broadcasting.
Require actual normal/race/static terminal results, independent QA rejection and
matched body-cost measurements. No query-speed or whole-CI gain is claimed.
This proposal does not resolve scale-39 or #29592, and does not complete #29249.

Independent gpt-6.1-sol xhigh review required typed final-overflow assertions
in all six D128 inline-fallback loops and named deletion mappings. These design
conditions are now closed: three D64 no-NULL destinations were corrected, and
two D128 masked generic scenarios also map to existing high-limb continuation
oracles. All 130 original scenarios, three benchmarks and eight direct helper
calls have named dispositions; mixed siblings and strong independent owners stay.

Phase A was committed as `1494ca5612`: 137 explicit-scale caller migrations and
seven unused definitions removed. All 37 controls, vet and incremental lint pass;
molint has two unchanged baseline diagnostics. Its review accepted that mechanical
checkpoint only.

Phase B is applied locally. The three concrete division tables have 157 cases;
18 selected test roots pass with no skips, including retained independent owners
and strengthened metadata/constant-publication consumers. Six private mutations,
each swallowing one D128 fallback error, are rejected by the corresponding typed
assertion; both real and unmutated-clone controls pass all 80 D128 cases. The D64
change removes only six proven-unreachable fallbacks and corrects their obsolete
scale-policy comment. Other scale paths and all D128 fallbacks remain.

The fast-test consolidation removes 470 lines while adding about 8.1 KB of
literal data and exact assertions; 138 other function bodies, including benchmarks,
are byte-identical. Line reduction alone is not a cost claim. Public precision
checks now cover the nearest accepted bound, exact bound, rounding carry and the
original legal-input overflow with typed errors and immediate fixture cleanup;
all four cases pass with no skips. The 19 selected roots pass with race. After
moving four constant-constructor cleanup registrations before error assertions,
both affected consumer roots pass with race again. Vet, molint and incremental
lint also pass after that cleanup at the current source hashes. The independent implementation
review accepts this nonfinal checkpoint. Its maintenance suggestions are applied:
wide literal rows are split by field, the K boundary formula is documented, and
the unused D256 error field/branch is removed. Its 41 cases pass with race again. Molint retains only its two baseline diagnostics. No push or whole-goal completion
is approved; scale-39 and full-function race #29592 remain failed and unwaived.


Three alternating same-binary profile pairs compare 27 original owners against
the three canonical tables plus the retained mixed Mod child. Both groups use
the same current producers and pass. Median summed test-body wall time is
5.706 ms versus 1.979 ms, CPU 6.170 ms versus 2.477 ms, Go allocation 919,816 B
versus 481,312 B, and allocation count 18,750 versus 6,496. These counters exclude
compile/link/init, between-root framework costs and public precision/metadata
additions. They establish a local test-body improvement, not query or whole-CI
speedup. Two additional private mutations independently omit the left/right
D256 admission predicate; each is rejected in VV/SV/VS, while both 41-case
real/cloned controls pass.

## 2026-10-04: full-scale alignment and current-main validation

The Decimal256 alignment defect found during consolidation is tracked in
[#29607](https://github.com/matrixorigin/matrixone/issues/29607). All eight fused
vector Add/Sub preparations now preserve the complete exponent and reuse checked
chunked multiplication beyond 38. Modulo narrowing proves both coefficient fit
and the bounded scale domain, retaining generic intermediate-overflow recovery.
The common one/two-factor paths, NULL ordering, error contracts and scalar paths
remain intact. The four previously failing scale39 result cases now pass.

The fix retains 112 original arithmetic table rows with unchanged inputs and
oracles. New cells cover signed wide alignment, the remaining vector branches,
masked overflow, modulo shapes/zero continuation and late-chunk failure. The
real planner/executor checks bound operand scales, exact coefficients, NULLs and
metadata. SQL BVT shares three rows across grouped operator checks and includes
precision65 success/error, continuation and table teardown. Four private wrong
implementations are rejected by their intended tests.

The branch was rebased onto main `3eab55f2ab`, including the merged #29595
corrections for #29592–#29594. Decimal64 rebase conflicts preserve all 15 main
boundary rows. Existing exact downscale and diagnostic oracles share the current
scale table; failures additionally assert that the original operand is retained.
The five separately reviewed alignment-fix files are byte-identical across rebase.

On this rebased source, complete types/function/plan normal tests pass, as do
complete types/function race tests, including the former #29592 failure. The
exact SQL BVT passes twice on one ready, test-owned CN: 33 checks per run, zero
ignored/abnormal checks, metadata comparison and zero-residue teardown. CN/TN
memory caches are configured at 32MB; Java uses a 256MB heap ceiling. Final
incremental static validation passes for the three changed Go package closures:
vet, molint and configured incremental lint all exit zero.

Pre-rebase matched measurements of the unchanged alignment owners cover 96
Add/Sub and 32 modulo cases, three samples per implementation, with zero Go
allocations and no material common-path regression. These measurements do not
establish whole-query, complete-package CPU or whole-CI improvements. Earlier
failed race evidence is retained as historical evidence; its gate is closed by
validation of the corrected dependency, rather than by repeating the old code.
The wider #29249 task remains ongoing.


## Final decimal and expression contracts (PR #29618)

This section consolidates the PR's added design and evidence. Earlier sections
are the inherited design record. The delivery fixes #29614 and #29615 and relates
to #29249. Validation claims below identify their scope; internal API probes do
not establish SQL reachability or workload throughput.

### Production responsibilities

| Contract | Existing owner and correction | Boundaries retained |
| --- | --- | --- |
| Unsigned rounded division (#29614) | `Div256` reuses `div256TruncQuoRem`, compares the unsigned remainder against the parity-aware half divisor, and propagates quotient carry | Signed callers restore sign; SQL adapters check signed range and retain BigInt only for scale overflow. Ordinary SQL already protected this primitive failure. |
| Declared precision (#29615) | `decimal256BatchArith` rejects a remaining magnitude sign bit after absolute-value normalization, before its precision comparison | Common add/sub/mul/div publication owner; constrained widths 1..75; internal width-76 carrier bypass preserved. No per-kernel precision checks. |
| Bounded rescaling | `ScaleInplace` reuses `Div128`; `ScaleInplace`/`ScaleTruncate` return immediately for zero coefficients | Remove unused `Div128InPlace`; nonzero rounding and error-state semantics retained. Unknown external Go API consumers are outside the compatibility claim. |
| Decimal CAST reduction | Six D128/D256 adapters reuse source `Scale`, then existing target parser at scale zero | Remove six-caller BigInt helper and discarded formatting; growth, explicit CAST clamping, target precision and executor-owned selection retained. |
| Empty widening | `decimal64ToDecimal128Array` returns for zero logical rows | Existing result reset owns length/NULL cleanup; positive-length BCE and constant replication retained. |
| Parameter acquisition | Fixed/string reuse requires complete Type equality; fixed reuse also admits wrapper shape before decoding | Rejected reuse does not decode or mutate. `GenerateFunctionFixedTypeParameter` remains the conversion owner; no new cache, state or buffer. |
| Unary and decimal-prefix admission | Shared bounded mask publication and existing string conversion templates | Preserve row indexes, NULL/error policy and historical binary-input distinction. See [unary design](20261004-unary-selection-execution.md). |

The #29614 primitive regression includes seven independent rounding boundaries,
exact zero errors, half parity and quotient carry. A scan-expression row retains
operand/result metadata and NULL checks. Existing AVG, interpolation and CU
accounting consumers remain covered. The former alternative division algorithm
was retired when main supplied the canonical quotient/remainder owner.

For #29615, legal SQL inputs can produce coefficient -2^255 while carrying
DECIMAL(65,12), which cannot represent it. The shared 11-cell arithmetic precision
holder preserves the old 65-digit product/66-digit overflow cases and adds
minimum-output VV/SV/VS, selection, NULL and width-76 controls. Four distinct
rounded-division precision cases remain separate. The existing planner/executor
root and service BVT check the exact SQL; BVT also checks the last valid and first
invalid declared coefficients, recovery after errors and a typed NULL.

Converted parameter tests exercise D64/F32/F64 constant and flat inputs through
NULL, values, changed payload, nullable flat values and recovery in one frame
slot. Native D128 is the successful-reuse control. Direct rejected reuse leaves
the cached wrapper intact; acquisition replaces it via the existing conversion
owner. The real `plusFn` consumer checks left/right NULL recovery, complete result
type and exact coefficients. D64+F64 ordinary SQL binds to F64: the panic evidence
is at the function API, not a claimed SQL crash.

### Test ownership and retirement

Expected coefficients use literal limbs or an independent integer oracle.
Masks assert exact membership/cardinality and untouched initially masked scratch
outputs. Strict errors assert their category; scratch rollback is not promised.
VV/SV/VS denote the actual physical loops. Scale-route witnesses use real source
scales and distinguish inline success from checked fallback. Pure helpers retain
separate argument/status assertions rather than being replaced by batch success.

The following complete maps retain the old modulo and D64/D128 IntDiv contracts.
In modulo names S/X/Y mean equal/dividend/divisor scaling, 64/W distinguish D128
small/wide divisors, and N/M/P/E/Z mean ordinary, initial mask, permissive zero,
strict error and scalar zero. IntDiv additionally uses H for a wide dividend and
numeric suffixes for actual scale differences.

#### Modulo retirement map

| Old owner | Old children → retained named destinations |
| --- | --- |
| `TestModByZero_NullBehavior` | `(root)` → `128:S64_VV_N`, `64:S_VV_N` |
| `TestNullHandling` | `D128Mod_WithNulls` → `128:X64_VV_M` |
| `TestD64Mod` | `VecVec` → `64:S_VV_N`; `ScalarVec` → `64:S_SV_N`; `VecScalar` → `64:S_VS_N`; `Kernel` → `64:Kernel`; `DiffScale_VecVec` → `64:X_VV_N`; `DiffScale_ScalarVec` → `64:X_SV_N`; `DiffScale_VecScalar` → `64:Y_VS_N` |
| `TestD128Mod` | `VecVec` → `128:S64_VV_N`; `ScalarVec` → `128:S64_SV_N`; `VecScalar` → `128:S64_VS_N`; `Kernel` → `128:Kernel`; `DiffScale_VecVec` → `128:X64_VV_N`; `DiffScale_ScalarVec` → `128:X64_SV_N`; `DiffScale_VecScalar` → `128:Y_VS_N` |
| `TestD128Mod_DiffScale` | `VecVec_Scale1GT` → `128:Y_VV_N`; `VecVec_Scale1LT` → `128:X64_VV_N`; `ScalarVec` → `128:Y_SV_N`; `VecScalar` → `128:Y_VS_N` |
| `TestD64Mod_DiffScale` | `VecVec_Scale1GT` → `64:Y_VV_N`; `VecVec_Scale1LT` → `64:X_VV_N`; `ScalarVec` → `64:Y_SV_N`; `VecScalar` → `64:Y_VS_N` |
| `TestD128Mod_NullPaths` | `SameScale_VecVec_Nulls` → `128:S64_VV_M`; `SameScale_ConstDiv_Nulls` → `128:S64_VS_M`; `DiffScale_VecVec_Nulls` → `128:Y_VV_M`; `DiffScale_ConstDiv_Nulls` → `128:Y_VS_M`; `LargeDivisor_SameScale_Nulls` → `128:SW_VV_M` |
| `TestD64Mod_ScaleXPath` | `VecVec_NoNull` → `64:X_VV_N`; `VecVec_Nulls` → `64:X_VV_M`; `ConstLeft_NoNull` → `64:X_SV_N`; `ConstLeft_Nulls` → `64:X_SV_M`; `ConstRight_NoNull` → `64:X_VS_N`; `ConstRight_Nulls` → `64:X_VS_M` |
| `TestD128Mod_SameScale_LargeDivisors` | `VecVec_LargeBoth` → `128:SW_VV_N`; `ConstDiv_Large` → `128:SW_VS_N`; `ConstDividend_Large` → `128:SW_SV_N`; `VecVec_LargeBoth_Nulls` → `128:SW_VV_M` |
| `TestD128Mod_DiffScale_AllDispatches` | `ConstDividend_DiffScale_Large` → `128:Y_SV_N`; `ConstDivisor_DiffScale_Large` → `128:YW_VS_N`; `VecVec_DiffScale_Large_Nulls` → `128:Y_VV_M` |
| `TestD128Mod_AllDispatches_Extra` | `SameScale_ConstDividend_NoNull` → `128:S64_SV_N`; `SameScale_ConstDividend_Nulls` → `128:S64_SV_M`; `SameScale_ConstDivisor_NoNull` → `128:S64_VS_N`; `SameScale_VecVec_NoNull` → `128:S64_VV_N`; `DiffScale_ConstDividend_NoNull` → `128:Y_SV_N`; `DiffScale_ConstDividend_Nulls` → `128:Y_SV_M`; `DiffScale_ConstDivisor_NoNull` → `128:Y_VS_N` |
| `TestD64Mod_SameScale_AllDispatches` | `ConstLeft_NoNull` → `64:S_SV_N`; `ConstLeft_Nulls` → `64:S_SV_M`; `ConstRight_NoNull` → `64:S_VS_N`; `ConstRight_Nulls` → `64:S_VS_M` |
| `TestD64Mod_ConstPaths_Extra` | `ScaleX_ConstLeft_Nulls` → `64:X_SV_M`; `ScaleX_ConstRight_Nulls` → `64:X_VS_M`; `NotScaleX_ConstLeft_NoNull` → `64:Y_SV_N`; `NotScaleX_ConstLeft_Nulls` → `64:Y_SV_M`; `NotScaleX_ConstRight_Nulls` → `64:Y_VS_M`; `NotScaleX_VecVec_Nulls` → `64:Y_VV_M` |
| `TestMiscEdgePaths` | `D128IntDiv_ZeroConst_ShouldError` → `retained unchanged`; `D256IntDiv_ZeroConst_ShouldError` → `retained unchanged`; `D64Mod_SameScale_VecVec_Nulls` → `64:S_VV_M` |
| `TestD128Mod_DivByZeroPaths` | `VecVec_DiffScale_DivByZero` → `128:XW_VV_N`; `VecVec_DiffScale_DivByZero_WithNull` → `128:XW_VV_P`; `ConstVec_DiffScale_DivByZero` → `128:XW_SV_N`; `VecConst_ZeroDivisor` → `128:X64_VS_Z`; `VecConst_SameScale_ZeroDivisor` → `128:S64_VS_Z`; `VecVec_SameScale_DivByZero` → `128:SW_VV_N` |
| `TestD64Mod_DivByZeroPaths` | `VecVec_SameScale_DivByZero` → `64:S_VV_N`; `VecVec_DiffScale_DivByZero` → `64:X_VV_N`; `VecVec_DiffScale_DivByZero_WithNull` → `64:X_VV_P`; `ConstVec_DivByZero` → `64:S_SV_N`; `VecConst_ZeroDivisor` → `64:S_VS_Z`; `ScaleX_DivByZero` → `64:X_VV_N`; `ScaleX_DivByZero_WithNull` → `64:X_VV_P` |
| `TestD64Mod_ConstAndScalePaths` | `ConstVec_ScaleX` → `64:X_SV_N`; `VecConst_ScaleX` → `64:X_VS_N`; `ConstVec_DiffScale_NotScaleX` → `64:Y_SV_N`; `VecConst_DiffScale_NotScaleX` → `64:Y_VS_N`; `ConstVec_SameScale` → `64:S_SV_N` |
| `TestD128Mod_ConstAndLargeScalePaths` | `ConstVec_DiffScale_ScaleX` → `128:XW_SV_N`; `VecConst_DiffScale_ScaleX` → `128:XW_VS_N`; `ConstVec_SameScale` → `128:SW_SV_N`; `VecConst_SameScale` → `128:SW_VS_N`; `VecConst_SameScale_Large` → `128:SW_VS_N` |
| `TestD64Mod_ScaleXConstPaths` | `ScaleX_VecVec_NoNull` → `64:X_VV_N`; `ScaleX_ConstVec_NoNull` → `64:X_SV_N`; `ScaleX_ConstVec_WithNull` → `64:X_SV_M`; `ScaleX_VecConst_NoNull` → `64:X_VS_N`; `ScaleX_VecConst_WithNull` → `64:X_VS_M`; `ScaleX_VecVec_DivZero_Error` → `64:X_VV_E`; `ScaleX_ConstVec_DivZero_Nullify` → `64:X_SV_N`; `ScaleX_ConstVec_DivZero_Error` → `64:X_SV_E`; `ScaleX_VecConst_Zero_Nullify` → `64:X_VS_Z`; `ScaleX_VecConst_Zero_Error` → `64:X_VS_E`; `SameScale_VecVec_DivZero_Error` → `64:S_VV_E`; `SameScale_ConstVec_DivZero_Error` → `64:S_SV_E`; `SameScale_VecConst_Zero_Error` → `64:S_VS_E` |
| `TestD64Mod_NonScaleXConstPaths` | `NonScaleX_VecVec_NoNull` → `64:Y_VV_N`; `NonScaleX_ConstVec_NoNull` → `64:Y_SV_N`; `NonScaleX_ConstVec_WithNull` → `64:Y_SV_M`; `NonScaleX_VecConst_NoNull` → `64:Y_VS_N`; `NonScaleX_VecConst_WithNull` → `64:Y_VS_M`; `NonScaleX_DivZero_ConstVec_Error` → `64:Y_SV_E`; `NonScaleX_DivZero_VecConst_Error` → `64:Y_VS_E`; `NonScaleX_DivZero_VecConst_Nullify` → `64:Y_VS_Z`; `NonScaleX_DivZero_ConstVec_Nullify` → `64:Y_SV_N`; `NonScaleX_DivZero_VecVec_Error` → `64:Y_VV_E` |
| `TestD128Mod_ConstAndShouldError` | `SameScale_VecVec_DivZero_Error` → `128:S64_VV_E`; `SameScale_ConstVec_DivZero_Error` → `128:S64_SV_E`; `SameScale_VecConst_Zero_Error` → `128:S64_VS_E`; `DiffScale_ConstVec_NoNull` → `128:Y_SV_N`; `DiffScale_ConstVec_WithNull` → `128:Y_SV_M`; `DiffScale_VecConst_NoNull` → `128:Y_VS_N`; `DiffScale_VecConst_WithNull` → `128:Y_VS_M`; `DiffScale_DivZero_ConstVec_Error` → `128:Y_SV_E`; `DiffScale_DivZero_VecConst_Error` → `128:Y_VS_E`; `DiffScale_DivZero_VecConst_Nullify` → `128:Y_VS_Z`; `DiffScale_DivZero_ConstVec_Nullify` → `128:Y_SV_N`; `DiffScale_DivZero_VecVec_Error` → `128:Y_VV_E` |

#### D64/D128 integer-division retirement map

| Old owner | Old children → retained children |
| --- | --- |
| `TestD64IntDiv` | `VecVec` → `64:S_VV_N`, `64:S_VV_E`; `ScalarVec` → `64:S_SV_N`, `64:S_SV_E`; `VecScalar` → `64:S_VS_N`, `64:S_VS_E`; `DivByZero_Null` → `64:S_VV_P`; `DiffScale` → `64:Y2_VV_N` |
| `TestD128IntDiv` | `VecVec` → `128:S_VV_N`, `128:S_VV_E`; `ScalarVec` → `128:S_SV_N`, `128:S_SV_E`; `VecScalar` → `128:S_VS_N`, `128:S_VS_E`; `DivByZero_Null` → `128:S_VV_P`, `128:H_VV_N`; `DiffScale` → `128:Y2_VV_N`; `LargeValues_Fallback` → `128:W_VV_P` |
| `TestD128IntDiv_LargeValues` | `VecVec_Large` → `128:W_VV_P`; `ConstDiv_Large` → `128:W_VS_N`; `ConstDividend_Large` → `128:W_SV_P`; `DiffScale_Large` → `128:Y4W_VV_N`, `128:Y16W_VV_N` |
| `TestD64IntDiv_NotCanInline` | `VecVec_NoNull` → `64:X14_VV_N`; `VecVec_Nulls` → `64:X14_VV_M`; `ConstLeft_NoNull` → `64:X14_SV_N`; `ConstLeft_Nulls` → `64:X14_SV_M`; `ConstRight_NoNull` → `64:X14_VS_N`; `ConstRight_Nulls` → `64:X14_VS_M` |
| `TestD128IntDiv_SameScale_AllDispatches` | `VecVec_NoNull` → `128:S_VV_P`, `128:H_VV_N`; `ConstDividend_Large` → `128:W_SV_P`; `ConstDividend_NoNull` → `128:S_SV_P`, `128:H_SV_N`; `VecVec_Nulls` → `128:S_VV_M`; `VecVec_Large_Nulls` → `128:W_VV_M` |
| `TestD128IntDiv_AllDispatches_Extra` | `SameScale_ConstLeft_NoNull` → `128:S_SV_P`, `128:H_SV_N`; `DiffScale_ConstLeft_NoNull` → `128:Y4_SV_N`; `DiffScale_ConstLeft_Nulls` → `128:Y4_SV_M`; `DiffScale_ConstRight_NoNull` → `128:Y4_VS_N`; `DiffScale_ConstRight_Nulls` → `128:Y4_VS_M`; `DiffScale_VecVec_Nulls` → `128:Y4_VV_M`; `SameScale_ConstLeft_Nulls` → `128:S_SV_M` |
| `TestD64IntDiv_ConstPaths` | `ConstLeft_NoNull` → `64:S_SV_P`; `ConstLeft_Nulls` → `64:S_SV_M`, `64:S_SV_P`; `ConstRight_Nulls` → `64:S_VS_M`; `VecVec_Nulls` → `64:S_VV_M`, `64:S_VV_P` |
| `TestD128IntDiv_DivByZeroPaths` | `VecVec_DivByZero` → `128:W_VV_P`; `ConstVec_DivByZero` → `128:W_SV_P`; `HighScale_ScaleLtScale1` → `128:Y4W_VV_N`, `128:Y16W_VV_N`; `ConstVec_DivByZero_WithNull` → `128:W_SV_M`; `VecConst_ZeroDivisor` → `128:S_VS_Z`, `128:H_VS_N` |
| `TestD64IntDiv_DivByZeroPaths` | `VecVec_DivByZero` → `64:S_VV_P`; `ConstVec_DivByZero` → `64:S_SV_P`; `ConstVec_DivByZero_WithNull` → `64:S_SV_M`, `64:S_SV_P`; `VecVec_DivByZero_WithNull` → `64:S_VV_M`, `64:S_VV_P`; `VecConst_ZeroDivisor` → `64:S_VS_Z`; `HighScale_ScaleLtScale1` → `64:Y16_VV_N`, `64:Y8_VV_N` |
| `TestD64IntDiv_InlineFallbackPaths` | `VecVec_LargeValues` → `64:Y16_VV_N`, `64:Y8_VV_N`; `ConstVec_LargeValues` → `64:Y8_SV_N`, `64:Y4_SV_N`; `VecConst_LargeValues` → `64:Y8_VS_N`, `64:Y4_VS_N`, `64:Y4_VS_Z` |
| `TestD128IntDiv_InlineFallbackPaths` | `VecVec_NoNull` → `128:S_VV_P`, `128:H_VV_N`; `ConstVec_NoNull` → `128:S_SV_P`, `128:H_SV_N`; `VecConst_NoNull` → `128:S_VS_Z`, `128:H_VS_N`; `VecConst_WithNull` → `128:H_VS_M` |
| `TestD128IntDiv_ShouldErrorAndConst` | `SameScale_VecVec_DivZero_Error` → `128:S_VV_N`, `128:S_VV_E`; `SameScale_ConstVec_DivZero_Error` → `128:S_SV_N`, `128:S_SV_E`; `SameScale_VecConst_Zero_Error` → `128:S_VS_N`, `128:S_VS_E`; `DiffScale_ConstVec_NoNull` → `128:Y4_SV_N`; `DiffScale_ConstVec_WithNull` → `128:Y4_SV_M`; `DiffScale_VecConst_NoNull` → `128:Y4_VS_N`; `DiffScale_DivZero_ConstVec_Error` → `128:Y4_SV_E`; `DiffScale_DivZero_VecConst_Error` → `128:Y4_VS_E` |
| `TestD64IntDiv_ShouldErrorPaths` | `VecVec_DivZero_Error` → `64:S_VV_N`, `64:S_VV_E`; `ConstVec_DivZero_Error` → `64:S_SV_N`, `64:S_SV_E`; `VecConst_Zero_Error` → `64:S_VS_N`, `64:S_VS_E` |
| `TestD64IntDiv_ConstDivZeroShouldError` | `ConstVec_DivZero_Error` → `64:S_SV_N`, `64:S_SV_E`; `ConstVec_DivZero_Nullify` → `64:S_SV_P`; `VecConst_Zero_Nullify` → `64:S_VS_Z`; `DiffScale_ConstVec_NoNull` → `64:Y8_SV_N`, `64:Y4_SV_N`; `DiffScale_VecConst_NoNull` → `64:Y8_VS_N`, `64:Y4_VS_N`, `64:Y4_VS_Z`; `DiffScale_VecConst_Zero_Nullify` → `64:Y8_VS_N`, `64:Y4_VS_N`, `64:Y4_VS_Z`; `DiffScale_ConstVec_DivZero_Error` → `64:Y4_SV_E`; `DiffScale_VecConst_Zero_Error` → `64:Y4_VS_E`; `DiffScale_ConstVec_WithNull` → `64:Y4_SV_M` |

#### Other retained families

| Former coverage | Retained owner and independent additions |
| --- | --- |
| Eight direct D256 narrow-dispatch children | All nine strengthened dispatch rows are retained within `TestD256IntDiv`; remove unused `scale` argument. Exact quotients and masks replace success-only checks; add genuine positive adjustment above 19. |
| Six D256 roots, 45 calls/1,261 rows | Existing `TestD256IntDiv`: 42 named calls/109 rows; real narrow/generic VV/SV/VS, signed/truncated results, scale direction, late pre-scan, masks, strict/permissive zero and six nearest inline-rejection controls. Retire `refD256IntDiv`, its sole helper and `hugeD256`. HighMagnitude, widening consumer, ScaleAlignmentOverflow and 50 benchmark bodies remain. |
| Five CAST roots, 27 calls | Shared precision holder initially 33 cells: preserve every input/type/shape/NULL contract; add half-neighbors, 19-digit chunk boundaries, positive carry and negative precision rejection. Admission's 13 Boolean assertions and retyping root remain independent. |
| Six widening-vector and four constant calls | Precision holder expands to 42 cells: retain all metadata/NULL maps and three-row replication; add empty narrowing control. Registered colexec consumer checks both NULL shapes, 0→2→0→2 reuse and cleanup. |
| Two parameter-reuse roots | Shared `TestFunctionParameterReuseTransitions` preserves plain/nullable/constant-NULL, rejected append and fixed length 1 versus string length 0. Complete-type replacement and unchanged rejected-wrapper assertions remain; fixed and string scalar values now participate. Frame-growth identity remains separate. |
| Four metadata-retyping roots, six calls | Preserve temporal subtraction, raw intervals, string add/sub and LEAST/GREATEST initial-result challenges; independently declare full expected Type. |
| Fixed expected-vector construction | Existing generic literal comparator covers 23 fixed OIDs; float64 retains epsilon/NaN policy and float32 exactness. Keep row count, full Type, wrong expectation-type and bounds failures, five empty/short-mask/NULL extent controls. Variable encodings retain vector comparison. |
| Three integer-assignment roots, 83 calls | `TestIntegerAssignmentContracts`: 79 preserved calls; three decimal overflows and float64 uint8 upper tie move into batches with identical offending literals/type/scale/destination. 101 total calls/12 exact errors; five source types add mask/NULL/reuse transitions (22 executions). |
| Eight string and nine JSON width rows | `TestCastStringWidthContracts` retains all 17 representations/literals/flags/VARCHAR(3) policies; six exact error categories and eleven full-metadata successes. Pure UTF8/trailing-space/width helpers remain. |
| Nine assignment-ignore roots, 64 children | Preserve literals, warnings/history, NULL, selection, binary inputs and errors. One runner accepts selection directly; remove nil-only forwarding wrapper. Each call keeps a private Process/warning session. |
| Numeric round/ceil/floor/truncate fixtures | Keep all 11 roots in each of `func_math_complex_test.go` and `func_binary_test.go`, their cases and reuse ordering; replace unused disk FS with existing MemoryFS. Temporal/format/IO tests excluded. |

All fixture-owned vectors/results are released before FileService.Close and
Process.Free, followed by a zero-pool assertion. A Process does not own service
closure. Shared parent fixtures are serial and have no mutable configuration
between children. Warning sessions remain isolated. The arithmetic reuse consumer
also uses the existing MemoryFS fixture instead of building disk services.

The shared result oracle compares complete Type before decoding, including empty
and all-NULL results. Its dedicated corruption checks independently vary OID,
Size, Width, Scale, Charset and notNull. Expected fixed slices are never broadcast;
NULL payload can be absent, but missing non-NULL payload remains an error.

### Validation and measured costs

The durable evidence ledger retains commands, source hashes, selections, terminal
statuses, original snapshots and private mutant controls. Package normal/race,
incremental vet/configured lint and directly affected planner/colexec regressions
cover the final code. Unchanged arithmetic evidence includes full-width integer
oracles and a 1,122-pair boundary challenge. These pairs are not test-node counts.
Molint baseline diagnostics are disclosed; an exit-zero result is not a claim of
zero diagnostics. Unrelated skipped plan children do not count as coverage.

Service BVT runs `decimal_256_literal` twice on one isolated instance with ordinary
expected-result comparison, catalog teardown checks and owned service/port release.
The original 42-check runs did not include #29615's exact SQL; the amended case
adds four boundary statements. Internal unary/API probes remain separate from
service reachability claims.

Historical matched measurements below use the same native binary and alternating
old/new order. Test-family measurements include fixtures/assertions/cleanup and
exclude build/link/init, queueing and GC preparation. Production microbenchmarks
exclude setup unless stated. They establish neither full-CI savings nor SQL/TPCC
throughput gains. Extra regression coverage sometimes costs more.

| Scope / evidence directory | Measured result and limitation |
| --- | --- |
| Modulo test owners / `29249-modulo-owners-20261004` | 119→91 calls; 7,612→307 rows; median wall/CPU/bytes/allocations down 66.8%/63.0%/46.1%/60.7%. D256 production same/different-scale controls -0.8%/+0.2%, zero allocations. |
| D64/D128 IntDiv family / `29249-intdiv-owners-20261004` | 75→82 calls closes real gaps; 3,288→241 rows. Test wall/CPU/bytes/allocations down 60.9%/55.5%/29.4%/50.3%. |
| Initial D256 dispatch table / `29249-d256-intdiv-20261004` | Wall +5.5%, CPU +6.9%, bytes -3.3%, allocations +17.3%; no saving claimed for this intermediate holder. Final family below supersedes it. |
| Final D256 family / `29249-d256-intdiv-20261004/canonical/latest-main` | Test wall/CPU/bytes/allocations down 44.3%/38.9%/24.6%/44.5%. Rounded primitive one-word 12.351→9.317ns, wide 3,171.099→287.595ns; two adapters eliminate 432–448B/15–17 allocations. |
| Zero scaling / `29249-scale-owner-20261004` | Nonzero controls within 1.8%; zero work becomes independent of exponent. At exponent 190,000: in-place 28,969→3.469ns, D128/D256 truncation 30,526.5/69,918.5→5.465/3.264ns. Bounded internal-API controls only. |
| CAST / `29249-cast-contract-20261004` | Six 256-row scale-delta-30 adapters improve 90.3–98.7%; 77–373KB/2,561–17,921 allocations→96B/1. Expanded test family wall/CPU -11.0%/-8.4%, bytes/allocations +13.9%/+10.4%. |
| Empty widening / `29249-empty-cast-20261004` | Expanded tests wall +4.3%, CPU +5.7%, bytes +15.5%, allocations +3.3%; stronger empty/error contracts justify added cost. |
| Parameter holder / `29249-empty-parameter-20261004` | Before this follow-up: wall 40.5→69.2µs, CPU 45.5→80.5µs, bytes 8,392→15,512, allocations 94→159. Excludes converted cells and consumer; not a saving claim. |
| Numeric fixtures / `29249-rounding-fixture-20261004`, `29249-rounding-binary-fixture-20261004` | Wall/CPU/bytes/allocations down 72.4%/68.8%/24.8%/34.5% and 59.6%/56.7%/20.1%/18.9%; avoids 30 and 42 per-service TempDir requests. |
| Fixed-result oracle / `29249-fixed-result-oracle-20261004` | Six CAST cases: wall/CPU -66.6%, bytes -74.5%, allocations -61.5%; variable encodings excluded. |
| Integer assignment / `29249-integer-assignment-family-20261004/final-main-37ba071` | Wall/CPU/bytes/allocations down 48.4%/47.5%/13.5%/11.8%, with added error and reuse coverage. |
| Width / `29249-width-family-20261004` | The 17-case family wall/CPU/bytes/allocations down 86.2%/84.4%/62.3%/63.2%. |
| Assignment-ignore / `29249-assignment-ignore-family-20261004` | Wall/CPU/bytes/allocations down 77.5%/78.0%/38.4%/46.6%, preserving warning isolation. |
| Prefix / `29249-prefix-constant-selection-20261004` | D128 constant wall/CPU -70.4%/-69.6%, flat -5.0%/-4.9%; unary-string controls within 0.4%. Median zero allocations, four noisy samples at 48–80B. |

Private producer mutations challenge exact errors, masked writes, scale fallback,
rounding parity/carry, metadata and inactive callbacks. A mutant failing the new
holder proves that holder's contract; it does not imply the entire old suite
missed the defect. Regression tests do not construct expected values with the
production routine under test.
