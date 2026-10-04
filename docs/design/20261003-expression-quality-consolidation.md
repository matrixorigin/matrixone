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


## 2026-10-04: modulo owner consolidation and magnitude repair

This is a local checkpoint. #29249 remains ongoing. User instructions restrict
current work to local investigation, fixes and tests; no new external review,
GitHub update or push is performed. The requested external model review was
terminated, not approved. Live BVT and production performance remain open for
the SQL-visible magnitude repair recorded locally in commit `08adf47b10`.

### Ownership and independent contracts

The separate repair keeps unsigned magnitude comparison and logical shift
normalization inside `Decimal128.Mod128`. Signed arithmetic callers retain sign
restoration. D256 modulo reuses existing `d256NarrowAllAbsFit64` admission, which
rejects the negative power-of-two divisor whose absolute value exceeds uint64.
No new predicate, execution path, state, allocation or fixture is introduced.
The public planner/executor table now checks minimum-magnitude reduction, its
second correction, negative-power divisors, metadata and NULL propagation.
Normal types/function/plan and incremental vet/molint/lint passed this repair.
A private independent integer oracle passed 90 selected magnitude boundaries.

Batch modulo tests now live in existing `TestD64Mod` and `TestD128Mod` typed
tables. Each named row makes one call. Only `Kernel` uses the live factory;
other rows target the batch owner. Coefficients are reviewed literal limbs:
production Mod/Scale/Minus/parse/format methods do not construct expectations.
Each row owns fresh output and bitmap state. Assertions cover full coefficients,
exact NULL count/membership and untouched initially masked output. New NULL
payload is unspecified. Strict errors assert `ErrDivByZero` and the unchanged
bitmap, without promising rollback of scratch results.

`S` is same scale; `X`/`Y` scale dividend/divisor; `64`/`W` distinguish D128
small/wide divisor admission. `N` starts with an empty bitmap, `M` proves strict
masked success, `P` combines old masks and evaluated-zero continuation, `E`/`Z`
prove strict/permissive zero. VV/SV/VS name the real physical loops. The raw
strict scalar-zero precheck remains distinct from public all-masked admission.

Original generated D128 alignment remained below 2^95, so it could not prove
checked D128 overflow. Retained literal rows preserve successful one-factor
alignment, signs, wide divisors and nonzero high remainders. Factor differences
19/20/38, second-factor carry, signed-range rejection and fallback from original
operands now have separate exact witnesses. Strict/permissive policies have
identical nonzero arithmetic; their distinct zero outcomes are tested separately.
All 50 benchmark bodies and shared generators remain byte-identical. The two
mixed IntDiv children and preceding shared RNG consumption are unchanged.

### Complete retirement map

The following maps every old batch child before deletion. `64:` and `128:`
refer to the retained typed owners above. The two direct helper owners remain
separate, with strengthened complete-limb assertions; their argument/status
contracts are not replaced by batch success assertions.

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

### Matched cost and validation scope

In one native test binary with identical production/native inputs, eight samples
per mode alternate old/new execution order. The scope includes the same unchanged
mixed IntDiv and direct helper owners. Measurements sum owner function bodies,
including their child assertions/harness; outer root/sample harness, GC
preconditioning, build/link/init and queue time are excluded.

| Median | Before | After | Reduction |
| --- | ---: | ---: | ---: |
| Test body wall time | 3.853 ms | 1.281 ms | 66.8% |
| Process CPU within measured bodies | 4.338 ms | 1.606 ms | 63.0% |
| Go allocated bytes | 638,072 | 344,176 | 46.1% |
| Go allocations | 12,895.5 | 5,064.5 | 60.7% |

Batch invocations decrease from 119 to 91 and output rows from 7,612 to 307,
with stronger independent oracles. This shared-host measurement establishes the
mapped test-body improvement, not whole-package, query or CI speedup. Final function-package normal validation passes 2,054 roots and 8,231 children.
Complete types/function race passes 2,269 roots and 8,980 children; the public
planner/executor race owner passes one root and 15 children. Final incremental
vet, molint and configured lint all exit zero. Unchanged types/plan normal and
static evidence from the isolated magnitude repair is reused.

Three task-private producer mutations—masked output clobber, wrong strict-zero
error class and dropped high remainder—each survive all 21 old owners. The new
typed tables reject all three with the intended runtime assertions, without
build failure or panic. Matched old/new controls also pass. The earlier direct
helper oracle mutation evidence remains valid because those helper bodies,
inputs and full-coefficient assertions are unchanged by this consolidation.

This proves the local test-consolidation checkpoint. The SQL-visible repair still
requires live BVT and production-performance evidence; external review and
publication remain constrained by the user's local-only instruction. The full
#29249 objective is not complete.


The local production benchmark checkpoint uses unchanged existing D256 modulo
same-scale/different-scale bodies and the unchanged D128 control, each over 8,192
rows. Before/after/after/before ordering supplies six measurements per mode/name.
Median D256 same-scale batch time changes from 61,759.5 to 61,261.5 ns (-0.8%);
different-scale changes from 61,563 to 61,708 ns (+0.2%). The unchanged D128
control changes from 40,352 to 40,101 ns (-0.6%). All samples report zero bytes
and allocations per batch. This small shared-host experiment shows no material
common-path regression in those workloads; it does not establish a query/TPCC
speedup. Live SQL BVT and requested external review remain unperformed under the
local-only instruction. No new PR, PR update or push has been made for this repair.


## IntDiv inventory and unsigned division owner correction

The next batch inventory finds 14 D64/D128 IntDiv roots, 75 children and 912
function-span lines. All 75 kernel calls pass unchanged. An independent integer
oracle agrees with 3,108 representable result rows, but no retained input rejects
checked dividend scaling. Several names incorrectly claim inline rejection or
non-inline execution. This is an inventory checkpoint, not permission to delete
those owners; their complete retirement mapping and cost comparison remain open.

The boundary challenge additionally proves a public DECIMAL(65) DIV failure:
positive 2^127-1 and 2^127-2 divided by negative 2^127 return an internal quotient
correction error rather than zero. The narrowed D128 quotient/remainder owner
uses signed comparisons on unsigned absolute magnitudes. Direct equal 2^127
magnitudes also panic in the original normalized estimator. The ordinary `/`
zero-result SQL control passes before repair; no SQL-visible panic is claimed.

The correction stays in `div128TruncQuoRem` and `Div128`'s half-up decision.
Comparison, normalization, product and remainder consistently use unsigned
limbs. A logical half dividend keeps bits.Div64's high limb below its normalized
divisor. This produces the same quotient estimate because the original
normalized effective divisor is even. The wide divisor bounds the truncated
quotient to one limb. Its full product retains an overflow limb; a high estimate
is corrected by subtracting the divisor once, rather than repeating signed
multiplication. Remainder must still be below the divisor. The rounding threshold
uses ceil(y/2) without signed shifts or doubling, and carries into the quotient's
high limb. Zero error behavior and the existing small-divisor path are preserved.
No new kernel, fallback, conversion owner or runtime allocation is introduced.
The historical IntDiv diagnostic compatibility owner remains live and unchanged.

Seven boundary rows extend the existing independent math/big half-up owner,
covering the magnitude minimum, half-threshold neighbors, wide-divisor correction
and rounded quotient carry. Two literal SQL result rows extend the existing
planner/executor table rather than adding another fixture owner. That table now
checks both int64 and Decimal256 results with exact values, type/scale/width and
NULL membership. Cleanup is registered immediately for the process, input and
executor. A private 4,096-sample full-width probe independently checks quotient,
remainder and rounding; it is evidence, not a new delivery test suite.

This correction does not complete IntDiv consolidation or the full #29249 goal.
The local issue draft and terminal evidence live under
`/home/xupeng/matrixone-qa-evidence/29249-intdiv-owners-20261004`.
Live-server BVT, external review and publication remain unperformed under the
user's local-only instruction.


The repaired production owner passes complete types/function/plan normal tests
and complete types/function race tests. The final shared SQL table separately
passes normal and race validation (two roots, 17 children); final source hashes
remain unchanged across those checks. The broader normal plan suite has two
unrelated pre-existing skipped children; neither is counted as coverage. Vet,
molint and configured incremental lint pass for all three affected packages,
with the final plan test adjustment checked again. Two partial-fix mutations
(signed half threshold and signed remainder guard) survive the previous
independent boundary table but are rejected by the extended table's actual
runtime assertions. No build failure or panic is used for those mutation results.

A private same-native-binary microbenchmark alternates before/after order with
six samples per mode for each fixed operand pair, checking exact quotient and
remainder before timing. Median small-divisor cost is 4.397 → 4.398 ns; ordinary
wide-divisor cost is 13.825 → 5.052 ns; below-divisor cost is 2.898 → 1.812 ns.
The high-estimate correction sample falls from 3,752 → 6.221 ns and from 1,096
bytes/47 allocations to zero: signed overflow diagnostics are no longer created
while correcting an otherwise valid unsigned product. Other samples allocate
zero bytes in both modes. This shared-host primitive measurement excludes
build/link/init and establishes no SQL, package or CI speedup. All 50 existing
batch benchmarks and the 14 inventoried IntDiv test owners remain byte-identical.


## IntDiv typed contract consolidation

The complete map below precedes retirement of the 14 inventoried owners (75
children). The existing D64/D128 roots retain literal, signed int64 quotients,
exact error categories, exact NULL count/membership and masked-output sentinels.
Raw kernels own the selected arithmetic/mask/zero contracts; SQL metadata,
selection, diagnostics and memory lifecycle remain with the existing public
owners in `div_issue_test.go` and the planner/executor table. D256, direct helper
and mixed IntDiv owners remain unchanged. The private `refD128IntDiv` oracle is
retired with all its callers; the shared D256 reference and benchmark input
conversions remain live and unchanged.

Rows use S for equal scales, X for dividend scaling, Y for divisor scaling,
W for wide divisors and H for wide dividends with one-limb divisors. VV/SV/VS
name the physical input shapes; N/P/M/E/Z distinguish ordinary values,
permissive zero, initial masks, strict errors and scalar zero admission. Numeric
suffixes record the actual scale difference. Previous inaccurate branch labels
are replaced by actual runtime route/range evidence, not preserved as truth.

Nearest checked-scaling controls use K=floor((2^127-1)/10^18): -K scales inline,
-(K+1) rejects scaling and still returns -2^63 after the D256 fallback with
divisor 2^64-1. VV, SV and VS all exercise that real fallback, including masking
an otherwise overflowing row. Other independent gaps cover late wide-divisor
admission, signed minimum coefficients, BIGINT overflow, positive non-inline
scale adjustment and strict masked-zero versus scalar-zero admission. No row
uses production arithmetic to derive its expected quotient. Error paths do not
assert rollback of scratch results. Existing mask and newly admitted NULLs are
combined in the same permissive rows so bitmap interaction is observable.

`64:` and `128:` below refer to children of the retained typed roots. Multiple
destinations preserve different outcomes formerly present within one route.

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


The candidate contains 36 D64 and 46 D128 calls over 241 result rows, replacing
75 calls over 3,288 rows. The seven additional calls close genuine gaps above;
they are not a claim of reduced batch invocation count. Both retained function
bodies total 176 lines, replacing 925 owner/helper lines. All 50 benchmark
bodies and every non-inventoried surviving function body remain byte-identical. The
now-unused `largeD128` fixture retires with its complete caller set; it adds six
retired lines to the 925 owner/helper lines above. Validation
and matched test-body cost evidence are required before claiming this stage.


Validation passes the full function package normal and race suites before the
final fixture retirement/masked-input adjustment. The final affected typed
owners separately pass normal and race (two roots, 82 children), with unchanged
other function bodies verified for evidence reuse. Final function-package vet,
molint and configured incremental lint exit zero. A private independent integer
probe agrees with all 163 evaluated representable quotient rows and identifies
six actual checked-scaling rejection rows, replacing zero in the old inventory.
The final source hashes remain unchanged across validation and mutation runs.

Three real producer mutations—masked output clobber, incorrect strict-zero
error category and false inline success after rejected scaling—each pass all
14 old mapped owners and fail the new table's intended runtime assertions.
There are no build failures or panics in that matrix. The false-inline mutation
is already rejected by existing direct-helper tests; the new batch coverage
adds the missing end-to-end arithmetic/result-conversion contract, rather than
claiming the whole old test suite missed the helper status error.

Eight alternately ordered samples per mode run in one native binary using
identical production code. Median summed test-body wall time falls from 2.275
to 0.889 ms (60.9%); process CPU within measured bodies from 2,525 to 1,124 us
(55.5%); allocated bytes from 330,112 to 232,944 (29.4%); allocations from 5,930
to 2,945 (50.3%). The measured bodies include child assertions/harness and all
new gaps; outer root/sample harness, GC preconditioning, build/link/init and
queue time are excluded. This proves improvement for the mapped test bodies,
not whole-package or CI speedup. Production/SQL bodies are unchanged at this
checkpoint, so their still-valid preceding evidence is reused; no new BVT or
external publication is claimed. The broader #29249 goal remains incomplete.


## Scale owner cleanup and bounded zero coefficient work

`Div128InPlace` has one repository caller: `Decimal128.ScaleInplace`, passing
only a temporary one-limb power of ten after chunk reduction (exponent 1..19).
The divisor's scratch mutation cannot escape that temporary; its wide-divisor
branch has no repository consumer. ScaleInplace now uses the existing rounded
`Div128` owner and the duplicated exported method is removed. This removes a
kernel API with no surviving repository callers, not a SQL/protocol API; no
compatibility claim is made for unknown external Go consumers.

The value `Scale` owners already treat zero as an identity for every exponent.
That existing invariant now also applies at entry to D128 ScaleInplace and
D128/D256 ScaleTruncate. Zero does not enter the multiplier chunk loop. All
nonzero paths, result rounding and error-state rules remain unchanged. Existing
multi-chunk rounding, minimum coefficient and typed overflow owners remain
intact. Two signed exponent extremes extend the existing boundary owner with
exact zero coefficients for the three formerly inconsistent entry points;
redundant ordinary zero exponent permutations are omitted. A private 50-input
old/new comparison checks in-place result/error state and D128 truncation;
independent integer half-up checks cover the successful scale-down results.

Full types/function/plan normal tests and full types/function race tests pass.
The full run has the same two unrelated skipped plan children, not counted as
coverage. All three affected packages pass incremental vet, molint and lint.
The final two-extreme boundary table is checked separately after trimming the
ordinary zero duplicates. This internal identity/owner cleanup changes no SQL
result, error or metadata contract, so an additional live BVT would repeat the
internal claim; earlier SQL-visible repairs still have their own open BVT gate.

A private same-binary, alternately ordered microbenchmark provides six samples
per mode with exact final result assertions and zero allocations throughout.
Median nonzero in-place scale-down changes 10.265 → 10.360 ns (+0.9%); nonzero
D128 truncation 9.628 → 9.453 ns (-1.8%); D256 truncation 17.460 → 17.365 ns
(-0.5%). No material common-path change is established by these small samples.
For bounded positive exponents 190 and 190,000, zero in-place work changes
34.015 → 3.374 ns and 28,969 → 3.469 ns. At 190,000, zero D128/D256 truncation
changes 30,526.5 → 5.465 ns and 69,918.5 → 3.264 ns. These deliberately bounded
internal-API controls demonstrate removal of exponent-proportional zero work;
they are not SQL, query or CI performance claims. No production benchmark or
server fixture is added to the delivered tests.

## D256 integer division dispatch contract checkpoint

The integer result has no result scale. `d256IntDivViaD128` never reads its
`scale` argument: one production caller and fifteen direct test calls only pass
it through the signature. Remove that argument and the caller's constant-zero
local; retain the adjustment, source scales and initial mask snapshot. No
arithmetic, admission, result, error or NULL publication rule changes.

Before replacement, eight `TestD256IntDivViaD128_AllPaths` children are mapped
individually in `dispatch-retirement-ledger.json`. Their three shapes, mask
states and zero/negative adjustments remain; exact literal quotients and
untouched masked scratch slots replace `NoError` alone. The former comment
claimed positive adjustment above 19, but no old call used it. One additional
named row supplies that missing cell with a 10^20 divisor and representable
signed quotients. Input slices are shared only after proving the kernels read
coefficients by value; each child gets fresh result and NULL state. Cardinality
plus the sole expected member proves the whole NULL bitmap. This owner shrinks
95 → 56 lines and 128 → 27 processed rows. All fifty benchmark bodies and the
independent scale-alignment overflow owner are unchanged.

Seven D256 roots/34 children pass normally and with race; the full function
package passes 2,042 roots/8,239 children. Incremental vet, molint and lint pass.
A private independent signed-coefficient model checks all 23 evaluated results.
Two producer mutants (wrong quotient and masked-slot overwrite) pass the old
mapped owner, then fail real final assertions; this is not a claim about the
entire old suite. Evidence lives in `29249-d256-intdiv-20261004`.

Eight alternating same-binary samples measure owner bodies, child fixtures and
assertions, excluding outer roots, GC preconditioning, build/link/init and
queueing. Median wall time is 83.903 → 88.509 µs (+5.5%), process CPU 108 →
115.5 µs (+6.9%), bytes 25,216 → 24,376 (-3.3%), allocations 234 → 274.5
(+17.3%). These small measurements establish no runtime saving. An initial
version cost more and was simplified without weakening its oracle. The extra
semantic cell and exact assertions have a disclosed cost; consolidate the
remaining duplicate D256 owners before claiming an overall improvement. No
server/BVT is needed for unused internal argument retirement and UT-only work;
prior SQL repairs retain their separate open BVT gate. #29249 remains active.


## Current goal: historical bug families and independent repair challenges

The user changed the priority: group previous bugs by shared ownership and
failure mechanism, reorganize their unit tests, independently challenge the
repairs, identify new problems, file issues, repair the common owner, validate,
and add minimal independent regressions. Test consolidation and cost reduction
support this lifecycle. A stage is not completion of #29249. The unit of test
redesign is a class of behavior, rather than a test per issue.

The active decimal family spans signed values versus unsigned coefficients,
scale admission, rounded/truncated division and remainder, narrowing bounds,
and vector NULL/zero/error policy. Existing D64/D128 consolidation and scale
cleanup are earlier parts of this family; D256 dispatch consolidation remains
open and requires a renewed old-to-retained map after the upstream changes.

## Latest-main D256 rounded consumer checkpoint

With the explicitly permitted Git fetch, rebase the seven unpublished commits
onto main c3fbe9ce744aa877ec944c86258916fc5e45b63b. Preserve the upstream
quotient/remainder owner and its floor, modulo, format and high-magnitude tests.
Do not reapply the saved alternate pre-rebase algorithm: latest main already
fixes the old DIV/modulo counterexamples. A pre-rebase failure is not evidence
of an outstanding current-main defect.

A separate current-baseline native assertion proves that unsigned rounded
Div256 returns zero for 3*10^76 / 10^64 instead of 3*10^12. Rounded division
still duplicates the former doubled-dividend/signed-comparison algorithm.
Ordinary SQL division previously protected this domain with an allocation-heavy
headroom fallback, so this primitive failure does not establish wrong SQL
results for that protected adapter. Reuse the existing div256TruncQuoRem owner
byte-for-byte. Compare the remainder with floor(divisor/2) using unsigned borrow
and divisor parity, then propagate quotient carry with unsigned addition. No
new division algorithm, state or helper is introduced. Rounding cannot overflow
the unsigned quotient: divisor one has remainder zero; larger divisors leave at
least one quotient bit of capacity.

Remove the private truncate forwarding wrapper with its sole caller. The SQL
adapter now admits all fixed-width scale-adjusted magnitudes; retain its signed
quotient range check and the bounded BigInt path for actual scale overflow.
Trace the other rounded consumers (Decimal128 wide fallback, Decimal256.Div,
AVG finalization, linear interpolation and CU accounting); their sign/scale
responsibilities remain at their existing callers.

Seven distinct rounding boundary cells plus exact zero-error/result assertions
extend the existing unsigned owner rather than introducing a new test root or
repeating its floor matrix. One scan-expression row reuses the binder/executor
fixture for two values and NULL, output type/width/scale and operand scales.
Existing cases are retained. Production changes add 35/delete 50 lines (net -15),
unit/public tests add 35 lines, and documentation is accounted separately.

All seven validation steps finish with zero exits and unchanged source hashes:
independent 2,048 full-width pairs, vet, molint, incremental lint, full normal
(types/function/plan), full race (types/function), and public-expression race.
Normal tests select 219/2,044/3,490 roots; race selects 219/2,043 roots (one
existing race-mode exclusion). Existing AVG, interpolation and accounting
consumer tests also pass. Half-parity and top-word carry mutants fail real
assertions in the new boundaries, without build failure or panic.

Six alternating samples per mode in the same binary measure the primitive and
adapter separately. Median one-word rounding is 12.351 to 9.317 ns; wide
rounding is 3,171.099 to 287.595 ns. Truncation/below-divisor controls differ by
less than 1%, with zero allocations for every primitive sample. Two adapter
controls change 565.583 to 184.713 ns (432 B/15 allocations to zero) and 578.264
to 233.388 ns (448 B/17 allocations to zero). These fixed-input microbenchmarks
exclude build/fixture/queueing cost; no end-to-end SQL, CI savings or broad
workload claim follows. Evidence, current-main issue draft and new-family
inventory live in `29249-d256-intdiv-20261004/canonical/latest-main`.

The real-service BVT gate for earlier SQL-visible repairs remains open. The new
rounded-owner checkpoint preserves protected ordinary SQL results while
removing the allocation-heavy fallback; its exact primitive and real expression
consumer evidence do not substitute for the separate earlier BVT obligation.
Independent gpt-6.1-sol xhigh stage review approves this checkpoint with no
blockers after checking the final source hashes, independent models/mutations,
shared owner, callers, test purpose and cost limits. Family consolidation and
the earlier real-service BVT obligation remain open. Publication was pending at this checkpoint. The subsequent explicit user
permission to file sufficiently proven issues authorizes issue creation; the
verified primitive defect is now recorded in issue #29614. Push/PR publication
remains unperformed.


## D256 integer division family consolidation

Review the class, not one issue at a time. Fresh current-baseline observation
maps 45 actual D256 calls/1,261 processed rows before deleting any owner. Six
weak or duplicate roots become the existing TestD256IntDiv contract table with
42 named calls/109 rows. All nine earlier exact dispatch rows remain, along
with real narrow/generic VV/SV/VS admission, sign/truncation, two-limb divisors,
positive/negative scale adjustments, late generic pre-scan, combined initial
masks and new zero NULLs, and strict error categories. The initially large
random fixture actually selected narrow dispatch; the replacement names and
coefficients state the path they exercise. Fifteen synthetic direct narrow
calls with adjustment six but source scales 4/4 become real batch calls with
source scales 0/6, preserving their mechanism without bypassing admission.

Six neighboring inline-rejection cells distinguish a representable negative
MinInt64 quotient from its unrepresentable positive counterpart for all three
shapes. The independently derived coefficient is ceil(2^127/10); scaling it by
ten yields 2^127+2, rejected by the signed128 inline admission. Division by
MaxUint64 truncates to 2^63. Add generic masks and strict vector masked-zero
versus scalar-zero all-masked rejection as separate policies. Expected values
are literals, not calls to a decimal producer. Cardinality plus all expected
NULL members proves the bitmap; initial masked outputs keep their sentinels.
Do not demand rollback of scratch results when an error is returned.

HighMagnitude (including its D128 widening consumer) and ScaleAlignmentOverflow
remain byte-identical, as do all fifty benchmark bodies. Retire refD256IntDiv,
its sole reference-owner helper and hugeD256 with their only callers. The final
Go test delta is +101/-415 lines (net -314); no production code changes in this
stage. Documentation additions and private evidence are accounted separately.
The named retirement ledger explains each old call, independent retained
contract, metadata correction and strengthened oracle before removal.

All nine serial gates are terminal with unchanged source hashes: independent
math/big observer, matched family costs, truncation-to-rounding and masked-write
mutants, incremental vet/molint/lint, full function normal and race. The full
D256 subset now exercises 53 calls/128 rows and independently checks 80 success
results, of which the central table supplies 64. Full normal selects 2,039
roots/8,250 children; race selects 2,038/8,250. Both mutants fail actual final
assertions, without build failure or panic. Their failure proves retained
contracts, not that the complete old suite would let both mutations survive.
An initial focused compile exposed the now-unused slices import; cleanup and
successful rerun supersede that build failure, whose evidence is retained.

Eight alternating samples in one binary compare the six frozen old owner
bodies and their helpers with the retained class, under the same production
implementation. Medians: wall 838.255 to 467.225 microseconds (-44.3%); process
CPU 963 to 588 microseconds (-38.9%); bytes 164,560 to 124,144 (-24.6%);
allocations 2,708 to 1,502.5 (-44.5%). Measurements include owner children,
fixtures, assertions and consolidation of the six former root invocations;
they exclude build/init/queueing, outer measurement groups and GC preconditioning.
They support a test-family cost reduction, not whole-package/CI or SQL speedup.

Independent gpt-6.1-sol xhigh review APPROVE confirms the map, exact boundary
arithmetic, protected owners/benchmarks, caller retirement, source hashes,
mutants and cost provenance. Issue #29614 covers the prior production repair;
this test-only consolidation needs no separate product-bug issue or extra
server BVT. Existing SQL repairs still retain their distinct real-service BVT
gate. The broader historical-bug-family challenge remains active.


## Historical decimal repair class: SQL consumers and precision policy

Independent native SQL-expression challenges cover 18 constant/scan cases from
previous decimal division/scale-reduction failure mechanisms: narrow widening,
wide control, D128/D256 source widths, positive/negative half-neighbors, ROUND
and CAST, two scan values and NULL. Results are decoded from coefficient words
with independent math/big rational arithmetic; no production Format or decimal
producer computes the oracle. All corrected cases pass. Three initial failures
were invalid historical expectations: current default div_precision_increment
is four, so scale-zero division already rounds to 0.1235 before outer CAST(65,6).
Retain that control; never report this policy change as a fresh arithmetic bug.

Source scale three makes division scale seven and adjustment 22; the numerator
then exceeds signed128. Private route observation proves d128DivOneToD256 runs
for both constant and scan variants, while the actual wide control does not use
that route. The intermediate coefficient 1234568 reduces once more on the
explicit outer CAST, yielding 123457 at scale six. The minimum missing consumer
contract is added as one row to the existing scan-expression owner, retaining
values, outer scale/type, inner scale, target operand metadata and NULL. Do not
deliver all 18 discovery cases as overlapping new test roots. Existing signed,
half-boundary and Scale owners retain their distinct primitive contracts.

Six validation gates finish with zero exits and unchanged relevant hashes:
focused consumer, vet/molint/incremental lint, full plan normal and focused
public-path race. gpt-6.1-sol xhigh independently approves the corrected
historical oracle, actual widening path, one-row regression and evidence.
Production delta is zero, test delta is one table row; report additions are
separate. Two existing unrelated skipped plan subcases are not claimed as
executed coverage. This remains in-process SQL evidence, not server BVT.


## Shared declared-precision boundary: signed minimum (2026-10-04)

This stage follows the amended historical-bug-family goal: organize tests by
shared ownership and failure mechanism, challenge the fixes, file proven new
issues, repair their common owner, and retain minimal independent regressions.

- Current-main baseline `c3fbe9ce744aa877ec944c86258916fc5e45b63b` reproduces
  [#29615](https://github.com/matrixorigin/matrixone/issues/29615): two legal
  operands produce `MinInt256` under `DECIMAL(65,12)`, leaking a 77-digit
  coefficient. The nearest negative coefficient and positive counterpart reject.
- `decimal256BatchArith` owns post-kernel precision enforcement for addition,
  subtraction, multiplication and division. The absolute magnitude of the
  minimum signed coefficient retains its high bit; signed `Less` mistakenly
  treats it as smaller than a positive precision limit. After normalization,
  reject a remaining sign bit before the existing comparison. Constrained
  widths 1..75 have positive limits below 2^255; internal width-76 bypass stays.
  No new state, helper, allocation, per-kernel check or execution path is added.
- Replace the existing multiplication-only precision holder with one shared
  arithmetic precision holder. Preserve its exact 65-digit product and 66-digit
  overflow oracles; add signed precision boundaries, legal minimum outputs
  through VV/SV/VS, NULL/filtered rows, and a raw four-limb width-76 compatibility
  control. The four division precision cases remain: their scale and rounding
  contracts are distinct. One public binder/executor regression proves SQL
  reachability; the discovery matrix is not copied wholesale into delivery.
- The final holder uses 11 cells with at most two rows, existing function
  fixtures, per-child vector cleanup, and one shared process with cleanup. Exact
  literal outputs, metadata, NULL bits and `ErrOutOfRange` are checked. No sleep,
  random scheduling, server or extra framework is needed. Before the production
  fix, the three minimum-output shape cells failed with a missing error while
  all seven original/new controls passed. This is direct sensitivity evidence.
- D128 add/sub/multiply SQL coercion widens from original operand domains before
  choosing its kernel when the aligned result exceeds precision 38; a legal
  SQL result reaching the physical signed minimum does not remain in that
  carrier. This does not claim unrelated D128 paths have been fully audited.
- Evidence is retained under `canonical/latest-main/logical-range`: baseline
  SQL terminal/log, `precision-family-map.json`, before/after regression logs,
  and `regression-validation-terminal.json`. Whole-thread server BVT and broader
  historical-family closure remain separate open requirements.

Final stage validation: all seven serial checks terminated with exit 0 and
unchanged source hashes: focused family/public tests, incremental vet, molint,
incremental lint, full function/plan normal tests, full function race, and
public-path race. Lint reports zero new issues. Molint has six existing
diagnostics in five source files unchanged from the verified main base;
its terminal is zero, and no new diagnostic is introduced. Plan normal has
two existing skipped children, which are not claimed as executed coverage.
6.1-sol xhigh approved the design and delivered code; final evidence selection
and source binding are retained separately from broader uncompleted QA/BVT.

## Decimal CAST contracts and rescale ownership (2026-10-04)

Ordinary/implicit decimal CAST precision and rescaling form one contract family;
issue numbers and individual adapter names do not define separate fixtures.
Six D128/D256 reduction adapters now reuse the source carrier's existing `Scale`
owner, then format the reduced coefficient and use the existing target precision
parser at scale zero. Remove the sole six-caller BigInt rounding helper and four
discarded formats. Growth, target precision/error categories and explicit SQL
CAST's separate clamp entry remain unchanged. Selection remains owned by the
expression executor; no second masking implementation is introduced.

Five repeated test roots (27 calls) become one 33-cell holder with shared process
cleanup and per-child vector cleanup. The retirement ledger preserves every old
input, metadata, oracle, constant shape and NULL contract. Seven old reduction
cells also cover exact half/below-half, negative and NULL rows, including 19-digit
chunk boundaries; six added cells cover carry/nearest-valid and negative precision
rejection previously hidden behind the first error. Admission's 13 exact Boolean
assertions and the independent source-retyping root retain separate ownership;
the growth benchmark body is unchanged. Expected coefficients are exact literals
or raw limbs, not computed by the production scaling owner.

One registered colexec consumer covers selected overflow suppression, NULL,
unmasked precision error, shrinking-batch reuse, complete metadata and zero
remaining pool bytes. It exercises the real ordinary CAST; no fake evaluator,
service, sleep or new test framework is required.

Evidence: `29249-cast-contract-20261004` retains the 27-call retirement ledger,
33-cell selection audit, original source snapshots, independent discovery probes,
seven-gate terminal/hash binding and same-condition measurements. All seven
serial gates passed: focused tests, vet, molint, incremental lint, full function/
plan/colexec normal tests, full function race, and public consumer race. Lint has
zero new issues; molint diagnostics are in seven unchanged base source files.
Two pre-existing plan skips are excluded from coverage claims.

Six alternating paired microbenchmark samples use actual `NewCast`, prebuilt
256-row vectors and scale delta 30. Median adapter time falls 90.3–98.7%; each
batch now uses 96 bytes/one allocation instead of 77–373 KB/2,561–17,921 allocations.
This is not SQL-throughput, explicit-CAST or short-delta evidence. In the same
binary, eight alternating old/new test-family samples include all added function-family coverage, excluding the
new colexec consumer:
wall time falls 11.0%, CPU 8.4%, but allocated bytes rise 13.9% and allocations
10.4%. The extra precision/error coverage has a measured cost; fixture sharing
alone is not claimed to reduce all resource dimensions. Build/link/startup and
GC preparation are excluded from those test-body comparisons.
The final holder kills a private mutant that parses the reduced D128→D64
coefficient using target scale: both the original reduction cell and positive
carry rejection fail their exact assertions. Permanent source remains unchanged.

## Widening batch row-domain contracts (2026-10-04)

Rebase the existing local series onto freshly fetched main `a3aece1894` before
this stage. The pre-fix D64→D128 batch owner is byte-identical to that main revision.
A registered ColumnRef CAST on a zero-row batch reaches both same-scale BCE
paths and reads index -1. A physically empty normal vector reproduces it too. Separately, a stored
constant coefficient 10000000000 at (18,2), narrowed to (10,2), reports precision
failure even when no row is requested. Both NULL shapes and the constant error
were demonstrated before the fix; these are batch/expression API findings, not
a claimed server SQL reproducer.

Return immediately for zero length at `decimal64ToDecimal128Array`. This keeps
positive-length BCE and constant replication, while existing result reset owns
length/NULL cleanup. Do not add generic executor/CAST short-circuit policy or
another validation/storage path. Remove the adjacent obsolete comments claiming
D128 scale is below 18 and its now-optimized conversion is temporary/too slow.
Physically empty constants are handled as scalar NULL by `IsConstNull`;
that distinct representation contract is explicitly outside this checkpoint.

Map all six old vector calls into the existing precision holder (now 42 cells),
retaining their metadata, inputs and NULL maps, including the old misleadingly
named negative case that actually has no NULL. Replace four verbose constant
cases with literal-limb three-row replication cases, adding one empty narrowing
failure-boundary control. A real registered consumer checks both NULL shapes,
0→2→0→2 reuse, complete metadata, fresh payload after NULL reset, and pool cleanup.
Full bitmap emptiness subsumes per-row non-NULL checks; comparable whole-Type
checks retain every metadata field without reflection. No extra fixture framework
is introduced. Test code falls by 77 lines; implementation grows by two net lines.

Evidence is under `29249-empty-cast-20261004`: fresh-main replay, complete ten-call
retirement ledger, before/after logs and source-bound validation terminals. Seven
initial gates pass, including full function/plan/colexec normal, full function
race and public consumer race. The subsequent assertion-only cleanup changes no
production or consumer byte; five focused normal/race and incremental static
gates bind the final test source. Reuse the broader gates under that explicit
semantic freshness argument. Two pre-existing plan skips are not executed
coverage; lint has zero new issues and molint diagnoses only unchanged files.

Eight alternating paired same-binary samples include all function-family
coverage and exclude the new colexec consumer, build/link and GC preparation.
Final medians observe wall +4.3%, CPU +5.7%, allocated bytes +15.5% and allocations
+3.3% versus the old family. This stage does not claim a resource reduction;
the newly verified empty/error and metadata contracts justify the measured cost.
The issue draft is preserved; GitHub creation returned 403 (integration access),
so no published issue is claimed. Current-head service BVT remains open.


## Parameter acquisition and reuse contract (2026-10-04)

Repeated acquisition must preserve the complete effective type, source, values,
and NULL semantics of initial acquisition. Before repair, a D64 constant acquired
as D128 succeeds once and panics on the next acquisition: reuse decodes physical
D64 storage as D128 instead of invoking the existing conversion owner. Changing
same-OID input metadata also leaves the old wrapper metadata behind.

Both fixed and string reuse owners now compare the complete Type before decoding
or mutating the wrapper. A mismatch returns false to the existing Generate
fallback. Conversion remains owned by Generate; no conversion buffer, alternate
state machine, or per-branch metadata update is introduced. Converted inputs are
reconstructed on subsequent acquisition; the previous panic is not a valid
performance baseline for that path.

The two old reuse test roots map to a shared transition fixture. It preserves
plain/nullable/constant-NULL reuse decisions, rejected constant append, and the
original fixed length 1 versus string length 0. It adds exact payload, complete
metadata, source and cleanup oracles. A real same-slot OptGet replacement uses a
changed type and different payload, checking both the fresh wrapper and the
unchanged rejected wrapper. The frame-growth identity test remains separate.
Six conversion cells cover D64/F32/F64 and normal/constant storage with independent
literal coefficients. A production plusFn consumer checks two exact 3.23 results.
This proves the function API contract, not a normal SQL binding counterexample:
the current D64+F64 resolver converts to F64.

Final focused normal/race, vet and incremental lint passed. Molint exited zero
with two unsafe import diagnostics at source sites unchanged from main; this
is not a zero-diagnostic claim. A private overlay removing both guards fails both
transition cells, confirming that type rejection is actually tested. Evidence
and final source hashes are in `29249-empty-parameter-20261004`.

Eight alternating same-binary fixture pairs compare the old two reuse roots with
the new transition holder. Median wall rises from 40.5 to 69.2 microseconds, CPU
from 45.5 to 80.5 microseconds, bytes from 8,392 to 15,512 and allocations from 94
to 159. Additional metadata/cache/value/cleanup coverage has a small absolute
cost, but this is not a resource reduction. These measurements exclude the six
conversion cells and the function consumer. No total-suite speedup, service BVT,
or normal SQL reproduction is claimed. Issue publication remains unavailable
through the integration (earlier HTTP 403); no new issue or PR was created.


## Numeric rounding fixture family (2026-10-04)

The 11 roots in func_math_complex_test.go contain 10 Process construction sites;
the integer boundary helper needs no Process. These numeric functions do not use
file services, yet NewProcess(t) constructs three disk services via TempDir.
Reuse existing NewProcess(nil), whose three named services use disabled-cache
MemoryFS with the same Process configuration. Each root retains its independent
fixture. No new fixture API, cache, worker, or shared global state is introduced.

Register cleanup immediately: close fixture-owned file services, Free the Process,
and verify pool zero. Process.Free does not own file-service closure; runtime
services remain runtime-owned. Original inputs, expected values, NULL/error
contracts, and assertions are preserved after normalizing the fixture edits.

Eight alternating same-binary paired measurements over all 11 roots include
per-root cleanup and exclude build/link and pre-sample GC. Median wall falls
1.496ms to 0.413ms (-72.4%), CPU 1.581ms to 0.494ms (-68.8%), allocated bytes
239,492 to 179,984 (-24.8%) and allocations 2,444.5 to 1,600 (-34.5%). The change
avoids 30 per-service TempDir calls per family run; this is not a physical total
directory count or a whole-CI speedup. Disk/IO tests are outside this change.
Evidence is in 29249-rounding-fixture-20261004. Final focused normal/race, vet
and incremental lint passed; molint exits zero with two unsafe-import diagnostics
in unchanged main files. This fixture-only change needs no service BVT.


The same fixture rule applies to the 11 numeric ceil/floor/round/truncate roots
in func_binary_test.go, including numeric string parsing, NULL/selection,
precision-frame reuse and dynamic digits. Temporal/format/IO tests are excluded.
Ten construction sites execute 14 Processes per full family run, avoiding 42
per-service TempDir requests. Each Process remains independent at its existing
scope, including parent-scoped shared Processes whose nested tests finish before
cleanup. The pure integer boundary root still requires no fixture.

Eight alternating same-binary pairs, including per-root cleanup and excluding
build/link and pre-sample GC, reduce median wall 2.677ms to 1.081ms (-59.6%), CPU
2.835ms to 1.226ms (-56.7%), allocated bytes 517,124 to 413,220 (-20.1%) and
allocations 6,178.5 to 5,012 (-18.9%). Inputs, oracles and reuse order remain
unchanged. Evidence is in 29249-rounding-binary-fixture-20261004. Final focused
normal/race, vet and incremental lint passed. Molint exits zero with two unsafe
import diagnostics at unchanged main source sites; no zero-diagnostic claim.


## Shared result metadata oracle (2026-10-04)

FunctionTestCase.Run previously compared only OID, accepting an exact coefficient
with a corrupted decimal scale. Compare the complete Type after row-count checks
and before value decoding; diagnostics use %#v to expose field differences.
NewFunctionTestResult already provides an explicit Type, without a wildcard
contract. No new option or alternative comparator is introduced.

Four existing retyping roots (six function calls) preserve their original result
initialization and independently declare the final expected metadata after case
construction: temporal subtraction and raw intervals publish width=scale; string
add/sub publishes scale 6; LEAST/GREATEST retains the initial Time(64,0) challenge
and expects Time(64,2). Values, NULL, warnings, errors and reuse behavior remain.
Production retyping is unchanged.

One shared lightweight fixture verifies a correct result and isolated OID, Size,
Width, Scale, Charset and notNull corruptions, plus empty-row and all-NULL
metadata errors. A literal coefficient is written at an existing row before
metadata mutation, avoiding row-count interference. RunAndFree releases every
case, with pool zero asserted per cell. The owning function package full normal
suite passed, as did full race, vet and incremental lint. Molint exits zero with
existing unsafe-import source diagnostics. The old OID-only guard mutation passes
the correct/OID controls but fails seven remaining metadata cells. Final source
hashes and actual test selection are bound in 29249-function-oracle-20261004. This test-only change requires no
service BVT and makes no production performance claim.

### Fixed-result literal expectations

The shared `FunctionTestCase.Run` oracle now compares all 24 supported fixed
OID branches directly against their literal Go slices and NULL flags. The
existing generic comparator covers 23 OID branches (22 Go types); float64 keeps
its epsilon/NaN policy, while float32 remains exact. Complete result metadata
and row count are checked before value decoding. Actual constants still use the
production parameter wrapper; expected slices do not broadcast.

Expected NULL is checked before indexing its payload. A short NULL mask implies
non-NULL for remaining rows; a NULL row may have no payload. A missing non-NULL
payload still panics, and wrong Go expectation types still raise a type assertion
error, including empty results. The existing ownership suite retains mismatch,
reuse, float, filtering, allocation rejection, and cleanup checks. Five extent
cells cover typed empty, short masks, trailing NULL without payload, all NULL
without payload, and non-NULL bounds failure using that same fixture.

Only the redundant fixed expected vector and bitmap construction are removed.
Variable encodings retain their existing vector comparison path; fixed vector
constructors remain necessary for actual input construction. No SQL behavior or
production implementation changes.

A private same-binary comparison used six real CAST cases and eight alternating
pairs of 3,000 evaluations, with case setup outside timing and expected-vector
cleanup inside the old path. Median wall/CPU fell 66.6%, allocated bytes 74.5%,
and allocations 61.5%. These are measurements of the selected fixed-result
oracle workload, not whole-package, CI, or production throughput gains. Evidence:
`29249-fixed-result-oracle-20261004/cost-summary.json`. The retained variable
encoding paths are checked by the owning function package, not included in that
performance estimate.

### Integer assignment test family

Three former roots now share one existing lightweight Process under
`TestIntegerAssignmentContracts`. The groups have identical session/configuration
and run serially; numeric assignment reads SQL mode but does not modify it.
Vectors remain case-owned, each group checks pool zero, and the outer root closes
its MemoryFS and Process. Target-only subtest layers had no independent fixture
and are replaced with typed rows carrying source/target/operation diagnostics.

All 83 old calls map to retained or enhanced coverage: 79 remain, while three
decimal overflow calls and the float64 uint8 upper tie move to batches containing
the same source type, scale, destination and offending literal. Errors now require
`ErrOutOfRange`. Five physical source types cover overflow, partial mask, source
NULL and valid reuse; the common all-mask/uint8 reset owner uses a float64 control.
These are 22 state executions, not 22 subtests. Error checks inspect only the
written first row, without claiming rollback or a shortened result domain.
Successful comparisons retain complete metadata, length, literal values and NULL.

The final family executes 101 calls with 12 errors versus the old 83/10.
Same-binary measurements use eight alternating pairs with setup and cleanup
included; old cleanup is normalized to the same explicit resource release.
Median wall/CPU fell 48.4%/47.5%, allocated bytes 13.5%, and allocations 11.8%.
The earlier extra-subtest candidates increased allocations and were rejected.
These are test-family costs, not CI or production throughput estimates. Coverage,
mutation and validation evidence is in
`29249-integer-assignment-family-20261004/final-main-37ba071`.
No production implementation or SQL contract changes in this stage.

### String and JSON width-owner test family

`TestCastStringWidthContracts` replaces two roots and their single-use helpers.
All eight string and nine JSON rows retain their source representation, literals,
VARCHAR(3) target, strict flag and trailing-space policy. The actual `strToStr`
and `jsonToStr` owners receive the original flags; no production path changes.
The shared lightweight Process has no mutable configuration in this family.
Each case immediately registers vector release and pool-baseline verification;
the root closes its MemoryFS and Process and verifies pool zero.

Six failures require their exact `ErrInternal` or `ErrCastWidthExceeded` category.
Eleven successes additionally check complete Type, length one and non-NULL output.
The old string oracle accepted any error; no production defect is claimed from
that test weakness. Existing pure width-bound and UTF8/trailing-space tests remain
separate because they prove different contracts. No target cross-product is added.

Eight alternating same-binary pairs include encoding, fixture and cleanup; old
resource release is normalized. Median wall/CPU decrease 86.2%/84.4%, allocated
bytes 62.3%, allocations 63.2%. These are costs of the selected 17-case family,
not package, CI or production performance. Mapping and terminal evidence live in
`29249-width-family-20261004`. This test-only stage does not require a service BVT;
earlier production-stage BVT obligations remain open.

### Assignment-ignore string conversion test family

All nine roots and 64 children keep their literal results, warning contents and
counts, NULL, selection, binary-input and error oracles. One common runner now
accepts selection explicitly; the nil-only forwarding wrapper is retired.
Each call retains its own Process and warning session because several children
perform consecutive conversions and their warning histories must stay isolated.
`NewProcess(nil)` preserves the default timezone/configuration without disk FS
setup; these numeric and temporal conversion paths do not access files.
Case cleanup frees vectors before closing the FileService and Process and checks
pool zero. Successful results additionally require complete destination Type and
the original input row domain. Error tails and rollback are not asserted.

Eight alternating same-binary pairs include the entire family, fixtures and
cleanup, with old resource release normalized. Median wall/CPU fall 77.5%/78.0%,
allocated bytes 38.4% and allocations 46.6%. These are selected test-family costs,
not CI or production throughput. Test source gains one line overall; the benefit
is removal of disk setup and stricter shared ownership/oracles, not source-size
reduction. Evidence is in `29249-assignment-ignore-family-20261004`.
This stage changes no production behavior and needs no service BVT; earlier
production-stage BVT obligations remain pending.

### Inactive unary conversion and decimal-prefix execution ownership

The existing string/bytes-to-fixed error-check templates now return before reading
or converting input for zero rows, and recognize a fully inactive bitmap within
the existing bounded mask scan. AllNull remains owned by its original early return.
Partial constant evaluation still invokes its conversion once; selected NULLs are
published by the common execution owner. No new execution template or callback
adapter is introduced. Three duplicated decimal-prefix constant branches and their
row-loop prefix dispatches are retired in favor of the existing string template.
The admission condition preserves the prior binary decoder distinction; binary
constant/flat conversion inconsistency is not silently changed by this patch.

Two regression roots cover literal prefix parsing, all three physical decimal
widths, constant/flat input, mask flags and bitmaps, NULL, empty batches, binary
controls with legal precision, and same-wrapper shape/payload reuse. The shared
owner tests inject a failing conversion, proving zero calls for inactive rows and
one original error for active constants. Long and short masks challenge the actual
row domain. Private parser-entry instrumentation additionally verifies empty and
fully masked prefix constants do no parsing. Old-owner overlays fail the new
literal NULL/error assertions; no parser counter or product test hook is added.

Production code decreases 17 lines. New regression code is reviewed separately;
its value is the common callback and real CAST contracts, not its size. Eight
alternating same-binary pairs at 1,024 rows and 500 evaluations measure only the
selected healthy kernels, with setup/warmup outside timing and resets inside.
DECIMAL128 constant wall/CPU decrease 70.4%/69.6%; flat decrease 5.0%/4.9%.
Existing unary-string flat costs are within 0.4%, treated as unchanged. Both old
and new loops have median zero measured allocations; four samples contain one
48–80-byte allocation. No CI or SQL throughput claim follows.
Evidence: `29249-prefix-constant-selection-20261004`.
Planner production sites create Charset=255 targets, but an externally visible
SQL reproduction and service BVT remain unverified; internal NewCast and shared
kernel behavior are the proved scope. The issue report is drafted locally, with
publication unavailable due to the known integration403.
