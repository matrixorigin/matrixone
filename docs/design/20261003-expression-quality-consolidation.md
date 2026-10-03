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
