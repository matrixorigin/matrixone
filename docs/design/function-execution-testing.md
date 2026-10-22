# Function execution and test ownership

This document describes the shared expression, arithmetic and test-fixture
boundaries used by `pkg/sql/plan/function`. Function-specific SQL semantics remain
with their implementation and feature designs, including
[temporal compatibility](temporal-compatibility.md).

## Execution owners

| Responsibility | Owner | Boundary |
| --- | --- | --- |
| SQL result metadata | [Planner binders](../../pkg/sql/plan/base_binder.go) | Preserve precision, scale and source identity through binding and execution; physical storage type alone is insufficient. |
| Constant materialization | [Expression construction](../../pkg/sql/colexec/evalExpression.go) | Validate source metadata before allocation, retain allocation-account selection, and release incomplete construction before executor ownership transfers. SQL type, runtime string domain and prepared provenance are distinct. |
| Row admission and result publication | [Typed execution templates](../../pkg/sql/plan/function/baseTemplate.go) and registered adapters | Own selection, NULL publication and the function's error policy; do not move these into scalar arithmetic helpers. |
| Checked arithmetic | [Temporal helpers](../../pkg/sql/plan/function/func_binary.go) and [decimal primitives](../../pkg/container/types/decimal.go) | Own numeric bounds and rounding. Adapters retain conversion, formatting, timezone and diagnostic responsibilities. |
| Parameter-wrapper reuse | [Vector function tools](../../pkg/container/vector/functionTools.go) | Require complete type equality and compatible wrapper shape before decoding. Rejected reuse leaves replacement to the existing conversion owner. |
| Zonemap scratch vectors | [Zonemap expression evaluation](../../pkg/sql/colexec/evalExpression.go) | Release temporary operands and clear scratch slots on success and failure. Pruning and actual residual evaluation have separate correctness oracles. |

## Selection and unary execution

`FunctionSelectList` owns the flag and bitmap representation. `AllNull` suppresses
all rows; otherwise `AnyNull=false` means evaluate all rows, even if a stale
bitmap remains. A short partial mask leaves unlisted rows active. Entries beyond
the logical row count cannot affect the result.

`applyFunctionSelection` publishes bounded selection NULLs. Typed templates keep
their own fixed/variable and all-NULL result publication. For the null-propagating
unary templates, empty input, all-inactive input and source NULL suppress scalar
conversion. An active non-NULL constant invokes conversion once; flat inputs
invoke it for each active, non-NULL row.

Keep error propagation, NULL-on-error and callback-produced NULLs with their
existing typed owners. A common mask operation must not clear NULLs published by
a callback. Row-aware constant callbacks receive row zero; flat callbacks retain
the actual row index, including binary-JSON provenance. Variable results must copy
borrowed callback bytes before that storage is reused.

These template contracts do not establish SQL reachability for every injected
mask combination. They also do not replace adapter-specific admission rules.

## Temporal adaptation

`doCalendarInterval` and `doTimeInterval` own checked arithmetic. DATE, DATETIME,
TIMESTAMP, TIME, integer-date and string adapters retain their different
conversions, precision, timezone, formatting and warning policies. In particular,
TIMESTAMPADD string formatting and DATE_ADD string formatting are not
interchangeable. A NULL-on-error template that swallows fatal errors or invokes a
constant callback once cannot replace an adapter with per-evaluated-row warnings.

`TimestampAddDate` resolves its result metadata before interpreting result
backing storage. Ordinary SQL binds a constant unit, but internal dynamic-unit
calls remain supported. Unit admission precedes result retyping and operand NULL
suppression: a selected-out dynamic unit is skipped, while a unit on a NULL-date
row can still determine DATE versus DATETIME and fractional precision. A late
invalid unit must still be rejected after an earlier MICROSECOND unit.

Changing DATE output to DATETIME checks `SetTypeAndFixData` before accessing the
new backing representation. Denied growth preserves the original vector, data,
length and allocation usage. Arithmetic overflow diagnostics count evaluated
rows, including constant operands; masked or NULL operands do not add warnings.
Integer-date unit restrictions and empty-batch admission stay with that adapter.

## Decimal and result metadata boundaries

Decimal kernels retain their actual vector/vector, scalar/vector and
vector/scalar routes, checked scale alignment and fallback. Preserve signed,
truncated and rounded results, strict/permissive zero-divisor policies, exact NULL
maps and untouched initially masked scratch values. Raw arithmetic errors do not
promise rollback of scratch output.

Unsigned `Div256` uses the canonical quotient/remainder owner, including half
parity and quotient carry; signed callers restore the sign and check range.
`decimal256BatchArith` owns declared-precision checks after physical arithmetic,
including the minimum signed coefficient whose absolute magnitude retains its
sign bit. Internal width-76 carriers retain their distinct admission boundary.
Keep precision checks at the publication owner rather than duplicating them in
each kernel.

Rescaling and decimal CAST adapters retain their checked rounding, growth,
clamping and target-precision policies. Zero coefficients and empty widening do
not require value work; the existing result-reset owner still handles length and
NULL state. Reusing a physical OID does not permit dropping width, scale, charset,
nullability or other logical type fields from reuse and comparison checks.

## Function test fixtures

The fixture implementation lives in
[`func_testcase_test.go`](../../pkg/sql/plan/function/func_testcase_test.go), not
in production compilation. It uses the same `executeLogicOfOverload` signature
as registered functions.

| Operation | Ownership and result |
| --- | --- |
| `newVectorByType` | Owns the current vector until append and NULL installation succeed; incomplete construction frees it and preserves the original failure. |
| `NewFunctionTestCase` | Owns admitted inputs and result during construction; partial failure rolls them back through `Free`. Successful return transfers the case to the caller. |
| `Free` | Releases inputs and result, tolerates repeated cleanup, and leaves the process, file service and evaluator caller-owned. |
| `Run` | Resets/admits the result, evaluates with the current selection, and compares successful cardinality and complete `types.Type` before decoding, including empty/all-NULL results. |
| `DebugRun` | Uses the same result admission and selection. Admission failure returns nil; evaluator failure can return a borrowed partial result with the error. |
| `RunAndFree` | Uses `Run` with deferred destruction for terminal consumers. Consumers that inspect, rebind or reuse results retain the case until the final borrow. |
| `Benchmark` | Checks correctness before timing; each `B.Loop` iteration resets/admits the result, evaluates once and checks error polarity. The caller owns final case destruction. |

Results returned by `DebugRun` or `GetResultVectorDirectly` are borrowed until
the next reset or `Free`. Release loop-owned cases before the next iteration.
Register cleanup immediately after acquiring a resource, before fallible setup
or assertions. A helper returning a borrowed result must retain its owner through
the caller's inspection. Release vectors/results before their service and process
owners; a process does not close a caller-owned file service.

`wantErr` checks error presence only. Error classes, diagnostics, warnings and
partial-result guarantees require dedicated assertions. Fixed-value comparisons
remain exact except for the established float64 epsilon/paired-NaN policy;
float32 remains exact. Variable encodings and geometry retain their separate
comparison rules. Expected values and full type metadata must be independent of
the implementation under test.

Leak assertions observe both native and Go-heap ownership, and allocation-account
usage when applicable, before fallback cleanup. Native `CurrNB` alone cannot
detect retained Go-backed vectors. Fallback teardown must not hide a failed
ownership assertion. Share fixtures only across compatible configuration and
lifetime boundaries, keeping warning/session state and explicit wrapper reuse
under the scenario's control.

## Regression ownership

The following existing tests own distinct contracts. Internal callback and
helper tests complement real planner/executor consumers and SQL BVT; they do not
substitute for them. This table is a navigation aid, not a claim that tests were
rerun or that the repository-wide quality audit is complete.

| Contract | Existing regression owner |
| --- | --- |
| Construction rollback, borrowing, cleanup and complete result metadata | `TestFunctionTestCaseOwnership` and `TestFunctionResultMetadataContract` in [the fixture tests](../../pkg/sql/plan/function/func_testcase_test.go). |
| Unary admission, errors, reuse and borrowed callback bytes | [Template selection tests](../../pkg/sql/plan/function/base_template_selection_test.go). |
| Calendar/string arithmetic and TIMESTAMPADD wrapper reuse, denied growth and warning counts | [Temporal function tests](../../pkg/sql/plan/function/func_binary_test.go); [diagnostics and integer-date admission](../../pkg/sql/plan/function/temporal_compatibility_coverage_test.go); [binder metadata](../../pkg/sql/plan/base_binder_timestampadd_test.go). |
| Decimal physical routes and independent wide/fallback boundaries | [Batch arithmetic tests](../../pkg/sql/plan/function/arith_decimal_fast_test.go), [wide arithmetic tests](../../pkg/sql/plan/function/arith_decimal_wide_test.go) and [primitive tests](../../pkg/container/types/decimal_test.go). |
| CAST precision, assignment, width and published precision | [CAST precision](../../pkg/sql/plan/function/func_cast_decimal_precision_test.go), [integer assignment](../../pkg/sql/plan/function/func_cast_integer_assignment_test.go), [string width](../../pkg/sql/plan/function/func_cast_width_test.go), [arithmetic publication](../../pkg/sql/plan/function/decimal_arithmetic_widen_test.go) and [Decimal256 consumers](../../pkg/sql/plan/function/decimal256_support_test.go). |
| Parameter representation, NULL/value transitions and rejected reuse | `TestFunctionParameterReuseTransitions` and neighboring tests in [the vector owner](../../pkg/container/vector/functionTool_test.go). |
| Constant admission/metadata and zonemap temporary ownership | [Expression allocation tests](../../pkg/sql/colexec/evalExpressionAllocation_test.go), [constant materialization](../../pkg/sql/colexec/evalExpression_test.go) and [zonemap/residual tests](../../pkg/sql/colexec/evalExpression_zonemap_test.go). |

## Historical consolidation records

The completed work under [#29249](https://github.com/matrixorigin/matrixone/issues/29249)
previously mixed current contracts with local checkpoints, private probe reports,
model reviews and benchmark tables. The committed records remain available at
the immutable pre-consolidation revision below. Historical pass/fail results and
measurements describe their recorded inputs, not current validation or whole-CI
performance; private local artifacts are not included in these links.

| Record | Preserved history |
| --- | --- |
| [Expression consolidation](https://github.com/matrixorigin/matrixone/blob/8f9c50468ea06cd52554f2a5ef94cfb137d930f6/docs/design/20261003-expression-quality-consolidation.md) | Earlier stage retirement maps, DATE_FORMAT fixture mapping, non-function cleanup history and measurements. |
| [Modulo retirement map](https://github.com/matrixorigin/matrixone/blob/8f9c50468ea06cd52554f2a5ef94cfb137d930f6/docs/design/20261003-expression-quality-consolidation.md#modulo-retirement-map) and [integer-division retirement map](https://github.com/matrixorigin/matrixone/blob/8f9c50468ea06cd52554f2a5ef94cfb137d930f6/docs/design/20261003-expression-quality-consolidation.md#d64d128-integer-division-retirement-map) | Complete former-test to retained-case mappings. |
| [Fixture migration](https://github.com/matrixorigin/matrixone/blob/8f9c50468ea06cd52554f2a5ef94cfb137d930f6/docs/design/20261003-function-test-fixture-ownership.md) | Consumer lifetime inventory, mutation reports and validation chronology. |
| [Temporal consolidation](https://github.com/matrixorigin/matrixone/blob/8f9c50468ea06cd52554f2a5ef94cfb137d930f6/docs/design/20261003-temporal-execution-consolidation.md) | Original alternatives and implementation plan. |
| [Unary consolidation](https://github.com/matrixorigin/matrixone/blob/8f9c50468ea06cd52554f2a5ef94cfb137d930f6/docs/design/20261004-unary-selection-execution.md) | Historical entry inventory, private admission probes and measurements. |
