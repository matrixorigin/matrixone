# FORMAT_BYTES and FORMAT_PICO_TIME design

Revision: D1, 2026-10-13. Owning issue: matrixorigin/matrixone#29998. Implementation PR: [#30036](https://github.com/matrixorigin/matrixone/pull/30036).
Reviewer/designer: actual `gpt-6-astra`, reasoning effort `medium`, selected for this design session. Exposed session/agent identifier: `/root/design` (no UUID is exposed by the tool).
Inspected source revision: `0a4db0e6a6517e489a333ad9073dfcdc5d26aff4`. No production or test code modified by this design session.
Decision: **PASS**, for this D1 design, with delivery bookkeeping and implementation validation below required before delivery. No unresolved design question.

## Classification and record

The implementation is a focused compatibility restoration, expected below 500 production lines in one owning package, but it introduces two previously unavailable public SQL functions. Treat it conservatively as an additive public compatibility contract: the complexity trigger in `feature-design-review.md` applies despite its size. This document supplies the distinct design-first phase. Before source implementation, copy this exact approved content into `docs/design/format-performance-schema-functions.md` and version it; record its Git revision in the task record and link that reviewed revision in the eventual implementation PR. Parent may replace the implementation-PR placeholder without another semantic design review. No separate RFC or design-only PR is needed. Changing the numerical or conversion decisions below requires focused re-review.

Risk is R2, public deterministic SQL behavior. Registration, conversion consumer, vector evaluation and SQL result tests form one closure. No worker, catalog format, allocator, wire representation or state machine is introduced.

## Problem, evidence and invariant

Evidence E01 reports that both functions independently fail during planning. E03/E04 establish the documented MySQL function surface and the exact MySQL 8.0.46 implementation and expected results. E05 and this source inspection confirm both names absent from the registry. A live baseline SQL reproduction is still pending the parent's native/service build; this design does not claim to have executed it.

The first owner is `pkg/sql/plan/function`: `function_id.go` owns name/ID mapping, `list_builtIn.go` owns overload/type resolution, and the execution function owns formatting. The cause is missing registration and implementation, not a formatter or cast bug.

Invariant: a single numeric expression or supported numeric string resolves to each named function, passes through the established DOUBLE conversion, and yields the corresponding MySQL 8.0.46 human-readable VARCHAR result for finite values, preserving SQL NULL. Equal evaluated DOUBLE values produce equal strings regardless of literal, column, aggregate or parameter origin. Units are selected from the absolute unrounded DOUBLE; sign is preserved by formatting the original signed value. The smallest negation is either unresolved `FORMAT_BYTES(1024)` or a result other than `1.00 KiB`; independently, `FORMAT_PICO_TIME(1000000000)` must be `1.00 ms`.

Scope is these two functions. No new parser syntax, Performance Schema tables, sys schema aliases, global coercion rules, DECIMAL formatting domain, locale option, runtime setting or general formatting framework.

Primary references, already captured by the parent:
- https://dev.mysql.com/doc/refman/8.0/en/performance-schema-functions.html
- MySQL tag mysql-8.0.46 `sql/item_pfs_func.cc`, and `mysql-test/suite/perfschema/{t,r}/native_func_format_{bytes,time}.{test,result}` (saved under task records).

## Contract and implementation

1. Append `FORMAT_BYTES = 596`, `FORMAT_PICO_TIME = 597`, and advance `FUNCTION_END_NUMBER = 598` at the inspected base. Preserve every allocated old ID. If main changes before delivery, allocate after the then-current end without renumbering existing functions. Register lowercase names.
2. Add one strict standard-function overload (ID 0) for each: argument `types.T_float64`, result `types.T_varchar`. Use `fixedTypeMatchWithBoolNumericCast`, following SIN/SINH and the hyperbolic functions. Do not set a return width of 11 simply because MySQL advertises that width: large exponents and signed output can be longer. The ordinary VARCHAR return type is sufficient and avoids truncation.
3. Put the two small execution wrappers and pure formatter helpers in a coherent `func_format_units.go` (name is editorial). Each wrapper uses `opUnaryFixedToStr[float64]`. Keep helpers unexported. An at-most-seven-entry immutable unit table or explicit descending comparisons are both acceptable; avoid per-row dynamic table construction. A tiny shared helper for the common final rendering is justified by exactly these two consumers; a configurable framework is not.
4. Bytes select the largest threshold not greater than `abs(value)`: `1024^1..1024^6` map to KiB, MiB, GiB, TiB, PiB, EiB. Below 1024 use bytes. Time thresholds are 1000 ns, 1000000 us, 1000000000 ms, 1000000000000 s, 60000000000000 min, 3600000000000000 h and 86400000000000000 d. Below 1000 use ps. Compare against absolute values; divide the signed value by the selected divisor once. Do not repeatedly divide and round at intermediate units.
5. For finite base units, truncate toward zero by integer conversion. Width is minimum 4 for bytes and 3 for ps, followed by one space and unit. Thus zero is `"   0 bytes"` and `"  0 ps"`, -0.9 is also rendered as integer zero, and -1023 is `"-1023 bytes"`. Conversion is safe because these branches are bounded to magnitude below 1024/1000. Negative zero loses its sign in these base-unit integer branches, matching MySQL.
6. For scaled units, use two digits after the decimal point. Before rounding, if `abs(scaled) >= 100000`, use lowercase scientific notation with two fractional digits and explicit signed exponent; otherwise use fixed notation. Standard Go `fmt.Sprintf("%4.2f %s", ...)` / `"%4.2e %s"` or equivalent standard-library formatting is suitable. Do not apply `math.Round(value*100)/100`, decimal rounding, epsilon adjustments, or promotion to the next unit after rounding. MySQL uses libc sprintf of the evaluated binary DOUBLE; normal nearest-even binary-to-decimal formatting is the reference, including ties and neighboring representable values. In particular, 1048575 bytes yields `1024.00 KiB`, and 999999999999 ps yields `1000.00 ms`.
7. All finite float64 inputs have bounded output: fixed mode has magnitude below 100000 before rounding; scientific mode has a bounded float64 exponent. A conservative bound of 32 ASCII output bytes per row covers this implementation. MaxFloat64 must take scientific mode, never expand to hundreds of decimal digits.
8. NULL and selection are handled by the existing template. It handles empty batches, constant replication, constant NULL, row NULLs and masked rows, returns append/allocation errors and does not retain output across invocations. Do not bypass its `FunctionSelectList` handling. Its existing constant-path evaluation may calculate a pure value once for partially selected rows, which is safe because these helpers have no warnings or side effects.

### Conversion and defined limitations

The existing `fixedTypeMatchWithBoolNumericCast` opts BOOL into the numeric context without affecting unrelated functions. Integer, unsigned integer, float, decimal and text inputs use the existing cast to float64. This is intentional even for integers beyond 2^53: MySQL invokes `val_real`, so an exact integer/DECIMAL formatting path would change thresholds and rounding. NULL/unknown parameter resolution uses existing matcher behavior; normal resolver errors reject zero/two arguments and unsupported types.

Inspection confirms the string execution path `strToFloatWithProc` -> `parseBytesToFloat` -> `parseStringToFloatWithBitSize`. MySQL mode consumes a decimal prefix, returns zero when absent, clamps overflowing decimal text to finite MaxFloat64, and `appendNumericCoercionWarning` supplies code 1292 for nonempty truncated text; empty text returns zero without a warning. Prepared BOOL provenance and prepared DOUBLE conversion already have established shared paths. Native mode remains strict. Do not add a second parser, warnings inside the formatter, or global cast changes. Validate representative SQL paths rather than claiming all coercion combinations have been newly proved.

MO already accepts extension float syntax such as NaN/Inf and nondecimal prefixes in shared casts, unlike the finite MySQL contract under consideration. Preserve shared casts. Define deterministic bounded extension rendering: positive/negative infinity follows magnitude selection to the largest unit and standard Go formatting (`+Inf EiB`, `-Inf EiB`, `+Inf d`, `-Inf d`); NaN is handled before any integer conversion and uses width-padded standard floating spelling with the base unit (`" NaN bytes"`, `" NaN ps"`). This avoids platform-dependent NaN-to-integer conversion. These are explicitly MO extension results, not claims of MySQL equivalence. Test them cheaply at the pure helper seam. Native strict-input errors and any existing differences for binary/temporal/JSON or numeric-extension inputs are inherited, not repaired in this issue.

## Ownership, cost and terminal paths

Flow: SQL name -> function ID/type checker -> existing cast, if needed -> encoded overload 0 -> strict vector wrapper -> formatter -> result vector -> frontend VARCHAR. Same encoded overload serves normal and prepared evaluation. Do not edit planner or cast code unless a newly demonstrated defect demands separate design review.

There is no retained state. Input vectors are borrowed; the executor/result wrapper owns result storage, and its append methods copy the short bytes from the formatter. QuickStrToBytes use is already encapsulated by the template. Per evaluated nonconstant row cost is at most seven magnitude comparisons, one division and bounded formatting/copy; O(rows) CPU and output memory, O(1) local state. Constant inputs format once then replicate through the existing template. No new goroutines, I/O, locks, retry, cache, queue, metrics or log cardinality. Input string scanning is existing cast work proportional to input length.

Success appends the result; NULL/selection does not inspect the corresponding data; append errors propagate to the established executor cleanup. Empty input does no work. Reset/reuse is owned by the existing result wrapper and must be exercised. Cancellation has no new wait or operation to interrupt; work per row is bounded. Partial result cleanup remains framework-owned on errors. No new state persists over cancellation/retry/restart, so separate fault injection, race and restart fixtures are not warranted. No new authorization or tenant boundary exists.

## Alternatives and delivery compatibility

Status quo leaves the documented functions unavailable and fails the issue's requested behavior. SQL/UDF rewriting or sys wrappers add deployment dependencies and duplicate selection/conversion without addressing native resolution. Separate integer, decimal and string overloads increase dispatch/coercion surface and diverge from MySQL DOUBLE semantics. A per-row custom vector loop duplicates proven NULL/selection/append handling. Select one DOUBLE overload plus the existing template for correctness and minimal maintenance.

Existing IDs/encodings and data/storage formats remain stable. Older binaries do not know the new IDs and cannot evaluate these new functions; do not advertise mixed-version support for queries that use them. Deploy to all query-executing CNs before using the new functions, following the existing additive-builtin convention. Existing queries continue unchanged. There is no migration/backfill or persistent data transformation. Views or stored SQL referring to these functions require supporting binaries when evaluated; before rollback remove/avoid such use and reprepare affected statements. No additional RPC version mechanism or feature flag is justified for this bounded additive patch. Single-CN SQL proof suffices because no routing/protocol implementation is changed. If repository delivery policy exposes a previously unobserved function-version requirement, resolve that policy before push.

## Validation and acceptance

Use E01-E05 as prior evidence; do not rerun reference retrieval. Preserve independent runtime baseline queries when the native build finishes. Typed resolver and encoded-execution tests prove integration separately from helper output tests. Use explicit expected strings from MySQL source/results; never generate expected values with the implementation under test.

| Contract/risk | Cheapest proof |
|---|---|
| Every unit, signs, zero, fractions | Pure helper table with exact strings; positive/negative representative each unit, base-unit truncation and padding |
| Threshold order and DOUBLE domain | Boundary pairs at each unit with `Nextafter` where integer N-1 is unrepresentable; just-below promotion rendering; uint64/large DECIMAL SQL example |
| Fixed/scientific formatting | Both sides of 100000 in highest unit, negative mirror, MaxFloat64; exact binary tie (e.g. scaled 1.125) plus neighbor; source-result known large values |
| Null, masking, reuse, constants | Existing NewFunctionTestCase seam, mixed 3-row vector, constant repeated to 3 rows, constant NULL, empty batch, partial and all selection, re-evaluate after resetting selection/NULL state |
| Registration and consumers | GetFunctionByName argument type matrix including BOOL/text/decimal/any; VARCHAR return and encoded ID 0; wrong arity and unsupported type; RunFunctionDirectly result |
| Public SQL conversion/results | One coherent BVT case: independent original repros; numeric/unsigned/DECIMAL/text/BOOL/NULL, small ordered table with mixed NULL and aggregate, prepared statement reuse, wrong arity |
| Inherited coercion behavior | SQL numeric prefix and invalid text with warning check, empty text, one native-mode strict error with session restoration; no cast implementation changes |
| Bounded extension behavior | Pure helper NaN/±Inf and MaxFloat64 tests, no new SQL extension conformance claim |

Fixture inventory already identifies function test helpers and no matching BVT for these names. Parent should finish neighboring BVT inventory before choosing the coherent case path; keep it independently runnable. Use at most three/four table rows and immediate table/statement teardown. No added UT cluster. BVT uses the existing test-owned service and small SQL workload; no timing assertions or sleeps. Delimit base-unit strings in SQL (e.g. brackets) or use exact comparison so test-output whitespace normalization cannot erase the oracle.

Acceptance requires: both baseline failures recorded or explicitly attributed as pre-existing issue/source evidence; focused named UT selection and execution with count=1 through mo-cgo-test; one normal owning function-package run; gofmt and incremental vet/lint; reviewed BVT expected results; exact changed BVT in normal comparison mode twice on the same ready instance with teardown postconditions checked between runs; parent final astra review against this design with zero blockers. Reuse any valid exact-head evidence. Broader BVT group is required only if shared harness/setup conventions change. No benchmark, race, multi-CN, restart or full-repository static scan is indicated by this closure.

Potential implementation surprises are failing standard-format differential checks, a cast path failing a claimed numeric/text input, or a repository rollout policy requiring explicit version gating. They must be resolved from evidence rather than silently weakening assertions. Current design decisions are complete; these are implementation acceptance checks, not deferred architecture choices.
