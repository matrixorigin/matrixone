# Function test fixture ownership — v2

Refs #29249; anchored to c638482643. Actual gpt-6.1-sol/xhigh design v2
approved on 2026-10-03. This is a continuation, not closure of the wider audit.

## Contract and existing owners

- `newVectorByType` owns one vector until append and NULL installation succeed.
  Check append errors; an unfinished return frees that vector and preserves the
  original panic/error identity.
- `NewFunctionTestCase` owns admitted inputs and its result until successful
  return. Roll back partial construction through the same destructor.
- `FunctionTestCase.Free` owns input/result destruction. It never destroys the
  caller's process, file service, or evaluator. Repeated cleanup is harmless.
- `Run` retains comparison and reuse semantics. Its result is borrowed through
  inspection, until reset or Free. `RunAndFree` wraps the same Run with deferred
  destruction for genuinely terminal consumers.

## Migration and retirement

Audit every caller by lifetime: terminal evaluation, setup that can fail,
borrowed result, rebinding/reuse, escaped case, and benchmark lifetime. Release
nonterminal loop cases before the next iteration; register rollback immediately
in helpers that evaluate before returning. Do not double-run to inspect errors.

Reuse `executeLogicOfOverload`; remove the duplicate evaluator type including
three production factories. Move the remaining fixture declarations to test
compilation. Moving a file is not source reduction. Retire copied timestamp
construction and XML/bitwise/AES cleanup facades with their reverse callers.
Preserve values, rows, NULLs, masks, errors, warnings, metadata, and raw WKB
assertions through the final borrowed inspection.

## QA and evidence

Ordinary constructor inputs use Go-heap backing, so off-heap pool capacity
cannot deny that admission. Cover actual unsupported-input and partial-JSON
failures; verify quota rejection at the existing result-admission account.
Use two zero uint64 rows with real Sleep, uint8 output, a one-byte account, and
the existing Done observer to prove no evaluator entry. Assert original panic
identity, exact native/Go-heap/account baselines before fallback cleanup.

Exercise real successful evaluation, rebinding/reuse, terminal panic cleanup,
and benchmark-then-Run lifetime. Mutations must detect omission of constructor
rollback, helper rollback, input release, or result release. Run nonempty focused
selection, owning package normal, mapped race evidence, incremental SCA, and
production compilation with verified native artifacts. No new SQL behavior or
BVT is introduced. Measure cost with frozen alternating runs before claiming it.

Known independent boundaries remain: weak wantErr oracles, DebugRun selection
and double execution, detached-worker shutdown, formal clock publication, and
the 342-file audit.

## Consumer coverage and ownership map

| Consumers | Release boundary | Preserved independent coverage |
|---|---|---|
| Immediate, sequential and switch-selected terminal cases | Same Run comparison, followed by deferred Free | All original inputs, expected values, rows, NULLs, types and branch selection |
| Arithmetic, casts, temporal and spatial readers | Lexical Free after final inspection; release old case before overwrite | Precision/scale, raw WKB/WKT, input immutability, masks, typed errors and warnings |
| JSON, JQ, LOAD_FILE and assignment-ignore borrowed helpers | Existing bounded child-test cleanup registered before admission/evaluation | JSON literals, prepared provenance, depth/errors, I/O and NULL/mask oracles |
| Masked DIV and timestamp-pair helper callers | Return copied scalar/type; Free before helper return | Seven DIV values/NULL masks; thirteen timestamp-pair full type inspections plus original value comparisons |
| TIME assignment helper | Guard before ownership transfer; caller Free after assertions, per-iteration loop scope | Strict/non-strict/ignore values, error codes, warning rows/messages and scale boundaries |
| AES/XML/BIT and reusable regex/template cases | Canonical destructor; reusable cases remain live through last borrow | Fixed ciphertext, typed errors, cancellation, rebinding, shuffle and payload assertions |
| SERIAL/SERIAL_FULL | Per-scenario case Free before operator Close | Exact decoded tuple/UUID/NULL/geometry plus explicit execution, cardinality and type assertions |
| NOW/SYSDATE precision | One execution per existing boundary, child-test Free | All 18 boundaries; exact type/scale, NULL, precision quantum and typed errors |
| UTC timestamp precision | Existing registered UTC tests, immediate input/output cleanup | All nine retired cases mapped to exact values/metadata and typed errors; existing non-constant admission tests retained |
| Benchmarks | B.Loop owns timing/count; child case Free precedes parent process Free | All 169 Cast cases (two expected errors), 13-row Abs and six 8192-row DateFormat batches; untimed literal checks and per-iteration error polarity |

Geodetic geometry32 cases now select encoded inputs and result type before their
sole construction. The discarded construction was never evaluated; invalid-unit
and unsupported-SRID scenarios remain for both algorithms and representations.

## Validation status and limits

The six ownership scenarios cover real constructor rollback, partial JSON
failure, reuse, comparison panic, borrowed child lifetime and result quota
rejection. Private mutations omitting constructor/input/result/helper release
fail exact account baselines. Fatal JSON probes are expected failures, not green
tests. Forcing NOW/SYSDATE precision to zero fails all twelve positive-precision
cases while retaining zero/invalid-precision controls.

At the reviewed ownership/comparison checkpoint, normal (6.327s), race
(11.071s), vet and incremental lint passed with all 221 Go source hashes
unchanged. Actual gpt-6.1-sol/xhigh review approved; equality and NULL-skip
mutations were rejected.
Production-only compilation remains valid from the ownership checkpoint.
Rebase or subsequent edits require evidence reconciliation before delivery.
The seven no-marker candidates were inspected: one missing destructor was fixed;
the others transfer to explicit consumers or guarded callers. This inventory
is not proof of the entire repository audit or of every destructor's correctness.

The retained Sleep inputs run under virtual time with exact duration oracles;
payload admission covers the last slot, duplicate and first overflow in bounded
batches. XML still reaches the exact work limit; LIKE retains forced budget
exhaustion, an NTT block boundary, and positive/negative search offsets. Private
production mutations are rejected by each corresponding oracle.

The old Abs benchmark panics at its first evaluation because the lazy result
vector was not admitted. Benchmark now reuses Run for untimed admission and
PreExtendAndReset before each real evaluation. B.Loop replaces the fixed 100000
loop; no error is ignored. DateFormat's 100 identical cases become one batch,
retaining every value, NULL, constant and expected field. The unused multi-case
builder and old benchmark helper are retired with their callers.
DebugRun now admits/reset results through the same vector owner and forwards
its existing selection. Its 26 callers and the helper's masked execution branch
no longer prepare or dispatch separately; raw evaluator errors still return the
borrowed partial vector. The helper alone returns nil on error. Existing reuse
and quota scenarios prove fresh masked execution, mask reset and rejection
before evaluator entry. Six decimal benchmarks now check literal coefficient
123400 and full source/result types, with no unused target-vector payload.
The DebugRun increment passed full-package normal tests, all 29 affected UTs
under race, six decimal benchmarks, vet and incremental lint. Independent
gpt-6.1-sol xhigh review approved this local closure; the full-package race gate
remains failed under #29592.
Normal package tests and all 176 benchmarks passed at 1x; the four affected UTs
and all 176 benchmarks passed under race at 2x. Native/heap-baseline and B.N+1
real-entry probes passed; both post-admission Cast input mutants failed timed
error guards. Vet and incremental lint passed. Full-package race failed at the
known regexp2 timeout case before benchmarks ran; that failure remains an open
delivery gate. Benchmark code/evidence passed independent xhigh review; it is
not approval of overall delivery or proof that the dependency was repaired.

Twenty-three exact fixed-type comparisons share the existing vector wrapper
path. Float64 retains epsilon and paired-NaN semantics; float32 stays exact.
The existing reuse fixture checks literal first/last-row diagnostics, both NULL
mismatch polarities, both-NULL continuation, mask reset and double cleanup.

Matched three-pair package runs before comparison deduplication measured wall
9.079s to 6.281s and CPU 2.397s to 2.132s; RSS 168544 to 170472 KiB did not
improve. These are serial local measurements, not CI savings or proof of the
repository-wide objective. The formal regexp2 stale-clock dependency remains
unresolved despite a deterministic private reproducer and private fix.
Moving the fixture to test compilation does not count as a source reduction.
Keep #29249 open; the broader audit and final review/delivery gates remain active.
