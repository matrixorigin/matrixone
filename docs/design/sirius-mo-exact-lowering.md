# Embedded MO exact-decimal lowering

Design version: 1. Owner: MatrixOne planning, compile and CN execution.
Tracking: [#28968](https://github.com/matrixorigin/matrixone/issues/28968),
parent [#28966](https://github.com/matrixorigin/matrixone/issues/28966).

The user approved this C/D design before implementation on 2026-10-08.
This document records PR C of the delivery sequence in merged #29690.
Semantic authority remains #29449 document blob
`42a89f09a1d168d02b9583cb3ea7b4de6dbb5634`.
Native B, Sirius #27, merged as
`5ea60cd31955d0dced2adcbcd3df0772207b79ef`, is the native dependency.

## Decision and invariant

For each admitted embedded SELECT, validation, serialization, MO read
publication, native preparation and result reconstruction must agree on the
physical decimal width, precision, scale and nullability selected by MO.
Values, NULLs and public numeric error classes must match native MO.
The negation is any descriptor drift, coefficient narrowing, extra rounding,
error reclassification or fallback after explicit embedded selection.

The native runtime owns one immutable capability snapshot. Expose it through
`Capabilities() uint64` on the bridge runtime and configured backend. Native
ABI-v1 MO-input/result capabilities retain their existing mandatory check.
Mask `16u` selects the complete MO exact-decimal v1 family; unknown bits do not
enable another family. Flight returns no embedded numeric capability.

`ExportEmbeddedMO` accepts an explicit value export profile derived from that
snapshot. Candidate owns a copy, and every validation, read-schema and build
pass uses it. There is no mutable global profile or per-query capability
cache. The zero profile retains legacy emission. A capability change in another
runtime cannot change a previously validated candidate's serialization.

## Wire and semantic closure

The exact profile uses `urn:matrixone:sirius:exact-decimal:v1` and its
`mo_exact_decimal` type consistently for Decimal64/128/256, including narrow
declared precisions in physically wide carriers. It never infers width from
precision. Parameters are physical bits, precision and scale, plus Substrait
nullability. Internal precision 76 is valid for a 256-bit working type; public
results are limited to precision 65.

Exact literals use the approved `ExactDecimalLiteral` protobuf with a single
fixed-width little-endian coefficient, exactly 8/16/32 bytes. Preserve MO's
canonical literal/protocol provenance. Decode a planner-owned numeric literal
with MO's exact parser where its internal representation requires that step;
this does not admit arbitrary runtime string-to-decimal conversion.

Admit the native family's fourteen scalar and four aggregate operations by
bound overload ID and complete operand/result descriptors, rather than display
names. Normal checked casts retain the approved source/domain restrictions;
explicit, assignment/comparison and unsupported source conversions remain
ineligible. Preserve CASE/COALESCE types, exact comparisons, aggregate phases,
and existing structural group/join/sort consumers. Unsupported reachable
shapes fail before readers start. No new projection pushdown or MO fragment
execution substitutes for an unsupported native operation.

Retain Native B's signature limits, including SUM/AVG results whose physical
width is at least their input width. A physically wide, low-precision input
whose MO-bound SUM returns Decimal128 is declined; changing that result to
Decimal256 would violate descriptor identity. Scan-owned filters remain MO
reader work in both validation and emission, rather than being validated as
Flight expressions and then omitted from the embedded wire plan.

All decimal-bearing parts of one exact closure use the extension. Standard
Flight emission stays on its existing type/function contract. Declaration
anchors and serialization are deterministic. Original and encoded plans retain
the existing 16 MiB limit. Current-main Q1-Q22 inventory includes the complete
reachable expression graph, not only the first reported decline.

## Input, output and errors

Decimal256 input is a 32-byte fixed-width coefficient through the existing
MO-native publisher. Credit is acquired before outgoing allocation/copy;
constants are charged for expansion and oversized batches split at row
boundaries. Keep 64 MiB input/result windows and the existing one-active-query,
sixteen-waiter, default-two-stream contract.

Reuse the existing typed borrowed-result decoder: its fixed-width path already
supports 32-byte types. Validate native schema, lengths, validity and descriptor
identity before installing vectors. Native result credit survives the fill
callback, and the existing buffer lease owns the Go copy. Partial failure,
cancellation and result-writer errors unwind through the existing owners.

Native status 12 maps to `moerr.ErrOutOfRange` (MySQL 1690 / SQLSTATE 22003).
Status 13 maps to `moerr.ErrInvalidInput` (MySQL 20301 / SQLSTATE HY000).
The operation that failed owns the error: scalar overflow inside SUM remains
out-of-range; SUM final overflow and checked-cast failure remain invalid-input.
Producer cancellation during cleanup cannot replace that primary error.
GPU-unavailable/quiescence failure keeps the fatal-runtime contract.
Messages remain bounded and do not add row contents.

Extend the existing query-local terminal evidence with the selected profile,
capability snapshot and verified output descriptors/digests. Keep account/query
identity, GPU task counts, source/result accounting and terminal health together.
There is no new registry, unbounded collector or high-cardinality metric label.

## Ownership and validation map

| Closure | First owner and terminal path | Risk / proof |
| --- | --- | --- |
| Capability/profile | Service runtime -> candidate value -> serialization | R2: missing capability, legacy controls, candidate/profile retention |
| Types/functions/literals | MO bound plan -> exact encoder -> native prepared schema | R2: independent descriptor/wire vectors, overload negatives, complete inventory |
| Input/results | MO reader -> acquired native credit -> GPU -> result lease -> fill -> release | R3: width/NULL/constant/slice boundaries, partial failure, cancellation and reuse |
| Errors/evidence | Native terminal status -> typed Go error -> public MySQL result/terminal event | R3: failing-operation identity, cleanup races, query-local metadata |
| SDK/build | Merged native pin -> clean fingerprinted SDK -> linked MO consumer | R1/R2: source/header/proto/artifact identity, supported build modes and loadability |

Host unit tests prove arithmetic encoding, admission and control contracts;
they do not replace GPU execution evidence. Required real GPU/public tests cover
all widths, high limbs/signs, NULLs, exact scalar/aggregate values, joins/sorting,
masked errors and healthy reuse. Reuse the approved prepared-division controls
at increments 0/4/10/30 and verify bound result metadata after re-execution.

Keep public SQL fixtures small and deterministic, with native MO as the
independent value/metadata/error oracle. Test malformed input and absent
capabilities before readers start. Build CPU-only, Sirius-enabled and combined
Sirius/cuVS profiles using the current go.mod toolchain and controlled CGo
wrapper. Run repository SCA and self-review before delivery.

The selected design preserves one typed execution boundary. Alternatives of a
global exporter toggle, mixed ordinary/exact decimal closure, float/narrowing
conversion or post-selection fallback violate the invariant. A second result
decoder/allocator would duplicate existing ownership without another need.

## Delivery boundary

C pins merged Native B and remains opt-in; current backend/default selection
does not change. D starts after C merges and versions the real native-MO and
embedded-MO campaign, with bounded transient row spools, all-22 SF1/SF10,
streams=2 plus 1/4 controls, one excluded warm-up/five measured suites and ten
separate Q9 repetitions. Existing performance/resource gates remain required.

E/F are excluded from this implementation round. Their separate design must
resolve production recovery authority before default cutover and Flight
retirement. A nil/unready lease manager is not proof that Flight is empty.
No direct-TAE, storage/directory-lock, Docker image/base or fallback work is
introduced here. #28968 remains open until D acceptance; #28966 remains open
through cutover, verified release availability and retirement.

## Implementation evidence (2026-10-09)

The implementation base is `83d82b8ee0cd694e0c6a7146902d74ae4dfb415a`.
The selected native SDK is generated from clean merged Sirius
`5ea60cd31955d0dced2adcbcd3df0772207b79ef`, with importer
`95d9ce8d78490db3991ab6145653716aa3ec42c9`. Tests use the current
Go 1.27.1 toolchain and frozen Pixi `mo` environment.

The complete Q1-Q22 MO exporter inventory passes validation and deterministic
serialization. Focused descriptor/overload/literal, Decimal256 publication,
borrowed-result, error-identity and schema-evidence tests pass. The exporter,
bridge and CN owning-package suites pass. The full compile suite fails
`TestRequiredIVFWorkersFallbackAsWholeQuery/{supported,canceled}`; both failures
reproduce at the verified clean base with the same GPU-linked host test mode.
They are not reported as a passing full compile suite.
All four owning-package suites also pass in normal CPU mode, including the full
compile suite. The reproduced IVF failure is specific to the GPU-tagged host
test mode; changing or skipping that unrelated test is outside C.

The combined Sirius/cuVS release build passes SDK verification, linking and
packaging. Real MySQL tests pass fixed-width Decimal64/128/256 values and NULLs,
scalar overflow (including inside SUM), SUM final overflow, healthy runtime
reuse, and binary-prepared division at increments 0/4/10/30 with independent
value and public metadata assertions. Terminal events confirm exact profile,
capability mask 31, completed GPU tasks, no fallback and zero retained input
and result credit after cleanup. Public fixture setup remains one isolated
cluster; each selected run takes about 9 seconds including startup/teardown.
CASE/COALESCE inactive-overflow masking and division-by-zero NULL controls
also pass. CPU-only release compilation and binary loading pass with the frozen
compiler; the host compiler's failure in unchanged jemalloc is an environment
failure, not a code result. The default full repository static-check gate and
the affected closure with `gpu,sirius,sirius_integration` tags pass.
The Sirius-enabled release profile without MO's separate cuVS flag also builds,
packages, initializes the actual GPU runtime and passes the public all-width
round-trip control. Sirius execution remains on the GPU in this profile.

The review traces each changed hunk through the five closure rows above. Q1
retains the existing input-lease publication/release owner and result-batch
cleanup inside fill; query Close joins readers before destruction. Q2 retains
the existing independent query cancellation/deadline and credit-release paths.
Q3 retains 64 MiB windows, bounded descriptors and logical constant expansion;
schema evidence adds at most the already validated 1024 descriptors once per
terminal query. The capability/profile values are immutable before publication;
this change adds no shared mutable state, worker, wait or accumulating registry.
The independent public error/reuse cases close the new error-class path without
changing general frontend error policy.

**Merge blocker:** `TestExactEmbeddedTPCHNativePreparation/q1` crashes in the
pinned importer's `SubstraitToDuckDB::TransformRootOp`. Its root-name iterator
uses `SkipColumnNames` on DuckDB carrier types. Decimal256's private aliased
four-field STRUCT carrier is an opaque Substrait user-defined scalar, so its
limbs must not consume additional SQL root names. Advancing by four skips later
headings and eventually reads outside `RelRoot.names`. The native crash stack
and current importer source identify that exact path. Single-result numeric
tests passing do not close this multi-result failure.

C must remain a draft until a prerequisite importer fix is merged, Sirius pins
that merged importer, and C pins the merged Sirius dependency. Then rerun all
22 native preparations and the full public numeric fixture. No MO-side carrier
name padding, coefficient narrowing, SQL rewrite, fallback or skipped assertion
is an acceptable substitute. D remains dependent on merged and validated C.

The user approved the prerequisite fix PRs on 2026-10-09. Importer
[duckdb-substrait #5](https://github.com/matrixorigin/duckdb-substrait/pull/5)
adds a query-scoped opaque-carrier policy and bounds both root-name consumers;
it is ready for review with distribution CI limits recorded. The Sirius callback and width-preserving following-column
regression are prepared in draft
[Sirius #28](https://github.com/matrixorigin/sirius/pull/28). Its importer pin
remains at the prior merged revision until #5 merges. Joint development binding
validation passes 1009 assertions in 84 cases, including 157 assertions in three
canonical importer cases. Pinned changed-file Sirius hooks pass. These results
are not full C native/public acceptance or proof that the draft builds against
its old importer pin. Merge and pin dependencies in order before C delivery.
CI found an overly strict trailing cursor check; corrected importer head
`6693c3a4d779fb0e15fa07e4ce4166f0572f3952` retains legacy final-STRUCT
headings and checks every actual name read. Its independent value/name regression
passes. Corrected distribution CI has seven failures: the clean base's known
STRUCT field-selection failure/crash and five later failures now independently
reproduced at clean importer `95d9ce8d78490db3991ab6145653716aa3ec42c9`
and its bundled DuckDB `d8cdaa33fda8df955cc76ef58a280f68f4cd43fa`.
The five explicitly selected cases reproduce DISTINCT_FROM, CTE (TPCH/TPCDS),
the old user-defined-literal expected-message mismatch and an empty-plan root
failure. Full distribution CI is not green; C-unit CI remains pending. No SQL
assertion is relaxed, skipped or changed to obtain these results.

## Merged importer follow-up (2026-10-09)

Importer #5 merged as `99c7ca3b6f8f3159239e119ed2982d42f98c4690`, with
the same source tree as the tested final fix. Sirius #28 now pins that merged
revision at head `433cadd43a10442b0825a23234fb6d859207e9d5` and is ready
for review. Its clean frozen-Pixi build passes 852 assertions / 81 native
binding cases and 1468 / 8 production numeric C ABI GPU cases. The independent
C consumer passes real GPU work, credit accounting and runtime cleanup/reuse.
All eight SDK exporter tests and pinned changed-file hooks pass. The regenerated
ABI-v1 SDK records the exact clean head, with all 73 artifact fingerprints,
C header and canonical literal schema verified. Current-head CI is reported
separately; these local results do not imply its compiler jobs passed.

The recorded MO pin remains merged Native B until Sirius #28 merges. Then
advance C to that merged Sirius revision and rerun all-22 native preparation
and the full public numeric fixture. No unmerged native pin or full acceptance
is claimed, and D still follows merged, validated C.

## Consumer corrections after Sirius #28 (2026-10-09)

Sirius #28 merged as `af4dc60152b3e14f263c7fe863b29ac7e154de30`;
C now pins that clean merged revision and merged importer #5. The original
root-name crash is closed. Full native preparation exposed two remaining MO
wire omissions: absent constant fetch bounds and declared nullability on
decimal column references. Emit both constant fetch modes for embedded plans,
using count -1 and offset 0 for absent bounds. Preserve each MO-bound decimal
column descriptor with the existing checked exact cast function: Substrait
field selections alone cannot carry the projected descriptor, and a grouped
aggregate's required internal value can be exposed by a nullable MO projection.
This retains physical width, precision and scale and leaves the result schema
comparison intact. Legacy Flight emission retains its existing wire shape.

Emit condition-free embedded INNER joins as Substrait CrossRel. MO has already
placed their predicates in surrounding filters; JOIN ON true otherwise binds
as DuckDB ANY_JOIN when the exact path intentionally skips its ordinary
optimizer. This changes no join order, multiplicity, predicate or numeric join
key admission and introduces no new native operator.

For embedded DATE extraction, lower the already admitted year/month/day fields
to their direct native functions. DuckDB's generic date_part function is not a
supported Sirius GPU expression. Quarter must decline before readers start
until the native executor supports it. Do not broaden the DATE-only semantic
admission to time durations, session-zone timestamps or tolerant text parsing.

The full public numeric fixture also exposed cuDF AST IS_NULL on Decimal256's
private STRUCT carrier. Correct that in the native expression owner using its
top-level validity mask and the caller's stream/resource, then publish a
separate prerequisite Sirius fix. Keep the public NULL-predicate assertion and
merged-pin delivery gate; do not substitute a SQL rewrite or fallback.
