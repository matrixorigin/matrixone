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
