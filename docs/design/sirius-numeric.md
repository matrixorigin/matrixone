# Exact numeric semantics for embedded Sirius

Design version: 1.

Owner: MatrixOne query planning and Sirius execution.

Tracking: [#28968](https://github.com/matrixorigin/matrixone/issues/28968).
Parent migration: [#28966](https://github.com/matrixorigin/matrixone/issues/28966).

Status: proposed for design review. This N0 revision records the contract and
the current inventory only. It is not approval to lower another numeric
signature, change eligibility, or enable embedded execution by default.

## 1. Decision and invariant

Sirius must execute MatrixOne exact numerics with the MatrixOne planner's
physical type, declared precision, scale, nullability, value, rounding,
overflow, division-by-zero, and result-metadata semantics. The invariant is:

> For every admitted row and every exact-numeric expression, embedded Sirius
> produces the same value or MatrixOne error class and the same result metadata
> as native MatrixOne execution under the same statement and session mode.

The negation is any value, NULL bit, error class, column type, precision, scale,
or nullability difference. A value-only comparison is insufficient.

The selected representation is a versioned Substrait user-defined exact
decimal type and function family. It covers MatrixOne `DECIMAL64`,
`DECIMAL128`, and `DECIMAL256` uniformly. Sirius implements their semantics
natively. It must not convert an exact value or intermediate to floating point,
narrow a coefficient or declared type, execute an exact-numeric fragment back
in MatrixOne, or retry/fall back after embedded execution has been selected.

Until the complete capability is approved and implemented, the exporter keeps
declining unsupported signatures before readers, leases, or native execution
start. Such declines are blockers for [#28968](https://github.com/matrixorigin/matrixone/issues/28968)
and the default cutover in [#28966](https://github.com/matrixorigin/matrixone/issues/28966),
not evidence of parity.

### Goals

- Preserve all exact-numeric semantics exercised by canonical TPC-H Q1-Q22.
- Preserve MatrixOne's wider arithmetic and aggregate domains, including
  `DECIMAL256` intermediates and results.
- Make unsupported signatures explicit and side-effect-free at preparation.
- Provide independent value, error, NULL, and result-metadata evidence before
  production lowering is enabled.

### Non-goals

- Changing MatrixOne's numeric type derivation or SQL-visible behavior.
- Making approximate `FLOAT` or `DOUBLE` behavior an oracle for `DECIMAL`.
- Adding arbitrary-precision SQL numerics beyond MatrixOne's current domains.
- Splitting one exact-numeric expression tree between Sirius and MatrixOne.
- Changing on-disk table formats, Flight compatibility, or the migration
  lifecycle defined in `docs/design/sirius-embedded.md`.

## 2. Current exporter inventory

`pkg/sql/plan/substrait/numeric_inventory_test.go` is the executable inventory.
It binds every canonical TPC-H query, records the exact exporter outcome and
failure, and keeps eligible queries as positive controls with exact output
metadata.

| Queries | Current result | Stable reason and representative evidence |
| --- | --- | --- |
| Q2, Q4, Q12, Q13, Q16, Q18, Q21, Q22 | Eligible | Positive controls include `MIN(DECIMAL64(15,2))`, `SUM(BIGINT) -> DECIMAL128(38,0)`, `SUM(DECIMAL64(15,2)) -> DECIMAL128(37,2)`, and `AVG(DECIMAL64(15,2)) -> DECIMAL128(19,6)`. |
| Q1, Q3, Q5, Q7, Q9, Q10, Q14, Q15, Q19 | Declined | `expression`: a wide `SUM` has no declared Sirius semantic equivalence; common paths publish `DECIMAL256(65,4)`. |
| Q6, Q11 | Declined | `type`: `DECIMAL256` is unsupported; `SUM(DECIMAL128(38,...))` widens to `DECIMAL256(60,...)`. |
| Q8 | Declined | `expression`: an internally widened `DECIMAL256 * DECIMAL256 -> DECIMAL256` signature is unsupported. |
| Q17, Q20 | Declined | `expression`: mixed precision/scale multiply overloads have no declared Sirius semantic equivalence. |

This is 8 eligible and 14 declined queries. The first reported error is not a
complete numeric-needs list: an earlier aggregate admission failure can hide a
later multiply or division. Implementation planning must inspect the complete
bound expression graph, while the inventory continues to pin the public first
failure.

## 3. Logical and physical representation

The new Substrait extension URI is `urn:matrixone:sirius:exact-decimal:v1` and
its user-defined type name is `mo_exact_decimal`. Its ordered integer
parameters are:

1. physical coefficient width: `64`, `128`, or `256` bits;
2. declared precision;
3. scale.

Substrait nullability remains on the type. The physical width is not inferred
from precision: the planner can deliberately use `DECIMAL256(15,2)` as an
internal cast domain. Each signature therefore compares all four fields:
physical width, precision, scale, and nullability.

The valid type domains are precision 1..18 for a 64-bit coefficient, 1..38 for
a 128-bit coefficient, and 1..76 for a 256-bit coefficient, always with
`0 <= scale <= precision`. A public 256-bit result additionally requires
precision at most 65. Non-canonical parameters are rejected during preparation
rather than normalized by either endpoint.

The coefficient is a signed two's-complement scaled integer. Native input,
result, and literal encodings use exactly 8, 16, or 32 bytes in little-endian
limb order, with an independent validity bitmap. A literal uses the `Any` type
URL `type.googleapis.com/matrixone.sirius.numeric.v1.ExactDecimalLiteral`.
That message contains one `bytes coefficient_le` field whose length must match
the physical-width parameter; precision and scale live only in the type.
Variable-length numeric strings, floating-point encodings, and host-dependent
C/C++ object layouts are forbidden at this boundary.

MatrixOne's public SQL decimal precision is capped at 65. `DECIMAL256` also has
a 76-digit physical working envelope, visible in internal casts used to avoid
premature overflow. Sirius must preserve those internal types and the signed
256-bit physical range, but must enforce a public result's declared precision
at its publication boundary. It must not mistake an internal precision of 76
for permission to publish a 76-digit SQL result.

All exact-decimal scans, literals, casts, scalar functions, aggregates,
comparisons, grouping keys, join keys, sort keys, and results in one admitted
expression closure use this type. Using standard Substrait `decimal` for one
part and the extension for another would let DuckDB silently choose different
coercion or result rules and is not allowed.

The same extension URI owns the v1 function names
`mo_decimal_add`, `mo_decimal_subtract`, `mo_decimal_multiply`,
`mo_decimal_divide`, `mo_decimal_integer_divide`, `mo_decimal_modulo`,
`mo_decimal_negate`, `mo_decimal_cast`, `mo_decimal_equal`,
`mo_decimal_not_equal`, `mo_decimal_less`, `mo_decimal_less_equal`,
`mo_decimal_greater`, `mo_decimal_greater_equal`, `mo_decimal_sum`,
`mo_decimal_avg`, `mo_decimal_min`, and `mo_decimal_max`. `CASE`, grouping,
sorting, and joins consume the same type and comparison kernels. The full
input and output type parameters form the signature; matching a function name
alone is never sufficient.

## 4. MatrixOne semantic contract

The bound MatrixOne plan is the authority for operand casts and result type
metadata. Sirius validates that type against the negotiated signature and then
computes into that exact type. It does not independently derive a more
convenient DuckDB type.

### 4.1 Precision and scale

The capability must cover these existing MatrixOne rules:

- Addition and subtraction align operands exactly. For a widened
  `DECIMAL256` result, scale is `max(s1, s2)` and precision is
  `min(max(p1-s1, p2-s2) + scale + 1, 65)`. Narrower planner results retain
  their published `DECIMAL64` or `DECIMAL128` envelope.
- Multiplication uses
  `result_scale = min(s1+s2, max(12, s1, s2))`. A widened result precision is
  `min(p1+p2, 65)`; narrower multiplication currently publishes a
  `DECIMAL128(38,result_scale)` result.
- Division uses
  `result_scale = max(s1, min(12, s1+6))` and publishes precision 38 or 65
  according to the selected MatrixOne physical result domain.
- `SUM(DECIMAL(p,s))` preserves scale and reserves 22 aggregate digits:
  precision is `min(p+22,65)`. It promotes to `DECIMAL256` when precision
  exceeds 38.
- `AVG` preserves the input's integer capacity while normally adding four
  fractional digits. Decimal scale is capped at 38 but is never reduced below
  a valid input scale; the result promotes to `DECIMAL256` when the
  `DECIMAL128` domain is insufficient.
- `MIN` and `MAX` preserve the argument type. Comparisons return boolean but
  must compare exact values after MatrixOne's scale alignment. Casts, `CASE`,
  grouping, joins, and sort keys retain the exact bound operand/result types.

These formulas document current behavior and its counterexamples. The emitted
plan still carries the result type explicitly, and Sirius must reject a
signature whose declared result disagrees with it.

### 4.2 Rounding

Scale reduction and ordinary exact division round the discarded magnitude
half up, then restore the sign; a tie therefore rounds away from zero. For
example, reducing `1.25` and `-1.25` to scale one yields `1.3` and `-1.3`.
`DIV` and scale-truncating operations truncate toward zero instead. There is
one rounding point at the MatrixOne-defined result scale: chunked scaling,
partial aggregation, device transfer, and output conversion must not introduce
an earlier or second rounding step.

Each cast or function signature is admitted only after boundary tests prove
its MatrixOne rounding mode. A generic C++/DuckDB cast is not accepted merely
because typical values agree.

### 4.3 Overflow

For a declared SQL `DECIMAL(p,s)` result, a non-NULL coefficient is valid only
when its magnitude is less than `10^p`. A result outside that domain returns
the same MatrixOne numeric error class. It must not wrap, saturate, clamp
metadata, become NULL, become infinity, or trigger local replay.

Intermediate arithmetic uses the full selected MatrixOne physical domain.
`SUM` uses a full `DECIMAL256` partial state when required so values can cancel
across input batches and merge order does not create a false intermediate
overflow. The declared precision check occurs once when the aggregate result
is published. Ordinary scalar results enforce their declared precision before
the row is published.

The positive 65-digit boundary and its negative counterpart must succeed when
representable; the corresponding 66-digit result must fail. Physical signed
256-bit overflow is also an error even if a later scale reduction might have
made a wrapped value appear small.

### 4.4 Division and zero

Division first propagates an input NULL or an execution mask. For an evaluated
non-NULL zero divisor, the statement kind is part of the semantic contract:

- `SELECT` produces NULL regardless of strict SQL mode;
- `INSERT` and `UPDATE` return MatrixOne's division-by-zero error only when
  strict mode and `ERROR_FOR_DIVISION_BY_ZERO` are both active;
- `INSERT IGNORE`, non-strict DML, a NULL operand, and a masked row produce
  NULL and must not raise that error.

The result uses the scale formula above and half-away-from-zero rounding.
`DIV` is separate: it truncates toward zero and returns its MatrixOne integer
domain. Modulo preserves MatrixOne's aligned-scale remainder behavior. A
native failure after any result becomes visible is terminal and cannot restart
the query on MatrixOne.

### 4.5 NULL and aggregate state

Exact arithmetic, comparisons, and casts retain their MatrixOne strictness:
an input NULL produces NULL unless the function's SQL contract says otherwise.
`SUM`, `AVG`, `MIN`, and `MAX` ignore NULL inputs and return NULL for an empty
or all-NULL group where MatrixOne does. Grouping treats NULL according to SQL
group-key semantics.

Sirius must use the plan's output nullability, not infer it from a sampled
batch. In particular, a scalar aggregate over a non-nullable input can still
have a nullable output because the input relation may be empty.

### 4.6 Result metadata

For every output ordinal Sirius returns, and the Go result decoder restores,
the exact MatrixOne:

- physical OID (`DECIMAL64`, `DECIMAL128`, or `DECIMAL256`);
- precision and scale;
- nullable/required flag and row validity;
- heading and ordinal.

Trailing fractional zeroes are represented by scale metadata, not discarded.
Two equal coefficients tagged `DECIMAL(10,2)` and `DECIMAL(12,4)` are not
metadata-equivalent. MySQL protocol metadata, CTAS schema derivation, prepared
results, and downstream expressions must observe the native MatrixOne type.

## 5. Planning, capability, and failure flow

The first owner is the MatrixOne exporter. The Sirius runtime advertises one
exact capability version plus the complete scalar, aggregate, cast, and
comparison signature set. Preparation proceeds as follows:

1. MatrixOne binds the query and derives every operand and result type.
2. The exporter validates the complete reachable expression graph against the
   exact-decimal v1 capability, including working types and output metadata.
3. It emits extension type/function anchors only if every signature is
   supported. Partial exact-numeric offload is forbidden.
4. Sirius validates the anchors, parameters, schemas, and capability version
   again before accepting a query handle.
5. Only then may MatrixOne start readers or publish a storage lease.

An unknown version, missing signature, invalid precision/scale, coefficient
width mismatch, or unsupported session mode is a typed not-eligible result at
preparation. An explicitly selected embedded backend surfaces that decline; it
does not report success after CPU, Flight, or native-MatrixOne fallback.
Operational failures after preparation are execution errors and never
eligibility declines.

No persisted format changes. Mixed MO/Sirius revisions negotiate capability;
if either side lacks exact-decimal v1, preparation fails before data access.
Rollback disables the capability and restores the current explicit declines.

## 6. Counterexamples and independent oracles

| Semantic boundary | Smallest counterexample | Required oracle |
| --- | --- | --- |
| No floating conversion | `9007199254740993` and `9007199254740992` | Values remain distinct through scan, arithmetic, grouping, and result. |
| No Decimal128 narrowing | 38-digit maximum plus one | Exact `DECIMAL256(39,0)` result, not overflow or rounding. |
| Signed tie rounding | `1.25`, `-1.25` reduced to scale one | `1.3`, `-1.3`. |
| Multiply result domain | 38-digit value times a 20-digit value | Exact widened result or declared-precision overflow at the same boundary as MO. |
| Division scale | `DECIMAL(...,2) 1.00 / 8` | Value `0.12500000` with the planned scale eight. |
| Division by zero | `SELECT 1/0`; strict `INSERT` of `1/0`; `INSERT IGNORE`; `NULL/0`; masked `1/0` | SELECT/IGNORE/NULL/masked cases are NULL; only qualifying strict DML errors. |
| Aggregate cancellation | `max + max - max - max` across separate partial states | Exact zero independent of merge order. |
| Public precision bound | Largest 65-digit magnitude and a 66-digit result | Boundary succeeds; overflow has the MatrixOne error class. |
| Empty aggregate | `SUM` and `AVG` over zero rows | NULL value and nullable result metadata. |
| Metadata identity | Equal coefficient under `(10,2)` and `(12,4)` | Precision, scale, OID, and protocol metadata remain distinct. |

The black-box oracle is native MatrixOne SQL execution with exact values,
errors, NULLs, and client metadata captured independently from Sirius. The
white-box oracle inspects bound MatrixOne expression types and decoded
Substrait extension types. Expected results must not be computed by the Sirius
kernel under test.

## 7. Resource and performance contract

- Decimal64, Decimal128, and Decimal256 buffers remain fixed-width 8, 16, and
  32 bytes. The parent design's 64 MiB input/result windows include these
  buffers, validity bitmaps, descriptors, and codec scratch.
- Exact arithmetic runs in Sirius native/device kernels. There is no per-row
  host callback, heap allocation, string conversion, or CPU fallback.
- Aggregate state is bounded per admitted group and charged before allocation.
  A wide `SUM`/`AVG` state uses at least one 32-byte coefficient plus its
  count/validity metadata and participates in existing spill/admission.
- Plan/type overhead is linear in expression count and remains under the
  existing 16 MiB plan limit.
- Implementation PRs report decimal-kernel throughput and numeric-heavy TPC-H
  wall time against the same Sirius revision. More than 10% regression on the
  relevant kernel or query blocks rollout unless the design review accepts a
  documented correctness/performance tradeoff. Correctness gates cannot be
  waived for performance.

No new trust boundary is introduced. Type parameters, lengths, and coefficient
widths are validated before allocation; malformed plans cannot request
unbounded precision or buffers. Errors never include row data beyond the
existing bounded MatrixOne numeric diagnostic contract.

## 8. Alternatives

| Alternative | Decision |
| --- | --- |
| Convert wide decimals to `DOUBLE` | Rejected: loses integers above 53 bits, changes rounding, grouping, comparison, overflow, and metadata. |
| Clamp all numerics to Substrait/DuckDB decimal precision 38 | Rejected: Q1/Q3/Q5 and other plans require wider intermediates and aggregates. |
| Execute only the unsupported numeric fragment in MatrixOne | Rejected: creates an unbounded split boundary, duplicate scheduling, and fallback after admission. |
| Use standard Substrait decimal for values up to 38 and the extension only above 38 | Rejected: one expression could cross semantic engines and inherit different casts or result rules. |
| Versioned exact-decimal user-defined type and functions | Selected: preserves the complete type identity and makes capability negotiation and rejection explicit. |

## 9. Delivery and approval gates

Numeric work is separate from the ten-PR embedding map in
`docs/design/sirius-embedded.md`:

1. **N0 (this change):** design and exact Q1-Q22 exporter inventory only.
2. **Native contract:** Sirius type/function extension, fixed-width buffers,
   exact kernels, aggregate state, errors, and capability advertisement.
3. **MO lowering:** exporter and bridge support after the native contract is
   merged and pinned.
4. **Parity and rollout:** public SQL differential evidence, all-22 TPC-H,
   metadata, GPU, failure, and performance gates.

There is a hard approval gate between N0 and production lowering. Before any
MO or Sirius production path emits or accepts `mo_exact_decimal`, reviewers
must approve an exact revision of this document and record that revision in
the implementation PR. Approval requires agreement on the extension schema,
all semantic rules above, error mapping, capability version, and validation
matrix. A draft, issue comment, or passing inventory test is not approval.

Before embedded Sirius can become the default, evidence at the pinned MO and
Sirius revisions must show:

- all 22 canonical TPC-H queries are eligible without float conversion,
  narrowing, local execution, fallback, or skipped numeric assertions;
- native MO and Sirius values, NULLs, errors, headings, OIDs, precision, scale,
  and nullability match for the counterexample matrix;
- focused MO exporter/decoder tests, Sirius kernel tests, aggregate merge-order
  tests, and the real embedded public SQL path pass;
- malformed capability/type inputs fail before readers start;
- the GPU and resource/performance budgets above pass; and
- no result-visible or operational failure replays the query elsewhere.

N0 has no SQL-visible behavior change, so no BVT is added here. The production
lowering cannot use that exemption: it requires a public embedded SQL parity
case in addition to package tests.
