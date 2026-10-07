# Low-precision scalar float types: bf16, float16, float8, float4

Status: accepted · Issue: #20567 · Scope: new SQL scalar column types

## Motivation

AI/ML workloads store weights, activations, and embeddings in narrow floating-point
formats to cut memory and bandwidth. MatrixOne already has the narrow formats as
*vector element* types (`vecbf16`, `vecf16`, ...); this adds them as first-class
**scalar** column types so an ordinary column can hold one low-precision float:

```sql
CREATE TABLE t (a bf16, b float16, c float8, d float4);
```

Previously each of these returned a 1064 syntax error.

## The four types

| SQL type  | Bit layout            | Bytes | Finite range (approx) | Special values |
|-----------|-----------------------|:-----:|-----------------------|----------------|
| `bf16`    | bfloat16 (e8m7)       |   2   | same exponent as f32  | Inf, NaN       |
| `float16` | IEEE binary16 (e5m10) |   2   | ±65504                | Inf, NaN       |
| `float8`  | OCP FP8 **e4m3**      |   1   | ±448                  | NaN only (no Inf) |
| `float4`  | OCP MXFP4 **e2m1**    |   1   | ±6 (values {0,½,1,1½,2,3,4,6}) | none (all codes finite) |

These are the *contract*: the on-disk byte width and the exact bit interpretation
are fixed and must not change once shipped, so persisted data and any future
GPU kernels agree on the encoding.

## Decisions

- **Format choices.** `float8` is FP8 **e4m3** (the widely used inference format:
  NVIDIA Transformer Engine, OCP FP8) rather than e5m2. `float4` is **e2m1**, which
  is bit-compatible with NVIDIA MXFP4 (Blackwell), so the stored bytes are GPU-ready.
  A second FP8 encoding (e5m2) may be added later as its own type; the e4m3 choice
  does not preclude it.
- **Naming.** The SQL keywords are exactly `bf16`, `float16`, `float8`, `float4`.
  No aliases. `float4`/`float8` denote *bit width* (4-bit / 8-bit float), not the
  PostgreSQL `float4`/`float8` synonyms for real/double.
- **No native arithmetic.** These formats have too few bits for direct arithmetic.
  Every operation widens the operand to `float32`, computes on the existing
  float32 path, and rounds the result back into the narrow format. A `bf16`/
  `float16`/`float8`/`float4` value in an expression therefore promotes to
  `float32`; only a stored column is narrowed again. Binary operators widen through
  the operator cast rules; any other function, aggregate or window function without an
  overload for these types resolves by casting the argument to `float32` (the
  implicit-cast table ranks `float32` first, then `float64`), and the JSON aggregates,
  percentiles and RANGE frames with an offset take the argument as `float32`.
  `MIN`/`MAX` and `SUM`/`AVG` also take the argument as `float32` and return what they
  return over a `float32` column: `MIN`/`MAX` a `float32`, `SUM`/`AVG` a `float64`.
  The value window functions (`FIRST_VALUE`, `LAG`, ...) and
  `IF`/`CASE`/`COALESCE`/`IFNULL`/`NULLIF` over one of these types keep the column
  type, so the projections a multi-table `UPDATE` or `INSERT ... ON DUPLICATE
  KEY UPDATE` builds from them write cells of the column's width. Branches of two
  different narrow types widen to `float32`.
- **Comparison with a literal.** A numeric literal compared with a column of these types
  (`=`, `<>`, `!=`, `<=>`, `<`, `<=`, `>`, `>=`, `IN`, `NOT IN`, `BETWEEN`) is rounded to
  the column's type, as the stored
  value was, and the column is compared without a cast, as for `float` and `vecbf16`: a
  row inserted as `-0.1` (stored as `-0.100097656` in `bf16`) matches `WHERE a = -0.1`. A
  prepared parameter (binary protocol or `EXECUTE ... USING @v` with a numeric or
  decimal-numeral value) is rounded the same way when the executed value is inside the
  type's finite range, including inside an `IN` list; such a plan depends on the value
  and is not cached by parameter type. Two literals that round to the same value are the
  same constant for filter simplification (`a = 1.1 AND a = 1` on a `float4` column). A literal
  or parameter outside the finite range, and any other expression, keeps the comparison
  in `float32`/`float64`. Inside or outside is decided on the value `CAST` rounds the source
  to (to odd, from the exact decimal or text value), so a value `CAST` rejects is never
  narrowed: `f <= 6.0000001` on a `float4` column, or `g <= 448.000001` on a `float8`
  column, compares wide although float32 rounds those literals to the maximum. For `<`/`>` this follows the column's grid: a literal that
  rounds down compares as its rounded value.
- **One zero.** A zero of either sign is stored as `+0` (code 0 in every format) by
  casts, writes, `LOAD` and user variables, so equal values have equal bits for hashing
  (`GROUP BY`, `DISTINCT`, joins) and equality. Peer groups (window `PARTITION BY`,
  `ORDER BY` ties) compare the float32 value.
- **Rounding and range.** A value rounds once, to nearest even, from its exact value:
  a float32 source directly; a wider source (float64, decimal, an integer above 2^24,
  decimal text) through float32 rounded to odd from the exact value — the integer's bits,
  the decimal's or the text's exact rational value — which keeps the information the
  final rounding needs, so text far beyond float64 precision still rounds on the right
  side of a tie. Vector elements (`vecbf16`, `vecf16`, `vecf64` casts to narrower vectors)
  round the same way. An out-of-range error names the value given. Every
  cast and write rejects a value outside the type's finite range with a "data out of
  range" error (e.g. `float8` above ±448, `float4` above ±6) and NaN or ±Inf with an
  invalid-input error, so no SQL path stores a saturated or non-finite value. The
  codec's own float32 conversion saturates and keeps NaN where the format has a NaN
  slot; it is only reached after these checks.
- **Casts.** Each type casts to and from `float32`, `float64`, decimal, the integer
  types, and character strings, rounding once as above, with the same range and
  finiteness checks; widening to `float32`/`float64` is exact.
- **Not a key.** A `bf16`/`float16`/`float8`/`float4` column cannot be part of a primary
  key, unique key, secondary index or `CLUSTER BY` key; DDL rejects it. Key encoding,
  row locking and TN merge/dedup have no support for these types.
- **External forms.** CDC and ISCP SQL, `SELECT … INTO OUTFILE` (CSV and JSON), external
  writes, JSON values and data-branch diff/merge use the widened float32 value; every
  value of these types is exact in float32, so the text converts back to the same bits.
  Marshalled vectors (WAL, batches sent between services) carry the raw 1-/2-byte
  encoding.

## Invariants

- **Engine paths.** Sorting and top-k comparators, window partitioning, the value
  window functions, `ON DUPLICATE KEY UPDATE` change detection, the change reader
  (`table_changes`, CDC), marshalled vectors and parquet `LOAD`/export handle these
  types as fixed-size 2-byte (`bf16`, `float16`) or 1-byte (`float8`, `float4`) values.

- **Ordering is by value, not by bits.** Sorting, comparison, `MIN`/`MAX`, range
  and zonemap pruning order these columns by their float value. The raw 1-/2-byte
  encodings do not sort monotonically (the sign bit inverts negative ordering), so
  any comparison path must widen to float before comparing.
- **Aggregation accumulates in a wide type.** `SUM`/`AVG` run on the `float32`
  argument and accumulate and return `float64`; no aggregate rounds back to the
  narrow type.
- **Precision loss is expected and one-directional.** Writing a value that the
  format cannot represent exactly stores the nearest representable value; reading
  it back yields that stored value. Round-tripping a value already in the format
  is exact.
- **Presentation.** On the wire and in `information_schema` the column reports its
  own type name; values render as their widened float.

## Out of scope (separate work)

- **GPU-accelerated float4/float8 compute** (distance kernels, low-precision vector
  indexes) depends on NVIDIA/cuVS FP4/FP8 support and is GPU-build-gated; the CPU
  scalar types here carry the GPU-compatible bit layout so that work can build on
  them later.
- **Sparse vectors (`sparsevec`)** — a distinct, variable-length type family with
  its own storage format and distance kernels — are deferred to a separate design
  and PR.
