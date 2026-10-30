# Vector functions and distance semantics

This page is the reference for what the vector functions return, and for what each vector
index computes when it serves an `ORDER BY <distance>(col, q) LIMIT k` query. The BVT cases
`test/distributed/cases/vector/vector_distance_semantics.sql` and (GPU indexes)
`test/distributed/gpu_cases/vector/vector_distance_semantics_gpu.sql` check the behavior
stated here; statements about internals name the code they describe.

## Distances: smaller is nearer, so `inner_product` is negative

Every distance function returns a value where **smaller means nearer**, so
`ORDER BY <distance>(v, q) LIMIT k` (ascending, the default) returns the k nearest rows.
An index serves only that ascending form.

For that reason `inner_product` returns the **negated** dot product, `-Σ aᵢbᵢ`, not the dot
product itself. The most similar vectors have the largest dot product and so the most
negative `inner_product`. This is by design, the same convention as pgvector's `<#>`
operator, and not a sign error (see #29703, #29789, #31667 and #30475).

```sql
SELECT inner_product('[1,2,3]', '[4,5,6]');   -- -32  (dot product 32)
SELECT inner_product('[1,2]', '[-3,-4]');     -- 11   (dot product -11: far apart)
SELECT id FROM t ORDER BY inner_product(v, '[1,2,3]') LIMIT 10;  -- largest dot product first
```

`cosine_similarity` is the one similarity, where larger means nearer. No index serves it; use
`cosine_distance` (`1 - cosine_similarity`) for an index search.

## Distance functions

`a`, `b` are vectors of the same dimension. The result is `DOUBLE`.

| Function | Value | Range | Nearer | Zero vector |
|---|---|---|---|---|
| `inner_product(a, b)` | `-Σ aᵢbᵢ` | any real | smaller | `-0` with a zero vector |
| `cosine_distance(a, b)` | `1 - clamp(a·b / (‖a‖‖b‖), -1, 1)` | `[0, 2]` | smaller | `1` (also for two zero vectors) |
| `cosine_similarity(a, b)` | `clamp(a·b / (‖a‖‖b‖), -1, 1)` | `[-1, 1]` | **larger** | error: `cosine similarity: one of the vector is zero` |
| `l2_distance(a, b)` | `sqrt(Σ (aᵢ - bᵢ)²)` | `≥ 0` | smaller | as the formula |
| `l2_distance_sq(a, b)` | `Σ (aᵢ - bᵢ)²` | `≥ 0` | smaller | as the formula |
| `l1_distance(a, b)` | `Σ \|aᵢ - bᵢ\|` | `≥ 0` | smaller | as the formula |

Example, `a = [1,2,3]`, `b = [4,5,6]`:
- `inner_product`: `-32`
- `cosine_similarity`: `0.9746318459510803`
- `cosine_distance`: `0.025368154048919678`
- `l2_distance`: `5.196152210235596`
- `l2_distance_sq`: `27`
- `l1_distance`: `9`

Common rules:

- **Types.** `a` and `b` are `vecf32`, `vecf64`, `vecbf16`, `vecf16`, `vecint8` or `vecuint8`;
  the overloads take two arguments of the same type. A `vecf32` and a `vecf64` argument are
  also accepted together (one is cast to the other's type).
- **Precision** depends on the vector type:
  - `vecf32`, `vecf64`: every function except `l2_distance_sq` rounds its result into the
    float32 domain (`metric.RoundDistanceToElemDomain`), so a `vecf64` input gives a
    float32-precision value: `l2_distance` of `vecf64` `[3.1]` and `[0]` is
    `3.0999999046325684`, while `l2_distance_sq` of the same pair is `9.610000000000001`.
  - `vecbf16`, `vecf16`, `vecint8`, `vecuint8`: the narrow kernels compute the sums
    (`inner_product`, `l1_distance`, `l2_distance_sq`) in float32 for `vecbf16`/`vecf16` and
    exactly in integers for `vecint8`/`vecuint8`. `l2_distance` and `cosine_distance` are
    computed from those sums in float64 and **not** rounded: `l2_distance` of `vecint8`
    `[3,1]` and `[0,0]` is `3.1622776601683795` (the `vecf32` value is `3.1622776985168457`),
    and of `vecbf16` `[1.5,2.25,3]` and `[0.5,1,7]` it is `4.3084219849035215` (`vecf32`:
    `4.308422088623047`). `cosine_similarity` is computed in float32 for every type.
- **NULL.** A NULL argument gives NULL.
- **Dimension mismatch** is an error:
  - `cosine_similarity` on any type, and `cosine_distance`, `l2_distance`, `l2_distance_sq`
    and `l1_distance` on `vecf32`/`vecf64`: `invalid input: vector ops between different
    dimensions (2, 3) is not permitted.`
  - `inner_product` on any type, and `cosine_distance`, `l2_distance`, `l2_distance_sq` and
    `l1_distance` on `vecbf16`, `vecf16`, `vecint8` and `vecuint8`: `internal error: vector
    dimension not matched`.
- **Overflow.** A result that overflows is an error, e.g. `l2_distance_sq('[1e20,0]',
  '[0,0]')`: `l2 distance: vector magnitude is too large, the result overflows the element
  domain`.
- **Empty vector.** `'[]'` is rejected when the vector is parsed (`malformed vector input`).

## Other vector functions and operators

`T` is the argument's vector type.

| Function or operator | Accepted types | Result |
|---|---|---|
| `summation(v)` | all six | `DOUBLE` |
| `l1_norm(v)`, `l2_norm(v)` | all six | `DOUBLE` |
| `vector_dims(v)` | all six | `BIGINT` |
| `normalize_l2(v)` | all six | `T` for `vecf32`, `vecf64`, `vecbf16`, `vecf16`; **`vecf32` for `vecint8`, `vecuint8`**; a zero vector returns the zero vector |
| `subvector(v, start[, len])` | all six | `T` |
| `abs(v)` | `vecf32`, `vecf64` | `T` |
| `sqrt(v)` | `vecf32`, `vecf64` | **`vecf64`** for both |
| `+`, `-`, `*`, `/` (vector with vector, or vector with a number) | `vecf32`, `vecf64` | `T` |
| `=`, `<>`, `<`, `<=`, `>`, `>=`, `IN` | all six | `BOOL` |
| `coalesce`, `if`, `case` over one vector type | all six | `T` |
| `hex(v)`, `to_base64(v)` | `vecf32`, `vecf64` | `TEXT` |
| `CAST(v AS vecf32(N) / vecf64(N) / vecbf16(N) / vecf16(N) / vecint8(N) / vecuint8(N))` | all six | the target type. To `vecint8`/`vecuint8` each element is rounded to the nearest integer and saturated (`[-300, 300, 1.6]` → `[-128, 127, 2]`); an element that overflows a float target is an error |
| `COUNT(DISTINCT v)`, `GROUP BY v` | all six | |
| `ANY_VALUE(v)` | all six | `T` |
| `MIN`, `MAX`, `SUM`, `AVG` over vectors | none | rejected (`invalid argument aggregate function`) |

The narrow types (`vecbf16`, `vecf16`, `vecint8`, `vecuint8`) are rejected by `abs`, `sqrt`,
the arithmetic operators, `hex` and `to_base64`; cast them to `vecf32` first.

## Indexes: which function each `op_type` serves

An index search replaces `ORDER BY <function>(col, q) LIMIT k` when the function matches the
index's `op_type`. `EXPLAIN` then shows `Vector Index Scan on <index> [<function>]`. The
`ORDER BY` must be ascending; `DESC` is answered without the index.

| SQL function | `op_type` that serves it |
|---|---|
| `l2_distance` | `vector_l2_ops`, `vector_l2sq_ops` |
| `l2_distance_sq` | `vector_l2_ops`, `vector_l2sq_ops` |
| `inner_product` | `vector_ip_ops` |
| `cosine_distance` | `vector_cosine_ops` |
| `l1_distance` | `vector_l1_ops` (IVF-FLAT only) |
| `cosine_similarity` | none |

## Column types and quantization per index

Column types an index can be built on (with any `op_type` the index accepts):

| Index | Vector column types |
|---|---|
| IVF-FLAT | `vecf32`, `vecf64`, `vecbf16`, `vecf16`, `vecint8`, `vecuint8` |
| HNSW | `vecf32`, `vecf64`, `vecbf16`, `vecf16`, `vecint8`, `vecuint8` |
| CAGRA | `vecf32`, `vecf16` only |
| IVF-PQ | `vecf32`, `vecf16` only |

On a `vecint8`/`vecuint8` column, IVF-FLAT serves all five `op_type`s and HNSW serves
`l2`, `l2sq`, `ip` and `cosine`.

`op_type`s accepted with a `QUANTIZATION` (here on a `vecf32` column):

| Index | `float16` | `bf16` | `int8`, `uint8` |
|---|---|---|---|
| IVF-FLAT | `l2`, `l2sq`, `ip`, `cosine`, `l1` | `l2`, `l2sq`, `ip`, `cosine`, `l1` | `l2`, `l2sq` only |
| HNSW | `l2`, `l2sq`, `ip`, `cosine` | `l2`, `l2sq`, `ip`, `cosine` | `l2`, `l2sq` only |
| CAGRA | `l2`, `l2sq`, `ip`, `cosine` | rejected | `l2`, `l2sq` only |
| IVF-PQ | `l2`, `l2sq`, `ip`, `cosine` | rejected | `l2`, `l2sq` only |

`int8`/`uint8` quantization is L2-only in every index because its affine quantizer preserves
L2 geometry only; `ip`, `cosine` and `l1` are rejected with it. This rule is about
quantizing a float column; it does not apply to a `vecint8`/`vecuint8` column.

## What each index computes internally

Every index ranks by a distance where smaller is nearer. The **L2 indexes rank by squared L2**
and never take a square root while searching: the root is monotonic, so it does not change
the order. When the query asks for `l2_distance`, the square root is applied once to the
returned score.

| Index | `op_type`s | Internal metric | Value returned to SQL |
|---|---|---|---|
| IVF-FLAT | `l2`, `l2sq`, `ip`, `cosine`, `l1` | Ranks the probed entries with the SQL distance functions: `l2_distance_sq` for `l2`/`l2sq`, `inner_product`, `cosine_distance`, `l1_distance` (`ivfflat/search.go`) | Without `QUANTIZATION`, the scalar function's value in the float32 domain; for `l2_distance`, the square root of the squared L2. With `QUANTIZATION`, the distance between the quantized entry and the quantized query (see Notes) |
| HNSW (usearch) | `l2`, `l2sq`, `ip`, `cosine` | usearch `L2sq`, `InnerProduct`, `Cosine` (`OpTypeToUsearchMetric`) | Squared L2, or its square root for `l2_distance`; for `ip`, usearch's score minus 1, which is `-dot` (`DistanceTransformHnsw`); cosine distance |
| CAGRA (cuVS, GPU) | `l2`, `l2sq`, `ip`, `cosine` | cuVS `L2Expanded`, `InnerProduct`, `CosineExpanded` (`metric/gpu.go`); the inner-product score is negated in `cgo/cuvs` (`transform_distance`) | Squared L2, or its square root for `l2_distance`; `-dot`; cosine distance |
| IVF-PQ (cuVS, GPU) | `l2`, `l2sq`, `ip`, `cosine` | As CAGRA, on product-quantized codes | cuVS's distance from the compressed codes, transformed as for CAGRA and not re-ranked with the exact vectors (`ivfpq/search_gpu.go`): an **approximate** distance, which can differ from the scalar function, and so can the order |

Notes:

- **Precision.** HNSW and CAGRA return their own float32 score rather than recomputing the
  scalar function. It agrees with the scalar function in the float32 domain but not always
  bitwise: in one test HNSW returned `1.940000057220459` where `l2_distance_sq` returns
  `1.9399999380111694`. A CAGRA cosine distance near 0 carries float32 rounding, so it can
  be slightly below 0 (one test returned `-1.1920928955078125e-7` for two vectors with the
  same direction).
- **Narrow columns.** On a `vecbf16`, `vecf16`, `vecint8` or `vecuint8` column, HNSW and
  IVF-FLAT return a float32 `l2_distance`, while the scalar function returns an unrounded
  float64 one (see Precision): for the `vecint8` row `[-1,0,2]` and the query `[0,0,0]` the
  index returns `2.2360680103302`, the scalar function `2.23606797749979`.
- **Quantization.** With `QUANTIZATION`, an index stores each vector in the quantized type and
  quantizes the query as well, so the score it returns is the distance between the quantized
  vectors, not the scalar function on the stored column. The difference is the quantization
  error, which can be much larger than float32 rounding and can change the order:
  `[1.001,0]` and `[1.002,0]` are both `[1,0]` in `bf16`, so an IVF-FLAT `QUANTIZATION
  'bf16'` index returns `0` where `l2_distance` returns `0.0009999275207519531`.
- **Zero cosine query.** HNSW rejects a zero (or subnormal) query vector for cosine.

## Where it is implemented

- SQL registration: `pkg/sql/plan/function/list_builtIn.go`.
- Scalar functions: `pkg/sql/plan/function/func_binary.go`, `pkg/vectorize/moarray/external.go`.
- Kernels: `pkg/vectorindex/metric/` (`InnerProduct` returns `-dot`).
- `op_type` mapping and score transforms: `pkg/vectorindex/metric/types.go`
  (`OpTypeServesDistFunc`, `DistanceTransformHnsw`, `DistanceTransformIvfflat`).
- cuVS inner-product sign: `cgo/cuvs/index_base.hpp` (`transform_distance`).
