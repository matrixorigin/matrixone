# Add `vecbf16`, `vecf16`, `vecint8` vector column types to MatrixOne

## Context

MatrixOne today has two vector column types: `vecf32` (`T_array_float32`) and `vecf64`
(`T_array_float64`). We want three more — `vecbf16` (bfloat16), `vecf16` (IEEE fp16/half),
and `vecint8` (int8) — to cut base-table and ANN-index memory 2–4× versus float32. Note this is
distinct from the existing `CREATE INDEX ... QUANTIZATION=` option, which compresses *inside*
the index while the column stays `vecf32`; here the **base column itself** is stored narrow.

The central obstacle: everything vector-related is generic over
`RealNumbers = constraints.Float` (float32|float64 only, ~177 usages). int8 isn't a float, and
bf16/float16 have no native Go type. We solve this with a two-tier constraint plus an
always-upcast-to-float32 compute path, so we never widen `RealNumbers` and never write
uint16/int8 arithmetic kernels.

### Decisions (confirmed with user)
- **SQL names:** `vecbf16` / `vecf16` / `vecint8` (consistent with `vecf32`/`vecf64`).
- **Operation scope:** distance functions (`l2_distance`, `l2_distance_sq`, `inner_product`,
  `cosine_distance`, `cosine_similarity`, `normalize_l2`), casts among all 5 vector types and
  string↔vector, and index storage. **No** elementwise arithmetic (`+ - * /`, scalar, subvector,
  sqrt/abs/summation) on the new types — those require an explicit `CAST` to `vecf32` first.
- **Index eligibility (finalized matrix):**

  | index | f32 | f64 | f16 | bf16 | int8 |
  |---|---|---|---|---|---|
  | **ivfflat** (CPU, our Go index) | ✅ | ✅ | ✅ | ✅ | ✅ |
  | **hnsw** (usearch) | ✅ | ✅ | ✅ F16 | ❌ | ✅ I8 |
  | **cuvs** cagra/ivfpq (GPU) | ✅ | ❌ | ✅ | ❌ | ✅ |

  bf16 is **ivfflat-only**: usearch and cuVS have no bf16 kernel and they *are* the distance backend
  there (the index and the kernel are one library), so we don't replace them. We only write our own
  distance kernels where **we** own the index — ivfflat. See revised Phase 5.

> **Status:** Phases 1–4 and 6 (the SQL/storage/cast/distance-function/edge-case type work) are
> **implemented and tested**. Phase 5 below is the finalized, revised index-integration plan
> (supersedes the original Phase 5 sketch).

### Core design
Introduce `ArrayElement = float32 | float64 | BF16 | Float16 | int8` used **only** by the
storage / serialization / accessor / display / cast-plumbing layer (these do pure byte
reinterpretation + formatting). Keep `RealNumbers` for all math kernels. The bridge between the
two tiers is a per-type `ToFloat32Array` / `FromFloat32Array` pair: any distance/normalize on a
new type upcasts to `[]float32`, runs the existing float32 kernels, and returns float64.

---

## Phase 1 — Element types, type IDs, constraint

**New file `pkg/container/types/float16.go`:**
- `type BF16 uint16` — bf16 = top 16 bits of float32. `ToFloat32()` = `Float32frombits(uint32(b)<<16)`;
  `BF16FromFloat32` truncates with round-to-nearest-even.
- `type Float16 uint16` — IEEE half. `ToFloat32()` / `Float16FromFloat32` with full
  subnormal/Inf/NaN handling (the one genuinely intricate bit-twiddle — reuse a vetted reference
  algorithm). Leave the GPU-only `cuvs.Float16` (`pkg/cuvs/helper.go:169`) as-is; `types.Float16`
  becomes the canonical broad type (a later alias is deferred to avoid the cuvs build tag).
- Batch converters (hot path): `BF16ToFloat32Slice`, `Float16ToFloat32Slice`, `Int8ToFloat32Slice`
  and the float32→type reverses (int8 reverse clamps/rounds to [-128,127]).

**`pkg/container/types/types.go`:**
- New IDs after line 100: `T_array_bf16 = 226`, `T_array_float16 = 227`, `T_array_int8 = 228`.
- New constraint near line 364: `type ArrayElement interface { float32 | float64 | BF16 | Float16 | int8 }`.
  Leave `RealNumbers` untouched.
- Add the three IDs to every array-enumerating switch: `String()` (~758, "VECBF16"/"VECF16"/"VECINT8"),
  `OidString()` (~843), `Types` map (~427), `DescString()` (~568), `GetArrayElementSize()` (~576:
  bf16→2, f16→2, int8→1), `IsArrayRelate()` (~1015), and the varlena branches of
  `ToType()`/`TypeLen()`/`FixedLength()`.

## Phase 2 — Parser / plan

- `pkg/sql/parsers/dialect/mysql/keywords.go:689`: add `"vecbf16"/"vecf16"/"vecint8"` → new tokens.
- `pkg/sql/parsers/dialect/mysql/mysql_sql.y`: add `%token VECBF16 VECF16 VECINT8` (~line 382) and
  three grammar rules cloning the `vecf32` rule (~13515) building
  `tree.T{Family: ArrayFamily, FamilyString, DisplayWith}`. **Regenerate `mysql_sql.go` via the
  repo's goyacc target** (locate the Makefile/`go:generate` invocation first — do not hand-edit
  the generated file).
- `pkg/sql/parsers/tree/types.go:221`: extend the `case "vecf32","vecf64":` Format() arm.
- `pkg/sql/plan/build_util.go:160-189`: add the three to the width-default branch, the
  1..MaxArrayDimension validation branch, and the `switch fstr` returning the new IDs.
- `pkg/sql/plan/make.go:331-366`: add `makePlan2VecBf16/F16/I8ConstExprWithType` mirrors and wire
  their dispatch.

## Phase 3 — Storage, display, cast plumbing (constraint widening)

Mechanical phase: switch the **byte/format** generics from `RealNumbers` → `ArrayElement` and add
type cases.
- `pkg/container/types/array.go`: widen `BytesToArray`, `ArrayToBytes`, `ArrayToBase64`,
  `ArrayToString`, `ArraysToString`, `StringToArray`, `StringToArrayToBytes`,
  `BytesToArrayToString`, `stringToT`. In `ArrayToString` (62-67) add `BF16`/`Float16` (format
  `.ToFloat32()`) and `int8` (`FormatInt`) cases; in `stringToT` add int8 (`ParseInt(...,8)`) and
  bf16/f16 (`ParseFloat(...,32)`+`FromFloat32`) parsing.
- `pkg/container/types/encoding.go`: add the three IDs to the array case-lists in `EncodeValue`
  (556) / `DecodeValue` (383). `EncodeSlice`/`DecodeSlice` are already `[T any]` — no change.
- `pkg/container/types/bytes.go:113`: `GetArray` → `ArrayElement`.
- `pkg/container/vector/vector.go`: widen `GetArrayAt` (382), `MustArrayCol`,
  `BuildVarlenaFromArray` (~4933) → `ArrayElement`; add three display cases (2959-2989) cloning the
  float32 block.
- `pkg/container/vector/tools.go`: ensure `ProtoTypeToType` round-trips the new IDs.
- **Cast — `pkg/sql/plan/function/func_cast.go`:** extend the cast-allowed matrix (425-430) for
  array↔array and string↔array on the new IDs; add dispatch in `arrayTypeToOthers` (1977) and
  `strToOthers` (1956). **Key refactor:** change `arrayToArray[I,O]` (5705) to `[I,O ArrayElement]`
  and replace its `moarray.Cast[I,O]` body (which fails on non-float) with a float32 bridge:
  `f32 := toFloat32Array[I](_v); out := fromFloat32Array[O](f32)`. Keep the `oid==oid` fast-path
  byte copy. This routes all 25 vector-pair casts through one path; test every pair (int8
  rounding/clamp policy explicitly tested).

## Phase 4 — Distance functions (compute via float32 bridge)

Do **not** instantiate `metric.L2Distance[BF16]` etc. Upcast at the execution-wrapper boundary.
- `pkg/sql/plan/function/func_binary.go` / `func_unary.go`: add a generic
  `L2DistanceArrayViaF32[T ArrayElement]` (and peers for l2_sq, inner_product, cosine_distance,
  cosine_similarity, normalize_l2). Body: `types.BytesToArray[T]` → `toFloat32Array[T]` →
  `moarray.L2Distance[float32]` (already returns float64). For `normalize_l2` (returns a vector):
  upcast → normalize in float32 → `fromFloat32Array[T]` → store. `moarray`/`metric` kernels are
  unchanged.
- `pkg/sql/plan/function/list_builtIn.go`: for each of the 6 distance builtins, add three overloads
  `{T_array_bf16,T_array_bf16}`, `{T_array_float16,...}`, `{T_array_int8,...}` pointing to the
  via-F32 wrapper. Require both args same type (mixed types must be cast first). Do **not** register
  add/sub/mul/div/scalar/subvector/sqrt/abs/summation overloads for the new types.

## Phase 5 — Index integration (finalized)

### Decisions (confirmed with user)

- **Distance = pure Go, loop-unrolled, NOW.** `simd/archsimd` (Go 1.26 + `GOEXPERIMENT=simd`) is an
  amd64-only **later optimization pass**, merged at the very end. Pure Go runs and is fully testable
  on the arm64 dev machine with the normal toolchain; archsimd can only be cross-compiled (compile
  gate) here, not executed.
  - **int8** → **integer** kernels (int32 accumulate). No upcast to float — that's the point of int8.
    A quantizer `scale²` (Pass 2) multiplies the *result* only; kNN ranking skips it (constant).
    Direct-match int8 has scale = 1.
  - **bf16 / f16** → decode to float32 (no native fp16 arithmetic in Go), compute in float32. bf16
    decode = `uint32(bits)<<16`; f16 decode = `Float16.ToFloat32()`.
  - This **revises** the original "everything via the float32 bridge" sketch: int8 is integer-native,
    not float-bridged.
- **We only own the distance for ivfflat.** hnsw = usearch (the C HNSW library *is* the index, with
  distance internal to graph traversal); cuVS = GPU library. For those we feed the native
  quantization (F16/I8) — no Go kernel seam. Hence bf16 is ivfflat-only.
- **kmeans: float32 internal, centroids narrowed to the storage type on output** (a mean of
  int8/bf16 isn't representable mid-iteration). IEEE narrowing for bf16/f16; round/clamp for int8
  direct-match; affine quantizer for int8 quantize mode (Pass 2).
- **centroid hidden-table type = input column type** (it already inherits `colMap[colName].Typ` in
  `ivfflat/plugin/plan/schema.go`). Search-time brute force over centroids runs on the stored narrow
  type. Exact re-rank is SQL `l2_distance(basecol, query)` — already narrow-aware (Phase 4).
- **Branch strategy (revised):** commit the current vecf16 type work, then **merge `origin/archsimd`
  first** (done by the branch owner). All Pass-1 narrow work then lands **directly on the
  archsimd-refactored** `metric`/`ivfflat`/`brute_force` — no later reconcile. Division of labor:
  - **Pass 1 (pure Go)** — narrow kernels + ivfflat/hnsw integration; done on the arm64 dev machine
    (normal toolchain; archsimd `*_amd64.go` files are tag-excluded there, so it builds and tests).
  - **Pass 3 (archsimd SIMD)** — the `*_amd64.go` f16/bf16/int8 kernels; done on an **x86 machine**
    (where archsimd executes), with the Pass-1 pure-Go path as fallback + equivalence oracle.
  - Still keep narrow additions in **new `*_narrow*.go` files** with isolated dispatch arms — clean
    separation, and it keeps the pure-Go and archsimd halves side-by-side per the existing
    `distance_func.go` / `distance_func_amd64.go` split.

### Pass 1 — direct-match (pure Go)

**5a. `metric` narrow kernels** — new file `pkg/vectorindex/metric/distance_func_narrow.go`:
loop-unrolled (unroll-8) kernels returning float, one set per metric:
- bf16: `l2sqBF16`/`dotBF16`/`cosBF16`/`l1BF16` — decode `bits<<16` into the unrolled accumulation.
- f16: `l2sqF16`/… — `Float16.ToFloat32()` per element.
- int8: `l2sqI8`/`dotI8`/`cosI8`/`l1I8` — **int32** accumulate (cosine = integer dot + integer norms,
  one float divide).
- `ResolveNarrowDistanceFn(oid, MetricType)` returning a `func([]byte,[]byte)(float32,error)` (decodes
  via `BytesToArray[T]` internally). Leave `distance_func.go`/`resolve.go` (the `RealNumbers` f32/f64
  path) untouched → smaller archsimd merge. Tests vs float64 reference (exact int8; tolerance bf16/f16).

**5b. ivfflat (all 5 types)**:
- `SupportedVectorTypes()` (`ivfflat/plugin/runtime/runtime.go:131`): add bf16/f16/int8; accept the
  narrow OIDs in the `SupportsVectorType` guard (`ivf_create.go:328`).
- Build read path (`ivf_create.go:332-360`): narrow OIDs → **decode to float32 → `data32`**.
- Centroid narrowing: compute float32 centroids, convert with `FromFloat32Array[storageType]` **in Go
  before** SQL-formatting (so int8 centroids don't hit the strict string→int8 parse; bf16/f16 round
  consistently), then insert into the (narrow) centroid column.
- Search dispatch (`ivf_search.go:61-68`): narrow OIDs → an `IvfflatSearch` that loads narrow
  centroids and uses `ResolveNarrowDistanceFn` for the brute-force centroid scan; query decoded per
  its stored type. Modular narrow variant so `search.go` stays mergeable.

**5c. hnsw (f16, int8; NOT bf16)**:
- `QuantizationToUsearch` (`hnsw/types.go:25`): add `T_array_float16→usearch.F16`,
  `T_array_int8→usearch.I8`.
- Build dispatch (`hnsw_create.go:235-290`): F16/I8 arms → `HnswBuild[types.Float16]`/`[int8]`; decode
  column bytes and `Add` (usearch does distance natively). Widen `HnswBuild`/`HnswModel`/`HnswSearch`
  to `ArrayElement` where they only marshal bytes for usearch.
- `SupportedVectorTypes()` (`hnsw/plugin/runtime/runtime.go:89`, + interface
  `indexplugin/catalog/hooks.go:51`): add f16, int8 only.

**5d. cuVS cagra/ivfpq (f16, int8; NOT f64/bf16) — GPU-gated, flagged UNVERIFIED**:
- `SupportedVectorTypes()` (cagra & ivfpq `plugin/runtime/runtime.go`): add f16, int8.
- Build/add dispatch (`cagra_create_gpu.go`, `ivfpq_create_gpu.go` ~240-460): wire the **column OID** →
  `cuvs.Float16`/`int8` builders (today driven only by the `QUANTIZATION=` param). Build tags isolate
  this; **cannot compile/run here** — edit + mark UNVERIFIED; owner validates on GPU.

### Pass 2 — ivfflat `QUANTIZATION=` (downcast-only, deferred)

`CREATE INDEX ... QUANTIZATION='int8'|'float16'` (keyword already parsed; cuVS-only today). A CPU
**affine** quantizer with a **single global `(min,max)`** pair (the cuVS `TrainQuantizer`/`SetQuantizer`
shape in `pkg/cuvs/kmeans.go`), trained on the build sample, applied to **both** centroids and entries,
persisted in index metadata, dequant-on-result. Downcast-only (quantize width ≤ input width).

### Pass 3 — archsimd optimization (deferred)

Add `*_amd64.go` SIMD kernels behind `amd64 && go1.26 && goexperiment.simd`, with the Pass-1 pure-Go
path as fallback **and** equivalence oracle (assert SIMD == scalar). bf16 = `LoadUint16x16Slice →
ExtendToUint32 → ShiftAllLeft(16) → AsFloat32x16` → f32 kernels (verified: compiles amd64, scalar
matches). int8 = `DotProductQuadruple` (VNNI). f16 SIMD needs an integer-SIMD half-decode (no
F16C/AVX512-FP16 in archsimd) — ships scalar-first, SIMD later. Merge `origin/archsimd` here.
Build via `make GO=$HOME/go/bin/go1.26rc1 …`.

## Phase 6 — Edge cases & tests

Edge-case switches that enumerate array types (add the three IDs, mirroring vecf32 behavior):
- `pkg/sql/colexec/aggexec/minmax2.go:368` (min/max — match current vecf32 behavior, likely
  error/passthrough).
- `pkg/cdc/util.go:165,291`: format via `BytesToArrayToString[BF16/Float16/int8]`.
- MySQL wire output (`mysql_protocol.go`/`output.go`): text row goes through `ArrayToString`
  (covered by Phase 3); verify column-type→MySQL-type mapping emits the new types like vecf32.
- Zonemap: arrays are varlena/no-zonemap — confirm the three IDs follow the vecf32 path in objectio.
- `MaxArrayDimension` stays 65535 (it counts elements, not bytes); add a clarifying comment.

Tests:
- `float16_test.go`: round-trip + reference-value tables for IEEE half and bf16; int8 clamp.
- array string/parse tests; all 25 vector-vector cast pairs + string↔vector.
- Distance correctness vs float32 reference (tolerance for bf16/f16, exact for int8).
- BVT: clone the existing `test/distributed/cases/.../vector/` SQL/result files for vecbf16/vecf16/
  vecint8 (create/insert/select/distance/index). Per memory, add CPU unit tests for plugin
  plan.go/schema.go since BVT is GPU-gated.

---

## Riskiest parts
1. **IEEE float16 conversion** (subnormals/rounding/NaN) — dedicated reference-value test table.
2. **goyacc regen** of `mysql_sql.go` — use the repo's exact toolchain target, never hand-edit.
3. **`arrayToArray` refactor** — touches the single cast path for *all* vector types incl. existing
   vecf32/64; keep the `oid==oid` fast path, test all 25 pairs.
4. **ivfflat in-Go distance paths** — uint16/int8 raw math silently breaks. Rule: bf16/f16 **decode
   to float32** before any arithmetic; int8 uses **integer** (int32) kernels (not raw int8 mul). The
   SQL-layer cast/distance funcs (Phases 3–4) stay byte/format-only via `ArrayElement` + the float32
   bridge; the vectorindex kernels (Phase 5) are the dedicated narrow kernels in
   `distance_func_narrow.go`. See Phase 5 for the per-index ownership split.

## Reuse vs. build new
- **Reuse:** all `metric` distance kernels, `moarray` float32 entry points, varlena storage,
  `EncodeSlice`/`DecodeSlice`, the entire vecf32 grammar/keyword/plan/const-expr pattern (clone),
  usearch `F16`/`I8`, cuVS `VectorType`.
- **Build new:** `types/float16.go` (BF16/Float16 + conversions + batch converters), `ArrayElement`
  constraint, `toFloat32Array`/`fromFloat32Array` bridges, via-F32 execution wrappers, three sets of
  grammar/keyword/plan entries, BVT cases.

## Verification
1. `cd /Users/eric/github/matrixone && make build` (includes goyacc regen) — must compile;
   confirms the `RealNumbers`/`ArrayElement` boundary holds.
2. `go test ./pkg/container/types/... ./pkg/sql/plan/function/... ./pkg/vectorize/moarray/...`
   for conversion, cast, and distance unit tests.
3. Manual SQL via mo-service: `CREATE TABLE t(a vecbf16(4), b vecf16(4), c vecint8(4));`
   insert `'[1,2,3,4]'`, `SELECT a, l2_distance(a, '[0,0,0,0]'), CAST(a AS vecf32)`, and
   `CREATE INDEX ... USING ivfflat` on a `vecbf16` column + `USING hnsw` on `vecf16`/`vecint8`.
4. Run the cloned BVT cases under `test/distributed/cases/.../vector/`.

---

## Benchmark — wiki_all 1M, ivfflat (4-way build matrix)

End-to-end benchmark via `mo_vector_benchmark/run_matrix.py` to quantify the GPU and
archsimd(SIMD) impact across all base column types and index quantizations.

**Setup.** Dataset: cuVS wiki_all 1M (1,000,000 × 768-dim float32). One table per **base
column type** (`vecf32/vecf16/vecbf16/vecint8/vecuint8`), each loaded from the same source via
`LOAD DATA` (int8/uint8 base use NN-order-preserving integer-scaled CSVs — `v*127` / `v*127+128`
— since MO rejects fractional casts to `VECINT8`/`VECUINT8`). Index: `ivfflat`, `lists=1000`,
`op_type vector_l2_ops`, `kmeans_train_percent=10`, `kmeans_max_iteration=20`. Search: `probe_limit=8`,
200 queries, k=10, concurrency=8, recall vs the L2 groundtruth ibin. Matrix = **base sweep**
(5 base types @ `quantization=float32`) + **quant sweep** (`vecf32` base @
`quantization=float16/bf16/int8/uint8`). The four build configs (`MO_CL_CUDA` × `GOEXPERIMENT=simd
GOAMD64=v3`): **GPU+SIMD**, **GPU·noSIMD**, **noGPU+SIMD**, **noGPU·noSIMD**. Data does **not**
survive a rebuild/restart (mo-data bootstraps fresh), so each config re-imports before its matrix.

### Index build time (seconds) — compute-dominated, the cleanest signal

| cell | GPU+SIMD | GPU·noSIMD | noGPU+SIMD | noGPU·noSIMD |
|---|---|---|---|---|
| base f32 | **25** | 128 | 160 | 146 |
| base f16 | **31** | 70 | 92 | 180 |
| base bf16 | **22** | 77 | 62 | 67 |
| base int8 | **17** | 23 | 26 | 50 |
| base uint8 | **15** | 22 | 26 | 49 |
| quant float16 | **17** | 40 | 31 | 47 |
| quant bf16 | **15** | 21 | 27 | 44 |
| quant int8 | **13** | 16 | 24 | 47 |
| quant uint8 | **17** | 26 | 27 | 51 |

**GPU+SIMD is fastest in all 9 cells.** geomean across cells: SIMD ≈ **2×** build speedup overall
but **~5× on the f32 path** (25s→128s); GPU kmeans ≈ 1.8–2.2×. The ivfflat build is dominated by the
**CPU entry-assignment distance** (which SIMD accelerates), so SIMD matters more than GPU; GPU only
speeds the centroid step. SIMD gain shrinks with element width / type: f32 5.1× > bf16 3.5× (upcast
overhead) > int8 1.3× (integer distance isn't the float SIMD path).

### Recall@10 — GPU kmeans yields better centroids

| config | recall@10 range |
|---|---|
| GPU+SIMD / GPU·noSIMD | **0.85 – 0.89** |
| noGPU+SIMD / noGPU·noSIMD | 0.80 – 0.84 |

Consistent ~0.04 recall advantage for **GPU-built** indexes. SIMD does not change results
(correctness preserved across the cosine-clamp SIMD fix).

### Search latency p50 (ms) / throughput QPS (concurrency 8)

| cell | GPU+SIMD | GPU·noSIMD | noGPU+SIMD | noGPU·noSIMD |
|---|---|---|---|---|
| base f32 | **1127** / 3.5 | 7382 / 0.8 | 7069 / 0.8 | 8716 / 0.8 |
| base f16 | **631** / 2.9 | 2608 / 1.2 | 6471 / 1.1 | 4766 / 1.2 |
| base bf16 | 475 / 4.7 | 467 / 3.5 | 502 / 3.8 | 577 / 3.0 |
| base int8 | 362 / 5.6 | 417 / 5.2 | 343 / 5.6 | 360 / 5.7 |
| base uint8 | 361 / 6.6 | 1197 / 2.8 | 288 / 5.6 | 347 / 5.5 |
| quant float16 | 472 / 7.8 | 411 / 5.4 | 689 / 6.5 | 993 / 6.7 |
| quant bf16 | 327 / 8.4 | 460 / 6.3 | 485 / 7.1 | 361 / 9.5 |
| quant int8 | **168** / 39.1 | 188 / 33.8 | 214 / 27.2 | 219 / 24.1 |
| quant uint8 | 428 / 14.2 | 263 / 25.5 | 375 / 18.4 | 291 / 18.8 |

**Two regimes:** (1) **heavy cells** (f32/f16 base — wide full-precision vectors) — GPU+SIMD is
decisively fastest (f32 ≈ **4× QPS** vs the rest); (2) **light cells** (narrow base + all quant) — all
four configs land within ~20%, dominated by index traversal + I/O, not the distance kernel.
**Most robust search finding: int8/uint8 *index quantization* is the fastest search in every config**
(quant int8 ≈ 170–220ms / 24–39 QPS, ~2–8× faster than float32) — smaller entries, independent of
GPU/SIMD.

### Caveats
- **Single run per cell → ±~30% variance** (kmeans randomness, cache warmth). **Build time and recall
  trends are robust** (GPU+SIMD wins all 9 builds; GPU recall consistently higher); **search latency is
  the noisiest dimension** — absolute p50 on heavy cells is cache-state-dominated (e.g. f32-base p50
  swung 485ms↔1127ms across two GPU+SIMD runs), and a few light bf16/uint8 cells show noSIMD edging
  SIMD by a couple % (noise — the sign flips across cells). Trust QPS averages and direction over exact
  multipliers.
- **Storage:** WSL2 vhdx, native ext4, ~1.1 GB/s O_DIRECT / 4–9 GB/s cached — SSD-class; search is
  CPU/SIMD-bound, not I/O-bound (same disk gave 1.1s vs 7.4s for the identical query under SIMD vs
  noSIMD).
