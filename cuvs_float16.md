# Float16 base type (+ native int8/uint8 quantization) for ivfpq & cagra

## Context

The GPU/cuVS indexes **ivfpq** and **cagra** only accept `vecf32` base columns. Goal: accept a
**`vecf16`** base column and either store it natively as `half`, or **quantize it to int8/uint8**
(or half/f32) for the main index. `vecf32` base keeps working (incl. its existing QUANTIZATION).

**int8/uint8 as a *base column* stays deferred** — verified: cuVS `brute_force::search` has no
int8/uint8 overload (compile error; doc comment left in `cgo/cuvs/test/brute_force_test.cu`), and
the CDC **overflow tier** is a brute force, so an int8/uint8 base can't back its overflow.

**Two user decisions shape the architecture:**
1. **Overflow/CDC tier runs in the BASE type only (f32 or f16)** — never the storage type. So an
   int8/uint8-*quantized* index has an f16/f32 overflow (cuVS-supported), which is what makes
   int8/uint8 *quantization* viable and also fixes the currently-broken int8-quant overflow.
2. **f16→int8/uint8 quantization uses the native half-source quantizer** (cuVS
   `preprocessing::quantize::scalar` has `half` overloads, scalar.hpp:348-415) — **no f32 detour**.

v1 covers, for both indexes: build, unfiltered + filtered (INCLUDE) search, and CDC/incremental
sync, for **base ∈ {f32, f16}** with **storage ∈ {f32, f16, int8, uint8}** (storage = base, or a
downcast quantization).

## Core architecture — base type `B` vs storage type `Q`

- **`B` (base type)** ∈ {`float32`, `cuvs.Float16`}: the column type, the **query** type, and the
  **overflow brute-force** type. Both are cuVS-brute-force-supported.
- **`Q` (storage type)** ∈ {`float32`, `cuvs.Float16`, `int8`, `uint8`}: the **main cuVS index**
  type. Either `Q == B` (direct, no quantization) or `width(Q) ≤ width(B)` (downcast quantization).
- **Build/Model/Search carry both `B` and `Q`** (two type params, e.g. `IvfpqModel[B, Q]`). Main
  index = cuVS `[Q]`; overflow = `GpuBruteForce[B]`.
- Two operation families, by purpose:
  - **native (`Q==B`)**: `AddChunk[B]` / `Search[B]` / `SearchWithFilter[B]` — raw, no quantizer.
    Used by the direct main index AND always by the overflow tier.
  - **quantize (`B→Q`)**: `AddChunkQuantize` / `SearchQuantize` / `SearchQuantizeWithFilter` —
    base-type input, quantized to `Q`. Uses the **half-source** quantizer when `B=f16`, the
    float-source quantizer when `B=f32`. (These replace the misnamed `AddChunkFloat`/`SearchFloat32`.)
- `types.Float16` vs `cuvs.Float16` (both uint16, distinct) → one shared checked-copy bridge helper.

## Plan (both indexes; `cagra` mirrors `ivfpq` file-for-file)

### 0. C++ / cgo — native half-source quantize path (new C++)
Today the quantize path is float32-only: `gpu_{ivf_pq,cagra}_add_chunk_float(const float*)`,
`..._train_quantizer(const float*)`, and the f32-query quantize inside search. cuVS already
supports half-source scalar quantization, and our `scalar_quantizer_t<S>` is templated on `S`
(quantize.hpp:50). Add the `half` source variants for **both ivf_pq and cagra**:
- `..._train_quantizer_half(const half* train_data, ...)` — instantiate `scalar_quantizer_t<half>`.
- `..._add_chunk_quantize_half(const half* data, ...)` — quantize half→`Q` (transform<int8_t/uint8_t>).
- half-query filtered/unfiltered search that quantizes the half query → `Q` (mirror the existing
  float-query 1-byte-T quantize in `search_*_internal`).
Mirror the existing float-quantize C wrappers exactly (`ivf_pq_c.cpp`/`cagra_c.cpp` dispatch on
`qtype`). Relink `cgo/libmo.so`. (Brute force needs NO change — overflow is always B∈{f32,f16},
both already supported.)

### 1. cuVS Go bindings (pkg/cuvs)
- Wire the new half-quantize C funcs: `GpuIvfPq[Q].AddChunkQuantizeHalf([]cuvs.Float16)`,
  `TrainQuantizerHalf`, and a half-query `SearchQuantize`/`...WithFilter`. (Native `AddChunk([]Q)`
  / `Search([]Q)` / `SearchWithFilter([]Q)` and the f32 quantize variants already exist.)
- `MultiGpuIvfPq`/`MultiGpuCagra`: add native `SearchWithFilter([]Q)` (twin of
  `SearchFloat32WithFilter`) and the half-query quantize search wrappers.
- Shared f16 bridge helper in `pkg/cuvs/helper.go`: `F16FromTypes([]types.Float16) []cuvs.Float16`.

### 2. Schema / DDL validation (no GPU; planner-only)
- `pkg/vectorindex/{ivfpq,cagra}/plugin/runtime/runtime.go` — `SupportedVectorTypes()` →
  `{T_array_float32, T_array_float16}` (NOT int8/uint8/bf16 as base).
- `pkg/vectorindex/{ivfpq,cagra}/plugin/plan/schema.go` — accept f16 base; **QUANTIZATION is
  downcast-only**: allow when `width(Q) ≤ width(B)` (f16→int8/uint8 OK; f16→f32 rejected as
  upcast), mirroring ivfflat's guard (`ivfflat/plugin/plan/schema.go`). Update messages.

### 3. Build path — two-type dispatch + native/quantize add
`pkg/vectorindex/{ivfpq,cagra}/{build,model}_gpu.go` + `pkg/sql/colexec/table_function/{ivfpq,cagra}_create_gpu.go`:
- Parameterize `IvfpqBuild[B,Q]` / `IvfpqModel[B,Q]`. The create table-fn dispatches on
  `(baseOid, quantization)` → the right `[B,Q]` instantiation. Valid combos: B=f32→Q∈{f32,f16,int8,uint8};
  B=f16→Q∈{f16,int8,uint8}.
- Wrapper methods: native `AddChunk(chunk []B)` / build `Add(id, vec []B)` (→ cuVS `AddChunk[Q]`
  when Q==B); quantize `AddChunkQuantize(chunk []B)` / `AddQuantize` (→ half- or float-source
  quantizer per B). Rename the old `AddChunkFloat`/`AddFloat` to the quantize names.
- Per-row: decode the base column to native `[]B` (`BytesToArray[float32]` or
  `BytesToArray[types.Float16]`+bridge); route to `Add` (Q==B) or `AddQuantize` (Q≠B).

### 4. Search path — base query, native/quantize main + base overflow
`pkg/vectorindex/{ivfpq,cagra}/search_gpu.go` + `pkg/sql/colexec/table_function/{ivfpq,cagra}_search_gpu.go`:
- Parameterize `IvfpqSearch[B,Q]`; `newXxxAlgo` dispatches on `(KeyPartType, Quantization)`.
- Decode query → native `[]B`. Main index: `Search[B]`/`SearchWithFilter[B]` when Q==B, else
  `SearchQuantize`/`SearchQuantizeWithFilter` (B→Q). Set storage qtype from the QUANTIZATION
  option (or =B when none); validate `faVec.GetType().Oid == KeyPartType`.
- **Overflow field becomes `GpuBruteForce[B]`** (base type), fed/searched natively
  (`AddChunk([]B)` / `Search`/`SearchWithFilter([]B)`). Distances merge with the main index
  (both approximate true float L2). FilterStore INCLUDE filter unchanged.

### 5. CDC / incremental sync — native base type
- Widen `VectorIndexCdc[T types.RealNumbers]` (pkg/vectorindex/types.go:266) to
  `types.ArrayElement` (RealNumbers ⊂ ArrayElement → f32/f64 users unaffected) + a Float16 cdc
  codec, so CDC carries native `B`. The CDC reader decodes the f16 source column to `Float16`;
  `sync.go` and the model overflow buffer become `[]B`.
- Overflow/tail folds via native `AddChunk([]B)`.
- **cagra extend caveats:** cuVS cannot `extend()` a half cagra index (and verify int8/uint8) →
  route cagra incremental through rebuild for unsupported `Q`. Verify whether the existing
  `QUANTIZATION='f16'` cagra path already has this rebuild branch and reuse it.

### 6. Non-GPU build parity (`//go:build !gpu`)
Add `errGPURequired` stubs in `*_cpu.go.bak` for the new/renamed exported methods
(`Add`/`AddChunk`, `AddQuantize`/`AddChunkQuantize`, `Search`/`SearchWithFilter`,
`SearchQuantize`/`SearchQuantizeWithFilter`) and the two-type-param signatures. Verify the
non-GPU build compiles.

### 7. Tests (mirror ivfflat under test/distributed/cases/vector/)
- **CPU CI (DDL only)**: CREATE INDEX ivfpq/cagra on `vecf16` succeeds; on `vecint8`/`vecuint8`/
  `vecbf16` base rejected; `vecf16` + QUANTIZATION='int8'/'uint8' accepted; + QUANTIZATION='float32'
  (upcast) rejected.
- **GPU runner (gpu tag)**: for each {ivfpq,cagra} × {f16 direct, f16→int8, f16→uint8}: insert
  known vectors, build, KNN `ORDER BY l2_distance` vs brute-force ground truth (tolerance for the
  quantized cases); a filtered (INCLUDE) query; an incremental-insert (CDC) case exercising the
  base-type overflow (and the cagra rebuild branch where Q is unextendable).

## Critical files
- `cgo/cuvs/{ivf_pq,cagra}.hpp` + `{ivf_pq,cagra}_c.{h,cpp}` — native half-source quantize path (step 0)
- `pkg/cuvs/{ivf_pq,cagra,multi_index}.go` + `helper.go` — half-quantize bindings, native `SearchWithFilter([]Q)`, f16 bridge
- `pkg/vectorindex/{ivfpq,cagra}/{build,model,search}_gpu.go` — `[B,Q]` params; native `Add`/`AddChunk`/`Search` + `*Quantize`; overflow `GpuBruteForce[B]`
- `pkg/sql/colexec/table_function/{ivfpq,cagra}_{create,search}_gpu.go` — `(base,quant)` dispatch + base decode
- `pkg/vectorindex/{ivfpq,cagra}/plugin/plan/schema.go` + `plugin/runtime/runtime.go` — accept f16 + downcast-only guard
- `pkg/vectorindex/types.go` — `VectorIndexCdc[T]` → `ArrayElement` + Float16 codec
- `pkg/vectorindex/{ivfpq,cagra}/sync.go` (+ CDC reader) — native B CDC; cagra rebuild branch
- `pkg/vectorindex/{ivfpq,cagra}/{build,model}_cpu.go.bak` — `!gpu` stub parity

## Riskiest points
1. **Two-type-param `[B,Q]` refactor** — threads through build/model/search/sync; ~7 valid combos
   in the dispatch switches. Largest structural change; keep the f32-only combos byte-identical.
2. **New half-source quantize C++** — train/transform/search for half→int8/uint8 (step 0); verify
   cuVS `scalar<half>` train+transform link and that quantized recall matches the f32-source path.
3. **`VectorIndexCdc[T]` widening** (RealNumbers→ArrayElement) + Float16 codec — shared CDC code;
   existing f32/f64 users must stay byte-identical.
4. **cagra extend** unsupported for half (verify int8/uint8) — rebuild branch.
5. **Float16 bridge** — one shared checked-copy helper.
6. **Non-GPU build** — `.bak` stubs.

## Verification
1. Non-GPU build compiles (`.bak` stubs present).
2. GPU build + relink `cgo/libmo.so`; the cuvs C++ test (`make test_cuvs_worker`) passes incl. a
   new half→int8 quantize test; `mo-service` up.
3. DDL: f16 base (success); int8/uint8/bf16 base (rejected); f16+QUANT int8/uint8 (success);
   f16+QUANT float32 upcast (rejected).
4. Functional per {index}×{f16, f16→int8, f16→uint8}: build, KNN vs ground truth; filtered query;
   self-distance≈0 sanity (direct f16).
5. Incremental: insert post-build, confirm base-type overflow + CDC fold correctly (incl. cagra rebuild).
6. New BVT cases (CPU tier in CI; GPU tier on the GPU runner).
