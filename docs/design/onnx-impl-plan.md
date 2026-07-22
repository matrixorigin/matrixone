# Implementation Plan: `onnx_run` builtin

Companion to [onnx.md](./onnx.md). This is the concrete engineering plan derived from a codebase
survey. All file:line references were verified against the current tree.

## 0. Goal recap

```sql
select onnx_run(model, input, input_shape, output_shape) from T;
```
- `model`   — `varbinary` (raw model bytes) **or** `datalink` (file in a stage).
- `input`   — json, flattened tensor values.
- `input_shape`  — json `{"dim":[...], "dtype":"int16"}`.
- `output_shape` — json shape, or `NULL` when the output is not a plain tensor (e.g. sklearn
  random-forest returns a label tensor + a sequence-of-maps probability output).
- Returns json. Tensor output → json array shaped per `output_shape`. Non-tensor → json encoding
  of whatever the network produced.

Per-query the operator caches one ONNX session and reuses it across rows; the session is closed
when the function expression is Released.

---

## 1. Dependencies & build wiring

### 1.1 Go module
- Add `github.com/yalue/onnxruntime_go` to `go.mod` (`go get`, then `go mod tidy`). It is **cgo-free
  Go** — it `dlopen`s the shared library at runtime via `SetSharedLibraryPath` + `InitializeEnvironment`.
  No new cgo link flags in `cgo/`.

### 1.2 Shared library
- The `thirdparties/Makefile` `onnxruntime` target (already added) downloads the correct
  `onnxruntime.so` / `onnxruntime_arm64.so` / `onnxruntime_arm64.dylib` into
  `thirdparties/install/lib/`.
- Runtime path resolution: the loader (see §2) must locate the `.so`. Order to try:
  1. env override `MO_ONNXRUNTIME_LIB` (explicit path);
  2. alongside the `mo-service` binary (dir of `os.Executable()`), platform-specific filename;
  3. `thirdparties/install/lib/<platform-file>` relative to repo root (dev / test).
- Deployment: ensure the packaging step ships the platform `.so` next to `mo-service` (follow-up in
  release scripts — note it, out of scope for the first PR beyond the dev/test path).

---

## 2. Package `pkg/mlai/onnx` — runtime init + core eval

New package. Keeps **all** onnxruntime_go usage isolated so the SQL layer stays thin.

### 2.1 One-time environment init (`onnx.go`)
- `package onnx` with a `sync.Once`-guarded initializer (NOT a bare `init()` that panics — the DB
  must start even if ONNX init fails).
- `func ensureInit() error`: resolve lib path (§1.2), `ort.SetSharedLibraryPath(path)`,
  `ort.InitializeEnvironment()`. Store the result (nil or error) in a package var guarded by the Once.
- `func Available() error`: returns the cached init error. Every `onnx_run` call first checks this;
  if init failed, the function call errors (`moerr`), but startup already succeeded.
- Rationale from design: "If failed to initialize, later ALL function call onnx should fail, but the
  database should start normally." Do the Once lazily on first `onnx_run`, so a cluster with no ONNX
  usage never touches the library.

### 2.2 Shape & dtype (`shape.go`)
- `type Shape struct { Dim []int64; Dtype string }`; `func ParseShape(js []byte) (*Shape, error)`
  (json unmarshal + validate dims non-negative, product fits int).
- `dtype` string → onnxruntime element type. Supported set mapped to `onnxruntime_go` generics:
  `int8,uint8,int16,uint16,int32,uint32,int64,uint64,float32(float),float64(double),bool`.
  Reject unknown dtype with a clear `moerr`.

### 2.3 Session wrapper (`session.go`)
- `type Session struct { s *ort.DynamicAdvancedSession; inputNames, outputNames []string; modelKey string }`.
- `func NewSession(modelBytes []byte) (*Session, error)`:
  - `ort.GetInputOutputInfoWithONNXData(modelBytes)` → discover input/output names (examples
    hardcode names; we must auto-discover since the SQL API doesn't pass them).
  - `ort.NewDynamicAdvancedSessionWithONNXData(modelBytes, inputNames, outputNames, nil)`.
  - `modelKey` = hash (e.g. `xxhash`/sha256) of modelBytes, used by the op to detect a changed
    model argument across rows (see §3.3).
- `func (s *Session) Close() error` → `s.s.Destroy()`.

### 2.4 Eval (`session.go`)
- `func (s *Session) Run(inputVals []byte /*json*/, in *Shape, out *Shape) ([]byte /*json*/, error)`:
  1. Build input tensor: type-switch on `in.Dtype`. For each dtype T:
     - unmarshal the `input` json array into `[]T`, verify `len == product(in.Dim)`;
     - `ort.NewTensor(ort.NewShape(in.Dim...), data)` → `ort.Value`.
     - (generic dispatch: a `switch in.Dtype { case "float32": ... }` with a helper
       `buildTensor[T]` per branch, since Go generics need compile-time T.)
  2. `outputs := make([]ort.Value, len(s.outputNames))` (all nil → auto-allocated).
  3. `s.s.Run([]ort.Value{inputTensor}, outputs)`; `defer` Destroy on input + each non-nil output.
  4. Encode result to json:
     - **Tensor output & `out != nil`**: read `outputs[0].(*ort.Tensor[T]).GetData()` (dispatch on
       `out.Dtype`), reshape per `out.Dim`, marshal nested json array.
     - **Non-tensor output (`out == nil`)**: walk each output `ort.Value` by its `GetONNXType()`:
       `*ort.Tensor[T]` → array; `*ort.Sequence` → `GetValues()` recurse; `*ort.Map` →
       `GetKeysAndValues()` → json object. Produce a json object/array capturing all outputs.
       This is what `non_tensor_outputs` (sklearn random forest) needs.
- Keep a small recursive `valueToJSON(v ort.Value)` helper for the non-tensor path.

### 2.5 Concurrency note
- `DynamicAdvancedSession.Run` is not guaranteed goroutine-safe. The op instance is per
  expression-executor; within one operator, rows are processed sequentially on the eval goroutine,
  so a single cached session is fine. Do **not** share a `Session` across operators.

---

## 3. SQL builtin registration (`pkg/sql/plan/function/`)

### 3.1 `function_id.go`
- Add `ONNX_RUN = 549` before the sentinel; bump `FUNCTION_END_NUMBER` to 550 (line ~774).
- Add `"onnx_run": ONNX_RUN` to `functionIdRegister` (name→id map, ~line 1039 area).
- Update `function_id_test.go` id-mirror map (`ONNX_RUN: 549`) or the test fails.

### 3.2 `func_builtin_onnx.go` (new impl file)
- `type opOnnxRun struct { sess *onnx.Session; modelKey string }` — per-expression state.
- `func newOpOnnxRun() *opOnnxRun`.
- `func (op *opOnnxRun) Close() error` → close cached session (freeFn).
- `func (op *opOnnxRun) Reset(...) error` → close + nil the session (resetFn, for executor reuse).
- `func (op *opOnnxRun) eval(params []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error`:
  - `if err := onnx.Available(); err != nil { return moerr... }` (init-failed path).
  - `rs := vector.MustFunctionResult[types.Varlena](result)`.
  - Wrap args:
    - model: `vector.GenerateFunctionStrParameter(params[0])` → bytes; if
      `params[0].GetType().Oid == types.T_datalink` resolve via
      `datalink.NewDatalink(str, proc).GetBytes(proc)`, else use bytes directly.
      (Pattern mirrors `func_builtin_w.go` wasm builtin, `func_unary.go:4182` LoadFileDatalink.)
    - input / input_shape / output_shape: `GenerateFunctionStrParameter` (T_json is Varlena).
  - Per row `j`: honor `selectList.Contains(j)` → append null; null model/input → null out.
  - **Session caching**: compute model key (hash of model bytes) once per distinct model; if
    `op.sess == nil || op.modelKey != key` → close old, `onnx.NewSession(bytes)`, store. Common case
    (constant model column / literal) builds the session on row 0 and reuses it for all rows.
  - `out, err := op.sess.Run(inputBytes, inShape, outShape)`; `rs.AppendBytes(out, false)`.

### 3.3 `list_builtIn.go`
- Register `ONNX_RUN` in the `builtins` slice. Two overloads for the polymorphic first arg:
  - overload 0: `args: []types.T{types.T_varbinary, types.T_json, types.T_json, types.T_json}`.
  - overload 1: `args: []types.T{types.T_datalink, types.T_json, types.T_json, types.T_json}`.
  - `retType`: `func([]types.Type) types.Type { return types.T_json.ToType() }`.
  - `checkFn: fixedTypeMatch`, `class: plan.Function_STRICT`, `layout: STANDARD_FUNCTION`.
  - Each overload: `newOpWithFree: func() (...) { op := newOpOnnxRun(); return op.eval, op.Reset, op.Close }`.
    This is the exact `serial`/`serial_full` lifecycle (`list_builtIn.go:2842`,
    `func_builtin_serial.go`), which the executor honors: `freeFn` runs in
    `FunctionExpressionExecutor.Free` (`evalExpression.go:981`), `resetFn` in `ResetForNextQuery`.
- `output_shape` NULL is passed as a json null / SQL NULL literal; treat NULL output_shape as
  "non-tensor mode" inside eval.

### 3.4 Type-coercion note
- Callers pass `input`/shapes as json literals (`'{"dim":[1,1,4],"dtype":"float32"}'`). Confirm the
  planner coerces string literal → `T_json` for these arg positions (JSON_QUOTE-style). If literal
  strings don't auto-cast to json for a T_json arg slot, add `varchar` overloads or an implicit cast.
  Verify during implementation with a smoke query.

---

## 4. Tests (BVT)

Convert examples from `onnxruntime_go_examples` to BVT cases.

### 4.1 Test resources
- Check in ONNX models (NOT the python generators):
  - `sum_and_difference/sum_and_difference.onnx`
  - `non_tensor_outputs/sklearn_randomforest.onnx`  + the **iris dataset** it uses.
  - (bonus) `mnist/mnist.onnx`.
- Location: `test/distributed/resources/onnx/` (new dir). Models are small (KB–MB); no LFS needed.
  Keep them out of the `*.so`-ignored path; they are `.onnx`, not ignored.

### 4.2 Cases `test/distributed/cases/function/onnx/` (or `.../onnx_run/`)
- `sum_and_difference`: load model as varbinary via `load_file`/stage or a `datalink`, run with
  a known `[1,1,4]` float32 input, assert the `[1,1,2]` sum/difference output json.
- `non_tensor_outputs`: run the random-forest model on iris rows (input `[N,4]` float32,
  `output_shape` NULL), assert predicted label + probability-map json.
- (bonus) `mnist`: single-image inference, assert argmax label.
- Multi-row reuse test: a table with N rows sharing the same model literal → verifies session is
  built once and reused (correctness; perf is incidental).
- Error paths: init-unavailable behavior, bad dtype, shape/length mismatch, malformed model bytes →
  expect clean SQL errors, not crashes.
- Remember `-- @separator:table` if EXPLAIN/multiline (per repo BVT convention).

### 4.3 Model loading in tests
- Two ingestion styles to cover both arg types:
  - varbinary: `load_file(cast('stage://...' as datalink))` or read into a blob column.
  - datalink: pass a `stage://` datalink directly as arg 0 (exercises §3.2 datalink branch).

---

## 5. Work order / PR slicing

1. **PR-1 (infra)**: `thirdparties/Makefile` onnx target (done) + go.mod dep + `pkg/mlai/onnx`
   package (init, shape, session, eval) with Go unit tests using the checked-in models. No SQL yet.
   - Unit-testable without the DB; validates lib loading + tensor/non-tensor paths in isolation.
2. **PR-2 (SQL)**: `function_id.go` + `func_builtin_onnx.go` + `list_builtIn.go` registration +
   `function_id_test.go`. Smoke test via `mysql` client.
3. **PR-3 (BVT)**: resources + cases + generated `.result` files (`run.sh -m genrs`).

Splitting keeps each PR reviewable; PR-1 carries the risky runtime-loading logic behind unit tests.

---

## 6. Open questions / risks

- **Lib deployment beside mo-service**: release/packaging must copy the platform `.so`. First PR
  only guarantees the dev/test resolution path; file a follow-up for release scripts.
- **Model size**: `datalink.GetBytes` does `io.ReadAll` into memory; `LoadFileDatalink` caps at
  `types.MaxBlobLen` (64MB) but `GetBytes` itself doesn't. Large models are loaded whole into RAM —
  acceptable for v1, document the limit.
- **dtype breadth**: v1 supports the numeric set + bool + **float16** (enables the `mnist_float16`
  bonus). String tensors (`example_strings.onnx`) are out of scope; reject with a clear error.
- **json literal → T_json coercion** for shape args — verify in §3.4; may need varchar overloads.
- **Thread-safety**: one session per operator instance; never share across goroutines/operators.
- **Session cache key**: hashing model bytes each row is wasteful if the model is a big per-row
  blob. For the common constant-model case it's cheap (hash once, reuse). If a query truly varies
  the model per row, every row rebuilds a session — acceptable but document it.
```

---

### Verified reference points
- Stateful builtin lifecycle: `serial` at `list_builtIn.go:2842`, `func_builtin_serial.go`;
  executor free path `evalExpression.go:981`, reset `evalExpressionReset.go:65`.
- Datalink read: `pkg/datalink/datalink.go` `GetBytes`/`NewReadCloser`; wasm precedent
  `func_builtin_w.go`; `LoadFileDatalink` `func_unary.go:4182`.
- Str/blob arg + varlena result: `vector.GenerateFunctionStrParameter`, `GetStrValue`,
  `vector.MustFunctionResult[types.Varlena]`, `rs.AppendBytes`.
- Types: `T_json=62`, `T_varbinary=65`, `T_datalink=72`; `MaxBlobLen=64MB`.
- onnxruntime_go API: `SetSharedLibraryPath`/`InitializeEnvironment`,
  `GetInputOutputInfoWithONNXData`, `NewDynamicAdvancedSessionWithONNXData`,
  `NewTensor`/`NewShape`, `DynamicAdvancedSession.Run([]Value,[]Value)`, `Tensor[T].GetData`,
  `Sequence.GetValues`, `Map.GetKeysAndValues`.
