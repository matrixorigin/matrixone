# Approx Percentile Argument Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reject invalid `approx_percentile` percentile arguments with MatrixOne errors at the earliest reliable boundary and eliminate raw panics from dynamic or malformed inputs.

**Architecture:** A shared planner gate enforces the no-column stable-expression contract for both aggregate and window binding. Compile-time extraction validates the evaluated vector before reading it, and executor parsing preserves the same finite `[0,1]` invariant as the final boundary.

**Tech Stack:** Go, MatrixOne planner/function/compile/aggexec packages, `moerr`, `testify/require`.

## Global Constraints

- Reject prepared-statement parameters and variables; the configuration must be a compile-time constant.
- Do not change percentile computation, result types, or the generic function type-check API.
- Production changes must follow RED → GREEN TDD and remain limited to the reviewed argument contract.
- Run CGo-dependent verification with the repository's existing `cgo` and `thirdparties/install` artifacts.

---

### Task 1: Enforce the planner contract

**Files:**
- Create: `pkg/sql/plan/base_binder_approx_percentile_test.go`
- Modify: `pkg/sql/plan/base_binder.go`

**Interfaces:**
- Consumes: `rule.IsConstant(*plan.Expr, bool)` and `isNullExpr(*plan.Expr)`.
- Produces: `validateApproxPercentileArgs(context.Context, []*plan.Expr) error`, called by `BindFuncExprImplByPlanExpr`.

- [ ] **Step 1: Write failing planner tests**

Create table-driven tests that pass an `int64` value column as arg0 and each arg1 below to `BindFuncExprImplByPlanExpr`:

```go
func TestBindApproxPercentileRequiresStableNonNullPercentile(t *testing.T) {
    ctx := context.Background()
    value := &planpb.Expr{
        Typ: planpb.Type{Id: int32(types.T_int64)},
        Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0, Name: "v"}},
    }
    percentileColumn := &planpb.Expr{
        Typ: planpb.Type{Id: int32(types.T_float64)},
        Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1, Name: "p"}},
    }

    _, err := BindFuncExprImplByPlanExpr(ctx, "approx_percentile", []*planpb.Expr{value, makePlan2NullConstExprWithType()})
    require.ErrorContains(t, err, "percentile argument of approx_percentile must be a non-null constant")

    _, err = BindFuncExprImplByPlanExpr(ctx, "approx_percentile", []*planpb.Expr{value, percentileColumn})
    require.ErrorContains(t, err, "percentile argument of approx_percentile must be a non-null constant")
}

func TestBindApproxPercentileAcceptsFoldableConstants(t *testing.T) {
    ctx := context.Background()
    value := &planpb.Expr{
        Typ: planpb.Type{Id: int32(types.T_int64)},
        Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0, Name: "v"}},
    }
    foldable, err := BindFuncExprImplByPlanExpr(ctx, "+", []*planpb.Expr{
        makePlan2Float64ConstExprWithType(0.4),
        makePlan2Float64ConstExprWithType(0.1),
    })
    require.NoError(t, err)

    for _, percentile := range []*planpb.Expr{
        makePlan2Float64ConstExprWithType(0.95), foldable,
    } {
        _, err = BindFuncExprImplByPlanExpr(ctx, "approx_percentile", []*planpb.Expr{DeepCopyExpr(value), percentile})
        require.NoError(t, err)
    }
}
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
go test -count=1 -run 'TestBindApproxPercentile' ./pkg/sql/plan
```

Expected: both invalid-input assertions fail because current binding accepts NULL and column expressions.

- [ ] **Step 3: Add the minimal shared planner gate**

Import `pkg/sql/plan/rule`, then add:

```go
func validateApproxPercentileArgs(ctx context.Context, args []*Expr) error {
    if len(args) != 2 {
        return nil
    }
    percentile := args[1]
    if percentile == nil || isNullExpr(percentile) || !rule.IsConstant(percentile, false) {
        return moerr.NewInvalidInput(ctx,
            "percentile argument of approx_percentile must be a non-null constant")
    }
    return nil
}
```

Call it at the start of `BindFuncExprImplByPlanExpr` only when `name == "approx_percentile"`, before implicit casts can hide a NULL literal.

- [ ] **Step 4: Run the focused planner tests and verify GREEN**

Run the Step 2 command. Expected: PASS.

---

### Task 2: Make compile extraction panic-safe

**Files:**
- Modify: `pkg/sql/compile/operator_test.go`
- Modify: `pkg/sql/compile/operator.go`

**Interfaces:**
- Produces: `getPercentileConfig(*vector.Vector) ([]byte, error)`.
- Consumers: the aggregate configuration branches in `constructWindow` and `constructGroup`.

- [ ] **Step 1: Convert existing success tests and add failing invalid-vector tests**

Use `vector.NewConstFixed` for the existing numeric success cases and assert `(cfg, err)`. Add table-driven invalid cases:

```go
func TestGetPercentileConfigRejectsInvalidVectors(t *testing.T) {
    mp, err := mpool.NewMPool("test_pct_config_invalid", 0, mpool.NoFixed)
    require.NoError(t, err)
    defer mpool.DeleteMPool(mp)

    flat := vector.NewVec(types.T_float64.ToType())
    require.NoError(t, vector.AppendFixed(flat, 0.5, false, mp))
    nullVec := vector.NewConstNull(types.T_float64.ToType(), 1, mp)
    unsupported, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("0.5"), 1, mp)
    require.NoError(t, err)
    below, err := vector.NewConstFixed(types.T_float64.ToType(), -0.1, 1, mp)
    require.NoError(t, err)
    above, err := vector.NewConstFixed(types.T_float64.ToType(), 1.1, 1, mp)
    require.NoError(t, err)
    nan, err := vector.NewConstFixed(types.T_float64.ToType(), math.NaN(), 1, mp)
    require.NoError(t, err)
    inf, err := vector.NewConstFixed(types.T_float64.ToType(), math.Inf(1), 1, mp)
    require.NoError(t, err)

    for _, vec := range []*vector.Vector{flat, nullVec, unsupported, below, above, nan, inf} {
        t.Run(vec.GetType().String(), func(t *testing.T) {
            require.NotPanics(t, func() {
                _, err := getPercentileConfig(vec)
                require.Error(t, err)
            })
        })
        vec.Free(mp)
    }
}
```

- [ ] **Step 2: Run the focused compile test and verify RED**

Run:

```bash
go test -count=1 -run 'TestGetPercentileConfig' ./pkg/sql/compile
```

Expected: compile failure because `getPercentileConfig` still returns only `[]byte`; after adapting the test signature, invalid inputs also expose the current panic behavior.

- [ ] **Step 3: Implement validated extraction and propagate errors**

Implement:

```go
func getPercentileConfig(vec *vector.Vector) ([]byte, error) {
    if vec == nil || !vec.IsConst() {
        return nil, moerr.NewInvalidInputNoCtx(
            "percentile argument of approx_percentile must be a constant")
    }
    if vec.Length() == 0 || vec.IsConstNull() {
        return nil, moerr.NewInvalidInputNoCtx(
            "percentile argument of approx_percentile cannot be NULL")
    }

    var p float64
    switch vec.GetType().Oid {
    case types.T_float64:
        p = vector.MustFixedColWithTypeCheck[float64](vec)[0]
    case types.T_float32:
        p = float64(vector.MustFixedColWithTypeCheck[float32](vec)[0])
    case types.T_int64:
        p = float64(vector.MustFixedColWithTypeCheck[int64](vec)[0])
    case types.T_int32:
        p = float64(vector.MustFixedColWithTypeCheck[int32](vec)[0])
    case types.T_decimal64:
        value := vector.MustFixedColWithTypeCheck[types.Decimal64](vec)[0]
        p = types.Decimal64ToFloat64(value, vec.GetType().Scale)
    case types.T_decimal128:
        value := vector.MustFixedColWithTypeCheck[types.Decimal128](vec)[0]
        p = types.Decimal128ToFloat64(value, vec.GetType().Scale)
    default:
        return nil, moerr.NewInvalidInputNoCtxf(
            "unsupported percentile type %s for approx_percentile", vec.GetType().String())
    }

    if math.IsNaN(p) || math.IsInf(p, 0) || p < 0 || p > 1 {
        return nil, moerr.NewInvalidInputNoCtxf(
            "percentile argument of approx_percentile must be finite and in [0,1], got %v", p)
    }
    return []byte(strconv.FormatFloat(p, 'f', -1, 64)), nil
}
```

At both call sites, call `free()` before propagating an extraction error with the existing compile panic/recovery convention.

- [ ] **Step 4: Run focused compile tests and verify GREEN**

Run the Step 2 command. Expected: PASS with no panic.

---

### Task 3: Preserve the invariant at executor parsing

**Files:**
- Modify: `pkg/sql/colexec/aggexec/approx_percentile_test.go`
- Modify: `pkg/sql/colexec/aggexec/approx_percentile.go`

**Interfaces:**
- Modifies: `parsePercentileConfig(any) (float64, error)`.

- [ ] **Step 1: Add failing non-finite configuration tests**

Extend `TestApproxPercentileExec_SetExtraInformation_Invalid` with `[]byte("NaN")`, `[]byte("+Inf")`, and `[]byte("-Inf")`, asserting an error for each.

- [ ] **Step 2: Run the focused executor test and verify RED**

Run:

```bash
go test -count=1 -run 'TestApproxPercentileExec_SetExtraInformation_Invalid' ./pkg/sql/colexec/aggexec
```

Expected: `NaN` is accepted by the current `p < 0 || p > 1` check.

- [ ] **Step 3: Implement the finite range check**

Replace the range condition with:

```go
if math.IsNaN(p) || math.IsInf(p, 0) || p < 0 || p > 1 {
    return 0, moerr.NewInvalidInputNoCtxf(
        "approx_percentile: percentile must be finite and in [0,1], got %v", p)
}
```

- [ ] **Step 4: Run the focused executor test and verify GREEN**

Run the Step 2 command. Expected: PASS.

---

### Task 4: Verify, self-review, and obtain independent review

**Files:**
- Review all files changed since commit `19270fb52d77b09c1c98411128d4f58f374b23f0`.

**Interfaces:**
- Produces: verified fix commit and a strict read-only subagent verdict.

- [ ] **Step 1: Format and perform static diff checks**

Run `gofmt` on changed Go files, `git diff --check`, conflict-marker scans, and inspect `git diff --stat 19270fb52..HEAD` plus working-tree changes.

- [ ] **Step 2: Run focused and package verification with CGo paths**

Run uncached tests for:

```text
./pkg/sql/plan
./pkg/sql/plan/function
./pkg/sql/compile
./pkg/sql/colexec/aggexec
```

Then run `go build` and `go vet` for the same packages with `CGO_CFLAGS`, `CGO_LDFLAGS`, `DYLD_LIBRARY_PATH`, and `-ldflags` pointing at `/Users/yanghaoyang/repo/matrixone/{cgo,thirdparties/install}`.

- [ ] **Step 3: Run MatrixOne self-review gates**

Trace planner → compile → executor closure, apply Q1–Q3 to vector/free paths, confirm both compile error branches call `free()`, and confirm prepared parameters are rejected before compile evaluation.

- [ ] **Step 4: Commit the implementation**

Stage only the contract and regression-test files and commit with:

```bash
git commit -m "fix: validate approx percentile argument contract"
```

- [ ] **Step 5: Dispatch a strict read-only subagent review**

Provide base `19270fb52d77b09c1c98411128d4f58f374b23f0`, the implementation head, the approved design, the GitHub request-changes text, and require explicit Critical/Important/Minor findings plus a merge verdict. Fix every valid Critical or Important finding via a new RED → GREEN cycle.

- [ ] **Step 6: Re-run all verification after review-driven changes**

Repeat Step 2 and the diff/self-review gates; do not rely on pre-review test output.

- [ ] **Step 7: Push and update the review thread**

Push to `origin/fix/issue-24550-percentile-quantile-agg`. Reply in the existing inline thread with the planner/compile/executor changes and verification evidence, then resolve the thread only after GitHub shows the new head and the PR is mergeable.
