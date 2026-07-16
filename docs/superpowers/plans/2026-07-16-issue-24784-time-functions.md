# Issue #24784 TIME Functions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `HOUR(TIME)`, `TIME_FORMAT`, and `MAKETIME` preserve large hours, signs, fractional seconds, and SQL-visible precision.

**Architecture:** Keep the fix in the existing SQL function registry and vectorized execution functions. Widen only the TIME overload of HOUR, centralize sign handling in `timeFormat`, and normalize MAKETIME seconds to whole seconds plus microseconds before constructing `types.Time`.

**Tech Stack:** Go, MatrixOne vectorized SQL functions, `types.Time`, `testify`, distributed SQL test files.

## Global Constraints

- Work from `upstream/main` in the isolated `codex/fix-24784-time-functions` worktree.
- HOUR must cover every value through `types.MaxHourInTime` and ignore the TIME sign.
- TIME_FORMAT must emit one leading minus sign for negative TIME values.
- MAKETIME accepts hours in `[-838,838]`, minutes in `[0,59]`, and seconds in `[0,60)`.
- Round fractions to at most six microsecond digits and normalize carries.
- Invalid or out-of-range MAKETIME rows append NULL; warnings and saturation are out of scope.
- Add no dependencies and do not restructure the existing large function files.

---

### Task 1: Preserve Complete HOUR(TIME) Values

**Files:**
- Modify: `pkg/sql/plan/function/func_unary_test.go`
- Modify: `pkg/sql/plan/function/func_unary.go`
- Modify: `pkg/sql/plan/function/list_builtIn.go`

**Interfaces:**
- Consumes: `types.Time.ClockFormat()`.
- Produces: TIME overload results as `uint32`; HOUR overload 2 returns `types.T_uint32`.

- [ ] **Step 1: Write the failing test**

Extend the TIME case in `initHourTestCase` with positive and negative 272-hour values and `types.MaxHourInTime`. Its expected result is:

```go
NewFunctionTestResult(types.T_uint32.ToType(), false,
    []uint32{15, 0, 23, 272, 272, uint32(types.MaxHourInTime)},
    []bool{false, false, false, false, false, false})
```

- [ ] **Step 2: Run RED**

Run the verification test command with `-run '^TestHour$'`. Expect a type mismatch and the 272-hour value folded to 16.

- [ ] **Step 3: Implement the minimal fix**

```go
func TimeToHour(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
    return opUnaryFixedToFixed[types.Time, uint32](ivecs, result, proc, length, func(v types.Time) uint32 {
        hour, _, _, _, _ := v.ClockFormat()
        return uint32(hour)
    }, selectList)
}
```

Change only HOUR overload 2 in `list_builtIn.go` to `types.T_uint32.ToType()`.

- [ ] **Step 4: Run GREEN**

Run `TestHour` again and expect PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/sql/plan/function/func_unary.go pkg/sql/plan/function/func_unary_test.go pkg/sql/plan/function/list_builtIn.go
git commit -m "fix: preserve complete time hours"
```

### Task 2: Preserve TIME_FORMAT Hours and Signs

**Files:**
- Modify: `pkg/sql/plan/function/func_binary_test.go`
- Modify: `pkg/sql/plan/function/func_binary.go`

**Interfaces:**
- Consumes: `ClockFormat` including `isNeg`.
- Produces: one leading sign and a full-hour `%T` rendering.

- [ ] **Step 1: Write the failing tests**

Extend the `%T` case with `123:45:06` and `-123:45:06`, expecting:

```go
[]string{"15:30:45", "123:45:06", "-123:45:06"}
```

Add a negative value with format `elapsed=%H:%i:%s`, expecting `-elapsed=123:45:06`.

- [ ] **Step 2: Run RED**

Run the verification test command with `-run '^TestTimeFormat$'`. Expect `%T` to produce `03:45:06` and the sign to be absent.

- [ ] **Step 3: Implement sign and hour handling**

At the start of `timeFormat`:

```go
hour, minute, sec, msec, isNeg := t.ClockFormat()
if isNeg {
    buf.WriteByte('-')
}
```

Change `%T` to:

```go
fmt.Fprintf(buf, "%02d:%02d:%02d", hour, minute, sec)
```

- [ ] **Step 4: Run GREEN**

Run `TestTimeFormat` again and expect PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/sql/plan/function/func_binary.go pkg/sql/plan/function/func_binary_test.go
git commit -m "fix: preserve time format hours and signs"
```

### Task 3: Preserve MAKETIME Fractions, Scale, Carries, and Negative Hours

**Files:**
- Modify: `pkg/sql/plan/function/func_binary_test.go`
- Modify: `pkg/sql/plan/function/function_test.go`
- Modify: `pkg/sql/plan/function/func_binary.go`
- Modify: `pkg/sql/plan/function/list_builtIn.go`

**Interfaces:**
- Produces: `makeTimeReturnType(parameters []types.Type) types.Type`.
- Produces: `makeTimeFromInt64(hour, minute, second int64, microsecond uint32, rs *vector.FunctionResult[types.Time], i uint64) error`.
- Produces: second extractors returning `(int64, uint32, bool)`.

- [ ] **Step 1: Write failing execution tests**

Add `TestMakeTimeFractionAndSign` with float64 scale-6 vectors for these rows:

```go
hours := []float64{12, -12, 12, 838, 12, 12}
minutes := []float64{34, 34, 59, 59, 34, 34}
seconds := []float64{56.789012, 56.789012, 59.9999996, 59.9999996, math.NaN(), math.Inf(1)}
expected := []types.Time{
    types.TimeFromClock(false, 12, 34, 56, 789012),
    types.TimeFromClock(true, 12, 34, 56, 789012),
    types.TimeFromClock(false, 13, 0, 0, 0), 0, 0, 0,
}
nulls := []bool{false, false, false, true, true, true}
```

Use a TIME(6) expected result.

- [ ] **Step 2: Write failing return-scale tests**

Add `TestMakeTimeReturnScale` in `function_test.go`. Assert all-int64 arguments return TIME(0). Resolve arguments `(int64, int64, decimal128(20,6))`; assert an implicit cast is requested and the result is TIME(6).

- [ ] **Step 3: Run RED**

Run with `-run 'TestMakeTimeFractionAndSign|TestMakeTimeReturnScale'`. Expect discarded fractions, NULL negative hours, and scale 0.

- [ ] **Step 4: Implement return scale**

```go
func makeTimeReturnType(parameters []types.Type) types.Type {
    scale := parameters[2].Scale
    if scale < 0 {
        scale = 0
    } else if scale > 6 {
        scale = 6
    }
    return types.T_time.ToTypeWithScale(scale)
}
```

Use this helper for every MAKETIME overload.

- [ ] **Step 5: Normalize float seconds**

Integer extractors return `(value, 0, null)`. The float extractor becomes:

```go
val, null := secondParam.GetValue(i)
if null || math.IsNaN(val) || math.IsInf(val, 0) || val < 0 || val >= 60 {
    return 0, 0, true
}
total := int64(math.Round(val * float64(types.MicroSecsPerSec)))
return total / types.MicroSecsPerSec, uint32(total % types.MicroSecsPerSec), false
```

- [ ] **Step 6: Construct the signed normalized TIME**

Validate `hour` in `[-838,838]`, `minute` in `[0,59]`, `second` in `[0,60]`, and `microsecond < types.MicroSecsPerSec`. Determine the sign before negating the hour. Call:

```go
timeValue := types.TimeFromClock(isNegative, uint64(hour), uint8(minute), uint8(second), microsecond)
```

Inspect `ClockFormat` after normalization and append NULL when the result hour exceeds 838. Pass microseconds through the MakeTime row loop.

- [ ] **Step 7: Run GREEN and adjacent regressions**

Run `TestMakeTimeFractionAndSign|TestMakeTimeReturnScale`, then `TestMakeTime|TestTimeFormat|TestHour`. Expect PASS.

- [ ] **Step 8: Commit**

```bash
git add pkg/sql/plan/function/func_binary.go pkg/sql/plan/function/func_binary_test.go pkg/sql/plan/function/function_test.go pkg/sql/plan/function/list_builtIn.go
git commit -m "fix: preserve maketime fractional seconds"
```

### Task 4: Add SQL-Visible Regression Coverage

**Files:**
- Modify: `test/distributed/cases/function/func_datetime_hour.test`
- Modify: `test/distributed/cases/function/func_datetime_hour.result`
- Modify: `test/distributed/cases/function/func_datetime_time_format.test`
- Modify: `test/distributed/cases/function/func_datetime_time_format.result`
- Modify: `test/distributed/cases/function/func_datetime_maketime.test`
- Modify: `test/distributed/cases/function/func_datetime_maketime.result`

**Interfaces:**
- Produces: SQL-visible regressions for all five compatibility changes.

- [ ] **Step 1: Add HOUR cases**

Add positive and negative `272:59:59` casts. Both expected HOUR values are `272`.

- [ ] **Step 2: Add TIME_FORMAT cases**

Add `%T` for `123:45:06` expecting `123:45:06`, and `elapsed=%H:%i:%s` for `-123:45:06` expecting `-elapsed=123:45:06`.

- [ ] **Step 3: Add and update MAKETIME cases**

Add:

```sql
SELECT MAKETIME(12, 34, 56.789012) AS fractional_seconds;
SELECT MAKETIME(-12, 34, 56.789012) AS negative_fractional;
SELECT MAKETIME(12, 59, 59.9999996) AS fractional_carry;
```

Expect `12:34:56.789012`, `-12:34:56.789012`, and `13:00:00.000000`. Update existing `MAKETIME(-1,15,30)` from NULL to `-01:15:30`; update `MAKETIME(12.7,15.8,30.9)` from `12:15:30` to `12:15:30.9` and revise its comment.

- [ ] **Step 4: Inspect and commit**

Run `git diff --check`, verify each statement has one result block, then commit the six files with `test: cover compatible time function semantics`.

### Task 5: Full Verification and Independent Review

**Files:**
- Inspect: complete diff versus `upstream/main`.
- Modify: only files implicated by a proven verification or review finding.

- [ ] **Step 1: Format and inspect**

Run gofmt on all changed Go files, `git diff --check`, `git diff --stat upstream/main...HEAD`, and `git status --short`.

- [ ] **Step 2: Build, vet, and test**

With the environment below, run `go build ./pkg/sql/plan/function`, `go vet ./pkg/sql/plan/function`, the complete function package test, and a dependent `pkg/sql/plan` test. Every command must exit 0.

- [ ] **Step 3: Prove red-green**

Retain the regression tests while temporarily reversing each implementation hunk. Run focused tests and record failures; restore the implementation and rerun to PASS. Confirm the restored diff is unchanged.

- [ ] **Step 4: Review**

Run `mo-self-review` against `upstream/main`, including functional closure and unhappy paths. Then dispatch the user-requested independent subagent with the issue, spec, plan, and full diff. Fix every concrete blocker and decision-log any rejected suggestion.

- [ ] **Step 5: Fresh final verification**

Repeat build, vet, complete package tests, dependent tests, `git diff --check`, and status inspection after all review fixes.

## Verification Environment

```bash
export CGO_CFLAGS="-I/Users/yanghaoyang/repo/matrixone/cgo -I/Users/yanghaoyang/repo/matrixone/thirdparties/install/include"
export CGO_LDFLAGS="-L/Users/yanghaoyang/repo/matrixone/thirdparties/install/lib -lusearch_c"
export DYLD_LIBRARY_PATH="/Users/yanghaoyang/repo/matrixone/cgo:/Users/yanghaoyang/repo/matrixone/thirdparties/install/lib"
export GO_TEST_LDFLAGS="-extldflags '-L/Users/yanghaoyang/repo/matrixone/cgo -lmo -L/Users/yanghaoyang/repo/matrixone/thirdparties/install/lib -Wl,-rpath,/Users/yanghaoyang/repo/matrixone/cgo -Wl,-rpath,/Users/yanghaoyang/repo/matrixone/thirdparties/install/lib'"
go test -ldflags="$GO_TEST_LDFLAGS" -count=1 -timeout 120s -v ./pkg/sql/plan/function
```
