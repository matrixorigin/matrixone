# Approx Percentile Argument Contract Design

## Goal

Make `approx_percentile(value, percentile)` reject invalid percentile expressions with a user-facing error and guarantee that no invalid SQL, prepared parameter, or malformed plan can trigger an index or type panic while extracting the percentile configuration.

## Contract

- `percentile` must be a stable no-column expression.
- Literal `NULL`, column references, subqueries, and volatile expressions are rejected during binding.
- Foldable constant expressions are accepted.
- Prepared-statement parameters are accepted because their value is constant for one execution; a bound `NULL`, non-finite value, or value outside `[0, 1]` is rejected during compile-time configuration extraction.
- The executor retains equivalent validation so malformed or externally constructed plans cannot bypass the contract.

## Design

### Planner gate

Add one shared validation step in `BindFuncExprImplByPlanExpr`, before implicit casts are inserted. Use the existing constant-expression classifier with variables and prepared parameters treated as execution constants. Reject literal `NULL` separately. Because aggregate and window binding both use this entry point, the rule applies consistently to `constructGroup` and `constructWindow` without duplicated binder logic.

### Compile extraction gate

Change `getPercentileConfig` to return `([]byte, error)`. Validate that the evaluated vector is constant, non-null, non-empty, and one of the supported numeric types before accessing its payload. Convert the first value to `float64`, reject NaN, infinity, and values outside `[0, 1]`, then serialize it. Existing compile call sites propagate the resulting MatrixOne error through their established panic/recovery path instead of allowing a runtime bounds or type panic.

### Executor gate

Keep `parsePercentileConfig` as the final validation boundary and extend its range check to reject non-finite values. This protects partial-result restore and any plan construction path that bypasses the SQL binder or compile extractor.

## Error behavior

All invalid percentile inputs return `moerr` invalid-input errors that identify the `approx_percentile` percentile argument. Raw index-out-of-range and type-assertion panics are not permitted.

## Tests

- Planner rejects literal `NULL` and a column-dependent percentile expression.
- Planner accepts a numeric literal, a foldable numeric expression, and a prepared parameter.
- Compile extraction rejects non-constant, const-null, empty, unsupported, out-of-range, NaN, and infinity vectors without panicking.
- Executor configuration parsing rejects out-of-range, NaN, and infinity values.
- Existing aggregate execution, merge, intermediate-state, function registration, GROUP construction, and WINDOW construction tests remain green.
- Run focused tests first, then `go build`, `go vet`, and uncached tests for `pkg/sql/plan`, `pkg/sql/plan/function`, `pkg/sql/compile`, and `pkg/sql/colexec/aggexec` with the repository CGo environment.

## Scope boundaries

Do not extend the generic function type-check API, change percentile computation, alter result types, or refactor unrelated aggregate configuration handling.
