# Issue #24784: MySQL-Compatible TIME Function Semantics

## Context

Issue #24784 reports three incorrect behaviors in the SQL function layer:

- `HOUR(TIME)` folds hours with `% 24`.
- `TIME_FORMAT(TIME, "%T")` folds hours with `% 24`.
- `MAKETIME(hour, minute, second)` discards fractional seconds.

Investigation also found two directly related compatibility gaps: `TIME_FORMAT`
drops the sign of negative `TIME` values, and `MAKETIME` rejects negative hours
that MySQL accepts. This change fixes all five behaviors on `main`.

## Compatibility Contract

The implementation will follow these rules:

1. `HOUR(TIME)` returns the absolute hour component without folding it into a
   24-hour clock. This matches MySQL, which ignores the sign when extracting the
   hour.
2. The `TIME` overload of `HOUR` returns `uint32`. MatrixOne's largest internal
   `TIME` hour, `types.MaxHourInTime` (`2,562,047,787`), fits in `uint32`, so the
   return type cannot truncate any valid MatrixOne `TIME` value.
3. `TIME_FORMAT` prefixes one minus sign for a negative `TIME`. The sign applies
   to the complete formatted result, matching MySQL's formatting flow.
4. `%H`, `%k`, and `%T` preserve the complete hour component. Twelve-hour
   specifiers (`%h`, `%I`, `%l`, `%p`, and `%r`) continue to use the hour modulo
   24 because they describe a time-of-day representation.
5. `MAKETIME` accepts hours from `-838` through `838`, minutes from `0` through
   `59`, and seconds from `0` up to but not including `60`. Negative hours create
   a negative `TIME`.
6. Fractional seconds are rounded to MatrixOne's six-digit microsecond storage
   precision. A rounded value that reaches one complete second carries into the
   next minute or hour.
7. The result `TIME` scale is inherited from the second argument's numeric scale
   and clamped to `0..6`. Integer seconds therefore produce scale 0, while a
   value such as `56.789012` produces scale 6.
8. `NaN`, positive or negative infinity, negative seconds, invalid minutes, and
   values whose normalized result exceeds `838:59:59.999999` return `NULL`.
9. Existing out-of-range policy remains unchanged: this work does not add
   MySQL-style warning emission or saturation for results beyond the supported
   `MAKETIME` range.

## Design

### HOUR(TIME)

`TimeToHour` will use `ClockFormat` and return its hour as `uint32`, without
`% 24`. The function registry will declare only the `TIME` overload as
`uint32`; timestamp and datetime overloads remain `uint8` because their hour is
always in `0..23`.

The sign returned by `ClockFormat` is intentionally ignored. This is a
documented compatibility choice, not an omission.

### TIME_FORMAT

`timeFormat` will retain the sign returned by `ClockFormat` and write `-` to the
output buffer before processing the format string. This keeps sign handling in
one place and avoids duplicating it across individual specifiers.

The `%T` branch in `makeTimeFormat` will format the complete hour directly.
Existing `%H` and `%k` behavior already preserves the complete hour and will not
change. Twelve-hour branches will remain unchanged.

### MAKETIME Fraction Conversion

The second-argument extractor will return a normalized representation containing
whole seconds and microseconds. Integer inputs produce zero microseconds. Float
inputs will:

1. reject non-finite values and values outside `[0, 60)`;
2. round `value * types.MicroSecsPerSec` to the nearest integer microsecond;
3. split the result into whole seconds and microseconds.

Splitting after rounding naturally represents a carry as 60 whole seconds and
zero microseconds. Construction will use total duration semantics through
`types.TimeFromClock`, which normalizes the carry. The normalized value is then
validated against the supported `MAKETIME` range before it is appended.

The hour helper will determine the sign, take the absolute hour after range
validation, and pass both to `types.TimeFromClock`. Range validation occurs
before absolute-value conversion, so there is no `math.MinInt64` overflow path.

### Result Precision

Each `MAKETIME` overload will use a shared return-type helper. It will copy the
third parameter's scale, clamp negative values to 0 and values above 6 to 6, and
return `types.New(types.T_time, 0, scale)`.

The planner preserves source numeric scale when it inserts an implicit cast to
`float64`, so decimal literals such as `56.789012` retain scale 6 through overload
resolution. This allows frontend serialization to call `Time.String2(6)` and
display the stored fraction.

## Error and Boundary Handling

- NULL propagation remains handled by the strict function contract and the
  existing vector null checks.
- Invalid rows append a NULL result without aborting evaluation of the batch.
- Floating-point validation happens before conversion to integers, preventing
  implementation-defined-looking results for `NaN` and infinity.
- A fractional carry is normalized before final range validation.
- No new allocations, goroutines, locks, waits, or external resources are
  introduced.

## Testing Strategy

Implementation will follow red-green-refactor:

1. Extend `TestHour` with hours above 23, a negative `TIME`, and
   `types.MaxHourInTime`; assert the `uint32` result type.
2. Extend `TestTimeFormat` with a large-hour `%T` case and negative formats that
   verify a single leading sign.
3. Add focused `MAKETIME` unit cases for six-digit fractions, negative hours,
   fractional carry, NULLs, invalid numeric values, and the upper boundary.
4. Add function-resolution tests proving that integer seconds return `TIME(0)`
   and a scale-6 numeric second returns `TIME(6)` after implicit casting.
5. Update distributed `MAKETIME`, `TIME_FORMAT`, and HOUR cases and expected
   results to exercise SQL-visible formatting.
6. Prove the regression tests fail without the implementation and pass with it.
7. Run formatting, package build, `go vet`, package tests with `-count=1`, a
   dependent-package regression test, and relevant race tests if the test
   environment supports them.

## Scope Boundaries

This change does not redesign MatrixOne's `TIME` representation, add new decimal
execution kernels, change time-zone behavior, or implement MySQL warning and
saturation behavior for out-of-range `MAKETIME` values.

