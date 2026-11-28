# Type dispatch

How code that behaves differently per data type is structured. The goal: adding
a type means one change in one place, and every caller is correct.

## Two kinds of switch

| Kind | What it does | Where it lives |
|------|--------------|----------------|
| **General** | Has to handle all, or almost all, types: format, parse, compare, hash, encode/decode, size, map key, SQL literal, min/max, statistics value, test data | Only in `pkg/container/types` or `pkg/container/vector`, as one switch or one table row per type. Callers get a function from it; they do not switch themselves. |
| **Overload** | Picks between a few related ids to call the right variant: a time function choosing its date/datetime/timestamp version, a kernel for a numeric subfamily | Anywhere. It names only the ids it handles, and its `default` returns an error or calls the general path. |

If a switch at a call site is growing toward "every type", it is a general
switch in the wrong place. Do not extend it, even to add the one type it misses.
Delete it and use the shared operation. If the shared operation lacks something,
add it once in `types`/`vector`, with its case for every type.

## Where the shared operations are

- `pkg/container/types/traits_table.go`: per-type static facts (size, family
  flags such as `IsDecimal`, `IsDateRelate`, `IsFloat`). Read a fact here, don't
  re-list the ids.
- `pkg/container/types`: one switch per value operation. `ValueText` /
  `AppendValueText` (the one text form, for logs, dumps and error messages),
  `AppendSQLLiteral` / `AppendMOSQLLiteral` (SQL literals), `ParseValue` (text
  to value), `RawToFloat64` (order-preserving float64 for statistics).
- `pkg/container/vector/type_ops.go`: the type-ops table. Each row gives the
  per-type functions for a vector: `GetAny`/`SetAny`, `RowKey` / `RowKeyView` /
  `RowKeyValue` (map keys), `ValueFromRaw` / `RawFromValue`, `ExportValue`
  (values for CDC and index maintenance), `SetConstValue`. A new per-type
  vector operation is a new column on this table, not a switch at the caller.
  `TestValueTextCoversTypeOps` checks every type id against them.
- Design: `docs/design/20261126-type-vft.md`.

Prefer function pointers: resolve the type once to a function (from the table,
or one switch that returns a closure or a generic instance), then call it.

## Never dispatch inside a vector loop

Every decision that depends on the type, the scale/width, or a constant argument
(is the vector const, null-free, the same type as another, ...) is made **once,
before the loop**. Then the loop calls one function, or runs one specialized
loop, with no per-row `switch`, type assertion, or `if` on those facts.

```go
// wrong: decides the type for every row
for i := 0; i < n; i++ {
    switch typ.Oid {
    case types.T_int64: ...
    case types.T_decimal128: ...
    }
}

// right: decide once, then loop
fn := opsFor(typ)          // or a switch that picks a generic instance
for i := 0; i < n; i++ {
    fn(vec, i)
}
```

The same applies to `if vec.IsConst()`, `if nulls.Any()`, and scale checks:
branch once into separate loops, don't test on every row.

## Checks

- `TestTypeSwitchFamilies` and `pkg/container/types/testdata/README.md`: a switch
  that names some members of a family must name all of them, or exactly one
  subfamily. `typeswitch_baseline.txt` may only shrink.
- When the test reports a missing member at a general switch outside
  `types`/`vector`, the fix is to remove that switch and route through the
  shared operation, not to add the member.
- When outputs differ between the site switches being merged (error text,
  number of fraction digits, quoting), pick one form and update the UT/BVT
  expected results.
