# Gobra verification of the Nulls core

`nulls.go` is formally verified with [Gobra](https://github.com/viperproject/gobra).
`nulls_ext.go` is not verified. It holds raw pointers, serialization, string and array conversions, closures and generics.

```
GOBRA_JAR=/path/to/gobra.jar Z3_EXE=/path/to/z3 ./gobra/verify.sh
```

A full run takes about 30 seconds.

## How the proof is put together

The proof imports `pkg/common/bitmap` and uses only the specifications of its methods. Gobra does not re-verify their bodies here. Run `pkg/common/bitmap/gobra/verify.sh` to verify them.

`verify.sh` passes `-o`, so Gobra reads only files with the `// +gobra` header. In the bitmap package those are `types.go`, `bitmap.go`, `bitmap_lemmas.gobra` and the `math/bits` stub. The `*_ext.go` files and the tests are left out of both packages. Imports resolve through `-m github.com/matrixorigin/matrixone` and `-I <repo root>`.

## What is proven

A non-nil `*Nulls` is valid when its bitmap keeps `bitmap.Inv`. The ghost `Has(r)` means row `r` is NULL; a nil `*Nulls` has no rows. For every function in `nulls.go`:

- **Memory safety and termination.** No nil dereference, out-of-range index or unbounded loop.
- **The bitmap invariant is preserved.**
- **Functional behavior, in terms of `Has`.**
  - `InitWithSize`, `NewWithSize`, `Reset`, `Clear`: no row is set. `Clone`, `InitWith`: the same rows and length as the source.
  - `Set`, `Add`, `AddRange`, `Build`: exactly the given rows are added, and the length grows to cover them. `Add` accepts rows in any order.
  - `Unset`, `Del`, `DelI64`, `RemoveRange`: exactly the given rows are removed. `DelI64` ignores negative rows.
  - `Or(nsp, m, r)`: `r` gains the rows of `nsp` and `m`; `r` may alias either. If both are empty, `r` is reset to length 0 and keeps none of its own rows.
  - `OrBitmap`, the `Or` method: union.
  - `Contains(r)` returns `Has(r)`. `Count`, `CountRange`, `Len`, `IsEmpty` and `Any` return the bitmap's values.
  - `FilterCount(nsp, sels)` counts the entries of `sels` that are NULL rows.
  - `Range(nsp, start, end, bias, b)`: row `i` of `b` is row `i+bias` of `nsp` for `i` in `[start-bias, end-bias)`, and no other row is set. If `nsp` has no rows, `b` is left unchanged.
  - `Filter(nsp, sels, false)`: the length becomes `len(sels)`, and row `i` becomes old row `sels[i]`. `sels` may be in any order and may hold negative or out-of-range entries.
  - `Filter(nsp, sels, true)` and `FilterInPlaceOrdered`, for strictly increasing, non-negative `sels`: as `bitmap.RemapOrdered`, in place. With `negate == true` the rows named by `sels` are removed and the kept rows are packed at the front. `FilterInPlaceOrdered` with `negate == false` then grows the length to `len(sels)`, matching `Filter`.
  - `FilterByMask(WithOffset)` and the `InPlace` variants: the same, in place, where the selected rows are `r + offset` for the set rows `r` of the selection bitmap. With `negate == false`, the length becomes `sels.Count()`.
  - `IsSame`: when it returns true, the two have the same rows.
  - Every `Filter*` function leaves a `Nulls` with no rows unchanged.

## Preconditions the Go code does not check

- `Set`, `TryExpand`, `Clear` and `RemoveRange` need a non-nil `*Nulls`. So does `r` in `Or`, and `b` in `Range`, which also must not be `nsp`.
- `Range` needs `bias <= start` and `bias <= end`.
- `Filter` with `negate == true`, and `FilterInPlaceOrdered`, need strictly increasing, non-negative `sels`. Unlike `bitmap.RemapOrdered`, they do not check it.
- `FilterByMask*` need a non-nil selection bitmap that is not the `Nulls`' own bitmap.
- Functions that overwrite a `Nulls` (`InitWith`, `InitWithSize`, `Reset`, `Build`) only need the struct, not a valid bitmap. A caller holding a valid one gives it up with the ghost `bitmap.Release`.

## What is trusted

- The bitmap specifications. They are proven in `pkg/common/bitmap`, which trusts its own bit-operation model and the `math/bits` stub specs. See `pkg/common/bitmap/gobra/README.md`.
- Gobra and Z3.

## Notes for editing

- `uint64` parameters need `requires 0 <= x`, because Gobra does not assume it.
- Loops that read a slice the method holds at ½ permission lose its values in Viper. Add the invariant `sels[k] == old(sels[k])`. In postconditions, write a whole expression over `sels` inside one `old(...)`.
- State loop invariants with `nsp.np.Has`, not `nsp.Has`. The bitmap postconditions trigger on `np.Has`, and a quantifier over `nsp.Has` does not fire across intermediate states.
- Gobra cannot spread a variadic parameter into another variadic call (`f(xs...)`). Pass it to a slice-typed helper, as `Add` does with `addRows`.
