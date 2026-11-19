# Gobra verification of the bitmap core

`bitmap.go` is formally verified with [Gobra](https://github.com/viperproject/gobra).
`bitmap_ext.go` (copy/remap/serialization, `unsafe`, generics) is not.

```
GOBRA_JAR=/path/to/gobra.jar Z3_EXE=/path/to/z3 ./gobra/verify.sh
```

Needs Java 11+ and Z3 4.16. A full run takes about 6 minutes.

## What is proven

For every method in `bitmap.go`:

- **Memory safety and termination.** Every word access is in bounds, and every loop terminates.
- **The invariant `Inv` is preserved.** It has three parts:
  - `len(data) == ceil(numBits/64)`, and the bitmap owns every word up to `cap(data)`;
  - no bit at or beyond `numBits` is set;
  - `popCount` is the number of set bits.
- **Functional behavior, in terms of the ghost `Has(r)`.** `Has(r)` means row `r` is below `Len` and its bit is set.
  - `Add`, `Remove`, `AddRange`, `RemoveRange`, `Clear`, `InitWithSize`, `Reset`: exactly the requested rows change.
  - `Or`, `OrBounded`, `And`: union and intersection with the other bitmap.
  - `Negate`: every row below `Len` flips.
  - `TryExpand`, `TryExpandWithSize`, `Reserve`: the rows are unchanged, and new rows are clear.
  - `Contains(r)` returns `Has(r)`.
  - `IteratorValue`, `Next`: each returns the next set row and skips none.
  - `CountIsRows`: `Count()` is the number of set rows. `EmptyIsNone`: `IsEmpty()` is true exactly when no row is set.

The proof assumes callers meet the preconditions, which the Go code does not check. `Add` and `AddRange` need rows below `Len`. `Or` and `And` need `b != n`. `Next` must be called only while `HasNext()` is true. The code in `bitmap_ext.go` is trusted to keep `Inv`.

## What is trusted

The trusted base has three parts:

- **The bit-operation model** in `stubs/math/bits/bits.gobra`. Gobra treats `& | ^ &^ << >>` as uninterpreted functions, so the model gives them meaning through `Bit(w, k)`.
- **The stub specs** for `bits.OnesCount64` and `bits.TrailingZeros64`.
- **Gobra and Z3** themselves.

`TestGobraBitModel` (`gobra_model_test.go`) checks every model axiom and stub spec against the real Go operators, for every bit position and many words.

All lemmas in `bitmap_lemmas.gobra` are proven. None of them is trusted.

## Writing verifiable code here

- Index words with `/64` and `%64`, never `>>6` and `&63`. For unsigned values the compiler emits the same code, and Gobra can reason about division.
- Write low masks as `^uint64(0) >> (64-t)`, not `(1<<t)-1`. The subtraction defeats the axiom triggers.
- Avoid `min`, `max`, `clear`, and `range` over an int. Gobra rejects them.
