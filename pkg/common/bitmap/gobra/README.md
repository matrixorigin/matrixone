# Gobra verification of the bitmap core

`bitmap.go` is formally verified with [Gobra](https://github.com/viperproject/gobra).
`bitmap_ext.go` is not verified. It holds copy and clone, serialization and validation, conversions to arrays and strings, and the code using `unsafe` and generics.

```
GOBRA_JAR=/path/to/gobra.jar Z3_EXE=/path/to/z3 ./gobra/verify.sh
```

Needs Java 11+ and Z3 4.16. A run takes about 12 minutes.

`verify.sh` gives every member of `bitmap.go` a Gobra run, and so a Z3 process, of its own, four at a time (`VERIFY_JOBS`). There are two exceptions. The lemmas get one run together. The pure functions, the predicates and `PeekNext` get one run together, because a pure function verified alone fails to check the predicate bodies it unfolds; the original proof behaves the same way. Viper verifies each member against the others' contracts, so separate runs prove the same as one run. In a single run, Silicon reuses each Z3 process across members, and several of these proofs then succeed or fail depending on which members that process verified before them. `verify.sh --one` does the single run anyway.

To verify only some members, pass their line numbers, e.g. `-i types.go bitmap.go@889,1010 bitmap_lemmas.gobra`. Gobra verifies all of `bitmap_lemmas.gobra` unless it is given as `bitmap_lemmas.gobra@1`.

## What is proven

For every method in `bitmap.go`:

- **Memory safety and termination.** Every word access is in bounds, and every loop terminates.
- **The invariant `Inv` is preserved.** It has three parts:
  - `len(data) == ceil(numBits/64)`, and the bitmap owns every word up to `cap(data)`;
  - no bit at or beyond `numBits` is set;
  - `popCount` is the number of set bits.
- **Functional behavior, in terms of the ghost `Has(r)`.** `Has(r)` means row `r` is below `Len` and its bit is set.
  - `Add`, `AddMany`, `Remove`, `AddRange`, `RemoveRange`, `Clear`, `InitWithSize`, `Reset`: exactly the requested rows change. An empty range is a no-op.
  - `InitWith(m)`: `n` becomes a copy of `m`. `Swap(m)`: `n` and `m` exchange contents.
  - `Or`, `OrBounded`, `And`: union and intersection with the other bitmap.
  - `Negate`: every row below `Len` flips.
  - `TryExpand`, `TryExpandWithSize`, `Reserve`: the rows are unchanged, and new rows are clear. `TryExpandWithSize(size)` sets the length to `max(Len, size)`.
  - `Contains(r)` returns `Has(r)`.
  - `IteratorValue`, `Next`: each returns the next set row and skips none.
  - `RemapOrdered(sels, negate)` and `RemapOrderedUnchecked`, for a strictly increasing, non-negative `sels`. `RemapOrdered` checks that first and panics with the bitmap unchanged; `RemapOrderedUnchecked` leaves it to the caller:
    - `negate == false`: `Len` becomes `min(Len, len(sels))`, and row `i` becomes old row `sels[i]`. Rows at or beyond `len(sels)` are clear.
    - `negate == true`: `Len` is unchanged. Each row not in `sels` moves to its position minus `Rank(sels, s)`, the number of selected rows below it, and the freed tail is clear.
  - `RemapMaskOrdered(WithOffset)(sels, negate, offset)`: like `RemapOrdered`, where the selected rows are `r + offset` for the set rows `r` of `sels`, and a row's rank is `sels.CntBelow(r)`, the number of set rows of `sels` below it.
  - The remaps work a word at a time and in place. The kernels are proven separately:
    - `loadBits` reads 64 bits at any bit offset.
    - `compactBits` gathers the bits of a word under a keep mask, so that kept bit `b` lands at `PcUpTo(keep, b)`.
    - `remapPut` and `appendKept` append bits to an output stream that is written behind the word being read.
    - `selectWord` and `gatherSpan` build one output word of an ordered select.
    - `removedMask` turns the rows of `sels` in one word into a bit mask, and `bitsFrom` reads 64 rows of a selection bitmap at any offset.
    - `selectMaskWord` and `removeMaskWord` are one loop step of the mask remaps. Their contracts are the loops' invariants.
  - `Filter(sels)`: returns a bitmap of length `len(sels)` whose row `i` is old row `sels[i]`.
  - `IsSame(b)`: true exactly when the data words are equal. Equal words imply the same rows. Bit lengths are not compared, so the same rows stored in a different number of words compare unequal.
  - `CountRange(start, end)`: the number of set rows in `[start, min(end, Len))`.
  - `AnySetNotIn(other, start, end)`: whether some row in `[start, end)` is set in `n` and not in `other`.
  - `CountIsRows`: `Count()` is the number of set rows. `EmptyIsNone`: `IsEmpty()` is true exactly when no row is set.

The ghost method `Release` gives up `Inv` and returns plain access to the struct, for callers about to reinitialize it.

The proof assumes callers meet the preconditions, which the Go code does not check. `Add` and `AddRange` need rows below `Len`. `Or` and `And` need `b != n`. `Next` must be called only while `HasNext()` is true. The code in `bitmap_ext.go` is trusted to keep `Inv`.

## Use from other packages

`types.go`, `bitmap.go`, `bitmap_lemmas.gobra` and the `math/bits` stub carry the `// +gobra` header. A package that imports the bitmap and runs Gobra with `-o` sees only those files, so it gets the verified specifications and none of `bitmap_ext.go`. `pkg/container/nulls/gobra/verify.sh` works this way.

## What is trusted

The trusted base has three parts:

- **The bit-operation model** in `stubs/math/bits/bits.gobra`. Gobra treats `& | ^ &^ << >>` as uninterpreted functions, so the model gives them meaning through `Bit(w, k)`. Shifts by any count are modeled as Go defines them: a shift of 64 or more gives zero.
- **The stub specs** for `bits.OnesCount64` and `bits.TrailingZeros64`.
- **Gobra and Z3** themselves.

`TestGobraBitModel` (`gobra_model_test.go`) checks every model axiom and stub spec against the real Go operators, for every bit position and many words.

All lemmas in `bitmap_lemmas.gobra` are proven. None of them is trusted.

## Writing verifiable code here

- Index words with `/64` and `%64`, never `>>6` and `&63`. Gobra can reason about division but not shifts.
  - For unsigned values the compiler emits the same code.
  - For a signed value, convert first (`uint64(x)/64`). Signed division adds a sign fix-up, which made `RemapOrdered` 25% slower until it was converted.
- Write low masks as `^uint64(0) >> (64-t)`, not `(1<<t)-1`. The subtraction defeats the axiom triggers.
- Avoid `min`, `max`, `clear`, and `range` over an int. Gobra rejects them.
- Let lemmas and read-only methods take a smaller permission fraction than the caller holds, for example ⅛ or ¼. A callee that takes all of a caller's fraction makes Viper forget the bitmap's contents.
- Keep quantified facts out of lemma postconditions unless they are needed. A redundant `forall r :: Has(r) == old(Has(r))` turned a 40-second run into one that never finished.
- Prove fiddly arithmetic in small, heap-free lemmas in `bitmap_lemmas.gobra`.
- Keep each SMT query small. Put a loop body's semantic step into a heap-free ghost lemma over sequences, as `removeStepRank` and `selectMaskStep` do. Put work done per word into a function with its own contract, as `appendKept` and `removedMask` are. A loop body that carries many quantified invariants becomes unstable: assertions that passed fail after unrelated edits.
- Pass shared terms as ghost parameters, such as `pos` to `appendKept`, so that a callee's postcondition and the next lemma's precondition use the same term.
- A quantified assertion whose body does not mention the trigger term of the facts it needs cannot use them. Restate those facts with the trigger term in the conclusion.
- When a method is slow, run with `--assertTimeout 30000`. A single overloaded check then fails with its line number instead of hanging the whole run.
