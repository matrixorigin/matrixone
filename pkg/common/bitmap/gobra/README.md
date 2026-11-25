# Gobra verification of the bitmap core

`bitmap.go` and `bitmap_select.go` (the row-selection primitives) are formally verified with [Gobra](https://github.com/viperproject/gobra).
`bitmap_ext.go` is not verified. It holds copy and clone, serialization and validation, conversions to arrays and strings, and the code using `unsafe` and generics.
`bitmap_mem.go` (pool allocation) and `bitmap_view.go` (read-only bitmaps and views) are not verified either; `bitmap_mem.gobra` gives their contracts.

```
GOBRA_JAR=/path/to/gobra.jar Z3_EXE=/path/to/z3 ./gobra/verify.sh
```

Needs Java 11+ and Z3 4.16. A run takes about 12 minutes.

`verify.sh` gives every member of `bitmap.go` a Gobra run, and so a Z3 process, of its own, four at a time (`VERIFY_JOBS`). There are two exceptions. The lemmas get one run together. The pure functions, the predicates and `PeekNext` get one run together, because a pure function verified alone fails to check the predicate bodies it unfolds; the original proof behaves the same way. Viper verifies each member against the others' contracts, so separate runs prove the same as one run. In a single run, Silicon reuses each Z3 process across members, and several of these proofs then succeed or fail depending on which members that process verified before them. `verify.sh --one` does the single run anyway.

`bitmap_select.go` gets one run per member too, with `bitmap.go` loaded but not re-verified (`bitmap.go@1`). Runs for `bitmap.go`'s members leave `bitmap_select.go` out, so new members do not change the proof context of existing ones. Nothing in `bitmap.go` calls into `bitmap_select.go`, so the split loses nothing.

`removeOrderedWord`/`removeOrdered` and `removeMaskWord`/`removeMask` state their output relation through the named ghost functions `RemoveRel` and `RemoveMaskRel`. Spelled out as a quantifier in their contracts and loop invariants, these proofs passed or failed depending on unrelated edits elsewhere in `bitmap.go`, and even on the directory Gobra ran in, with identical files. With the relation named, each check is a match on one term, and all four members verify from every directory tried.

To verify only some members, pass their line numbers, e.g. `-i types.go bitmap.go@889,1010 bitmap_mem.gobra bitmap_lemmas.gobra`. Gobra verifies all of `bitmap_lemmas.gobra` unless it is given as `bitmap_lemmas.gobra@1`.

## What is proven

For every method in `bitmap.go`:

- **Memory safety and termination.** Every word access is in bounds, and every loop terminates.
- **The invariant `Inv` is preserved.** It has four parts:
  - the bitmap is owned: it has a pool and no view shift;
  - `len(data) == ceil(numBits/64)`, and the bitmap owns every word up to `cap(data)`;
  - no bit at or beyond `numBits` is set;
  - `popCount` is the number of set bits.
- **Functional behavior, in terms of the ghost `Has(r)`.** `Has(r)` means row `r` is below `Len` and its bit is set.
  - `Add`, `AddMany`, `Remove`, `AddRange`, `RemoveRange`, `Clear`, `InitWithSize`, `Reset`: exactly the requested rows change. An empty range is a no-op.
  - `InitWith(m)`: `n` becomes a copy of `m`. `Swap(m)`: `n` and `m` exchange contents, pools included.
  - Allocation failure: `InitWithSize`, `InitWith` and `Filter` return the pool's error and leave `n` `Raw` (it keeps its words, for reinitialization or `Free`). `TryExpand*`, `Reserve` and `growWords` return it and leave `n` unchanged.
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
  - Row-selection primitives (#33181):
    - `InitWithBools(values, nulls)`: row `r` is set exactly when `values[r]` is true and `r` is not in `nulls`; the length is `len(values)`.
    - `InitDeposit(base, sel)`: row `r` is set exactly when `r` is in `base` and `sel` holds `r`'s rank among `base`'s rows (`CntS`); the length is `base.Len()`.
    - `KeepFirstN(k)`: a row stays set exactly when it was set and fewer than `k` set rows lie below it. `keepLowBits(w, r)` keeps the `r` lowest set bits of a word; the rank argument is the heap-free lemma `keepFirstRows`.
    - `Word(i)`: bit `b` is row `64i+b`. `NumWords()` is `ceil(Len/64)`.
    - `depositBits(w, s)`: bit `b` is set exactly when bit `b` of `w` is set and bit `PcUpTo(w, b)` of `s` is set.

The `Init*` constructors and `Reset` take a `Raw` bitmap: the struct plus the words it owns, if it has a pool. The ghost method `Release` turns `Inv` into `Raw`, for callers about to reinitialize or free a bitmap. The ghost `Unowned` makes a pool-less struct (such as a zero `Bitmap`) `Raw`.

Read-only bitmaps (no pool) and views are outside the proof. `Inv` implies an owned bitmap, so the branches of the verified methods that dispatch to `bitmap_view.go` are unreachable for a verified caller; their stubs say `requires false`, and Gobra proves every such call dead.

The proof assumes callers meet the preconditions, which the Go code does not check. `Add` and `AddRange` need rows below `Len`. `Or` and `And` need `b != n`. `Next` must be called only while `HasNext()` is true. The code in `bitmap_ext.go` is trusted to keep `Inv`.

## Use from other packages

`types.go`, `bitmap.go`, `bitmap_lemmas.gobra` and the `math/bits` stub carry the `// +gobra` header. A package that imports the bitmap and runs Gobra with `-o` sees only those files, so it gets the verified specifications and none of `bitmap_ext.go`. `pkg/container/nulls/gobra/verify.sh` works this way.

## What is trusted

The trusted base has four parts:

- **The memory primitives** in `bitmap_mem.gobra`: `allocWords` returns fresh zero words from the pool, `replaceWords` returns the old words and installs new ones, `Free`, `prepareInit`, `mustWrite` (a no-op on an owned bitmap) and `MPool`. Their bodies are in `bitmap_mem.go`. `mustOK` is trusted to return only for a nil error (it panics otherwise).
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
- The bit model has no axiom for unary minus or `w & (w-1)`. To visit set bits, use `t := bits.TrailingZeros64(w)` and `w &^= uint64(1) << uint(t)`; the compiler emits `TZCNT` and the cost is the same.
- A quantified relation that crosses a call or a loop boundary (a postcondition the caller's loop invariant restates) is checked by quantifier instantiation, and Z3 may miss the instantiation for reasons as unrelated as the file path. Name such a relation with a ghost pure function, as `RemoveRel` does, and state contracts and invariants with it. The quantifier is still available where a proof needs it, through the function's definition.
- Add new members in a separate file and verify them with `bitmap.go` loaded, as `bitmap_select.go` does, instead of growing `bitmap.go`.
- Keep heavy rank reasoning out of methods that hold the heap. Written as one method, `KeepFirstN` did not finish in 40 minutes. With the word loop in the helper `keepLowBits` and the rank facts in the lemma `keepFirstRows` (over plain sequences), each part verifies in a few minutes.
- A ghost call that takes a fraction of a predicate (`rowFacts`, `EmptyIsNone`) makes pure values read before it unrelated to those read after it. Call such lemmas before computing a pure value, such as `IsEmpty()`, that later reasoning depends on.
- Over a slice, `for i := range d /*@ with i0 @*/` works, but Gobra cannot establish bounds on `i` or `i0` when `d` is empty. Guard them: `invariant len(d) > 0 ==> 0 <= i && i <= len(d) && 0 <= i0 && i0 <= len(d)`, and bound quantifiers by `x < len(d)` too. Keep the range form: `for i := range d { d[i] = 0 }` compiles to a memory clear.
- Silicon proves a quantified precondition most reliably when it is the same term as a fact in context. Pass the caller's exact term (`64*out+int(nbits)`, not a ghost copy `pos`) and keep the output stream as a ghost `seq` (`SD`, `sd0`/`sd1`) rather than `seq(n.data)`; `removeOrdered` needed both, plus the identity lemma `carryRemove` at the end of the loop body.
- Let lemmas and read-only methods take a smaller permission fraction than the caller holds, for example ⅛ or ¼. A callee that takes all of a caller's fraction makes Viper forget the bitmap's contents.
- Keep quantified facts out of lemma postconditions unless they are needed. A redundant `forall r :: Has(r) == old(Has(r))` turned a 40-second run into one that never finished.
- Prove fiddly arithmetic in small, heap-free lemmas in `bitmap_lemmas.gobra`.
- Keep each SMT query small. Put a loop body's semantic step into a heap-free ghost lemma over sequences, as `removeStepRank` and `selectMaskStep` do. Put work done per word into a function with its own contract, as `appendKept` and `removedMask` are. A loop body that carries many quantified invariants becomes unstable: assertions that passed fail after unrelated edits.
- Pass shared terms as ghost parameters, such as `pos` to `appendKept`, so that a callee's postcondition and the next lemma's precondition use the same term.
- A quantified assertion whose body does not mention the trigger term of the facts it needs cannot use them. Restate those facts with the trigger term in the conclusion.
- When a method is slow, run with `--assertTimeout 30000`. A single overloaded check then fails with its line number instead of hanging the whole run.
