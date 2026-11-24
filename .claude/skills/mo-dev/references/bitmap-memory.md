# Bitmap / Nulls Memory Model

Read this before you create, hold, copy, or free a `bitmap.Bitmap` or
`nulls.Nulls`, or review code that does.

## Storage

- A `Bitmap`'s words come off-heap from its `mpool.MPool` (`Bitmap.mp`).
  They go back to that pool only in `Free`. The Go GC never reclaims them, so
  a `Bitmap` that owns words and is dropped without `Free` leaks pool memory.
- `Nulls` is a thin wrapper around one `Bitmap`. `Nulls.Free` is `np.Free`.
- A bitmap has exactly one pool for its whole life. `Init(mp)` gives an empty
  bitmap its pool, and `Init*` calls that name a different pool return an
  error. Do not move words between pools, and do not pass a "per-call" pool
  that differs from the bitmap's own (same rule as Vector; see the vector
  ownership notes in `pkg/container/vector`).
- Owned words always begin at the allocation's first byte (`data[:1]`
  identifies the block for `mpool.Free`). Never reslice owned `data` from the
  front.

## The three legal states

Every `Bitmap`/`Nulls` in the engine is in exactly one of these states.

1. **Inside a Vector (the common case).** The vector's `nsp` is owned by the
   vector and uses the vector's `mp`. The vector manages its whole lifecycle:
   `Vector.Free` / reset / `CleanOnlyData` free or reuse it. Code that works
   on a vector's nulls (`v.GetNulls()`, `nulls.Add(v.GetNulls(), ...)`) must
   **not** `Free` it, keep it after the vector is freed or reused, or install
   a `Nulls` that owns another pool's words into the vector.
   A **read-only vector** (`v.mp == nil`) may still own its `nsp`. Arrow
   validity (`arrowbridge.installValidity`), Sirius results, and
   `Vector.DetachNulls` all give one an nsp allocated from a pool. The
   vector is still its single owner: `Vector.Free` frees it. Never treat
   "read-only vector" as "owns nothing". Free it, don't overwrite the
   struct, and check `v.GetNulls().IsReadOnly()` before reusing it as a
   view target (`WindowInto` rejects such a target).
2. **Read-only (`mp == nil`).** It references words it does not own:
   `UnmarshalNoCopy` (decoded from a buffer), `InitView` (rows `[start, end)`
   of another bitmap, sharing its words), or a zero value. It never allocates
   or writes. A write panics with `ErrReadOnly`, and an allocating call
   (`InitWithSize`, `TryExpand`, ...) returns an error. `Free` only drops the
   reference. The view or decoded bitmap is valid only while the source
   words are alive and unchanged; it must never outlive its source buffer or
   bitmap. `IsReadOnly()` tells the states apart.
3. **Owned and managed directly by another component.** For example, an
   operator's or a table reader's own `Nulls`/`Bitmap`, a delete mask, or a
   tombstone bitmap. It has a pool, and its holder **owns it and must `Free`
   it** on every exit path: normal end, error, cancel, `Reset`/reuse, and
   when it is replaced by a new allocation. Treat it like any other mpool
   allocation in the owner's `Free`/`Reset` (see
   [operator-pipeline.md](operator-pipeline.md) for operator Reset/Free
   rules).

Anything else is a bug: a pooled bitmap with no clear owner, two owners that
both `Free`, a read-only view that outlives its source, or a direct owner
that forgets `Free`.

## Rules

- **Pick the state when you create it.** If the bitmap is going into a
  vector, build it with the vector's pool and hand it to the vector. If it is
  only read, prefer a read-only view or `UnmarshalNoCopy` (no allocation). If
  it is a standalone working bitmap, record who frees it.
- **Copying the struct is not copying the bitmap.** `b2 := *b` (or a `Nulls`
  value copy) makes two headers over the same pooled words, and a double
  `Free` or a use-after-free follows. Use `InitWith`/`Clone` for a copy,
  `Swap` to exchange contents or hand a bitmap over (the giver keeps the
  receiver's old contents and still frees them).
- **Re-initialization reuses words.** `InitWithSize`, `InitWith`, `Reset`
  and `Build` on an owned bitmap keep its pool and may reuse its words. On a
  read-only bitmap they first drop the borrowed words and take the given
  pool. That bitmap then becomes owned (state 3, or state 1 inside a vector),
  and someone must `Free` it.
- **Allocation can fail.** `Init*`, `Clone`, `TryExpand*`, `Reserve`,
  `Filter`, `Range` return the pool's error. Propagate it. The in-place
  writers (`Add`, `Set`, `AddRange`, `Or`) panic instead.
- **`Free` is idempotent** and leaves the pool attached, so a freed bitmap
  can be re-initialized. Freeing a read-only bitmap never touches the source.
- **Leak checks.** Tests should use a pool with leak accounting
  (`mpool.MustNewZero` / `mp.CurrNB()`) and assert it returns to zero after
  the component's `Free`. This is the cheapest proof that state 3 is managed.

## Known traps (found in the 2026-11 leak review)

- **Pools with an overflow path.** `fileservice.Pool.Get` allocates with
  `newFunc` when its slots are busy and calls `finallyFunc` on `Put` of that
  value. A pool of bitmaps needs a `finallyFunc` that calls `Free`;
  `objectio.BitmapPool` leaked every overflow bitmap until it had one.
- **Partly built batches on error.** A function that creates a batch or
  vectors and then fails must close or clean them on that path (for example
  `baseObject.Scan`, `HybridScanByBlock`, `convertRowsIntoBatchInLocation`). A
  partly built batch carries its `Deletes`.
- **Field overwritten by a new allocation.** Free the old value first, even
  when every caller seems to free it before re-entry.
- **Discarded results.** `_ = f()` or `_, err = f()` that drops an
  `objectio.Bitmap` or `*Nulls` leaks it. Release or Free it.

## Verified core

`pkg/common/bitmap/bitmap.go` and `pkg/container/nulls/nulls.go` are formally
verified with Gobra (`pkg/common/bitmap/gobra/README.md`,
`pkg/container/nulls/gobra/README.md`). The proof covers owned bitmaps only.
Read-only bitmaps and views live in `bitmap_view.go`, and pool allocation in
`bitmap_mem.go`, both trusted via `bitmap_mem.gobra`. When you change a
contract in those files, rerun both `gobra/verify.sh` scripts. They cap each
JVM at 2 GB and size the job count from available memory. Never start Gobra
runs in an unbounded loop or background fan-out: about 20 runs at once
exhausted the 61 GB dev machine.
