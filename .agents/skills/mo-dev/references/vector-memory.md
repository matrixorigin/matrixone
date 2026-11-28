# Vector Memory Model

Read this before you create, hold, pass through, modify, window, or free a
`vector.Vector`, or review code that does. Read it before you touch the
`Vector` struct at all. Every execution operator and every storage read path
follows these rules. For the batches that carry vectors, see
[batch-memory.md](batch-memory.md); for a vector's null bitmap, see
[bitmap-memory.md](bitmap-memory.md).

## Do not add fields to `Vector`

**Do not add a field to `vector.Vector` unless it is truly necessary, and
never on your own decision.** A `Vector` is a plain container: its type,
length, data, area, nulls and its one owner pool. Borrow flags, leases,
reference counts, offsets and extra allocation state do not belong in it.
That state belongs to the owner: the operator, the cache, or the lender.

**If a field really must be added, stop and ask a human developer to review
the design carefully, and continue only after they confirm it.** An AI agent
must not decide on its own to add a field to `Vector`.

## Ownership

- **Every vector has one owner.** During query execution the owner is
  usually an operator.
- The owner has an mpool and creates its vectors with that pool
  (`vector.NewVec(typ, mp)`). The vector remembers its pool (`v.MPool()`), and
  all of its memory is allocated from that pool, off heap. A vector that is
  never freed is a real leak: the GC does not reclaim it.
- **The pool must match exactly.** A call on a vector that takes an `mp` must
  get the very pool the vector was created with. Even a view of the same
  pool (for example `WithAccount`) is a mismatch. A mismatch returns an error,
  and `Free` panics (`checkMP`, `mustCheckMP`). Do not relax the check to make
  a call pass. Fix the caller so the vector is created with the right pool.
- **A nil pool means a read-only reference.** A vector that only references
  memory it does not own (the FileService cache, an Arrow buffer) is created
  with a nil pool. It is guaranteed read-only: any write or allocation fails,
  and `Free` only drops the reference.

## Operators and pools

- Operators that hold a lot of data (sort/order, top, group, hash build, and
  similar) create **their own pool** (`colexec.NewOperatorMPool`) for
  accounting and wholesale reset. Other operators use the process pool
  (`proc.Mp()`).
- **How many vectors an operator creates is fixed by the query plan, not by
  the amount of data.** Create each vector once and let the data flow
  through it: reuse it on every call, for example `CleanOnlyData` and refill.
  Creating new vectors for every batch (a `Dup` or a copied window on every
  `Call`) is a smell.

## Batch lifetime and modification

- Vectors travel between operators in batches. The lifetime promise: **a
  batch returned by `Call` stays valid until the next `Call`.**
- An operator **may** take a vector out of its input batch and put it
  straight into its output batch (pass-through). The promise above keeps it
  alive long enough.
- An operator **frees only the vectors it created**, never ones passed
  through to it.
- An operator that needs to **modify** a vector creates its own vector and
  writes the modified data there. **Never modify an input vector in place.**
- `Window` (on a vector or a batch) returns a read-only reference with a nil
  pool. It allocates and copies nothing, and, like pass-through, is valid
  only while the source vector is unchanged (for an input, until the next
  `Call`). Releasing a window only drops the reference (`Free(nil)`,
  `batch.FreeWindow()`). To modify those rows, or keep them longer, copy them
  into your own vector with `CloneWindow`/`CloneWindowTo`.
- Do not get around the pool check by freeing a vector with its own pool:
  `vec.Free(vec.MPool())`, or `batch.CleanOwned()`, which is for tests only.
- Writing into the transaction workspace (`Transaction.WriteBatch`) copies
  the data into the workspace's pool. The caller still owns its batch and
  frees it.

## Table scan

- Vectors read from the buffer pool (cache) have either a nil pool (read-only
  references) or the buffer pool's own pool.
- That guarantees query execution cannot modify them: every operator's pool
  differs from theirs, so the strict pool check rejects any write.
