# Batch Memory Model

Read this before you create, hold, copy, encode, decode, or free a
`batch.Batch`, or review code that does. Read it before you touch the
`Batch` struct at all. For the vectors a batch carries, see
[vector-memory.md](vector-memory.md).

## Do not add fields to `Batch`

**Do not add a field to `batch.Batch`, and do not give an existing field a
new meaning, unless a human developer has directly asked for that field, or
has reviewed and approved the design that needs it.** An agent's own
judgement that a field would be convenient is not enough. Neither is a
reviewer bot's suggestion, or a plan you wrote yourself.

`Batch` is the unit every operator, reader, spool, codec and RPC passes
around. Each field is copied with every value copy, and every `Clean`, view,
`Clone`, encode and decode path has to handle it. One field with an unclear
owner turns into a leak, a double free or a use-after-free somewhere far
from where it was added. The batch cleanup (#33182, #33184, #33199, #33265)
removed `Attrs`, `physicalRows`, the leases (`HoldLease` and friends,
`pkg/container/bufferlease`) and the codec's name and legacy sections for
exactly that reason. Do not bring any of them back under another name.

State that seems to belong on a batch almost always has a better owner:

| You want the batch to carry | Put it here instead |
|---|---|
| column names, a schema | whoever defines the columns: the plan, the table definition, the workspace entry; pass it beside the batch (`Relation.Write(ctx, bat, attrs)`, `ReplayBatch.Attrs`) |
| a pin, lease or reference on memory the vectors borrow | the lender that lent it (for example `ReaderFilterRequest.Pins`, `objectio.CachePins`) |
| per-operator scratch, counters, state between `Call`s | the operator's own struct, freed in its `Reset`/`Free` |
| message or transport metadata | the message (`api.Entry`, `pipeline.Message`), not the batch |
| a "physical" row count, a second selection | the selection bitmap's length already is the physical row count |

If none fits and you still believe a field is needed, **stop and ask the
human developer.** Say what the field holds, who allocates and frees it,
what each of `Clean`, `ViewInto`, `Window`, `Clone`, `MarshalBinary` and
decode must do with it, and why each alternative above fails. Do not add it
"for now" and mention it in the PR.

The same rule applies to the codec: do not add sections to the batch
encoding.

## Fields and their owners

| Field | Owned by the batch when | Released by |
|---|---|---|
| `Vecs` | the vector's own `mp` is the pool the batch's owner cleans with | `Clean(mp)` frees each vector with `mp`. A vector of another pool panics (`mustCheckMP`). A read-only vector (`mp == nil`) is only dropped. |
| `rowCount` | — | the logical row count. With a selection, it is the selected count. |
| `selection` / `ownSel` | `selection == &ownSel` (filled through `OwnSelection(mp, n)`) | `Clean`, `FreeWindow`, `FreeSelection`. A selection borrowed through `SetSelection` belongs to the producer. |
| `extraBuf` / `mp` | `mp != nil`: allocated from `mp` (`SetExtraBuf`, `AdoptExtraBuf`, `SetAccountedExtraBuffer`) | `Clean`, `DropExtraBuffer`. With `mp == nil` it borrows bytes the batch does not own (views, decodes without a pool). |
| `Recursive`, `ShuffleIDX` | plain flags | — |

`mp` is the pool for the batch's own allocations, which today means only
`extraBuf`. It is not the vectors' pool, and a nil `mp` means "owns nothing
from a pool".

## The legal states

Every batch is in exactly one of these states. Pick it when you create the
batch, and record who releases it.

1. **Owned.** Its vectors come from one pool, and its owner releases it with
   `Clean(thatPool)` on every exit path: normal end, error, cancel,
   `Reset`/reuse, and replacement by a new batch. Constructors: `NewWithTypes`,
   `Clone`, `Dup`, decodes given a pool (`UnmarshalBinaryWithAnyMp`,
   `UnmarshalFramedFrom`), `BatchSet` batches once taken. `CleanOnlyData` keeps
   the memory for the next batch, so it is not a release. `CleanOwned` is for
   test fixtures only.
2. **View.** It references vectors, a selection or bytes it does not own,
   and is valid only while its source is alive and unchanged. It is released
   by dropping it, never by `Clean`:
   - `ViewInto`, `PhysicalView`, `SelectColumns`: they share the source's vectors.
   - `Window`: read-only window vectors, released with `FreeWindow`.
   - `ViewBinary`, `ViewTrustedBinary`: views of an encoding. `ViewTrustedBinaryInto`
     reuses the batch's vectors, and `DropViews` drops them. Never pass a
     batch whose vectors are someone else's to `ViewTrustedBinaryInto`: it
     decodes into those vectors.
3. **Borrowed across an operator edge.** A batch an operator returns, and the
   vectors and selection in it, are valid until that producer's next `Call`.
   A consumer that keeps any of it longer copies it (`Clone`, `Dup`,
   `OwnReadOnlyVectors`, `CompactInto`, `BatchSet.Append`). The batch never pins
   anything: whoever lent the memory keeps it alive (see
   `docs/design/20261124-row-selection-bitmap-remaplist.md`, decisions 3 and 4).

Anything else is a bug: a batch with no clear releaser, two owners that both
`Clean`, a view used after its source was freed or reused, or `Clean` on a
view, which frees the source's vectors.

## Rules

- **Copying the struct is not copying the batch.** `b2 := *b` shares the
  vectors, `extraBuf` and `ownSel` storage. Then only one copy may be cleaned,
  and the copy must not call `OwnSelection`. Use `Clone`/`Dup` for a copy,
  and `ViewInto` for an explicit view.
- **Free with the owning pool.** `Clean(mp)` and `Vector.Free(mp)` must get the
  pool the vectors came from. Do not pass a convenient "per-call" pool.
- **Selected batches.** Operations that treat physical rows as logical rows
  reject a selected batch (`requireCompacted`). Compact first with `CompactInto`.
  Spools reject selected batches.
- **Partly built batches on error.** A function that creates a batch, and
  fails part way, cleans what it made on that path.
- **Allocation can fail.** `Clone`, `OwnSelection`, `SetExtraBuf`, `PreExtend` and
  the appends return the pool's error. Propagate it.

## Encoding and messages

- `MarshalBinary` is the only batch encoding. It is never persisted:
  everything is in-cluster RPC or query-scoped spill, and upgrade is
  shutdown only. It has no version or compatibility sections; keep it that
  way.
- `batch.Encode` (into `api.EncodedBatch`) **borrows** the batch. The encoder
  writes it straight into the message buffer when the message is marshaled.
  The batch must stay unchanged until then, and a changed size fails the
  marshal rather than overrunning.
- `api.EncodedBatch.Unmarshal` keeps a **view** of the receive buffer, which
  the receiver reuses after dispatch. Anything that outlives the dispatch call
  keeps `Detach()` bytes, or better, copies only the columns it keeps. Logtail
  replay keeps an owned copy of the user columns for inserts and of the
  primary key for deletes. Keeping a view would pin the whole encoding.
- Message buffers are off-heap `mpool.MessageBuffer`s owned by the code that
  manages the messages. Never put a message buffer in `sync.Pool`: an
  off-heap buffer the GC drops is leaked.

## Tests

- Use a leak-accounting pool (`mpool.MustNewZero`) and assert `CurrNB()` is
  zero after the owner's release.
- For views and borrowed bytes, overwrite or poison the source
  (`mpool.EnableDebugPoisonOnFree`, `clear(buf)`) and check the kept data still
  reads correctly. That is how a missed copy shows up.
