# Row selection: bitmaps and RemapList

2026-11-24

Replace every row-selection index list with a bitmap, and move every reordering list (sort, gather, buckets) into one RemapList type, so each query allocates each structure once and never converts between them.

## Decisions

1. **A bitmap is the only representation of "which rows survive".** `[]int64` sels used as a filter, `[]uint8` flags in `UnionBatch`, and the `[]bool` select list in expression evaluation are all replaced by `bitmap.Bitmap`. `Shrink([]int64)` and the use of `Union(sels)` for filtering go away.
2. **Lists stay only where rows are reordered or repeated.** Sort and top permutations, join gathers, hash buckets and inverse (old→new) maps move into one `RemapList` type with one gather kernel. A bitmap cannot express these, and a list should not be used for anything else.
3. **A selection is owned or borrowed by its batch, like a vector.** A batch carries storage for its own selection. A producer, or a consumer that narrows a selection, fills its output batch's storage; a consumer that forwards a selection unchanged, and every view, borrows the input's bitmap read-only until the producer's next `Call`, exactly as batches already borrow vectors. Copy only to modify. `Clean` frees only the batch's own storage. A batch never pins anything: whoever lends memory to a batch keeps it alive (decision 4). Every other bitmap and every RemapList keeps one owner: the operator or reader that produces it allocates it once (sized for the largest batch), re-initializes it per batch without reallocating, and frees it in `Free`.
4. **The lender keeps borrowed memory alive; the batch holds no leases.** A borrowed (read-only) vector is valid until its producer's next `Call`. Memory the producer owns (the Arrow reader's record, a native result window) needs no lease: the producer keeps it until it reuses or releases the batch. Only memory from a shared resource, such as the FileService cache, is pinned, and the producer holds the pins, not the batch. A consumer that keeps a batch longer copies it.
5. **Each phase is its own PR with a benchmark.** The work is split so each step removes a conversion and can be measured on its own.

## Findings

The code base holds four representations of the same row selection, and the hottest path converts between them up to four times per batch.

| Representation | Where it lives | Main consumers |
| --- | --- | --- |
| `[]int64` sels (also `[]int32`) | filters, DML, storage search functions, delete lists | `Vector.Shrink`, `Vector.Union`, `Batch.Shrink` |
| `bitmap.Bitmap` | batch selection (`Batch.SetSelection`), delete masks, join matched rows | `ShrinkByMask`, `Contains`, iterators |
| `[]uint8` flags | intersect, intersectall, minus, minusall, aggregation state | `Vector.UnionBatch(..., flags)` |
| `[]bool` select list / `FunctionSelectList` | expression evaluation | about 600 per-row checks in `pkg/sql/plan/function` |

The filtered table scan converts one selection at every hop:

1. The predicate's bool vector becomes `[]int64` (`table_scan/reader_selection.go`, `compactPredicateRows`).
2. blockio turns the sels into the published bitmap (`publishBatchSelection`, `InitWithRows`).
3. Each downstream predicate or projection expands it to `[]bool` (`ExpandBools`).
4. Each function node under a partial selection turns the `[]bool` back into `[]int64`, gathers its inputs and scatters its result (`evalSelectedRows`).
5. A consumer that needs flat rows converts the bitmap to `[]int64` again and calls `Union` (`SelectionRows`, `CompactInto`).

Other findings:

- **The function select list is dead weight.** A partial selection always takes `evalSelectedRows`, which calls the function with a nil list. Function bodies only ever see "all rows", yet every call copies the list row by row into `FunctionSelectList` (`evalExpression.go`, around line 1917).
- **The storage read path turns delete masks into lists.** `BlockDataReadInner` and `blockDataReadWithFilter` convert the delete bitmap to `[]int64` with `ToI64Array` and then call `Shrink(neg)` per column. The code carries a `TODO: avoid this transformation`, and `ShrinkByMask` already exists.
- **Storage search functions are bitmaps in disguise.** The PK filter's bloom-filter hit bytes and `combineOffsetFuncs`' dense marks array are converted to `[]int64`. The tombstone chain then runs up to five `DeleteFunc(Contains)` passes over those sels per block.
- **Bitmap re-initialization allocates.** On main, `Bitmap.InitWithSize` calls `make` on every call, so the filter allocates a new selection bitmap for every batch. `InitWithRows` already reuses capacity.
- **Bucket splitting is implemented three times.** shuffle appends into `[][]int32`, spill runs a counting sort into a flat array plus offsets (`classifyRows`), and partition keeps a flat `[]int64` plus boundaries.
- **The vector has six gather entry points.** `Union`, `UnionInt32`, `Shuffle`, `ShuffleWithBuf`, `UnionMulti` and `TryUnionFlatFixedRows` each have their own per-type switch over the same copy.

## Where sels are used today

About 110 call sites are pure row selection (ascending, unique) and can become bitmaps; about 30 reorder or repeat rows and become RemapLists; about 10 are contiguous ranges that should be a window copy. Counts are approximate call sites outside tests, from a survey of main.

| Area | Row selection (bitmap) | Reorder or repeat (RemapList) | Contiguous range (window) |
| --- | --- | --- | --- |
| blockio read path | delete masks (`ToI64Array` → `Shrink` neg), filter sels, TopN input, membership staging | — | — |
| readutil, disttae | PK search functions, bloom-filter hits, tombstone chain, workspace delete lists | logtail-replay sorts (`change_handle.go`) | `change_handle.go` identity sels |
| objectio, TAE | `CompactByBitmap`, `DedupSortedBatches`, backup and GC partitions | mergesort `SortBlockColumns`, merge and transfer maps | checkpoint reader identity sels, objectio window copy |
| table_scan, filter, projection | predicate results, `SelectionRows` → `Union` compaction | — | — |
| DML operators | constraint_util, multi_update, preinsert, preinsertunique, lockop, s3writer | — | offset |
| joins | matched-row bitmaps at finalize, semi/anti probe rows | probe and build gathers (`inner_payload`, `rightRows`, `GetSels`) | — |
| set operators, aggregation | `UnionBatch` byte flags | group-by row → group mapping (out of scope) | — |
| sort, top, window | — | order, mergeorder, top, mergetop, window permutations | — |
| shuffle, partition, spill | — | `[][]int32` buckets, `classifyRows`, partition selections | — |
| expression evaluation | `[]bool` select list, `FunctionSelectList` | — | apply, `Values` identity sels |
| frontend | data_branch hashdiff, `mysql_cmd_executor` bool → `Shrink` | — | — |

Three sites need care: the hash-join left rows are ascending but stay paired one-to-one with the right-side gather; the vector TopN rows are ascending but stay paired with a `dists` array; and `data_branch_hashdiff.go:573` relies on an `ORDER BY` for ascending input to `Shrink`.

## Design: bitmap as the row selection

The batch selection that already exists is the starting point, with one change: the selection's storage moves from the producing operator into the batch, and a batch's selection is either its own or borrowed, under the rule batches already follow for vectors.

**How the batch selection works on main.** `Batch.SetSelection(sel)` stores a pointer; the batch never owns the bitmap, and the physical row count is `sel.Len()`. The filter's `ctr.selection.rows`, table_scan's `ctr.selection.rows`, and the reader's `ReaderFilterRequest.PublishSelection` (owned by table_scan) are the owners. A consumer may read the bitmap until the producer's next `Call`, the same lifetime as the batch's vectors. projection and evalProjection forward the input's pointer unchanged. Only operators that declare `vm.SelectionConsumer` (filter and projection today) receive a selected batch; every other consumer gets compacted rows, and pSpool rejects a selected batch.

**Why change it.** A batch owns the vectors it creates and frees them in `Clean`, and it may also hold its input's vectors read-only until the producer's next `Call`; it copies a vector only to modify it. The selection was the exception: its storage always belonged to an operator, so the batch could only borrow it. Moving the storage into the batch makes the selection follow the same rule as the vectors, lets `Clean` release it, and removes the producers' separate selection state. Nothing is copied that is not copied today.

| Site | Main | Owned or borrowed |
| --- | --- | --- |
| table_scan / blockio | fills table_scan's bitmap, passed as `PublishSelection` | fills the output batch's own selection; same work |
| filter | copies the input's bitmap into its own `s.rows`, then narrows it | copies it into its output batch's own selection, then narrows it; same work |
| projection, evalProjection | forward the input's pointer | borrow the input's selection; unchanged |
| views (`ViewInto`) | share the parent's pointer | borrow the parent's selection; unchanged |

**The batch API.**

```go
// OwnSelection returns the batch's own selection storage, cleared and sized to
// physicalRows, with its words allocated from mp. The caller fills it (or
// copies an input selection into it and narrows it) and then publishes it with
// SetSelection. It fails only when mp cannot allocate the words; the batch is
// then left unselected.
func (bat *Batch) OwnSelection(mp *mpool.MPool, physicalRows int) (*bitmap.Bitmap, error)
// SetSelection publishes sel as the batch's logical rows. sel is either the
// batch's own storage (from OwnSelection) or a borrowed bitmap that stays valid
// and unmodified until the producer's next Call.
func (bat *Batch) SetSelection(sel *bitmap.Bitmap)
func (bat *Batch) Selection() *bitmap.Bitmap // read-only; nil when the batch is not selected
func (bat *Batch) ClearSelection()           // not selected; keeps the storage
// FreeSelection drops the selection and returns the own storage's words to
// their pool, for a batch that is never Cleaned (a view kept in operator state).
func (bat *Batch) FreeSelection()
func (bat *Batch) HoldsSelectionMemory() bool // the own storage still holds pool words
```

A producer handles the error like any other allocation failure in `Call`:

```go
sel, err := out.OwnSelection(mp, physicalRows)
if err != nil {
	return nil, err
}
// fill sel ...
out.SetSelection(sel)
```

The batch owns its selection exactly when `Selection()` is its own storage; no flag records it.

**Rules.**

- Every row-selection producer publishes its result as the output batch's selection instead of compacting into a list.
- A consumer that forwards a selection unchanged borrows it (`SetSelection(input.Selection())`).
- A consumer that narrows a selection copies the input's bitmap into its output batch's own storage and clears bits there. It never modifies a bitmap it borrowed.
- A consumer that needs flat rows compacts with a gather by mask; it never converts the bitmap to a list.
- A view borrows everything: the parent's vectors, `extraBuf` and selection. `ViewInto` borrows the parent's selection and `PhysicalView` clears it. A view is dropped, never cleaned.
- A batch copied by value (`b := *bat`) shares its original's storage, so it must never fill its own selection; it may only borrow. The by-value copies in the batch codecs are short-lived and never selected.
- Selected batches still stop at pipeline roots, codecs and spools (`vm.ConfigureSelectionEdges`, `errSelectedBatch`, pSpool's check). Whether a selection should ever cross those boundaries is a separate decision.
- Bitmaps that are not about the current batch keep their own owner and the producer rule: the join's matched-row bitmap covers the build side and is sized once when the build finishes; the reader's delete mask is reused for every block.

**Ownership and allocation.** A batch holds at most 8192 rows, so a full selection bitmap is 128 words (1 KiB).

1. A batch's own selection storage lives in the batch and is reused for every batch it carries. Producers reuse their output batches, so in practice it is allocated once per producer.
2. The words come from the pool passed to `OwnSelection`. Each call with the same pool clears the existing words and reuses them when their capacity covers the new row count, and reallocates from the pool otherwise. A call with a different pool (a new execution) first returns the old words to their pool.
3. The batch's release paths return the words to their pool: `Clean`, `FreeWindow` and `CleanOwned`. A batch that is never Cleaned, such as a filter's output view kept in operator state, calls `FreeSelection` when its owner is freed or reset. `ClearSelection`, `SetRowCount` and `CleanOnlyData` keep the words for the next batch.
4. An operator whose output batch owns selection words frees them in `Reset` as well as `Free`, since nothing reads the output after `Reset`. Its allocation-account checks count those words as live memory (`HoldsSelectionMemory`).

Other bitmaps and RemapLists follow the producer rule: the owner allocates once, in `Prepare` or on the first `Call`, re-initializes per batch, and frees in `Free`.

Both rules fit the pool-owned bitmaps of #33144, where every bitmap has one owner that frees it. For a batch's own selection that owner is the batch.

Operations that treat physical rows as the logical rows (`Shrink`, `ShrinkByMask`, `Shuffle`, `Union*`, `CloneTo`/`Clone`/`Dup`, `CloneSelectedColumnsTo`, `Append*`) return an error on a selected batch, like the codecs. A caller compacts first.

**Borrowed vectors and pins.** Selections and vectors follow the same lending rule, and so does the memory under a borrowed vector:

| Lender | What it lends | How it keeps it alive |
| --- | --- | --- |
| table_scan via blockio | columns borrowed from the FileService cache | the scan's `ReaderFilterRequest` holds the cache pins in an `objectio.CachePins` (`Pins`), a plain list of cache entries and their capacity reservations. The reader releases them before it reuses the output batch for the next block; the scan releases them in `Reset` and `Free`. Borrowing is opt-in: only columns the request explicitly retains (`ColumnUses`) are borrowed, so a caller that keeps batches (ivfflat) gets owned copies. |
| Arrow external reader | columns aliasing its record batch | retains the record it published from until `ReleaseBatch`, only when the batch has a borrowed (read-only) column; a fully materialized batch borrows nothing, so its record is not kept while the next one is read. Varlen descriptors live in its reusable `arrowbridge.Descriptors` scratch |
| sirius | columns aliasing the native result window | nothing: the window stays valid for the callback that consumes and cleans the batch |

Every path that keeps a batch past the producer's next `Call` already copies: pSpool, the remote codec, spill, and operators that buffer their input.

**Primitives to add to `pkg/common/bitmap`.**

| Primitive | Replaces | Used by |
| --- | --- | --- |
| `InitWithSize` that reuses words (done in #33144: reuses words from the same pool when their capacity suffices) | per-batch `make` | every owner |
| Build from a bool vector, a word at a time | per-row `Add` loops | filter, table_scan, frontend, DML |
| `Vector.UnionByMask` / `Batch.UnionByMask` (append selected rows) | `Union(sels)` used as a filter, `UnionBatch` flags | compaction at every boundary |
| Compose (map a selection over live rows back to physical rows) | rank/select loops over the delete mask | blockio (`read.go:1270`, `positionsOfSelectedRows`), `compactInvisibleAppendRows` |
| Take the first N set bits | positional split of `ls.sels` | disttae workspace reads |

**Expression evaluation.** `ExpressionExecutor.Eval` takes the bitmap instead of `[]bool`. `evalSelectedRows` gathers its inputs by mask and scatters the result by mask. The per-call copy into `FunctionSelectList` goes away, and the list becomes nil or a single "all rows" flag.

## Design: RemapList

Every list that reorders or repeats rows does one of five things, and the first four fit one type.

| Pattern | Today | Examples |
| --- | --- | --- |
| Permute one batch | `[]int64` filled with 0..n-1, sorted by `pkg/sort`, applied by `Shuffle` / `ShuffleWithBuf`, nulls remapped by `nulls.Filter` | order, mergeorder, top, mergetop, window, mergesort `SortBlockColumns`, logtail-replay sorts |
| Gather from one source, with repeats | `Union(left)`, `UnionMulti(row, cnt)` | hash-join probe side (`inner_payload`) |
| Gather from many source batches | global row ids (`GetSels`, `[]int32`) or `[]FixedSourceRow{batch,row}` | hash-join build side, `JoinBatches` |
| Split into buckets | `[][]int32` per bucket; flat ids + offsets; flat `[]int64` + boundaries | shuffle (`BatchSet.Union`), spill (`classifyRows`), partition |
| Inverse map (old row → new row, -1 if dropped) | `mapping []int`, transfer maps | mergesort `ReshapeBatches`, aobj merger, transfer |

**The type** (`remaplist.go`):

```go
// RemapList maps output rows to source rows: output row i is source row Rows()[i].
// Rows may be in any order and may repeat. Optional bucket offsets split it into
// contiguous runs, one per bucket.
type RemapList struct {
    rows []int32        // source row per output row
    offs []int32        // bucket boundaries; nil when there are no buckets
    mp   *mpool.MPool   // owner's pool; allocated once, reused, freed once
}

func (r *RemapList) Init(mp *mpool.MPool, capacity int) error
func (r *RemapList) Free()
func (r *RemapList) Reset()                    // length 0, keep capacity
func (r *RemapList) Identity(n int) error      // 0..n-1, the starting point for a sort
func (r *RemapList) Append(row int32) error
func (r *RemapList) Repeat(row int32, n int) error
func (r *RemapList) Rows() []int32             // pkg/sort sorts this in place
func (r *RemapList) BuildBuckets(hashes []uint64, buckets int, shift uint64) error // the one counting sort
func (r *RemapList) Bucket(b int) []int32
func (r *RemapList) Invert(srcRows int, dst []int32) []int32 // old→new with -1, for mergesort/transfer
```

**Consumers.** One `Vector.Gather(src, rows []int32, mp)` per type, plus `Batch.Gather`, replaces `Union`, `UnionInt32` and the gather inside `Shuffle`, and remaps the null bitmap in the same pass so `nulls.Filter(sels)` disappears. An in-place permutation is a gather into scratch followed by a swap, which is what `Shuffle` does today. `BatchSet.Union`, the spill scatter and partition's materialization take `Bucket(b)`.

**Choices.**

- **Element type `int32`.** Per-batch lists are bounded at 8192 rows, and the hashmap and spill already use `int32`. Whole-input sorts (order, window, partition) use `int64` today; with `int32` they cap at 2^31 rows per sort, past the point where they must spill. `Append` and `Identity` return an overflow error instead of keeping two element types.
- **`pkg/sort` sorts `[]int32`** (or becomes generic over `int32 | int64`). About 15 callers change mechanically.
- **Many-source gathers use global row ids** (`batch<<13 | row`, as `JoinBatches` decodes them) instead of `[]FixedSourceRow`.
- **Inverse maps get only `Invert`.** They point the other way and are used only by TN merge and transfer, so their structure stays.
- **Location.** Next to bitmap (`pkg/common/bitmap/remaplist.go`) shares the pool-ownership helpers; a sibling package (`pkg/common/remap`) keeps "a bitmap is a selection" clean. Recommendation: the sibling package.
- **Ownership** follows the bitmap rule: one owner allocates once per query, re-initializes per batch, frees in `Free`. Lists for whole-input sorts are sized once, when the input is complete.

## Implementation plan

Eleven PRs in two tracks, bitmap and RemapList, each removing a conversion or a duplicate and each measured with its own benchmark. The tracks are independent until the last cleanup. Moving the selection's storage into the batch and the pins out of it comes first, in the `Batch` cleanup PR (#33184, issue #33182). For the selection, it adds `OwnSelection`, makes blockio and filter fill their output batch's own selection, turns `ReaderFilterRequest.PublishSelection` into a flag, and removes the producers' own selection bitmaps; projection and evalProjection already borrow and do not change. For pins, it removes `Batch.leases`: the scan's request holds the cache pins, the Arrow reader retains its record and owns its descriptor scratch, and sirius needs none. With no lender left that needs a shared ref-counted lease, it also removes `pkg/container/bufferlease`: the runtime-filter `KeyPayload` keeps a private reference count, and cache pins are an `objectio.CachePins` list.

| # | Track | PR | What changes | Measured by |
| --- | --- | --- | --- | --- |
| 1 | Bitmap | Primitives | `InitWithSize` reuses words; build from bool vector word-wise; `UnionByMask`; compose; take-first-N | bitmap microbenchmarks; filter allocation count per batch |
| 2 | Bitmap | Storage read path | delete masks via `ShrinkByMask`; search functions and bloom-filter hits fill a bitmap; tombstone chain works on the bitmap; TAE `CompactByBitmap` stops building lists | blockio and readutil benchmarks; TPC-H scan with deletes |
| 3 | Bitmap | Scan, filter, projection | the selection stays a bitmap from blockio through filter and projection; compaction uses `UnionByMask` | filtered scan benchmark; conversions per batch reach zero |
| 4 | Bitmap | Expression evaluation | `Eval` takes the bitmap; `evalSelectedRows` gathers and scatters by mask; the `FunctionSelectList` copy goes away | expression benchmarks under partial selection |
| 5 | Bitmap | Remaining selections | DML operators, join finalize, set operators' `[]uint8` flags, frontend loops; contiguous ranges become window copies | per-operator benchmarks; BVT |
| 6 | Bitmap | Remove list APIs | delete `Shrink([]int64)`, `Union`-as-filter and `FilterInPlaceOrdered` | build and full test run |
| 7 | RemapList | Type and gather | `RemapList`, generic `Vector.Gather` / `Batch.Gather` with the null remap in the same pass | gather microbenchmarks against `Union` and `Shuffle` |
| 8 | RemapList | Buckets | shuffle, spill and partition use `BuildBuckets` | shuffle and spill benchmarks |
| 9 | RemapList | Sorts | order, mergeorder, top, mergetop, window, mergesort and logtail sorts use `Identity` → sort → `Gather`; remove `Shuffle`, `ShuffleWithBuf`, `UnionInt32` | sort and top-k benchmarks |
| 10 | RemapList | Joins | probe and build gathers use RemapList with global row ids; `TryUnionFlatFixedRows` goes away | hash-join benchmarks; TPC-H join queries |
| 11 | RemapList | Inverse maps | mergesort and transfer use `Invert` | merge benchmarks |

Every PR follows the same checks:

- New code allocates its bitmap or RemapList once per owner and frees it once; the `bitmapleak` build from #33144 confirms no leaks.
- Converted call sites assert ascending input under the race build, as `Shrink` does today, until the list APIs are removed.
- Unit tests for each package the PR touches, the affected BVT cases, and a before/after benchmark in the PR description.

## Risks and open questions

- **Silent wrong rows.** A selection that was not really ascending, now applied as a bitmap, keeps the wrong rows without an error. Mitigation: the race-build ascending assertion on every converted site, and the three known exceptions (join left rows, TopN with `dists`, `data_branch_hashdiff.go:573`) handled explicitly.
- **Sparse selections.** A point lookup is 1 element as a list and 128 words as a bitmap. That is bounded, but hot loops must iterate words, not bits.
- **The `int32` cap.** Whole-input sorts above 2^31 rows would now fail instead of running. Open question: is there any path that sorts that many rows in memory without spilling?
- **Dependency on #33144.** The ownership rule assumes pool-owned bitmaps with an explicit `Free`. If #33144 changes, the allocation details here change with it.
- **Batches copied by value.** A by-value copy of a `Batch` shares its original's selection storage. Once PR 1 makes re-initialization reuse words, a copy that filled its own selection would overwrite the original's. Mitigation: the rule above, and the only by-value copies today are short-lived codec views that are never selected.
- **Join hot path.** PR 10 touches the most performance-sensitive code; it goes last and only with a benchmark showing no regression.
- **Open question: location.** `pkg/common/bitmap/remaplist.go` or a sibling `pkg/common/remap` package.
