# Row selection: bitmaps and RemapList

2026-11-24

Replace every row-selection index list with a bitmap, and move every reordering list (sort, gather, buckets) into one RemapList type, so each query allocates each structure once and never converts between them.

## Decisions

1. **A bitmap is the only representation of "which rows survive".** `[]int64` sels used as a filter, `[]uint8` flags in `UnionBatch`, and the `[]bool` select list in expression evaluation are all replaced by `bitmap.Bitmap`. `Shrink([]int64)` and the use of `Union(sels)` for filtering go away.
2. **Lists stay only where rows are reordered or repeated.** Sort and top permutations, join gathers, hash buckets and inverse (old→new) maps move into one `RemapList` type with one gather kernel. A bitmap cannot express these, and a list should not be used for anything else.
3. **One owner, one allocation, one free per query.** The operator or reader that produces a bitmap or RemapList allocates it once (sized for the largest batch), re-initializes it per batch without reallocating, and frees it in `Free`. Downstream code only borrows it, for example through `Batch.SetSelection`.
4. **Each phase is its own PR with a benchmark.** The work is split so each step removes a conversion and can be measured on its own.
5. **RemapList lives in its own package, `pkg/common/remaplist`.** A bitmap stays only "which rows survive"; the package that reorders rows is separate and depends only on `mpool`.

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
- **Bitmap re-initialization allocates on main.** There, `Bitmap.InitWithSize` calls `make` on every call, so the filter allocates a new selection bitmap for every batch. #33144 fixes this: `InitWithSize` on an owned bitmap clears and reuses its words when they are large enough (`resetWords`), as `InitWithRows` already did.
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

The batch selection that already exists is the model: a producer owns one bitmap and lends it to its consumers through the batch.

**How the batch selection works on main.** `Batch.SetSelection(physicalRows, sel)` stores a pointer; the batch never owns the bitmap. The filter's `ctr.selection.rows`, table_scan's `ctr.selection.rows`, and the reader's `ReaderFilterRequest.PublishSelection` (owned by table_scan) are the owners. A consumer may read the bitmap until the producer's next `Call`, the same lifetime as the batch's vectors. Only operators that declare `vm.SelectionConsumer` (filter and projection today) receive a selected batch; every other consumer gets compacted rows.

**Rules.**

- Every row-selection producer publishes its result as the output batch's selection instead of compacting into a list.
- A consumer that narrows the selection copies the input bitmap into its own bitmap and clears bits there (at most 128 words). It never modifies the producer's bitmap.
- A consumer that needs flat rows compacts with a gather by mask; it never converts the bitmap to a list.
- Bitmaps that are not about the current batch keep their own owner and the same rule: the join's matched-row bitmap covers the build side and is sized once when the build finishes; the reader's delete mask is reused for every block.

**Ownership and allocation.** A batch holds at most 8192 rows, so a full selection bitmap is 128 words (1 KiB).

1. The owner allocates once, in `Prepare` or on the first `Call`, with capacity for the largest batch.
2. Each batch re-initializes the bitmap to that batch's row count by clearing the existing words. It never reallocates.
3. The owner frees it once, in `Free`. Consumers neither allocate nor free.

This relies on `InitWithSize` reusing capacity, which #33144 provides, and on the pool-owned bitmaps of #33144, where every bitmap has one owner that frees it.

**Primitives to add to `pkg/common/bitmap`.**

| Primitive | Replaces | Used by |
| --- | --- | --- |
| `InitWithSize` that reuses words (done in #33144) | per-batch `make` | every owner |
| Build from a bool vector, a word at a time | per-row `Add` loops | filter, table_scan, frontend, DML |
| `Vector.UnionByMask` / `Batch.UnionByMask` (append selected rows) | `Union(sels)` used as a filter, `UnionBatch` flags | compaction at every boundary |
| Compose (map a selection over live rows back to physical rows) | rank/select loops over the delete mask | blockio (`read.go:1270`, `positionsOfSelectedRows`), `compactInvisibleAppendRows` |
| Take the first N set bits | positional split of `ls.sels` | disttae workspace reads |

**The verified core.** `bitmap.go` is formally verified with Gobra (`pkg/common/bitmap/gobra`). A new primitive that works on words (build from bools, compose, take first N) is verified too, with a contract stated in terms of `Has`, and the PR runs `gobra/verify.sh`. New primitives live in `bitmap_select.go`, which `verify.sh` verifies with `bitmap.go` loaded but leaves out of the runs for `bitmap.go`'s own members, so they do not change existing proofs' context. Code that needs `unsafe`, generics, or vector types stays out of the core: `UnionByMask` belongs to the vector package and only reads the bitmap through its verified methods.

**Expression evaluation.** `ExpressionExecutor.Eval` takes the bitmap instead of `[]bool`. `evalSelectedRows` gathers its inputs by mask and scatters the result by mask. The per-call copy into `FunctionSelectList` goes away, and the list becomes nil or a single "all rows" flag.

PR 4 does this:

- `Eval(proc, batches, sel *bitmap.Bitmap)`. nil selects every row. Filter, projection and table_scan pass their selection bitmap directly, so `Batch.SelectionMask` (the `ExpandBools` conversion) is gone.
- IF, CASE, COALESCE and JSON ELT build each branch's rows in two executor-owned bitmaps (`branchRows`, `branchScratch`), allocated from the executor's pool on first use and freed in `Free`. COALESCE narrows the remaining rows with each argument's NULL bitmap a word at a time.
- `evalSelectedRows` gathers with `UnionByMask` and scatters with the new `Vector.ScatterByMask`, its inverse: fixed-width values a word at a time, unselected rows marked NULL a run at a time.
- A kernel is called only when every row is selected, so it receives one constant all-rows `FunctionSelectList` instead of a per-call copy. It stays non-nil: some kernels choose their path by whether the list is nil.

`BenchmarkEvalSelection` (one 8192-row batch, µs, PR 3 → PR 4):

| expression | all rows | half | tenth |
| --- | --- | --- | --- |
| `a + b` | 52.9 → 52.8 | 218 → 84 | 29.7 → 19.7 |
| `IF(a > 0, a + b, b)` | 317 → 207 | 573 → 257 | 94 → 63 |
| `CASE` with two conditions | 294 → 202 | 750 → 282 | 144 → 73 |
| `COALESCE(a, b)` | 84 → 40 | 250 → 55 | 38 → 14 |

The kernels' own masking paths (`FunctionSelectList` with `AnyNull`) are no longer reached from the executor. Removing them is part of PR 6.

## Design: RemapList

Every list that reorders or repeats rows does one of five things, and the first four fit one type.

| Pattern | Today | Examples |
| --- | --- | --- |
| Permute one batch | `[]int64` filled with 0..n-1, sorted by `pkg/sort`, applied by `Shuffle` / `ShuffleWithBuf`, nulls remapped by `nulls.Filter` | order, mergeorder, top, mergetop, window, mergesort `SortBlockColumns`, logtail-replay sorts |
| Gather from one source, with repeats | `Union(left)`, `UnionMulti(row, cnt)` | hash-join probe side (`inner_payload`) |
| Gather from many source batches | global row ids (`GetSels`, `[]int32`) or `[]FixedSourceRow{batch,row}` | hash-join build side, `JoinBatches` |
| Split into buckets | `[][]int32` per bucket; flat ids + offsets; flat `[]int64` + boundaries | shuffle (`BatchSet.Union`), spill (`classifyRows`), partition |
| Inverse map (old row → new row, -1 if dropped) | `mapping []int`, transfer maps | mergesort `ReshapeBatches`, aobj merger, transfer |

**The type** lives in its own package, `pkg/common/remaplist` (`list.go`). The concept is called RemapList in this document; the Go type is `remaplist.List`, so callers do not write `remaplist.RemapList`. The package imports only `mpool`; the gathers that read a list live in the vector and batch packages.

```go
// List maps output rows to source rows: output row i is source row Rows()[i].
// Rows may be in any order and may repeat. Optional bucket offsets split it into
// contiguous runs, one per bucket.
type List struct {
    rows []int32        // source row per output row; words from mp
    offs []int32        // bucket boundaries; nil when there are no buckets
    mp   *mpool.MPool   // owner's pool; allocated once, reused, freed once
}

func (r *List) Init(mp *mpool.MPool, capacity int) error
func (r *List) Free()
func (r *List) Reset()                    // length 0, keep capacity
func (r *List) Identity(n int) error      // 0..n-1, the starting point for a sort
func (r *List) Append(row int32) error
func (r *List) Repeat(row int32, n int) error
func (r *List) Rows() []int32             // pkg/sort sorts this in place
func (r *List) BuildBuckets(hashes []uint64, buckets int, shift uint64) error // the one counting sort
func (r *List) Bucket(b int) []int32
func (r *List) InvertInto(srcRows int, dst *List) error // old→new with -1, for mergesort/transfer
```

`InvertInto` writes into another list the caller owns, so the inverse map follows the same allocate-once rule instead of taking a heap slice.

**Consumers.** One `Vector.Gather(src, rows []int32, mp)` per type, plus `Batch.Gather`, replaces `Union`, `UnionInt32` and the gather inside `Shuffle`, and remaps the null bitmap in the same pass so `nulls.Filter(sels)` disappears. An in-place permutation is a gather into scratch followed by a swap, which is what `Shuffle` does today. `BatchSet.Union`, the spill scatter and partition's materialization take `Bucket(b)`.

**Choices.**

- **Element type `int32`.** Per-batch lists are bounded at 8192 rows, and the hashmap and spill already use `int32`. Whole-input sorts (order, window, partition) use `int64` today; with `int32` they cap at 2^31 rows per sort, past the point where they must spill. `Append` and `Identity` return an overflow error instead of keeping two element types.
- **`pkg/sort` sorts `[]int32`** (or becomes generic over `int32 | int64`). About 15 callers change mechanically.
- **Many-source gathers use global row ids** (`batch<<13 | row`, as `JoinBatches` decodes them) instead of `[]FixedSourceRow`.
- **Inverse maps get only `InvertInto`.** They point the other way and are used only by TN merge and transfer, so their structure stays.
- **Location: its own package, `pkg/common/remaplist`.** Putting it next to bitmap would share the pool-ownership helpers, but would mix "which rows survive" with "which rows go where". The few lines of pool handling (allocate `int32` words from `mp`, free them) are duplicated instead. The package stays outside the Gobra proof.
- **Ownership** follows the bitmap rule: one owner allocates once per query, re-initializes per batch, frees in `Free`. Lists for whole-input sorts are sized once, when the input is complete.

## Implementation plan

Eleven PRs in two tracks, bitmap and RemapList, each removing a conversion or a duplicate and each measured with its own benchmark. The tracks are independent until the last cleanup.

| # | Track | PR | What changes | Measured by |
| --- | --- | --- | --- | --- |
| 1 | Bitmap | Primitives | build from bool vector word-wise; `UnionByMask`; compose; take-first-N (`InitWithSize` reuse is already in #33144); all Gobra-verified, in `bitmap_select.go` | bitmap microbenchmarks; filter allocation count per batch; `gobra/verify.sh` |
| 2 | Bitmap | Storage read path | delete masks via `ShrinkByMask`; TAE `CompactByBitmap` stops building lists; `UnionBatch` moves NULLs a word at a time (`OrShifted`); mask loops read owned words directly. Search results stay lists (see "Storage search results stay lists") | shrink, `UnionBatch` and gather microbenchmarks; block read with deletes |
| 3 | Bitmap | Scan, filter, projection | the selection stays a bitmap from blockio through filter and projection; compaction uses `UnionByMask` | filtered scan benchmark; conversions per batch reach zero |
| 4 | Bitmap | Expression evaluation | `Eval` takes the bitmap; `evalSelectedRows` gathers and scatters by mask; the `FunctionSelectList` copy goes away | expression benchmarks under partial selection |
| 5 | Bitmap | Remaining selections | DML operators, join finalize, set operators' `[]uint8` flags, frontend loops; contiguous ranges become window copies | per-operator benchmarks; BVT |
| 6 | Bitmap | Remove list APIs | delete `Shrink([]int64)`, `Union`-as-filter and `FilterInPlaceOrdered` | build and full test run |
| 7 | RemapList | Type and gather | new package `pkg/common/remaplist` with `remaplist.List`; generic `Vector.Gather` / `Batch.Gather` with the null remap in the same pass | gather microbenchmarks against `Union` and `Shuffle` |
| 8 | RemapList | Buckets | shuffle, spill and partition use `BuildBuckets` | shuffle and spill benchmarks |
| 9 | RemapList | Sorts | order, mergeorder, top, mergetop, window, mergesort and logtail sorts use `Identity` → sort → `Gather`; remove `Shuffle`, `ShuffleWithBuf`, `UnionInt32` | sort and top-k benchmarks |
| 10 | RemapList | Joins | probe and build gathers use RemapList with global row ids; `TryUnionFlatFixedRows` goes away | hash-join benchmarks; TPC-H join queries |
| 11 | RemapList | Inverse maps | mergesort and transfer use `InvertInto` | merge benchmarks |

Every PR follows the same checks:

- New code allocates its bitmap or RemapList once per owner and frees it once. Tests run the owner on a pool and assert `CurrNB()` returns to zero after `Free`, as the #33144 tests do.
- Converted call sites assert ascending input under the race build until the list APIs are removed. This assertion is new: `Shrink` and `nulls.FilterInPlaceOrdered` do not check today, so PR 1 adds the check.
- Unit tests for each package the PR touches, the affected BVT cases, and a before/after benchmark in the PR description.

## Storage search results stay lists

PR 2 measured the storage search chain before converting it, and the search results stay `[]int64`. That covers the PK search functions (`ReadFilterSearchFuncType`), bloom-filter hits, `ApplyTombstones`, and the `ioutil` row loaders.

- **The producers build the list natively.** Sorted searches return a few points or a range. Linear scans and bloom-filter tests append one row per hit, which costs the same as setting one bit.
- **The consumers gather by list.** The cache row copy, the row-id builder and the TopN loaders take the list. Converting the results to a bitmap would add a conversion at the producer, or a word-wise gather at every consumer.
- **A mask gather is slower than a gather by an existing list at every density.** Gathering 8192 `int64` rows, with the mask read straight from its words:

| selected rows | `Union(sels)` | `UnionByMask` |
| --- | --- | --- |
| 1 | 4 ns | 142 ns |
| 16 | 8 ns | 197 ns |
| 256 | 82 ns | 220 ns |
| 2048 | 634 ns | 973 ns |
| 4096 | 1.1 µs | 1.9 µs |

The mask loop's `word &= word-1` is a serial dependency, while a list gather's iterations are independent. A mask wins only when it replaces building the list. That is the case for a delete mask, or a predicate result that is already a bitmap, but not for a search result.

- **The tombstone chain stays on the list.** On a list of k hits, each pass costs O(k), and the chain stops as soon as the list is empty. The bitmap form (`GetTombstones`, then AND NOT) always runs every pass.

Decision 1 therefore does not cover storage search results for now: they are sparse hit lists, and they stay lists. The code comments at `ReadFilterSearchFuncType`, `engine.DataSource.ApplyTombstones` and `blockio.ReadDataByFilter` point here. This is to be re-examined later, for example after PR 3 makes the scan selection a bitmap end to end, or if a faster mask gather closes the gap.

## Risks and open questions

- **Silent wrong rows.** A selection that was not really ascending, now applied as a bitmap, keeps the wrong rows without an error. Mitigation: the race-build ascending assertion on every converted site, and the three known exceptions (join left rows, TopN with `dists`, `data_branch_hashdiff.go:573`) handled explicitly.
- **Sparse selections.** A point lookup is 1 element as a list and 128 words as a bitmap. That is bounded, but hot loops must iterate words, not bits.
- **The `int32` cap.** Whole-input sorts above 2^31 rows would now fail instead of running. Open question: is there any path that sorts that many rows in memory without spilling?
- **Dependency on #33144.** The ownership rule assumes pool-owned bitmaps with an explicit `Free`. If #33144 changes, the allocation details here change with it.
- **Join hot path.** PR 10 touches the most performance-sensitive code; it goes last and only with a benchmark showing no regression.
