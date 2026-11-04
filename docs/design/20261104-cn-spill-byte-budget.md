# CN spill-run byte budget

Issue: [#32146](https://github.com/matrixorigin/matrixone/issues/32146)

Implementation: [PR #32155](https://github.com/matrixorigin/matrixone/pull/32155)

## Problem and contract

A CN data writer can hold a multirow spill run substantially larger than its byte target: the existing admission rule includes the row that crosses the target. At the initial PR head, two rows with 800-byte payloads formed a 1656-byte batch with a 1 KiB target in sorted and unsorted writers, both synchronous and pipelined. The observed issue was in the data writer; the same `Sinker` also writes tombstone objects, whose primary key may be variable length, so the same byte budget applies to that path.

For an ordinary byte-bounded CN data or tombstone writer, a run's conservative materialized charge must stay at or below its positive target. If one row alone exceeds the target, it forms a one-row run and is spilled immediately. The charge covers fixed-width slots and varlena descriptors, together with a conservative bound for both retained area bytes and live non-inline payload after materialization. Constants are charged across their logical rows; aliased values are charged for each materialization. Null and inline values have no external live-payload charge. Arithmetic must not wrap.

The writer admits the largest input prefix that fits the **carried conservative charge**. Dead area and per-column physical/expanded maxima can make this charge greater than live bytes, so this does not promise the densest possible live-byte packing. A run is not an RSS bound: input batches, buffer capacity, bitmaps, sorting and serialization buffers, and queued pipeline jobs have separate costs. A single row remains indivisible.

Each persisted object's blocks must be full except for its last block. This is required by the stats-only block iterator. Sorted output is sorted within each object; synchronous unsorted output keeps its existing row order. Pipeline jobs may complete out of order. All modes preserve every row and its values exactly once.

## Admission and copy accounting

The sinker owns the staged run. Its per-batch bounds remain aligned with staged batches and travel with a popped partial batch. The remaining budget subtracts both staged and current charges. A bound cannot be replaced with physical `Batch.Size()` after aliasing or sorting.

A whole-input conservative bound is computed per column using the greater of physical area and expanded live payload. For append-built disjoint vectors, the existing area proof avoids a descriptor scan. If the whole input fits, retain the bulk-copy path. For a bulk input copied in block-sized windows, the physical size increment plus constant expansion accounts for the materialized window. Bounds and size arithmetic are checked for overflow.

When the whole-input bound does not fit, examine the candidate rows and admit the prefix before the first row that would exceed the remaining budget. Equality fits and triggers a spill. A zero-length prefix has two cases:

- If the run already contains rows, stage its current partial batch, then explicitly spill the nonempty run even if its charge is below target. Staging may itself trigger a spill, so do not spill a second time if the run is already empty. Retry the same input row with a fresh budget.
- If the run is empty, compact-copy exactly one oversized row, stage it, and spill immediately.

Each iteration therefore consumes input or spills a nonempty run. No empty object or empty merge is created, and an oversized row cannot absorb a neighbor.

A full-window copy of a disjoint varlena vector can include dead area. If that whole-source copy would cost more than the selected rows' live charge, use the conservative whole-input charge if it fits. Otherwise compact the copy by using two non-whole windows, or `UnionOne` for a singleton, and continue admission. Dead source area alone does not make a live row oversized. Partial windows and non-disjoint or borrowed sources must be accounted according to what the actual copy path retains.

`WriteOwned` may transfer a full block only when its length, constant, sorting, borrowing and preceding-partial-block constraints allow it and its conservative charge fits the remaining budget. Otherwise it uses the copying path. On any returned result, `owned=true` means the caller must release its reference even if an error is also returned. A full owned block cannot be placed after a partial block in the same object.

For a whole-input copy, exact per-column payload sizes already computed for non-disjoint varlena vectors may be reused as preallocation hints. Keep small-schema scratch on the stack; for a wider schema, allocate per-call hint storage only when an eligible scanned column can use it. Initialize unfilled entries to `-1` and use hints only for that unchanged whole source. Wide fixed or disjoint-only schemas incur no hint allocation. There is no persistent payload-size cache.

## Options and ownership

The two options have distinct contracts:

- `WithByteBoundedRuns` enables byte-budget admission where independently returned runs are permitted. `allMergeSorted` and `dedupAll` retain their existing single-result behavior. `flushOnSync` and unrelated sinker users retain their existing behavior.
- `WithUnpublishedRunCleanup` separately enables cleanup of unpublished bounded-run objects. Its file-sinker factory must write to the supplied file service and return stats naming those objects. Enabling byte budgeting does not by itself impose this factory or cleanup contract. The ordinary CN data and tombstone writers enable both options.

`pushStaged` takes batch ownership even when spilling returns an error. Synchronous spilling releases staged and merge buffers on success or failure. Pipeline submission transfers its batches even when enqueue fails; cancellation and cleanup drain workers before collecting successful sibling object stats. On error or cancellation, the bounded-run cleanup owner preserves the original error, releases buffers, aborts further writes until reset, and attempts to delete known unpublished objects with the existing bounded cleanup context. Failed deletes retain object names for retry. Objects already transferred to results or the caller are outside this run cleanup and must not be deleted by it.

A panic after direct `WriteOwned` transfer can unwind before the caller receives the ownership result; the caller may still hold the batch while the sinker has returned it to a free list. This is an existing ownership limit, not a guarantee supplied by this design. Any change to that path must establish one effective owner and validate it. Separately, `SyncAndFillBlockInfoBat` takes results before metadata expansion succeeds, and a partial file write may fail before returning stats. This design does not claim transaction-level orphan cleanup beyond the known run-owned objects.

The change introduces no new on-disk format, wire or catalog contract. Existing consumers must process the complete object-stats list. More objects and more partial final blocks can increase metadata, I/O and compression costs; mixed-version and rollback behavior continue to use existing object and transaction formats.

## Alternatives

Including the crossing row is inexpensive but permits the observed multirow overshoot. Exact optimal live-byte packing would require scanning otherwise cheap append-built vectors or adding stronger maintained metadata, while also handling physically dead area. The conservative charge preserves the common bulk path and may produce underfilled runs. A hard process-memory limit would require accounting beyond this sinker and is outside this change.

## Validation

- Read back actual data objects in sorted and unsorted, synchronous and pipeline modes. Cover below, equal and above target; the two 800-byte rows; repeated writes and a staged partial; and an oversized row between small rows. Check each entire object's charge, permitting only a one-row oversized exception, plus row identity, values and applicable ordering. For tombstones, verify variable-length primary-key runs split into multiple readable objects, every object stat reaches the caller, and a later write failure deletes the earlier unpublished run.
- Verify both a full block followed by a partial final block in one object and a full block followed by an oversized row in a separate object. Compare stats-only iteration with actual block row counts. Cover nulls, inline values, constants, aliases, borrowed windows, dead area, partial refills, and direct and fallback `WriteOwned` paths.
- Inject copy, sort, sink, sync and submission failures and cancellation after an earlier successful spill. Verify ownership, original error, cleanup, failed-delete retry, and absence of empty-run loops. Run affected normal and race tests, owning packages and SQL consumers; run incremental formatting and static checks over the complete PR delta. Exercise the CN SQL case for sorted and unsorted writes, rollback and update, and verify teardown and repeatability.
- Compare base, initial PR head and final implementation with identical inputs and interleaved repeated runs. Include fixed, fresh disjoint and aliased inputs, narrow and wide schemas, under-threshold writes, sorted/unsorted spills and skewed rows. Measure throughput, allocations, object/block counts, stored bytes, mpool high water and isolated process peak RSS with both a fake sink and a real-file or SQL workload. A repeatable representative normal-path regression above 5% beyond noise requires profiling and optimization or an explicitly reviewed tradeoff. Verify that wide alias hints are beneficial and that wide fixed/disjoint inputs add no hint allocation.
