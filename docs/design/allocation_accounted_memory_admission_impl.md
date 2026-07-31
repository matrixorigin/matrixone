# Allocation-Accounted Memory Admission: Implementation Plan

- Status: implementation validation
- Tracking issue:
  [#26459](https://github.com/matrixorigin/matrixone/issues/26459)
- Architecture:
  [Allocation-Accounted Memory Admission RFC](../rfcs/00000000_allocation_accounted_memory_admission.md)
- Baseline at plan creation: `main` at `38ce3a774`
- Rebased implementation baseline: `main` at `5b9eeb54ec`
- Allocation-site closure baseline (PR 3): `f61b64d56c`
- Lifecycle/activation heads (PRs 4--10): `8633757dc1`, `5ae8eca00a`,
  `8e4b689f45`, `c9ad0ea810`, `e072568998`, `eec23dcdc5`, and
  `383fc6dce3`
- Owner-atomic activation and final cleanup: `656e254fe6`
- Merged prerequisite: #26455 at `93e8b22d2`
- Independent design review: completed against RFC commit `a7d54cb5f`
- Activation status: enabled for the closed HashBuild expression owner set.
  Build/probe key closure is checked statement-atomically. Unsupported
  expression families keep all participating HashBuild/HashJoin/DedupJoin
  owners in that local attempt on the legacy path; they are not partially
  mixed into an exact generation.

## 1. Purpose and rules

The RFC owns architecture and invariants. This file contains only implementation
decisions, allocation-site status, PR scopes, and evidence for #26459.

Every implementation PR follows these rules:

1. Migrate a complete owner closure: alloc, grow, reuse, handoff, Reset, Free,
   and failure rollback.
2. One site is legacy, allocation-accounted, synchronously reclaimable named
   scratch, or small statically bounded headroom metadata.
3. Enable exact accounting and remove the same owner's legacy hard charge in
   one PR.
4. Do not call the account per row or on within-capacity reuse.
5. Do not store a mutable current account on Process or MPool.
6. Spill and cleanup memory remain accounted and preserve bounded progress.
7. Every merged PR is independently safe; a later PR cannot repair an unsafe
   interval.
8. The final state has no permanent legacy/exact behavior switch.

## 2. Starting point and allocation-site ledger

### Allocation and container boundaries

- `pkg/common/mpool/mpool.go`: `memHdr`, `Alloc`, `Grow`, `Grow2`, `Free`, and
  `GrowCapacity`. `memHdr` is currently fixed at 16 bytes. Cross-pool Free
  already delegates to the original MPool.
- `pkg/container/vector/vector.go`: data and varlen area are separate physical
  buffers. `Reset*` retains capacity; `Free` releases owned buffers; `cantFree*`
  identifies non-owning views.
- `pkg/container/batch/batch.go`: Clone/Dup/Union allocate destination buffers;
  `CleanOnlyData` retains capacity and `Clean` frees vectors.
- `pkg/sql/colexec/evalExpression*.go` and
  `pkg/container/vector/functionTools.go`: FunctionResult and expression
  executors retain result capacity across Reset and release it at Free.

### HashBuild and spill boundaries

- `pkg/sql/colexec/hashbuild/{budget,hashmap,spill,types}.go`: copied batches,
  expression estimates, hash-table tokens, auxiliary buffers, runtime-filter
  marshal, and spill scratch.
- `pkg/common/hashmap`, `pkg/container/hashtable`: physical hash-table blocks
  and resize plans.
- `pkg/vm/message/joinMapMsg.go`: reference-counted producer-to-consumer
  handoff; final Free may happen after HashBuild Reset.
- `pkg/sql/colexec/spillutil/join_spill.go`: decoded batches, retained reuse,
  scatter/row-ID/encode/decode buffers, and BucketReader/Writer cleanup.
- HashJoin, DedupJoin, and RightDedupJoin share the pressure and handoff
  contract.

### Merged #26455 boundary

#26455 is the immediate correctness prerequisite, not the final accounting
mechanism:

- it gives each HashBuild-owned expression root a generation-scoped retained
  lease and fixes reuse/reset lifetime mismatches;
- it still reconciles executor-owned capacity at the operator layer and keeps
  estimator-derived admission for uncovered growth overlap;
- it does not attach provenance to each MPool allocation, cover arbitrary
  built-in Go-heap scratch, or provide statement terminal finalization;
- an activation PR removes the matching #26455 retained lease at the same time
  exact allocation provenance becomes complete for that owner;
- exact MPool charges must never be stacked on the #26455 charge for the same
  physical capacity.

Ledger states:

- `L`: legacy prediction/reservation;
- `D`: exact primitive exists but this production owner is dormant;
- `A`: allocation-accounted;
- `S`: named bounded explicit scratch;
- `H`: small, statically bounded Go/runtime metadata covered by CN headroom;
- `R`: removed.

The independent review rejected owner-class rows as proof of closure. The
working ledger is allocation-site based:

| Allocation site | Allocator/mode and size | Terminal owner | Current | Target/blocker |
| --- | --- | --- | ---: | --- |
| `mpool.memHdr` and account-ID side map | Go maps; one pointer record plus optional account record per live allocation | pointer removal at physical deallocation | H | bounded by the measured finite registry/allocation-slot policy |
| `Vector.data` | MPool; capacity from `Grow`, on/off-heap follows `v.offHeap` | owning `Vector.Free` | A | activated HashBuild destinations use immutable off-heap provenance |
| `Vector.area` | MPool; independent varlen payload capacity | owning `Vector.Free` | A | activated HashBuild destinations use immutable off-heap provenance |
| `Vector.nsp/gsp` bitmap data | allocation-accounted off-heap `[]uint64`; independent geometric capacity, paired replacement admission | owning `Vector.Free`; `Reset` retains and clears only published words | A | selected HashBuild vectors and expression results are active |
| `FunctionResult.vec` data/area | off-heap Vector; rows and appended payload | executor `Free` | A | active for the closed HashBuild expression set |
| `FunctionResult.convenientParam` | Go slice; expression arity, not rows | executor `Free`/reuse | H | bounded by plan expression arity, not input rows or payload |
| decimal parameter conversion | retained allocation-accounted off-heap buffer; `rows*sizeof(Decimal128)` for decimal64/float32/float64 promotion | `FunctionResult.Free`; Reset/evaluation reuse capacity | A | active when present in a closed expression owner |
| IFF/CASE/COALESCE selection arrays | allocation-accounted off-heap `[]bool`; one or two arrays of `rows`, retained by executor | executor `Free`/reuse | A | CASE is active in the closed expression set |
| selected row IDs | allocation-accounted off-heap `[]int64`; capacity up to `rows`, retained by executor | executor `Free`/reuse | A | active when present in a closed expression owner |
| selected parameter/result vectors | allocation-accounted off-heap Vector capacities | executor `Free` | A | active when present in a closed expression owner |
| `JSON_ROW` output | one retained allocation-accounted function scratch buffer, then copy into the admitted result; both physical capacities are charged | `FunctionResult.Free`; row publication copies synchronously | D | A after expression-owner activation; arity-bounded column closures are cleared after every call |
| float array-distance row descriptors and result scratch | Go `[][]T` with one descriptor per row plus `[]float32` with one value per row | call return / GC | R | caller row accessor plus the upper half of the admitted `[]float64` result backing |
| GPU array-distance flattened inputs | allocation-accounted caller scratch; `query bytes + rows*dimension*4` for the SQL float32 GPU path; legacy callers retain the C allocator | GPU job retains the caller slice until Wait; launch failure rolls back before return | D | A only after the GPU-tag build/tests pass; the legacy API remains unaccounted and is not an activated owner |
| `NORMALIZE_L2` output scratch | pooled or per-row Go arrays with one output element per input element | pool / call return and GC | R | normalize directly into admitted varlen result storage |
| AES/ENCODE/DECODE output scratch | Go payload slices, one plaintext/ciphertext-sized allocation per row; AES padding could append into spare input capacity | call return / GC | R | validate size/padding first and write directly into admitted result storage |
| JQ visible output | one retained allocation-accounted function scratch buffer, then copy into the admitted result | `FunctionResult.Free`; row publication copies synchronously | D | A after expression-owner activation; `TRY_JQ` propagates infrastructure/account rejection instead of converting it to SQL NULL |
| JQ object sort keys and gojq value graph | Go slices/maps/interfaces proportional to input JSON structure | row completion / GC | L | requires a separately bounded or allocation-accounted JSON execution owner; blocks JQ activation |
| JSON_ARRAY/OBJECT/KEYS/PRETTY output and object-key scratch | direct storage-compatible ByteJSON/result builders; JSON_OBJECT keys use retained allocation-accounted function scratch | result or `FunctionResult.Free` | D | A after expression-owner activation |
| JSON parse/path/modify/merge/schema execution | Go strings, paths, maps/slices, ByteJson modification payloads, and schema graphs proportional to row input | row completion / GC | L | requires an admitted JSON execution arena or streaming algorithms; blocks these functions from activation |
| HASH/IN/PREFIX_IN/narrow-array conversion scratch | retained allocation-accounted function scratch, bounded per hash chunk or exact tuple/array bytes | `FunctionResult.Free` | D | A after expression-owner activation |
| codec output (HEX/base64/MD5/COMPRESS/UNCOMPRESS/random/quote/date formatting) | writes directly into admitted result backing; flate's fixed codec state remains Go/runtime memory | result Free; codec state ends at call return | D | A after result activation; flate state needs an explicit fixed-headroom measurement before H classification |
| geometry parse/overlay scratch | Go point/ring/interval/match slices proportional to geometry payload | row completion / GC | L | admitted per-row geometry workspace or streaming algorithm; remains an activation blocker |
| regexp compile/match/output scratch | Go regexp program and match/output buffers proportional to pattern/input | operator cache / row completion / GC | L | bounded regexp owner or exclusion from expression activation |
| H3/S2 neighborhood scratch | Go slices; some S2 paths are statically bounded, H3 grid-disk output scales with radius | row completion / GC | L | split proved fixed bounds from data-scaled paths before activation |
| JSON cast visible serialization | `MarshalJSON` payload slices proportional to the JSON value | row completion / GC | L | direct visible writer or exclusion from expression activation |
| hash-table initial cell block | off-heap `mpool.MakeSlice(..., true)`; 16 KiB int / 32 KiB string | hash map / `JoinMap.FreeMemory` | A | physical allocation is the sole charge |
| hash-table replacement/appended cell blocks | off-heap blocks, at most 4 MiB each; old+new overlap is physically visible | hash map / `JoinMap.FreeMemory` | A | replacement overlap is admitted before publication |
| hash-table `cells`/`newBlocks` descriptors | owning off-heap descriptor buffer; initial and replacement capacities are distinct physical allocations | hash map / `JoinMap.FreeMemory` | A | no row-scaled Go descriptor backing remains |
| hash-table `ResizePlan` and callback | fixed-size Go values/closures, one per table/resize | resize return / hash map Free | H | table-count bounded metadata; no row-scaled backing |
| `GroupSels.{tmp,vals,offsets}` | allocation-accounted off-heap buffers; O(build rows/groups) | builder or `JoinMap.FreeMemory` | A | provenance follows final JoinMap consumer |
| copied build-batch vector buffers | MPool Vector data/area | builder or `JoinMap.FreeMemory` | A | physical Vector leases replace projected batch tokens |
| spill marshal/coalesce buffers | allocation-accounted off-heap streaming buffers; exact serialized size and bounded 64 KiB per-bucket coalesce capacity | spill phase cleanup | A | optional coalesce degrades to direct write under pressure |
| spill hash values | allocation-accounted off-heap `[]uint64`; geometric capacity, `8*cap` | spill phase cleanup | A | exact capacity and rollback covered |
| spill row IDs | allocation-accounted off-heap `[]int32`; geometric capacity, `4*cap` | spill phase cleanup | A | exact capacity and rollback covered |
| spill counts/offsets/positions | Go `[]int32`; O(bucket count), bucket count finite | spill cleanup | H | bucket count is the fixed spill fanout |
| selected spill bucket vectors | allocation-accounted off-heap Vector capacities | per-call selected-batch cleanup | A | adaptive unpublished windows bound progress |
| BucketReader decoded vectors | allocation-accounted MPool Vector data/area | reusable batch cleanup / `BucketReader.Close` | A | replacement and retry start from a clean record |
| BucketReader input buffer | one 64 KiB Go buffer per production spill reader; direct legacy readers use a bounded 4 MiB default | `BucketReader.Close` / GC | H | reader count is bounded by the live SpillEngine set and the statement/CN hash cap retains fixed non-payload headroom |
| `pSpool` cached Vector data/area | provenance-bearing detached MPool buffers; accounted data/area sites cannot cross and legacy buffers keep a guarded fast path | `spoolBuffer.clean` or the receiving Vector's `Free` | A | activated vectors retain immutable provenance through cache reuse |
| runtime-filter serialized payload | one allocation-accounted payload; optional filter publication | message release | A | capacity pressure degrades to PASS before publication |
| spill disk and FD | disk/FD ledgers | file removal/close | A | A |

This is the known-site ledger for the activated HashBuild/join owner. Generic SQL
functions with unbounded JSON, geometry, regexp, H3/S2, or JSON-cast Go scratch
remain `L`; the build and probe activation gates reject the local statement's
entire owner set when one of those families appears. Consequently an activated
owner has no estimator-gated expression subtree, while an unsupported plan
remains wholly legacy and is an explicit later migration rather than a
partially exact owner.
No row named “other” or “unbounded scratch” can declare closure.

Batch destination propagation is now `D`: Clone, Dup, selected-column copy,
Union destinations, windows, reader decode, Clean, and FreeColumns preserve
the immutable destination selection without creating a synthetic batch-level
charge. `pSpool` now transfers a detached buffer together with its immutable
selection and data/area site, reuses it only for the same provenance, and
returns every cache ID on construction failure. Its allocation-unaccounted
production path retains the old raw-slice representation behind guards that
reject an accounted Vector. `Vector.SetTypeAndFixData` now returns a failed
growth admission and rolls type and length back without losing the original
backing.

Bitmap-aware selections are also `D`. Null and grouping backing remain
independent physical allocations, are included in `Vector.Allocated`, and use
the same immutable account/owner with distinct sites. The legacy `Bitmap`
footprint is unchanged: a tagged nonnegative/bitwise-complemented length
records backing ownership without adding a field. Accounted Vector growth
admits both bitmap replacements before either publishes, raw unadmitted bitmap
growth fails instead of escaping to the Go heap, Reset retains capacity while
clearing only represented words, copy/reader decode fills admitted backing
directly, and Free is the one terminal release owner.

## 3. Decisions required before production integration

### A. Allocation metadata representation

PR 0 prototype results on linux/amd64, Go 1.26.4, i7-11700:

| Representation | Map construction bytes/base entry | insert+delete median (five-run range) |
| --- | ---: | ---: |
| current 16-byte `memHdr` | 55.94 | 40.13 ns (39.97--40.40) |
| inline 24-byte header, all allocations | 83.97 | 40.21 ns (40.00--40.67) |
| side map, 1% accounted | 56.24 | 40.17 ns (40.16--40.43) |
| side map, 10% accounted | 58.29 | 40.81 ns (40.77--41.19) |
| side map, 100% accounted | 93.76 | 49.39 ns (48.39--49.89) |

A bounded fixed registry prototype used 40.04 bytes per slot, including its
account state. Concurrent lookup across 1,024 generation-tagged handles
measured 0.4278 ns/op aggregate median (0.2876--0.4463) at `GOMAXPROCS=8`,
versus 5.554 ns/op (5.225--5.682) for the `sync.Map` comparison; both reported
zero allocations. The exact command, environment, and all five samples are in
[the raw benchmark record](evidence/26459_allocation_accounting_bench.txt).

The same artifact now records real unaccounted baselines at `GOMAXPROCS=8`:

| Existing path | Median | Five-run range |
| --- | ---: | ---: |
| sharded MPool alloc/free, 64 B | 245.6 ns | 242.4--249.6 ns |
| sharded MPool alloc/free, 4 KiB | 291.8 ns | 290.4--295.6 ns |
| sharded MPool alloc/free, 64 KiB | 999.2 ns | 991.2--1,003 ns |
| sharded MPool grow, 64 B to 64 KiB | 1,267 ns | 1,244--1,279 ns |
| `noLock` MPool alloc/free, 64 B | 222.7 ns | 222.5--226.0 ns |
| parallel sharded MPool alloc/free, 64 B | 209.4 ns | 176.6--213.2 ns |
| fixed Vector pre-extend/free, 8,192 rows | 1,042 ns | 1,028--1,089 ns |
| varlen Vector data+area pre-extend/free | 28,011 ns | 27,634--28,170 ns |
| fixed Vector Reset/capacity reuse | 2.810 ns | 2.691--2.852 ns |

These measurements establish the pre-integration comparison baseline. The
prototype-map results are still not final accounted-MPool results, so they do
not by themselves freeze the representation.

A test-only account-aware wrapper around the real MPool then measured:

| Prototype path | Median | Five-run range | Delta from sharded baseline |
| --- | ---: | ---: | ---: |
| alloc/free, 64 B | 327.0 ns | 321.4--329.7 ns | +33.1% |
| alloc/free, 4 KiB | 367.6 ns | 361.9--381.9 ns | +26.0% |
| alloc/free, 64 KiB | 1,085 ns | 1,072--1,106 ns | +8.6% |
| grow, 64 B to 64 KiB | 1,400 ns | 1,390--1,418 ns | +10.5% |
| parallel alloc/free, 64 B | 227.2 ns | 203.1--233.5 ns | +8.5% |

Every sample reported zero Go allocations. This wrapper intentionally takes a
second metadata-shard lock after MPool has already published its pointer
header. The 26--33% small-allocation cost rejects that shape for production;
PR 1 must publish the optional account side record under MPool's existing
pointer-shard transaction and re-run the comparison. The wrapper remains a
conservative upper bound and validates real allocation/growth rollback.

Provisional choice:

- keep `memHdr` at 16 bytes and replace the final `offHeap` byte with a
  three-state allocation kind: on-heap, unaccounted off-heap, or accounted
  off-heap;
- store a compact 16-byte `pointer -> account pointer + owner/site` lease only
  for accounted allocations in the same pointer shard, or in the same
  pool-local metadata store for `noLock` pools. The direct account pointer
  keeps the original generation alive through late physical `Free` without a
  process-global registry lookup;
- publish/remove the pointer header and optional account ID in one metadata
  transaction under the same lock; a metadata failure rolls back both before
  allocation publication;
- use a finite registry of reusable slots and encode `(slot, generation)` in
  the handle; reuse a slot only after attempt-owned seal and exact zero, and
  retire it rather than allowing the generation counter to wrap;
- reserve one finite CN-local allocation-metadata slot before publishing an
  accounted allocation or replacement and return it on physical Free;
- size the registry and side-record backing stores from explicit CN headroom,
  rather than allowing maps to grow without a hard count bound.

The first hash-table activation fixes the initial sizing policy:

- reserve 131,072 generation slots. The production registry plus a live
  64-byte account measures 80.09 bytes/slot; budgeting 128 bytes/slot reserves
  16 MiB. This default exceeds the
  frontend `max_connections` system variable's declared upper bound of 100,000
  and leaves 31,072 slots for remote/internal attempts. Only an attempt whose
  physical plan contains an activated owner opens a slot. The slot limit is
  itself the hard supported activated-attempt concurrency; a deployment that
  intends to support more must raise it and pass the same headroom check before
  enabling the owner;
- every integer/string cell block is at least 16 KiB. After the outer
  descriptor is made an owning accounted off-heap allocation, every live
  table has at most one live descriptor buffer per live table version.
  Therefore the first activation uses
  `3 * ceil(MPoolGlobalCap / 16 KiB)` allocation-metadata slots. Two units
  cover every live cell block plus its table's published descriptor; the third
  covers one unpublished replacement descriptor per live table before the
  matching cell allocation either publishes or rolls back;
- the production base pointer map plus a 16-byte all-accounted lease map
  measures 111.87 bytes/entry, of which 55.93 bytes/entry is incremental over
  the existing pointer map. Budget 128 bytes/allocation-metadata slot to cover
  sparse shards and Go-map growth overlap, and 128 bytes/generation slot for
  the final account fields. At MPool's 1 GiB minimum global cap this is 24 MiB
  plus 16 MiB; at
  larger caps the allocation component is 2.34375% of the MPool cap and the
  fixed registry fraction decreases. PR 1 must measure construction, sparse
  occupancy, and grow/evacuate high water; exceeding these conservative
  constants blocks merge rather than silently consuming payload headroom;
- startup must reserve
  `allocationSlots * 128 + generationSlots * 128` bytes outside the MPool
  payload cap. If the host/container limit cannot supply it, the activated
  owner is refused at startup rather than running with an unproved headroom
  assumption.

Later activations with smaller allocations must derive a new simultaneous
allocation bound and resize this headroom before they can become `A`; they
cannot inherit the hash-cell formula merely because the generic API exists.

This choice is not frozen until real MPool alloc/grow/free, cross-pool Free,
deleted-pool fallback, real accounted ratios, and P50/P99 concurrent latency
match the prototype result.

Metadata is `H`, not silently included in payload capacity. Safety comes from
finite generation and allocation-metadata slot limits; slot exhaustion is a
typed exact-pressure result. Each activation PR must also show that its
supported simultaneous allocations and generations fit the configured limits,
using measured pointer/side/registry bytes per entry and resulting aggregate
CN headroom. This prevents a safe-but-impractical false metadata-pressure
regression.

### B. Account-aware API shape

The provisional API rules are:

- only an explicit first off-heap allocation accepts an account, owner, and
  site;
- ordinary `Grow` inherits account provenance from allocation metadata and
  takes no replacement account argument;
- account-A memory cannot grow under account B;
- an accounted on-heap allocation is rejected;
- unaccounted-to-accounted conversion allocates a new destination;
- ordinary unaccounted callers keep current behavior;
- helper delegation cannot silently drop the account.

One `AllocationCapacity` rule must cover initial allocation, growth, runtime
rounding, and `CapLimit-kMemHdrSz`. `recordPtrHdr` failure, allocator panic,
cross-pool Free, deleted-pool fallback, and pool teardown are explicit
transaction branches.

### C. Generation owner and terminal snapshot

One execution attempt of `Compile` on each CN owns the generation. The
statement `ResourceRoot` aggregates its immutable terminal snapshot, but does
not own allocation release. HashBuild Reset is also not the generation owner.

The attempt opens before `prePipelineInitializer`/operator Prepare and owns:

```text
all local scopes, remote notifiers, and message consumers quiescent
  -> close and drain that attempt's MessageBoard
  -> seal new admission
  -> release remaining live leases
  -> used=0
  -> export one immutable snapshot
  -> remove registry entry
```

`Scope.Run` defers pipeline cleanup, and `Scope.MergeRun` joins pre-scopes and
remote notifier goroutines before returning. Therefore the local hook belongs
in a deferred attempt finalizer around `Compile.runOnce`, after its result and
before retry transition or attempt publication. A retry finalizes the failed
attempt before `buildRetryCompile` opens the next generation.

The remote hook belongs after `Scope.MergeRun` and in the existing
`runCompile.clear` terminal defer, which releases operators, resets the
MessageBoard, and snapshots the remote MPool before replying. PR 4 must add an
explicit MessageBoard close-and-drain operation: ordinary multi-CN `Reset`
only removes the board from `StmtIDToBoard` because producers may still access
it, whereas the attempt hook runs after the existing sender/receiver cleanup
barriers have proved quiescence.

The same deferred finalizer covers success, error, panic, cancellation, retry,
broadcast, remote execution, and a JoinMap freed after producer Reset.
`SetStmtProfile` turnover, frontend `StatementInfo.EndStatement`, and
`HashBuildBudgetGeneration.Close` are observability or operator boundaries,
not acceptable release substitutes.

If terminal cleanup ends nonzero, the attempt coordinator exports one immutable
invariant-failure snapshot and retains a release-capable tombstone. That CN
admits no new accounted generation until all such tombstones drain to zero, so
registry growth is bounded by generations already active at detection. A
deadline escalates owner/site diagnostics and allows controlled CN restart; it
never deletes live provenance.

Generation open and suspension publication use one CN-local linearization
gate. An open that linearizes after suspension cannot publish.

PR 4 derives this terminal matrix:

| Attempt path | Required terminal ordering | Oracle |
| --- | --- | --- |
| local success | scopes join -> board close/drain -> seal -> zero -> publish | one valid snapshot, no queued message |
| local error/cancel/panic | cancellation -> every started scope cleanup/join -> board drain -> seal | one terminal snapshot; no goroutine or allocation survives |
| failure before `runOnce` | opened generation -> initializer rollback -> board drain -> seal | zero or one named invariant failure, never an abandoned open slot |
| retry | attempt N fully finalizes -> attempt N+1 opens | old handles are stale; no cross-attempt publication |
| remote execution | remote `MergeRun` joins -> `runCompile.clear`/board drain -> snapshot -> response | parent receives one immutable child snapshot |
| broadcast/late JoinMap Free | producer Reset -> every consumer cleanup -> queued refs drain | physical final Free releases the original generation exactly once |
| prepared reuse | attempt finalizes and replaces its board -> cached pipeline Reset -> next attempt opens | no retained accounted capacity crosses statement generations |
| nonzero terminal | seal -> failure snapshot -> tombstone/suspend -> late Free | no new open until every tombstone reaches zero |

### D. Go-heap boundary

`MPool.Alloc(..., false)` records requested bytes but `Free` does not reclaim
them synchronously. Therefore:

- all data/row/payload-scaled controlled allocations move off-heap before
  activation;
- small Go metadata may be `H` only with a static bound and separate CN
  headroom;
- no data-scaled Go slice may be relabeled `S` to bypass migration.

### E. Pressure and operation rollback

Capacity pressure, sealed generation, account mismatch, allocator-size limit,
and invariant corruption are distinct typed results. Only capacity pressure is
recoverable.

Each retryable owner records an operation checkpoint and cleanup/restart rule.
For example, `Vector.PreExtendWithArea` may grow data and then fail area growth;
that retained growth is valid accounting state but not proof that re-running
the logical operation is idempotent. Before a shared controller exists, an
exact rejection is a controlled terminal pressure error.

The PR 0 reference model validates the provisional shared rule:

- replacements remain private while the old allocation stays published and
  charged;
- all new allocations and replacements commit as one logical operation;
- cancellation or later allocation failure frees private allocations and
  restores the checkpoint before retry;
- a second attempt cannot begin while the failed operation remains active;
- retry may reduce the requested capacity, but cannot duplicate publication;
- an owner that cannot preserve or reconstruct the checkpoint is not
  retryable and returns the typed pressure error after cleanup.

For each spill owner choose an already allocated reusable buffer, a finite
progress sub-cap, or a smaller chunk. Normal work and progress allocations stay
under the same total query/CN cap; no uncharged emergency scratch is allowed.

## 4. Pull request sequence

### PR 0: model and measured design decisions

Scope:

- close decisions A--E with prototypes and benchmarks;
- finalize bounded owner/site enums;
- implement a test-only reference state machine;
- record MPool/vector allocation baselines;
- complete the first activation owner inventory and record the generation
  method for later expression/spill inventories without changing production
  behavior.

Current evidence:

- the reproducible test-only artifact is
  `experiment/26459-allocation-accounting-validation` at `cde44cd099`:
  `pkg/common/mpool/allocation_account_validation_test.go` and
  `pkg/common/mpool/allocation_account_benchmark_test.go` and
  `pkg/container/vector/allocation_account_validation_test.go`;
- the test-only model passes alloc, within-capacity reuse, old+new growth,
  injected unpublished failures, views, deep copy, Reset, multi-allocation
  checkpoint/commit/rollback, cancellation before and after allocation,
  smaller retry without duplicate publication, bounded generation and
  allocation-metadata slots, stale slot generations, exact metadata overlap
  on replacement, generation-counter exhaustion without wrap, accounted
  on-heap rejection, sealed-error precedence, handoff, cross-pool Free,
  sealed-vs-capacity errors, zero finalization, nonzero
  tombstone/suspension/drain, normal and `noLock` pool teardown,
  open-vs-suspend linearization, stale generation, and 20,000 deterministic
  randomized operations;
- the metadata and contention microbenchmarks in section 3 passed five runs;
- real unaccounted MPool alloc/free/grow and Vector allocate/reuse baselines in
  section 3 passed five runs;
- the account-aware real-MPool wrapper passes allocation, reuse, old+new growth,
  rollback, and final-zero validation; its five-run result rejects a second
  side-metadata lock for production;
- at `GOMAXPROCS=8`, a serialized mutex acquire/release prototype measured
  80.73 ns/op median versus 21.11 ns/op for a two-operation atomic prototype,
  so real aggregate-account contention remains a mandatory design benchmark;
- production behavior is unchanged.

Gate:

- the existing MPool/Vector baseline and rejected separate-lock shape have
  reproducible five-run measurements; the selected same-shard transaction is
  an explicit PR 1 merge gate;
- the model covers alloc, grow, failure, view/copy, Reset, operation rollback,
  cancellation/retry, finite metadata slots, handoff, cross-pool Free, seal,
  and stale generation;
- per-entry metadata and maximum simultaneous allocation/generation counts
  prove finite aggregate CN headroom;
- the site ledger is complete for cell and descriptor initial allocation,
  replacement, segmented growth, rollback, and terminal Free of the hash-table
  first activation;
- generation owner, Go-heap classification, typed errors, and retry checkpoint
  decisions are closed;
- production owners remain `L`.

PR 0 design gates are closed. The metadata representation remains provisional
until PR 1's integrated benchmark passes, and no production owner may switch
from `L` to `A` before PRs 1--4 close. The separate-lock prototype is a
recorded rejected design, not an implementation candidate.

### PR 1: generic account and MPool allocation transaction

Scope:

- low-level account contract below SQL/process;
- compact account-ID registry and dormant `HashBuildBudgetGeneration` adapter;
- finite generation/allocation-metadata slot limits and their typed pressure
  results;
- account-aware alloc, Grow/Grow2, Free, and immutable snapshots.

Required behavior:

- reject accounted on-heap allocation;
- reserve the complete new capacity before allocation;
- for growth keep old and complete new capacity live until publication;
- roll back on admission, MPool/global-cap, metadata, or allocation failure;
- use one allocation-capacity rule for initial and growth boundaries;
- release through normal and cross-pool physical Free, deleted-owner-pool
  fallback, and `noLock` teardown that physically deallocates;
- retain metadata and charge when normal-pool teardown only unregisters the
  pool, and report live accounted allocations there as an invariant;
- reject account mismatch and stale handles.

Gate:

- exact/one-byte-short, allocator rounding, `CapLimit-kMemHdrSz`, and
  `GrowCapacity` boundaries;
- old+new overlap;
- injected failure or panic at every unpublished step, including metadata;
- atomic header/account-ID publication and removal for sharded and `noLock`
  pool metadata;
- normal-pool unregister plus late Free, `noLock` physical teardown, and no
  premature release in either case;
- concurrent acquire/release, double Free, seal, and final zero;
- measured unaccounted/accounted alloc/free/grow overhead and concurrent
  generation P50/P99 latency;
- no production owner selects an account.

The current PR 1 candidate is
`feature/26459-allocation-account` at generic commit `766e1501c3` and
HashBuild-adapter commit `0655af4443`. It remains dormant. Its same-lock
pointer/lease transaction, finite registry, stale-handle checks, old+new
growth, deleted-pool/noLock lifetime rules, and tokenless HashBuild adapter are
implemented. Returned-error and panic rollback is injected after account,
metadata, global stats, pool stats, physical allocation, and header
publication for both sharded and `noLock` metadata. Normal package tests,
package vet, full package race tests, and the focused lifecycle/rollback race
matrix at 100 repetitions pass. Representation, latency, and integrated
benchmarks are recorded in the evidence artifact. No production owner selects
an account, and no legacy hard charge has been removed.

### PR 2: dormant Vector and Batch propagation

Scope:

- optional account selection for owning off-heap Vector buffers;
- data and area growth;
- Batch Clone/Dup/Union destination propagation.

Required behavior:

- Reset retains charge; Free releases it;
- within-capacity append performs no account operation;
- aliases/views/const/shared area do not create another charge;
- deep copies use the destination account;
- at the PR 2 boundary, on-heap null/group bitmaps remain explicit later-PR
  blockers and are not silently included in the Vector charge;
- HashBuild production remains legacy until a later owner migration.

Gate:

- randomized fixed/varlen append;
- Reset/reuse/Free, views, partial selection, copy rollback, cross-pool Free;
- package race tests and vector benchmarks;
- Vector/Batch ledger rows become `D`.

The current PR 2 candidate is
`feature/26459-vector-propagation` at commit `dbfee20ecc`. It remains dormant.
It adds one immutable shared selection pointer to Vector and Batch, accounts
the first owned off-heap data/area allocation, lets later Grow/Grow2 inherit
the physical MPool lease, and rejects implicit conversion to on-heap or
no-copy aliases. Reset retains the selection and charge; Free clears the
selection after the physical allocations release their leases. Views carry no
selection, while Batch windows retain only the destination context needed for
a later deep copy.

The implementation also closes two error edges found during self-review:
reader growth publishes the replacement buffer before a short read can return,
so cleanup never retains a freed old pointer, and no-copy Batch decode
explicitly detaches an empty Vector selection while retaining the Batch
destination context.

Fresh local evidence on linux/amd64, Go 1.26.4, i7-11700:

| Vector operation, `GOMAXPROCS=8` | Legacy median | Accounted median | Difference |
| --- | ---: | ---: | ---: |
| fixed pre-extend/free, 8,192 rows | 1,026 ns | 1,116 ns | +8.8% |
| varlen data+1 MiB area pre-extend/free | 76,766 ns | 77,650 ns | +1.2% |
| accounted fixed Reset/reuse | n/a | 1.524 ns | no account operation |

Fixed paths remain 0 B/op and 0 allocs/op. Both varlen paths report the same
48 B/op and 2 allocs/op, so accounting adds no Go allocation. Randomized
fixed/varlen append, separate data/area charge, within-capacity reuse, Reset,
Free, views, partial selection, cross-owner copies, metadata rollback,
cross-pool Free, sealed accounts, shuffle replacement, copy/reader decode, and
Batch Clone/Dup/Union/FreeColumns pass. Every new and directly affected test
passed an exact `-race -count=100` run; both owning packages passed complete
race runs, build, vet, coverage, and dependent HashBuild/SQL/engine package
tests. No production owner selects an account and no legacy hard gate is
removed.

### PR 3: allocation-site closure and dormant propagation

Scope:

- complete the generated expression/built-in and spill allocation-site ledger;
- propagate dormant accounts through FunctionResult, expression result,
  selected result, decoded Vector, and off-heap scratch constructors;
- replace data-scaled Go vector null/group bitmaps,
  selection/conversion/hash/row-ID/serialization buffers with off-heap owners
  or direct output;
- do not enable production accounting or remove a legacy hard gate.

Gate:

- every reachable `make`, capacity-growing `append`, `bytes.Buffer`, MPool, and
  nested executor result has allocator, bound, terminal owner, and test;
- fixed/varlen/const/null, CAST, CONCAT, CASE, nested and selected paths close;
- repeated Eval/Reset/Free and construction failure reach the same terminal
  owners;
- all migrated rows become `D`; production rows remain `L`.

The current dormant PR 3 candidate is
`feature/26459-expression-propagation` at commit `fd2fcc953b`, rebased on
`main` commit `60e36bef64`. Its ordered closure commits are:

- `06d39287e4`: generic allocation-accounted MPool ownership;
- `968c68df4c`: dormant HashBuild budget bridge;
- `941212edf0`: Vector propagation;
- `61a6e7a0e2`: expression propagation;
- `b03ec24c2f`, `309d070c71`, and `556d64d5ba`: spill scratch, streaming,
  and serialization;
- `86e041ff53`: retained Vector/spool ownership gaps;
- `2b05fdac79`: Vector bitmap and conversion scratch;
- `24b5f6fef7` and `57940c8f0f`: direct-output row/function scratch removal;
- `674f78dea9`: general retained function-scratch provenance;
- `fd2fcc953b`: direct codecs, HASH/IN/PREFIX, GPU SQL flattening, and
  ByteJSON/JQ/JSON output closure.

Its propagation call chains are:

```text
NewExpressionExecutorWithAllocation
  -> recursive expression construction
  -> constant/result/scratch bitmap-aware AllocationAccountSelection
  -> FunctionResult result/parameter/function scratch or selected/decode Vector growth
  -> MPool AllocAccounted/Grow/Free

NewSpillEngineWithAllocation
  -> BucketReader decoded/reused Batch selection
  -> scatter hash/row typed slices and selected Batch selection
  -> exact streaming record and optional coalesce buffers
  -> MPool AllocAccounted/Grow/Free

Pipeline spool accounted copy
  -> detach Vector data/area with immutable provenance
  -> cache only by matching selection and data/area site
  -> attach to the next owning Vector or free at spool cleanup
```

The candidate adds no production caller of either dormant constructor and
removes no legacy reservation. It covers fixed, varlen, NULL, decoded-vector,
nested `CASE(CONCAT(CAST))`, folded and non-folded result transfer, partial
selection, repeated Reset/reuse, construction rollback, zero-length retained
scratch growth, decoded-record merge/error cleanup, scatter selected-vector
peak, and capacity-failure cleanup. Typed slices grow geometrically only on
the accounted path; the old and replacement capacities are simultaneously
charged until publication, and terminal cleanup frees a zero-length view by
its retained capacity.

Vector null/group bitmap growth now follows the same rule. Both replacement
buffers are admitted before publication, so rejection of the second buffer
rolls the first unpublished buffer back and preserves both old owners.
`pSpool` releases bitmap backing after data/area detach, recreates it under the
destination provenance, and preserves grouping semantics including constant
vectors. Accounted window/duplicate/decode paths pre-admit bitmap coverage
before legacy raw bitmap APIs can mutate it.

Decimal128 promotion from decimal64, float32, and float64 now writes into one
retained allocation-accounted parameter buffer per argument. Const promotion
uses the scalar wrapper and allocates no row scratch; normal/null promotion
reuses the admitted buffer across evaluations. Capacity, sealed-account, and
metadata failures return through the function executor instead of panicking or
falling back to a Go slice. The allocation selection and parameter buffer must
share the same account and owner.

The spill record path now streams Bitmap, Nulls, Vector, and Batch wire formats
directly into one allocation-accounted off-heap buffer. It computes the exact
wire size before admission, retains one record buffer for the phase, bounds
each bucket's coalesce buffer at 64 KiB, and degrades optional coalescing to a
direct write when payload or metadata capacity rejects it. Wire round trips,
legacy byte equivalence, retained-buffer reuse, coalesce fallback, write
failure, and phase cleanup are covered.

The pipeline spool now preserves Batch and Vector selection through cached
copies, keeps data and area allocation sites distinct, refuses cross-account
reuse, returns the cache slot after allocation failure, and fixes a legacy
non-last cache removal that previously dropped a still-owned buffer
descriptor. `SetTypeAndFixData` publishes a fixed-width type change only after
growth succeeds and propagates its error through all four callers.

The first syntax inventory over non-test expression/builtin and spill sources
found 484 candidate lines. The latest focused function scan still reports
candidate syntax, not live-byte proof: the largest `make([]...)` concentrations
are `func_unary.go` and `func_binary.go` (28 each) and
`func_builtin_json.go` (26); the largest potentially growing `append` groups
are `func_unary.go` (37), `func_builtin.go` (36), `func_cast.go` (16), and
`func_builtin_json.go`/`func_binary.go` (13 each). Buffer/builder and JSON
serialization scans are recorded in the site rows above. Admin/CTL/UDF code,
arity-only slices, fixed stack buffers, and legacy-only branches are not
silently counted as controlled payload, but each must be explicitly excluded
or bounded before activation. Vector null/group backing, decimal conversion,
direct codecs, HASH/IN/PREFIX scratch, SQL GPU flattening, and visible
JSON/JQ output no longer create data-scaled unaccounted payload on the dormant
path. JQ graphs, JSON parse/modify/schema, geometry, regexp, scalable H3, and
JSON cast serialization remain explicit activation blockers; the dormant
constructor is not a license to enable partial accounting.

The first follow-up scan also removed two allocations instead of moving them:
`FIELD` now writes directly into its pre-extended result and retains
arity-bounded parameter wrappers, while `VALUES` uses contiguous `UnionBatch`
instead of constructing one row ID per input row. Their exact tests passed
`-race -count=100` and the complete Function package passed normal, vet, and
race runs. Remaining candidates must first make this same
direct-output/streaming test before introducing a scratch owner.

The second follow-up removes rather than accounts four more scratch families:

- `JSON_ROW` no longer owns one encoder and buffer per row. It prepares
  arity-bounded typed column closures once, streams one complete row through a
  single reusable encoder, and clears every closure on success, error, or
  panic. Legacy execution reuses one `bytes.Buffer`; dormant exact execution
  uses one retained allocation-accounted function buffer and then copies the
  completed row into the admitted result. The scratch and result capacities
  are both physical and therefore both charged. The unsigned path preserves
  the full `uint64` range.
- float array distance no longer materializes one `[]T` descriptor per input
  row or a second result slice. The metric layer accepts a synchronous row
  accessor and caller-owned output; SQL uses the upper half of its already
  admitted `[]float64` result bytes as `[]float32` scratch and converts forward
  only after Wait. The SQL float32 GPU path now flattens query and row inputs
  into caller-owned allocation-accounted function scratch, and the GPU job
  retains that slice until Wait. The public legacy GPU API keeps its C-allocator
  behavior and is not part of the activated owner.
- `NORMALIZE_L2` writes float32, float64, BF16, Float16, int8, and uint8 results
  directly into admitted varlen storage. The old float pools and per-row
  widened/narrowed arrays are deleted.
- AES ECB/CBC and legacy ENCODE/DECODE write directly into admitted result
  storage. AES validates the final decrypted block before result publication,
  and padding is assembled in one stack block, removing both payload copies
  and the old possibility that `append` modified spare input-vector capacity.

The final dormant closure adds one retained `FunctionResult` scratch owner with
a site distinct from decimal parameter conversion. Detection is allocation-free;
the first nonzero resize admits physical capacity, geometric growth charges old
and replacement capacity until publication, Reset retains it, and
`FunctionResult.Free` is the sole terminal release. It is used for exact HASH
key chunks, fixed/string IN tuples, PREFIX_IN entries, narrow array conversion,
JSON_OBJECT keys, JQ/JSON_ROW visible output, JSON modify value
pre-materialization, and SQL float32 GPU flattening. One-byte-short tests cover
the generic allocator boundaries, and capacity-rejection tests cover each
new scalable function family. `TRY_JQ` suppresses jq/domain errors but never an output
allocation failure.

Storage-compatible ByteJSON encoders now write scalar, array, object,
object-key-array, typed, opaque, bit, and nested values directly into Vector
area backing. JSON_ARRAY, JSON_OBJECT, JSON_KEYS, JSON_PRETTY, JSON_QUOTE, and
JSON_ROW no longer build a second output-sized ByteJSON or visible-text slice.
JSON_EXTRACT no longer retains `rows * path-count` path arrays: nonconstant
paths are parsed one row at a time into arity-sized reusable storage. This does
not close the parser/modifier/schema graphs themselves; those remain `L`.

HEX/UNHEX, MD5, base64, vector-base64, COMPRESS/UNCOMPRESS, RANDOM_BYTES,
CHAR, MAKE_SET, EXPORT_SET, bitwise strings, array casts, quote, date/time
formatting, and FROM_UNIXTIME now publish directly into admitted result
capacity. The legacy production path for two-pass-capable formatting remains
one-pass; exact two-pass sizing is selected only by the dormant allocation
owner, so this PR does not impose duplicate formatting work before activation.

`FunctionResult.AppendBytesWithBuilder` is the shared publication primitive;
`AppendBytesWithFill` is its exact-size convenience wrapper. It admits result
data/area capacity before exposing a row, accepts an actual length no larger
than the admitted capacity, collapses a short result into inline storage when
possible, and publishes length only after successful completion. Error,
invalid-length, and panic paths restore the unpublished varlena header and area
length. Capacity acquired before rollback remains owned and charged to the
result until normal reuse or `Free`; no allocation is released without its
terminal owner.

Fresh five-run benchmarks on linux/amd64, Go 1.26.4, i7-11700:

| 8,192-row function path | Before median | `fd2fcc953b` median | Before allocation | `fd2fcc953b` allocation |
| --- | ---: | ---: | ---: | ---: |
| float32 L2-squared batch distance, 128 dimensions | about 483 us | 437.8 us | 229,488 B/op, 6 allocs/op | 112 B/op, 4 allocs/op |
| `JSON_ROW`, fresh operator | about 838 us | 472.7 us | 1,704,081 B/op, 16,387 allocs/op | 520 B/op, 8 allocs/op |
| `JSON_ROW`, reused operator | about 474.7 us | 471.4 us | about 831 B/op, 8 allocs/op | 264 B/op, 5 allocs/op |

The direct-output candidate passed each new or directly affected behavioral
test separately under race with a 30-second adaptive budget. Every measured
test used `-count=100` except the 0.35-second incompressible DEFLATE-bound test,
which used `-count=85`; no empty regex was accepted as stress evidence.
ByteJSON, Vector, metric, Function, and colexec passed complete normal and race
runs; the same package closure passed vet and `golangci-lint` with zero issues.
Coverage for that closure is 78.3% ByteJSON, 50.9% Vector, 90.3% metric, 54.8%
Function, and 64.3% colexec. The unsafe result alias has its own functional
test, and error/invalid-length/panic rollback proves result length, area
length, reuse, and final account zero.

The GPU source path passed manual ownership review: exact SQL execution sizes
and fills caller-owned scratch before launch, a successful launch transfers a
retained slice reference to the job, Wait drops it only after the asynchronous
kernel terminates, and dimension/launch failure publishes no job owner. The
legacy nil-scratch API keeps its existing C-buffer allocate/rollback/Wait
contract. This host has neither `nvcc` nor a CUDA conda environment, so the
GPU-tag build and tests were not run locally. That validation gap remains an
explicit merge/activation gate; CPU success is not treated as GPU evidence.

Fresh local validation on linux/amd64 with CGO and the repository-built
third-party artifacts passes:

- complete normal tests for MPool, Vector, Batch, SQL util, expression,
  pSpool, spillutil, HashBuild, HashJoin, DedupJoin, RightDedupJoin,
  Connector, Dispatch, Function, and Process;
- build and vet for the same package closure;
- exact `-race -count=100` runs for every new test plus directly affected flow
  control, BucketReader merge, scatter lifecycle, and re-spill tests;
- complete race runs for MPool, Vector, Batch, SQL util, expression, spillutil,
  pSpool, Connector, and Dispatch;
- package coverage after the new closures of 74.0% MPool, 77.0% Bitmap, 41.2%
  Nulls, 48.3% Vector, 73.4% Batch, 82.1% pSpool, 78.8% spillutil, and 54.1%
  Function.

For `a63c68b07b`, every new bitmap, buffer, Vector, decimal-conversion, spool,
spill, and directly affected aggregate test passed an exact
`-race -count=100` run after the final edit. Bitmap, MPool, Vector, pSpool,
spillutil, aggregate, expression, and Function packages then passed complete
`-race -count=1` runs; the full affected normal-test and vet closure also
passed with repository-built CGO third parties.

The existing constant-flow-control benchmark remains 0 B/op and
0 allocs/op. Five-run medians at `GOMAXPROCS=8` were 4.480 ns/op on the PR 2
base and 4.464 ns/op on the PR 3 candidate; this focused benchmark shows no
measurable legacy fast-path regression, but it is not an activation-level
performance result.

The legacy 8,192-row pipeline-spool copy/reuse benchmark remains 200 B/op and
2 allocs/op. An interleaved five-run comparison at `GOMAXPROCS=8` measured a
1,542 ns/op median at commit `9f88a0e83e` and 1,540 ns/op at `1f82b218c2`;
the guarded allocation-unaccounted fast path has no measurable regression.

At `a63c68b07b`, the same legacy pipeline-spool benchmark remains 200 B/op and
2 allocs/op with a 1,545 ns/op five-run median. The constant flow-control
benchmark remains 0 B/op and 0 allocs/op with a 4.259 ns/op median. The tagged
bitmap ownership representation is what preserves the legacy allocation
footprint.

An 8,192-row first pre-extend/free costs 1,059 ns/op for the legacy Vector,
1,159 ns/op for data-only dormant accounting, and 1,986 ns/op when the two
independent bitmap allocations are also admitted; all three remain 0 B/op and
0 allocs/op. This one-time physical-allocation cost is explicit rather than
hidden. Capacity reuse is the steady-state path: empty Reset measured
2.431 ns/op for data-only accounting and 2.650 ns/op with bitmap ownership,
again with zero Go allocations. Activation performance tests must retain this
distinction instead of presenting the first-allocation cost as a per-row cost.

### PR 4: statement lifecycle and minimum pressure foundation

Implementation: `8633757dc1`. The attempt coordinator now owns board drain,
owner teardown, exactly-once terminal completion, immutable export, and the
release-capable tombstone/suspension path. Typed lifecycle failures are
disjoint from retryable capacity pressure.

Scope:

- add the attempt-owned post-pipeline/MessageBoard-close seal/finalize hook;
- export one immutable valid or invariant-failure generation snapshot;
- retain release-capable tombstones and suspend new accounted generations
  after nonzero terminal cleanup;
- introduce non-overlapping capacity, sealed, mismatch, allocator-limit, and
  invariant error reasons;
- define operation checkpoint/rollback helpers needed by later retry;
- keep owner accounting dormant.

Gate:

- success, execution error, cancellation, retry, local/remote scope, broadcast,
  and message-board teardown each seal exactly once;
- JoinMap/spill payload released after producer Reset still releases the
  original generation;
- zero finalization exports once and removes the registry entry;
- nonzero terminal cleanup exports one failure snapshot, retains a
  release-capable tombstone, stops new accounted generations on that CN, and
  removes the tombstone only after late Free reaches zero;
- concurrent terminal failures are bounded by generations active at first
  detection, and stale IDs cannot resolve after removal;
- a race test proves every successfully published generation linearized before
  suspension and every later open is rejected;
- closed generation never enters reclaim/spill/retry;
- no production legacy charge is removed.

### PR 5: hash-table cell-block activation

Implementation: `5ae8eca00a`. Integer and string cells plus their descriptor
backing are allocation-accounted through initial allocation, both resize
modes, producer/consumer handoff, and terminal Free. Activated maps do not
install the legacy resize reservation owner.

Scope:

- activate only the integer/string hash-table cell blocks;
- replace the Go `[][]Cell` outer backing store with an owning off-heap
  descriptor buffer whose initial and replacement capacities use the same
  account;
- attach the account to initial allocation, full replacement, segmented
  appended blocks, and consumer-side empty-map growth;
- remove only the matching `hashMapReservationOwner`/`ResizeReservation`
  budget charge;
- retain legacy copied-batch, expression, auxiliary, and spill charges.

Gate:

- int/string initial, no-op, full replacement, segmented reuse, stale plan,
  injected allocation failure, and terminal Free are covered;
- old+new replacement and old+appended-block peaks match live cell allocation
  plus descriptor allocation capacity;
- descriptor replacement rollback is atomic with cell-block publication, and
  no data-scaled Go backing array remains;
- the first-activation metadata-slot formula holds at exact and one-slot-short
  boundaries;
- consumer growth after HashBuild handoff keeps original provenance;
- generation final snapshot reaches zero;
- #25782 high-cardinality no-OOM regression and hash-table performance pass.

### PR 6: copied batches and JoinMap activation

Implementation: `8e4b689f45`. Copied build vectors, GroupSels, dedup scratch,
delete bitmaps, and final JoinMap ownership retain immutable provenance until
the last consumer releases them.

Scope:

- copied build-batch destinations;
- HashmapBuilder-to-JoinMap ownership handoff.

Required behavior:

- replace projected/reconciled batch tokens with physical leases;
- preserve provenance through producer Reset and broadcast consumers;
- let physical Free replace budget-only `SetMemoryRelease` callbacks.

Gate:

- empty/single/large build and both resize modes;
- failed publish, cancellation, multiple consumers, duplicate cleanup;
- old-generation allocation freed after a new generation opens;
- #25782 high-cardinality and #26413 external-table self-join regressions pass
  before activation merges;
- copied-batch/JoinMap site rows become `A` or proved `H`.

### PR 7: expression owner activation

Implementation: `c9ad0ea810`. COL/LIT/PARAM/VAR/VEC/FOLD plus audited CONCAT,
CASE, varchar EQUAL, and integer-to-string/literal string CAST trees use exact
result, bitmap, conversion, and function-scratch allocations. A HashBuild
containing any other expression family is not activated at all; it cannot mix
an exact map owner with a legacy expression estimator.

Scope:

- activate the complete HashBuild-owned expression site closure;
- remove exactly the corresponding
  `expressionVectorPeak`/`expressionTypePeak` hard charge;
- return controlled terminal pressure until a proved retry checkpoint supports
  a smaller-batch retry;
- do not stack exact leases on #26455's operator-held lifetime charge.

Gate:

- every activated expression site is `A` or proved `H`; no data-scaled Go
  allocation remains;
- one-byte-short and real single-value-over-cap diagnostics name actual
  capacity;
- partial growth/evaluation failure publishes no duplicate rows;
- generation final snapshot reaches zero;
- #26454 workload and expression performance regressions pass.

### PR 8 family: spill and runtime-filter closures

Implementation: `e072568998`. Decoded reuse, selected vectors, hash/row IDs,
marshal/coalesce buffers, recursive rebuild, spill disk/FD, pSpool provenance,
and runtime-filter payload publication all have explicit allocation or
resource owners. Optional coalesce and runtime filters degrade without
publishing partial state.

Split into independently safe owner closures:

1. decoded batches and retained reader reuse;
2. scatter/hash/row-ID/codec/coalesce buffers and forward-progress policy;
3. runtime-filter payload transfer and PASS degradation.

Each sub-PR removes only its matching hard reservation. Required gates include
first spill, recursive spill, skew, empty bucket, EOF, failure/cancel at every
I/O/publication edge, minimum progress over cap, message destruction, and final
memory/disk/FD zero. The scatter/codec closure must pass #26174 fulltext INSERT;
the decoded/retained-reader closure must pass #26192 LOAD DATA. A runtime-filter
closure adds its own build/probe and PASS-degradation workload before merging.

### PR 9: unified join pressure controller and remaining legacy deletion

Implementation: `eec23dcdc5`. HashBuild, SpillEngine, HashJoin, DedupJoin, and
RightDedupJoin share typed pressure classification and a monotonic retry guard.
Only memory-capacity failures retry; lifecycle, invariant, disk, and FD
failures are terminal. Unpublished input windows shrink, optional coalescing
is disabled once, and real minimum-unit failure is finite and diagnostic.

Implement across HashBuild, HashJoin, DedupJoin, and RightDedupJoin:

```text
exact capacity rejection
  -> rollback to operation checkpoint
  -> reclaim -> retry
  -> spill/re-spill -> retry
  -> reduce batch -> retry
  -> degrade optional owner
  -> controlled minimum-unit error
```

Gate:

- retry only after usage decreases, spill advances, input shrinks, or optional
  work is disabled;
- partially grown retained buffers and output publication are idempotent;
- no infinite pressure loop;
- sealed/lifecycle errors never retry;
- cancellation/downstream failure is covered in every state;
- remaining multiplier hard gates and duplicate token owners are deleted;
- every site row is `A`, justified `H`, `S`, or `R`.

### PR 10: workload, performance, and cleanup

Local implementation and benchmark harness: `383fc6dce3`. The harness covers
resident int/varchar keys at 32 and 8,192 rows, complete spill scatter,
#26454's expression, copy ownership, lifecycle latency, and concurrent release
storms. Raw commands and medians are in
[`evidence/26459_activation_validation.md`](evidence/26459_activation_validation.md).

This re-runs all incident workloads together and supplies long-run and
comparative confirmation; it is not the first incident-level validation of an
earlier activation.

Required workloads:

- #25782 high-cardinality case;
- #26174 fulltext INSERT;
- #26192 LOAD DATA;
- #26413 Hive external-table self-join;
- #26454 string-expression join;
- TPCH 100G non-spill and TPCH 1T spill.

Required proof:

- no CN OOM/restart and no cap increase/spill-disable workaround;
- account never exceeds cap and every generation returns to zero;
- only a real minimum allocation can produce terminal pressure;
- diagnostics name owner/site and attempted response;
- resident and spill performance are compared separately with profiles;
- concurrent-generation P50/P99, release storm, and high-frequency TP
  allocation results meet the recorded gate;
- temporary migration helpers are removed.

Current validation status:

- the complete affected package matrix passes after rebasing on
  `5b9eeb54ec`;
- the complete affected package matrix, the same matrix under `-race`, a
  20-iteration focused lifecycle/pressure race stress, affected-package vet,
  and `make build` pass at `656e254fe6`;
- local regression tests cover the five incident mechanisms without an
  estimator-only rejection on the activated owner;
- TPCH 100G/1T candidate-versus-main runs remain the remote workload gate and
  must be recorded in the linked evidence before this document moves from
  `implementation validation` to `implemented`.

## 5. Verification and conservation model

Every semantic PR runs build, vet, complete package tests, focused adaptive race
stress, and dependent package tests for its closure. Likely packages are:

```text
pkg/common/mpool
pkg/container/vector
pkg/container/batch
pkg/sql/colexec
pkg/sql/colexec/{hashbuild,hashjoin,dedupjoin,rightdedupjoin,spillutil}
pkg/vm/{message,process}
```

Before direct tests that can reach usearch, build `thirdparties` and use the
repository CGO include/library/rpath environment. Compilation or one successful
SQL run is not completion evidence.

The completed PR 0 reference model must track allocation ID, account ID,
allocator mode, capacity, pool, logical owner, and
unpublished/live/freed/tombstone state. Its required generated operations
include allocation, within-capacity reuse, replacement growth, injected
failure, Free, cross-pool Free, view/copy, handoff, Reset, seal, finalization,
stale ID, operation checkpoint, cancellation, and retry.

The current artifact generates allocation, growth, handoff, and Free, and
separately tests failure injection, cross-pool Free, seal, zero/nonzero
terminal paths, stale generation-tagged slots, bounded registry and allocation
metadata, teardown, open/suspend linearization, view/copy, Reset,
multi-allocation checkpoints, cancellation, rollback, and smaller retry
without duplicate publication.

After every operation:

```text
account.used
  = sum(live accounted allocation capacities)
  + sum(live named scratch)
```

Failed unpublished work leaves allocator/account state unchanged; no allocation
releases twice; sealed generations admit nothing new; final cleanup reaches
zero. Accounted on-heap allocation is rejected. A logical retry may retain
already published reusable capacity but cannot retain partially published
rows. Deterministic CI seeds print on failure; longer randomized runs may run
nightly.

## 6. Resource Accounting integration

Admission and SQL Resource Accounting share allocator facts, not mutable
control state. Each query-CN generation has a stable identity and its
`Compile` attempt coordinator exports exactly one immutable controlled-domain
snapshot:

- cap, exact peak, and final live bytes;
- alloc/grow/free and real pressure counts;
- reclaim/spill/batch-reduction/degrade responses;
- invariant quality.

Initial integration keeps `statement_info.stats[2]` unchanged and exposes the
controlled-domain snapshot in physical-plan/operator diagnostics. Resource
Accounting may mark a missing/inconsistent snapshot and aggregate terminal
facts, but its summaries never feed hard admission. Multiple operators may
reference one generation; their diagnostics must not be summed as separate
physical domains. The controlled-domain and MPool-domain peaks are not declared
equal until allocator mode and owner coverage match.

No per-allocation log or Prometheus series is added. Owner/site values are
bounded enums and counters are emitted at generation/operator completion or
terminal pressure.

## 7. Rollout, rollback, and completion

Rollout:

- rebase every PR on current `main`;
- keep generic primitives dormant until one complete owner closure migrates;
- enable exact accounting and remove the same legacy charge atomically;
- update the issue ledger only after evidence exists.

Rollback reverts an owner's exact enablement, legacy removal, pressure response,
and tests together. Never roll back only the accounting or pressure half. If a
missing owner is found, complete the closure before rollout or revert that
owner; do not add another permanent estimator multiplier.

Done means:

- every site-ledger row is `A`, justified `H`, `S`, or `R`;
- every `H` row has a per-entry cost, maximum-live-count proof, and aggregate
  CN headroom;
- allocation/growth failure is atomic;
- attempt-owned seal/finalize and exactly-once snapshot export are proven;
- nonzero terminal generations retain release provenance without unbounded
  registry growth;
- Reset, Free, broadcast, and cross-generation handoff preserve provenance;
- data-scaled controlled Go allocations have moved off-heap;
- operation-level retry checkpoints prevent partial output replay;
- bounded forward progress is accounted;
- predictions cannot terminally reject SQL;
- no data-scaled HashBuild-owned allocation remains outside coverage;
- the original no-OOM case and all listed false-budget regressions pass;
- performance gates pass with evidence;
- Resource Accounting receives immutable diagnostics without entering the
  admission decision;
- legacy hard estimators and duplicate token owners are deleted.
