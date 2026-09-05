# Named-snapshot index reads and index-cache residency

- Status: Review required
- Issues: [#27941](https://github.com/matrixorigin/matrixone/issues/27941),
  [#27927](https://github.com/matrixorigin/matrixone/issues/27927)
- Implementation: branch `bug_27941`

## 1. Problem and contract

A `{snapshot = ...}` query must read the index as it existed at that timestamp.
Before this work the fulltext/fulltext2 and vector table functions loaded the
index through the shared `VectorIndexCache` under the *current* transaction, so a
historical `MATCH` or top-K returned current-index results — or, when the
current-generation candidates were all inserted after the snapshot, nothing at
all.

Fixing that introduces a second problem. A historical read cannot share the
current-generation cache entry, so each distinct snapshot timestamp becomes a
**separate resident copy of an index**, keyed `<index table>@<physical>-<logical>`.
Nothing bounded how many such copies a CN held.

The contract this design implements:

1. **Historical correctness.** A named-snapshot read resolves and reads the index
   at the snapshot's timestamp *and* under the snapshot's owning tenant.
2. **Isolation from the current generation.** A historical load never serves, nor
   pollutes, the current-generation entry; concurrent same-snapshot readers still
   share one load.
3. **Bounded residency.** Resident index bytes are bounded by an operator-set
   budget, per tenant and CN-wide, without failing ordinary queries.
4. **No silent unbounding.** An unset budget is derived from the machine, and a
   capacity that cannot be read is an *error* rather than an invented number
   (§5.1); a catalog outage keeps the last known cap. Nothing quietly disables
   the bound.
5. **Provenance.** A generation records the base-table version it was built from,
   so "does this index actually cover the snapshot I asked for" is answerable.

## 2. Scope and non-goals

Covers fulltext, fulltext2, hnsw, ivfflat, cagra and ivfpq as cache consumers,
and fulltext2/hnsw/cagra/ivfpq for metadata provenance.

Non-goals:

- **Consuming `build_ts`.** This design records it. Detecting and reporting "this
  generation predates the requested snapshot" is a read-path change left out.
- **Fulltext v1 provenance.** It has no metadata table — its schema creates only
  the `(doc_id, pos, word)` postings table — so there is nowhere to record it
  without inventing one.
- **Fair-share arithmetic.** Budgets are explicit operator numbers, not computed
  splits (see §5.3).
- **Versioning fulltext2/cagra/ivfpq CDC tails.** They write no metadata row at
  all, so there is nothing to carry a version (§6.2).

## 3. Historical reads

The planner already resolves `{snapshot = ...}` into `TableFunction.ScanSnapshot`,
carrying a timestamp and, for an account-level snapshot, the owning tenant.

`SqlProcess.ApplyScanSnapshot` binds **both** halves and returns the effective
timestamp:

- `SnapshotTS` makes the nested index-table SQL run on a transaction cloned at
  that timestamp (`txnForRun`).
- `AccountIDOverride` makes it resolve under the snapshot's account.

Binding only the timestamp is a correctness bug, not an omission: for an
account-level snapshot `planSnapshotFromRecord` sets `Tenant.TenantID` to the
snapshot's account, so a `sys` session reading another account's snapshot would
scan the base table as that account while resolving `__mo_index_secondary_...`
as account 0 — table-not-found, or silently empty results. Every other
`ScanSnapshot` consumer in the compile layer binds the pair; the table functions
now do too, through one helper so no call site can bind half of it.

It binds only when the timestamp is non-empty and strictly older than the current
transaction — the same predicate the compile layer wraps its own clone in, so the
two layers agree on when a read is historical.

The cache key carries the same effective timestamp, which is what keeps a
historical generation from serving or polluting the current one.

## 4. Why bytes, not counts

The first bound was a count of resident snapshot generations, per index and
per CN. That is the wrong unit. A server with a hundred tenants each holding a
small index breaks under any count low enough to bound memory, while any count
high enough to admit them bounds nothing. Index sizes span orders of magnitude;
their number says nothing about the memory at risk.

The bound is therefore a **byte** budget, and it is not snapshot-specific: every
resident index is charged, because a current generation occupies the same memory
as a historical one.

## 5. The governor

### 5.1 Budgets

Two `ScopeGlobal` variables, each defaulting to `0` — which means **"size me from
this machine"**, not "unlimited". An unconfigured arena resolves to a share of
what the CN actually has:

| arena | automatic budget |
|---|---|
| host | 90% of RAM, or of the cgroup limit when it is lower |
| device | 90% of each GPU's total, summed |

**There is no fallback.** When capacity cannot be read, sizing returns an *error*
rather than a number. A budget invented from nothing describes no real machine,
and §5.3 refuses arrivals that exceed the budget — so guessing low silently fails
queries and guessing high silently over-commits. Neither is better than naming
the missing input, and the error says which variable to set.

`limits()` therefore returns an error, and its two callers treat it differently
on purpose: `makeRoom` **refuses** the load, because it cannot decide whether the
arrival fits and admitting it blind would be the guess this removes;
`chargeAndEnforce` **logs and skips**, because the load has already happened and
failing there would not un-spend it. The result is memoized with the value, so a
failing probe does not land on the query path once per miss.

The error is carried **per arena**, and refuses only an arrival that occupies the
arena that could not be sized. A CN whose GPU cannot be queried still knows its
own RAM exactly, so hnsw and fulltext2 keep loading there; only cagra/ivfpq, which
need the number that is missing, are refused. Joining the two errors instead —
with the device cap left at its default, so the failed probe is always consulted —
took the whole CN out of service for host-only indexes, permanently, because the
sizing result is memoized.

**No GPU is not a failure.** `count == 0` means the device arena does not apply,
so it gets no budget and `enforce` skips it — nothing charges device bytes
without a device. Only a GPU that exists and cannot be queried is an error.

Resolution is **per arena and on the SYS value alone**. Gating it on the tenant
as well would let a tenant that sets any value at all leave the CN-wide budget at
`0` for that arena — bypassing the CN limit by naming a bigger one of its own.
The tenant cap is an *additional* bound applied alongside the CN one (§5.2),
never a replacement for it.

So `0` never means "no limit". That matters most for an **upgraded** cluster: the
variable's value is persisted in `mo_mysql_compatibility_mode` at bootstrap, so a
cluster created before this feature keeps a stored `0` forever no matter what the
code default says. Resolving `0` in the governor — rather than only in the
variable's default — is what reaches it, along with an explicit
`set global … = 0` and the sessionless loads (idxcron, internal rebuilds) that
have no resolver at all.

The consequence is that the accounting always runs and the eviction path is
always live, so lowering `SET GLOBAL` later *governs a warm cache* instead of
switching accounting on. With a zero cap the governor used to return before
`enforce()`: nothing charged, nothing enumerated, residency genuinely unbounded
at one resident generation per distinct snapshot timestamp.

| variable | arena |
|---|---|
| `max_index_cache_size` | host RAM |
| `max_gpu_index_cache_size` | device VRAM |

They are separate because a CN has far more RAM than VRAM: one number large
enough to be a sane host budget never binds on the device, and one small enough
to bound VRAM cripples the host cache. The two sums are never added together, and
an eviction in one arena never takes an entry that holds nothing in it — evicting
a host-only index to relieve VRAM pressure frees no VRAM.

### 5.2 Per-tenant and CN-wide

`SET GLOBAL` in MatrixOne is **per account**, not cluster-wide: `GSysVarsMgr` is
keyed by account id and `mo_mysql_compatibility_mode` is a per-account table. So
a tenant's value caps that tenant, and the **SYS account's** value caps the whole
CN. The SYS value cannot be read through the caller's resolver, which resolves
for the calling tenant; it is read from the catalog as the SYS account on a
**fresh context** (`RunSqlAutoCommit` rebinds `TenantIDKey`), memoized for 15s so
`SET GLOBAL` takes effect without a restart.

The memo stamps every attempt, success or failure. Without that, a catalog outage
defeats it entirely: each cache miss re-attempts a 10s-timeout query, twice per
miss, serialized on one mutex. The lock is released across the query on a
*refresh* — every waiter already has a last-known value — but held for the
**first** fetch, where there is none: releasing it there would hand concurrent
misses the zero caps, which read as unlimited, and bypass the governor for the
whole first query at exactly the moment the cache is cold.

Ownership follows the **executing** account, not the calling one. A cross-account
snapshot read runs its index-table SQL as the snapshot's tenant
(`ApplyScanSnapshot` binds both the timestamp and the owning tenant), so the
resident entry is charged to that tenant and governed by *that* tenant's cap. The
caller's session resolver cannot answer for another account, so the owning
tenant's value is read from the catalog and memoized per account. Charging the
caller instead would let a SYS session make tenant data resident under a budget
the tenant never set.

### 5.3 Admission control: reclaim the idle, refuse the rest

**The rule is an overloaded HTTP server.** When a server is at capacity it
returns an error to the *new* request; it does not kill the requests already in
flight to make room. The cache behaves the same way:

1. Reclaim every **idle** entry the budget requires. That is the cache doing its
   job — an idle entry is holding bytes nobody is using.
2. If the arrival still does not fit, **refuse it**. `makeRoom` returns an error,
   the entry is torn down before `Load` allocates anything, and the caller gets
   `index cache is full: …` naming the budget, what is held, what was needed, and
   that nothing idle was left to reclaim.
3. **Never evict a busy entry for an arrival.** A search in flight is a live
   request and it wins.

The check runs *after* the reclaim, so a cache merely full of cold entries admits
the newcomer normally. Only genuine overload — nothing idle left to give —
refuses.

**An arrival that would be the arena's only occupant is always admitted**, however
large: there is nobody to protect, no eviction could have made room, and refusing
would fail a query that a cache with no policy at all would have served. This is
what keeps the budget from changing which workloads are possible — a single index
bigger than the budget still loads; what the budget bounds is how many stay
resident *together*.

That exemption is decided against residency **and arrivals in flight**. A load
that has passed admission is not resident yet — its entry is in the map but not
`STATUS_LOADED` — so on residency alone every member of a concurrent burst reads
an empty arena and every one of them takes the exemption meant for a lone index.
Each arrival therefore registers a *reservation* before it reclaims, and counts
the reservations **ahead of it in line** (an increasing sequence number) as
occupied bytes. Ordering by arrival is what makes the outcome first-come,
first-served rather than mutual refusal: if each counted the other, two loads that
fit one at a time would refuse each other and neither would run. The reservation
is released on every exit from `Load` — success, failure, or panic.

Choosing an idle victim is likewise a **claim, not a check**. The pass takes the
victim's search lock with `TryLock` and holds it through removal and destroy, so a
search arriving a moment after the decision cannot turn the free victim into the
blocking destroy the check existed to avoid. Asking `TryLock`/`Unlock` and
destroying later answered a question about the past — and with no busy fallback
left, the pass had already committed to that victim.

Rule (3) is not only fairness. `evictEntry` destroys synchronously and `Destroy`
takes the victim's write lock, so taking a busy victim parks the cache **miss**
that triggered the eviction behind the very search it interrupted. Preempting a
live query does not even buy the newcomer its memory promptly.

An earlier revision did the opposite on both counts: reclaim fell back to busy
victims once idle ones ran out, and an oversized index was admitted regardless on
the grounds that "refusing would fail ordinary SQL on a cache accounting rule".
The combination is the worst of both — the arrival evicts the warm working set,
then cannot be retained itself, so the warm set is destroyed for an entry that is
discarded. Nobody wins. An error is honest; silent thrashing is not.

The cost of this choice, stated plainly: **a query that used to succeed can now
return an error.** An index larger than the budget is refused rather than served
slowly, which also means the "map a file larger than RAM" capability (§6.1) is
not available to the *cache* — mapping still works, but retaining such an index
does not. That is deliberate. The budget's job is to bound how many indexes stay
hot, and an operator who wants a bigger one raises
`max_index_cache_size` / `max_gpu_index_cache_size`, which the error names.

Victims are coldest-first (`ExpireAt` slides on every search, so it is already an
LRU ordering), with two refinements:

- The entry just loaded is never the victim; its caller is about to use it.
- When the **CN-wide** cap binds, the loading account gives up its own entries
  first and the pass widens only when it has nothing left. Coldest-first alone
  lets a tenant that floods the CN evict a quiet neighbour's older entry before
  its own — the cap held, but the cost of holding it landed on the wrong tenant.

Usage is recomputed from the cache on each pass rather than tracked in a counter,
which removes a whole class of leak bugs, and is re-snapshotted per arena so one
arena's evictions are not double-counted by the next.

### 5.4 When a cap takes effect, and what it logs

Caps are consulted on a miss, and also from the housekeeping ticker. Miss-only
enforcement is not enough on its own: a hot working set renews its TTL
indefinitely, so an operator lowering `max_index_cache_size` on a busy CN would
see nothing shrink until traffic happened to miss. The 15s memo bounds how stale
the *value* is, not when it is next *applied*.

Housekeeping applies the memoized **CN-wide** value only — it has no session, so
it can neither read the catalog nor resolve a tenant's variables, and it does
nothing until a first miss has populated the memo. Per-tenant caps still take
effect at that tenant's next miss.

Per-victim eviction detail is logged at DEBUG, not INFO. Two indexes alternating
under a tight cap evict on every miss, and one INFO line per victim turns a steady
state into a log storm. Whole passes are at DEBUG too; INFO gets at most one line
per arena per 10s, carrying the totals since start rather than that pass alone. A
binding cap is a steady state, not an event, so the condition stays visible while
the volume stays bounded. `EvictionStats()` exposes cumulative entries and bytes,
loses nothing to that sampling, and is the thing to alert on.

### 5.5 Invalidation: the current generation, and everything

`Remove` drops the **current** generation only. It is the append path: every
algorithm's `sync.go` calls it on each CDC/ISCP flush, and an append cannot change
what a named snapshot returns — a snapshot generation is a read at a past
timestamp. Clearing history there threw every snapshot generation away on every
flush, so a workload that both writes and reads snapshots reloaded continuously.

`RemoveAllGenerations` drops the current entry **and** every
`<index table>@<physical>-<logical>` generation of it. That is the DDL path —
CREATE, DROP INDEX, DROP TABLE, DROP DATABASE — where the index table itself is
going away or being rebuilt, so its history is no longer readable. Without it a
drop left every historical generation resident until its TTL expired, pinning VRAM
for the cuVS algorithms and charging bytes for an index that no longer exists;
no exact-key evict matches those keys, and the staleness sweep deliberately skips
them, an immutable generation never being "stale".

## 6. Sizing

### 6.1 `GetIndexSize() (host, device)`

`VectorIndexSearchIf` reports its resident cost split by arena. Per algorithm:

| algorithm | host | device |
|---|---|---|
| hnsw | `nrow × 8 + FileSize` (same before and after load) | 0 |
| fulltext2 | `ndoc × estBytesPerDocHeap` | 0 |
| ivfflat | delegates to its centroid index | delegates |
| cagra / ivfpq | `HostComponentBytes` | `Σ DeviceComponentBytes` |
| brute force | dataset bytes | GPU variant only |

Two figures are measured rather than assumed:

**hnsw costs its allocation PLUS its mapping.** The search path loads with
`View()`, which mmaps the model, and usearch's `memory_usage()` skips the node
and vector bytes for a viewed index. Against usearch's own accounting the cost is
exactly linear in rows and independent of dimension:

| rows | dim | file | `MemoryUsage()` |
|---|---|---|---|
| 5 000 | 32 | 1.4 MB | 41,536 |
| 5 000 | 512 | 11.0 MB | 41,536 |
| 20 000 | 32 | 5.5 MB | 161,536 |
| 20 000 | 512 | 43.9 MB | 161,536 |
| 50 000 | 128 | 33.0 MB | 401,536 |

The deltas are exactly 8.000 bytes per row over a fixed ~1536-byte per-thread
term, so `nrow × 8` predicts `MemoryUsage()` closely — within 0.4% at 100k and
200k rows.

**But that is the allocator, not the cost.** `memory_usage()` counts only what
usearch allocates; the mapping never appears in it. Measured on a viewed index
(100k rows, dim 128, 63 MB file):

| quantity | measured | share of file |
|---|---|---|
| usearch `MemoryUsage()` | 0.76 MB | 1.2% |
| page tables (`VmPTE` growth) | 0.14 MB | 0.22% |
| **resident pages (`VmRSS` growth)** | **67.25 MB** | **107%** |

Charging the allocation alone under-states the entry by ~88×. The mapping is
nominally reclaimable page cache, but reclaiming it makes the next search fault
the graph back off disk — "reclaimable" in the sense a buffer pool is, which is
not a reason to leave it out of a memory budget.

So the charge is **`nrow × 8 + FileSize`**, the same before and after load. Page
tables are a third, far smaller cost — and per *address space*, not per thread,
so concurrency does not multiply them — which `FileSize` covers by a wide margin.
Using one formula on both paths makes the pre-load reservation equal the
post-load charge; a reservation that under-states the charge lets a load blow a
budget the reclaim pass just declared satisfied, and that equality is exactly
what the §5.3 admission decision rests on.

`FileSize` slightly *under*-charges (107% measured against 100% charged; the
remainder is search scratch). Left as is: `FileSize` is exactly known from the
metadata row, and the gap is far smaller than the headroom any budget leaves.

What this gives up: `mmap` decouples file size from memory requirement — a 42 GiB
file maps fine on a 21 GB host, and pages arrive only as touched, which is why
loading is lazy and a lightly-searched index really is cheaper than its file.
Charging `FileSize` forfeits that for *cached* indexes, since the budget assumes
the whole mapping. Charging measured residency instead (`mincore`/`smaps`) would
keep it, at the cost of per-mapping RSS accounting; not attempted here.

**cagra/ivfpq use the load gate's own measurement.** `cuvs.MeasureTar` already
splits a packed artifact into device- and host-resident components; the tar's
total is the wrong number for either budget.

### 6.2 Provenance: `nrow` and `build_ts`

The metadata tables gained two columns, appended last so `SELECT *` keeps
existing positions and readers guard on `len(bat.Vecs)`.

`build_ts` is the transaction `SnapshotTS` the content was built from. It is
deliberately distinct from the existing `timestamp` column, which is
`time.Now()` on the CN: a skewable wall clock that only orders generations and
cannot be compared with a snapshot's timestamp.

It is recorded **wherever the covered version is known**, which is both halves:

- A **build** reads the source table inside its own transaction, so that
  transaction's `SnapshotTS` is exactly the version captured.
- A **CDC sync** applies a change range and rewrites the generation, so the
  version it now covers is that range's upper bound — `DataRetriever.GetToTS()`,
  the same `status.To` that `UpdateWatermark` persists. hnsw's consumer supplies
  it through `HnswSync.SetBuildTS`.

Neither records the writing transaction's own `SnapshotTS`, and for CDC that is a
soundness point rather than a stylistic one: the sync transaction reads at some
`S >= To`, and `(To, S]` can hold changes committed after the range was collected
but never applied. Recording `S` would claim coverage the generation does not
have — worse than recording nothing, because `build_ts` exists precisely to be
trusted by a coverage check. `To` claims exactly what was applied.

`0` remains the unknown sentinel: a generation written before the column existed,
or by a direct non-ISCP caller with no iteration to name.

**Physical only, matching how MatrixOne stores a snapshot.** The column is
`bigint` holding `TS.Physical()`. `LogicalTime` is not stored and cannot be
recovered from it — the HLC increments logical only while the physical clock has
not advanced and resets it to 0 when it does (`HLCClock.now`), so the mapping is
not injective.

Nothing is lost for the comparison this column exists for. `mo_snapshots.ts` is
itself a `bigint`, and every reconstruction of a named snapshot's timestamp is
`timestamp.Timestamp{PhysicalTime: record.ts}` with logical left at 0. A named
snapshot is therefore physical-only by construction, so `build_ts` carries exactly
the fidelity MatrixOne stores a snapshot at, and the two are directly comparable.

The equal-physical case resolves favourably rather than ambiguously: with a
snapshot at `(P, 0)` and a generation at `(P, L>=0)`, `(P, 0) <= (P, L)` always
holds, so an ordinary `snapshot_ts <= build_ts` is exact for named snapshots.

Only a comparison against some OTHER timestamp — one carrying a non-zero logical,
which a named snapshot never does — would need to be strict to stay sound. That
case does not arise on this path.

fulltext2, cagra and ivfpq write no metadata row for a CDC tail at all — tails are
storage chunks — so for them CDC leaves the base generation's `build_ts` as it
was, and the tail itself is unversioned. Giving those tails a version would mean
writing metadata rows for them, which is a larger change than this design makes.

`nrow` gives hnsw a pre-load estimate (§6.1) that no other source provides at the
right granularity.

**What build_ts buys.** A base generation is built by reading the source table
inside a transaction at `SnapshotTS = X`, so its content is exactly the base data
as of X. A snapshot taken **at X** therefore yields a base table and an index that
provably agree — the index is covered by that snapshot in the strong sense, and
`build_ts` is what makes X recoverable after the fact.

For any later timestamp Y the agreement depends on what CDC appended in `(X, Y]`,
and a CDC-appended generation records no coverage, so it is not checkable. In
other words `build_ts` makes the BASE half of an index verifiable against a
snapshot; the CDC half is not, and closing that gap means plumbing the ISCP
iteration's upper bound through `DataRetriever` so a tail can record what it
covers.

This is the mechanism a future read-path check would rest on: given a request at
Y, compare Y against the generations resident at Y and report — or refuse — when
the index demonstrably predates the data the snapshot exposes.

**Upgrade.** Readers tolerating the old four-column shape is not sufficient:
a CN running this code *writes* six-value rows, so the first CDC sync or rebuild
against an index created earlier fails on a column-count mismatch. Because each
metadata table is created per index at `CREATE INDEX`, and `REINDEX` rewrites its
rows rather than the table, such an index would stay broken for writes forever.
The v4_0_6 tenant upgrade therefore alters every metadata table an account owns.
No fixed `UpgSql` can express DDL over a runtime-determined set of tables, so it
uses the existing escape hatch — a plain function called from
`HandleTenantUpgrade`, as `upgradeLegacyForeignKeyMetadata` does — leaving the
shared `UpgradeEntry` framework untouched.

## 7. Reclaiming before the load, not after

Charging only after `Load` means peak residency exceeds the budget by one whole
index — gigabytes for a cuVS artifact. It also runs *after* each algorithm's own
memory gate, and those gates sample **free** memory, so entries the governor is
about to reclaim read as memory that is gone and can veto a load that fits.

`VectorIndexSearchIf` therefore splits into `Preload` and `Load`:

```
Preload   measure: metadata, artifacts, cost      (entry lock held)
makeRoom  reclaim for what is about to arrive     (NO lock held)
Load      materialize                             (entry lock held)
```

The reclaim happening between two separate locked sections is the reason for
splitting the interface rather than exposing a `MakeRoom` the algorithms call
from inside `Load`: `Algo.Load` runs under the entry's write lock, so reclaiming
there would run the governor's catalog read under a lock held across an entire
index load, and would block on a victim's lock while holding the loader's.

cagra/ivfpq do the real split — fetch, `MeasureTar`, and the device gate all move
to `Preload`. The gate stays interleaved with the fetch loop so a doomed index is
still refused as soon as the running total says so, rather than after downloading
the remaining gigabytes. hnsw moves its metadata read; fulltext2 moves its doc
count. ivfflat is a no-op: its centroid size is not knowable before loading them.

## 8. Concurrency

`Search`/`SearchInto` re-wrap the **same caller-supplied algorithm** on every
retry attempt. A retry could therefore call into an algorithm that an evicting
goroutine was still inside `Destroy` on — two wrappers, two mutexes, one object.
Each wrapper now carries a `destroyed` channel closed when its teardown finishes,
and a retry that reuses the algorithm waits on it first.

That wait cannot hang: an entry becomes unreachable only through a
`CompareAndDelete` immediately followed by a destroy, at every removal site. It
is taken **only when a teardown was actually claimed** — `ErrInvalidState` is not
exclusive to eviction, and an internal SQL raising its own would otherwise leave
nobody to close the channel.

Relatedly, the governor reads sizes from atomics published under the entry lock,
never by calling into the algorithm, which a concurrent eviction may be tearing
down.

## 9. Change inventory

The branch carries two features that share a cause: historical reads created the
residency class that the governor bounds.

| theme | commits |
|---|---|
| Named-snapshot reads (§3) | fulltext, fulltext2, hnsw, cagra, ivfpq + tests |
| Snapshot tenant binding (§3) | `fix(snapshot)` |
| Byte governor (§4–5) | `feat(veccache)` ×4, SYS-read fixes |
| Sizing (§6.1) | `GetIndexSize`, hnsw `MemoryUsage`, cagra/ivfpq arenas |
| Provenance + upgrade (§6.2) | `feat(index)` |
| Preload/Load (§7) | `feat(veccache)`, `feat(cagra,ivfpq)` |
| Concurrency (§8) | `fix(veccache)` ×2 |
| Spill location | hnsw/ISCP LOCAL fileservice |

If the review prefers smaller units, the natural cut is **§3 (historical reads)**
as one PR and **§4–8 (residency)** as a second: the former is the bug fix, the
latter is what makes it safe to run. They are presented together because merging
§3 alone ships the unbounded-residency problem it creates.

## 10. Validation

- Unit and race: focused adaptive `-race -count=N` on the concurrency paths plus
  a full owning-package race run; a stress test interleaves loads with eviction.
- GPU: cagra, ivfpq, brute force and cache suites on real hardware, including the
  tar-ownership and early-abort tests that cover the `Preload` restructure.
- Upgrade: a real embedded-cluster test drives the public
  `HandleTenantUpgrade` — it builds a legacy-shaped metadata table, asserts the
  current-shape write **fails**, runs the upgrade, and requires the write to
  succeed and a second run to be a no-op.
- BVT: named-snapshot cases for fulltext, fulltext2, hnsw, cagra, ivfpq; a case
  for the two variables; `cases/vector` (2034) and `cases/fulltext2` (701) green.

### 10.1 Both build flavours

The device arena only exists under `//go:build gpu`, so a green run of one
flavour says nothing about the other. Both are required:

| flavour | result |
|---|---|
| `MO_CL_CUDA=1` | 71 packages, `-race` clean on `cache` and `hnsw` |
| `MO_CL_CUDA=0` | 68 packages |

The CPU run is not ceremonial — it caught a real defect: a test asserting a
positive device budget, where a CPU build correctly budgets **0** because there
is no device arena to bound. Switching flavours also requires clearing
`thirdparties/_jemalloc_build` and running `make clean` in `cgo/`, or the stale
objects from the other flavour fail the link.

### 10.2 Index build time is not affected

The branch touches load and search, not build, and a paired run on the same box
confirms it. Fresh `mo-data`, the same 1M wiki_all f32 CSVs, the same
`cagra_1M_local_f32` cell, the CN's fileservice memory cache at 8 GB:

| run | this branch | `main` (362b1fba26) |
|---|---|---|
| import 1M rows | 132 s | 244 s |
| CREATE INDEX, straight after import | 308 s | 462 s |
| CREATE INDEX, cluster settled | 195 s | 361 s |

The branch is *faster* in both pairings, so there is nothing here to attribute to
it. What actually drives the number is visible in the build's own log line:

```
CAGRA create: 6112 MB host available ... -> 1556093 rows fit   -> 1 sub-index   195 s
CAGRA create: 1710 MB host available ... ->  435520 rows fit   -> 3 sub-indexes 308 s
CAGRA create: 3475 MB host available ... ->  884933 rows fit   -> 2 sub-indexes 361 s (main)
                                          ->  307047 rows fit  -> 4 sub-indexes 462 s (main)
```

CAGRA sizes its sub-indexes from **free host RAM sampled when the build starts**,
so a run that begins under memory pressure rotates into more of them and pays for
each one's build, save and tar. That is the same mechanism on both branches, and
it is why a single build sample on this box is worth little.

A phase breakdown of the 195 s single-sub-index run, from the service log:

| phase | time |
|---|---|
| plan and start | 15 s |
| read source + GPU build | 76 s |
| `gpu_cagra_save_dir` | 7 s |
| tar the 3.3 GB `index.bin` | 22 s |
| 27 SQLs recording the artifact | 68 s |

Over half the wall clock is persisting the index, none of it CAGRA compute. The
3.3 GB is inherent to an f32 cell: 1M x 768 x 4 bytes of dataset plus the graph.

A Go CPU profile across the first build shows the other reason a post-import
sample reads high: TAE's merge of the freshly imported table runs concurrently,
`mergesort.(*merger[string]).merge` taking 25.8% of samples against
`libcuvs.so`'s 21.6%.

### 10.3 Real-GPU run

Recorded here because these branches are GPU-only: green CPU CI never executes
them, and the design names CAGRA/IVF-PQ snapshot, admission order and allocation
refusal as pre-merge requirements. Hardware: **NVIDIA RTX 5070 Laptop, 8151 MiB**,
single device, `MO_CL_CUDA=1`. Run with `-g -n`.

| case | total | passed |
|---|---|---|
| `vector_gpu_index_cache_size` (device arena; 1 MiB cap, three CAGRA indexes) | 36 | 36 |
| `vector_cagra_snapshot` | 21 | 21 |
| `vector_ivfpq_snapshot` | 25 | 25 |
| `vector_gpu_negative` (allocation refusal) | 42 | 42 |
| `vector_gpu_edge` | 15 | 15 |
| `vector_cagra_replicated` (simulated placement) | 18 | 18 |
| `vector_cagra_sharded` (simulated placement) | 30 | 30 |

VRAM sampled across the run. This is the part that distinguishes a real device
path from a CPU fallback — a fallback is a flat line:

```
1051 MiB  baseline
1180 MiB  three CAGRA indexes under the 1 MiB device cap
1324 MiB  peak
```

**Why `-n`.** mo-tester compares result-set *metadata* as well as values unless
`-n` is given. Without it, `vector_cagra_replicated` and `vector_cagra_sharded`
report 16/18 and 28/30 — and every one of those failures is a column-**length**
mismatch on `show create table` / `mo_catalog.mo_indexes`, never a value:
`RSRow` mismatches are zero in both, and the committed `.result` files carry
`[12,-1,0]` "unknown length" entries where this tester reports real lengths. It
is a `.result`-generation environment difference, so `-n` is the correct
comparison here, and the values agreeing at 18/18 and 30/30 is the proof.

**Not covered by this hardware**, and not claimed: multi-device placement is
exercised in *simulated* placement only, since this box has one GPU, and an 8 GB
card cannot drive the multi-GiB evict-then-readmit sequence a datacentre card
would. Raw tester logs are not committed — they are large and machine-specific.
