# Named-snapshot index reads and index-cache residency

- Status: **Design approved** 2026-09-07 (Eric), re-approved 2026-09-08 (Eric)
  for the two decisions added since — implementation complete
- Issues: [#27941](https://github.com/matrixorigin/matrixone/issues/27941),
  [#27927](https://github.com/matrixorigin/matrixone/issues/27927)
- Implementation: branch `bug_27941`

## 0. Design decisions and sign-off

This document is a contract, not a summary of a merged change: green CI and GPU
evidence show the implementation matches it, not that the contract is the right
one. These are the resource and lifecycle decisions that outlive this PR, and the
sign-off they carry. Implementation conformance is reviewed against them.

| decision | where | approved by / date |
|---|---|---|
| Admission **refuses** a load under overload rather than preempting | §5.3 | Eric — 2026-09-07 |
| The **only-occupant exception**: no index is ever unloadable | §5.3 | Eric — 2026-09-07 |
| Capacity **derivation** (90% of RAM/VRAM, sentinel handling, no fallback) | §5.1 | Eric — 2026-09-07 |
| Isolation and **eviction ownership** (per-tenant vs CN-wide, coldest-first, pay-first) | §5.2, §5.3 | Eric — 2026-09-07 |
| **Migration**: widening every index metadata table, and the rolling-upgrade contract | §7 | Eric — 2026-09-07 |
| **One metadata row per CDC tail frame** for fulltext2, cagra and ivfpq — reverses the original non-goal in §2, and tail sizing now depends on those rows | §7.1, §14 | Eric — 2026-09-08 |
| **CREATE-time capability gate**: below `MOProtocolVersion 57` a metadata table is born in the legacy shape, so an index created mid-rollout carries no provenance until something widens it | §12 | Eric — 2026-09-08 |

A later change to any of these rows is a change to the contract, not an
implementation detail: it needs the row re-approved, not just the code updated.
The last two rows are exactly that case, and were re-approved on 2026-09-08: the
first reverses the documented non-goal in §2, the second adds a rolling-upgrade
window whose accepted consequence is that an index created mid-rollout carries no
provenance until something widens it.

## 1. Problem and contract

A `{snapshot = ...}` query is asking for the index **as it existed at that
timestamp**. The fulltext/fulltext2 and vector table functions instead loaded the
index through the shared `VectorIndexCache` under the *current* transaction, so a
historical `MATCH` or top-K returned current-index results — or, when every
current-generation candidate had been inserted after the snapshot, nothing at
all.

Fixing that creates a second problem. A historical read cannot share the
current-generation cache entry, so **each distinct snapshot timestamp becomes a
separate resident copy of an index**, keyed `<index table>@<physical>-<logical>`.
Nothing bounded how many copies a CN held.

The contract this design implements:

1. **Historical correctness.** A named-snapshot read resolves and reads the index
   at the snapshot's timestamp *and* under the snapshot's owning tenant.
2. **Isolation from the current generation.** A historical load never serves, nor
   pollutes, the current-generation entry; concurrent same-snapshot readers still
   share one load.
3. **Bounded residency.** Resident index bytes are bounded by a budget — set by
   an operator, or derived from the machine — per tenant and CN-wide.
4. **No silent unbounding.** An unset budget is derived; a capacity that cannot be
   read, or is not a capacity at all, is an *error* rather than an invented
   number; a catalog outage keeps the last known cap. Nothing quietly disables the
   bound.
5. **No index becomes unloadable.** The budget bounds how many indexes stay
   resident *together*, never whether a given one can be loaded (§5.3).
6. **Provenance.** A generation records the base-table version it was built from,
   so "does this index actually cover the snapshot I asked for" is answerable.

## 2. Scope and non-goals

Covers fulltext, fulltext2, hnsw, ivfflat, cagra and ivfpq as cache consumers,
and fulltext2/hnsw/cagra/ivfpq for metadata provenance.

Non-goals:

- **Consuming `build_ts`.** This design records it. Detecting and reporting "this
  generation predates the requested snapshot" is a read-path change left out.
- **Fulltext v1 provenance.** It has no metadata table — its schema creates only
  the `(doc_id, pos, word)` postings table — so there is nowhere to record it.
- **Fair-share arithmetic.** Budgets are per-tenant and CN-wide bounds, not
  computed splits between tenants.
- ~~**Versioning fulltext2/cagra/ivfpq CDC tails.**~~ **Superseded.** This was a
  non-goal in the first revision, on the grounds that those algorithms wrote no
  metadata row for a tail. They now write ONE ROW PER TAIL FRAME, so the tail
  carries both its size and its version; see §7.1. Sizing depends on those rows,
  so this moved into scope rather than remaining an omission.
- **Charging measured residency instead of file size** for mmapped indexes (§6).

## 3. Historical reads

The planner already resolves `{snapshot = ...}` into `TableFunction.ScanSnapshot`,
carrying a timestamp and, for an account-level snapshot, the owning tenant.

`SqlProcess.ApplyScanSnapshot` binds **both** halves and returns the effective
timestamp:

- `SnapshotTS` makes the nested index-table SQL run on a transaction cloned at
  that timestamp (`txnForRun`).
- `SnapshotAccountID` makes it resolve under the snapshot's account.

Binding only the timestamp is a correctness bug, not an omission: for an
account-level snapshot `planSnapshotFromRecord` sets `Tenant.TenantID` to the
snapshot's account, so a `sys` session reading another account's snapshot would
scan the base table as that account while resolving `__mo_index_secondary_...` as
account 0 — table-not-found, or silently empty results. Every other
`ScanSnapshot` consumer in the compile layer binds the pair; the table functions
now do too, through one helper so no call site can bind half of it.

It binds only when the timestamp is non-empty and strictly older than the current
transaction — the same predicate the compile layer wraps its own clone in, so the
two layers agree on when a read is historical.

**The publisher outranks the snapshot tenant, by construction.** A publication
read already executes as the publisher (`getCompileTableScanDataSourceTxn` binds
`ScanSnapshot.Tenant`, then lets `PubInfo` override it), so identity resolution
is ordered rather than left to whichever field was written last:
`resolveAccountID()` returns `AccountIDOverride` when set and `SnapshotAccountID`
otherwise, and all four consumers route through it.

The cache key carries the same effective timestamp, which is what keeps a
historical generation from serving or polluting the current one.

## 4. Why bytes, not counts

The first bound was a count of resident snapshot generations, per index and per
CN. That is the wrong unit. A server with a hundred tenants each holding a small
index breaks under any count low enough to bound memory, while any count high
enough to admit them bounds nothing. Index sizes span orders of magnitude; their
number says nothing about the memory at risk.

The bound is therefore a **byte** budget, and it is not snapshot-specific: every
resident index is charged, because a current generation occupies the same memory
as a historical one.

## 5. The governor

`VectorIndexGovernor` is a type of its own, not a set of fields on the cache. The
cache is a map of loaded indexes with a TTL; the governor is a policy with its own
state — memoized catalog reads, derived machine capacity, in-flight admissions,
eviction counters. They meet in exactly two places: the governor reads the cache's
map to see what is resident, and calls back into it to evict.

### 5.1 Budgets, and where they come from

Two `ScopeGlobal` variables, one per arena:

| variable | arena |
|---|---|
| `max_index_cache_size` | host RAM |
| `max_gpu_index_cache_size` | device VRAM |

They are separate because a CN has far more RAM than VRAM: one number large
enough to be a sane host budget never binds on the device, and one small enough
to bound VRAM cripples the host cache. The two sums are never added together, and
an eviction in one arena never takes an entry that holds nothing in it — evicting
a host-only index to relieve VRAM pressure frees no VRAM.

**Both default to `0`, which means "size me from this machine", not "unlimited".**

| arena | derived budget |
|---|---|
| host | 90% of RAM, or of the cgroup limit when it is lower |
| device | 90% of each GPU's total, summed |

`0` never means "no limit". That matters most for an **upgraded** cluster: the
value is persisted in `mo_mysql_compatibility_mode` at bootstrap, so a cluster
created before this feature keeps a stored `0` forever no matter what the code
default says. Resolving `0` in the governor — rather than only in the variable's
default — is what reaches it, along with an explicit `set global … = 0` and the
sessionless loads (idxcron, internal rebuilds) that have no resolver at all.

An earlier revision advertised fixed ceilings (64 TiB host, 1440 GiB device) as
the defaults. Being non-zero they took priority over the derived budget, so on a
bootstrapped cluster the machine sizing never applied at all, and an operator
reading the variable saw a number that described no machine.

**There is no fallback.** When capacity cannot be read, sizing returns an *error*
rather than a number. A budget invented from nothing describes no real machine,
and §5.3 refuses arrivals that exceed the budget — so guessing low silently fails
queries and guessing high silently over-commits. The error names the variable to
set.

**A sentinel is not a capacity.** A container on cgroup v1 with no memory limit
puts `PAGE_COUNTER_MAX` in `memory.limit_in_bytes` (some kernels report
`LONG_MAX`), and `refreshQuotaConfig` stored that raw value as the machine total.
It parses as a perfectly good integer, so 90% of it is an ~8 EB "budget" that
bounds nothing — the cache would be accounted, evictable, and unlimited in fact,
which is the original OOM risk restored under a different name.

`system.NormalizeMemoryCapacity` maps such a reading to "no finite capacity
discovered", and both sources are normalised through it, because a caller
combining `MemoryTotal` with `CgroupMemoryLimit` must treat them the same way.
Crucially the container path then falls back to the **host's** physical memory
rather than to nothing: an unlimited cgroup means the container may use the
machine, so that is the capacity to size from. Refusing to size there would be a
different failure with the same symptom — every host-arena load rejected on a
perfectly healthy CN.

**The machine can be resized under a running CN.** `pkg/common/system` watches
`memory.max` precisely because pods are scaled vertically, so a budget derived
once at the first cache miss would keep sizing for the capacity the CN started
with — scaling a pod from 32 GiB to 8 GiB would leave a ~28.8 GiB budget on an
8 GiB pod. Housekeeping re-derives the host budget whenever the capacity it was
derived from has changed, and applies it to the warm cache (§5.5). The device
budget is not re-derived: GPUs are not added to or removed from a running CN, and
the probe is a CUDA call.

**No GPU is not a failure.** `count == 0` means the device arena does not apply,
so it gets no budget and `enforce` skips it — nothing charges device bytes
without a device. Only a GPU that exists and cannot be queried is an error.

**Sizing errors are per arena.** `limits()` carries one error per arena, and
admission consults only the arenas an arrival actually occupies. A CN whose CUDA
probe fails still knows its own RAM exactly, so hnsw and fulltext2 keep loading
there; only cagra/ivfpq, which need the number that is missing, are refused.
Joining the two errors instead — with the device cap left at its default, so the
failed probe is always consulted — took the whole CN out of service for host-only
indexes, permanently, because the sizing result is memoized.

Resolution is **on the SYS value alone**. Gating it on the tenant as well would
let a tenant that sets any value at all leave the CN-wide budget at `0` for that
arena — bypassing the CN limit by naming a bigger one of its own. The tenant cap
is an *additional* bound applied alongside the CN one (§5.2), never a replacement.

### 5.2 Whose bytes: per-tenant and CN-wide

`SET GLOBAL` in MatrixOne is **per account**, not cluster-wide: `GSysVarsMgr` is
keyed by account id and `mo_mysql_compatibility_mode` is a per-account table. So
a tenant's value caps that tenant, and the **SYS account's** value caps the whole
CN. The SYS value cannot be read through the caller's resolver, which resolves
for the calling tenant; it is read from the catalog as the SYS account on a
**fresh context** (`RunSqlAutoCommit` rebinds `TenantIDKey`), and memoized.

A tenant reading its OWN cap is answered by the session resolver, which is always
current, so `SET GLOBAL` on a tenant takes effect at that tenant's very next miss.
The memo covers the two reads a session cannot answer — the SYS account's, and
another tenant's on its behalf during a cross-account snapshot read.

The tenant value **is** memoized all the same, because housekeeping needs it to
reach a warm cache: a miss writes the resolver's answer into `acctLimits` on its
way past. What matters is the direction. The memo is written *from* the
authoritative read and never consulted *instead of* it, so a stale entry cannot
let a tenant keep loading against a cap it has already lowered.

That also bounds how late a changed tenant cap can land, without needing the TTL
to be short. The only thing that keeps the memo fresh is a miss — and a miss
enforces the resolver's current value itself. So a changed cap is applied at the
tenant's next miss, or at a housekeeping tick, **whichever comes first**, and
never later than one tick. A short memo TTL would not improve that; it would only
add catalog reads.

The memo stamps every attempt, success or failure. Without that, a catalog outage
defeats it entirely: each cache miss re-attempts a 10s-timeout query, twice per
miss, serialized on one mutex. The lock is released across the query on a
*refresh* — every waiter already has a last-known value — but held for the
**first** fetch, where there is none: releasing it there would hand concurrent
misses the zero caps, which read as unlimited, and bypass the governor for the
whole first query at exactly the moment the cache is cold. A failed read keeps
the last known value rather than falling open.

Ownership follows the **executing** account, not the calling one. A cross-account
snapshot read runs its index-table SQL as the snapshot's tenant, so the resident
entry is charged to that tenant and governed by *that* tenant's cap. The caller's
session resolver cannot answer for another account, so the owning tenant's value
is read from the catalog and memoized per account. Charging the caller instead
would let a SYS session make tenant data resident under a budget the tenant never
set.

### 5.3 Admission: reclaim the idle, refuse the rest

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

Rule (3) is not only fairness. Eviction destroys synchronously and destroy takes
the victim's write lock, so taking a busy victim parks the cache **miss** that
triggered the eviction behind the very search it interrupted. Preempting a live
query does not even buy the newcomer its memory promptly.

An earlier revision did the opposite on both counts: reclaim fell back to busy
victims once idle ones ran out, and an oversized index was admitted regardless.
The combination is the worst of both — the arrival evicts the warm working set,
then cannot be retained itself, so the warm set is destroyed for an entry that is
discarded.

#### What is guaranteed, and what it costs

**An arrival that would be the arena's only occupant is always admitted**,
however large. There is nobody to protect, no eviction could have made room, and
refusing would fail a query that a cache with no policy at all would have served.

| situation | outcome |
|---|---|
| arrival is the arena's only occupant, any size | **admitted** |
| arrival does not fit, something idle can be reclaimed | reclaim, then **admit** |
| arrival does not fit, only BUSY entries remain | **refused**, error names the budget |

So the cost, stated plainly: **a query that used to succeed can now return an
error** — but only ever when somebody else is already being served. The budget
bounds how many indexes stay resident *together*, never whether a given index can
be loaded. That also keeps the "map a file larger than RAM" property (§6)
available: such an index still loads, it simply cannot share the arena with a busy
neighbour. An operator who wants more of them resident raises the variable the
refusal names.

#### Concurrent arrivals must see each other

The sole-occupant exemption is decided against residency **and arrivals in
flight**. A load that has passed admission is not resident yet — its entry is in
the map but not `STATUS_LOADED` — so on residency alone every member of a
concurrent burst reads an empty arena and every one of them takes the exemption
meant for a lone index. N indexes then land on a budget sized for one.

Each arrival therefore registers a **reservation** before it reclaims, and counts
the reservations *ahead of it in line* (an increasing sequence number) as occupied
bytes. Ordering by arrival is what makes the outcome first-come-first-served
rather than mutual refusal: if each counted the other, two loads that fit one at a
time would refuse each other and neither would run. The reservation is released on
every exit from `Load` — success, failure, or panic.

#### Admission needs a size before the allocation

`makeRoom` returns immediately for an arrival that reports `(0,0)` — there is
nothing to account for — and it returns *before* it reserves. So an algorithm
whose `Preload` was a no-op and whose `GetIndexSize` only answered after `Load`
passed straight through admission, and a burst of its cold misses each allocated
in full. Post-load charging cannot bound that: by then the memory is spent.

ivfflat was that shape. Its estimate now comes from the configuration, which names
every factor: `Lists × Dimensions` elements of the centroid element type, plus the
slice header `GoBruteForceIndex` keeps per centroid row. It is charged to the arena
the load will actually use — the build-tagged `NewBruteForceIndex` dispatch sends
float32 centroids to cuVS only when the GPU build is present and effective GPU mode
is on, so the same estimate becomes device bytes there and host bytes otherwise.
The products are overflow-checked rather than wrapped, and the exact figure replaces
the estimate at `chargeAndEnforce`.

#### Choosing a victim is a claim, not a check

The reclaim pass takes the victim's search lock with `TryLock` and **holds it**
through removal and destroy. Asking `TryLock`/`Unlock` and destroying later
answered a question about the past: a search starting in that window turned the
"free" victim into exactly the blocking destroy the check existed to avoid, and
with no busy fallback left the pass had already committed to that victim.

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

### 5.4 Reclaiming before the load, not after

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
refused as soon as the running total says so, rather than after downloading the
remaining gigabytes. hnsw moves its metadata read, fulltext2 its doc count, and
ivfflat publishes the configuration-derived estimate above.

### 5.5 When a cap takes effect

Caps are consulted on a miss, and also from the housekeeping ticker. Miss-only
enforcement is not enough on its own: a hot working set renews its TTL
indefinitely, so an operator lowering a cap on a busy CN would see nothing shrink
until traffic happened to miss.

Housekeeping **refreshes** the value (`refreshMemoizedSysLimit`,
`refreshMemoizedAccountLimits`); it does not merely reuse the last one a miss left
behind. Reusing was the same hole one layer up: `limits()` is reached only on
a MISS, so a hot cache never refreshes the memo and a lowered `SET GLOBAL` would
not be applied for as long as the queries kept hitting — not within the window the
memo is meant to bound. The refresh is TTL-gated exactly as a miss's would be, and
needs no session: the CN uuid is remembered from an earlier read, and the caps are
catalog rows.

**Per-tenant caps take effect the same way.** Housekeeping cannot resolve a
tenant's session variables, but it does not need to — a tenant's cap is a row it
can read by account id. Only accounts that actually hold resident bytes are
consulted, so this costs one catalog read per resident tenant per TTL window, and
nothing on a cache holding one tenant's indexes.

Housekeeping also re-derives the automatic host budget when the machine's capacity
has changed (§5.1) — applying a reduced automatic budget is the same job as
applying a reduced operator one.

**Cadence and refresh set, precisely.** The housekeeping ticker fires every
`VectorIndexCacheTTL/2`; each tick dispatches the cap pass to its own goroutine,
single-flighted, so a tick is dropped rather than queued while one is running.
The memo is sized from that tick rather than set independently: `sysLimitTTL` is
`VectorIndexCacheTTL/4` (75s), comfortably under the 2.5-minute tick, so every pass
refreshes without the two racing at the boundary. A memo shorter than the tick
would be re-read on a cadence nothing acts on; one longer would make a pass find
its own value still fresh and skip the refresh it exists to perform.

The set it refreshes is derived from **current residency**, not from history:

- the CN-wide row, once per TTL window;
- one row per account that currently holds resident bytes;
- every other `acctLimits` entry is **pruned**, not refreshed.

That pruning is the load-bearing part. The memo gains an entry for every account
that has ever loaded an index on this CN, and nothing removed them, so refreshing
"every account seen" meant one catalog read per dead tenant, forever. An account
holding nothing has nothing this pass could evict, and its memo costs nothing to
rebuild at its next miss.

The whole pass also shares **one** deadline rather than one per read: per-read
timeouts bound each call but not their sum, and the sum is what the caller waits
for. When the budget is spent the remaining accounts keep their last known caps
and are revisited on the next tick — the same guarantee a failed read already
gives them.

**None of it runs on the lifecycle goroutine.** That goroutine also serves TTL
eviction, the lifecycle hooks and `Stop`/SIGTERM, and a catalog read is exactly
the kind of work that turns into a timeout when a cluster is unwell. Shutdown must
not queue behind a catalog.

Neither the SYS nor the tenant refresh can do anything until a first miss has run:
before that there is no CN uuid to query with, and nothing is resident to enforce
against.

### 5.6 Invalidation: the current generation, and everything

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
for the cuVS algorithms and charging bytes for an index that no longer exists: no
exact-key evict matches those keys, and the staleness sweep deliberately skips
them, an immutable generation never being "stale".

### 5.7 What it reports

Per-victim eviction detail is at DEBUG, and whole passes are too. INFO gets at
most one line per arena per 10s, carrying the totals since start rather than that
pass alone: a binding cap evicts on every miss — two indexes alternating under a
tight budget do it forever — so a line per pass turns an ordinary steady state
into a log storm that buries whatever else is being diagnosed.
`EvictionStats()` exposes cumulative entries and bytes, loses nothing to that
sampling, and is the thing to alert on.

## 6. Sizing: `GetIndexSize() (host, device)`

`VectorIndexSearchIf` reports its resident cost split by arena:

| algorithm | host | device |
|---|---|---|
| hnsw | `nrow × 8 + FileSize` (same before and after load) | 0 |
| fulltext2 | `ndoc × estBytesPerDocHeap` | 0 |
| ivfflat | config estimate before load; centroid index after | same, when the centroids go to the GPU |
| cagra / ivfpq | `HostComponentBytes` | `Σ DeviceComponentBytes` |
| brute force | dataset bytes | GPU variant only |

**hnsw costs its allocation PLUS its mapping.** The search path loads with
`View()`, which mmaps the model, and usearch's `memory_usage()` skips the node and
vector bytes for a viewed index (`if (!viewed_file_)`). Against usearch's own
accounting the cost is exactly linear in rows and independent of dimension:

| rows | dim | file | `MemoryUsage()` |
|---|---|---|---|
| 5 000 | 32 | 1.4 MB | 41,536 |
| 5 000 | 512 | 11.0 MB | 41,536 |
| 20 000 | 32 | 5.5 MB | 161,536 |
| 20 000 | 512 | 43.9 MB | 161,536 |
| 50 000 | 128 | 33.0 MB | 401,536 |

The deltas are exactly 8.000 bytes per row over a fixed ~1536-byte per-thread
term, so `nrow × 8` predicts `MemoryUsage()` within 0.4% at 100k and 200k rows.

**But that is the allocator, not the cost.** The mapping never appears in
`memory_usage()`. Measured on a viewed index (100k rows, dim 128, 63 MB file):

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
tables are a third, far smaller cost — and per *address space*, not per thread, so
concurrency does not multiply them — which `FileSize` covers by a wide margin.
Using one formula on both paths makes the pre-load reservation equal the post-load
charge; a reservation that under-states the charge lets a load blow a budget the
reclaim pass just declared satisfied, and that equality is what §5.3's admission
decision rests on.

`FileSize` slightly *under*-charges (107% measured against 100% charged; the
remainder is search scratch). Left as is: it is exactly known from the metadata
row, and the gap is far smaller than the headroom any budget leaves.

What this gives up: `mmap` decouples file size from memory requirement — a 42 GiB
file maps fine on a 21 GB host, and pages arrive only as touched. Charging
`FileSize` forfeits that for *cached* indexes. Charging measured residency instead
(`mincore`/`smaps`) would keep it, at the cost of per-mapping RSS accounting; not
attempted here.

**cagra/ivfpq use the load gate's own measurement.** `cuvs.MeasureTar` splits a
packed artifact into device- and host-resident components; the tar's total is the
wrong number for either budget.

## 7. Provenance: `nrow` and `build_ts`

The metadata tables gained two columns, appended last so `SELECT *` keeps existing
positions and readers guard on `len(bat.Vecs)`.

`build_ts` is the transaction `SnapshotTS` the content was built from. It is
deliberately distinct from the existing `timestamp` column, which is `time.Now()`
on the CN: a skewable wall clock that only orders generations and cannot be
compared with a snapshot's timestamp.

It is recorded **wherever the covered version is known**, which is both halves:

- A **build** reads the source table inside its own transaction, so that
  transaction's `SnapshotTS` is exactly the version captured.
- A **CDC sync** applies a change range and rewrites the generation, so the
  version it now covers is that range's upper bound — `DataRetriever.GetToTS()`,
  the same `status.To` that `UpdateWatermark` persists.

Neither records the writing transaction's own `SnapshotTS`, and for CDC that is a
soundness point: the sync transaction reads at some `S >= To`, and `(To, S]` can
hold changes committed after the range was collected but never applied. Recording
`S` would claim coverage the generation does not have — worse than recording
nothing, because `build_ts` exists precisely to be trusted by a coverage check.

`0` remains the unknown sentinel: a generation written before the column existed,
or by a direct non-ISCP caller with no iteration to name.

**Physical only, matching how MatrixOne stores a snapshot.** The column is
`bigint` holding `TS.Physical()`. `LogicalTime` is not stored and cannot be
recovered — the HLC increments logical only while the physical clock has not
advanced and resets it to 0 when it does — but nothing is lost for the comparison
this column exists for. `mo_snapshots.ts` is itself a `bigint`, and every
reconstruction of a named snapshot's timestamp is
`timestamp.Timestamp{PhysicalTime: record.ts}` with logical left at 0. The
equal-physical case resolves favourably rather than ambiguously: with a snapshot
at `(P, 0)` and a generation at `(P, L>=0)`, `(P, 0) <= (P, L)` always holds, so
an ordinary `snapshot_ts <= build_ts` is exact for named snapshots.

### 7.1 One metadata row per CDC tail frame

fulltext2, cagra and ivfpq record ONE metadata row per tail frame, keyed
`cdc_tail:<startChunkId>` — an id no base sub-index can collide with, since those
are `<index table>:<ts>:<n>` or `:<n>:<n>:<n>`. The row is written in the SAME
transaction as the chunks it describes, so an index's recorded coverage can never
disagree with what it actually stores — which a watermark read from elsewhere
(`mo_iscp_log`) cannot promise across multiple CNs.

Each row carries the frame's `checksum`, its `filesize`, and the `build_ts` the flush applied.
The checksum is not recomputed: `FrameCdcChunk` seals every chunk with a CRC32 in its footer, so
the row records what is already there. One chunk stores that chunk's CRC; a flush spanning
several stores the CRC over their CRCs in chunk order, which changes if any chunk's content
changes, if one goes missing, or if they come back reordered -- the cross-chunk binding no
per-chunk footer can provide. Plain hex, no algorithm tag: a reader reaches the column through
the row's `index_id`, so it already knows it holds a tail frame, and the chunk count selects
between the two rules. That
is what makes tail sizing cheap and exact: summing `filesize` over the rows reads
metadata only, where `SUM(LENGTH(data))` would project the blob column and read the
entire tail off storage just to measure it. §14 covers what happens when the rows
describe only part of a tail.

Two conditions gate emission, and they answer different questions — see §12.
Readers that mean "the bases" must exclude these rows
(`vectorindex.NotTailFrameSQL`); a reader that forgets loads a tail frame AS a base
sub-index, or folds the tail's bytes into the base totals.

`nrow` gives hnsw the pre-load estimate of §6 that no other source provides at the
right granularity.

**What `build_ts` buys.** A base generation is built by reading the source table
at `SnapshotTS = X`, so its content is exactly the base data as of X. A snapshot
taken **at X** therefore yields a base table and an index that provably agree, and
`build_ts` is what makes X recoverable after the fact. For any later Y the
agreement depends on what CDC appended in `(X, Y]`, which a CDC-appended
generation does not record — so `build_ts` makes the BASE half verifiable and
leaves the CDC half open. This is the mechanism a future read-path check would
rest on.

**Upgrade, and the rolling window.** Readers tolerating the old four-column shape
is not sufficient: a CN running this code *writes* the provenance columns, so the
first CDC sync or rebuild against an index created earlier would fail on a shape
mismatch. Because each
metadata table is created per index at `CREATE INDEX`, and `REINDEX` rewrites its
rows rather than the table, such an index would stay broken for writes forever.
The **v4_0_7** tenant upgrade therefore alters every metadata table an account
owns. No fixed `UpgSql` can express DDL over a runtime-determined set of tables,
so it uses the existing escape hatch — a plain function called from
`HandleTenantUpgrade`, as v4_0_6 does for legacy foreign-key metadata — leaving
the shared `UpgradeEntry` framework untouched.

The ALTER preserves `relkind`. A copy-rebuild that drops it un-hides an index
metadata table from restore and CLONE, which is a separate defect this branch also
fixes.

A rolling upgrade has **three** windows, and they need different answers. The
first is CREATE-time and is covered in full by §12; the other two are here:

| window | who meets what | answer |
|---|---|---|
| mixed CNs, a table is CREATED | a NEW CN would create a six-column table that an OLD CN's positional writer then fails on | `CREATE INDEX` builds the LEGACY shape until `MOProtocolVersion >= 57`; the migration widens it later (§12) |
| mixed CNs, migration not yet started | an OLD CN writes four positional values into a table another CN may have widened | `RequiredProtocolVersion` holds the tenant snapshot until every service reports the protocol carrying the new writer, so the widening cannot begin while such a CN is alive |
| all CNs new, migration running | a NEW CN serves a tenant whose tables are not widened yet — the migration is asynchronous and per tenant | the writer **names its columns** and omits the provenance ones until the table has them |

The second is the load-bearing half: the protocol barrier delays the migration, it
does not delay the tenant's traffic. So the writer asks the table
(`sqlexec.HasProvenanceColumns`, memoized per table, positive answers only — a
table never loses the columns) and emits the shape that table accepts. A named
INSERT is correct on both shapes; a positional one is correct on exactly one, and
which one depends on a migration the writer cannot see.

Omitting the columns costs provenance and nothing else: they are `not null
default 0`, and `0` is already the documented "unknown" sentinel. A read failure
answers "legacy" for the same reason — degrade to the shape that always works.

## 8. Concurrency

`Search`/`SearchInto` re-wrap the **same caller-supplied algorithm** on every
retry attempt, so a retry could call into an algorithm that an evicting goroutine
was still inside `Destroy` on — two wrappers, two mutexes, one object. Each
wrapper carries a `destroyed` channel closed when its teardown finishes, and a
retry that reuses the algorithm waits on it first.

Three things make that wait safe:

- **It is taken only when a teardown was actually claimed.** The retry gate is a
  private `errIndexDestroyed` sentinel compared with `errors.Is`, not the public
  `moerr.ErrInvalidState` code. That code is also raised by an *algorithm* for its
  own reasons (a paused txn client, a failed remote run), and retrying those spun
  forever: the entry stayed `STATUS_LOADED` and in the map, so each attempt
  re-invoked the same failing backend with nothing changed — 2,711,976 backend
  calls in 5 seconds, measured on the pre-fix code.
- **It cannot hang.** An entry becomes unreachable only through a
  `CompareAndDelete` immediately followed by a destroy, at every removal site.
- **It is cancellable.** The wait selects on the query's context, so a cancelled
  reader leaves immediately instead of waiting out an unrelated teardown.

Relatedly, the governor reads sizes from atomics published under the entry lock,
never by calling into the algorithm, which a concurrent eviction may be tearing
down.

## 9. Change inventory

The branch carries two features that share a cause: historical reads created the
residency class that the governor bounds.

| theme | area |
|---|---|
| Named-snapshot reads (§3) | fulltext, fulltext2, hnsw, cagra, ivfpq + tests |
| Snapshot tenant / publisher identity (§3) | `sqlexec`, compile layer |
| Byte governor (§4–5) | `pkg/vectorindex/cache` |
| Sizing (§6) | `GetIndexSize`, hnsw mmap accounting, cagra/ivfpq arenas |
| Provenance + upgrade (§7) | catalog columns, `v4_0_7` |
| Preload/Load split (§5.4) | `veccache`, `cagra`, `ivfpq`, `ivfflat` |
| Concurrency (§8) | `veccache` |
| Spill location | hnsw/ISCP LOCAL fileservice, not `$TMPDIR` |

If the review prefers smaller units, the natural cut is **§3 (historical reads)**
as one PR and **§4–8 (residency)** as a second: the former is the bug fix, the
latter is what makes it safe to run. They are presented together because merging
§3 alone ships the unbounded-residency problem it creates.

## 10. Validation

- **Unit and race.** Focused adaptive `-race -count=N` on the concurrency paths
  plus a full owning-package race run; a stress test interleaves loads with
  eviction. The owning package set is also clean at `-count=2`, which is what
  catches a test leaking process-global state into the next iteration.
  Every governor fix carries a control run proving the test fails on the
  pre-fix code — e.g. the idle-victim claim blocks 5.01s before and passes in
  0.21s after; the admission reservation admits 8 of 8 arrivals against a budget
  for one, before; reserve's ordering control reports an arrival seeing fewer
  predecessors than it has; and the CDC tail sizing probe panics instead of
  degrading when its recover is removed.
- **Upgrade.** A real embedded-cluster test drives the public
  `HandleTenantUpgrade`: it builds a legacy-shaped metadata table, asserts the
  current-shape write **fails**, runs the upgrade, and requires the write to
  succeed and a second run to be a no-op.
- **BVT.** Named-snapshot cases for fulltext, fulltext2, hnsw, cagra and ivfpq; a
  case for the two variables' scope and readback; a case that binds a real cap.
  Cases are isolated by account, so they are idempotent rather than passing only
  on a virgin cluster.

**No case may set a cap on the SYS account.** The index cache is one object per CN,
shared by every case the BVT runs concurrently, so a SYS cap governs the whole
cache and would evict a neighbouring case's warm indexes — a failure that appears
in the wrong file, only under concurrency, and not on a re-run. Every case that
binds a cap therefore creates its own account and sets the value inside that
session, where it governs only its own entries.
`TestBVTCapChangesStayInsideATenantAccount` scans the suite and fails on any
`SET GLOBAL` of either variable made on the default connection or as `sys`.
Session-scope spellings are exempt because both variables are `ScopeGlobal` and
the server refuses them, which is itself something a case asserts.

The cap-binding case alternates between **two** index keys, not one. A single index
is the arena's only occupant, which §5.3 always seats however small the cap, and
querying the same key repeatedly is a cache hit that reads no limit at all — so a
one-index case asserts nothing about the bound it claims to test. Alternating keys
under a cap that fits neither forces charge, eviction and reload on every query,
and every query must still return the same correct rows.

### 10.1 Both build flavours

The device arena only exists under `//go:build gpu`, so a green run of one flavour
says nothing about the other. Both are required:

| flavour | result |
|---|---|
| `MO_CL_CUDA=1` | 71 packages, `-race` clean on `cache` and `hnsw` |
| `MO_CL_CUDA=0` | 68 packages |

The CPU run is not ceremonial — it caught a real defect: a test asserting a
positive device budget, where a CPU build correctly budgets **0** because there is
no device arena to bound. Switching flavours also requires clearing
`thirdparties/_jemalloc_build` and running `make clean` in `cgo/`, or the stale
objects from the other flavour fail the link.

### 10.2 Real-GPU run

Recorded here because these branches are GPU-only: green CPU CI never executes
them, and the design names CAGRA/IVF-PQ snapshot, admission order and allocation
refusal as pre-merge requirements. Hardware: **NVIDIA RTX 5070 Laptop, 8151 MiB**,
single device, `MO_CL_CUDA=1`.

| case | total | passed |
|---|---|---|
| `vector_gpu_index_cache_size` (device arena, 1 MiB cap, three CAGRA indexes) | 36 | 36 |
| `vector_cagra_snapshot` | 21 | 21 |
| `vector_ivfpq_snapshot` | 25 | 25 |
| `vector_gpu_negative` (allocation refusal) | 42 | 42 |
| `vector_gpu_edge` | 15 | 15 |
| `vector_cagra_replicated` (simulated placement) | 18 | 18 |
| `vector_cagra_sharded` (simulated placement) | 30 | 30 |

The per-case counts above were re-measured on the current head and reproduce
exactly. Two earlier claims did NOT survive re-measurement and have been removed
rather than carried forward:

- a VRAM sample presented as proof the GPU is exercised. Re-sampled at 1 Hz across
  these same cases, device memory moves ~2 MiB (1549 -> 1551), not the ~273 MiB
  once recorded: these datasets are tiny and their allocations are shorter-lived
  than the sampling interval. The figure did not mean what it was cited for. What
  does establish GPU execution here is simpler -- CAGRA and IVF-PQ have no CPU
  build path, so `vector_cagra` and `vector_ivfpq` passing IS the GPU path.
- a 1M-row wiki_all run reporting zero admission refusals. It predates the
  accounting fixes, which deliberately CHANGE what admission charges (the CDC
  overflow is now reserved at Preload and the delete id-map is charged to the host
  budget), so its refusal count cannot be assumed to still hold. See §10.4.

**Re-run after the accounting fixes** (CDC overflow charged at Preload, the delete
id-map charged to the host budget, one probe for both device budgets), on a binary
built from that head:

A subset of `gpu_cases`, chosen for the paths this branch touches -- not the whole
suite (34 + 15 cases). 141 statements, all passing:

| case | from |
|---|---|
| `vector_gpu_index_cache_size` | `gpu_cases/vector` |
| `vector_gpu_negative`, `vector_gpu_edge` | `gpu_cases/vector` |
| `vector_cagra`, `vector_ivfpq` | `gpu_cases/vector` |
| `vector_cagra_replicated`, `vector_cagra_sharded` | `gpu_cases/vector` |
| `vector_cagra_snapshot`, `vector_ivfpq_snapshot` | `gpu_cases/pessimistic_transaction/vector` |

Also verified live on that binary: a CDC flush writes its `cdc_tail:<n>` row with
the frame's size, `build_ts` and checksum, `MATCH` answers from both the base and
the tail, and `gpu_mode` 0 and 1 agree on all of it (hnsw and cagra create, search,
CDC, cap set/reset, and the created metadata shape).

The BVT suites cover these paths but do not FORCE the states the accounting fixes
address, so each is driven directly:

| forced state | test |
|---|---|
| an already-resident, busy entry plus four concurrent CDC-only arrivals against a budget with room for one | `TestResidentPlusConcurrentCdcArrivalsCannotOversubscribe` — 1 admitted, 3 refused, resident never evicted. Not the sole-occupant case: there is somebody to protect, so the arithmetic stands alone |
| a CDC-only generation sized BEFORE Load | `TestCagraOverflowIsChargedBeforeLoad` — the Preload estimate charges it, and the real count supersedes rather than adds |
| the id-map's host delta, on a real index | `TestIdMapChargeIsNonZeroAndPerGeneration` (`-tags gpu`) — builds a real cagra index, replays a real delete through the native `ensure_id_index`, and requires a positive delta sized by the INDEX's rows; a second generation starts from its own baseline, and one that never deletes is charged nothing |
| both device budgets from one probe | `TestDeviceBudgetsComeFromOneProbe` — the aggregate is the fold of the per-card map, and a failed probe is retried rather than latched |
| a sizing probe on a process with no lock service | `TestCdcTailSizingSurvivesAProcessWithNoLockService` — degrades to no estimate instead of panicking the load |

GPU performance acceptance remains outstanding and is tracked separately.

**Why `-n`.** mo-tester compares result-set *metadata* as well as values unless
`-n` is given. Without it, `vector_cagra_replicated` and `vector_cagra_sharded`
report 16/18 and 28/30 — and every one of those failures is a column-**length**
mismatch on `show create table` / `mo_catalog.mo_indexes`, never a value: `RSRow`
mismatches are zero in both.

### 10.3 Index build time is unaffected

The branch touches load and search, not build. Measured on one box, fresh
`mo-data`, the same 1M wiki_all f32 CSVs and cell both sides:

| protocol | this branch | `main` |
|---|---|---|
| CREATE INDEX after a service restart (canonical) | **118 s** | — |
| CREATE INDEX, settled, no restart | 195 s | 361 s |
| CREATE INDEX, straight after the import | 308 s | 462 s |

118 s is inside the historical spread for this cell (four identical runs measured
96.4–171.5 s, median 110.2), and the branch is the faster side in both matched
pairings.

**Restarting between the load and the build is the measurement, not hygiene.**
CAGRA sizes its sub-indexes from free host RAM sampled once at build start, and
everything the CN still holds — the previous generation resident in the index
cache, its VRAM, the import's own footprint — comes off that sample. The four runs
above rotated into 1, 3, 2 and 4 sub-indexes respectively, and each one pays its
own GPU build, save and tar. A single build sample on a warm CN measures the
previous build.

These build timings were taken before the accounting fixes and are NOT re-measured
here. They remain valid because the fixes touch Preload, Load and the metadata
writer -- not the build path -- and the restart protocol above means a build
measurement never observes the cache anyway.

### 10.4 Outstanding

Stated so no reader has to infer coverage from silence.

- **A 1M-scale run after the accounting fixes.** The earlier wiki_all figure
  (zero admission refusals) predates them, and the fixes deliberately change what
  admission charges -- a CDC-heavy 1M generation now reserves its overflow at
  Preload, and any generation replaying a delete carries the id-map. The refusal
  count at that scale is therefore unknown, not zero. The functional paths are
  covered by §10.2 and the forced-state tests; what is missing is the scale.
- **GPU performance acceptance.** No QPS or latency comparison against `main` has
  been run for this branch. The distance kernels are untouched and the governor
  sits on the load path rather than the search path, so no regression is expected
  -- but expected is not measured.
- **The id-map prospective reservation.** The map is charged once it exists
  (§10.2) but NOT reserved before the replay that creates it. Reserving ahead
  means reserving rows x 40 for every CDC-active generation -- ~3.5 GB at 88M rows
  -- including the ones that replay no deletes at all. Charging after the fact
  leaves a window where the host budget is briefly short by that much. Deliberate,
  and the trade belongs to whoever owns the memory budget.

## 11. Decision log

Items raised by the self-review gate and deliberately not changed. Each is
recorded so a later review can dismiss it in one line instead of re-litigating it.

**`cachegen.CdcGenerationSqls` counts tail frame rows in `MAX(timestamp)`.**
Unlike `fulltext2`, it does not exclude the `cdc_tail:` rows. It is redundant
rather than wrong: a frame row's timestamp is written by the same flush that
writes the chunks it describes, so it can never exceed the generation the base
rows already report. Excluding it would change no result. Left alone because the
predicate is on the hot generation-probe path and the exclusion would only add a
term that never fires.

**The `provenanceShape` memo is never pruned.** It holds one entry per
`(db, table)` that has been observed widened, and entries are only added once the
table has the columns — a state that never reverts short of a DROP. The bound is
the number of index metadata tables in the cluster, each a few tens of bytes, so
the growth is bounded by schema size and not by traffic. A DROP + CREATE of the
same name re-creates the table already widened, so the stale positive is also
the correct answer.

**The first SYS cap read holds `sysLimit.mu` across the query.** One caller
queries while the others block, up to the 10 s SQL timeout, once per
`sysLimitTTL`. This is the pre-existing shape and it is what keeps a startup
thundering herd from issuing N identical queries. The blocked callers are cache
admissions, which have no deadline of their own, and the memo is refreshed by the
housekeeping tick well before it expires — so on the steady-state path nobody
waits at all.

**`DeleteAllBasesSqls` spares the tail's frame rows.** It was written to delete
every metadata row (`WHERE TRUE`), which contradicted its own contract that "the
tag=1 CdcTail is untouched". `fulltext2_create`'s `sealSegment` calls it WITHOUT
`DeleteTailSqls` -- a REBUILD deliberately keeps the tail's bytes -- so the rows
went while the chunks they described stayed. That stripped `build_ts` off a live
tail permanently, and left `tailPeakBytes` summing only whichever frames a later
flush appended: the fallback fires only when there are NO rows, so one new frame
would be reported as the size of the entire tail and the load budget would
under-reserve. The metadata delete is now predicated on
`index_id NOT LIKE 'cdc_tail:%'`. Compaction and re-index are unaffected -- both
call `DeleteTailSqls`, which removes the tail's chunks and rows together.

The other algorithms need no equivalent change: cagra and ivfpq clear the tail
only through re-index's full `DELETE FROM` of both hidden tables, so chunks and
frame rows always go together. `CagraModel.ToDeleteSql` / `IvfpqModel.ToDeleteSql`
would match `index_id = 'cdc_tail'` exactly and miss the `cdc_tail:N` rows, but
neither is reachable -- `hnsw` is the only caller of that method, and hnsw writes
no frame rows.

## 12. Rollout gates: two questions, two mechanisms

The provenance columns are gated twice, because two different things can go wrong and neither
check answers the other's question.

| Question | Scope | Mechanism | Gates |
|---|---|---|---|
| Is any un-upgraded CN still out there? | deployment | `MOProtocolVersion >= MORPCVersion57` | CREATE-time schema width, CDC tail row emission, the v4_0_7 migration |
| Does THIS table carry the columns now? | one table | `sqlexec.HasProvenanceColumns` probe | whether a writer NAMES them |

**Why CREATE needs the deployment gate.** The migration's `RequiredProtocolVersion` only covers
tables that already exist. `CREATE INDEX` is the other producer of a wide table, and during a
rolling upgrade a new CN would otherwise create one that an old CN is still serving. The old
vector writers are POSITIONAL --

```sql
INSERT INTO meta VALUES ('id', 'chk', ts, size)
```

-- so four values meet six columns and the statement fails on arity. A column `DEFAULT` cannot
repair that: the value count is rejected before any default is consulted. So below the gate the
metadata table is born in the legacy shape, and the tenant's migration widens it later.

fulltext2 is exempt and stays wide at CREATE: its writer already named its columns before this
change, so an extra defaulted column is invisible to it.

**Why emission needs it too.** A CDC tail frame row is keyed `cdc_tail:<n>` in the same table as
the base sub-indexes. An un-upgraded CN reads that table with `SELECT *` and would take the row
for a base segment and try to load it. The table's own shape used to prove no such reader was
left -- only the gated migration could widen it -- and CREATE-time widening breaks that
inference, so emission now asks the deployment directly, in addition to the shape probe.

**Consequence, accepted.** An index created while the rollout is in progress gets a legacy-shape
metadata table, and the tenant migration will not revisit it if it has already run. That index
carries no provenance until something widens it. This is the documented degraded mode -- `nrow`
and `build_ts` absent read as the `0` "unknown" sentinel, sizing falls back to the chunk bound,
and no correctness property depends on them -- so it is preferred over the alternative, which is
a rebuild on an old CN failing outright.

**Rollback.** `MOProtocolVersion` is lowered again for a rollback, so it is read live at each
decision rather than memoized. A table widened before a rollback still exists, and the positional
writer on a rolled-back CN would still fail against it; the gate cannot undo that, only avoid
adding to it. The shape probe's memo is positive-only for the same reason -- it records what a
table IS, not what the deployment allows.

## 13. Admission is linearized

Two ordering rules make the byte budget an actual bound rather than an approximation.

**Taking a place in line and publishing it are one step.** `pendingAhead` counts only arrivals
with a LOWER sequence number, so an arrival that has taken its number but not yet published is
invisible to precisely the arrivals that must see it. Split in two, this schedule seats both of
two loads that fit one at a time:

```
A takes seq=1, stalls before publishing
B takes seq=2, publishes, sees an empty line, is admitted
A publishes, ignores B for being behind it, is admitted too
```

Under `arrivalMu`, taking a later number proves every earlier one is already published. The lock
covers two map operations and never spans reclaim, Preload/Load, a Destroy, or a search.

**Pending is read before residents.** A finishing load becomes resident and only then drops its
reservation (`Load` publishes `STATUS_LOADED`; the deferred `release` runs after it returns).
Reading residents first would let a load that completes between the two reads be missed by both --
absent from the residents snapshotted before it finished, absent from the reservations read after
it let go -- so its bytes would be handed out twice. Reading pending first turns that same
interleaving into counting it twice, which only ever refuses too early.

## 14. Tail sizing bounds what it cannot measure

`tailPeakBytes` sums the per-frame rows for the frames that have them and bounds every chunk no
row accounts for by `MaxChunkSize`. The earlier rule -- fall back to the chunk count only when the
sum is zero -- was wrong for any tail that is described in part:

- a legacy tail already on disk when its tenant was migrated gains the columns but no rows for the
  frames already written, and the next flush appends one frame that does have a row;
- a transient shape-probe failure writes one flush's chunks without their rows, leaving a gap in
  the middle.

In both, the sum becomes positive, the fallback stops firing, and the whole tail is charged at the
size of the frames that happen to carry rows -- a 100 MiB tail plus a new 1 MiB frame charged as
1 MiB, while `LoadTailSegments` still loads all 101 MiB. Coverage is now compared directly: the
frame rows report the chunks their bytes occupy (`ceil(filesize / MaxChunkSize)`), and the
difference from the stored chunk count is bounded at the cap each chunk was written under. Exact
where it can be, an upper bound where it cannot, never below what the load will hold.

### 14.1 The uncovered bound is an upper bound, and it is transitional

Charging every uncovered chunk at `MaxChunkSize` overstates a tail of many SMALL
frames: a frame under 64 KiB occupies one chunk but is counted as a full one. A
100 k-frame legacy tail holding ~200 MB can therefore be charged ~19 GB, and
`checkTailLoadBudget` can refuse a MATCH on a CN that had room for it.

This is accepted, for three reasons. The direction is the safe one — the
alternative, believing a sum that describes part of a tail, under-reserves the
load this budget exists to bound. It is not measurable more cheaply: the only
exact source for an unrecorded chunk is `LENGTH(data)` on a blob column, which
reads the entire tail off storage to answer "how big is it". And it is
transitional rather than permanent: only chunks written before the per-frame rows
existed are uncovered, every new flush records its frames exactly, and compaction
rewrites the tail into frames that do — after which the figure is exact.

The refusal names the uncovered chunk count and the cap it applied, so an operator
can tell an over-estimate from a genuinely large tail, and points at the
compaction that makes it exact.
