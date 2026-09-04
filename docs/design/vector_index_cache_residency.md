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
4. **No silent unbounding.** A budget that cannot be read falls back to a defined
   value; an outage does not quietly disable the bound.
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
- **A CDC tail's data version.** Not knowable at the write site (§6.2).

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

Two `ScopeGlobal` variables, both defaulting to `0` = unlimited, so an
unconfigured deployment behaves exactly as before and pays nothing — with every
budget unset the governor returns before it walks the cache.

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
miss, serialized on one mutex.

### 5.3 Eviction, not refusal

Exceeding a budget evicts; it never fails a query. Refusing would fail ordinary
SQL on a cache accounting rule, and there is nothing special about a historical
generation — every resident index is charged and every one is evictable.

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

## 6. Sizing

### 6.1 `GetIndexSize() (host, device)`

`VectorIndexSearchIf` reports its resident cost split by arena. Per algorithm:

| algorithm | host | device |
|---|---|---|
| hnsw | usearch `MemoryUsage()`, or `nrow × 8` before load | 0 |
| fulltext2 | `ndoc × estBytesPerDocHeap` | 0 |
| ivfflat | delegates to its centroid index | delegates |
| cagra / ivfpq | `HostComponentBytes` | `Σ DeviceComponentBytes` |
| brute force | dataset bytes | GPU variant only |

Two figures are measured rather than assumed:

**hnsw does not cost its file size.** The search path loads with `View()`, which
mmaps the model, and usearch's `memory_usage()` skips the node and vector bytes
for a viewed index. Measured against usearch's own accounting, the cost is
exactly linear in rows and independent of dimension:

| rows | dim | file | viewed |
|---|---|---|---|
| 5 000 | 32 | 1.4 MB | 41,536 |
| 5 000 | 512 | 11.0 MB | 41,536 |
| 20 000 | 32 | 5.5 MB | 161,536 |
| 20 000 | 512 | 43.9 MB | 161,536 |
| 50 000 | 128 | 33.0 MB | 401,536 |

The deltas are exactly 8.000 bytes per row over a fixed ~1536-byte per-thread
term. Charging `FileSize` would over-state an hnsw entry by ~80× and evict it
against bytes that are reclaimable page cache on the LOCAL volume — and would
meter it on a different definition of "host resident" than fulltext2, which
excludes its mmap'd postings for the same reason.

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

It is recorded **only where it is knowable**. A build reads the source table
inside its own transaction, so that transaction's `SnapshotTS` is exactly the
version captured. A CDC-appended generation records `0`: the consumer writing the
row cannot see the change range it applied — `iscp.DataRetriever` exposes no
timestamps and the iteration's `[from, to]` stays upstream — and the sync
transaction's own timestamp would say when the sync *ran*. Recording that would
repeat the wall-clock conflation this column exists to fix, under a name implying
otherwise.

`nrow` gives hnsw a pre-load estimate (§6.1) that no other source provides at the
right granularity.

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
