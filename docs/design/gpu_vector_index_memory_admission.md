# GPU vector index memory admission

Issue: #27356

## Goal

A GPU vector index build or load must be **refused** when the memory it is about
to allocate does not fit, rather than allocating it and being OOM-killed. This
holds for both pools it spends:

- **device** memory (VRAM), for index data, build workspaces and uploads;
- **host** memory, for the capacity-sized build buffers, the INCLUDE columns,
  the int8/uint8 quantizer's staging arena, and the components a load
  materialises.

The rule the whole design follows: **a claim is taken immediately before the
allocation it covers, and released once the allocator has taken those bytes.**
That window — decided but not yet allocated — is exactly the window a live
free-memory reading cannot see, and it is the only window that needs covering.

## Scope

Supported end to end, meaning CREATE persists an index that can then be loaded
and searched:

| algorithm | distribution | storage |
|---|---|---|
| IVF-PQ | SINGLE_GPU, REPLICATED, SHARDED | f32 / f16 base, optional int8 / uint8 quantization |
| CAGRA | SINGLE_GPU, REPLICATED, SHARDED | f32 / f16 base, optional int8 / uint8 quantization |

Split (rotated) builds are supported: when the table exceeds one sub-index's
capacity, the build produces N sub-indexes and a query reads all of them.

### Design decision — CAGRA at 88M rows as a single index is OUT of scope

A single CAGRA index over 88M rows is **not** a supported configuration. Its
device-resident footprint (CAGRA keeps the raw vectors resident *and* the graph)
exceeds what a single card can hold, so no amount of eviction or rotation makes
it searchable; the aggregate hardware gate refuses it.

This is a deliberate limitation, not a defect:

- the gate compares against **total** VRAM, so the refusal is permanent and
  honest — an index that could never serve a query is better refused at CREATE
  than discovered at first search;
- IVF-PQ is the supported answer at that scale, because it does not keep the
  raw vectors device-resident;
- **rotation does not help here.** Splitting into sub-indexes does not reduce
  what one card must hold: a query reads every sub-index at once, so
  `PerDeviceDemand` sums them, and a non-shard component (`index.bin`) is
  charged to every participating device. Under SINGLE_GPU and REPLICATED the
  per-device demand is the whole table's footprint however many sub-indexes it
  is cut into;
- **SHARDED is the only mode that spreads it**, because shard *i* lands on
  device *i* and is charged only there. Whether 88M CAGRA fits that way is a
  question of aggregate VRAM across the card set, not of rotation;
- CAGRA remains supported as a single index at row counts whose footprint fits
  one card.

**Refused early, not after the whole build.** The aggregate hardware gate runs
after each sub-index is packed, not only at the end. `PerDeviceDemand` is
monotone -- a sub-index only ever adds bytes to a device -- so a running total
already over the ceiling guarantees the finished one is, and every sub-index
packed after that point would be work thrown away. The gate is better suited to
this than its load-path twin: its ceiling is total VRAM and does not move
between checks, where the load gate re-samples free memory and can refuse
transiently.

A partial refusal is worded differently from a final one ("needs at least N,
after K sub-index(es)"), so an operator does not size a fix from a figure that
is still rising.

SHARDED never reaches the incremental path: `planCapacity` refuses to combine it
with a split, since each sub-index would be packed as a sharded index and could
not be reloaded. It is therefore always one sub-index, and the completed check
is the only one that runs.

## Reference hardware

The admission rules are designed, tuned and reviewed against this baseline:

| | |
|---|---|
| host RAM | 512 GB → host budget (75%) **384 GB** |
| GPU | NVIDIA L40S, 48 GB VRAM → device budget (75%) **36 GB**, IVF-PQ build peak (65%) **31.2 GB** |
| CPU | 64 cores |
| host : VRAM ratio | **~10.7 : 1** |

The ratio is the part that matters for review, and "host memory" means what the
governor measures: the cgroup limit where one applies, not the DIMMs. A CN
capped at a gigabyte on a 512 GB host is therefore expressible -- that is
precisely why availability is read from the cgroup -- but it is not a
configuration this feature targets. A CN driving an L40S is not deployed under a
few tens of GB: at that size a single 1M-row int8 index (768 MB) plus its build
buffers already dominates the budget, so such a build fails for a more basic
reason than whatever a counterexample is trying to demonstrate.

**Counterexamples must be reachable from this envelope, or from documented
defaults, to be actionable** -- and one that depends on an unusual
host-to-VRAM ratio should say so, since the ratio is doing the work rather than
the rule under discussion.

For scale against the baseline: the largest staging arena the hard ceiling
permits (`kMaxQuantizerTrainLimit` = 1,000,000 rows, dim 768, f32 base) holds
2.86 GB of raw rows, and the figure the planner charges is its **replacement
peak with staged ids — 5.74 GB, 1.5% of the host budget.** Sizing failures
attributable to the quantizer sample are therefore not reachable on the
reference machine; they require both a raised `quantizer_train_limit` and a
host far smaller than this.

Defaults are the reviewed configuration. Tuned knobs (`quantizer_train_limit`,
`max_index_capacity`, `kmeans_train_percent`) shift cost onto the operator by
design, and the refusal messages name the knob and the direction to move it.

`quantizer_train_limit` additionally has a HARD ceiling
(`kMaxQuantizerTrainLimit` = 1,000,000 rows), enforced in `quantizer_staging_rows`
so it binds at every entry point rather than only at the DDL parameter. CREATE
INDEX rejects a larger value outright rather than clamping silently: handing a
statement less than it asked for would leave the operator believing they have a
sample they do not have.

## Ownership

**C++ is the engine; Go is the interface.** Both ledgers live in C++, beside the
allocations they govern:

| ledger | file | governs |
|---|---|---|
| device | `cgo/cuvs/device_memory.hpp` | build peaks, index loads, row uploads, quantizer training uploads |
| host | `cgo/cuvs/host_memory.hpp` | capacity buffers, INCLUDE columns, staging arena growth, load components, the id map |

Go retains exactly one job in this subsystem: **planning**, not admission.
`planCapacity` picks how many rows a sub-index may hold, from the VRAM fit, the
host fit, and any explicit `max_index_capacity`. That is a sizing question asked
once per CREATE; admission is a per-allocation question asked wherever the
allocation happens.

An earlier revision put the host ledger in Go. It could only bracket a cgo call,
so anything the native side allocated outside that bracket was charged to
nobody — and the workaround was to drag allocations earlier so they landed
inside a window Go could see. The allocation was bending to fit the accounting.
Moving the ledger into C++ removed that pressure and deleted the mechanism built
to relieve it.

## Invariants

1. **Claim before allocate, release after.** Released early, the bytes are
   counted nowhere and the next caller is admitted against them. Held late, they
   are counted twice — once in the ledger, once in the now-lower availability —
   and a concurrent build is denied headroom that exists.
2. **What a claim covers must be materialised before it is released.** A bare
   `reserve()` leaves pages unfaulted, and cgroup usage only moves on fault. The
   capacity buffers and the staging arena therefore `resize()` (which
   value-initialises, faulting every page) and restore the size afterwards,
   keeping the capacity.
3. **Admission is check-and-claim under one CAS.** Two callers cannot both pass
   against the same ledger value.
4. **A planning figure must bound the build it describes.** The staging charge is
   bounded by one sub-index's capacity, not the whole source. The SHARDED
   aggregate is aligned to the 32-row shard split before being multiplied, so
   the last shard — which absorbs the remainder — never exceeds the per-card
   figure it was derived from.

   "Bound" means the *peak the native side actually claims*, not the final size.
   The staging arena grows geometrically, and a growing `resize()` allocates the
   new buffer before freeing the old, so `stage_rows_locked` claims the whole
   replacement while the old buffer is still resident — against 75% of the
   availability left at that moment, with capacity already faulted in. It also
   stages ids beside the vectors whenever the caller supplies them, which the
   production build paths always do. The planner therefore charges
   `2 * (rawRowBytes + idBytes)` per staged row. Charging the final raw size
   instead admitted plans the native governor then refused deterministically.
5. **"Cannot measure" and "nothing available" are different answers.** An
   unreadable host admits without claiming (the capacity model already falls
   back to the device bound); a *measured* zero refuses. Collapsing them is what
   let a full cgroup disable the bound meant to stop the build.
6. **One list, one owner.** Values that both languages need — the
   host/device component classification, the quantizer staging row count, the
   per-index budget fraction — are defined once in C++ and read by Go, never
   restated.

## Alternatives considered

**Hold the host claim for the buffer's lifetime** (permitting a bare
`reserve()`). Rejected: every page would be counted twice as ingest faults it,
denying a concurrent build this claim's worth of headroom for the whole build.
Faulting up front costs one linear pass; holding costs minutes of false
pressure.

**Keep the host ledger in Go, reading availability with gosigar.** Rejected: see
Ownership. Retained only for the *planning* reader, where cgo per-call cost is
the reason gosigar is pure Go in the first place.

**Vendor a third-party meminfo library (sigar) for the C++ reader.** Rejected:
it has no cgroup concept, which is the part that matters in a container, and the
library is dormant. The reader is ~200 lines of `/proc` and cgroup parsing,
mirroring the Go rules case for case.

**Serialise builds instead of admitting them.** The build *call* is already
serialised per device by `device_build_mutex`, because the cuVS kmeans workspace
is not re-entrant. But that is not where the memory is taken: the capacity
buffers are allocated in the index constructor, from `InitEmpty` at the start of
CREATE INDEX, while `device_build_mutex` is taken much later inside `build()`
and only around the cuVS call. There is no lock to widen -- the allocations were
never on that path.

Serialising them would mean holding a lock from the constructor, through the
whole ingest (which scans the source table), to the end of the build: two
independent `CREATE INDEX` statements, each running for minutes, serialised
against each other at statement level. Rejected because:

- **it is a DDL concurrency change, not a locking detail.** Two CREATE INDEX
  statements are separate SQL executions; making one wait for the other for the
  duration of a table scan is a much larger semantic change than adding a ledger;
- **it still does not cover loads.** A load deserializes on the search path, from
  an unrelated session, and takes no build lock. Build-vs-load is one of the two
  overcommit shapes the device governor exists for; the other is load-vs-load,
  since the index cache deduplicates by cache key and two *different* cold
  indexes load concurrently;
- **it does not fix a single oversized build.** Serialising removes concurrency,
  not size: one build larger than the card still fails, so the capacity model and
  the admission check are needed regardless.

## Rollout and compatibility

- **No on-disk format change.** Packed artifacts are unchanged; the component
  classification only decides which bytes are charged to which pool.
- **No new configuration.** Budgets are fractions of measured memory (75% host,
  75% device default, 65% for IVF-PQ's build peak); `max_index_capacity` and
  `quantizer_train_limit` keep their existing meanings.
- **Behaviour change:** builds and loads that previously proceeded and risked an
  OOM kill may now be refused with a message naming the demand, the budget and
  the device. That is the intended change.
- **Platform:** Linux only, as the CUDA index already is. The host reader
  degrades to "cannot tell" elsewhere, which admits without claiming.

## Validation

- **C++ unit tests** (`cgo/cuvs/test/`): governor rules for both pools, the
  cgroup hierarchy walks against temp-directory fixtures, the shard-alignment
  boundary, and concurrent claims under a constrained ledger.
- **Go unit tests**: capacity planning and the staging bound (untagged, so
  ordinary CI exercises them without a GPU); the device gates against a fake
  budget.
- **GPU-tagged tests**: build/load/search per algorithm and distribution mode.
- **BVT**: `test/distributed/gpu_cases/vector/`, including multi-sub-index cases.
- **1M benchmark**, canonical settings k=20, probe=16, concurrency=8, n=5000,
  covering f32, f32→int8 and f16→int8 for both algorithms.

Not covered by automated tests, and stated rather than implied: the 88M
single-index configurations and SHARDED across physically distinct GPUs need
hardware this project's CI does not have. The arithmetic for those paths is
covered by unit tests against injected figures; the end-to-end behaviour is not.
