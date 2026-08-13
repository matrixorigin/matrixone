# Query memory control and attribution implementation plan

Issue: #25866

Foundation: #26459 / #26531

Related work: #26563, #25127, #25638, #3433, #26768

## Decision summary

MatrixOne should extend the allocation-accounted model that now protects the
HashBuild family. It should not create another estimator-based budget or tune a
new collection of payload multipliers.

The target has four parts:

1. one statement-attempt and CN-scoped resource generation controls exact
   query-owned allocation capacity, spill bytes, and spill file descriptors;
2. each migrated operator allocates through the existing `AllocationAccount`
   and has an explicit pressure response;
3. every data-scaled memory owner is classified as exact, explicitly bounded,
   or opaque/shared instead of being represented by a guessed exact charge;
4. statement diagnostics report bounded owner-class attribution and preserve
   enough pre-OOM evidence to explain memory that is outside the exact domain.

This is an implementation plan for #25866, not a replacement RFC for
`docs/rfcs/00000000_allocation_accounted_memory_admission.md` or
`docs/rfcs/00000000_sql_resource_accounting.md`. The two RFCs keep their
separate responsibilities:

- `AllocationAccount` authorizes and releases physical capacity;
- SQL resource accounting observes and persists terminal facts;
- process/node observation reconciles exact query-owned memory with shared and
  opaque memory, but does not fabricate query ownership.

There will be no shadow compatibility ledger. During migration a physical byte
has one safety owner. An operator switches to allocation-accounted control only
after its retained state, pressure response, and cleanup contract are closed.

## Completion target

For every data-scaled execution owner in the supported scope, exactly one of
the following must be true:

1. its live physical capacity is allocation-accounted and released by the
   physical owner;
2. its scratch is structurally bounded by a documented constant or owns an
   exact-capacity lease with one release path;
3. its state is externalized/spilled under bounded memory, disk, FD, queue, and
   depth limits;
4. it is an explicitly named opaque or shared domain with a quality flag and a
   process-level observation source.

"Not counted" is not a fifth state. Likewise, an estimated map entry size,
logical `Batch.Size`, SQL type maximum, or sampled RSS value is not an exact
allocation fact.

The safety contract remains:

```text
admit actual retained capacity
    -> execute in memory
    -> on typed pressure: reclaim / spill / reduce / degrade
    -> retry only after measurable progress
    -> otherwise return one controlled resource error
```

No supported path may escape the contract by raising a soft threshold, and no
optimizer improvement may be used as the only OOM defense.

## Current code facts

### Reusable foundation

| Area | Current implementation | Decision |
| --- | --- | --- |
| Physical ownership | `pkg/common/mpool/allocation_account.go` and account-aware MPool alloc/grow/free | Keep unchanged as the primitive |
| Provenance | every accounted allocation carries bounded owner/site metadata | Reuse; make owner IDs repository-wide before adding families |
| Allocate-copy-free overlap | MPool growth admits the actual replacement capacity while the old capacity remains live | Reuse; do not reintroduce multipliers |
| Pressure classification | capacity, lifecycle, ownership, allocator, disk, and FD failures are distinct | Generalize names, preserve semantics |
| Statement lifecycle | `pkg/sql/compile/allocation_account_lifecycle.go` installs one attempt account before `Prepare`, handles runtime clones, and validates terminal zero | Reuse for every migrated operator |
| Remote lifecycle | remote statement groups keep shared objects alive until all fragments on one CN quiesce | Preserve without adding an operator-specific remote protocol |
| Statement observation | `pkg/util/resource/summary.go` separates MPool-domain totals from diagnostic allocation-account totals | Extend with bounded owner-class facts; never add the same bytes twice |
| Allocation profile | `pkg/common/mpool/mpool_profile.go` already uses a fixed `[owner][site]` sample table for accounted allocations | Reuse for on-demand site detail |

The low-level account is already generic. At the M0 baseline
`494d77c8443b`, the remaining specialization was above it:

- `pkg/vm/process/hashbuild_budget.go` names the query/CN memory, spill-disk,
  and spill-FD controller after HashBuild;
- `Compile.ensureAllocationAccountLifecycle` obtains that controller through
  `Process.GetHashBuildBudget`;
- materialized CTE spill already borrows HashBuild's disk/FD budget, proving
  that the controller is serving a wider execution role;
- the user-facing pressure response still belongs to each operator and must
  remain operator-specific.

### Observability boundary today

`OperatorStats.MemorySize` is a producer-local diagnostic. Calls to
`Analyzer.Alloc` accumulate values and calls to `SetMemUsed` retain a maximum;
neither is a physical live-byte ledger. Statement billing instead uses MPool
domain summaries and allocation-account terminal snapshots.

The statement MPool peak epoch does not automatically include independent
operator pools. In particular, `Group` creates `group_mpool` and `MergeGroup`
creates `merge_group_mpool`, then deletes those pools in operator cleanup.
Their peak is visible only through the operator diagnostic unless their
allocations are attached to the statement account. This is both a control and
an attribution gap.

The current account terminal snapshot reports the total generation peak and a
single live owner/site on invariant failure. It does not report the live/peak
breakdown of a successful generation. The existing owner/site profile can
provide site detail when profiling is enabled, but it is not a durable
always-on owner summary.

### Major retained-state inventory

This inventory is ordered by reachable memory risk and reuse value, not by
package name.

| Owner | Current behavior | Gap | Planned boundary |
| --- | --- | --- | --- |
| HashBuild and join family | exact allocation account, query/CN cap, bounded shuffle spill, resident-only broadcast failure | controller and metrics are HashBuild-named; owner breakdown is not persisted | preserve behavior while extracting the generic controller |
| Group / MergeGroup | dedicated MPool; H8/HStr spill uses a local threshold; three repartition passes | no statement hard account; DISTINCT raises threshold to 1 TiB; max-depth data may remain resident; disk/FD and Go scratch are not under the shared generation | first new exact operator family |
| Aggregate executors | many result vectors use MPool; some implementations retain Go slices, maps, objects, or buffers | `AdditionalMemorySize` is observation, not admission; DISTINCT, JSON, median, percentile, window, and ordered concatenation have different ownership behavior | classify each aggregate and convert or bound data-scaled Go state before claiming Group closure |
| MergeOrder | mature multi-run spill, cancellation checks, fan-in 32 | logical batch-size threshold is the safety proxy; readers allocate 4 MiB Go buffers; open runs have no shared disk/FD tokens | exact retained batches/buffers plus generic spill resources |
| Top | spills when SQL LIMIT exceeds 16,384 | raw temp file, whole-batch marshal buffer, per-batch/index/reference Go slices, and whole-record `make([]byte, size)` on read | exact bounded top state and chunked spill decode |
| Fill | externalizes unflushable state | raw files/buffers and data-scaled Go snapshots are outside the shared budget | migrate after order operators using the same spill primitives |
| Materialized source | retains at most 64 MiB / 4096 batches, then spills | memory scratch uses approximate `CTEMemoryBudget`; disk/FD use HashBuild budget | use the generic generation for actual memory, disk, and FD capacity |
| Recursive CTE | `cteaccount` charges retained `Batch.Allocated` plus a logical-payload precharge against a separate 1 GiB budget | deliberately approximate second safety ledger; no common owner attribution | migrate actual batches to the statement account, retain only an operator spill/error policy, then delete the duplicate ledger |
| INTERSECT / INTERSECT ALL / MINUS | MPool hash table plus Go counter/selection slices | no spill and no statement hard account | exact hash/counter ownership; controlled failure first, external algorithm separately |
| Sample / recursive merge state | MPool hash tables and retained batches plus Go control slices | group/cardinality growth is not covered by a common account | migrate after the set operators |
| Fulltext top-K | spillable fixed-byte partitions plus node heap/mpool sampling and estimated per-document map cost | `agghtab`, `docLenMap`, `docIDMap`, score keys, and result maps remain Go-heap structures; #25638 remains open | partition/externalize side maps or move them to an allocator with physical hooks; sampled node gating remains a fallback, never an exact query charge |
| DML/delete/external/index paths | several retained maps, writer buffers, delete indexes, and native/GPU allocations | mixed MPool, Go heap, library, shared cache, and native ownership | inventory and migrate by concrete incident; native domains require allocator hooks |
| Fileservice/cache/runtime | shared cache, SDK buffers, Go runtime, allocator fragmentation | cannot be assigned exactly to one query | observe as separate shared/opaque domains and reconcile with RSS |

The inventory deliberately distinguishes "has spill code" from "is memory
safe". A spill threshold based on logical bytes is an early policy switch. The
hard allocation account is the safety boundary.

## Invariants

### I1. One physical owner

Every accounted allocation has one account, owner class, allocation site, and
release path. Views borrow. Copies allocate new capacity. Handoffs move the
physical object without changing its immutable provenance.

### I2. One hard memory fact

At every quiescent checkpoint:

```text
account.used == sum(live accounted physical capacities)
account.used == sum(owner-class current capacities)
```

An optional exact-capacity scratch lease may participate only when the
allocation API exposes the real capacity and the owner can release it exactly
once. A sampled or predicted value cannot participate.

### I3. Separate resources

Memory capacity, spill bytes, open FDs, spill depth, and queued work are
separate ledgers. A memory retry cannot consume unbounded disk or FDs, and a
disk/FD failure cannot masquerade as a reason to shrink an input batch.

### I4. Pressure requires a response

Installing an account on an operator without handling capacity failure is not
completion. Each operator declares one of:

- reclaim and retry;
- spill/externalize and retry;
- reduce an unpublished work unit and retry;
- degrade an optional structure;
- controlled terminal error.

A retry is legal only after retained capacity decreases, a spill epoch
advances, an input unit becomes smaller, or optional state is disabled.

### I5. Terminal zero is mandatory

Success, error, cancellation, timeout, client disconnect, failed prepare,
prepared reuse, retry, and partial remote failure all end with zero memory,
disk, and FD ownership for the generation. A mismatch is an invariant failure,
not normal capacity pressure.

### I6. Attribution is bounded

Always-on attribution uses fixed owner classes. Allocation sites are rendered
only in errors, terminal invariant evidence, and the existing allocation
profile. Query IDs, SQL, table names, tenants, and dynamic operator IDs do not
become metric labels.

The first implementation attributes exact capacity to
`statement attempt / CN / owner class / allocation site`. It does not claim an
exact physical-operator-instance peak when a plan contains multiple instances
of the same class. Existing physical-plan node statistics remain the instance
level diagnostic. A bounded per-attempt instance slot may be added later only
if class/site evidence is demonstrably ambiguous.

### I7. Observation does not authorize allocation

RSS, Go heap, cache size, logical batch size, and historical operator peaks may
drive alerts, early spill hints, or optimizer decisions. They never create or
release an exact physical charge.

### I8. Hot-path cost is allocation-boundary only

There is no per-row or per-batch accounting loop. Exact counters change only
on physical alloc/grow/free or on explicit scratch lease acquire/release.
Steady-state reuse with sufficient capacity adds no accounting operation.

## Target architecture

```text
Compile attempt on one CN
  -> ExecutionResourceGeneration
       -> query/CN memory controller
       -> spill-disk controller
       -> spill-FD controller
       -> AllocationAccount
            -> HashBuild owner/sites
            -> Group owner/sites
            -> Order/Top owner/sites
            -> CTE/Set/... owner/sites

Terminal observation
  -> exact account total + bounded owner-class totals
  -> MPool domain totals without double-counting account bytes
  -> operator diagnostics and spill facts
  -> shared/opaque process domains + quality flags
```

### Generic execution resource generation

The existing HashBuild budget implementation becomes the generic execution
resource controller only when the first non-HashBuild consumer is integrated.
The migration is a behavior-preserving rename/extraction, not a wrapper around
the old controller:

| Current | Target |
| --- | --- |
| `HashBuildBudget` | `ExecutionResourceBudget` |
| `HashBuildBudgetGeneration` | `ExecutionResourceGeneration` |
| `GetHashBuildBudget` | `GetExecutionResourceBudget` |
| `HashBuildBudgetComponent` | `ExecutionResourceComponent` |
| `ResolveHashBuildCeiling` | `ResolveExecutionMemoryCeiling` |

The target generation still implements `mpool.AllocationCapacityController`.
HashBuild keeps its operator-specific pressure messages and recovery leases;
those do not belong in the generic controller.

The migration updates `Process.Base`, compile lifecycle, materialized spill,
metrics, and tests in one mechanical PR. It leaves no permanent aliases and no
second set of counters. A separate commit in that PR may contain the mechanical
rename so behavioral review remains readable.

### Repository-wide owner catalog

Before a second family uses the account, owner IDs become a central bounded
catalog beside the owning type in `pkg/common/mpool`. Today multiple packages
independently use owner value `1`; that is adequate inside the single HashBuild
domain but is not a repository-wide attribution contract.

The catalog assigns stable classes such as HashBuild, Group, Order, Top, CTE,
Set, Fulltext, and DML. Allocation-site values remain private to the owner
class and are stable numeric provenance, not a second repository-wide name
catalog. Tests enforce:

- implemented owner IDs are unique, non-zero, and at most
  `AllocationOwnerCatalogMax`; the existing `AllocationOwnerMax` bound keeps
  unknown IDs serializable during rolling upgrades;
- numeric sites are unique within an owner; shared owner ranges have a complete
  non-overlap ledger;
- every account-aware constructor receives a non-zero owner/site selection;
- an unknown numeric value is still serializable during rolling upgrades.

The catalog is a small constants file and tests, not a runtime registry. M2
renders the stable owner name plus numeric site; human-readable site names can
be added later without changing the wire or profile identity.
Package-local colexec owner constants are implementation details rather than a
supported external API; migration removes them instead of retaining aliases
that could become a second catalog.

### Owner-class current and peak facts

`AllocationAccount` gains fixed-size per-owner current and peak counters. They
are updated in the same allocation transaction as the total account at
physical allocation boundaries. The counters are observational; total account
state remains the sole admission and release ledger.

Terminal export contains at most 63 sparse owner summaries. Remote and retry
merge follows the same peak semantics as MPool domains:

- current/live capacities are additive and must be zero at terminal;
- max owner-domain peak is a lower bound on the statement's largest owner;
- sum of independent owner peaks is an upper bound, not a synchronized peak;
- overflow or an owner-sum invariant failure sets a resource quality flag.

Statement summaries retain an immutable sparse owner set, so an owner-less
statement pays one pointer instead of a 63-entry table; merge is copy-on-write
only at terminal reduction. Plan export performs an explicit semantic clone.
During rolling upgrades, a pre-owner remote total remains usable but carries
`QualityPartial | QualityMissingAllocationOwner` through every newer hop. A
version that promises owner facts but omits or fabricates them is an invariant
failure rather than silently exact attribution.

Site-level always-on counters are intentionally omitted. The existing fixed
owner/site allocation profile provides site detail without a per-query dynamic
map, and allocation failures already include numeric owner/site provenance.

### Operator migration contract

Every operator family follows the same sequence:

1. enumerate retained MPool, Go-heap, native, file, FD, queue, and transfer
   owners;
2. assign stable owner/site constants;
3. install the immutable account before `Prepare` and before the first owned
   allocation;
4. route MPool vectors, batches, hash tables, buffers, and expression results
   through account-aware constructors;
5. convert data-scaled Go storage to MPool/accounted buffers, give it an exact
   capacity lease, externalize it, or declare it opaque with an explicit bound;
6. handle typed capacity pressure with monotonic progress;
7. move spill files to the generic disk/FD reservations;
8. prove success/error/cancel/reset/free terminal zero;
9. remove the former estimator or duplicate budget from the hard path;
10. benchmark the resident path before activation.

Implementing `SetAllocationAccount` is the activation boundary. An operator
must not implement it merely to collect partial telemetry while unbounded
retained state can still bypass the account.

## Milestones and PR boundaries

### M0. Freeze the inventory and baseline

Status (2026-08-11): complete at baseline `494d77c8443b`; reproducible evidence
is stored in `docs/design/evidence/25866_m0_execution_resource_baseline.md`.

This document is the initial inventory. Before changing a family, its PR adds a
focused ownership table covering every data-scaled field in that family and a
baseline benchmark for its resident path. The table may live in the PR body or
next to the package tests; it does not require a runtime registration
framework.

The M1 baseline is recorded in
`docs/design/evidence/25866_m0_execution_resource_baseline.md`.

Baseline artifacts include:

- account alloc/grow/free nanoseconds and allocations per operation;
- operator resident throughput and allocations per operation;
- forced-pressure behavior and terminal resource snapshot;
- current operator `MemorySize`, MPool peak, spill bytes/rows, and relevant
  Go-heap profile for the same deterministic input.

### M1. Generalize the controller without changing behavior

Status (2026-08-11): locally complete on baseline `494d77c8443b`. The final
normal/race/build/vet matrix and adjacent baseline/candidate performance
comparison are recorded in
`docs/design/evidence/25866_m0_execution_resource_baseline.md`. M2 is the next
implementation boundary; no M2 attribution state is included in M1.

Primary files:

- `pkg/vm/process/execution_resource_budget.go` and tests, renamed from
  `hashbuild_budget.go`;
- `pkg/vm/process/execution_recovery_capacity.go`;
- `pkg/vm/process/types.go` and process cleanup;
- `pkg/sql/compile/allocation_account_lifecycle.go`;
- `pkg/sql/compile/compile.go` materialized spill wiring;
- `pkg/util/metric/v2/execution_resource.go`, HashBuild's remaining
  operator-specific metric, and metric registration.

Deliverables:

- one generic execution resource generation;
- unchanged HashBuild caps, recovery behavior, spill behavior, error
  classification, and operator recovery actions;
- controller sentinel wording and the budget metric series become generic in
  the same change; old and new metric series are not emitted in parallel;
- materialized spill obtains disk/FD services through generic names; its
  approximate memory reservation remains unchanged until M5;
- no old/new controller aliases or duplicate metrics remain;
- all existing local/remote/prepared/retry tests pass unchanged in behavior.

This PR is mostly mechanical. It must not also migrate Group.

### M2. Add bounded owner attribution

Primary files:

- `pkg/common/mpool/allocation_account.go` and MPool tests;
- `pkg/common/mpool/mpool_profile.go`;
- `pkg/common/mpool/allocation_owner.go`, the central owner constants file;
- `pkg/util/resource/summary.go`;
- compile remote terminal serialization and physical-plan export.

Deliverables:

- exact fixed owner current/peak counters at allocation boundaries;
- quiescent `account.used == sum(owner.used)` validation;
- bounded remote/retry aggregation and quality flags;
- stable owner text and numeric site provenance in controlled errors and
  profile evidence;
- no change to hard admission decisions.

Performance gate: account-aware alloc/grow/free must not add a Go allocation,
lock, stack walk, or dynamic map lookup. A statistically significant resident
HashBuild regression above 2% blocks the change unless independently explained.

### M3. Migrate Group and MergeGroup

Status (2026-08-12): the account-aware storage, spill, recovery, and lifecycle
implementation is complete for the Group aggregate capability matrix below.
`Group` and `MergeGroup` implement the compile-time allocation-owner contract,
so compile attaches the statement account before `Prepare`; there is no
aggregate-dependent shadow execution path. Local correctness, failure-path,
lifecycle, and resident-performance evidence is recorded in the M3 evidence
document.

Within `aggexec`, `GroupAggFuncExec` is the single static contract for Group's
allocation ownership, capacity preflight, bounded spill codec, decoded group
count, extra-memory observation, and prepared-parameter state. Group constructs
and binds that interface once through its Group-specific factory, then keeps
typed resident/reload slices; execution does not repeat capability assertions
or allocate a throwaway MergeGroup probe.

Group and MergeGroup also use one transactional hash preview/commit path for
both accounted execution and direct compatibility callers. The preview's exact
new-group selection drives vector/aggregate preflight and publication; the old
normal-iterator insert, reconstructed insert bitmap, and worst-case duplicate
preallocation path have been removed. Compatibility differences are confined
to allocation ownership and the existing spill/wire boundary, not a parallel
grouping algorithm.

This is the highest-value new family because Group already has a spill
algorithm and dedicated MPool isolation, but not a statement hard boundary.

Primary files:

- `pkg/sql/colexec/group/exec2.go`, `helper.go`, `types2.go`, and
  `mergeGroup.go`;
- `pkg/sql/colexec/aggexec` constructors and retained state;
- `pkg/common/hashmap` / `pkg/container/hashtable` account-aware constructors;
- `pkg/sql/colexec/evalExpression.go` for an account-aware `ExprEvalVector`;
- shared spill buffer/reader utilities.

Required changes:

1. attach Group and MergeGroup to the compile attempt before `Prepare`;
2. account hash cells/descriptors, group-key batches, aggregate MPool vectors,
   expression results, spill decode/reload batches, and marshal/coalesce
   buffers;
3. classify every aggregate implementation as fixed/bounded, allocation
   accounted, aggregate-owned spill, or unsupported unbounded state;
4. replace data-scaled Go state in supported aggregates with accounted storage
   or a bounded external representation;
5. use the local spill threshold only as an early hint;
6. on actual capacity failure, spill/reclaim and retry only after progress;
7. reserve generic spill disk and FD capacity before file side effects;
8. reserve bounded recovery capacity for reload/remerge just as HashBuild does;
9. at `spillMaxPass`, either finish inside admitted capacity or return a
   controlled error; never silently retain beyond the hard account;
10. remove the 1 TiB DISTINCT escape. Ordinary DISTINCT uses the exact
    argument-arena spill codec; special DISTINCT implementations without that
    representation return a controlled factory error before publishing group
    state.

Aggregate closure is a prerequisite for enabling Group hard admission. In
particular, `AdditionalMemorySize` may remain a diagnostic, but it cannot be
the hard charge for Go maps/slices whose physical capacity is unknown.

Acceptance:

- resident and forced-spill results are identical for H8 and HStr;
- grouping sets, NULLs, collation, varlen values, and prepared metadata survive
  spill/reload;
- max-depth/skew, disk full, FD rejection, I/O error, cancellation, and reuse
  all terminate and clean exactly once;
- Group allocations appear under the statement account even though the
  dedicated operator MPool is deleted;
- ordinary DISTINCT cannot OOM by bypassing the account;
- common non-spill Group performance and allocations per operation do not
  materially regress.

The M3 implementation currently has the following capability matrix. Every
aggregate family reachable through Group has either a closed accounted path
or a controlled factory rejection; window-only IDs remain an independent
operator milestone.

| Aggregate family | Accounted Group mode | Physical boundary |
| --- | --- | --- |
| `BIT_AND`, `BIT_OR`, `BIT_XOR` | non-DISTINCT | state/result vectors are allocation-accounted |
| `VAR_POP`, `STDDEV_POP`, `VAR_SAMP`, `STDDEV_SAMP` | ordinary and DISTINCT | vector state plus the allocation-accounted DISTINCT argument arena; final result vectors use the same account |
| `ANY_VALUE`, `MIN`, `MAX` | normal SQL modes | winning state/result vectors are transferred without changing provenance |
| `MAX_BY`, `MAX_BY_NON_NULL` | non-DISTINCT | three correlated vectors are preflighted transactionally; per-chunk usage metadata has a fixed three-entry bound |
| `SUM`, `AVG`, `COUNT(expr)` | ordinary and DISTINCT | vector state, DISTINCT argument arena, null bitmap, and newly materialized result vectors are allocation-accounted |
| `COUNT(*)` | non-DISTINCT | state vector is transferred without changing provenance |
| unordered non-DISTINCT `GROUP_CONCAT` | supported | retained arguments and result vectors are accounted; one reusable account-backed finalization buffer is capped at the published result length; each formatted field is independently bounded by the existing 16-bit unit limit |
| `AVG_TW_CACHE`, `AVG_TW_RESULT` | non-DISTINCT | state and materialized result vectors are allocation-accounted |
| approximate percentile | supported | retained KLL levels, exact preflight growth, finalization scratch, and result vectors use the allocation account; stable partial and spill state stream the bounded sketch wire |
| bitmap | supported | retained values use an account-owned sorted representation; portable Roaring wires are validated and decoded without data-scaled Go-heap scratch, and final wire output preserves compatibility |
| HLL/approx-count | supported | the dense p=14 register array is MPool/account owned; its fixed 16 KiB per-group capacity is allocated before group publication, spill streams the stable HLL wire, and result vectors keep account provenance |
| median | ordinary and DISTINCT | retained fixed-width arguments use the allocation-accounted argument arena; Group spill streams that state and final selection uses account-backed scratch/result vectors |
| ordered percentile (`PERCENTILE_CONT`/`PERCENTILE_DISC`) | supported | accounted Group mode reuses the exact saved-argument arena and Group spill codec; final sorting scratch and result vectors use the same allocation account, while the standalone ordered-run path remains isolated to unaccounted callers |
| JSON array/object aggregates | supported | retained payloads, DISTINCT state, merge state, binary-JSON finalization scratch, and result vectors are allocation-accounted; final encoding writes into pre-admitted storage |
| unordered `GROUP_CONCAT` | ordinary and DISTINCT | retained arguments use the exact allocation-accounted arena; DISTINCT uses its key representation directly rather than an auxiliary Go hash; finalization writes through a bounded accounted buffer |
| ordered `GROUP_CONCAT` | ordinary and DISTINCT | retained concat/order payloads, selectors, DISTINCT ordering scratch, restored order vectors, spill state, finalization buffer, and results are allocation-accounted |

Aggregate executor Go metadata is structurally bounded: one chunk descriptor
and a constant number of vector pointers per 8,192 admitted group rows. It is
not charged as guessed payload. There is no aggregate-dependent runtime
fallback: production Group and MergeGroup use the same allocation-accounted
contract for every reachable aggregate family.

Window executor IDs share the `aggexec` factory and serialization helpers, but
normal plans place them under `colexec/window`, not in `Group.Aggs`. Their
serde must remain correct, while their retained-memory migration belongs to
the window operator milestone and does not block Group activation without a
demonstrated reachable Group plan.

This milestone should be split by closed behavioral contracts: aggregate
storage primitives, Group account integration, and spill resource/lifecycle
closure. The final activation commit deletes the superseded hard-threshold
path; the same physical allocation is never tracked by two hard ledgers.

### M4. Migrate MergeOrder, Top, and Fill

These are separate PRs sharing generic utilities, not one combined rewrite.

MergeOrder:

- account retained batches, computed order columns, tail batches, output
  batches, and marshal/decode storage;
- replace 4 MiB `bufio.Reader`/writer Go buffers with bounded accounted buffers
  or the existing accounted spill reader;
- give each run a growable disk token and each open file an FD token;
- compact runs incrementally before FD admission fails, keeping fan-in and run
  metadata bounded;
- retain `spillMemUsage + Batch.Size` only as an early hint.

Local status (2026-08-13): the MergeOrder slice is implemented and locally
closed; its ownership inventory, fault/lifecycle matrix, terminal-zero checks,
and resident benchmark are recorded in
`docs/design/evidence/25866_m4_mergeorder_baseline.md`. The independent Top
slice is also locally closed and recorded in
`docs/design/evidence/25866_m4_top_baseline.md`. The final independent Fill
slice is also locally closed and recorded in
`docs/design/evidence/25866_m4_fill_baseline.md`.

Top:

- account key vectors and bounded top rows;
- reserve capacity for `sels`, `rowRefs`, `orderedRefs`, and batch indexes by
  actual backing capacity or move them to MPool slices;
- replace raw `os.CreateTemp` with the query spill file service and disk/FD
  tokens;
- replace whole-record `make([]byte, info.size)` with bounded streaming decode;
- bound spill-index metadata independently of the number of input batches.

Local result: the old spill index and duplicate ordered-reference slice are
deleted. Accounted typed slices own selections and direct record references;
the query spill service owns one admitted file; the stable streaming Batch
codec preserves prepared provenance while rejecting spill Attrs/ExtraBuf; and
the output-chunk map is bounded at 8,192 rows. Resident performance remains
inside the 2% gate.

MergeTop, used as the final Top stage for multi-scope plans, now shares the
same `top` owner without inheriting an unaccounted resident bypass. Retained
rows, computed order expressions, heap selections, append rollback scratch,
and final Shuffle replacement capacity are admitted by their physical
allocations. The duplicate final permutation slice is removed by draining the
heap into its own selection backing. MergeTop has no external algorithm, so
unsupported pressure returns one controlled resource error and Reset closes
the prepared-statement allocation generation.

Fill:

- account replay/linear state and partition snapshots;
- replace raw marshal buffers with accounted buffers;
- assign disk/FD ownership to input/output/next files;
- preserve its forward/reverse replay semantics under cancellation and error.

Local result: retained batches, coordinates, endpoint values, replay batches,
and partition-key payloads use the Fill owner; a 1,024-batch structural bound
closes pointer metadata; streaming serialization removes the whole-record
buffer and borrowed child batches remove the spill-time payload duplicate;
each input/output/next file owns admitted FD and disk tokens. Exact pressure
spills retained progress and streams the rejected child only after memory has
been released. The resident benchmark improved 1.62%, and lifecycle/fault
tests close memory, disk, and FD ownership at zero.

All three must retain stable result ordering and prepared-value provenance.

### M5. Remove the duplicate CTE memory safety ledger

Materialized source and recursive CTE are migrated in separate changes.

For materialized source:

- cloned in-memory batches allocate under the statement account;
- the 64 MiB / 4096-batch limits remain spill policy, not a second physical
  ownership ledger;
- marshal/read scratch uses actual-capacity account-aware buffers;
- disk and FD reservations use the generic execution generation.

For recursive CTE:

- retained batches and maps allocate under CTE owner/sites;
- replacement admits the real replacement allocation overlap rather than
  `batchLogicalPayload`;
- convergent state may be spilled through the materialized-source mechanism
  where semantics permit;
- otherwise actual capacity pressure returns the existing actionable CTE
  error;
- after all callers migrate, delete `pkg/vm/process/cte_memory_budget.go` and
  `pkg/sql/colexec/cteaccount` as physical safety ledgers. If
  `cte_max_memory_bytes` remains, it is an operator policy ceiling evaluated
  from exact owner capacity, not separately reserved bytes.

The final state has one physical charge, one release, and one statement/CN
safety controller.

### M6. Migrate set and other hash-retaining operators

Start with `INTERSECT`, `INTERSECT ALL`, and `MINUS`:

- construct account-aware hash maps before their first allocation;
- move cardinality-scaled counters and flags from Go slices to accounted
  storage;
- account result buffers and expression keys;
- return a controlled capacity error when no external algorithm exists.

External set algorithms are a later availability/performance change and must
be reviewed separately. A controlled error is safer than adding an unproven
generic spill state machine.

Then apply the same contract to Sample, recursive merge, pre-insert uniqueness,
and other retained hash/batch owners selected by measured incidents. Each PR
must remove an old unsafe path rather than only add telemetry.

### M7. Close fulltext and opaque/native boundaries

The current fulltext checks in `pkg/fulltext/fixedbytepool.go` are useful node
headroom guardrails, but `MapMemPerItem`, periodic heap sampling, and
`ChargeSideBytes` are not exact query allocation ownership.

The fulltext closure should:

- partition or externalize `agghtab`, `docLenMap`, `docIDMap`, score traversal,
  and result state;
- use allocator-backed key/value storage where exact physical capacity can be
  admitted;
- bound unspill working sets, disk bytes, FDs, partition metadata, and top-K
  candidate storage under one generation;
- remove per-document estimates from hard query rejection once the real owners
  are controlled.

Native CPU/GPU vector-index memory enters the exact domain only after its
allocator exposes alloc/grow/free capacity hooks with one ownership contract.
Until then it is a named native/opaque domain with process-level metrics and a
quality flag. Adding only visible Go wrappers would claim false coverage.

Shared fileservice cache residency also remains outside query ownership. A
query may report bytes read, cache hits, and attributable request buffers, but
must not be charged the entire shared cache entry or cache target.

### M8. Preserve pre-OOM attribution and feed the optimizer

Terminal accounting alone cannot explain a process killed before cleanup.
After owner-class facts exist:

- persist a lightweight statement-start record before execution and finalize
  success/error/cancelled/aborted-by-restart states;
- have an external or process-level watcher capture RSS, global MPool, Go heap,
  cache/native domains, active generation caps/current/owner peaks, and the
  accounted allocation profile at staged pressure thresholds;
- write the evidence outside the pod and rate-limit it per process/threshold
  epoch;
- emit at most one terminal allocation summary per failed operator/query;
- keep allocation and cleanup paths free of synchronous logging and profile
  generation.

Process reconciliation uses non-overlapping domains at approximately the same
time:

```text
RSS sample
  = exact query-owned allocator domains
  + unaccounted allocator domains
  + Go heap/runtime domains
  + shared cache/native domains
  + measured residual
```

If sources overlap or timestamps differ materially, the summary is marked
partial; the residual is not forced to zero.

The optimizer work in #26768 may consume historical actual owner peaks, spill
bytes, and estimate/actual ratios to choose safer plans. Those observations do
not become executor hard admission. Optimizer mistakes remain bounded by the
runtime account.

## Milestone exit gates

The milestones are executable now, but they are not accepted by code review or
a green broad CI run alone. Each milestone closes only after the evidence below
is recorded against the exact candidate commit. M0 is the first implementation
task; production edits start after its baseline for the family being changed.

Every code milestone has the following common gate:

1. name every changed package and direct dependent package, then prove the
   patterns are non-empty with `go list`;
2. pass focused build, vet, unit tests, and the relevant race tests with a real
   exit status and no surviving test process;
3. exercise success, error, cancellation, Reset/Free, retry/reuse, and partial
   initialization for every changed owner or resource;
4. verify terminal memory, disk, and FD ownership is exactly zero;
5. compare focused resident benchmarks and `allocs/op` with the M0 baseline;
6. inspect the complete diff and prove that the milestone's subtraction item
   was removed rather than hidden behind a compatibility switch.

The stage-specific exit evidence is:

| Stage | Required evidence | Rejection condition |
| --- | --- | --- |
| M0 | ownership table for the selected family; reproducible commands, candidate/base commits, toolchain, focused resident benchmark, forced-pressure result, terminal snapshot, and relevant heap/profile evidence | an unclassified data-scaled field, an unrepeatable baseline, or a test that does not expose its real exit status |
| M1 | unchanged HashBuild/query/CN caps and recovery decisions under table-driven boundary tests; unchanged local/remote/prepared/retry results; all old controller names, aliases, and duplicate metrics absent; resident benchmark within the 2% gate | any changed pressure decision, compatibility controller, duplicate counter, or unexplained resident regression |
| M2 | alloc/grow/free/concurrent-free tests prove total current equals the sum of owner currents; owner peaks are monotonic; remote/retry aggregation and unknown-owner serialization are bounded; microbenchmarks show no new Go allocation, lock, stack walk, or dynamic map lookup | owner totals can diverge, attribution changes admission, cardinality is dynamic, or HashBuild exceeds the performance gate |
| M3 | resident-versus-forced-spill differential tests for H8/HStr and every supported aggregate; exact-cap and one-byte-short tests; skew, DISTINCT, max-pass, disk/FD failure, corrupt spill, cancellation, Reset/Free, and reuse; account/disk/FD terminal zero | any aggregate retains unbounded bypass state, DISTINCT still escapes through 1 TiB, max-pass can retain beyond the account, or a retry makes no measurable progress |
| M4 | separate evidence for MergeOrder, Top, and Fill: stable results/order, many-run and multi-pass spill, bounded buffer/index/run metadata, disk/FD admission before side effects, short I/O/corruption/cancellation cleanup, and resident benchmark | whole-record or unbounded Go-buffer growth remains, files bypass disk/FD ownership, result order changes, or the three operators are coupled into one non-revertible activation |
| M5 | materialized-source and recursive-CTE tests separately prove exact retained/replacement capacity, spill/error behavior, recursion convergence, cancellation/reuse, and terminal zero; repository search shows the approximate CTE ledger no longer authorizes physical memory | logical payload or a multiplier still creates/releases a physical charge, the same allocation has two safety ledgers, or deletion of the old ledger changes a successful in-budget result |
| M6 | set-operation differential tests cover empty, NULL, duplicate-heavy, high-cardinality, fixed/varlen, exact-cap, and one-byte-short inputs; hash and counter capacity are accounted; unsupported pressure returns one controlled error and cleans to zero | Go counter/flag capacity remains unbounded, a controlled-error-only implementation claims spill availability, or pressure can reach allocator OOM first |
| M7 | fulltext incident and adversarial-cardinality tests bound or externalize side maps and top-K state; exact allocator owners pass lifecycle tests; opaque/native/shared domains carry explicit quality flags; hard query rejection no longer uses estimates for migrated state | estimated per-item memory is presented as exact ownership, an unbounded side map remains, or native/shared memory is double-charged to queries without allocator hooks |
| M8 | a deterministic process-termination test leaves an externally readable start/pressure record; success/error/cancel/restart states reconcile non-overlapping domains; timestamp/overlap gaps set partial quality; rate-limit and benchmarks prove no synchronous logging/profile work on allocation paths | evidence disappears with the process, residual is forced to zero, labels are unbounded, or observation adds admission or hot-path I/O |

The expensive distributed benchmark is a consolidated release gate, not a
debug loop. It is triggered only after the relevant local correctness, race,
fault-injection, lifecycle, and performance gates converge. Its role is to
confirm #25782/#25127/#25638-class workloads, concurrent-statement fairness,
spill behavior, and end-to-end performance against the pinned main baseline;
it does not replace any stage-specific proof above.

## Subtraction plan

The work must simplify the current system as it expands coverage. Each
milestone has explicit deletions:

| Milestone | Code removed or demoted |
| --- | --- |
| M1 | HashBuild-specific controller/type/getter names and duplicate resource metrics |
| M3 | Group's duplicated auto-threshold formula as a safety boundary; 1 TiB DISTINCT bypass; max-depth unbounded retention; hard use of `AdditionalMemorySize` |
| M4 | MergeOrder logical-byte threshold as safety gate; raw Go spill buffers; Top raw temp/whole-record read path; unbudgeted run files |
| M5 | approximate CTE physical reservation and logical-payload precharge; HashBuild-named spill borrowing |
| M6 | unaccounted Go counter/flag slices in migrated set operators |
| M7 | fulltext per-item/map estimates as hard query ownership after real state is controlled |

Existing `OperatorStats.MemorySize` may remain as a display diagnostic, but no
migrated operator uses it for admission or releases a physical charge from it.

No milestone is complete if it leaves both an estimate ledger and an exact
ledger authorizing the same allocation.

## Validation matrix

### Allocation and lifecycle properties

- exact-cap and one-byte-short alloc/grow/replacement boundaries;
- failed reserve, failed physical allocation, failed publish, and failed
  transfer roll back without changing live ownership;
- concurrent allocation/free preserves total and owner sums;
- cross-MPool free releases the original owner/site exactly once;
- Reset preserves only explicitly reusable capacity; Free returns it;
- prepared execution, retry, and remote generations cannot capture each
  other's storage;
- cancellation/error/panic cleanup reaches memory/disk/FD zero;
- terminal non-zero state produces one invariant snapshot and suspends unsafe
  reuse until a late physical free drains the tombstone.

### Operator differential tests

For every spill-capable operator:

- resident result equals forced-spill result;
- empty, NULL, fixed-width, varlen, wide-row, severe skew, duplicate-heavy,
  and many-partition inputs;
- exact threshold, exact hard cap, and one-byte-short hard cap;
- multi-pass spill/reload and minimum indivisible input;
- disk full, FD exhaustion, short read/write, corrupt/truncated input, and
  cancellation at every phase boundary;
- Reset/Free/reuse after success and after every injected failure;
- remote execution and mixed local/remote ownership where the operator is
  dispatchable.

### Incident regressions

- #25782-style join/group expansion cannot exceed the finite statement/CN
  account;
- every false-budget regression listed by #26459 remains accepted when its real
  working set fits;
- #25127-style CTAS/INSERT SELECT reports exact controlled MPool owners and
  separate cache/Go/native domains;
- #25638 fulltext either completes inside the declared envelope or returns a
  controlled error without node OOM;
- concurrent large statements share the CN cap without starving unrelated
  small statements.

### Performance gates

- no new Go allocation, lock, channel, stack walk, or logging call per row;
- steady-state reuse does not call admission when no physical growth occurs;
- owner counters add fixed atomic work only at physical allocation boundaries;
- resident `allocs/op` does not increase;
- a reproducible non-spill regression above 2% requires explanation and blocks
  the relevant activation;
- spill performance is reported separately from resident performance;
- one consolidated end-to-end benchmark is run only after local correctness,
  race, fault-injection, and package benchmarks converge, rather than using an
  expensive remote workflow as the debug loop.

## Rollout and issue ownership

#25866 remains the end-to-end umbrella and acceptance contract.

- #26459 / #26531 are the completed allocation-account foundation;
- #26563 owns spill topology, bounded lifecycle, and pressure-response policy;
- #3433 owns HashAgg/HashJoin spill capability, with Group work implemented as
  focused subissues;
- #25127 owns CTAS/INSERT SELECT and shared/cache/remote accounting evidence;
- #25638 owns fulltext's non-spillable side state;
- #26768 owns optimizer decisions based on resource evidence, not executor
  admission.

Each milestone should have focused subissues and independently revertible PRs.
No single PR should combine controller extraction, a new operator family,
optimizer changes, and durable pre-OOM storage.

## Definition of done

This plan is complete when:

- HashBuild and every migrated family share one generic statement/CN resource
  generation and one allocation-owned memory path;
- Group/HashAgg, order/top/fill, CTE/materialized, set operators, and the
  incident-driven fulltext path have explicit bounded outcomes;
- every data-scaled owner in the maintained inventory is exact, bounded,
  externalized, or explicitly opaque/shared;
- statement diagnostics identify the dominant exact owner classes and expose
  missing/partial coverage without double-counting;
- pre-OOM evidence survives process death and reconciles exact allocator
  domains with Go/cache/native/residual domains;
- all lifecycle, fault, concurrency, remote, incident, and performance gates
  pass;
- superseded estimator gates, duplicate budgets, unbudgeted spill resources,
  and threshold bypasses listed in the subtraction plan are deleted.
