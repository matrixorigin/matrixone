# Bounded exact DISTINCT-key spill

Status: in review; implementation and validation ready for independent approval

Design version: 2 (2026-09-03)

Owner: MatrixOne query execution

Owning issue: [#27698](https://github.com/matrixorigin/matrixone/issues/27698)

Related partition-parallel work:
[#27720](https://github.com/matrixorigin/matrixone/issues/27720)

Implementation PR: [#28003](https://github.com/matrixorigin/matrixone/pull/28003)

## 1. Decision

MatrixOne will add an execution-local, Group-owned spill path for exact
`COUNT(DISTINCT ...)` argument sets. The existing arena skiplist remains the
normal representation. When its accounted state reaches the Group spill
threshold or a retryable capacity rejection, Group drains resident canonical
`(group key, DISTINCT arguments)` records into a bounded radix spool and
continues consuming input.

An intermediate Group never turns a local exact set into a scalar count. It
reads one bounded leaf at a time and emits exact-key subsets through the
existing partial-aggregate stream, so a downstream MergeGroup can still remove
duplicates from different workers. A final Group or MergeGroup deduplicates
each disjoint key partition, emits compact per-group count contributions into
a second spool partitioned by group hash, then consolidates those contributions
with the ordinary aggregate states before producing SQL results.

This design makes a global group and a single hot group spillable by the
identity being deduplicated rather than only by the final group key. It does
not add an approximate fallback, retain a table-sized wire object, or require
the complete distinct set to be resident during reload.

Version 1 applies to exact `COUNT(DISTINCT ...)`, including multiple arguments,
mixed aggregate lists, filters, grouping sets, local partial aggregation, and
MergeGroup. Other saved-argument DISTINCT families retain their current path
until section 13's family-specific finalization contracts are designed and
approved.

## 2. Problem and evidence

Saved-argument aggregates retain exact arguments in `aggState.argSkl`. A key is
currently:

```text
[chunk-local group row: uint16 big endian][canonical argument payload]
```

`argCnt` records the number of retained arguments per group. Arena growth
allocates a replacement, copies every node, publishes the replacement, and
then frees the old arena. Allocation-account preflight makes that transition
safe, but the final resident size remains proportional to global DISTINCT NDV.

Generic Group spill cannot close this bound:

- `H0`, used by a global aggregate, explicitly never enters generic Group
  spill;
- grouped spill partitions only by the final grouping hash;
- `SaveSpillIntermediateRows` serializes every saved argument for a selected
  group; and
- `UnmarshalSpillFromReader` reconstructs that complete saved-argument state.

Consequently, all keys of one hot group remain inseparable. Recursive group
spill can subdivide different groups, but it cannot reduce the resident key set
required by that group. The controlled maximum-depth error prevents an
infinite spill loop; it does not provide bounded-memory completion.

The planner optimizations in #27693 and #27790 increase ownership for eligible
shapes and remain valuable. They do not cover every mixed, multi-argument,
statistics-poor, DOP=1, or skewed execution, and plan parallelism is not a hard
memory bound. Executor spill must therefore be correct independently of the
chosen topology.

## 3. Invariants

### 3.1 Exactness

For every aggregate group `g` and admitted non-NULL argument tuple `d`:

```text
COUNT(DISTINCT d) for g = cardinality of the SQL-equality set {(g, d)}
```

Every SQL-equal `(g,d)` record reaches one logical distinct leaf. Hash equality
is only a routing condition. Full canonical-key equality remains authoritative
inside the leaf, so hash collisions cannot merge unequal values.

### 3.2 Intermediate ownership

An intermediate Group may remove duplicates only inside the data it owns. It
must emit exact keys, not counts, unless the plan proves that it is the final
owner of those keys. Version 1 uses the conservative rule unconditionally:
intermediate output always carries exact-key subsets.

### 3.3 Final ownership

A final Group or MergeGroup may convert a deduplicated leaf to count
contributions because radix leaves are disjoint in full `(g,d)` hash space.
Contributions for the same `g` are added exactly once in the group-result
consolidation phase.

### 3.4 Boundedness

At no point may resident DISTINCT memory grow with input rows or global NDV.
The live bound consists of:

- the ordinary resident Group state below its existing threshold;
- one bounded distinct work set;
- fixed radix metadata and bounded read/write buffers;
- one bounded external-sort merge fan-in when collision fallback is active;
- one bounded group-result consolidation work set; and
- the existing explicitly reserved Group recovery capacity.

Spilled disk bytes may grow with admitted input bytes, but they are charged to
the execution spill-disk budget and fail before an unreserved write.

### 3.5 Lifecycle

Every file, descriptor token, disk token, accounted buffer, iterator, and
aggregate staging object has one effective owner. Success, input error, output
error, cancellation, corrupt/truncated spill, capacity rejection, Reset, and
Free all reach that owner's cleanup exactly once.

## 4. Goals and non-goals

### Goals

- Complete a single-global-group high-NDV `COUNT(DISTINCT ...)` below a memory
  limit that cannot hold all keys.
- Preserve exact fixed, varlen, multi-argument, NULL, grouping, signed-zero,
  constant-vector, and collision semantics.
- Support ordinary Group, intermediate Group, and MergeGroup without a
  table-sized partial state or wire frame.
- Compose with existing group spill and allocation recovery rather than create
  an independent unaccounted memory controller.
- Allocate no partition machinery on the no-spill path.
- Bound repartition depth, external-sort fan-in, open files, buffers, metadata,
  retry work, and metrics cardinality.

### Non-goals for version 1

- Approximate distinct counting or a semantic fallback.
- Changing the planner's topology selector from #27693/#27790.
- Parallelizing leaf finalization; #27720 may add that after sharing this spool
  and canonical-key contract.
- Bounded saved-argument spill for SUM, AVG, variance, median, percentile,
  JSON aggregation, or GROUP_CONCAT.
- Persisting spill files across process restart or query retry.
- Treating spill as an upgrade-stable on-disk format.

## 5. Standards and precedent

SQL defines the exact DISTINCT result but does not prescribe a physical spill
algorithm or execution-local file format. The relevant precedent is therefore
query-execution design, not an interoperability standard.

[Robust External Hash Aggregation](https://duckdb.org/pdf/ICDE2024-kuiper-boncz-muehleisen-out-of-core.pdf)
uses local pre-aggregation, radix partitioning, and partition-wise finalization
so a complete output hash table need not remain resident. MatrixOne adopts the
same divide-and-conquer property, but its saved DISTINCT arguments and
Group/MergeGroup partial protocol require exact keys to survive intermediate
boundaries rather than finalizing every local partition immediately.

Graefe's
[Query Evaluation Techniques for Large Databases](https://web.stanford.edu/class/cs346/2014/graefe.pdf)
describes recursive hash partitioning, bounded external merge fan-in, and the
need for a sort-based fallback when skew or hash behavior prevents partition
progress. Version 1 applies those established bounds through depth-first radix
ownership and a fixed-fan-in external-sort fallback.

MatrixOne's existing Group spill supplies file service, accounting, recovery,
cancellation, and group-hash consolidation primitives. The selected design
extends those owners instead of importing another aggregate framework or
copying an engine-specific page layout.

## 6. Applicability and activation

The path is available when all of the following hold:

- Group has a hard allocation account and execution spill service;
- at least one aggregate is exact `COUNT(DISTINCT ...)` backed by saved
  arguments;
- argument admission uses the canonical exact-key grammar in section 7; and
- the execution spill FD and disk budgets admit the first spill wave.

Normal admission remains unchanged until either:

1. the retained DISTINCT bytes make `memUsed()` cross the configured Group
   spill threshold; or
2. aggregate preflight returns a retryable allocation-capacity rejection and
   at least one resident distinct key can be drained.

On activation, Group allocates the spool, drains all resident keys of eligible
aggregates, releases their arenas, and retries the same unpublished input work
unit. Later keys accumulate only up to one bounded resident work set before the
next drain. The activation decision is sticky for the execution generation;
Reset and reuse start again on the normal path.

For grouped state, capacity recovery chooses the action which has measurable
forward progress:

- drain eligible distinct keys when they own reclaimable bytes;
- otherwise use existing group spill when resident groups can be externalized;
- return the original controlled capacity error when neither action releases a
  non-zero owned state.

For `H0`, distinct drain is the spill path. The current `H0` generic-spill
exclusion remains valid for all other state.

## 7. Canonical key and equality contract

### 7.1 Shared equality authorities

The implementation reuses two existing equality authorities rather than
introducing an independent full-row encoder:

- the aggregate argument payload is copied directly from the resident
  `argSkl` key after its chunk-local group prefix; and
- the reversible typed group row is decoded through Group's existing spill
  vector codec and compared by the existing Group hashmap.

The logical full key is:

```text
version
group-column-count
typed group row under existing Group SQL equality
aggregate-ordinal
existing canonical DISTINCT argument payload
```

The saved-argument payload already length-delimits multi-column boundaries. The
aggregate ordinal prevents different DISTINCT aggregates with equal bytes from
sharing state accidentally. The spill record carries the typed group row and
its resident Group hash. Existing Group equality remains authoritative for
hash collisions and preserves string source, grouping, and type metadata.
Compatible sharing between identical argument sets is a future optimization,
not implicit version-1 behavior.

### 7.2 Type rules

- Any NULL DISTINCT argument rejects the input row before key construction,
  matching existing multi-argument COUNT semantics.
- NULL group values remain valid group-key components.
- Grouping-set rollup sentinels remain distinct from ordinary NULL.
- FLOAT32/FLOAT64 signed zero uses the canonical all-zero payload already used
  by resident DISTINCT state.
- NaN payloads retain the resident aggregate's current bitwise distinction;
  this design does not introduce a new NaN equivalence.
- Fixed integers, decimals, UUID, dates/times, enum, and other fixed values use
  their resident canonical representation.
- CHAR and promoted CHAR arguments use the same pad-space normalization already
  installed by the planner/executor; raw storage bytes must not bypass it.
- Varlen and multi-column values use length-delimited bytes; no concatenation
  ambiguity is permitted.
- Const vectors resolve the physical source row before NULL and payload access.
- Unsupported types fail activation before the first spool file is published.

### 7.3 Hashing and collisions

One stable route hash combines the resident Group hash, xxhash of the canonical
argument payload, and aggregate ordinal. Radix levels consume disjoint hash-bit
ranges; equal keys therefore always follow the same path.

Different keys with equal route hashes remain in the same leaf. The leaf uses
existing Group equality plus exact argument bytes, so a route-hash collision
cannot merge unequal values. If an oversized no-progress leaf contains one SQL
group—the global/single-hot-group target—external sort deduplicates by
aggregate ordinal and exact argument payload. A multi-group leaf continues to
use exact Group equality and bounded radix depth; an individually unfit
pathological multi-group collision returns the controlled no-progress error.

### 7.4 Empty, NULL-only, and count-range behavior

Empty global input retains the existing synthetic `H0` group and returns zero.
A grouped row whose DISTINCT arguments are all rejected by NULL semantics still
emits its group with count zero. The base ordinary-state contribution, not the
distinct spool, preserves those zero-key groups during final consolidation.

`argCnt` remains a bounded resident-work counter and may reset after each
successful drain. It is not the final cardinality. Leaf contributions and the
group-result accumulator use checked unsigned counts and reject a result above
the existing COUNT return range before conversion; wrapping a `uint32` or
`int64` is forbidden.

## 8. Data and control flow

### 8.1 Build and drain

```text
input batch
  -> existing expression/filter/NULL handling
  -> bounded aggregate preflight
  -> resident argSkl admission
  -> threshold/capacity trigger
  -> drain canonical (group row, aggregate ordinal, argument key) records
  -> radix spool
  -> release resident eligible DISTINCT arenas
  -> retry unpublished work unit or continue input
```

Drain iterates existing skiplists directly. It never first materializes a
slice of all keys. One record is encoded into reusable accounted scratch and
written before the next key is visited. Beginning a drain validates and freezes
the logical view without allocating an empty state for every aggregate chunk.
After the private wave is flushed, commit allocates one bounded replacement
chunk through recovery capacity, swaps it with the corresponding resident
chunk, releases the old chunk, and only then advances. Rehoming to ordinary
capacity uses the same one-chunk transition. Replacement overlap is therefore
constant rather than proportional to resident group count.

### 8.2 Intermediate Group

At an intermediate-output boundary:

1. drain the final resident eligible keys;
2. finalize each distinct radix leaf in bounded memory;
3. emit one or more partial group rows whose eligible aggregate contains only
   that bounded exact-key subset;
4. emit every ordinary/non-eligible aggregate state contribution exactly once
   and use neutral state on the extra group rows created only to carry another
   exact-key subset; and
5. free each leaf immediately after its partial batches are acknowledged by
   normal pipeline ownership.

The existing aggregate partial codec remains the wire representation. The
output generator changes from "one complete state chunk" to "bounded state
chunks, possibly repeating a group key." MergeGroup already treats equal group
rows as merge inputs. A partial batch's `ExtraBuf` is bounded by the same work
budget as the leaf; it never contains the complete hot-group set.

Radix depth is an I/O-routing bound, not permission to admit a complete
terminal leaf. If a no-progress or maximum-depth leaf exhausts its resident
work account, Group records the failing envelope's exact byte boundary and
publishes the already-admitted prefix. Group-key publication is preflighted,
and exact-argument insertion is mutation-free on a capacity rejection; a newly
published group row is therefore only a permitted neutral carrier row. After
normal output ownership returns, Group resumes the same file at the saved
boundary. A record is never skipped or counted twice, and the active file has
exactly one owner across calls.

### 8.3 Final Group or MergeGroup

At finalization:

1. drain final resident eligible keys;
2. externalize every ordinary/non-eligible group-state contribution exactly
   once into the group-result spool;
3. process one distinct leaf at a time and deduplicate the resident `argSkl`
   argument payloads, scoped by aggregate ordinal and checked with the existing
   Group equality contract;
4. count unique keys per group within that leaf;
5. emit compact `(typed group row, aggregate ordinal, uint64 contribution)`
   records into the group-result spool, partitioned by the existing group hash;
6. process one group-result bucket through the existing Group equality and
   aggregate-state merge contracts; and
7. emit each final group once.

Because each exact argument payload belongs to one distinct leaf, adding leaf cardinalities
is exact. Because all contributions for one SQL-equal group use the same group
hash and are checked by Group equality, the second phase collapses the group
without reconstructing its distinct keys.

### 8.4 Composition with generic group spill

Once distinct spilling is active, generic group spill must drain eligible
distinct keys before serializing a group record. Its spill record therefore
contains ordinary/non-eligible state and an empty eligible count-distinct set.
On reload, new eligible keys may again accumulate only to the bounded work
threshold and are drained to the same execution spool.

A generic spill record written before distinct activation may still contain an
eligible exact set. Eligible aggregate decode therefore has two replayable
modes: ordinary resident decode and validated streaming decode to the active
distinct controller. If ordinary decode receives a retryable capacity rejection,
Group frees unpublished staging, activates the controller, rewinds to the
record boundary, and streams each decoded canonical key directly to the private
spill wave. It never has to reconstruct the complete old record in `argSkl`.
The group row has already been validated and staged before aggregate decode, so
it supplies the stable group identity for that streaming transfer. A malformed
record or non-capacity error is terminal and is not replayed.

At finalization, resident group state and queued generic group-spill records are
streamed into the group-result spool. They are not all reloaded concurrently.
Existing recursive group spill then handles a result bucket containing too many
compact groups. A single hot group has only fixed-width contribution state in
this phase, so it cannot reproduce the original no-progress condition.

The result spool follows the same group-hash path as generic spill. Initial
writes are split across the 32 level-one group buckets. If generic reload
recurses, only that contribution parent is repartitioned into its next-level
children, the parent is destroyed after durable child publication, and the
terminal child is consumed and destroyed once. Thus a contribution is read at
most once per fixed spill level, rather than once for every terminal leaf.

## 9. Spool ownership and formats

### 9.1 Owners

`group.container` owns one `distinctSpillController` per execution generation.
The controller owns:

- the active radix parent;
- at most one child fanout being written;
- a depth-first stack of unopened sibling descriptors;
- external-sort run files and the current merge pass;
- the bounded group-result radix parents and current child fanout;
- at most one resumable intermediate-leaf file and byte cursor;
- reusable accounted encode/decode/read/write buffers; and
- every FD and disk reservation token associated with those files.

The aggregate executor owns only resident argument state. Before commit, a
failed drain leaves every resident chunk authoritative and destroys the
incomplete private wave. Commit transfers one chunk at a time to the flushed
private wave before releasing that chunk's arena. A commit or publication
failure is terminal for the operator generation: cleanup destroys the private
or published wave plus every remaining resident chunk, so no partial transfer
can continue into another input work unit.

### 9.2 Execution-local record envelope

Both spool kinds use an internal versioned envelope:

```text
magic | version | kind | payload-length | payload | payload-length | magic
```

Lengths are checked against the configured maximum record size before buffer
growth. Header/trailer mismatch, truncation, invalid type metadata, impossible
counts, trailing payload, or version mismatch returns a controlled invalid-spill
error. Decoders publish no group/key state before the whole record validates.

The format is not persisted beyond the query and has no mixed-version contract.
It may change with the binary because producer and consumer are the same
process. Partial aggregate batches retain their existing versioned MORPC
contract.

### 9.3 File publication

Child files are created lazily. A spill wave is private until every resident
key has been written and all child writers flush successfully. Publication then
atomically replaces the parent's/resident state's ownership with the non-empty
child descriptors. On failure, all private children are closed, reservations
released, and files removed by the spill service.

## 10. Bounded repartition and collision fallback

The exact-key radix fanout is 8. Processing is depth first, so live metadata is
at most 7 siblings per depth plus one active fanout;
it does not grow with the number of historical partitions.

A leaf is admitted only after exact preflight proves that its records, canonical
keys, dedup table, typed group staging, and output scratch fit the distinct work
budget plus the active recovery reservation.

If a leaf does not fit:

1. repartition it using the next three hash bits;
2. require measurable progress: at least two non-empty children and a largest
   child smaller than the parent;
3. stop radix repartition at three levels or immediately on no progress; and
4. for an H0 or proven single-group leaf, switch to bounded external sort by
   aggregate ordinal and exact argument payload.

External sort creates accounted runs no larger than the leaf work budget,
sorts by aggregate ordinal and exact payload, and uses pairwise merges arranged
as a fixed 64-level binary counter. At most two inputs and one output are open
per merge. Duplicate adjacent keys are removed, cancellation is checked between
records, and parent runs are deleted only after replacement publication.

A single record larger than the configured work budget cannot be subdivided.
It returns a controlled error naming the record size and required minimum; it
does not bypass the allocation account. Forced total hash collision therefore
completes via external sort as long as each individual record fits.

## 11. Memory, disk, and FD budget

All data-sized allocations use the Group allocation account and its existing
recovery-capacity class. No Go-heap byte slice, map, or per-key object may retain
data across a work-unit boundary.

Let:

- `G` be the configured Group spill threshold;
- `R` be the Group recovery reservation;
- `W` be the admitted distinct work-set bytes;
- `F=8` be exact-key radix fanout; and
- `M=2` be external-sort merge fan-in.

The implementation must choose `W` by exact allocation preflight such that:

```text
resident ordinary Group state + W + fixed(F, M) buffers <= G + R
```

Fixed buffers include one input record, one output record, radix counters and
cursors, one read buffer, at most `M` merge heads, and one group-result record.
Their capacities are recorded in tests and operator statistics. Replacement
growth releases discardable scratch before acquiring a larger buffer.

The contribution phase uses a fixed-depth 32-way radix spool aligned with the
Group spill path. Files are opened lazily; splitting one parent creates at most
one bounded child fanout, and consumed parents/terminal leaves close
immediately. The exact-key fanout, contribution fanout, generic Group fanout,
and pairwise merge are all fixed constants. Every descriptor is admitted by
the execution FD budget, so a tighter deployment limit produces a controlled
reservation error rather than an unreserved or unbounded descriptor set.

Every write reserves disk bytes before publication. A reservation rejection
stops the query and cleans all files. Disk consumption is observable but not
represented as resident-memory relief until the corresponding flush succeeds.

## 12. State machine and generation rules

```text
resident
  --threshold/capacity--> activating
  --successful drain----> spooling
  --activation failure--> resident

spooling
  --more input-----------> spooling
  --intermediate output--> emitting-exact-leaves
  --final EOF------------> finalizing-distinct
  --error/cancel---------> failed

finalizing-distinct
  --leaf contribution----> consolidating-groups
  --error/cancel---------> failed

consolidating-groups
  --bucket output--------> completed
  --error/cancel---------> failed

completed/failed
  --Reset/Free-----------> closed
```

Activation publishes the controller generation only after private buffers and
the first spill wave are ready. Old iterators, file descriptors, callbacks, or
tokens cannot survive Reset. Re-Prepare installs a new allocation and spill
generation; it never reuses paths or descriptors from the previous query.

Retry is limited to unpublished input work after a classified capacity
rejection. I/O errors, corrupt spill, disk/FD admission errors, cancellation,
and invariant failures are terminal for the query and are never replayed.

## 13. Aggregate-family boundary

Version 1 supports `COUNT(DISTINCT ...)` because disjoint leaf cardinalities
have an exact, order-independent compact merge: unsigned addition with checked
row-count bounds.

The following require separate reviewed reducers before opting in:

- **SUM/AVG DISTINCT:** define checked-overflow behavior and floating/decimal
  accumulation order across disjoint leaves; a mathematical equality claim is
  insufficient if it changes current error or bit-level result semantics.
- **variance/stddev DISTINCT:** define a stable mergeable moment state and its
  floating error contract.
- **GROUP_CONCAT DISTINCT:** preserve first-seen or explicit ORDER BY semantics;
  hash-leaf order is not a valid substitute.
- **median/percentile DISTINCT:** use a bounded external order-statistic/sort
  finalizer rather than rebuilding all unique values.
- **JSON and other saved arguments:** define duplicate equality, output order,
  and bounded result-size behavior first.

Unsupported families continue to use their current representation. If their
state alone exceeds the hard account, they return the existing controlled
capacity/no-progress error; they are never silently approximated or truncated.

## 14. Failure, cancellation, and cleanup

### Q1: one effective destruction owner

| Resource | Creation owner | Terminal owner |
| --- | --- | --- |
| resident arg arena | aggregate state | aggregate Free or successful drain transfer |
| private child wave | activation/repartition call | same call until atomic publication |
| published radix descriptor | distinct controller | leaf completion or controller close |
| external-sort run | sort pass | successful replacement publication or controller close |
| group-result bucket | distinct controller | final bucket output or controller close |
| FD/disk token | owning spill descriptor | descriptor close, exactly once |
| accounted buffer/table | controller work phase | phase defer or controller close |

`container.free`, `resetForSpill`, final success, cancellation, and every error
return converge on an idempotent controller close. `resetForSpill` may transfer
the controller to the next resident Group generation, but it does not close it;
only the outer execution generation owns terminal cleanup.

### Q2: every wait terminates

There are no per-key goroutines, channels, locks, or background workers.
Spill reads/writes are synchronous and use the query context. Every input,
radix, sort-run, merge-record, and output-batch work unit performs cancellation
checks. Cleanup does not wait for a consumer or enqueue behind data work.

### Q3: every accumulation is bounded

- resident keys: threshold plus exact preflight and drain;
- work tables/buffers: admitted `W` and fixed capacities;
- radix metadata: depth-first `O(F * maxDepth)`;
- sort heads: fixed `M`;
- retries: at most one replay per unpublished work unit after measurable
  capacity progress;
- files/FDs: execution disk/FD budgets and explicit maximum;
- metrics: fixed counter names with no group/key/partition labels; and
- partial output: leaf-sized batches under normal pipeline back-pressure.

Injected write/read/flush/seek/close failures, corrupt/truncated envelopes,
cancellation in every phase, and Reset/Free after partial initialization must
all leave zero files, descriptors, tokens, controller allocations, aggregate
arenas, and allocation-account debt.

## 15. Compatibility, rollout, and security

Spill files are execution-local and removed before query completion, so there
is no catalog, backup/restore, restart, upgrade, downgrade, or mixed-binary file
compatibility contract. Existing partial aggregate protocol framing is reused;
bounded repeated group rows are legal inputs to MergeGroup and require no new
MORPC capability.

The feature is enabled only for hard-accounted Group execution. Its no-spill
behavior is identical to current execution. A scoped internal test threshold
forces activation deterministically; no public session variable is added in
version 1. Rollback is removal of the activation path, with existing controlled
capacity errors retained.

Spill files use the existing per-query spill service and inherit tenant/query
isolation and path generation. SQL values, canonical keys, group rows, file
paths, and payload samples must not appear in logs or metric labels. Errors may
report bounded sizes, phase, level, counts, and aggregate ordinal only.

## 16. Observability

Add fixed-cardinality operator statistics:

- activation count and reason (threshold or capacity);
- resident distinct bytes/keys drained;
- input records and encoded bytes;
- radix partitions, max depth, respills, and no-progress fallbacks;
- external-sort runs, passes, bytes, and merge records;
- duplicate records removed and unique keys finalized;
- exact-key partial batches/rows/bytes;
- intermediate-leaf continuations;
- group-result contribution rows, reads, and consolidation spill bytes;
- peak admitted distinct work bytes;
- FD and disk reservation rejections; and
- cleanup failures.

Reasons are bounded enum counters, not dynamic labels. Existing Group spill
bytes/rows remain authoritative for ordinary group spill and are not double
counted as distinct-spool bytes.

## 17. Alternatives

### Keep group-hash spill unchanged

Rejected. It cannot subdivide one group's exact key set and `H0` does not enter
that path.

### Increase the arena or recovery reserve

Rejected. It changes the failure point but retains O(global NDV) memory and
cannot satisfy a hard bound.

### Approximate, truncate, or drop duplicate partitions

Rejected. It changes SQL results and violates the owning issue.

### Planner-only rewrite

The existing single-aggregate rewrite and DISTINCT-key parallel topology remain
preferred when eligible. A planner-only solution cannot guarantee a runtime
memory bound for mixed, skewed, unsupported-statistics, or single-owner plans,
and it does not close executor partial/reload state.

### Emit local counts from every worker

Rejected. Equal keys can occur in multiple workers, so summing local counts
over-counts unless canonical-key ownership is proven. Intermediate output must
remain exact-key data.

### One global external sort

It provides bounded exact dedup and is the collision fallback. Making it the
only path discards cheap radix separation for ordinary hashes and makes
parallel integration with #27720 harder. The selected hybrid pays sort cost
only for an oversized or no-progress leaf.

### Add a generic disk-backed set inside every aggregate

Rejected. Aggregate executors do not own process spill service, group-key
identity, partial-output back-pressure, disk/FD budgets, or Group lifecycle.
Putting files there would duplicate ownership and make mixed/group spill
composition harder. Group is the first complete owner.

## 18. Validation and acceptance

### 18.1 Deterministic unit coverage

Use injected byte thresholds and small batches; no sleeps or large data.

| Contract | Required cases |
| --- | --- |
| activation | below threshold allocates no controller; exact threshold; retryable preflight activation; terminal error is not retried |
| key equality | fixed, VARCHAR, multi-column boundaries, const vectors, NULL arguments, NULL groups, grouping sentinel, signed zero, NaN payload control, CHAR pad semantics |
| empty/count range | empty global input, grouped NULL-only input, repeated resident-count reset, and checked final-count overflow |
| collisions | forced total hash collision; unequal colliders retained; equal keys removed; external-sort fallback selected |
| global/hot group | `H0` and one grouped hot key complete with a work budget below full NDV state |
| mixed aggregates | each ordinary COUNT/SUM/AVG partial contribution emitted once while extra exact-key carrier rows remain neutral |
| partial merge | duplicates within one worker, across workers, across partial batches, and across spill waves count once |
| generic spill | distinct drain before group serialization; reload and respill; compact group-result recursive spill |
| envelopes | bad magic/version/kind/count/length/trailer, truncation, trailing bytes, and oversized record |
| failures | injected create/write/flush/read/seek/close/disk/FD/capacity failures at each ownership transition |
| lifecycle | success, cancellation in each phase, Reset, Free, repeated reuse, partial initialization, and cleanup error |
| accounting | peak allocation <= configured limit + recorded recovery reserve; terminal debt/files/FDs/tokens are zero |

Run each changed/directly affected concurrency/lifecycle test under the adaptive
race protocol, then the owning Group and aggexec packages once under race.

### 18.2 Public SQL and topology coverage

The public regression uses the smallest data that crosses an injected execution
threshold:

- global `COUNT(DISTINCT i)`;
- grouped one-hot-key integer and VARCHAR cases;
- `COUNT(DISTINCT a,b)` with duplicates and NULLs;
- mixed ordinary aggregates plus COUNT DISTINCT;
- one CN/DOP=1 and a multi-owner partial/MergeGroup topology; and
- no-spill controls with identical results.

`test/distributed/cases/qexec/group_h0_spill.sql` covers the global, grouped
integer and VARCHAR, multi-argument NULL, mixed-aggregate, and identical
no-spill result contracts. A `SELECT DISTINCT` subquery supplies an independent
relational oracle for the grouped VARCHAR cardinality.
The physical ownership and resource oracles stay in deterministic typed tests,
where they are observable rather than inferred from a public plan:

- `TestGroupedDistinctSpillFinalizationCompletesWithinHardAccount` and its
  MergeGroup twin use 8,192 groups, two distinct arguments per group, and a
  1 MiB hard account; they require multiple exact-key drains, generic group
  spill, exact results, bounded peak use, and zero allocation/disk/FD debt;
- `TestIntermediateDistinctSpillEmitsExactKeysAcrossWorkers` proves that two
  partial owners retain exact keys through MergeGroup and remove their shared
  key only at the final owner; and
- the public BVT compares spill and no-spill outputs for the same SQL result
  cells without coupling the external contract to an unstable plan topology or
  internal metric rendering.

This split follows the public/typed oracle boundary: SQL result semantics are
black-box, while exact operator ownership, forced thresholds, accounting and
spill residue are package contracts.

### 18.3 Performance evidence

Benchmarks cover low NDV, high duplicate ratio, near-unique input, fixed and
wide varlen keys, global/few/many groups, one hot group, no spill, radix spill,
and forced external sort. Report CPU, allocations, encoded/spilled bytes,
duplicate ratio, peak accounted memory, spill levels/runs, and wall time.

Acceptance requires:

1. exact results for every correctness cell;
2. global and hot-group completion below the full-set resident size;
3. peak accounted memory within the configured limit plus explicit recovery
   reserve;
4. no file, FD, token, goroutine, or allocation debt on every terminal path;
5. bounded exact-key partial output and correct cross-worker dedup;
6. no material low-NDV/no-spill regression and zero partition allocation on
   that path; and
7. no new unbounded Go-heap allocation, retry, metadata, log, or metric state.

## 19. Implementation sequence

After independent approval of an immutable design revision:

1. expose and test ownership-safe access to the resident `argSkl` argument
   payload and reuse the existing Group equality contract;
2. add the Group-owned controller, envelopes, budgets, and cleanup skeleton;
3. add resident count-distinct drain and H0/grouped activation;
4. add radix repartition and bounded collision sort;
5. add bounded exact-key intermediate output;
6. add final count contributions and group-result consolidation;
7. compose with generic group spill/reload and MergeGroup;
8. add observability and the complete failure/accounting matrix; and
9. add public SQL and performance evidence.

No implementation phase may weaken exact equality, bypass the allocation
account, or defer a terminal ownership path not closed by this design.

## 20. Decision log

| Decision | Rationale |
| --- | --- |
| Group owns DISTINCT spill | it is the first owner with group identity, process context, budgets, partial output, and terminal lifecycle |
| retain skiplist normal path | avoids partition cost and regression for low NDV/no spill |
| exact keys across intermediate boundaries | local counts cannot remove cross-worker duplicates safely |
| two-phase finalization | distinct hash partitions subdivide a hot group; group-hash consolidation emits each group once with compact state |
| count distinct only in v1 | its leaf result is exact and order-independent; other families need explicit reducer/order/error contracts |
| reuse resident argument and Group equality | prevents resident/spill drift without adding a second full-row codec |
| radix then bounded external sort | ordinary hashes partition cheaply; forced collisions still terminate exactly |
| depth-first ownership | bounds live metadata and descriptors independently of total partitions |
| existing partial protocol | bounded repeated group rows already have exact MergeGroup semantics and avoid a new wire generation |

There are no deferred version-1 correctness, ownership, boundedness, or
compatibility decisions. Extension to another DISTINCT aggregate family is a
new design decision under section 13, not an implementation detail.
