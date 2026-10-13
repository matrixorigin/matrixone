# ANALYZE Statistics Publication and Plan-Cache Freshness

> The publication, schema-fencing, and cache-invalidation contracts in this
> document remain active. The collection path is superseded by
> `docs/rfcs/20260904_manual_analyze_sampled_stats.md`, which performs bounded
> storage sampling instead of a derived full-table aggregate plus metadata
> refresh.

- Status: mandatory design review pending (stateful cache/concurrency lifecycle)
- Tracking issue: [matrixorigin/matrixone#27728](https://github.com/matrixorigin/matrixone/issues/27728)
- Implementation PR: [matrixorigin/matrixone#27758](https://github.com/matrixorigin/matrixone/pull/27758)
- Last updated: 2026-09-04

## 1. Problem and evidence

`ANALYZE TABLE` currently computes its SQL-visible result through a derived
aggregate query. Optimizer statistics, however, are owned by disttae's
process-local `GlobalStats` cache and are refreshed independently. A successful
ANALYZE therefore does not establish a boundary after which later statements on
the same CN must plan with the newly collected optimizer statistics.

The observable failure is a long-lived session that continues to reuse both its
three-second statistics cache and a cached logical plan after ANALYZE. In the
reported Q35-shaped workload, a plan built before ANALYZE kept the stale
non-shuffle topology. Reconnecting appeared to fix the query only because a new
session discarded both caches; reconnecting is not a valid publication
contract.

The implementation must also reject partial refreshes. Object statistics are
collected concurrently from S3 metadata. A missing/corrupt object, cancellation,
or executor shutdown must not be logged and then treated as a successful scan,
because that would publish a partially accumulated `StatsInfo` and invalidate
plans in favor of worse data.

## 2. Scope

This design covers:

- current, physical tables analyzed outside an already-active user transaction;
- synchronous publication into the local disttae optimizer-statistics cache;
- invalidation of dependent session statistics and plan-cache entries on the
  same CN process;
- serialization with automatic logtail-driven refreshes for the same physical
  table;
- bounded process/session metadata and executor shutdown/error behavior.

It intentionally does not provide:

- cluster-wide or cross-CN invalidation;
- publication for historical snapshots, views, temporary tables, subscriptions,
  or other relations that do not own current persistent optimizer statistics;
- publication of a derived query's uncommitted workspace view into the
  committed, process-global statistics cache;
- a persistent generation, wire-protocol change, catalog change, or on-disk
  format change;
- a guarantee that prepared statements whose physical compile lifecycle is
  independently retained will be rebuilt by this first phase.

## 3. Required invariants

### 3.1 Safety

1. A successful ANALYZE publication is all-or-nothing: every admitted object
   task completes successfully before the engine cache is replaced.
2. For one physical `(account_id, table_id)`, explicit and automatic refreshes
   publish in one serialized order.
3. The frontend generation advances only after the engine cache replacement
   succeeds, and the issuing session is tagged with that exact generation.
4. A plan is reusable only if every statistics generation captured while
   building it still equals the current generation.
5. Work that observed generation N cannot repopulate a session cache after N+1
   has been published.
6. Physical ownership is resolved before cache lookup. Tenant, system/cluster,
   and publisher identities must not alias solely because table IDs match.
7. A failed automatic refresh cannot replace a previously published statistics
   value. It may install a nil completion sentinel only when the table has no
   prior cache entry, so first-read waiters terminate without losing last-good
   state.

### 3.2 Liveness and ownership

1. Every frontend and engine admission token has exactly one effective release
   owner on success, error, and cancellation.
2. Every concurrent object task is completed by exactly one owner: a worker
   executes it, or executor shutdown rejects it. A rejected queued task must
   still release the caller's completion barrier.
3. Admission and executor submission observe caller cancellation. The shared
   traversal context also observes executor lifecycle cancellation, so shutdown
   cannot leave a producer blocked on a full queue, a running S3 task using an
   orphaned request, or a caller waiting for abandoned queued work. Async
   cancellation callbacks are notification mechanisms, never terminal
   predicates: refresh admission, zero-object fast paths, joined traversal, and
   cache publication synchronously re-read the owning lifecycle context before
   reporting or publishing success. Subscription and object I/O use a
   refresh-local context that preserves request values/deadlines and is canceled
   by either the request or the owner lifecycle. That linked context is created
   only after fixed-stripe admission, bounding simultaneously registered owner
   callbacks by the 64 refresh-admission tokens.
4. Unrelated tables normally remain parallel. A bounded hash-stripe collision
   may serialize refresh control work but cannot affect query execution.
5. A synchronous first-read waiter may sleep only while the exact refresh
   generation it enqueued is still owned by the current table subscription and
   has at least one queued or running producer. Cache completion, context
   cancellation, worker-lifecycle shutdown, producer exhaustion, and generation
   removal are durable predicates checked while holding `GlobalStats.mu`;
   `cond.Wait` atomically releases that same mutex. A notification is never
   treated as the predicate.
6. A producer may create or capture a refresh generation only while a live
   subscription owns its eventual cleanup. Failed queue admission may leave an
   idle record for that live subscription, but it cannot create process-lifetime
   metadata for an unsubscribed table and no waiter may depend on a rejected job.

### 3.3 Boundedness

1. Frontend publication admission uses 64 fixed stripes.
2. Engine refresh admission uses 64 fixed stripes.
3. The process-local table-generation registry retains at most 64K explicit
   keys. Compaction advances a reset generation before clearing the map, so all
   older missing-key labels become conservatively stale.
4. Session statistics remain bounded by the existing `StatsCache` policy; its
   generation tags reset atomically with that cache.
5. Plan dependencies are bounded by the plan-cache capacity and by the number
   of physical tables referenced by each cached plan.
6. Concurrent object work remains bounded by the existing worker count and
   2,048-entry executor queue. The fix must not add a channel/future allocation
   per object on the successful collection path.
7. Engine statistics and refresh-scheduling metadata share the table cleanup
   boundary. Once an unsubscribed table reaches `RemoveTid`, neither
   `statsInfoMap` nor `updatingMu.updating` retains any key for that table ID,
   and a late automatic or explicit refresh cannot recreate either entry or
   write into a replacement generation. Every refresh, including the first
   queued request, carries a non-nil scheduling-record pointer as its lifetime
   token. Logtail's cache-existence check and token capture use the same cleanup
   lock order, so removal cannot fall between those two producer steps.
8. An explicit refresh creates its table-lifetime token only after subscription
   and catalog resolution succeed, while holding the subscription lifecycle
   read lock. A failed subscription therefore retains no scheduling entry, and
   unsubscribe cleanup cannot fall between validation and token capture.
9. A first statistics read whose subscription fails returns without enqueueing
   automatic work. Retrying through the worker queue would create cache and
   scheduling state before any subscription lifetime can own its cleanup.
10. Prefetch and synchronous first-read producers capture their scheduling
    token while holding the subscription lifecycle read lock. A first-read also
    requires the exact `subEntry` observed after subscription, so cleanup and a
    replacement subscription cannot silently retarget old work.

## 4. Identity and visibility

The generation key is the physical owner:

```text
(account_id, table_id)
```

The account is resolved using the same precedence as relation resolution:

1. snapshot tenant, when applicable;
2. cluster/system ownership rules;
3. publication ownership where relation resolution selects the publisher;
4. explicit system-account overrides.

Current publication is skipped for historical AS OF references, publication
consumers, temporary tables, views, and non-persistent relation kinds. These
paths preserve the legacy derived ANALYZE result and do not claim a current
engine-cache boundary.

The same rule applies when a non-system account's physical scan receives an
implicit planner account filter (cluster tables and the tenant-filtered system
tables). Such a statement observes only a subset of a system-owned physical
table, so it returns the tenant-visible ANALYZE result without refreshing the
shared statistics key. The planner and publication gate share one account-
filter classifier rather than maintaining separate table lists.

An ANALYZE whose statement starts with an active transaction also preserves the
legacy result without global publication. The derived SQL can see the
transaction workspace, whereas `GlobalStats` is committed-object state. Mixing
those visibility domains would make uncommitted data process-global.

## 5. State and publication model

### 5.1 End-to-end order

For a publishable table, success follows this order:

```text
derived ANALYZE query succeeds
  -> acquire frontend table stripe
  -> acquire engine table stripe
  -> subscribe current partition and resolve current table definition
  -> submit visible-object tasks
  -> wait for every task result or rejection
  -> under the catalog table-change read lock, validate the observation's
     schema version and atomically replace GlobalStats entry
  -> wake engine waiters
  -> commit engine refresh scheduling metadata
  -> release engine stripe
  -> advance frontend table generation
  -> cache returned StatsInfo in issuing session under that generation
  -> release frontend stripe
  -> return ANALYZE success
```

There are two related linearization points:

- the engine data publication point is replacement of `statsInfoMap[key]` after
  all object tasks succeed and while the catalog schema version remains locked
  against concurrent table changes;
- the frontend reuse boundary is advancement of the table generation while the
  frontend publication stripe is still held.

The frontend point intentionally follows the engine point. Before generation
advancement, an old plan may still run against the old generation. After it,
new cache admission and later cache hits must reject that old generation.

### 5.2 Synchronous first-read wait protocol

`GlobalStats.Get(sync=true)` uses a level-triggered predicate rather than an
edge-triggered wakeup contract:

```text
subscribe and capture exact subEntry
  -> under subscription RLock, capture/create exact updateRecord
  -> register and enqueue a job carrying that updateRecord
  -> lock GlobalStats.mu
  -> if cache key exists (value or nil sentinel), return it
  -> if caller context is done, return nil
  -> under GlobalStats.mu -> updatingMu, if updateRecord is no longer current
     or has no queued/running producer, return nil
  -> cond.Wait (atomically publish waiter and release GlobalStats.mu)
  -> re-evaluate all predicates
```

`RemoveTid` takes `GlobalStats.mu -> updatingMu`, removes both the cache entry
and generation, then broadcasts before releasing `GlobalStats.mu`. Therefore
cleanup either happens before the waiter checks the generation, in which case
the durable generation predicate terminates it, or after `cond.Wait` has
atomically registered the waiter, in which case the broadcast wakes it. There
is no broadcast-before-wait gap. A stale worker may reject its generation
silently; progress never depends on that stale worker issuing another wakeup.
Each enqueue attempt increments the generation's queued-producer count before
it can become visible to a waiter; a forced sender blocked on a full queue is
therefore also a live producer with caller and lifecycle cancellation. Worker
admission atomically transfers that count to `inProgress` or consumes a
coalesced/rejected job. Queue rollback and
every path that removes the final running producer broadcast after persisting
the zero-producer predicate. Thus a waiter cannot depend on a job that was
accepted by the channel but later abandoned by debounce, coalescing, or a
different caller's cancellation.

Context cancellation uses the same rule: its callback acquires
`GlobalStats.mu` before broadcasting, so cancellation cannot fall between the
predicate check and waiter registration. A forced queue submission that is
rejected or canceled is never followed by a wait.
The `GlobalStats` lifecycle context is an independent terminal predicate for
both a forced queue submission and an already parked waiter. Stopping update
workers therefore cannot strand a request whose caller context remains live.

### 5.3 Automatic refresh interaction

Automatic logtail refresh and explicit ANALYZE share the engine stripe. An
automatic refresh commits its statistics entry and object-count/sampling
baseline before releasing that stripe. This prevents an older refresh from
overwriting the scheduling metadata of a newer explicit refresh.

The automatic-refresh cache transition is last-good preserving. Success
replaces the cached value; failure leaves an existing entry untouched. When the
first automatic attempt fails, it installs a nil completion sentinel and wakes
synchronous waiters, preserving the existing `GlobalStats.Get` termination
contract without representing the failure as a newer publication.

Frontend publication needs a separate stripe because it serializes the larger
engine-publication-plus-generation transaction. Without it, two concurrent
ANALYZE statements could publish engine results A then B but advance/cache their
frontend generations in the opposite order.

Hash collisions deliberately trade rare refresh serialization for fixed memory.
They do not merge table identity: engine maps, generations, session tags, and
plan dependencies remain keyed by the full physical key.

### 5.4 Plan and session-cache admission

Planning records the first generation observed for each physical table. A
generation change during repeated reads makes the completed plan ineligible for
cache admission. Cache lookup compares all recorded dependencies against the
current process registry; only dependent plans are removed.

The session statistics cache uses a `(physical key, generation)` tag in addition
to its historical table-ID lookup. A slow storage read can cache its result only
if the generation is unchanged on completion. Publication installs the exact
engine result into the issuing session under the newly advanced generation.

## 6. Failure, cancellation, and shutdown

The terminal behavior is:

| Failure phase | Engine cache | Engine metadata | Frontend generation/session cache | SQL result |
| --- | --- | --- | --- | --- |
| derived query fails | unchanged | unchanged | unchanged | error |
| frontend admission canceled | unchanged | unchanged | unchanged | cancellation |
| explicit subscribe/catalog resolution fails | unchanged | no generation retained before a cleanup owner exists | unchanged | error |
| initial statistics-read subscription fails | unchanged | no automatic work or generation admitted | unchanged | no statistics |
| prefetch outside a live subscription | unchanged | no generation admitted | unchanged | rejected |
| forced first-read queue admission canceled/rejected | unchanged | live-subscription record may remain for reuse and eventual cleanup; no waiter admitted | unchanged | no statistics |
| shared automatic producer canceled before publication | unchanged | queued/running producer count reaches zero and wakes all coalesced waiters | unchanged | no statistics |
| statistics worker lifecycle stops with queued work | unchanged | waiter terminates on lifecycle predicate; process-owned record dies with `GlobalStats` | unchanged | no statistics |
| subscription cleanup before first-read waiter parks | removed | exact generation removed; durable predicate terminates waiter | unchanged | no statistics |
| subscription cleanup after first-read waiter parks | removed | exact generation removed; cleanup broadcast terminates waiter | unchanged | no statistics |
| automatic subscribe/catalog resolution fails | last-good entry retained; nil completion sentinel only when absent | failed generation closed | unchanged | not an ANALYZE result |
| task submission canceled/rejected | unchanged | failed generation closed | unchanged | error |
| object task fails or is canceled | unchanged; local partial object discarded | failed generation closed | unchanged | error |
| engine cache publication succeeds | replaced | committed before engine release | generation must then advance while frontend token is held | success only after remaining steps |

The shared object executor owns queued tasks only after successful admission.
On normal operation, a worker removes a task and executes its callback. During
executor shutdown, new submissions fail, the executor lifecycle cancels the
shared context used by already-running callbacks, workers join those callbacks,
and a shutdown owner rejects every task left in the queue. Execution and
rejection both invoke the caller-provided completion callback exactly once.

The per-refresh error accumulator records the first non-nil task or rejection
error and waits for all admitted work before returning. Waiting is required
because callbacks mutate a refresh-local accumulator; returning early would let
old work race a discarded accumulator. Callback I/O receives the request
context, so cancellation terminates the expensive work without polling or
sleeps. The executor lifecycle context is the durable predicate; the shared task
context is only its cancellation-delivery path. Traversal uses a
check/register/check sequence around that delivery callback and re-reads both
contexts after joining all admitted work. Executor shutdown therefore remains a
failed traversal even if callback dispatch is delayed or a running callback
ignored cancellation and returned `nil`. The enclosing refresh applies the same
owner-lifecycle predicate at admission and cache publication, including the
zero-object path that does not traverse objects at all. A successful refresh
linearizes at the final lifecycle and generation checks performed while the
publication lock is held. The request and engine-owner cancellation predicates
are checked in that same critical section immediately before the cache swap,
with no intervening blocking operation. Cancellation observed there wins and
leaves the last-good cache and frontend generation unchanged. If the cache swap
wins first, the publication is complete and a later cancellation does not roll
it back.

Cancellation and deadline errors remain cancellation/deadline errors at the
public refresh boundary. Other object/metadata failures may be wrapped with
table-refresh context, but must remain a failed publication.

Retry starts a fresh refresh attempt. The engine reserves only an internal
table-lifetime token before work starts; the frontend reuse generation is not
advanced until engine publication succeeds, so a failed retry creates no
consumer-visible generation gap.

## 7. Compatibility and operations

`engine.StatsRefresher` is an optional in-process capability. Engines that do
not implement it preserve legacy ANALYZE behavior. There is no persisted or wire
state, so mixed binaries do not need protocol negotiation: each CN invalidates
only its own caches and a restart naturally starts a new local generation epoch
with empty sessions.

Rollback removes the optional publication call and process-local generation
tracking. The disttae statistics cache remains compatible with its existing
automatic refresh path. No data/catalog migration, backup/restore action, or
downgrade procedure is required.

Operational diagnosis should distinguish:

- derived ANALYZE query duration;
- synchronous disttae refresh duration and S3 request counts;
- refresh failures by subscribe, catalog, object I/O, cancellation, or executor
  shutdown;
- plan-cache invalidations caused by statistics generations;
- generation-registry compactions and refresh-stripe wait time.

The initial implementation can reuse current refresh logging and metrics, but
the above dimensions are the required observability target before enabling a
cross-CN extension.

## 8. Performance and capacity model

The common TP path with no recorded statistics dependency performs no new
generation-map read. A dependent cache hit takes one process-local read lock and
O(number of referenced physical tables) comparisons, with no allocations. The
local Apple M4 evidence at implementation revision `cb2327fd90` measured
1.773-1.785 ns for zero dependencies, 34.95-36.45 ns for one, 47.20-48.10 ns
for four, and 121.5-123.1 ns for sixteen, all with zero allocations. The waiter
repair does not touch that path; its producer accounting adds one integer under
the existing scheduling mutex only when a statistics refresh is enqueued.
These values are directional microbenchmark evidence, not a production latency
SLO.

ANALYZE adds a synchronous disttae object-metadata scan after its derived query.
This increases ANALYZE latency and S3 reads but moves the cost off the normal TP
execution path. The reported 10M-row Q35-shaped case completed that refresh in
approximately 181 ms and changed the next plan from stale non-shuffle to a
16-way hash plan. Production acceptance must continue to compare object count,
sampling mode, S3 requests, and end-to-end ANALYZE latency.

The executor repair keeps the existing closure and wait-group shape. It adds a
constant number of lifecycle reads per refresh, a success-path branch and error
accumulator, plus executor lifecycle bookkeeping; it does not add a check, result
channel, allocation, or goroutine per object. The refresh-local owner callback is
registered only after fixed-stripe admission, so at most 64 are active. Focused
benchmarks or allocation tests are required if the final implementation changes
that property.

## 9. Alternatives

### A. Keep TTL-only session caching

This is the status quo. It is simple and cross-CN-neutral, but cannot define a
synchronous ANALYZE boundary and leaves cached plans stale indefinitely. It is
rejected for correctness.

### B. Flush every session and plan cache on ANALYZE

A process-wide flush is easy to reason about but turns one table's maintenance
into unrelated TP plan rebuilds and requires enumerating/live-coordinating all
sessions. It has a larger latency and availability blast radius and is rejected.

### C. Rely only on the engine statistics-map replacement

This fixes fresh statistics reads but not a session's three-second cache or
logical plan cache. It also cannot fence a slow old read from repopulating a
session cache. It is insufficient.

### D. Broadcast generations across CNs

Cross-CN invalidation is the desired broader semantic but requires a distributed
ordering, retry, restart, compatibility, and bounded replay design. Adding it to
this repair would materially enlarge the failure surface. The selected first
phase is explicitly CN-local and leaves this as a separately designed feature.

### E. Selected: CN-local per-table generation after synchronous engine refresh

This gives a precise local boundary, invalidates only dependent state, requires
no persistent/wire change, and keeps ANALYZE cost out of the normal TP path. Its
tradeoffs are local-only scope, two bounded admission layers, and dependency
checks on affected plan-cache hits.

## 10. Verification map

| Contract | Deterministic evidence |
| --- | --- |
| successful publication and exact returned object | engine publication-boundary UT |
| failed engine refresh does not advance/cache | frontend publisher failure UT |
| one successful and one failed object task rejects partial stats | concurrent visible-object UT |
| pre-canceled, in-flight canceled, and shutdown-rejected work terminates | executor/visible-object cancellation UT |
| shutdown cannot become success when an in-flight callback returns nil | executor-lifecycle traversal UT |
| same-table refresh order; unrelated-table concurrency | frontend and engine admission race UT |
| failed automatic refresh preserves last-good stats and completes an absent first generation | injected subscribe-failure state-transition UT |
| table cleanup reclaims both statistics and refresh-scheduling entries; first-queued, late automatic, and late explicit work cannot recreate them or target a replacement lifetime | `RemoveTid` ownership/generation UT |
| failed explicit subscription creates no ownerless scheduling generation | injected subscribe-failure UT |
| failed initial-read subscription queues no ownerless automatic generation | injected subscribe-failure UT |
| synchronous first read returns the exact value published by its accepted producer | producer-publication UT |
| prefetch outside a live subscription creates no ownerless generation | focused ownership UT |
| cleanup before synchronous waiter registration cannot lose a wake | queue/admission phase-barrier UT |
| cleanup after synchronous waiter registration terminates the wait | condition-registration phase-barrier UT |
| caller cancellation after synchronous waiter registration terminates the wait | condition-registration phase-barrier UT |
| coalesced waiter terminates when another caller's producer is canceled at refresh admission | producer-transfer phase-barrier UT |
| synchronous waiter terminates when the statistics worker lifecycle stops | worker-lifecycle phase-barrier UT |
| replacement subscription rejects work captured from the old `subEntry` | subscription-generation UT |
| slow generation-N read cannot overwrite N+1 | session-cache race UT |
| plan build spanning publication is not cached | plan-cache generation UT |
| physical account/view/temporary/transaction rules | focused frontend table-driven UT and ANALYZE BVT |
| system account plus two tenant subsets cannot cross-publish cluster-table statistics | multi-session ANALYZE BVT |
| validate-V1/ALTER-V2/publication cannot publish a stale observation | catalog lock and barrier-based engine UT |
| cancellation at the final publication fence preserves last-good stats and its schema binding | publication phase-barrier UT |
| no-dependency and 1/4/16-dependency cache-hit cost | allocation/latency benchmarks |
| SQL-visible existing-session plan changes after ANALYZE | recorded real-service validation and explain-plan assertion |

Every new concurrency test uses explicit phase barriers and an outer timeout
only as a hang guard. The changed disttae and frontend packages require focused
normal tests, focused adaptive race stress, and one owning-package race run when
the local CGo environment can execute their linked test binaries.

## 11. Rollout and decision log

The initial rollout is behavior-on for disttae engines implementing the optional
capability. It fails closed: inability to complete the refresh returns an error
and leaves previous generations/caches valid. Legacy engines and excluded table
classes retain their previous behavior.

Decision log:

- Use physical `(account_id, table_id)` identity; names are lookup inputs, not
  generation identity.
- Publish only committed current-table state; do not globalize an active
  transaction's derived result.
- Keep the first phase CN-local; cross-CN publication requires another design.
- Use fixed stripes and conservative 64K compaction rather than unbounded
  per-table locks/generations.
- Wait for all admitted object work after the first failure so no callback can
  outlive its refresh-local accumulator.
- Preserve no per-object future/channel allocation in the successful scan path.
- Preserve the last successful automatic-refresh value on failure; use a nil
  sentinel only to complete an otherwise absent first generation.
- Make `RemoveTid` the common lifetime owner for published engine statistics
  and per-table refresh-scheduling metadata; every automatic and explicit
  refresh requires a non-nil update record as a lifetime token, so old work
  cannot recreate cleanup-owned state or target a replacement lifetime.
- Treat condition-variable broadcasts only as hints. The cache/context/exact
  generation predicates are checked under the condition mutex, and every
  producer generation is captured under a live subscription cleanup owner.
- Treat lifecycle callbacks only as cancellation delivery. Admission, joined
  traversal, zero-object completion, and publication re-read the request and
  owner contexts as durable predicates; linked owner callbacks are created only
  after fixed-stripe admission.
- Give each admitted automatic refresh one terminal owner that commits the
  cache, advances metadata only when that commit succeeds, and finally releases
  admission in that order.

Open approval item: an independent reviewer must approve this exact design
revision before the implementation is considered deliverable. There are no
known unresolved correctness decisions in the proposal itself.
