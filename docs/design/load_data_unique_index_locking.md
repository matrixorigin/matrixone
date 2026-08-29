# LOAD DATA Unique-Index Lock Ownership

- Status: causal validation and mandatory design review pending
- Design revision: v6
- Supersedes: v1-v5 in this PR
- Tracking issue: [matrixorigin/matrixone#27775](https://github.com/matrixorigin/matrixone/issues/27775)
- Design and implementation PR: [matrixorigin/matrixone#27814](https://github.com/matrixorigin/matrixone/pull/27814)
- Reused prerequisite: TN-ordered logtail read barrier from
  [matrixorigin/matrixone#27842](https://github.com/matrixorigin/matrixone/pull/27842),
  active at `MORPCVersion39`
- Required reviewers: one SQL planner/compile owner and one lockservice owner
- Authors: XuPeng-SH
- Last updated: 2026-08-30

> Production implementation is intentionally absent from the current PR head.
> The #26706 causal boundary must first be demonstrated. The exact v6 revision
> must then be approved by both required owner perspectives before production
> implementation starts. Green docs-only CI is delivery hygiene, not causal,
> implementation, or performance evidence.

## 1. Decision

Optimize only a narrow, positively identified class of large modern `LOAD DATA`
statements. The existing logical plan remains unchanged: it contains one
Exclusive full-domain base-table target and precise Exclusive row targets for
each synchronous regular UNIQUE hidden table.

On physical generation zero, compile validates the complete plan and
collects a statement-local vector of promotable hidden targets without removing
their row targets. Before any runtime source is initialized, the coordinator:

1. acquires existing metadata/general locks;
2. acquires the existing base-table full-domain lock;
3. acquires one Exclusive full-domain lock for each promoted hidden UNIQUE table,
   ordered by physical table ID;
4. invokes the existing `engine.LogtailReadBarrier` through a bounded context;
5. advances the same pessimistic-RC transaction snapshot beyond the returned
   applied frontier; and
6. returns one ordinary physical retry.

The read barrier already establishes the required real-time boundary for a
commit that has completed at TN:

```text
commit C completes before barrier B begins
  => C precedes B in TN publication order
  => C's logtail precedes B's response
  => the local CN applies C before B returns
```

For a committing pessimistic holder, TN completion and logtail-FIFO admission
precede lock release. LOAD can acquire the conflicting domain only after that
release, and it submits the marker afterwards. The marker is therefore ordered
after that commit even if the holder's client-facing `Commit` call has not yet
returned. The barrier also covers the already-committed/no-active-conflict
window. No owner-local lock timestamp, future HLC, idle heartbeat, new wire
message, or new transaction-client wait protocol is required.

On the retried physical generation, an exact coordinator-local proof authorizes
compile to remove only the covered hidden row targets from a local `LockOp` copy.
The canonical plan is never mutated. A logical rebuild or any proof mismatch
permanently disables the optimization for that `Compile.Run` and preserves exact
`main` behavior.

## 2. Problem, hypothesis, and missing evidence

Issue #27775 reports a reproducible indexed-LOAD regression on the same 3-CN
TKE topology and data source:

| Case | Previous good main | Regressed main | Change |
| --- | ---: | ---: | ---: |
| No index, 100M | 82.506 s | 88.392 s | 1.071x |
| Composite PK + indexes, 100M | 124.730 s | 268.632 s | 2.154x |
| PK + indexes, 100M | 116.962 s | 253.823 s | 2.170x |
| Composite PK + indexes, 1B | 1,150.165 s | 2,782.444 s | 2.419x |

Equal-window profiles show substantially more mutex delay and
`runtime._LostContendedRuntimeLock` while CPU utilization falls. The modern
indexed LOAD path repeatedly encodes and submits hidden UNIQUE lock keys; owner
side accumulation/coarsening is correlated with the regression. The no-index
control does not pay that work.

This is still a performance hypothesis, not a proven root cause. In particular,
the required A/B comparison between #26706's direct parent
`6742f958d466ac6bd538c631aaafbddff8ad3329` and merge
`35db232dfc3be3264c4dc0d2547a3429624392fb` has not been run. The old 5M
experiment changed the indexed/no-index ratio by only about 2.5% after
normalization. It is mechanism evidence and cannot establish causality or close
#27775.

### 2.1 Causal gate before implementation

Before production code is added to this PR:

1. deploy those two exact boundary commits on the same 3-CN/1-TN TKE topology;
2. alternate them for at least three successful 100M composite-PK/index runs
   each, with a same-run no-index control;
3. report medians, spread, normalized indexed/no-index ratio, exact SQL time,
   lock requests/coarsening, mutex profile, and effective configuration; and
4. require a reproducible boundary jump aligned with hidden-UNIQUE lock work.

If the boundary does not reproduce, this implementation is abandoned and the
`37c75ed2..32053f54` range is bisected. If the boundary reproduces but lock
request/coarsening and mutex evidence do not move with it, the mechanism is
rejected and profiling continues. A design approval may record that v6 is
correct conditional on the hypothesis, but it does not authorize production
implementation until this gate passes.

After implementation, a separate exact-current-main versus exact-final-head
100M/1B endpoint gate is still mandatory. The first gate establishes causality;
the second establishes that the chosen repair is effective and does not regress
the control.

## 3. First-principles contract

### 3.1 Safety invariants

For every optimized statement:

1. **Ownership:** before the first input row enters the pipeline, the transaction
   owns the existing base full domain and every promoted hidden UNIQUE full
   domain in one deterministic order.
2. **Freshness:** the runtime snapshot is later than an applied TN publication
   frontier ordered after every earlier TN commit whose pessimistic lock release
   allowed statement-wide ownership to be established, plus every commit that
   had already completed before the barrier began.
3. **Generation:** only a later physical generation in the same `Compile.Run`,
   transaction, schema target vector, and logical generation may consume the
   ownership/freshness proof.
4. **Fallback:** if positive eligibility or the exact proof is unavailable, no
   hidden row target is removed and behavior is exact `main`.
5. **Cleanup:** compile never unlocks. The statement-owned autocommit transaction
   is the sole owner of acquired locks and releases them exactly once through
   terminal commit or whole-transaction rollback.

Useful negations, which are review and test oracles:

- a hidden row target is removed before every ownership lock and the barrier
  have succeeded;
- a direct hidden-table commit entered TN publication order before its
  conflicting lock was released but remains absent from the retried snapshot;
- a logical rebuild, prepared/cache reuse, remote fragment, restarted
  transaction, or pooled compiler consumes stale proof;
- cancellation or timeout leaves the transaction running with a partial lock
  prefix;
- an unsupported protocol sends a v39 barrier message;
- one `Compile.Run` emits more than one optimization barrier or retry.

### 3.2 Liveness invariant

Every blocking edge reaches holder release, caller cancellation, client/stream
failure, deadlock resolution, or a finite timeout. In particular, all extra
promotion work runs under one statement-wide deadline that is independent of
the 24-hour default frontend session timeout. It starts immediately before the
first promoted hidden lock and covers every promoted lock, the logtail barrier,
and snapshot installation.

The promotion budget is:

```text
configured = txnclient.LockWaitTimeoutFromTxn(txnOp)
hard cap   = defines.DefaultLockWaitTimeoutSeconds (currently 120 s)
internal budget = min(configured, hard cap)
effective end   = min(now + internal budget, caller deadline if any)
```

Eligibility requires `configured > 0`. The frontend system variable has a
positive minimum; a missing internal/legacy value therefore fails closed to
exact `main` before any promoted hidden lock is acquired. Compiler creates the
child with `context.WithTimeoutCause(..., lockservice.ErrLockTimeout)` and passes
it through the existing context-aware table-lock helper and barrier. Only that
internal cause is reported as the ordinary non-retryable lock-wait timeout
class. If the parent caller cancels or its own deadline wins, the original
caller error is preserved. Neither case is converted into an automatic retry,
which would amplify an unhealthy lock/logtail path.

The 120-second hard cap bounds the aggregate extra promotion phase, not each
hidden target independently. It is a safety ceiling, not the expected latency.
The existing TN-ordered barrier normally waits only for real queue/network/apply
lag and has no fixed `MaxOffset` delay. Final scale evidence must show its
distribution and prove the cap does not create false failures.

### 3.3 Boundedness invariant

For one optimized statement:

```text
promoted targets      <= synchronous regular UNIQUE index count
compiler state        = O(plan nodes + index defs + lock targets)
ownership requests    = one base request + promoted target count
barriers              <= 1 per Compile.Run
optimization retries  <= 1 per Compile.Run
new goroutines        = 0
new channels          = 0
new logs              = 0
metric label values   = 4 fixed outcomes
input-sized retention = 0
```

The reused read-barrier implementation already bounds CN correlation state,
server admission, TN markers, and response queues. This PR adds no second queue,
request map, stream reader, marker, or background worker.

## 4. Exact eligibility

The optimization is enabled only when every positive condition below is true on
physical execution generation zero:

```text
top-level non-internal statement
AND TxnOptions.Autocommit=true
AND TxnOptions.ByBegin=false
AND pessimistic RC transaction
AND automatic statement retry enabled
AND modern LOAD plan with Query.LoadWriteS3=true
AND finite planner estimate Cost*Rowsize >= 1 GiB
AND physical execution generation zero
AND fresh logical generation (not prepared/session-cache reused)
AND active deployment protocol >= MORPCVersion39
AND engine implements engine.LogtailReadBarrier
AND txn lock-wait timeout is positive
AND one ordinary, non-temporary, non-partitioned user base table
AND no incoming or outgoing foreign keys
AND no unsupported index family or asynchronous index maintenance
AND real base PK physically encoded as T_int64 or canonical composite T_varchar
AND at least one existing synchronous default/BTREE UNIQUE hidden row target
AND every promoted hidden table has authoritative physical PK type T_int64
```

All conditions are positive. Missing, nil, contradictory, stale, duplicate, or
unrecognized metadata makes the whole transformation ineligible. There is no
partial optimization.

Explicit transactions are excluded even when `BEGIN` retains
`Autocommit=true`; both transaction options are required. `autocommit=0`, SI,
optimistic transactions, fake-PK tables, FK plans, partitions, temporary/system
tables, small/unknown/compressed-size-ambiguous inputs, legacy LOAD, prepare-time
plans, reused logical generations, and protocol <=38 keep exact `main` behavior.

The 1 GiB threshold is a fixed-cost admission guard, not a planner cache policy.
Current compilation already marks reachable `MULTI_UPDATE` plans non-cacheable
before frontend cache publication. No new planner helper, size-dependent cache
write, or duplicated constant is introduced.

## 5. Change and ownership map

| Closure | First owner | Consumers | Risk | Contract |
| --- | --- | --- | --- | --- |
| LOAD classification | root coordinator `Compile` | retry compile, local `LockOp` | R3 hot path/retry | atomic positive admission; canonical plan unchanged |
| promoted target vector | root coordinator execution proof | pre-pipeline ownership, retried compile | R3 generation/pooling | exact sorted value copy; schema bounded; root clears |
| base/hidden locks | transaction/lockservice | ordinary and direct physical writers | R3 distributed ownership | deterministic order; terminal txn releases once |
| logtail barrier | existing engine capability | LOAD coordinator | R3 distributed wait | v39-only; caller-bounded; no duplicated primitive |
| refreshed snapshot | transaction operator | retried physical sources | R3 RC correctness | install after successful barrier; snapshot strictly later |
| metric sample | compiler call epilogue | Prometheus/scale gate | R2 operations | one sample per attempted barrier; fixed labels/buckets |

The existing read-barrier implementation and wire protocol are dependencies,
not modified consumers. Their queue, stream, reconnect, ordering, and apply
contracts remain owned and tested by #27842.

## 6. Detailed design

### 6.1 Atomic analysis without planner mutation

The logical planner remains unchanged. Compiler analysis walks the reachable
coordinator statement graph once and requires exactly one candidate `LOCK_OP`
feeding the LOAD `MULTI_UPDATE` shape. It cross-checks:

- base `TableDef`, PK definition, column encoding, table identity, and existing
  full-domain base target;
- every synchronous regular UNIQUE `IndexDef` against one authoritative
  `MULTI_UPDATE.UpdateCtx` hidden-table definition;
- hidden object/table IDs, names, PK type, lock target mode, row position, and
  static-NULL omission;
- absence of FK, partition, temporary, unsupported algorithm, asynchronous,
  fake-PK, reused-generation, and contradictory duplicate shapes.

Analysis is two-pass and is attempted only on execution generation zero:

1. validate and copy the complete sorted promoted target vector into root-owned
   statement state;
2. publish eligibility only after every check succeeds.

Physical generation zero retains the exact row targets in its local
`LockOp`. This generation can therefore fall back before promotion without
reconstructing or mutating the plan. It never reaches pipeline execution after a
successful promotion because the barrier path returns the retry first.

The retried physical generation filters only value-equal promoted row targets
from a deep local copy after validating the completed proof. The canonical plan,
prepared/session state, remote payload source, and unrelated targets are never
modified.

### 6.2 Final admission and ownership acquisition

Immediately before acquiring the first promoted hidden lock, the coordinator
revalidates the local prerequisites that can drift after compilation:

- exact transaction ID/options/mode/isolation/open state;
- deployment protocol >=39;
- engine barrier capability;
- positive transaction lock-wait timeout;
- unchanged root proof phase and target vector.

If revalidation fails before the first promoted hidden lock, promotion is
disabled for that Run and the first generation continues with exact row targets.
The existing base lock may already be held; that is exact current LOAD behavior.

If metadata/base locking or any other path requests an ordinary retry before
the root reaches `retry-marked`, retry transition first makes promotion
`disabled`. The next generation then uses exact-main row targets. This avoids
starting a new ownership protocol after a generation has already failed or
executed work; only the retry deliberately emitted by completed promotion can
consume the proof.

Promoted hidden tables are then acquired in ascending physical table ID using
the existing Exclusive full-domain table-lock mechanism and the one promotion
context created before target 1. The existing context-aware helper is exported
without changing its lock semantics; no per-target deadline is restarted. Base
remains before hidden. No planner-global owner/group field or generic lock
ordering is added.

The promoted targets are copied from row targets into coordinator-owned
metadata; they are not inserted into the canonical plan. Thus the first
generation's `LockOp` still contains exact rows, but it cannot execute them:
the pre-pipeline promotion path returns the retry after fencing and before
`runOnce`. The later physical compile removes them only after validating the
root proof.

If a hidden acquisition waits, times out, deadlocks, is canceled, detects a
definition change, or fails after a partial prefix, the substantive error is
returned. The compiler does not attempt compensating unlock. The existing
autocommit terminal path rolls back the transaction and releases base plus every
hidden lock already acquired.

### 6.3 Reused TN-ordered freshness barrier

After every ownership lock succeeds, compiler reuses the still-live promotion
context described in section 3.2 and invokes:

```go
barrier := c.e.(engine.LogtailReadBarrier)
frontier, err := barrier.AcquireLogtailReadBarrier(barrierCtx)
```

No lifecycle gate, transaction-operator mutex, active-transaction shard lock,
or compiler pool lock is held across the call. Cancellation, deadline, stream
failure, engine shutdown, and the read-barrier implementation's own bounded
admission paths remain independent terminal edges.

On success, the existing engine capability has already waited for the shared CN
timestamp waiter to reach `frontier`. Compiler then calls the existing
`TxnOperator.UpdateSnapshot(barrierCtx, frontier)`. It may take the operator
mutex, but its timestamp check is now immediate against the same CN waiter; no
future or remote progress is awaited while that mutex is held. Compiler requires
the resulting transaction snapshot to be strictly greater than `frontier`.

The call is deliberately not routed through a new `TxnClient` optional
capability. V5 needed that capability only because it asked the transaction
operator to wait for a future HLC while supporting standalone client close. V6
makes the distributed wait engine-owned and bounded before the short existing
snapshot install, so the extra ownership boundary and generation protocol are
unnecessary.

### 6.4 Physical retry proof

After snapshot installation, the root execution object records:

```text
phase                  = fenced
txn ID                 = exact current transaction ID
logical generation     = exact fresh plan generation
first physical gen     = generation that acquired ownership
target vector          = sorted (physical table ID, PK type)
barrier frontier       = returned TN frontier
installed snapshot     = transaction snapshot, strictly > frontier
```

Compiler then returns one existing `ErrTxnNeedRetry`. Existing retry transition
cancels/joins remote work, invokes `RollbackLastStatement` for the empty first
attempt, increments the statement generation, and physically recompiles.
`RollbackLastStatement` rewinds workspace writes and statement metadata; it
does not call lockservice unlock. Transaction locks therefore remain owned by
the same transaction and exact transaction ID.

On the retried pre-pipeline pass, the ordinary base full-domain request may
re-enter. The completed proof suppresses a second promoted-hidden acquisition
and barrier. Hidden ownership persists from the first attempt; the local
`LockOp` now omits only the covered rows. Later ordinary physical retries may
reuse the same proof, but the optimization itself creates at most one retry and
one barrier.

The retried generation may filter the promoted row targets only when all fields
above match, its physical generation is later, the transaction is still open
pessimistic RC, and both the recorded and current snapshots remain strictly
greater than the barrier frontier. Mismatch is an internal fail-closed error; it
is never interpreted as proof.

Any logical-plan/definition rebuild first sets the root phase to `disabled` and
then rebuilds. That Run uses exact `main` row targets and cannot attempt a second
promotion barrier. The old full locks may remain until this one autocommit
statement terminates, reducing availability but not changing correctness or
cross-statement behavior.

Proof state is coordinator-local. It is not serialized into remote pipelines,
stored in planner/session caches, or copied into borrower compiles. Borrowers
read the root object; only the root clears it during release. Pool reset nils the
target slice and all generation fields.

### 6.5 Observability

One dedicated histogram measures only the engine barrier call:

```text
mo_txn_load_logtail_read_barrier_duration_seconds
  outcome={success,canceled,timeout,error}
```

It uses four pre-bound observers and
`prometheus.ExponentialBuckets(0.0001, 2, 21)` plus `+Inf`. One attempted barrier
emits exactly one sample in a post-call epilogue. Pre-admission fallback emits no
sample. Transaction, table, tenant, file, target, timestamp, error text, and
request identity never appear in labels. No new log site is added.

Existing lock wait, retry, compile, deadlock, and logtail metrics remain the
other operational signals. The final performance report correlates equal
windows rather than adding per-row, per-batch, per-target, or per-heartbeat
telemetry.

## 7. Correctness proof

Let `L` be successful acquisition of the last required ownership lock and `B`
the subsequent TN-ordered read barrier.

### 7.1 Pessimistic writer that was active at acquisition

An incompatible base or hidden writer prevents the corresponding full-domain
lock from succeeding. LOAD reaches `L` only after the holder rolls back or its
TN commit completes and lock ownership is released/resolved. TN admits the
commit to the ordered logtail FIFO before successful completion; the client
operator releases pessimistic locks only after the TN response. Consequently
the commit is already ahead of the later barrier marker when LOAD reaches `L`.
The existing barrier makes it locally visible before `B` returns even if the
writer's outer client-facing call is still finishing.

### 7.2 Writer that committed before acquisition

The writer no longer appears as an active conflict, but its commit still
completed before `B` begins. It is ordered before the barrier marker and is
applied locally before the barrier returns. This closes the no-active-conflict
window without trusting `NewLockAdd`, `HasPrevCommit`, an owner-local timestamp,
or lock-table generation state.

### 7.3 Concurrent or later writer

After its full-domain lock is acquired, a conflicting writer cannot acquire a
covered base/hidden key until LOAD commits or rolls back. A writer concurrent
with `B` may order on either side of the marker, but it cannot mutate a covered
domain before LOAD releases ownership. Its visibility is therefore irrelevant
to this LOAD generation.

This exclusion statement applies to pessimistic, lock-aware writers. An
optimistic writer does not honor either main's row locks or the promoted full
lock. V6 therefore does not claim new exclusion against it: the existing TN
write-write/constraint conflict is still authoritative. A commit ordered before
`B` becomes visible through the barrier; one ordered after `B` remains a
concurrent transaction and can make LOAD lose/retry exactly as under main.

### 7.4 Rollback and unknown commit outcome

Rollback produces no committed data. An unknown commit retains or transfers
lock cleanup to the existing resolver; LOAD cannot acquire the conflicting
domain until resolution. If resolution is commit, its TN publication admission
precedes the lock release that lets LOAD proceed and therefore precedes the
later barrier; if rollback, there is no version to observe.

### 7.5 Rebind, restart, and mixed lock owners

The proof never reads owner-local `tableCommittedAt` and requires no timestamp
to survive lock-table rebind/restart. Physical lock ownership supplies exclusion;
the independent TN barrier supplies commit-to-local-visibility ordering.

### 7.6 Snapshot and source generation

The engine barrier first applies every preceding commit locally. Updating the RC
snapshot strictly beyond its frontier makes those versions visible. Physical
recompile discards relation, tombstone, source, and remote payload state bound to
the old snapshot. No runtime source initializes before that recompile succeeds.

## 8. State machine

| State | Event | Guard | Next | Failure behavior |
| --- | --- | --- | --- | --- |
| exact-main | classifier succeeds | all positive predicates | eligible | otherwise continue exact main |
| eligible | final admission | txn/protocol/capability/budget still valid | acquiring | failure before hidden lock disables promotion |
| eligible | unrelated retry requested | no completed promotion proof | disabled | next generation is exact main |
| acquiring | hidden target N acquired | deterministic order | acquiring/owned | error returns; txn owns prefix until rollback |
| owned | read barrier begins | all base/hidden ownership held | fencing | bounded context owns cancellation/deadline |
| fencing | barrier returns | local apply >= frontier | installing | cancel/timeout/error returns; rollback releases locks |
| installing | snapshot update | open same txn; snapshot > frontier | retry-marked | error returns; no proof published |
| retry-marked | physical retry transition | exact existing retry path | fenced | transition failure rolls back transaction |
| fenced | retry compile validates proof | later physical generation; all fields equal | running | mismatch fails closed |
| fenced | logical rebuild requested | root atomically disables first | disabled | exact-main targets on rebuilt plan |
| running | execution/commit | ordinary autocommit terminal path | terminal | error/cancel causes whole-txn rollback |

No transition from `eligible`, `acquiring`, `owned`, `fencing`, or `installing`
can enter the data pipeline.

## 9. Unhappy-path audit

### 9.1 Q1: ownership and cleanup

| Resource | Owner | Terminal paths |
| --- | --- | --- |
| promoted target slice | root `Compile` execution object | cleared/nilled on release and pool reset |
| execution proof | root `Compile.Run` | consumed by physical retry; disabled on logical rebuild; cleared on release |
| base/hidden locks | transaction/lockservice | commit, rollback, resolver-owned unknown outcome |
| barrier request/correlation | existing #27842 implementation | success, caller cancel, stream failure, shutdown; bounded slots released there |
| promotion timer | compiler promotion scope | immediate `defer cancel()` on every return |
| metric timing values | stack/epilogue | exactly one observation after attempted call |

The compiler has no unlock callback, waiter goroutine, channel, remote proof, or
second destruction owner.

### 9.2 Q2: wait-for graph

```text
LOAD caller
  -> existing metadata/base lock
  -> one <=120s promotion context
    -> promoted hidden lock 1..N
     -> holder commit/rollback, caller cancel, lock timeout, deadlock victim
    -> engine.LogtailReadBarrier
     -> bounded CN admission
     -> bounded server/TN marker admission
     -> ordered logtail publication and CN apply
     -> caller cancel, stream/client failure, shutdown, or promotion timeout
    -> immediate TxnOperator.UpdateSnapshot against already-applied frontier
```

The barrier path holds no lock needed by commit, rollback, logtail publication,
client/stream close, or timeout delivery. Frontend terminal rollback begins when
the bounded call returns and releases the transaction-owned prefix.

### 9.3 Q3: accumulation

Target/proof memory is bounded by schema metadata. Barrier/retry/metric count is
one per Run. Existing read-barrier CN/server/TN queues each have fixed admission
bounds. There is no input-row, batch, file, transaction-ID, table-ID, tenant,
error, or heartbeat cardinality in retained state or telemetry.

## 10. Performance and availability model

Current indexed LOAD lock work is approximately:

```text
O(input batches * promoted UNIQUE targets * encoded keys)
```

The optimized ownership path is:

```text
O(promoted UNIQUE targets log targets)
+ one real TN publication/apply barrier
+ one physical recompile
```

Unlike v3-v5's future HLC fence, the reused barrier has no fixed clock-skew wait;
healthy idle latency reflects only real queue/network/apply progress. Unlike a
new generic lockservice coarsening policy, the optimization is restricted to a
statement that already owns the base table for its whole execution.

Availability changes remain explicit:

- direct physical hidden-table writers can wait on disjoint keys behind the
  promoted full domain;
- an arbitrary multi-table transaction containing such a writer can add a new
  deadlock edge;
- the LOAD holds base and hidden full locks during the barrier, physical retry,
  source initialization, execution, and commit;
- hidden-lock or barrier outage retains promoted ownership for at most one
  aggregate promotion phase bounded by the smaller configured/caller deadline
  and the 120-second cap, then whole-txn rollback begins.

Concurrent ordinary LOAD/DML for the same base table is already serialized by
the existing base full-domain owner. The additional availability surface is
therefore limited to direct physical hidden-table access and must be explicitly
accepted by the lockservice owner.

## 11. Compatibility, rollout, and rollback

- No new protobuf, MORPC message, catalog field, lock mode, table-range encoding,
  durable state, or planner cache policy is added.
- Optimization requires deployment protocol >=39. During rolling upgrade or
  downgrade with any older live service, it is disabled and exact row locking
  remains active.
- Engine capability and protocol are revalidated before the first promoted
  hidden lock. A later stream/protocol failure returns an error and rolls back;
  it never silently claims freshness.
- Mixed new/old CNs continue to contend in the same existing physical lock
  namespaces. An old CN simply never emits promoted ownership.
- Removing the implementation restores exact `main` behavior and requires no
  data migration, cleanup, replay, or catalog rewrite.
- Restart loses only ephemeral compile proof. Transaction/lockservice terminal
  handling remains authoritative for surviving or unresolved locks.

Security and tenant isolation are unchanged. The optimization adds no external
input, privilege bypass, object-name lookup, cross-tenant identifier, or new
trust boundary. Its broader physical ownership can reduce availability but
cannot grant data access.

## 12. Alternatives

### 12.1 Keep exact per-batch row locks

Correct and lowest design risk, but preserves the suspected indexed-only
contention and cannot meet the endpoint if the hypothesis is confirmed.

### 12.2 Treat the base lock as covering hidden tables and drop row locks

Rejected. Ordinary DML reaches base before synchronous hidden updates, but
physical hidden tables are addressable by internal/direct SQL paths. Removing
their row locks without physical hidden ownership leaves a reachable writer
outside the exclusion proof.

### 12.3 Change generic lockservice coarsening

Potentially useful as a separate lockservice optimization, but it affects every
large transaction, preserves per-batch encoding/submission, and changes global
contention policy. It is not required to test this narrower LOAD ownership
hypothesis. If the final endpoint gate fails, revisit lockservice contention
with profiles instead of broadening this PR preemptively.

### 12.4 Use generic table-lock return timestamps

Rejected. Full-table acquisition uses `bat=nil`; owner-local commit timestamps
are not durable causal proofs across bind/restart generations and can miss an
already committed writer with no active conflict.

### 12.5 Construct a future HLC and wait for idle heartbeat progress

Rejected by v6. It can be made correct under the clock-offset contract, but it
duplicates the stronger existing TN-ordered read barrier, adds a fixed delay up
to roughly `2*MaxOffset`, depends on future progress, and required a new
transaction-client wait/install lifecycle protocol. Reusing #27842 removes all
of those mechanisms.

### 12.6 Add a new LOAD-specific TN or lockservice RPC

Rejected. The generic v39 read barrier already supplies the exact publication
and local-apply boundary. A second RPC would duplicate wire, admission,
correlation, reconnect, upgrade, shutdown, and test obligations.

## 13. Validation map

### 13.1 Focused compiler and planner tests

- actual modern indexed-LOAD fixture is admitted;
- `BEGIN`, `START TRANSACTION`, `autocommit=0`, internal/derived execution,
  SI/optimistic, retry-disabled, protocol <=38, missing capability, missing
  timeout, prepared/reused generation, FK/partition/temp/system/fake-PK,
  unsupported index, unknown/small/NaN/Inf estimates are rejected;
- any ordinary generation-zero retry before promotion sticks to exact main;
- first generation retains every canonical and local row target;
- late classifier mismatch publishes no partial target vector;
- successful proof filters only exact promoted targets on the later physical
  generation;
- logical rebuild disables promotion before rebuilding and cannot emit a second
  barrier;
- compiler release/reuse clears root and borrower state.

### 13.2 Causal boundary evidence

Run section 2.1 before implementation. Archive exact commit/config identities,
raw per-case timings, metrics, and equal-window profiles. A visually plausible
profile or one faster indexed run does not pass; the normalized boundary and
lock-mechanism signals must move together. This evidence belongs on #27775 and
in the PR body so the issue, design, and implementation cannot drift.

### 13.3 Deterministic barrier/snapshot tests

Use an injected engine barrier and explicit phase channels, not sleeps:

- barrier is called only after base and every sorted hidden target succeeds;
- successful frontier is installed and source initialization occurs only on the
  retried generation;
- caller cancellation, engine error, stream-style error, timeout at configured
  and 120-second-cap boundaries, and snapshot-update error publish no proof;
- target 1 consumes most of the budget; target 2 and the barrier receive only
  the shared remainder rather than restarting a per-step timeout;
- caller context remains live while the internal barrier deadline fires;
- a shorter caller deadline remains the caller's error, while only the internal
  timeout maps to `ErrLockWaitTimeout`;
- timeout is non-retryable, waiter/call returns, whole-txn rollback releases the
  acquired prefix, and a competing writer proceeds;
- one attempted call emits exactly one fixed-outcome metric sample; fallback
  emits none; no race duplicates the sample.
- a TN-committed pessimistic writer is paused after TN success but before its
  outer `Commit` returns; lock release followed by LOAD's barrier still orders
  and applies that commit;
- an optimistic concurrent writer retains exact-main conflict/retry behavior;
  the test does not pretend the promoted lock excludes it.

The internal deadline is injected/configured to milliseconds in UT. The outer
test deadline is only a hang guard and is larger than the tested timeout.

### 13.4 Real lockservice integration

With minimal rows and deterministic barriers:

- direct hidden writer commits before acquisition with no active wait;
- base and hidden writers commit or roll back while LOAD waits;
- owner rebind/restart loses all owner-local timestamp history yet the reused
  TN barrier still supplies freshness;
- endpoint and interior hidden keys conflict with the promoted range;
- cancellation while target N is blocked rolls back the already owned prefix;
- direct-writer timeout/cancel/deadlock cycles terminate;
- repeated physical entry is reentrant and emits no second barrier.

### 13.5 Reused prerequisite evidence

Do not duplicate #27842's implementation tests in this PR. Reuse its exact
contracts for:

- commit-to-marker-to-stream-to-local-apply ordering;
- concurrent response correlation;
- v39 protocol gating;
- CN/server/TN admission bounds;
- cancel before/after admission, stream break, reconnect, shutdown drain, and
  exactly-once slot cleanup.

Add only one real consumer integration proving LOAD invokes that capability
after ownership and updates the transaction snapshot before retry.

### 13.6 Public SQL and performance layers

No new SQL result/error contract is introduced on the healthy path, so ordinary
BVT does not prove the optimization. Reuse existing indexed LOAD correctness
cases; keep timeout/fault ordering in deterministic component tests. Do not add
a sleep-based multi-session BVT.

Final endpoint evidence is mandatory:

1. alternating exact-main and exact-final-head on the same 3-CN environment;
2. three successful 100M runs for each indexed shape and no-index control;
3. median, spread, normalized indexed/no-index ratio, SQL duration, rows, lock
   requests/coarsening, barrier histogram, first/retry compile duration,
   retries, waits, timeouts, deadlocks, and relevant log count;
4. equal-window CPU and mutex profiles;
5. for each 100M indexed shape, recover at least 80% of the normalized
   regression gap from the reported bad ratio back toward the reported good
   ratio:

   ```text
   recovered = (R_bad - R_candidate) / (R_bad - R_good) >= 0.80
   R = indexed median / same-build no-index median
   ```

6. no no-index median regression greater than 15%;
7. one 1B confirmation only after the 100M gate passes, with row-count and
   profile/lock signals consistent with the 100M mechanism.

If the normalized gate fails, this mechanism is not accepted merely because
lock request counts or a microbenchmark improved.

## 14. Risk register

| Risk | Consequence | Control/evidence |
| --- | --- | --- |
| ineligible shape promoted | correctness/availability change | atomic positive classifier and negative matrix |
| first generation drops rows early | stale execution on failure/fallback | retain exact rows until completed proof and retry |
| writer commit absent from snapshot | missed duplicate/conflict | existing TN-ordered barrier plus snapshot > frontier |
| #26706 is not causal | wrong subsystem and wasted complexity | mandatory parent/merge A/B before implementation |
| protocol/capability missing | invalid wire call | v39/capability gates before promotion |
| promoted lock/barrier stalls | table-level blocking | one min configured/120s aggregate deadline, non-retryable rollback |
| timeout retries automatically | request/log amplification | ordinary lock-timeout class, no optimization retry |
| snapshot update waits under op mutex | terminal blockage | barrier first applies same shared waiter frontier; deterministic immediate-update assertion |
| stale proof crosses generation | row targets removed unsafely | exact txn/logical/physical/vector/frontier checks |
| logical rebuild repeats barrier | unbounded retry/wait | root sticky disable before rebuild |
| direct hidden writer blocked broadly | availability/deadlock surface | explicit lockservice-owner decision and scale/cycle tests |
| optimistic writer ignores promoted lock | false exclusion claim | preserve exact-main TN conflict semantics; explicit test and proof scope |
| metric/log storm | operational overload | one sample, four labels, fixed buckets, no new logs |
| hypothesis is wrong | complexity without endpoint gain | normalized 100M/1B acceptance gate and rollback plan |

## 15. Review convergence contract

The earlier review loop failed because it skipped causal proof, repaired the
most recent counterexample while retaining its mechanism, and repeatedly
declared “no new blocker” without freezing the complete decision surface. V6
changes the process:

1. Section 2.1 proves or rejects the suspected causal boundary before code is
   added; issue status cannot promote correlation to root cause.
2. This document is the single change map for safety, freshness, generation,
   timeout, ownership, performance, compatibility, observability, and tests.
3. Required owners review the same exact commit and return their complete
   blocker set against sections 3-14, rather than one serial concern per round.
4. A subjective availability/performance tradeoff is closed once its owner,
   assumptions, decision, and evidence requirement are recorded. It is reopened
   only by materially new evidence that invalidates those assumptions.
5. Correctness, hang, leak, compatibility, and mandatory evidence failures
   cannot be self-waived by the author.
6. Implementation starts only after the causal gate and both owner approvals.
   Implementation review checks conformance to this approved revision; a
   material deviation updates the design and reopens only the affected decision,
   not the entire history.
7. The PR body, issue status, and decision log must describe the same state. No
   comment may call the hypothesis a root cause or final fix before the endpoint
   gate passes.

Reviewer closure table:

| Perspective | Required decision |
| --- | --- |
| SQL planner/compile owner | classifier authority, canonical-plan isolation, retry generation, source rebuild, cache/fallback, timeout propagation |
| lockservice owner | base/hidden ownership invariant, direct-writer availability/deadlock tradeoff, partial-prefix rollback, timeout semantics |
| shared prerequisite | #27842 contract/version unchanged and reused, not re-reviewed as new code |
| performance gate | final implementation only; cannot be approved from docs CI or old 5M data |

## 16. Decision log

| Revision | Decision | Why superseded |
| --- | --- | --- |
| v1-v2 | implementation-first base/hidden ownership variants | over-broad planner/FK/wire changes and incomplete freshness proof |
| v3 | future HLC fence through `TxnOperator.UpdateSnapshot` | client close could not independently terminate a future wait held under operator mutex |
| v4 | client-owned future wait/install | admitted explicit transactions, duplicated planner cache policy, and lacked safe metric ownership |
| v5 | narrowed autocommit future-HLC design | still duplicated the existing v39 TN-ordered read barrier, paid fixed clock-skew latency, and let outage retention follow the 24-hour session timeout |
| v6 | causal gate, then reuse existing bounded TN-ordered read barrier | pending causal evidence and dual-owner approval |

Record reviewer handles, links, decisions, and the exact approved v6 commit here
before production implementation is pushed.
