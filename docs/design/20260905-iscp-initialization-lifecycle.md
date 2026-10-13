# Atomic ISCP initialization lifecycle

- Status: Review required
- Tracking issue: [#28175](https://github.com/matrixorigin/matrixone/issues/28175)
- Implementation PR: [#28176](https://github.com/matrixorigin/matrixone/pull/28176)
- Design revision: 2
- Last updated: 2026-09-05

## 1. Problem and evidence

An ISCP job persists progress and lifecycle metadata in one
`mo_catalog.mo_iscp_log` row. Before this change, the periodic watermark path
replaced `job_status` with a value containing only the next LSN. `Stage` then
decoded to its zero value, `Init`, even when initialization had already reached
`Running`. A later iteration could execute a non-idempotent `InitSQL` again.

The failure was observed on an asynchronous IVF index: the initial build wrote
five centroids, a maintenance flush changed `Stage` from `Running` to `Init`,
and the next iteration rebuilt metadata for the same generation. Vector search
then failed because the persisted centroid layout no longer matched `LISTS`.

The violated contract is broader than IVF:

> Within one `(account_id, table_id, job_name, job_id)` generation, maintenance
> may advance progress but cannot move lifecycle state backwards or authorize
> an initialization side effect. Initialization side effects and the durable
> evidence that suppresses their repetition have one commit point.

In the observed index case, base-table data is unchanged while consumer-owned
derived state can be inconsistent or unavailable until its job is recreated.
The lifecycle layer does not assume that every future `InitSQL` is harmless:
because the statements are not required to be idempotent, duplicate execution
is treated as a general correctness failure rather than an index-only defect.

## 2. Scope and non-goals

This design covers:

- ownership of `Stage`, LSN, watermark, and `job_state`;
- initialization, ordinary iteration, maintenance flush, retry, cancellation,
  failover, restart, and legacy ambiguous records;
- compatibility and operational handling during deployment;
- performance and validation obligations.

It does not change an index algorithm, row-change encoding, public SQL syntax,
wire protocol, catalog schema, taskservice ownership protocol, or backup/restore
policy. It does not attempt to repair a derived index whose initialization was
already repeated; that index must be rebuilt.

## 3. Persistent model and invariants

The catalog row is the durable authority. CN memory is a replayable cache.

| Field | Meaning | Writer rule |
| --- | --- | --- |
| `job_id` | generation identity | never reused for a replacement job |
| `job_state` | scheduler admission/result state | `Error` is terminal for the generation |
| `job_status.Stage` | initialization lifecycle | monotonic `Init -> Running` only |
| `job_status.LifecycleVersion` | initialization commit protocol | monotonic; missing/zero means atomic handling is not durably proven |
| `job_status.LSN` | conditional-write generation | every progress owner advances from one expected LSN |
| `watermark` | source progress included in derived state | never regresses for a live generation |
| error fields | durable failure evidence | maintenance preserves them |

Required invariants:

1. Only the worker that owns an admitted `Init` iteration may execute `InitSQL`.
2. `InitSQL` effects and `Stage=Running` plus their LSN commit atomically.
3. A watermark-only flush updates the existing JSON; it cannot reconstruct or
   clear lifecycle/error fields.
4. A full status write persists `max(durable Stage, incoming Stage)` and rejects
   a non-terminal stale writer that presents a lower stage. It also persists
   `max(durable LifecycleVersion, incoming LifecycleVersion)` so an error or
   ordinary status cannot erase protocol evidence.
5. Every status mutation compares the exact job ID and previous LSN. Zero
   affected rows means ownership was lost, not success and not a retryable
   transport failure.
6. A stopped generation cannot publish into a replacement generation. Startup
   repairs durable scheduler states and rebuilds memory before publishing a new
   worker.
7. A progress discontinuity fails closed. It becomes a durable terminal error
   before local state reports `Error`.
8. Transaction commit and rollback failures are returned to the caller. A
   catalog repair, GC, lease read, or replay is never reported as successful
   merely because its SQL statements completed before the transaction failed.

The negation of any invariant permits duplicate or skipped initialization,
stale-owner writes, a mismatch between catalog progress and derived data, or a
false-success decision after a transaction failed.

## 4. Ownership and transition model

| State/event | Effective owner | Commit or release point |
| --- | --- | --- |
| candidate selection | executor table entry | worker accepts the immutable iteration |
| admitted iteration | worker and its LSN | status CAS loses, final status commits, or worker is canceled |
| initialization effects | InitSQL transaction | transaction commits both effects and `Running`, otherwise rolls back both |
| ordinary consumer effects | consumer transaction | consumer watermark/status CAS commits |
| maintenance reservation | watermark flush transaction | commit publishes; any statement/commit error restores guarded local reservations |
| running consumer resources | registered runtime handle | unregister after consume/init returns |
| executor generation | taskservice active routine | cancellation drains workers before a successor is published |

### 4.1 Initialization

```text
Completed/Init/LSN=n
  -> worker admission (local Pending, immutable LSN=n+1)
  -> begin one transaction
  -> execute all InitSQL statements
  -> CAS status from LSN=n to Completed/Running/LSN=n+1/LifecycleVersion=1
  -> commit both effects
```

Statement failure, cancellation, panic, status-CAS loss, or commit failure
cannot expose only one side of the transaction. A retry after an ambiguous
commit first reads the catalog: if the transaction committed, the persisted
LSN/stage makes the old iteration lose CAS before any ordinary consumer effect;
if it aborted, initialization is safe to retry.

When an `Init` iteration returns an error, its compensating status write also
carries `LifecycleVersion=1`. That write can commit only if the atomic
transaction did not advance the expected LSN. It is therefore durable evidence
that a later retry is safe even when the original Commit call returned an
ambiguous error. Registration itself does not set this marker because the CN
creating the row need not be the active executor.

### 4.2 Ordinary and maintenance progress

An ordinary iteration first changes the durable status to `Running` while
advancing its LSN, then executes consumers. Consumer effects and their final
watermark update remain in the consumer transaction.

A maintenance flush is not an iteration and has no authority to initialize. It
uses `JSON_SET(job_status, '$.LSN', ..., '$.Stage', max(...))`, guarded by job ID,
non-terminal state, and expected LSN. The local entry remains `Pending` until
logtail replay observes the commit. A transaction failure restores only the
reservation that still has the same job generation, LSN, state, and flushed
watermark, so a delayed rollback cannot overwrite newer replay.

### 4.3 CAS and cleanup errors

A pure CAS loss means a newer durable owner won. The immutable iteration stops
without writing an error over that owner. A joined `CAS + rollback/cleanup`
failure also stops obsolete writes, but remains an error and is logged with the
cleanup cause. Transient failures without CAS retain the existing bounded retry
policy. Backoff never sleeps beyond its total duration budget.

The same finish rule applies to read/replay, startup repair, lease validation,
maintenance flush, and GC transactions. Error exits roll back; success is
returned and success logs are emitted only after Commit succeeds.

## 5. Legacy ambiguity and migration

Old code can persist each of these shapes after `InitSQL` commits:

```text
job_state=Completed, Stage=Init, LSN=0, InitSQL present
job_state=Completed, Stage=Init, LSN>0, InitSQL present
job_state=Completed, Stage=Init, retry/error fields present, InitSQL present
```

The first is produced by a crash between the old InitSQL transaction and its
first status transaction. The second can also be produced by the old
watermark-only writer. The third is possible when InitSQL commits, its status
flush fails, and the worker subsequently persists the iteration error. Each
shape has two indistinguishable histories:

- initialization committed and a later watermark flush erased `Running`; or
- a `startFromNow` job advanced progress before initialization executed.

Promoting the first history is correct but skips required initialization in the
second. Re-executing is correct for the second but can corrupt non-idempotent
derived state in the first. There is no remaining catalog evidence that can
select safely.

The atomic executor therefore persists `LifecycleVersion=1` only through the
completion or error-CAS paths described in section 4.1. The version is evidence
about the executor that handled the initialization, not the CN that registered
the job. Startup changes every unversioned `Completed/Init` row with InitSQL to
terminal `Error` in the same repair transaction and requires explicit job/index
recreation. LSN and error fields are intentionally not used as evidence: old
code could write every combination after InitSQL committed. Versioned `Init`
jobs, `Running` jobs, jobs without InitSQL, and already-terminal rows are
controls and remain untouched.

A freshly registered unversioned job is safe to run while it is observed by the
same upgraded executor generation: exclusive task ownership proves no legacy
executor handled it. If that generation stops before it persists completion or
error evidence, recovery cannot distinguish the fresh row from the legacy
crash shape and quarantines it. This rare availability cost is deliberate; the
only alternatives are guessing or adding a cross-version activation protocol.

The repair is idempotent: terminal rows no longer match. Recovery applies the
same classification in memory because the repair commit can precede local
logtail visibility. Already-corrupted derived data is never declared healthy.

## 6. Upgrade, downgrade, and mixed versions

No catalog column or wire change is introduced. The existing JSON document gains
an additive `LifecycleVersion` field; old binaries can parse it but their full
status writes do not preserve it. `Error` was already terminal and every legacy
status update is guarded by `job_state != Error`, so a committed quarantine
fences later old status writes.

Strict forward prevention begins when the active ISCP task owner runs the new
writer. During a rolling interval an old owner can still execute the old
watermark statement before ownership transfers; code in a new binary cannot
retroactively constrain that statement. Deployment must therefore either:

1. backport the atomic initialization protocol and marker-preserving status
   writes to every version that can own ISCP during the rollout; or
2. drain/transfer the singleton ISCP active routine to an upgraded CN and
   prevent task ownership from returning to an old CN until rollout completes.

Merely transferring ownership once is insufficient: an old owner can ignore or
erase the additive marker and reintroduce the split-transaction window. The
marker is never written by registration, so a new DDL CN cannot falsely certify
a job that is still handled by an old executor during roll-forward.

The taskservice active-routine handoff is the existing writer-serialization
boundary; this change does not add a second lease or process-global version
registry. A concurrently finishing old transaction and startup repair both
write the same catalog row, so normal transaction conflict handling permits at
most one catalog result. Operationally, do not claim historical jobs healthy
until the upgraded owner has completed startup repair.

Code rollback is format-compatible. It does not reverse a terminal quarantine:
doing so would guess which ambiguous history occurred. Recreate the affected
job instead. `mo_iscp_log` retains its existing restore-skip policy, so this PR
does not introduce a new backup/restore format or replay path.

## 7. Operations and diagnosis

Startup emits a warning only after the quarantine transaction commits and
includes the affected-row count. The durable rows are the low-cardinality source
of truth; operators can identify them without a new high-cardinality metric:

```sql
SELECT account_id, table_id, job_name, job_id, job_status
FROM mo_catalog.mo_iscp_log
WHERE job_state = 4
  AND JSON_UNQUOTE(JSON_EXTRACT(job_status, '$.ErrorMsg')) =
      'ambiguous ISCP initialization state without atomic lifecycle evidence; recreate the ISCP job';
```

For index jobs, drop and recreate the affected index after confirming that the
base table is healthy. Do not clear `Error` or edit `Stage` manually: neither
operation reconstructs whether old InitSQL effects committed. The error row is
retained until the normal job-drop/GC lifecycle removes it.

## 8. Performance and resource bounds

- Row processing, change collection, index algorithms, and data batches are
  unchanged.
- A normal full status update adds bounded JSON stage and lifecycle-version
  extraction/comparison.
- A maintenance flush remains one conditional update per eligible job in the
  existing shared transaction and avoids serializing/deserializing status in Go.
- Startup adds one O(number of ISCP jobs) catalog predicate evaluation. It
  allocates no job-sized Go collection and is bounded by the existing five-minute
  transaction context. It runs once per executor generation, outside query and
  ingestion row paths.
- Worker count, queue capacity, retry count/duration, transaction timeout, and
  goroutine ownership remain bounded by existing constants and contexts.
- No per-job metric label or permanent in-memory tombstone is added.

If the ISCP catalog grows enough for startup repair to approach its deadline,
the next design step is a catalog-level migration marker/index, not per-CN
caching or skipping the fail-closed classification.

## 9. Alternatives

### Promote every ambiguous row to `Running`

Rejected because it silently skips InitSQL for a legitimate `startFromNow` job.

### Re-execute every ambiguous initialization

Rejected because InitSQL is not required to be idempotent and this is the
observed corruption path.

### Preserve only `Stage` in the old maintenance reconstruction

Rejected as incomplete. It loses error fields and any future lifecycle fields;
in-place JSON mutation has one durable owner and composes with schema evolution.

### Add a catalog column or distributed capability service

Rejected for this repair. An additive marker in the existing JSON status is
enough to distinguish generations created under the atomic protocol. A catalog
DDL change or capability service would increase upgrade and rollback risk
without recovering the missing historical evidence.

### Keep ambiguous rows only in a CN-local quarantine

Rejected because another or older CN could schedule the same durable row.
Terminal catalog state is the cross-CN fence.

## 10. Validation and acceptance

Deterministic unit tests must prove:

- `Running` survives maintenance and full stale writes cannot regress it;
- clean-table and `startFromNow` `Init` work reaches the worker;
- InitSQL effects and `Running` commit/rollback together on success, statement
  failure, status failure, cancellation, and panic;
- pure and wrapped CAS losses stop immediately, while cleanup causes remain
  observable and transient failures retry;
- shared iterations validate all members before local progress;
- partial/failed maintenance transactions restore only their own reservations;
- replay, lease-check, repair, and GC commit failures are surfaced rather than
  converted into false success;
- legacy ambiguity (`LSN=0`, advanced LSN, and retry/error fields) and every
  nearest versioned/non-Init/terminal control classify identically in SQL and Go;
- quotes and backslashes in persisted status/spec/name values cannot break the
  generated catalog SQL.

Owning-package tests run in normal and race mode. Public evidence must create a
real asynchronous index, observe `Init -> Running`, advance the watermark,
verify derived-table cardinality and query results, restart the service, and
verify the same results without another InitSQL execution. A two-CN handoff
validates generation ownership. A supported-version rollout must additionally
follow one of the two deployment conditions in section 6; until then the PR
must describe mixed-version prevention as eventual rather than immediate.

Acceptance requires no stage regression, no duplicate initialization, no
skipped initialization, no stale-owner overwrite, bounded cancellation/retry,
no loss of existing public test coverage, and no measurable row-processing hot
path regression.

## 11. Decision log

- Lifecycle evidence is stored as an additive version in the existing JSON row;
  no catalog DDL or wire protocol is introduced.
- Initialization and its durable completion use one transaction.
- Historical ambiguity fails closed and is recoverable only by job recreation.
- Mixed-version strictness starts at upgraded task ownership; backport or
  controlled task transfer is a rollout prerequisite, not a guessed runtime
  capability.
- Catalog rows and bounded startup logging provide diagnosis without adding
  high-cardinality metrics or another long-lived state owner.
