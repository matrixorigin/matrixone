# Linearizable feature-quota admission

- Status: Review required
- Issues: [#27833](https://github.com/matrixorigin/matrixone/issues/27833),
  [#27718](https://github.com/matrixorigin/matrixone/issues/27718), and
  [#26087](https://github.com/matrixorigin/matrixone/issues/26087)
- Implementation: [#27844](https://github.com/matrixorigin/matrixone/pull/27844)
- Protocol dependency: [#27842](https://github.com/matrixorigin/matrixone/pull/27842)

## 1. Problem and contract

`mo_feature_registry` and `mo_feature_limit` are admission-control state. A
committed administrator change must constrain every feature operation that is
invoked afterwards, including an operation routed to another CN. CN logtail
application is asynchronous, so starting an ordinary local transaction is not
enough to provide that guarantee.

The required invariants are:

1. **Real-time freshness:** if a registry or quota update commits before a
   feature operation starts, the operation observes that update.
2. **Atomic finite admission:** successful concurrent creates never publish
   more owned objects than the finite quota permits.
3. **Transactional publication:** the serialization lock is retained until the
   admitted snapshot or branch metadata commits or rolls back.
4. **Isolation preservation:** quota admission never advances a caller's fixed
   SI snapshot or silently changes its transaction mode.
5. **Fail closed:** a missing dependency, canceled wait, malformed result, or
   unavailable freshness barrier rejects the feature operation.

The negation of (1) is the production bug: CN-A commits `quota = 0`, then CN-B
uses its lagging catalog snapshot and admits a branch or snapshot. The negation
of (2) is the concurrent bug: two creators both count the same old metadata and
both commit past the limit.

## 2. Scope and non-goals

This design covers `CREATE SNAPSHOT` and `DATA BRANCH CREATE` admission. It does
not put a global barrier on ordinary SQL, cache reads, DML, or unrelated DDL.
Deletes do not consume quota and remain outside quota admission serialization.

The design does not make feature state available when TN/logtail is unavailable.
Availability during that failure would require admitting work without knowing
the committed policy, which violates the control-plane contract.

## 3. Freshness primitive and linearization

On protocol v39 and later, the CN asks TN for a read barrier. TN places the
barrier marker in the same FIFO publication queue as transaction logtails. The
returned timestamp therefore follows all transactions published before the
marker. The CN waits until that frontier is applied locally and advances the
owning RC workspace to it before reading quota state.

The initial barrier is the real-time fence for every registry or quota commit
that completed before the operation was invoked. Policy changes that overlap
the operation may be ordered on either side. The remaining admission points
are:

- disabled and unlimited admission linearize at the fresh quota read;
- finite branch quota, usage, and publication linearize while holding the
  account's quota row; the post-lock barrier closes commits observed while
  waiting without changing the operation's invocation-time freshness contract;
- finite snapshot admission linearizes while holding the stable lineage-owner
  publication row.

## 4. Snapshot admission

`CREATE SNAPSHOT` already acquires the lineage-owner publication lock before
checking quota. The flow is:

1. begin the owning pessimistic RC background transaction;
2. acquire the stable lineage-owner publication lock;
3. acquire a TN-ordered read barrier and advance the workspace;
4. read registry state, quota, and current user-owned snapshot count;
5. publish the snapshot row in the same transaction;
6. commit, or roll back on every error.

The lineage lock serializes concurrent snapshot publishers even when no
snapshot row exists yet. Advancing after acquiring it is essential: a waiter
can otherwise retain the snapshot from before the preceding owner committed.
Branch-managed protection snapshots remain excluded from user snapshot usage.

## 5. DATA BRANCH admission

Branch quota is account-wide and covers both table and database branches. The
quota row `(account_id, 'BRANCH', '')` is the stable serialization owner.

The normal flow is:

1. acquire a fresh frontier and read registry/quota;
2. reject immediately when disabled, or continue without a lock when unlimited;
3. for a finite quota, require a pessimistic RC owning transaction;
4. lock the quota row with `SELECT ... FOR UPDATE`;
5. while retaining the row lock, acquire another TN-ordered frontier, advance
   the workspace, and re-read the locked quota;
6. count live branch metadata and admit only if `usage + increment <= quota`;
7. publish all table metadata for the table or database branch in the same
   transaction and retain the quota lock through commit.

The second frontier is not redundant. A creator can wait behind a previous
creator or an administrator update; the transaction's pre-wait snapshot may not
contain the commit that released the lock. Re-reading the quota also closes
finite-to-disabled and finite-to-unlimited transitions.

Database branch admission passes the number of source tables as `increment`, so
a single statement is rejected atomically rather than partially publishing up
to the remaining capacity.

## 6. Fixed-snapshot transactions

Advancing an active SI workspace would violate repeatable-read semantics. When
DATA BRANCH runs with a fixed caller snapshot, a short independent pessimistic
RC control transaction performs the barrier and initial quota read:

- disabled rejects the operation;
- unlimited permits the operation without changing the outer snapshot;
- finite rejects with an actionable instruction to retry outside the active
  transaction, because its serialization lock cannot safely be transferred to
  the outer SI transaction.

The control transaction is request-owned, has one begin/finish lifecycle, and
is closed after commit or rollback. Cancellation is propagated to transaction
creation and barrier waits, while background-session cleanup retains an
independent transaction context for rollback.

## 7. Lock order, ownership, and unhappy paths

The branch create lock order is:

```text
optional target-account lock
    -> branch quota row
        -> existing clone/branch metadata locks
            -> publication and commit
```

Snapshot creation acquires the lineage-owner lock and then performs quota reads;
it does not acquire the branch quota row. Feature-limit administration only
updates the quota row. Branch deletion can reduce usage without acquiring the
quota row and therefore cannot cause over-admission.

There is one owner for each resource:

| Resource | Owner | Release condition |
| --- | --- | --- |
| TN read-barrier admission | barrier request | response, cancellation, send failure, or stream close |
| independent control transaction | `queryQuotaInIndependentTxn` | commit/rollback followed by executor close |
| finite branch quota lock | branch background transaction | branch publication commit/rollback |
| snapshot lineage lock | snapshot background transaction | snapshot publication commit/rollback |
| copied executor result | feature checker | function return |

All barrier, lock, and SQL failures return an error before publication. A
canceled request cannot turn into an allow decision. The outer deferred
transaction finalizer rolls back clone/snapshot work, and background executor
close supplies a final cleanup path if the request context is already canceled.

## 8. Rolling upgrades and compatibility

Protocol v39 advertises the TN-ordered barrier. While any live service keeps the
cluster protocol below v39, admission uses the legacy HLC fence:

1. select a timestamp strictly beyond the local uncertainty interval;
2. wait for CN logtail application to reach that timestamp;
3. advance the workspace and read quota.

The fallback preserves correctness but can wait roughly one `MaxOffset` for
disabled/unlimited and two for finite branch admission. The v39 wire capability
must remain disabled until every live service supports it. No catalog schema or
SQL syntax changes are introduced by this PR.

## 9. Performance model

The new cost is paid only by snapshot and DATA BRANCH create statements:

| Path | v39 barriers | Quota serialization |
| --- | ---: | --- |
| snapshot | 1 | existing lineage-owner lock |
| branch disabled | 1 | none |
| branch unlimited | 1 | none |
| branch finite | 2 | one account quota-row lock |
| ordinary SQL | 0 | none |

Finite branch creation is intentionally serialized per target account because
the quota itself is account-wide. Different accounts use different quota rows
and remain concurrent. No polling, retry sleep, unbounded map, goroutine, or
additional cluster process is added by feature admission.

## 10. Validation matrix

Required evidence before merge:

- unit tests for v39 barrier success, v38 fallback, below-fence rejection,
  missing transaction/workspace, and barrier/workspace failures;
- unit tests proving finite branch takes and re-reads the quota lock, while
  disabled/unlimited avoid it and an explicit owner account is used;
- race-tested TN queue, manager, RPC correlation, cancellation, and CN consumer
  barrier paths from #27842;
- two-CN authentication acceptance from #27842;
- two-CN branch transitions `1 -> 0 -> -1 -> 0 -> 1` using a connection opened
  before CN-A mutates quota;
- concurrent finite table/database branch admission and optimistic/SI behavior;
- concurrent snapshot disabled, finite, and unlimited admission on both CNs.

Topology assertions use distinct frontend ports. Setup waits are allowed only
to make source schema visible; the quota mutation-to-admission assertion has no
sleep or retry that could hide stale-state behavior.

## 11. Alternatives rejected

- **Read the local catalog directly:** violates real-time cross-CN freshness.
- **Sleep or retry in tests/production:** probabilistic, adds latency, and has no
  ordering relationship with TN publication.
- **Use only a quota-row lock:** a waiter can still retain a pre-wait RC snapshot;
  disabled/unlimited paths would also take unnecessary locks.
- **Advance the caller's SI workspace:** breaks repeatable-read semantics.
- **Always run DATA BRANCH in an independent transaction:** changes the atomicity
  contract of explicit user transactions and can publish work the caller later
  rolls back.
- **Serialize all accounts on one global row:** correct but creates an avoidable
  cross-tenant bottleneck.
