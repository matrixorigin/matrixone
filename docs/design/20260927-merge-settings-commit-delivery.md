# Commit ordered merge settings delivery (#29403)

## Scope

This is an ordinary bug fix to the existing TN commit and merge scheduler
paths. It adds no SQL capability, persistent state, wire protocol, or worker.
The durable catalog is authoritative. A failed commit must not change the
scheduler. A late callback from an older successful commit must not overwrite
a newer setting or resurrect a setting deleted by a newer commit.

## Failure and design

`HandleCommit` currently runs callbacks before TN commit, and delays DDL
callbacks with a five second timer. A late WW conflict leaves the scheduler
changed while the catalog rolls back. The timer can reorder successful
transactions. Moving callbacks only after `txn.Wait` fixes abort leakage but
not optimistic overtaking: `ApplyCommit` makes MVCC nodes visible before the
request goroutine resumes.

Each commit attempt now owns its ordered callbacks. They run on the original
request goroutine only after a successful `txn.Wait`, before `TxnMgr.DeleteTxn`
releases the transaction from controller tracking. Failed/retried attempts
discard their callbacks. A callback may wait for the scheduler queue without
occupying the shared TN apply-worker pool. Stop-aware generation sends release
blocked producers during close. Same-transaction table registration is queued
by `ApplyCommit` before its settings callbacks; a repeated metadata event for
the same table ID preserves the supporter and setting.
The scheduler allocates its first stop-aware generation at construction, so
catalog notifications made after the notifier is attached but before `Start`
are retained and remain cancellable if a producer is blocked at shutdown.

Each setting notification carries its TN commit timestamp. The scheduler
retains the latest timestamp in the existing per-table supporter and rejects
only strictly older messages. Equal timestamps retain FIFO order because one
UPDATE may send a delete then an insert. A delete or invalid newer setting
also advances the timestamp, preventing resurrection. No separate version
map or persistent timestamp is needed.

The existing asynchronous bootstrap carries no per-row commit timestamps;
its messages have zero timestamp, so a runtime commit already applied in the
same generation cannot be overwritten by a late bootstrap message. The
offline snapshot timestamp is at least the TN's maximum replayed commit TS
when it is captured. Replay-to-write promotion still has a separate defect:
the scheduler is constructed before WAL replay completes, so its table list
and captured snapshot may be stale. That path needs an irreversible replay
handoff and a cancellable fresh bootstrap; it is tracked in #29415 rather
than silently changing the controller protocol in this focused fix.

## Validation map

* TN lifecycle test: callback runs once after success and before transaction
  deletion; failed and retry-class attempts run none.
* Scheduler test: newer set, repeated table metadata event, older delayed set;
  newer delete, older delayed set; equal-TS delete then set; malformed newer
  setting; pre-Start replay notification; full queue plus stop for config and
  catalog event producers.
* Public SQL BVT: late WW conflict keeps catalog and `merge show` unchanged;
  DDL+setting A then B preserves B; same-transaction new table and setting
  are visible after commit. Optimistic same-key overtaking is represented by
  deterministic timestamp reversal in the scheduler test.
* Run focused tests, owning packages, focused race stress, and affected package
  race tests. The normal commit path must allocate no callback when there are
  no settings writes.

## Cost and limits

The scheduler stores one `types.TS` per active table supporter and performs
one comparison per settings notification. No catalog read is added to a
settings commit. Queue sends remain bounded by the existing 4096 messages and
can block a settings request until capacity is available or the scheduler
stops. The replay-to-write bootstrap defect is a separate recovery boundary
tracked in #29415.
