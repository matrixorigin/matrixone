# DDL lifecycle lock ordering

## Incident and invariant

Issue #28317 reports a real cross-database cycle in main nightly run
34041258710. Transaction A creates a table, retaining the View metadata gate,
then drops that table and requests the SNAPSHOT owner-lifecycle gate.
Transaction B alters an unrelated table, retaining SNAPSHOT while its copy
CREATE requests View. Both gates are internal; the application tables do not
overlap. Deadlock detection correctly chooses a victim.

The invariant is transaction-wide: a transaction must own SNAPSHOT before it
owns View, including when CREATE and DROP are different statements of BEGIN.
Reordering only DROP's local code cannot establish that invariant.

## Change

All View gate entry points first perform the existing pessimistic SNAPSHOT
locking read in the same transaction and under the same system-tenant context.
The original View gate remains held as well. Revalidation marker writes and
lineage owner write barriers are unchanged. In particular, the new locking
read does not replace commit-time owner write validation or publish another
feature-registry MVCC version.

The common ordering is used by compile-time cleanup/revalidation, background
revalidation and recovery, account inheritance, publication invalidation,
Snapshot/PITR restore, and account reconciliation. The exported revalidation
SQL sequence has the same ordered prefix.

## Ownership, failure and compatibility

- Both locks belong to the existing transaction. There is no new goroutine,
  resolver, retry loop, timer, cache, or persistent state.
- Failure acquiring SNAPSHOT stops before acquiring View or mutating markers.
- Failure acquiring View propagates through the existing rollback path; this
  helper must not independently unlock a transaction's earlier locks.
- SQL contexts and transaction cleanup retain responsibility for cancellation
  and release. Existing typed catalog-not-ready fallbacks are retained.
- Retaining the original View row lock preserves the common exclusion point
  with old CNs. A mixed-version cluster can still encounter the original
  inversion through an old CN; eliminating it requires updating those CNs too.
- There is no catalog schema, wire format, WAL, or stored-data migration.

## Cost and limits

Each View gate acquisition adds one locking SELECT on an existing indexed
SNAPSHOT row. Ordinary DML paths are unchanged. View-only DDL transactions now
also retain SNAPSHOT until their transaction ends, so long explicit DDL
transactions can delay Snapshot/lineage operations. This is a real additional
serialization boundary, not a zero-cost performance optimization.

The fix removes the demonstrated two-gate inversion; it does not remove all
possible application/catalog deadlocks, nor fix the separate concurrent
primary-key ALTER convoy reported in the same nightly. It does not justify
raising client timeouts as a substitute for addressing that convoy.

## Validation map

This is an R3 lock-order closure, not a new feature or protocol redesign.

- Catalog helper UT: exact SNAPSHOT -> View order, first-lock failure and
  second-lock failure, with no later operation after a failure.
- Compile/frontend UT: preserve marker transitions, activation/recovery,
  system-tenant execution, typed missing-catalog compatibility and propagation
  of both gate errors.
- Public SQL regression: two connections, disjoint databases, minimal rows,
  synchronize on a real gate waiter; require both DDL transactions to finish
  and verify schema/data. Include rollback and cancellation controls.
- Existing package tests: catalog, databranchutils, compile and frontend.
- Rolling-upgrade limitation and additional DDL serialization are documented
  rather than claimed absent.

The concrete command results are maintained in the task handoff; a passing
unit suite alone is not evidence that the SQL regression is fixed.

### Local evidence (2026-09-07)

Base: upstream main `6eee64625e`. Tests ran on macOS arm64 with Go 1.26.4
and the repository CGo wrapper. No remote cluster was modified.

- Full catalog, databranchutils, compile and frontend suites: exit 0.
- `TestIssue28317ViewSnapshotGateOrderSQL`: exit 0, 11.834s including the
  shared two-CN fixture startup. DROP/COMMIT, holder rollback and waiter
  cancellation controls all pass; both CNs check final schema and data.
- The same SQL test with `-race`: exit 0, 14.810s. This run precedes the
  final test-only cleanup-registration move and stronger client-cancel error
  assertion. Its log already records the asserted `context canceled`; product
  code and the successful scenario paths are unchanged. Normal mode was rerun
  after those test changes.
- Existing public SQL controls
  `TestIssue27718ConcurrentSnapshotQuota` and
  `TestIssue26120SnapshotBranchKeepsHistoricalParentIdentity`: exit 0,
  13.695s together. Quota covers pessimistic RC and optimistic SI.
- Pre-fix ordered reproduction already demonstrated the cycle at deployed
  `9ae27d218` and previous daily `845fff8ee9`. The fresh main base was not
  separately rerun without the patch; do not claim an exact-base red/green
  measurement or a production performance result from these functional tests.

Client cancellation is not an acknowledgment of server rollback. The cancel
control accepts either complete schema outcome, checks data integrity and
continued progress, and does not require a canceled client to imply that ALTER
could never have committed. The original DROP/COMMIT scenario requires both
transactions to succeed, without accepting deadlock or retry as success.

Raw local logs: `/tmp/issue28317-packages.log` and
`/tmp/issue28317-harness/issue28317-{normal-review,race,public-controls}.log`.
