# Batch initial role privilege inserts

Owner: frontend account/bootstrap initialization. Tracking: [#29562](https://github.com/matrixorigin/matrixone/issues/29562). Implementation: the PR containing this document.

## Problem and cost

Ordinary account creation executes 30 accountadmin and one public default-privilege INSERT separately; system bootstrap executes 34 moadmin and one public INSERT. All rows already belong to the same initialization transaction. Each statement unnecessarily repeats parsing, planning, compilation and execution. Latest completed race CI (37069556794, checked ffee1890cc) takes 71m59s in Unit Testing, including issues 1488.64s. This change targets repeated work rather than increasing cluster concurrency or reducing test scenarios; CI savings remain to be measured.

## Contract and ownership

Default privilege lists and privilegeEntriesMap remain authoritative. Preserve every role ID/name, object type/ID, privilege ID/name/level, operation user, grant option and row order. Format each row timestamp with the existing clock and UTC precision. Use one multi-row INSERT per initialized role at the existing frontend initialization owner, for both system and ordinary tenants. Public remains its own statement in its existing position. Runtime GRANT/REVOKE and upgrade SQL remain independent consumers of the existing single-row format.

The existing transaction owns atomic publication and rollback; the helper only constructs SQL. No new transaction, execution abstraction, persistent state, background work, cache, interface or configuration is introduced. Multi-row VALUES uses the existing SQL INSERT implementation. A row failure aborts its statement and propagates to the existing transaction owner; later initialization statements must not execute. Cancellation follows existing executor semantics. There is no externally visible partial default privilege set before commit.

Generated SQL is bounded by the existing fixed privilege lists (maximum 34 rows), uses only internal constants and numeric tenant/user IDs, and contains no new user-controlled quoting surface. An empty list returns no statement; production callers supply the nonempty existing lists. Bootstrap schema and mixed-version catalog contents remain identical. Rollback consists solely of reverting this producer; there is no data migration.

## Alternatives

Keep one statement per privilege: lowest code change but repeats expensive execution for the same table and transaction. Merge arbitrary SQL strings or introduce a generic batching executor: broader ownership and error/ordering ambiguity, unnecessary for this bounded producer. Batch both roles and all catalog tables together: saves a few additional statements but increases coupling. Selected: one shared fixed-table SQL builder used by the two existing initialization paths; factor the existing column/value format so upgrade statements cannot drift. No test fixture sharing or test scenario removal is required.

## Validation and delivery

Before implementation, independently review this exact document and the transaction/consumer closure using gpt-6.1-sol / xhigh. Compare parsed INSERT columns and row expressions against the existing single-row format, excluding only independently generated timestamp values. Cover system/accountadmin/public and empty input. Verify execution stops and returns the original error at a rejected batch. Use a real SQL integration scenario to compare persisted initial privilege sets and privilege boundaries for both system and a freshly created account, and preserve all existing account lifecycle/restore tests. Reuse the existing cluster fixture; do not add a cluster per matrix cell.

Run the owning frontend package in normal/race modes and affected real issues tests, then a matched complete issues race pair with identical eight-CPU limits, build tags and NVMe runtime storage. Compare test identities, wall, CPU, cumulative allocation and peak RSS independently. Run focused formatting, vet, configured lint and err-check. Do not claim minutes saved in CI from microbenchmarks, or peak-memory savings from cumulative allocation. No assertions, scenarios, retries or skips may be weakened to manufacture gains.
