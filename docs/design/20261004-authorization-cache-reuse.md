# Reuse authorization decisions against current catalog state

Owner: frontend authorization and disttae catalog reads. Follow-up to #29532.
Initial validation base: `5e965dece9e`; rebased onto `ff53ee2eb0c`.

## Problem and contract

#29532 clears session privileges at every statement to observe remote REVOKE
and RESTORE. In the TPCC 10/10 comparison of `96196b4a9c` and `ae56c83b62`,
authorization CPU rises from 23 to 1,025 seconds per five-minute window.
Repeated catalog SQL adds parsing, planning, compilation and allocation.

Reuse a cached grant only when its catalog dependencies are unchanged at a
snapshot obtainable by a new authorization transaction. An older data transaction
snapshot is insufficient. Existing protected-database, view-chain, active-role
and ownership checks remain the permission decision owners.

## Ownership and validity

Disttae supplies a comparable token for the existing session privilege cache:
mo_user, mo_role, mo_user_grant, mo_role_grant, mo_role_privs, mo_database and
mo_tables. The last two cover object ownership and physical catalog table IDs.
Each entry holds the physical table ID, existing shared-state identity and
applied logtail watermark. Copies and GC preserve identity; reconstruction does
not. Tokens retain only tiny identities, never old row/object trees. The applied
watermark covers row inserts/deletes; the last-flush timestamp does not.

Reject reuse when logtail is not ready. Otherwise acquire a read-only transaction
through TxnClient.New and close it without workspace, writes or storage commit.
This reuses existing freshness, admission, cancellation and close ownership;
it does not claim stronger consistency than current authorization reads.
Propagate its snapshot lower bound to the session so cache-miss background
transactions cannot evaluate permissions before the captured token.

Capture only ready subscription states with no pending apply and applied
watermarks below the exclusive read snapshot. Missing/future catalogs or a
catalog-generation transition provide no reusable token. Uncertainty clears the
cache and uses existing authorization SQL; actual snapshot/subscription errors
propagate. Capture before evaluation: a later commit must invalidate those
results next time, never stamp them with a newer version.

Role/identity changes, manual clear and cache-mode changes keep their existing
invalidation. View chains still require metadata checks. EXECUTE validates its
bound statement once; its shell and BEGIN/COMMIT/ROLLBACK consume no grants and
need no authorization snapshot. Negative lookups allocate nothing; positive
scopes are capped at 1,024. Table/view scope storage shares one implementation;
the unused replacement method and unconsumed atomic counters are removed.

## Complexity and validation

There is no persistent epoch, wire/config change, broadcaster, worker or second
permission cache. The hot path exchanges repeated SQL for one read-only snapshot
and seven catalog lookups. Ordinary data writes preserve the token; unrelated
metadata changes can conservatively invalidate it. TTL invalidation would permit
stale grants; an administrator exemption would leave ordinary-user performance
unfixed and redefine catalog-based authorization.

Main-agent design/self-review: R3 authorization, generation and hot-path changes;
no subagents. Component tests cover watermark boundaries, pending application,
reconstruction, failures and scope capacity. Reuse the authenticated two-CN
fixture for text/SQL-prepared/binary-prepared queries, remote revoke/regrant,
role switching, old data transactions, DROP/recreate and cancellation. The six
existing #29399 restore/PITR tests retain their original behavioral oracles.

## Local performance boundary

Same-base one-row workload, three rounds of 1,500 prepared point reads and 300
BEGIN / SELECT FOR UPDATE / UPDATE / COMMIT transactions. Median microseconds:

| Workload | Per-statement clear | Unsafe reuse control | This change |
|---|---:|---:|---:|
| Admin point | 336 | 152 | 149 |
| Ordinary point | 1,413 | 156 | 173 |
| Admin transaction | 5,994 | 5,879 | 5,596 |
| Ordinary transaction | 8,907 | 5,767 | 5,625 |

The unsafe control disables freshness validation only to estimate historical
cache cost; it is not shipped. Transaction latency is near that control; the
ordinary point read still costs about 17 microseconds more. These small macOS
ARM64 measurements, on a machine with other activity, do not prove unchanged
full TPCC performance. Full TPCC comparison remains required to claim parity.
