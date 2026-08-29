- Status: in-progress
- Start Date: 2026-08-26
- Authors: iamlinjunhong
- Implementation PR: [#27623](https://github.com/matrixorigin/matrixone/pull/27623)
- Issue for this RFC: [#27602](https://github.com/matrixorigin/matrixone/issues/27602)

# Lossless temporary-table migration through Proxy

## Summary

When Proxy moves a client connection from a draining CN, preserve the
session-visible temporary tables and the prepared statements that reference
them. The migration transfers a bounded temporary-table identity snapshot, then
the target creates target-owned temporary clones before it restores prepared
statements. It never transfers a source session's physical temporary relation
as target-owned state.

## Design decision

Independent review of this exact v37 RFC and its patch-equivalent production
series passed on 2026-08-28. The decision records that the ownership transfer,
partial-clone and unknown-result handling, retry/idempotency, fail-closed
mixed-version policy, bounds, rollback behavior, and topology acceptance map
are coherent. The review also confirms that the checked-in two-CN + Proxy
Connector/J result remains applicable after the v37-only protocol-gate
renumbering. Decision record: [review 5052244278](https://github.com/matrixorigin/matrixone/pull/27623#pullrequestreview-5052244278).

## Problem and invariant

Temporary aliases currently live in a frontend `Session`, while Proxy migration
previously transferred only database, variables, and prepared-statement state.
After a successful handoff, the new session therefore could not resolve a
temporary table. A prepared statement referring to that table then failed during
target replay and Proxy retried the same failed migration.

For every migration admitted by a v37 Proxy and two v37 CNs:

1. each exported user-visible `(database, alias)` resolves on the target to a
   newly created target-owned physical temporary relation with the same data and
   indexes;
2. the source physical relation remains owned and cleaned only by the source
   session; and
3. target failure, cancellation, or timeout cannot make target cleanup delete a
   source physical relation.

Temporary aliases for hidden index relations are intentionally not exported:
cloning the visible table recreates them. Deleted database mappings are removed
with statement and transaction rollback journaling before a snapshot is made.
Because another session can drop or recreate a database without mutating this
session's local alias map, the target also discards exactly `BadDB` and
`NoSuchTable` clone entries; those catalog outcomes prove the source mapping is
stale. All other clone failures remain fatal.

## Protocol and lifecycle

`MigrateConnFromRequest.TempTableMigrationSupported` is Proxy's v37 capability.
The source returns `TempTableStateExported` even for an empty snapshot, so a new
Proxy can distinguish an empty state from an old source. `MigrateConnToRequest`
then carries `MigrateTempTable { Database, Alias, PhysicalName }` entries.

```text
source session --bounded snapshot--> Proxy --v37 request--> target session
     |                                                    |
     | owns source physical tables                         | installs a short internal alias
     |                                                    v
     +<-- remains owner             CREATE TEMPORARY TABLE alias CLONE source-alias
                                                          |
                                                          v
                                             remove borrowed alias; COMMIT;
                                             restore prepared statements
```

The target's borrowed source alias exists only while its one clone statement is
executing and is removed with `defer`. A successfully cloned table is registered
by normal DDL as target-owned. If clone, commit, cancellation, or target session
creation fails, Proxy closes the target session; that cleanup owns only target
clones. The source session remains intact, so a retry starts from the same source
snapshot and never shares cleanup ownership. `Routine.migrateOnce` makes one
target request idempotent for duplicate delivery; a new target connection after
a failed/unknown request has a fresh session and safely retries from source.

Migration is admitted only at the existing transaction-safe boundary. The clone
batch commits explicitly before variable and prepared-statement restoration,
including the case where compatibility replay restored `autocommit=0`; no client
transaction is committed by this internal batch.

## Compatibility, rollout, and rollback

v37 is the feature version. A new source rejects a session with temporary
tables when contacted by an old Proxy. A new Proxy rejects an old source that
cannot state whether its snapshot is empty, and rejects a pre-v37 target when
the snapshot is non-empty. These fail-closed paths leave the session on the
source rather than silently dropping temporary state.

Rollout requires Proxy and all eligible CNs to support v37 before temporary
table sessions can move. During rollback or a mixed deployment, ordinary
sessions still use the existing migration protocol; sessions with temporary
tables stay on their current CN until they are closed or the compatible fleet is
restored. No persistent/catalog format changes are introduced.

## Bounds and failure policy

The snapshot contains only names, never table rows or index payloads. It is
limited to 1,024 visible tables and to the existing 16 MiB migration RPC size
limit. The source checks both limits before handoff and returns the existing
`OkExpectedNotSafeToStartTransfer` result when either is exceeded. This is an
explicit no-handoff policy: there is no target clone attempt, no partial target
state, and no target-side five-second failure loop. A client can reduce temporary
state or close; the draining CN retains the session safely in the meantime.

Within the admitted bound, clone cost is bounded by Proxy's fixed transfer
timeout. Timeout/cancellation has the target-close cleanup described above;
source ownership is unchanged. The count bound prevents an unbounded sequence
of clone statements from consuming that timeout or control-plane resources.

## Alternatives

1. Drop temporary state on handoff: rejected because it silently changes SQL
   session semantics and breaks prepared statements.
2. Transfer the source physical name as the target alias: rejected because the
   target could clean a source-owned relation after partial failure.
3. Reject every session that owns a temporary table: safe but makes normal
   temporary-table workloads permanently pin a draining CN even when cloning is
   available.

## Validation and acceptance

Focused frontend tests cover snapshot identity, hidden-index exclusion, dropped
database rollback/recreate behavior, target ownership, clone/commit failures,
and the over-limit no-handoff policy. Proxy tests cover capability negotiation
and the v37 target requirement.

Before this RFC can move from draft to in-progress, a dedicated multi-process
two-CN + Proxy regression must run through Connector/J with server prepared
statements. It must create a temporary indexed table, drain its owning CN, and
prove rows/indexes, SQL `PREPARE`, and binary `COM_STMT` execution after one
handoff; it must also cover commit/rollback admission and assert no repeated
target migration failure. The executable acceptance test is
`xtool/jstfu/src/test/java/io/matrixone/jstfu/ProxyTempTableMigrationE2ETest.java`.
For a local topology, start the existing two-CN launch configuration with
`mo-service -with-proxy -launch etc/launch-with-proxy/launch.toml`, then run
the test with `MO_PROXY_TEMP_TABLE_E2E_URL` set to its Proxy Connector/J URL
(including `useServerPrepStmts=true`). The environment gate keeps ordinary
jstfu test runs independent of a local MatrixOne cluster.

The exact-head execution record is checked in as
[`20260827_proxy_temp_table_migration_e2e.md`](20260827_proxy_temp_table_migration_e2e.md).
It records the two-CN + Proxy launch, Connector/J server-prepare configuration,
and all three acceptance results for `eed54db29b02a70a777fefd086871d3cda09d548`.

## Open questions

No implementation-blocking question remains. The topology acceptance test is
checked in and was run against the two-CN + Proxy launch configuration. The
independent design decision above advanced this RFC to in-progress.
