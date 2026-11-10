# Database Inventory Producer Map

This map accompanies [R2 publication](native_rebase_r2_milestone_1.md).
Production EXPECT publication stays disabled. Epoch protects directory identity;
it does not replace source-table data dependencies.

## Common Mutation Owners

| Producer | Actual transaction owner and inventory boundary |
| --- | --- |
| CREATE DATABASE | `disttae.Engine.Create`: initialize the new DB ID in the existing owner |
| DROP DATABASE | `Engine.Delete`: reserve before relation/routine/settings retirement; delete inventory last in that owner |
| CREATE/DROP relation | `txnDatabase.createWithID/deleteTable`: increment before catalog/workspace mutation |
| In-place ALTER, constraints, RENAME | `txnTable.AlterTable`: increment before mutation and before taking workspace mutex |
| TRUNCATE and physical replacement | The actual relation delete/create lifecycle, not an extra public handler |
| Index create/drop | Index relation/schema lifecycle and the common boundaries above |
| ALTER VIEW | Compiler deletes/creates the view relation in its existing owner |
| Sequence create/alter/drop | Physical relation lifecycle; ordinary NEXTVAL is not a directory producer |
| ALTER DATABASE defaults | `Scope.AlterDatabase`: same-owner checked increment before replacing defaults; no-op leaves epoch unchanged |
| Legacy compatibility ALTER | `doAlterDatabaseConfig`: capture authorized DB identity, reserve its token and write quoted settings in the same background transaction |
| Foreground compatibility CREATE | Initialize immediately after real engine creation, before statement finalization; no independent response-stage transaction |
| Compatibility/routine database retirement | `Engine.Delete` borrows the existing operator; removed response-stage name-only writers must not run again |
| Function create/replace/drop | Capture token before definition lookup; reserve in the actual `BackgroundExec` metadata writer |
| Procedure create/replace/drop | Existence lookup and persistence share the write transaction; reserve the captured token there |
| Clone | New target database initializes a fresh row; target routine writers use its identity, not the source epoch |
| Table Restore | Existing target ID advances through normal relation mutation; never restore an old epoch |
| Database/account Restore | Recreated DB IDs initialize new rows; historical inventory is excluded from bulk catalog copying |

Multiple real mutations may increment more than once. Epoch is not an exact
operation count. Each checked inventory write must affect exactly one row;
zero, duplicate, NULL, missing or exhausted state is an error, not version zero.
Only known system databases are excluded, never a missing user row.

## Ownership And Failure Rules

The disttae internal executor uses `WithTxn` and `WithDisableIncrStatement` with
the authenticated account/time zone. It neither creates another transaction nor
advances the outer statement. Results close before returning. A failed epoch or
business write goes through the existing statement/transaction rollback owner.

Frontend routines capture `(account, DB ID, epoch)` before reading definitions.
The persistence helper requires the same account and one-row conditional update;
a same-name replacement cannot retarget a stale definition. Clone uses its actual
target tenant. Procedures no longer perform an existence read outside BEGIN.

Database-wide routine and compatibility deletion runs before identity retirement
in that reserved transaction, not after a response or in another connection.
Rollback restores all domains. DROP ACCOUNT preserves the routine catalogs until
its user-database loop finishes. Imported UDF files are retained: a commit alone
does not prove snapshots/clones released them. This is metadata retirement, not
a new reference-safe UDF GC implementation.

Generated SQL quotes values under canonical SQL mode. A hostile setting value
must not broaden the database/tenant predicate. Permission checks remain before
the mutation; ordinary users cannot write inventory directly.

## Upgrade And Bulk Paths

Fresh tenant bootstrap installs the table. Existing tenants use the pending
4.0.7 shutdown upgrade from released 4.0.6. Create/backfill precedes normal
user-relation migration operations. Backfill is ordered, bounded and idempotent:
skip existing rows without resetting their values. Missing runtime rows fail
closed until migration completes.

Scheduled, login and manual migration share the existing account-lock/page and
final-readiness owner. Cancellation or unknown page completion cannot mark ready
or move an in-memory cache ahead. Caller-supplied transactions never commit
independently. Restart may repeat acknowledged pages safely.

Account snapshot, cluster timestamp and PITR restore share database retirement
and recreation before bulk routine copying. New IDs invalidate old tokens;
retained IDs must advance. `systemCatalogRestorePolicies` excludes historical
inventory from direct copy. A real tenant snapshot restore test checks old-token
rejection, restored data, callable function/procedure and scoped permissions.
Shared-owner evidence does not imply every timestamp/PITR schedule was executed.

Snapshots, compaction and object GC do not increment directory epochs. Internal
inventory writes do not recurse. Source DML uses the separate logical-write
history, not a catalog UPDATE on every application write.

## Validation Map

| Boundary | Primary maintained oracle |
| --- | --- |
| Same-owner executor/results and exact affected count | `disttae/database_inventory_test.go`, `frontend/database_inventory_test.go` |
| Relation, view, sequence, index, defaults and no-op SQL | `TestDatabaseInventoryRelationProducers` |
| Routine creation and retirement rollback/commit | `TestDatabaseInventoryRoutineCreation`, `TestDatabaseInventoryRoutineRetirement` |
| Foreground compatibility creation/no-op/rollback | `TestDatabaseCompatibilityCreationTransaction` |
| Clone, same-ID table restore and new-ID DB restore | `TestDatabaseInventoryCloneAndSnapshotRestore` |
| Account routine restore and tenant visibility | `TestDatabaseInventoryAccountRoutineRestore` |
| Idempotent backfill and registered readiness | `TestDatabaseInventoryBackfillSQL`, `TestDatabaseInventoryRegisteredTenantUpgrade` |
| Released binary, physical old backup and actual migration SIGKILL | `embed/rebase_released_upgrade_test.go` |
| Shared commit conflict and cross-CN publication | `TestNativeRebaseInventoryPublication`, `TestNativeRebaseCrossCNSourceProtection` |

Keep qualification identities and raw outcomes outside source. Reuse evidence
only when the affected owner, source and configuration match. Selected SQL and
shared-boundary tests are not a claim of exhaustive DDL schedules, full package
race coverage or production R2 activation.
