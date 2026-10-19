# Native materialized views

Design for [#30079](https://github.com/matrixorigin/matrixone/issues/30079),
implemented by [#30085](https://github.com/matrixorigin/matrixone/pull/30085).
Status: experimental P1; pending maintainer design review.
This is the design entry point for the PR. Commands for reproducing validation
are in [the test runner README](../../test/materialized_view/README.md).
This document supersedes the earlier `00000000_materialized_views.md` RFC and
the separate implementation notes. In particular, the transaction model below
replaces the old SI-only execution sketch, and the recovery policy below
replaces the old proposal for paused account/cluster restores.

## Goal and scope

Repeated dashboard queries scan the same facts and recompute the same aggregates
and joins. Logical views save SQL but still execute that work on every read.
Application-maintained summary tables leave publication, refresh failure and
dependency management to each application.

Provide a database-owned, read-only stored SELECT result. The central invariant
is that readers see one complete successful publication: its rows, source
snapshot and successful refresh identity commit together. A failed refresh
preserves the previous publication. Before the first success, every read fails
with `MV_NOT_POPULATED`, including reads simplified to an empty result.

P1 supports explicit queries against the MV, deferred initial population, manual
refresh and periodic full refresh. It is disabled by default with
`experimental_materialized_view`. It does not implement automatic query rewrite,
incremental maintenance, aggregate-table write semantics, nested MVs or logical
history/clone/restore integration. Those require separate designs and acceptance
criteria before expanding this contract.

## SQL contract

```sql
SET experimental_materialized_view = 1;
CREATE MATERIALIZED VIEW sales_summary REFRESH MANUAL AS
  SELECT region, COUNT(*) AS n, SUM(amount) AS total
  FROM sales GROUP BY region;
REFRESH MATERIALIZED VIEW sales_summary;
SELECT * FROM sales_summary;
ALTER MATERIALIZED VIEW sales_summary REFRESH EVERY INTERVAL 1 MINUTE;
SHOW CREATE MATERIALIZED VIEW sales_summary;
ALTER MATERIALIZED VIEW sales_summary REFRESH MANUAL;
DROP MATERIALIZED VIEW sales_summary;
```

CREATE validates the definition and registers dependencies in the same
transaction as relation creation; it does not populate rows. Manual REFRESH
waits for completion. Periodic refresh has a minimum interval of 60 seconds.
Source DML becomes visible in the MV only after a successful refresh.
`SHOW CREATE TABLE` also returns the logical MV definition and current policy.

Definitions may use deterministic projections, filters, inner joins, GROUP BY,
SUM/COUNT/MIN/MAX/AVG and an explicit scalar-function allowlist. Sources must be
persistent native tables in the same account and database. The definition is
limited to 16 sources and 256 output columns, with a bounded AST binding walk.
CTEs, DISTINCT, windows, subqueries, ORDER BY, LIMIT, parameters, user variables,
UDFs, qualified functions, historical/external sources, views and MVs are rejected.

Refresh runs with the captured database, SQL mode, timezone, division precision
and identifier-case setting. Format 2 distinguishes an explicitly empty SQL
mode from an absent field. Reinterpreting the definition using the worker's
session defaults would change results and is rejected.

## Implementation and ownership

An MV is a native relation of kind `m`, with the `mo_materialized_view` property
and ordinary storage for its rows. No new storage engine is introduced.

| Component | Responsibility |
| --- | --- |
| `pkg/sql/parsers/tree/materialized_view.go` | SQL statement types; MySQL grammar supplies parsing |
| `pkg/catalog/materialized_view.go` | Persisted definition, source identity and format validation |
| `pkg/sql/plan/materialized_view.go` | Definition validation, source binding and query admission |
| `pkg/frontend/materialized_view.go` | User/definer privileges and SQL command admission |
| `pkg/sql/compile/materialized_view.go` | Native relation lifecycle, dependency and read/write guards |
| `pkg/mv/service.go` | Refresh state, transactional publication and account lifecycle fencing |
| `pkg/mv/task.go` | Reconciliation, existing taskservice executor 17 and bounded cleanup |
| `pkg/bootstrap/versions/v4_0_18/upgrade.go` | Catalog installation and legacy-definition migration |

Four global catalogs store rows scoped by account ID:

| Catalog | Persistent state |
| --- | --- |
| `mo_materialized_views` | Definition, policy epoch, requested/successful sequence, populated state, last run, source physical/logical timestamp and next due time |
| `mo_mv_dependencies` | Source database/table physical and logical identities and schema versions |
| `mo_mv_refresh_runs` | Run identity, epoch, sequence, status and successful source snapshot |
| `mo_mv_account_state` | Account revision, retirement and the shared lifecycle lock |

## Refresh and failure handling

1. Validate the active account, definer and source grants. Acquire bounded
   per-CN admission and reserve a durable run ID and requested sequence.
2. Take the account's existing metadata/lifecycle lock in a separate pessimistic
   read-committed transaction: `SELECT revision FROM mo_mv_account_state WHERE
   account_id = ? FOR UPDATE`. Hold this fence until publication finishes. This
   publication fence does not write the account revision.
3. Start a separate optimistic snapshot-isolation transaction, with a minimum
   committed timestamp taken from the fence. Load the current definition,
   check its epoch and sequence, and validate the pinned source identities and
   schema versions. The account lock serializes lifecycle operations and
   publishers within an account, including publishers on different CNs.
4. Delete old backing rows and insert the stored SELECT result in that SI
   transaction. Only a private capability bound to the account, MV table and
   exact transaction ID authorizes these writes. Verify that the complete
   source snapshot timestamp has not changed during execution.
5. In the same transaction, mark the MV populated and commit successful
   sequence, run ID, source physical/logical timestamp and terminal run status.
   This commit is the publication point. Release the fence through the existing
   executor's transaction cleanup.
6. On error, cancellation or timeout, roll back the replacement. Record a
   sanitized failure conditionally; it must not overwrite an already committed
   success. If a commit response is lost, resolve the durable last-run identity
   before reporting failure. Duplicate delivery of the same successful run is
   a no-op; an older sequence is superseded and an obsolete policy epoch cannot
   publish.

The SI snapshot is acquired after the outer lock is granted, using
`WithMinCommittedTS(fence.Txn().SnapshotTS())`; it is not a snapshot chosen before
waiting for a competing lifecycle operation. Ordinary rollback/commit releases
both transactions through their respective executor owners. After process loss,
native transaction recovery releases the lock and rolls back an uncommitted
replacement; a committed publication is resolved by its durable run identity.
The 300-second refresh context bounds lock waiting and execution, and bounded
transaction retries do not add a second independent lock owner.

The reconciler uses the existing taskservice rather than a separate worker
system. It recovers interrupted runs and schedules due policies with stable
run identities. Policy changes advance the epoch. DROP and account retirement
use the same account fence, so a delayed task cannot resurrect removed metadata.
Task retries retain the run identity; transaction retries are owned by the
existing executor and bounded to three attempts.

Admission allows two active refreshes per CN, with fixed-size account buckets
for active/pending work; bucket collisions conservatively serialize accounts.
Each account is limited to 128 MVs. Refresh has a 300-second deadline and a
512 MiB target-write budget. Reconciliation reads bounded pages; history cleanup
removes bounded batches beyond the recent 100 runs or seven days. These limits
bound admission and retained history, not the source scan cost of a full refresh.

## Readers, DDL and security

Readers use ordinary MVCC and see a complete stored generation at their chosen
snapshot. Cross-CN visibility follows MO's normal snapshot propagation; it does
not imply globally fresh reads. A causal handoff is required when a test or
application needs a second CN to observe a completed publication immediately.

Readers need both MV and source privileges. Refresh rechecks the captured
definer's existence, active role, source grants and account status; a scheduler
does not bypass user admission through its privileged catalog executor.
Suspended, restricted or retired accounts cannot refresh. Failure messages in
refresh history do not persist raw SQL, data or connection errors.

Public INSERT/UPDATE/DELETE/LOAD writes to an MV are rejected. DUMP/LOAD and
native DATA BRANCH consumers reject MV endpoints, including unpopulated MVs,
before accessing their rows, files or merge operations. Branch snapshot and
authorization checks still run before kind admission.

Source DML remains allowed. Destructive source DDL is rejected after ordinary
name/lifecycle locking and dependency validation. In particular, source DELETE
must execute row DML rather than becoming TRUNCATE while an MV depends on it.
Prepared DELETE rebinds when it could have cached truncate eligibility before
another session created the MV. Prepared SHOW CREATE also rebinds so a changed
refresh policy is exported correctly. Dropping an MV removes its dependencies;
database deletion restricts surviving MVs. Account deletion fences tasks and
atomically removes its MV objects.

## Compatibility and Git4Data

Migration 4.0.18/offset 5 requires 4.0.17 and common protocol 122. The upstream
4.0.17 MEDIUMINT tenant repair stays intact and runs first. Main retains protocol
120 for session ownership and 121 for DECIMAL DIV; MV admission uses its own
protocol 122 so neither older capability implies MV support. Catalog installation
is transactional and restartable; task executor 17 is registered only after
catalog and common-protocol readiness. A protocol-121 CN blocks MV migration
and task admission. An old binary cannot open the upgraded catalog for downgrade.

Definition format 1 remains readable, but refresh requires recreation with
format 2 because missing authoring context cannot safely be guessed. Migration
pauses legacy periodic policies and advances their epoch. Disabling the session
feature gate prevents new feature use; it is not a catalog downgrade mechanism.

Git4Data contributes native MVCC source timestamps and physical/logical source
identity to provenance. These identify the snapshot used for a publication;
they are not Git commits or a supported MV checkout/clone API. Logical branch,
backup/export, restore and historical MV behavior are outside P1. Operators must
retain source definitions/data and recreate/repopulate MVs for those workflows;
P1 does not promise that copying backing rows reconstructs a valid MV. Logical
snapshot restore/clone and PITR reject MV objects; account/cluster restore does
not reconstruct MVs as paused objects. System-catalog restore preserves the live
MV control catalogs instead of replacing them with historical refresh metadata.
Native crash/restart recovery of the same database is supported and is tested
separately from these unsupported logical restore operations.

The P1 decisions are therefore: retain the outer account RC lock for correctness
despite serializing account refreshes across CNs; choose the SI source snapshot
after that lock; and reject logical MV restoration rather than inventing missing
definition/definer/provenance state. These decisions require maintainer review
of this design revision; prior design approval does not imply approval of the
updated contracts.

## Alternatives and cost

[StarRocks asynchronous MVs](https://docs.starrocks.io/docs/using_starrocks/async_mv/Materialized_view/)
provide the precedent for a stored SELECT, explicit refresh policies and later
rewrite. MO uses its own SI transactions, source MVCC timestamps and taskservice;
StarRocks partition versions and edit-log publication do not map directly to it.

| Approach | Benefit | Cost or reason not selected for P1 |
| --- | --- | --- |
| Logical view / application summary table | Existing functionality; no new catalog | Recomputes every read or leaves safe publication and recovery to applications |
| Native full-refresh MV (selected) | Atomic publication using existing storage and transactions; independently testable SQL contract | Recomputes all source rows and retains a stored result; freshness follows refresh policy |
| Incremental aggregate table / CDC maintenance | Lower refresh cost for suitable updates | Adds update/retraction semantics, change tracking and recovery coordination before establishing the basic MV contract |
| Immediate transparent rewrite | Existing queries benefit without changing SQL | Requires equivalence, freshness and cost proofs; incorrect matching can return stale or wrong results |

For N input rows and M result rows, each refresh executes the original query and
replaces M stored rows. Benefit comes from repeated reads of a much smaller
result. High-cardinality summaries may save little and still incur refresh and
storage costs. Per-account serialization also limits refresh concurrency. Users
should compare query savings with refresh cost and acceptable staleness before
enabling a periodic policy. No current-revision speedup is assumed.

## Validation and rollout criteria

| Contract | Executable coverage |
| --- | --- |
| Parsing, deterministic definitions, captured semantics and native guards | Tests beside catalog, parser, planner, compiler and frontend owners |
| Atomic rows/provenance, stale epochs, replay, failure and bounded admission | `pkg/mv/service_test.go`, `pkg/mv/task_test.go` |
| SQL correctness, permissions, tenant lifecycle, empty-result reads and cleanup | `test/distributed/cases/view/materialized_view.sql`; runner `-functional` / `-edge-check` |
| Branch rejection, prepared source DELETE, DUMP/LOAD and current SHOW CREATE | Runner `-consumer-check`, including independent session/CN changes |
| Publication visibility and concurrent refresh across CNs | Runner `-second-dsn`, with causal snapshot handoffs |
| Real old/new catalog and protocol admission, interrupted migration, downgrade refusal and crash/replay | `optools/materialized_view_recovery.py` with native binaries and probe |
| Query savings and refresh cost | Runner benchmark: full result equality, EXPLAIN, 50 alternating warm pairs and high-cardinality control |

The BVT wrapper integrates the small SQL, failure, consumer and two-CN contracts
with the existing Compose services and coverage path. Native consumer validation
must pass in both RC and SI configurations. Full recovery validation must retain
ordinary pre-upgrade data, install exactly four MV catalogs, reject an old
consumer and prove before/after-commit replay without duplicate publication.

The [runner README](../../test/materialized_view/README.md) describes reproduction
and links retained execution evidence. Rollout remains default-off while the
design is under maintainer review. Required CI, compatibility and recovery
checks must pass before enabling an experimental deployment. Rewrite,
incremental maintenance and logical history integration remain separate follow-up
decisions tracked in #30079.
