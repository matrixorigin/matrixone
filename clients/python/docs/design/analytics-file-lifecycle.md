# Design and implement file-relation ownership, strict import and recovery semantics

## Parent Issue

https://github.com/matrixorigin/matrixone/issues/30048

Tracking sub-feature: https://github.com/matrixorigin/matrixone/issues/30051

## Detail of Subtask

Make file relations safe to use in context-managed notebooks and services: own their catalog objects, query in bounded batches, import transactionally, and expose precise failure/recovery semantics.

## Design document

### Lifecycle and ownership

`FileRelation` holds the merged schema, SQL expression, exact owned external-table identities and source-check closures. Use `with ... as files`; close result streams before closing the relation. Independent relations may run concurrently. Sharing one relation between concurrent query/import/close operations is outside this API's contract.

The relation retains the original SQLAlchemy engine for creation, query, import and cleanup. Reconnecting the Client to a different endpoint or account does not retarget an existing relation. A pinned engine alone does not freeze pooled connections' current database: public SQL `USE` and different connections in one pool can select different databases.

For **each** source, acquire its CREATE transaction connection and read `SELECT DATABASE()` on that same connection. Require a non-empty valid database identifier. Store ownership as an immutable `(database, table)` pair and reserve it immediately before CREATE, before CREATE or COMMIT can have an uncertain outcome. Connection acquisition, database lookup or identifier-validation failure adds no reservation for that attempt; any previously reserved sources still undergo cleanup. Read the actual session database, never infer it from the engine URL or Client connection options. No CREATE is retried.

Qualify every owned CREATE, relation SELECT and DROP with that frozen database and table. Quote the two identifier components separately, including database names containing dots or backticks; never split a captured database string on dots. The reusable `.sql` expression contains ordinary SQL quoting and literal percent signs. Driver-executed CREATE/DROP/import escape literal percent signs once for PyMySQL interpolation; parameters remain bound separately. Custom query SQL follows the existing DBAPI percent/parameter convention.

A definite, non-disconnect MySQL table-exists rejection (1050) releases only that attempted identity without DROP. The pre-existing object is not owned by this relation; even a forced UUID collision must preserve its contents. The rejected CREATE is not retried.

On any construction failure, close the partially constructed relation using a new transaction/usable pooled connection from the original engine. `DROP TABLE IF EXISTS` is restricted to its exact reserved `(database, table)` pairs, so both committed and rolled-back CREATE outcomes can be reclaimed. Remove an identity only after the DROP transaction returns successfully. A failed or ambiguously acknowledged DROP remains reserved and can be retried through the same relation, even if the cleanup connection uses another database. Attempt every pending DROP, retain failed identities, and report the first cleanup error. There is no prefix scan, background reaper, blind construction retry or caller-supplied cleanup engine/name.

The original construction exception remains primary. It exposes `attempted_tables` (all reservations at failure) and `owned_tables` (reservations still pending after cleanup) as tuples of fully quoted `database.table` SQL strings, retaining their string shape while making database identity unambiguous. It also exposes `file_relation` (the scoped owner whose `close()` can be called again). If cleanup also fails, attach `cleanup_error`. Successful cleanup leaves `owned_tables=()` and a closed relation. Diagnostic logging cannot replace the original error or this recovery evidence. The same error precedence applies when a context body already failed: failed relation cleanup attaches `cleanup_error`, retains pending ownership for retry, and a logger failure cannot replace the body error. Use one relation-scoped cleanup-after-error helper for construction and context exit; a context with no primary error propagates close failure normally.

The state transitions are **unreserved → connection/database verified → qualified identity reserved before CREATE → returned relation after acknowledged COMMIT → removed only after acknowledged DROP**. Failure at any point after reservation goes to cleanup; a cleanup failure retains the same owner and pending names. This ordinary exception path is separate from the SIGKILL limitation below.

`files.sql`, `files.query(...)` and `files.import_into(...)` rerun source checks. The SQL expression remains reusable inside aggregate/filter SQL, but the SDK cannot enforce later checks after a caller caches that string. No immutable snapshot is implied.

### Query contract

Default file queries supply the relation's original Arrow schema. Custom SQL derives result types from protocol metadata unless the caller supplies a schema. Query parameters use DBAPI `%s` binding. The shared unbuffered `ArrowStream` owns its connection independently and uses the relation's original engine; relations do not promise a connection-private catalog. A caller-supplied unqualified table in custom SQL or `import_into(table)` retains ordinary resolution on that operation's connection. Qualify such targets when database selection matters; source-table ownership is always qualified.

### Atomic import

`files.import_into(existing_table)` executes one `INSERT INTO ... SELECT ...` transaction. Quote target identifiers including DBAPI percent handling. Before work, record timezone/SQL mode/TN PK-check settings; enable UTC, strict conversion and `mo_pk_check_by_dn=1`. Keep those settings through COMMIT, then restore them before returning a healthy connection.

Failures before COMMIT roll back the import. A disconnected COMMIT acknowledgement raises `AnalyticsCommitUnknownError` with `committed=None`, requiring reconciliation. A proven commit followed by restoration failure raises `AnalyticsCleanupError` with `committed=True`. Do not automatically retry either outcome. Secondary rollback/restore exceptions attach to the primary error instead of replacing it.

### Security and crash boundary

Creation requires CREATE/DROP; import requires target write privileges. Ordinary external tables may be visible to other users with broad privileges, so callers needing privacy must use a suitable private workspace database. The SDK does not bypass CN authorization or store remote credentials.

SIGKILL bypasses Python finally blocks and can leave catalog entries. Recovery must track and reclaim artifacts owned by the interrupted job; prefix-only global deletion can destroy another active client's objects. Automatic crash reaping is outside this change.

### Acceptance criteria

Successful/failed normal contexts leave no owned tables or checked-out connections. Failed imports cannot partially append. Independent relations and tenant accounts do not overwrite/read each other's objects. Commit and cleanup outcomes remain inspectable.

## Test cases

| ID | Input/action | Required result | Automated coverage |
|---|---|---|---|
| L01 | Invalid SQL inside context; failure after first external-table creation | Primary error preserved and all already-owned objects dropped | `test_query_errors_cleanup_and_pool_reuse`, `test_partial_relation_creation_cleanup` |
| L02 | Concurrent relations over distinct sources | Distinct names and complete correct independent results | `test_concurrent_relations` |
| L03 | Delete or change a file after relation creation; overwrite same-schema remote object | Missing/drift error; same-schema update visible | `test_source_deleted_after_creation_fails`, `test_changed_schema_rejected_before_query`, `test_remote_live_same_schema_update_is_visible` |
| L04 | Import overflow while caller session is permissive | Error, no partial data; restore caller settings | `test_file_import_strict_under_permissive_session`, `test_file_import_and_timezone_pool_isolation` |
| L05 | Create relation/import on CN2 and read on CN1 | Shared catalog visibility and exact count/sum | `test_cross_cn_external_and_import` |
| L06 | Two accounts, same logical names; read-only role | Account isolation; unauthorized creation/import rejected | `test_external_catalog_is_account_isolated`, `test_read_only_user_cannot_create_or_append` |
| L07 | 8-thread mixed 90-second measured soak after 30-second warmup | Zero unexpected errors, owned catalog leaks and outstanding connections; FD/RSS thresholds enforced | `run_soak.py` |
| L08 | Kill client while holding an external relation | Record orphan owned names; restart/reconnect and explicitly reclaim only those names | `run_chaos.py` (`client_external`) |
| L09 | CREATE commits but its COMMIT acknowledgement is dropped | Original error retained; fresh-connection DROP reclaims the exact reserved UUID; independent catalog check finds no orphan | `test_relation_create_lost_commit_acknowledgement` |
| L10 | CREATE failure plus failed DROP or lost DROP acknowledgement | Original error includes attempted/pending names, cleanup error and retryable scoped relation; no CREATE retry; later close reclaims only pending names | `test_relation_construction_ownership_faults` |
| L11 | Forced UUID collision or a definite table-exists transaction rejection | Existing object and its values preserved; no DROP and no CREATE retry | `test_relation_name_collision_preserves_existing_table`, `test_commit_collision_does_not_drop_foreign_table` |
| L12 | Create in a, public `USE b`, with/without unrelated b.same_name; also switch before CREATE | Query reads the actual creating database; close drops only the owned object, preserves sentinel 42, and independently leaves no owned entry | Focused database-switch ownership regressions and native public-Client probe |
| L13 | Multi-connection/multi-source pool, database punctuation, failed database lookup; reconnect Client before query/import/cleanup | Each identity uses its actual creating session database; quoting survives driver interpolation; no reservation before an attempted CREATE; all relation operations retain original engine | Deterministic ownership fault model plus applicable native boundary controls |
| L14 | Context body fails, then DROP and warning logger fail; repeat close after recovery | Original body error preserved with cleanup evidence; exact identity remains retryable; successful context without a primary propagates cleanup failure | Deterministic relation ownership/logger fault model |

## Implementation and validation

Implementation: https://github.com/matrixorigin/matrixone/pull/30054

Design decision: **accepted for implementation**, review `PR30054-DESIGN-2026-10-25` by `gpt-6.1-sol` with `xhigh` reasoning. This approval covers the four analytics designs together; implementation acceptance remains subject to their validation criteria. The PR must link the exact committed design revision. Shared alternatives, budgets, security and delivery decisions are in [the reviewed contract](analytics-file-lifecycle.md#reviewed-design-and-delivery-contract).

Transaction/type/stream helpers are shared with the Arrow interoperability sub-features. Same-host dual-CN validation does not prove cross-host failover. See the test README for reproducible commands and limits. Historical local reports are not current acceptance evidence.


## Reviewed design and delivery contract

This section applies to all four analytics designs, issues #30048–#30053 and PR #30054. Approval `PR30054-DESIGN-2026-10-25` is a distinct design-first decision; the PR records the approved versioned revision and reviewer invocation. An implementation deviation affecting these invariants requires review of that decision before delivery.

### Architecture and alternatives

Reuse existing MatrixOne external tables and Arrow IPC LOAD, SQLAlchemy pooling and PyMySQL's MySQL row protocol. [DBAPI 2.0](https://peps.python.org/pep-0249/) defines bound execution and errors; [MySQL identifier quoting](https://dev.mysql.com/doc/refman/8.0/en/identifiers.html) guides separately quoted components; [SQLAlchemy reset-on-return](https://docs.sqlalchemy.org/en/20/core/pooling.html#reset-on-return) does not establish an immutable session database. [Arrow columnar/IPC types](https://arrow.apache.org/docs/format/Columnar.html) define the staging/result representation. Native LOAD and scanner limitations remain MatrixOne-specific and require real-server proof.

Hand-written DDL and buffered results leave lifecycle and memory work to every caller. Native read_* functions, connection-private temporary tables and Arrow Flight could improve that boundary, but need new server capabilities or transport contracts; holding one connection for a relation would also occupy scarce pool capacity and does not solve uncertain persistent CREATE outcomes. Reuse the current server instead. Row INSERT uploads avoid a shared staging path but add per-row encoding/statement work; IPC staging is the selected opt-in batch path. Fully buffering then slicing violates the streaming goal; SSCursor is the supported bounded-row alternative. Reading only engine configuration, resetting every checkout to a configured database, or scanning a name prefix cannot prove exact ownership after pooled-session changes; capture each actual creating session's database instead. No new framework or background service is needed.

### Owners, bounds and control

The relation owns at most `max_files` catalog identities (default 256), source checks and SQL branches after discovery. SQL/schema state scales with files times columns. Glob/path discovery may materialize matches before enforcing the catalog limit; this API is not a global memory quota. File checks use bounded Arrow blocks, but validating CSV performs additional full-source I/O on each checked access. Remote filesystem timeouts and credentials remain caller-managed. Missing/drifting sources fail without claiming an immutable snapshot.

An upload owns one restrictive, uniquely created staging file and one transaction connection. The default staging limit is 1 GiB, checked after each batch and the IPC footer; overshoot may include the final encoded batch/footer. Conversion uses slices (default 65,536 rows); producer memory and a single large value are outside that budget. A stream owns one checked-out connection, an unbuffered cursor and at most the current row/conversion batch; callers retaining returned batches own that memory. `to_arrow()` deliberately materializes the remainder. Pool size/overflow/wait and driver connect/read/write timeouts retain their configured bounds. There are no background workers, unbounded retry queues or automatic write retries.

These synchronous APIs introduce no whole-operation deadline or server KILL contract. An explicit stream timeout is a positive finite socket inactivity budget, not a total deadline; omitted overrides retain Client query-timeout behavior. Ordinary Python exceptions trigger scoped cleanup, while abandoned streams require context exit/close. A disconnected COMMIT stays unknown; it is never reclassified as rollback or silently retried. Postcommit cleanup failures preserve acknowledged commit state, and secondary rollback/session/staging/relation cleanup errors attach to an existing primary error. SIGKILL cannot execute Python cleanup and requires explicit recovery of known job-owned artifacts.

### Security, compatibility and rollout

All SQL uses the caller's original endpoint/account and server authorization. Source paths and load options are bound values; generated/column/database identifiers are validated and quoted. External tables are ordinary catalog objects and require suitable CREATE/DROP privileges. They do not create a private SQL namespace: sensitive workloads need a caller-provisioned private database. Tenant local-file restrictions are enforced by the server; a test environment that forbids such paths is not permission to bypass the restriction. Remote Arrow credentials/stages are caller-managed. Do not log credentials or authentication packets.

The feature is additive and opt-in through the `analytics` extra; importing/using core SDK APIs must still work without PyArrow. No server schema/wire/config migration or asynchronous API is introduced. Existing buffered query/load APIs retain their behavior. The new draft recovery fields remain tuples of strings but now include quoted database identity. SQLAlchemy 1.4/2.x and the declared PyArrow minimum require supported API-boundary coverage; a larger historical matrix is not silently claimed current. Native Arrow LOAD capability, shared staging visibility and compatible scanner behavior are prerequisites; a server's advertised version alone does not certify them.

Roll out in a test/private workspace using one source and a small upload/query first, then enable caller-selected batch/file/staging limits. Disabling the opt-in client path needs no catalog migration. Exit existing contexts before upgrading/removing the SDK; a crashed job's retained external tables/files need exact-identity manual reclamation. Inspect error type, `committed`, row count and scoped pending ownership for diagnosis. Do not promise cross-host/cloud certification, a fixed memory ceiling, exactly-once recovery after SIGKILL, or a new performance ratio.

### Acceptance and evidence

Retain the distinct F/U/S/L test oracles and reproducible fault/memory/crash tools. Focused injected tests prove identity capture, failure ordering, quoting and pool/reconnect controls with one or two files/rows. Real public Client/SQLAlchemy/PyMySQL execution proves `USE` before/after creation, query/import origin and independent catalog/sentinel cleanup. Existing native lost-CREATE/COMMIT-ack and collision controls must still pass with qualified ownership; a model cannot replace this changed server boundary. No new SQL engine behavior is introduced, so these SDK integration tests supply the relevant public-path proof without duplicating them in mo-tester SQL files.

Run the affected analytics tests and existing SDK offline suite, configured Python formatting/static checks and package build. Preserve nonempty selection, terminal exit status, source/test/environment fingerprints and teardown evidence outside the tracked tree. Reuse previous tests only when their relevant inputs and raw outcomes remain available; label historical summaries, skipped topology/permission cases and pending CI accurately. No Go delta means Go vet/race is inapplicable; there is no Python race detector gate to fabricate. Distinct relations/pools get deterministic interleaving tests; this design adds no same-relation thread-safety promise.

Million-row memory and crash/restart are existing product acceptance dimensions, so retain their runnable tools and any independently valid evidence. An unrelated full soak/chaos campaign is not needed to prove this identity repair when the affected boundary is closed by focused native faults and catalog controls. Historical raw outcomes that cannot be reconciled remain a stated qualification limit, not a passing current run. Generated local evidence JSON and calibration histories belong outside source; preserve the actual oracles and report only supported claims.
