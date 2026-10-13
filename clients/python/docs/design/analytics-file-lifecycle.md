# Design and implement file-relation ownership, strict import and recovery semantics

## Parent Issue

https://github.com/matrixorigin/matrixone/issues/30048

Tracking sub-feature: https://github.com/matrixorigin/matrixone/issues/30051

## Detail of Subtask

Make file relations safe to use in context-managed notebooks and services: own their catalog objects, query in bounded batches, import transactionally, and expose precise failure/recovery semantics.

## Design document

### Lifecycle and ownership

`FileRelation` holds the merged schema, SQL expression, owned external-table names and source-check closures. Use `with ... as files`; close result streams before closing the relation.

Construction creates uniquely named ordinary external tables. Record each name only after successful creation. If a later creation fails, close the partially constructed relation. Normal `close()` drops only recorded owned names and retains failed drops for a possible retry. Preserve primary query/construction exceptions when cleanup also fails. Account-level isolation follows MatrixOne's catalog and privileges, not a Python-only namespace.

`files.sql`, `files.query(...)` and `files.import_into(...)` rerun source checks. The SQL expression remains reusable inside aggregate/filter SQL, but the SDK cannot enforce later checks after a caller caches that string. No immutable snapshot is implied.

### Query contract

Default file queries supply the relation's original Arrow schema. Custom SQL derives result types from protocol metadata unless the caller supplies a schema. Query parameters use DBAPI `%s` binding. The shared unbuffered `ArrowStream` owns its connection independently; relations do not promise a connection-private catalog.

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

## Implementation and validation

The implementation PR is linked from the parent and sub-feature issues. Design review and merge acceptance are pending.

Transaction/type/stream helpers are shared with the Arrow interoperability sub-features. Same-host dual-CN validation does not prove cross-host failover. See the committed test README and evidence for reproducible commands and limits.
