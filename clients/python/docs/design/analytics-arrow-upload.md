# Design and implement checked DataFrame/Arrow uploads with atomic commit outcomes

## Parent Issue

https://github.com/matrixorigin/matrixone/issues/30049

Tracking sub-feature: https://github.com/matrixorigin/matrixone/issues/30052

## Detail of Subtask

Implement `upload_arrow` and `upload_dataframe` using native Arrow IPC LOAD, with checked type conversion, bounded staging and precise transaction outcomes.

## Design document

### API and normalization

`upload_arrow(table, data, staging_dir, schema=None, create=True, batch_size=65536, max_staging_bytes=..., server_staging_dir=None)` returns the number of rows after an acknowledged successful transaction. `upload_dataframe` delegates to it.

Accept Arrow Table/RecordBatch/RecordBatchReader, Polars `to_arrow()` producers and pandas frames via `Table.from_pandas(..., preserve_index=False)`. Explicit schemas must match a reader schema or be applied with safe casts. Slice incoming batches to the configured batch size. A pandas/Polars source may already occupy its full input size; the API does not promise a global memory quota.

### Types and precision

Support scalar signed/unsigned integers, floats, boolean, string/binary, Decimal128, date/time/timestamp and decoded dictionaries. Reject nested/list/map/struct, Decimal256, unsupported duration input and untyped all-NULL columns.

Use TEXT/BLOB for variable data. Widen Decimal precision below 19 to 19 for the native Decimal128 bridge. Normalize timestamps to microseconds and aware values to UTC using checked Arrow casts; reject sub-microsecond loss. Validate non-nullable fields before writing. Reject invalid/reserved/duplicate identifiers and quote SQL identifiers safely, including percent-bearing names.

### Shared staging and native LOAD

Create one owned unique IPC file in the caller's existing staging directory. Write converted batches and check `max_staging_bytes` after each batch and at completion. Map the directory to a different server mount with `server_staging_dir` if needed. The client and CN must see the same bytes; this is non-LOCAL LOAD, not Arrow Flight or a network upload protocol. The size cap is checked after a batch, not a hard memory/quota reservation.

### Transaction and failure states

After staging completes, create a new table (`create=True`) or append (`create=False`) and execute native Arrow `LOAD DATA` in one transaction. Hold UTC, STRICT_ALL_TABLES and TN primary-key checks through COMMIT, then restore session settings.

- Before-commit errors: roll back creation/append; no partial rows.
- Disconnection during COMMIT: `AnalyticsCommitUnknownError`, `committed=None`; reconcile rather than retry.
- Acknowledged commit followed by session/staging cleanup failure: `AnalyticsCleanupError`, `committed=True`, with row count.
- If a primary input/DB error is already active, attach `rollback_error`, `cleanup_error` or `staging_cleanup_error` instead of replacing it.

Remove only the owned staging file in finally. SIGKILL can leave it, so external recovery must track job-owned artifacts. The SDK deliberately does not retry uncertain writes or overwrite an existing table by default.

### Acceptance criteria

All supported scalar edge cases round-trip, precision loss fails, late failures are atomic, concurrent primary-key checking admits only one winner, and every normal path reclaims staging files/connections while preserving commit evidence.

## Test cases

| ID | Input/action | Required result | Automated coverage |
|---|---|---|---|
| U01 | Every integer width at endpoints; batch sizes 1/7/1024; Decimal precision through 38 | Exact values, NULLs and scale survive | `test_integer_endpoints`, `test_decimal_extremes` |
| U02 | 16 seeded mixed datasets, Unicode/binary, NaN/Inf/-0, pandas/Polars/dictionary/UTC | Exact scalar contract and floating special-value semantics | `test_random_mixed_roundtrip`, `test_float_special_values`, `test_dataframe_pandas_polars_and_append`, `test_dictionary_and_timezone_normalization` |
| U03 | Input reader raises after an earlier batch; non-nullable NULL; staging cap/cast failure | No target/partial append and no staging leak | `test_input_reader_failure_and_not_null_cleanup`, `test_upload_failures_atomic_and_cleanup` |
| U04 | Duplicate PK after >20,000 earlier staged rows; overlapping concurrent appends/creates | Roll back complete failed writes; exactly one winner | `test_rollback_if_late_batch_duplicate`, `test_concurrent_duplicate_append_no_partial_rows`, `test_concurrent_same_target_exactly_one_creator` |
| U05 | 16 writers distributed across two CNs, shared PK | Exactly one atomic winner, same complete rows on both CNs | `test_cross_cn_pk_conflict_is_atomic`, `test_cross_cn_arrow_visibility` |
| U06 | Corrupt IPC before LOAD; drop connection before COMMIT | Rollback verified by reconnecting independently | `test_corrupt_staging_file_rolls_back_target`, `test_disconnect_before_commit_leaves_no_partial_create` |
| U07 | Forward COMMIT to server, discard successful reply | Unknown-outcome error and independently verified committed rows | `test_lost_commit_acknowledgement_is_unknown_not_rollback` |
| U08 | Fail session restoration or unlink after COMMIT; fail unlink during input error | True committed flag after success; preserve original error during failure | `test_session_restore_failure_reports_committed_write`, `test_staging_unlink_failure_preserves_commit_evidence`, `test_cleanup_failure_does_not_mask_original_reader_error` |
| U09 | Kill server precommit/postcommit/active native LOAD; kill client precommit | Exact recovered count/sum, no partial data; record and reclaim owned orphan files | `run_chaos.py` (`precommit`, `postcommit`, `active_load`, `client_precommit`) |
| U10 | Two tenant accounts with identical table names; read-only role | Isolation and authorization remain enforced | `test_arrow_tables_are_account_isolated`, `test_read_only_user_cannot_create_or_append` |

## Implementation and validation

The implementation PR is linked from the parent and sub-feature issues. Design review and merge acceptance are pending.

Native Arrow LOAD must be enabled/available. Local validation used a current-source server; support is not inferred from the server's advertised version. Published-release and production-cloud certification are not claimed.
