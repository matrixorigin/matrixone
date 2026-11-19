# Design and implement file schema inference, multi-file relations and Hive partitions

## Parent Issue

https://github.com/matrixorigin/matrixone/issues/30048

Tracking sub-feature: https://github.com/matrixorigin/matrixone/issues/30050

## Detail of Subtask

Provide `read_parquet`, `read_csv`, `read_json` and the common `read_files` path. A relation must expose a stable expanded source list and a logical Arrow schema suitable for native MatrixOne scans.

## Design document

### API and source discovery

`read_files(paths, format, schema=None, union_by_name=False, delimiter=',', hive_partitioning=False, server_paths=None, filesystem=None, max_files=256)` returns `FileRelation`.

- Local paths/globs expand deterministically and are deduplicated. Enforce 1..max_files before creating catalog objects.
- Remote sources require an explicit file list and Arrow filesystem. Map it one-to-one to server-visible file or `stage://` paths; do not infer credentials or expand remote globs.
- Use Parquet metadata and initial Arrow CSV/JSON blocks to infer schemas. PyArrow 18 lacks `open_json`, so its JSON fallback reads at most 1 MiB and discards an incomplete trailing record. An oversized first record is rejected.
- Explicit schemas change types, not CSV header order or Parquet's underlying timestamp precision.

### Schema contract

Reject case-insensitive duplicate/reserved/invalid identifiers and unsupported nested, Decimal256 or untyped-null columns. Quote backticks and account for DBAPI percent processing.

Default mode requires equal logical schemas. `union_by_name` merges supported schemas, inserts typed NULL for missing columns and marks sparse fields nullable. Reject potentially lossy integer-to-float, unsafe signed/unsigned and Decimal-to-float promotions. Hive keys become strings (preserving zero-padding); the default partition marker becomes NULL. Reject collisions with physical columns.

Native file scans truncate nanosecond timestamps/time, so reject those metadata types even when an explicit schema is supplied. Require a prior checked rewrite to microseconds.

### Native SQL plan

Create one uniquely named ordinary external table per source file using bound path/format parameters, then produce a parenthesized `UNION ALL` with aligned projections and typed missing fields. Native MatrixOne executes the query; DuckDB is only a test oracle. These are not connection-private temporary tables or new native SQL table functions.

On each actual CREATE connection, capture and validate its current database, then reserve that immutable `(database, generated table)` identity before CREATE, including its transaction's COMMIT. Qualify CREATE and every SELECT with separately quoted database/table identifiers; a changed pooled default database must not retarget an owned source. A lost acknowledgement must leave a cleanup owner even when construction never returns a relation. See [the lifecycle design](analytics-file-lifecycle.md) for scoped cleanup, uncertain outcomes and recovery evidence on the original exception.

### Validation and consistency

Validate all CSV record widths, quoting and UTF-8 in bounded blocks because the native scanner can silently ignore extra fields. This costs additional client I/O, including remote reads. CSV uses MatrixOne's `\N` null convention. Quoted newlines and BOM are supported.

Capture source schema checks and run them before relation SQL access/query/import. Missing files and observed schema drift fail; same-schema content changes remain visible. Preflight and scan are not an atomic snapshot. JSON inference/checks inspect the initial block; malformed later input must still fail during native scan.

### Acceptance criteria

Supported files produce correct logical columns/aggregates, unsafe mappings fail before exposing a relation, and errors during construction reclaim previously created owned catalog objects.

## Test cases

| ID | Input/action | Required result | Automated coverage |
|---|---|---|---|
| F01 | Parquet/CSV/JSONL, 8 fixed random seeds per format | Complete aggregates match independent DuckDB values | `test_file_differential`, `test_file_query_duckdb_oracle` |
| F02 | Mixed schemas, sparse non-nullable columns, signed/unsigned extremes, Decimal/float | Correct nullable merge or explicit loss-prevention error | `test_multifile_union_and_hive`, `test_sparse_nonnullable_schema_merge`, `test_union_integer_precision`, `test_union_decimal_does_not_become_float` |
| F03 | Hive zero-padded/default-null partitions | Correct string/NULL partition contract | `test_hive_default_null`, `test_multifile_union_and_hive` |
| F04 | Truncated Parquet, bad JSON, CSV extra/missing fields including a late row beyond 1 MiB | Fail rather than silently discard or truncate | `test_corrupt_parquet_rejected`, `test_bad_json_fails_closed`, `test_bad_csv_fails_closed`, `test_csv_late_extra_field_rejected` |
| F05 | CSV BOM/quoted newline/custom delimiter/quoted null; explicit header mismatch | Preserve values/conventions; reject mismatched header | `test_csv_embedded_newline_and_bom`, `test_csv_null_and_custom_delimiter`, `test_csv_explicit_schema_header_mismatch` |
| F06 | Nanosecond Parquet with/without explicit schema | Reject before native precision loss | `test_nanosecond_parquet_rejected_before_loss` |
| F07 | 64 files/limit exceeded, hostile identifiers | Correct bounded relation or validation error | `test_many_files_and_limit`, `test_hostile_column_names` |
| F08 | S3-compatible Parquet codecs, remote CSV/JSON missing objects and drift | Correct values or explicit error | `test_remote_parquet_codecs`, `test_remote_missing_object_is_error`, `test_remote_schema_drift_rejected` |
| F09 | PyArrow 18 JSON with an incomplete block tail / >1 MiB first record | Exactly one bounded read / explicit rejection | `test_arrow18_json_inference_is_bounded`, `test_arrow18_json_oversize_record_rejected` |

## Tradeoffs and open boundaries

Initial-block inference can miss later type changes; callers can provide an explicit schema. Full CSV validation adds I/O. Remote glob discovery, immutable snapshots, nested scan types and native SQL read_* functions are not delivered here.

## Implementation and validation

Implementation: https://github.com/matrixorigin/matrixone/pull/30054

Design decision: **accepted for implementation**, review `PR30054-DESIGN-2026-10-25` by `gpt-6.1-sol` with `xhigh` reasoning. This approval covers the four analytics designs together; implementation acceptance remains subject to their validation criteria. The PR must link the exact committed design revision. Shared alternatives, budgets, security and delivery decisions are in [the reviewed contract](analytics-file-lifecycle.md#reviewed-design-and-delivery-contract).

Tests reside in `clients/python/tests/analytics/`; the PR includes design documents, commands and recorded local evidence. Local S3 emulation is not cloud-provider certification.
