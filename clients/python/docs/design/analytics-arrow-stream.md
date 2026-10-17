# Design and implement bounded Arrow query results, checked casts and stream cleanup

## Parent Issue

https://github.com/matrixorigin/matrixone/issues/30049

Tracking sub-feature: https://github.com/matrixorigin/matrixone/issues/30053

## Detail of Subtask

Provide `client.analytics.query(sql, params=None, schema=None, batch_size=65536, network_timeout=None)` returning a context-managed Arrow RecordBatch iterator.

## Design document

### Connection and result lifecycle

Acquire a SQLAlchemy raw connection, use `driver_connection` (with the older 1.4 fallback), and create a PyMySQL SSCursor. This is real unbuffered retrieval rather than a buffered cursor with client-side slicing. Execute caller SQL with DBAPI-bound parameters and fetch at most `batch_size` rows per batch.

Temporarily use UTC and strict SQL mode. Full exhaustion restores settings and returns a healthy connection. Early close, cast/read failure or unread rows invalidate the connection rather than draining the entire result. Context exit is required for abandoned streams. This releases client resources; it is not a server-side KILL/cancellation contract.

### Type metadata and checked conversion

Derive the source Arrow schema from MySQL field metadata, not the first row, so empty/all-NULL results retain types. Handle integer sign/width, Decimal precision/scale, boolean, float, date/timestamp and binary versus text. SQL TIME defaults to Arrow duration in microseconds to preserve negative and >24-hour values. Reject duplicate output names (callers should alias them) and unsupported protocol types.

Build a batch in its protocol-derived types, then apply any explicit schema with safe casts. Do not construct directly in the requested target type: Arrow array construction can truncate fractional floats even with `safe=True`. Add explicit checks for inexact float narrowing and timestamp-to-date information loss. Reject boolean values outside 0/1; boolean-to-integer conversion remains lossless.

### Timeouts and bounds

`network_timeout` is a positive finite read/write inactivity timeout, defaulting to `Client.query_timeout`; it is not a whole-query deadline. Restore overridden socket timeouts on normal exhaustion. The synchronous Client must pass its existing connect/read/write timeout options to PyMySQL.

Iteration bounds row count per batch, not bytes per cell. `to_arrow()` intentionally materializes the remaining result. Transport remains MySQL row protocol converted to Arrow; neither zero-copy result transport nor Flight is claimed.

### Acceptance criteria

Preserve declared/protocol scalar types and NULLs, reject requested lossy conversions, retain bounded batches on large results, and release pool capacity after exhaustion, early close, type errors, stalled networks and server crashes.

## Test cases

| ID | Input/action | Required result | Automated coverage |
|---|---|---|---|
| S01 | Empty/all-NULL result and exact explicit schema | Stable metadata-derived types and correct NULLs | `test_empty_parquet_and_explicit_all_null`, `test_all_null_explicit_schema` |
| S02 | Negative/>24-hour TIME and boolean columns; boolean-to-integer schema | Preserve duration and lossless bool/int contract | `test_protocol_time_duration_and_boolean`, `test_boolean_to_integer_is_lossless` |
| S03 | Fractional float to integer; narrow float; timestamp/date or timestamp-unit loss | Explicit conversion error with no leaked connection | `test_explicit_result_schema_cannot_silently_truncate`, `test_result_conversion_failure_releases_connection` |
| S04 | Read first batch then exit; repeat early close 160 times | No drain of all results, FD/connection counts remain bounded | `test_early_close_and_bound_batch`, `test_repeated_partial_result_no_fd_leak` |
| S05 | Single-connection pool, retain a stream then attempt another | Expected exhaustion; reuse succeeds after close | `test_small_pool_exhaustion_recovers` |
| S06 | Actual packet proxy stalls result delivery with 250 ms timeout | Timeout, zero checked-out connections and successful subsequent query | `test_stalled_network_times_out_and_releases_pool` |
| S07 | Consume 1,000,000 rows with 256-byte payload under +128 MiB address-space cap | Exact count/checksum; successful streamed read while buffered control hits memory/error deadline | `run_soak.py` constrained-memory workers |
| S08 | Kill server after first streamed batch, then restart | Interrupted read error, released connection, complete persistent source data after restart | `run_chaos.py` (`streaming`) |
| S09 | SQLAlchemy 1.4.54/2.1.4 and PyArrow 18.0/18.1/21 | Compatibility matrix passes without skips | Full `tests/analytics` matrix |

## Implementation and validation

The implementation PR is linked from the parent and sub-feature issues. Design review and merge acceptance are pending.

Local measured 1M-row streaming peaked at 161.68 MiB RSS from a 153.28 MiB baseline. This is a reproducible workload measurement, not a fixed memory bound for every query. The same constrained buffered control exceeded its 30-second deadline; it was terminated, not falsely reported as a returned MemoryError.
