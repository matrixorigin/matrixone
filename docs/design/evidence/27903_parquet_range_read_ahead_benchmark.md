# PR 27903 Parquet range read-ahead benchmark

Date: 2026-08-31 (Darwin/arm64, Apple M4)

This is the deterministic local benchmark for the bounded ReaderAt used by the
row-group fanout path.  It reads an 8 MiB synthetic payload in sequential 64
KiB ReaderAt calls with a fixed 100 microsecond simulated range latency.  It is
not an endpoint throughput claim; endpoint wall time, memory, and network
resource capture remain rollout evidence for a representative object store.

Command:

```sh
.agents/skills/mo-dev/scripts/mo-cgo-test \
  -run '^$' -bench '^BenchmarkParquetRangeReadAheadSequential$' \
  -benchtime=1x -count=1 ./pkg/sql/colexec/external
```

Terminal result:

```text
BenchmarkParquetRangeReadAheadSequential/direct-10       1  21724833 ns/op
  8388608 fetched_bytes/op  0 peak_cache_bytes  128 range_calls/op
BenchmarkParquetRangeReadAheadSequential/read_ahead-10   1   6851625 ns/op
  8388608 fetched_bytes/op  262144 peak_cache_bytes  32 range_calls/op
PASS
```

The instrumented benchmark reports range calls, fetched bytes, retained cache,
and the configured range latency.  The corresponding focused tests verify
sequential coalescing, sparse-read amplification bounds, error propagation,
and concurrent ReaderAt safety.
