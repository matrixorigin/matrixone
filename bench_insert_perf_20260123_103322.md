# Insert Benchmark Report

- db: bench_insert_perf
- total_rows: 10000000
- insert_rows: 81920
- batch_size: 8192
- batches: 10
- started_at: 20260123_103322

|strategy|total_sec|avg_ms_per_batch|rows_per_sec|
|---|---|---|---|
values_list|15.160|1516.049|5404
values_select|16.953|1695.303|4832
subquery_select|0.562|56.167|145851
