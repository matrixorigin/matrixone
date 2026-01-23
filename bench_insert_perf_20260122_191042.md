# Insert Benchmark Report

- db: bench_insert_perf
- total_rows: 10000000
- insert_rows: 81920
- batch_size: 8192
- batches: 10
- started_at: 20260122_191042

|strategy|total_sec|avg_ms_per_batch|rows_per_sec|
|---|---|---|---|
values_list|16.861|1686.127|4858
values_select|19.714|1971.412|4155
subquery_select|0.434|43.361|188923
