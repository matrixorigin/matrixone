# Delete Benchmark Report

- db: bench_delete_perf
- total_rows: 10000000
- delete_rows: 81920
- batch_size: 8192
- batches: 10
- started_at: 20260122_190508

|strategy|total_sec|avg_ms_per_batch|rows_per_sec|
|---|---|---|---|
in_list|25.240|2524.032|3246
subquery_in|0.332|33.198|246763
join_keys|0.350|34.965|234295
range|0.394|39.372|208067
