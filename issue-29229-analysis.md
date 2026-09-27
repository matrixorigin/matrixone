# Issue 29229: LOAD scheduling and Parquet budget costs

## Updated evidence (2026-09-28)

The earlier conclusion that assignment casts were the likely f423-specific
amplifier was not established by the experiments. A more direct regression is
the planner's interpretation of BLOB capacity as estimated payload size.

`f42330b618` changes newly declared BLOB Width from 0 to 65535.
`estimateLoadRowsize` uses `GetRowSizeFromTableDef * 0.8`; the latter sums
positive declared widths. `makeLoadExternalStats` divides file bytes by this
estimate and computes blocks from estimated rows. `GetExecType` then uses
estimated rows and blocks for TP/AP and multi-CN classification.

The original 31-column DDL and 10,078,239,726-byte file reproduce:

| Calculation | Estimated row bytes | Estimated rows | Blocks | Scan execution class |
| --- | ---: | ---: | ---: | --- |
| f423 | 53,100 | 189,798 | 24 | TP |
| BLOB capacity excluded from Parquet payload estimate | 691.2 | 14,580,787 | 1,780 | AP multi-CN |

These are planner estimates, not measured row sizes or actual row counts.
`TestIssue29229ParquetLoadExecutionClass` demonstrates this deterministically.

## Profiles

The e5 profile is now retrievable; the earlier report that it was unavailable
must not be treated as a permanent limitation. All following queries select
the exact namespace and `matrixorigin_io_component="CNSet"`.

| Commit | UTC window on September 27 | CPU seconds | bytealg.Count self seconds |
| --- | --- | ---: | ---: |
| e5d7afd | 07:30–08:10 (40 min) | 63,502.30 | 44,518.48 |
| 8d6fc5a (cast experiment) | 12:00–13:30 (90 min) | 6,026.60 | 3,418.63 |
| 93b5ed8 (budget experiment) | 14:10–15:30 (80 min) | 2,302.31 | 20.31 |

Windows differ; totals must not be compared as equal-duration measurements.
Average sampled CPU consumption is approximately 26.46, 1.12, and 0.48 cores,
respectively. Whole-process profiles also include background work. They support
the loss-of-parallelism diagnosis, but do not alone prove operator DOP.

The original f423 profile also has the definition-level scanning hotspot.
`rowsToSourceBudget` repeatedly slices individual rows; parquet-go's nullable
page slicing scans each definition-level prefix. This pre-existing overhead
also exists in e5, which completes using much more parallel CPU capacity.

## End-to-end experiments

| Run | Change | Result |
| --- | --- | --- |
| 36302687736 | e5 parent | SQL success, 2737854.33 ms |
| 36305928057 | f423 | cancelled after more than 100 min, no completion time |
| 36315657099 | f423, skip Parquet same-type BLOB/TEXT cast | cancelled after more than 100 min |
| 36315703208 | f423, binary-search source byte budget | SQL success, 5115168.07 ms |

The cast experiment retains the new BLOB width and therefore the bad estimate.
The budget experiment removes a CPU hotspot but retains the scheduling issue.
Neither isolates the metadata-width effect. Cancellation at 100 minutes is not
a demonstrated natural 180-minute timeout. Low S3 CPU time does not exclude
network latency or bandwidth limitations, because waiting is absent from CPU
profiles.

## Candidate changes and validation

Preserve declared BLOB widths and all assignment validation. For the Parquet
schema fallback estimate only, use the legacy BLOB estimate rather than the
family's maximum capacity. This is a compatibility estimate, not a substitute
for future Parquet footer-based cardinality statistics.

Retain the experimental dictionary prefix binary search, with additional
independent boundary tests for optional dictionary strings, nulls, empty values,
skewed lengths, and nonzero page offsets.

Local focused tests passed for both packages:

    go test ./pkg/sql/plan ./pkg/sql/colexec/external \
      -run 'Test.*(Parquet|LoadExternalStats|ApplyLoadAssignmentCasts)' -count=1

The combined candidate has not yet been validated by a new full COS/TKE run.
