# Statement authorization admission: matched TCP measurements

Measured on 2026-10-17. The statement-owned design shares one current snapshot and one catalog proof across role/grant validation, and transfers the completed admission to the first data transaction. No stale empty-cache shortcut, TTL, user isolation change or deadline increase is used.

## Source and protocol

Baseline: official main `aa4b6eddfdad54d11be6fd75a5675eb9801f6690`. The candidate was frozen from design commit `23c27718e085cdc9aec20e54b745a98842f9a67f` plus the source overlay whose per-file SHA-256 hashes are in the raw provenance. It includes the exclusive-frontier correction: a snapshot A is ready through A.Prev(); rechecking A itself would unnecessarily wait for a new logtail notification. The deliberately interrupted pre-correction candidate run is excluded.

Both plain source exports use the identical harness (SHA-256 `a1a41754ab3adda0a39464b43e64667e289411f8044d49b2dde5b9bf72c53280`) and identical count-only instrumentation. The TxnClient interface and production metrics are unchanged; the count-only observer is not shipped in production. Native inputs are identical, read-only Darwin/arm64 libraries. Runner: Apple M4, 16 GiB, 10 logical CPUs; Go 1.27.1, CGO=1, GOMAXPROCS=10, matrixone_test, no race instrumentation.

One authenticated sequential TCP client, one CN, two rows. A present mandatory rule filters the second row for reader and accountadmin roles. Every request consumes and verifies the complete result. Separate sessions cover text and a retained binary prepared statement. Configurations are default (sacrificing freshness=1, unlimited), preserve (-1, unlimited), and limited (1, 100 transaction admissions/s).

Every cell separates connection/PREPARE/first execution, runs one five-second warmup, then three five-second measured rounds. There are 24 cells per revision and 240 records total: 48 first-use, 48 warmup and 144 measured. All completed runs end in PASS with no SQL/protocol/filter errors. One base/default log was recovered with its full terminal PASS and all 40 samples after a driver interruption; its OS exit status was not retained. All other process exits are recorded as zero. Only one cluster ran at a time, without simultaneous builds or tests. Order: default base/candidate, preserve candidate/base, limited base/candidate.

## Warm measurements

Each value is the median of three measured rounds; warmup and first-use are excluded. Paired values are base → candidate. Latencies are milliseconds. Admission attempts/successes are observed at the real limiter call sites and include background transactions; the table reports attempts per request. All measured SQL requests succeeded. Process-wide attempt/success deltas can differ because background admissions cross sampling boundaries. Snapshot observations use the existing process-wide determination histogram. They are observations per request, not isolated proof counts.

| Mode | Rules | Principal | Protocol | Requests/s (base → candidate) | Throughput change | p50 ms | p95 ms | Admissions/request | Snapshots/request |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| default | absent | reader | text | 8241.78 → 8083.25 | -1.9% | 0.119 → 0.122 | 0.135 → 0.138 | 2.003 → 1.003 | 2.003 → 1.003 |
| default | absent | reader | binary | 9764.90 → 9573.63 | -2.0% | 0.100 → 0.104 | 0.119 → 0.115 | 2.002 → 1.002 | 2.002 → 1.002 |
| default | absent | admin | text | 8342.13 → 8137.39 | -2.5% | 0.111 → 0.122 | 0.154 → 0.135 | 2.003 → 1.004 | 2.003 → 1.004 |
| default | absent | admin | binary | 9945.88 → 9717.01 | -2.3% | 0.093 → 0.103 | 0.120 → 0.115 | 2.002 → 1.003 | 2.002 → 1.003 |
| default | present | reader | text | 7272.72 → 7406.33 | +1.8% | 0.130 → 0.134 | 0.162 → 0.150 | 2.003 → 1.003 | 2.003 → 1.003 |
| default | present | reader | binary | 9719.46 → 9773.49 | +0.6% | 0.098 → 0.103 | 0.124 → 0.115 | 2.002 → 1.003 | 2.002 → 1.003 |
| default | present | admin | text | 7481.93 → 7380.09 | -1.4% | 0.131 → 0.133 | 0.147 → 0.151 | 2.003 → 1.003 | 2.003 → 1.003 |
| default | present | admin | binary | 10018.38 → 9698.59 | -3.2% | 0.101 → 0.103 | 0.109 → 0.115 | 2.002 → 1.003 | 2.002 → 1.003 |
| preserve | absent | reader | text | 143.63 → 300.63 | +109.3% | 7.629 → 3.851 | 8.372 → 4.206 | 2.155 → 1.084 | 2.155 → 1.084 |
| preserve | absent | reader | binary | 143.20 → 301.94 | +110.9% | 7.665 → 3.849 | 8.369 → 4.168 | 2.159 → 1.084 | 2.159 → 1.084 |
| preserve | absent | admin | text | 141.96 → 303.09 | +113.5% | 7.712 → 3.851 | 8.369 → 4.158 | 2.161 → 1.083 | 2.161 → 1.083 |
| preserve | absent | admin | binary | 142.87 → 300.09 | +110.0% | 7.671 → 3.858 | 8.285 → 4.175 | 2.161 → 1.084 | 2.161 → 1.084 |
| preserve | present | reader | text | 147.71 → 302.60 | +104.9% | 6.431 → 3.852 | 8.246 → 4.160 | 2.157 → 1.084 | 2.157 → 1.084 |
| preserve | present | reader | binary | 148.63 → 300.19 | +102.0% | 6.330 → 3.852 | 8.146 → 4.166 | 2.149 → 1.080 | 2.149 → 1.080 |
| preserve | present | admin | text | 142.17 → 298.26 | +109.8% | 7.702 → 3.852 | 8.343 → 4.179 | 2.155 → 1.081 | 2.155 → 1.081 |
| preserve | present | admin | binary | 142.62 → 296.68 | +108.0% | 7.685 → 3.762 | 8.386 → 4.358 | 2.152 → 1.072 | 2.152 → 1.072 |
| limited | absent | reader | text | 41.92 → 83.04 | +98.1% | 20.086 → 10.078 | 40.136 → 20.296 | 2.381 → 1.202 | 2.386 → 1.202 |
| limited | absent | reader | binary | 40.91 → 82.40 | +101.4% | 20.088 → 10.068 | 40.229 → 20.232 | 2.444 → 1.213 | 2.444 → 1.214 |
| limited | absent | admin | text | 42.11 → 79.05 | +87.7% | 20.092 → 10.043 | 40.163 → 20.535 | 2.374 → 1.255 | 2.374 → 1.263 |
| limited | absent | admin | binary | 41.05 → 80.60 | +96.4% | 20.083 → 10.044 | 40.188 → 20.197 | 2.437 → 1.242 | 2.437 → 1.240 |
| limited | present | reader | text | 41.71 → 82.05 | +96.7% | 20.082 → 10.023 | 40.230 → 20.232 | 2.392 → 1.219 | 2.397 → 1.217 |
| limited | present | reader | binary | 41.32 → 79.86 | +93.3% | 20.069 → 10.035 | 40.225 → 20.252 | 2.425 → 1.255 | 2.420 → 1.252 |
| limited | present | admin | text | 40.75 → 78.66 | +93.0% | 20.082 → 10.037 | 40.233 → 20.188 | 2.459 → 1.272 | 2.454 → 1.272 |
| limited | present | admin | binary | 40.17 → 79.65 | +98.3% | 20.121 → 10.042 | 40.179 → 20.244 | 2.490 → 1.256 | 2.490 → 1.256 |

The measured throughput changes across the eight cells in each mode are: default -3.2% to +1.8%, preserve +102.0% to +113.5%, limited +87.7% to +101.4%. The full per-round values are retained for inspecting variability; these single-client measurements do not establish production mixed-load capacity or a latency SLA.

Default settings retain a small measured warm CPU cost, rather than zero regression: the rules-present admin binary case is consistently slower in all three candidate rounds (9670–9778 requests/s versus 9977–10058), median throughput -3.2%, p50 +1.83 microseconds and p95 +5.42 microseconds. These observations must not all be attributed to noise. The explicit table and raw rounds retain the other small default movements; no production capacity or tail-distribution guarantee follows from this single-client matrix.

## First use

One first-use observation per cell, within an already-started cluster; this is not a machine-cold or cluster-start benchmark. Connection, PREPARE and first-execution admissions/timings are all in the raw records. Cold cache fills perform real authorization transactions and are not waived. Paired latency values below are base → candidate.

| Mode | Rules | Principal | Protocol | Connect ms | PREPARE ms | First execution ms | First execution admissions |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| default | absent | reader | text | 1.957 → 2.156 | 0.000 → 0.000 | 1.993 → 2.162 | 5 → 3 |
| default | absent | reader | binary | 4.202 → 1.979 | 2.189 → 1.961 | 0.151 → 0.192 | 2 → 1 |
| default | absent | admin | text | 2.093 → 2.066 | 0.000 → 0.000 | 0.729 → 0.700 | 5 → 3 |
| default | absent | admin | binary | 2.102 → 2.024 | 0.874 → 0.894 | 0.136 → 0.161 | 2 → 1 |
| default | present | reader | text | 3.961 → 2.067 | 0.000 → 0.000 | 9.198 → 2.315 | 5 → 3 |
| default | present | reader | binary | 2.132 → 1.955 | 2.271 → 2.158 | 0.188 → 0.170 | 2 → 1 |
| default | present | admin | text | 2.026 → 3.169 | 0.000 → 0.000 | 0.883 → 0.914 | 5 → 3 |
| default | present | admin | binary | 2.024 → 2.115 | 0.765 → 1.004 | 0.127 → 0.170 | 2 → 1 |
| preserve | absent | reader | text | 7.568 → 4.159 | 0.000 → 0.000 | 21.945 → 3.780 | 5 → 3 |
| preserve | absent | reader | binary | 11.910 → 8.092 | 22.095 → 8.705 | 3.646 → 3.410 | 2 → 1 |
| preserve | absent | admin | text | 12.037 → 9.891 | 0.000 → 0.000 | 21.096 → 4.597 | 5 → 3 |
| preserve | absent | admin | binary | 11.648 → 11.156 | 14.570 → 3.094 | 7.496 → 3.012 | 2 → 1 |
| preserve | present | reader | text | 11.331 → 7.496 | 0.000 → 0.000 | 20.798 → 4.668 | 5 → 3 |
| preserve | present | reader | binary | 6.380 → 5.747 | 17.064 → 7.118 | 4.780 → 0.822 | 2 → 1 |
| preserve | present | admin | text | 9.933 → 11.351 | 0.000 → 0.000 | 16.938 → 3.515 | 5 → 3 |
| preserve | present | admin | binary | 7.967 → 11.434 | 20.323 → 5.654 | 5.607 → 1.998 | 2 → 1 |
| limited | absent | reader | text | 12.752 → 13.678 | 0.000 → 0.000 | 53.117 → 32.046 | 5 → 3 |
| limited | absent | reader | binary | 40.052 → 19.402 | 104.433 → 36.141 | 35.910 → 4.732 | 4 → 1 |
| limited | absent | admin | text | 27.136 → 59.902 | 0.000 → 0.000 | 61.151 → 59.810 | 6 → 5 |
| limited | absent | admin | binary | 19.228 → 19.968 | 80.113 → 30.145 | 39.142 → 9.838 | 4 → 1 |
| limited | present | reader | text | 11.803 → 13.210 | 0.000 → 0.000 | 54.283 → 43.275 | 5 → 4 |
| limited | present | reader | binary | 19.962 → 20.007 | 96.116 → 33.718 | 33.527 → 5.994 | 4 → 1 |
| limited | present | admin | text | 30.565 → 36.459 | 0.000 → 0.000 | 50.820 → 30.072 | 5 → 3 |
| limited | present | admin | binary | 20.001 → 19.169 | 50.632 → 30.227 | 19.386 → 9.824 | 2 → 1 |

## Correctness and reuse limits

Race validation retains two-CN ADD/replacement/DROP, current authorization with older RR data, fail-closed prepared handles and all six resource-cleanup cases. A new two-CN barrier orders a remote combined rule/grant commit between the current statement’s role and grant reads: both stay at A, and the next statement sees B. Principal changes, SQL batches, real implicit TRUNCATE commit, transactional prepared CREATE, nested authorization and cancellation/queue cleanup remain covered. Token/frontier tests assert actual admission counts, exact A even when a waiter returns later B, and creation without any subsequent notification.

The exact shared-deadline pagination case also passes with matching race/tags/CGO locally: original head 12.36s, clean main 11.69s, candidate 12.05s before the frontier correction; the final corrected candidate passes in the combined four-case run (individual test 3.17s, total 27.88s). The combined process/fixture context differs, so its per-case time is not a matched performance comparison. The old Linux/x64 184.69s shared-context timeout remains unattributed; no flaky/pre-existing claim or deadline change is made.

The design-only commit is retained after rebase as `02c72d4fc4c19301d1c2222607ed117a23c7019b`; its approved revision-2 blob is unchanged. Delivery is rebased onto official main `a322466757b9b0165c59fbda565608653d923b2a`. Relative to the measured baseline, main adds CSV LOAD parsing/testing hooks, CloneBytes boundary tests without a production change, and a nil-primary-key guard for combined ALTER DROP COLUMN. The benchmark uses ordinary table SELECT; the selected DDL controls use TRUNCATE/CREATE, not combined ALTER column drops. Authorization, transaction/native inputs and the harness are unchanged, so measured evidence is reused with its actual revisions retained.

## Reproduce

Use two plain git-archive exports of the exact baseline and candidate; copy the same current benchmark file into both. Apply [instrument-snapshot-admissions.py](instrument-snapshot-admissions.py) to each export. It refuses a Git worktree and replaces exactly the two limiter call sites with identical attempts/success counters. Build both binaries before measurement with Go 1.27.1, CGO=1, GOWORK=off, matrixone_test and the same matching native include/library/rpath flags. The raw provenance retains the exact build arguments, source and instrumentation hashes, binary hashes, per-run arguments, environment, timestamps, terminal results and log SHA-256 digests.

For each frozen binary, serially run the following once for each mode (default, preserve, limited), using the same runner and native runtime paths:

```sh
MO_ROLE_FRESHNESS_BENCH_CONFIG=default GOMAXPROCS=10 \
  "$MO_PERF_BINARY" -test.run='^$' \
  -test.bench='^BenchmarkRoleRuleFreshnessTCP$' -test.benchtime=1x \
  -test.timeout=10m -test.v
```

Do not run other benchmarks, tests or builds concurrently. Extract ROLE_FRESHNESS JSON records; preserve every first-use, warmup and measured round. The [raw samples and provenance](statement-authorization-admission-results.jsonl) contain the complete current matrix; the [earlier request-only report](role-rule-freshness.md) remains historical evidence of the previous regression.
