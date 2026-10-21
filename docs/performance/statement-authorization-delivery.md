# Statement authorization incremental repair: complete matched matrix

The default performance requirement remains open. Publication of the verified parser and authorization hot-path repairs does not accept a remaining regression; completion of this protocol under shared-runner load is not a clean performance acceptance pass.

## Inputs and method

Baseline: official main `b111224b0c624773389b390cd3cf1223126b6ee2`. Candidate runtime: `3f2326ea6f8b093a5316d72cd7279f6eba6e72d3`. Identical harness SHA-256 `9156ddb83c79b4502cc36b6a29b351a3c9a627bdeb5b2f568db8966361586217`. Exact archive/blob checks, source and native manifests, commands, binary hashes, exits and all 320 samples are in [raw evidence](statement-authorization-delivery-results.jsonl).

Darwin/arm64 Apple M4 16 GiB; Go1.27.1 CGO1 matrixone_test normal GOMAXPROCS10; single client; same read-only native inputs; no task-owned concurrent build/test/cluster; external compiler activity is retained per run and limits performance inference.

Toolchain process observations in the raw run records show concurrent work from other tasks on this shared runner. This limits throughput/latency attribution. It is not evidence that the residual regression is noise and cannot by itself establish a clean performance acceptance pass. No external process was stopped or modified.

The candidate freshness-preserving process waited 4m51.416649875s for the runner-wide embedded-cluster admission lock, held by another active sqlintegration process. No service or measured request had started during that wait. It resumed normally under the original binary/configuration/deadline after lock release; the setup events are retained in the raw run record. This startup wait is excluded from all measured rounds.

default candidate/base, preserve base/candidate, limited candidate/base; each cell first-use + 1x5s warmup + 3x5s measured. First-use and warmup are excluded from warm medians. Every request consumes and validates the complete result `[1,2]` without rules or `[1]` with rules. The measured default text variants include a trailing delimiter and a literal semicolon. Admission/snapshot counts are process-wide observations, including background work; deterministic owner tests provide exact per-statement reuse proof. This single-client experiment does not establish mixed-load capacity.

## Runner admission waits

Exclusive cluster admission is preserved across all processes. Setup waits are excluded from the sample phases; no timeout or pool/lock namespace was changed.

| Mode | Revision | Admission wait |
| --- | --- | --- |
| default | candidate | 1.692736083s |
| default | base | 9.5613295s |
| preserve | base | 5.340225417s |
| preserve | candidate | 4m51.416649875s |
| limited | candidate | 4.227463959s |
| limited | base | 3m8.063712208s |

## Warm results

Rules/admin flags are Boolean. TP is median requests/s over the three measured rounds; p95 is the median per-round p95 in microseconds. Admission figures are median attempts per request. All failed and unfavorable results remain.

| Mode | Rules | Admin | Protocol | Base TP | Candidate TP | Change | Base p95 us | Candidate p95 us | Base/Candidate admissions |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| default | False | False | binary | 9588.48 | 9106.71 | -5.02% | 113.46 | 120.67 | 2.002/1.003 |
| default | False | False | text | 7995.21 | 7723.11 | -3.40% | 137.50 | 144.54 | 2.003/1.003 |
| default | False | False | text-delimiter | 8022.65 | 7853.87 | -2.10% | 136.38 | 142.17 | 2.003/1.003 |
| default | False | False | text-literal | 2181.57 | 7113.73 | +226.08% | 1002.29 | 175.38 | 2.010/1.003 |
| default | False | True | binary | 6548.79 | 8819.17 | +34.67% | 345.88 | 131.75 | 2.003/1.003 |
| default | False | True | text | 4491.93 | 7813.77 | +73.95% | 596.58 | 143.42 | 2.005/1.003 |
| default | False | True | text-delimiter | 4072.56 | 7584.11 | +86.22% | 645.50 | 146.75 | 2.005/1.003 |
| default | False | True | text-literal | 3646.23 | 7069.46 | +93.88% | 670.67 | 159.50 | 2.006/1.004 |
| default | True | False | binary | 8860.22 | 2844.52 | -67.90% | 129.92 | 800.46 | 2.003/1.008 |
| default | True | False | text | 4565.90 | 6725.65 | +47.30% | 531.75 | 180.62 | 2.005/1.004 |
| default | True | False | text-delimiter | 6474.73 | 2751.84 | -57.50% | 196.12 | 915.04 | 2.003/1.009 |
| default | True | False | text-literal | 6146.26 | 1444.11 | -76.50% | 164.38 | 1343.29 | 2.004/1.017 |
| default | True | True | binary | 9488.77 | 3790.03 | -60.06% | 113.62 | 616.58 | 2.002/1.006 |
| default | True | True | text | 6603.96 | 2906.88 | -55.98% | 178.92 | 981.50 | 2.003/1.009 |
| default | True | True | text-delimiter | 4699.54 | 2355.55 | -49.88% | 554.75 | 1124.79 | 2.005/1.009 |
| default | True | True | text-literal | 5609.85 | 6812.93 | +21.45% | 360.54 | 163.58 | 2.004/1.004 |
| limited | False | False | binary | 41.07 | 82.45 | +100.73% | 40251.21 | 20343.21 | 2.440/1.213 |
| limited | False | False | text | 40.00 | 79.09 | +97.74% | 40484.88 | 20656.62 | 2.495/1.267 |
| limited | False | True | binary | 42.05 | 78.66 | +87.08% | 40204.21 | 20411.17 | 2.379/1.272 |
| limited | False | True | text | 41.25 | 80.48 | +95.10% | 40227.88 | 20148.17 | 2.420/1.245 |
| limited | True | False | binary | 42.23 | 81.86 | +93.85% | 40249.71 | 20273.04 | 2.368/1.220 |
| limited | True | False | text | 40.35 | 80.60 | +99.76% | 40511.67 | 20409.21 | 2.475/1.238 |
| limited | True | True | binary | 40.32 | 82.44 | +104.47% | 40303.88 | 20431.17 | 2.480/1.213 |
| limited | True | True | text | 40.84 | 80.66 | +97.52% | 40556.54 | 20890.92 | 2.449/1.238 |
| preserve | False | False | binary | 136.04 | 266.32 | +95.76% | 8432.00 | 5127.46 | 2.160/1.088 |
| preserve | False | False | text | 135.38 | 267.44 | +97.54% | 8439.50 | 5024.08 | 2.163/1.093 |
| preserve | False | True | binary | 135.60 | 271.58 | +100.29% | 8586.08 | 4930.29 | 2.162/1.091 |
| preserve | False | True | text | 134.51 | 263.94 | +96.23% | 8613.25 | 5492.83 | 2.151/1.091 |
| preserve | True | False | binary | 135.76 | 262.89 | +93.65% | 8662.29 | 5079.21 | 2.160/1.094 |
| preserve | True | False | text | 135.75 | 259.90 | +91.46% | 8384.50 | 5363.04 | 2.171/1.097 |
| preserve | True | True | binary | 135.90 | 266.46 | +96.08% | 8349.79 | 5173.00 | 2.160/1.082 |
| preserve | True | True | text | 134.39 | 251.79 | +87.36% | 8487.17 | 5573.67 | 2.145/1.094 |

## First use, kept separate

One observation per revision/cell; it is not a cold-start distribution. Raw records also retain connect/PREPARE latency and admission counts.

| Mode | Rules | Admin | Protocol | Base first us | Candidate first us | Base/Candidate first admissions |
| --- | --- | --- | --- | ---: | ---: | ---: |
| default | False | False | binary | 159.00 | 1009.67 | 2/1 |
| default | False | False | text | 2240.71 | 2616.12 | 5/3 |
| default | False | False | text-delimiter | 2043.08 | 2575.12 | 5/3 |
| default | False | False | text-literal | 2042.88 | 2161.21 | 5/3 |
| default | False | True | binary | 819.62 | 148.21 | 2/1 |
| default | False | True | text | 846.12 | 826.38 | 5/3 |
| default | False | True | text-delimiter | 985.38 | 696.00 | 5/3 |
| default | False | True | text-literal | 805.29 | 959.92 | 5/3 |
| default | True | False | binary | 175.50 | 167.04 | 2/1 |
| default | True | False | text | 5206.46 | 4140.12 | 5/3 |
| default | True | False | text-delimiter | 3669.83 | 12772.88 | 5/3 |
| default | True | False | text-literal | 2982.33 | 6227.17 | 5/3 |
| default | True | True | binary | 185.08 | 774.33 | 2/1 |
| default | True | True | text | 1531.96 | 8842.38 | 5/3 |
| default | True | True | text-delimiter | 1015.42 | 4634.79 | 5/3 |
| default | True | True | text-literal | 836.46 | 5499.29 | 5/3 |
| limited | False | False | binary | 17788.96 | 1207.67 | 2/1 |
| limited | False | False | text | 51495.21 | 33323.04 | 5/3 |
| limited | False | True | binary | 19564.67 | 19687.33 | 2/2 |
| limited | False | True | text | 82162.25 | 51580.38 | 7/6 |
| limited | True | False | binary | 12861.04 | 1360.00 | 2/1 |
| limited | True | False | text | 125938.79 | 36230.38 | 11/3 |
| limited | True | True | binary | 18317.62 | 9220.79 | 2/1 |
| limited | True | True | text | 101187.71 | 29933.29 | 11/3 |
| preserve | False | False | binary | 7463.00 | 5197.58 | 2/1 |
| preserve | False | False | text | 20253.75 | 6920.54 | 5/3 |
| preserve | False | True | binary | 7317.92 | 3565.50 | 4/1 |
| preserve | False | True | text | 18651.12 | 6123.62 | 5/3 |
| preserve | True | False | binary | 7127.21 | 3110.12 | 2/1 |
| preserve | True | False | text | 28408.71 | 14801.17 | 5/3 |
| preserve | True | True | binary | 6592.08 | 4159.50 | 2/1 |
| preserve | True | True | text | 21503.25 | 88942.12 | 5/3 |

## Functional and delivery scope

All nineteen branch patches survive the rebase unchanged onto the stated baseline. Refreshed owning frontend, MySQL parser, transaction client and disttae race/vet checks pass, as do the four real cross-CN/prepared lifecycle consumer cases and incremental lint. The historical [hot-path pilots](statement-authorization-hot-path.md) retain previous failures and cost diagnostics; their different measured revisions are not combined with this matrix.

The scanner preflight keeps boundary discovery under the existing MySQL lexer and avoids constructing a discarded AST for a single COM_QUERY with a trailing/literal/comment semicolon. Eager ordinary-batch rejection and SQL-mode-changing batch continuations remain covered. Authorization proof freshness, principal/generation checks, A/A.Prev boundaries, cancellation and prepared resource cleanup remain mandatory.

The report provides complete replacement measurement evidence, including the omitted single-statement text paths. Any remaining negative default results require further work before performance acceptance; no reviewer request, exemption or merge approval is implied. New remote CI is pending publication and has not been claimed to pass.
