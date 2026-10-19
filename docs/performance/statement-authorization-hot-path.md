# Statement authorization hot-path repair: validation still open

Measured on 2026-10-17. **The default-throughput acceptance gate is not met.** Source and race checks passing do not close review 5557316266 / thread 4302289253. No owner waiver, noise classification or successful full matrix is claimed.

## Actual inputs and method

Baseline is official main `ba6b31941074cdaaf906ba2ec3883bc3b3eab85f`. The latest measured candidate runtime is `58218e449873e5f5c9c8a603e9d80edef98e4571`. Both binaries use the same benchmark SHA-256 `9156ddb83c79b4502cc36b6a29b351a3c9a627bdeb5b2f568db8966361586217`, count-only admission instrumentation and read-only native inputs. Plain git-archive exports were verified against every tracked blob before instrumentation; source manifests, exact build commands and binary hashes are in the [raw evidence](statement-authorization-hot-path-results.jsonl).

Darwin/arm64, Apple M4, 16 GiB, Go 1.27.1, CGO=1, GOMAXPROCS=10, matrixone_test, normal mode. No own build, test or second cluster ran concurrently with a throughput process. The runner is a shared desktop; process observations are retained locally. A same-binary baseline control did not explain away the candidate regression.

The selected cell is default freshness, unlimited admission, mandatory rules present, accountadmin, one authenticated TCP client and a retained binary prepared SELECT. Every request consumes and checks the complete result `[1]`. Each process records first use, one five-second warmup and three five-second measured rounds. Warmup and first use are excluded below. All processes exit zero, with no SQL/protocol/result errors.

## Unsuccessful pilots

Each row identifies its actual candidate input. These are bounded diagnostic pilots of the previously worst cell, not the full acceptance matrix. All failed pilots are retained.

| Pilot | Candidate | Base median requests/s | Candidate median requests/s | Change |
| --- | --- | ---: | ---: | ---: |
| 1 | `97bd2cac8727` | 9934.06 | 9556.43 | -3.80% |
| 2 | `b9a5e165381e` | 9828.86 | 9646.59 | -1.85% |
| 3 | `e9c5a11818dd` | 9792.30 | 9642.28 | -1.53% |
| 4 | `29f356dc784e` | 9755.29 | 9656.99 | -1.01% |
| 5 | `4221bcdb7231` | 9997.28 | 9757.94 | -2.39% |
| 6 | `58218e449873` | 9851.86 | 9705.80 | -1.48% |

The sixth pilot retains a p95 increase of approximately 1.3–1.5 microseconds. Reclassifying this cost as small or accepted would not satisfy the feedback.

## Predeclared independent-process controls

After a counter-instrumented diagnostic changed observed throughput, three additional independent process pairs were fixed in advance, alternating order. Each process still used one warmup and three measured rounds. All results, including the slow baseline round, are retained; none is selected as the passing result.

| Pair / order | Base measured requests/s | Candidate measured requests/s | Paired median change |
| --- | --- | --- | ---: |
| 1 / base/candidate | 9846.54, 9853.87, 9909.97 | 9790.38, 9823.43, 9791.43 | -0.63% |
| 2 / candidate/base | 9797.40, 9768.60, 9759.01 | 9765.06, 9687.75, 9705.04 | -0.65% |
| 3 / base/candidate | 9858.59, 9801.77, 8992.39 | 9682.96, 9676.82, 9694.18 | -1.21% |

Across all nine measured rounds per revision, medians are 9801.77 → 9705.04 requests/s (-0.99%). All three paired medians remain negative. This does not demonstrate resolution. The independent baseline control after pilot five returned 9998.52 / 10010.95 / 9988.33 requests/s; background activity alone is not proof that the regression is noise.

## Source and functional closure

Revision 3 of the [design](../design/statement-authorization-admission.md) records the changes and unchanged authority boundaries. The MySQL scanner avoids the discarded single-statement AST. Epoch context/value storage, waiter notification, prepared policy materialization, cancellation adapters, idle-clock bookkeeping, repeated terminal cleanup and prepared lookup locking avoid redundant work. Eight-table proof sampling, principal/generation checks, exact A, A.Prev(), cold read fallback and transaction failure owners remain.

Current-source evidence (race, vet=all, short, matrixone_test, CGO=1):

- Full frontend: 28.339s; full disttae: 14.518s; full transaction client: 10.727s; full MySQL parser: 2.154s. The latter three retain unchanged relevant source from their recorded test runs.
- Four real consumer cases pass together in 22.767s: cross-CN statement epoch, remote rule changes, prepared owner lifecycle and prepared row-rule protocol lifecycle. The epoch fixture also proves a first-statement implicit TRUNCATE commit followed by a same-packet INSERT uses a live sibling context.
- New End completion and refresh-End controls pass 100 race repetitions in 2.778s. Earlier caller-cancellation, shared lifetime and actual eight-table clock controls also pass 100 repetitions.
- Incremental configured lint reports zero issues. Full policy capture still clones immutable maps; the prepared refresh micro measures 36.7–38.0ns, zero bytes and zero allocations.
- Parser/consumer controls cover trailing delimiters, literal/comment semicolons, executable comments, SQL mode, ordinary eager batch errors and real batch continuation. Scanner preflight reduces the measured helper to about 600ns and one allocation, from the historical discarded-AST costs of 1323–2059ns and 18–31 allocations. These helper figures are not TCP throughput.

## Diagnostic limits and remaining work

A separate archive adds phase counters only for diagnosis. Measured phase deltas show approximately one acquisition and one reuse per request, three prepared stamp hits, zero legacy fallback, zero zero-proof/error counts, and rule reload counts 1 / 0 / 0 across the measured phases (whole-process background work included). Repeated cold loading is not established as the cause. These injected counters are not shipped in production or used for acceptance.

An identical temporary synchronous-consumer hook on both revisions acquires the real routine request owner, retains real prepared execution, encoding, proof and transaction lifecycle, and captures only physical protocol output. It validates every binary row and terminal packet, then restores the original connection; smoke tests and subsequent normal TCP requests pass. For 200000 fixed executions, baseline elapsed time is 8.609877666s and candidate 8.905093875s: about 43.05 → 44.53 microseconds/request. Whole-process allocation deltas are about 35108 → 36480 bytes and 395.26 → 405.51 objects per request. These profiled diagnostic results include background activity and are not a capacity comparison. They show the remaining difference also appears without TCP waiting, but do not justify omitting a freshness or cancellation check.

At this historical pilot checkpoint the identical 32-cell harness (the original 24 plus eight default text-delimiter/text-literal cells) had not yet been run in full because the selected default cell still failed. The later [complete delivery matrix](statement-authorization-delivery.md) records actual refreshed baseline/candidate inputs and all first-use, warmup and measured samples, including shared-runner contention and admission waits. It does not establish clean default performance acceptance. A passing replacement default comparison and final overall acceptance review remain required. Historical preserve/limited improvements are not substituted for this missing default evidence. Historical CI run 39017328791 succeeded at remote head `5db8b2eb5edfb3124ee9a0fa009ce2e0e073800e`; it is not current-head CI evidence. No failed-CI repair is claimed.
