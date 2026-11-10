# Tuple COUNT DISTINCT validation evidence

This bundle closes the retained-evidence request in [PR #30998 review](https://github.com/matrixorigin/matrixone/pull/30998#pullrequestreview-5633797167). These are fresh executions after rebase, not the earlier d32 benchmark or predictions from plan tests.

## Source and build identity

The clean baseline is `94faf0209b9ac3254d31906f92e34405f1988203`; the tested candidate is `3d1b86fcaee0b83eb10c50b2b5cee141ab3f0396`. See source-identity.json for the pkg/cmd trees and changed Go file hashes. The later evidence-only commit changes none of these executable/test inputs.

build-receipts.json records commands, clean checkout checks, timestamps, exit status and SHA256 for both native service binaries. baseline-buildinfo.txt and candidate-buildinfo.txt retain go version -m output. Both builds used the repository prebuilt-native target; native-provenance.txt and unchanged Makefile/CGo/thirdparty/module inputs establish reuse eligibility. Binaries are not checked in.

Platform: darwin/arm64, Go 1.27.1, GOEXPERIMENT=simd. Public correctness uses GOMAXPROCS=4; controlled timing uses 8. Both variants use GOMEMLIMIT=3GiB, 4 GiB statement memory/spill limits, the same on-disk data and identical configurations. These are local measurements; CI is separate.

## Acceptance matrix

| Requirement | Execution evidence |
| --- | --- |
| Correctness at DOP 1/8, grouped/global, tuple spelling, reversed arguments | public.log.txt, public-results.json and public_qa.py.txt; expected counts include 333337 |
| Distinct tuples differing only in a nonselected component | axis_qa.py.txt, AXIS_QA_PASS and representative physical plans; exact 666674 |
| CHAR/PAD SPACE and Unicode | public/string scripts and plans; mixed-aggregate exact fallback is an independent result oracle |
| NULL, all-invalid, empty and outer-join controls | public-results.json, outer_join_qa.py.txt, public.log.txt and invalid/outer-join physical plans; both outer-join orders return 333337 |
| Actual two-CN topology and exact results | remote-results.json and both coordinator plans include 54200 and 54500; 5M rows reduce to 2.5M tuples; grouped counts 625000 each |
| Prepared reuse | remote-results.json has two executions from each coordinator; all return 2500000 |
| Actual forced spill | public and both remote EXPLAIN ANALYZE retain nonzero SpillRows; scripts assert this, not merely an 8 MiB setting |
| Cancellation, survivor and cleanup | cancel-results.json, cancel-cleanup.json and script: both marked queries active while new spill files exist; KILL returns 1317 within 20 s; survivor returns 5M, canceled connection reused, zero new spill files remain |
| Controlled performance and fallback controls | perf-results.json raw samples, perf-summary.json, perf.log.txt and physical/analyze plans for every case; every measured invocation checks exact output |
| UT/race/static | test-receipts.json, test-selection.json, raw logs and sca-results.json; source/command/exit status retained |

public-receipt.json and perf-receipt.json bind the terminal log, harness hashes, binary hashes and source tree to the actual command and exit status. The public receipt snapshots all then-present diagnostic scripts, including scripts it did not execute. public-snapshot-* preserves the public run’s executed harness/wrapper/configs and its then-unused performance script. The final performance receipt lists only its executed script/import closure. Public execution used the earlier shared local fixture; final performance uses the private dataset below. The source/binaries are unchanged. Raw engine plans and executed-source snapshots retain their original trailing whitespace; these archival bytes are not formatter-normalized. Production/test source passes the normal whitespace check; the archive is validated by exact checksums. All files are covered by SHA256SUMS (except the checksum file itself). No full service logs, binaries, arbitrary local database files or external-service credentials are included. The scripts/configs retain only the harmless local fixture root password 111.

## Timing protocol and results

One sequential ABBA run: baseline, candidate, candidate, baseline. Each phase warms every case five times, then takes ten interleaved samples/case. Thus each case has 20 measured warm samples/version. All eight controls are included: tuple, reversed tuple, duplicates, CHAR, unary, mixed aggregates, low NDV, all-invalid. max_dop=8, agg_spill_mem=0; no compiler, linter or other task-owned service runs during timing. See perf-host.txt for host facts.

| Case | Baseline median ms | Candidate median ms | Change |
| --- | ---: | ---: | ---: |
| unary | 22.433 | 23.568 | +5.06% |
| tuple | 458.803 | 36.698 | -92.00% |
| reverse | 439.405 | 36.166 | -91.77% |
| mixed | 356.310 | 358.283 | +0.55% |
| duplicates | 498.223 | 29.077 | -94.16% |
| char | 369.703 | 42.924 | -88.39% |
| invalid | 3.270 | 2.891 | -11.59% |
| lowndv | 36.704 | 36.506 | -0.54% |

## Reproduction and artifact boundaries

The .py.txt files are the exact diagnostic sources used by the receipts; the suffix keeps this archive out of automatic test execution. To rerun, copy them to a separate directory with .py names, set harness.py work/runtime/root to private local paths, and retain the supplied CN/TN/log configs with those paths adjusted. Use a native build matching the checked-out source and a Python environment with pymysql. The configs use loopback ports and isolated local disk services; do not point them at a production cluster.

fixture-schema.json retains the actual SHOW CREATE TABLE output. The seed table contains k=0..4999999, g8=k%8, with the other columns shown in that schema. The seed can be generated using fixture-bootstrap.sql.txt and generate_series(0,4999999); the public scripts show the exact derived-table inserts and full-statistics refresh. For the final performance campaign, bootstrap.py created the private dataset 30998-isolated-94fa once before any timing. fixture-load.json, fixture-stats.json and bootstrap-receipt.json retain its actual creation/count/statistics audit. It is reused without repopulation across phases; this is a warm query benchmark, not ingestion/restart timing. Both binary variants read the identical persisted files. For an empty store: install the archived cn.toml.txt as runtime/cn-4g.toml (the filename read by harness.py), plus runtime/log.toml and runtime/tn.toml; adjust all paths and launch.toml together. Start the candidate manually with -launch launch.toml, connect to SQL 55401 without a default database, run fixture-bootstrap.sql.txt to create q33_bench and the seed database/table, then stop that owned service. Only then run the recorded harness, whose readiness connection requires q33_bench. Derived fixtures live in qa30995 and are created by the public scripts. Physical block layout/cache state can change absolute timings on another machine.

The first performance attempt ended before any sample because the earlier shared temporary store no longer contained q33_bench. perf-startup-failed-receipt.json/log/excerpt retain that failure; the cause of store removal is unproven. None of its observations enter performance samples. To prevent further interference, the final campaign uses a new task-owned isolated-runtime directory with the same budgets/ports, a separately recorded dataset and freshly populated tables. No product code was changed for this environment failure.

Run build_both.py (clean baseline then named candidate, sequential checkout), run_checks.py, derive_sca.py/run_sca.py, then run_experiment.py public. Then create the private performance fixture with bootstrap.py (or follow the manual bootstrap recipe above). Only after all builds/checks/bootstrap exit, run run_experiment.py perf. The initial public proof remains a separate execution on its recorded dataset; do not relabel it as having run on the new benchmark store. The receipts retain the exact original argv/cwd; local directory names are historical and should be adjusted together. static tools are the pinned versions in tool-versions.txt. The final public campaign runs the remote stage last: no query is sent after peer shutdown; this avoids unrelated topology-withdrawal issue #30993.

This bundle supports the bounded non-NULL tuple optimization. Nullable/unproven tuples retain the original executor; it does not claim acceleration of general mixed aggregates or all data distributions. It is retained command/result evidence, not a new production benchmark framework or a claim that CI passed.
