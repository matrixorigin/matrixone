# Issue #25782 controlled spill harness

This historical reproduction harness was validated against the locked `main` commit `cd741923c`. It compares a broadcast `HashBuild` with a shuffle `HashBuild` positive control using the same physical tables, join key, and predicate. The physical scale is fixed at 132,096 rows per table (1,024 baseline rows followed by a 131,072-row high-cardinality tail); planner statistics are patched only to select the two plans.

The current worktree contains the complete PR-scoped no-OOM fix: hierarchical HashBuild admission, transactional hashmap resize, bounded Shuffle spill/re-spill, deterministic Broadcast build errors, resource-ledger accounting, and fixed acceptance classification. The reviewed single-PR, multi-commit design is in [`FIX_PLAN.md`](FIX_PLAN.md), and the latest acceptance evidence is in [`HANDOFF.md`](HANDOFF.md).

Do not use a historical phase-level `REPRODUCED` result by itself as proof that the fix passed. Fixed acceptance requires `classification.overall=FIXED_ACCEPTED`, correct SQL results, valid source/binary provenance, and final zero-OOM telemetry. A separate hard-budget run also sets `join_spill_mem` above the dataset, observes query-budget rejection followed by positive spill on both CNs, and returns the exact `132096` result.

## Usage

Build the binary under its reviewed cgroup, then prepare and start a private runtime:

```sh
cd /home/mo/worktrees/mo-25782-main
optools/repro/issue25782/build.sh
optools/repro/issue25782/prepare.sh --runtime /absolute/private/runtime
optools/repro/issue25782/start.sh --runtime /absolute/private/runtime
REPRO_ALLOWED=1 optools/repro/issue25782/run.sh --runtime /absolute/private/runtime
```

`start.sh` refuses a binary whose reported commit does not match the locked source, records its SHA256, and verifies the same hash through every running process.  `run.sh` rechecks those process identities and hashes before any SQL.  It also refuses a non-local proxy (and a non-6001 port unless an explicitly approved override is set).  The default `EXIT` trap calls `stop.sh`; pass `--keep` when the runtime and artifacts must be inspected after the run.  `MO_25782_QUERY_TIMEOUT` controls the per-query timeout (default 120 seconds).  A missing cgroup controller leaves the runtime smoke-only and `source_gate` refuses reproduction mode; this is intentional.

## Safety boundary

The preflight growth gate must leave the reviewed 4 GiB host reserve.  During each query the harness samples cgroup `memory.current`, `memory.peak`, swap, and `memory.events`, together with host `MemAvailable` and memory PSI.  If a CN reaches 90% of its cgroup limit, `memory.events` reports an OOM, swap grows, `MemAvailable` drops below 4 GiB, PSI exceeds the configured limit, or the query times out, the client is cancelled and the phase is `INCONCLUSIVE`.  No test intentionally exhausts memory or relies on a cgroup OOM.

The run records a private, mode-0700 result directory containing:

- `manifest.source`, `manifest.config`, `manifest.growth`, and per-phase manifests (source/binary identity, config, SQL, and return codes);
- `sql.full.sql` plus every SQL phase;
- complete setup, plan, and execution output under `execution/` and plans under `plans/`;
- per-phase safety samples and flags under `monitor/`;
- immutable evidence snapshots, SHA256 provenance, completion markers, `evidence/operators.<phase>.tsv`, and `classification.{broadcast,shuffle,overall}`;
- final stopped-state telemetry proving cleanup, zero OOM/swap growth, and bounded CN memory peaks.

The lifecycle `status.sh`/evidence hook should write one TSV row for every `HashBuild` operator instance:

```text
instance_id<TAB>cn_id<TAB>input_rows<TAB>is_shuffle<TAB>SpillRows<TAB>SpillSize
```

All six fields are required and `input_rows` must be at least the 1,000-row spill threshold.  CN identity, operator input, shuffle state, and spill counters must come from the operator/PHYPLAN ANALYZE evidence; a table's total row count is never a substitute.  If the hook cannot obtain these fields, classification is `INCONCLUSIVE`.

`run.sh` extracts those fields from each executed `HashBuild` in `EXPLAIN
PHYPLAN ANALYZE` (scope `addr`, `CallNum`, `InRows`, `SpillRows`, and
`SpillSize`).  A coordinator reports runtime counters only for its locally
executed operators; remote scopes are plan placeholders.  Therefore each phase
uses two identical executions, explicitly routed through the proxy to CN1 and
CN2.  Evidence IDs retain the attempt number, and production classification
binds attempt 1/2 to the requested CN1/CN2 identities.  This is two coordinator
perspectives over the same query and data, not a claim that one PHYPLAN report
contains both CNs' runtime counters.  The `is_shuffle` value is derived only
from the same phase's machine plan gate and the locked broadcast
(`ret.IsShuffle=false`) / shuffle (`ret.IsShuffle=true`) constructors; any
missing or mismatched attempt identity still produces `INCONCLUSIVE`.

## Plan and SQL gates

`01_setup.sql` creates the bounded tables, refreshes small-sample statistics, appends the high-cardinality tail, flushes objects, and executes the required `set @@join_spill_mem=1000;` / `select @@join_spill_mem;` check.  `02_broadcast_plan.sql` patches the build estimate to 1,024 rows; `03_shuffle_plan.sql` patches both estimates above the main shuffle thresholds. `06_hard_budget_exec.sql` raises the soft threshold to 1 GiB so a metrics-observed rejection-to-spill transition can only come from hard admission. The run executes each plan gate immediately before its query so a later stats patch cannot affect the earlier target.

Both `EXPLAIN` and `EXPLAIN PHYPLAN` must contain a hash join/hash build.  The broadcast gate rejects a shuffle annotation on the HashJoin line; the positive-control gate requires one.  A failed gate prevents that query from executing.  The execution phase uses `EXPLAIN PHYPLAN ANALYZE`, so runtime `SpillRows`/`SpillSize` can be collected per operator.

## Classification

The only outcomes are `REPRODUCED`, `NOT_REPRODUCED`, and `INCONCLUSIVE`:

- broadcast `REPRODUCED`: every evidenced broadcast HashBuild has `is_shuffle=false` and zero spill counters;
- shuffle positive-control `REPRODUCED`: both routed attempts contain threshold-eligible (`InRows >= 1000`) HashBuilds and spill on both CNs; naturally empty partitions are accepted only when their spill counters remain zero;
- complete, contradictory evidence gives `NOT_REPRODUCED` for that phase;
- missing/mixed operator evidence, a failed plan gate, timeout, non-zero query process exit, safety cancellation, or cgroup OOM gives `INCONCLUSIVE`.

In fixed mode, the overall result is `FIXED_ACCEPTED` only when the expected broadcast and shuffle phase evidence is complete, both SQL results are exact, the safety monitors remain clean, and final telemetry is valid. Historical mode retains the legacy `REPRODUCED` overall result for old-build comparison.

Cleanup is performed by `99_cleanup.sql` unless `--keep` is used.  To avoid leaving data behind after an interrupted run, invoke `stop.sh --runtime ...` and remove the private runtime only after reviewing the saved artifacts.
