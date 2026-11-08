# Bounded single-CN shuffle Bloom filters

Historical design, superseded on 2026-11-09 by the approved early-domain plan:
[early complete runtime domains, reviewed revision adebe845cb4](https://github.com/matrixorigin/matrixone/blob/adebe845cb4a77d97cf7dd0ae4b2efaf87dc224a/docs/design/20261109-early-runtime-filter-domains.md).
The implementation now collects one complete filtered source-scan domain and
applies it in eligible local scans, including necessary equality components of
composite joins. The earlier private-per-producer shuffle hook is removed.
Current acceptance requires a confirmed Q9 gain at SF10 and SF100, all-query
and Total regression checks, and no spill. SF1000 is outside this replacement's
acceptance scope. The text below records the original implementation decision
and its former acceptance rule; it does not describe the replacement.

Owner: #30844. Base: main 38f4ba680442ec00be281cbeb707dbfbe1d18bc7.
Status: design approved by the user's “Implement the plan” instruction on 2026-11-03.
Risk: R3 (hot path, resource ownership and generation lifecycle).

## Contract and implementation

Only accelerate already-local ordinary INNER/left-SEMI joins on one same-type integer equality key. Preserve placement, join order and cardinality estimates. Exclude null-safe/composite/string keys, preserved-side joins, correlated dependencies and reused shuffle topologies. Respect the disable hint. Distributed shuffle stays PASS.

A query-generation-owned coordinator registers actual duplicated producer IDs. Producers insert already-evaluated keys into private CBloomFilters with the same geometry/seed (1% target). Admit complete producer+scratch peak before allocation, within the execution budget and 64 MiB total per query. If it does not fit, use PASS. Merge completed filters; publish only after all producers succeed. DROP requires all producers to prove no joinable keys. Spill/unsupported/recoverable allocation failures fail open; actual build errors retain query-error propagation and unblock waiters. Completion is idempotent and generation-bound.

Probe shuffle tests the immutable complete filter before routing, using reusable scratch and existing selections. Bypass further testing after 8192 rows if fewer than 10% were rejected. Direct local handles avoid wire/protobuf changes. No key-vector retention, per-row CGo calls or intermediate filtered batch. Add fallible construction and supplied-buffer testing helpers without changing existing callers.

Do not delay probe scanning or ordinary routing while the optional domain is incomplete: pass those batches through the existing path and enable testing only after completion is observable. This preserves build/probe overlap without consulting partial membership or adding buffering. Copy only the worker identity needed by filter-enabled duplicated HashBuild operators; ordinary HashBuild metadata behavior is unchanged.

The private complete-domain notification replaces the probe Shuffle's legacy dependency wait when the optional handle is attached. HashJoin still owns partition join-map readiness and empty-build behavior. The coordinator keeps its complete-domain publication rule; no partial membership is published or tested.

Execution generation owns coordinator, reservations and filters; consumers lease them. Cancel/reset closes waits and frees only after live builders/consumers finish. Existing per-producer DROP finalization must not run for coordinator-owned filters.

Alternatives: partition-local filters before global routing are incorrect; cross-CN fan-in and scan Bloom pushdown require broader protocols and are deferred. Exact IN alone is cardinality-limited for larger builds.

## Validation and delivery

Focused deterministic tests cover boundaries, fallback, allocation refusal and reset/spill.
For shared state, add cancellation/completion races and owning-package race evidence.
Use existing fixtures and the controlled CGo wrapper; no benchmark-time input/result verification.
Benchmark this candidate alone against clean main with identical data/config/CPUs, exclude
the cold run, and separate scored wall time from instruction/CPU profiling. Use five paired
SF10/SF100 measurements and three paired full-workload 1T measurements where required.
Readiness criterion updated by the user on 2026-11-04: require no confirmed query/total regression; a confirmed speedup is not required. Keep the existing resource, correctness and applicable scale gates.
SF10 stays local; SF100 uses server129 only when available. Report a Total row.
No new load/profiling is authorized while server129 remains busy.
Use the repository PR template and keep benchmark evidence outside the squash-message section.
