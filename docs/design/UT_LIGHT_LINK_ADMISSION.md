# Race-UT light-stage link admission (revision 1)

Issue #28419; implementation PR #29094. Supersedes the default-three decision
in `UT_LIGHT_RESOURCE_BUDGET.md`, not its coverage or rollback requirements.

## Evidence and contract

Run 35374600281 / job 105696270026 took 62m47s. Light took 24m26s;
sampled CPU consumption averaged about 3.05 cores. Samples show both a
compile-only prefix and simultaneous linkers with individual RSS above 2 GiB.
The cgroup peak equalled 16 GiB, but file cache was substantial, OOM/max events
were zero and memory PSI was small. These facts justify separating task and
link admission, not claiming unlimited memory or a predicted ten-minute gain.

Invariants: identical package/test selection and results; full race; unchanged
timeouts; one runner; no additional active embedded cluster; at most three Go
link processes in the light stage by default. `go test` continues to own its
action DAG, cache, execution, output, exit status and package cleanup.

## Design and alternatives

Restore the light task ceiling from three to six, still capped by UT_PARALLEL.
Use Go's existing `-toolexec` protocol for a small, Linux-only shell adapter:
non-link tools and `-V=full` queries exec the original tool unchanged. Actual
links acquire one of three kernel `flock` slots and then exec the original
linker with the same arguments, environment, stdout/stderr and process group.
The lock descriptor survives exec and is released by process exit, including
failure or SIGKILL. The adapter creates no persistent worker or child process
around the linker. It does not alter the compiler/tool identity or artifact.

The runner creates a private mktemp directory with a fixed slot count before
the light stage. It never replaces/unlinks live slot files. It removes these
files after the foreground/background light process group has been joined;
cancellation uses the existing TERM/KILL group-drain path first. A missing
lock facility falls back to the prior three-task cap. Bad slot configuration,
file errors, or unexpected flock errors fail closed. Waiting wrappers are
bounded by Go's task budget and are cancellable by the existing group signals.
Nonblocking slot scans with a short admission poll avoid waiting behind one
specific long link when another slot becomes free. Fairness is best-effort;
the finite action DAG and unchanged outer watchdog bound the run.

UT_LINK_PARALLEL defaults to three, supports 1..64, and zero explicitly disables
the adapter for A/B. The light/issues overlap remains off. Other stages and
cluster admission remain unchanged. Rollback: UT_LIGHT_PARALLEL=3 and
UT_LINK_PARALLEL=0. Explicit external tool wrappers must not be silently lost.

Alternatives: keeping p=3 leaves compilation under-admitted; unrestricted p=6
can double heavy linker fan-out; prebuilding all test binaries duplicates Go's
cache/execution ownership and already failed to show a critical-path gain.
The selected adapter is intentionally smaller than a new test scheduler. It
still consumes a Go task slot while waiting, so a phase containing only links
cannot become faster merely by raising the task budget.

## Diagnostic work reduction

The heartbeat currently reparses every report from byte zero each minute.
Replace only its diagnostic reader with a Python incremental reader, keeping
the authoritative raw reports and failure-time AWK reader unchanged. A
heartbeat-owned state file holds each report's identity, complete-line offset
and active cases. Reset on replacement/truncation, retry incomplete final lines
on the next heartbeat, forget removed reports, and atomically replace state.
No background reader, queue or unbounded event history is added. Active state
is bounded by the suite's active cases; report count by existing stage workers.
Corrupt/missing state rebuilds from the raw source. JSON strings are escaped in
diagnostics; parsing failure must not affect test status or overwrite raw data.

Record visible CPU quota/cpuset alongside existing pressure counters and log
link admission wait separately from test execution. Do not label a leaf quota
as the effective host quota. Cache seeding is not changed without evidence of
invalid/missing entries; an existing cache is never cleared for this change.

## Validation and design-first decision

R3 closures: runner/adapter lock ownership, cancellation, diagnostic state.
No product API, SQL, wire or persisted data changes; no additional BVT needed.
Validate independent slot overlap and hard cap, failure/kill release, waiting
cancellation, version/non-link passthrough, runner fallback/argument dispatch,
real Go race build/cache compatibility, and complete normal/race optools tests.
Reader tests cover append, terminal events, duplicate reports, truncation,
replacement, missing files, partial lines, corrupt state and escaped names;
compare its output with the existing full-scan oracle and benchmark no-change
heartbeats. Resource metrics use synthetic cgroups, not host-dependent asserts.

Design-first review (2026-09-19, revision 1): PASS. Complexity trigger is a new
bounded link admission boundary, so this design precedes implementation.
Accepted tradeoffs: Linux flock dependency with conservative fallback; polling
only while slots are busy; best-effort fairness; no asserted CI speedup before
the next completed run. The exec boundary retains Go's error/cache semantics,
kernel-owned leases avoid stale tokens, and cleanup follows group drain.
Implementation deviations require an updated design review before delivery.
