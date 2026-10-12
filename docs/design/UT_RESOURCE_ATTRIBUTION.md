# Race UT resource attribution

Related issues: #29039, #28419.

Run 35176331513, job 105058855532, PR #29031 head b9491ab697:

- The light stage runs from 03:01:51 to 03:18:08 UTC with `-race -p 6`.
  At 03:05:27 the cgroup holds 13,645,627,392 bytes with no active test case
  reported. The historical peak first reaches 17,181,179,904 bytes by 03:06:27.
  This does not identify individual compiler, linker, test or file-cache usage.
- At 03:31:29, near the restore failure, the resource throttler reports process
  RSS of 3.088 GiB. Nearby cgroup samples are approximately 10 GiB. The earlier
  16 GiB historical peak cannot establish memory pressure at the failure time.
- TestDataBranchDiffAsFile passes in 247.24 seconds; its special-column child
  takes 177.19 seconds. The PR design records an earlier CI parent time of
  70.24 seconds. These different runs do not isolate the effect of the patch.
- Small WAL appends are also slow before that DML case: at 03:56:00 an append
  of 3,983 bytes takes 1.505 seconds. Dragonboat reports batches of delayed
  LocalTick messages. Scheduling, I/O and process pauses remain possible;
  neither a leak nor CPU throttling is proved by these logs.

## Diagnostic change

Reuse the existing heartbeat process inventory to record at most eight largest
RSS processes, with PID, parent PID and executable name, never arguments. Add
selected memory.stat fields, memory.events, cpu.stat and cumulative pressure
counters from the resolved cgroup v2 memory boundary to the existing checkpoint.
The output identifies its cgroup scope. A CPU limit at another ancestor may
require separate investigation. Process inventory is for the visible PID
namespace and is not an exact cgroup memory breakdown.

Read counter differences between timestamps; do not interpret cumulative
throttling/pressure totals or lifetime memory peaks as instantaneous pressure.
Memory fields overlap (file includes shmem; kernel includes slab), and summed
RSS double-counts shared pages. Missing cgroup files are tolerated. No polling
worker, heap dump, test retry, timeout or scheduling change is introduced.

## Decision boundary

Use process RSS to separate compile/link peaks from long-lived test growth, and
anon/file/shmem counters to separate process memory from file-cache pressure.
Correlate CPU throttling and pressure deltas with the slow interval before
changing concurrency. If a test process grows across fixture reuse, collect
heap/native allocation and goroutine profiles for that process before changing
cleanup or caches. This patch supplies attribution; it does not claim a suite
speedup or fix the historical failure.

## Validation

The shell scheduler test file passes all tests in 14.431 seconds on macOS/arm64.
Focused resource-field and heartbeat cancellation tests also pass. Fixtures
cover overlapping memory fields, event and throttling counters, pressure totals
and missing files. Shell syntax and whitespace checks pass. Linux CI resource
and wall-time improvement remain unmeasured; no production or SQL behavior,
test selection or coverage is changed.

## Containing shared CN-state failures

Run 35177731314, job 105063157387, exposes a separate, concrete lifecycle
defect. `TestGroupConcatNamedTimeZoneRemoteOwner` fails its Draining RPC
before registering restoration. The subsequent prepared-cache test observes
the same CN still Draining. `TestConvRowBasesRemoteFallback` has the same
unsafe ordering. This is contamination within one DML test process, not shared
Go state between different package binaries.

Both consumers now use the existing `withCNDraining` contract:

- An ambiguous Draining response fails the original test, does not run the
  query body, and marks the fixture for disposal. No compensating write can
  race a late Draining commit.
- An acknowledged transition must be authoritatively visible before queries.
  Its restoration runs on normal return, Goexit or panic using a fresh context;
  reuse requires verified topology. Unverified restoration also invalidates.
- Query resources and saved runtime/plan values unwind first. Database DROP
  is omitted only for a fixture owned by destruction; the client still closes.
  The outer test defer discards the fixture after Run releases its mutex. A
  close error remains a test failure and the existing fixture owner blocks reuse.

These callers are serial: no DML test enters the shared fixture in parallel.
This approach does not authorize parallel reuse across the unlock/discard gap.
No production, shared-helper, scheduling or overall test-timeout changes are
made. Existing two-row SQL, remote-plan and old-protocol fallback assertions
remain. Fake tests cover the ambiguous write and failed restoration; new
cancel/panic controls complement existing Goexit coverage without a cluster.

The two migrated callers adopt the existing helper's 30-second phase budget
instead of the legacy API's 3-second write timeout. This is an explicit
failure-path tradeoff, not a claim of identical worst-case latency: a stalled
phase can wait longer, and invalidation can require shutdown and later startup.
The healthy path retains two writes and two successful authoritative refreshes,
the shared fixture and unchanged data sizes. Failed writes are not retried;
only existing bounded authoritative-read polling is reused. No suite speedup
or solution to the initial control-plane/resource stall is claimed. #29039
remains open for that attribution work.
