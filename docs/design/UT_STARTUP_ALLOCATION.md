# Demand-sized startup allocation for race UT

Related issue: #28419. Design accepted before implementation against
`e66c9da813ccbb8b9d1eedbdc40b619b7ee5dff3` on 2026-09-17.
Independent design: GPT-6 medium, session
`01a0aef9-1c5b-7102-8413-438fd0668fef`, revision D5. The implementation PR
contains the final validation and review record.

## Evidence and objective

Recent race UT jobs took 50–63 minutes when successful and sometimes exhausted
their 70-minute runner budget. Case elapsed includes overlapping admission
wait: one 359-second Arrow test spent about 325 seconds waiting. Removing that
counter does not remove work from the critical path.

An isolated macOS/arm64 Go 1.26.4 race run of `TestRunSQLWithFrontend` at the
base above passed in 11.59 seconds. With the shared single-CN fixture still
live, its sampled Go heap was 532 MiB: WAL records 204 MiB, transaction trace
queues 92 MiB, and FIFO cache construction approximately 101 MiB. This is
startup allocation evidence, not a leak diagnosis or a Linux CI speedup.

The objective is to remove unused allocation/initialization work on the existing
runner while retaining tests, race instrumentation, admission and topology.

## Changes and ownership

* WAL clients keep their eager connections, factory/retry behavior, pool size
  and write-token protocol. Only their payload record is deferred. The first
  append allocates at least the configured initial payload size, then sets the
  exact entry length and backend-owned header. A zero-length first entry also
  gets a valid header. Exclusive checkout owns the record through append/retry;
  return to the pool transfers ownership. An unused return needs no record.
  Existing subsequent growth and oversized-return policy are unchanged.
* FIFO caches keep all shard locks, non-nil maps, callbacks, ghost identity,
  pending-byte accounting and data limits. Empty maps no longer reserve 1024
  entries in each of 256 shards. Index storage grows with actual occupancy.
  Dense insertion may pay map growth earlier, so steady-state and colliding-key
  benchmarks are controls. This does not shrink maps after a historical peak.
* Embedded testing uses the existing CN trace buffer setting: zero becomes
  1024 entries per queue before scenario callbacks. Explicit settings and later
  callbacks win; non-testing defaults remain unchanged. Tracing remains
  functional with existing blocking backpressure and no dropped-event path.
  Additional CNs use the same construction boundary. Trace consumer validation
  crosses the configured queue capacity and checks every persisted record.

No new workers, retries, locks, cache publication, wire formats, or persistent
state are introduced. The allocation changes do affect production WAL/cache
constructors; the smaller trace default applies only to embedded testing.
If every WAL client writes or a cache grows dense, its memory can approach the
old footprint. This is demand allocation, not a new hard memory limit.

## Alternatives and scope

Do not raise concurrency or remove admission to hide waiting. Existing
light/issues overlap has already regressed measured runtime/headroom and stays
off. The optional embedded prebuild path and cross-run build-cache compatibility
require separate evidence; they are not changed or counted as gains here.
Existing fixture sharing already preserves meaningful restart/configuration
boundaries. Removing CNs, tests, assertions or those boundaries is not an
acceptable speedup. A lazy backend connection pool or lazy trace-service state
machine would change substantially more failure/ownership behavior than these
allocation corrections and is deferred.

## Validation and claims

Test first zero/small/large WAL append, unused checkout/return, buffer reuse,
backend record identity, factory failure and pool close; run WAL append/replay
consumers. Run FIFO normal/race tests and cache consumers, plus construction
and dense steady-state controls. Test trace configuration precedence, real
consumer progress beyond capacity, and embedded SQL/expansion consumers.
Preserve the existing package/case inventory and use named terminal results.

Measure prebuilt baseline/candidate executables to separate compile time from
service work, with matched mode and resource settings. Heap profiles omit race
runtime, native allocations and file cache; report RSS separately. Record
local workload and whole-CI results separately. A local allocation improvement
does not prove a full 50-minute job is faster. Do not wait for or describe pending
CI as passed; use subsequent same-runner results to assess end-to-end benefit.
