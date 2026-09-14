# Recoverable mmap reclamation

Revision: v1, 2026-09-11. Owner: malloc maintainers; delivery: XuPeng-SH.
Incident: [#28736](https://github.com/matrixorigin/matrixone/issues/28736).
Series: [main #28737](https://github.com/matrixorigin/matrixone/pull/28737),
[4.2-dev #28739](https://github.com/matrixorigin/matrixone/pull/28739).
Status: proposed; the exact reviewed commit and decision are recorded separately
in `mmap-reclamation-review.md`. That record, not this label, controls acceptance.

## Problem and decision boundary

An ENOMEM from cached munmap currently terminates the service via panic.
The fixed-size allocator instead discards the error and loses reclamation
ownership. Neither response is appropriate for recoverable kernel pressure.
Linux can merge adjacent anonymous mappings; removing an interior allocation
may need another VMA and fail at max_map_count. This is documented in
[munmap(2)](https://man7.org/linux/man-pages/man2/munmap.2.html).
The incident reports a node limit of 65530, but the failed process's map count
was not captured. VMA exhaustion is a hypothesis, not a measured incident fact.
The isolated 55 experiment demonstrates this mechanism at a limit of 1048576.

This is a resource-controller/lifecycle design, despite the bug-fix label.
Process-wide admission, deferred ownership and background retry/logging trigger
the design gate. This document repairs an omitted design phase; it does not
claim that the original implementation was written after design approval.
Implementation acceptance must follow the separately recorded design decision.

Goals: preserve exclusive ownership after failed frees; recover without a
process crash when kernel pressure permits; bound retry/log work; preserve
normal-path performance; leave useful failure evidence.
Non-goals: eliminate VMA fragmentation, cure memory exhaustion, guarantee
request success during pressure, govern Go/libc/native allocations outside this
package, change host sysctls, or add arena allocation in this patch.

## Alternatives, independently of the current implementation

| Choice | Correctness and availability | Cost / decision |
|---|---|---|
| Panic/restart | OS eventually reclaims memory, but one recoverable free kills all work | Existing failure; reject |
| Log and forget | Keeps service running but permanently loses ownership | Reject regardless of logging limits |
| Retry while allowing unlimited new mappings | Owns failures but can accumulate an unlimited stream of retained mappings | Reject |
| Fixed queue cap, then drop/block/panic | Drop leaks; blocking can deadlock cleanup; panic restores the incident | Reject as a general ownership bound |
| Bounded arenas / fewer mappings | Addresses mapping geometry more directly | Larger allocator redesign, fragmentation/RSS/performance/ownership changes; evaluate separately |
| Retain failures and pause fresh package mmap | Preserves ownership and bounds new mapping identities during pressure | Selected; accepts process-wide degraded availability |

The gate is process-wide because the kernel's map-count resource is per process,
not per allocator or tenant. Keeping one allocator open can consume space needed
to recover another. We intentionally do not retry EINVAL/other errors, which may
signal ownership violations. The change does not hide invalid frees as pressure.

## Ownership, states and concurrency

Successful allocations keep their existing ownership contracts. After the caller
has relinquished a mapping, exactly one release path attempts munmap. On success
the mapping is gone. On ENOMEM it transfers to the process-lifetime reclaimer;
its exact slice remains live and must never return to a reuse pool.

| State / event | Transition and owner |
|---|---|
| Open, fresh mmap | Atomic admission check, then existing mmap syscall |
| First failed free | Under queue mutex: close admission, append owned mapping, start one timer |
| Pending, another failed free | Append under the same mutex; no additional timer |
| Retry | Detach up to 256 entries; keep detached bytes/count accounted; unlock before diagnostics/syscalls |
| Retry ENOMEM | Retain ownership; append behind queued arrivals for fair rotation |
| Retry success | Drop slice reference; subtract successfully freed count/bytes under mutex |
| Pending count zero | Under mutex: mark inactive and reopen admission |
| Pending count nonzero | Schedule the next bounded pass; never open admission prematurely |

The admission check is not a barrier waiting for all in-flight allocations.
Calls that observed open before closure may complete afterward. The retained
identity bound is existing live/cache/pool/channel mappings plus these admitted
calls, not a fixed byte cap. Other allocators/runtime mappings are outside it.
Each failed mapping creates one queue node; retries reuse that node. Severe Go
heap exhaustion can still prevent allocating bookkeeping: this is not OOM-proof.

Retry and enqueue serialize state transitions, not syscalls. Detached batches
remain owned by the sole retry callback, so concurrent producers cannot free or
reuse them. No per-request context owns deferred reclamation: cancellation must
not discard a failed free. There is no in-process reset, close or replacement of
the singleton. On process exit, the kernel owns final address-space cleanup;
restart starts empty. No retry state is persisted or transferred between nodes.

## Work, lifetime and diagnostic bounds

- One scheduled/running retry owner; at most 256 munmap attempts per pass.
- Initial delay 1s; no progress doubles delay to 2/4/8/16/30s, capped at 30s.
  Progress resets delay to 1s. New arrivals do not reset it.
- Retry lifetime is deliberately unbounded during persistent ENOMEM. Work per
  pass and scheduling rate are bounded, not time to recovery. A large backlog
  can take minutes or longer; even after pressure drops, recovery is not instant.
- A report opportunity at most once per 30s, including across failure/recovery
  episodes. Capture original failing stack/address/size, observed timestamp,
  pending bytes/mappings and total failed attempts, not a stack per mapping.
- Linux diagnostics count maps with bounded scanner storage, 128 MiB input cap
  and a cooperative 250ms work deadline, checked every 1024 records. Proc reads
  themselves are not interruptible deadlines; kernel/scheduler delay can exceed
  250ms. Do not claim a hard wall-clock bound. Small proc files are capped at
  16 KiB; no smaps/page-table walks. Non-Linux reports unavailable diagnostics.
- Diagnostic capture precedes retry, outside queue/cache locks, so recovery does
  not erase all map-count evidence. It can delay that retry but cannot block
  enqueue or scrape through the queue mutex.
- One lazy process-lifetime log writer, one in-flight and one buffered report.
  Saturated submissions are dropped; blocked logging never blocks reclamation.
  There is no flush guarantee on process exit. Rate limits govern submission;
  a previously blocked sink may later emit its two retained reports together.
- Three fixed-cardinality scrape-time metrics: `mo_mmap_reclaim_pending_bytes`,
  `mo_mmap_reclaim_pending_mappings`, `mo_mmap_reclaim_failures_total`.
  Detached batches count as pending; the total includes failed initial frees
  and retries. No per-address metric labels and no normal-path metric updates.

## Normal-path budget and platform assumptions

Small libc allocation and cache/pool hits remain unchanged. A fresh mmap pays
one atomic read; successful free makes its existing munmap call without queue
locking, diagnostics or new allocation. Cold failure handling uses O(pending
mapping count) nodes and bounded per-pass diagnostic storage/work.
No benchmark can guarantee zero overhead for all workloads. Acceptance is no
consistent material regression in same-host A/B allocator measurements, no extra
normal-path allocations, and transparent reporting of noise/outliers.

The design relies on the supported unix.Munmap wrapper retaining its mapping
registration when the syscall fails. Exact original slices, not arbitrary merged
address ranges, are retried. The Linux split-at-limit behavior is experimentally
covered; portable ENOMEM handling does not assume every platform has that cause.

## Operations, security and delivery

A single stuck mapping can deny all fresh package-owned mmap allocations.
This is an explicit availability tradeoff versus process death or unbounded
retention, not a guarantee that the service remains healthy. Cached reuse and
libc allocations can proceed; callers of fresh mmap may receive allocation errors.
This controller limits its own logs/retries, not all upstream request-error logs.

On sustained nonzero pending metrics, operators should inspect the first failure
log, compare complete map counts with the host limit, and check node/cgroup
memory pressure. Reduce new workload and let existing owners release mappings.
If map-count pressure is confirmed and host headroom permits, an operator can
raise the host limit; the service must not do so automatically. If pressure
persists, preserve evidence and use role-appropriate drain/restart procedures.
Do not force-drop pending ownership or reset the admission flag to restore traffic.
The process will not self-heal merely because a timer runs: reclamation needs a
successful syscall. VMA growth-source investigation remains owned by #28736.

Canary rollout: compare allocator/query errors, pending duration/count/bytes,
failure deltas and existing latency/throughput against the prior version. Alert
on persistent pending state and growth, not every failed attempt. Exact paging
thresholds belong to deployment SLOs; tune before broad rollout. Pause rollout
if degraded state persists or previously healthy workload performance worsens.
Rollback is a binary rollback/restart with normal service drain, not live
controller removal; the old binary restores the original panic risk.

No SQL/API signature, disk/catalog/wire format or persisted state changes.
Mixed versions have independent process-local controllers; backup/restore and
upgrade need no data migration. No new privileges or automatic system tuning.
Addresses, stack symbols and cgroup paths are operationally sensitive: use the
existing restricted service-log access controls, never user-facing SQL results.
One tenant can contribute to shared process pressure; this is not tenant-isolated
capacity or a substitute for workload admission control.

## Branch adaptation

4.2-dev covers SimpleCAllocator direct/cache frees and fixed-size mmap background
frees. Its tests use the existing public fixed-size constructor; no hybrid
allocator or allocator-selection change is imported. Main additionally routes
its existing HybridMmapAllocator through the same owner. The controller and
diagnostic contract are shared; no branch-specific thresholds or policies.

## Acceptance and evidence reuse

| Contract | Required evidence |
|---|---|
| Retain/retry/free exactly once, partial failure | Deterministic injected unmap tests and accounting assertions |
| Concurrent arrivals / detached ownership | Phase-barrier test, focused repeated race, owning package race |
| Backoff, batching, flapping limits | Fake clock/timer test; no scheduler sleeps |
| Logging cannot block recovery | Blocked sink with many submissions and bounded queue oracle |
| Admission scope and recovery | Fresh-mmap rejection alongside successful libc/cache/pool reuse |
| Real kernel failure | Opt-in isolated Linux allocator test: actual ENOMEM, retained bytes, pressure removal, recovery |
| Consumer compatibility | malloc and mpool normal/race suites on each branch |
| Normal cost | Same-host base/candidate benchmark, allocations and contention; report limitations |

Existing evidence: #28737 at 87fcca569d and #28739 at 2e2a5b31e4 report owning
normal/race suites, focused repeated race, affected-package lint, and two final
real-kernel episodes on 55 per branch. Main's A/B pinned medians were 2588 vs
2603.5 ns/op direct and 5854 vs 5842 ns/op cached, with a reported direct outlier;
do not relabel this as new 4.2 performance measurement. Pressure tests are not
normal CI: up to 1.1M VMAs and ~4 GiB untouched virtual space require dedicated
host headroom. No production service or sysctl is altered by those tests.

Changes only to this document do not invalidate code/test evidence. A change to
ownership, admission, retries, dependency behavior, fixture or mode does. No
full-server workload/production-root-cause proof is claimed. Before broad rollout,
maintainers/operations own validating deployment thresholds and workload impact.
