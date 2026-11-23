# Non-evicting allocation for late scan reads

Owner: #30849; implementation PR #32093.
Status: revised three-PR coding plan approved by the user's “Implement the plan” instruction on 2026-11-03. The compact-group and local-shuffle-filter designs remain independent and unchanged.
Risk: R3 (hot path, resource ownership and generation lifecycle).

## Contract and implementation

Retain normal allocation and S3-FIFO behavior before the existing reader threshold. Beyond it use MemoryCacheNoEvict: cache lookups remain normal, but conversion may reserve only unused cache backing capacity. A first touch can be cached immediately when space is available. Refusal uses transient decode storage, never forced eviction or a wait for pinned buffers. Explicit SkipMemoryCacheWrites overrides this mode.

Use the actual allocator backing size, not CachedDataSize estimates. Pending reservations and all live backing allocations, including evicted buffers held by readers, remain charged until native deallocation. A reservation moves pending ownership to FIFO membership on publication without releasing its live backing charge. Idle pending allocations stay charged and can be reused without a new reservation. Allocation failure and final release discharge ownership exactly once. Backing bytes exclude native arena fragmentation and process RSS; existing pressure reclamation remains authoritative.

Non-evicting publication rechecks capacity, close and pressure under the existing accounting guard and rejects rather than evicting. Successful publication uses ordinary S3-FIFO queue placement, ghosts, callbacks and ownership handoff. Admission state is destination-scoped. Only matching cache-owned, reservation-backed results qualify; foreign/transient results must not be rehomed through ordinary allocation. Apply the same mode to LocalFS, S3FS, disk conversion and DecodeFromBytes. Shared-decode keys keep policy separation and conversion uses the selected allocator; sharing/fill coordination cannot upgrade bypassed data. Cancel, decode failure, duplicate insertion, flush and close preserve one release owner. With current main's reference-owning S3-FIFO, conditional publication takes index shard -> accounting guard, then uses ordinary insertion notifications and small/main/ghost queue placement outside those locks. Final backing release is atomic and cannot reacquire cache locks. Validated remote hits select the same entry-scoped allocator before copying RPC-owned bytes, so allocation cannot bypass the policy.

Remove the full-key observation FIFO, its cloned keys, quotas and observation-specific rollback/cleanup. SF100 diagnostics showed 1.9 million late candidates with zero admission because 16 MiB of history retained only 56,299 of roughly 278,000 recurring late keys. Preserve that evidence as superseded-implementation evidence. No frequency sketch, new cache tier, controller, read-ahead subsystem, worker, user knob, allocator-interface expansion, persistence format or protocol change. Encoded-page/mmap changes remain outside this PR.

Alternatives: unrestricted allocation repeats the reverted 1T exposure; rejecting only at Update is too late because allocation can evict. Free-capacity-only allocation does not replace stale residents on behalf of late scans; normal readers retain existing S3-FIFO replacement. Decoded residency can still compete with query and OS file-cache memory, so a performance claim requires measurement.

Known capacity refusal uses a conservative negative hint before allocator-size/pressure locks and before advertising memory publication to I/O/fill coordination. Otherwise a full-cache read could serialize followers that must read again because nothing was cached. Pending fills can still accept joins, disk publication remains independent, and transient conversion sharing remains available. This hint never grants capacity; the locked reservation is authoritative.

## Validation and delivery

Focused deterministic tests cover first-touch fill, resident-preserving full-cache refusal, competing reservations, held buffers after eviction, actual/estimated size differences, publication-time pressure/shrink, ordinary ghosts, explicit suppression, destination isolation, allocation/decode failure, cancel and reset/close. Extend existing fixtures; verify disk conversion and shared-decode paths preserve the allocation mode.
For shared state, add cancellation/completion races and owning-package race evidence.
Use existing fixtures and the controlled CGo wrapper; no benchmark-time input/result verification.
Benchmark this candidate alone against clean main with identical data/config/CPUs, exclude
the cold run, and separate scored wall time from instruction/CPU profiling. Use five paired
SF10/SF100 measurements and three paired full-workload 1T measurements where required.
Readiness criterion updated by the user on 2026-11-04: require no confirmed query/total regression; a confirmed speedup is not required. Keep the existing resource, correctness and applicable scale gates.
SF10 stays local. The specifically authorized local SF100 disk test uses all 32 CPUs, 48 GiB application memory, 16 GiB CN cache, 32 GiB agg_spill_mem and no swap; any SF100 spill invalidates the run. Reuse loaded images; no data/result verification. Run a pilot before completing five pairs. Report all query wall times and a Total row. SF100 all-memory and the original constrained-memory SF1000 protocol use server129 only when available.
No new load/profiling is authorized while server129 remains busy.
Use the repository PR template and keep benchmark evidence outside the squash-message section.
