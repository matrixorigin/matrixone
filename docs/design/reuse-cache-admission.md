# Bounded reuse-aware decoded cache admission

Owner: #30849. Base: main 38f4ba680442ec00be281cbeb707dbfbe1d18bc7.
Status: design approved by the user's “Implement the plan” instruction on 2026-11-03.
Risk: R3 (hot path, resource ownership and generation lifecycle).

## Contract and implementation

Retain normal admission before the reader threshold. Beyond it use an internal reuse-only policy: an unseen key records metadata and decodes transiently; a seen key can request existing cache admission. Explicit SkipMemoryCacheWrites overrides both history and payload admission. Only valid positive CachedDataSize estimates qualify initially.

Decide before decoded allocation, so rejected reads do not evict useful cache contents merely to allocate temporary decode storage. Existing physical reservations, pin accounting, ownership transfer and rejection remain authoritative. Do not count retries of the same read as reuse. Shared decode/fill cannot upgrade a bypass decision.

Use existing FIFO ghosts, keyed by canonical path/offset/size. History retains no payload, is bounded by represented decoded bytes and a metadata allowance min(configured capacity/1024,64 MiB), with that allowance deducted from payload capacity. Include retained paths and conservative item/map overhead. Capacity shrink trims both; delete/flush/close remove relevant history. No new cache worker, user knob, on-disk format or protocol.

Alternatives: removing the cutoff repeats the reverted 1T exposure; rejecting only at Update is too late because allocation can evict; retaining encoded pages is a separate policy and is outside this PR. Default-on acceptance requires independent SF10/SF100 and paired equivalent 1T regression evidence.

## Validation and delivery

Focused deterministic tests cover boundaries, fallback, allocation refusal and reset/spill.
For shared state, add cancellation/completion races and owning-package race evidence.
Use existing fixtures and the controlled CGo wrapper; no benchmark-time input/result verification.
Benchmark this candidate alone against clean main with identical data/config/CPUs, exclude
the cold run, and separate scored wall time from instruction/CPU profiling. Use five paired
SF10/SF100 measurements and three paired full-workload 1T measurements where required.
Require a confirmed target TPC-H wall-time gain and no confirmed query/total regression.
SF10 stays local; SF100 uses server129 only when available. Report a Total row.
No new load/profiling is authorized while server129 remains busy.
Use the repository PR template and keep benchmark evidence outside the squash-message section.
