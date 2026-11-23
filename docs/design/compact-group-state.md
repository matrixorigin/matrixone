# Compact group lookup and decimal aggregate addressing

Owner: #30841 / #30847. Base: main 38f4ba680442ec00be281cbeb707dbfbe1d18bc7.
Status: design approved by the user's “Implement the plan” instruction on 2026-11-03.
Risk: R3 (hot path, resource ownership and generation lifecycle).

## Contract and implementation

Keep the generic group hash table authoritative. For ordinary one/two-column non-Unicode flat CHAR(1) grouping without NULL/GROUPING/string-source metadata, derive a direct canonical-key-to-group-ID index after 512 eligible rows. Empty canonical CHAR is code 0; one byte is byte+1. All-hit chunks may reuse existing IDs; any miss uses the whole existing transactional preview/preflight/commit path. Publish index entries only after aggregate growth/fill succeeds. Generation teardown, rebuild, reset and spill discard it. Optional capacity rejection disables the index for that generation.

The index (257 or 66049 uint64 slots plus bounded chunk scratch) is MPool-owned and charged to the existing group allocation account. No vectors or strings are retained. The generic table, output keys, spill and wire formats do not change. A private existing-only preview avoids fabricating a transactional insert plan.

For non-DISTINCT decimal SUM/AVG with one resident chunk and <=256 groups, replace local group hashing with direct group-ID indexing, touched-group tracking and one scatter per touched group. Checked accumulation additionally requires <=256 input rows per call; larger checked calls retain the existing 255-slot combiner and its direct updates after that cache fills. Widening-safe accumulation retains the dense path for larger calls. Larger states keep the current path. Preserve NULL/constant/offset/overflow semantics and per-group accumulation order; do not change AggFuncExec, decimal types, widening or expression fusion.

Alternatives: a replacement perfect hash table would duplicate spill/publication machinery; aggregate pointer APIs would expand every executor contract. This derived index and private kernel dispatch keep those owners intact.

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
