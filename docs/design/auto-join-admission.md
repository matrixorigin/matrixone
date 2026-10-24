# Automatic join spill and CN admission

Base: `956a8683890a374875e2ec23c3df652c624d885a`, on
`v4.1.6-hotfix-20261021` (`ed9d8aacc74448ed0deffd93925c049f9686f151`).
This is a focused HashBuild/HashJoin policy correction, not a planner or global
memory-controller change. Design revision 2 was independently reviewed before
implementation; the review added the empty-build guard below.

## Problem and contract

The real 1T Q9 runs at 956a entered spill after copying, with
`reason=logical-byte-threshold`, `threshold=524756704`, `input_rows=8200192`.
The preceding patch changed capacity accounting to logical payload, but still
enforced the computed static share after successful CN admission. The threshold
is exactly `(67168858112 - 0) / 16 / 8`; it does not express current CN headroom.
These observations identify that trigger, not the full EOF build size or the
reason for every difference from ed9.

For a spill-capable HashBuild, or a spilled HashJoin bucket rebuild, allow
ordinary queries to exceed the computed automatic share only when all of:

* the original `SpillThreshold` is exactly zero (automatic mode);
* `ImportMemory` is nil;
* an actual CN throttler is present;
* all build-key expressions have the existing capacity contract.

The first owner remains the builder: copy growth is acquired before copying,
map construction peak is acquired before building, and a successful JoinMap
owns the transferred reservation until its last consumer frees it. On spill,
error or cancellation, existing builder and operator cleanup retire ownership.
The negation is allocating copied build/map capacity after admission refuses it.

Rebuild uses the same policy at every depth. Below the existing maximum depth,
admission denial or unknown map bound repartitions. At maximum depth it cleans
the builder and returns `ErrOOM`; it neither builds without a reservation nor
repartitions indefinitely nor waits for upstream JoinMaps to release memory.
An empty build skips map admission and retains existing unmatched-probe output.

Explicit thresholds, controlled imports, absent CN throttlers and unsupported
key expressions keep their existing policy. No new setting, planner rewrite,
wire field, background worker, threshold constant or workload exception is added.

## Change and validation map

* R3: initial collection, map allocation and reservation transfer; HashBuild
  payload-policy and reservation fixtures, owning package normal/race tests.
* R3: rebuild, repartition, maximum depth and empty LEFT build; HashJoin payload
  fixture plus denied copy/map, depth-zero/deep empty column/expression keys,
  and finite-budget competition/release tests; owning package normal/race.
* Public behavior/performance: reuse ordinary explicit-spill SQL coverage;
  validate the candidate through real SQL and the matching 3-CN 1T harness.
  Local 100G is not proof for the real 1T spill or performance objective.
* Static/build: format, owning-package vet/lint and compile consumers with the
  repository's CGO/native dependency contract. Native artifacts must be copied
  into this isolated tree only after their source and artifact hashes match.

There is one key-contract check per build/rebuilt bucket, no new per-row work,
wait, log stream, global lock, queue or persistent state. Existing batch copy and
map-bound work is retained. Functional tests use injected small thresholds and
budgets, independently inspect actual rows and ownership, and require no sleeps.

## Limits and acceptance

This is a reservation contract for the newly eligible copied-build/map path,
not a bound on all operator/process memory. Reader frames, scatter/output and
legacy consumers are outside it. In particular a legacy explicit-threshold
deep bucket can still allocate without admission while this path holds more
memory; this can increase OOM risk under that concurrent workload. Eliminating
that legacy bypass needs a separate, wider compatibility decision.

CN admission may still legitimately reject a complete 1T build. Therefore no
claim of zero spill or restored performance follows from this design or UTs.
Candidate acceptance requires actual spill-reason/SQL/performance evidence;
the prior green workflow alone did not meet that requirement.
