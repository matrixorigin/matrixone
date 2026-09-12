# Exact PRE storage integration and gated local DOP

- Status: approved in conversation before implementation.
- Base: main `cd04bb4c1af5bc595e2147dc645dfa754f4c395b`, including #28488.
- Branch: `fix/exact-pre-storage-local-dop`; one new replacement PR.
- References: #28002 and #27854. The old branch/PR stays untouched.

## Contract and scope

Make exact PRE domains eligible for existing storage Top-K. Scalar PRE must not
silently lose its domain before bounded ranking. This is exact membership within
the chosen IVF search domain, not globally exact nearest-neighbor search. Keep
nprobe, candidate budgets, visibility, distance semantics, final rechecks and
the existing small-domain/full-centroid policy.

Main already filters integer membership before storage ranking and implements
mandatory SEMI membership. Reuse both rather than claiming those are missing.
Extend MustApply to standalone scalar PRE, and safely enable existing storage
paths for required membership and full-centroid domains. No new decoder,
FileService layer, ranking algorithm, or cross-CN domain protocol is included.

The alternatives are retaining the local materializing fallback (unnecessary
embedding work), adding another reader/decoder layer (duplicates #28488), and
enabling unmeasured DOP by default (not justified). The approved choice reuses
existing execution paths and keeps local DOP opt-in.

## Required domain and execution

Set RuntimeFilterSpec.MustApply on both producer and consumer for standalone
scalar PRE, preserving existing SEMI requirements and correlated APPLY behavior.
Keep required domains coordinator-local regardless of diagnostic placement hints.
Validate terminal, payload, cardinality and key type before opening entry readers.
DROP is exact emptiness; unavailable, malformed, canceled or admission-rejected
required input cannot become an unrestricted bounded scan. Preserve the current
HashBuild budget/terminal protocol and propagate its errors. Keep must_apply=12;
do not add required_vector_search_domain or copy the old generated protobuf.

Use one admitted exact integer docfilter through FilterHint.BF for membership-only
storage Top-K. For exact membership/residual expressions, use the existing
ReadWithFilterAndTopK path, including #28488's fused chunks. Unsupported ordering,
distance conversion or reader capability retains exact local filtering before
ranking. An approximate Bloom filter alone never authorizes required-domain
bounded ranking.

Pass an explicit all-centroids decision from the existing small-domain policy;
that case uses a version-only physical prefix. Nil centroid IDs cannot silently
mean an unrestricted scan. Do not change catalog/hidden-table encoding or RPC
capabilities.

## Opt-in DOP and interfaces

Add optimizer_hints='vectorLocalDOP=1', using the existing parser. Other values
keep one reader. Enable only coordinator-local, synchronous, uncorrelated,
single-round PRE with an exact shareable integer domain. Async, adaptive,
noninteger, correlated and distributed paths remain single-reader.

Append advisory VectorIndexScan.scan_work at free field 18, with estimated rows,
blocks, stored vector bytes per row and objects. Preserve through copies,
serialization and EXPLAIN. Estimate from hidden entries-table statistics and
nprobe/lists, never output width, LIMIT or scalar selectivity. Missing/invalid
statistics choose one reader; only cancellation propagates lookup failure.
Use existing scan costing and cap DOP by blocks, known objects, effective max_dop
and coordinator CPU. Estimates are not pruning promises; small-domain full scans
may exceed the initial-probe estimate.

Generate work metadata only when planning with opt-in. Recheck the current gate
at execution compilation so disabling it caps cached plans at one. Enabling a
plan without work metadata requires replanning. Keep Hooks.NewReader and APPLY
unchanged; add an optional parallel-reader capability returning exactly the
requested count. Generic compilation dispatches by capability, not algorithm name.

## Generation ownership

One generation owns immutable domain bytes/filter, metadata version, centroid
route and one snapshot clone. Prepare shared state before publishing readers,
under a generation-owned parent-derived context and without a mutex across I/O.
Each reader owns scanner, heap/results and child cancellation. Local shard
ordinals are private to entry scanning, preserving logical CN identity and
centroid-cache keys. Objects belong to one shard; in-memory entries to one owner.

| Audit | Invariant |
|---|---|
| Q1 | One generation payload/filter and reader-owned shares; every mpool allocation reaches Free. Partial construction and every terminal path release all acquired ownership. Readers never close the parent transaction. |
| Q2 | Parent cancellation terminates initialization/reads; closing one reader cannot cancel shared work needed by siblings. No new background worker, mutex-held I/O or cross-CN wait. |
| Q3 | DOP and shard ordinals are bounded; no per-reader domain or centroid duplication, new cache or pool. Empty domains return the required number of empty readers. |

## Validation and rollout

UT/BVT cover scalar PRE and SEMI domains, empty/unavailable/malformed inputs,
nearer nonmembers, threshold boundaries, strings/composite keys, prepared reuse,
snapshots/tenant identity, exact-before-heap ordering and version-bounded full
scans. DOP tests cover gates/caps, statistics fallback, disjoint shards, in-memory
ownership, exact reader counts, excluded modes and partial-open/Close cleanup.
Reuse small fixtures, run owning packages, applicable adaptive race stress, full
SCA, and SQL regressions twice with teardown verification. A multi-CN fixture
proves the required entry scan remains coordinator-local.

Performance arms: A=newest main including #28488; B=replacement at DOP1;
C=the same replacement with DOP enabled. Pin identical data/index/queries/probes,
separate controlled cold/warm phases, interleave repeated runs at concurrency1
and100, and record QPS/p50/p95/p99/recall/result counts, CPU, peak memory, scanned
bytes and actual DOP. C must improve median single-client latency over B, preserve
concurrency100 throughput, stay within110% of B for tail latency/CPU/peak-memory/
scanned-byte costs, and lose at most0.001 recall before any default-on proposal.
Report B versus A separately. Missing/inconclusive Wiki-10M evidence leaves the
gate off; local microbenchmarks are not workload recovery evidence.

Rebase newest authoritative main before every push and revalidate affected work.
Publish only to aunjgr, one non-draft PR with implementation text separate from
validation. No machine-specific notes. Do not close #28002, download Wiki-10M,
dispatch remote benchmarks or enable DOP by default without separate approval.
