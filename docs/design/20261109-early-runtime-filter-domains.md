# Early runtime-filter domains

Owner: #30844; implementation: replacement of #32098; series: #32379 / #30838.
Status: implementation scope and completed-cardinality admission amendment approved
by the requester on 2026-11-09 ("approve. go on").
Risk: R3, planner/compiler/reader ownership, synchronization and hot-path work.
This design is independent of dictionary vectors and cache PR #32093.

## Problem and invariant

The retained SF100 Q9 execution emits 600,037,902 LINEITEM rows into a two-key
join. The existing #32098 candidate admits only one-key local joins and tests
membership after scan output is constructed. Its independent benchmarks do not
demonstrate a useful Q9 gain. Historical instruction and CPU measurements are
motivation, not acceptance of current main or this replacement.

An optional domain may reject a row only when its key cannot occur in a
successful exact join. It must be a complete superset of the required build-key
projection at the same statement snapshot. The exact join and all its equality
conditions remain authoritative. A partial build, missing producer, malformed
binding, budget refusal, spill or failed optional construction cannot prove
absence. Genuine query errors and cancellation retain their original outcome.

The first implementation supports read-only, uncorrelated, unshared single-step
SELECT trees, plain same-type int16/int32/int64/uint16/uint32/uint64 keys, ordinary
equality and INNER joins. It preserves join order, estimates, physical placement
and the runtime-filter disable hint. It does not force single-CN execution.
Prepared, multi-CN, volatile/error-sensitive paths, outer/anti/null-safe joins,
limits and other unproven boundaries stay on ordinary execution.

Precedent is DuckDB 1.5.6's pinned
[join-filter pushdown](https://github.com/duckdb/duckdb/blob/069cc9f9b5be802405797faecc284961b07c70ef/src/optimizer/join_filter_pushdown_optimizer.cpp).
Its optional filter is not a replacement for SQL equality. MO additionally needs
explicit complete-worker and colocated-delivery proofs for its pipeline scopes.

## Logical proof and plan annotation

Analyze the optimized, finalized query using structural column lineage. Reuse the existing
deep-runtime-filter tree and truncation-safety checks. Track equality lineage by
binding identity, never by table/column spelling. Start with a selective build
domain already produced for an equijoin. An ancestor INNER join may apply a
single component of a composite key as a necessary condition when lineage proves
it. Filters and direct column projections are transparent only where the existing
safety checks prove that skipping input cannot suppress observable errors.

For Q9, filtered PART.p_partkey is a necessary domain for both PARTSUPP.ps_partkey
and LINEITEM.l_partkey. The former feeds the PART join; the latter also feeds the
ancestor composite supplier/part join. A domain from filtered PART is allowed to
contain keys later removed by other build-side joins: extra keys cause false
positives, not false negatives. No SQL-specific names or values enter the rule.

Store candidate domains in a new optional Query annotation, separate from
RuntimeFilterProbeList/BuildList readiness messages. Each descriptor identifies
the originating join, source scan and source column, immutable integer key type,
and target scan/column pairs and known probe NDV cost hints. NDV affects profitability,
never the equality proof. Column identities use the finalized scan schema,
not a pre-remap expression slot. Include descriptors in deep-copy contracts.
Recompute/revalidate endpoints before physical binding; missing or changed nodes
disable a candidate before physical execution.

Older readers ignore this annotation and therefore create no new wait. Old plans
without it use ordinary execution. This is not a new runtime RPC message or a
dictionary wire format. Compilation removes unsupported annotations before scope
serialization; local pointer bindings never cross a CN.

Annotations are optimization hints, not authority: compilation rederives the
key/type/lineage and dependency proof from the current plan. Invalid or stale
annotations disable the optimization. Handles and reservations belong to one
statement generation and cannot be shared across tenants, snapshots or sessions.

## Delivery and readiness

Compile binds a candidate only after actual scopes and placement exist. Verify
one statement generation/message board, complete actual source-worker identities,
colocated scan consumers, and an acyclic start/wait graph. The producer must be
independent of every scan waiting for its domain and started concurrently before
those waits. Reuse existing topology enumeration rather than inferring producer
count from probe fanout. A safe logical equality is insufficient without this
physical proof.

Keep the existing shuffle readiness and HashJoin join-map contracts. Optional
membership has its own completion; it does not replace a dependency notification
or add a wait for unrelated/large hash builds. If natural readiness cannot be
proven, disable the candidate. Do not add a global barrier or polling timer.

Collect source keys in TableScan after its ordinary filters have accepted rows.
The independent filtered source, rather than a downstream composite hash build,
owns completion. Require that source to have no incoming runtime-filter wait or
prerequisite probe; reject cycles between admitted domains. This makes filtered
dimension keys available before unrelated large build/probe work finishes.

Bind completed membership to TableScan before its first batch. PR1 applies it to
eager scan output before shuffle/routing. Existing static/residual predicates
continue to execute with their established error and NULL semantics. Later reader
integration uses ReaderFilterRequest/ReadWithFilter; it is a separate enabling PR.

## One domain, one physical filter

The current private-replica geometry multiplies a globally sized filter by the
number of producers. Replace it with one query-generation-owned CBloomFilter per
domain, sized using the admitted global build estimate and the existing 1%
target. Keep the existing 10% source-cardinality / probe-NDV admission threshold and
disable controls. A bounded filtered source may be considered even when its
estimated ratio exceeds 10%: the completed source's actual PK row count makes
the final profitability decision for each target. Unknown or invalid NDV uses
PASS. The estimate still sizes the one filter; its budget is never enlarged.
If the observed source count already fails every target's ratio, stop optional
construction and publish PASS early, restoring ordinary scan overlap. Partial
counts never authorize membership rejection or an empty-domain result.

Reserve the sum of all admitted filters, coordinator state and actual bounded
probe scratch before native allocation, within the execution-resource budget and
64 MiB combined query cap. No resize, second cache or user knob is introduced.
Underestimated cardinality can reduce effectiveness but must not introduce false
negatives. The actual allocated backing, not the nominal bit count, is charged.

Source scan workers add already evaluated keys to the common filter in bounded vector
chunks under the coordinator mutex. Native writes are never concurrent. Check
cancellation between chunks; do not hold this mutex across RPC, channel waits or
downstream execution. This serialization is a measured cost, not assumed free.
Only completed membership is read, with reusable scratch; no per-row CGo calls.

State transitions:

| State | Event | Result |
|---|---|---|
| candidate | topology/admission fails | disabled; ordinary execution |
| admitted | first valid preparation | allocate once and register actual workers |
| building | producer adds keys | bounded writes; not visible to probes |
| building | all registered workers finish | seal immutable membership or proven empty domain |
| building | optional refusal/spill/unsupported state | PASS; release after workers stop |
| any live state | real build error/cancel | propagate primary outcome and unblock consumers |
| sealed/PASS/failed | scopes stop/reset | close generation, free filter/scratch/reservation once |

DROP requires complete successful evidence of no joinable keys. Duplicate
completion is idempotent; inconsistent identities are errors. Compile owns final
cleanup after builders and consumers stop, before closing allocation accounts.
Close/cancel notification must not depend on a blocked producer or consumer.

## Diagnostics and cost model

Retain per-domain admission/rejection reason, estimated/actual build rows, producer
completion, construction time and bytes, rows tested/rejected, and downstream
shuffle/probe rows. Render these in the native diagnostics actually collected by
the harness; storing an unrendered ExtraStat is insufficient. Counters add per
batch, not atomics per row. Do not log keys or high-cardinality values.

Construction is O(build rows), membership O(probe rows times hashes), with one
native filter plus bounded scratch. Expected benefit is fewer payload rows copied,
shuffled and probed. Key scanning and block decompression may remain unchanged.
No scan-byte or speedup claim follows merely from rejecting rows.

## Alternatives and delivery

The status quo is cheap but retains the measured excess work. The old optional
shuffle-only filter is safe within its envelope but acts too late and misses
composite/reused cases. Broadcasting exact IN keys is cardinality-limited; a
partition-local filter is unsafe before routing without a separate partition
proof. The selected complete-domain scan binding provides a necessary predicate
without a join-order rewrite or cross-CN fan-in protocol. Remove the superseded
shuffle-only producer/consumer hook in #32098 rather than running both.

Deterministic UTs cover lineage, duplicate/NULL/empty domains, extra composite
conditions, aliasing, unsupported predicates, worker identities, partial build,
spill, budget refusal, cancellation and reset. Tiny SQL/BVT oracles compare the
unchanged query with an optimization-disabled control; native work diagnostics
prove actual scan delivery separately from correctness.

Q9 is the predeclared primary workload. Require attributable row/work reduction
and a negative upper 95% paired wall-time bound at SF10 and SF100 memory profiles.
All 22 queries and Total must meet the agreed regression gate in memory and disk
profiles. Cold full suites are excluded; five independent process pairs; identical
settings; Holm query checks with raw intervals published; separate Total check;
no SF100 spills. Benchmark scripts execute/drain and collect resources, never
verify datasets or reference results. No current speedup is claimed.

Accepted boundaries: single-CN binding first; same-type integer necessary domains;
one shared filter with bounded serialized construction; ordinary fallback; no
logical join rewrite or new cache policy. A material deviation requires review of
the affected design before implementation/delivery.

## Admission amendment review

The live SF10 Q9 plan estimates filtered PART at 400,000 / 2,000,000 rows
(20%), so the former estimate-only 10% gate prevented activation. The requester
approved using completed source cardinality while retaining the 10% NDV ratio,
64 MiB query cap, unchanged estimates/placement and the full Q9/all22 acceptance
protocol. Single-column PK source rows conservatively bound distinct build keys.
Each probe checks actual completed rows against its own known finite NDV; an
unprofitable or invalid target uses PASS. Early PASS is allowed when every
target is already unprofitable; it never establishes absence. Source workers
still execute ordinary query work and propagate their errors.

Design review: PASS before implementation of the amendment. The source remains
independent, partial membership remains invisible, memory geometry is unchanged,
and final gain/regression evidence is still required. Capture estimate/observed
rows, filter bytes, completion and per-target PASS reasons in physical stats.

## Eager-stage profitability envelope

Earlier eager-domain revisions showed a confirmed Q9 gain alongside a confirmed
Q10 regression. Q10 already performs selective return-flag filtering in the scan;
post-filter membership cannot eliminate that string-comparison work and adds
source construction/synchronization. A bounded coalescing experiment reduced
native calls but still failed the unchanged end-to-end gate; its buffering is
removed and the failed evidence is retained.

This eager-scan stage admits only targets without existing row, block or runtime
scan filters. Existing filtered reader paths keep ordinary execution and overlap.
The separately planned reader integration can compose membership before their
predicates/payload construction. This is a structural consumer-cost boundary,
with no query/table-specific names and no acceptance-rule change. Single-source
geometry, complete-domain correctness, memory cap, estimates and join placement
are unchanged. Refinement review: PASS before this narrower admission is enabled.
