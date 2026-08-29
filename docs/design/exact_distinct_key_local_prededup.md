# Exact DISTINCT-Key Parallel Aggregation With Local Pre-Deduplication

Status: proposed MVP design; implementation evidence recorded 2026-08-29

Owner issue: [#27720](https://github.com/matrixorigin/matrixone/issues/27720)

Related bounded-memory work:
[#27698](https://github.com/matrixorigin/matrixone/issues/27698)

Implementation PR:
[#27790](https://github.com/matrixorigin/matrixone/pull/27790)

## Problem and invariant

MatrixOne already rewrites a single unary `COUNT(DISTINCT d)` into ordinary
grouping:

```text
input -> Group by (g..., d) -> COUNT(d) by g...
```

Without a shuffle, every scan worker first deduplicates its own pairs and a
MergeGroup later reconciles equal pairs from different workers. That preserves
exactness and reduces duplicates before exchange, but a global or few-group
query can leave the cross-worker pair merge with too few physical owners.

The required correctness invariant is:

```text
Every equal (g, d) pair reaches one final pair owner before COUNT(d), while
worker-local duplicates are removed before network exchange.
```

Routing by unary `d` is safe: equal `(g,d)` pairs necessarily have equal `d`.
Hash collisions only co-locate unequal pairs; the receiving Group remains the
authoritative equality check.

## Cost boundary

Let:

- `N` be input rows;
- `D` be global distinct `(g,d)` pairs; and
- `L` be the sum of worker-local distinct pair counts.

Then `D <= L <= N`. A raw-row shuffle always exchanges `N` rows, while the
existing local Group can reduce that to `L`. `N` and `D` do not determine `L`:
the same global statistics can describe worker-local duplicates (`L` near `D`)
or cross-worker duplicates/nearly unique input (`L` near `N`). Therefore this
MVP does not force raw-row exchange or pretend that the missing distribution
dimension can be recovered from table NDV.

## Selected topology

For the eligible shape, build:

```text
input
  -> local Group by (g..., d)
  -> hash shuffle surviving pairs by d
  -> final Group by (g..., d)
  -> local COUNT(d) by g...
  -> MergeGroup fixed COUNT state
```

The planner marks the first pair Group local while it chooses shuffle strategy,
so ordinary high-cardinality heuristics cannot move the exchange ahead of it.
The second pair Group is explicitly marked to shuffle its already-deduplicated
input by `d`.

At compilation, the serialized plan shape carries the physical proof: the
shuffled, aggregate-free final pair Group references every output key of its
aggregate-free child pair Group in the same order. Only this exact redundant
`Group(X) -> shuffled Group(X)` shape may compile the child as one Group per
input scope without a MergeGroup. The parent exchange then consumes those
local outputs directly and completes global pair ownership. Limits, offsets,
filters, non-identity projections, grouping sets, aggregate states,
missing/reordered keys, and a non-shuffled parent all reject the local-only
compile path.

The compiler already removes a normal one-bucket Group shuffle when DOP is one
and only one stage owner exists, so DOP=1 retains the non-shuffle execution.

## Alternatives and decision

- **Complete final-group ownership (Path A).** Shuffling by a high-NDV final
  group key is preferable when it exposes at least 64 complete final owners;
  the existing planner keeps that path, and Path B is its low-owner complement.
- **Shuffle raw rows by `d`.** This exposes DISTINCT-key owners but exchanges
  all `N` input rows. It was rejected because a worker may already hold many
  duplicates; the local Group is an existing spillable reduction that can make
  the exchange proportional to `L` instead.
- **Keep the cross-worker MergeGroup.** This exchanges local pair outputs but
  leaves global/few-group exact state behind one completion owner. It remains
  the fallback when statistics or key support are insufficient, but does not
  solve the target owner bottleneck.
- **Sample `L` and choose adaptively at runtime.** This would distinguish
  worker-local from cross-worker duplicate placement, but requires a new
  observation, decision, and topology-switch protocol. It is the principled
  follow-up for the cost-model limitation, not an implicit claim of this MVP.
- **Canonical packed `(g...,d)` ownership.** It generalizes to multi-argument
  DISTINCT but adds serialization/type/version work that unary `d` does not
  require for correctness. Equal pairs already imply equal `d`, so the ordinary
  receiving Group remains the equality authority in this phase.

The selected path reuses existing Group and shuffle operators, adds no stateful
service or wire format, and preserves conservative fallback whenever its
compile-time proof or statistics are incomplete.

## Eligibility and fallbacks

Path B is considered only when:

- every grouping flag is active;
- the aggregate list contains exactly one unary `COUNT(DISTINCT d)`;
- `d` is a direct column with an existing hash-shuffle type: signed or unsigned
  16/32/64-bit integer, CHAR, VARCHAR, or TEXT;
- selected DISTINCT NDV passes the existing exact-state shuffle cost test; and
- every active final grouping key has a reliable NDV estimate, and the highest
  of those estimates is below the existing 64-owner threshold, or the
  aggregate is global.

Missing/unreliable statistics, grouping sets, expression/tuple/multi-argument
keys, unsupported types, small DISTINCT state, and 64+ final owners keep their
existing physical topology. The established logical rewrite for single
`COUNT(DISTINCT)` and `SUM(DISTINCT)` remains; only eligible COUNT gains the
local-pre-dedup Path B.

Mixed aggregate lists remain unchanged. Materializing SUM/AVG per pair can
change checked-overflow order, while MIN/MAX or many ordinary states can amplify
retained/spilled state by `D * partial-state-width`. Those shapes need a separate
accumulator and cost design rather than an implicit extension of this MVP.

## Ownership and compatibility

Both pair stages are ordinary spillable Group operators and use existing
`SpillMem`, allocation-account, Reset, Free, serialization, and cancellation
ownership. No new executor, aggregate state, function ID, protobuf field,
protocol generation, file, goroutine, queue, lock, or persistent format is
introduced.

The planner-local markers are consumed before serialization; ordinary
`HashMapStats` records the final pair exchange, and the exact adjacent Group
shape proves that its child is a redundant local reduction. Existing shuffle
compatibility, including versioned string-owner mapping, remains authoritative.

NULL `d` values are locally reduced, routed to one final owner, retained as one
`(g,NULL)` pair, and excluded by final `COUNT(d)`. Empty global input returns
zero through the existing rewrite.

## Validation and acceptance

Typed planner coverage must prove:

- the exact `local pair Group -> shuffled final pair Group` hierarchy;
- the first Group cannot shuffle even with stale/high ordinary shuffle state;
- the second Group shuffles its child Group output by `d` and exposes multiple
  owners when DOP permits;
- DOP=1 relies on the existing one-owner compile fallback;
- global/few-group selection and the 64-owner, small/missing-NDV, low-ratio,
  grouping-set, expression, unsupported-type, SUM(DISTINCT), and mixed controls;
- the worker-local-duplicate counter-shape exchanges local pair output, not raw
  scan rows; and
- outward bindings and exact COUNT semantics are unchanged.

Typed compiler coverage must additionally pass four independent input scopes
through the recognized local pair Group, observe four local Group outputs with
no MergeGroup scope, and then observe four final pair owners after the parent
shuffle. A composite final group key with one known-low NDV and one unavailable
NDV is the conservative-selection counterexample and must retain Path A.

Before merge, execution evidence must compare the old topology and this one for:

- global and few-group queries;
- worker-local, cross-worker, and nearly unique distributions;
- integer and string keys, NULL, skew, spill and non-spill;
- DOP=1 and multi-DOP, one CN and multi-CN; and
- the balanced 64+ final-group control.

Report exact-oracle results, physical owner count, local surviving pairs,
shuffle rows/bytes and skew, CPU, allocations, peak accounted memory, spill
activity, and wall time. Acceptance requires no DOP=1 or balanced-control
regression and a demonstrated benefit on the intended global/few-group
multi-owner workloads.

## Recorded execution evidence

The branch-built service was run as the repository's two-CN launch topology
(32 cores per CN, 64 aggregate owners). Persisted test tables contained 200,000
rows. `table_stats(..., 'patch', ...)` supplied a 2,000,000-row, 1,024-block
cost shape so multi-CN placement and Path B were selected naturally; no
`execType` or shuffle hint was used. Changing only the patched final-group NDV
from 4 to 128 selected Path A on the same SQL and persisted data, providing the
pre-change topology control.

Public SQL exact-result checks produced:

| Case | Result |
|---|---:|
| global integer / string / near-unique DISTINCT | 9,500 / 10,000 / 200,000 |
| four groups, integer DISTINCT | 2,000 / 2,500 / 2,500 / 2,500 |
| four groups, string DISTINCT | 2,500 per group |
| skewed 90% / 10% groups | 9,000 / 500 |
| empty global input | 0 |
| 128-group fallback | `SUM(group counts) = COUNT(DISTINCT (g,d))` |

The 128-group query retained the established two-Aggregate Path A plan. A
composite `GROUP BY` with NDV 4 for its first key and unavailable NDV for its
second key also retained Path A. Global integer and few-group string queries
both produced the three-Aggregate local/shuffle/final Path B plan.

For the intended worker-local-duplicate case, each `(g,d)` pair occurred about
100 times. Both paths returned group counts `501, 500, 500, 500`, equal to an
independent `SELECT DISTINCT g,d` oracle.

| Two-CN metric | Path B | Path A control |
|---|---:|---:|
| rows entering pair shuffle | 2,097 | 200,000 |
| bytes entering pair shuffle | 24.57 KiB | 2.29 MiB |
| final pair rows | 2,001 | 2,001 |
| final owners / rows per owner | 64 / 12..68 | 64 / 12..68 |
| statement wall time | 33.53 ms | 35.18 ms |
| summed active time | 293.99 ms | 399.01 ms |
| summed CPU time | 296.51 ms | 399.64 ms |
| summed wait time | 719.39 ms | 863.68 ms |
| max accounted domain memory | 427.61 KiB | 7.21 MiB |
| summed operator memory | 28,141,280 B | 27,212,328 B |
| spill bytes | 0 | 0 |

Thus the target shape reduced exchange volume by 95x, CPU by 26%, and peak
accounted memory by 94%. The extra local stage slightly increased cumulative
operator-memory accounting while sharply reducing simultaneous peak state.

With about five local copies per pair, Path B reduced shuffle rows from 200,000
to 40,080 and max accounted domain memory from 5.84 MiB to 3.19 MiB. With
`agg_spill_mem=256`, the same exact-result query reported 924,948 spill bytes,
3.31 MiB peak accounted memory, 2.90 s summed CPU, and 77.58 ms statement wall
time. Its non-spill control reported zero spill bytes, 3.19 MiB peak, 447.09 ms
summed CPU, and 30.10 ms wall time.

The low-local-reduction case emitted 190,025 rows from 200,000 inputs before
shuffle; the fully near-unique case emitted all 200,000. Both remained exact.
The measured near-unique Path B wall time was 42.57 ms versus 45.79 ms for its
Path A control, but it achieved no row reduction, so the documented absence of
an `L` estimate remains a real cost-model limitation rather than a claimed
universal win.

On one CN at DOP 1, the physical one-bucket exchange was removed. Path B's two
pair Groups consumed 8 ms and 2 ms with 4.47 MiB maximum per Group, versus 11 ms
and 4.47 MiB for the Path A pair Group; exact results were unchanged. A one-CN
multi-DOP run and the two-CN run above both showed worker Groups before stream
fan-in and shuffle, with no pre-shuffle MergeGroup reconciliation.

Canonical multi-column ownership, runtime sampling, raw-row near-unique
fast paths, and broader adaptive topology selection remain follow-up work.
