# Exact DISTINCT-Key Parallel Aggregation With Local Pre-Deduplication

Status: proposed MVP design (2026-08-28)

Owner issue: [#27720](https://github.com/matrixorigin/matrixone/issues/27720)

Related bounded-memory work:
[#27698](https://github.com/matrixorigin/matrixone/issues/27698)

Implementation branch: `codex/distinct-key-local-prededup`

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

The first pair Group is explicitly marked local so ordinary high-cardinality
shuffle heuristics cannot move the exchange ahead of it. The second pair Group
is explicitly marked to shuffle its already-deduplicated input by `d`.

The compiler already removes a normal one-bucket Group shuffle when DOP is one
and only one stage owner exists, so DOP=1 retains the non-shuffle execution.

## Eligibility and fallbacks

Path B is considered only when:

- every grouping flag is active;
- the aggregate list contains exactly one unary `COUNT(DISTINCT d)`;
- `d` is a direct column with an existing hash-shuffle type: signed or unsigned
  16/32/64-bit integer, CHAR, VARCHAR, or TEXT;
- selected DISTINCT NDV passes the existing exact-state shuffle cost test; and
- the highest usable final-group-key NDV is below the existing 64-owner
  threshold, or the aggregate is global.

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
`HashMapStats` records the physical result. Existing shuffle compatibility,
including versioned string-owner mapping, remains authoritative.

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

Canonical multi-column ownership, runtime sampling, raw-row near-unique
fast paths, and broader adaptive topology selection remain follow-up work.
