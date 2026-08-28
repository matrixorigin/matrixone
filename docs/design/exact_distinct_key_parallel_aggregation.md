# Exact DISTINCT-Key Parallel Aggregation

Status: proposed single-aggregate MVP; independent approval is tracked in
implementation PR #27762 (2026-08-28)

Owner issue: [#27720](https://github.com/matrixorigin/matrixone/issues/27720)

Related bounded-memory work:
[#27698](https://github.com/matrixorigin/matrixone/issues/27698)

Implementation PR:
[#27762](https://github.com/matrixorigin/matrixone/pull/27762)

## Problem and invariant

The established single-aggregate rewrite converts unary `COUNT(DISTINCT d)`
into two ordinary grouping stages:

```text
input -> Group by (g..., d) -> COUNT(d) by g...
```

It removes saved-argument DISTINCT state, but a global aggregate or a query
with only a few final groups can still expose too few physical owners. The
required invariant is:

```text
For every active final group g and DISTINCT value d, all equal (g, d) rows are
deduplicated by one owner before the final COUNT is computed.
```

For the unary MVP, routing by `d` is a safe coarsening: equal `(g, d)` pairs
necessarily have equal `d`. Hash collisions only co-locate unequal pairs; the
receiving Group remains the authoritative equality check.

## Selected MVP

When the existing logical rewrite is applied to a single
`COUNT(DISTINCT d)`, mark its synthesized `(g..., d)` Group to hash-shuffle raw
rows by `d`. The ordinary Group then deduplicates exact pairs, and the existing
outer `COUNT(d)` produces final results.

Select this physical ownership only when:

- every grouping flag is active;
- the aggregate list contains exactly one unary `COUNT(DISTINCT d)`;
- `d` is a direct column with a supported hash-shuffle type: signed or unsigned
  16/32/64-bit integer, CHAR, VARCHAR, or TEXT;
- selected DISTINCT NDV passes the existing exact-state shuffle cost test; and
- the highest usable final-group-key NDV is below the existing 64-owner
  threshold, or the aggregate is global.

Missing or unreliable statistics, grouping sets, expressions, tuples,
multi-argument DISTINCT, unsupported key types, small DISTINCT state, and 64+
final owners retain their current physical topology. The established logical
rewrite for single `COUNT(DISTINCT)` and `SUM(DISTINCT)` remains unconditional;
only eligible COUNT receives forced `d` ownership.

## Deliberate boundary

This PR is one narrow phase of #27720 and does not close that issue. It does
not decompose mixed aggregate lists. In particular, these remain unchanged:

```sql
SELECT COUNT(DISTINCT d), SUM(x) FROM t;
SELECT COUNT(DISTINCT d), AVG(x), MIN(payload) FROM t;
```

Materializing ordinary SUM or AVG per `(g, d)` can change checked-overflow
behavior because partial regrouping changes the addition order. Materializing
MIN/MAX or many ordinary states per pair can also grow work and retained/spilled
state by `NDV(d) * partial-state-width`. Supporting mixed global/few-group
queries therefore requires a separately approved accumulator semantic and cost
model; it is not hidden inside this MVP.

Canonical multi-column distribution, multiple DISTINCT argument sets, local
adaptive pre-deduplication, transported hash reuse, runtime skew adaptation,
and topology observability also remain follow-up work. #27693 remains the
preferred complete-group-owner path for eligible balanced mixed aggregates.

## Ownership, memory, and lifecycle

The eligible path instantiates no saved-argument DISTINCT `argSkl` and no
per-key ordinary aggregate states. The inner ordinary Group owns one exact
`(g..., d)` key per surviving pair and uses its existing `SpillMem` contract,
which can partition those independent pair groups by their complete group hash.
The outer COUNT state is fixed-width.

No new aggregate executor, function ID, wire field, protocol generation,
goroutine, file format, queue, lock, or cleanup state is introduced. Existing
Group, shuffle, vector, allocation-account, spill, Reset, and Free ownership
remain authoritative.

Fallback plans are unchanged. Unsupported DISTINCT shapes can still retain one
saved-argument state whose memory grows with NDV; #27698 owns bounded-memory
completion for those paths. This MVP makes no universal bounded-memory claim
for #27720 or #27698.

For `d IS NULL`, the pair stage retains `(g, NULL)` and final `COUNT(d)` excludes
it. Empty global input returns zero, matching the existing logical rewrite.

## Planner and compatibility contract

The synthesized pair aggregate is marked only inside `QueryBuilder`.
`determineShuffleForGroupBy` consumes that marker and writes the ordinary
`HashMapStats` shuffle decision. No protobuf or remote-pipeline capability is
added, so rolling upgrades use the existing hash-shuffle compatibility rules,
including the current string-owner mapping selected for each execution.

## Validation and acceptance

Deterministic unit coverage must prove:

- global and few-group high-NDV COUNT choose DISTINCT-key ownership;
- the 64-owner boundary retains the existing topology;
- small/missing NDV, grouping sets, expressions, unsupported keys, and
  `SUM(DISTINCT)` do not force the new ownership;
- mixed SUM/AVG/wide MIN shapes are not rewritten or mutated;
- the outward aggregate binding and exact COUNT result contract remain
  unchanged; and
- NULL, duplicates, empty input, hash collisions, DOP=1, multi-DOP, spill, and
  one-/multi-CN execution match an independent exact oracle.

The performance matrix must compare global and few-group high-NDV COUNT at
DOP=1 and multi-DOP, while retaining a balanced 64+ group control. Report CPU,
allocations, peak accounted memory, shuffle rows/bytes and skew, spill activity,
and wall time. A single-process microbenchmark cannot substitute for proving
real multi-owner execution.

Acceptance is exact result equivalence plus demonstrated multi-owner execution
and benefit for the eligible global/few-group shapes without regressing the
balanced control. Broader adaptive and mixed-aggregate support remains
explicit follow-up work.
