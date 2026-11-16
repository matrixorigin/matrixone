# Reuse qualifying grouped measures

Owner: #30857. Approved by the requester on 2026-11-16 as independent PR 3.
Base: f70c37b16e. This does not reopen deferred Q15 producer sharing.

## Contract and design

Reuse a qualifying grouped SUM for a later SUM over the identical fact-row bag:
same table/snapshot, filters, argument, key and bound semantics after alias
remapping. Start with one direct integer key and one exact integer/decimal SUM.
Reject DISTINCT, floating arithmetic, changed filters, shared/barrier nodes and
insufficient totality/overflow proofs. Statistics cannot prove bag equality.

Prove intervening dimension joins preserve at most one row per group using
declared SQL-compatible primary keys. Convert key membership into an INNER join
that carries the existing qualifying measure; remove the repeated fact join/scan
and replace the final grouping with projection only after proving one surviving
row per semantic grouping key. Preserve HAVING, missing-dimension filtering,
NULL/all-NULL results, output precision, ordering/LIMIT and snapshot identity.

With finite positive estimates, require removed fact-scan bytes to exceed added
qualified-key/measure transport. Construct the complete replacement before
mutation and remap references/types/names with existing infrastructure. Honor
aggregate-pushdown controls. Recalculate statistics/distribution normally.
The existing qualifier is the only measure owner: no shared cache/materialized
producer, new aggregate state, API or persistence protocol is introduced.

## Validation

Test bag/filter/type mismatches, duplicate/unproven dimension keys, orphan and
NULL keys, missing dimensions, NULL quantities, decimal domains and HAVING
boundaries. Independent SQL checks preserve rows/types and ordering/LIMIT.
Verify unchanged Q18 eliminates its second fact scan and repeated measure work.
Benchmark this branch alone against main, local SF10 memory/disk and SF100 disk,
one process/configuration with excluded cold plus five warm full suites. Report
all 22 queries and Total with wall/instructions/allocations/copies and no spill,
swap or script data/result verification. Fewer scans do not imply a 2x speedup.

R2: bag identity, join multiplicity and expression remapping require functional
and public-path closure. Existing aggregate/operator resource ownership remains
authoritative. Confirmed regressions require diagnosis and a reviewed revision.

## Approved regression revision — 2026-11-16

The requester approved the four proposed fixes with “then fix”.

Preserve the original dimension-join tree and predicate placement. Prune the unique repeated fact leaf only when its sole ordinary key connector is implied by the retained qualifying join and no other region predicate references the retired fact. Convert the existing SEMI edge to INNER, expose the SUM, and retain the original sibling subtree. Generate no self-equalities or Cartesian dimension reconstruction. Verify unchanged surviving scan predicates and normal qualifier parallelism before timing.

## Approved qualification-order revision — 2026-11-16

The requester approved this revision with “Implement the plan”. Keep qualification
as SEMI through normal join ordering and pushdown. Run measure reuse immediately
after `pushdownSemiAntiJoins` and `removeImpliedSemiJoins`, then recalculate
statistics only when it changes the plan. Require the qualifying edge's probe to
be a direct surviving dimension scan owning the probe key; reject a probe spanning
joins, a probe on the retired fact, or disabled SEMI pushdown. Retain all existing
semantic, ownership, totality, uniqueness and cost checks before mutation.

Carry the existing SUM at that lowered edge, prune the sole redundant fact
connector, and preserve the other dimension joins and predicates. Remaining
normal operand orientation, distribution and runtime-filter passes stay in use.
No new planner pass, executor mechanism, public interface, cache or producer.

The observed regression materialized unqualified dimension-join rows before
applying the grouped qualification. Loaded-schema operator diagnostics must show
early qualification, bounded dimension materialization, one fact scan and normal
qualifier parallelism. A runtime-filter tag or estimated cardinality is not proof
that filtering occurred. Main's repeated scan is already runtime-filtered: do not
credit the entire base-table size or forecast a proportional speedup. Add focused
lowered-probe/fallback and full-planner ordering assertions, retain independent
SQL oracles, then run the existing local cold-excluded/five-warm all22 protocol.
