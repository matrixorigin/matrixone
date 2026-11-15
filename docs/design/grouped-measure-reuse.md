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
