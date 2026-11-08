# Correlated composite-primary-key point pruning

Issue: [#32365](https://github.com/matrixorigin/matrixone/issues/32365)
Implementation: [#32377](https://github.com/matrixorigin/matrixone/pull/32377)
Related 4.2 implementation: [#32371](https://github.com/matrixorigin/matrixone/pull/32371)

## Problem and contract

For a composite key `(tenant, session, request)`, a query may request a few
correlated identities:

```sql
WHERE tenant = ? AND
      ((session = ? AND request = ?) OR (session = ? AND request = ?))
```

On main at `d7185c9a92`, both SQL-prepared and binary-prepared executions
returned correct results, but the first form scanned 100,001 rows in the
persisted test table. The comparable factored single-session `IN` form read
only the two matching rows. A 50-identity prepared UPDATE showed the same
access-path problem. This is an optimization issue, not a demonstrated wrong
result.

For original scan predicate `F` and derived point predicate `C`, a candidate is
legal only when **every row where `F` is SQL TRUE also satisfies `C`**. It is a
necessary condition, not a replacement for `F`. Pruning must not suppress or
introduce observable errors, warnings, or effects, and must preserve parameter
identity, SQL NULL behavior, duplicate handling, transaction semantics, and
access control. SQL equality with NULL is unknown, not true; MySQL's documented
NULL comparison behavior is relevant to MatrixOne's MySQL interface
([MySQL 8.0 manual](https://dev.mysql.com/doc/refman/8.0/en/working-with-null.html)).
Physical tuple equality is only usable when the resolved SQL equality domain
has the same relevant semantics. In particular, a residual cannot recover a
SQL-equal row excluded by an incorrectly encoded point.

The goal is to avoid broad prefix scans for bounded, provable sets of complete
correlated primary-key points, including prepared SELECT and DML. The original
predicate and normal fallback remain authoritative. General OR index union,
Cartesian expansion of independent ORs, new tuple-lookup storage APIs, and
changing secondary-index or join costing are not goals.

## Alternatives and ownership

1. **Keep the existing prefix scan and ask users to factor queries.** It has no
   new planning cost and is correct, but does not solve cross-session batches.
2. **Normalize all ORs into a general union or publish points before costing.**
   This could cover more expressions, but needs a broader cost model and can
   change join, vector-index, covering-index, and limit decisions. Independent
   OR expansion can also grow combinatorially. That work is outside this fix.
3. **Capture a bounded proposal before composite-key lowering and publish it
   after access selection if its proof survives.** This retains the existing
   optimizer, encoding and reader owners while covering the reported workload.
   It adds transient proof, planning work, and value-dependent replanning; the
   tradeoffs and limits below are part of the decision.

The existing scan `FilterList` owns SQL semantics; `BlockFilterList` is derived
pruning state. Existing composite-key lowering owns encoding, existing access
selection owns index decisions, and `readutil` owns PK filter selection and
vector cleanup. The query planner owns only a short-lived proposal and shared
admission budget. See the preceding
[secondary-index and composite-key design](20261024-secondary-index-primary-prefix.md)
for the existing early/late lowering boundary. No catalog, on-disk, wire, or
client API is added.

## Analysis and publication

After scan predicate pushdown and before ordinary composite-key lowering, walk
eligible scan predicates and collect facts on the physical primary-key parts.
Each selected OR arm must, together with inherited conjunctions, imply one
complete tuple for the same scan. Select one independently sufficient OR; do
not multiply separate ORs. Incomplete arms, unsupported expressions, malformed
metadata, absent proof, or exhausted budget leave the established access path
in place. Keep original parameter ordinals and literal provenance.

Accept only exact, total and effect-free equality values and comparison domains
whose serialization is a safe necessary condition. Reuse the existing resolved
comparison/collation checks and total-predicate proof; do not treat type IDs
alone as a proof. CHAR padding, native Unicode comparisons, floating-point
signed zero and unproven conversions fall back. Additional residuals must also
be total and effect-free because early pruning may otherwise skip their
observable evaluation. A copied implicit TEXT-to-VARCHAR parameter cast is
eligible only when its ordinary overload and current bytes establish that it
cannot truncate or warn; explicit or unproved casts fall back.

The proposal stays outside the plan while early vector selection, join/cost
passes and normal secondary-index selection run. It records the source scan
identity, relation/snapshot and key metadata, candidate, and exact serialized
predicate witnesses including parameter ordinal and literal source. Before an
existing late composite-key lowering, validate the old witnesses; only an
already valid proposal can take new witnesses after that owner performs its
semantics-preserving rewrite. Before publication, require the same surviving
scan, metadata and witnesses, and prove any added conjunct is safe to skip.
An untracked scan copy, replacement, changed witness, or failed proof falls
back; do not infer validity from table identity alone. The original logical and
physical predicates remain. Publication adds the existing hidden-key `IN`
filter after access decisions, with the existing block-filter rules and hint.

The reader may prefer exact `IN` over a simultaneous prefix for `AND` only when
all valid points are inside the prefix. It keeps the existing merge fallback
and vector ownership/cleanup; the full SQL predicate still filters rows.
Text EXPLAIN displays a redacted serialized list as `IN (<opaque>)`; complete
JSON formatting follows the existing JSON contract.

## Prepared execution and lifetime

Initial PREPARE without execution bindings cannot establish actual value sizes
and falls back. During execution planning, direct string parameters and the
allowed transport cast use the current process parameter vector—the bytes read
by the executor—not declared width or a potentially different user-variable
representation. A bound NULL remains subject to SQL three-valued semantics.
Each occurrence is charged, including shared parameters.

First reject shapes that can never form a safe complete candidate without
reading execution values. For an otherwise eligible candidate, both admission
and value-dependent rejection mark the existing prepared-binding state as
value dependent. The frontend therefore does not retain a type-only plan that
would hide a later smaller binding after an oversized one. Existing unrelated
value-dependency flags are not cleared. Retry uses its existing execution
binding snapshot; candidate analysis adds no new retry, transaction or durable
state.

The proposal and witnesses are query-planning-local. If planning errors or is
cancelled, their Go-owned memory becomes unreachable with the planning attempt;
no background work or separate close protocol is introduced. Existing planning
cancellation checkpoints remain authoritative. Each pure analysis walk is
bounded as described below rather than creating a new context or deadline.

## Resource and performance policy

The query shares an 8 MiB **admission charge** across its steps and scans. It
accounts for actual string bytes with worst-case escaping, tuple and expression
copies, witness and metadata sizes, and the existing serial executor's minimum
packer and capacity-growth bound, with a four-copy allowance. The traversal
limits are 2,048 visited nodes, depth 256, and 128 tuples per candidate. The
budget is conservative and monotonic for one planning attempt: rejection or a
later discarded proposal need not refund previously consumed charge. It is not
a hard limit on existing SQL plan memory, executor memory, concurrent queries,
or distributed reader/CN fan-out. Reject on overflow or insufficient charge;
do not allocate based on a declared string width as though it bounded runtime
parameter bytes. No candidate is required for correctness.

Candidate construction is proportional to the admitted expression/tuple size;
witness matching is bounded by the admitted filters and can be quadratic in
filter count. Additional binding, exact witness serialization, candidate copies
and value-dependent replanning cost CPU and allocations. No-OR and ineligible
paths should remain cheap, and ineligible shapes must retain normal prepared
plan-cache reuse. Additional performance changes must preserve the same proof,
resource charge, and binder/error contracts rather than replacing them with an
unverified shortcut.

On one Ryzen 9 7900X host, Go 1.27.1, one local CN and a persisted 100,001-row
table, three fresh-cluster samples of ten warmed binary prepared COUNTs over 50
scattered points had medians 615.7 ms baseline and 71.3 ms with the change.
Scan input was 100,001 versus 50 rows, while touched blocks were 14 versus 13;
this is not a proportional block-I/O improvement or a multi-CN latency claim.
A separate 50-point execution-bound planner benchmark measured approximately
1.36 to 1.55 ms, 831 to 1,034 KB, and 16,209 to 18,276 allocations per plan.
A race test was active during the local measurements; they motivate scrutiny of
planning overhead, not a general performance guarantee.

## Compatibility, rollout and validation

There is no persisted metadata or protocol change and no new runtime setting.
Shutdown upgrades and restarts need no migration. The optimizer may choose a
new derived scan filter and EXPLAIN output; user SQL, results, warnings and
errors must remain compatible. Unsupported cases fall back automatically. A
full operational rollback requires reverting the optimizer change and restarting
with that build; there is no claim of a runtime kill switch. Existing EXPLAIN
and scan statistics are the observability path. No parameter values are newly
logged, and existing authorization/tenant boundaries remain in force.

Acceptance evidence must cover:

- Correlated/shared facts, incomplete and sibling ORs, duplicates, NULLs,
  unsafe residuals, exact witnesses, relation/snapshot changes, scan copies,
  late lowering, and all resource limits with fail-open behavior.
- SQL and binary prepared reuse with changed bindings, over-budget then small
  values, warnings/errors, rollback and committed DML; retry must retain the
  same binding snapshot.
- CHAR padding, Unicode overrides, float signed zero, binary/NUL, decimal and
  temporal controls; nullable outer joins, forced/ignored secondary indexes,
  early IVF paths, and block-filter-disabled behavior.
- Reader IN/prefix operand orders, partial overlap, vector lifetime, cached
  searches and cleanup; normal and race tests for affected owners.
- The real persisted 2- and 50-point workloads, scan rows and blocks, planner
  overhead and incremental static checks. Compare performance on matched inputs
  and report setup, sampling and limits rather than promising general speedups.

Remaining limitation: execution fan-out and multi-CN performance were not
bounded or benchmarked by the local one-CN admission charge and measurements.
A separate change is needed if a hard cluster-wide memory limit or a broader
cost-based access strategy becomes a requirement.
