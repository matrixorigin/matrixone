# Bounded group-key materialization optimization

Status: **APPROVED for implementation, design v1.2, 2026-10-25**.

Design gate: **gpt-6.1-sol / xhigh**, parent-selected gate agent
`/root/clickbench_design_20261025`. Reviewed MatrixOne revision:
`dae02bbaaaa18aac7fa9b788e42f4483b3adf091`, clean isolated worktree
`/private/tmp/mo-clickbench-systematic-20261025`. DuckDB source comparator:
`cc9648d736fd7e4fb2fc813cba6f6d34932c3386`, clean
`/Users/xupeng/github/duckdb`. This is design approval, not implementation or
performance acceptance. The delivery owner retains implementation, QA, and final
independent model review responsibility.

Version this content before production edits at
`docs/design/group_key_materialization.md` (a design-only commit is sufficient).
The complete change affects shared planner/executor hot paths and memory
publication, so use the mo-dev hot-path design gate. The task record is the owning
work item; add the eventual implementation PR link to the versioned document.

Tracking: Related to https://github.com/matrixorigin/matrixone/issues/27665.
Implementation PR: pending validation and publication.

## Problem and evidence

The goal is a general reduction of redundant group-key retention and string-copy
work, preserving SQL semantics and bounded admission. Query identities, table
names, constants, and ClickBench schema must never enter product guards.

E02 screened all 43 original ClickBench queries on identical official first-1M
rows and schema, at four threads. This sample covers one date and is not the full
100M benchmark. The parent subsequently built the exact local DuckDB source and
replaced the wheel reference: Q40 209/18.5 ms, Q34 96.4/13.6 ms, Q37 74.7/7.82 ms,
Q24 98/13.4 ms (MO/DuckDB). The persistent DuckDB CLI includes a constant marker
query, so its reported times include that conservative overhead. Retain raw
artifacts and exact configuration. MO statement 4 GiB plus Go soft limit 3 GiB
and DuckDB engine 4 GiB are not equivalent total-process memory caps.

The existing CPU profiles were collected with a concurrent compiler. They prove
CPU shape, not accepted elapsed latency. Q34 memmove is 51.4% of samples; 72.7% of
its callers are `buildVarlenaNoInline`, 22.5% are mpool replacement copies. Q40
memmove is 47.5%; `BuildVarlenaFromVarlena` is 33.7% cumulative and
`preflightBuildChunk` 29.1%. The source explains two independent opportunities:

1. `appendAggNode` in `query_builder.go` appends a full physical equality tuple
   when any logical key needs PAD SPACE normalization. In Q40 five SQL keys
   become ten retained keys, although three numeric keys and URL are unchanged.
   Only the CASE-derived key needs a separate representation. The hash tuple has
   five components; the other copies are retained, propagated, spilled, and
   copied without contributing a new equality domain.
2. `Vector.unionBatch` uses the existing contiguous varlena bulk-copy helper only
   for `flags == nil`. Group publication always supplies insertion flags. Even
   an all-ones selection therefore performs per-row payload copies. Q34 has only
   URL as its group key, proving that opportunity 1 alone cannot address its cost.

`mpool.GrowCapacity` already amortizes growth (roughly 1.25x at large sizes).
Exact-growth diagnosis is rejected. Do not change global growth or accounting.
Q24's wide TopN/scan cost is a control and a separate investigation, not an
aggregation performance claim. Q29's backslash/backreference mismatch was a
comparison artifact, resolved with NO_BACKSLASH_ESCAPES and `$1`; integer AVG
format/scale and valid TopN ties/unordered outputs need comparison-aware oracles.

## Contract, first owners, and bounds

The planner first owns logical versus physical equality representation. The
Vector first owns physical bytes and its preflight proof. Group owns the
preview/admit/commit transition and retained batches; it must not gain another
storage owner.

The following invariants are mandatory:

- Logical group columns remain the identical visible prefix, with identical
  positions, types, collation/PAD SPACE metadata, values, and provenance.
- Every logical equality component uses either its unchanged visible value or
  its existing canonical expression. A transformed comparison key never replaces
  the visible representation. Keep every logical component; this change does
  not infer functional dependencies or deduplicate across different ordinals.
- Explicit hash-key positions are valid, ordered, unique positions in retained
  GroupBy. Empty retains the existing all-columns meaning. Existing FD proofs,
  grouping-set guards, shuffle routing, and DISTINCT semantics remain valid.
- For valid admitted immutable source vectors, all-selected preflighted union is
  semantically equivalent to the same contiguous logical range with nil flags,
  including NULL/grouping bits and all row metadata.
- Preflight reserves all backing allocations before Group hash/key/aggregate
  publication. Publication consumes the proof once and performs no allocation.
  It copies only the exact live payload admitted for this selection.
- Each retained allocation has one existing account/owner and normal cleanup.
  No borrowed source reference, new cache, queue, heap, goroutine, or retained
  per-batch copy plan is introduced.

The negations are wrong groups/outputs, changed exposed types, invalid positional
references, a new charge after preflight, copying dead/unselected payload, or a
nonzero account after cleanup. These have direct test oracles below.

## Approved implementation A: compact physical key retention

Keep `buildPadSpacePhysicalKeyList`'s **full tuple** contract. UNION/INTERSECT/
MINUS and prepared set-operation rebuilding depend on it; do not make that list
sparse or alter its wire format.

At conversion to aggregate retention, use one small pure helper taking visible
expressions and an optional full physical equality list. It returns retained
GroupBy and hash positions:

1. Validate equal list lengths when physical keys are present. Do not mutate
   inputs on an error.
2. Preserve the visible prefix. For each ordinal i, compare the complete typed
   physical expression with the visible expression at **that same ordinal**.
   Exact structural equality (the existing protobuf equality facility) permits
   reuse of position i. Append a separately owned/deep-copied physical expression
   only when it differs, and record its new position.
3. If no physical expression differs, keep no hidden suffix and allow the legacy
   nil/all-columns hash representation. Otherwise record the complete N-element
   equality selected-slot subset, including reused visible positions, sorted in
   ascending order for the existing executor grammar. Clone slice backing
   before appending; do not alias/mutate a child or cached-plan slice.

For a root bound ColRef, DISTINCT binding may enrich only the visible expression's
Name/TblName/DbName diagnostic labels. Normalize only those three fields in a
shallow copy of the physical Expr and ColRef before complete protobuf equality.
Keep RelPos/ColPos, complete type/provenance, unknown fields, and all remaining
metadata exact. Never mutate either input. Complex expressions remain exact.
This v1.1 amendment was approved by the same gpt-6.1-sol/xhigh design gate before
its implementation; broad structural/type-loose matching is not approved.

For example, `[a, pad_value, url]` becomes four retained columns and hash
positions `[0,2,3]`, rather than six columns and `[3,4,5]`. Q40 becomes six retained
columns and five equality positions `[0,1,2,4,5]`; its CASE canonical key stays
hidden. Keep visible/hidden expression order; sort only the selected-slot list.
Do not search for equal expressions at other ordinals.

Integrate this same conversion in:

- `appendAggNode`, including extending an existing all-active GroupingFlag only
  for appended hidden keys. The inactive grouping-column guard remains unchanged.
- `rewriteDistinctToAGG`, preserving a visible-prefix projection when hidden keys
  exist. Once converted, retire the consumed DISTINCT-only equality list if no
  AGG consumer needs it; the copied canonical expressions now live in GroupBy.
- The dependent UNION DISTINCT group construction in
  `compile/dependent_kernel.go`. A small exported pure planner helper is
  acceptable to avoid another divergent conversion. Preserve the source set
  node's full equality list and visible output projection.

`distinct_agg.go` already appends a single changed argument key and uses actual
list lengths/explicit positions. It should need no redesign: test mixed compact
group keys with rewritten and generic DISTINCT. Change it only if a concrete
positional assumption is found. Existing consumers (compile, remap, shuffle,
Group/MergeGroup, spill, prepare visitors) already accept arbitrary valid hash
positions; verify that closure, without rewriting them speculatively.

Cost: planning O(number of expressions and their structure); execution retains
N+R columns instead of 2N when R of N keys are transformed. Each removed retained
column eliminates its descriptors/payload and downstream copies per new group.
Q40 removes four retained columns, including one URL payload. Q34 is unchanged
by A. No speedup percentage is promised from source alone.

## Approved implementation B1: reuse existing bulk copy for all-selected union

In `Vector.unionBatch`, after existing const/alias handling, recognize the
preflighted case where `addCnt == cnt`: its 0/1 selection shape has already been
proved. Use `unionBatchContiguousVarlenRange` for that logical row range as well
as the existing nil-flags case. Do not rescan flags just to detect all ones. Do
not change mixed-selection behavior. Preserve original flags for grouping,
prepared-parameter kinds, binary/text row domains, string-source overrides, and
deferred normalization. Reuse the existing serializer/rebase/fallback code.

**Admission detail:** `areaDisjoint` promises distinct live ranges, not that
every byte in `w.area` is live. `SetNull`/`SetNulls` can leave dead payload.
The helper's whole-vector shortcut must therefore not blindly copy all source
area for the new preflighted route. Either pass the exact selected-byte proof
into the private helper or otherwise guard this shortcut. It may use the whole
area only when its byte length equals the admitted live payload. Otherwise use
the existing descriptor-span scan, or fall back before mutation. A borrowed
Window likewise must use its live logical span.

Before mutation, require a successful bulk span to fit the exact admitted byte
count and already available destination area capacity. A preflighted call must
never invoke a growth allocator after discovering a larger span. Reordered,
overlapping, repeated, or gapped source areas use the existing per-row fallback.
NULL headers remain cleared and non-inline offsets are rebased exactly as in
the existing helper. Preserve or conservatively clear area-disjoint metadata;
do not advertise a stronger property after fallback.

Cost: the same O(rows + live bytes), one payload copy and one descriptor-range
copy for eligible contiguous ranges, no added persistent state or heap
allocation. Low-NDV/mixed units remain on their old path. Growth replacement
copies remain, so this is intentionally a bounded improvement.

## Alternatives and explicit scope limits

| Choice | Decision |
|---|---|
| A alone | Sound and useful for mixed physical-key tuples; does not address ordinary URL grouping. Keep independently measurable. |
| B1 existing-helper reuse | Selected: smallest shared correction for a proven missed equivalent path. |
| B2 coalesce mixed selected payload spans | Deferred. First measure B1 on high/moderate/low NDV. Extra serializer/state/scan work needs evidence of exposure and incremental gain plus a design amendment. |
| Reserve guessed full-batch string bytes / larger growth factor | Rejected here: existing amortization works; guesses add memory/admission/spill tradeoffs and do not remove initial row-copy calls. |
| Shared string arena, borrowed retained vectors, hash-table/string-heap redesign | Deferred: changes lifetime, spill, accounting, source pinning and merge ownership; evidence does not justify that scope. |
| Query/column/type-width-specific rules | Rejected. Eligibility follows equivalent representation and proven selection only. |

No new setting/flag, SQL syntax, account class, persistent format, transport
field, telemetry cardinality, or distributed protocol is required. Old plans
remain valid and mixed versions already understand explicit hash positions.
Rollback is a normal code revert. Memory can decrease; allocation-account
ceilings, temporary peak admission, recovery floors and spill policy remain.

Group keeps its immutable preview -> capacity preflight -> commit order. On a
pre-publication rejection, the resident prefix may spill and retry the same
offset, with no duplicate insertion. Cancellation, errors after publication,
Reset, Free, and prepared reuse use their existing owners. B1 must not turn a
recoverable capacity rejection into a post-commit allocation failure. No new
wait or service lifecycle is involved.

## Risk-to-validation map

Reuse existing fixtures, extend coherent suites, and register resource cleanup
immediately. Keep functional fixtures small; performance data belongs in the
benchmark harness. Each row below proves a distinct boundary.

| Risk | Cheapest sufficient evidence |
|---|---|
| Wrong equality position/type or changed visible tuple | Planner typed UT for mixed numeric/plain variable-string/promoted-CHAR keys, transformed first/middle/last, multiple transformed keys, no transformation, all-active flags, inactive GROUPING SET/ROLLUP control. Assert prefix types/positions and canonical functions, not plan text. Include shape rejection and no input mutation. |
| DISTINCT conversion diverges | Typed SQL planner cases for SELECT DISTINCT mixed keys, COUNT DISTINCT with compact group keys, sibling aggregate/generic DISTINCT, tuple DISTINCT, derived/CTE paths. Extend dependent UNION equality-key UT with mixed identity/canonical keys; preserve ordinary set-list shape and prepared rebuilding controls. |
| Prepared mutation/provenance | Reuse cached prepare twice with typed CHAR/VARCHAR arguments; assert template unchanged, types/equality and results equivalent to fresh execution. |
| B1 copies wrong rows/bytes | Extend `unionbatch_fastpath_test.go` / selected-preflight suite: all ones, one zero control, zero rows, inline threshold pairs, long strings, NULLs, stale long descriptor made NULL, nonzero source offset, borrowed Window, reordered/gapped/repeated descriptors. Oracle is explicit original values/NULL/grouping plus UnionOne reference. Assert destination live area length, not values alone. |
| B1 loses row metadata or allocates at commit | All-selected nonempty destination with mixed string sources, prepared kinds, binary/text rows, grouping bits; normal override and deferred-normalization callers. Assert metadata and source immutability. Account snapshots before/after publication must be identical; exact-cap preflight succeeds, insufficient admission rejects before length/area mutation. Free and finalize to zero. |
| Group boundary/retry/reuse | Existing Group account fixtures: all-selected unit split across current/standby batches, partial unit and duplicate-only control, preflight rejection then spill/retry, Reset/reuse and Free. Assert rows/counts, once-only publication and zero residual account. Reuse applicable existing cases rather than clone them. |
| SQL result/compatibility | Small public SQL case with mixed PAD SPACE/plain keys, NULL/empty/trailing spaces and multiple equal representatives, SELECT DISTINCT, rewritten+generic COUNT DISTINCT, grouping-set control, prepared re-execution. Assert deterministic group counts and exposed types; do not force an unspecified representative among equal padded strings. Validate canonical-key shuffle references with typed consumers; existing wire/transport tests remain reusable. |
| Parallel/spill/resource regression | Public baseline/candidate parity at DOP 1, 2, 4, with no-spill and forced-spill controls. Use existing test-only spill/account seams for narrow deterministic failure boundaries, plus an actual bounded-memory public spill execution. Low/moderate/high NDV and narrow/wide strings are independent performance controls. More CNs are needed only if evidence shows a changed remote/shuffle consumer boundary. |

Derive the final package closure from the complete diff. Expected owners:
`pkg/sql/plan`, `pkg/container/vector`, and `pkg/sql/compile` if its conversion
changes. Group is a directly affected real consumer and its focused account/
spill tests are required even without production changes there. Prove selected
tests are nonempty. Run new/directly affected tests with `-count=1`, owning
packages once normal, incremental gofmt/vet/lint, and affected tests once with
`-race -count=1` as required by issue-to-pr. Use the repository CGo wrapper where
transitive native prerequisites apply. No repeated race stress is justified by
these sequential refinements unless a concrete concurrent failure appears.

## Performance and delivery acceptance

Measure A and B1 separately before combined acceptance. A typed plan must show
Q40's retained-key reduction; B1's pure vector benchmark must show an all-selected
contiguous long-string gain against the old implementation, with mixed/reordered
controls. No permanent counters or diagnostic-only state should be delivered.
Public CPU stacks and timings can prove real exposure; a temporary diagnostic
build is optional only if the source of a missing gain remains uncertain.

After compiler/download/import activity stops, run paired warmed baseline and
candidate measurements on identical data/configuration with a predeclared
bounded count (e.g. 2 warmups, 5 timed runs). Record medians, spread, source/data
fingerprints, result comparison and memory/spill evidence. Use all 43 first-1M
queries as the broad control, then key aggregation queries and neighbor workloads
on the dispersed official 10M sample. The 18-column focused 10M projection cannot
prove original 105-column SELECT-* Q24 behavior; label that scope explicitly.
Keep the exact local-source DuckDB reference separate from old wheel numbers.

Require reproducible improvement in at least one shared targeted public workload
and eligible vector benchmark. Do not keep B1 for a microbenchmark-only win if
realistic controls lose. Flag a candidate median regression exceeding both 5%
and 2 ms for a bounded discriminating rerun; explain environmental causes or fix/
remove the responsible change. Inspect any increased statement peak/spill before
acceptance. Do not claim universal no-regression or full ClickBench parity from
the sampled data. A disproven performance premise means revise scope rather than
add an unreviewed string framework.

Before delivery, reconcile the final diff with this design, capture actual
normal/race/static terminal outcomes, and obtain the separate gpt-6.1-sol/xhigh
overall review. Remaining Q24/other string-growth costs are explicit follow-up
opportunities, not blockers for a measured general improvement.

## Design review record

- Scope: A compact aggregate equality retention plus B1 all-selected existing
  varlena bulk-copy integration; no architecture or growth-policy redesign.
- Trigger: shared execution hot path, planner/executor and admission contract.
- Decision: **PASS for this scope**. No unresolved design blocker.
- Mandatory implementation constraint: exact-live-byte preflight bound for B1;
  `areaDisjoint` alone is insufficient.
- Deferred: B2 mixed-span copies and string-storage redesign require evidence and
  separate approval. Implementation/performance/QA evidence remains pending.

### A2: keep the executor's sorted subset grammar

The mixed tuple initially emitted positions in logical ordinal order. Real Q40
execution rejected [0,1,2,5,4]: Group and MergeGroup require strictly increasing
positions. Sort only the newly constructed physical-key position list before
returning it. This list is a selected set of retained GroupBy slots, not a
logical-to-physical ordinal map. Keep the visible prefix and hidden expressions
unchanged. Every local, merge, spill/reload and transport consumer uses these
same positions. DISTINCT appends positions after the existing retained prefix,
so its extension remains sorted. Preserve existing strict-subset validation.

Approved by gpt-6.1-sol/xhigh on 2026-10-25 after tracing those consumers. Test
complete selected-key membership and ascending indexes, then execute Q40 and
mixed-key DISTINCT/COUNT DISTINCT through real service and spill paths.

### Deliberate boundary

Normal set-operation mergeDistinctSetScopes still retains its complete physical
tuple after nullable-domain relaxation. It remains correct and is outside this
measured ClickBench aggregation change; no claim is made that every UNION,
INTERSECT or MINUS factory benefits. B2 sparse-span coalescing is also deferred.

## Implementation validation (2026-10-25)

Environment: Apple M4, 10 CPUs, 16 GiB; Go 1.27.1, native macOS arm64,
production GOEXPERIMENT=simd build, four execution threads. Main baseline is
`dae02bbaaaa18aac7fa9b788e42f4483b3adf091`; DuckDB is the local source-built
`cc9648d736fd7e4fb2fc813cba6f6d34932c3386`. Both engines use the official
ClickBench schema and the same persisted input values. The full suite uses the
first 1M rows and all 105 columns. The 10M fixture projects 18 needed columns
from dispersed original row groups (0,10,...,220 plus the tail); it has 2,608,215
URL values and contains no CounterID=62, so Q37–Q43 there are empty controls.
Positive Q40 evidence comes from the full-width 1M fixture.

| Measurement | Main | Candidate | Interpretation |
|---|---:|---:|---|
| Q40, 1M, resident, ABBA paired median | 184.65 ms | 151.49 ms | 18.0% lower elapsed time |
| Q40 retained columns / EXPLAIN aggregate memory | 10 / 324.18 MiB | 6 / 250.81 MiB | Same 5 physical equality axes, less retained payload; memory is not RSS |
| Q40, 1M, 1 MiB spill threshold, DOP4 | 4.199 s | 3.229 s | 23.1% lower elapsed time; actual SpillRows observed |
| Q34, 10M, automatic spill, ABBA median | 2.041 s | 1.960 s | Modest 4% difference; not claimed as a robust latency gain |
| Q34, 10M, explicit 4 GiB aggregate budget | 863.52 ms | 863.77 ms | Essentially unchanged; no spill in the recorded plan |
| Vector all-selected, 256 × 128-byte values | 2252 ns | 907.4 ns | 2.48× throughput in existing preflight+publish cycle |
| Same vector benchmark, every other row selected | 1271 ns | 1244 ns | Sparse fallback control retained |

Paired Q40 and Q38 use main/candidate/candidate/main process blocks with a
warmup and 3 timed samples per block. Paired Q34 uses 2 warmups and 5 timed
samples per block. Microbenchmarks use 3 × 300 ms. Q38's first-screen regression
did not repeat: paired medians are 30.65/29.91 ms. These are local observations,
not portable guarantees. The source-built DuckDB remains faster: Q40 1M is
18.50 ms, and Q34 10M at a 4 GiB engine limit is 160.36 ms. MO statement limits,
operator spill thresholds, Go's 3 GiB soft limit and DuckDB's engine budget are
different scopes; they are not equivalent total-process memory caps. Automatic
MO Q34 spills at roughly 512 MiB, unlike the explicit 4 GiB control. No budget
or growth policy was changed to obtain the optimization.

All 43 original queries execute on the candidate. Q29 uses
NO_BACKSLASH_ESCAPES and the engine's `$1` replacement syntax. Ordinary and
candidate rows agree after handling SQL's unordered results and legitimate TopN
ties; Q12/Q19/Q31/Q33/Q40 additionally agree with main and source-built DuckDB
under deterministic tie-break ordering (AVG normalized to four decimal places).
The existing 5-row BVT extension passed normal mo-tester comparison twice,
35/35 statements each, with teardown checked. Public controls additionally cover
DOP1/2/4, NDV1/64/65536, string width8/128, NULL/PAD SPACE, prepared reuse,
ROLLUP, scalar/tuple DISTINCT, automatic and forced spill. Full owning-package
normal UT and relevant owning/consumer race checks pass. Incremental gofmt,
vet, repository molint and configured golangci-lint pass on vector, group,
compile, planner and directly affected explain packages.
