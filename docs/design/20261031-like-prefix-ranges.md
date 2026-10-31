# Costed LIKE-prefix access through regular indexes

## Problem and ownership

The planner rewrites literal LIKE prefixes to `prefix_eq`, but regular-index
selection does not accept that string-content predicate. A tuple-field prefix
is a different representation: string terminators and embedded NUL escaping
prevent using `serial(prefix)` as a content-prefix probe.

The optimizer derives a typed half-open range `[prefix, successor)` only when
constructing a regular-index candidate. It reuses `in_range`, existing domain
validation, and serialized `prefix_in_range` lowering. Execution, storage,
index metadata, and runtime filters retain their existing owners.

## Semantic boundary

Only direct, visible logical string columns in a byte-ordered, non-CHAR,
non-Unicode-collation domain qualify. Default-escape parsing extracts a literal
prefix. Explicit ESCAPE, parameters or folded literals retaining their source,
computed subjects, invalid text patterns, and absent finite successors retain
the existing predicate. Text successors advance Unicode scalars; binary
successors advance bytes.

Pure trailing-percent patterns can use the equivalent `prefix_eq` predicate.
Mixed patterns retain the complete LIKE predicate and its necessary prefix;
the correlated predicates do not count as independent selectivity estimates.
Exact residual evaluation remains after a potentially lossy encoded lookup.
Only a range on the first index part is introduced here.

## Generic distribution statistics

Min/max and NDV cannot distinguish a rare prefix from a common prefix with the
same extrema. Existing manual ANALYZE already scans snapshot-visible values;
that scan collects an optional byte-range sample without additional readers,
background tasks, or a second statistics cache.

For C eligible selected columns, each receives `min(2048, floor(16384/C))`
reservoir slots. Zero slots disable collection. A slot retains a 128-bit
priority, a fixed row identity, at most 64 value bytes, a length, and a NULL bit.
Priorities hash the row identity with a separate domain from row admission.
Bottom-k selection includes NULL and duplicate rows and is independent of
column values and traversal order. Priority hashing occurs once per retained
row per column pass. The total slot budget applies across all passes.

Publish only when every persisted block was selected. Partial block sampling
clusters rows and cannot support the row-level uncertainty model. Full-row
coverage with no reservoir loss is marked as a census. The additive statistics
payload includes the schema version, column sequence, type and collation
identity, exact population, sample size including NULL, census flag, and sorted
non-NULL byte prefixes. Repeated and empty values retain their mass.

For half-open bounds of at most 64 bytes, truncation preserves membership:
equality with the lower bound remains included and equality with the upper
bound remains excluded. This contract does not extend to a strict lower or
inclusive upper bound. If m out of n sampled rows match, a census uses m/n;
a non-census uses `min(1, m/n + 2*sqrt(log(200)/(2*n)))`. This conservative
sampling margin assumes uniform row sampling under the hash model. It is not
a guarantee against deterministic-seed correlation, stale statistics, or future
writes. NULL rows belong to n and never to m.

## Planning and lifetime

The planner validates population, bounded counts, sort order, domain, schema,
and column identity before using a distribution. Populations beyond exact
floating-point integer representation are unknown. Missing or invalid data
rejects the new automatic prefix candidate. FORCE retains existing semantics.

Candidate work uses the sampled upper range fraction before suffix residuals.
The existing cost model charges index processing, extraction, PK transfer,
and backfill as applicable. The new candidate must beat the base scan even on
small tables; existing selectivity and cardinality caps also apply. An exactly
30% measured range satisfies the inclusive selectivity cap. LIMIT cannot
reduce candidate work before required residuals.

A row-frequency estimate is not a block selectivity estimate. Unsorted prefix
costing preserves independently useful block pruning. Prefix block filters
remain executable even when the row-frequency estimate is one.

GlobalStats publishes one complete analyzed generation. Samples are deeply
copied and uncollected columns do not inherit old distributions. Metadata-only
refresh, partition merging, transient workspace estimates, historical snapshots,
and manual statistic patches discard the sample. Patching a sampled generation
copies before mutation so existing readers retain their owner. No catalog
migration or new execution protocol is required. Sample values remain internal
statistics and must not appear in SQL output, EXPLAIN, or logs.

Committed-data statistics can become stale until the existing refresh path
replaces them. There is no new per-DML invalidation mechanism; exact residuals
preserve results regardless of the estimate. Unanalyzed tables intentionally
retain their conservative access choice.

## Validation contracts

Regression coverage includes public SQL collection and automatic selection,
the original 500-row 30% case, scan-result controls, forced-path semantic checks,
memory and persisted rows, escapes, multibyte text, NUL, NULL, LIMIT, and prepared
parameter reuse. Pure tests cover sample frequency, traversal independence,
fixed capacity, truncation boundaries, malformed metadata, and encoded-range
membership against independent raw-byte classification. Generation tests cover
copy ownership, partial collection, patching, and transient views. Performance
validation compares selective and broad distributions with the same extrema,
including a non-covering payload workload, without forced access hints.
