# Costed LIKE-prefix access through regular indexes

Issue: #31867. Implementation: PR #31940.

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
A prefix range can omit duplicate prefix evaluation only when its typed upper
bound equals the raw-byte prefix successor. This is a value proof, including
NUL and multibyte values, rather than a character whitelist. Unicode-successor
gaps retain the exact predicate. The shared residual owner supplies both
candidate costing and the actual index filter; complete mixed LIKE predicates
remain executable.
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
The shared regular-index scorer accounts for index input, actual decoding and
output, PK transfer, base blocks and post-filter row work. For K lookup rows and
an independently pruned base input budget R, its conservative dispersed-key
estimate is `min(R, max(K, ceil(K)*BlockMaxRows))`. It saturates before multiplying
to avoid overflow. Physical PK input is included exactly once; an unsorted or
fake PK cannot borrow the ordered-PK block-search estimate. Base residuals and
join materialization remain charged on filtered rows.

PK filter capacity applies only to backfill candidates, after index-side
residuals. Covering scans build no such filter and are compared with the base
scan without this absolute cap. Matching owns semantic legality; candidate cost
and capacity have one owner in the scorer. Unknown prepared bounds retain the
existing uncertainty comparison rather than using a neutral ranking estimate
as a capacity bound. FORCE, full-primary-key access and spatial policy retain
their established responsibilities.

The new prefix candidate must beat the base scan even on small tables; LIMIT
cannot reduce work before required residuals. Broad prefixes lose by their real
estimated work. The sample upper bound can conservatively reject useful
noncovering paths; this tradeoff avoids charging a dispersed backfill as if it
read only its result rows. It is not a guarantee against every possible cache,
layout or stale-statistics performance change.

A row-frequency estimate is not layout evidence. The shared scorer discounts
base input using physical key ordering or fully observed object layouts;
missing/high-overlap layouts do not turn heuristic row selectivity into skipped
blocks. Unsorted prefixes do not discount base input from their sampled row
frequency. Independently useful pruning is preserved, and executable block
filters are not removed by the scorer's conservative input estimate. Their
column identity is resolved through canonical table metadata. Object overlap
excludes self-overlap and cannot prove block clustering: for O observed objects
its planning fraction is at least `min(1, Overlap + 1/O)`. Combine object-level
predicates within that group, then clamp it to at least `1/O` before combining
independent sorted-block pruning. This uses average object sizes, not a hard
bound for skewed layouts. Unknown/partial object or column observations give no such
credit; an object sampling ratio of one alone does not establish column coverage. If static pruning is explicitly disabled, both the base
and hidden-key input are charged as full scans; per-key search after a block
read does not restore block pruning.

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
fixed capacity, truncation boundaries, malformed metadata, shape-specific
capacity, block-input saturation and PK ordering, and encoded-range
membership against independent raw-byte classification. Generation tests cover
copy ownership, partial collection, patching, and transient views. Performance
validation compares selective and broad distributions with the same extrema,
including a non-covering payload workload, without forced access hints.

Optional backfill residuals share admission between construction and costing.
They require one direct column and literal, parameter or literal-list/vector
bounds; deterministic computed bounds are insufficient because they may throw
on an encoded-range false positive. Cross-column, volatile, diagnostic and LIKE
residuals remain with the base scan. Published layout evidence must be nonempty
and valid for the scan view; snapshots, transient workspace estimates and NULL
frequency cannot borrow non-NULL object-layout credit.

Range backfill preserves the first supported ordinary candidate and the first
supported logical-prefix candidate, without enumerating every predicate.
They compete through the existing scorer, so rejecting either cannot hide the
other. Expanding the historical choice requires a lazy once-per-scan proof of
the complete filter list using the existing total-predicate contract. Dynamic
parameters/variables, folded-literal provenance and unproved error/diagnostic
expressions keep the historical choice. This protects observable evaluation
behavior while allowing selective literal IN alongside a broad prefix. Native
non-UCA VARCHAR IN additionally admits a checked literal vector through its
byte-hash evaluation contract; SQL equality identity and physical index-key
compatibility remain separate proofs. Dynamic or encoded vector provenance
cannot bypass the common fixed-input check.

Literal LIKE-to-equality rewrites refresh their former LIKE frequency before
filter sorting only in linear, single-scan statements with proved stable filter
evaluation. Multi-step or branched plans retain the historical estimate because
repricing another branch can change observable error evaluation order.
