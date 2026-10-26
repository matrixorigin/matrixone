# Reuse identical text subjects in static OR/LIKE filters

## Problem and contract

A filter such as `LOWER(CONCAT(IFNULL(input, ''), ' ', IFNULL(output, ''))) LIKE '%error%' OR ... LIKE '%timeout%' OR ... LIKE '%retry%'` evaluates the same string expression for every LIKE leaf. The execution engine currently evaluates these ordinary OR children eagerly. The change computes a narrowly admitted identical subject once per OR invocation while preserving every LIKE operation, their order, and the existing SQL result.

Every admitted LIKE must receive the same bytes, NULL state, length, type, collation and provenance it would receive from independent evaluation under that invocation's batches and selection mask. The first original demand computes the subject. Sharing must not move evaluation across OR invocations, CASE arms, batches, masks, processes or query executions.

The optimization changes no SQL, Unicode folding, LIKE semantics, planner metadata, wire/storage format, index, spill default or runtime configuration. It is deliberately narrower than general common-subexpression elimination.

## Admission and work bounds

Only fresh static filter roots owned by Filter or TableScan are eligible. Generic expression construction, projections, joins, runtime filters, diagnostic-owned constant filters, existing memo/deferred executors and all-constant subjects retain their existing paths.

An admitted subtree contains registered eager OR nodes and two to eight LIKE leaves with non-NULL scalar string-literal patterns. Each subject must be a string function tree with a structurally proven row-dependent path, and all subjects must be fully identical typed protobuf expressions. Validation checks both plan and executor shape. The pure-function allowlist is LOWER, CONCAT, registered string COALESCE, and exact `CASE/IFF(ISNULL(column), string_literal, same_column)` null substitution. The latter admits the binder's IFNULL representation and its verified string flow-control metadata. Casts, variables, parameters, UDFs, unsupported domains, hidden literal sources and effects are rejected.

String COALESCE identities are checked against their actual registered CHAR, VARCHAR or TEXT argument signature, including retained CHAR identities that the current binder canonicalizes to VARCHAR. Declaration IDs are not encoded registry positions. A column in an unneeded COALESCE tail does not prove row dependence: only the first argument after explicit NULL literals can supply that proof. LOWER and CONCAT propagate any child proof because their existing folder requires every argument to fold. This preserves lazy constant folding without evaluating constants during admission; a constant function that returns NULL may conservatively retain the generic path.

Discovery permits at most 512 node visits per root, including failed attempts and revisits. Each subject occurrence permits at most 64 nodes and depth 16; each candidate permits at most 4096 total literal bytes and each subject identity at most 16 KiB. Full typed equality follows validation of these bounds. Exhausting the discovery budget leaves the entire root unchanged. No global cache, hash table or per-row identity check is added.

## First-invocation policy

Each operator owns one admission-completed boolean. An empty or terminal batch returned before filtering does not consume the decision. On the first nonempty filter invocation the operator closes the decision window before evaluating any root. Immediately before each static root's first Eval, it attempts reuse only if the current remaining batch has at least 1024 rows. Earlier runtime or static filters may have reduced that row count.

Small or unreached roots remain generic for the remainder of that executor lifetime, even if later batches are larger. An error closes the same decision window; any memo owner already installed before Eval remains installed. Reset preserves the decision and surviving static executors; Free and fresh construction establish a new lifetime. Both the owning static slot and the corresponding borrowed combined slot are replaced before Eval. TableScan's normal and synchronous reader-filter paths use the same admission point.

The threshold avoids charging expression discovery to common tiny filters. It is a conservative policy, not a universal speedup guarantee: short large batches can still incur setup cost, while long small batches may intentionally miss reuse. A nested selection can also reduce useful work below the outer batch size.

## Ownership, masks and error handling

Discovery records non-overlapping OR owners without mutation. It constructs all replacement wrappers and references before committing changes. Each group retains its first subject executor and frees the other fresh duplicates. One OR-local memo state holds a fixed reference count; the last reference's Free releases the retained executor. Combined operator slices borrow these roots and do not free them.

The OR-local wrapper clears its cached vector reference on every Eval entry, including EvalWithoutResultReusing. Only a successful subject evaluation populates it. Its scope is the individual OR, so separate CASE masks never share cached values. Reset traverses the original expression and retained subject once; errors and pipeline cleanup retain a complete owner graph. The cache borrows an existing result vector rather than creating a second owner.

No new goroutine, synchronization, asynchronous callback, retry or blocking cleanup is introduced. Existing operator serial evaluation remains required. Discovery state is bounded and temporary; retained memo state is bounded by the admitted expression and released with its owner. Input string/vector sizes and concurrent-query memory remain subject to the engine's existing resource management.

## Validation and limits

Typed tests cover admitted and rejected domains/shapes, identity boundaries, CASE masks, NULL/Unicode values, deterministic errors, reset/reuse, budget fallback, alias replacement and balanced cleanup. Actual Filter.Call and TableScan reader-filter consumers cover 1023/1024 boundaries, empty input, runtime/static shrink, small-to-large batches, unreached/error roots, Reset and Free. Separate public SQL checks cover prepared reuse and native result metadata. Generic construction is unchanged.

Performance acceptance uses a clean baseline and candidate built with the same source base, compiler, native library and flags; paired A/B/A runtime samples use identical original tables, SQL and hard resource limits. Full result oracles, tiny-query controls, neighboring queries, profile admission and actual memory/OOM/swap/spill counters accompany latency and CPU measurements. Controlled real-data results are reported separately from historical cross-host benchmark totals. A selected-query 100M run cannot replace the historical full-suite total.

The design and implementation were reviewed before the paired runtime campaign. Evidence and machine-specific scripts are retained outside the source change. Successful tested cases establish their measured behavior; they do not establish universal regression freedom or whole-engine OOM immunity.
