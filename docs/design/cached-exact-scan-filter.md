# Exact residual filtering before cached-column materialization

Owner: #30850. Implementation: branch `perf/exact-cached-scan-filter`.
This revision implements the coding plan approved in the conversation on 2026-10-29.

## Purpose and scope

Ordinary persisted scans currently materialize owned columns, evaluate predicates
into Boolean vectors, build selections, and repeatedly shrink the batch. Execute
a small exact predicate program inside ObjectIO's existing cache lease instead,
then materialize the surviving physical rows once. Keep cache limits, I/O policy,
SQL plans, storage formats, and output ownership unchanged.

The initial program accepts complete ordered conjunctions of built-in integer,
DATE, and Decimal64 comparisons against resolved literals of the same execution
OID/scale. BETWEEN lowers to two ordered comparisons. Admission never evaluates
another expression, parses a literal again, or changes type coercion. Reject
casts, unresolved parameters/folds, OR, strings/collations, floating point,
volatility, runtime filters, or any unsupported subtree as a whole.

## Interfaces and execution

An immutable `objectio.ExactScanFilter` owns typed scalar bounds and ordered
predicates. The scan compiles it once per Prepare from the bound expression tree.
A single internal `engine.ReaderFilterRequest` carries that program, the existing
owned-batch evaluator, and a metadata-only observation callback. Update callers
and consumers together; do not create a parallel Reader interface or expose
cache-backed vectors through the callback.

The fast read initially requires fixed-width projected columns and an ordinary
committed, non-appendable block,
no physical-rowid output, no Top-K/ordered limit, and a complete admitted program.
All other blocks and in-memory sources use the existing evaluator. Use current
storage selections or current tombstones to establish visible physical rows
before evaluating residual predicates. Retain block-coordinate ordering.

Use the existing column IOVector read and cache-policy/lease owners. A specialized
`ioutil.LoadColumnsDataByExactFilter` operation shares the existing read helper;
it performs no second fetch or speculative cache lookup. ObjectIO binds source
vectors privately, validates them, evaluates admitted predicates on visible rows,
and copies each output column once through existing Vector admission/metadata
owners. Unsupported source representations fall back to owned materialization
using the same already-pinned entries, before any filtered output is published.
Malformed data remains an error. Constant and NULL values retain SQL WHERE truth:
only TRUE survives. No cached source is normalized or mutated.

The predicate program compacts one block-bounded row selection in place between
predicates. It returns owned output plus scalar counts/duration, never borrowed
vectors or cache memory. Comparison kernels dispatch once per predicate/type;
no per-row expression dispatcher, allocation, or indirect callback is added.
Use the existing numeric comparison semantics, including signed Decimal64.
Adjacent inclusive bounds on one flat, non-NULL column with no preselection
execute as a single BETWEEN loop. This avoids an intermediate compaction and
second gather of the lower-bound survivors. Constants, nullable sources and
preselected inputs retain the existing per-predicate execution.

## Ownership, bounds, and observability

IOVector scope owns every cache lease. ObjectIO's bound vector views terminate
before that scope releases. Selection scratch has one effective cleanup owner
on success, empty output, error, cancellation, and reset; do not retain input
references in the prepared program. Per-reader mutable execution scratch reuses
its vector headers and selection; Reset/Free close it, and every call clears all
borrowed views before returning. The program is bounded by its admitted
expression count; row scratch is bounded by the existing object-block row domain.
There is no new cache, worker, queue, wait, or persistent state.

Output vectors retain their destination types, allocation account, metadata and
capacity reuse. Existing cleanup owns partial owned output on failure. The
metadata observer counts visible pre-filter rows and represented input bytes,
records predicate CPU separately from filesystem wait, and prevents the generic
filter from running a second time. Generic execution retains its existing input
accounting. Reset and Free release the request/program with the scan generation.
Reader summaries retain their existing eager path.

## Alternatives and validation

Retaining the current evaluator avoids integration cost but retains temporary
Boolean vectors and repeated materialization/shrink work. Broad read-only vectors
would require every downstream mutator and lifetime to participate. The scoped
exact operation keeps borrowed bytes with their existing owner and gives generic
operators the same owned output contract.

Extend existing ObjectIO, block-read and table-scan tests for exact primitive
boundaries, NULL/constant/empty inputs, visible/deleted rows, selection ordering,
source immutability, malformed storage, allocation failure and reuse. Compare
with the existing generic evaluator using independently expected rows. Keep
unsupported expressions and filtered-row errors on their current path. Add a
small SQL BVT for eligible DATE/Decimal64 predicates and unsupported fallback.

Validate each candidate alone against current main containing #31767, using local
warm SF10, the same effective 16 GiB cache and 32 GiB application budget, cold
calls excluded, and separate latency/profile runs. Benchmark scripts execute and
drain without data/result verification. Report actual materialization work,
allocations, peak memory and Q1-Q22 effects. Confirm slower controls independently;
a confirmed regression blocks delivery. Structural counters and exploratory
profiles under a busy host are exposure evidence, not timing acceptance.
