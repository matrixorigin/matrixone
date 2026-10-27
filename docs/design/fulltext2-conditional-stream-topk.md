# Conditional FULLTEXT2 stream Top-K

Status: design review, revision 1, 2026-10-27. Implementation is blocked until
the separate design review approves this revision. Owner: fulltext optimization
work for issue #31119 and existing draft PR #31407. This is a continuation of
that PR, not a new PR or a change to the benchmark workload.

## Problem and acceptance

The original MS MARCO/DL19 query selects the primary key and MATCH score,
filters on score > 0, orders by score DESC and primary key ASC, and takes 1000
rows. Its plan retains a source-table INNER JOIN. An explicit ORDER BY clears
the existing candidate limit. FULLTEXT2 consequently scores and copies every
matching key into the SQL pipeline before the final Top-K. Existing profiles
show substantial key copying and hash-table work. The preceding row-key and
score-batch optimization reduced the 20-query median sum by approximately 5%;
this does not address the number of candidates crossing the index boundary.

The invariant is exact equivalence to the existing unlimited stream followed
by the retained source join, positive-score filter, and SQL sort/limit. For an
admitted query, ordered keys and float32 score bits must be identical, including
ties at K. For every other query or visibility state, run the existing stream.
No analyzer, BM25 arithmetic, SQL text, index definition, stored data, spill
default, recall target, or relevance metric changes.

Delivery requires deterministic admission/rejection and collector tests, public
SQL counterexamples, owning-package/static checks, cancellation/race evidence,
and a repeatable full-corpus same-resource A/B measurement with identical
results. No speedup is assumed. Failed or inconclusive candidates are excluded
from PR delivery. A historical ES run remains a labelled end-to-end reference,
not a same-token-stream scoring comparison.

## Selected scope

This first step uses the original exhaustive stream scorer with an O(K) heap
before key materialization. It does not change term accumulation order or use
WAND pruning. Existing per-index liveness resolves duplicate keys before the
collector. The heap stores segment/ordinal/float32 references, compares scores
first and primary key bytes on ties, and materializes only the winners.

Planner admission is deliberately narrow:

- One FULLTEXT2 BM25 bag-of-words MATCH stream, source primary key and score
  projection only, and the existing simple source-PK INNER JOIN.
- Exactly ORDER BY that score DESC, source PK ASC (INTERNAL means ASC).
- No remaining source predicates, join predicates beyond PK equality, other
  MATCH streams, SQL_CALC_FOUND_ROWS, windows or aggregation affecting this
  pagination. The only retained score predicate is exactly score > 0, optionally
  with the lossless float32-to-float64 cast on score and an exact zero constant.
- A single non-null VARCHAR PK with byte ordering: persisted charset identities
  0 through 3 at collation revision 0. This includes the current default
  CharsetUTF8 identity; its SQL ordering and VARCHAR key equality retain raw
  bytes despite its general-ci name. Unicode/UCA collations,
  composite or hidden PKs, alternate types, partitioned sources, publication or
  named-snapshot reads are excluded initially.
- No INCLUDE-column output or INCLUDE predicates. Pattern parameters may be
  used, but LIMIT and OFFSET must be nonnegative literals with overflow-checked
  1 <= LIMIT + OFFSET <= 8192. The original offset and limit remain outside.

These are reusable relational/typing conditions, not dataset names, token
strings, row counts or special query IDs. A miss leaves the plan unchanged.

## Visibility proof and runtime fallback

Early Top-K cannot precede a join that may reject candidates: deleting the
highest-ranked key could otherwise yield too few results. Keep the source join
and prove that it cannot reject an admitted candidate for this read.

The proof is evaluated at execution, for the actual cache generation pinned by
VectorIndexCache.Search's existing shared lock. Planning-time freshness is not
sufficient. No source relation or transaction proof is retained across queries.

1. The current query uses an ordinary, non-mirror, non-snapshot transaction with
   a nonnil read-only workspace. A worker reconstructed without the coordinator's
   workspace cannot prove absence of local writes and must fall back. Current
   transaction identity and full snapshot timestamp must be available.
2. The loaded index records an ephemeral full load snapshot L. Record it only
   when Load used an ordinary non-mirror transaction, its workspace was read-only
   before and after loading, and its full snapshot timestamp stayed unchanged.
   Unknown or writer-loaded generations are ineligible. This state is immutable
   after publication and needs no new persistent format or cache key.
3. Require a successfully captured generation, no CDC-tail chunks in that
   generation (loadedTail == -1, not merely zero tail segments), positive known
   build watermark B, and L <= the current full read snapshot S. No-tail limits
   initial rollout and test scope; it does not replace the snapshot proof because
   MERGE can incorporate older unprovenanced tail frames.
4. Resolve the same source relation by trusted table identity and account under
   the current transaction. Require the existing SourceCommitTSProvider and no
   current-transaction source writes. Its exact source-change bar C, including
   retention rules, must be nonempty and C <= types.BuildTS(B, 0). A greater C,
   logical timestamp beyond B, unavailable capability, or lookup/coverage error
   causes unlimited fallback. The provider may use B as its early-exit hint.
5. Require B < S.PhysicalTime, conservatively rejecting ambiguous/future build
   boundaries. Recheck the current read-only/transaction identity before
   accepting the proof. Cancellation returns the cancellation error, never a
   fresh attempt on the unlimited path.

Reasoning: the recorded build watermark covers source changes through B; C <= B
means the source has no later visible changes. L <= S, a read-only load, and the
causal source-commit-before-index-publication order prevent index keys from
future or private writes from entering this read. Existing liveness leaves at
most one visible version per key. Therefore the retained PK join does not discard
eligible candidates. Old/missing provenance, async updates/deletes, transaction
local writes, mirror workers and newer warm-cache generations fail closed.

The implementation must check source ID/name/account consistency, avoid resetting
shared relation handles, and reuse existing relation/provider contracts. The
optional query-local proof callback may run under the cache search lock; it must
only read source state, honor the query context and never recurse into an index
search or eviction. It owns no long-lived relation or transaction resources.

## Protocol and ownership

Attach an optional conditional Top-K descriptor to the existing FULLTEXT2
ScanOptions JSON, with a versioned semantic name and a bounded literal K. Keep
IndexSearchScan.CandidateLimit nil. Old readers ignore the new JSON field and
stream all rows; the retained SQL operators still produce the full result.
New readers receiving old plans also stream. No MORPC, catalog, storage or
configuration version change is needed. Descriptor corruption/unsupported
values must never truncate output; decline or follow the existing decode error
contract. Use the existing plugin construction boundary, not a new algorithm
dispatch site or generalized plugin framework.

The reader retains its existing streaming goroutine, bounded channel and Emit
ownership contract. Its derived cancellation context must reach the collector.
The collector is query-local, holds at most 8192 fixed-size references, and is
destroyed when Search returns. Segment references remain inside the existing
cache read lock; only copied output batches cross Emit. Existing consumers own
successfully emitted pooled key buffers; failed Emit recycles them in the
producer. Cancellation or collection failure emits no partial successful Top-K.
No new goroutine, background refresh, cache, global mutable state, file or retry.

Check cancellation at least once per 8192 visited candidate documents, including
documents rejected by membership/score checks, and at segment/end boundaries.
Checking only when a winning row is emitted is insufficient for an exhaustive
scan. Use the existing sink stopped/error propagation to release iterators,
filters, the cache lock and reader Close without waiting for the full search.
Generic stream cleanup must continue to return any producer-owned pooled buffer
on an error before flush.

Cost: O(M) original scoring plus O(M log K) worst-case heap work, typically O(1)
for losers; O(K) retained references (roughly 192 KiB at 24 bytes/reference),
plus existing output-batch memory. Do not copy PKs for losing rows. Variable key
bytes materialized for winners have the same existing batch limits as the old
stream. The source-proof overhead is paid only for the narrow admitted shape;
measure it on small and no-match queries as well as the full corpus. Keep the
old hot path free of proof/heap work when no descriptor is present.

## Alternatives and tradeoffs

1. Status quo: simplest, correct fallback, but transports all matches through SQL.
2. Existing WAND Top-K directly: attractive pruning, but current pruning and heap
   use score-only ties and may change float accumulation. It also cannot safely
   truncate ahead of source visibility filtering. Deferred until separately
   proven; this design does not promise to close the entire ES gap.
3. Exact source-key membership before ranking: safe with a complete bitmap/set,
   but reading/materializing all source keys adds large work and memory.
4. Adaptive refill after source join: can preserve correctness, but requires a
   continuation/generation protocol; fixed overfetch multiples are not exact.

Bounded Top-K collection is a standard ranked-retrieval technique (for example,
Lucene's TopScoreDocCollector). SQL's explicit secondary key and MO's async index
visibility are additional requirements here; Lucene's collector is precedent,
not an interoperability contract or permission to change MO semantics.

## Validation map and delivery

- Planner: positive public binding path and nearest controls (different order,
  residual filter, second MATCH, casts, UCA, LIMIT/OFFSET/overflow, found-rows,
  unsupported identities). Inspect actual scan descriptor and retained operators.
- Collector: independent exhaustive-sort oracle for K=1/boundary/more-than-N,
  multiple segments, equal scores at cutoff, prefix/non-ASCII/binary VARCHAR
  keys, zero/negative/NaN scores, and original float bits. No scorer rewrite.
- Freshness: stale/newer loaded generation, logical TS boundary, unknown/missing
  metadata, tail/delete-only-tail, local/mirror/snapshot/writer-loaded workspace,
  provider error and cancellation; every rejected state uses unlimited output.
- Public SQL: original BM25 shape vs an explicitly non-admitted unlimited oracle;
  same transaction DELETE, prepared reuse after write, committed delete while
  index lags, multi-segment ties, offset, empty/no-match, small K and close/cancel.
  Distinguish original stale-index behavior from new early-truncation defects.
- Lifecycle: deterministic cancel/Emit failure with bounded heap and buffer
  ownership; focused race tests cover immutable cached metadata and concurrent
  query-local collectors. No production failpoints or test-only knobs.
- Real evidence: full 8,841,823 passage corpus, original 20 fixed DL19 queries and
  original index/SQL, matched binaries/config/data/resource budget; warmups plus
  repeated timed A/B and order/float-bit comparison. Capture actual emitted
  candidate reduction, CPU, peak memory, OOM/swap/spill and small-query overhead.
  Source build and benchmarks do not run concurrently on the same CPU budget.

Rollback is reverting the optional collector/descriptor changes in PR #31407;
existing persisted indexes remain readable. The PR stays Draft until evidence
and review are complete. No tail/distributed/Unicode acceleration is claimed.
Any expansion of these guards requires an amended design and review first.
