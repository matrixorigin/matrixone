# FULLTEXT2 parallel segment search, parallel build, token_filter analyzer and high-df term cutoff

- Status: Review required
- Tracking issue: [#30402](https://github.com/matrixorigin/matrixone/issues/30402)
- Implementation PR: [#30618](https://github.com/matrixorigin/matrixone/pull/30618)
- Design revision: 6
- Last updated: 2026-10-24

## Problem and intent

A FULLTEXT2 index answers a query by searching every live segment (a tag=0 base
plus tag=1 CDC-tail deltas) and merging their top-k. Four independent, opt-in
improvements share this PR because they ship in one binary and touch the same
search and build paths:

1. **Parallel segment search.** The multi-segment top-k walk is CPU-bound and was
   serial. A new per-index worker count lets the independent per-segment searches
   run concurrently, lowering BM25 query latency. The same change adds cross-segment
   pruning — a shared lower bound on the global k-th best plus a per-segment upper
   bound that lets whole uncompetitive segments be skipped — so segments spare each
   other work. All of it is performance-only: the merged top-k is identical at any
   worker count and with pruning on or off.

2. **`token_filter` analyzer (`stop`).** A Lucene-style analyzer chain on the index.
   Step 1 is the `stop` filter: it drops stopwords (the builtin English+Chinese
   list) so the index never stores their postings. It shrinks the index and removes
   the highest-frequency posting lists from query work.

3. **High-df term cutoff (`search_df_cutoff`).** A session/global setting that, for
   a ranked bag-of-words (`IN BM25 MODE`) query only, drops query terms present in
   at least a given fraction of the live corpus before searching. Such terms have
   near-zero idf, so they contribute almost nothing to ranking while dominating the
   posting work.
   Unlike the first two, this one **changes results** and is off by default; its
   contract is below.

4. **Parallel build (`threads_build`).** The CREATE / REINDEX build tokenized every
   row, finished every sealed segment and wrote it on one thread. Tokenization now
   runs on several workers and sealed segments are finished in the background while
   the next segment fills; the produced segments are byte-identical to a serial
   build.

5. **Score-ordered LIMIT pushdown.** An explicit `ORDER BY <score> DESC LIMIT n`
   over a MATCH query now bounds the index search to a fixed number of top-scoring
   candidates, as the implicit relevance order already did, instead of streaming
   every match into the sort. Its contract is below.

None of the changes alters the segment on-disk format, the posting layout, the
transaction protocol, or CDC/cron ownership of the asynchronously maintained
index. All are off by default in behavior: parallel search and parallel build yield
identical results,
`token_filter` is only present when explicitly requested at CREATE, and the cutoff
is disabled unless set.

## Scope and non-goals

In scope: the per-index search worker count and its resolution/activation; the
cross-segment pruning bounds and their results-identical/certification contract; the
peak result-memory and error/cancellation contract of the concurrent search; the
`token_filter` WITH option and the `stop` filter; the symmetry of tokenization
across the build, CDC, and query paths; the maintenance (REINDEX, idxcron)
behavior of a `token_filter` index; the query-time high-df term cutoff, its
accepted result change, and its configuration lifetime; and the parallel build's
identical-output, ordering, memory, transaction-ownership and failure contract;
and which explicit score orderings bound the index search, and how the bound
tolerates index lag.

Out of scope: any posting/segment format or wire change; a positional (phrase)
analyzer; non-`stop` filters (`synonym` and others are reserved names only);
parallel writes of segments (they stay serialized in the build's transaction);
parallelizing the CDC tail or the idxcron merge; the per-segment phrase walk; and
changing the vector index's own search-thread knob.

## Parallel segment search contract

### Results-identical invariant

The worker count is a performance hint only. For every query, score formula, k,
prefilter, and segment set, the merged top-k — doc ids, scores, and order — is
identical for any worker count, including one worker. Each per-segment search is
fully independent (its own dense ord space, WAND cursors, and local heap); a
worker writes only its own result slot, and the merge runs serially in segment
order after all workers finish. The one piece of shared mutable state is a
results-neutral pruning bound (see "Cross-segment pruning"), never a result or a
heap; no worker can change another's output, only let it finish sooner. This
invariant is what lets the knob be an ungated performance control rather than a
correctness-affecting one.

### Worker-count semantics (decision)

The control is an integer per-index search worker count, resolved from a
session/global variable, mirroring the vector index's existing search-thread
knob so operators learn one model:

- **unset / `0` → the deployment's effective scheduler parallelism** (bounded by
  the process's effective GOMAXPROCS, which is container-quota aware). This is the
  default and matches the vector-index knob; a fresh index therefore parallelizes.
- **`N > 0` → at most `N` worker goroutines**, capped by the segment count.

Worker goroutines are multiplexed by the Go scheduler onto the deployment's
GOMAXPROCS OS threads, so concurrent queries share one CPU budget rather than
multiplying it: the goroutine count rises with concurrency, but true CPU
parallelism stays bounded by GOMAXPROCS regardless of how many queries run at
once. The knob bounds goroutine fan-out per query, not a separate CPU pool.

### Warm-cache knob ownership (decision)

A loaded index is cached and reused across queries. The worker count is bound once,
when an index generation is first loaded into the cache, and every subsequent query
served from that warm generation uses that bound value — a later query's own
variable setting does not re-tune a generation already resident. This is an
accepted consequence of caching a read-only search object: because the knob is
results-neutral, a stale worker count only changes latency, never answers. An
operator who must change the effective worker count forces a fresh load (a new
generation via CDC/REBUILD, or a cache eviction); the knob is not a per-execution
guarantee and is documented as a per-load hint. Correctness never depends on which
value won the first load.

### Peak result-memory and drainage bound

Each per-segment search returns at most k results, so the concurrent phase holds
at most `segments × k` Result values simultaneously before the serial merge
reduces them to k. A Result is a doc id, a score, and any requested INCLUDE values
— it is independent of posting-list length and corpus size. This is a higher peak
than a strictly incremental single-heap merge (which would hold ≈k), and it
applies even at one worker, because all per-segment slots are materialized before
the merge. The bound is `O(segments × k)` small records, chosen for a simple,
race-free merge; it is not proportional to matches, postings, or document size.

This bound holds because only the bounded per-segment searches are parallelized: the
ranked bag-of-words (WAND) walk holds an `O(k)` heap and the pure-conjunction walk is
bounded by its term count and block size. The one path with per-segment scratch
proportional to the segment's document count — the dense full-boolean evaluator used
for general MUST/MUST-NOT/mixed queries — is therefore run **serially**, one segment at
a time, so its live scratch stays `O(one segment)` rather than `workers × O(N)`. The
parallel worker count applies only to the bounded paths; the dense path ignores it.

Every spawned worker is bounded by a semaphore and completes — writing its slot or
its error — before the call returns; no worker outlives the search. A per-segment
error is recorded per slot and surfaced after all workers drain, so a failing
segment neither leaks a goroutine nor blocks the others; the merge is skipped on
error. Query cancellation is the caller's existing context on the surrounding
pipeline; the bounded worker set drains rather than being abandoned.

### Cross-segment pruning (decision)

The ranked bag-of-words search prunes documents whose score cannot reach the
current top-k. Independent segments can share this pruning so one segment's
progress spares another's work. Two bounds cooperate, and both are perf-only: the
merged top-k is identical whether or not they fire.

- **A shared lower bound on the global k-th best.** The segments share one
  monotonically rising score below which no document can enter the merged top-k. It
  is a *certified* lower bound: it is only ever raised to a score for which at least
  k live, filter-admitted documents are known to score at or above it — a segment's
  own complete top-k of k such documents. A segment holding fewer than k admitted
  documents never contributes, because its lowest score is not evidence that k
  documents exist above it. This certification is the load-bearing invariant: raising
  the bound from anything less (a smaller segment's partial heap) would push it above
  the true global k-th best and silently drop competitive documents.

- **A per-segment upper bound.** Each segment has a cheap ceiling on the best score
  any of its documents could achieve, derived from per-term maximum impacts without
  scanning documents. A whole segment whose ceiling is **strictly** below the shared
  lower bound cannot hold a top-k document and is skipped without being searched. The
  comparison is strict so a document that could *tie* the global k-th best is never
  skipped — the top-k, ties included, is preserved exactly.

Segments are visited in decreasing ceiling order so the shared lower bound rises as
early as possible, maximizing how much later segments can skip. Ordering and skipping
change no result.

The direction of the bounds is essential and asymmetric: a skip is sound because an
*upper* bound is compared against a *lower* bound, but the shared lower bound may
**not** be seeded or predicted from ceilings (upper bounds) — a pruning floor must be
certified by actual computed scores, never estimated, or it risks exceeding the true
k-th best. For this reason the bound starts at negative infinity and only rises
through real scores; a query whose segments never individually reach k documents
prunes nothing and simply runs the unpruned search.

These mechanisms are unconditional and carry no configuration — they are a property
of the search, not a tunable, and add nothing to the session/global or cluster
capability surface. They apply only to the ranked bag-of-words shape; phrase,
boolean, and conjunctive queries are unaffected. The cross-segment skip/ordering
additionally engages only where an earlier segment's bound can be established before
a later segment is searched (serial, or a fan-out with more segments than workers);
in a single parallel wave only the shared lower bound applies.

### Activation and compatibility (decision)

Parallel search plumbs the per-index worker count through the index scan options,
and every node honors the resolved value. The unset/`0` default resolves to the
effective parallelism at the search site; it is not a request for serial execution.
MatrixOne upgrades by full-cluster shutdown, so no node runs without the option and
no capability gate is needed.

## Parallel build contract

### Identical-output invariant

For every corpus, parser, `token_filter`, capacity setting and worker count, the
parallel build produces the same segments as the serial build: the same segment
boundaries, the same segment ids, and byte-identical serialized segments. Rows are
tokenized concurrently but fed to the segment builder in source order, segments are
sealed under the same capacity rule, and each sealed segment is finished from exactly
the rows it would have held serially. Only the time at which work happens changes.

### Stages and ownership (decision)

The build has three stages:

1. **Tokenization** of each row's text runs on a pool of workers. Reading the source
   row (including datalink content) happens on the operator thread, which hands the
   workers owned copies; workers touch no vector, process or transaction state.
   Parsers that produce terms without a tokenizer (json tuple terms, json_value) run
   inline on the operator thread.
2. **Segment finishing** (building the posting structures, serializing, staging the
   bytes in a local file) runs in the background once a segment seals, while the
   operator continues filling the next segment.
3. **Writing** a finished segment into the index tables runs on the operator thread
   only, in segment order, inside the build's transaction, and as soon as the oldest
   finished segment is ready — overlapping with the tokenization of later rows.

All SQL issued by the build, including the one-time removal of prior base segments,
stays on the operator thread. The transaction is never used from two threads.

### Memory bound (decision)

Besides the open segment being filled, at most a small fixed number of sealed
segments are being finished at once (one, or two with four or more workers); the
operator waits for a finisher before sealing beyond that. Each in-flight segment is
bounded by the same per-segment capacities as the serial build, so peak build memory
grows by at most that many segments. Finished segments wait for their write as local
files, not in memory. Queued rows awaiting tokenization are bounded by a row count and
a byte budget proportional to the worker count.

### Failure and cancellation

The first error from a worker, a finisher or a write fails the build. A panic in a
worker or a finisher is re-raised on the operator thread, so it surfaces as a query
error rather than a process crash. On failure or cancellation the operator waits for
running finishers to stop and removes every staged file that was not written; no
background work outlives the statement.

### Activation and compatibility (decision)

The worker count is the `fulltext2_threads_build` session/global setting (0, the
default, resolves to the effective parallelism; 1 keeps the serial build), carried in
the build configuration to the operator that runs the build. Because the produced
segments are identical at any value, no capability gate is needed. Background
builds that have no session (scheduled rebuilds) use the default.

## token_filter analyzer contract

### Bag-of-words-only invariant (decision)

`token_filter` drops tokens at index time, so the index physically omits their
postings. A positional phrase query reasons about byte gaps between stored tokens;
a dropped stopword leaves a gap the phrase cannot account for, so phrase matching
against a stop-filtered index is ill-defined. The contract resolves this by
restricting `token_filter` to a **bag-of-words** index: it requires
`POSITION_FREE=TRUE`, which stores no positions and is already answerable only by
the ranked bag-of-words (BM25) mode. `POSITION_FREE` in turn requires the word
parser (gojieba), so `token_filter` inherits that. Natural-language and boolean
(phrase) modes are rejected for such an index by the existing position-free check;
no new rejection path is needed. A query that is entirely stopwords matches nothing
and returns an empty result (not an error), the same as querying any term the index
does not contain — matching standard stopword semantics.

### Build/CDC/query symmetry invariant

The filter chain is persisted as an index parameter and reconstructed identically
on every path that tokenizes: the CREATE build, the CDC tail build, the scheduled
(idxcron) rebuild, and the BM25 query. All build the same filter from the same
persisted names over the same word tokenizer, so a token dropped at index time is
also dropped from the query. Because a stop-filtered index has no stopword
postings, applying the filter on the query side is a results-neutral optimization
(a stopword query term would find zero postings and contribute zero to BM25
either way); it is applied for clarity and to avoid setting up empty cursors.

### Write-side consistency and activation (decision)

The invariant a `token_filter` index promises is that **no segment holds a stopword
posting**. Every writer must therefore strip identically. A node that predates the
analyzer and ran the index's asynchronous tail build or a scheduled rebuild would
not strip, leaking stopword postings into its segments. This is not merely the lost
size reduction: a document's recorded length is the count of the tokens indexed for
it, and that length feeds both the per-document BM25 length-normalization factor and
the index-wide average length. A non-stripping writer therefore records a larger
length for its documents and perturbs the score of **every** query served from the
affected generation, not only a query that mentions a stopword. The write-side
invariant is thus a ranking-correctness requirement, not a size optimization.

Every write path strips with the same filter. MatrixOne upgrades by full-cluster
shutdown, so a non-stripping writer never runs alongside a stripping one and CREATE
needs no capability gate. The read side needs no gate either, per the results-neutral
argument above.

### Maintenance invariant (REINDEX / idxcron)

`token_filter` and `POSITION_FREE` are immutable index properties after CREATE. A
rebuild (manual REINDEX or scheduled idxcron) re-derives the index from the source
table and re-reads both from the persisted parameters, so the stripped, bag-of-words
shape is preserved. A merge folds the tail into the base by re-serializing
already-tokenized live documents; it never re-tokenizes, so it cannot reintroduce a
stopword. Turning `POSITION_FREE` off on a `token_filter` index via REINDEX is
rejected: it would otherwise leave a positional index built with stopwords stripped,
re-enabling the phrase modes against an index that cannot answer them — the very
state the bag-of-words invariant forbids, reached by bypassing the CREATE guard. To
make such an index positional, drop and recreate it without `token_filter`.

## High-df term cutoff contract

### Semantics and scope (decision)

The cutoff is a fraction `c` of the live corpus size `N`. A value of `0` (the
default) disables it; a value outside `(0, 1]` is rejected when set. When enabled,
a query term is dropped before the search if its corpus-global live document
frequency is at least `c × N`. Document frequency is the same live, cross-segment
count the BM25 idf already uses, so the decision is identical on every node and
for every segment layout of the same corpus.

It applies **only** to ranked bag-of-words retrieval (`IN BM25 MODE`, a
disjunction of single terms), on both the bounded top-k path and the unbounded
streaming path. It never touches natural-language or boolean-mode queries: an
explicit required (`+term`) or excluded term, a phrase, a prefix, or a group is
always honored, so a user who writes an operator always gets exactly that
operator's semantics.

### Accepted result change (decision)

This is a deliberate precision/recall trade, not a results-neutral optimization:

- A document matched **only** by dropped terms is no longer returned. For a top-k
  query this is inconsequential, since such a document scored near zero and was
  not competitive; for an unbounded query the match count shrinks.
- Remaining documents lose the dropped terms' contribution to their score. Because
  a term present in a fraction `c` of documents has idf near zero, the change is
  small and the relative ranking of documents that match a surviving term is
  essentially preserved, but scores are not bit-identical to the uncut query.
- At `c = 1`, only terms present in every live document are dropped; those have
  idf exactly zero. When the surviving terms match at least `k` documents, the
  top-k is identical to the uncut query; otherwise the uncut query fills the
  remainder with zero-score documents matched only by the dropped terms, which
  the cut query omits.

### Never-empty invariant

Over the whole live corpus, the cutoff must never turn a query that matches
documents into one that matches none merely by dropping terms. The original terms
are kept unchanged when the query has at most one term, when every term would be
dropped, or when no surviving term occurs in any live document. In each case the
query runs as if the cutoff were off.

The decision is made on corpus-global document frequency, before any other
predicate of the query is applied. With additional predicates (an INCLUDE-column
prefilter, a membership filter, or a residual base-table filter), the surviving
terms may match no document that also satisfies those predicates, so a filtered
query can return fewer rows, including none, than the uncut query. That is part
of the accepted result change above, not a violation of this invariant.

### Configuration lifetime (decision)

The value is read per query plan from the session (or global default), unlike the
worker count, which is bound to a loaded index. Changing it changes the result
set, so a change of value invalidates the session's cached and prepared plans; an
unchanged re-set keeps them warm. A query therefore always runs with the value in
effect when it executes, never one captured by an earlier plan.

### Compatibility (decision)

Because the cutoff changes results, every node that executes a scan for the same
query must apply the same value. The planner resolves the value once and ships it in
the scan options, and every node honors it; with full-cluster shutdown upgrade no
node runs without the option, so no capability gate is needed. Nothing is persisted: the
setting never changes the index, so it can be turned off, or the feature reverted,
at any time without migration.

## Score-ordered LIMIT pushdown contract

### Eligibility

The search is bounded only when the ORDER BY is exactly one key, the MATCH score
descending with default null placement, and the query has a LIMIT. A query with
any other or additional ORDER BY key, with `SQL_CALC_FOUND_ROWS`, or with a filter,
join or aggregate between the sort and the search that the bound cannot see
through, is not bounded. The sort above the search always re-applies ORDER BY,
LIMIT and OFFSET.

### Secondary ORDER BY keys are not bounded (decision)

Decided by @cpegeric, 2026-10-23 (PR #30618 review P1).

`ORDER BY score DESC, k2 LIMIT n` is not bounded. With rows tied at the n-th score,
`k2` decides which of them are returned, and the search ranks by score only, so a
bounded search could drop the row SQL requires. The query runs unbounded and
returns the exact result.

To re-order the top n by another key with the bound, rank in a derived table and
sort the page outside it:

```sql
SELECT * FROM (
  SELECT id, MATCH(body) AGAINST('q') AS score FROM t
  WHERE MATCH(body) AGAINST('q') ORDER BY score DESC LIMIT n
) AS page ORDER BY id;
```

The inner query is bounded; which rows tied at its cutoff enter the page is
unspecified, as for any score-only top n.

Rejected alternatives: carrying the secondary key into the search (by primary key
or INCLUDE column), or keeping every row tied at the cutoff through each segment,
worker and merge. Both diverge from the vector indexes, which bound only a
single distance key, and add tie handling to every top-k path.

### Index lag (decision)

Decided by @cpegeric, 2026-10-24 (PR #30618 review P2). Follow-up:
[#30853](https://github.com/matrixorigin/matrixone/issues/30853).

The search reads only the index. Matching, scores and INCLUDE values come from the
text and values the index holds. The index holds a write once ISCP has applied it and
the CN's index cache has loaded that version; a transaction's uncommitted writes are
never in it. Until then, for the reading transaction's own writes and for commits
ISCP has not applied alike:

- A row not in the index (a new insert) is not matched.
- A row whose text changed is matched and scored by its indexed text.
- When the plan joins the search to a scan of the source table, the join drops rows
  deleted from the source and re-checks predicates on INCLUDE columns and the primary
  key against current values. A bounded search keeps the post-filter over-fetch of
  `LIMIT + OFFSET` that the vector indexes use; when the join drops more of its
  candidates than that over-fetch adds, the page has fewer than LIMIT rows, possibly
  none.
- When the plan answers the query from the index alone (no scan of the source table),
  deleted rows and old INCLUDE values are returned as indexed.

The bound never grows to make the page complete. Rejected alternatives: proving index
freshness before every bounded search with an exact-scan fallback; falling back to an
unbounded search whenever the searched generation is behind the source, which on a
large index (88M rows) streams millions of candidates into the join; dropping the
bound whenever the transaction has written the source table; widening the bound by the
number of rows the transaction deleted, which is unbounded for a large delete and does
not correct changed text; and counting committed deletes after the searched generation
from the source's change state.

## Source fan-out and resource bounds

Parallel search adds, per query, at most `min(workers, segments)` concurrent
worker goroutines and the `O(segments × k)` result buffer described above; it adds
no process-global state and nothing proportional to posting or corpus size.

`token_filter` adds one process-wide stopword set, loaded once from the builtin
dictionary and shared read-only across all indexes and queries; the per-token cost
is one set membership test. It adds no per-query growth. The builtin set is loaded
lazily and fails closed: if the dictionary file is absent the build or query that
needs it fails cleanly rather than silently indexing or querying every token (the
word parser already validates the same dictionary directory at initialization, so a
working gojieba deployment always has the file).

## Alternatives considered

- **Parallelize at a coarser (statement) granularity instead of per segment:**
  would not help a single MATCH, which is one scan node; per-segment is the unit of
  independent work.
- **Incremental single-heap merge to hold ≈k instead of `segments × k`:** lower
  peak, but requires a concurrency-safe shared heap across workers (lock contention
  on the hot path) or a serial merge interleaved with the walk; the per-segment
  slot model keeps each worker lock-free and the merge trivial, and the peak is
  small records bounded by k and segment count.
- **A positional `token_filter` (strip stopwords but keep positions) to support
  phrase modes:** re-introduces the byte-gap ambiguity; deferred as a separate
  design with explicit position-increment semantics.
- **`token_filter` on the trigram/ngram or json parsers:** those tokenize
  differently at query time (overlapping trigrams / atomic values), so a
  stopword-stripped build would not match; restricted to the word parser.
- **Dropping high-df terms at build time (a frequency-based stop list) instead of
  at query time:** shrinks the index, but makes the cut permanent, ties it to the
  corpus at build time, and requires a rebuild to change; the query-time cutoff
  is reversible per session and needs no persisted state.
- **A fixed absolute df threshold instead of a corpus fraction:** would cut
  nothing on a small corpus and nearly everything on a large one; a fraction
  scales with the index.
- **Making the worker count a strict per-execution guarantee:** would require
  bypassing or re-tuning the shared read-only cache per query, coupling a
  performance knob to cache lifetime for no correctness benefit.

## Rollout and rollback

All three features are additive and opt-in. The high-df cutoff persists nothing
and defaults to off, so enabling, disabling or reverting it never requires a
migration. Parallelism defaults to the deployment's
effective parallelism and yields identical results, so it needs no data migration;
parallel search has no persisted invariant to break.

`token_filter` is different, because its invariant is persisted and write-side.
It is absent unless requested at CREATE. Because a non-stripping writer corrupts BM25
ranking corpus-wide (see the write-side section), every binary that builds or
maintains a `token_filter` index — through the CDC tail, a scheduled rebuild, or a
merge — must strip for as long as the index exists. Downgrade to a binary that
predates the analyzer while a `token_filter` index is present is **unsupported**.
Rollback is accordingly a source revert only once no `token_filter` index
exists (drop such indexes first); no catalog, hidden-table, persisted-metadata, or
protocol migration is introduced, and a plain revert with no such index present is
safe.

## Validation contract

Parallel build: a corpus mixing tokenized rows, parser-produced terms, empty rows,
INCLUDE values and both integer and varchar primary keys is built serially and through
the pipeline with one to eight workers and both finisher limits, under capacities that
seal many segments; every segment's id and serialized bytes must match the serial
build. Error, panic, write-failure and abort paths must leave no staged file. The
concurrent paths run under the race detector. End to end, the same table built with
one worker and with eight workers must answer queries identically, including with
`token_filter = 'stop'`.

Parallel search: a results-equality check between one worker and many workers must
use a `k` smaller than the number of matches so a competitive, truncated top-k is
actually exercised, and must compare an explicit single worker against an explicit
multi-worker run rather than relying on the default's resolution. The owning
package runs under the race detector. End-to-end behavior is validated with the
worker count set before the index is loaded, so serial and parallel execution are
each actually taken.

Cross-segment pruning: a single-segment index — where neither the shared bound nor
the segment skip can engage — is the oracle; the same documents split across many
segments, searched serially and in parallel, must return a top-k identical to that
oracle for a truncating `k`. The suite must include a segment holding **fewer than
`k`** documents, because that is the case where an uncertified lower bound would
inflate above the true k-th best and drop competitive documents — the regression the
certification invariant forbids — and it must cover the concurrent pruning path under
the race detector.

High-df cutoff: an index with a term present in every document, a mid-frequency
term, and a graded rare term is searched across multiple segments so the cutoff
reads the global document frequency. Tests must show: at `c = 1`, with surviving
terms that match at least `k` documents, the top-k keys and scores equal the uncut
query; a mid-frequency term is dropped at a cutoff at or
below its frequency and kept above it; the disabled values leave every query
unchanged; a boolean-mode query is never cut; and the never-empty invariant holds
for a single term, for all terms crossing the cutoff, and for a survivor that
occurs in no document. A BVT checks the user-visible match-count and score change
through SQL. A frontend test proves a change of value invalidates cached and
prepared plans while an unchanged re-set keeps them.

`token_filter`: unit tests cover the stop filter (drop, case folding, position
preservation, builtin load), the BM25 query dropping stopword terms, the schema
validation (requires `POSITION_FREE`; rejects a non-word parser and an unknown
filter name), the grammar round-trip, and the REINDEX guard rejecting
`POSITION_FREE=FALSE` on a `token_filter` index. A mixed-writer test builds the same
documents by a stripping writer and a non-stripping writer and reads both: it proves
the read side is robust for a stopword query (both return empty, because the reader
strips the query term) and that a non-stripping writer records a different document
length and so a different BM25 score for an ordinary query — the corpus-wide ranking
perturbation that makes write-side stripping a lifetime requirement of the index.
A BVT exercises the full surface:
the reject paths, the persisted parameter and SHOW CREATE round-trip, the BM25
stopword behavior (a stopword and an all-stopword query return empty, a mixed query
equals its content terms), the NL/boolean rejection, and the REINDEX guard. The PR
records the measured index-size reduction and the query-latency effect (including
with parallel search) so the evidence stays tied to the published commit.
