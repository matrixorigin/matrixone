// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fulltext2

import (
	"context"
	"math"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// Fulltext2Query is the query payload passed through VectorIndexCache.Search to a
// Fulltext2Search: the raw MATCH pattern, whether it is boolean mode, and the
// relevance formula (BM25/TF-IDF, resolved per query from the ft2_relevancy_algorithm
// session var). The parser is a fixed property of the index (carried in cfg), so it
// is NOT part of the per-query payload — the same cached index serves every query.
type Fulltext2Query struct {
	Pattern []byte
	Boolean bool
	// BagOfWords is IN BM25 MODE: ranked bag-of-words retrieval (each token an OR
	// term, no positional phrase), so it works on a POSITION_FREE index. Takes
	// precedence over Boolean.
	BagOfWords bool
	Algo       ScoreAlgo
	// FilterBytes is the optional serialized docfilter membership — the WHERE-clause
	// prefilter pushed down as a runtime filter (built in C from the eligible pks),
	// applied INSIDE the search so a pushed LIMIT bounds the filtered set. nil = none.
	FilterBytes []byte
	// ScoreRange is the optional pushed-down relevance interval from a
	// `MATCH(...) <op> const` predicate. Applied inside the search so out-of-range rows
	// never cross into the join above. nil = no bound.
	ScoreRange *ScoreRange
	// IncludePredsJSON is the optional pushed-down predicate set on INCLUDE columns
	// (IncludePredicateSpec JSON), evaluated in Go against the stored per-doc INCLUDE
	// values INSIDE the search — so a WHERE on an INCLUDE column needs no second base scan.
	// nil = none. ANDed with FilterBytes.
	IncludePredsJSON []byte
}

// Fulltext2Search adapts a loaded fulltext2 Index to veccache.VectorIndexSearchIf so
// the positional index shares the VectorIndexCache (load-once, RW-shared, TTL
// eviction) with bm25 and the vector plugins, keyed by its storage table name.
// Before this, fulltext2_search reloaded the whole index (LoadAllBases +
// LoadTailSegments + NewIndex) on EVERY query — ~1s per query at 50K vs bm25's ~3ms.
// A loaded base segment's postings (docID/tf blocks + positions) are views into a
// shared read-only mmap, not the Go heap — reclaimable OS page cache, not GC-scanned;
// Destroy munmaps them (block-postings format, storage.go/segment.go).
type Fulltext2Search struct {
	cfg    TableConfig
	idx    *Index
	loaded bool

	// Generation captured at Load (same txn snapshot as the loaded data) for the cache's
	// cross-CN freshness check (IsStale, called off the housekeeping goroutine): loadedTs =
	// MAX(metadata.timestamp) (REBUILD/MERGE), loadedTail = MAX(tag=1 CdcTail chunk_id) (CDC
	// append). cnUUID/accountID are the durable handles to re-query in the background.
	// genValid is false when capture failed / no resolver (unit tests) → IsStale is a no-op.
	cnUUID     string
	accountID  uint32
	loadedTs   int64
	loadedTail int64
	genValid   bool
}

var _ veccache.VectorIndexSearchIf = (*Fulltext2Search)(nil)

// NewFulltext2Search returns an unloaded search handle; the cache calls Load before
// the first Search.
func NewFulltext2Search(cfg TableConfig) *Fulltext2Search {
	return &Fulltext2Search{cfg: cfg}
}

// Load reads the index from the chunk store: the tag=0 base sub-indexes plus the
// tag=1 CdcTail delta frames (+ delete set), assembled into a queryable Index with
// global stats and per-pk liveness. An index created on an empty table has no tag=0
// base, so segs may hold only tail segments (or be empty → a loaded, doc-less index).
func (s *Fulltext2Search) Load(sqlproc *sqlexec.SqlProcess) error {
	// Fail fast on the QUERY path if the bases' per-doc metadata won't fit the heap
	// budget, rather than OOM-killing the CN (which takes down every query on the node).
	// This guard is on Load, NOT LoadAllBases, so CompactSegments (MERGE) — the remedy —
	// stays exempt and can still load the bases to reclaim dead docs.
	if err := checkBaseLoadBudget(sqlproc, s.cfg); err != nil {
		return err
	}
	bases, err := LoadAllBases(sqlproc, s.cfg)
	if err != nil {
		return err
	}
	tails, deletes, err := LoadTailSegments(sqlproc, s.cfg)
	if err != nil {
		freeSegs(bases) // munmap the base segments on a tail-load error (don't leak the mappings)
		return err
	}
	segs := append(bases, tails...)
	s.idx = NewIndex(segs, deletes)
	s.loaded = true

	// Capture the generation + durable handles for IsStale. Same txn as the load, so the
	// captured generation matches the loaded snapshot exactly. On any capture failure genValid
	// stays false and IsStale reports the entry as uncheckable-hence-stale (evict + reload to
	// retry capture) rather than pinning it in cache forever — see IsStale.
	s.cnUUID = sqlproc.GetService()
	if acc, e := sqlproc.GetAccountID(); e == nil {
		if ts, tail, e2 := LoadGeneration(sqlproc, s.cfg); e2 == nil {
			s.accountID, s.loadedTs, s.loadedTail, s.genValid = acc, ts, tail, true
		}
	}
	return nil
}

// IsStale reports whether the underlying index has changed since this entry was loaded, by
// comparing the load-time generation to the current one (a REBUILD/MERGE bumps the metadata
// timestamp; a CDC append bumps the tag=1 tail chunk_id). Run on the cache housekeeping
// goroutine (NOT the search path), so it opens its own short auto-commit txn via the durable
// cnUUID/accountID captured at load.
//
// On a query ERROR it returns (true, err): the most likely cause is the index tables were
// dropped/rebuilt out from under us (DROP INDEX, restore), so the dead entry must be
// reclaimed — the next search reloads, or fails cleanly if the index is truly gone. The err
// is surfaced only for logging. (A no-op reload for a genuinely-transient blip is cheaper
// than pinning a possibly-dead entry until TTL.) No captured generation (capture failed at load,
// or no service to re-query) ⇒ (true, nil): the entry cannot self-check freshness, so evict it to
// force a reload that retries capture. Returning (false, nil) would pin a hot entry — whose TTL
// keeps sliding on every search — in cache indefinitely, serving pre-CDC/rebuild data forever;
// the bounded reload (one per freshness sweep) self-heals the moment capture succeeds.
func (s *Fulltext2Search) IsStale() (bool, error) {
	if !s.genValid || s.cnUUID == "" {
		return true, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ts, tail, err := QueryGeneration(ctx, s.cnUUID, s.accountID, s.cfg)
	if err != nil {
		return true, err
	}
	stale := ts != s.loadedTs || tail != s.loadedTail
	logutil.Debugf("[ft2-isstale] index=%s loadedGen=(ts=%d,tail=%d) curGen=(ts=%d,tail=%d) stale=%v",
		s.cfg.IndexTable, s.loadedTs, s.loadedTail, ts, tail, stale)
	return stale, nil
}

// prepare validates the query, resolves the top-k bound, and builds the WHERE prefilter,
// shared by Search and SearchInto. free is ALWAYS non-nil and must be deferred by the caller
// even on error (it frees the C-backed docfilter created here, so it never outlives the
// query — the cached Index is shared, so the filter must be per-query local). empty=true means
// a loaded but doc-less index (matches nothing) — the caller returns an empty result.
func (s *Fulltext2Search) prepare(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (q Fulltext2Query, k int, pf *prefilter, free func(), empty bool, err error) {
	free = func() {}
	if !s.loaded || s.idx == nil {
		err = moerr.NewInternalError(proc.GetContext(), "fulltext2 index not loaded")
		return
	}
	if s.idx.NumDocs() == 0 {
		empty = true
		return
	}
	var ok bool
	if q, ok = query.(Fulltext2Query); !ok {
		err = moerr.NewInternalError(proc.GetContext(), "fulltext2 search: invalid query payload")
		return
	}
	// rt.Limit is uint; a value past MaxInt32 (an absurd pushed LIMIT) would wrap negative
	// in int(...), and 0 means "no pushed LIMIT" — return the whole result set (the TVF
	// paginates). Mirror bm25's clamp.
	k = int(rt.Limit)
	if rt.Limit > uint(math.MaxInt32) {
		k = math.MaxInt32
	} else if k <= 0 {
		k = int(s.idx.NumDocs())
	}
	if len(q.FilterBytes) > 0 || len(q.IncludePredsJSON) > 0 || q.ScoreRange != nil {
		pf = &prefilter{scoreRange: q.ScoreRange}
		if len(q.FilterBytes) > 0 {
			var filter docfilter.MembershipFilter
			if filter, err = docfilter.New(q.FilterBytes); err != nil {
				return
			}
			free = filter.Free // freed via the caller's defer, incl. the error paths below
			pf.docFilter = filter
		}
		if len(q.IncludePredsJSON) > 0 {
			var preds []compiledIncludePred
			if preds, err = compileIncludePredicates(q.IncludePredsJSON, s.idx.includeTypes(), s.idx.pkType()); err != nil {
				return
			}
			pf.include = preds
		}
	}
	return
}

// runTopK dispatches the bounded top-k search (ranked bag-of-words vs positional query).
func (s *Fulltext2Search) runTopK(q Fulltext2Query, k int, pf *prefilter) ([]Result, error) {
	var (
		res []Result
		err error
	)
	if q.BagOfWords {
		res, err = s.idx.SearchBagOfWords(q.Pattern, s.cfg.Parser, q.Algo, k, pf)
	} else {
		res, err = s.idx.SearchQuery(q.Pattern, q.Boolean, s.cfg.Parser, q.Algo, k, pf)
	}
	if err != nil || pf == nil || pf.scoreRange == nil {
		return res, err
	}
	// Drop out-of-range results. This runs AFTER the top-k heap, so it can only be combined
	// with a pushed LIMIT when the range has no upper bound: the k highest scores are then
	// exactly the k highest in range. The planner enforces that -- it refuses to push a
	// candidate LIMIT whenever it lifts a score predicate (see the limitExpr gate in
	// applyJoinFullTextIndices) -- so k here is the whole result set.
	kept := res[:0]
	for _, r := range res {
		if pf.scoreRange.contains(r.Score) {
			kept = append(kept, r)
		}
	}
	return kept, nil
}

// Search runs the WAND positional query (NL exact-phrase or boolean) and returns
// ([]any pks of the source type, []float64 scores). The covered LIMIT path is served by the
// box-free SearchInto instead; this []any form is the streaming (Emit) path and the legacy
// non-covered return.
func (s *Fulltext2Search) Search(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	q, k, pf, free, empty, err := s.prepare(proc, query, rt)
	defer free()
	if err != nil {
		return nil, nil, err
	}
	if empty {
		return []any{}, []float64{}, nil
	}

	// No-LIMIT streaming path: when the caller passes an Emit callback (the TVF does this
	// only for a query with no pushed LIMIT), yield every matching doc in bounded batches —
	// no top-K heap, no materialization. Results are handed off through Emit, so return empty
	// keys/distances (mirrors bm25). wantInclude tells the stream to carry each doc's INCLUDE
	// values (segment order) through Emit's 3rd arg.
	if rt.Emit != nil {
		wantInclude := len(rt.RequestedIncludeColumns) > 0
		var serr error
		if q.BagOfWords {
			serr = s.idx.StreamBagOfWords(q.Pattern, s.cfg.Parser, q.Algo, pf, wantInclude, rt.Emit)
		} else {
			serr = s.idx.StreamQuery(q.Pattern, q.Boolean, s.cfg.Parser, q.Algo, pf, wantInclude, rt.Emit)
		}
		if serr != nil {
			return nil, nil, serr
		}
		return []any{}, []float64{}, nil
	}

	results, err := s.runTopK(q, k, pf)
	if err != nil {
		return nil, nil, err
	}
	keysOut := make([]any, len(results))
	dist := make([]float64, len(results))
	for i, r := range results {
		keysOut[i] = r.Pk
		dist[i] = float64(r.Score)
	}
	return keysOut, dist, nil
}

// SearchInto is the box-free LIMIT-path twin of Search: it fills the caller-owned out
// SearchOutput (pk column, scores, covered INCLUDE columns) instead of returning boxed []any
// keys — so a warm query allocates nothing for its results (the TVF pools out and Resets it
// per query). out.Keys is the pk in ColumnBuffer form (box-free for every pk type), out.Dists
// the float32 scores, out.Include one nullable buffer per FULL index INCLUDE column (populated
// only when rt.RequestedIncludeColumns is set; the TVF projects what it needs by segPos).
func (s *Fulltext2Search) SearchInto(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, out *vectorindex.SearchOutput) error {
	if out == nil {
		return moerr.NewInternalError(proc.GetContext(), "fulltext2 SearchInto: nil out")
	}
	q, k, pf, free, empty, err := s.prepare(proc, query, rt)
	defer free()
	if err != nil {
		return err
	}
	// Reset the caller-owned buffers (reused across queries → alloc-free after warmup).
	pkType := s.idx.pkType()
	if out.Keys == nil {
		out.Keys = &vectorindex.ColumnBuffer{}
	} else {
		out.Keys.Reset()
	}
	out.Keys.Type = types.T(pkType)
	out.Dists = out.Dists[:0]
	if empty {
		out.Include = out.Include[:0]
		return nil
	}

	results, err := s.runTopK(q, k, pf)
	if err != nil {
		return err
	}
	// pk column (box-free re-encode of each already-materialized boxed pk) + scores.
	w, fixed := fixedPkByteWidth(pkType)
	if !fixed {
		w = 0
	}
	for i := range results {
		appendAnyPk(out.Keys, w, results[i].Pk)
		out.Dists = append(out.Dists, results[i].Score)
	}
	return s.fillInclude(rt, results, out)
}

// fillInclude populates out.Include with one nullable ColumnBuffer per FULL index INCLUDE
// column (segment order), covering the whole result set — so a covered LIMIT query reads its
// projected INCLUDE columns straight from the index (no base-table JOIN, no map[string][]any,
// no per-row reflection append). The TVF maps its requested output columns to segment
// positions (by includeOut.segPos), exactly as the streaming consumer does. No-op (empties
// out.Include) when no include columns were requested. The buffers are REUSED across queries
// (Reset in place) so includes are alloc-free after warmup, like out.Keys/out.Dists.
//
// Source note: this re-encodes each result's already-materialized boxed Include value — the
// top-k's per-winner decodeInclude boxing (O(limit), bounded) is retained; what this removes
// is the transport map + the reflection AppendAny.
func (s *Fulltext2Search) fillInclude(rt vectorindex.RuntimeConfig, results []Result, out *vectorindex.SearchOutput) error {
	if len(rt.RequestedIncludeColumns) == 0 {
		out.Include = out.Include[:0]
		return nil
	}
	it := s.idx.includeTypes()
	if cap(out.Include) < len(it) {
		out.Include = make([]*vectorindex.ColumnBuffer, len(it))
	} else {
		out.Include = out.Include[:len(it)]
	}
	for c := range it {
		if out.Include[c] == nil {
			out.Include[c] = &vectorindex.ColumnBuffer{}
		} else {
			out.Include[c].Reset()
		}
		out.Include[c].Type = types.T(it[c])
	}
	for r := range results {
		inc := results[r].Include
		for c := range out.Include {
			var v any
			if c < len(inc) {
				v = inc[c]
			}
			if err := appendAnyInclude(out.Include[c], v); err != nil {
				return err
			}
		}
	}
	return nil
}

// SearchFloat32 is unsupported (fulltext scores are float64; the vector float32
// fast-path does not apply).
func (s *Fulltext2Search) SearchFloat32(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	return moerr.NewInternalError(proc.GetContext(), "fulltext2 search: SearchFloat32 not supported")
}

// Destroy munmaps the loaded index's base segments (their posting blocks + positions
// are views into those mappings), then drops it. The cache holds the write lock
// around this, so no search is in flight.
func (s *Fulltext2Search) Destroy() {
	s.idx.Free()
	s.idx = nil
	s.loaded = false
}
