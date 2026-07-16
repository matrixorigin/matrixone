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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	Algo    ScoreAlgo
	// FilterBytes is the optional serialized docfilter membership — the WHERE-clause
	// prefilter pushed down as a runtime filter (built in C from the eligible pks),
	// applied INSIDE the search so a pushed LIMIT bounds the filtered set. nil = none.
	FilterBytes []byte
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
	return nil
}

// Search runs the WAND positional query (NL exact-phrase or boolean) and returns
// ([]any pks of the source type, []float64 scores).
func (s *Fulltext2Search) Search(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	if !s.loaded || s.idx == nil {
		return nil, nil, moerr.NewInternalError(proc.GetContext(), "fulltext2 index not loaded")
	}
	if s.idx.NumDocs() == 0 {
		// A loaded but empty index (no docs yet) matches nothing.
		return []any{}, []float64{}, nil
	}
	q, ok := query.(Fulltext2Query)
	if !ok {
		return nil, nil, moerr.NewInternalError(proc.GetContext(), "fulltext2 search: invalid query payload")
	}
	// rt.Limit is uint; a value past MaxInt32 (an absurd pushed LIMIT) would wrap
	// negative in int(...), and 0 means "no pushed LIMIT" — return the whole result
	// set (the TVF's call() paginates). Mirror bm25's clamp.
	k := int(rt.Limit)
	if rt.Limit > uint(math.MaxInt32) {
		k = math.MaxInt32
	} else if k <= 0 {
		k = int(s.idx.NumDocs())
	}
	// Build the WHERE prefilter once per query from the pushed-down membership bytes;
	// it resolves against each segment's ord→pk dict inside SearchQuery. Freed here so
	// the C-backed filter never outlives the query (the cached Index is shared, so the
	// filter must be per-query local, never stored on s.idx).
	var filter docfilter.MembershipFilter
	if len(q.FilterBytes) > 0 {
		filter, err = docfilter.New(q.FilterBytes)
		if err != nil {
			return nil, nil, err
		}
		defer filter.Free()
	}

	// No-LIMIT streaming path: when the caller passes an Emit callback (the TVF does
	// this only for a query with no pushed LIMIT), yield every matching doc in bounded
	// batches — no top-K heap, no materialization of the whole result set. Results are
	// handed off through Emit, so return empty keys/distances (mirrors bm25).
	if rt.Emit != nil {
		if serr := s.idx.StreamQuery(q.Pattern, q.Boolean, s.cfg.Parser, q.Algo, filter, rt.Emit); serr != nil {
			return nil, nil, serr
		}
		return []any{}, []float64{}, nil
	}

	results, err := s.idx.SearchQuery(q.Pattern, q.Boolean, s.cfg.Parser, q.Algo, k, filter)
	if err != nil {
		return nil, nil, err
	}
	keysOut := make([]any, len(results))
	dist := make([]float64, len(results))
	for i, r := range results {
		keysOut[i] = r.Pk
		dist[i] = r.Score
	}
	return keysOut, dist, nil
}

// SearchFloat32 is unsupported (fulltext scores are float64; the vector float32
// fast-path does not apply).
func (s *Fulltext2Search) SearchFloat32(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	return moerr.NewInternalError(proc.GetContext(), "fulltext2 search: SearchFloat32 not supported")
}

// UpdateConfig refreshes the table config from a freshly-built search handle (the
// cache passes the newest one on each call).
func (s *Fulltext2Search) UpdateConfig(newalgo veccache.VectorIndexSearchIf) error {
	if n, ok := newalgo.(*Fulltext2Search); ok {
		s.cfg = n.cfg
	}
	return nil
}

// Destroy munmaps the loaded index's base segments (their posting blocks + positions
// are views into those mappings), then drops it. The cache holds the write lock
// around this, so no search is in flight.
func (s *Fulltext2Search) Destroy() {
	s.idx.Free()
	s.idx = nil
	s.loaded = false
}
