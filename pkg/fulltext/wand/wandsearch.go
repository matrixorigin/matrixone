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

package wand

import (
	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// WandQuery is the query payload passed through VectorIndexCache.Search to a
// WandSearch: the jieba-tokenized query terms (duplicates → weight) plus an
// optional serialized docfilter membership payload (the WHERE-clause prefilter
// pushed down as a runtime filter, exactly as fulltext_index_scan receives it).
type WandQuery struct {
	Terms       []string
	FilterBytes []byte
}

// docFilterMembership applies a docfilter.MembershipFilter (built from the
// WHERE-clause pks) to the WAND walk: a candidate doc ord is allowed iff its pk
// bytes pass the filter. For integer PKs this is the C int64 cbitmap
// (mo_cbitmap_contain); for other PKs a bloom (false positives removed by the
// downstream join to the filtered source).
type docFilterMembership struct {
	m *WandModel
	f docfilter.MembershipFilter
}

func (d *docFilterMembership) Contains(ord int64) bool {
	raw, err := encodePk(d.m.PkType, d.m.PkAt(ord))
	if err != nil {
		return false
	}
	return d.f.Test(raw)
}

// baseChunkId is the recency key assigned to the tag=0 compacted-main segment
// when it is searched alongside tag=1 CdcTail delta segments. It sits below
// every tail frame's chunk_id (which start at 0), so any pk re-inserted or
// deleted in the tail supersedes its base copy (see ComputeLiveness).
const baseChunkId int64 = -1

// WandSearch adapts the loaded WAND segments to veccache.VectorIndexSearchIf so a
// retrieval index shares the VectorIndexCache (load-once, RW-shared, TTL
// eviction) with the vector plugins. The index is keyed in the cache by its
// storage table name.
type WandSearch struct {
	cfg TableConfig
	// segs[0] is the tag=0 compacted-main segment (ChunkId=baseChunkId) WHEN a base
	// exists; the remaining entries are the tag=1 CdcTail delta segments in
	// chunk_id order (ChunkId = frame chunk_id). An index created on an empty table
	// has no tag=0 base, so segs may hold only tail segments (or be empty).
	// deletes is pk -> max delete-frame chunk_id from the tag=1 log.
	segs    []*WandModel
	deletes map[any]int64
	// loaded distinguishes "never loaded" (Search errors) from "loaded but empty"
	// (an index with no docs yet → Search returns zero rows, not an error).
	loaded bool
}

var _ veccache.VectorIndexSearchIf = (*WandSearch)(nil)

// NewWandSearch returns an unloaded search handle; the cache calls Load before
// the first Search.
func NewWandSearch(cfg TableConfig) *WandSearch {
	return &WandSearch{cfg: cfg}
}

// Load reads the index from the WAND chunk store: the tag=0 compacted-main
// segment (offset-reassembled blob under its own index_id) plus the tag=1
// CdcTail delta frames (one complete frame per chunk_id, in append order),
// assembled into the ordered segment set + delete map searched with liveness.
func (s *WandSearch) Load(sqlproc *sqlexec.SqlProcess) error {
	// The tag=0 base is optional: an index created on an empty table has no
	// compacted-main segment, only tag=1 CDC deltas (or nothing yet).
	base, err := LoadBaseOptional(sqlproc, s.cfg, s.cfg.IndexTable)
	if err != nil {
		return err
	}
	if base != nil {
		base.ChunkId = baseChunkId
	}
	frames, err := loadTailFrames(sqlproc, s.cfg)
	if err != nil {
		if base != nil {
			base.Free()
		}
		return err
	}
	tail, deletes, err := AssembleFrames(frames)
	if err != nil {
		if base != nil {
			base.Free()
		}
		return err
	}
	var segs []*WandModel
	if base != nil {
		segs = append(segs, base)
	}
	s.segs = append(segs, tail...)
	s.deletes = deletes
	s.loaded = true
	return nil
}

// Search runs WAND top-K and returns ([]any doc-ids of the source pk type,
// []float64 scores).
func (s *WandSearch) Search(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	if !s.loaded {
		return nil, nil, moerr.NewInternalError(proc.GetContext(), "wand index not loaded")
	}
	if len(s.segs) == 0 {
		// A loaded but empty index (no docs yet) matches nothing.
		return []any{}, []float64{}, nil
	}
	q, ok := query.(WandQuery)
	if !ok {
		return nil, nil, moerr.NewInternalError(proc.GetContext(), "wand search: invalid query payload")
	}
	limit := int(rt.Limit)
	if limit <= 0 {
		limit = 1
	}
	// The WHERE prefilter is pk-based, so it must resolve against each segment's
	// own ord→pk dictionary — build one membership per segment.
	var mkAllow func(*WandModel) Membership
	if len(q.FilterBytes) > 0 {
		f, ferr := docfilter.New(q.FilterBytes)
		if ferr != nil {
			return nil, nil, ferr
		}
		defer f.Free()
		mkAllow = func(m *WandModel) Membership { return &docFilterMembership{m: m, f: f} }
	}
	res := searchSegsLive(s.segs, s.deletes, q.Terms, limit, mkAllow)
	keysOut := make([]any, len(res))
	dist := make([]float64, len(res))
	for i, r := range res {
		keysOut[i] = r.DocID
		dist[i] = r.Score
	}
	return keysOut, dist, nil
}

// SearchFloat32 is unsupported (fulltext scores are float64; the vector
// float32 fast-path does not apply).
func (s *WandSearch) SearchFloat32(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	return moerr.NewInternalError(proc.GetContext(), "wand search: SearchFloat32 not supported")
}

// UpdateConfig refreshes the table config from a freshly-built search handle
// (the cache passes the newest one on each call).
func (s *WandSearch) UpdateConfig(newalgo veccache.VectorIndexSearchIf) error {
	if n, ok := newalgo.(*WandSearch); ok {
		s.cfg = n.cfg
	}
	return nil
}

// Destroy frees the off-heap (C-allocated) postings and drops the model. The
// cache holds the write lock around this, so no search is in flight.
func (s *WandSearch) Destroy() {
	logutil.Debugf("[wand] WandSearch.Destroy: freeing %d cached segments for index=%s", len(s.segs), s.cfg.IndexTable)
	freeSegs(s.segs)
	s.segs = nil
	s.deletes = nil
	s.loaded = false
}
