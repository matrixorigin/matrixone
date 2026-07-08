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
	"encoding/binary"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	m       *WandModel
	f       docfilter.MembershipFilter
	scratch [8]byte // reused encode buffer for the hot integer-PK path (Test copies out)
}

func (d *docFilterMembership) Contains(ord int64) bool {
	v := d.m.PkAt(ord)
	// Contains runs once per candidate on the Block-Max walk hot path. For the common
	// integer PKs, encode straight into a reused scratch buffer (byte-identical to
	// packUint*) instead of allocating a fresh slice per call via encodePk. Test reads
	// the bytes synchronously and does not retain them, so reuse is safe.
	var raw []byte
	switch types.T(d.m.PkType) {
	case types.T_int64:
		binary.LittleEndian.PutUint64(d.scratch[:], uint64(v.(int64)))
		raw = d.scratch[:8]
	case types.T_uint64:
		binary.LittleEndian.PutUint64(d.scratch[:], v.(uint64))
		raw = d.scratch[:8]
	case types.T_int32:
		binary.LittleEndian.PutUint32(d.scratch[:4], uint32(v.(int32)))
		raw = d.scratch[:4]
	case types.T_uint32:
		binary.LittleEndian.PutUint32(d.scratch[:4], v.(uint32))
		raw = d.scratch[:4]
	case types.T_uuid:
		// The membership filter (docfilter.buildBloomBytes -> CBloomFilter.addFixedVector)
		// hashes each source uuid as its RAW 16 bytes (typeSize=16). Probe with the same
		// raw bytes — NOT encodePk(uuid), which is the 36-char canonical string and would
		// never hit the same bloom cell, rejecting every candidate (B1).
		u := v.(types.Uuid)
		raw = u[:]
	default:
		var err error
		if raw, err = encodePk(d.m.PkType, v); err != nil {
			return false
		}
	}
	return d.f.Test(raw)
}

// WandSearch adapts the loaded WAND segments to veccache.VectorIndexSearchIf so a
// retrieval index shares the VectorIndexCache (load-once, RW-shared, TTL
// eviction) with the vector plugins. The index is keyed in the cache by its
// storage table name.
type WandSearch struct {
	cfg TableConfig
	// segs holds the tag=0 base sub-indexes (each carrying its metadata.chunk_id
	// recency — 0 for a full build, K for a compacted one) followed by the tag=1
	// CdcTail delta segments (ChunkId = frame chunk_id). An index created on an
	// empty table has no tag=0 base, so segs may hold only tail segments (or be empty).
	// deletes is pk -> max delete-frame chunk_id from the tag=1 log.
	segs    []*WandModel
	deletes map[any]int64
	// Precomputed at Load (query-independent): per-segment liveness and the corpus
	// stats. ComputeLiveness is O(total docs); computing it here (once per cache
	// load) instead of per query is item 3 of the Phase-C scaling plan. live is
	// parallel to segs (nil ⇒ every ord live).
	live       []Membership
	gN         int64
	gAvgDocLen float64
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
	// The tag=0 base may be several capacity-bounded sub-indexes, one, or none (an
	// index created on an empty table has only tag=1 CDC deltas). Load them all.
	bases, err := LoadAllBases(sqlproc, s.cfg)
	if err != nil {
		return err
	}
	// Each base carries its recency key (model.Recency) from metadata.chunk_id — 0
	// for a full-build base (oldest, below the tail which starts at 1), K for a
	// folded/merged base — so ComputeLiveness dedups bases + tail uniformly.
	tail, deletes, _, err := loadTailSegments(sqlproc, s.cfg)
	if err != nil {
		freeSegs(bases)
		return err
	}
	s.segs = append(bases, tail...)
	s.deletes = deletes
	// Precompute the query-independent liveness + corpus stats once here, so the
	// per-query path (Search) skips the O(total-docs) ComputeLiveness scan.
	s.live = ComputeLiveness(s.segs, s.deletes)
	s.gN, s.gAvgDocLen = corpusStats(s.segs)
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
	// rt.Limit is uint; a value past MaxInt32 (e.g. an absurd pushed LIMIT) would
	// wrap negative in int(...) and get clamped to 1, silently truncating the
	// top-K. Clamp such values to "effectively all" instead of wrapping.
	limit := int(rt.Limit)
	if rt.Limit > uint(math.MaxInt32) {
		limit = math.MaxInt32
	} else if limit <= 0 {
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
	// Combine the load-cached liveness with the per-query WHERE prefilter. When
	// there's a filter, build a FRESH slice (never mutate the cached s.live, which
	// is shared across all queries between reloads).
	live := s.live
	if mkAllow != nil {
		live = make([]Membership, len(s.segs))
		for i, seg := range s.segs {
			var base Membership
			if i < len(s.live) {
				base = s.live[i]
			}
			live[i] = andAllow(mkAllow(seg), base)
		}
	}
	res := searchSegmentsLiveStats(s.segs, q.Terms, limit, nil, live, s.gN, s.gAvgDocLen)
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
	s.live = nil
	s.gN = 0
	s.gAvgDocLen = 0
	s.loaded = false
}
