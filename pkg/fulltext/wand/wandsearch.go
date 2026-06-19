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

// WandSearch adapts a loaded WandModel to veccache.VectorIndexSearchIf so a
// retrieval index shares the VectorIndexCache (load-once, RW-shared, TTL
// eviction) with the vector plugins. The index is keyed in the cache by its
// storage table name (== model Id).
type WandSearch struct {
	cfg   TableConfig
	model *WandModel
}

var _ veccache.VectorIndexSearchIf = (*WandSearch)(nil)

// NewWandSearch returns an unloaded search handle; the cache calls Load before
// the first Search.
func NewWandSearch(cfg TableConfig) *WandSearch {
	return &WandSearch{cfg: cfg}
}

// Load reads + deserializes the index from the WAND chunk store.
func (s *WandSearch) Load(sqlproc *sqlexec.SqlProcess) error {
	m, err := LoadFromStorage(sqlproc, s.cfg, s.cfg.IndexTable)
	if err != nil {
		return err
	}
	s.model = m
	return nil
}

// Search runs WAND top-K and returns ([]any doc-ids of the source pk type,
// []float64 scores).
func (s *WandSearch) Search(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	if s.model == nil {
		return nil, nil, moerr.NewInternalError(proc.GetContext(), "wand index not loaded")
	}
	q, ok := query.(WandQuery)
	if !ok {
		return nil, nil, moerr.NewInternalError(proc.GetContext(), "wand search: invalid query payload")
	}
	limit := int(rt.Limit)
	if limit <= 0 {
		limit = 1
	}
	var allow Membership
	if len(q.FilterBytes) > 0 {
		f, ferr := docfilter.New(q.FilterBytes)
		if ferr != nil {
			return nil, nil, ferr
		}
		defer f.Free()
		allow = &docFilterMembership{m: s.model, f: f}
	}
	res := s.model.Search(q.Terms, limit, allow)
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
	if s.model != nil {
		s.model.Free()
		s.model = nil
	}
}
