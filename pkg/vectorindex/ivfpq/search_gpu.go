//go:build gpu

// Copyright 2022 Matrix Origin
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

package ivfpq

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// IvfpqSearch implements cache.VectorIndexSearchIf for GPU IVF-PQ indexes.
type IvfpqSearch[T cuvs.VectorType] struct {
	Idxcfg        vectorindex.IndexConfig
	Tblcfg        vectorindex.IndexTableConfig
	Indexes       []*IvfpqModel[T]
	MultiIndex    *cuvs.MultiGpuIvfPq[T]
	Overflow      *cuvs.GpuBruteForce[T] // CDC insert overflow; nil when no overflow records exist
	Devices       []int
	ThreadsSearch int64
}

func NewIvfpqSearch[T cuvs.VectorType](idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig, devices []int) *IvfpqSearch[T] {
	nthread := vectorindex.GetConcurrency(tblcfg.ThreadsSearch)
	return &IvfpqSearch[T]{
		Idxcfg:        idxcfg,
		Tblcfg:        tblcfg,
		Devices:       devices,
		ThreadsSearch: nthread,
	}
}

// Search implements cache.VectorIndexSearchIf.
func (s *IvfpqSearch[T]) Search(sqlproc *sqlexec.SqlProcess, anyquery any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	query, ok := anyquery.([]float32)
	if !ok {
		return nil, nil, moerr.NewInternalErrorNoCtx("IvfpqSearch: query type mismatch")
	}

	limit := rt.Limit

	if s.MultiIndex == nil {
		return []int64{}, []float64{}, nil
	}

	dim := uint32(s.Idxcfg.CuvsIvfpq.Dimensions)
	sp := cuvs.DefaultIvfPqSearchParams()
	// Prefer the per-query Probe from RuntimeConfig (set from session
	// `probe_limit` by the planner / table function on every call) over
	// the cached Tblcfg.Nprobe, which only reflects the value that was
	// in effect when this IvfpqSearch was first inserted into the
	// VectorIndexCache. UpdateConfig is a no-op on this type, so without
	// reading rt.Probe here, changes to `probe_limit` would not propagate.
	if rt.Probe > 0 {
		sp.NProbes = uint32(rt.Probe)
	} else if s.Tblcfg.Nprobe > 0 {
		sp.NProbes = uint32(s.Tblcfg.Nprobe)
	}
	var (
		neighbors64 []int64
		dists32     []float32
	)
	if rt.FilterJSON != "" {
		neighbors64, dists32, err = s.MultiIndex.SearchFloat32WithFilter(query, 1, dim, uint32(limit), sp, rt.FilterJSON)
	} else {
		neighbors64, dists32, err = s.MultiIndex.SearchFloat32(query, 1, dim, uint32(limit), sp)
	}
	if err != nil {
		return nil, nil, err
	}

	reskeys := make([]int64, 0, limit)
	resdistances := make([]float64, 0, limit)
	for i, k := range neighbors64 {
		if k == -1 {
			continue
		}
		reskeys = append(reskeys, k)
		resdistances = append(resdistances, metric.DistanceTransformIvfflat(
			float64(dists32[i]),
			metric.DistFuncNameToMetricType[rt.OrigFuncName],
			metric.MetricType(s.Idxcfg.CuvsIvfpq.Metric),
		))
	}

	return reskeys, resdistances, nil
}

// SearchFloat32 implements cache.VectorIndexSearchIf.
func (s *IvfpqSearch[T]) SearchFloat32(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	keys, dists, err := s.Search(proc, query, rt)
	if err != nil {
		return err
	}
	if keys == nil {
		return nil
	}
	ks, ok := keys.([]int64)
	if !ok {
		return moerr.NewInternalErrorNoCtx("IvfpqSearch: unknown keys type")
	}
	copy(outKeys, ks)
	for i, d := range dists {
		outDists[i] = float32(d)
	}
	return nil
}

// Load implements cache.VectorIndexSearchIf.
func (s *IvfpqSearch[T]) Load(sqlproc *sqlexec.SqlProcess) error {
	indexes, err := LoadMetadata[T](sqlproc, s.Tblcfg.DbName, s.Tblcfg.MetadataTable)
	if err != nil {
		return err
	}
	if len(indexes) > 0 {
		indexes, err = s.loadIndexes(sqlproc, indexes)
		if err != nil {
			return err
		}
	}
	s.Indexes = indexes
	if err = s.loadCdcTail(sqlproc); err != nil {
		return err
	}
	if err = s.buildOverflow(); err != nil {
		return err
	}
	s.MultiIndex = s.buildMultiIndex()
	return nil
}

// loadCdcTail mirrors cagra.CagraSearch.loadCdcTail — see that for the
// architectural commentary. Differs only in the IndexConfig type slot and
// the GpuIvfPq element type.
func (s *IvfpqSearch[T]) loadCdcTail(sqlproc *sqlexec.SqlProcess) error {
	var (
		includeBytesPerRow int
		colMetaJSON        string
	)
	// Prefer a loaded sub-index's IncludeBytesPerRow + colMetaJSON.
	// Falls back to the CdcOpHeader record persisted by the small-tail
	// emit path when no sub-index exists for this index slice.
	for _, m := range s.Indexes {
		if m.Index != nil {
			includeBytesPerRow = m.IncludeBytesPerRow
			colMetaJSON = m.Index.GetFilterColMetaJSON()
			break
		}
	}

	stub := &IvfpqModel[T]{Id: vectorindex.CdcTailId}
	chunks, err := stub.loadCdcEventsFromDB(sqlproc, s.Tblcfg)
	if err != nil {
		return err
	}
	if len(chunks) == 0 {
		return nil
	}
	cuvscdc.SortChunks(chunks)

	if colMetaJSON == "" {
		colMetaJSON, err = cuvscdc.PeekColMetaJSON(chunks)
		if err != nil {
			return err
		}
		if colMetaJSON != "" {
			includeBytesPerRow, err = cuvscdc.CdcIncludeBytesPerRow(colMetaJSON)
			if err != nil {
				return err
			}
		}
	}

	dim := int(s.Idxcfg.CuvsIvfpq.Dimensions)
	delPkids, ovPkids, ovVecs, ovInc, err := replayEventChunks(chunks, dim, includeBytesPerRow)
	if err != nil {
		return err
	}
	if len(delPkids) == 0 && len(ovPkids) == 0 {
		return nil
	}

	for _, m := range s.Indexes {
		if m.Index != nil {
			if err = m.Index.DeleteIds(delPkids); err != nil {
				return err
			}
		}
	}

	s.Indexes = append(s.Indexes, &IvfpqModel[T]{
		Id:                   vectorindex.CdcTailId,
		DeletedPkids:         delPkids,
		OverflowPkids:        ovPkids,
		OverflowVecs:         ovVecs,
		OverflowIncludeBytes: ovInc,
		IncludeBytesPerRow:   includeBytesPerRow,
		OverflowColMetaJSON:  colMetaJSON,
	})
	return nil
}

// addOverflowFilterChunks — see cagra/search_gpu.go for docs.
func addOverflowFilterChunks[T cuvs.VectorType](
	bf *cuvs.GpuBruteForce[T],
	colMetaJSON string,
	includeBytes []byte,
	nrows uint64,
	includeBytesPerRow int,
) error {
	colData, colNulls, err := cuvscdc.SplitIncludeBytes(colMetaJSON, includeBytes, nrows, includeBytesPerRow)
	if err != nil {
		return err
	}
	for i := range colData {
		if err = bf.AddFilterChunk(uint32(i), colData[i], colNulls[i], nrows); err != nil {
			return err
		}
	}
	return nil
}

// buildOverflow assembles a single GpuBruteForce index from the union of every
// loaded model's CDC insert overflow. When the underlying index has INCLUDE
// columns, the brute-force is set up with the matching FilterStore so a
// filtered query can prefilter overflow rows.
func (s *IvfpqSearch[T]) buildOverflow() error {
	total := uint64(0)
	for _, m := range s.Indexes {
		total += uint64(len(m.OverflowPkids))
	}
	if total == 0 {
		s.Overflow = nil
		return nil
	}

	cuvsMetric, ok := metric.MetricTypeToCuvsMetric[metric.MetricType(s.Idxcfg.CuvsIvfpq.Metric)]
	if !ok {
		return moerr.NewInternalErrorNoCtx("IvfpqSearch: unsupported metric type for overflow")
	}
	dim := uint32(s.Idxcfg.CuvsIvfpq.Dimensions)

	device := 0
	if len(s.Devices) > 0 {
		device = s.Devices[0]
	}

	bf, err := cuvs.NewGpuBruteForceEmpty[T](
		total, dim, cuvsMetric, uint32(s.ThreadsSearch), device)
	if err != nil {
		return err
	}
	if err = bf.Start(); err != nil {
		bf.Destroy()
		return err
	}

	// INCLUDE-column wiring — pull the col-meta JSON from the first loaded
	// model (every shard agrees by construction). For small-data-only
	// indexes (no tag=0 sub-index ever built) the synthetic CDC-tail model
	// carries the colMetaJSON recovered from the CdcOpHeader record.
	var (
		colMetaJSON        string
		includeBytesPerRow int
	)
	for _, m := range s.Indexes {
		if m.Index != nil {
			colMetaJSON = m.Index.GetFilterColMetaJSON()
			includeBytesPerRow = m.IncludeBytesPerRow
			break
		}
	}
	if colMetaJSON == "" {
		for _, m := range s.Indexes {
			if m.OverflowColMetaJSON != "" {
				colMetaJSON = m.OverflowColMetaJSON
				includeBytesPerRow = m.IncludeBytesPerRow
				break
			}
		}
	}
	if colMetaJSON != "" && includeBytesPerRow > 0 {
		if err = bf.SetFilterColumns(colMetaJSON, total); err != nil {
			bf.Destroy()
			return err
		}
	}

	for _, m := range s.Indexes {
		if len(m.OverflowPkids) == 0 {
			continue
		}
		count := uint64(len(m.OverflowPkids))
		if err = bf.AddChunkFloat(m.OverflowVecs, count, m.OverflowPkids); err != nil {
			bf.Destroy()
			return err
		}
		if colMetaJSON != "" && includeBytesPerRow > 0 {
			if err = addOverflowFilterChunks(bf, colMetaJSON, m.OverflowIncludeBytes, count, includeBytesPerRow); err != nil {
				bf.Destroy()
				return err
			}
		}
	}
	if err = bf.Build(); err != nil {
		bf.Destroy()
		return err
	}
	s.Overflow = bf
	return nil
}

// buildMultiIndex assembles a MultiGpuIvfPq from the loaded indexes.
// Returns nil when there is nothing to search — either no sub-indexes
// loaded AND no brute-force overflow built. The empty-MultiIndex case
// feeds into Search, which returns []int64{}, []float64{} on
// s.MultiIndex == nil — that's the load-bearing path for "no main
// index + no brute-force → empty result". Any future regression here
// will fail TestIvfpqSearchEmpty.
func (s *IvfpqSearch[T]) buildMultiIndex() *cuvs.MultiGpuIvfPq[T] {
	cuvsMetric, ok := metric.MetricTypeToCuvsMetric[metric.MetricType(s.Idxcfg.CuvsIvfpq.Metric)]
	if !ok {
		return nil
	}
	gpuIndices := make([]*cuvs.GpuIvfPq[T], 0, len(s.Indexes))
	for _, model := range s.Indexes {
		if model.Index != nil {
			gpuIndices = append(gpuIndices, model.Index)
		}
	}
	if len(gpuIndices) == 0 && s.Overflow == nil {
		// Empty index: no sub-indexes AND no brute-force overflow.
		// Search returns an empty result via its nil-MultiIndex guard.
		return nil
	}
	dim := uint32(s.Idxcfg.CuvsIvfpq.Dimensions)
	return cuvs.NewMultiGpuIvfPq(gpuIndices, s.Overflow, dim, cuvsMetric)
}

// loadIndexes loads each model's index data from the database.
func (s *IvfpqSearch[T]) loadIndexes(sqlproc *sqlexec.SqlProcess, indexes []*IvfpqModel[T]) ([]*IvfpqModel[T], error) {
	for _, idx := range indexes {
		idx.Devices = s.Devices
		if err := idx.LoadIndex(sqlproc, s.Idxcfg, s.Tblcfg, s.ThreadsSearch, true); err != nil {
			for _, idx2 := range indexes {
				idx2.Destroy()
			}
			return nil, err
		}
	}
	return indexes, nil
}

// Destroy implements cache.VectorIndexSearchIf.
func (s *IvfpqSearch[T]) Destroy() {
	s.MultiIndex = nil
	if s.Overflow != nil {
		s.Overflow.Destroy()
		s.Overflow = nil
	}
	for _, idx := range s.Indexes {
		idx.Destroy()
	}
	s.Indexes = nil
}

// UpdateConfig implements cache.VectorIndexSearchIf.
func (s *IvfpqSearch[T]) UpdateConfig(newalgo cache.VectorIndexSearchIf) error {
	return nil
}
