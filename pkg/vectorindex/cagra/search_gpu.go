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

package cagra

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// CagraSearch implements cache.VectorIndexSearchIf for GPU CAGRA indexes.
// Unlike HnswSearch, there is no concurrency gate (Cond/Mutex) because CAGRA
// manages GPU thread concurrency internally via its worker pool.
type CagraSearch[T cuvs.VectorType] struct {
	Idxcfg        vectorindex.IndexConfig
	Tblcfg        vectorindex.IndexTableConfig
	Indexes       []*CagraModel[T]
	MultiIndex    *cuvs.MultiGpuCagra[T] // built once in Load; nil until indexes are loaded
	Overflow      *cuvs.GpuBruteForce[T] // CDC insert overflow; nil when no overflow records exist
	Devices       []int
	ThreadsSearch int64
}

func NewCagraSearch[T cuvs.VectorType](idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig, devices []int) *CagraSearch[T] {
	nthread := vectorindex.GetConcurrency(tblcfg.ThreadsSearch)
	return &CagraSearch[T]{
		Idxcfg:        idxcfg,
		Tblcfg:        tblcfg,
		Devices:       devices,
		ThreadsSearch: nthread,
	}
}

// Search implements cache.VectorIndexSearchIf.
func (s *CagraSearch[T]) Search(sqlproc *sqlexec.SqlProcess, anyquery any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	query, ok := anyquery.([]float32)
	if !ok {
		return nil, nil, moerr.NewInternalErrorNoCtx("CagraSearch: query type mismatch")
	}

	limit := rt.Limit

	if s.MultiIndex == nil {
		return []int64{}, []float64{}, nil
	}

	dim := uint32(s.Idxcfg.CuvsCagra.Dimensions)
	sp := cuvs.DefaultCagraSearchParams()
	if s.Idxcfg.CuvsCagra.ITopkSize > 0 {
		sp.ItopkSize = s.Idxcfg.CuvsCagra.ITopkSize
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

	// multiGpuSearch returns results in ascending order (nearest first); -1 marks empty slots.
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
			metric.MetricType(s.Idxcfg.CuvsCagra.Metric),
		))
	}

	return reskeys, resdistances, nil
}

// SearchFloat32 implements cache.VectorIndexSearchIf.
// Writes results directly into caller-provided slices to avoid heap allocation.
func (s *CagraSearch[T]) SearchFloat32(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	keys, dists, err := s.Search(proc, query, rt)
	if err != nil {
		return err
	}
	if keys == nil {
		return nil
	}
	ks, ok := keys.([]int64)
	if !ok {
		return moerr.NewInternalErrorNoCtx("CagraSearch: unknown keys type")
	}
	copy(outKeys, ks)
	for i, d := range dists {
		outDists[i] = float32(d)
	}
	return nil
}

// addOverflowFilterChunks demuxes the per-shard row-major INCLUDE byte stream
// into per-column data + null bitmap and feeds them to the brute-force index
// in column order. Mirrors how the build path populates the cuvs main
// index's FilterStore.
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

// Load implements cache.VectorIndexSearchIf: loads metadata then index data from the database.
func (s *CagraSearch[T]) Load(sqlproc *sqlexec.SqlProcess) error {
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

// loadCdcTail loads the tag=1 event-log rows persisted by CDC under the
// fixed vectorindex.CdcTailId sentinel, replays them to derive the
// (deleted, overflow) state, applies the deletes to every loaded sub-index
// (DeleteIds is idempotent on unknown pkids), and appends a synthetic model
// carrying the overflow to s.Indexes (Index=nil — the cdc_tail has no
// tag=0). The existing buildOverflow / buildMultiIndex paths skip nil-Index
// entries, so the synthetic model only contributes its overflow/deletes.
//
// includeBytesPerRow comes from the first sub-index that successfully
// loaded; cdc_tail's INSERT records share the col-meta layout with the main
// index by construction (CDC writer side is fed the same colMetaJSON). If
// no sub-index loaded (empty index — never built, or built and dropped),
// we have no col-meta and skip; cdc_tail data is moot without a main index.
func (s *CagraSearch[T]) loadCdcTail(sqlproc *sqlexec.SqlProcess) error {
	var (
		includeBytesPerRow int
		colMetaJSON        string
	)
	// Prefer a loaded sub-index's IncludeBytesPerRow + colMetaJSON —
	// the model file already carries them. Falls back to the
	// colMetaJSON embedded in the first tag=1 chunk's frame header
	// section (writer-side invariant of CdcAppendEventsSql) when no
	// sub-index exists for this index slice.
	for _, m := range s.Indexes {
		if m.Index != nil {
			includeBytesPerRow = m.IncludeBytesPerRow
			colMetaJSON = m.Index.GetFilterColMetaJSON()
			break
		}
	}

	stub := &CagraModel[T]{Id: vectorindex.CdcTailId}
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

	dim := int(s.Idxcfg.CuvsCagra.Dimensions)
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

	s.Indexes = append(s.Indexes, &CagraModel[T]{
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

// buildOverflow assembles a single GpuBruteForce index from the union of every
// loaded model's CDC insert overflow. Returns nil overflow when no model has
// any overflow records — keeps the existing fast path unchanged.
//
// When the underlying index has INCLUDE columns, the brute-force is set up
// with the matching FilterStore so a filtered query can prefilter overflow
// rows the same way the main cagra index does.
func (s *CagraSearch[T]) buildOverflow() error {
	total := uint64(0)
	for _, m := range s.Indexes {
		total += uint64(len(m.OverflowPkids))
	}
	if total == 0 {
		s.Overflow = nil
		return nil
	}

	cuvsMetric, ok := metric.MetricTypeToCuvsMetric[metric.MetricType(s.Idxcfg.CuvsCagra.Metric)]
	if !ok {
		return moerr.NewInternalErrorNoCtx("CagraSearch: unsupported metric type for overflow")
	}
	dim := uint32(s.Idxcfg.CuvsCagra.Dimensions)

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
	// model (every shard agrees by construction). Empty → no INCLUDE on this
	// index, leave the brute-force filter store empty. For small-data-only
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

// buildMultiIndex assembles a MultiGpuCagra from the loaded indexes.
// Returns nil when there is nothing to search — either no sub-indexes
// loaded AND no brute-force overflow built (empty index, small-data-only
// with no rows, etc.). The empty-MultiIndex case feeds into Search,
// which returns []int64{}, []float64{} on s.MultiIndex == nil — that's
// the load-bearing path for "no main index + no brute-force → empty
// result". Any future regression here will fail TestCagraSearchEmpty.
func (s *CagraSearch[T]) buildMultiIndex() *cuvs.MultiGpuCagra[T] {
	cuvsMetric, ok := metric.MetricTypeToCuvsMetric[metric.MetricType(s.Idxcfg.CuvsCagra.Metric)]
	if !ok {
		return nil
	}
	gpuIndices := make([]*cuvs.GpuCagra[T], 0, len(s.Indexes))
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
	dim := uint32(s.Idxcfg.CuvsCagra.Dimensions)
	return cuvs.NewMultiGpuCagra(gpuIndices, s.Overflow, dim, cuvsMetric)
}

// loadIndexes loads each model's index data from the database.
// On any error it destroys all partially-loaded indexes and returns the error.
func (s *CagraSearch[T]) loadIndexes(sqlproc *sqlexec.SqlProcess, indexes []*CagraModel[T]) ([]*CagraModel[T], error) {
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
func (s *CagraSearch[T]) Destroy() {
	s.MultiIndex = nil // does not own GPU resources; GpuCagra instances are owned by Indexes
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
func (s *CagraSearch[T]) UpdateConfig(newalgo cache.VectorIndexSearchIf) error {
	return nil
}
