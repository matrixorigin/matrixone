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
type IvfpqSearch[B, Q cuvs.VectorType] struct {
	Idxcfg        vectorindex.IndexConfig
	Tblcfg        vectorindex.IndexTableConfig
	Indexes       []*IvfpqModel[B, Q]
	MultiIndex    *cuvs.MultiGpuIvfPq[B, Q]
	Overflow      cuvs.BruteForceOverflow[B] // CDC insert overflow; nil when no overflow records exist
	Devices       []int
	ThreadsSearch int64
}

func NewIvfpqSearch[B, Q cuvs.VectorType](idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig, devices []int) *IvfpqSearch[B, Q] {
	nthread := vectorindex.GetConcurrency(tblcfg.ThreadsSearch)
	return &IvfpqSearch[B, Q]{
		Idxcfg:        idxcfg,
		Tblcfg:        tblcfg,
		Devices:       devices,
		ThreadsSearch: nthread,
	}
}

// Search implements cache.VectorIndexSearchIf.
func (s *IvfpqSearch[B, Q]) Search(sqlproc *sqlexec.SqlProcess, anyquery any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
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
		// Filtered: any base (f32 or vecf16) routes the native base-typed (B) query
		// through the const-B* search_quantize_with_filter path — cuVS converts B to
		// storage T (B==T copy for direct, learned-quantizer for compressed). The
		// query asserts to []B for both float32 (B==float) and Float16 (B==half) base.
		qB, ok := anyquery.([]B)
		if !ok {
			return nil, nil, moerr.NewInternalErrorNoCtx("IvfpqSearch: filtered query type mismatch")
		}
		neighbors64, dists32, err = s.MultiIndex.SearchQuantizeWithFilter(qB, 1, dim, uint32(limit), sp, rt.FilterJSON)
	} else if query, ok := anyquery.([]float32); ok {
		// f32 base, unfiltered (direct, or f32 base + QUANTIZATION quantized to T).
		neighbors64, dists32, err = s.MultiIndex.SearchFloat32(query, 1, dim, uint32(limit), sp)
	} else if qh, ok := anyquery.([]cuvs.Float16); ok {
		// vecf16 base (B == half), unfiltered.
		if qt, isT := anyquery.([]Q); isT {
			// f16-direct (storage T==Float16): native half search.
			neighbors64, dists32, err = s.MultiIndex.Search(qt, 1, dim, uint32(limit), sp)
		} else {
			// f16 -> int8/uint8 quantized: quantize the half query to T, native search.
			neighbors64, dists32, err = s.MultiIndex.SearchQuantizeHalf(qh, 1, dim, uint32(limit), sp)
		}
	} else {
		return nil, nil, moerr.NewInternalErrorNoCtx("IvfpqSearch: query type mismatch")
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
func (s *IvfpqSearch[B, Q]) SearchFloat32(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
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
func (s *IvfpqSearch[B, Q]) Load(sqlproc *sqlexec.SqlProcess) (err error) {
	indexes, err := LoadMetadata[B, Q](sqlproc, s.Tblcfg.DbName, s.Tblcfg.MetadataTable)
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
	// From here the GPU sub-indexes are owned by s. If a later step fails, the
	// cache drops the entry WITHOUT calling Destroy (see VectorIndexCache.Search),
	// and there is no finalizer, so release them here to avoid orphaning GPU
	// memory on every failed load. Destroy is idempotent and safe on partial state.
	defer func() {
		if err != nil {
			s.Destroy()
		}
	}()
	if err = s.loadCdcTail(sqlproc); err != nil {
		return err
	}
	if err = s.buildOverflow(); err != nil {
		return err
	}
	s.MultiIndex, err = s.buildMultiIndex()
	return err
}

// loadCdcTail mirrors cagra.CagraSearch.loadCdcTail — see that for the
// architectural commentary. Differs only in the IndexConfig type slot and
// the GpuIvfPq element type.
func (s *IvfpqSearch[B, Q]) loadCdcTail(sqlproc *sqlexec.SqlProcess) error {
	var (
		includeBytesPerRow int
		colMetaJSON        string
	)
	// Prefer a loaded sub-index's IncludeBytesPerRow + colMetaJSON.
	// Falls back to the colMetaJSON embedded in the first tag=1
	// chunk's frame header section when no sub-index exists for this
	// index slice.
	for _, m := range s.Indexes {
		if m.Index != nil {
			includeBytesPerRow = m.IncludeBytesPerRow
			colMetaJSON = m.Index.GetFilterColMetaJSON()
			break
		}
	}

	stub := &IvfpqModel[B, Q]{Id: vectorindex.CdcTailId}
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
	delPkids, ovPkids, ovVecs, ovInc, err := replayEventChunks[B](chunks, dim, includeBytesPerRow)
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

	s.Indexes = append(s.Indexes, &IvfpqModel[B, Q]{
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
func addOverflowFilterChunks[B, OB cuvs.VectorType](
	bf *cuvs.GpuBruteForce[B, OB],
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
func (s *IvfpqSearch[B, Q]) buildOverflow() error {
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

	// cuVS brute force can only store float/half. Pick the overflow storage type
	// OB from the index storage Q: keep Q when float/half, else fall back to base
	// B (which is always float/half). The type-erased BruteForceOverflow[B] holds
	// either concrete *GpuBruteForce[B, OB].
	var (
		ov  cuvs.BruteForceOverflow[B]
		err error
	)
	switch cuvs.GetQuantization[Q]() {
	case cuvs.F32, cuvs.F16:
		ov, err = buildOverflowBF[B, Q](s.Indexes, total, dim, cuvsMetric, device, uint32(s.ThreadsSearch))
	default: // INT8/UINT8: brute force can't store these → store base B.
		ov, err = buildOverflowBF[B, B](s.Indexes, total, dim, cuvsMetric, device, uint32(s.ThreadsSearch))
	}
	if err != nil {
		return err
	}
	s.Overflow = ov
	return nil
}

// buildOverflowBF — see cagra/search_gpu.go.
func buildOverflowBF[B, OB cuvs.VectorType, Q cuvs.VectorType](
	indexes []*IvfpqModel[B, Q],
	total uint64, dim uint32, cuvsMetric cuvs.DistanceType, device int, threads uint32,
) (cuvs.BruteForceOverflow[B], error) {
	bf, err := cuvs.NewGpuBruteForceEmpty[B, OB](total, dim, cuvsMetric, threads, device)
	if err != nil {
		return nil, err
	}
	if err = bf.Start(); err != nil {
		bf.Destroy()
		return nil, err
	}

	var (
		colMetaJSON        string
		includeBytesPerRow int
	)
	for _, m := range indexes {
		if m.Index != nil {
			colMetaJSON = m.Index.GetFilterColMetaJSON()
			includeBytesPerRow = m.IncludeBytesPerRow
			break
		}
	}
	if colMetaJSON == "" {
		for _, m := range indexes {
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
			return nil, err
		}
	}

	for _, m := range indexes {
		if len(m.OverflowPkids) == 0 {
			continue
		}
		count := uint64(len(m.OverflowPkids))
		// Base-typed (B) overflow vectors; AddChunkQuantize converts B -> Q storage
		// on the C++ side (native store when B==Q, f32->f16 cast otherwise).
		if err = bf.AddChunkQuantize(m.OverflowVecs, count, m.OverflowPkids); err != nil {
			bf.Destroy()
			return nil, err
		}
		if colMetaJSON != "" && includeBytesPerRow > 0 {
			if err = addOverflowFilterChunks(bf, colMetaJSON, m.OverflowIncludeBytes, count, includeBytesPerRow); err != nil {
				bf.Destroy()
				return nil, err
			}
		}
	}
	if err = bf.Build(); err != nil {
		bf.Destroy()
		return nil, err
	}
	return bf, nil
}

// buildMultiIndex assembles a MultiGpuIvfPq from the loaded indexes.
// Returns nil when there is nothing to search — either no sub-indexes
// loaded AND no brute-force overflow built. The empty-MultiIndex case
// feeds into Search, which returns []int64{}, []float64{} on
// s.MultiIndex == nil — that's the load-bearing path for "no main
// index + no brute-force → empty result". Any future regression here
// will fail TestIvfpqSearchEmpty.
func (s *IvfpqSearch[B, Q]) buildMultiIndex() (*cuvs.MultiGpuIvfPq[B, Q], error) {
	cuvsMetric, ok := metric.MetricTypeToCuvsMetric[metric.MetricType(s.Idxcfg.CuvsIvfpq.Metric)]
	if !ok {
		// Unsupported metric is a real error — surface it rather than returning a
		// nil index, which Search would treat as an (empty) success.
		return nil, moerr.NewInternalErrorNoCtxf("IvfpqSearch: unsupported metric type %v", s.Idxcfg.CuvsIvfpq.Metric)
	}
	gpuIndices := make([]*cuvs.GpuIvfPq[B, Q], 0, len(s.Indexes))
	for _, model := range s.Indexes {
		if model.Index != nil {
			gpuIndices = append(gpuIndices, model.Index)
		}
	}
	if len(gpuIndices) == 0 && s.Overflow == nil {
		// Empty index: no sub-indexes AND no brute-force overflow. Not an error —
		// Search returns an empty result via its nil-MultiIndex guard.
		return nil, nil
	}
	dim := uint32(s.Idxcfg.CuvsIvfpq.Dimensions)
	return cuvs.NewMultiGpuIvfPq(gpuIndices, s.Overflow, dim, cuvsMetric), nil
}

// loadIndexes loads each model's index data from the database.
func (s *IvfpqSearch[B, Q]) loadIndexes(sqlproc *sqlexec.SqlProcess, indexes []*IvfpqModel[B, Q]) ([]*IvfpqModel[B, Q], error) {
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
func (s *IvfpqSearch[B, Q]) Destroy() {
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
func (s *IvfpqSearch[B, Q]) UpdateConfig(newalgo cache.VectorIndexSearchIf) error {
	return nil
}
