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
		return []uint32{}, []float64{}, nil
	}

	dim := uint32(s.Idxcfg.CuvsCagra.Dimensions)
	sp := cuvs.DefaultCagraSearchParams()
	neighbors64, dists32, err := s.MultiIndex.SearchFloat32(query, 1, dim, uint32(limit), sp)
	if err != nil {
		return nil, nil, err
	}

	// multiGpuSearch returns results in ascending order (nearest first); -1 marks empty slots.
	reskeys := make([]uint32, 0, limit)
	resdistances := make([]float64, 0, limit)
	for i, k := range neighbors64 {
		if k == -1 {
			continue
		}
		reskeys = append(reskeys, uint32(k))
		resdistances = append(resdistances, metric.DistanceTransformIvfflat(
			float64(dists32[i]),
			metric.DistFuncNameToMetricType[rt.OrigFuncName],
			metric.MetricType(s.Idxcfg.CuvsCagra.Metric),
		))
	}

	return reskeys, resdistances, nil
}

// SearchFloat32 implements cache.VectorIndexSearchIf (int64 key variant — not used by CAGRA).
func (s *CagraSearch[T]) SearchFloat32(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	return moerr.NewInternalErrorNoCtx("CagraSearch: use SearchFloat32WithKeyUint32 for uint32 keys")
}

// SearchFloat32WithKeyUint32 implements cache.VectorIndexSearchIf.
// Writes results directly into caller-provided slices to avoid heap allocation.
func (s *CagraSearch[T]) SearchFloat32WithKeyUint32(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, outKeys []uint32, outDists []float32) error {
	keys, dists, err := s.Search(proc, query, rt)
	if err != nil {
		return err
	}
	if keys == nil {
		return nil
	}
	ks, ok := keys.([]uint32)
	if !ok {
		return moerr.NewInternalErrorNoCtx("CagraSearch: unknown keys type")
	}
	copy(outKeys, ks)
	for i, d := range dists {
		outDists[i] = float32(d)
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
	s.MultiIndex = s.buildMultiIndex()
	return nil
}

// buildMultiIndex assembles a MultiGpuCagra from the loaded indexes.
// Returns nil when no indexes are ready (empty or all Index fields are nil).
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
	if len(gpuIndices) == 0 {
		return nil
	}
	dim := uint32(s.Idxcfg.CuvsCagra.Dimensions)
	return cuvs.NewMultiGpuCagra(gpuIndices, nil, dim, cuvsMetric)
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
	for _, idx := range s.Indexes {
		idx.Destroy()
	}
	s.Indexes = nil
}

// UpdateConfig implements cache.VectorIndexSearchIf.
func (s *CagraSearch[T]) UpdateConfig(newalgo cache.VectorIndexSearchIf) error {
	return nil
}
