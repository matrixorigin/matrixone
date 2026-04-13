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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/concurrent"
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
	query, ok := anyquery.([]T)
	if !ok {
		return nil, nil, moerr.NewInternalErrorNoCtx("CagraSearch: query type mismatch")
	}

	limit := rt.Limit

	if len(s.Indexes) == 0 {
		return []int64{}, []float64{}, nil
	}

	// FastMaxHeapSafe keeps the K (=limit) nearest neighbours across all sub-indexes.
	// CAGRA distances are float32, so we use that type directly to avoid boxing.
	keysBuf := make([]int64, limit)
	distsBuf := make([]float32, limit)
	h := vectorindex.NewFastMaxHeapSafe[float32](int(limit), keysBuf, distsBuf)

	nthread := int(vectorindex.GetConcurrency(0))
	if nthread > len(s.Indexes) {
		nthread = len(s.Indexes)
	}

	exec := concurrent.NewThreadPoolExecutor(nthread)
	err = exec.Execute(sqlproc.GetContext(),
		len(s.Indexes),
		func(ctx context.Context, thread_id int, start, end int) error {
			subindex := s.Indexes[start:end]
			for j := range subindex {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				ikeys, idists, err2 := subindex[j].Search(query, limit)
				if err2 != nil {
					return err2
				}
				for k := range ikeys {
					h.Push(ikeys[k], idists[k])
				}
			}
			return nil
		})
	if err != nil {
		return nil, nil, err
	}

	reskeys := make([]int64, 0, limit)
	resdistances := make([]float64, 0, limit)

	for {
		key, dist, ok2 := h.Pop()
		if !ok2 {
			break
		}
		reskeys = append(reskeys, key)
		resdistances = append(resdistances, metric.DistanceTransformIvfflat(
			float64(dist),
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
	switch ks := keys.(type) {
	case []int64:
		copy(outKeys, ks)
	case []any:
		for i, k := range ks {
			outKeys[i] = k.(int64)
		}
	default:
		return moerr.NewInternalErrorNoCtx("CagraSearch: unknown keys type")
	}
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
	return nil
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
	for _, idx := range s.Indexes {
		idx.Destroy()
	}
	s.Indexes = nil
}

// UpdateConfig implements cache.VectorIndexSearchIf.
func (s *CagraSearch[T]) UpdateConfig(newalgo cache.VectorIndexSearchIf) error {
	return nil
}
