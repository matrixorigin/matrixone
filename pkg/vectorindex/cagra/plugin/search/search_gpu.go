//go:build gpu

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package search

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/indexplugin/search/planreader"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	cagraPkg "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func newReader(s *search) (engine.Reader, error) {
	return planreader.New(&gpuSearcher{search: s}), nil
}

// newAlgo creates the cuVS search of one query; it is replaced in tests.
var newAlgo = func(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) veccache.VectorIndexSearchIf {
	devices, _ := cuvs.GetGpuDeviceList()
	// test-only: mirror the build-side device simulation so search loads the same
	// SHARDED / REPLICATED topology. No-op when gpu_multi_simulation < 2.
	devices = vectorindex.SimulateDevices(devices, tblcfg.GpuMultiSimulation)
	// Dispatch on (base type B, storage type Q): indices store Q, overflow is B.
	q := metric.QuantizationType(idxcfg.CuvsCagra.Quantization)
	if types.T(tblcfg.KeyPartType) == types.T_array_float16 {
		switch q {
		case metric.Quantization_INT8:
			return cagraPkg.NewCagraSearch[cuvs.Float16, int8](idxcfg, tblcfg, devices)
		case metric.Quantization_UINT8:
			return cagraPkg.NewCagraSearch[cuvs.Float16, uint8](idxcfg, tblcfg, devices)
		default:
			return cagraPkg.NewCagraSearch[cuvs.Float16, cuvs.Float16](idxcfg, tblcfg, devices)
		}
	}
	switch q {
	case metric.Quantization_F16:
		return cagraPkg.NewCagraSearch[float32, cuvs.Float16](idxcfg, tblcfg, devices)
	case metric.Quantization_INT8:
		return cagraPkg.NewCagraSearch[float32, int8](idxcfg, tblcfg, devices)
	case metric.Quantization_UINT8:
		return cagraPkg.NewCagraSearch[float32, uint8](idxcfg, tblcfg, devices)
	default:
		return cagraPkg.NewCagraSearch[float32, float32](idxcfg, tblcfg, devices)
	}
}

type gpuSearcher struct {
	*search
}

// Next returns the whole result as one chunk.
func (s *gpuSearcher) Next(ctx context.Context) (planreader.Chunk, bool, error) {
	if err := ctx.Err(); err != nil {
		return planreader.Chunk{}, false, err
	}
	var query any
	var dims int
	if s.queryType == types.T_array_float16 {
		// A vecf16 query is decoded natively to half.
		h := types.BytesToArray[types.Float16](s.query)
		dims = len(h)
		if len(h) > 0 {
			query = unsafe.Slice((*cuvs.Float16)(unsafe.Pointer(&h[0])), len(h))
		}
	} else {
		f := types.BytesToArray[float32](s.query)
		dims, query = len(f), f
	}
	if uint(dims) != s.idxcfg.CuvsCagra.Dimensions {
		return planreader.Chunk{}, false, moerr.NewInvalidInput(s.proc.Ctx, fmt.Sprintf(
			"vector ops between different dimensions (%d, %d) is not permitted.", s.idxcfg.CuvsCagra.Dimensions, dims))
	}
	veccache.Cache.Once()
	// Named-snapshot search (#27927): the index-load SQL runs on a txn cloned at
	// the snapshot TS, and the cache key carries that TS.
	sp := sqlexec.NewSqlProcess(s.proc)
	cacheKey := s.tblcfg.IndexTable
	if ets := sp.ApplyScanSnapshot(s.snapshot); ets != nil {
		cacheKey = veccache.SnapshotKey(s.tblcfg.IndexTable, *ets)
	}
	rt := vectorindex.RuntimeConfig{
		Limit:        uint(s.limit),
		OrigFuncName: s.tblcfg.OrigFuncName,
		FilterJSON:   s.filterJSON,
	}
	keys, distances, err := veccache.Cache.Search(sp, cacheKey, newAlgo(s.idxcfg, s.tblcfg), query, rt)
	if err != nil {
		return planreader.Chunk{}, false, err
	}
	ids, ok := keys.([]int64)
	if !ok {
		return planreader.Chunk{}, false, moerr.NewInternalError(s.proc.Ctx, "keys is not []int64")
	}
	return planreader.Chunk{Keys: ids, Scores: distances}, false, nil
}

func (*gpuSearcher) Close() error { return nil }
