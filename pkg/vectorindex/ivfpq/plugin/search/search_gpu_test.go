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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// recordingSearch is a cached index whose searches return keys, and record the
// query and runtime configuration they ran with.
type recordingSearch struct {
	keys    any
	err     error
	queries *[]any
	rts     *[]vectorindex.RuntimeConfig
}

func (m *recordingSearch) Search(_ *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (any, []float64, error) {
	*m.queries = append(*m.queries, query)
	*m.rts = append(*m.rts, rt)
	if m.err != nil {
		return nil, nil, m.err
	}
	return m.keys, []float64{0.5, 1.5}, nil
}
func (m *recordingSearch) SearchFloat32(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, []int64, []float32) error {
	return nil
}
func (m *recordingSearch) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	return nil
}
func (m *recordingSearch) Preload(*sqlexec.SqlProcess) error { return nil }
func (m *recordingSearch) GetIndexSize() (int64, int64)      { return 0, 0 }
func (m *recordingSearch) Destroy()                          {}
func (m *recordingSearch) Load(*sqlexec.SqlProcess) error    { return nil }
func (m *recordingSearch) BuildTS() int64                    { return 0 }

// stubAlgo replaces newAlgo for the test; searches return keys or err.
func stubAlgo(t *testing.T, keys any, err error) (*[]any, *[]vectorindex.RuntimeConfig) {
	queries := new([]any)
	rts := new([]vectorindex.RuntimeConfig)
	saved := newAlgo
	newAlgo = func(vectorindex.IndexConfig, vectorindex.IndexTableConfig) veccache.VectorIndexSearchIf {
		return &recordingSearch{keys: keys, err: err, queries: queries, rts: rts}
	}
	t.Cleanup(func() { newAlgo = saved })
	return queries, rts
}

// searchSeq names the cache entry of each searchFor search.
var searchSeq int

// searchFor returns a search of a dims-wide query of queryType over a fresh
// cache entry.
func searchFor(t *testing.T, queryType types.T, dims int32) *search {
	searchSeq++
	proc := testutil.NewProc(t)
	spec := ivfpqSpec(t, queryType, `{"op_type":"vector_l2_ops"}`)
	spec.HiddenTables[1].Object.ObjName = fmt.Sprintf("%s_%d", t.Name(), searchSeq)
	req := ivfpqRequest(queryType, dims)
	if queryType == types.T_array_float16 {
		req.QueryPayload = types.ArrayToBytes([]types.Float16{1, 2, 3})
	} else {
		req.QueryPayload = types.ArrayToBytes([]float32{1, 2, 3})
	}
	s, err := newSearch(proc, spec, req)
	require.NoError(t, err)
	return s
}

func TestIvfpqGpuSearcherSearchesTheCachedIndex(t *testing.T) {
	queries, rts := stubAlgo(t, []int64{1, 2}, nil)
	r, err := newReader(searchFor(t, types.T_array_float32, 3))
	require.NoError(t, err)
	require.NoError(t, r.Close())

	chunk, more, err := (&gpuSearcher{search: searchFor(t, types.T_array_float32, 3)}).Next(context.Background())
	require.NoError(t, err)
	require.False(t, more)
	require.Equal(t, []int64{1, 2}, chunk.Keys)
	require.Equal(t, []float64{0.5, 1.5}, chunk.Scores)
	require.Equal(t, []float32{1, 2, 3}, (*queries)[0])
	rt := (*rts)[0]
	require.Equal(t, uint(5), rt.Limit)
	require.Equal(t, "l2_distance", rt.OrigFuncName)
	require.Equal(t, "[]", rt.FilterJSON)
	require.Equal(t, uint(7), rt.Probe)
}

func TestIvfpqGpuSearcherDecodesHalfQueries(t *testing.T) {
	queries, _ := stubAlgo(t, []int64{1, 2}, nil)
	_, _, err := (&gpuSearcher{search: searchFor(t, types.T_array_float16, 3)}).Next(context.Background())
	require.NoError(t, err)
	require.Len(t, *queries, 1)
	require.Len(t, (*queries)[0], 3)
}

func TestIvfpqGpuSearcherRejects(t *testing.T) {
	stubAlgo(t, []int64{1}, nil)
	_, _, err := (&gpuSearcher{search: searchFor(t, types.T_array_float32, 4)}).Next(context.Background())
	require.ErrorContains(t, err, "different dimensions (4, 3)")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = (&gpuSearcher{search: searchFor(t, types.T_array_float32, 3)}).Next(ctx)
	require.ErrorIs(t, err, context.Canceled)

	stubAlgo(t, []any{int64(1)}, nil)
	_, _, err = (&gpuSearcher{search: searchFor(t, types.T_array_float32, 3)}).Next(context.Background())
	require.ErrorContains(t, err, "keys is not []int64")

	stubAlgo(t, nil, moerr.NewInternalErrorNoCtx("search failed"))
	_, _, err = (&gpuSearcher{search: searchFor(t, types.T_array_float32, 3)}).Next(context.Background())
	require.ErrorContains(t, err, "search failed")
	require.NoError(t, (&gpuSearcher{}).Close())
}

// newAlgo builds the cuVS search of every base and storage type pairing.
func TestIvfpqNewAlgoDispatchesOnBaseAndStorage(t *testing.T) {
	for _, base := range []types.T{types.T_array_float32, types.T_array_float16} {
		for _, q := range []metric.QuantizationType{metric.Quantization_F32, metric.Quantization_F16, metric.Quantization_INT8, metric.Quantization_UINT8} {
			var idxcfg vectorindex.IndexConfig
			idxcfg.CuvsIvfpq.Quantization = uint16(q)
			algo := newAlgo(idxcfg, vectorindex.IndexTableConfig{KeyPartType: int32(base)})
			require.NotNil(t, algo)
			algo.Destroy()
		}
	}
}
