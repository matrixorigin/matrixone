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

package search

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw"
	"github.com/stretchr/testify/require"
	usearch "github.com/unum-cloud/usearch/golang"
)

// The searcher stores the index's QUANTIZATION and searches with the query's base type.
func TestNewSearcherQuantization(t *testing.T) {
	proc := testutil.NewProc(t)

	spec := hnswSpec("idx")
	spec.Index.IndexAlgoParams = `{"op_type":"vector_l2_ops","quantization":"float16"}`
	s, err := newSearcher(proc, spec, f32Request([]float32{1, 2}, 10))
	require.NoError(t, err)
	require.Equal(t, usearch.F16, s.idxcfg.Usearch.Quantization)
	require.Equal(t, int32(types.T_array_float32), s.queryType)

	f64 := f32Request([]float32{1, 2}, 10)
	f64.QueryPayload = types.ArrayToBytes([]float64{1, 2})
	f64.QueryType = plan.Type{Id: int32(types.T_array_float64), Width: 2}
	spec = hnswSpec("idx")
	s, err = newSearcher(proc, spec, f64)
	require.NoError(t, err)
	require.Equal(t, usearch.F64, s.idxcfg.Usearch.Quantization, "no QUANTIZATION stores the base type")
	require.Equal(t, int32(types.T_array_float64), s.queryType)

	spec = hnswSpec("idx")
	spec.Index.IndexAlgoParams = `{"op_type":"vector_l2_ops","quantization":"float32"}`
	s, err = newSearcher(proc, spec, f64)
	require.NoError(t, err)
	require.Equal(t, usearch.F32, s.idxcfg.Usearch.Quantization)
	require.Equal(t, int32(types.T_array_float64), s.queryType, "a float64 base is searched with float64 queries whatever it stores")

	spec = hnswSpec("idx")
	spec.Index.IndexAlgoParams = `{"op_type":"vector_l2_ops","quantization":"int4"}`
	_, err = newSearcher(proc, spec, f32Request([]float32{1, 2}, 10))
	require.Error(t, err)
}

// A query is searched in its own element type.
func TestNewAlgoQueryType(t *testing.T) {
	var idxcfg vectorindex.IndexConfig
	var tblcfg vectorindex.IndexTableConfig
	for typ, want := range map[types.T]any{
		types.T_array_float32: (*hnsw.HnswSearch[float32])(nil),
		types.T_array_float64: (*hnsw.HnswSearch[float64])(nil),
		types.T_array_float16: (*hnsw.HnswSearch[types.Float16])(nil),
		types.T_array_bf16:    (*hnsw.HnswSearch[types.BF16])(nil),
		types.T_array_int8:    (*hnsw.HnswSearch[int8])(nil),
		types.T_array_uint8:   (*hnsw.HnswSearch[uint8])(nil),
	} {
		require.IsType(t, want, newAlgo(idxcfg, tblcfg, int32(typ)), typ.String())
	}
}

// A narrow query is searched in its own element type; the searcher learns the dict table and
// the indexed column's type from the scan.
func TestNarrowQuerySearch(t *testing.T) {
	_, created := stubAlgo(t)
	proc := testutil.NewProc(t)
	for _, c := range []struct {
		typ     types.T
		payload []byte
	}{
		{types.T_array_float16, types.ArrayToBytes(types.Float32ToFloat16Slice([]float32{1, 2}))},
		{types.T_array_bf16, types.ArrayToBytes([]types.BF16{types.BF16FromFloat32(1), types.BF16FromFloat32(2)})},
		{types.T_array_int8, types.ArrayToBytes([]int8{1, 2})},
		{types.T_array_uint8, types.ArrayToBytes([]uint8{1, 2})},
	} {
		spec := hnswSpec(t.Name() + c.typ.String())
		spec.Index.Parts = []string{"v"}
		spec.SourceTableDef = &plan.TableDef{Name: "t", Cols: []*plan.ColDef{{Name: "v", Typ: plan.Type{Id: int32(c.typ), Width: 2}}},
			Name2ColIndex: map[string]int32{"v": 0}}
		spec.HiddenTables = append(spec.HiddenTables, &plan.IndexHiddenTableRef{Role: catalog.Hnsw_TblType_Dict, Object: &plan.ObjectRef{ObjName: "dict"}})
		req := searchplugin.Request{QueryPayload: c.payload, QueryType: plan.Type{Id: int32(c.typ), Width: 2}, CandidateBudget: 2}
		s, err := newSearcher(proc, spec, req)
		require.NoError(t, err, c.typ.String())
		require.Equal(t, int32(c.typ), s.queryType)
		_, _, err = s.Next(context.Background())
		require.NoError(t, err, c.typ.String())
		got := (*created)[len(*created)-1].tblcfg
		require.Equal(t, "dict", got.DictTable)
		require.Equal(t, int32(c.typ), got.KeyPartType)
		require.NoError(t, s.Close())
	}

	// a query of a non-vector type is rejected
	req := searchplugin.Request{QueryPayload: []byte{1, 0, 0, 0, 0, 0, 0, 0}, QueryType: plan.Type{Id: int32(types.T_int64), Width: 1}, CandidateBudget: 1}
	_, err := newSearcher(proc, hnswSpec(t.Name()+"_bad"), req)
	require.Error(t, err)
}
