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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	ivfpqplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/overfetch"
	"github.com/stretchr/testify/require"
)

func ivfpqSpec(t *testing.T, partType types.T, algoParams string) *plan.IndexSearchScan {
	opts, err := ivfpqplan.EncodeScanOptions(ivfpqplan.ScanOptions{
		ThreadsSearch: 3, BatchWindow: 64, GpuMultiSimulation: 2, Nprobe: 7, KeyPartType: int32(partType), FilterJSON: `[]`,
	})
	require.NoError(t, err)
	return &plan.IndexSearchScan{
		SourceTable:      &plan.ObjectRef{SchemaName: "db"},
		SourceTableDef:   &plan.TableDef{Name: "src"},
		Index:            &plan.IndexDef{IndexAlgoParams: algoParams},
		DistanceFunction: "l2_distance",
		AlgoOptions:      opts,
		HiddenTables: []*plan.IndexHiddenTableRef{
			{Role: catalog.Ivfpq_TblType_Metadata, Object: &plan.ObjectRef{ObjName: "meta"}},
			{Role: catalog.Ivfpq_TblType_Storage, Object: &plan.ObjectRef{ObjName: "idx"}},
		},
	}
}

func ivfpqRequest(queryType types.T, dims int32) searchplugin.Request {
	return searchplugin.Request{
		QueryPayload:    []byte{1, 2, 3, 4},
		QueryType:       plan.Type{Id: int32(queryType), Width: dims},
		CandidateBudget: 5,
	}
}

func TestNewSearchBuildsConfig(t *testing.T) {
	proc := testutil.NewProc(t)
	s, err := newSearch(proc, ivfpqSpec(t, types.T_array_float32, `{"op_type":"vector_l2_ops","quantization":"int8"}`),
		ivfpqRequest(types.T_array_float32, 3))
	require.NoError(t, err)
	require.Equal(t, uint(3), s.idxcfg.CuvsIvfpq.Dimensions)
	require.Equal(t, uint16(metric.Quantization_INT8), s.idxcfg.CuvsIvfpq.Quantization)
	require.Equal(t, "db", s.tblcfg.DbName)
	require.Equal(t, "src", s.tblcfg.SrcTable)
	require.Equal(t, "meta", s.tblcfg.MetadataTable)
	require.Equal(t, "idx", s.tblcfg.IndexTable)
	require.Equal(t, "l2_distance", s.tblcfg.OrigFuncName)
	require.Equal(t, int64(3), s.tblcfg.ThreadsSearch)
	require.Equal(t, int64(64), s.tblcfg.BatchWindow)
	require.Equal(t, int64(2), s.tblcfg.GpuMultiSimulation)
	require.Equal(t, int32(types.T_array_float32), s.tblcfg.KeyPartType)
	require.Equal(t, uint64(5), s.limit)

	s, err = newSearch(proc, ivfpqSpec(t, types.T_array_float32, `{"op_type":"vector_l2_ops"}`),
		searchplugin.Request{QueryType: plan.Type{Id: int32(types.T_array_float32), Width: 3}})
	require.NoError(t, err)
	require.Equal(t, uint64(1), s.limit)
	require.Equal(t, "[]", s.filterJSON)
	require.Equal(t, uint(7), s.tblcfg.Nprobe)
}

func TestNewSearchF16StoresHalf(t *testing.T) {
	proc := testutil.NewProc(t)
	s, err := newSearch(proc, ivfpqSpec(t, types.T_array_float16, `{"op_type":"vector_l2_ops"}`),
		ivfpqRequest(types.T_array_float16, 2))
	require.NoError(t, err)
	require.Equal(t, uint16(metric.Quantization_F16), s.idxcfg.CuvsIvfpq.Quantization)

	s, err = newSearch(proc, ivfpqSpec(t, types.T_array_float16, `{"op_type":"vector_l2_ops","quantization":"int8"}`),
		ivfpqRequest(types.T_array_float16, 2))
	require.NoError(t, err)
	require.Equal(t, uint16(metric.Quantization_INT8), s.idxcfg.CuvsIvfpq.Quantization)
}

func TestNewSearchRejectsInvalidScans(t *testing.T) {
	proc := testutil.NewProc(t)
	params := `{"op_type":"vector_l2_ops"}`
	req := ivfpqRequest(types.T_array_float32, 1)
	_, err := newSearch(nil, ivfpqSpec(t, types.T_array_float32, params), req)
	require.ErrorContains(t, err, "requires a process")
	_, err = newSearch(proc, &plan.IndexSearchScan{}, req)
	require.ErrorContains(t, err, "missing source or index metadata")

	for name, change := range map[string]func(*plan.IndexSearchScan){
		"op_type":   func(s *plan.IndexSearchScan) { s.Index.IndexAlgoParams = `{"op_type":"vector_none"}` },
		"json":      func(s *plan.IndexSearchScan) { s.Index.IndexAlgoParams = `{` },
		"options":   func(s *plan.IndexSearchScan) { s.AlgoOptions = []byte(`{`) },
		"no tables": func(s *plan.IndexSearchScan) { s.HiddenTables = s.HiddenTables[:1] },
	} {
		spec := ivfpqSpec(t, types.T_array_float32, params)
		change(spec)
		_, err = newSearch(proc, spec, req)
		require.Error(t, err, name)
	}

	_, err = newSearch(proc, ivfpqSpec(t, types.T_array_float32, params), ivfpqRequest(types.T_array_float16, 1))
	require.ErrorContains(t, err, "does not match the index base column type")
	_, err = newSearch(proc, ivfpqSpec(t, types.T_array_float32, params), ivfpqRequest(types.T_array_float64, 1))
	require.ErrorContains(t, err, "must be a float32 array")
}

func TestPostFilterBudgetIsPostFilterLimit(t *testing.T) {
	for _, k := range []uint64{1, 2, 10, 100, 1000} {
		require.Equal(t, overfetch.PostFilterLimit(k), Hooks{}.PostFilterCandidateBudget(k))
	}
}
