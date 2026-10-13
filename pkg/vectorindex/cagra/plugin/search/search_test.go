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
	cagraplan "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/overfetch"
	"github.com/stretchr/testify/require"
)

func cagraSpec(t *testing.T, partType types.T, algoParams string) *plan.IndexSearchScan {
	opts, err := cagraplan.EncodeScanOptions(cagraplan.ScanOptions{
		ThreadsSearch: 3, BatchWindow: 64, GpuMultiSimulation: 2, KeyPartType: int32(partType), FilterJSON: `[]`,
	})
	require.NoError(t, err)
	return &plan.IndexSearchScan{
		SourceTable:      &plan.ObjectRef{SchemaName: "db"},
		SourceTableDef:   &plan.TableDef{Name: "src"},
		Index:            &plan.IndexDef{IndexAlgoParams: algoParams},
		DistanceFunction: "l2_distance",
		AlgoOptions:      opts,
		HiddenTables: []*plan.IndexHiddenTableRef{
			{Role: catalog.Cagra_TblType_Metadata, Object: &plan.ObjectRef{ObjName: "meta"}},
			{Role: catalog.Cagra_TblType_Storage, Object: &plan.ObjectRef{ObjName: "idx"}},
		},
	}
}

func cagraRequest(queryType types.T, dims int32) searchplugin.Request {
	return searchplugin.Request{
		QueryPayload:    []byte{1, 2, 3, 4},
		QueryType:       plan.Type{Id: int32(queryType), Width: dims},
		CandidateBudget: 5,
	}
}

func TestNewSearchBuildsConfig(t *testing.T) {
	proc := testutil.NewProc(t)
	s, err := newSearch(proc, cagraSpec(t, types.T_array_float32, `{"op_type":"vector_l2_ops","quantization":"int8"}`),
		cagraRequest(types.T_array_float32, 3))
	require.NoError(t, err)
	require.Equal(t, uint(3), s.idxcfg.CuvsCagra.Dimensions)
	require.Equal(t, uint16(metric.Quantization_INT8), s.idxcfg.CuvsCagra.Quantization)
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

	s, err = newSearch(proc, cagraSpec(t, types.T_array_float32, `{"op_type":"vector_l2_ops"}`),
		searchplugin.Request{QueryType: plan.Type{Id: int32(types.T_array_float32), Width: 3}})
	require.NoError(t, err)
	require.Equal(t, uint64(1), s.limit)
	require.Equal(t, "[]", s.filterJSON)
}

func TestNewSearchF16StoresHalf(t *testing.T) {
	proc := testutil.NewProc(t)
	s, err := newSearch(proc, cagraSpec(t, types.T_array_float16, `{"op_type":"vector_l2_ops"}`),
		cagraRequest(types.T_array_float16, 2))
	require.NoError(t, err)
	require.Equal(t, uint16(metric.Quantization_F16), s.idxcfg.CuvsCagra.Quantization)

	s, err = newSearch(proc, cagraSpec(t, types.T_array_float16, `{"op_type":"vector_l2_ops","quantization":"int8"}`),
		cagraRequest(types.T_array_float16, 2))
	require.NoError(t, err)
	require.Equal(t, uint16(metric.Quantization_INT8), s.idxcfg.CuvsCagra.Quantization)
}

func TestNewSearchRejectsInvalidScans(t *testing.T) {
	proc := testutil.NewProc(t)
	params := `{"op_type":"vector_l2_ops"}`
	req := cagraRequest(types.T_array_float32, 1)
	_, err := newSearch(nil, cagraSpec(t, types.T_array_float32, params), req)
	require.ErrorContains(t, err, "requires a process")
	_, err = newSearch(proc, &plan.IndexSearchScan{}, req)
	require.ErrorContains(t, err, "missing source or index metadata")

	for name, change := range map[string]func(*plan.IndexSearchScan){
		"op_type":   func(s *plan.IndexSearchScan) { s.Index.IndexAlgoParams = `{"op_type":"vector_none"}` },
		"json":      func(s *plan.IndexSearchScan) { s.Index.IndexAlgoParams = `{` },
		"options":   func(s *plan.IndexSearchScan) { s.AlgoOptions = []byte(`{`) },
		"no tables": func(s *plan.IndexSearchScan) { s.HiddenTables = s.HiddenTables[:1] },
	} {
		spec := cagraSpec(t, types.T_array_float32, params)
		change(spec)
		_, err = newSearch(proc, spec, req)
		require.Error(t, err, name)
	}

	_, err = newSearch(proc, cagraSpec(t, types.T_array_float32, params), cagraRequest(types.T_array_float16, 1))
	require.ErrorContains(t, err, "does not match the index base column type")
}

func TestPostFilterBudgetIsPostFilterLimit(t *testing.T) {
	for _, k := range []uint64{1, 2, 10, 100, 1000} {
		require.Equal(t, overfetch.PostFilterLimit(k), Hooks{}.PostFilterCandidateBudget(k))
	}
}

func TestExplainSettings(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts cagraplan.ScanOptions
		want []string
	}{
		{"zero", cagraplan.ScanOptions{}, []string{`ThreadsSearch: 0`, `BatchWindow: 0`, `GpuMultiSimulation: 0`, `KeyPartType: 0`}},
		{"ThreadsSearch", cagraplan.ScanOptions{ThreadsSearch: 8}, []string{`ThreadsSearch: 8`, `BatchWindow: 0`, `GpuMultiSimulation: 0`, `KeyPartType: 0`}},
		{"BatchWindow", cagraplan.ScanOptions{BatchWindow: 8}, []string{`ThreadsSearch: 0`, `BatchWindow: 8`, `GpuMultiSimulation: 0`, `KeyPartType: 0`}},
		{"GpuMultiSimulation", cagraplan.ScanOptions{GpuMultiSimulation: 8}, []string{`ThreadsSearch: 0`, `BatchWindow: 0`, `GpuMultiSimulation: 8`, `KeyPartType: 0`}},
		{"KeyPartType", cagraplan.ScanOptions{KeyPartType: 8}, []string{`ThreadsSearch: 0`, `BatchWindow: 0`, `GpuMultiSimulation: 0`, `KeyPartType: 8`}},
		{"maximum signed", cagraplan.ScanOptions{ThreadsSearch: 9223372036854775807, BatchWindow: 9223372036854775807, GpuMultiSimulation: 9223372036854775807, KeyPartType: 2147483647}, []string{"ThreadsSearch: 9223372036854775807", "BatchWindow: 9223372036854775807", "GpuMultiSimulation: 9223372036854775807", "KeyPartType: 2147483647"}},
		{"integer boundaries", cagraplan.ScanOptions{ThreadsSearch: -9223372036854775808, BatchWindow: -9223372036854775808, GpuMultiSimulation: -9223372036854775808, KeyPartType: -2147483648}, []string{`ThreadsSearch: -9223372036854775808`, `BatchWindow: -9223372036854775808`, `GpuMultiSimulation: -9223372036854775808`, `KeyPartType: -2147483648`}},
		{"empty array", cagraplan.ScanOptions{FilterJSON: `[]`}, []string{`ThreadsSearch: 0`, `BatchWindow: 0`, `GpuMultiSimulation: 0`, `KeyPartType: 0`, `Filter Predicates: []`}},
		{"predicates", cagraplan.ScanOptions{FilterJSON: ` [ {"col":-1,"op":"=","val":9007199254740993}, {"col":0,"op":"in","vals":[1,2]} ] `}, []string{`ThreadsSearch: 0`, `BatchWindow: 0`, `GpuMultiSimulation: 0`, `KeyPartType: 0`, `Filter Predicates:  [ {"col":-1,"op":"=","val":9007199254740993}, {"col":0,"op":"in","vals":[1,2]} ] `}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := cagraplan.EncodeScanOptions(tc.opts)
			require.NoError(t, err)
			before := append([]byte(nil), data...)
			spec := &plan.IndexSearchScan{AlgoOptions: data}
			got, err := Hooks{}.ExplainSettings(spec, true)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			compact, err := Hooks{}.ExplainSettings(spec, false)
			require.NoError(t, err)
			require.Nil(t, compact)
			require.Equal(t, before, spec.AlgoOptions)
		})
	}
	for _, spec := range []*plan.IndexSearchScan{nil, {}, {AlgoOptions: []byte(`{}`)}} {
		got, err := Hooks{}.ExplainSettings(spec, true)
		require.NoError(t, err)
		require.Equal(t, []string{`ThreadsSearch: 0`, `BatchWindow: 0`, `GpuMultiSimulation: 0`, `KeyPartType: 0`}, got)
	}
	malformed := &plan.IndexSearchScan{AlgoOptions: []byte(`{`)}
	got, err := Hooks{}.ExplainSettings(malformed, true)
	require.Error(t, err)
	require.Nil(t, got)
	// Compact output must not add decoding or reject metadata it previously ignored.
	got, err = Hooks{}.ExplainSettings(malformed, false)
	require.NoError(t, err)
	require.Nil(t, got)
}
