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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/indexplugin/search/planreader"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/overfetch"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
	usearch "github.com/unum-cloud/usearch/golang"
)

// recordingSearch is a cached index whose searches return keys 1 and 2 and
// record the configuration and runtime limit they ran with.
type recordingSearch struct {
	idxcfg vectorindex.IndexConfig
	tblcfg vectorindex.IndexTableConfig
	limits *[]uint
}

func (m *recordingSearch) Search(_ *sqlexec.SqlProcess, _ any, rt vectorindex.RuntimeConfig) (any, []float64, error) {
	*m.limits = append(*m.limits, rt.Limit)
	return []int64{1, 2}, []float64{0.5, 1.5}, nil
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

// stubAlgo replaces newAlgo for the test and returns the recorded searches.
func stubAlgo(t *testing.T) (*[]uint, *[]*recordingSearch) {
	limits := new([]uint)
	created := new([]*recordingSearch)
	saved := newAlgo
	newAlgo = func(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) veccache.VectorIndexSearchIf {
		s := &recordingSearch{idxcfg: idxcfg, tblcfg: tblcfg, limits: limits}
		*created = append(*created, s)
		return s
	}
	t.Cleanup(func() { newAlgo = saved })
	return limits, created
}

func hnswSpec(indexTable string) *plan.IndexSearchScan {
	return &plan.IndexSearchScan{
		SourceTable:      &plan.ObjectRef{SchemaName: "db", ObjName: "t"},
		SourceTableDef:   &plan.TableDef{Name: "t"},
		Index:            &plan.IndexDef{IndexAlgo: catalog.MoIndexHnswAlgo.ToString(), IndexAlgoParams: `{"op_type":"vector_l2_ops","m":"16","ef_construction":"64","ef_search":"40"}`},
		DistanceFunction: "l2_distance",
		AlgoOptions:      []byte(`{"threads_search":3}`),
		HiddenTables: []*plan.IndexHiddenTableRef{
			{Role: catalog.Hnsw_TblType_Metadata, Object: &plan.ObjectRef{ObjName: indexTable + "_meta"}},
			{Role: catalog.Hnsw_TblType_Storage, Object: &plan.ObjectRef{ObjName: indexTable}},
		},
	}
}

func f32Request(query []float32, budget uint64) searchplugin.Request {
	return searchplugin.Request{
		QueryPayload:    types.ArrayToBytes(query),
		QueryType:       plan.Type{Id: int32(types.T_array_float32), Width: int32(len(query))},
		ResultLimit:     budget,
		CandidateBudget: budget,
	}
}

func TestHnswReaderSearchesTheCachedIndex(t *testing.T) {
	limits, created := stubAlgo(t)
	proc := testutil.NewProc(t)
	reader, err := Hooks{}.NewReader(proc, hnswSpec(t.Name()), f32Request([]float32{1, 2, 3}, 5))
	require.NoError(t, err)
	defer reader.Close()

	attrs := []string{planreader.KeyColumn, planreader.ScoreColumn}
	out := batch.NewWithSize(2)
	out.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	out.Vecs[1] = vector.NewVec(types.T_float64.ToType())
	defer out.Clean(proc.Mp())
	done, err := reader.Read(context.Background(), attrs, nil, proc.Mp(), out)
	require.NoError(t, err)
	require.False(t, done)
	require.Equal(t, []int64{1, 2}, vector.MustFixedColNoTypeCheck[int64](out.Vecs[0]))
	require.Equal(t, []float64{0.5, 1.5}, vector.MustFixedColNoTypeCheck[float64](out.Vecs[1]))
	done, err = reader.Read(context.Background(), attrs, nil, proc.Mp(), out)
	require.NoError(t, err)
	require.True(t, done)

	require.Equal(t, []uint{5}, *limits)
	require.Len(t, *created, 1)
	got := (*created)[0]
	require.Equal(t, vectorindex.IndexTableConfig{
		DbName: "db", SrcTable: "t", MetadataTable: t.Name() + "_meta", IndexTable: t.Name(),
		OrigFuncName: "l2_distance", ThreadsSearch: 3,
	}, got.tblcfg)
	require.Equal(t, usearch.F32, got.idxcfg.Usearch.Quantization)
	require.Equal(t, uint(3), got.idxcfg.Usearch.Dimensions)
	require.Equal(t, uint(16), got.idxcfg.Usearch.Connectivity)
	require.Equal(t, uint(64), got.idxcfg.Usearch.ExpansionAdd)
	require.Equal(t, uint(40), got.idxcfg.Usearch.ExpansionSearch)
	require.Equal(t, vectorindex.HNSW, got.idxcfg.Type)
}

func TestHnswReaderFloat64AndEmptyBudget(t *testing.T) {
	limits, created := stubAlgo(t)
	proc := testutil.NewProc(t)
	req := searchplugin.Request{
		QueryPayload:    types.ArrayToBytes([]float64{1, 2}),
		QueryType:       plan.Type{Id: int32(types.T_array_float64), Width: 2},
		CandidateBudget: 4,
	}
	s, err := newSearcher(proc, hnswSpec(t.Name()), req)
	require.NoError(t, err)
	chunk, more, err := s.Next(context.Background())
	require.NoError(t, err)
	require.False(t, more)
	require.Equal(t, []int64{1, 2}, chunk.Keys)
	require.Equal(t, usearch.F64, (*created)[0].idxcfg.Usearch.Quantization)
	require.NoError(t, s.Close())

	// a zero budget searches nothing
	empty, err := newSearcher(proc, hnswSpec(t.Name()+"_empty"), f32Request([]float32{1}, 0))
	require.NoError(t, err)
	chunk, more, err = empty.Next(context.Background())
	require.NoError(t, err)
	require.False(t, more)
	require.Nil(t, chunk.Keys)
	require.Equal(t, []uint{4}, *limits)

	canceled, err := newSearcher(proc, hnswSpec(t.Name()+"_canceled"), f32Request([]float32{1}, 1))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = canceled.Next(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestHnswReaderRejectsInvalidScans(t *testing.T) {
	stubAlgo(t)
	proc := testutil.NewProc(t)
	_, err := Hooks{}.NewReader(nil, hnswSpec(t.Name()), f32Request([]float32{1}, 1))
	require.ErrorContains(t, err, "requires a process")
	_, err = Hooks{}.NewReader(proc, &plan.IndexSearchScan{}, f32Request([]float32{1}, 1))
	require.ErrorContains(t, err, "missing source or index metadata")

	for name, change := range map[string]func(*plan.IndexSearchScan){
		"op_type":   func(s *plan.IndexSearchScan) { s.Index.IndexAlgoParams = `{"op_type":"vector_none"}` },
		"m":         func(s *plan.IndexSearchScan) { s.Index.IndexAlgoParams = `{"op_type":"vector_l2_ops","m":"x"}` },
		"json":      func(s *plan.IndexSearchScan) { s.Index.IndexAlgoParams = `{` },
		"options":   func(s *plan.IndexSearchScan) { s.AlgoOptions = []byte(`{`) },
		"no tables": func(s *plan.IndexSearchScan) { s.HiddenTables = s.HiddenTables[:1] },
	} {
		spec := hnswSpec(t.Name())
		change(spec)
		_, err = Hooks{}.NewReader(proc, spec, f32Request([]float32{1}, 1))
		require.Error(t, err, name)
	}

	intQuery := f32Request([]float32{1}, 1)
	intQuery.QueryType = plan.Type{Id: int32(types.T_int64)}
	_, err = Hooks{}.NewReader(proc, hnswSpec(t.Name()), intQuery)
	require.Error(t, err)

	// the query dimension must match the column's
	mismatch := f32Request([]float32{1, 2}, 1)
	mismatch.QueryType.Width = 3
	s, err := newSearcher(proc, hnswSpec(t.Name()), mismatch)
	require.NoError(t, err)
	_, _, err = s.Next(context.Background())
	require.ErrorContains(t, err, "different dimensions (3, 2)")
}

func TestHnswPostFilterBudgetIsPostFilterLimit(t *testing.T) {
	for _, k := range []uint64{1, 2, 10, 100, 1000} {
		require.Equal(t, overfetch.PostFilterLimit(k), Hooks{}.PostFilterCandidateBudget(k))
	}
}
