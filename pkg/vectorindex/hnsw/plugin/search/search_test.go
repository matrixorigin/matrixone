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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/indexplugin/search/planreader"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	hnswplan "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/overfetch"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	usearch "github.com/unum-cloud/usearch/golang"
)

// recordingSearch is a cached index whose searches return keys 1 and 2 and
// record the configuration and runtime limit they ran with.
type recordingSearch struct {
	idxcfg  vectorindex.IndexConfig
	tblcfg  vectorindex.IndexTableConfig
	limits  *[]uint
	buildTS int64
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
func (m *recordingSearch) BuildTS() int64                    { return m.buildTS }

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

	// a zero budget searches one candidate
	one, err := newSearcher(proc, hnswSpec(t.Name()+"_one"), f32Request([]float32{1}, 0))
	require.NoError(t, err)
	_, more, err = one.Next(context.Background())
	require.NoError(t, err)
	require.False(t, more)
	require.Equal(t, []uint{4, 1}, *limits)

	canceled, err := newSearcher(proc, hnswSpec(t.Name()+"_canceled"), f32Request([]float32{1}, 1))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := len(*limits)
	_, _, err = canceled.Next(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, *limits, before, "cancelled entry must not search the cache")
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

func TestExplainSettings(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts hnswplan.ScanOptions
		want []string
	}{
		{"zero", hnswplan.ScanOptions{}, []string{`ThreadsSearch: 0`}},
		{"ThreadsSearch", hnswplan.ScanOptions{ThreadsSearch: 8}, []string{`ThreadsSearch: 8`}},
		{"maximum signed", hnswplan.ScanOptions{ThreadsSearch: 9223372036854775807}, []string{"ThreadsSearch: 9223372036854775807"}},
		{"integer boundaries", hnswplan.ScanOptions{ThreadsSearch: -9223372036854775808}, []string{`ThreadsSearch: -9223372036854775808`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := hnswplan.EncodeScanOptions(tc.opts)
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
		require.Equal(t, []string{`ThreadsSearch: 0`}, got)
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

func TestHnswReaderSearchedGenerationSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		build                          int64
		source                         types.TS
		historical, replace, wantRetry bool
	}{
		{name: "same_tick", build: 100, source: types.BuildTS(99, 0), wantRetry: true},
		{name: "older", build: 99, source: types.BuildTS(99, 0)},
		{name: "source_logical", build: 99, source: types.BuildTS(99, 1), wantRetry: true},
		{name: "historical_same_tick", build: 100, source: types.BuildTS(99, 0), historical: true, wantRetry: true},
		{name: "historical_older", build: 99, source: types.BuildTS(99, 0), historical: true},
		{name: "receipt_survives_replacement", build: 100, source: types.BuildTS(99, 0), replace: true, wantRetry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			limits, _ := stubAlgo(t)
			proc := testutil.NewProc(t)
			spec := hnswSpec(t.Name())
			spec.SourceTableDef.TblId = 1
			read := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
			current := read
			if tc.historical {
				current = timestamp.Timestamp{PhysicalTime: 200}
				spec.ScanSnapshot = &plan.Snapshot{TS: &read}
			}
			op := &hnswReaderTxn{snapshot: current}
			proc.Base.TxnOperator = op
			source := &hnswReaderEngine{source: tc.source}
			proc.Base.SessionInfo.StorageEngine = source
			storage := t.Name()
			t.Cleanup(func() { veccache.Cache.RemoveAllGenerations(storage, "test_cleanup") })
			seed := func(key string, build int64) error {
				_, _, err := veccache.Cache.Search(nil, key, &recordingSearch{limits: limits, buildTS: build}, nil, vectorindex.RuntimeConfig{})
				return err
			}
			key := storage
			if tc.historical {
				key = veccache.SnapshotKey(storage, read)
				other := int64(100)
				if tc.build == 100 {
					other = 99
				}
				require.NoError(t, seed(storage, other))
			}
			require.NoError(t, seed(key, tc.build))
			*limits = nil
			var replacementErr error
			if tc.replace {
				source.afterSearch = func() { veccache.Cache.Remove(key); replacementErr = seed(key, 99) }
			}
			reader, err := Hooks{}.NewReader(proc, spec, f32Request([]float32{1, 2, 3}, 5))
			require.NoError(t, err)
			defer reader.Close()
			out := batch.NewWithSize(2)
			out.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			out.Vecs[1] = vector.NewVec(types.T_float64.ToType())
			defer out.Clean(proc.Mp())
			done, err := reader.Read(context.Background(), []string{planreader.KeyColumn, planreader.ScoreColumn}, nil, proc.Mp(), out)
			require.NoError(t, replacementErr)
			if tc.wantRetry {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrVectorNeedRetryWithPreMode), "got %v", err)
				require.Zero(t, out.RowCount(), "unproven candidates must not be emitted")
			} else {
				require.NoError(t, err)
				require.False(t, done)
				require.Equal(t, []int64{1, 2}, vector.MustFixedColNoTypeCheck[int64](out.Vecs[0]))
				require.Equal(t, []float64{0.5, 1.5}, vector.MustFixedColNoTypeCheck[float64](out.Vecs[1]))
				done, err = reader.Read(context.Background(), []string{planreader.KeyColumn, planreader.ScoreColumn}, nil, proc.Mp(), out)
				require.NoError(t, err)
				require.True(t, done)
			}
			require.NotNil(t, source.readTxn)
			require.Equal(t, read, source.readTxn.SnapshotTS())
			if tc.historical {
				require.NotSame(t, op, source.readTxn)
			} else {
				require.Same(t, op, source.readTxn)
			}
			if tc.replace {
				build, ok := veccache.Cache.GetBuildTS(key)
				require.True(t, ok)
				require.Equal(t, int64(99), build)
			}
		})
	}
}

type hnswReaderTxn struct {
	client.TxnOperator
	snapshot timestamp.Timestamp
}

func (t *hnswReaderTxn) SnapshotTS() timestamp.Timestamp { return t.snapshot }
func (t *hnswReaderTxn) Txn() txn.TxnMeta                { return txn.TxnMeta{SnapshotTS: t.snapshot} }
func (t *hnswReaderTxn) CloneSnapshotOp(ts timestamp.Timestamp) client.TxnOperator {
	return &hnswReaderTxn{snapshot: ts}
}

type hnswReaderEngine struct {
	engine.Engine
	source      types.TS
	readTxn     client.TxnOperator
	afterSearch func()
}

func (e *hnswReaderEngine) GetRelationById(_ context.Context, op client.TxnOperator, _ uint64) (string, string, engine.Relation, error) {
	e.readTxn = op
	if e.afterSearch != nil {
		e.afterSearch()
	}
	return "db", "t", hnswReaderSource{source: e.source}, nil
}

type hnswReaderSource struct {
	engine.Relation
	source types.TS
}

func (r hnswReaderSource) SourceCommitTS(context.Context, types.TS) (types.TS, error) {
	return r.source, nil
}
