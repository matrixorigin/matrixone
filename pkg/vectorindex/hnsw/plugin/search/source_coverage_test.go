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

package search_test

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	indexcoverage "github.com/matrixorigin/matrixone/pkg/indexplugin/coverage"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/indexplugin/search/planreader"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	hnswcoverage "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/coverage"
	hnswsearch "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/search"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/require"
)

func TestHnswPersistedDeleteCoverage(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })

	// Persist one TN tombstone with its original DELETE commit, rather than a
	// synthetic watermark. Object lifecycle time is deliberately later.
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	t.Cleanup(func() { bat.Clean(mp) })
	var blockID types.Blockid
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.NewRowid(&blockID, 0), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], types.BuildTS(150, 0), false, mp))
	bat.SetRowCount(1)
	writer := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_CommitTS, fs)
	_, err = writer.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	state := logtailreplay.NewPartitionState("", false, 42, false)
	state.UpdateDuration(types.BuildTS(1, 0), types.MaxTs())
	require.NoError(t, state.HandleObjectEntry(ctx, fs, objectio.ObjectEntry{
		ObjectStats: writer.GetObjectStats(), CreateTime: types.BuildTS(180, 0),
	}, true))
	source := &partitionSourceEngine{state: state, fs: fs, mp: mp}

	for _, tc := range []struct {
		name    string
		build   int64
		key     int64
		covered bool
	}{
		{name: "behind", build: 100, key: 1},
		{name: "caught_up", build: 150, key: 2, covered: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage := t.Name()
			t.Cleanup(func() { veccache.Cache.RemoveAllGenerations(storage, "test_cleanup") })
			_, _, err := veccache.Cache.Search(nil, storage, &deleteCoverageSearch{
				buildTS: tc.build, key: tc.key,
			}, nil, vectorindex.RuntimeConfig{})
			require.NoError(t, err)
			op := &coverageTxn{snapshot: timestamp.Timestamp{PhysicalTime: 200}}
			_, _, rel, err := source.GetRelationById(ctx, op, 42)
			require.NoError(t, err)
			provider := rel.(engine.SourceCommitTSProvider)
			sourceTS, err := provider.SourceCommitTS(ctx, types.TS{})
			require.NoError(t, err)
			require.Equal(t, types.BuildTS(150, 0), sourceTS)
			spec := &plan.IndexSearchScan{
				SourceTable:      &plan.ObjectRef{SchemaName: "db", ObjName: "t"},
				SourceTableDef:   &plan.TableDef{Name: "t", TblId: 42},
				Index:            &plan.IndexDef{IndexAlgo: catalog.MoIndexHnswAlgo.ToString(), IndexAlgoParams: `{"op_type":"vector_l2_ops","m":"16","ef_construction":"64","ef_search":"40"}`},
				DistanceFunction: "l2_distance",
				HiddenTables: []*plan.IndexHiddenTableRef{
					{Role: catalog.Hnsw_TblType_Metadata, Object: &plan.ObjectRef{ObjName: storage + "_meta"}},
					{Role: catalog.Hnsw_TblType_Storage, Object: &plan.ObjectRef{ObjName: storage}},
				},
			}
			covered, _, err := (hnswcoverage.Hooks{}).CoversSnapshot(ctx, indexcoverage.Request{
				CNUUID: "cn0", Txn: op, TableID: 42, IndexDef: spec.Index,
				IndexStorageTable: storage, Snapshot: types.BuildTS(200, 0),
				IndexMetadataDB: "db", IndexMetadataTable: storage + "_meta",
				SourceCommitTS: provider.SourceCommitTS,
			})
			require.NoError(t, err)
			require.Equal(t, tc.covered, covered)

			proc := testutil.NewProc(t)
			proc.Base.TxnOperator = op
			proc.Base.SessionInfo.StorageEngine = source
			reader, err := (hnswsearch.Hooks{}).NewReader(proc, spec, searchplugin.Request{
				QueryPayload: types.ArrayToBytes([]float32{1, 2, 3}),
				QueryType:    plan.Type{Id: int32(types.T_array_float32), Width: 3},
				ResultLimit:  1, CandidateBudget: 1,
			})
			require.NoError(t, err)
			defer reader.Close()
			out := batch.NewWithSize(2)
			out.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			out.Vecs[1] = vector.NewVec(types.T_float64.ToType())
			defer out.Clean(proc.Mp())
			_, err = reader.Read(ctx, []string{planreader.KeyColumn, planreader.ScoreColumn}, nil, proc.Mp(), out)
			if !tc.covered {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrVectorNeedRetryWithPreMode), "got %v", err)
				require.Zero(t, out.RowCount(), "a stale deleted candidate must not escape to the join")
				return
			}
			require.NoError(t, err)
			require.Equal(t, []int64{2}, vector.MustFixedColNoTypeCheck[int64](out.Vecs[0]))
			require.Equal(t, []float64{0.5}, vector.MustFixedColNoTypeCheck[float64](out.Vecs[1]))
		})
	}
}

type deleteCoverageSearch struct {
	buildTS int64
	key     int64
}

func (s *deleteCoverageSearch) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	return []int64{s.key}, []float64{0.5}, nil
}

type partitionSourceEngine struct {
	engine.Engine
	state *logtailreplay.PartitionState
	fs    fileservice.FileService
	mp    *mpool.MPool
}

func (e *partitionSourceEngine) GetRelationById(_ context.Context, op client.TxnOperator, _ uint64) (string, string, engine.Relation, error) {
	return "db", "t", partitionSourceRelation{source: e, snapshot: types.TimestampToTS(op.SnapshotTS())}, nil
}

type partitionSourceRelation struct {
	engine.Relation
	source   *partitionSourceEngine
	snapshot types.TS
}

func (r partitionSourceRelation) SourceCommitTS(ctx context.Context, mustExceed types.TS) (types.TS, error) {
	s := r.source
	info, err := s.state.SourceCommitTSAt(ctx, r.snapshot, s.fs, s.mp, mustExceed)
	return info.Max(), err
}

func (s *deleteCoverageSearch) BuildTS() int64                  { return s.buildTS }
func (*deleteCoverageSearch) Load(*sqlexec.SqlProcess) error    { return nil }
func (*deleteCoverageSearch) Preload(*sqlexec.SqlProcess) error { return nil }
func (*deleteCoverageSearch) Destroy()                          {}
func (*deleteCoverageSearch) GetIndexSize() (int64, int64)      { return 0, 0 }
func (*deleteCoverageSearch) SearchFloat32(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, []int64, []float32) error {
	return nil
}
func (*deleteCoverageSearch) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	return nil
}

type coverageTxn struct {
	client.TxnOperator
	snapshot timestamp.Timestamp
}

func (t *coverageTxn) SnapshotTS() timestamp.Timestamp { return t.snapshot }
func (t *coverageTxn) Txn() txn.TxnMeta                { return txn.TxnMeta{SnapshotTS: t.snapshot} }
