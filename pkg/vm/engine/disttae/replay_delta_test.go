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

package disttae

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/require"
)

func TestReplayAutomaticallyConfirmsNamedDelta(t *testing.T) {
	ctx := context.Background()
	sid := t.Name()
	rt := runtime.NewRuntime(metadata.ServiceType_CN, sid, logutil.GetGlobalLogger())
	runtime.SetupServiceBasedRuntime(sid, rt)
	t.Cleanup(func() { runtime.CompareAndDeleteServiceBasedRuntime(sid, rt) })
	cluster := clusterservice.NewMOCluster(sid, nil, time.Hour, clusterservice.WithDisableRefresh())
	t.Cleanup(cluster.Close)
	rt.SetGlobalVariables(runtime.ClusterService, cluster)
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	packer := types.NewPacker()
	t.Cleanup(packer.Close)
	ctrl := gomock.NewController(t)
	cli := mock_frontend.NewMockTxnClient(ctrl)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	cli.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(op, nil)
	var workspace client.Workspace
	op.EXPECT().AddWorkspace(gomock.Any()).Do(func(w client.Workspace) {
		workspace = w
		t.Cleanup(w.(*Transaction).proc.Free)
	})
	op.EXPECT().GetWorkspace().DoAndReturn(func() client.Workspace { return workspace }).AnyTimes()
	op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
	op.EXPECT().Commit(gomock.Any()).Return(nil)
	e := &Engine{service: sid, cli: cli, mp: mp, ls: &prefetchTestLockService{id: sid}, partitions: make(map[[2]uint64]*logtailreplay.Partition), globalStats: &GlobalStats{tailC: make(chan *logtail.TableLogtail, 16)}, packerPool: fileservice.NewPool(1, func() *types.Packer { return packer }, func(p *types.Packer) { p.Reset() }, nil)}
	e.catalog.Store(cache.NewCatalog())
	tw := client.NewTimestampWaiter(runtime.GetLogger(sid))
	t.Cleanup(tw.Close)
	e.pClient.receivedLogTailTime.initLogTailTimestamp(tw)
	for i := range e.pClient.receivedLogTailTime.tList {
		e.pClient.receivedLogTailTime.tList[i].Store(timestamp.Timestamp{PhysicalTime: 50})
	}
	routines := newTestRoutineControllers(16)
	makeTail := func(b *batch.Batch, id uint64, name string, seq uint32) logtail.TableLogtail {
		_, err := fillRandomRowidAndZeroTs(b, mp)
		require.NoError(t, err)
		ts := vector.MustFixedColWithTypeCheck[types.TS](b.Vecs[cache.MO_TIMESTAMP_IDX])
		for i := range ts {
			ts[i] = types.BuildTS(100, 0)
		}
		encoded, err := batch.BatchToProtoBatch(b)
		require.NoError(t, err)
		return logtail.TableLogtail{Table: &api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: id, DbName: catalog.MO_CATALOG, TbName: name, PrimarySeqnum: seq}, Commands: []api.Entry{{EntryType: api.Entry_Insert, TableId: id, TableName: name, DatabaseId: catalog.MO_CATALOG_ID, DatabaseName: catalog.MO_CATALOG, Bat: encoded}}}
	}
	cols, err := catalog.GenColumnsFromDefs(7, "delayed_table", "db", 90, 80, []engine.TableDef{&engine.AttributeDef{Attr: engine.Attribute{Name: "delta_column", Type: types.T_uint64.ToType()}}})
	require.NoError(t, err)
	cols[0].IsUnsigned = 0
	cols[0].Comment = "delta-100"
	cb, err := catalog.GenCreateColumnTuples(cols, mp, packer)
	require.NoError(t, err)
	t.Cleanup(func() { cb.Clean(mp) })
	tb, err := catalog.GenCreateTableTuple(catalog.Table{AccountId: 7, TableId: 90, DatabaseId: 80, TableName: "delayed_table", DatabaseName: "db", Version: 100}, mp, packer)
	require.NoError(t, err)
	t.Cleanup(func() { tb.Clean(mp) })
	to := timestamp.Timestamp{PhysicalTime: 100}
	response := &logtail.UpdateResponse{To: &to, LogtailList: []logtail.TableLogtail{makeTail(cb, catalog.MO_COLUMNS_ID, catalog.MO_COLUMNS, catalog.MO_COLUMNS_ATT_CPKEY_IDX), makeTail(tb, catalog.MO_TABLES_ID, catalog.MO_TABLES, catalog.MO_TABLES_CPKEY_IDX)}}
	calls := 0
	exec := executor.NewMemExecutor(func(query string) (executor.Result, error) {
		calls++
		if query == catalog.MoColumnsBatchQuery {
			require.NoError(t, dispatchUpdateResponse(ctx, e, response, routines, time.Now()))
			q := &cache.TableItem{AccountId: 7, DatabaseId: 80, Name: "delayed_table", Ts: to}
			require.False(t, e.GetLatestCatalogCache().GetTable(q), "delta must still be delayed inside replay")
		}
		return executor.Result{Mp: mp}, nil
	})
	rt.SetGlobalVariables(runtime.InternalSQLExecutor, exec)
	require.NoError(t, e.pClient.replayCatalogCache(ctx, e))
	require.Equal(t, 3, calls)
	q := &cache.TableItem{AccountId: 7, DatabaseId: 80, Name: "delayed_table", Ts: to}
	require.True(t, e.GetLatestCatalogCache().GetTable(q))
	require.Equal(t, uint32(100), q.Version)
	require.Equal(t, "delta_column", q.TableDef.Cols[0].Name)
	require.Equal(t, "delta-100", q.TableDef.Cols[0].Comment)
	q.Ts = timestamp.Timestamp{PhysicalTime: 99}
	require.False(t, e.GetLatestCatalogCache().GetTable(q))
}
