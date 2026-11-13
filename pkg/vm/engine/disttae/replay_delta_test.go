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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/require"
)

func TestReplayAutomaticallyConfirmsNamedDelta(t *testing.T) {
	for _, tc := range []struct {
		name      string
		replayErr error
		canceled  bool
	}{
		{name: "commit"},
		{name: "rollback", replayErr: errors.New("injected catalog SELECT failure")},
		{name: "canceled", replayErr: context.Canceled, canceled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replayErr := tc.replayErr
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			sid := t.Name()
			rt := runtime.NewRuntime(metadata.ServiceType_CN, sid, logutil.GetGlobalLogger(), runtime.WithClock(types.NewMockHLCClock(100)))
			runtime.SetupServiceBasedRuntime(sid, rt)
			t.Cleanup(func() { runtime.CompareAndDeleteServiceBasedRuntime(sid, rt) })
			cluster := clusterservice.NewMOCluster(sid, nil, time.Hour, clusterservice.WithDisableRefresh())
			t.Cleanup(cluster.Close)
			rt.SetGlobalVariables(runtime.ClusterService, cluster)
			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			packer := types.NewPacker()
			t.Cleanup(packer.Close)
			colexec.NewServer(sid)
			blocker := &replayBootstrapLockService{
				prefetchTestLockService: prefetchTestLockService{id: sid},
				entered:                 make(chan struct{}), release: make(chan struct{}),
			}
			sender := &snapshotReadTxnSender{}
			cli := &replayTxnClient{TxnClient: client.NewTxnClient(sid, sender, client.WithLockService(blocker))}
			cli.Resume()
			t.Cleanup(func() { require.NoError(t, cli.Close()) })
			e := &Engine{service: sid, cli: cli, mp: mp, ls: blocker, partitions: make(map[[2]uint64]*logtailreplay.Partition), globalStats: &GlobalStats{tailC: make(chan *logtail.TableLogtail, 16)}, packerPool: fileservice.NewPool(1, func() *types.Packer { return packer }, func(p *types.Packer) { p.Reset() }, nil)}
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
			var workspace *Transaction
			exec := executor.NewMemExecutor(func(query string) (executor.Result, error) {
				calls++
				if workspace == nil {
					workspace = cli.op.GetWorkspace().(*Transaction)
					t.Cleanup(workspace.proc.Free)
				}
				if query == catalog.MoColumnsBatchQuery {
					if replayErr != nil {
						if tc.canceled {
							cancel()
						}
						return executor.Result{}, replayErr
					}
					if err := dispatchUpdateResponse(ctx, e, response, routines, time.Now()); err != nil {
						return executor.Result{}, err
					}
					q := &cache.TableItem{AccountId: 7, DatabaseId: 80, Name: "delayed_table", Ts: to}
					if e.GetLatestCatalogCache().GetTable(q) {
						return executor.Result{}, errors.New("delta published before replay confirmation")
					}
				}
				return executor.Result{Mp: mp}, nil
			})
			rt.SetGlobalVariables(runtime.InternalSQLExecutor, exec)
			finished := make(chan struct{})
			var replayResult error
			go func() {
				defer close(finished)
				replayResult = e.pClient.replayCatalogCache(ctx, e)
			}()
			defer func() { close(blocker.release); <-finished }()
			select {
			case <-blocker.entered:
				t.Fatal("catalog replay entered pessimistic lock cleanup before CN readiness")
			case <-finished:
			case <-time.After(5 * time.Second):
				t.Fatal("catalog replay did not finish")
			}
			require.ErrorIs(t, replayResult, replayErr)
			require.Equal(t, 3, calls)
			require.Equal(t, txn.TxnMode_Optimistic, cli.op.Txn().Mode)
			require.Equal(t, timestamp.Timestamp{PhysicalTime: 50}, cli.op.SnapshotTS())
			require.NotNil(t, workspace)
			require.True(t, workspace.removed, "both commit and rollback must retire the real workspace")
			var active int
			cli.IterTxnIDs(func([]byte) bool { active++; return true })
			require.Zero(t, active)
			require.Zero(t, sender.calls)
			if replayErr != nil {
				require.Equal(t, txn.TxnStatus_Aborted, cli.op.Status())
				return
			}
			require.Equal(t, txn.TxnStatus_Committed, cli.op.Status())
			q := &cache.TableItem{AccountId: 7, DatabaseId: 80, Name: "delayed_table", Ts: to}
			require.True(t, e.GetLatestCatalogCache().GetTable(q))
			require.Equal(t, uint32(100), q.Version)
			require.Equal(t, "delta_column", q.TableDef.Cols[0].Name)
			require.Equal(t, "delta-100", q.TableDef.Cols[0].Comment)
			q.Ts = timestamp.Timestamp{PhysicalTime: 99}
			require.False(t, e.GetLatestCatalogCache().GetTable(q))
		})
	}
}

// replayBootstrapLockService models the CN readiness wait seen by ordinary
// pessimistic Unlock. Cleanup releases the gate even in an old-code control.
type replayBootstrapLockService struct {
	prefetchTestLockService
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *replayBootstrapLockService) GetServiceID() string { return s.id }

func (s *replayBootstrapLockService) Unlock(context.Context, []byte, timestamp.Timestamp, ...lock.ExtraMutation) error {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return nil
}

type replayTxnClient struct {
	client.TxnClient
	op client.TxnOperator
}

func (c *replayTxnClient) New(ctx context.Context, ts timestamp.Timestamp, options ...client.TxnOption) (client.TxnOperator, error) {
	op, err := c.TxnClient.New(ctx, ts, options...)
	c.op = op
	return op, err
}
