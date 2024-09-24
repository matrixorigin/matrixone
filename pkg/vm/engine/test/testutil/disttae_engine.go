// Copyright 2024 Matrix Origin
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

package testutil

import (
	"context"

	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	logservice2 "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/service"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type TestDisttaeEngine struct {
	Engine              *disttae.Engine
	logtailReceiver     chan morpc.Message
	broken              chan struct{}
	wg                  sync.WaitGroup
	ctx                 context.Context
	cancel              context.CancelFunc
	txnClient           client.TxnClient
	txnOperator         client.TxnOperator
	timestampWaiter     client.TimestampWaiter
	mp                  *mpool.MPool
	workspaceThreshold  uint64
	insertEntryMaxCount int
}

func NewTestDisttaeEngine(
	ctx context.Context,
	fs fileservice.FileService,
	rpcAgent *MockRPCAgent,
	storage *TestTxnStorage,
	options ...TestDisttaeEngineOptions,
) (*TestDisttaeEngine, error) {
	de := new(TestDisttaeEngine)
	de.logtailReceiver = make(chan morpc.Message)
	de.broken = make(chan struct{})
	for _, opt := range options {
		opt(de)
	}

	if de.mp == nil {
		de.mp, _ = mpool.NewMPool("test", 0, mpool.NoFixed)
	}
	mp := de.mp

	de.ctx, de.cancel = context.WithCancel(ctx)

	initRuntime()

	wait := make(chan struct{})
	de.timestampWaiter = client.NewTimestampWaiter(runtime.GetLogger(""))

	txnSender := service.NewTestSender(storage)
	de.txnClient = client.NewTxnClient("", txnSender, client.WithTimestampWaiter(de.timestampWaiter))

	de.txnClient.Resume()

	hakeeper := newTestHAKeeperClient()
	colexec.NewServer(hakeeper)

	var engineOpts []disttae.EngineOptions
	if de.insertEntryMaxCount != 0 {
		engineOpts = append(engineOpts, disttae.WithInsertEntryMaxCount(de.insertEntryMaxCount))
	}
	if de.workspaceThreshold != 0 {
		engineOpts = append(engineOpts, disttae.WithWorkspaceThreshold(de.workspaceThreshold))
	}

	catalog.SetupDefines("")
	de.Engine = disttae.New(ctx, "", de.mp, fs, de.txnClient, hakeeper, nil, 1, engineOpts...)
	de.Engine.PushClient().LogtailRPCClientFactory = rpcAgent.MockLogtailRPCClientFactory

	go func() {
		done := false
		for !done {
			select {
			case <-wait:
				done = true
			default:
				de.timestampWaiter.NotifyLatestCommitTS(de.Now())
				time.Sleep(time.Millisecond * 100)
			}
		}
	}()

	op, err := de.txnClient.New(ctx, types.TS{}.ToTimestamp())
	if err != nil {
		return nil, err
	}

	close(wait)

	de.txnOperator = op
	if err = de.Engine.New(ctx, op); err != nil {
		return nil, err
	}

	qc, _ := qclient.NewQueryClient("", morpc.Config{})
	sqlExecutor := compile.NewSQLExecutor(
		"127.0.0.1:2000",
		de.Engine,
		mp,
		de.txnClient,
		fs,
		qc,
		hakeeper,
		nil, //s.udfService
	)
	runtime.ServiceRuntime("").SetGlobalVariables(runtime.InternalSQLExecutor, sqlExecutor)

	runtime.ServiceRuntime("").SetGlobalVariables(
		runtime.ProcessCodecService,
		process.NewCodecService(
			de.txnClient,
			fs,
			new(mockLockService),
			qc,
			hakeeper,
			nil,
			de.Engine,
		))

	// InitLoTailPushModel presupposes that the internal sql executor has been initialized.
	err = de.Engine.InitLogTailPushModel(ctx, de.timestampWaiter)
	//err = de.prevSubscribeSysTables(ctx, rpcAgent)
	return de, err
}

func (de *TestDisttaeEngine) NewTxnOperator(
	ctx context.Context,
	commitTS timestamp.Timestamp,
	opts ...client.TxnOption,
) (client.TxnOperator, error) {
	op, err := de.txnClient.New(ctx, commitTS, opts...)
	if err != nil {
		return nil, err
	}

	ws := de.txnOperator.GetWorkspace().CloneSnapshotWS()
	ws.BindTxnOp(op)
	ws.(*disttae.Transaction).GetProc().GetTxnOperator().UpdateSnapshot(ctx, commitTS)
	ws.(*disttae.Transaction).GetProc().GetTxnOperator().AddWorkspace(ws)
	op.AddWorkspace(ws)

	return op, err
}

func (de *TestDisttaeEngine) waitLogtail(ctx context.Context) error {
	ts := de.Now()
	ticker := time.NewTicker(time.Second)
	ctx, cancel := context.WithTimeout(ctx, time.Second*60)
	defer cancel()

	done := false
	for !done {
		select {
		case <-ctx.Done():
			return moerr.NewInternalErrorNoCtx("wait partition state waterline timeout")
		case <-ticker.C:
			latestAppliedTS := de.Engine.PushClient().LatestLogtailAppliedTime()
			ready := de.Engine.PushClient().IsSubscriberReady()
			if latestAppliedTS.GreaterEq(ts) && ready {
				done = true
			}
			logutil.Infof("wait logtail, latestAppliedTS: %s, targetTS: %s, done: %v, subscriberReady: %v\n",
				latestAppliedTS.ToStdTime().String(), ts.ToStdTime().String(), done, ready)
		}
	}

	return nil
}

func (de *TestDisttaeEngine) analyzeDataObjects(state *logtailreplay.PartitionState,
	stats *PartitionStateStats, ts types.TS) (err error) {

	iter, err := state.NewObjectsIter(ts, false, false)
	if err != nil {
		return err
	}

	for iter.Next() {
		item := iter.Entry()
		if item.Visible(ts) {
			stats.DataObjectsVisible.ObjCnt += 1
			stats.DataObjectsVisible.BlkCnt += int(item.BlkCnt())
			stats.DataObjectsVisible.RowCnt += int(item.Rows())
			stats.Details.DataObjectList.Visible = append(stats.Details.DataObjectList.Visible, item)
		} else {
			stats.DataObjectsInvisible.ObjCnt += 1
			stats.DataObjectsInvisible.BlkCnt += int(item.BlkCnt())
			stats.DataObjectsInvisible.RowCnt += int(item.Rows())
			stats.Details.DataObjectList.Invisible = append(stats.Details.DataObjectList.Invisible, item)
		}
	}

	return
}

func (de *TestDisttaeEngine) analyzeInmemRows(
	state *logtailreplay.PartitionState,
	stats *PartitionStateStats,
	ts types.TS,
) (err error) {

	distinct := make(map[objectio.Blockid]struct{})
	iter := state.NewRowsIter(ts, nil, false)
	for iter.Next() {
		stats.InmemRows.VisibleCnt++
		distinct[iter.Entry().BlockID] = struct{}{}
	}

	stats.InmemRows.VisibleDistinctBlockCnt += len(distinct)
	if err = iter.Close(); err != nil {
		return
	}

	distinct = make(map[objectio.Blockid]struct{})
	iter = state.NewRowsIter(ts, nil, true)
	for iter.Next() {
		distinct[iter.Entry().BlockID] = struct{}{}
		stats.InmemRows.InvisibleCnt++
	}
	stats.InmemRows.InvisibleDistinctBlockCnt += len(distinct)
	err = iter.Close()
	return
}

func (de *TestDisttaeEngine) analyzeCheckpoint(
	state *logtailreplay.PartitionState,
	stats *PartitionStateStats,
	ts types.TS,
) (err error) {

	ckps := state.Checkpoints()
	for x := range ckps {
		locAndVersions := strings.Split(ckps[x], ";")
		stats.CheckpointCnt += len(locAndVersions) / 2
		for y := 0; y < len(locAndVersions); y += 2 {
			stats.Details.CheckpointLocs[0] = append(stats.Details.CheckpointLocs[0], locAndVersions[y])
			stats.Details.CheckpointLocs[1] = append(stats.Details.CheckpointLocs[1], locAndVersions[y+1])
		}
	}

	return
}

func (de *TestDisttaeEngine) analyzeTombstone(
	state *logtailreplay.PartitionState,
	stats *PartitionStateStats,
	ts types.TS,
) (outErr error) {

	iter, err := state.NewObjectsIter(ts, false, true)
	if err != nil {
		return err
	}

	for iter.Next() {
		item := iter.Entry()
		if item.Visible(ts) {
			stats.TombstoneObjectsVisible.ObjCnt += 1
			stats.TombstoneObjectsVisible.BlkCnt += int(item.BlkCnt())
			stats.TombstoneObjectsVisible.RowCnt += int(item.Rows())
			stats.Details.TombstoneObjectList.Visible = append(stats.Details.TombstoneObjectList.Visible, item)
		} else {
			stats.TombstoneObjectsInvisible.ObjCnt += 1
			stats.TombstoneObjectsInvisible.BlkCnt += int(item.BlkCnt())
			stats.TombstoneObjectsInvisible.RowCnt += int(item.Rows())
			stats.Details.TombstoneObjectList.Invisible = append(stats.Details.TombstoneObjectList.Invisible, item)
		}
	}

	return
}

func (de *TestDisttaeEngine) SubscribeTable(
	ctx context.Context, dbID, tbID uint64, setSubscribed bool,
) (err error) {
	ticker := time.NewTicker(time.Second)
	timeout := 5

	for range ticker.C {
		if timeout <= 0 {
			logutil.Errorf("test disttae engine subscribe table err %v, timeout", err)
			break
		}

		err = de.Engine.TryToSubscribeTable(ctx, dbID, tbID)
		if err != nil {
			timeout--
			logutil.Errorf("test disttae engine subscribe table err %v, left trie %d", err, timeout)
			continue
		}

		break
	}

	if err == nil && setSubscribed {
		de.Engine.PushClient().SetSubscribeState(dbID, tbID, disttae.Subscribed)
	}

	return
}

func (de *TestDisttaeEngine) GetPartitionStateStats(
	ctx context.Context, databaseId, tableId uint64,
) (
	stats PartitionStateStats, err error) {

	if err = de.waitLogtail(ctx); err != nil {
		return stats, err
	}

	var (
		state *logtailreplay.PartitionState
	)

	ts := types.TimestampToTS(de.Now())
	state = de.Engine.GetOrCreateLatestPart(databaseId, tableId).Snapshot()

	// data objects
	if err = de.analyzeDataObjects(state, &stats, ts); err != nil {
		return
	}

	// ckp count
	if err = de.analyzeCheckpoint(state, &stats, ts); err != nil {
		return
	}

	// in mem rows
	if err = de.analyzeInmemRows(state, &stats, ts); err != nil {
		return
	}

	if err = de.analyzeTombstone(state, &stats, ts); err != nil {
		return
	}
	// tombstones
	return
}

func (de *TestDisttaeEngine) GetTxnOperator() client.TxnOperator {
	return de.txnOperator
}

func (de *TestDisttaeEngine) Now() timestamp.Timestamp {
	return timestamp.Timestamp{PhysicalTime: time.Now().UnixNano()}
}

func (de *TestDisttaeEngine) Close(ctx context.Context) {
	de.timestampWaiter.Close()
	de.txnClient.Close()
	close(de.logtailReceiver)
	de.cancel()
	de.wg.Wait()
}

func (de *TestDisttaeEngine) GetTable(
	ctx context.Context,
	databaseName, tableName string,
) (
	database engine.Database,
	relation engine.Relation,
	txn client.TxnOperator,
	err error,
) {

	if txn, err = de.NewTxnOperator(ctx, de.Now()); err != nil {
		return nil, nil, nil, err
	}

	if database, err = de.Engine.Database(ctx, databaseName, txn); err != nil {
		return nil, nil, nil, err
	}

	if relation, err = database.Relation(ctx, tableName, nil); err != nil {
		return nil, nil, nil, err
	}

	return
}

func (de *TestDisttaeEngine) CreateDatabaseAndTable(
	ctx context.Context,
	databaseName, tableName string,
	schema *catalog2.Schema,
) (
	database engine.Database,
	table engine.Relation,
	err error,
) {

	var txn client.TxnOperator

	if txn, err = de.NewTxnOperator(ctx, de.Now()); err != nil {
		return nil, nil, err
	}

	if err = de.Engine.Create(ctx, databaseName, txn); err != nil {
		return nil, nil, err
	}

	if database, err = de.Engine.Database(ctx, databaseName, txn); err != nil {
		return nil, nil, err
	}

	var engineTblDef []engine.TableDef
	if engineTblDef, err = EngineTableDefBySchema(schema); err != nil {
		return nil, nil, err
	}

	if err = database.Create(ctx, tableName, engineTblDef); err != nil {
		return nil, nil, err
	}

	if table, err = database.Relation(ctx, tableName, nil); err != nil {
		return nil, nil, err
	}

	if err = txn.Commit(ctx); err != nil {
		return nil, nil, err
	}

	return
}

func initRuntime() {
	runtime.ServiceRuntime("").SetGlobalVariables(runtime.ClusterService, new(mockMOCluster))
	runtime.ServiceRuntime("").SetGlobalVariables(runtime.LockService, new(mockLockService))
}

var _ clusterservice.MOCluster = new(mockMOCluster)

type mockMOCluster struct {
}

func (mc *mockMOCluster) GetCNService(
	selector clusterservice.Selector, apply func(metadata.CNService) bool) {
}
func (mc *mockMOCluster) GetTNService(
	selector clusterservice.Selector, apply func(metadata.TNService) bool) {
}
func (mc *mockMOCluster) GetAllTNServices() []metadata.TNService {
	return []metadata.TNService{{
		LogTailServiceAddress: disttae.FakeLogtailServerAddress,
		Shards:                []metadata.TNShard{GetDefaultTNShard()},
	}}
}
func (mc *mockMOCluster) GetCNServiceWithoutWorkingState(
	selector clusterservice.Selector, apply func(metadata.CNService) bool) {
}
func (mc *mockMOCluster) ForceRefresh(sync bool)                                        {}
func (mc *mockMOCluster) Close()                                                        {}
func (mc *mockMOCluster) DebugUpdateCNLabel(uuid string, kvs map[string][]string) error { return nil }
func (mc *mockMOCluster) DebugUpdateCNWorkState(uuid string, state int) error           { return nil }
func (mc *mockMOCluster) RemoveCN(id string)                                            {}
func (mc *mockMOCluster) AddCN(metadata.CNService)                                      {}
func (mc *mockMOCluster) UpdateCN(metadata.CNService)                                   {}

var _ lockservice.LockService = new(mockLockService)

type mockLockService struct {
}

func (ml *mockLockService) GetServiceID() string          { return "" }
func (ml *mockLockService) GetConfig() lockservice.Config { return lockservice.Config{} }
func (ml *mockLockService) Lock(ctx context.Context, tableID uint64, rows [][]byte,
	txnID []byte, options lock.LockOptions) (lock.Result, error) {
	return lock.Result{}, nil
}
func (ml *mockLockService) Unlock(ctx context.Context, txnID []byte,
	commitTS timestamp.Timestamp, mutations ...lock.ExtraMutation) error {
	return nil
}
func (ml *mockLockService) IsOrphanTxn(context.Context, []byte) (bool, error) { return false, nil }
func (ml *mockLockService) Close() error                                      { return nil }
func (ml *mockLockService) GetWaitingList(ctx context.Context, txnID []byte) (bool, []lock.WaitTxn, error) {
	return false, nil, nil
}
func (ml *mockLockService) ForceRefreshLockTableBinds(targets []uint64, matcher func(bind lock.LockTable) bool) {
}
func (ml *mockLockService) GetLockTableBind(group uint32, tableID uint64) (lock.LockTable, error) {
	return lock.LockTable{}, nil
}
func (ml *mockLockService) IterLocks(func(tableID uint64, keys [][]byte, lock lockservice.Lock) bool) {
}
func (ml *mockLockService) CloseRemoteLockTable(group uint32, tableID uint64, version uint64) (bool, error) {
	return false, nil
}

type mockShardService struct {
}

func MockShardService() *mockShardService {
	return &mockShardService{}
}

func (ms *mockShardService) Config() shardservice.Config {
	return shardservice.Config{Enable: true}
}

func (ms *mockShardService) GetStorage() shardservice.ShardStorage {
	return nil
}

func (ms *mockShardService) Read(cxt context.Context, req shardservice.ReadRequest, opts shardservice.ReadOptions) error {
	return nil
}

func (ms *mockShardService) HasLocalReplica(tableID, shardID uint64) (bool, error) {
	return true, nil
}

func (ms *mockShardService) HasAllLocalReplicas(tableID uint64) (bool, error) {
	return false, nil
}

func (ms *mockShardService) GetShardInfo(table uint64) (uint64, pb.Policy, bool, error) {
	return table, 1, true, nil
}

func (ms *mockShardService) Create(ctx context.Context, table uint64, txnOp client.TxnOperator) error {
	return nil
}

func (ms *mockShardService) Delete(ctx context.Context, table uint64, txnOp client.TxnOperator) error {
	return nil
}

func (ms *mockShardService) ReplicaCount() int64 {
	return 1
}

func (ms *mockShardService) TableReplicaCount(tableID uint64) int64 {
	return 1
}

func (ms *mockShardService) Close() error {
	return nil
}

var _ logservice.CNHAKeeperClient = new(testHAKeeperClient)

type testHAKeeperClient struct {
	id atomic.Uint64
}

func newTestHAKeeperClient() *testHAKeeperClient {
	ha := &testHAKeeperClient{}
	ha.id.Store(0x3fff)
	return ha
}

func (ha *testHAKeeperClient) Close() error { return nil }
func (ha *testHAKeeperClient) AllocateID(ctx context.Context) (uint64, error) {
	return ha.id.Add(1), nil
}
func (ha *testHAKeeperClient) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	return 0, nil
}
func (ha *testHAKeeperClient) AllocateIDByKeyWithBatch(ctx context.Context, key string, batch uint64) (uint64, error) {
	return 0, nil
}
func (ha *testHAKeeperClient) GetClusterDetails(ctx context.Context) (logservice2.ClusterDetails, error) {
	return logservice2.ClusterDetails{}, nil
}
func (ha *testHAKeeperClient) GetClusterState(ctx context.Context) (logservice2.CheckerState, error) {
	return logservice2.CheckerState{}, nil
}

func (ha *testHAKeeperClient) GetBackupData(ctx context.Context) ([]byte, error) {
	return nil, nil
}

func (ha *testHAKeeperClient) SendCNHeartbeat(ctx context.Context, hb logservice2.CNStoreHeartbeat) (logservice2.CommandBatch, error) {
	return logservice2.CommandBatch{}, nil
}

func (ha *testHAKeeperClient) UpdateNonVotingReplicaNum(ctx context.Context, num uint64) error {
	return nil
}

func (ha *testHAKeeperClient) UpdateNonVotingLocality(ctx context.Context, locality logservice2.Locality) error {
	return nil
}
