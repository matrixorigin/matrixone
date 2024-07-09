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
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	logservice2 "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/service"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

type TestDisttaeEngine struct {
	Engine          *disttae.Engine
	logtailReceiver chan morpc.Message
	broken          chan struct{}
	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
	txnClient       client.TxnClient
	txnOperator     client.TxnOperator
	timestampWaiter client.TimestampWaiter
}

func NewTestDisttaeEngine(ctx context.Context, mp *mpool.MPool,
	fs fileservice.FileService, rpcAgent *MockRPCAgent) (*TestDisttaeEngine, error) {
	de := new(TestDisttaeEngine)

	de.logtailReceiver = make(chan morpc.Message)
	de.broken = make(chan struct{})

	de.ctx, de.cancel = context.WithCancel(ctx)

	initRuntime()

	wait := make(chan struct{})
	de.timestampWaiter = client.NewTimestampWaiter()

	de.txnClient = client.NewTxnClient(
		&service.TestSender{},
		client.WithTimestampWaiter(de.timestampWaiter))

	de.txnClient.Resume()

	hakeeper := newTestHAKeeperClient()
	colexec.NewServer(hakeeper)

	de.Engine = disttae.New(ctx, mp, fs, de.txnClient, hakeeper, nil, 0)
	de.Engine.PushClient().LogtailRPCClientFactory = rpcAgent.MockLogtailRPCClientFactory

	go func() {
		for {
			select {
			case <-wait:
				break
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

	err = de.Engine.InitLogTailPushModel(ctx, de.timestampWaiter)
	if err != nil {
		return nil, err
	}

	//err = de.prevSubscribeSysTables(ctx, rpcAgent)

	return de, err
}

func (de *TestDisttaeEngine) NewTxnOperator(ctx context.Context,
	commitTS timestamp.Timestamp, opts ...client.TxnOption) (client.TxnOperator, error) {
	op, err := de.txnClient.New(ctx, commitTS, opts...)

	op.AddWorkspace(de.txnOperator.GetWorkspace())
	de.txnOperator.GetWorkspace().BindTxnOp(op)

	return op, err
}

func (de *TestDisttaeEngine) CountStar(ctx context.Context, databaseId, tableId uint64) (totalRows uint32, err error) {
	var state *logtailreplay.PartitionState
	ts := de.Now()
	ticker := time.NewTicker(time.Second)
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	done := false
	for !done {
		select {
		case <-ctx.Done():
			return 0, moerr.NewInternalErrorNoCtx("wait partition state waterline timeout")
		case <-ticker.C:
			latestAppliedTS := de.Engine.PushClient().LatestLogtailAppliedTime()
			if latestAppliedTS.GreaterEq(ts) {
				done = true
				break
			}
		}
	}

	state = de.Engine.GetOrCreateLatestPart(databaseId, tableId).Snapshot()

	err = disttae.ForeachSnapshotObjects(de.Now(), func(obj logtailreplay.ObjectInfo, isCommitted bool) error {
		totalRows += obj.Rows()
		return nil
	}, state)
	if err != nil {
		return 0, err
	}

	iter := state.NewRowsIter(types.TimestampToTS(ts), nil, false)
	for iter.Next() {
		totalRows++
	}

	if err = iter.Close(); err != nil {
		return 0, err
	}

	// how to get deletes?

	return totalRows, nil
}

func (de *TestDisttaeEngine) GetTxnOperator() client.TxnOperator {
	return de.txnOperator
}

func (de *TestDisttaeEngine) Now() timestamp.Timestamp {
	return timestamp.Timestamp{PhysicalTime: time.Now().UnixNano()}
}

func (de *TestDisttaeEngine) Close(ctx context.Context) {
	de.timestampWaiter.Close()
	close(de.logtailReceiver)
	de.cancel()
	de.wg.Wait()
}

func initRuntime() {
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, new(mockMOCluster))
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.LockService, new(mockLockService))
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
	return []metadata.TNService{metadata.TNService{LogTailServiceAddress: disttae.FakeLogtailServerAddress}}
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
