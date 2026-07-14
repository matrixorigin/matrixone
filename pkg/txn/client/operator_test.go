// Copyright 2022 Matrix Origin
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

package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"github.com/matrixorigin/matrixone/pkg/util/protoc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func mustEnterRunSQL(
	t *testing.T,
	tc *txnOperator,
	cancel context.CancelFunc,
	sql string,
) uint64 {
	t.Helper()
	token, err := tc.TryEnterRunSqlWithTokenAndSQL(cancel, sql)
	require.NoError(t, err)
	require.NotZero(t, token)
	return token
}

type checkLockTableBindLockService struct {
	lockservice.LockService
	binds  []lock.LockTable
	err    error
	calls  int
	cancel context.CancelFunc
}

type trackingUnlockLockService struct {
	lockservice.LockService
	unlockCount int
	unlockErr   error
}

func (s *trackingUnlockLockService) Unlock(context.Context, []byte, timestamp.Timestamp, ...lock.ExtraMutation) error {
	s.unlockCount++
	return s.unlockErr
}

func (s *checkLockTableBindLockService) GetLatestLockTableBind(bind lock.LockTable) (lock.LockTable, error) {
	s.calls++
	if s.cancel != nil {
		s.cancel()
	}
	if s.err != nil {
		return lock.LockTable{}, s.err
	}
	if len(s.binds) >= s.calls {
		return s.binds[s.calls-1], nil
	}
	return bind, nil
}

type trackingWorkspace struct {
	readonly        bool
	commitRequests  []txn.TxnRequest
	commitErr       error
	rollbackErr     error
	finalizeAction  func()
	commitCount     int
	rollbackCount   int
	finalizeCount   int
	unknownCount    int
	haveDDL         bool
	snapshotOffset  int
	sqlCount        uint64
	boundTxn        TxnOperator
	cloneSnapshotTS int64
	ccprTxn         bool
	ccprTaskID      string
	syncJobID       string
}

type blockingRollbackWorkspace struct {
	trackingWorkspace
	started chan struct{}
	release chan struct{}
}

type blockingFinalizeWorkspace struct {
	trackingWorkspace
	started chan struct{}
	release chan struct{}
}

func (w *blockingRollbackWorkspace) Rollback(ctx context.Context) error {
	w.rollbackCount++
	select {
	case w.started <- struct{}{}:
	default:
	}
	select {
	case <-w.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *blockingFinalizeWorkspace) FinalizeCommit(ctx context.Context) {
	w.finalizeCount++
	select {
	case w.started <- struct{}{}:
	default:
	}
	select {
	case <-w.release:
	case <-ctx.Done():
	}
}

func (w *trackingWorkspace) Readonly() bool {
	return w.readonly
}

func (w *trackingWorkspace) StartStatement() {
}

func (w *trackingWorkspace) EndStatement() {
}

func (w *trackingWorkspace) IncrStatementID(context.Context, bool) error {
	return nil
}

func (w *trackingWorkspace) AdvanceSnapshot(context.Context, timestamp.Timestamp) error {
	return nil
}

func (w *trackingWorkspace) RollbackLastStatement(context.Context) error {
	return nil
}

func (w *trackingWorkspace) UpdateSnapshotWriteOffset() {
	w.snapshotOffset = len(w.commitRequests)
}

func (w *trackingWorkspace) GetSnapshotWriteOffset() int {
	return w.snapshotOffset
}

func (w *trackingWorkspace) WriteOffset() uint64 {
	return uint64(len(w.commitRequests))
}

func (w *trackingWorkspace) Adjust(uint64) error {
	return nil
}

func (w *trackingWorkspace) Commit(context.Context) ([]txn.TxnRequest, error) {
	w.commitCount++
	return w.commitRequests, w.commitErr
}

func (w *trackingWorkspace) FinalizeCommit(context.Context) {
	w.finalizeCount++
	if w.finalizeAction != nil {
		w.finalizeAction()
	}
}

func (w *trackingWorkspace) FinalizeCommitWithUnknownResult(context.Context) {
	w.unknownCount++
}

func (w *trackingWorkspace) Rollback(context.Context) error {
	w.rollbackCount++
	return w.rollbackErr
}

func (w *trackingWorkspace) IncrSQLCount() {
	w.sqlCount++
}

func (w *trackingWorkspace) GetSQLCount() uint64 {
	return w.sqlCount
}

func (w *trackingWorkspace) CloneSnapshotWS() Workspace {
	return w
}

func (w *trackingWorkspace) BindTxnOp(op TxnOperator) {
	w.boundTxn = op
}

func (w *trackingWorkspace) SetHaveDDL(flag bool) {
	w.haveDDL = flag
}

func (w *trackingWorkspace) GetHaveDDL() bool {
	return w.haveDDL
}

func (w *trackingWorkspace) PPString() string {
	return "trackingWorkspace"
}

func (w *trackingWorkspace) SetCloneTxn(snapshot int64) {
	w.cloneSnapshotTS = snapshot
}

func (w *trackingWorkspace) SetCCPRTxn() {
	w.ccprTxn = true
}

func (w *trackingWorkspace) IsCCPRTxn() bool {
	return w.ccprTxn
}

func (w *trackingWorkspace) SetCCPRTaskID(taskID string) {
	w.ccprTaskID = taskID
}

func (w *trackingWorkspace) GetCCPRTaskID() string {
	return w.ccprTaskID
}

func (w *trackingWorkspace) SetSyncProtectionJobID(jobID string) {
	w.syncJobID = jobID
}

func (w *trackingWorkspace) GetSyncProtectionJobID() string {
	return w.syncJobID
}

func TestRead(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		result, err := tc.Read(ctx, []txn.TxnRequest{newTNRequest(1, 1), newTNRequest(2, 2)})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result.Responses))
		assert.Equal(t, []byte("r-1"), result.Responses[0].CNOpResponse.Payload)
		assert.Equal(t, []byte("r-2"), result.Responses[1].CNOpResponse.Payload)
	})
}

func TestWrite(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		assert.Empty(t, tc.mu.txn.TNShards)
		result, err := tc.Write(ctx, []txn.TxnRequest{newTNRequest(1, 1), newTNRequest(2, 2)})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result.Responses))
		assert.Equal(t, []byte("w-1"), result.Responses[0].CNOpResponse.Payload)
		assert.Equal(t, []byte("w-2"), result.Responses[1].CNOpResponse.Payload)

		assert.Equal(t, uint64(1), tc.mu.txn.TNShards[0].ShardID)
		assert.Equal(t, 2, len(tc.mu.txn.TNShards))
	})
}

func TestWriteWithCacheWriteEnabled(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		assert.Empty(t, tc.mu.txn.TNShards)
		responses, err := tc.Write(ctx, []txn.TxnRequest{newTNRequest(1, 1), newTNRequest(2, 2)})
		assert.NoError(t, err)
		assert.Empty(t, responses)
		assert.Equal(t, uint64(1), tc.mu.txn.TNShards[0].ShardID)
		assert.Equal(t, 2, len(tc.mu.txn.TNShards))
		assert.Empty(t, ts.getLastRequests())
	}, WithTxnCacheWrite())
}

func TestRollback(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		tc.mu.txn.TNShards = append(tc.mu.txn.TNShards, metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 1}})
		err := tc.Rollback(ctx)
		assert.NoError(t, err)

		requests := ts.getLastRequests()
		assert.Equal(t, 1, len(requests))
		assert.Equal(t, txn.TxnMethod_Rollback, requests[0].Method)
	})
}

func TestRollbackWithClosedTxn(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		ts.setManual(func(sr *rpc.SendResult, err error) (*rpc.SendResult, error) {
			return nil, moerr.NewTxnClosed(ctx, tc.reset.txnID)
		})

		tc.mu.txn.TNShards = append(tc.mu.txn.TNShards, metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 1}})
		err := tc.Rollback(ctx)
		assert.NoError(t, err)
	})
}

func TestRollbackWithNoWrite(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		err := tc.Rollback(ctx)
		assert.NoError(t, err)
		assert.Empty(t, ts.getLastRequests())
	})
}

func TestRollbackReadOnly(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		err := tc.Rollback(ctx)
		assert.NoError(t, err)
		assert.Empty(t, ts.getLastRequests())
	}, WithTxnReadyOnly())
}

func TestCommit(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		tc.mu.txn.TNShards = append(tc.mu.txn.TNShards, metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 1}})
		err := tc.Commit(ctx)
		assert.NoError(t, err)
		assert.Equal(t, tc.mu.txn.SnapshotTS.Next(), tc.mu.txn.CommitTS)

		requests := ts.getLastRequests()
		assert.Equal(t, 1, len(requests))
		assert.Equal(t, txn.TxnMethod_Commit, requests[0].Method)
	})
}

func TestCommitFinalizesPreparedWorkspaceAfterCommitSuccess(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		ws := &trackingWorkspace{
			commitRequests: []txn.TxnRequest{newTNRequest(1, 1)},
		}
		tc.AddWorkspace(ws)

		err := tc.Commit(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, ws.commitCount)
		require.Equal(t, 1, ws.finalizeCount)
		require.Zero(t, ws.rollbackCount)
		require.Equal(t, txn.TxnStatus_Committed, tc.mu.txn.Status)
	})
}

func TestCommitRollsBackPreparedWorkspaceAfterCommitSendError(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		ws := &trackingWorkspace{
			commitRequests: []txn.TxnRequest{newTNRequest(1, 1)},
		}
		tc.AddWorkspace(ws)
		ts.setManual(func(*rpc.SendResult, error) (*rpc.SendResult, error) {
			return nil, assert.AnError
		})

		err := tc.Commit(ctx)
		require.ErrorIs(t, err, assert.AnError)
		require.Equal(t, 1, ws.commitCount)
		require.Zero(t, ws.finalizeCount)
		require.Equal(t, 1, ws.rollbackCount)
		require.Equal(t, txn.TxnStatus_Aborted, tc.mu.txn.Status)
	})
}

func TestCommitFinalizesPreparedWorkspaceWithoutRollbackAfterTxnUnknown(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		ws := &trackingWorkspace{
			commitRequests: []txn.TxnRequest{newTNRequest(1, 1)},
		}
		tc.AddWorkspace(ws)
		ts.setManual(func(*rpc.SendResult, error) (*rpc.SendResult, error) {
			return nil, moerr.NewTxnUnknown(ctx, "test")
		})

		err := tc.Commit(ctx)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnUnknown))
		require.Equal(t, 1, ws.commitCount)
		require.Zero(t, ws.finalizeCount)
		require.Equal(t, 1, ws.unknownCount)
		require.Zero(t, ws.rollbackCount)
		require.Equal(t, txn.TxnStatus_Active, tc.mu.txn.Status)
		require.True(t, tc.reset.cannotCleanWorkspace.Load())

		// A retained direct handle must be rejected before it can run workspace
		// rollback after an unknown finalization.
		err = tc.Rollback(ctx)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed), err)
		require.Zero(t, ws.rollbackCount)
		require.Equal(t, txn.TxnStatus_Active, tc.mu.txn.Status)
	})
}

func TestCommitWithNoWrite(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		err := tc.Commit(ctx)
		assert.NoError(t, err)
		assert.Empty(t, ts.getLastRequests())
		assert.Equal(t, txn.TxnStatus_Committed, tc.mu.txn.Status)
	})
}

func TestCommitReadOnly(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		err := tc.Commit(ctx)
		assert.NoError(t, err)
		assert.Empty(t, ts.getLastRequests())
		assert.Equal(t, txn.TxnStatus_Committed, tc.mu.txn.Status)
	}, WithTxnReadyOnly())
}

func TestCommitWithLockTables(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		r := runtime.DefaultRuntime()
		runtime.SetupServiceBasedRuntime("", r)
		runtime.SetupServiceBasedRuntime("s1", r)

		c := clusterservice.NewMOCluster("", nil, time.Hour, clusterservice.WithDisableRefresh())
		defer c.Close()
		r.SetGlobalVariables(runtime.ClusterService, c)

		s := lockservice.NewLockService(lockservice.Config{ServiceID: "s1"})
		defer func() {
			assert.NoError(t, s.Close())
		}()

		tc.mu.txn.Mode = txn.TxnMode_Pessimistic
		tc.lockService = s
		tc.AddLockTable(lock.LockTable{Table: 1})
		tc.mu.txn.TNShards = append(tc.mu.txn.TNShards, metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 1}})
		err := tc.Commit(ctx)
		assert.NoError(t, err)

		requests := ts.getLastRequests()
		assert.Equal(t, 1, len(requests))
		assert.Equal(t, txn.TxnMethod_Commit, requests[0].Method)
		assert.Equal(t, 1, len(requests[0].Txn.LockTables))
	})
}

func TestCommitWithLockTablesChanged(t *testing.T) {
	tableID1 := uint64(10)
	tableID2 := uint64(20)
	tableID3 := uint64(30)
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		lockservice.RunLockServicesForTest(
			zap.DebugLevel,
			[]string{"s1"},
			time.Second,
			func(lta lockservice.LockTableAllocator, ls []lockservice.LockService) {
				s := ls[0]

				_, err := s.Lock(ctx, tableID1, [][]byte{[]byte("k1")}, tc.reset.txnID, lock.LockOptions{})
				assert.NoError(t, err)
				_, err = s.Lock(ctx, tableID2, [][]byte{[]byte("k1")}, tc.reset.txnID, lock.LockOptions{})
				assert.NoError(t, err)
				_, err = s.Lock(ctx, tableID3, [][]byte{[]byte("k1")}, tc.reset.txnID, lock.LockOptions{})
				assert.NoError(t, err)

				ts.setManual(func(sr *rpc.SendResult, err error) (*rpc.SendResult, error) {
					sr.Responses[0].TxnError = txn.WrapError(moerr.NewLockTableBindChanged(ctx), 0)
					sr.Responses[0].CommitResponse = &txn.TxnCommitResponse{
						InvalidLockTables: []uint64{tableID1, tableID2},
					}
					return sr, nil
				})

				tc.mu.txn.Mode = txn.TxnMode_Pessimistic
				tc.lockService = s

				// table 1 hold bind same as lockservice, commit failed, will removed
				tc.AddLockTable(lock.LockTable{Table: tableID1, ServiceID: s.GetServiceID(), Version: lta.GetVersion()})
				// table 2 hold stale bind with lockservice, cannot remove bind in lockservice
				tc.AddLockTable(lock.LockTable{Table: tableID2, ServiceID: s.GetServiceID(), Version: 0})
				// table 3 is valid
				tc.AddLockTable(lock.LockTable{Table: tableID3, ServiceID: s.GetServiceID(), Version: lta.GetVersion()})

				tc.mu.txn.TNShards = append(tc.mu.txn.TNShards, metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 1}})
				err = tc.Commit(ctx)
				assert.Error(t, err)
				assert.Equal(t, txn.TxnStatus_Aborted, tc.mu.txn.Status)

				// table 1 will be removed
				bind, err := s.GetLockTableBind(0, tableID1)
				require.NoError(t, err)
				require.Equal(t, lock.LockTable{}, bind)

				// table 2 will be kept
				bind, err = s.GetLockTableBind(0, tableID2)
				require.NoError(t, err)
				require.NotEqual(t, lock.LockTable{}, bind)

				// table 3 will be kept
				bind, err = s.GetLockTableBind(0, tableID3)
				require.NoError(t, err)
				require.NotEqual(t, lock.LockTable{}, bind)
			},
			nil)
	})
}

func TestCheckLockTableBindsChanged(t *testing.T) {
	tableID := uint64(10)
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		lockservice.RunLockServicesForTest(
			zap.DebugLevel,
			[]string{"s1"},
			time.Second,
			func(lta lockservice.LockTableAllocator, ls []lockservice.LockService) {
				s := ls[0]

				_, err := s.Lock(ctx, tableID, [][]byte{[]byte("k1")}, tc.reset.txnID, lock.LockOptions{})
				require.NoError(t, err)

				tc.mu.txn.Mode = txn.TxnMode_Pessimistic
				tc.lockService = s
				require.NoError(t, tc.AddLockTable(lock.LockTable{
					Table:     tableID,
					ServiceID: s.GetServiceID(),
					Version:   lta.GetVersion() - 1,
					Valid:     true,
				}))

				err = tc.CheckLockTableBinds(ctx)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))

				err = tc.CheckLockTableBinds(ctx)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))
			},
			nil)
	})
}

func TestCheckLockTableBindsChangedWithStaleLocalCache(t *testing.T) {
	tableID := uint64(10)
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		lockservice.RunLockServicesForTest(
			zap.DebugLevel,
			[]string{"s1"},
			time.Second,
			func(lta lockservice.LockTableAllocator, ls []lockservice.LockService) {
				s := ls[0]

				_, err := s.Lock(ctx, tableID, [][]byte{[]byte("k1")}, tc.reset.txnID, lock.LockOptions{})
				require.NoError(t, err)
				hold, err := s.GetLockTableBind(0, tableID)
				require.NoError(t, err)
				require.True(t, hold.Valid)

				newerServiceID := fmt.Sprintf("%019d%s", time.Now().UnixNano()+int64(time.Second), hold.ServiceID[19:])
				current := lta.Get(newerServiceID, hold.Group, hold.Table, hold.OriginTable, hold.Sharding)
				require.True(t, current.Valid)
				require.True(t, current.Changed(hold))

				local, err := s.GetLockTableBind(0, tableID)
				require.NoError(t, err)
				require.False(t, local.Changed(hold))

				tc.mu.txn.Mode = txn.TxnMode_Pessimistic
				tc.lockService = s
				require.NoError(t, tc.AddLockTable(hold))

				err = tc.CheckLockTableBinds(ctx)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))
			},
			nil)
	})
}

func TestCheckLockTableBindsRPCErrClearsThrottle(t *testing.T) {
	hold := lock.LockTable{Table: 10, ServiceID: "s1", Version: 1, Valid: true}
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		s := &checkLockTableBindLockService{err: moerr.NewInternalErrorNoCtx("test")}
		tc.mu.txn.Mode = txn.TxnMode_Pessimistic
		tc.lockService = s
		require.NoError(t, tc.AddLockTable(hold))

		err := tc.CheckLockTableBinds(ctx)
		require.Error(t, err)
		require.Equal(t, 1, s.calls)
		require.True(t, tc.mu.lastLockTableBindCheck.IsZero())

		s.err = nil
		err = tc.CheckLockTableBinds(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, s.calls)
	})
}

func TestCheckLockTableBindsCanceledContextClearsThrottle(t *testing.T) {
	hold := lock.LockTable{Table: 10, ServiceID: "s1", Version: 1, Valid: true}
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		s := &checkLockTableBindLockService{}
		tc.mu.txn.Mode = txn.TxnMode_Pessimistic
		tc.lockService = s
		require.NoError(t, tc.AddLockTable(hold))

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		err := tc.CheckLockTableBinds(cancelled)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 0, s.calls)
		require.True(t, tc.mu.lastLockTableBindCheck.IsZero())

		err = tc.CheckLockTableBinds(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, s.calls)
	})
}

func TestCheckLockTableBindsCanceledContextDuringLoopClearsThrottle(t *testing.T) {
	hold1 := lock.LockTable{Table: 10, ServiceID: "s1", Version: 1, Valid: true}
	hold2 := lock.LockTable{Table: 20, ServiceID: "s1", Version: 1, Valid: true}
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		cancelled, cancel := context.WithCancel(ctx)
		s := &checkLockTableBindLockService{cancel: cancel}
		tc.mu.txn.Mode = txn.TxnMode_Pessimistic
		tc.lockService = s
		require.NoError(t, tc.AddLockTable(hold1))
		require.NoError(t, tc.AddLockTable(hold2))

		err := tc.CheckLockTableBinds(cancelled)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, s.calls)
		require.True(t, tc.mu.lastLockTableBindCheck.IsZero())
	})
}

func TestCheckLockTableBindsClosedTxnSkipsBindLookup(t *testing.T) {
	hold := lock.LockTable{Table: 10, ServiceID: "s1", Version: 1, Valid: true}
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		s := &checkLockTableBindLockService{}
		tc.mu.txn.Mode = txn.TxnMode_Pessimistic
		tc.lockService = s
		require.NoError(t, tc.AddLockTable(hold))
		tc.mu.closed = true

		err := tc.CheckLockTableBinds(ctx)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
		require.Equal(t, 0, s.calls)
	})
}

func TestCheckLockTableBindsCleanSecondCallIsThrottled(t *testing.T) {
	hold := lock.LockTable{Table: 10, ServiceID: "s1", Version: 1, Valid: true}
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		s := &checkLockTableBindLockService{}
		tc.mu.txn.Mode = txn.TxnMode_Pessimistic
		tc.lockService = s
		require.NoError(t, tc.AddLockTable(hold))

		require.NoError(t, tc.CheckLockTableBinds(ctx))
		require.NoError(t, tc.CheckLockTableBinds(ctx))
		require.Equal(t, 1, s.calls)
	})
}

func TestContextWithoutDeadlineWillPanic(t *testing.T) {
	runOperatorTests(t, func(_ context.Context, tc *txnOperator, _ *testTxnSender) {
		defer func() {
			if err := recover(); err != nil {
				return
			}
			assert.Fail(t, "must panic")
		}()

		_, err := tc.Write(context.Background(), nil)
		assert.NoError(t, err)
	})
}

func TestMissingSenderWillPanic(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	newTxnOperator("", nil, nil, txn.TxnMeta{})
}

func TestMissingTxnIDWillPanic(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	newTxnOperator("", nil, newTestTxnSender(), txn.TxnMeta{})
}

func TestReadOnlyAndCacheWriteBothSetWillPanic(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	newTxnOperator(
		"",
		nil,
		newTestTxnSender(),
		txn.TxnMeta{ID: []byte{1}, SnapshotTS: timestamp.Timestamp{PhysicalTime: 1}},
		WithTxnReadyOnly(),
		WithTxnCacheWrite())
}

func TestWriteOnReadyOnlyTxnWillPanic(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		defer func() {
			if err := recover(); err != nil {
				return
			}
			assert.Fail(t, "must panic")
		}()

		_, err := tc.Write(ctx, nil)
		assert.NoError(t, err)
	}, WithTxnReadyOnly())
}

func TestWriteOnClosedTxnWillPanic(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		tc.mu.closed = true
		_, err := tc.Write(ctx, nil)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
	})
}

func TestReadOnClosedTxnWillPanic(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		tc.mu.closed = true
		_, err := tc.Read(ctx, nil)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
	})
}

func TestCacheWrites(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		responses, err := tc.Write(ctx, []txn.TxnRequest{txn.NewTxnRequest(&txn.CNOpRequest{OpCode: 1})})
		assert.NoError(t, err)
		assert.Empty(t, responses)
		assert.Equal(t, 1, len(tc.mu.cachedWrites))
		assert.Equal(t, 1, len(tc.mu.cachedWrites[0]))
	}, WithTxnCacheWrite())
}

func TestCacheWritesWillInsertBeforeRead(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		result, err := tc.Write(ctx, []txn.TxnRequest{newTNRequest(1, 1), newTNRequest(2, 2), newTNRequest(3, 3)})
		assert.NoError(t, err)
		assert.Empty(t, result)
		assert.Equal(t, 3, len(tc.mu.cachedWrites))
		assert.Equal(t, 1, len(tc.mu.cachedWrites[1]))
		assert.Equal(t, 1, len(tc.mu.cachedWrites[2]))
		assert.Equal(t, 1, len(tc.mu.cachedWrites[3]))

		result, err = tc.Read(ctx, []txn.TxnRequest{newTNRequest(11, 1), newTNRequest(22, 2), newTNRequest(33, 3), newTNRequest(4, 4)})
		assert.NoError(t, err)
		assert.Equal(t, 4, len(result.Responses))
		assert.Equal(t, []byte("r-11"), result.Responses[0].CNOpResponse.Payload)
		assert.Equal(t, []byte("r-22"), result.Responses[1].CNOpResponse.Payload)
		assert.Equal(t, []byte("r-33"), result.Responses[2].CNOpResponse.Payload)
		assert.Equal(t, []byte("r-4"), result.Responses[3].CNOpResponse.Payload)

		requests := ts.getLastRequests()
		assert.Equal(t, 7, len(requests))
		assert.Equal(t, uint32(1), requests[0].CNRequest.OpCode)
		assert.Equal(t, uint32(11), requests[1].CNRequest.OpCode)
		assert.Equal(t, uint32(2), requests[2].CNRequest.OpCode)
		assert.Equal(t, uint32(22), requests[3].CNRequest.OpCode)
		assert.Equal(t, uint32(3), requests[4].CNRequest.OpCode)
		assert.Equal(t, uint32(33), requests[5].CNRequest.OpCode)
		assert.Equal(t, uint32(4), requests[6].CNRequest.OpCode)

		assert.Equal(t, 0, len(tc.mu.cachedWrites))
	}, WithTxnCacheWrite())
}

func TestReadOnAbortedTxn(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		ts.setManual(func(result *rpc.SendResult, err error) (*rpc.SendResult, error) {
			for idx := range result.Responses {
				result.Responses[idx].Txn = &txn.TxnMeta{Status: txn.TxnStatus_Aborted}
			}
			return result, err
		})
		responses, err := tc.Read(ctx, []txn.TxnRequest{txn.NewTxnRequest(&txn.CNOpRequest{OpCode: 1})})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
		assert.Empty(t, responses)
	})
}

func TestWriteOnAbortedTxn(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		ts.setManual(func(result *rpc.SendResult, err error) (*rpc.SendResult, error) {
			for idx := range result.Responses {
				result.Responses[idx].Txn = &txn.TxnMeta{Status: txn.TxnStatus_Aborted}
			}
			return result, err
		})
		result, err := tc.Write(ctx, []txn.TxnRequest{txn.NewTxnRequest(&txn.CNOpRequest{OpCode: 1})})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
		assert.Empty(t, result)
	})
}

func TestWriteOnCommittedTxn(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		ts.setManual(func(result *rpc.SendResult, err error) (*rpc.SendResult, error) {
			for idx := range result.Responses {
				result.Responses[idx].Txn = &txn.TxnMeta{Status: txn.TxnStatus_Committed}
			}
			return result, err
		})
		result, err := tc.Write(ctx, []txn.TxnRequest{txn.NewTxnRequest(&txn.CNOpRequest{OpCode: 1})})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
		assert.Empty(t, result)
	})
}

func TestWriteOnCommittingTxn(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		ts.setManual(func(result *rpc.SendResult, err error) (*rpc.SendResult, error) {
			for idx := range result.Responses {
				result.Responses[idx].Txn = &txn.TxnMeta{Status: txn.TxnStatus_Committing}
			}
			return result, err
		})
		result, err := tc.Write(ctx, []txn.TxnRequest{txn.NewTxnRequest(&txn.CNOpRequest{OpCode: 1})})
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
		assert.Empty(t, result)
	})
}

func TestSnapshotTxnOperator(t *testing.T) {
	runOperatorTests(t, func(_ context.Context, tc *txnOperator, _ *testTxnSender) {
		assert.NoError(t, tc.AddLockTable(lock.LockTable{Table: 1}))

		v, err := tc.Snapshot()
		assert.NoError(t, err)

		tc2 := newTxnOperatorWithSnapshot(tc.logger, tc.sender, v)
		assert.True(t, tc2.mu.txn.Mirror)

		tc2.mu.txn.Mirror = false
		assert.Equal(t, tc.mu.txn, tc2.mu.txn)
		assert.False(t, tc2.opts.coordinator)
		tc2.opts.coordinator = true
		assert.Equal(t, tc.opts.options, tc2.opts.options)
		assert.Equal(t, 1, len(tc2.mu.lockTables))
	}, WithTxnReadyOnly(), WithTxnDisable1PCOpt())
}

func TestApplySnapshotTxnOperator(t *testing.T) {
	runOperatorTests(t, func(_ context.Context, tc *txnOperator, _ *testTxnSender) {
		snapshot := &txn.CNTxnSnapshot{}
		snapshot.Txn.ID = tc.mu.txn.ID
		assert.NoError(t, tc.ApplySnapshot(protoc.MustMarshal(snapshot)))
		assert.Equal(t, 0, len(tc.mu.txn.TNShards))

		snapshot.Txn.TNShards = append(snapshot.Txn.TNShards, metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 1}})
		assert.NoError(t, tc.ApplySnapshot(protoc.MustMarshal(snapshot)))
		assert.Equal(t, 1, len(tc.mu.txn.TNShards))

		snapshot.Txn.TNShards = append(snapshot.Txn.TNShards, metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 2}})
		assert.NoError(t, tc.ApplySnapshot(protoc.MustMarshal(snapshot)))
		assert.Equal(t, 2, len(tc.mu.txn.TNShards))

		snapshot.LockTables = append(snapshot.LockTables, lock.LockTable{Table: 1})
		assert.NoError(t, tc.ApplySnapshot(protoc.MustMarshal(snapshot)))
		assert.Equal(t, 1, len(tc.mu.lockTables))
	})
}

func TestDebugTxnOperator(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		responses, err := tc.Debug(ctx,
			[]txn.TxnRequest{txn.NewTxnRequest(&txn.CNOpRequest{OpCode: 1, Payload: []byte("OK")})})
		assert.NoError(t, err)
		assert.Equal(t, len(responses.Responses), 1)
		assert.Equal(t, responses.Responses[0].CNOpResponse.Payload, []byte("OK"))
	})
}

func TestAddLockTable(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		assert.NoError(t, tc.AddLockTable(lock.LockTable{Table: 1}))
		assert.Equal(t, 1, len(tc.mu.lockTables))

		// same lock table
		assert.NoError(t, tc.AddLockTable(lock.LockTable{Table: 1}))
		assert.Equal(t, 1, len(tc.mu.lockTables))

		// changed lock table
		assert.Error(t, tc.AddLockTable(lock.LockTable{Table: 1, Version: 2}))
	})
}

func TestUpdateSnapshotTSWithWaiter(t *testing.T) {
	runTimestampWaiterTests(t, func(waiter *timestampWaiter) {
		runOperatorTests(t,
			func(
				ctx context.Context,
				tc *txnOperator,
				_ *testTxnSender) {
				tc.timestampWaiter = waiter
				tc.mu.txn.SnapshotTS = newTestTimestamp(10)
				tc.mu.txn.Isolation = txn.TxnIsolation_SI

				ts := int64(100)
				c := make(chan struct{})
				go func() {
					defer close(c)
					waiter.NotifyLatestCommitTS(newTestTimestamp(ts))
				}()
				<-c
				require.NoError(t, tc.UpdateSnapshot(context.Background(), newTestTimestamp(0)))
				require.Equal(t, newTestTimestamp(ts).Next(), tc.Txn().SnapshotTS)
			})
	})
}

func TestRollbackMultiTimes(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		require.NoError(t, tc.Rollback(ctx))
		err := tc.Rollback(ctx)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed), err)
	})
}

func TestWaitCommittedLogAppliedInRCMode(t *testing.T) {
	lockservice.RunLockServicesForTest(
		zap.InfoLevel,
		[]string{"s1"},
		time.Second,
		func(lta lockservice.LockTableAllocator, ls []lockservice.LockService) {
			l := ls[0]
			tw := NewTimestampWaiter(util.GetLogger(""))
			initTS := newTestTimestamp(1)
			tw.NotifyLatestCommitTS(initTS)
			runOperatorTestsWithOptions(
				t,
				func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
					require.Equal(t, initTS.Next(), tc.mu.txn.SnapshotTS)

					_, err := l.Lock(ctx, 1, [][]byte{[]byte("k1")}, tc.mu.txn.ID, lock.LockOptions{})
					require.NoError(t, err)

					tc.mu.txn.TNShards = append(tc.mu.txn.TNShards, metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 1}})

					ctx2, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()
					st := time.Now()
					c := make(chan struct{})
					go func() {
						defer close(c)
						time.Sleep(time.Second)
						tw.NotifyLatestCommitTS(initTS.Next().Next())
					}()
					require.NoError(t, tc.Commit(ctx2))
					<-c
					require.True(t, time.Since(st) > time.Second)
				},
				newTestTimestamp(0).Next(),
				[]TxnOption{WithTxnMode(txn.TxnMode_Pessimistic), WithTxnIsolation(txn.TxnIsolation_RC)},
				WithTimestampWaiter(tw),
				WithEnableSacrificingFreshness(),
				WithLockService(l))
		},
		nil)
}

func TestCannotCommitRunningSQLTxn(t *testing.T) {
	runOperatorTests(
		t,
		func(
			ctx context.Context,
			tc *txnOperator,
			_ *testTxnSender,
		) {
			defer func() {
				if err := recover(); err != nil {
					require.NotNil(t, err)
				}
			}()

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			mustEnterRunSQL(t, tc, cancel, "test")
			_ = tc.Commit(ctx)
		},
	)
}

func TestCannotRollbackRunningSQLTxn(t *testing.T) {
	runOperatorTests(
		t,
		func(
			ctx context.Context,
			tc *txnOperator,
			_ *testTxnSender,
		) {
			defer func() {
				if err := recover(); err != nil {
					require.NotNil(t, err)
				}
			}()

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			mustEnterRunSQL(t, tc, cancel, "test")
			_ = tc.Rollback(ctx)
		},
	)
}

func TestEmptyLockSkipped(t *testing.T) {
	runOperatorTests(
		t,
		func(
			ctx context.Context,
			tc *txnOperator,
			_ *testTxnSender,
		) {
			require.False(t, tc.LockSkipped(1, lock.LockMode_Exclusive))
		},
	)
}

func TestLockSkipped(t *testing.T) {
	runOperatorTests(
		t,
		func(
			ctx context.Context,
			tc *txnOperator,
			_ *testTxnSender,
		) {
			require.True(t, tc.LockSkipped(1, lock.LockMode_Exclusive))
			require.False(t, tc.LockSkipped(1, lock.LockMode_Shared))
			require.False(t, tc.LockSkipped(2, lock.LockMode_Exclusive))
		},
		WithTxnSkipLock(
			[]uint64{1},
			[]lock.LockMode{lock.LockMode_Exclusive},
		),
	)
}

func TestHasLockTable(t *testing.T) {
	runOperatorTests(
		t,
		func(
			ctx context.Context,
			tc *txnOperator,
			_ *testTxnSender,
		) {
			require.NoError(t, tc.AddLockTable(lock.LockTable{Table: 1}))
			require.True(t, tc.HasLockTable(1))
			require.False(t, tc.HasLockTable(2))
		},
	)
}

func TestBase(t *testing.T) {
	runOperatorTests(
		t,
		func(
			ctx context.Context,
			tc *txnOperator,
			_ *testTxnSender,
		) {
			require.NotNil(t, tc.TxnRef())
			require.Equal(t, tc.Txn().SnapshotTS, tc.SnapshotTS())
			newSnapshotTS := newTestTimestamp(42)
			tc.SetSnapshotTS(newSnapshotTS)
			require.Equal(t, newSnapshotTS, tc.SnapshotTS())
			require.NotEqual(t, timestamp.Timestamp{}, tc.CreateTS())
			require.Equal(t, txn.TxnStatus_Active, tc.Status())
		},
	)
}

func TestInitResetsLockTableBindCheckThrottle(t *testing.T) {
	runOperatorTests(
		t,
		func(
			ctx context.Context,
			tc *txnOperator,
			_ *testTxnSender,
		) {
			tc.mu.Lock()
			tc.mu.lastLockTableBindCheck = time.Now()
			tc.mu.lockTableBindChanged = true
			tc.mu.Unlock()

			tc.init(txn.TxnMeta{ID: []byte("reused-txn")})

			tc.mu.RLock()
			defer tc.mu.RUnlock()
			require.True(t, tc.mu.lastLockTableBindCheck.IsZero())
			require.False(t, tc.mu.lockTableBindChanged)
		},
	)
}

func runOperatorTests(
	t *testing.T,
	tc func(context.Context, *txnOperator, *testTxnSender),
	options ...TxnOption) {
	runOperatorTestsWithOptions(t, tc, newTestTimestamp(0), options)
}

func runOperatorTestsWithOptions(
	t *testing.T,
	tc func(context.Context, *txnOperator, *testTxnSender),
	minTS timestamp.Timestamp,
	options []TxnOption,
	clientOptions ...TxnClientCreateOption) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	RunTxnTests(
		func(
			c TxnClient,
			ts rpc.TxnSender) {
			txn, err := c.New(ctx, minTS, options...)
			assert.Nil(t, err)
			tc(ctx, txn.(*txnOperator), ts.(*testTxnSender))
		},
		clientOptions...)
}

func newTNRequest(op uint32, tn uint64) txn.TxnRequest {
	return txn.NewTxnRequest(&txn.CNOpRequest{
		OpCode: op,
		Target: metadata.TNShard{
			TNShardRecord: metadata.TNShardRecord{ShardID: tn},
		},
	})
}

func TestCommitSendErrorSetsCommitTSWorkaround(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		ts.setManual(func(sr *rpc.SendResult, err error) (*rpc.SendResult, error) {
			return nil, assert.AnError
		})
		tc.mu.txn.TNShards = append(tc.mu.txn.TNShards, metadata.TNShard{TNShardRecord: metadata.TNShardRecord{ShardID: 1}})
		start := time.Now().UnixNano()
		err := tc.Commit(ctx)
		assert.Error(t, err)
		commitTS := tc.mu.txn.CommitTS.PhysicalTime
		assert.InDelta(t, start+10000000000, commitTS, 1e9, "CommitTS should be set to now+10s on send error")
	})
}

func TestRunSQLTrackerSealCancelsExistingAndRejectsNewTokens(t *testing.T) {
	var tracker runSQLTracker
	runningCtx, runningCancel := context.WithCancel(context.Background())
	runningToken := tracker.enterTokenWithSQL(runningCancel, "running sql")
	require.NotZero(t, runningToken)

	drained := make(chan struct{})
	tracker.sealAndRunWhenDrained(func() {
		close(drained)
	})

	select {
	case <-runningCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("sealing did not cancel an already registered SQL token")
	}

	rejectedCtx, rejectedCancel := context.WithCancel(context.Background())
	require.Zero(t, tracker.enterTokenWithSQL(rejectedCancel, "new sql"))
	select {
	case <-rejectedCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("sealing did not reject a new SQL token")
	}

	tracker.exitToken(runningToken)
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not run after the final SQL token exited")
	}
}

func TestCancelAndWaitRunningSQL(t *testing.T) {
	runOperatorTests(
		t,
		func(
			ctx context.Context,
			tc *txnOperator,
			_ *testTxnSender,
		) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			c := make(chan struct{})
			sqlCancelCtx, sqlCancel := context.WithCancel(context.Background())
			token := mustEnterRunSQL(t, tc, sqlCancel, "test sql")

			go func() {
				defer close(c)
				defer tc.ExitRunSqlWithToken(token)
				<-sqlCancelCtx.Done()
			}()

			err := tc.CancelAndWaitRunningSQL(ctx, 0)
			require.NoError(t, err)
			<-c
		},
	)
}

func TestCommitSealsRunSQLRegistrationBeforeWaiting(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		runningCtx, runningCancel := context.WithCancel(context.Background())
		runningToken := mustEnterRunSQL(t, tc, runningCancel, "running sql")

		commitErr := make(chan error, 1)
		go func() {
			commitErr <- tc.Commit(ctx)
		}()

		select {
		case <-runningCtx.Done():
		case <-time.After(time.Second):
			t.Fatal("commit did not cancel the registered SQL")
		}

		lateCtx, lateCancel := context.WithCancel(context.Background())
		lateToken, err := tc.TryEnterRunSqlWithTokenAndSQL(lateCancel, "late sql")
		require.Zero(t, lateToken)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
		select {
		case <-lateCtx.Done():
		case <-time.After(time.Second):
			t.Fatal("commit did not reject the late SQL registration")
		}

		tc.ExitRunSqlWithToken(runningToken)
		require.NoError(t, <-commitErr)
	})
}

func TestCommitSealPreservesCurrentRunSQLToken(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		currentCtx, currentCancel := context.WithCancel(context.Background())
		currentToken := mustEnterRunSQL(t, tc, currentCancel, "commit statement")

		commitCtx := WithRunSQLSkipToken(ctx, currentToken)
		require.NoError(t, tc.Commit(commitCtx))
		select {
		case <-currentCtx.Done():
			t.Fatal("commit canceled its own run-SQL token")
		default:
		}

		tc.ExitRunSqlWithToken(currentToken)
		currentCancel()
	})
}

func TestWriteAndCommitSealsAndWaitsForRunningSQL(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		currentCtx, currentCancel := context.WithCancel(context.Background())
		currentToken := mustEnterRunSQL(t, tc, currentCancel, "write-and-commit")
		otherCtx, otherCancel := context.WithCancel(context.Background())
		otherToken := mustEnterRunSQL(t, tc, otherCancel, "other sql")

		resultC := make(chan *rpc.SendResult, 1)
		errC := make(chan error, 1)
		go func() {
			result, err := tc.WriteAndCommit(
				WithRunSQLSkipToken(ctx, currentToken),
				[]txn.TxnRequest{newTNRequest(1, 1)},
			)
			resultC <- result
			errC <- err
		}()

		select {
		case <-otherCtx.Done():
		case <-time.After(time.Second):
			t.Fatal("WriteAndCommit did not cancel the other SQL")
		}
		select {
		case <-currentCtx.Done():
			t.Fatal("WriteAndCommit canceled its own SQL token")
		default:
		}

		lateCtx, lateCancel := context.WithCancel(context.Background())
		lateToken, err := tc.TryEnterRunSqlWithTokenAndSQL(lateCancel, "late sql")
		require.Zero(t, lateToken)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
		select {
		case <-lateCtx.Done():
		case <-time.After(time.Second):
			t.Fatal("WriteAndCommit did not reject the late SQL")
		}

		tc.ExitRunSqlWithToken(otherToken)
		require.NoError(t, <-errC)
		if result := <-resultC; result != nil {
			result.Release()
		}
		tc.ExitRunSqlWithToken(currentToken)
		currentCancel()
	})
}

func TestRunSQLFastRejectDoesNotWaitForCommitLock(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		tc.mu.Lock()
		tc.mu.txn.TNShards = []metadata.TNShard{{TNShardRecord: metadata.TNShardRecord{ShardID: 1}}}
		tc.mu.Unlock()

		sendStarted := make(chan struct{}, 1)
		releaseSend := make(chan struct{})
		ts.setManual(func(result *rpc.SendResult, err error) (*rpc.SendResult, error) {
			sendStarted <- struct{}{}
			<-releaseSend
			return result, err
		})

		commitErr := make(chan error, 1)
		go func() {
			commitErr <- tc.Commit(ctx)
		}()
		select {
		case <-sendStarted:
		case <-time.After(time.Second):
			t.Fatal("commit did not reach the blocking sender")
		}

		rejected := make(chan error, 1)
		go func() {
			_, cancel := context.WithCancel(context.Background())
			_, err := tc.TryEnterRunSqlWithTokenAndSQL(cancel, "late sql")
			rejected <- err
		}()
		select {
		case err := <-rejected:
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
		case <-time.After(100 * time.Millisecond):
			t.Fatal("late SQL waited for the commit operator lock")
		}

		close(releaseSend)
		require.NoError(t, <-commitErr)
	})
}

func TestCancelAndWaitRunningSQL_WithKeepToken(t *testing.T) {
	runOperatorTests(
		t,
		func(
			ctx context.Context,
			tc *txnOperator,
			_ *testTxnSender,
		) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			sqlCancelCtx1, sqlCancel1 := context.WithCancel(context.Background())
			token1 := mustEnterRunSQL(t, tc, sqlCancel1, "test sql 1")

			sqlCancelCtx2, sqlCancel2 := context.WithCancel(context.Background())
			token2 := mustEnterRunSQL(t, tc, sqlCancel2, "test sql 2")

			c1 := make(chan struct{})
			go func() {
				defer close(c1)
				defer tc.ExitRunSqlWithToken(token1)
				<-sqlCancelCtx1.Done()
			}()

			c2 := make(chan struct{})
			go func() {
				defer close(c2)
				defer tc.ExitRunSqlWithToken(token2)
				select {
				case <-sqlCancelCtx2.Done():
				case <-ctx.Done():
				}
			}()

			err := tc.CancelAndWaitRunningSQL(ctx, token2)
			require.NoError(t, err)

			<-c1
			sqlCancel2()
			<-c2
		},
	)
}

func TestCancelAndWaitRunningSQL_Timeout(t *testing.T) {
	old := runningSQLWaitTimeout
	runningSQLWaitTimeout = time.Second
	defer func() {
		runningSQLWaitTimeout = old
	}()

	runOperatorTests(
		t,
		func(
			ctx context.Context,
			tc *txnOperator,
			_ *testTxnSender,
		) {
			_, sqlCancel := context.WithCancel(context.Background())
			token := mustEnterRunSQL(t, tc, sqlCancel, "test sql")
			defer tc.ExitRunSqlWithToken(token)

			// Do not share the helper's one-second setup deadline with the
			// timeout being tested. Slow CI must observe this timeout, not an
			// unrelated parent-context expiration.
			waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := tc.CancelAndWaitRunningSQL(waitCtx, 0)
			require.ErrorIs(t, err, context.DeadlineExceeded)
		},
	)
}

func TestCommitAndRollbackTimeoutDeferFullCleanupUntilRunningSQLExits(t *testing.T) {
	old := runningSQLWaitTimeout
	runningSQLWaitTimeout = time.Second
	defer func() {
		runningSQLWaitTimeout = old
	}()

	for _, action := range []struct {
		name string
		run  func(*txnOperator, context.Context) error
	}{
		{name: "commit", run: (*txnOperator).Commit},
		{name: "rollback", run: (*txnOperator).Rollback},
		{name: "write-and-commit", run: func(tc *txnOperator, ctx context.Context) error {
			result, err := tc.WriteAndCommit(ctx, []txn.TxnRequest{newTNRequest(1, 1)})
			if result != nil {
				result.Release()
			}
			return err
		}},
	} {
		t.Run(action.name, func(t *testing.T) {
			runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
				closedC := make(chan struct{})
				tc.AppendEventCallback(ClosedEvent, TxnEventCallback{Func: func(context.Context, TxnOperator, TxnEvent, any) error {
					close(closedC)
					return nil
				}})
				workspace := &trackingWorkspace{}
				lockService := &trackingUnlockLockService{}
				tc.AddWorkspace(workspace)
				tc.lockService = lockService
				tc.mu.Lock()
				tc.mu.txn.TNShards = []metadata.TNShard{{TNShardRecord: metadata.TNShardRecord{ShardID: 1}}}
				tc.mu.txn.Mode = txn.TxnMode_Pessimistic
				tc.mu.Unlock()

				_, sqlCancel := context.WithCancel(context.Background())
				token := mustEnterRunSQL(t, tc, sqlCancel, "stuck sql")

				// Keep the test context well beyond the injected running-SQL
				// timeout so a slow scheduler cannot turn this into a false pass.
				waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				err := action.run(tc, waitCtx)
				require.ErrorIs(t, err, context.DeadlineExceeded)
				assert.Empty(t, ts.getLastRequests())
				require.Zero(t, workspace.rollbackCount)
				require.Zero(t, lockService.unlockCount)
				_, err = tc.Read(waitCtx, nil)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))

				tc.ExitRunSqlWithToken(token)

				select {
				case <-closedC:
				case <-time.After(time.Second):
					t.Fatal("deferred rollback did not close the transaction")
				}
				require.Equal(t, 1, workspace.rollbackCount)
				require.Equal(t, 1, lockService.unlockCount)
				requests := ts.getLastRequests()
				require.Len(t, requests, 1)
				require.Equal(t, txn.TxnMethod_Rollback, requests[0].Method)
				tc.mu.RLock()
				require.True(t, tc.mu.closed)
				tc.mu.RUnlock()
			})
		})
	}
}

func TestCloseAsAbortedAndRunSQLExitDoNotDeadlock(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		_, sqlCancel := context.WithCancel(context.Background())
		token := mustEnterRunSQL(t, tc, sqlCancel, "concurrent close")
		start := make(chan struct{})
		done := make(chan struct{}, 2)

		go func() {
			<-start
			tc.closeAsAborted(ctx, assert.AnError)
			done <- struct{}{}
		}()
		go func() {
			<-start
			tc.ExitRunSqlWithToken(token)
			done <- struct{}{}
		}()
		close(start)
		for i := 0; i < cap(done); i++ {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("closeAsAborted and SQL exit deadlocked")
			}
		}

		tc.mu.RLock()
		require.True(t, tc.mu.closed)
		tc.mu.RUnlock()
		_, err := tc.TryEnterRunSqlWithTokenAndSQL(nil, "after close")
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
	})
}

func TestRunningSQLTimeoutSealsNewSQLUntilRollbackCompletes(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		closedC := make(chan struct{})
		tc.AppendEventCallback(ClosedEvent, TxnEventCallback{Func: func(context.Context, TxnOperator, TxnEvent, any) error {
			close(closedC)
			return nil
		}})
		workspace := &trackingWorkspace{}
		lockService := &trackingUnlockLockService{}
		tc.AddWorkspace(workspace)
		tc.lockService = lockService
		tc.mu.Lock()
		tc.mu.txn.TNShards = []metadata.TNShard{{TNShardRecord: metadata.TNShardRecord{ShardID: 1}}}
		tc.mu.txn.Mode = txn.TxnMode_Pessimistic
		tc.mu.Unlock()

		_, runningCancel := context.WithCancel(context.Background())
		runningToken := mustEnterRunSQL(t, tc, runningCancel, "stuck sql")
		require.NotZero(t, runningToken)

		commitCtx, cancelCommit := context.WithCancel(ctx)
		cancelCommit()
		require.ErrorIs(t, tc.Commit(commitCtx), context.Canceled)

		rejectedCtx, rejectedCancel := context.WithCancel(context.Background())
		rejectedToken, err := tc.TryEnterRunSqlWithTokenAndSQL(rejectedCancel, "new sql")
		require.Zero(t, rejectedToken)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
		select {
		case <-rejectedCtx.Done():
		case <-time.After(time.Second):
			t.Fatal("new SQL was not canceled after rollback was scheduled")
		}

		// A rejected token has no registration and must not alter the running-SQL
		// counters when the compile cleanup calls ExitRunSqlWithToken.
		tc.ExitRunSqlWithToken(rejectedToken)
		tc.ExitRunSqlWithToken(runningToken)

		select {
		case <-closedC:
		case <-time.After(time.Second):
			t.Fatal("deferred rollback did not close the transaction")
		}
		require.Equal(t, 1, workspace.rollbackCount)
		require.Equal(t, 1, lockService.unlockCount)
		requests := ts.getLastRequests()
		require.Len(t, requests, 1)
		require.Equal(t, txn.TxnMethod_Rollback, requests[0].Method)
	})
}

func TestConcurrentRunningSQLTimeoutSchedulesOneRollback(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		closedC := make(chan struct{})
		tc.AppendEventCallback(ClosedEvent, TxnEventCallback{Func: func(context.Context, TxnOperator, TxnEvent, any) error {
			close(closedC)
			return nil
		}})
		workspace := &trackingWorkspace{}
		lockService := &trackingUnlockLockService{}
		tc.AddWorkspace(workspace)
		tc.lockService = lockService
		tc.mu.Lock()
		tc.mu.txn.TNShards = []metadata.TNShard{{TNShardRecord: metadata.TNShardRecord{ShardID: 1}}}
		tc.mu.txn.Mode = txn.TxnMode_Pessimistic
		tc.mu.Unlock()

		_, runningCancel := context.WithCancel(context.Background())
		runningToken := mustEnterRunSQL(t, tc, runningCancel, "stuck sql")
		require.NotZero(t, runningToken)

		errC := make(chan error, 2)
		for _, action := range []func(context.Context) error{tc.Commit, tc.Rollback} {
			go func(action func(context.Context) error) {
				timeoutCtx, cancel := context.WithCancel(ctx)
				cancel()
				errC <- action(timeoutCtx)
			}(action)
		}
		var canceled, closed int
		for range 2 {
			err := <-errC
			switch {
			case errors.Is(err, context.Canceled):
				canceled++
			case moerr.IsMoErrCode(err, moerr.ErrTxnClosed):
				closed++
			default:
				t.Fatalf("unexpected concurrent terminal-call error: %v", err)
			}
		}
		require.Equal(t, 1, canceled)
		require.Equal(t, 1, closed)

		tc.ExitRunSqlWithToken(runningToken)
		select {
		case <-closedC:
		case <-time.After(time.Second):
			t.Fatal("deferred rollback did not close the transaction")
		}
		require.Equal(t, 1, workspace.rollbackCount)
		require.Equal(t, 1, lockService.unlockCount)
		requests := ts.getLastRequests()
		require.Len(t, requests, 1)
		require.Equal(t, txn.TxnMethod_Rollback, requests[0].Method)
	})
}

func TestDeferredRollbackRejectsLaterPublicTerminalCalls(t *testing.T) {
	actions := []struct {
		name string
		run  func(*txnOperator, context.Context) error
	}{
		{name: "commit", run: (*txnOperator).Commit},
		{name: "rollback", run: (*txnOperator).Rollback},
		{name: "write-and-commit", run: func(tc *txnOperator, ctx context.Context) error {
			result, err := tc.WriteAndCommit(ctx, []txn.TxnRequest{newTNRequest(1, 1)})
			if result != nil {
				result.Release()
			}
			return err
		}},
	}

	for _, owner := range actions {
		t.Run(owner.name, func(t *testing.T) {
			runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
				closedC := make(chan struct{})
				tc.AppendEventCallback(ClosedEvent, TxnEventCallback{Func: func(context.Context, TxnOperator, TxnEvent, any) error {
					close(closedC)
					return nil
				}})
				workspace := &blockingRollbackWorkspace{
					started: make(chan struct{}, 1),
					release: make(chan struct{}),
				}
				lockService := &trackingUnlockLockService{}
				tc.AddWorkspace(workspace)
				tc.lockService = lockService
				tc.mu.Lock()
				tc.mu.txn.TNShards = []metadata.TNShard{{TNShardRecord: metadata.TNShardRecord{ShardID: 1}}}
				tc.mu.txn.Mode = txn.TxnMode_Pessimistic
				tc.mu.Unlock()

				_, runningCancel := context.WithCancel(context.Background())
				runningToken := mustEnterRunSQL(t, tc, runningCancel, "stuck sql")
				ownerCtx, cancelOwner := context.WithCancel(ctx)
				cancelOwner()
				require.ErrorIs(t, owner.run(tc, ownerCtx), context.Canceled)

				assertRejected := func(phase string) {
					t.Helper()
					for _, action := range actions {
						t.Run(phase+"/"+action.name, func(t *testing.T) {
							callCtx, cancelCall := context.WithTimeout(context.Background(), 100*time.Millisecond)
							defer cancelCall()
							err := action.run(tc, callCtx)
							require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed), err)
							require.NoError(t, callCtx.Err(), "terminal call did not fail fast")
						})
					}
				}

				assertRejected("before-drain")
				tc.ExitRunSqlWithToken(runningToken)
				select {
				case <-workspace.started:
				case <-time.After(time.Second):
					t.Fatal("deferred rollback did not start")
				}
				assertRejected("after-drain")

				close(workspace.release)
				select {
				case <-closedC:
				case <-time.After(time.Second):
					t.Fatal("deferred rollback did not finish")
				}
				require.Equal(t, 1, workspace.rollbackCount)
				require.Equal(t, 1, lockService.unlockCount)
				requests := ts.getLastRequests()
				require.Len(t, requests, 1)
				require.Equal(t, txn.TxnMethod_Rollback, requests[0].Method)
			})
		})
	}
}

func TestTerminalCallOwnerRejectsConcurrentPublicTerminalCalls(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
		tc.mu.Lock()
		tc.mu.txn.TNShards = []metadata.TNShard{{TNShardRecord: metadata.TNShardRecord{ShardID: 1}}}
		tc.mu.Unlock()

		sendStarted := make(chan struct{}, 1)
		releaseSend := make(chan struct{})
		ts.setManual(func(result *rpc.SendResult, err error) (*rpc.SendResult, error) {
			sendStarted <- struct{}{}
			<-releaseSend
			return result, err
		})

		commitErr := make(chan error, 1)
		go func() {
			commitErr <- tc.Commit(ctx)
		}()
		select {
		case <-sendStarted:
		case <-time.After(time.Second):
			t.Fatal("commit did not reach the blocking sender")
		}

		concurrentCalls := []func(context.Context) error{
			tc.Commit,
			tc.Rollback,
			func(ctx context.Context) error {
				result, err := tc.WriteAndCommit(ctx, []txn.TxnRequest{newTNRequest(1, 1)})
				if result != nil {
					result.Release()
				}
				return err
			},
		}
		for _, call := range concurrentCalls {
			callCtx, cancelCall := context.WithTimeout(context.Background(), 100*time.Millisecond)
			err := call(callCtx)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed), err)
			require.NoError(t, callCtx.Err(), "terminal call did not fail fast")
			cancelCall()
		}

		close(releaseSend)
		require.NoError(t, <-commitErr)
	})
}

func TestClosedOperatorRejectsSequentialPublicTerminalCalls(t *testing.T) {
	actions := []struct {
		name string
		run  func(*txnOperator, context.Context) error
	}{
		{name: "commit", run: (*txnOperator).Commit},
		{name: "rollback", run: (*txnOperator).Rollback},
		{name: "write-and-commit", run: func(tc *txnOperator, ctx context.Context) error {
			result, err := tc.WriteAndCommit(ctx, []txn.TxnRequest{newTNRequest(2, 2)})
			if result != nil {
				result.Release()
			}
			return err
		}},
	}

	for _, owner := range actions {
		t.Run(owner.name, func(t *testing.T) {
			runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
				workspace := &trackingWorkspace{
					commitRequests: []txn.TxnRequest{newTNRequest(1, 1)},
				}
				tc.AddWorkspace(workspace)
				tc.mu.Lock()
				tc.mu.txn.TNShards = []metadata.TNShard{{TNShardRecord: metadata.TNShardRecord{ShardID: 1}}}
				tc.mu.Unlock()

				require.NoError(t, owner.run(tc, ctx))
				require.Equal(t, txnTerminalCallSealed, txnTerminalCallState(tc.terminalCall.Load()))
				beforeTxn := tc.Txn()
				beforeRequests := append([]txn.TxnRequest(nil), ts.getLastRequests()...)
				beforeCommit := workspace.commitCount
				beforeRollback := workspace.rollbackCount
				beforeFinalize := workspace.finalizeCount

				for _, later := range actions {
					t.Run("then-"+later.name, func(t *testing.T) {
						callCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
						defer cancel()
						err := later.run(tc, callCtx)
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed), err)
						require.NoError(t, callCtx.Err(), "sequential terminal call did not fail fast")
						require.Equal(t, beforeTxn, tc.Txn())
						require.Equal(t, beforeRequests, ts.getLastRequests())
						require.Equal(t, beforeCommit, workspace.commitCount)
						require.Equal(t, beforeRollback, workspace.rollbackCount)
						require.Equal(t, beforeFinalize, workspace.finalizeCount)
					})
				}
			})
		})
	}
}

func TestTerminalCallFailureBeforeCloseReopensGate(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		tc.AppendEventCallback(CommitEvent, TxnEventCallback{Func: func(
			context.Context,
			TxnOperator,
			TxnEvent,
			any,
		) error {
			return assert.AnError
		}})

		err := tc.Commit(ctx)
		require.ErrorIs(t, err, assert.AnError)
		require.Equal(t, txnTerminalCallIdle, txnTerminalCallState(tc.terminalCall.Load()))
		tc.mu.RLock()
		require.False(t, tc.mu.closed)
		tc.mu.RUnlock()

		require.NoError(t, tc.Rollback(ctx))
		require.Equal(t, txnTerminalCallSealed, txnTerminalCallState(tc.terminalCall.Load()))
	})
}

func TestRestartRejectedUntilTerminalOwnerFinishesFinalization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	RunTxnTests(func(c TxnClient, _ rpc.TxnSender) {
		client := c.(*txnClient)
		op, err := c.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		tc := op.(*txnOperator)
		workspace := &blockingFinalizeWorkspace{
			trackingWorkspace: trackingWorkspace{
				commitRequests: []txn.TxnRequest{newTNRequest(1, 1)},
			},
			started: make(chan struct{}, 1),
			release: make(chan struct{}),
		}
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(workspace.release) }) }
		defer release()
		tc.AddWorkspace(workspace)

		commitC := make(chan error, 1)
		go func() { commitC <- tc.Commit(ctx) }()
		select {
		case <-workspace.started:
		case <-ctx.Done():
			t.Fatal("commit did not reach workspace finalization")
		}
		require.Equal(t, txnTerminalCallActive, txnTerminalCallState(tc.terminalCall.Load()))

		restartCtx, cancelRestart := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, err = client.RestartTxn(restartCtx, tc, timestamp.Timestamp{})
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed), err)
		require.NoError(t, restartCtx.Err(), "restart did not fail fast while terminal owner was finalizing")
		cancelRestart()

		release()
		require.NoError(t, <-commitC)
		restarted, err := client.RestartTxn(ctx, tc, timestamp.Timestamp{})
		require.NoError(t, err)
		require.NoError(t, restarted.Rollback(ctx))
	})
}

func TestRunSQLExitDoesNotBlockDeferredRollback(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
		closedC := make(chan struct{})
		tc.AppendEventCallback(ClosedEvent, TxnEventCallback{Func: func(context.Context, TxnOperator, TxnEvent, any) error {
			close(closedC)
			return nil
		}})
		workspace := &blockingRollbackWorkspace{
			started: make(chan struct{}, 1),
			release: make(chan struct{}),
		}
		tc.AddWorkspace(workspace)

		_, runningCancel := context.WithCancel(context.Background())
		runningToken := mustEnterRunSQL(t, tc, runningCancel, "stuck sql")
		commitCtx, cancelCommit := context.WithCancel(ctx)
		cancelCommit()
		require.ErrorIs(t, tc.Commit(commitCtx), context.Canceled)

		exitC := make(chan struct{})
		go func() {
			tc.ExitRunSqlWithToken(runningToken)
			close(exitC)
		}()
		select {
		case <-workspace.started:
		case <-time.After(time.Second):
			t.Fatal("deferred rollback did not start")
		}
		select {
		case <-exitC:
		case <-time.After(time.Second):
			t.Fatal("SQL exit blocked on deferred rollback")
		}

		close(workspace.release)
		select {
		case <-closedC:
		case <-time.After(time.Second):
			t.Fatal("deferred rollback did not finish")
		}
		require.Equal(t, 1, workspace.rollbackCount)
	})
}

func TestRunSQLTrackerDoesNotReuseTokensAcrossRestart(t *testing.T) {
	var tracker runSQLTracker
	oldToken := tracker.enterTokenWithSQL(nil, "old generation")
	require.NotZero(t, oldToken)
	require.Equal(t, "enter:1, exit:0", tracker.counterString())
	tracker.exitToken(oldToken)
	require.Equal(t, "enter:1, exit:1", tracker.counterString())

	tracker.reset(true)
	require.Equal(t, "enter:0, exit:0", tracker.counterString())
	tracker.open()
	newToken := tracker.enterTokenWithSQL(nil, "new generation")
	require.NotZero(t, newToken)
	require.NotEqual(t, oldToken, newToken)

	// A delayed or duplicated old-generation Exit must not remove current work.
	tracker.exitToken(oldToken)
	require.True(t, tracker.active())
	tracker.exitToken(newToken)
	require.False(t, tracker.active())

	exhausted := runSQLTracker{nextID: ^uint64(0)}
	exhaustedCtx, exhaustedCancel := context.WithCancel(context.Background())
	require.Zero(t, exhausted.enterTokenWithSQL(exhaustedCancel, "exhausted token space"))
	select {
	case <-exhaustedCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("rejected exhausted registration was not canceled")
	}
}

func BenchmarkRunSQLTrackerEnterExit(b *testing.B) {
	tc := &txnOperator{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		token, err := tc.TryEnterRunSqlWithTokenAndSQL(nil, "select 1")
		if err != nil {
			b.Fatal(err)
		}
		tc.ExitRunSqlWithToken(token)
	}
}

func BenchmarkRunSQLTrackerEnterExitParallel(b *testing.B) {
	tc := &txnOperator{}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			token, err := tc.TryEnterRunSqlWithTokenAndSQL(nil, "select 1")
			if err != nil {
				b.Fatal(err)
			}
			tc.ExitRunSqlWithToken(token)
		}
	})
}

func BenchmarkRunSQLTrackerRejected(b *testing.B) {
	tc := &txnOperator{}
	tc.reset.runSQLTracker.seal()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tc.TryEnterRunSqlWithTokenAndSQL(nil, "select 1"); err == nil {
			b.Fatal("sealed tracker accepted SQL")
		}
	}
}

func TestMalformedTNResponsesReturnErrors(t *testing.T) {
	tc := &txnOperator{}

	require.Error(t, tc.checkTxnError(&txn.TxnError{TxnErrCode: ^uint32(0)}, nil))
	require.Error(t, tc.checkResponseTxnStatusForReadWrite(txn.TxnResponse{
		Txn: &txn.TxnMeta{Status: txn.TxnStatus(99)},
	}))
	require.Error(t, tc.checkResponseTxnStatusForCommit(txn.TxnResponse{
		Txn: &txn.TxnMeta{Status: txn.TxnStatus_Active},
	}))
	require.Error(t, tc.checkResponseTxnStatusForRollback(txn.TxnResponse{
		Txn: &txn.TxnMeta{Status: txn.TxnStatus_Active},
	}))
}

func TestMalformedReadWriteResponseDoesNotPublishTxnState(t *testing.T) {
	for _, method := range []txn.TxnMethod{
		txn.TxnMethod_Read,
		txn.TxnMethod_Write,
	} {
		t.Run(method.String(), func(t *testing.T) {
			runOperatorTests(t, func(ctx context.Context, tc *txnOperator, ts *testTxnSender) {
				before := tc.Txn()
				poisonCommitTS := before.SnapshotTS.Next().Next()
				ts.setManual(func(result *rpc.SendResult, err error) (*rpc.SendResult, error) {
					require.NoError(t, err)
					require.NotEmpty(t, result.Responses)
					for i := range result.Responses {
						result.Responses[i].Txn.Status = txn.TxnStatus(99)
						result.Responses[i].Txn.CommitTS = poisonCommitTS
					}
					return result, nil
				})

				request := []txn.TxnRequest{newTNRequest(1, 1)}
				var result *rpc.SendResult
				var err error
				switch method {
				case txn.TxnMethod_Read:
					result, err = tc.Read(ctx, request)
				case txn.TxnMethod_Write:
					result, err = tc.Write(ctx, request)
				default:
					t.Fatalf("unexpected method %s", method)
				}
				require.Error(t, err)
				require.Nil(t, result)

				after := tc.Txn()
				require.Equal(t, before.Status, after.Status)
				require.Equal(t, before.CommitTS, after.CommitTS)
				require.Equal(t, txn.TxnStatus_Active, after.Status)

				// A protocol-invalid non-terminal response is an operation error, not
				// an implicit concurrent rollback. The caller retains terminal
				// ownership and can safely roll the still-active transaction back.
				ts.setAuto()
				require.NoError(t, tc.Rollback(ctx))
			})
		})
	}
}

type legacyRunSQLTxnOperator struct {
	TxnOperator
	token      uint64
	exitTokens []uint64
}

func (op *legacyRunSQLTxnOperator) EnterRunSqlWithTokenAndSQL(context.CancelFunc, string) uint64 {
	return op.token
}

func (op *legacyRunSQLTxnOperator) ExitRunSqlWithToken(token uint64) {
	op.exitTokens = append(op.exitTokens, token)
}

func TestTryEnterRunSQLSupportsLegacyTxnOperator(t *testing.T) {
	legacy := &legacyRunSQLTxnOperator{token: 42}
	token, err := TryEnterRunSqlWithTokenAndSQL(legacy, nil, "select 1")
	require.NoError(t, err)
	require.Equal(t, uint64(42), token)

	canceled := make(chan struct{})
	legacy.token = 0
	token, err = TryEnterRunSqlWithTokenAndSQL(legacy, func() { close(canceled) }, "select 2")
	require.Zero(t, token)
	require.NoError(t, err)
	legacy.ExitRunSqlWithToken(token)
	require.Equal(t, []uint64{0}, legacy.exitTokens)
	select {
	case <-canceled:
		t.Fatal("legacy zero token was incorrectly treated as admission rejection")
	default:
	}
}

func TestWithTxnLockWaitTimeout(t *testing.T) {
	runOperatorTests(
		t,
		func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
			require.Equal(t, time.Duration(0), LockWaitTimeoutFromTxn(tc))
		},
		WithTxnLockWaitTimeout(0),
	)

	runOperatorTests(
		t,
		func(ctx context.Context, tc *txnOperator, _ *testTxnSender) {
			require.Equal(t, 60*time.Second, LockWaitTimeoutFromTxn(tc))
			wrapped := struct{ TxnOperator }{TxnOperator: tc}
			require.Equal(t, 60*time.Second, LockWaitTimeoutFromTxn(wrapped))
		},
		WithTxnLockWaitTimeout(60*time.Second),
	)
}
