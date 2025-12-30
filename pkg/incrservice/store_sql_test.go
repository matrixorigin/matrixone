// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package incrservice

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	mock_executor "github.com/matrixorigin/matrixone/pkg/util/executor/test"
)

func TestDeleteWhenAccountNotExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	ctx = defines.AttachAccountId(ctx, 12)

	mockSqlExecutor := mock_executor.NewMockSQLExecutor(ctrl)
	mockSqlExecutor.EXPECT().ExecTxn(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, execFunc func(txn executor.TxnExecutor) error, opts executor.Options) error {
		return execFunc(nil)
	}).AnyTimes()

	// account not exists
	var executedSQLs []string
	mockSqlExecutor.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
		executedSQLs = append(executedSQLs, sql)
		res := executor.Result{}
		return res, nil
	}).AnyTimes()

	s := &sqlStore{
		exec: mockSqlExecutor,
	}

	err := s.Delete(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, 1, len(executedSQLs))
}

func TestDeleteWhenAccountExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	ctx = defines.AttachAccountId(ctx, 12)

	mockSqlExecutor := mock_executor.NewMockSQLExecutor(ctrl)
	mockSqlExecutor.EXPECT().ExecTxn(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, execFunc func(txn executor.TxnExecutor) error, opts executor.Options) error {
		return execFunc(nil)
	}).AnyTimes()
	var executedSQLs []string
	mockSqlExecutor.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
		executedSQLs = append(executedSQLs, sql)
		bat := &batch.Batch{}
		bat.SetRowCount(1)
		res := executor.Result{
			Batches: []*batch.Batch{bat},
		}
		return res, nil
	}).AnyTimes()

	s := &sqlStore{
		exec: mockSqlExecutor,
	}

	err := s.Delete(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(executedSQLs))
}

var _ lockservice.LockService = new(testLockService)

type testLockService struct {
}

func (tls *testLockService) GetServiceID() string {
	return ""
}

func (tls *testLockService) GetConfig() lockservice.Config {
	return lockservice.Config{
		ServiceID: "",
	}
}

func (tls *testLockService) Lock(ctx context.Context, tableID uint64, rows [][]byte, txnID []byte, options lock.LockOptions) (lock.Result, error) {
	//TODO implement me
	panic("implement me")
}

func (tls *testLockService) Unlock(ctx context.Context, txnID []byte, commitTS timestamp.Timestamp, mutations ...lock.ExtraMutation) error {
	//TODO implement me
	panic("implement me")
}

func (tls *testLockService) IsOrphanTxn(ctx context.Context, bytes []byte) (bool, error) {
	return false, moerr.NewInternalErrorNoCtx("return error")
}

func (tls *testLockService) Close() error {
	//TODO implement me
	panic("implement me")
}

func (tls *testLockService) GetWaitingList(ctx context.Context, txnID []byte) (bool, []lock.WaitTxn, error) {
	//TODO implement me
	panic("implement me")
}

func (tls *testLockService) ForceRefreshLockTableBinds(targets []uint64, matcher func(bind lock.LockTable) bool) {
	//TODO implement me
	panic("implement me")
}

func (tls *testLockService) GetLockTableBind(group uint32, tableID uint64) (lock.LockTable, error) {
	//TODO implement me
	panic("implement me")
}

func (tls *testLockService) IterLocks(f func(tableID uint64, keys [][]byte, lock lockservice.Lock) bool) {
	//TODO implement me
	panic("implement me")
}

func (tls *testLockService) CloseRemoteLockTable(group uint32, tableID, version uint64) (bool, error) {
	//TODO implement me
	panic("implement me")
}

var _ client.TxnOperator = new(testTxnOperator)

type testTxnOperator struct {
}

func (tTxnOp *testTxnOperator) GetOverview() client.TxnOverview {
	return client.TxnOverview{}
}

func (tTxnOp *testTxnOperator) CloneSnapshotOp(snapshot timestamp.Timestamp) client.TxnOperator {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) IsSnapOp() bool {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) Txn() txn.TxnMeta {
	return txn.TxnMeta{}
}

func (tTxnOp *testTxnOperator) TxnOptions() txn.TxnOptions {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) TxnRef() *txn.TxnMeta {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) Snapshot() (txn.CNTxnSnapshot, error) {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) UpdateSnapshot(ctx context.Context, ts timestamp.Timestamp) error {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) SnapshotTS() timestamp.Timestamp {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) CreateTS() timestamp.Timestamp {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) Status() txn.TxnStatus {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) ApplySnapshot(data []byte) error {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) Read(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) Write(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) WriteAndCommit(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) Commit(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) Rollback(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) AddLockTable(locktable lock.LockTable) error {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) HasLockTable(table uint64) bool {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) AddWaitLock(tableID uint64, rows [][]byte, opt lock.LockOptions) uint64 {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) RemoveWaitLock(key uint64) {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) LockSkipped(tableID uint64, mode lock.LockMode) bool {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) GetWaitActiveCost() time.Duration {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) AddWorkspace(workspace client.Workspace) {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) GetWorkspace() client.Workspace {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) AppendEventCallback(event client.EventType, callbacks ...func(client.TxnEvent)) {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) Debug(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) NextSequence() uint64 {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) EnterRunSqlWithTokenAndSQL(_ context.CancelFunc, _ string) uint64 {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) ExitRunSqlWithToken(_ uint64) {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) EnterIncrStmt() {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) ExitIncrStmt() {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) EnterRollbackStmt() {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) ExitRollbackStmt() {
	//TODO implement me
	panic("implement me")
}

func (tTxnOp *testTxnOperator) SetFootPrints(id int, enter bool) {
	//TODO implement me
	panic("implement me")
}

func Test_Allocate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	ctx = defines.AttachAccountId(ctx, 12)

	txnOp := &testTxnOperator{}

	var updateCnt atomic.Int32

	sqlExecutor := executor.NewMemExecutor2(
		func(sql string) (executor.Result, error) {
			if strings.HasPrefix(sql, "select offset, step from mo_increment_columns where table_id") {
				typs := []types.Type{
					types.New(types.T_uint64, 64, 0),
					types.New(types.T_uint64, 64, 0),
				}

				memRes := executor.NewMemResult(
					typs,
					mpool.MustNewZero())
				memRes.NewBatch()
				executor.AppendFixedRows(memRes, 0, []uint64{1})
				executor.AppendFixedRows(memRes, 1, []uint64{1})
				return memRes.GetResult(), nil
			} else if strings.HasPrefix(sql, "update mo_increment_columns set offset =") {
				if updateCnt.Load() > 0 {
					return executor.Result{AffectedRows: 1}, nil
				}
				updateCnt.Add(1)
			}

			return executor.Result{}, nil
		},
		txnOp,
	)

	s := &sqlStore{
		exec: sqlExecutor,
		ls:   &testLockService{},
	}

	_, _, _, err := s.Allocate(ctx, 10, "a", 1, nil)
	require.NoError(t, err)
	//require.Equal(t, 2, len(executedSQLs))
}

func Test_Allocate_Retry_When_Rows_Count_Invalid(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	ctx = defines.AttachAccountId(ctx, 12)

	txnOp := &testTxnOperator{}

	var selectCnt atomic.Int32

	sqlExecutor := executor.NewMemExecutor2(
		func(sql string) (executor.Result, error) {
			if strings.HasPrefix(sql, "select offset, step from mo_increment_columns where table_id") {
				cnt := selectCnt.Add(1)
				if cnt == 1 {
					// Return 0 rows
					typs := []types.Type{
						types.New(types.T_uint64, 64, 0),
						types.New(types.T_uint64, 64, 0),
					}
					memRes := executor.NewMemResult(typs, mpool.MustNewZero())
					// No rows added
					return memRes.GetResult(), nil
				}

				// Return 1 row
				typs := []types.Type{
					types.New(types.T_uint64, 64, 0),
					types.New(types.T_uint64, 64, 0),
				}

				memRes := executor.NewMemResult(
					typs,
					mpool.MustNewZero())
				memRes.NewBatch()
				executor.AppendFixedRows(memRes, 0, []uint64{1})
				executor.AppendFixedRows(memRes, 1, []uint64{1})
				return memRes.GetResult(), nil

			} else if strings.HasPrefix(sql, "update mo_increment_columns set offset =") {
				return executor.Result{AffectedRows: 1}, nil
			}

			return executor.Result{}, nil
		},
		txnOp,
	)

	s := &sqlStore{
		exec: sqlExecutor,
		ls:   &testLockService{},
	}

	_, _, _, err := s.Allocate(ctx, 10, "a", 1, nil)
	require.NoError(t, err)
	require.Equal(t, int32(2), selectCnt.Load())
}

func Test_Allocate_Retry_When_AffectedRows_Invalid(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	ctx = defines.AttachAccountId(ctx, 12)

	txnOp := &testTxnOperator{}

	var updateCnt atomic.Int32

	sqlExecutor := executor.NewMemExecutor2(
		func(sql string) (executor.Result, error) {
			if strings.HasPrefix(sql, "select offset, step from mo_increment_columns where table_id") {
				typs := []types.Type{
					types.New(types.T_uint64, 64, 0),
					types.New(types.T_uint64, 64, 0),
				}

				memRes := executor.NewMemResult(
					typs,
					mpool.MustNewZero())
				memRes.NewBatch()
				executor.AppendFixedRows(memRes, 0, []uint64{1})
				executor.AppendFixedRows(memRes, 1, []uint64{1})
				return memRes.GetResult(), nil
			} else if strings.HasPrefix(sql, "update mo_increment_columns set offset =") {
				cnt := updateCnt.Add(1)
				if cnt == 1 {
					return executor.Result{AffectedRows: 0}, nil
				}
				return executor.Result{AffectedRows: 1}, nil
			}

			return executor.Result{}, nil
		},
		txnOp,
	)

	s := &sqlStore{
		exec: sqlExecutor,
		ls:   &testLockService{},
	}

	_, _, _, err := s.Allocate(ctx, 10, "a", 1, nil)
	require.NoError(t, err)
	require.Equal(t, int32(2), updateCnt.Load())
}
