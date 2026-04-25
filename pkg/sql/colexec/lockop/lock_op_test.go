// Copyright 2023 Matrix Origin
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

package lockop

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	mock_lock "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var testFunc = func(
	proc *process.Process,
	rel engine.Relation,
	analyzer process.Analyzer,
	tableID uint64,
	eng engine.Engine,
	bat *batch.Batch,
	idx int32,
	partitionIdx int32,
	from, to timestamp.Timestamp) (bool, error) {
	return false, nil
}

var (
	sid = ""
)

func TestPerformLockSkipsRepeatedTableLock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	proc.Base.TxnOperator = txnOp

	txnOp.EXPECT().LockSkipped(uint64(1), lock.LockMode_Exclusive).Return(false)
	txnOp.EXPECT().HasLockTable(uint64(1)).Return(true)

	arg := NewArgumentByEngine(nil)
	arg.AddLockTarget(1, nil, 0, types.T_int32.ToType(), -1, -1, nil, false)
	arg.LockTable(1, false)

	bat := batch.NewWithSize(1)
	vec := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed(vec, int32(1), false, proc.Mp()))
	bat.Vecs[0] = vec
	bat.SetRowCount(1)
	defer func() {
		bat.Clean(proc.Mp())
		arg.Free(proc, false, nil)
	}()

	require.NoError(t, performLock(bat, proc, arg, nil, -1))
}

func TestLockWithRetryReturnsBackendErrorWhenDeadlineExceededStopsBoundedRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lockSvc := mock_lock.NewMockLockService(ctrl)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txnpb.TxnMeta{ID: []byte("txn1")}).AnyTimes()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	lockSvc.EXPECT().
		Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
		Return(lock.Result{}, moerr.NewBackendCannotConnectNoCtx("retryable")).
		Times(1)

	start := time.Now()
	_, err := lockWithRetry(
		ctx,
		lockSvc,
		1,
		nil,
		[]byte("txn1"),
		lock.LockOptions{},
		txnOp,
		nil,
		nil,
		LockOptions{},
		types.Type{},
	)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect))
	require.Less(t, time.Since(start), 200*time.Millisecond)
}

func TestLockWithRetryReturnsBackendErrorWhenCanceledContextStopsBoundedRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lockSvc := mock_lock.NewMockLockService(ctrl)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txnpb.TxnMeta{ID: []byte("txn1")}).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	lockSvc.EXPECT().
		Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
		Return(lock.Result{}, moerr.NewBackendCannotConnectNoCtx("retryable")).
		Times(1)

	start := time.Now()
	_, err := lockWithRetry(
		ctx,
		lockSvc,
		1,
		nil,
		[]byte("txn1"),
		lock.LockOptions{},
		txnOp,
		nil,
		nil,
		LockOptions{},
		types.Type{},
	)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect))
	require.Less(t, time.Since(start), 200*time.Millisecond)
}

func TestLockWithRetryReturnsBackendErrorWhenContextCanceledDuringRetryWait(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lockSvc := mock_lock.NewMockLockService(ctrl)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txnpb.TxnMeta{ID: []byte("txn1")}).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lockSvc.EXPECT().
		Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
		DoAndReturn(func(context.Context, uint64, [][]byte, []byte, lock.LockOptions) (lock.Result, error) {
			time.AfterFunc(20*time.Millisecond, cancel)
			return lock.Result{}, moerr.NewBackendCannotConnectNoCtx("retryable")
		}).
		Times(1)
	txnOp.EXPECT().HasLockTable(uint64(1)).Return(false).Times(1)

	start := time.Now()
	_, err := lockWithRetry(
		ctx,
		lockSvc,
		1,
		nil,
		[]byte("txn1"),
		lock.LockOptions{},
		txnOp,
		nil,
		nil,
		LockOptions{},
		types.Type{},
	)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect))
	require.Less(t, time.Since(start), defaultWaitTimeOnRetryLock)
}

func TestLockWithRetryStopsWhenBackendRetryBudgetExceeded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lockSvc := mock_lock.NewMockLockService(ctrl)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txnpb.TxnMeta{ID: []byte("txn1")}).AnyTimes()

	budget := 10 * time.Millisecond
	oldWait := defaultWaitTimeOnRetryLock
	oldBudget := defaultMaxWaitTimeOnRetryBackendLock
	defaultWaitTimeOnRetryLock = 50 * time.Millisecond
	defaultMaxWaitTimeOnRetryBackendLock = budget
	defer func() {
		defaultWaitTimeOnRetryLock = oldWait
		defaultMaxWaitTimeOnRetryBackendLock = oldBudget
	}()

	ctx := context.Background()
	expectedErr := moerr.NewBackendCannotConnectNoCtx("retryable")

	gomock.InOrder(
		lockSvc.EXPECT().
			Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
			Return(lock.Result{}, expectedErr),
		txnOp.EXPECT().HasLockTable(uint64(1)).Return(false),
		lockSvc.EXPECT().
			Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
			Return(lock.Result{}, expectedErr),
		txnOp.EXPECT().HasLockTable(uint64(1)).Return(false),
	)

	start := time.Now()
	_, err := lockWithRetry(
		ctx,
		lockSvc,
		1,
		nil,
		[]byte("txn1"),
		lock.LockOptions{},
		txnOp,
		nil,
		nil,
		LockOptions{},
		types.Type{},
	)
	elapsed := time.Since(start)
	require.ErrorIs(t, err, expectedErr)
	require.GreaterOrEqual(t, elapsed, budget)
	require.Less(t, elapsed, 100*time.Millisecond)
}

func TestLockWithRetryDoesNotResetBackendRetryBudgetAfterBindChange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lockSvc := mock_lock.NewMockLockService(ctrl)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txnpb.TxnMeta{ID: []byte("txn1")}).AnyTimes()

	oldWait := defaultWaitTimeOnRetryLock
	oldBudget := defaultMaxWaitTimeOnRetryBackendLock
	defaultWaitTimeOnRetryLock = 50 * time.Millisecond
	defaultMaxWaitTimeOnRetryBackendLock = 10 * time.Millisecond
	defer func() {
		defaultWaitTimeOnRetryLock = oldWait
		defaultMaxWaitTimeOnRetryBackendLock = oldBudget
	}()

	ctx := context.Background()
	gomock.InOrder(
		lockSvc.EXPECT().
			Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
			Return(lock.Result{}, moerr.NewBackendCannotConnectNoCtx("retryable")),
		txnOp.EXPECT().HasLockTable(uint64(1)).Return(false),
		lockSvc.EXPECT().
			Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
			Return(lock.Result{}, moerr.NewLockTableBindChangedNoCtx()),
		txnOp.EXPECT().HasLockTable(uint64(1)).Return(false),
	)

	start := time.Now()
	_, err := lockWithRetry(
		ctx,
		lockSvc,
		1,
		nil,
		[]byte("txn1"),
		lock.LockOptions{},
		txnOp,
		nil,
		nil,
		LockOptions{},
		types.Type{},
	)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))
	require.Less(t, time.Since(start), oldWait)
}

func TestLockWithRetryStopsWhenRollingRestartRetryBudgetExceeded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lockSvc := mock_lock.NewMockLockService(ctrl)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txnpb.TxnMeta{ID: []byte("txn1")}).AnyTimes()

	oldWait := defaultWaitTimeOnRetryLock
	oldBudget := defaultMaxWaitTimeOnRetryBackendLock
	defaultWaitTimeOnRetryLock = 50 * time.Millisecond
	defaultMaxWaitTimeOnRetryBackendLock = 10 * time.Millisecond
	defer func() {
		defaultWaitTimeOnRetryLock = oldWait
		defaultMaxWaitTimeOnRetryBackendLock = oldBudget
	}()

	ctx := context.Background()
	expectedErr := moerr.NewRetryForCNRollingRestart()

	gomock.InOrder(
		lockSvc.EXPECT().
			Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
			Return(lock.Result{}, expectedErr),
		lockSvc.EXPECT().
			Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
			Return(lock.Result{}, expectedErr),
	)

	start := time.Now()
	_, err := lockWithRetry(
		ctx,
		lockSvc,
		1,
		nil,
		[]byte("txn1"),
		lock.LockOptions{},
		txnOp,
		nil,
		nil,
		LockOptions{},
		types.Type{},
	)
	require.ErrorIs(t, err, expectedErr)
	require.Less(t, time.Since(start), oldWait)
}

func TestLockWithRetryDoesNotRetryBackendErrorWhenLockTableAlreadyHeld(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lockSvc := mock_lock.NewMockLockService(ctrl)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)

	oldWait := defaultWaitTimeOnRetryLock
	oldBudget := defaultMaxWaitTimeOnRetryBackendLock
	defaultWaitTimeOnRetryLock = 50 * time.Millisecond
	defaultMaxWaitTimeOnRetryBackendLock = 10 * time.Millisecond
	defer func() {
		defaultWaitTimeOnRetryLock = oldWait
		defaultMaxWaitTimeOnRetryBackendLock = oldBudget
	}()

	ctx := context.Background()
	expectedErr := moerr.NewBackendCannotConnectNoCtx("retryable")

	gomock.InOrder(
		lockSvc.EXPECT().
			Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
			Return(lock.Result{}, expectedErr),
		txnOp.EXPECT().HasLockTable(uint64(1)).Return(true),
	)

	start := time.Now()
	_, err := lockWithRetry(
		ctx,
		lockSvc,
		1,
		nil,
		[]byte("txn1"),
		lock.LockOptions{},
		txnOp,
		nil,
		nil,
		LockOptions{},
		types.Type{},
	)
	require.ErrorIs(t, err, expectedErr)
	require.Less(t, time.Since(start), defaultWaitTimeOnRetryLock)
}

func TestLockWithRetryFailsFastWhenBackendRetryBudgetDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lockSvc := mock_lock.NewMockLockService(ctrl)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txnpb.TxnMeta{ID: []byte("txn1")}).AnyTimes()

	oldWait := defaultWaitTimeOnRetryLock
	oldBudget := defaultMaxWaitTimeOnRetryBackendLock
	defaultWaitTimeOnRetryLock = 50 * time.Millisecond
	defaultMaxWaitTimeOnRetryBackendLock = 0
	defer func() {
		defaultWaitTimeOnRetryLock = oldWait
		defaultMaxWaitTimeOnRetryBackendLock = oldBudget
	}()

	ctx := context.Background()
	expectedErr := moerr.NewBackendCannotConnectNoCtx("retryable")

	gomock.InOrder(
		lockSvc.EXPECT().
			Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
			Return(lock.Result{}, expectedErr),
		txnOp.EXPECT().HasLockTable(uint64(1)).Return(false),
	)

	start := time.Now()
	_, err := lockWithRetry(
		ctx,
		lockSvc,
		1,
		nil,
		[]byte("txn1"),
		lock.LockOptions{},
		txnOp,
		nil,
		nil,
		LockOptions{},
		types.Type{},
	)
	require.ErrorIs(t, err, expectedErr)
	require.Less(t, time.Since(start), defaultWaitTimeOnRetryLock)
}

func TestLockWithRetryRetriesInsideLoopAndReturnsSecondResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lockSvc := mock_lock.NewMockLockService(ctrl)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)

	ctx := context.Background()
	expected := lock.Result{HasConflict: true}

	gomock.InOrder(
		lockSvc.EXPECT().
			Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
			Return(lock.Result{}, moerr.NewBackendCannotConnectNoCtx("retryable")),
		txnOp.EXPECT().HasLockTable(uint64(1)).Return(false),
		lockSvc.EXPECT().
			Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
			Return(expected, nil),
	)

	start := time.Now()
	result, err := lockWithRetry(
		ctx,
		lockSvc,
		1,
		nil,
		[]byte("txn1"),
		lock.LockOptions{},
		txnOp,
		nil,
		nil,
		LockOptions{},
		types.Type{},
	)
	require.NoError(t, err)
	require.Equal(t, expected, result)
	require.GreaterOrEqual(t, time.Since(start), defaultWaitTimeOnRetryLock)
}

func TestLockWithRetryKeepsSuccessfulResultAfterContextCanceled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lockSvc := mock_lock.NewMockLockService(ctrl)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lockSvc.EXPECT().
		Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
		DoAndReturn(func(context.Context, uint64, [][]byte, []byte, lock.LockOptions) (lock.Result, error) {
			cancel()
			return lock.Result{}, nil
		}).
		Times(1)

	_, err := lockWithRetry(
		ctx,
		lockSvc,
		1,
		nil,
		[]byte("txn1"),
		lock.LockOptions{},
		txnOp,
		nil,
		nil,
		LockOptions{},
		types.Type{},
	)
	require.NoError(t, err)
}

func TestLockWithRetryKeepsNonRetryableErrorAfterContextCanceled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lockSvc := mock_lock.NewMockLockService(ctrl)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	expectedErr := moerr.NewInternalErrorNoCtx("boom")

	lockSvc.EXPECT().
		Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
		DoAndReturn(func(context.Context, uint64, [][]byte, []byte, lock.LockOptions) (lock.Result, error) {
			cancel()
			return lock.Result{}, expectedErr
		}).
		Times(1)

	_, err := lockWithRetry(
		ctx,
		lockSvc,
		1,
		nil,
		[]byte("txn1"),
		lock.LockOptions{},
		txnOp,
		nil,
		nil,
		LockOptions{},
		types.Type{},
	)
	require.ErrorIs(t, err, expectedErr)
}

func TestCallLockOpWithNoConflict(t *testing.T) {
	runLockNonBlockingOpTest(
		t,
		[]uint64{1},
		[][]int32{{0, 1, 2}},
		func(proc *process.Process, arg *LockOp) {
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = testFunc
			result, err := vm.Exec(arg, proc)
			require.NoError(t, err)

			vec := result.Batch.GetVector(1)
			values := vector.MustFixedColWithTypeCheck[types.TS](vec)
			assert.Equal(t, 3, len(values))
			for _, v := range values {
				assert.Equal(t, types.TS{}, v)
			}
		},
		client.WithEnableRefreshExpression(),
	)
}

func TestCallLockOpWithConflict(t *testing.T) {
	tableID := uint64(10)
	runLockNonBlockingOpTest(
		t,
		[]uint64{tableID},
		[][]int32{{0, 1, 2}},
		func(proc *process.Process, arg *LockOp) {
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = testFunc

			arg.ctr.parker.Reset()
			arg.ctr.parker.EncodeInt32(0)
			conflictRow := arg.ctr.parker.Bytes()
			_, err := proc.GetLockService().Lock(
				proc.Ctx,
				tableID,
				[][]byte{conflictRow},
				[]byte("txn01"),
				lock.LockOptions{})
			require.NoError(t, err)

			c := make(chan struct{})
			go func() {
				defer close(c)
				result, err := vm.Exec(arg, proc)
				require.NoError(t, err)

				vec := result.Batch.GetVector(1)
				values := vector.MustFixedColWithTypeCheck[types.TS](vec)
				assert.Equal(t, 3, len(values))
				for _, v := range values {
					assert.Equal(t, types.BuildTS(math.MaxInt64, 1), v)
				}
			}()
			require.NoError(t, lockservice.WaitWaiters(proc.GetLockService(), 0, tableID, conflictRow, 1))
			require.NoError(t, proc.GetLockService().Unlock(proc.Ctx, []byte("txn01"), timestamp.Timestamp{PhysicalTime: math.MaxInt64}))
			<-c
		},
		client.WithEnableRefreshExpression(),
	)
}

func TestCallLockOpWithConflictWithRefreshNotEnabled(t *testing.T) {
	tableID := uint64(10)
	runLockNonBlockingOpTest(
		t,
		[]uint64{tableID},
		[][]int32{{0, 1, 2}},
		func(proc *process.Process, arg *LockOp) {
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = testFunc

			arg.ctr.parker.Reset()
			arg.ctr.parker.EncodeInt32(0)
			conflictRow := arg.ctr.parker.Bytes()
			_, err := proc.GetLockService().Lock(
				proc.Ctx,
				tableID,
				[][]byte{conflictRow},
				[]byte("txn01"),
				lock.LockOptions{})
			require.NoError(t, err)

			c := make(chan struct{})
			go func() {
				defer close(c)
				arg2 := &LockOp{
					OperatorBase: vm.OperatorBase{
						OperatorInfo: vm.OperatorInfo{
							Idx:     1,
							IsFirst: false,
							IsLast:  false,
						},
					},
				}
				arg2.ctr.retryError = nil
				arg2.targets = arg.targets
				arg2.Prepare(proc)
				arg2.ctr.hasNewVersionInRange = testFunc
				child := arg.GetChildren(0).(*colexec.MockOperator)
				resetChildren(arg2, child.GetBatchs()[0])
				defer arg2.ctr.parker.Close()

				_, err = vm.Exec(arg2, proc)
				assert.NoError(t, err)

				resetChildren(arg2, nil)
				_, err = vm.Exec(arg2, proc)
				require.Error(t, err)
				assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
			}()
			require.NoError(t, lockservice.WaitWaiters(proc.GetLockService(), 0, tableID, conflictRow, 1))
			require.NoError(t, proc.GetLockService().Unlock(proc.Ctx, []byte("txn01"), timestamp.Timestamp{PhysicalTime: math.MaxInt64}))
			<-c
		},
	)
}

func TestCallLockOpWithHasPrevCommit(t *testing.T) {
	tableID := uint64(10)
	runLockNonBlockingOpTest(
		t,
		[]uint64{tableID},
		[][]int32{{0, 1, 2}},
		func(proc *process.Process, arg *LockOp) {
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = testFunc

			arg.ctr.parker.Reset()
			arg.ctr.parker.EncodeInt32(0)
			conflictRow := arg.ctr.parker.Bytes()

			// txn01 commit
			_, err := proc.GetLockService().Lock(
				proc.Ctx,
				tableID,
				[][]byte{conflictRow},
				[]byte("txn01"),
				lock.LockOptions{})
			require.NoError(t, err)
			require.NoError(t, proc.GetLockService().Unlock(proc.Ctx, []byte("txn01"), timestamp.Timestamp{PhysicalTime: math.MaxInt64}))

			// txn02 abort
			_, err = proc.GetLockService().Lock(
				proc.Ctx,
				tableID,
				[][]byte{conflictRow},
				[]byte("txn02"),
				lock.LockOptions{})
			require.NoError(t, err)

			c := make(chan struct{})
			go func() {
				defer close(c)
				arg2 := &LockOp{
					OperatorBase: vm.OperatorBase{
						OperatorInfo: vm.OperatorInfo{
							Idx:     1,
							IsFirst: false,
							IsLast:  false,
						},
					},
				}
				arg2.ctr.retryError = nil
				arg2.targets = arg.targets
				arg2.Prepare(proc)
				arg2.ctr.hasNewVersionInRange = testFunc
				child := arg.GetChildren(0).(*colexec.MockOperator)
				resetChildren(arg2, child.GetBatchs()[0])
				defer arg2.ctr.parker.Close()

				_, err = vm.Exec(arg2, proc)
				assert.NoError(t, err)

				resetChildren(arg2, nil)
				_, err = vm.Exec(arg2, proc)
				require.Error(t, err)
				assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry))
			}()
			require.NoError(t, lockservice.WaitWaiters(proc.GetLockService(), 0, tableID, conflictRow, 1))
			require.NoError(t, proc.GetLockService().Unlock(proc.Ctx, []byte("txn02"), timestamp.Timestamp{}))
			<-c
		},
	)
}

func TestCallLockOpWithHasPrevCommitLessMe(t *testing.T) {
	tableID := uint64(10)
	runLockNonBlockingOpTest(
		t,
		[]uint64{tableID},
		[][]int32{{0, 1, 2}},
		func(proc *process.Process, arg *LockOp) {
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = testFunc

			arg.ctr.parker.Reset()
			arg.ctr.parker.EncodeInt32(0)
			conflictRow := arg.ctr.parker.Bytes()

			// txn01 commit
			_, err := proc.GetLockService().Lock(
				proc.Ctx,
				tableID,
				[][]byte{conflictRow},
				[]byte("txn01"),
				lock.LockOptions{})
			require.NoError(t, err)
			require.NoError(t, proc.GetLockService().Unlock(proc.Ctx, []byte("txn01"), timestamp.Timestamp{PhysicalTime: math.MaxInt64 - 1}))

			// txn02 abort
			_, err = proc.GetLockService().Lock(
				proc.Ctx,
				tableID,
				[][]byte{conflictRow},
				[]byte("txn02"),
				lock.LockOptions{})
			require.NoError(t, err)

			c := make(chan struct{})
			go func() {
				defer close(c)
				arg2 := &LockOp{
					OperatorBase: vm.OperatorBase{
						OperatorInfo: vm.OperatorInfo{
							Idx:     1,
							IsFirst: false,
							IsLast:  false,
						},
					},
				}
				arg2.ctr.retryError = nil
				arg2.targets = arg.targets
				arg2.Prepare(proc)
				arg2.ctr.hasNewVersionInRange = testFunc
				child := arg.GetChildren(0).(*colexec.MockOperator)
				resetChildren(arg2, child.GetBatchs()[0])
				defer arg2.ctr.parker.Close()

				proc.GetTxnOperator().TxnRef().SnapshotTS = timestamp.Timestamp{PhysicalTime: math.MaxInt64}

				_, err = vm.Exec(arg2, proc)
				assert.NoError(t, err)

				resetChildren(arg2, nil)
				_, err = vm.Exec(arg2, proc)
				require.NoError(t, err)
			}()
			require.NoError(t, lockservice.WaitWaiters(proc.GetLockService(), 0, tableID, conflictRow, 1))
			require.NoError(t, proc.GetLockService().Unlock(proc.Ctx, []byte("txn02"), timestamp.Timestamp{}))
			<-c
		},
	)
}

func TestLockWithHasNewVersionInLockedTS(t *testing.T) {
	tw := client.NewTimestampWaiter(runtime.GetLogger(""))
	stopper := stopper.NewStopper("")
	stopper.RunTask(func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Millisecond * 100):
				tw.NotifyLatestCommitTS(timestamp.Timestamp{PhysicalTime: time.Now().UTC().UnixNano()})
			}
		}
	})
	runLockNonBlockingOpTest(
		t,
		[]uint64{1},
		[][]int32{{0, 1, 2}},
		func(proc *process.Process, arg *LockOp) {
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = func(
				proc *process.Process,
				rel engine.Relation,
				analyzer process.Analyzer,
				tableID uint64,
				eng engine.Engine,
				bat *batch.Batch,
				idx int32,
				partitionIdx int32,
				from, to timestamp.Timestamp) (bool, error) {
				return true, nil
			}

			_, err := vm.Exec(arg, proc)
			require.NoError(t, err)
			require.Error(t, arg.ctr.retryError)
			require.True(t, moerr.IsMoErrCode(arg.ctr.retryError, moerr.ErrTxnNeedRetry))
		},
		client.WithTimestampWaiter(tw),
	)
	stopper.Stop()
}

func TestLockOpResetClearsLockCount(t *testing.T) {
	arg := NewArgumentByEngine(nil)
	arg.ctr.lockCount = 7
	arg.ctr.defChanged = true
	arg.ctr.retryError = moerr.NewTxnNeedRetryNoCtx()

	arg.Reset(nil, false, nil)

	require.Equal(t, int64(0), arg.ctr.lockCount)
	require.False(t, arg.ctr.defChanged)
	require.Nil(t, arg.ctr.retryError)
}

func runLockNonBlockingOpTest(
	t *testing.T,
	tables []uint64,
	values [][]int32,
	fn func(*process.Process, *LockOp),
	opts ...client.TxnClientCreateOption) {
	runLockOpTest(
		t,
		func(proc *process.Process) {
			bat := batch.NewWithSize(len(tables) * 2)
			bat.SetRowCount(len(tables) * 2)

			defer func() {
				bat.Clean(proc.Mp())
			}()

			offset := int32(0)
			pkType := types.New(types.T_int32, 0, 0)
			tsType := types.New(types.T_TS, 0, 0)
			arg := NewArgumentByEngine(nil)
			arg.OperatorBase.OperatorInfo = vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			}
			for idx, table := range tables {
				arg.AddLockTarget(table, nil, offset, pkType, -1, offset+1, nil, true)

				vec := vector.NewVec(pkType)
				vector.AppendFixedList(vec, values[idx], nil, proc.Mp())
				bat.Vecs[offset] = vec

				vec = vector.NewVec(tsType)
				bat.Vecs[offset+1] = vec
				offset += 2
			}
			resetChildren(arg, bat)

			fn(proc, arg)
			arg.Free(proc, false, nil)
		},
		opts...)
}

func runLockOpTest(
	t *testing.T,
	fn func(*process.Process),
	opts ...client.TxnClientCreateOption) {
	defer leaktest.AfterTest(t)()
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			runtime.SetupServiceBasedRuntime("s1", rt)

			lockservice.RunLockServicesForTest(
				zap.DebugLevel,
				[]string{"s1"},
				time.Second,
				func(_ lockservice.LockTableAllocator, services []lockservice.LockService) {
					rt.SetGlobalVariables(runtime.LockService, services[0])

					// TODO: remove
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					s, err := rpc.NewSender(rpc.Config{}, rt)
					require.NoError(t, err)

					opts = append(opts, client.WithLockService(services[0]))
					c := client.NewTxnClient(sid, s, opts...)
					c.Resume()
					defer func() {
						assert.NoError(t, c.Close())
					}()
					txnOp, err := c.New(ctx, timestamp.Timestamp{})
					require.NoError(t, err)

					proc := process.NewTopProcess(
						ctx,
						mpool.MustNewZero(),
						c,
						txnOp,
						nil,
						services[0],
						nil,
						nil,
						nil,
						nil)
					require.Equal(t, int64(0), proc.Mp().CurrNB())
					defer func() {
						require.Equal(t, int64(0), proc.Mp().CurrNB())
					}()
					fn(proc)
				},
				nil,
			)
		},
	)
}

func resetChildren(arg *LockOp, bat *batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}
