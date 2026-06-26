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
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
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

func forceLockRetryMemoryPressure(t *testing.T, level lockRetryMemoryPressureLevel) {
	oldPressure := getLockRetryMemoryPressureLevel
	getLockRetryMemoryPressureLevel = func() lockRetryMemoryPressureLevel {
		return level
	}
	t.Cleanup(func() {
		getLockRetryMemoryPressureLevel = oldPressure
	})
}

func TestLockWaitTimeoutUsesCurrentSessionValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().TxnOptions().Return(txnpb.TxnOptions{
		LockWaitTimeout: int64(60 * time.Second),
	}).AnyTimes()

	proc := process.NewTopProcess(
		context.Background(),
		mpool.MustNewZero(),
		nil,
		txnOp,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil)
	proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		require.Equal(t, "lock_wait_timeout", varName)
		require.True(t, isSystemVar)
		require.False(t, isGlobalVar)
		return int64(2), nil
	})
	require.Equal(t, 2*time.Second, lockWaitTimeout(proc, txnOp))

	proc.SetResolveVariableFunc(nil)
	proc.GetSessionInfo().LockWaitTimeout = 3
	require.Equal(t, 3*time.Second, lockWaitTimeout(proc, txnOp))

	proc.GetSessionInfo().LockWaitTimeout = 0
	require.Equal(t, 60*time.Second, lockWaitTimeout(proc, txnOp))
}

func TestLockOpHelpers(t *testing.T) {
	op := NewArgument()
	defer op.Release()

	require.Equal(t, opName, op.TypeName())
	require.Equal(t, opName+": lock-op()", op.String())

	op = op.WithLockSharding(lock.Sharding_KeepBind).WithLockGroup(7).WithLockMode(lock.LockMode_Shared).WithLockTable(true, true)
	require.Equal(t, lock.Sharding_KeepBind, op.targets[0].lockRows) // placeholder to keep compiler from optimizing? no
}

func TestRefreshLockWaitOptionsUsesRemainingDeadline(t *testing.T) {
	options := lock.LockOptions{
		LockWaitDeadline: time.Now().Add(1500 * time.Millisecond).UnixNano(),
		LockWaitTimeout:  60,
	}

	refreshed, err := refreshLockWaitOptions(options)
	require.NoError(t, err)
	require.Greater(t, refreshed.LockWaitTimeout, int64(0))
	require.LessOrEqual(t, refreshed.LockWaitTimeout, int64(2))
	require.Equal(t, options.LockWaitDeadline, refreshed.LockWaitDeadline)
}

func TestRefreshLockWaitOptionsReturnsTimeoutAfterDeadline(t *testing.T) {
	options := lock.LockOptions{LockWaitDeadline: time.Now().Add(-time.Second).UnixNano()}

	_, err := refreshLockWaitOptions(options)
	require.ErrorIs(t, err, lockservice.ErrLockTimeout)
}

func TestLockOpTargetHelpers(t *testing.T) {
	op := NewArgument()
	defer op.Release()

	expr := plan2.MakePlan2Int32ConstExprWithType(1)
	op.AddLockTarget(11, &plan.ObjectRef{SchemaName: "db", ObjName: "t1"}, 0, types.T_int32.ToType(), -1, -1, expr, false)
	op.AddLockTarget(22, &plan.ObjectRef{SchemaName: "db", ObjName: "t2"}, 1, types.T_int64.ToType(), -1, -1, nil, true)

	require.Equal(t, []*plan.Expr{expr}, op.GetLockRowsExpressions())

	folded, err := op.RewriteLockRowsExpressions(func(e *plan.Expr) (*plan.Expr, bool, error) {
		return plan2.MakePlan2Int32ConstExprWithType(2), true, nil
	})
	require.NoError(t, err)
	require.True(t, folded)
	require.NotNil(t, op.targets[0].lockRows)
	require.NotNil(t, op.targets[1].objRef)

	folded, err = op.RewriteLockRowsExpressions(func(e *plan.Expr) (*plan.Expr, bool, error) {
		return e, false, nil
	})
	require.NoError(t, err)
	require.False(t, folded)

	dst := NewArgument()
	defer dst.Release()
	dst.CopyTargetsFrom(op)
	require.Equal(t, len(op.targets), len(dst.targets))
	require.Equal(t, op.targets[0].tableID, dst.targets[0].tableID)
	require.Equal(t, op.targets[1].objRef.ObjName, dst.targets[1].objRef.ObjName)

	pipelineTargets := op.CopyToPipelineTarget()
	require.Len(t, pipelineTargets, 2)
	require.Equal(t, uint64(11), pipelineTargets[0].TableId)
	require.Equal(t, uint64(22), pipelineTargets[1].TableId)

	op.LockTable(11, true)
	require.True(t, op.targets[0].lockTable)
	require.True(t, op.targets[0].changeDef)
	require.Equal(t, lock.LockMode_Exclusive, op.targets[0].mode)

	op.LockTableWithMode(22, lock.LockMode_Shared, false)
	require.True(t, op.targets[1].lockTable)
	require.False(t, op.targets[1].changeDef)
	require.Equal(t, lock.LockMode_Shared, op.targets[1].mode)
}

func TestAddLockTargetWithPartitionAndMode(t *testing.T) {
	op := NewArgument()
	defer op.Release()

	one := op.AddLockTargetWithPartitionAndMode(
		[]uint64{33},
		lock.LockMode_Shared,
		0,
		types.T_int32.ToType(),
		1,
		nil,
		true,
		2,
	)
	require.Len(t, one.targets, 1)
	require.Equal(t, uint64(33), one.targets[0].tableID)
	require.Equal(t, int32(-1), one.targets[0].partitionColumnIndexInBatch)

	many := op.AddLockTargetWithPartition(
		[]uint64{44, 55},
		0,
		types.T_int64.ToType(),
		1,
		nil,
		false,
		3,
	)
	require.Len(t, many.targets, 3)
	require.Equal(t, uint64(44), many.targets[1].tableID)
	require.Equal(t, uint64(55), many.targets[2].tableID)
	require.Equal(t, int32(3), many.targets[1].filterColIndexInBatch)
	require.NotNil(t, many.targets[1].filter)
}

func TestHasNewVersionInRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	eng := mock_frontend.NewMockEngine(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txnpb.TxnMeta{ID: []byte("txn1")}).AnyTimes()
	proc := process.NewTopProcess(
		context.Background(),
		mpool.MustNewZero(),
		nil,
		txnOp,
		nil,
		eng,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	analyzer := process.NewTempAnalyzer()
	bat := batch.New(true)

	changed, err := hasNewVersionInRange(proc, rel, analyzer, 1, eng, nil, 0, -1, timestamp.Timestamp{}, timestamp.Timestamp{})
	require.NoError(t, err)
	require.False(t, changed)

	eng.EXPECT().GetRelationById(gomock.Any(), txnOp, uint64(2)).Return("", "", nil, assert.AnError)
	changed, err = hasNewVersionInRange(proc, nil, analyzer, 2, eng, bat, 0, -1, timestamp.Timestamp{}, timestamp.Timestamp{})
	require.Error(t, err)
	require.False(t, changed)

	eng.EXPECT().GetRelationById(gomock.Any(), txnOp, uint64(3)).Return("", "", rel, nil)
	rel.EXPECT().PrimaryKeysMayBeModified(gomock.Any(), gomock.Any(), gomock.Any(), bat, int32(0), int32(-1)).Return(true, nil)
	changed, err = hasNewVersionInRange(proc, nil, analyzer, 3, eng, bat, 0, -1, timestamp.Timestamp{}, timestamp.Timestamp{})
	require.NoError(t, err)
	require.True(t, changed)
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
	forceLockRetryMemoryPressure(t, lockRetryMemoryPressureNormal)

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
	forceLockRetryMemoryPressure(t, lockRetryMemoryPressureNormal)

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
	forceLockRetryMemoryPressure(t, lockRetryMemoryPressureNormal)

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

func TestLockWithRetryRetriesBindChangedInExplicitUserTxnBeforeLockHeld(t *testing.T) {
	forceLockRetryMemoryPressure(t, lockRetryMemoryPressureNormal)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lockSvc := mock_lock.NewMockLockService(ctrl)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)

	oldWait := defaultWaitTimeOnRetryLock
	defaultWaitTimeOnRetryLock = 50 * time.Millisecond
	defer func() {
		defaultWaitTimeOnRetryLock = oldWait
	}()

	ctx := context.Background()
	expected := lock.Result{HasConflict: true}

	gomock.InOrder(
		lockSvc.EXPECT().
			Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
			Return(lock.Result{}, moerr.NewLockTableBindChangedNoCtx()),
		txnOp.EXPECT().HasLockTable(uint64(1)).Return(false),
		lockSvc.EXPECT().
			Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
			Return(expected, nil),
	)

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
}

func TestLockWithRetryRetriesBindChangedInBeginTxnBeforeLockHeld(t *testing.T) {
	forceLockRetryMemoryPressure(t, lockRetryMemoryPressureNormal)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lockSvc := mock_lock.NewMockLockService(ctrl)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)

	oldWait := defaultWaitTimeOnRetryLock
	defaultWaitTimeOnRetryLock = 50 * time.Millisecond
	defer func() {
		defaultWaitTimeOnRetryLock = oldWait
	}()

	ctx := context.Background()
	expected := lock.Result{HasConflict: true}

	gomock.InOrder(
		lockSvc.EXPECT().
			Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
			Return(lock.Result{}, moerr.NewLockTableBindChangedNoCtx()),
		txnOp.EXPECT().HasLockTable(uint64(1)).Return(false),
		lockSvc.EXPECT().
			Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
			Return(expected, nil),
	)

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
}

func TestLockWithRetryRetriesBindChangedInAutocommitTxn(t *testing.T) {
	forceLockRetryMemoryPressure(t, lockRetryMemoryPressureNormal)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lockSvc := mock_lock.NewMockLockService(ctrl)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)

	oldWait := defaultWaitTimeOnRetryLock
	defaultWaitTimeOnRetryLock = time.Millisecond
	defer func() {
		defaultWaitTimeOnRetryLock = oldWait
	}()

	ctx := context.Background()
	expected := lock.Result{HasConflict: true}

	gomock.InOrder(
		lockSvc.EXPECT().
			Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
			Return(lock.Result{}, moerr.NewLockTableBindChangedNoCtx()),
		txnOp.EXPECT().HasLockTable(uint64(1)).Return(false),
		lockSvc.EXPECT().
			Lock(ctx, uint64(1), gomock.Nil(), []byte("txn1"), lock.LockOptions{}).
			Return(expected, nil),
	)

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
}

func TestLockWithRetryDoesNotRetryBindChangedWhenLockTableAlreadyHeld(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lockSvc := mock_lock.NewMockLockService(ctrl)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)

	oldWait := defaultWaitTimeOnRetryLock
	defaultWaitTimeOnRetryLock = 50 * time.Millisecond
	defer func() {
		defaultWaitTimeOnRetryLock = oldWait
	}()

	ctx := context.Background()
	expectedErr := moerr.NewLockTableBindChangedNoCtx()

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
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))
	require.Less(t, time.Since(start), defaultWaitTimeOnRetryLock)
}

func TestLockWithRetryStopsWhenBindChangedRetryBudgetExceeded(t *testing.T) {
	forceLockRetryMemoryPressure(t, lockRetryMemoryPressureNormal)

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
	expectedErr := moerr.NewLockTableBindChangedNoCtx()

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
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))
	require.GreaterOrEqual(t, elapsed, budget)
	require.Less(t, elapsed, 100*time.Millisecond)
}

func TestLockRetryBacksOffUnderHighMemoryPressure(t *testing.T) {
	oldPressure := getLockRetryMemoryPressureLevel
	oldBackoff := lockRetryMemoryBackoff
	oldBudget := defaultMaxWaitTimeOnRetryBackendLock
	defer func() {
		getLockRetryMemoryPressureLevel = oldPressure
		lockRetryMemoryBackoff = oldBackoff
		defaultMaxWaitTimeOnRetryBackendLock = oldBudget
	}()

	getLockRetryMemoryPressureLevel = func() lockRetryMemoryPressureLevel {
		return lockRetryMemoryPressureHigh
	}
	lockRetryMemoryBackoff = 25 * time.Millisecond
	defaultMaxWaitTimeOnRetryBackendLock = time.Second

	state := lockRetryState{}
	wait, ok := getRetryWaitDuration(moerr.NewLockTableBindChangedNoCtx(), &state)
	require.True(t, ok)
	require.Equal(t, lockRetryMemoryBackoff, wait)
	require.True(t, state.useMemoryRetrySlot)
}

func TestLockRetryHighMemoryUsesBoundedQueue(t *testing.T) {
	oldSlots := lockRetryHighMemorySlots
	defer func() {
		lockRetryHighMemorySlots = oldSlots
	}()

	lockRetryHighMemorySlots = make(chan struct{}, 1)
	lockRetryHighMemorySlots <- struct{}{}
	go func() {
		time.Sleep(20 * time.Millisecond)
		<-lockRetryHighMemorySlots
	}()

	state := lockRetryState{
		backendRetryDeadline: time.Now().Add(time.Second),
		useMemoryRetrySlot:   true,
	}
	start := time.Now()
	require.True(t, waitToRetryLock(context.Background(), time.Millisecond, &state))
	require.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)
}

func TestLockRetryHighMemoryQueueHonorsBackendRetryDeadline(t *testing.T) {
	oldSlots := lockRetryHighMemorySlots
	defer func() {
		lockRetryHighMemorySlots = oldSlots
	}()

	lockRetryHighMemorySlots = make(chan struct{}, 1)
	lockRetryHighMemorySlots <- struct{}{}

	state := lockRetryState{
		backendRetryDeadline: time.Now().Add(20 * time.Millisecond),
		useMemoryRetrySlot:   true,
	}
	start := time.Now()
	require.False(t, waitToRetryLock(context.Background(), time.Second, &state))
	require.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)
	require.Less(t, time.Since(start), 100*time.Millisecond)
}

func TestLockRetryStopsUnderCriticalMemoryPressure(t *testing.T) {
	oldPressure := getLockRetryMemoryPressureLevel
	oldBudget := defaultMaxWaitTimeOnRetryBackendLock
	defer func() {
		getLockRetryMemoryPressureLevel = oldPressure
		defaultMaxWaitTimeOnRetryBackendLock = oldBudget
	}()

	getLockRetryMemoryPressureLevel = func() lockRetryMemoryPressureLevel {
		return lockRetryMemoryPressureCritical
	}
	defaultMaxWaitTimeOnRetryBackendLock = time.Second

	state := lockRetryState{}
	_, ok := getRetryWaitDuration(moerr.NewLockTableBindChangedNoCtx(), &state)
	require.False(t, ok)
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

func TestCallLockOpLocksTableAtEOFWhenNoRowsProduced(t *testing.T) {
	tableID := uint64(10)
	runLockOpTest(
		t,
		func(proc *process.Process) {
			pkType := types.New(types.T_int32, 0, 0)
			arg := NewArgumentByEngine(nil)
			arg.OperatorBase.OperatorInfo = vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			}
			arg.AddLockTarget(tableID, nil, 0, pkType, -1, -1, nil, true)
			arg.LockTable(tableID, false)
			resetChildren(arg, nil)
			defer arg.Free(proc, false, nil)

			require.NoError(t, arg.Prepare(proc))
			_, err := vm.Exec(arg, proc)
			require.NoError(t, err)
			require.True(t, proc.GetTxnOperator().HasLockTable(tableID))
		},
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

				proc.GetTxnOperator().SetSnapshotTS(timestamp.Timestamp{PhysicalTime: math.MaxInt64})

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

// TestCopyTargetsFrom verifies that CopyTargetsFrom creates independent deep copies
// of targets, preventing race conditions in parallel execution.
func TestCopyTargetsFrom(t *testing.T) {
	src := NewArgument()
	defer src.Release()

	// Add targets to source
	src.AddLockTarget(
		100, // tableID
		&plan.ObjectRef{SchemaName: "test", ObjName: "t1"},
		0,                      // primaryColumnIndexInBatch
		types.T_int64.ToType(), // primaryColumnType
		-1,                     // partitionColIndexInBatch
		1,                      // refreshTimestampIndexInBatch
		nil,                    // lockRows
		false,                  // lockTableAtTheEnd
	)
	src.AddLockTarget(
		200,
		&plan.ObjectRef{SchemaName: "test", ObjName: "t2"},
		2,
		types.T_varchar.ToType(),
		-1,
		3,
		nil, // simplified - no lockRows expr
		true,
	)

	// Copy targets to destination
	dst := NewArgument()
	defer dst.Release()
	dst.CopyTargetsFrom(src)

	// Verify copy is independent
	require.Equal(t, len(src.targets), len(dst.targets), "targets length should match")

	// Modify source and verify destination is unchanged
	src.targets[0].tableID = 999
	src.targets[0].objRef.ObjName = "modified"
	require.Equal(t, uint64(100), dst.targets[0].tableID, "dst tableID should be unchanged")
	require.Equal(t, "t1", dst.targets[0].objRef.ObjName, "dst objRef should be unchanged")

	// Verify all fields were copied correctly
	require.Equal(t, uint64(200), dst.targets[1].tableID)
	require.Equal(t, "t2", dst.targets[1].objRef.ObjName)
	require.True(t, dst.targets[1].lockTableAtTheEnd)
}

// TestCopyTargetsFromEmpty verifies CopyTargetsFrom handles empty targets correctly.
func TestCopyTargetsFromEmpty(t *testing.T) {
	src := NewArgument()
	defer src.Release()

	dst := NewArgument()
	defer dst.Release()

	// Copy empty targets
	dst.CopyTargetsFrom(src)
	require.Nil(t, dst.targets)
}

func TestCopyToPipelineTargetIncludesPartitionColIdx(t *testing.T) {
	pkType := types.New(types.T_int32, 0, 0)
	arg := NewArgumentByEngine(nil)
	defer arg.Release()

	arg.AddLockTarget(1, nil, 0, pkType, 2, -1, nil, false)

	targets := arg.CopyToPipelineTarget()
	require.Len(t, targets, 1)
	require.Equal(t, int32(2), targets[0].PartitionColIdxInBat)
}

func TestLockTableIfLockCountIsZeroWithLockRows(t *testing.T) {
	runLockOpTest(t, func(proc *process.Process) {
		pkType := types.New(types.T_int32, 0, 0)
		arg := NewArgumentByEngine(nil)
		arg.OperatorBase.OperatorInfo = vm.OperatorInfo{Idx: 0}
		arg.AddLockTarget(1, nil, 0, pkType, -1, -1, plan2.MakePlan2Int32ConstExprWithType(42), false)

		require.NoError(t, arg.Prepare(proc))
		arg.ctr.hasNewVersionInRange = testFunc

		require.NoError(t, lockTalbeIfLockCountIsZero(proc, arg))

		arg.Free(proc, false, nil)
	})
}

func TestDedupLockRows(t *testing.T) {
	cases := []struct {
		name string
		in   [][]byte
		want [][]byte
	}{
		{
			name: "nil",
			in:   nil,
			want: nil,
		},
		{
			name: "empty",
			in:   [][]byte{},
			want: [][]byte{},
		},
		{
			name: "single",
			in:   [][]byte{{0x01}},
			want: [][]byte{{0x01}},
		},
		{
			name: "no-dup-already-sorted",
			in:   [][]byte{{0x01}, {0x02}, {0x03}},
			want: [][]byte{{0x01}, {0x02}, {0x03}},
		},
		{
			name: "no-dup-unsorted-output-sorted",
			in:   [][]byte{{0x03}, {0x01}, {0x02}},
			want: [][]byte{{0x01}, {0x02}, {0x03}},
		},
		{
			name: "with-dup-mixed",
			in:   [][]byte{{0x02}, {0x01}, {0x02}, {0x03}, {0x01}},
			want: [][]byte{{0x01}, {0x02}, {0x03}},
		},
		{
			name: "all-dup",
			in:   [][]byte{{0x05}, {0x05}, {0x05}},
			want: [][]byte{{0x05}},
		},
		{
			name: "multibyte-keys",
			in:   [][]byte{{0x01, 0x02}, {0x01, 0x01}, {0x01, 0x02}},
			want: [][]byte{{0x01, 0x01}, {0x01, 0x02}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := dedupLockRows(c.in)
			require.Equal(t, c.want, got)
		})
	}
}

func TestDedupLockRows_PreservesSetSemantics(t *testing.T) {
	a := [][]byte{{0x03}, {0x01}, {0x02}, {0x01}, {0x03}}
	b := [][]byte{{0x01}, {0x02}, {0x03}, {0x02}, {0x01}}

	require.Equal(t, dedupLockRows(a), dedupLockRows(b),
		"different orderings of the same multiset must dedupe to the same slice")
}

func TestDedupLockRows_Idempotent(t *testing.T) {
	in := [][]byte{{0x01}, {0x02}, {0x03}, {0x04}}
	once := dedupLockRows(in)
	twice := dedupLockRows(append([][]byte(nil), once...))
	require.Equal(t, once, twice)
}
