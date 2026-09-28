// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lockop

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type admissionLockService struct {
	lockservice.LockService
	lockFn func(context.Context, uint64, [][]byte, []byte, lock.LockOptions) (lock.Result, error)
}

func (s admissionLockService) Lock(ctx context.Context, table uint64, rows [][]byte, txn []byte, opts lock.LockOptions) (lock.Result, error) {
	return s.lockFn(ctx, table, rows, txn, opts)
}

func admissionKeys(t *testing.T, proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("SNAPSHOT"), false, proc.Mp()))
	return bat
}

func TestLockRowsAdmissionKeepsOwnerWithoutSnapshotWait(t *testing.T) {
	waiter := &failAfterInitialTimestampWaiter{}
	runLockOpTest(t, func(proc *process.Process) {
		bat := admissionKeys(t, proc)
		defer bat.Clean(proc.Mp())
		op := proc.GetTxnOperator()
		defer func() { require.NoError(t, op.Rollback(proc.Ctx)) }()
		snapshot := op.Txn().SnapshotTS
		ls := proc.GetLockService()
		var granted lock.Result
		proc.Base.LockService = admissionLockService{LockService: ls, lockFn: func(ctx context.Context, table uint64, rows [][]byte, id []byte, opts lock.LockOptions) (lock.Result, error) {
			require.Equal(t, lock.Granularity_Row, opts.Granularity)
			require.Equal(t, uint32(7), opts.Group)
			require.True(t, opts.KeepRows)
			require.Equal(t, op.Txn().ID, id)
			require.Len(t, rows, 1)
			var err error
			granted, err = ls.Lock(ctx, table, rows, id, opts)
			return granted, err
		}}
		ts, err := LockRowsForAdmissionWithContext(proc.Ctx, nil, proc, 41, bat, 0, *bat.Vecs[0].GetType(), lock.LockMode_Shared, 7)
		require.NoError(t, err)
		require.Equal(t, granted.Timestamp, ts)
		require.True(t, snapshot.Less(ts), "cold table timestamp must exercise the skipped wait")
		require.True(t, op.HasLockTable(41))
		require.Empty(t, op.GetOverview().WaitLocks)
		require.Equal(t, snapshot, op.Txn().SnapshotTS)
		require.Equal(t, 1, waiter.calls, "only transaction creation may call the timestamp waiter")
		require.Equal(t, "SNAPSHOT", bat.Vecs[0].GetStringAt(0), "caller still owns the keys")

		// The ordinary API still performs its existing cold timestamp wait.
		proc.Base.LockService = ls
		err = LockTableForSnapshotRefreshWithContext(proc.Ctx, nil, proc, 42, *bat.Vecs[0].GetType(), lock.LockMode_Shared, false)
		require.ErrorIs(t, err, assert.AnError)
	}, client.WithTimestampWaiter(waiter))
}

func TestLockRowsAdmissionTransportAndBinding(t *testing.T) {
	oldWait := defaultWaitTimeOnRetryLock
	defaultWaitTimeOnRetryLock = 0
	t.Cleanup(func() { defaultWaitTimeOnRetryLock = oldWait })
	forceLockRetryMemoryPressure(t, lockRetryMemoryPressureNormal)
	runLockOpTest(t, func(proc *process.Process) {
		bat := admissionKeys(t, proc)
		defer bat.Clean(proc.Mp())
		op := proc.GetTxnOperator()
		defer func() { require.NoError(t, op.Rollback(proc.Ctx)) }()
		ctx, cancel := context.WithTimeout(proc.Ctx, 5*time.Second)
		defer cancel()
		deadline, _ := ctx.Deadline()
		ls := proc.GetLockService()
		attempts := 0
		proc.Base.LockService = admissionLockService{LockService: ls, lockFn: func(ctx context.Context, table uint64, rows [][]byte, id []byte, opts lock.LockOptions) (lock.Result, error) {
			attempts++
			require.Equal(t, deadline.UnixNano(), opts.LockWaitDeadline)
			require.Equal(t, lock.Granularity_Row, opts.Granularity)
			if attempts == 1 {
				return lock.Result{}, moerr.NewLockTableBindChangedNoCtx()
			}
			if attempts == 4 {
				return lock.Result{}, nil // Forwarding without an owner bind is not admission.
			}
			res, err := ls.Lock(ctx, table, rows, id, opts)
			if attempts == 3 {
				res.LockedOn.Version++ // Existing owner must reject a changed binding.
			}
			return res, err
		}}
		admit := func() (timestamp.Timestamp, error) {
			return LockRowsForAdmissionWithContext(ctx, nil, proc, 43, bat, 0, *bat.Vecs[0].GetType(), lock.LockMode_Exclusive, 0)
		}
		_, err := admit()
		require.NoError(t, err)
		require.Equal(t, 2, attempts)
		require.True(t, op.HasLockTable(43))
		ts, err := admit()
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged), "%v", err)
		require.True(t, ts.IsEmpty(), "failed binding must not publish an admission timestamp")
		require.Equal(t, 3, attempts)
		ts, err = admit()
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged), "%v", err)
		require.True(t, ts.IsEmpty())
		require.Equal(t, 4, attempts)
		require.False(t, op.HasLockTable(0))
		require.Empty(t, op.GetOverview().WaitLocks)
	}, client.WithTimestampWaiter(immediateLockTimestampWaiter{}))
}

func TestLockRowsAdmissionRefusesWideningAndInvalidOwners(t *testing.T) {
	runLockOpTest(t, func(proc *process.Process) {
		bat := admissionKeys(t, proc)
		defer bat.Clean(proc.Mp())
		// Exceed the fetcher's configured batch threshold with two distinct keys.
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("BRANCH"), false, proc.Mp()))
		ls := proc.GetLockService()
		cfg := ls.GetConfig()
		cfg.MaxLockRowCount = 1
		calls := 0
		proc.Base.LockService = admissionLockService{LockService: lockServiceConfigOverride{LockService: ls, cfg: cfg}, lockFn: func(_ context.Context, _ uint64, rows [][]byte, _ []byte, opts lock.LockOptions) (lock.Result, error) {
			calls++
			require.Equal(t, lock.Granularity_Row, opts.Granularity)
			require.Len(t, rows, 2)
			return lock.Result{}, moerr.NewLockNeedUpgradeNoCtx()
		}}
		admit := func() error {
			_, err := LockRowsForAdmissionWithContext(proc.Ctx, nil, proc, 44, bat, 0, *bat.Vecs[0].GetType(), lock.LockMode_Exclusive, 0)
			return err
		}
		require.True(t, moerr.IsMoErrCode(admit(), moerr.ErrLockNeedUpgrade))
		require.Equal(t, 1, calls, "no fallback range request")
		require.False(t, proc.GetTxnOperator().HasLockTable(44))
		proc.GetTxnOperator().TxnRef().Isolation = txnpb.TxnIsolation_SI
		require.Error(t, admit())
		proc.GetTxnOperator().TxnRef().Isolation = txnpb.TxnIsolation_RC
		proc.GetTxnOperator().TxnRef().Mode = txnpb.TxnMode_Optimistic
		require.Error(t, admit())
		proc.GetTxnOperator().TxnRef().Mode = txnpb.TxnMode_Pessimistic
		bat.Vecs[0].GetNulls().Add(0)
		require.Error(t, admit())
		bat.Vecs[0].GetNulls().Reset()
		nullKeys := batch.NewWithSize(1)
		nullKeys.Vecs[0] = vector.NewConstNull(types.T_varchar.ToType(), 1, proc.Mp())
		defer nullKeys.Clean(proc.Mp())
		_, err := LockRowsForAdmissionWithContext(proc.Ctx, nil, proc, 44, nullKeys, 0, types.T_varchar.ToType(), lock.LockMode_Shared, 0)
		require.Error(t, err, "constant null must not succeed without locking")
		bat.Vecs[0].SetLength(0)
		require.Error(t, admit())
		_, err = LockRowsForAdmissionWithContext(proc.Ctx, nil, proc, 44, nil, 0, types.T_varchar.ToType(), lock.LockMode_Shared, 0)
		require.Error(t, err)
		require.Equal(t, 1, calls)
		require.NoError(t, proc.GetTxnOperator().Rollback(proc.Ctx))
	}, client.WithTimestampWaiter(immediateLockTimestampWaiter{}))
}

func TestLockRowsAdmissionCancellation(t *testing.T) {
	runLockOpTest(t, func(proc *process.Process) {
		bat := admissionKeys(t, proc)
		defer bat.Clean(proc.Mp())
		op := proc.GetTxnOperator()
		defer func() { require.NoError(t, op.Rollback(proc.Ctx)) }()
		ls := proc.GetLockService()
		ctx, cancel := context.WithCancel(proc.Ctx)
		defer cancel()
		calls := 0
		proc.Base.LockService = admissionLockService{LockService: ls, lockFn: func(ctx context.Context, _ uint64, _ [][]byte, _ []byte, _ lock.LockOptions) (lock.Result, error) {
			calls++
			cancel()
			return lock.Result{}, ctx.Err()
		}}
		admit := func(ctx context.Context) error {
			_, err := LockRowsForAdmissionWithContext(ctx, nil, proc, 45, bat, 0, *bat.Vecs[0].GetType(), lock.LockMode_Shared, 0)
			return err
		}
		require.ErrorIs(t, admit(ctx), context.Canceled)
		require.False(t, op.HasLockTable(45))
		require.Empty(t, op.GetOverview().WaitLocks)
		require.ErrorIs(t, admit(ctx), context.Canceled)
		expired, done := context.WithDeadline(proc.Ctx, time.Now().Add(-time.Second))
		defer done()
		require.ErrorIs(t, admit(expired), context.DeadlineExceeded)
		require.Equal(t, 1, calls, "canceled/expired admission must not send another request")
		proc.Base.LockService = admissionLockService{LockService: ls, lockFn: func(context.Context, uint64, [][]byte, []byte, lock.LockOptions) (lock.Result, error) {
			panic("transport panic")
		}}
		require.PanicsWithValue(t, "transport panic", func() { _ = admit(proc.Ctx) })
		require.Empty(t, op.GetOverview().WaitLocks)
		require.Equal(t, "SNAPSHOT", bat.Vecs[0].GetStringAt(0))

		// Cancellation after a successful grant must not lose its binding.
		grantedCtx, cancelGrant := context.WithCancel(proc.Ctx)
		defer cancelGrant()
		proc.Base.LockService = admissionLockService{LockService: ls, lockFn: func(ctx context.Context, table uint64, rows [][]byte, id []byte, opts lock.LockOptions) (lock.Result, error) {
			res, err := ls.Lock(ctx, table, rows, id, opts)
			cancelGrant()
			return res, err
		}}
		require.NoError(t, admit(grantedCtx))
		require.ErrorIs(t, grantedCtx.Err(), context.Canceled)
		require.True(t, op.HasLockTable(45))
		require.Empty(t, op.GetOverview().WaitLocks)
	}, client.WithTimestampWaiter(immediateLockTimestampWaiter{}))
}
