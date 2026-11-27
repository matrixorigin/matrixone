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

package iscp

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type repairTestWorkspace struct{ client.Workspace }

func (repairTestWorkspace) AdvanceSnapshot(context.Context, timestamp.Timestamp) error { return nil }

type repairTestTxn struct{ *statusTestTxn }

func (*repairTestTxn) GetWorkspace() client.Workspace { return repairTestWorkspace{} }

type repairTestClient struct {
	client.TxnClient
	txn client.TxnOperator
	err error
}

type handoffTestClient struct {
	client.TxnClient
	calls      atomic.Int32
	repaired   atomic.Bool
	rolledBack chan struct{}
}

func (c *handoffTestClient) New(context.Context, timestamp.Timestamp, ...client.TxnOption) (client.TxnOperator, error) {
	if c.calls.Add(1) == 1 {
		return &statusTestTxn{}, nil
	}
	return &handoffTestTxn{client: c}, nil
}

type handoffTestTxn struct {
	client.TxnOperator
	client *handoffTestClient
}

func (*handoffTestTxn) GetWorkspace() client.Workspace { return repairTestWorkspace{} }
func (*handoffTestTxn) Commit(context.Context) error   { panic("stale recovery committed") }
func (t *handoffTestTxn) Rollback(context.Context) error {
	close(t.client.rolledBack)
	return nil
}

func (c *repairTestClient) New(context.Context, timestamp.Timestamp, ...client.TxnOption) (client.TxnOperator, error) {
	return c.txn, c.err
}

func TestRepairAbandonedJobsTransactionCreationFailure(t *testing.T) {
	createErr := errors.New("transaction creation failed")
	for _, tc := range []struct {
		name      string
		withTxn   bool
		err       error
		rollbacks int
	}{
		{name: "error without transaction", err: createErr},
		{name: "error with transaction", withTxn: true, err: createErr, rollbacks: 1},
		{name: "missing transaction"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			txn := &statusTestTxn{}
			client := &repairTestClient{err: tc.err}
			if tc.withTxn {
				client.txn = &repairTestTxn{txn}
			}
			exec := &ISCPTaskExecutor{txnEngine: statusTestEngine{}, cnTxnClient: client}
			err := exec.repairAbandonedJobs(t.Context())
			require.Error(t, err)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			}
			require.Equal(t, tc.rollbacks, txn.rollbacks)
			require.Zero(t, txn.commits)
		})
	}
}

func TestRepairAbandonedJobsRequiresCurrentOwner(t *testing.T) {
	sqlErr := errors.New("storage unavailable")
	commitErr := errors.New("commit acknowledgement lost")
	rollbackErr := errors.New("rollback failed")
	for _, tc := range []struct {
		name        string
		ownerCount  int64
		guardRows   uint64
		guardErr    error
		commitErr   error
		rollbackErr error
		panicGuard  bool
		wantInvalid bool
		commits     int
		rollbacks   int
	}{
		{name: "current owner", ownerCount: 1, guardRows: 1, commits: 1},
		{name: "superseded owner", ownerCount: 1, wantInvalid: true, rollbacks: 1},
		{name: "missing daemon", wantInvalid: true, rollbacks: 1},
		{name: "duplicate daemons", ownerCount: 2, wantInvalid: true, rollbacks: 1},
		{name: "guard failed", ownerCount: 1, guardErr: sqlErr, rollbacks: 1},
		{name: "guard and rollback failed", ownerCount: 1, guardErr: sqlErr, rollbackErr: rollbackErr, rollbacks: 1},
		{name: "commit acknowledgement lost", ownerCount: 1, guardRows: 1, commitErr: commitErr, commits: 1},
		{name: "guard panics", ownerCount: 1, panicGuard: true, rollbacks: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			txn := &statusTestTxn{commitErr: tc.commitErr, rollbackErr: tc.rollbackErr}
			exec := &ISCPTaskExecutor{cnUUID: "old-cn", taskID: 7, txnEngine: statusTestEngine{}, cnTxnClient: &repairTestClient{txn: &repairTestTxn{txn}}}
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
			old := ExecWithResult
			t.Cleanup(func() { ExecWithResult = old })
			calls := 0
			ExecWithResult = func(_ context.Context, sql, _ string, _ client.TxnOperator) (executor.Result, error) {
				calls++
				if strings.Contains(sql, "mo_catalog.mo_iscp_log") {
					return executor.Result{AffectedRows: 1}, nil
				}
				if strings.HasPrefix(sql, "SELECT count(*)") {
					result := executor.NewMemResult([]types.Type{types.T_int64.ToType()}, mp)
					result.NewBatchWithRowCount(1)
					require.NoError(t, executor.AppendFixedRows(result, 0, []int64{tc.ownerCount}))
					return result.GetResult(), nil
				}
				require.Contains(t, sql, "mo_task.sys_daemon_task")
				if tc.panicGuard {
					panic("guard panic")
				}
				return executor.Result{AffectedRows: tc.guardRows}, tc.guardErr
			}
			var err error
			if tc.panicGuard {
				require.PanicsWithValue(t, "guard panic", func() { _ = exec.repairAbandonedJobs(t.Context()) })
			} else {
				err = exec.repairAbandonedJobs(t.Context())
			}
			if tc.wantInvalid {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidTask), "%v", err)
			}
			if tc.guardErr != nil {
				require.ErrorIs(t, err, tc.guardErr)
			}
			if tc.commitErr != nil {
				require.ErrorIs(t, err, tc.commitErr)
			}
			if tc.rollbackErr != nil {
				require.ErrorIs(t, err, tc.rollbackErr)
			}
			if !tc.wantInvalid && tc.guardErr == nil && tc.commitErr == nil && !tc.panicGuard {
				require.NoError(t, err)
			}
			if tc.ownerCount == 1 {
				require.Equal(t, 3, calls)
			} else {
				require.Equal(t, 2, calls)
			}
			require.Equal(t, tc.commits, txn.commits)
			require.Equal(t, tc.rollbacks, txn.rollbacks)
		})
	}
}

func TestRetryStopsWhenLeaseIsLost(t *testing.T) {
	invalid := moerr.NewInvalidTask(context.Background(), "old-cn", 7)
	for _, returned := range []error{invalid, errors.Join(invalid, errors.New("rollback failed"))} {
		calls := 0
		err := retry(t.Context(), func() error {
			calls++
			return returned
		}, 5, time.Hour, time.Hour)
		require.ErrorIs(t, err, invalid)
		require.Equal(t, 1, calls)
	}
}

func TestWorkerRecoveryRollsBackAfterLeaseHandoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	owner, terminate := context.WithCancel(ctx)
	defer terminate()
	data, mp := newISCPLogResult(t, []iscpLogBatch{{
		jobNames: []string{"index_idx"}, jobIDs: []uint64{3},
		jobSpecs: []string{mustMarshalJobSpec(t, "idx")}, jobStatuss: []string{mustMarshalJobStatus(t, 7, JobStage_Init)},
	}})
	vector.MustFixedColWithTypeCheck[int8](data.Batches[0].Vecs[4])[0] = ISCPJobState_Running
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	storage := &handoffTestClient{rolledBack: make(chan struct{})}
	old := ExecWithResult
	t.Cleanup(func() { ExecWithResult = old })
	ExecWithResult = func(_ context.Context, sql, _ string, _ client.TxnOperator) (executor.Result, error) {
		switch {
		case strings.Contains(sql, "SELECT job_name, job_id, job_spec"):
			return data, nil
		case strings.Contains(sql, "UPDATE mo_catalog.mo_iscp_log"):
			storage.repaired.Store(true)
			return executor.Result{AffectedRows: 1}, nil
		case strings.HasPrefix(sql, "SELECT count(*)"):
			result := executor.NewMemResult([]types.Type{types.T_int64.ToType()}, mp)
			result.NewBatchWithRowCount(1)
			if err := executor.AppendFixedRows(result, 0, []int64{1}); err != nil {
				return executor.Result{}, err
			}
			return result.GetResult(), nil
		case strings.Contains(sql, "UPDATE mo_task.sys_daemon_task"):
			// The successor owns the task row, so the old owner's CAS matches no row.
			return executor.Result{}, nil
		default:
			return executor.Result{}, moerr.NewInternalErrorNoCtx("unexpected SQL in handoff test")
		}
	}
	workerCtx, workerCancel := context.WithCancel(owner)
	w := &worker{cnUUID: "old-cn", cnEngine: statusTestEngine{}, cnTxnClient: storage, ctx: workerCtx, cancel: workerCancel, taskChan: make(chan *IterationContext, 1), mp: mp}
	exec := &ISCPTaskExecutor{cnUUID: "old-cn", taskID: 7, ownerCtx: owner, terminate: terminate, running: true, worker: w, cancel: func() {}, txnEngine: statusTestEngine{}, cnTxnClient: storage, option: &ISCPExecutorOption{RetryTimes: 1}}
	w.exec = exec
	w.onItem(NewIterationContext(0, 10, []string{"index_idx"}, []uint64{3}, []uint64{7}, types.TS{}, types.TS{}))
	select {
	case <-storage.rolledBack:
	case <-ctx.Done():
		t.Fatal("stale recovery did not roll back")
	}
	select {
	case <-owner.Done():
	case <-ctx.Done():
		t.Fatal("stale executor did not terminate")
	}
	exec.runningMu.Lock()
	exec.runningMu.Unlock()
	require.True(t, storage.repaired.Load(), "test must reach the tentative repair")
}

func TestExecutorAdmissionLastsUntilCancel(t *testing.T) {
	ctx := t.Context()
	_, err := NewISCPTaskExecutor(ctx, nil, nil, "admission-cn", 0, nil, nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidTask), "%v", err)
	first, err := NewISCPTaskExecutor(ctx, nil, nil, "admission-cn", 1, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Cancel() })
	_, err = NewISCPTaskExecutor(ctx, nil, nil, "admission-cn", 1, nil, nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrExecutorRunning), "%v", err)
	require.NoError(t, first.Pause())
	_, err = NewISCPTaskExecutor(ctx, nil, nil, "admission-cn", 1, nil, nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrExecutorRunning), "%v", err)

	other, err := NewISCPTaskExecutor(ctx, nil, nil, "other-cn", 2, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = other.Cancel() })
	require.NoError(t, first.Cancel())
	replacement, err := NewISCPTaskExecutor(ctx, nil, nil, "admission-cn", 1, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = replacement.Cancel() })
	// An older executor's repeated cleanup must not release its successor.
	require.NoError(t, first.Cancel())
	_, err = NewISCPTaskExecutor(ctx, nil, nil, "admission-cn", 1, nil, nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrExecutorRunning), "%v", err)
}
