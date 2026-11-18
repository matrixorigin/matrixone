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

type repairTestWorkspace struct {
	client.Workspace
	events *[]string
	err    error
}

func (w repairTestWorkspace) AdvanceSnapshot(context.Context, timestamp.Timestamp) error {
	if w.events != nil {
		*w.events = append(*w.events, "advance_snapshot")
	}
	return w.err
}

type repairTestTxn struct {
	*statusTestTxn
	events     *[]string
	advanceErr error
}

func (t *repairTestTxn) GetWorkspace() client.Workspace {
	return repairTestWorkspace{events: t.events, err: t.advanceErr}
}

type repairTestClient struct {
	client.TxnClient
	txn client.TxnOperator
	err error
}

func (*repairTestClient) GetLatestCommitTS() timestamp.Timestamp { return timestamp.Timestamp{} }

type handoffTestClient struct {
	client.TxnClient
	calls      atomic.Int32
	repaired   atomic.Bool
	rolledBack chan struct{}
}

func (*handoffTestClient) GetLatestCommitTS() timestamp.Timestamp { return timestamp.Timestamp{} }

func (c *handoffTestClient) New(context.Context, timestamp.Timestamp, ...client.TxnOption) (client.TxnOperator, error) {
	switch c.calls.Add(1) {
	case 1:
		return &statusTestTxn{}, nil
	case 2:
		return &repairTestTxn{statusTestTxn: &statusTestTxn{}}, nil
	default:
		return &handoffTestTxn{client: c}, nil
	}
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
				client.txn = &repairTestTxn{statusTestTxn: txn}
			}
			exec := &ISCPTaskExecutor{txnEngine: statusTestEngine{}, cnTxnClient: client}
			err := exec.repairAbandonedJobBatch(t.Context(), []JobRuntimeKey{{AccountID: 0, TableID: 10, JobName: "index_idx", JobID: 3}})
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
			events := []string{}
			exec := &ISCPTaskExecutor{cnUUID: "old-cn", taskID: 7, txnEngine: statusTestEngine{}, cnTxnClient: &repairTestClient{txn: &repairTestTxn{statusTestTxn: txn, events: &events}}}
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
			old := ExecWithResult
			t.Cleanup(func() { ExecWithResult = old })
			calls := 0
			ExecWithResult = func(_ context.Context, sql, _ string, _ client.TxnOperator) (executor.Result, error) {
				calls++
				if strings.Contains(sql, "mo_catalog.mo_iscp_log") {
					require.Contains(t, sql, "drop_at IS NULL", "repair should leave retired jobs untouched")
					events = append(events, "repair_jobs")
					return executor.Result{AffectedRows: 1}, nil
				}
				if strings.HasPrefix(sql, "SELECT count(*)") {
					events = append(events, "count_owners")
					result := executor.NewMemResult([]types.Type{types.T_int64.ToType()}, mp)
					result.NewBatchWithRowCount(1)
					require.NoError(t, executor.AppendFixedRows(result, 0, []int64{tc.ownerCount}))
					return result.GetResult(), nil
				}
				require.Contains(t, sql, "mo_task.sys_daemon_task")
				events = append(events, "guard_lease")
				if tc.panicGuard {
					panic("guard panic")
				}
				return executor.Result{AffectedRows: tc.guardRows}, tc.guardErr
			}
			var err error
			if tc.panicGuard {
				require.PanicsWithValue(t, "guard panic", func() {
					_ = exec.repairAbandonedJobBatch(t.Context(), []JobRuntimeKey{{AccountID: 0, TableID: 10, JobName: "index_idx", JobID: 3}})
				})
			} else {
				err = exec.repairAbandonedJobBatch(t.Context(), []JobRuntimeKey{{AccountID: 0, TableID: 10, JobName: "index_idx", JobID: 3}})
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
				require.Equal(t, []string{"count_owners", "advance_snapshot", "repair_jobs", "guard_lease"}, events)
			} else {
				require.Equal(t, 1, calls)
				require.Equal(t, []string{"count_owners"}, events)
			}
			require.Equal(t, tc.commits, txn.commits)
			require.Equal(t, tc.rollbacks, txn.rollbacks)
		})
	}
}

// Each committed batch owns only its selected jobs. If ownership is lost,
// earlier commits remain valid and the next batch must not commit.
func TestRepairAbandonedJobsBatchBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name           string
		rows           int
		selectionError bool
		snapshotError  bool
		loseOwnerOn    int
		wantCommits    int
		wantRollbacks  int
		wantUpdates    int
	}{
		{name: "empty still checks ownership", rows: 0, wantCommits: 2},
		{name: "empty rejects lost owner", rows: 0, loseOwnerOn: 1, wantCommits: 1, wantRollbacks: 1},
		{name: "selection error releases results", rows: 1, selectionError: true, wantRollbacks: 1},
		{name: "snapshot error rolls back", snapshotError: true, wantRollbacks: 1},
		{name: "ownership lost in second batch", rows: 129, loseOwnerOn: 2, wantCommits: 2, wantRollbacks: 1, wantUpdates: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
			txn := &statusTestTxn{}
			snapshotErr := errors.New("snapshot unavailable")
			scanTxn := &repairTestTxn{statusTestTxn: txn}
			if tc.snapshotError {
				scanTxn.advanceErr = snapshotErr
			}
			testClient := &repairTestClient{txn: scanTxn}
			exec := &ISCPTaskExecutor{cnUUID: "current-cn", taskID: 7, txnEngine: statusTestEngine{}, cnTxnClient: testClient}
			old := ExecWithResult
			t.Cleanup(func() { ExecWithResult = old })
			updates, guards := 0, 0
			selectionErr := errors.New("candidate scan failed")
			ExecWithResult = func(_ context.Context, sql, _ string, _ client.TxnOperator) (executor.Result, error) {
				switch {
				case strings.HasPrefix(sql, "SELECT account_id, table_id, job_name, job_id FROM mo_catalog.mo_iscp_log"):
					if tc.rows == 0 {
						return executor.Result{}, nil
					}
					result := executor.NewMemResult([]types.Type{types.T_uint32.ToType(), types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType()}, mp)
					result.NewBatchWithRowCount(tc.rows)
					accounts, tables, ids := make([]uint32, tc.rows), make([]uint64, tc.rows), make([]uint64, tc.rows)
					names := make([]string, tc.rows)
					for i := range names {
						tables[i], ids[i], names[i] = 10, uint64(i+1), "index_idx"
					}
					require.NoError(t, executor.AppendFixedRows(result, 0, accounts))
					require.NoError(t, executor.AppendFixedRows(result, 1, tables))
					require.NoError(t, executor.AppendStringRows(result, 2, names))
					require.NoError(t, executor.AppendFixedRows(result, 3, ids))
					if tc.selectionError {
						return result.GetResult(), selectionErr
					}
					return result.GetResult(), nil
				case strings.HasPrefix(sql, "SELECT count(*)"):
					result := executor.NewMemResult([]types.Type{types.T_int64.ToType()}, mp)
					result.NewBatchWithRowCount(1)
					require.NoError(t, executor.AppendFixedRows(result, 0, []int64{1}))
					return result.GetResult(), nil
				case strings.HasPrefix(sql, "UPDATE mo_catalog.mo_iscp_log"):
					updates++
					require.Contains(t, sql, "drop_at IS NULL")
					return executor.Result{AffectedRows: 1}, nil
				case strings.HasPrefix(sql, "UPDATE mo_task.sys_daemon_task"):
					guards++
					if guards == tc.loseOwnerOn {
						return executor.Result{}, nil
					}
					return executor.Result{AffectedRows: 1}, nil
				default:
					return executor.Result{}, moerr.NewInternalErrorNoCtx("unexpected SQL in repair test")
				}
			}
			err := exec.repairAbandonedJobs(t.Context())
			if tc.snapshotError {
				require.ErrorIs(t, err, snapshotErr)
			} else if tc.selectionError {
				require.ErrorIs(t, err, selectionErr)
			} else if tc.loseOwnerOn != 0 {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidTask), "%v", err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantCommits, txn.commits)
			require.Equal(t, tc.wantRollbacks, txn.rollbacks)
			require.Equal(t, tc.wantUpdates, updates)
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
		case strings.HasPrefix(sql, "SELECT account_id, table_id, job_name, job_id FROM mo_catalog.mo_iscp_log"):
			result := executor.NewMemResult([]types.Type{types.T_uint32.ToType(), types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType()}, mp)
			result.NewBatchWithRowCount(1)
			if err := executor.AppendFixedRows(result, 0, []uint32{0}); err != nil {
				return executor.Result{}, err
			}
			if err := executor.AppendFixedRows(result, 1, []uint64{10}); err != nil {
				return executor.Result{}, err
			}
			if err := executor.AppendStringRows(result, 2, []string{"index_idx"}); err != nil {
				return executor.Result{}, err
			}
			if err := executor.AppendFixedRows(result, 3, []uint64{3}); err != nil {
				return executor.Result{}, err
			}
			return result.GetResult(), nil
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
