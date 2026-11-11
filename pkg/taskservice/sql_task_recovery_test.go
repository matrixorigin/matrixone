// Copyright 2026 Matrix Origin
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

package taskservice

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestSQLTaskRecoveryCreditsAndGuards(t *testing.T) {
	r := newSQLTaskRecovery(16, 9, nil)
	t.Cleanup(func() { r.close() })

	var credits []*sqlTaskCredit
	for i := 0; i < sqlTaskNameCapacity; i++ {
		credit := r.reserve(sqlTaskManualReceipt, 1, "task")
		require.NotNil(t, credit)
		credits = append(credits, credit)
	}
	require.Nil(t, r.reserve(sqlTaskManualReceipt, 1, "task"))
	require.NotNil(t, r.reserve(sqlTaskScheduledReceipt, 1, "task"), "the scheduled share is independent")
	require.NotNil(t, r.reserve(sqlTaskManualReceipt, 2, "task"), "other tenants retain capacity")
	for _, credit := range credits {
		r.release(credit)
		r.release(credit)
	}
	require.NotNil(t, r.reserve(sqlTaskManualReceipt, 1, "task"))
	require.Nil(t, r.reserve(sqlTaskManualReceipt, 1, strings.Repeat("x", 513)))

	key := sqlTaskGuardKey{account: 1, value: "task"}
	first := r.tryGuard(key, 512)
	require.NotNil(t, first)
	require.Nil(t, r.tryGuard(key, 512))
	r.releaseGuard(first)
	second := r.tryGuard(key, 512)
	require.NotNil(t, second)
	r.releaseGuard(first) // an old owner's duplicate cleanup cannot free a successor
	require.Nil(t, r.tryGuard(key, 512))
	r.releaseGuard(second)
	require.NotNil(t, r.tryGuard(key, 512))
	require.Nil(t, r.tryGuard(sqlTaskGuardKey{value: strings.Repeat("x", 51)}, 50))

	require.NotNil(t, r.reserve(sqlTaskManualReceipt, 3, "unrelated"))
}

func TestSQLTaskRecoveryDoesNotLetOneBadReceiptBlockOthers(t *testing.T) {
	attemptedBad, completed := make(chan struct{}, 1), make(chan uint64, 2)
	r := newSQLTaskRecovery(16, 9, func(_ context.Context, receipt *sqlTaskReceipt) bool {
		if receipt.Run.RunID == 1 {
			select {
			case attemptedBad <- struct{}{}:
			default:
			}
			return false
		}
		completed <- receipt.Run.RunID
		return true
	})
	t.Cleanup(func() { r.close() })
	for _, item := range []struct {
		class sqlTaskReceiptClass
		id    uint64
	}{
		{sqlTaskManualReceipt, 1}, {sqlTaskManualReceipt, 2}, {sqlTaskScheduledReceipt, 3},
	} {
		credit := r.reserve(item.class, 1, fmt.Sprintf("task-%d", item.id))
		require.NotNil(t, credit)
		require.True(t, r.enqueue(&sqlTaskReceipt{SQLTaskReceiptRequest: SQLTaskReceiptRequest{Run: SQLTaskRun{RunID: item.id}}, credit: credit}))
		if item.id == 1 {
			<-attemptedBad
		}
	}
	got := []uint64{}
	for len(got) < 2 {
		select {
		case id := <-completed:
			got = append(got, id)
		case <-time.After(2 * time.Second):
			t.Fatal("a failing receipt blocked unrelated work")
		}
	}
	require.ElementsMatch(t, []uint64{2, 3}, got)
}

func TestSQLTaskRecoveryKeepsDifferentRunsOfSameFiring(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	completed := make(chan uint64, 1)
	r := newSQLTaskRecovery(16, 9, func(ctx context.Context, receipt *sqlTaskReceipt) bool {
		if receipt.Run.RunID == 1 {
			select {
			case <-entered:
			default:
				close(entered)
			}
			select {
			case <-ctx.Done():
				return false
			case <-release:
				return true
			}
		}
		completed <- receipt.Run.RunID
		return true
	})
	t.Cleanup(func() { r.close() })
	claim := SQLTaskClaim{ID: 9, Runner: "cn1", Epoch: 1}
	first := r.reserve(sqlTaskScheduledReceipt, 1, "task")
	second := r.reserve(sqlTaskScheduledReceipt, 1, "task")
	require.NotNil(t, first)
	require.NotNil(t, second)
	// A's admission may have rolled back despite a lost COMMIT reply. The
	// same firing can then acquire B and finish before A reconciles.
	require.True(t, r.enqueue(&sqlTaskReceipt{SQLTaskReceiptRequest: SQLTaskReceiptRequest{Kind: SQLTaskReceiptAdmission, Claim: claim, Run: SQLTaskRun{RunID: 1}}, credit: first}))
	<-entered
	require.True(t, r.enqueue(&sqlTaskReceipt{SQLTaskReceiptRequest: SQLTaskReceiptRequest{Kind: SQLTaskReceiptTerminal, Claim: claim, Run: SQLTaskRun{RunID: 2}}, credit: second}))
	close(release)
	select {
	case id := <-completed:
		require.Equal(t, uint64(2), id)
	case <-time.After(2 * time.Second):
		t.Fatal("the successor result was lost")
	}
}

func TestSQLTaskRecoveryShutdownReleasesPendingGauge(t *testing.T) {
	entered := make(chan struct{})
	r := newSQLTaskRecovery(16, 9, func(ctx context.Context, _ *sqlTaskReceipt) bool {
		close(entered)
		<-ctx.Done()
		return false
	})
	t.Cleanup(func() { r.close() })
	before := testutil.ToFloat64(v2.SQLTaskManualPendingGauge)
	credit := r.reserve(sqlTaskManualReceipt, 1, "task")
	require.NotNil(t, credit)
	require.True(t, r.enqueue(&sqlTaskReceipt{SQLTaskReceiptRequest: SQLTaskReceiptRequest{Run: SQLTaskRun{RunID: 9}}, credit: credit}))
	<-entered
	require.Equal(t, before+1, testutil.ToFloat64(v2.SQLTaskManualPendingGauge))
	count, sample := r.close()
	require.Equal(t, 1, count)
	require.Equal(t, []uint64{9}, sample, "the canceled in-flight receipt must be reported")
	require.Equal(t, before, testutil.ToFloat64(v2.SQLTaskManualPendingGauge))
}

type unavailableSQLTaskReceiptStore struct {
	TaskStorage
	base      *memTaskStorage
	available atomic.Bool
}

type rolledBackAdmissionStore struct {
	*unavailableSQLTaskReceiptStore
	first atomic.Bool
}

func (s *rolledBackAdmissionStore) AcquireSQLTaskRun(ctx context.Context, definition SQLTask, run SQLTaskRun, admission *SQLTaskRunAdmission) (uint64, error) {
	if s.first.CompareAndSwap(false, true) {
		if !admission.Reserve(definition.AccountID, definition.TaskName) {
			return 0, ErrSQLTaskRecoveryCapacity
		}
		// Simulate a rollback whose COMMIT acknowledgement was lost, including
		// the reserved auto-increment ID and recovery credit.
		s.base.Lock()
		id := s.base.nextIDLocked()
		s.base.Unlock()
		return id, errors.New("injected uncertain admission COMMIT")
	}
	return s.base.AcquireSQLTaskRun(ctx, definition, run, admission)
}

func TestSQLTaskReacquiredFiringKeepsBothMetadataReceipts(t *testing.T) {
	ctx := context.Background()
	base := NewMemTaskStorage().(*memTaskStorage)
	outage := &unavailableSQLTaskReceiptStore{TaskStorage: base, base: base}
	store := &rolledBackAdmissionStore{unavailableSQLTaskReceiptStore: outage}
	service := NewTaskService(runtime.DefaultRuntime(), store).(*taskService)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	definition := mustAddSQLTaskForExecutorTest(t, base, func(definition *SQLTask) {
		definition.TriggerCount, definition.RetryLimit = 1, 0
		definition.GateCondition, definition.SQLBody = "", "insert into target values (1)"
	})
	spec := newSQLTaskContextForTest(definition, SQLTaskTriggerScheduled)
	spec.TriggerCount = 1
	firing := newTaskFromMetadata(BuildSQLTaskMetadata(spec))
	firing.Status, firing.TaskRunner, firing.Epoch = task.TaskStatus_Running, "cn1", 1
	firing.ParentTaskID = fmt.Sprintf("sql-task:%d", definition.TaskID)
	mustAddTestAsyncTask(t, base, 1, firing)
	firing = mustGetTestAsyncTask(t, base, 1)[0]
	var calls atomic.Int32
	fake := &fakeInternalExecutor{execHook: func(context.Context, string, ie.SessionOverrideOptions) error {
		calls.Add(1)
		return nil
	}}
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fake }, service, "cn1").TaskExecutor()
	var pending *SQLTaskMetadataPendingError
	err := executor(ctx, &firing)
	require.ErrorAs(t, err, &pending)
	require.Zero(t, calls.Load(), "the tentative admission did not start SQL")
	pending = nil
	err = executor(ctx, &firing)
	require.ErrorAs(t, err, &pending)
	require.EqualValues(t, 1, calls.Load())
	baseRuns := mustGetTestSQLTaskRun(t, base, 1)
	require.Equal(t, SQLTaskStatusRunning, baseRuns[0].Status)

	outage.available.Store(true)
	require.Eventually(t, func() bool {
		runs, err := base.QuerySQLTaskRun(ctx)
		if err != nil || len(runs) != 1 || runs[0].Status != SQLTaskStatusSuccess {
			return false
		}
		tasks, err := base.QueryAsyncTask(ctx)
		return err == nil && len(tasks) == 1 && tasks[0].Status == task.TaskStatus_Completed
	}, 5*time.Second, 20*time.Millisecond)
	require.EqualValues(t, 1, calls.Load(), "metadata recovery must not rerun the body")
}

func (s *unavailableSQLTaskReceiptStore) PublishSQLTaskReceipt(ctx context.Context, receipt SQLTaskReceiptRequest) (bool, error) {
	if !s.available.Load() {
		return false, errors.New("injected metadata outage")
	}
	return s.base.PublishSQLTaskReceipt(ctx, receipt)
}

func TestSQLTaskExecutionRecoversMetadataWithoutReplayingBody(t *testing.T) {
	for _, tc := range []struct {
		name    string
		trigger string
		class   sqlTaskReceiptClass
		unknown bool
	}{
		{"manual known result", SQLTaskTriggerManual, sqlTaskManualReceipt, false},
		{"scheduled known result", SQLTaskTriggerScheduled, sqlTaskScheduledReceipt, false},
		{"unknown business outcome", SQLTaskTriggerManual, sqlTaskManualReceipt, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			base := NewMemTaskStorage().(*memTaskStorage)
			store := &unavailableSQLTaskReceiptStore{TaskStorage: base, base: base}
			store.available.Store(true)
			service := NewTaskService(runtime.DefaultRuntime(), store).(*taskService)
			t.Cleanup(func() { require.NoError(t, service.Close()) })
			definition := mustAddSQLTaskForExecutorTest(t, base, func(definition *SQLTask) {
				definition.TriggerCount, definition.RetryLimit = 1, 0
				definition.GateCondition, definition.SQLBody = "", "insert into target values (1)"
			})
			unknown := moerr.NewTxnUnknown(ctx, "txn-1")
			calls := 0
			fake := &fakeInternalExecutor{execHook: func(context.Context, string, ie.SessionOverrideOptions) error {
				calls++
				store.available.Store(false)
				if tc.unknown {
					return unknown
				}
				return nil
			}}
			executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fake }, service, "cn1")
			spec := newSQLTaskContextForTest(definition, tc.trigger)
			spec.TriggerCount = 1
			var err error
			if tc.class == sqlTaskScheduledReceipt {
				firing := newTaskFromMetadata(BuildSQLTaskMetadata(spec))
				firing.Status, firing.TaskRunner, firing.Epoch = task.TaskStatus_Running, "cn1", 1
				firing.ParentTaskID = fmt.Sprintf("sql-task:%d", definition.TaskID)
				mustAddTestAsyncTask(t, store, 1, firing)
				firing = mustGetTestAsyncTask(t, store, 1)[0]
				err = executor.TaskExecutor()(ctx, &firing)
			} else {
				err = executor.ExecuteContext(ctx, spec, true)
			}
			var pending *SQLTaskMetadataPendingError
			require.ErrorAs(t, err, &pending)
			require.True(t, pending.Recovering)
			if tc.unknown {
				require.ErrorIs(t, err, unknown)
			} else {
				require.Nil(t, pending.Cause)
			}
			require.Equal(t, 1, calls)
			require.Equal(t, SQLTaskStatusRunning, mustGetTestSQLTaskRun(t, store, 1)[0].Status)
			manual := newSQLTaskContextForTest(definition, SQLTaskTriggerManual)
			require.ErrorIs(t, executor.ExecuteContext(ctx, manual, true), ErrSQLTaskOverlap)
			require.Equal(t, 1, calls)
			require.Eventually(t, func() bool {
				service.sqlRecovery.mu.Lock()
				defer service.sqlRecovery.mu.Unlock()
				return service.sqlRecovery.pending[tc.class].Len() == 1
			}, time.Second, time.Millisecond)
			store.available.Store(true)
			require.Eventually(t, func() bool {
				service.sqlRecovery.mu.Lock()
				defer service.sqlRecovery.mu.Unlock()
				return service.sqlRecovery.used[tc.class] == 0
			}, 2*time.Second, time.Millisecond)
			stored := mustGetTestSQLTaskRun(t, store, 1)[0]
			if tc.unknown {
				require.Equal(t, SQLTaskStatusRunning, stored.Status)
				require.Equal(t, int(moerr.ErrTxnUnknown), stored.ErrorCode)
				_, err = store.AcquireSQLTaskRun(ctx, definition, newTestSQLTaskRun(definition.TaskID, definition.TaskName, SQLTaskStatusRunning), testSQLTaskAdmission(nil, 0))
				require.ErrorIs(t, err, ErrSQLTaskOverlap)
			} else {
				require.Equal(t, SQLTaskStatusSuccess, stored.Status)
				if tc.class == sqlTaskScheduledReceipt {
					require.Equal(t, task.TaskStatus_Completed, mustGetTestAsyncTask(t, store, 1)[0].Status)
				}
				_, err = store.AcquireSQLTaskRun(ctx, definition, newTestSQLTaskRun(definition.TaskID, definition.TaskName, SQLTaskStatusRunning), testSQLTaskAdmission(nil, 0))
				require.NoError(t, err)
			}
			require.Equal(t, 1, calls, "metadata recovery must not execute the SQL body")
		})
	}
}

func TestSQLTaskRecoveryRetriesAndRotatesShares(t *testing.T) {
	var recovered atomic.Bool
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	attempts := make(chan sqlTaskReceiptClass, 16)
	var calls atomic.Int32
	r := newSQLTaskRecovery(16, 9, func(ctx context.Context, receipt *sqlTaskReceipt) bool {
		if calls.Add(1) == 1 {
			close(firstStarted)
			select {
			case <-releaseFirst:
			case <-ctx.Done():
				return false
			}
		}
		attempts <- receipt.credit.class
		return recovered.Load()
	})
	t.Cleanup(func() { r.close() })

	for class := sqlTaskManualReceipt; class < sqlTaskReceiptClasses; class++ {
		credit := r.reserve(class, 1, "task")
		require.NotNil(t, credit)
		receipt := &sqlTaskReceipt{SQLTaskReceiptRequest: SQLTaskReceiptRequest{Claim: SQLTaskClaim{ID: uint64(class) + 1}}, credit: credit}
		require.True(t, r.enqueue(receipt))
		require.False(t, r.enqueue(receipt), "a credit can be transferred only once")
		if class == sqlTaskManualReceipt {
			<-firstStarted
		}
	}
	close(releaseFirst)
	for _, want := range []sqlTaskReceiptClass{sqlTaskManualReceipt, sqlTaskScheduledReceipt} {
		select {
		case got := <-attempts:
			require.Equal(t, want, got)
		case <-time.After(2 * time.Second):
			t.Fatal("worker did not rotate across receipt shares")
		}
	}

	// Restore the dependency after every share has seen the outage.
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.pending[0].Len()+r.pending[1].Len() == 2
	}, time.Second, time.Millisecond)
	recovered.Store(true)
	for i := 0; i < 2; i++ {
		select {
		case <-attempts:
		case <-time.After(2 * time.Second):
			t.Fatal("worker did not retry after recovery")
		}
	}
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.used == [sqlTaskReceiptClasses]int{} && !r.started
	}, time.Second, time.Millisecond)
	credit := r.reserve(sqlTaskManualReceipt, 1, "later-task")
	require.NotNil(t, credit)
	require.True(t, r.enqueue(&sqlTaskReceipt{credit: credit}))
	select {
	case got := <-attempts:
		require.Equal(t, sqlTaskManualReceipt, got)
	case <-time.After(time.Second):
		t.Fatal("worker did not restart for a later receipt")
	}
}
