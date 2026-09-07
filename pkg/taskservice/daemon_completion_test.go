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

package taskservice

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/require"
)

// One stored row and barriers cover both pointer replacement and same-object
// reuse. Lease expiry is fixture state, never a scheduler sleep.
func TestDaemonStartupCompletionFencesReplacement(t *testing.T) {
	for _, restart := range []bool{false, true} {
		for _, reuse := range []bool{false, true} {
			name := "start"
			if restart {
				name = "restart"
			}
			if reuse {
				name += "/same-object"
			} else {
				name += "/same-CN-ABA"
			}
			t.Run(name, func(t *testing.T) {
				r, store := newDaemonHandleTestRunner(t)
				ctx := context.Background()
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				defer func() { once.Do(func() { close(release) }); r.stopper.Stop() }()
				initial := newDaemonTaskForTest(1, task.TaskStatus_Created, "")
				if restart {
					initial.TaskStatus = task.TaskStatus_RestartRequested
				}
				initial.Metadata.Executor = task.TaskCode_InitCdcStableEpoch
				initial.TaskType = task.TaskType_CreateCdc
				initial.LastHeartbeat = time.Now().Add(-time.Minute)
				mustAddTestDaemonTask(t, store, 1, initial)
				old := &daemonTask{task: initial, executor: func(context.Context, task.Task) error {
					close(entered)
					<-release
					return errors.New("late old startup failure")
				}}
				h := newStartTask(r, old)
				if restart {
					h = newRestartStartTask(r, old)
				}
				require.NoError(t, h.Handle(ctx))
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("startup not entered")
				}
				var replacement *daemonTask
				if reuse {
					requested := old.taskSnapshot()
					requested.TaskStatus = task.TaskStatus_ResumeRequested
					mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{requested})
					ar := ActiveRoutine(&mockFuncActiveRoutine{})
					old.activeRoutine.Store(&ar)
					require.NoError(t, newResumeTask(r, old).Handle(ctx))
					replacement = old
				} else {
					other := old.taskSnapshot()
					other.TaskRunner = "r2"
					other.LastRun = nextDaemonClaimTime(other.LastRun, time.Now())
					other.LastHeartbeat = time.Now()
					mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{other})
					r.doSendHeartbeat(ctx)
					require.False(t, r.exists(initial.ID))
					other.LastHeartbeat = time.Now().Add(-r.options.heartbeatTimeout - time.Second)
					mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{other})
					replacement = &daemonTask{task: other}
					ok, err := r.startDaemonTask(ctx, replacement, false)
					require.NoError(t, err)
					require.True(t, ok)
				}
				expected := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, initial.ID))[0]
				once.Do(func() { close(release) })
				r.stopper.Stop() // joins the old completion defer
				got := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, initial.ID))[0]
				require.Equal(t, expected, got)
				local, ok := r.getDaemonTask(initial.ID)
				require.True(t, ok)
				require.Same(t, replacement, local)
				require.NoError(t, r.service.HeartbeatDaemonTask(ctx, expected))
			})
		}
	}
}

func TestDaemonCompletionStorageFieldOwnership(t *testing.T) {
	for _, release := range []bool{false, true} {
		for _, change := range []string{"current", "generation", "runner", "status", "missing"} {
			name := "error/" + change
			if release {
				name = "release/" + change
			}
			t.Run(name, func(t *testing.T) {
				r, store := newDaemonHandleTestRunner(t)
				ctx := context.Background()
				claim := newDaemonTaskForTest(1, task.TaskStatus_Running, r.runnerID)
				claim.LastRun = time.Now().UTC().Truncate(time.Microsecond)
				current := claim
				current.LastHeartbeat = claim.LastRun.Add(time.Second)
				switch change {
				case "generation":
					current.LastRun = claim.LastRun.Add(time.Microsecond)
				case "runner":
					current.TaskRunner = "r2"
				case "status":
					current.TaskStatus = task.TaskStatus_CancelRequested
				}
				if change != "missing" {
					mustAddTestDaemonTask(t, store, 1, current)
				}
				claim.Details = cloneDaemonTaskDetails(claim.Details)
				claim.Details.Error = "startup failed"
				n, err := r.service.UpdateDaemonTaskError(ctx, claim, release)
				require.NoError(t, err)
				if change == "missing" {
					require.Zero(t, n)
					return
				}
				got := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, claim.ID))[0]
				if change != "current" {
					require.Zero(t, n)
					require.Equal(t, current, got)
					return
				}
				require.Equal(t, 1, n)
				expected := current
				expected.Details = claim.Details
				if release {
					expected.TaskStatus = task.TaskStatus_RestartRequested
					expected.TaskRunner = ""
					expected.LastHeartbeat = time.Time{}
				}
				require.Equal(t, expected, got)
				if release {
					n, err = r.service.UpdateDaemonTaskError(ctx, claim, true)
					require.NoError(t, err)
					require.Zero(t, n)
				}
			})
		}
	}
}

// The runner owns lifetime cleanup after Attach. A zero-row completion CAS is
// different from a backend failure: the former preserves a newer control
// request's routine, the latter must not keep renewing an already failed task.
func TestDaemonCompletionCleanupOwnership(t *testing.T) {
	for _, restart := range []bool{false, true} {
		for _, outcome := range []string{"failure", "storage-error", "superseded-control", "success"} {
			name := "start/" + outcome
			if restart {
				name = "restart/" + outcome
			}
			t.Run(name, func(t *testing.T) {
				r, store := newDaemonHandleTestRunner(t)
				claim := newDaemonTaskForTest(1, task.TaskStatus_Running, r.runnerID)
				claim.Metadata.Executor = task.TaskCode_InitCdcStableEpoch
				claim.TaskType = task.TaskType_CreateCdc
				claim.LastRun = time.Now().UTC().Truncate(time.Microsecond)
				stored := claim
				if outcome == "superseded-control" {
					stored.TaskStatus = task.TaskStatus_ResumeRequested
				}
				mustAddTestDaemonTask(t, store, 1, stored)
				routine := newMockActiveRoutine()
				ar := ActiveRoutine(routine)
				local := &daemonTask{task: claim}
				local.activeRoutine.Store(&ar)
				r.addDaemonTask(local)
				if outcome == "storage-error" {
					hook := &serviceWithDaemonHook{TaskService: r.service}
					hook.setUpdateErrorFn(func(context.Context, task.DaemonTask, bool) (int, error) {
						return 0, context.DeadlineExceeded
					})
					r.service = hook
				}
				var execErr error
				if outcome != "success" {
					execErr = errors.New("startup failed")
				}
				for range 2 { // duplicate callbacks have one effective cleanup owner
					r.completeDaemonTask(context.Background(), local, claim, restart, execErr)
				}
				retained := outcome == "success" || outcome == "superseded-control"
				require.Equal(t, retained, r.exists(claim.ID))
				require.Equal(t, !retained, local.claimLost.Load())
				if retained {
					require.Empty(t, routine.cancelC)
					got := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, claim.ID))[0]
					require.Equal(t, stored, got)
				} else {
					require.Len(t, routine.cancelC, 1)
				}
			})
		}
	}
}

// One row, explicit factory completion and explicit lease expiry distinguish
// admission still in progress from admission that can never Attach. Exercise
// the real dispatcher afterward: removal alone is not the recovery oracle.
func TestDaemonPreAttachFailureRecoversControlRequests(t *testing.T) {
	for _, code := range []task.TaskCode{task.TaskCode_InitCdc, task.TaskCode_InitCdcStableEpoch} {
		for _, restart := range []bool{false, true} {
			for _, status := range []task.TaskStatus{task.TaskStatus_PauseRequested, task.TaskStatus_CancelRequested,
				task.TaskStatus_ResumeRequested, task.TaskStatus_RestartRequested} {
				name := "start/" + status.String()
				if restart {
					name = "restart/" + status.String()
				}
				name = code.String() + "/" + name
				t.Run(name, func(t *testing.T) {
					r, store := newDaemonHandleTestRunner(t)
					defer r.stopper.Stop()
					ctx := context.Background()
					initial := newDaemonTaskForTest(1, task.TaskStatus_Created, "")
					initial.Metadata.Executor = code
					initial.TaskType = task.TaskType_CreateCdc
					initial.LastHeartbeat = time.Now().Add(-time.Minute)
					if restart {
						initial.TaskStatus = task.TaskStatus_RestartRequested
					}
					mustAddTestDaemonTask(t, store, 1, initial)
					local := &daemonTask{task: initial}
					ok, err := r.startDaemonTask(ctx, local, restart)
					require.NoError(t, err)
					require.True(t, ok)
					claim := local.taskSnapshot()
					requested := claim
					requested.TaskStatus = status
					mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{requested})
					if status == task.TaskStatus_CancelRequested {
						// Before factory completion, a missing routine is legitimately
						// still attaching and must not be prematurely retired.
						require.NoError(t, newCancelTask(r, claim.ID).Handle(ctx))
						require.True(t, r.exists(claim.ID))
						require.Equal(t, requested, mustGetTestDaemonTask(t, store, 1)[0])
					}
					failure := errors.New("factory query failed before Attach")
					r.completeDaemonTask(ctx, local, claim, restart, failure)
					require.False(t, r.exists(claim.ID))
					require.True(t, local.claimLost.Load())
					r.doSendHeartbeat(ctx)
					require.Equal(t, requested, mustGetTestDaemonTask(t, store, 1)[0], "completion must preserve the new request and stop renewal")
					// Queued callbacks cannot reopen the retired object.
					require.NoError(t, newResumeTask(r, local).Handle(ctx))
					require.NoError(t, newRestartTask(r, local).Handle(ctx))
					attachCtx := context.WithValue(ctx, daemonAttachmentContextKey{}, daemonAttachment{owner: local, claim: claim})
					require.Error(t, r.Attach(attachCtx, claim.ID, newMockActiveRoutine()))
					require.Equal(t, requested, mustGetTestDaemonTask(t, store, 1)[0])

					// Recovery uses the existing lease timeout; inject expiry rather
					// than sleeping or weakening production lease predicates.
					requested.LastHeartbeat = time.Now().Add(-r.options.heartbeatTimeout - time.Second)
					mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{requested})
					attached := make(chan error, 1)
					r.RegisterExecutor(initial.Metadata.Executor, func(execCtx context.Context, spec task.Task) error {
						err := r.Attach(execCtx, spec.GetID(), newMockActiveRoutine())
						attached <- err
						return err
					})
					r.dispatchTaskHandle(ctx)
					require.Len(t, r.pendingTaskHandle, 1)
					require.NoError(t, (<-r.pendingTaskHandle).Handle(ctx))
					want := task.TaskStatus_Running
					switch status {
					case task.TaskStatus_PauseRequested:
						want = task.TaskStatus_Paused
					case task.TaskStatus_CancelRequested:
						want = task.TaskStatus_Canceled
					default:
						select {
						case err := <-attached:
							require.NoError(t, err)
						case <-time.After(5 * time.Second):
							t.Fatal("replacement did not attach")
						}
						r.stopper.Stop() // join replacement startup before final assertions
					}
					got := mustGetTestDaemonTask(t, store, 1)[0]
					require.Equal(t, want, got.TaskStatus)
					r.completeDaemonTask(ctx, local, claim, restart, failure)
					require.Equal(t, got, mustGetTestDaemonTask(t, store, 1)[0], "duplicate old completion must not disturb recovery")
					if want == task.TaskStatus_Running {
						replacement, exists := r.getDaemonTask(claim.ID)
						require.True(t, exists)
						require.NotSame(t, local, replacement)
						require.True(t, got.LastRun.After(claim.LastRun))
						require.NoError(t, r.service.HeartbeatDaemonTask(ctx, got))
					}
				})
			}
		}
	}
}

func TestDaemonAttachFencesOrigin(t *testing.T) {
	r, _ := newDaemonHandleTestRunner(t)
	claim := newDaemonTaskForTest(1, task.TaskStatus_Running, r.runnerID)
	claim.LastRun = time.Now()
	origin := &daemonTask{task: claim}
	r.addDaemonTask(origin)
	ctx := context.WithValue(context.Background(), daemonAttachmentContextKey{}, daemonAttachment{owner: origin, claim: claim})
	first := &mockFuncActiveRoutine{}
	require.NoError(t, r.Attach(ctx, claim.ID, first))
	newClaim := claim
	newClaim.LastRun = claim.LastRun.Add(time.Microsecond)
	origin.publishClaim(newClaim)
	require.Error(t, r.Attach(ctx, claim.ID, &mockFuncActiveRoutine{}))
	require.Same(t, first, *origin.activeRoutine.Load())
	r.removeDaemonTaskIf(claim.ID, origin)
	replacement := &daemonTask{task: newClaim}
	r.addDaemonTask(replacement)
	require.Error(t, r.Attach(ctx, claim.ID, &mockFuncActiveRoutine{}))
	require.Nil(t, replacement.activeRoutine.Load())
}

func TestLifecycleCompletionPreservesClaimAndHeartbeat(t *testing.T) {
	for _, status := range []task.TaskStatus{task.TaskStatus_ResumeRequested, task.TaskStatus_RestartRequested} {
		for _, supersede := range []bool{false, true} {
			name := status.String() + "/heartbeat"
			if supersede {
				name = status.String() + "/new-claim"
			}
			t.Run(name, func(t *testing.T) {
				r, store := newDaemonHandleTestRunner(t)
				claim := newDaemonTaskForTest(1, status, r.runnerID)
				claim.LastRun = time.Now().UTC().Truncate(time.Microsecond)
				mustAddTestDaemonTask(t, store, 1, claim)
				local := &daemonTask{task: claim}
				var expected task.DaemonTask
				inflight := func() error {
					expected = local.taskSnapshot()
					expected.LastHeartbeat = expected.LastRun.Add(time.Second)
					if supersede {
						expected.LastRun = expected.LastRun.Add(time.Microsecond)
					}
					mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{expected})
					return nil
				}
				ar := ActiveRoutine(&mockFuncActiveRoutine{resume: inflight, restart: inflight})
				local.activeRoutine.Store(&ar)
				if status == task.TaskStatus_ResumeRequested {
					require.NoError(t, newResumeTask(r, local).Handle(context.Background()))
				} else {
					require.NoError(t, newRestartTask(r, local).Handle(context.Background()))
				}
				got := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, claim.ID))[0]
				require.Equal(t, expected.LastRun, got.LastRun)
				require.Equal(t, expected.LastHeartbeat, got.LastHeartbeat)
				if !supersede {
					expected.TaskStatus = task.TaskStatus_Running
				}
				require.Equal(t, expected.TaskStatus, got.TaskStatus)
			})
		}
	}
}

func TestControlStatusCompletionFencesClaim(t *testing.T) {
	for _, status := range []task.TaskStatus{task.TaskStatus_PauseRequested, task.TaskStatus_CancelRequested} {
		for _, supersede := range []bool{false, true} {
			name := status.String() + "/heartbeat"
			if supersede {
				name = status.String() + "/superseded"
			}
			t.Run(name, func(t *testing.T) {
				r, store := newDaemonHandleTestRunner(t)
				claim := newDaemonTaskForTest(1, status, r.runnerID)
				claim.LastRun = time.Now().UTC().Truncate(time.Microsecond)
				mustAddTestDaemonTask(t, store, 1, claim)
				routine := newMockActiveRoutine()
				ar := ActiveRoutine(routine)
				local := &daemonTask{task: claim}
				local.activeRoutine.Store(&ar)
				r.addDaemonTask(local)
				hook := &serviceWithDaemonHook{TaskService: r.service}
				r.service = hook
				changed := claim
				changed.LastHeartbeat = claim.LastRun.Add(time.Second)
				if supersede {
					changed.LastRun = claim.LastRun.Add(time.Microsecond)
				}
				hook.updateStatusFn = func(ctx context.Context, id uint64, target task.TaskStatus, updateAt, endAt time.Time, conds ...Condition) (int, error) {
					mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{changed})
					return hook.TaskService.UpdateDaemonTaskStatus(ctx, id, target, updateAt, endAt, conds...)
				}
				if status == task.TaskStatus_PauseRequested {
					require.NoError(t, newPauseTask(r, local).Handle(context.Background()))
				} else {
					require.NoError(t, newCancelTask(r, claim.ID).Handle(context.Background()))
				}
				got := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, claim.ID))[0]
				require.Equal(t, changed.LastRun, got.LastRun)
				require.Equal(t, changed.LastHeartbeat, got.LastHeartbeat)
				if supersede {
					require.Equal(t, changed.TaskStatus, got.TaskStatus)
				} else if status == task.TaskStatus_PauseRequested {
					require.Equal(t, task.TaskStatus_Paused, got.TaskStatus)
				} else {
					require.Equal(t, task.TaskStatus_Canceled, got.TaskStatus)
				}
				if status == task.TaskStatus_CancelRequested && supersede {
					select {
					case <-routine.cancelC:
						t.Fatal("stale completion cancelled replacement")
					default:
					}
				}
			})
		}
	}
}
