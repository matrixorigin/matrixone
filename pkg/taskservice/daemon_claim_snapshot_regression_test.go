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

package taskservice

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/require"
)

func TestStartDaemonTaskRejectsStaleClaimSnapshot(t *testing.T) {
	for _, status := range []task.TaskStatus{task.TaskStatus_Running, task.TaskStatus_RestartRequested} {
		for _, change := range []string{"runner", "last_run", "both", "fresh_heartbeat", "future_last_run_fresh_heartbeat", "future_last_run_expired_heartbeat"} {
			t.Run(status.String()+"/"+change, func(t *testing.T) {
				r, store := newDaemonHandleTestRunner(t)
				stale := newDaemonTaskForTest(1, status, "prior-owner")
				stale.LastRun = time.Now().Add(-time.Hour).Truncate(time.Microsecond)
				stale.LastHeartbeat = time.Now().Add(-time.Minute)
				current := stale
				current.LastHeartbeat = time.Now().Add(-time.Minute)
				if change == "runner" || change == "both" {
					current.TaskRunner = "new-owner"
				}
				if change == "last_run" || change == "both" {
					current.LastRun = time.Now().Add(time.Hour).Truncate(time.Microsecond)
				}
				if change == "fresh_heartbeat" || change == "future_last_run_fresh_heartbeat" {
					current.LastHeartbeat = time.Now()
				}
				if change == "future_last_run_fresh_heartbeat" || change == "future_last_run_expired_heartbeat" {
					current.LastRun = time.Now().Add(time.Hour).Truncate(time.Microsecond)
					stale.LastRun = current.LastRun
				}
				mustAddTestDaemonTask(t, store, 1, current)
				restart := status == task.TaskStatus_RestartRequested

				if change == "future_last_run_expired_heartbeat" {
					before := time.Now()
					dt := &daemonTask{task: stale}
					started, err := r.startDaemonTask(context.Background(), dt, restart)
					after := time.Now()
					require.NoError(t, err)
					require.True(t, started)
					stored := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, current.ID))[0]
					require.True(t, stored.LastRun.After(current.LastRun))
					require.False(t, stored.LastHeartbeat.Before(before))
					require.False(t, stored.LastHeartbeat.After(after))
					require.False(t, stored.UpdateAt.Before(before))
					require.False(t, stored.UpdateAt.After(after))
					require.True(t, dt.lastConfirmedHeartbeat().Equal(stored.LastHeartbeat))
					return
				}

				started, err := r.startDaemonTask(context.Background(), &daemonTask{task: stale}, restart)
				require.NoError(t, err)
				require.False(t, started, "a superseded claim or fresh heartbeat must prevent admission")
				stored := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, current.ID))[0]
				require.Equal(t, current.TaskRunner, stored.TaskRunner)
				require.True(t, current.LastRun.Equal(stored.LastRun))
				if change == "fresh_heartbeat" || change == "future_last_run_fresh_heartbeat" {
					return
				}

				started, err = r.startDaemonTask(context.Background(), &daemonTask{task: stored}, restart)
				require.NoError(t, err)
				require.True(t, started, "a fresh expired claim remains recoverable")
				stored = mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, current.ID))[0]
				require.Equal(t, r.runnerID, stored.TaskRunner)
				require.True(t, stored.LastRun.After(current.LastRun))
			})
		}
	}
}

// Recovery publishes a logical generation and a wall-clock lease separately.
func TestRecoveryClaimFutureGenerationUsesWallClockHeartbeat(t *testing.T) {
	for _, status := range []task.TaskStatus{task.TaskStatus_ResumeRequested, task.TaskStatus_RestartRequested} {
		t.Run(status.String(), func(t *testing.T) {
			r, store := newDaemonHandleTestRunner(t)
			initial := newDaemonTaskForTest(1, status, r.runnerID)
			initial.Metadata.Executor = task.TaskCode_InitCdcStableEpoch
			initial.LastRun = time.Now().Add(time.Hour).Truncate(time.Microsecond)
			initial.LastHeartbeat = time.Now()
			mustAddTestDaemonTask(t, store, 1, initial)
			tracker := &claimTrackingActiveRoutine{}
			routine := ActiveRoutine(tracker)
			local := &daemonTask{task: initial}
			local.activeRoutine.Store(&routine)
			r.addDaemonTask(local)
			before := time.Now()
			var err error
			if status == task.TaskStatus_ResumeRequested {
				err = newResumeTask(r, local).Handle(context.Background())
			} else {
				err = newRestartTask(r, local).Handle(context.Background())
			}
			after := time.Now()
			require.NoError(t, err)
			claim := tracker.latestClaim()
			require.True(t, claim.LastRun.After(initial.LastRun))
			require.False(t, claim.LastHeartbeat.Before(before))
			require.False(t, claim.LastHeartbeat.After(after))
		})
	}
}
