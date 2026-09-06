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

package embed

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/stretchr/testify/require"
)

// Memory and sqlmock storage cannot prove wire precision or affected-row
// semantics. Reuse the shared cluster and one paused row: no extra service,
// scheduler sleeps, CDC sink, or volume fixture is needed for this contract.
func TestDaemonClaimSQLRoundTrip(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		service, ok := cn.RawService().(taskServiceGetter).GetTaskService()
		require.True(t, ok)
		storage := service.GetStorage()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		// This shared fixture has no TestOnly daemon; use its supported executor
		// predicate for discovery, then the concrete generated ID for all writes.
		cond := taskservice.WithTaskExecutorCond(taskservice.EQ, task.TaskCode_TestOnly)
		claim := task.DaemonTask{
			Metadata:   task.TaskMetadata{ID: t.Name()},
			TaskStatus: task.TaskStatus_Paused,
			CreateAt:   time.Now(), UpdateAt: time.Now(),
			Details: &task.Details{Account: "sys"},
		}
		n, err := storage.AddDaemonTask(ctx, claim)
		require.NoError(t, err)
		require.Equal(t, 1, n)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanupCancel()
			n, err := storage.DeleteDaemonTask(cleanupCtx, cond)
			require.NoError(t, err)
			require.Equal(t, 1, n)
		}()
		rows, err := storage.QueryDaemonTask(ctx, cond)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		claim = rows[0]
		require.Equal(t, t.Name(), claim.Metadata.ID)
		cond = taskservice.WithTaskIDCond(taskservice.EQ, claim.ID)
		claim.TaskRunner = "claim-test-cn"
		claim.LastRun = time.Date(2026, 8, 1, 12, 0, 0, 123456000, time.UTC)
		claim.LastHeartbeat = claim.LastRun.Truncate(time.Second)
		n, err = storage.UpdateDaemonTask(ctx, []task.DaemonTask{claim}, cond)
		require.NoError(t, err)
		require.Equal(t, 1, n)
		rows, err = storage.QueryDaemonTask(ctx, cond)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.True(t, claim.LastRun.Equal(rows[0].LastRun), "claim must survive SQL serialization")

		t.Run("duplicate heartbeat is still owned", func(t *testing.T) {
			for range 2 {
				n, err := storage.HeartbeatDaemonTask(ctx, []task.DaemonTask{rows[0]})
				require.NoError(t, err)
				require.Equal(t, 1, n)
			}
		})
		t.Run("same-second replacement fences stale owner", func(t *testing.T) {
			replacement := claim
			replacement.LastRun = claim.LastRun.Add(time.Microsecond)
			n, err := storage.UpdateDaemonTask(ctx, []task.DaemonTask{replacement}, cond)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			for _, stale := range []task.DaemonTask{claim, rows[0]} {
				n, err = storage.HeartbeatDaemonTask(ctx, []task.DaemonTask{stale})
				require.NoError(t, err)
				require.Zero(t, n)
			}
			n, err = storage.HeartbeatDaemonTask(ctx, []task.DaemonTask{replacement})
			require.NoError(t, err)
			require.Equal(t, 1, n)
			replacement.TaskRunner = "other-cn"
			n, err = storage.HeartbeatDaemonTask(ctx, []task.DaemonTask{replacement})
			require.NoError(t, err)
			require.Zero(t, n)
		})
		t.Run("completion predicates and field ownership", func(t *testing.T) {
			for _, release := range []bool{false, true} {
				current := claim
				current.TaskStatus = task.TaskStatus_Running
				current.LastHeartbeat = time.Now().UTC().Truncate(time.Second)
				current.LastRun = claim.LastRun.Add(2 * time.Microsecond)
				n, err := storage.UpdateDaemonTask(ctx, []task.DaemonTask{current}, cond)
				require.NoError(t, err)
				require.Equal(t, 1, n)
				// All storage consumers must carry the bound claim argument,
				// including readers/deleters and whole-row claim admission.
				matching, err := storage.QueryDaemonTask(ctx, cond, taskservice.WithLastRun(current.LastRun))
				require.NoError(t, err)
				require.Len(t, matching, 1)
				obsolete, err := storage.QueryDaemonTask(ctx, cond, taskservice.WithLastRun(claim.LastRun))
				require.NoError(t, err)
				require.Empty(t, obsolete)
				n, err = storage.DeleteDaemonTask(ctx, cond, taskservice.WithLastRun(claim.LastRun))
				require.NoError(t, err)
				require.Zero(t, n)
				n, err = storage.UpdateDaemonTask(ctx, []task.DaemonTask{current}, cond,
					taskservice.WithLastRun(current.LastRun))
				require.NoError(t, err)
				require.Equal(t, 1, n)
				stale := claim
				stale.TaskStatus = task.TaskStatus_Running
				stale.Details = &task.Details{Account: "sys", Error: "obsolete failure"}
				n, err = service.UpdateDaemonTaskError(ctx, stale, release)
				require.NoError(t, err)
				require.Zero(t, n)
				n, err = service.UpdateDaemonTaskStatus(ctx, current.ID, task.TaskStatus_Paused,
					time.Now(), time.Time{}, taskservice.WithLastRun(stale.LastRun))
				require.NoError(t, err)
				require.Zero(t, n)
				completed := current
				completed.LastHeartbeat = claim.LastHeartbeat // deliberately stale input
				completed.Details = &task.Details{Account: "sys", Error: "current failure"}
				n, err = service.UpdateDaemonTaskError(ctx, completed, release)
				require.NoError(t, err)
				require.Equal(t, 1, n)
				got, err := storage.QueryDaemonTask(ctx, cond)
				require.NoError(t, err)
				require.Len(t, got, 1)
				require.True(t, current.LastRun.Equal(got[0].LastRun))
				require.Equal(t, "current failure", got[0].Details.Error)
				if release {
					require.Empty(t, got[0].TaskRunner)
					require.True(t, got[0].LastHeartbeat.IsZero())
					require.Equal(t, task.TaskStatus_RestartRequested, got[0].TaskStatus)
				} else {
					require.Equal(t, current.TaskRunner, got[0].TaskRunner)
					require.True(t, current.LastHeartbeat.Equal(got[0].LastHeartbeat))
					n, err = service.UpdateDaemonTaskStatus(ctx, current.ID, task.TaskStatus_Paused,
						time.Now(), time.Time{}, taskservice.WithLastRun(current.LastRun))
					require.NoError(t, err)
					require.Equal(t, 1, n)
				}
			}
		})
	})
}
