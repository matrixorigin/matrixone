// Copyright 2022 Matrix Origin
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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	storages = map[string]func(*testing.T) TaskStorage{
		"mem":     createMem,
		"refresh": createRefresh,
	}
)

func createMem(t *testing.T) TaskStorage {
	return NewMemTaskStorage()
}

func createRefresh(t *testing.T) TaskStorage {
	return newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return "", nil },
		NewFixedTaskStorageFactory(NewMemTaskStorage()))
}

// TODO: move to cluster testing.
// func createMysql(t *testing.T) TaskStorage {
// 	storage, err := newMysqlTaskStorage("root:root@tcp(127.0.0.1:12345)/mo_task")
// 	require.NoError(t, err)
// 	return storage
// }

func TestAddAsyncTask(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, s.Close())
			}()

			v := newTestAsyncTask("t1")
			mustAddTestAsyncTask(t, s, 1, v)
			mustAddTestAsyncTask(t, s, 0, v)
			assert.Equal(t, 1, len(mustGetTestAsyncTask(t, s, 1)))
		})
	}
}

func TestUpdateAsyncTask(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, s.Close())
			}()

			v := newTestAsyncTask("t1")
			mustAddTestAsyncTask(t, s, 1, v)

			tasks := mustGetTestAsyncTask(t, s, 1)
			tasks[0].Metadata.Executor = 1
			mustUpdateTestAsyncTask(t, s, 1, tasks)
		})
	}
}

func TestUpdateAsyncTaskWithConditions(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, s.Close())
			}()

			mustAddTestAsyncTask(t, s, 1, newTestAsyncTask("t1"))
			tasks := mustGetTestAsyncTask(t, s, 1)

			mustUpdateTestAsyncTask(t, s, 0, tasks, WithTaskRunnerCond(EQ, "t2"))
			mustUpdateTestAsyncTask(t, s, 1, tasks, WithTaskRunnerCond(EQ, "t1"))

			tasks[0].Metadata.Context = []byte{1}
			mustUpdateTestAsyncTask(t, s, 0, tasks, WithTaskIDCond(EQ, tasks[0].ID+1))
			mustUpdateTestAsyncTask(t, s, 1, tasks, WithTaskIDCond(EQ, tasks[0].ID))
			tasks[0].Metadata.Context = []byte{1, 2}
			mustUpdateTestAsyncTask(t, s, 1, tasks, WithTaskIDCond(GT, 0))
		})
	}
}

func TestDeleteAsyncTaskWithConditions(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, s.Close())
			}()

			mustAddTestAsyncTask(t, s, 1, newTestAsyncTask("t1"))
			mustAddTestAsyncTask(t, s, 1, newTestAsyncTask("t2"))
			mustAddTestAsyncTask(t, s, 1, newTestAsyncTask("t3"))
			tasks := mustGetTestAsyncTask(t, s, 3)

			mustDeleteTestAsyncTask(t, s, 0, WithTaskRunnerCond(EQ, "t4"))
			mustDeleteTestAsyncTask(t, s, 1, WithTaskRunnerCond(EQ, "t1"))

			mustDeleteTestAsyncTask(t, s, 0, WithTaskIDCond(EQ, tasks[len(tasks)-1].ID+1))
			mustDeleteTestAsyncTask(t, s, 2, WithTaskIDCond(GT, tasks[0].ID))

			mustGetTestAsyncTask(t, s, 0)
		})
	}
}

func TestQueryAsyncTaskWithConditions(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, s.Close())
			}()

			mustAddTestAsyncTask(t, s, 1, newTestAsyncTask("t1"))
			mustAddTestAsyncTask(t, s, 1, newTestAsyncTask("t2"))
			mustAddTestAsyncTask(t, s, 1, newTestAsyncTask("t3"))
			tasks := mustGetTestAsyncTask(t, s, 3)

			mustGetTestAsyncTask(t, s, 1, WithLimitCond(1))
			mustGetTestAsyncTask(t, s, 1, WithTaskRunnerCond(EQ, "t1"))
			mustGetTestAsyncTask(t, s, 2, WithTaskIDCond(GT, tasks[0].ID))
			mustGetTestAsyncTask(t, s, 3, WithTaskIDCond(GE, tasks[0].ID))
			mustGetTestAsyncTask(t, s, 3, WithTaskIDCond(LE, tasks[2].ID))
			mustGetTestAsyncTask(t, s, 2, WithTaskIDCond(LT, tasks[2].ID))
			mustGetTestAsyncTask(t, s, 1, WithLimitCond(1), WithTaskIDCond(GT, tasks[0].ID))
			mustGetTestAsyncTask(t, s, 1, WithTaskIDCond(EQ, tasks[0].ID))
		})
	}
}

func TestAddAndQueryCronTask(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, s.Close())
			}()

			mustQueryTestCronTask(t, s, 0)

			v1 := newTestCronTask("t1", "cron1")
			v2 := newTestCronTask("t2", "cron2")
			mustAddTestCronTask(t, s, 2, v1, v2)
			mustQueryTestCronTask(t, s, 2)

			mustAddTestCronTask(t, s, 0, v1, v2)
			mustQueryTestCronTask(t, s, 2)
		})
	}
}

func TestUpdateCronTask(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, s.Close())
			}()

			v1 := newTestCronTask("t1", "cron1")
			v2 := newTestAsyncTask("t1-cron-1")
			v3 := newTestAsyncTask("t1-cron-2")

			ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
			defer cancel()

			n, err := s.UpdateCronTask(ctx, v1, v2)
			assert.NoError(t, err)
			assert.Equal(t, 0, n)

			mustAddTestCronTask(t, s, 1, v1)
			mustAddTestAsyncTask(t, s, 1, v2)

			v1 = mustQueryTestCronTask(t, s, 1)[0]

			n, err = s.UpdateCronTask(ctx, v1, v2)
			assert.NoError(t, err)
			assert.Equal(t, 0, n)

			n, err = s.UpdateCronTask(ctx, v1, v3)
			assert.NoError(t, err)
			assert.Equal(t, 0, n)

			v1.TriggerTimes++
			n, err = s.UpdateCronTask(ctx, v1, v3)
			assert.NoError(t, err)
			assert.Equal(t, 2, n)
		})
	}
}

func TestAddDaemonTask(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, s.Close())
			}()

			v := newTestDaemonTask(1, "t1")
			mustAddTestDaemonTask(t, s, 1, v)
			mustAddTestDaemonTask(t, s, 0, v)
			assert.Equal(t, 1, len(mustGetTestDaemonTask(t, s, 1)))
		})
	}
}

func TestUpdateDaemonTask(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, s.Close())
			}()

			v := newTestDaemonTask(1, "t1")
			mustAddTestDaemonTask(t, s, 1, v)

			tasks := mustGetTestDaemonTask(t, s, 1)
			tasks[0].Metadata.Executor = 1
			mustUpdateTestDaemonTask(t, s, 1, tasks)
		})
	}
}

func TestUpdateDaemonTaskWithConditions(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, s.Close())
			}()

			mustAddTestDaemonTask(t, s, 1, newTestDaemonTask(1, "t1"))
			tasks := mustGetTestDaemonTask(t, s, 1)

			mustUpdateTestDaemonTask(t, s, 0, tasks, WithTaskRunnerCond(EQ, "t2"))
			mustUpdateTestDaemonTask(t, s, 1, tasks, WithTaskRunnerCond(EQ, "t1"))
			mustUpdateTestDaemonTask(t, s, 0, tasks,
				WithTaskExecutorCond(EQ, task.TaskCode_InitCdc))
			mustUpdateTestDaemonTask(t, s, 1, tasks,
				WithTaskExecutorCond(EQ, task.TaskCode_TestOnly))

			tasks[0].Metadata.Context = []byte{1}
			mustUpdateTestDaemonTask(t, s, 0, tasks, WithTaskIDCond(EQ, tasks[0].ID+1))
			mustUpdateTestDaemonTask(t, s, 1, tasks, WithTaskIDCond(EQ, tasks[0].ID))
			tasks[0].Metadata.Context = []byte{1, 2}
			mustUpdateTestDaemonTask(t, s, 1, tasks, WithTaskIDCond(GT, 0))
		})
	}
}

func TestUpdateDaemonTaskStatusPreservesLeaseAndPayload(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, s.Close())
			}()

			v := newTestDaemonTask(1, "t1")
			v.TaskStatus = task.TaskStatus_Running
			v.LastHeartbeat = time.Now().Add(-time.Minute)
			v.Metadata.Context = []byte("payload")
			mustAddTestDaemonTask(t, s, 1, v)

			updateAt := time.Now()
			endAt := updateAt.Add(time.Second)
			n, err := s.UpdateDaemonTaskStatus(
				context.Background(),
				v.ID,
				task.TaskStatus_Canceled,
				updateAt,
				endAt,
				WithTaskStatusCond(task.TaskStatus_Running),
				WithTaskRunnerCond(EQ, v.TaskRunner),
			)
			require.NoError(t, err)
			require.Equal(t, 1, n)

			got := mustGetTestDaemonTask(t, s, 1, WithTaskIDCond(EQ, v.ID))[0]
			require.Equal(t, task.TaskStatus_Canceled, got.TaskStatus)
			require.Equal(t, updateAt, got.UpdateAt)
			require.Equal(t, endAt, got.EndAt)
			require.Equal(t, v.TaskRunner, got.TaskRunner)
			require.Equal(t, v.LastHeartbeat, got.LastHeartbeat)
			require.Equal(t, v.Metadata, got.Metadata)
		})
	}
}

func TestRequestDaemonTaskStatusPreservesClaim(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() { require.NoError(t, s.Close()) }()
			ctx := context.Background()
			observed := newDaemonTaskForTest(42, task.TaskStatus_Running, "owner")
			observed.Metadata.ID = "request-status"
			observed.AccountID = 17
			observed.Account = "tenant"
			observed.TaskType = task.TaskType_CreateCdc
			observed.LastRun = time.Date(2026, 8, 1, 12, 0, 0, 123456000, time.UTC)
			current := observed
			current.LastHeartbeat = observed.LastRun.Add(time.Second)
			mustAddTestDaemonTask(t, s, 1, current)
			for _, stale := range []task.DaemonTask{
				func() task.DaemonTask { v := observed; v.AccountID++; return v }(),
				func() task.DaemonTask { v := observed; v.TaskStatus = task.TaskStatus_Paused; return v }(),
				func() task.DaemonTask { v := observed; v.TaskRunner = "other"; return v }(),
				func() task.DaemonTask { v := observed; v.LastRun = v.LastRun.Add(time.Microsecond); return v }(),
			} {
				n, err := s.RequestDaemonTaskStatus(ctx, stale, task.TaskStatus_PauseRequested)
				require.NoError(t, err)
				require.Zero(t, n)
			}
			n, err := s.RequestDaemonTaskStatus(ctx, observed, task.TaskStatus_PauseRequested)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			rows, err := s.QueryDaemonTask(ctx, WithTaskIDCond(EQ, observed.ID))
			require.NoError(t, err)
			require.Len(t, rows, 1)
			current.TaskStatus = task.TaskStatus_PauseRequested
			require.Equal(t, current, rows[0])
			n, err = s.RequestDaemonTaskStatus(ctx, current, current.TaskStatus)
			require.NoError(t, err)
			require.Equal(t, 1, n, "matched same-state requests count as matched rows")
			rows, err = s.QueryDaemonTask(ctx, WithTaskType(EQ, task.TaskType_CreateCdc.String()), WithAccountID(EQ, 18), WithAccount(EQ, "tenant"))
			require.NoError(t, err)
			require.Empty(t, rows, "a later matching condition cannot override a failed account predicate")
			rows, err = s.QueryDaemonTask(ctx, WithTaskType(EQ, task.TaskType_CreateCdc.String()), WithAccountID(EQ, 17), WithAccount(EQ, "tenant"))
			require.NoError(t, err)
			require.Len(t, rows, 1)
		})
	}
}

func TestDeleteDaemonTaskWithConditions(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, s.Close())
			}()

			mustAddTestDaemonTask(t, s, 1, newTestDaemonTask(1, "t1"))
			mustAddTestDaemonTask(t, s, 1, newTestDaemonTask(2, "t2"))
			mustAddTestDaemonTask(t, s, 1, newTestDaemonTask(3, "t3"))
			tasks := mustGetTestDaemonTask(t, s, 3)

			mustDeleteTestDaemonTask(t, s, 0, WithTaskRunnerCond(EQ, "t4"))
			mustDeleteTestDaemonTask(t, s, 1, WithTaskRunnerCond(EQ, "t1"))

			mustDeleteTestDaemonTask(t, s, 0, WithTaskIDCond(EQ, tasks[len(tasks)-1].ID+1))
			mustDeleteTestDaemonTask(t, s, 2, WithTaskIDCond(GT, tasks[0].ID))

			mustGetTestDaemonTask(t, s, 0)
		})
	}
}

func TestQueryDaemonTaskWithConditions(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, s.Close())
			}()

			t1 := newTestDaemonTask(1, "t1")
			t2 := newTestDaemonTask(2, "t2")
			t2.Metadata.Executor = task.TaskCode_InitCdc
			t3 := newTestDaemonTask(3, "t3")
			mustAddTestDaemonTask(t, s, 1, t1)
			mustAddTestDaemonTask(t, s, 1, t2)
			mustAddTestDaemonTask(t, s, 1, t3)
			tasks := mustGetTestDaemonTask(t, s, 3)

			mustGetTestDaemonTask(t, s, 1, WithLimitCond(1))
			mustGetTestDaemonTask(t, s, 1, WithTaskRunnerCond(EQ, "t1"))
			mustGetTestDaemonTask(t, s, 2, WithTaskIDCond(GT, tasks[0].ID))
			mustGetTestDaemonTask(t, s, 3, WithTaskIDCond(GE, tasks[0].ID))
			mustGetTestDaemonTask(t, s, 3, WithTaskIDCond(LE, tasks[2].ID))
			mustGetTestDaemonTask(t, s, 2, WithTaskIDCond(LT, tasks[2].ID))
			mustGetTestDaemonTask(t, s, 1, WithLimitCond(1), WithTaskIDCond(GT, tasks[0].ID))
			mustGetTestDaemonTask(t, s, 1, WithTaskIDCond(EQ, tasks[0].ID))
			testOnly := mustGetTestDaemonTask(t, s, 2,
				WithTaskExecutorCond(EQ, task.TaskCode_TestOnly))
			for _, daemonTask := range testOnly {
				require.Equal(t, task.TaskCode_TestOnly, daemonTask.Metadata.Executor)
			}
			cdc := mustGetTestDaemonTask(t, s, 1,
				WithTaskExecutorCond(EQ, task.TaskCode_InitCdc))
			require.Equal(t, task.TaskCode_InitCdc, cdc[0].Metadata.Executor)
		})
	}
}

func TestAddQueryUpdateDeleteSQLTask(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, s.Close())
			}()

			mustGetTestSQLTask(t, s, 0)

			t1 := newTestSQLTask("task-1", 1)
			t2 := newTestSQLTask("task-2", 2)
			mustAddTestSQLTask(t, s, 2, t1, t2)
			mustAddTestSQLTask(t, s, 0, t1)

			tasks := mustGetTestSQLTask(t, s, 2)
			mustGetTestSQLTask(t, s, 1, WithTaskName(EQ, "task-1"))
			mustGetTestSQLTask(t, s, 1, WithAccountID(EQ, 2))
			mustGetTestSQLTask(t, s, 1, WithSQLTaskEnabled(true), WithLimitCond(1))

			changed := tasks[0]
			changed.Timezone = "Asia/Shanghai"
			n, err := s.AlterSQLTask(context.Background(), tasks[0], changed, SQLTaskSetSchedule)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			assert.Equal(t, "Asia/Shanghai", mustGetTestSQLTask(t, s, 1, WithTaskIDCond(EQ, tasks[0].TaskID))[0].Timezone)

			mustDeleteTestSQLTask(t, s, 1, WithTaskIDCond(EQ, tasks[1].TaskID))
			mustGetTestSQLTask(t, s, 1)
		})
	}
}

func TestAddUpdateQuerySQLTaskRun(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, s.Close())
			}()

			run1 := newTestSQLTaskRun(1, "task-1", SQLTaskStatusRunning)
			run1.TriggerType = SQLTaskTriggerScheduled
			run2 := newTestSQLTaskRun(1, "task-1", SQLTaskStatusFailed)
			run2.TriggerType = SQLTaskTriggerManual
			run2.AccountID = 2
			run2.RunnerCN = "cn2"
			mustAddTestSQLTaskRun(t, s, 2, run1, run2)

			runs := mustGetTestSQLTaskRun(t, s, 2)
			latest := mustGetTestSQLTaskRun(t, s, 1, WithLimitCond(1))
			require.Equal(t, runs[0].RunID, latest[0].RunID)
			mustGetTestSQLTaskRun(t, s, 1, WithTaskIDCond(EQ, 1), WithSQLTaskRunStatus(EQ, SQLTaskStatusRunning))
			mustGetTestSQLTaskRun(t, s, 1, WithSQLTaskTriggerType(EQ, SQLTaskTriggerManual))
			mustGetTestSQLTaskRun(t, s, 1, WithSQLTaskRunnerCond(EQ, "cn2"))
			mustGetTestSQLTaskRun(t, s, 1, WithAccountID(EQ, 1))
			mustGetTestSQLTaskRun(t, s, 1, WithAccountID(EQ, 2))

			running := mustGetTestSQLTaskRun(t, s, 1, WithSQLTaskRunStatus(EQ, SQLTaskStatusRunning))[0]
			running.Status = SQLTaskStatusSuccess
			resolved, err := s.PublishSQLTaskReceipt(context.Background(), SQLTaskReceiptRequest{Kind: SQLTaskReceiptTerminal, Run: running})
			require.NoError(t, err)
			require.True(t, resolved)
			assert.Equal(t, SQLTaskStatusSuccess, mustGetTestSQLTaskRun(t, s, 1, WithSQLTaskRunIDCond(EQ, running.RunID))[0].Status)
		})
	}
}

func TestQueryLatestSQLTaskRun(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, s.Close())
			}()

			run1 := newTestSQLTaskRun(1, "task-1", SQLTaskStatusRunning)
			run2 := newTestSQLTaskRun(1, "task-1", SQLTaskStatusSuccess)
			run3 := newTestSQLTaskRun(2, "task-2", SQLTaskStatusFailed)
			run4 := newTestSQLTaskRun(2, "task-2", SQLTaskStatusSuccess)
			run4.AccountID = 2
			mustAddTestSQLTaskRun(t, s, 4, run1, run2, run3, run4)

			latest := mustGetLatestTestSQLTaskRun(t, s, 1, []uint64{1, 2}, 2)
			require.Equal(t, uint64(1), latest[0].TaskID)
			require.Equal(t, SQLTaskStatusSuccess, latest[0].Status)
			require.Equal(t, uint64(2), latest[1].TaskID)
			require.Equal(t, SQLTaskStatusFailed, latest[1].Status)

			latest = mustGetLatestTestSQLTaskRun(t, s, 2, []uint64{2}, 1)
			require.Equal(t, SQLTaskStatusSuccess, latest[0].Status)
			mustGetLatestTestSQLTaskRun(t, s, 1, nil, 0)
		})
	}
}

func TestTriggerSQLTask(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, s.Close())
			}()

			sqlTask := newTestSQLTask("task-1", 1)
			sqlTask.TriggerCount = 0
			mustAddTestSQLTask(t, s, 1, sqlTask)
			sqlTask = mustGetTestSQLTask(t, s, 1)[0]
			sqlTask.TriggerCount++
			sqlTask.NextFireTime = 100

			asyncTask := newTestAsyncTask("sql-task:1:1")
			n, err := s.TriggerSQLTask(context.Background(), sqlTask, asyncTask)
			require.NoError(t, err)
			require.Equal(t, 2, n)

			mustGetTestAsyncTask(t, s, 1, WithTaskMetadataId(EQ, asyncTask.Metadata.ID))
			assert.Equal(t, uint64(1), mustGetTestSQLTask(t, s, 1, WithTaskIDCond(EQ, sqlTask.TaskID))[0].TriggerCount)

			n, err = s.TriggerSQLTask(context.Background(), sqlTask, asyncTask)
			require.NoError(t, err)
			require.Equal(t, 0, n)
		})
	}
}

func TestTriggerSQLTaskRejectsStaleScheduleAndPreservesChanges(t *testing.T) {
	ctx := context.Background()
	store := NewMemTaskStorage()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	definition := newTestSQLTask("trigger-generation", 1)
	_, err := store.AddSQLTask(ctx, definition)
	require.NoError(t, err)
	definition = mustGetTestSQLTask(t, store, 1)[0]
	stale := definition
	stale.TriggerCount++
	async := newTestAsyncTask("generation-test")

	_, err = store.AlterSQLTask(ctx, definition, definition, SQLTaskSuspend)
	require.NoError(t, err)
	affected, err := store.TriggerSQLTask(ctx, stale, async)
	require.NoError(t, err)
	require.Zero(t, affected)

	changed := mustGetTestSQLTask(t, store, 1)[0]
	_, err = store.AlterSQLTask(ctx, changed, changed, SQLTaskResume)
	require.NoError(t, err)
	changed.GateCondition = "new gate"
	_, err = store.AlterSQLTask(ctx, changed, changed, SQLTaskSetGate)
	require.NoError(t, err)
	changed = mustGetTestSQLTask(t, store, 1)[0]
	affected, err = store.TriggerSQLTask(ctx, stale, async)
	require.NoError(t, err)
	require.Zero(t, affected)

	current := stale
	current.ScheduleGeneration = changed.ScheduleGeneration
	current.TriggerCount = changed.TriggerCount + 1
	affected, err = store.TriggerSQLTask(ctx, current, async)
	require.NoError(t, err)
	require.Equal(t, 2, affected)
	got := mustGetTestSQLTask(t, store, 1)[0]
	require.Equal(t, "new gate", got.GateCondition)
	require.Equal(t, current.TriggerCount, got.TriggerCount)
}

func TestAlterSQLTaskAfterConcurrentTrigger(t *testing.T) {
	for name, factory := range storages {
		for _, action := range []SQLTaskAlterAction{SQLTaskSuspend, SQLTaskSetSchedule} {
			t.Run(fmt.Sprintf("%s/%d", name, action), func(t *testing.T) {
				ctx := t.Context()
				store := factory(t)
				t.Cleanup(func() { require.NoError(t, store.Close()) })
				mustAddTestSQLTask(t, store, 1, newTestSQLTask("concurrent-trigger", 1))
				observed := mustGetTestSQLTask(t, store, 1)[0]
				trigger := observed
				for i := 0; i < 2; i++ {
					trigger.TriggerCount++
					n, err := store.TriggerSQLTask(ctx, trigger, newTestAsyncTask(fmt.Sprintf("firing-%d", i)))
					require.NoError(t, err)
					require.Equal(t, 2, n)
				}
				updated := observed
				updated.CronExpr, updated.Timezone, updated.NextFireTime = "0 * * * * *", "UTC", 123
				n, err := store.AlterSQLTask(ctx, observed, updated, action)
				require.NoError(t, err)
				require.Equal(t, 1, n)
				got := mustGetTestSQLTask(t, store, 1)[0]
				require.Equal(t, trigger.TriggerCount+1, got.TriggerCount)
				require.Equal(t, got.TriggerCount, got.ScheduleGeneration)
				if action == SQLTaskSuspend {
					require.False(t, got.Enabled)
				} else {
					require.Equal(t, updated.CronExpr, got.CronExpr)
					require.Equal(t, updated.Timezone, got.Timezone)
					require.Equal(t, updated.NextFireTime, got.NextFireTime)
				}
				n, err = store.AlterSQLTask(ctx, observed, updated, action)
				require.NoError(t, err)
				require.Zero(t, n, "a competing definition change must not be overwritten")
			})
		}
	}
}

func TestAlterSQLTaskDoesNotUndoConcurrentSuspension(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := factory(t)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			_, err := store.AddSQLTask(ctx, newTestSQLTask("alter-generation", 1))
			require.NoError(t, err)
			before := mustGetTestSQLTask(t, store, 1)[0]
			require.Zero(t, before.TriggerCount)
			require.Equal(t, uint64(1), before.ScheduleGeneration)

			affected, err := store.AlterSQLTask(ctx, before, before, SQLTaskSuspend)
			require.NoError(t, err)
			require.Equal(t, 1, affected)
			suspended := mustGetTestSQLTask(t, store, 1)[0]
			require.False(t, suspended.Enabled)
			require.Equal(t, uint64(2), suspended.TriggerCount)
			require.Equal(t, uint64(2), suspended.ScheduleGeneration)

			staleResume := before
			staleResume.NextFireTime = 100
			affected, err = store.AlterSQLTask(ctx, before, staleResume, SQLTaskResume)
			require.NoError(t, err)
			require.Zero(t, affected)

			staleGate := before
			staleGate.GateCondition = "new gate"
			affected, err = store.AlterSQLTask(ctx, before, staleGate, SQLTaskSetGate)
			require.NoError(t, err)
			require.Equal(t, 1, affected)
			got := mustGetTestSQLTask(t, store, 1)[0]
			require.False(t, got.Enabled)
			require.Equal(t, suspended.ScheduleGeneration, got.ScheduleGeneration)
			require.Equal(t, "new gate", got.GateCondition)
		})
	}
}

func TestSQLTaskRunBarrierSurvivesAgeAndRecreation(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := factory(t)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			definition := newTestSQLTask("long-lived", 1)
			definition.TimeoutSeconds = 0
			_, err := store.AddSQLTask(ctx, definition)
			require.NoError(t, err)
			definition = mustGetTestSQLTask(t, store, 1)[0]
			oldRun := newTestSQLTaskRun(definition.TaskID, definition.TaskName, SQLTaskStatusRunning)
			oldRun.TriggerType = SQLTaskTriggerManual
			oldRun.StartedAt = time.Now().Add(-48 * time.Hour)
			oldRun.RunID, err = store.AcquireSQLTaskRun(ctx, definition, oldRun, testSQLTaskAdmission(nil, 0))
			require.NoError(t, err)
			_, err = store.AcquireSQLTaskRun(ctx, definition, oldRun, testSQLTaskAdmission(nil, 0))
			require.ErrorIs(t, err, ErrSQLTaskOverlap)

			_, err = store.DeleteSQLTask(ctx, WithTaskIDCond(EQ, definition.TaskID))
			require.NoError(t, err)
			_, err = store.AddSQLTask(ctx, definition)
			require.NoError(t, err)
			recreated := mustGetTestSQLTask(t, store, 1)[0]
			require.NotEqual(t, definition.TaskID, recreated.TaskID)
			newRun := newTestSQLTaskRun(recreated.TaskID, recreated.TaskName, SQLTaskStatusRunning)
			newRun.TriggerType = SQLTaskTriggerManual
			_, err = store.AcquireSQLTaskRun(ctx, recreated, newRun, testSQLTaskAdmission(nil, 0))
			require.ErrorIs(t, err, ErrSQLTaskOverlap)

			oldRun.Status = SQLTaskStatusSuccess
			resolved, err := store.PublishSQLTaskReceipt(ctx, SQLTaskReceiptRequest{Kind: SQLTaskReceiptTerminal, Run: oldRun})
			require.NoError(t, err)
			require.True(t, resolved)
			_, err = store.AcquireSQLTaskRun(ctx, recreated, newRun, testSQLTaskAdmission(nil, 0))
			require.NoError(t, err)
		})
	}
}

func TestDeleteSQLTaskCleansQueuedAsyncChildren(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, s.Close())
			}()

			sqlTask := newTestSQLTask("task-delete-queued", 1)
			sqlTask.TriggerCount = 0
			mustAddTestSQLTask(t, s, 1, sqlTask)
			sqlTask = mustGetTestSQLTask(t, s, 1)[0]
			sqlTask.TriggerCount++

			parentTaskID := fmt.Sprintf("sql-task:%d", sqlTask.TaskID)
			asyncTask := newTestAsyncTask(parentTaskID + ":1")
			asyncTask.ParentTaskID = parentTaskID
			affected, err := s.TriggerSQLTask(context.Background(), sqlTask, asyncTask)
			require.NoError(t, err)
			require.Equal(t, 2, affected)
			mustGetTestAsyncTask(t, s, 1, WithTaskParentTaskIDCond(EQ, parentTaskID))
			mustGetTestSQLTaskRun(t, s, 0, WithTaskIDCond(EQ, sqlTask.TaskID))

			unrelated := newTestAsyncTask("unrelated")
			unrelated.ParentTaskID = "sql-task:999999"
			mustAddTestAsyncTask(t, s, 1, unrelated)

			mustDeleteTestSQLTask(t, s, 1, WithTaskIDCond(EQ, sqlTask.TaskID))
			mustGetTestSQLTask(t, s, 0, WithTaskIDCond(EQ, sqlTask.TaskID))
			mustGetTestAsyncTask(t, s, 0, WithTaskParentTaskIDCond(EQ, parentTaskID))
			mustGetTestAsyncTask(t, s, 1, WithTaskParentTaskIDCond(EQ, unrelated.ParentTaskID))
			mustGetTestSQLTaskRun(t, s, 0, WithTaskIDCond(EQ, sqlTask.TaskID))

			_, err = s.AcquireSQLTaskRun(context.Background(), sqlTask,
				newTestSQLTaskRun(sqlTask.TaskID, sqlTask.TaskName, SQLTaskStatusRunning), testSQLTaskAdmission(nil, 0))
			require.ErrorIs(t, err, ErrSQLTaskNotFound)
		})
	}
}

func TestAcquireAndCompleteSQLTaskRun(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, s.Close())
			}()

			sqlTask := newTestSQLTask("task-1", 1)
			mustAddTestSQLTask(t, s, 1, sqlTask)
			sqlTask = mustGetTestSQLTask(t, s, 1)[0]

			run := newTestSQLTaskRun(sqlTask.TaskID, sqlTask.TaskName, SQLTaskStatusRunning)
			run.StartedAt = time.Now()
			run.ScheduledAt = run.StartedAt.Add(-time.Minute)
			admitted := run
			runID, err := s.AcquireSQLTaskRun(context.Background(), sqlTask, run, testSQLTaskAdmission(nil, 0))
			require.NoError(t, err)
			require.NotZero(t, runID)

			_, err = s.AcquireSQLTaskRun(context.Background(), sqlTask, run, testSQLTaskAdmission(nil, 0))
			require.ErrorIs(t, err, ErrSQLTaskOverlap)

			run.RunID = runID
			run.Status = SQLTaskStatusFailed
			run.FinishedAt = time.Now()
			run.DurationSeconds = 1.25
			run.RowsAffected, run.ErrorCode, run.ErrorMessage, run.GateResult = 3, 7, "recorded detail", false
			// Completion is allowed to change only the outcome, not the admitted
			// execution identity or its original scheduling/start timestamps.
			run.StartedAt = run.StartedAt.Add(time.Hour)
			run.ScheduledAt = run.ScheduledAt.Add(time.Hour)
			run.TriggerType = SQLTaskTriggerScheduled
			resolved, err := s.PublishSQLTaskReceipt(context.Background(), SQLTaskReceiptRequest{Kind: SQLTaskReceiptTerminal, Run: run})
			require.NoError(t, err)
			require.True(t, resolved)

			runs := mustGetTestSQLTaskRun(t, s, 1, WithSQLTaskRunIDCond(EQ, runID))
			require.Equal(t, SQLTaskStatusFailed, runs[0].Status)
			require.Equal(t, admitted.StartedAt, runs[0].StartedAt)
			require.Equal(t, admitted.ScheduledAt, runs[0].ScheduledAt)
			require.Equal(t, admitted.TriggerType, runs[0].TriggerType)
			require.Equal(t, run.FinishedAt, runs[0].FinishedAt)
			require.Equal(t, run.DurationSeconds, runs[0].DurationSeconds)
			require.Equal(t, run.RowsAffected, runs[0].RowsAffected)
			require.Equal(t, run.ErrorCode, runs[0].ErrorCode)
			require.Equal(t, run.ErrorMessage, runs[0].ErrorMessage)
			require.Equal(t, run.GateResult, runs[0].GateResult)

			run2 := newTestSQLTaskRun(sqlTask.TaskID, sqlTask.TaskName, SQLTaskStatusRunning)
			run2.StartedAt = time.Now()
			runID2, err := s.AcquireSQLTaskRun(context.Background(), sqlTask, run2, testSQLTaskAdmission(nil, 0))
			require.NoError(t, err)
			require.NotEqual(t, runID, runID2)
		})
	}
}

func TestSQLTaskTimeoutDoesNotProveRunStopped(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			store := factory(t)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			definition := newTestSQLTask("timed-task", 1)
			definition.TimeoutSeconds = 1
			mustAddTestSQLTask(t, store, 1, definition)
			definition = mustGetTestSQLTask(t, store, 1)[0]
			run := newTestSQLTaskRun(definition.TaskID, definition.TaskName, SQLTaskStatusRunning)
			run.StartedAt = time.Now().Add(-10 * time.Minute)
			mustAddTestSQLTaskRun(t, store, 1, run)
			_, err := store.AcquireSQLTaskRun(context.Background(), definition, run, testSQLTaskAdmission(nil, 0))
			require.ErrorIs(t, err, ErrSQLTaskOverlap)
			runs := mustGetTestSQLTaskRun(t, store, 1, WithTaskIDCond(EQ, definition.TaskID))
			require.Equal(t, SQLTaskStatusRunning, runs[0].Status)
			require.Empty(t, runs[0].ErrorMessage)
		})
	}
}

func mustGetTestAsyncTask(t *testing.T, s TaskStorage, expectCount int, conds ...Condition) []task.AsyncTask {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tasks, err := s.QueryAsyncTask(ctx, conds...)
	require.NoError(t, err)
	require.Equal(t, expectCount, len(tasks))
	return tasks
}

func mustAddTestAsyncTask(t *testing.T, s TaskStorage, expectAdded int, tasks ...task.AsyncTask) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	n, err := s.AddAsyncTask(ctx, tasks...)
	require.NoError(t, err)
	require.Equal(t, expectAdded, n)
}

func mustUpdateTestAsyncTask(t *testing.T, s TaskStorage, expectUpdated int, tasks []task.AsyncTask, conds ...Condition) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	n, err := s.UpdateAsyncTask(ctx, tasks, conds...)
	require.NoError(t, err)
	require.Equal(t, expectUpdated, n)
}

func mustDeleteTestAsyncTask(t *testing.T, s TaskStorage, expectUpdated int, conds ...Condition) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	n, err := s.DeleteAsyncTask(ctx, conds...)
	require.NoError(t, err)
	require.Equal(t, expectUpdated, n)
}

func mustAddTestCronTask(t *testing.T, s TaskStorage, expectAdded int, tasks ...task.CronTask) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000)
	defer cancel()

	n, err := s.AddCronTask(ctx, tasks...)
	require.NoError(t, err)
	require.Equal(t, expectAdded, n)
}

func mustQueryTestCronTask(t *testing.T, s TaskStorage, expectQueryCount int) []task.CronTask {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	tasks, err := s.QueryCronTask(ctx)
	require.NoError(t, err)
	require.Equal(t, expectQueryCount, len(tasks))
	return tasks
}

func mustAddTestDaemonTask(t *testing.T, s TaskStorage, expectAdded int, tasks ...task.DaemonTask) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	n, err := s.AddDaemonTask(ctx, tasks...)
	require.NoError(t, err)
	require.Equal(t, expectAdded, n)
}

func mustGetTestDaemonTask(t *testing.T, s TaskStorage, expectCount int, conds ...Condition) []task.DaemonTask {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tasks, err := s.QueryDaemonTask(ctx, conds...)
	require.NoError(t, err)
	require.Equal(t, expectCount, len(tasks))
	return tasks
}

func mustUpdateTestDaemonTask(t *testing.T, s TaskStorage, expectUpdated int, tasks []task.DaemonTask, conds ...Condition) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	n, err := s.UpdateDaemonTask(ctx, tasks, conds...)
	require.NoError(t, err)
	require.Equal(t, expectUpdated, n)
}

func mustAddTestSQLTask(t *testing.T, s TaskStorage, expectAdded int, tasks ...SQLTask) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	n, err := s.AddSQLTask(ctx, tasks...)
	require.NoError(t, err)
	require.Equal(t, expectAdded, n)
}

func mustGetTestSQLTask(t *testing.T, s TaskStorage, expectCount int, conds ...Condition) []SQLTask {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tasks, err := s.QuerySQLTask(ctx, conds...)
	require.NoError(t, err)
	require.Equal(t, expectCount, len(tasks))
	return tasks
}

func mustDeleteTestSQLTask(t *testing.T, s TaskStorage, expectDeleted int, conds ...Condition) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	n, err := s.DeleteSQLTask(ctx, conds...)
	require.NoError(t, err)
	require.Equal(t, expectDeleted, n)
}

func mustAddTestSQLTaskRun(t *testing.T, s TaskStorage, expectAdded int, runs ...SQLTaskRun) {
	t.Helper()
	if holder, ok := s.(*refreshableTaskStorage); ok {
		holder.mu.RLock()
		defer holder.mu.RUnlock()
		s = holder.mu.store
	}
	store, ok := s.(*memTaskStorage)
	require.True(t, ok)
	require.Equal(t, expectAdded, len(runs))
	store.Lock()
	defer store.Unlock()
	for _, run := range runs {
		run.RunID = store.nextIDLocked()
		store.sqlTaskRuns[run.RunID] = run
	}
}

func mustGetTestSQLTaskRun(t *testing.T, s TaskStorage, expectCount int, conds ...Condition) []SQLTaskRun {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	runs, err := s.QuerySQLTaskRun(ctx, conds...)
	require.NoError(t, err)
	require.Equal(t, expectCount, len(runs))
	return runs
}

func mustGetLatestTestSQLTaskRun(t *testing.T, s TaskStorage, accountID uint32, taskIDs []uint64, expectCount int) []SQLTaskRun {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	runs, err := s.QueryLatestSQLTaskRun(ctx, accountID, taskIDs)
	require.NoError(t, err)
	require.Equal(t, expectCount, len(runs))
	return runs
}

func mustDeleteTestDaemonTask(t *testing.T, s TaskStorage, expectUpdated int, conds ...Condition) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	n, err := s.DeleteDaemonTask(ctx, conds...)
	require.NoError(t, err)
	require.Equal(t, expectUpdated, n)
}

func newTestAsyncTask(id string) task.AsyncTask {
	v := task.AsyncTask{}
	v.Metadata.ID = id
	v.TaskRunner = id
	return v
}

func newTestCronTask(id, cron string) task.CronTask {
	v := task.CronTask{}
	v.Metadata.ID = id
	v.CronExpr = cron
	return v
}

func newTestDaemonTask(id uint64, mid string) task.DaemonTask {
	v := task.DaemonTask{}
	v.ID = id
	v.Metadata.ID = mid
	v.TaskRunner = mid
	return v
}

func newTestSQLTask(name string, accountID uint32) SQLTask {
	return SQLTask{
		TaskName:           name,
		AccountID:          accountID,
		DatabaseName:       "db1",
		CronExpr:           "0 * * * *",
		Timezone:           "UTC",
		SQLBody:            "select 1",
		GateCondition:      "select 1",
		Enabled:            true,
		TriggerCount:       0,
		ScheduleGeneration: 1,
		NextFireTime:       10,
		RetryLimit:         1,
		TimeoutSeconds:     30,
		Creator:            "root",
	}
}

func newTestSQLTaskRun(taskID uint64, taskName, status string) SQLTaskRun {
	return SQLTaskRun{
		TaskID:        taskID,
		TaskName:      taskName,
		AccountID:     1,
		Status:        status,
		TriggerType:   SQLTaskTriggerManual,
		AttemptNumber: 1,
		GateResult:    true,
		RunnerCN:      "cn1",
	}
}

func testSQLTaskAdmission(claim *SQLTaskClaim, sequence uint64) *SQLTaskRunAdmission {
	admission := &SQLTaskRunAdmission{TriggerSequence: sequence, Reserve: func(uint32, string) bool { return true }}
	if claim != nil {
		admission.AsyncTaskID, admission.RunnerCN, admission.Epoch = claim.ID, claim.Runner, claim.Epoch
		admission.ParentTaskID, admission.MetadataID = claim.ParentTaskID, claim.MetadataID
	}
	return admission
}

func TestAlterSQLTaskCanceledBeforeMutation(t *testing.T) {
	store := NewMemTaskStorage()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	mustAddTestSQLTask(t, store, 1, newTestSQLTask("canceled-alter", 1))
	original := mustGetTestSQLTask(t, store, 1)[0]
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := store.AlterSQLTask(ctx, original, original, SQLTaskSuspend)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, original, mustGetTestSQLTask(t, store, 1)[0])
}
