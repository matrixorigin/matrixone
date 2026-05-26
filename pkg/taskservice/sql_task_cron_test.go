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
	"github.com/stretchr/testify/require"
)

func TestScheduleSQLTaskCatchUpPersistsTriggerState(t *testing.T) {
	oldFetchInterval := fetchInterval
	fetchInterval = 100 * time.Millisecond
	t.Cleanup(func() {
		fetchInterval = oldFetchInterval
	})

	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store).(*taskService)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	sqlTask := newTestSQLTask("task-scheduled", 1)
	sqlTask.CronExpr = "0 0 0 1 1 *"
	sqlTask.Timezone = "UTC"
	sqlTask.NextFireTime = time.Now().Add(-time.Minute).UnixMilli()
	mustAddTestSQLTask(t, store, 1, sqlTask)
	sqlTask = mustGetTestSQLTask(t, store, 1, WithTaskName(EQ, "task-scheduled"))[0]

	ts.StartScheduleSQLTask()
	defer ts.StopScheduleSQLTask()

	waitHasTasks(t, store, 5*time.Second, WithTaskParentTaskIDCond(EQ, fmt.Sprintf("sql-task:%d", sqlTask.TaskID)))

	updated := mustGetTestSQLTask(t, store, 1, WithTaskIDCond(EQ, sqlTask.TaskID))[0]
	require.Equal(t, uint64(1), updated.TriggerCount)
	require.Greater(t, updated.NextFireTime, sqlTask.NextFireTime)

	runs, err := store.QueryAsyncTask(context.Background(), WithTaskParentTaskIDCond(EQ, fmt.Sprintf("sql-task:%d", sqlTask.TaskID)))
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.Equal(t, "sql-task:1:1", runs[0].Metadata.ID)
}

func TestScheduleSQLTaskSkipsManualOnlyTask(t *testing.T) {
	oldFetchInterval := fetchInterval
	fetchInterval = 100 * time.Millisecond
	t.Cleanup(func() {
		fetchInterval = oldFetchInterval
	})

	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store).(*taskService)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	sqlTask := newTestSQLTask("task-manual", 1)
	sqlTask.CronExpr = ""
	sqlTask.NextFireTime = 0
	mustAddTestSQLTask(t, store, 1, sqlTask)

	ts.StartScheduleSQLTask()
	defer ts.StopScheduleSQLTask()

	time.Sleep(500 * time.Millisecond)
	tasks, err := store.QueryAsyncTask(context.Background())
	require.NoError(t, err)
	require.Empty(t, tasks)
}

func TestScheduleSQLTaskStartStopNoop(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store).(*taskService)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	ts.StopScheduleSQLTask()
	ts.StartScheduleSQLTask()
	require.NotNil(t, ts.sqlCrons.stopper)
	require.NotNil(t, ts.sqlCrons.jobs)

	ts.StartScheduleSQLTask()
	ts.StopScheduleSQLTask()
	require.Nil(t, ts.sqlCrons.stopper)
	require.Nil(t, ts.sqlCrons.jobs)

	ts.StopScheduleSQLTask()
}

func TestLoadSQLTasksAddsReplacesAndRemoves(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store).(*taskService)
	defer func() {
		for id := range ts.sqlCrons.jobs {
			ts.removeSQLTask(id)
		}
		require.NoError(t, ts.Close())
	}()
	ts.sqlCrons.jobs = make(map[uint64]*sqlTaskCronJob)

	sqlTask := newTestSQLTask("task-refresh", 1)
	sqlTask.CronExpr = "0 0 0 1 1 *"
	sqlTask.Timezone = "UTC"
	sqlTask.NextFireTime = time.Now().Add(time.Hour).UnixMilli()
	mustAddTestSQLTask(t, store, 1, sqlTask)
	sqlTask = mustGetTestSQLTask(t, store, 1, WithTaskName(EQ, "task-refresh"))[0]

	ts.loadSQLTasks(context.Background())
	require.Len(t, ts.sqlCrons.jobs, 1)
	require.Equal(t, sqlTask.CronExpr, ts.sqlCrons.jobs[sqlTask.TaskID].task.CronExpr)

	sqlTask.CronExpr = "0 */2 * * * *"
	mustUpdateTestSQLTask(t, store, 1, []SQLTask{sqlTask}, WithTaskIDCond(EQ, sqlTask.TaskID))
	ts.loadSQLTasks(context.Background())
	require.Len(t, ts.sqlCrons.jobs, 1)
	require.Equal(t, "0 */2 * * * *", ts.sqlCrons.jobs[sqlTask.TaskID].task.CronExpr)

	sqlTask.Enabled = false
	mustUpdateTestSQLTask(t, store, 1, []SQLTask{sqlTask}, WithTaskIDCond(EQ, sqlTask.TaskID))
	ts.loadSQLTasks(context.Background())
	require.Empty(t, ts.sqlCrons.jobs)
}

func TestSQLTaskCronJobEdges(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store).(*taskService)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	_, err := newSQLTaskCronJob(SQLTask{CronExpr: "bad cron"}, ts)
	require.Error(t, err)

	oldTask := newTestSQLTask("task-old", 1)
	newTask := oldTask
	require.False(t, sqlTaskNeedsRefresh(oldTask, newTask))
	newTask.Timezone = "Asia/Shanghai"
	require.True(t, sqlTaskNeedsRefresh(oldTask, newTask))

	job := &sqlTaskCronJob{s: ts, task: SQLTask{TaskID: 999}}
	job.Run()

	sqlTask := newTestSQLTask("task-disabled", 1)
	sqlTask.Enabled = false
	sqlTask.CronExpr = "0 * * * * *"
	mustAddTestSQLTask(t, store, 1, sqlTask)
	sqlTask = mustGetTestSQLTask(t, store, 1, WithTaskName(EQ, "task-disabled"))[0]
	job.task = sqlTask
	job.Run()
	tasks, err := store.QueryAsyncTask(context.Background())
	require.NoError(t, err)
	require.Empty(t, tasks)

	sqlTask.Enabled = true
	sqlTask.CronExpr = "bad cron"
	mustUpdateTestSQLTask(t, store, 1, []SQLTask{sqlTask}, WithTaskIDCond(EQ, sqlTask.TaskID))
	job.task = sqlTask
	job.Run()
	tasks, err = store.QueryAsyncTask(context.Background())
	require.NoError(t, err)
	require.Empty(t, tasks)
}
