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
