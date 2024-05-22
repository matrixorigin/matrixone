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

			tasks[0].Metadata.Context = []byte{1}
			mustUpdateTestDaemonTask(t, s, 0, tasks, WithTaskIDCond(EQ, tasks[0].ID+1))
			mustUpdateTestDaemonTask(t, s, 1, tasks, WithTaskIDCond(EQ, tasks[0].ID))
			tasks[0].Metadata.Context = []byte{1, 2}
			mustUpdateTestDaemonTask(t, s, 1, tasks, WithTaskIDCond(GT, 0))
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

			mustAddTestDaemonTask(t, s, 1, newTestDaemonTask(1, "t1"))
			mustAddTestDaemonTask(t, s, 1, newTestDaemonTask(2, "t2"))
			mustAddTestDaemonTask(t, s, 1, newTestDaemonTask(3, "t3"))
			tasks := mustGetTestDaemonTask(t, s, 3)

			mustGetTestDaemonTask(t, s, 1, WithLimitCond(1))
			mustGetTestDaemonTask(t, s, 1, WithTaskRunnerCond(EQ, "t1"))
			mustGetTestDaemonTask(t, s, 2, WithTaskIDCond(GT, tasks[0].ID))
			mustGetTestDaemonTask(t, s, 3, WithTaskIDCond(GE, tasks[0].ID))
			mustGetTestDaemonTask(t, s, 3, WithTaskIDCond(LE, tasks[2].ID))
			mustGetTestDaemonTask(t, s, 2, WithTaskIDCond(LT, tasks[2].ID))
			mustGetTestDaemonTask(t, s, 1, WithLimitCond(1), WithTaskIDCond(GT, tasks[0].ID))
			mustGetTestDaemonTask(t, s, 1, WithTaskIDCond(EQ, tasks[0].ID))
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
