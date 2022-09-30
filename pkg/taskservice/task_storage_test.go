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

	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	storages = map[string]func(*testing.T) TaskStorage{
		"mem": createMem,
	}
)

func createMem(t *testing.T) TaskStorage {
	return NewMemTaskStorage()
}

//func createMysql(t *testing.T) TaskStorage {
//	storage, err := NewMysqlTaskStorage("mysql", "wzr:1234@/task")
//	require.NoError(t, err)
//	return storage
//}

func dropTable(s TaskStorage) error {
	if m, ok := s.(*mysqlTaskStorage); ok {
		_, err := m.db.Exec("drop table sys_async_task")
		if err != nil {
			return err
		}
		_, err = m.db.Exec("drop table sys_cron_task")
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

func TestAddTask(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, dropTable(s))
				assert.NoError(t, s.Close())
			}()

			v := newTestTask("t1")
			mustAddTestTask(t, s, 1, v)
			mustAddTestTask(t, s, 0, v)
			assert.Equal(t, 1, len(mustGetTestTask(t, s, 1)))
		})
	}
}

func TestUpdateTask(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, dropTable(s))
				assert.NoError(t, s.Close())
			}()

			v := newTestTask("t1")
			mustAddTestTask(t, s, 1, v)

			tasks := mustGetTestTask(t, s, 1)
			tasks[0].Metadata.Executor = 1
			mustUpdateTestTask(t, s, 1, tasks)
		})
	}
}

func TestUpdateTaskWithConditions(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, dropTable(s))
				assert.NoError(t, s.Close())
			}()

			mustAddTestTask(t, s, 1, newTestTask("t1"))
			tasks := mustGetTestTask(t, s, 1)

			mustUpdateTestTask(t, s, 0, tasks, WithTaskRunnerCond(EQ, "t2"))
			mustUpdateTestTask(t, s, 1, tasks, WithTaskRunnerCond(EQ, "t1"))

			tasks[0].Metadata.Context = []byte{1}
			mustUpdateTestTask(t, s, 0, tasks, WithTaskIDCond(EQ, tasks[0].ID+1))
			mustUpdateTestTask(t, s, 1, tasks, WithTaskIDCond(EQ, tasks[0].ID))
			tasks[0].Metadata.Context = []byte{1, 2}
			mustUpdateTestTask(t, s, 1, tasks, WithTaskIDCond(GT, 0))
		})
	}
}

func TestDeleteTaskWithConditions(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, dropTable(s))
				assert.NoError(t, s.Close())
			}()

			mustAddTestTask(t, s, 1, newTestTask("t1"))
			mustAddTestTask(t, s, 1, newTestTask("t2"))
			mustAddTestTask(t, s, 1, newTestTask("t3"))
			tasks := mustGetTestTask(t, s, 3)

			mustDeleteTestTask(t, s, 0, WithTaskRunnerCond(EQ, "t4"))
			mustDeleteTestTask(t, s, 1, WithTaskRunnerCond(EQ, "t1"))

			mustDeleteTestTask(t, s, 0, WithTaskIDCond(EQ, tasks[len(tasks)-1].ID+1))
			mustDeleteTestTask(t, s, 2, WithTaskIDCond(GT, tasks[0].ID))

			mustGetTestTask(t, s, 0)
		})
	}
}

func TestQueryTaskWithConditions(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, dropTable(s))
				assert.NoError(t, s.Close())
			}()

			mustAddTestTask(t, s, 1, newTestTask("t1"))
			mustAddTestTask(t, s, 1, newTestTask("t2"))
			mustAddTestTask(t, s, 1, newTestTask("t3"))
			tasks := mustGetTestTask(t, s, 3)

			mustGetTestTask(t, s, 1, WithLimitCond(1))
			mustGetTestTask(t, s, 1, WithTaskRunnerCond(EQ, "t1"))
			mustGetTestTask(t, s, 2, WithTaskIDCond(GT, tasks[0].ID))
			mustGetTestTask(t, s, 3, WithTaskIDCond(GE, tasks[0].ID))
			mustGetTestTask(t, s, 3, WithTaskIDCond(LE, tasks[2].ID))
			mustGetTestTask(t, s, 2, WithTaskIDCond(LT, tasks[2].ID))
			mustGetTestTask(t, s, 1, WithLimitCond(1), WithTaskIDCond(GT, tasks[0].ID))
			mustGetTestTask(t, s, 1, WithTaskIDCond(EQ, tasks[0].ID))
		})
	}
}

func TestAddAndQueryCronTask(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			defer func() {
				assert.NoError(t, dropTable(s))
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
				assert.NoError(t, dropTable(s))
				assert.NoError(t, s.Close())
			}()

			v1 := newTestCronTask("t1", "cron1")
			v2 := newTestTask("t1-cron-1")
			v3 := newTestTask("t1-cron-2")

			ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
			defer cancel()

			n, err := s.UpdateCronTask(ctx, v1, v2)
			assert.NoError(t, err)
			assert.Equal(t, 0, n)

			mustAddTestCronTask(t, s, 1, v1)
			mustAddTestTask(t, s, 1, v2)

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

func mustGetTestTask(t *testing.T, s TaskStorage, expectCount int, conds ...Condition) []task.Task {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tasks, err := s.Query(ctx, conds...)
	require.NoError(t, err)
	require.Equal(t, expectCount, len(tasks))
	return tasks
}

func mustAddTestTask(t *testing.T, s TaskStorage, expectAdded int, tasks ...task.Task) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	n, err := s.Add(ctx, tasks...)
	require.NoError(t, err)
	require.Equal(t, expectAdded, n)
}

func mustUpdateTestTask(t *testing.T, s TaskStorage, expectUpdated int, tasks []task.Task, conds ...Condition) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	n, err := s.Update(ctx, tasks, conds...)
	require.NoError(t, err)
	require.Equal(t, expectUpdated, n)
}

func mustDeleteTestTask(t *testing.T, s TaskStorage, expectUpdated int, conds ...Condition) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	n, err := s.Delete(ctx, conds...)
	require.NoError(t, err)
	require.Equal(t, expectUpdated, n)
}

func mustAddTestCronTask(t *testing.T, s TaskStorage, expectAdded int, tasks ...task.CronTask) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
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

func newTestTask(id string) task.Task {
	v := task.Task{}
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
