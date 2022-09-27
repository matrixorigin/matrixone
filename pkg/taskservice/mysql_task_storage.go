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
	"database/sql"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

var (
	sysWantedTables = map[string]struct{}{
		"sys_async_task": {},
		"sys_cron_task":  {},
	}

	createSqls = []string{
		`create table if not exists sys_async_task (
			task_id                     int primary key auto_increment,
			task_metadata_id            varchar(16) unique not null,
			task_metadata_executor      int,
			task_metadata_context       blob,
			task_max_retry_times        int,
			task_retry_interval         int,
			task_delay_duration         int,
			task_parent_id              varchar(16),
			task_status                 int not null,
			task_runner                 varchar(16),
			task_epoch                  int,
			task_execute_after          bigint(20),
			last_heartbeat              bigint(20),
			result_code                 int,
			error_msg                   text,
			create_at                   bigint(20),
			end_at                      bigint(20),
			constraint tsk_prnt_fr foreign key (task_parent_id) references sys_async_task(task_metadata_id))`,
		`create table if not exists sys_cron_task (
			cron_task_id				int primary key auto_increment,
    		task_metadata_id            varchar(16) unique not null,
			task_metadata_executor      int,
			task_metadata_context       blob,
			task_max_retry_times        int,
			task_retry_interval         int,
			task_delay_duration         int,
			cron_expr					varchar(100) not null,
			next_time					bigint(20))`,
	}

	addAsyncTask = `insert into sys_async_task(
                           task_metadata_id,
                           task_metadata_context,
                           task_max_retry_times,
                           task_retry_interval,
                           task_delay_duration,
                           task_parent_id,
                           task_status,
                           create_at,
					) values (?, ?, ?, ?, ?, ?, ?, ?)`

	updateAsyncTask = `update sys_async_task set 
                        	task_id=?,
							task_metadata_id=?,
							task_metadata_executor=?,
							task_metadata_context=?,
							task_max_retry_times=?,
							task_retry_interval=?,
							task_delay_duration=?,
							task_parent_id=?,
							task_status=?,
							task_runner=?,
							task_epoch=?,
							task_execute_after=?,
							last_heartbeat=?,
							result_code=?,
							error_msg=?,
							create_at=?,
							end_at=? where `

	selectAsyncTask = `select 
    						task_id,
							task_metadata_id,
							task_metadata_executor,
							task_metadata_context,
							task_max_retry_times,
							task_retry_interval,
							task_delay_duration,
							task_parent_id,
							task_status,
							task_runner,
							task_epoch,
							task_execute_after,
							last_heartbeat,
							result_code,
							error_msg,
							create_at,
							end_at 
						from sys_async_task`

	insertCronTask = `insert into sys_cron_task (
                           task_metadata_id,
                           task_metadata_context,
                           task_max_retry_times,
                           task_retry_interval,
                           task_delay_duration,
                           cron_expr,
                           next_time
                    ) values (?, ?, ?, ?, ?, ?, ?)`
)

// used for testing
type mysqlTaskStorage struct {
	sync.RWMutex

	db *sql.DB

	// Used for testing. Make some changes to the data before updating.
	preUpdate     func()
	preUpdateCron func() error
}

func NewMysqlTaskStorage(driver, dsn string) TaskStorage {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		panic(err)
	}

	for _, s := range createSqls {
		_, err = db.Exec(s)
		if err != nil {
			panic(err)
		}
	}

	return &mysqlTaskStorage{
		db: db,

		preUpdateCron: func() error { return nil },
		preUpdate:     func() {},
	}
}

func (m *mysqlTaskStorage) Close() error {
	err := m.db.Close()
	if err != nil {
		return err
	}
	return nil
}

func (m *mysqlTaskStorage) Add(ctx context.Context, tasks ...task.Task) (int, error) {
	m.Lock()
	defer m.Unlock()

	n := 0
	for _, t := range tasks {
		if m.taskExists(t.Metadata.ID) {
			continue
		}

		exec, err := m.db.Exec(
			addAsyncTask,
			t.Metadata.ID,
			t.Metadata.Context,
			t.Metadata.Options.MaxRetryTimes,
			t.Metadata.Options.RetryInterval,
			t.Metadata.Options.DelayDuration,
			t.ParentTaskID,
			t.Status,
			t.CreateAt,
		)
		if err != nil {
			return n, err
		}
		affected, err := exec.RowsAffected()
		if err != nil {
			panic(err)
		}
		n += int(affected)
	}

	return n, nil
}

func (m *mysqlTaskStorage) Update(ctx context.Context, tasks []task.Task, condition ...Condition) (int, error) {
	if m.preUpdate != nil {
		m.preUpdate()
	}

	c := conditions{}
	for _, cond := range condition {
		cond(&c)
	}

	clause := buildWhereClause(c)
	if clause == "" {
		return 0, moerr.NewInternalError("cannot update tasks without conditions")
	}
	update := updateAsyncTask + clause

	m.Lock()
	defer m.Unlock()
	n := 0
	for _, t := range tasks {
		exec, err := m.db.Exec(update,
			t.ID,
			t.Metadata.ID,
			t.Metadata.Executor,
			t.Metadata.Context,
			t.Metadata.Options.MaxRetryTimes,
			t.Metadata.Options.RetryInterval,
			t.Metadata.Options.DelayDuration,
			t.ParentTaskID,
			t.Status,
			t.TaskRunner,
			t.Epoch,
			nil,
			t.LastHeartbeat,
			t.ExecuteResult.Code,
			t.ExecuteResult.Error,
			t.CreateAt,
			t.CompletedAt,
		)
		if err != nil {
			return 0, err
		}
		affected, err := exec.RowsAffected()
		if err != nil {
			panic(err)
		}
		n += int(affected)
	}
	return n, nil
}

func (m *mysqlTaskStorage) Delete(ctx context.Context, condition ...Condition) (int, error) {
	c := conditions{}
	for _, cond := range condition {
		cond(&c)
	}
	clause := buildWhereClause(c)
	m.Lock()
	defer m.Unlock()
	exec, err := m.db.Exec("delete from sys_async_task where " + clause)
	if err != nil {
		return 0, err
	}
	affected, err := exec.RowsAffected()
	if err != nil {
		panic(err)
	}
	return int(affected), nil
}

func (m *mysqlTaskStorage) Query(ctx context.Context, condition ...Condition) ([]task.Task, error) {
	c := conditions{}
	for _, cond := range condition {
		cond(&c)
	}

	clause := buildWhereClause(c)
	var query string
	if clause != "" {
		query = selectAsyncTask + " where " + clause
	} else {
		query = selectAsyncTask
	}
	rows, err := m.db.Query(query)
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			return
		}
	}(rows)
	if err != nil {
		return nil, err
	}

	tasks := make([]task.Task, 0)

	for rows.Next() {
		var t task.Task
		err := rows.Scan(
			&t.ID,
			&t.Metadata.ID,
			&t.Metadata.Executor,
			&t.Metadata.Context,
			&t.Metadata.Options.MaxRetryTimes,
			&t.Metadata.Options.RetryInterval,
			&t.Metadata.Options.DelayDuration,
			&t.ParentTaskID,
			&t.Status,
			&t.TaskRunner,
			&t.Epoch,
			nil,
			t.LastHeartbeat,
			t.ExecuteResult.Code,
			t.ExecuteResult.Error,
			t.CreateAt,
			t.CompletedAt,
		)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, nil
}

func (m *mysqlTaskStorage) AddCronTask(ctx context.Context, cronTask ...task.CronTask) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mysqlTaskStorage) QueryCronTask(ctx context.Context) ([]task.CronTask, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mysqlTaskStorage) UpdateCronTask(ctx context.Context, cronTask task.CronTask, t task.Task) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mysqlTaskStorage) filter(c conditions, task task.Task) bool {
	ok := true

	if c.hasTaskIDCond {
		switch c.taskIDOp {
		case EQ:
			ok = task.ID == c.taskID
		case GT:
			ok = task.ID > c.taskID
		case GE:
			ok = task.ID >= c.taskID
		case LE:
			ok = task.ID <= c.taskID
		case LT:
			ok = task.ID < c.taskID
		}
	}

	if ok && c.hasTaskRunnerCond {
		switch c.taskRunnerOp {
		case EQ:
			ok = task.TaskRunner == c.taskRunner
		}
	}

	if ok && c.hasTaskStatusCond {
		switch c.taskStatusOp {
		case EQ:
			ok = task.Status == c.taskStatus
		case GT:
			ok = task.Status > c.taskStatus
		case GE:
			ok = task.Status >= c.taskStatus
		case LE:
			ok = task.Status <= c.taskStatus
		case LT:
			ok = task.Status < c.taskStatus
		}
	}

	if ok && c.hasTaskEpochCond {
		switch c.taskEpochOp {
		case EQ:
			ok = task.Epoch == c.taskEpoch
		case GT:
			ok = task.Epoch > c.taskEpoch
		case GE:
			ok = task.Epoch >= c.taskEpoch
		case LE:
			ok = task.Epoch <= c.taskEpoch
		case LT:
			ok = task.Epoch < c.taskEpoch
		}
	}

	if ok && c.hasTaskParentIDCond {
		switch c.taskParentTaskIDOp {
		case EQ:
			ok = task.ParentTaskID == c.taskParentTaskID
		}
	}
	return ok
}

func (m *mysqlTaskStorage) taskExists(taskMetadataID string) bool {
	var exists bool
	err := m.db.QueryRow("select exists(select * from sys_async_task where task_metadata_id=?)", taskMetadataID).Scan(&exists)
	if err != nil {
		panic(err)
	}
	return exists
}

func buildWhereClause(c conditions) string {
	var clause string

	if c.hasTaskIDCond {
		clause = fmt.Sprintf("task_id%s%d", OpName[c.taskIDOp], c.taskID)
	}

	if c.hasTaskRunnerCond {
		if clause != "" {
			clause += " AND "
		}
		clause += fmt.Sprintf("task_runner%s%s", OpName[c.taskRunnerOp], c.taskRunner)
	}

	if c.hasTaskStatusCond {
		if clause != "" {
			clause += " AND "
		}
		clause += fmt.Sprintf("task_status%s%d", OpName[c.taskStatusOp], c.taskStatus)
	}

	if c.hasTaskEpochCond {
		if clause != "" {
			clause += " AND "
		}
		clause += fmt.Sprintf("task_epoch%s%d", OpName[c.taskEpochOp], c.taskEpoch)
	}

	if c.hasTaskParentIDCond {
		if clause != "" {
			clause += " AND "
		}
		clause += fmt.Sprintf("task_parent_id%s%s", OpName[c.taskParentTaskIDOp], c.taskParentTaskID)
	}

	return clause
}
