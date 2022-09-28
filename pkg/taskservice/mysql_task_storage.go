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

	_ "github.com/go-sql-driver/mysql"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

var (
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
			task_status                 int,
			task_runner                 varchar(16),
			task_epoch                  int,
			last_heartbeat              bigint(20),
			result_code                 int,
			error_msg                   text,
			create_at                   bigint(20),
			end_at                      bigint(20))`,
		`create table if not exists sys_cron_task (
			cron_task_id				int primary key auto_increment,
    		task_metadata_id            varchar(16) unique not null,
			task_metadata_executor      int,
			task_metadata_context       blob,
			task_max_retry_times        int,
			task_retry_interval         int,
			task_delay_duration         int,
			cron_expr					varchar(100) not null,
			next_time					bigint(20),
			trigger_times				int,
			create_at					bigint(20),
			update_at					bigint(20))`,
	}

	insertAsyncTask = `insert into sys_async_task(
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
                           last_heartbeat,
                           create_at,
                           end_at
					) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	updateAsyncTask = `update sys_async_task set 
							task_metadata_executor=?,
							task_metadata_context=?,
							task_max_retry_times=?,
							task_retry_interval=?,
							task_delay_duration=?,
							task_parent_id=?,
							task_status=?,
							task_runner=?,
							task_epoch=?,
							last_heartbeat=?,
							result_code=?,
							error_msg=?,
							create_at=?,
							end_at=? where task_id=?`

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
							last_heartbeat,
							result_code,
							error_msg,
							create_at,
							end_at 
						from sys_async_task`

	insertCronTask = `insert into sys_cron_task (
                           task_metadata_id,
						   task_metadata_executor,
                           task_metadata_context,
                           task_max_retry_times,
                           task_retry_interval,
                           task_delay_duration,
                           cron_expr,
                           next_time,
                           trigger_times,
                           create_at,
                           update_at
                    ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	selectCronTask = `select 
    						cron_task_id,
    						task_metadata_id,
    						task_metadata_executor,
    						task_metadata_context,
    						task_max_retry_times,
    						task_retry_interval,
    						task_delay_duration,
    						cron_expr,
    						next_time,
    						trigger_times,
    						create_at,
    						update_at
						from sys_cron_task`

	updateCronTask = `update sys_cron_task set 
							task_metadata_executor=?,
    						task_metadata_context=?,
    						task_max_retry_times=?,
    						task_retry_interval=?,
    						task_delay_duration=?,
    						cron_expr=?,
    						next_time=?,
    						trigger_times=?,
    						create_at=?,
    						update_at=? where cron_task_id=?`
)

// used for testing
type mysqlTaskStorage struct {
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
	n := 0
	for _, t := range tasks {
		if m.taskExists(t.Metadata.ID) {
			continue
		}
		exec, err := m.db.Exec(
			insertAsyncTask,
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
			t.LastHeartbeat,
			t.CreateAt,
			t.CompletedAt,
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

	where := buildWhereClause(c)
	var update string
	if where != "" {
		update = updateAsyncTask + " and " + where
	} else {
		update = updateAsyncTask
	}
	n := 0
	for _, t := range tasks {
		execResult := &task.ExecuteResult{
			Code:  0,
			Error: "",
		}
		if t.ExecuteResult != nil {
			execResult.Code = t.ExecuteResult.Code
			execResult.Error = t.ExecuteResult.Error
		}

		exec, err := m.db.Exec(update,
			t.Metadata.Executor,
			t.Metadata.Context,
			t.Metadata.Options.MaxRetryTimes,
			t.Metadata.Options.RetryInterval,
			t.Metadata.Options.DelayDuration,
			t.ParentTaskID,
			t.Status,
			t.TaskRunner,
			t.Epoch,
			t.LastHeartbeat,
			execResult.Code,
			execResult.Error,
			t.CreateAt,
			t.CompletedAt,
			t.ID,
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
	where := buildWhereClause(c)

	exec, err := m.db.Exec("delete from sys_async_task where " + where)
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

	where := buildWhereClause(c)
	var query string
	if where != "" {
		query = selectAsyncTask + " where " + where
	} else {
		query = selectAsyncTask
	}
	query += buildLimitClause(c)

	rows, err := m.db.Query(query)
	defer func(rows *sql.Rows) {
		if rows == nil {
			return
		}
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
		var codeOption sql.NullInt32
		var msgOption sql.NullString
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
			&t.LastHeartbeat,
			&codeOption,
			&msgOption,
			&t.CreateAt,
			&t.CompletedAt,
		)
		if err != nil {
			return nil, err
		}
		if codeOption.Valid {
			t.ExecuteResult = &task.ExecuteResult{}
			code, err := codeOption.Value()
			if err != nil {
				return nil, err
			}
			t.ExecuteResult.Code = task.ResultCode(code.(int32))

			msg, err := msgOption.Value()
			if err != nil {
				return nil, err
			}
			t.ExecuteResult.Error = msg.(string)
		}

		tasks = append(tasks, t)
	}
	return tasks, nil
}

func (m *mysqlTaskStorage) AddCronTask(ctx context.Context, cronTask ...task.CronTask) (int, error) {
	n := 0
	for _, t := range cronTask {
		if m.cronTaskExists(t.Metadata.ID) {
			continue
		}

		exec, err := m.db.Exec(
			insertCronTask,
			t.Metadata.ID,
			t.Metadata.Executor,
			t.Metadata.Context,
			t.Metadata.Options.MaxRetryTimes,
			t.Metadata.Options.RetryInterval,
			t.Metadata.Options.DelayDuration,
			t.CronExpr,
			t.NextTime,
			t.TriggerTimes,
			t.CreateAt,
			t.UpdateAt,
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

func (m *mysqlTaskStorage) QueryCronTask(ctx context.Context) ([]task.CronTask, error) {
	rows, err := m.db.Query(selectCronTask)
	defer func(rows *sql.Rows) {
		if rows == nil {
			return
		}
		err := rows.Close()
		if err != nil {
			return
		}
	}(rows)
	if err != nil {
		return nil, err
	}

	tasks := make([]task.CronTask, 0)

	for rows.Next() {
		var t task.CronTask
		err := rows.Scan(
			&t.ID,
			&t.Metadata.ID,
			&t.Metadata.Executor,
			&t.Metadata.Context,
			&t.Metadata.Options.MaxRetryTimes,
			&t.Metadata.Options.RetryInterval,
			&t.Metadata.Options.DelayDuration,
			&t.CronExpr,
			&t.NextTime,
			&t.TriggerTimes,
			&t.CreateAt,
			&t.UpdateAt,
		)
		if err != nil {
			return nil, err
		}

		tasks = append(tasks, t)
	}

	return tasks, nil
}

func (m *mysqlTaskStorage) UpdateCronTask(ctx context.Context, cronTask task.CronTask, t task.Task) (int, error) {
	if err := m.preUpdateCron(); err != nil {
		return 0, err
	}

	if m.taskExists(t.Metadata.ID) {
		return 0, nil
	}
	triggerTimes, err := m.getTriggerTimes(cronTask.Metadata.ID)
	if err == sql.ErrNoRows || triggerTimes != cronTask.TriggerTimes-1 {
		return 0, nil
	}

	update, err := m.Add(ctx, t)
	if err != nil {
		return 0, err
	}
	exec, err := m.db.Exec(updateCronTask,
		cronTask.Metadata.Executor,
		cronTask.Metadata.Context,
		cronTask.Metadata.Options.MaxRetryTimes,
		cronTask.Metadata.Options.RetryInterval,
		cronTask.Metadata.Options.DelayDuration,
		cronTask.CronExpr,
		cronTask.NextTime,
		cronTask.TriggerTimes,
		cronTask.CreateAt,
		cronTask.UpdateAt,
		cronTask.ID)
	if err != nil {
		return 0, err
	}
	affected, err := exec.RowsAffected()
	if err != nil {
		panic(err)
	}

	return update + int(affected), nil
}

func (m *mysqlTaskStorage) drop(ctx context.Context) error {
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

func (m *mysqlTaskStorage) taskExists(taskMetadataID string) bool {
	var exists bool
	err := m.db.QueryRow("select exists(select * from sys_async_task where task_metadata_id=?)", taskMetadataID).Scan(&exists)
	if err != nil {
		panic(err)
	}
	return exists
}

func (m *mysqlTaskStorage) getTriggerTimes(taskMetadataID string) (uint64, error) {
	var triggerTimes uint64
	err := m.db.QueryRow("select trigger_times from sys_cron_task where task_metadata_id=?", taskMetadataID).Scan(&triggerTimes)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, err
		}
		panic(err)
	}
	return triggerTimes, nil
}

func (m *mysqlTaskStorage) cronTaskExists(taskMetadataID string) bool {
	var exists bool
	err := m.db.QueryRow("select exists(select * from sys_cron_task where task_metadata_id=?)", taskMetadataID).Scan(&exists)
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
		clause += fmt.Sprintf("task_runner%s'%s'", OpName[c.taskRunnerOp], c.taskRunner)
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
		clause += fmt.Sprintf("task_parent_id%s'%s'", OpName[c.taskParentTaskIDOp], c.taskParentTaskID)
	}

	return clause
}

func buildLimitClause(c conditions) string {
	if c.limit != 0 {
		return fmt.Sprintf(" limit %d", c.limit)
	}
	return ""
}
