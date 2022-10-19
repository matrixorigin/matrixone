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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"go.uber.org/multierr"
)

var (
	createDB = []string{
		`create database if not exists `,
		`use `,
	}

	createSqls = []string{
		`create table if not exists sys_async_task (
			task_id                     int primary key auto_increment,
			task_metadata_id            varchar(16) unique not null,
			task_metadata_executor      int,
			task_metadata_context       blob,
			task_metadata_option        varchar(1000),
			task_parent_id              varchar(16),
			task_status                 int,
			task_runner                 varchar(16),
			task_epoch                  int,
			last_heartbeat              bigint,
			result_code                 int null,
			error_msg                   varchar(1000) null,
			create_at                   bigint,
			end_at                      bigint)`,
		`create table if not exists sys_cron_task (
			cron_task_id				int primary key auto_increment,
    		task_metadata_id            varchar(16) unique not null,
			task_metadata_executor      int,
			task_metadata_context       blob,
			task_metadata_option 		varchar(1000),
			cron_expr					varchar(100) not null,
			next_time					bigint,
			trigger_times				int,
			create_at					bigint,
			update_at					bigint)`,
	}

	insertAsyncTask = `insert into sys_async_task(
                           task_metadata_id,
                           task_metadata_executor,
                           task_metadata_context,
                           task_metadata_option,
                           task_parent_id,
                           task_status,
                           task_runner,
                           task_epoch,
                           last_heartbeat,
                           create_at,
                           end_at) values `

	updateAsyncTask = `update sys_async_task set 
							task_metadata_executor=?,
							task_metadata_context=?,
							task_metadata_option=?,
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
							task_metadata_option,
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
                           task_metadata_option,
                           cron_expr,
                           next_time,
                           trigger_times,
                           create_at,
                           update_at
                    ) values `

	selectCronTask = `select 
    						cron_task_id,
    						task_metadata_id,
    						task_metadata_executor,
    						task_metadata_context,
    						task_metadata_option,
    						cron_expr,
    						next_time,
    						trigger_times,
    						create_at,
    						update_at
						from sys_cron_task`

	updateCronTask = `update sys_cron_task set 
							task_metadata_executor=?,
    						task_metadata_context=?,
    						task_metadata_option=?,
    						cron_expr=?,
    						next_time=?,
    						trigger_times=?,
    						create_at=?,
    						update_at=? where cron_task_id=?`
)

type mysqlTaskStorage struct {
	dsn string
	db  *sql.DB
}

func NewMysqlTaskStorage(dsn, dbname string) (TaskStorage, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	for _, s := range createDB {
		if _, err = db.Exec(s + dbname); err != nil {
			return nil, multierr.Append(err, db.Close())
		}
	}

	// TODO: Can not init here. Consider how many CNs are started and how many times they will be executed,
	// the initialization logic needs to be moved to the initialization of HaKeeper
	for _, s := range createSqls {
		if _, err = db.Exec(s); err != nil {
			return nil, multierr.Append(err, db.Close())
		}
	}

	db.SetConnMaxIdleTime(time.Second * 10)
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	return &mysqlTaskStorage{
		dsn: dsn,
		db:  db,
	}, nil
}

func (m *mysqlTaskStorage) Close() error {
	return m.db.Close()
}

func (m *mysqlTaskStorage) Add(ctx context.Context, tasks ...task.Task) (int, error) {
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = conn.Close()
	}()
	if len(tasks) == 0 {
		return 0, nil
	}

	sqlStr := insertAsyncTask
	vals := make([]any, 0, len(tasks)*13)

	for _, t := range tasks {
		j, err := json.Marshal(t.Metadata.Options)
		if err != nil {
			return 0, err
		}

		sqlStr += "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),"
		vals = append(vals, t.Metadata.ID,
			t.Metadata.Executor,
			t.Metadata.Context,
			string(j),
			t.ParentTaskID,
			t.Status,
			t.TaskRunner,
			t.Epoch,
			t.LastHeartbeat,
			t.CreateAt,
			t.CompletedAt,
		)
	}

	if sqlStr == insertAsyncTask {
		return 0, nil
	}
	sqlStr = sqlStr[0 : len(sqlStr)-1]
	stmt, err := conn.PrepareContext(ctx, sqlStr)
	if err != nil {
		return 0, err
	}
	exec, err := stmt.Exec(vals...)
	if err != nil {
		dup, err := removeDuplicateTasks(err, tasks)
		if err != nil {
			return 0, err
		}
		add, err := m.Add(ctx, dup...)
		if err != nil {
			return add, err
		}
		return add, nil
	}
	affected, err := exec.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(affected), nil
}

func (m *mysqlTaskStorage) Update(ctx context.Context, tasks []task.Task, condition ...Condition) (int, error) {
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = conn.Close()
	}()

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
		execResult := &task.ExecuteResult{}
		if t.ExecuteResult != nil {
			execResult.Code = t.ExecuteResult.Code
			execResult.Error = t.ExecuteResult.Error
		}

		j, err := json.Marshal(t.Metadata.Options)
		if err != nil {
			return 0, err
		}

		exec, err := conn.ExecContext(ctx, update,
			t.Metadata.Executor,
			t.Metadata.Context,
			string(j),
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
			return 0, nil
		}
		n += int(affected)
	}
	return n, nil
}

func (m *mysqlTaskStorage) Delete(ctx context.Context, condition ...Condition) (int, error) {
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = conn.Close()
	}()

	c := conditions{}
	for _, cond := range condition {
		cond(&c)
	}
	where := buildWhereClause(c)

	exec, err := conn.ExecContext(ctx, "delete from sys_async_task where "+where)
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
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()

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

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		_ = tx.Commit()
		_ = rows.Close()
	}(rows)

	tasks := make([]task.Task, 0)

	for rows.Next() {
		var t task.Task
		var codeOption sql.NullInt32
		var msgOption sql.NullString
		var options string
		err := rows.Scan(
			&t.ID,
			&t.Metadata.ID,
			&t.Metadata.Executor,
			&t.Metadata.Context,
			&options,
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
		if err := json.Unmarshal([]byte(options), &t.Metadata.Options); err != nil {
			return nil, err
		}

		if codeOption.Valid {
			t.ExecuteResult = &task.ExecuteResult{}
			code, err := codeOption.Value()
			if err != nil {
				return nil, err
			}
			t.ExecuteResult.Code = task.ResultCode(code.(int64))

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
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = conn.Close()
	}()

	sqlStr := insertCronTask
	vals := make([]any, 0)
	for _, t := range cronTask {
		sqlStr += "(?, ?, ?, ?, ?, ?, ?, ?, ?),"

		j, err := json.Marshal(t.Metadata.Options)
		if err != nil {
			return 0, err
		}

		vals = append(vals,
			t.Metadata.ID,
			t.Metadata.Executor,
			t.Metadata.Context,
			string(j),
			t.CronExpr,
			t.NextTime,
			t.TriggerTimes,
			t.CreateAt,
			t.UpdateAt,
		)

	}
	if sqlStr == insertCronTask {
		return 0, nil
	}
	sqlStr = sqlStr[0 : len(sqlStr)-1]
	stmt, err := conn.PrepareContext(ctx, sqlStr)
	if err != nil {
		return 0, err
	}
	exec, err := stmt.Exec(vals...)
	if err != nil {
		dup, err := removeDuplicateCronTasks(err, cronTask)
		if err != nil {
			return 0, err
		}
		add, err := m.AddCronTask(ctx, dup...)
		if err != nil {
			return add, err
		}
		return add, nil
	}
	affected, err := exec.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(affected), nil
}

func (m *mysqlTaskStorage) QueryCronTask(ctx context.Context) ([]task.CronTask, error) {
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()

	rows, err := conn.QueryContext(ctx, selectCronTask)
	defer func(rows *sql.Rows) {
		if rows == nil {
			return
		}
		_ = rows.Close()
	}(rows)
	if err != nil {
		return nil, err
	}

	tasks := make([]task.CronTask, 0)

	for rows.Next() {
		var t task.CronTask
		var options string
		err := rows.Scan(
			&t.ID,
			&t.Metadata.ID,
			&t.Metadata.Executor,
			&t.Metadata.Context,
			&options,
			&t.CronExpr,
			&t.NextTime,
			&t.TriggerTimes,
			&t.CreateAt,
			&t.UpdateAt,
		)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(options), &t.Metadata.Options); err != nil {
			return nil, err
		}

		tasks = append(tasks, t)
	}

	return tasks, nil
}

func (m *mysqlTaskStorage) UpdateCronTask(ctx context.Context, cronTask task.CronTask, t task.Task) (int, error) {
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = conn.Close()
	}()

	ok, err := m.taskExists(ctx, conn, t.Metadata.ID)
	if err != nil || ok {
		return 0, err
	}

	triggerTimes, err := m.getTriggerTimes(ctx, conn, cronTask.Metadata.ID)
	if err == sql.ErrNoRows || triggerTimes != cronTask.TriggerTimes-1 {
		return 0, nil
	}

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer func(tx *sql.Tx) {
		_ = tx.Rollback()
	}(tx)
	stmt, err := tx.Prepare(insertAsyncTask + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return 0, err
	}

	j, err := json.Marshal(t.Metadata.Options)
	if err != nil {
		return 0, err
	}
	update, err := stmt.Exec(
		t.Metadata.ID,
		t.Metadata.Executor,
		t.Metadata.Context,
		string(j),
		t.ParentTaskID,
		t.Status,
		t.TaskRunner,
		t.Epoch,
		t.LastHeartbeat,
		t.CreateAt,
		t.CompletedAt)
	if err != nil {
		return 0, err
	}
	exec, err := tx.Exec(updateCronTask,
		cronTask.Metadata.Executor,
		cronTask.Metadata.Context,
		string(j),
		cronTask.CronExpr,
		cronTask.NextTime,
		cronTask.TriggerTimes,
		cronTask.CreateAt,
		cronTask.UpdateAt,
		cronTask.ID)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	affected1, err := exec.RowsAffected()
	if err != nil {
		return 0, err
	}
	affected2, err := update.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(affected2) + int(affected1), nil
}

func (m *mysqlTaskStorage) taskExists(ctx context.Context, conn *sql.Conn, taskMetadataID string) (bool, error) {
	var exists bool
	err := conn.QueryRowContext(ctx, "select exists(select * from sys_async_task where task_metadata_id=?)", taskMetadataID).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (m *mysqlTaskStorage) getTriggerTimes(ctx context.Context, conn *sql.Conn, taskMetadataID string) (uint64, error) {
	var triggerTimes uint64
	err := conn.QueryRowContext(ctx, "select trigger_times from sys_cron_task where task_metadata_id=?", taskMetadataID).Scan(&triggerTimes)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return triggerTimes, nil
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

func removeDuplicateTasks(err error, tasks []task.Task) ([]task.Task, error) {
	me, ok := err.(*mysql.MySQLError)
	if !ok {
		return nil, err
	}
	if me.Number != moerr.ER_DUP_ENTRY {
		return nil, err
	}
	key := strings.Split(me.Message, "'")[1]
	i := 0
	for _, t := range tasks {
		if t.Metadata.ID != key {
			tasks[i] = t
			i++
		}
	}
	tasks = tasks[:i]
	return tasks, nil
}

func removeDuplicateCronTasks(err error, tasks []task.CronTask) ([]task.CronTask, error) {
	me, ok := err.(*mysql.MySQLError)
	if !ok {
		return nil, err
	}
	if me.Number != moerr.ER_DUP_ENTRY {
		return nil, err
	}
	key := strings.Split(me.Message, "'")[1]
	i := 0
	for _, t := range tasks {
		if t.Metadata.ID != key {
			tasks[i] = t
			i++
		}
	}
	tasks = tasks[:i]
	return tasks, nil
}
