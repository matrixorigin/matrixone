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
	"errors"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

var (
	insertAsyncTask = "insert into sys_async_task(" +
		"task_metadata_id," +
		"task_metadata_executor," +
		"task_metadata_context," +
		"task_metadata_option," +
		"task_parent_id," +
		"task_status," +
		"task_runner," +
		"task_epoch," +
		"last_heartbeat," +
		"create_at," +
		"end_at) values "

	updateAsyncTask = "update sys_async_task set " +
		"task_metadata_executor=?," +
		"task_metadata_context=?," +
		"task_metadata_option=?," +
		"task_parent_id=?," +
		"task_status=?," +
		"task_runner=?," +
		"task_epoch=?," +
		"last_heartbeat=?," +
		"result_code=?," +
		"error_msg=?," +
		"create_at=?," +
		"end_at=? " +
		"where task_id=?"

	selectAsyncTask = "select " +
		"task_id," +
		"task_metadata_id," +
		"task_metadata_executor," +
		"task_metadata_context," +
		"task_metadata_option," +
		"task_parent_id," +
		"task_status," +
		"task_runner," +
		"task_epoch," +
		"last_heartbeat," +
		"result_code," +
		"error_msg," +
		"create_at," +
		"end_at from sys_async_task where 1=1"

	insertCronTask = "insert into sys_cron_task(" +
		"task_metadata_id," +
		"task_metadata_executor," +
		"task_metadata_context," +
		"task_metadata_option," +
		"cron_expr," +
		"next_time," +
		"trigger_times," +
		"create_at," +
		"update_at) values "

	selectCronTask = "select " +
		"cron_task_id," +
		"task_metadata_id," +
		"task_metadata_executor," +
		"task_metadata_context," +
		"task_metadata_option," +
		"cron_expr," +
		"next_time," +
		"trigger_times," +
		"create_at," +
		"update_at from sys_cron_task where 1=1"

	updateCronTask = "update sys_cron_task set " +
		"task_metadata_executor=?," +
		"task_metadata_context=?," +
		"task_metadata_option=?," +
		"cron_expr=?," +
		"next_time=?," +
		"trigger_times=?," +
		"create_at=?," +
		"update_at=? where cron_task_id=?"

	countTaskId = "select count(task_metadata_id) from sys_async_task where task_metadata_id=?"

	getTriggerTimes = "select trigger_times from sys_cron_task where task_metadata_id=?"

	deleteAsyncTask = "delete from sys_async_task where 1=1"

	insertDaemonTask = "insert into sys_daemon_task (" +
		"task_metadata_id," +
		"task_metadata_executor," +
		"task_metadata_context," +
		"task_metadata_option," +
		"account_id," +
		"account," +
		"task_type," +
		"task_status," +
		"create_at," +
		"update_at," +
		"details) values "

	updateDaemonTask = "update sys_daemon_task set " +
		"task_metadata_executor=?, " +
		"task_metadata_context=?, " +
		"task_metadata_option=?, " +
		"task_type=?, " +
		"task_status=?, " +
		"task_runner=?, " +
		"last_heartbeat=?, " +
		"update_at=?, " +
		"end_at=?, " +
		"last_run=?, " +
		"details=? where task_id=?"

	heartbeatDaemonTask = "update sys_daemon_task set last_heartbeat=? where task_id=?"

	deleteDaemonTask = "delete from sys_daemon_task where 1=1"

	selectDaemonTask = "select " +
		"task_id, " +
		"task_metadata_id, " +
		"task_metadata_executor, " +
		"task_metadata_context, " +
		"task_metadata_option, " +
		"account_id, " +
		"account, " +
		"task_type, " +
		"task_runner, " +
		"task_status, " +
		"last_heartbeat, " +
		"create_at, " +
		"update_at, " +
		"end_at, " +
		"last_run, " +
		"details from sys_daemon_task where 1=1"
)

type mysqlTaskStorage struct {
	db *sql.DB
}

func newMysqlTaskStorage(dsn string) (TaskStorage, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(3)

	return &mysqlTaskStorage{
		db: db,
	}, nil
}

func (m *mysqlTaskStorage) Close() error {
	return m.db.Close()
}

func (m *mysqlTaskStorage) PingContext(ctx context.Context) error {
	return m.db.PingContext(ctx)
}

func (m *mysqlTaskStorage) AddAsyncTask(ctx context.Context, tasks ...task.AsyncTask) (int, error) {
	if taskFrameworkDisabled() {
		return 0, nil
	}

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
	exec, err := m.db.ExecContext(ctx, sqlStr[:len(sqlStr)-1], vals...)
	if err != nil {
		dup, err := removeDuplicateAsyncTasks(err, tasks)
		if err != nil {
			return 0, err
		}
		add, err := m.AddAsyncTask(ctx, dup...)
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

func (m *mysqlTaskStorage) UpdateAsyncTask(ctx context.Context, tasks []task.AsyncTask, condition ...Condition) (int, error) {
	if taskFrameworkDisabled() {
		return 0, nil
	}

	if len(tasks) == 0 {
		return 0, nil
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}

	prepare, err := tx.PrepareContext(ctx, updateAsyncTask+buildWhereClause(newConditions(condition...)))
	if err != nil {
		return 0, err
	}
	defer prepare.Close()
	n := 0
	for _, t := range tasks {
		err := func() error {
			execResult := &task.ExecuteResult{
				Code: task.ResultCode_Success,
			}
			if t.ExecuteResult != nil {
				execResult.Code = t.ExecuteResult.Code
				execResult.Error = t.ExecuteResult.Error
			}

			j, err := json.Marshal(t.Metadata.Options)
			if err != nil {
				return err
			}

			exec, err := prepare.ExecContext(ctx,
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
				return err
			}
			affected, err := exec.RowsAffected()
			if err != nil {
				return nil
			}
			n += int(affected)
			return nil
		}()
		if err != nil {
			if e := tx.Rollback(); e != nil {
				return 0, errors.Join(e, err)
			}
			return 0, err
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return n, nil
}

func (m *mysqlTaskStorage) DeleteAsyncTask(ctx context.Context, condition ...Condition) (int, error) {
	if taskFrameworkDisabled() {
		return 0, nil
	}
	exec, err := m.db.ExecContext(ctx, deleteAsyncTask+buildWhereClause(newConditions(condition...)))
	if err != nil {
		return 0, err
	}
	affected, err := exec.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(affected), nil
}

func (m *mysqlTaskStorage) QueryAsyncTask(ctx context.Context, condition ...Condition) ([]task.AsyncTask, error) {
	if taskFrameworkDisabled() {
		return nil, nil
	}

	c := newConditions(condition...)
	query := selectAsyncTask +
		buildWhereClause(c) +
		buildOrderByClause(c) +
		buildLimitClause(c)

	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	tasks := make([]task.AsyncTask, 0)
	for rows.Next() {
		var t task.AsyncTask
		var codeOption sql.NullInt32
		var msgOption sql.NullString
		var options string
		if err := rows.Scan(
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
		); err != nil {
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

			if msgOption.Valid {
				msg, err := msgOption.Value()
				if err != nil {
					return nil, err
				}
				t.ExecuteResult.Error = msg.(string)
			}
		}

		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

func (m *mysqlTaskStorage) AddCronTask(ctx context.Context, cronTask ...task.CronTask) (int, error) {
	if taskFrameworkDisabled() {
		return 0, nil
	}

	if len(cronTask) == 0 {
		return 0, nil
	}

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
	exec, err := m.db.ExecContext(ctx, sqlStr[:len(sqlStr)-1], vals...)
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

func (m *mysqlTaskStorage) QueryCronTask(ctx context.Context, condition ...Condition) (tasks []task.CronTask, err error) {
	if taskFrameworkDisabled() {
		return nil, nil
	}
	var rows *sql.Rows
	rows, err = m.db.QueryContext(ctx, selectCronTask+buildWhereClause(newConditions(condition...)))
	if err != nil {
		return nil, err
	}
	defer func() {
		if rows == nil {
			return
		}
		err = errors.Join(err, rows.Close(), rows.Err())
	}()

	tasks = make([]task.CronTask, 0)

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

func (m *mysqlTaskStorage) UpdateCronTask(ctx context.Context, cronTask task.CronTask, asyncTask task.AsyncTask) (int, error) {
	if taskFrameworkDisabled() {
		return 0, nil
	}

	// task exists in async task table
	ok, err := m.taskExists(ctx, asyncTask.Metadata.ID)
	if err != nil || ok {
		return 0, err
	}

	triggerTimes, err := m.getTriggerTimes(ctx, cronTask.Metadata.ID)
	if errors.Is(err, sql.ErrNoRows) || triggerTimes != cronTask.TriggerTimes-1 {
		return 0, moerr.NewInternalError(ctx, "cron task trigger times not match")
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func(tx *sql.Tx) {
		_ = tx.Rollback()
	}(tx)

	j, err := json.Marshal(asyncTask.Metadata.Options)
	if err != nil {
		return 0, err
	}
	inserted, err := tx.ExecContext(
		ctx,
		insertAsyncTask+"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		asyncTask.Metadata.ID,
		asyncTask.Metadata.Executor,
		asyncTask.Metadata.Context,
		util.UnsafeBytesToString(j),
		asyncTask.ParentTaskID,
		asyncTask.Status,
		asyncTask.TaskRunner,
		asyncTask.Epoch,
		asyncTask.LastHeartbeat,
		asyncTask.CreateAt,
		asyncTask.CompletedAt)
	if err != nil {
		return 0, err
	}

	updated, err := tx.ExecContext(
		ctx,
		updateCronTask,
		cronTask.Metadata.Executor,
		cronTask.Metadata.Context,
		util.UnsafeBytesToString(j),
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
	affected1, err := updated.RowsAffected()
	if err != nil {
		return 0, err
	}
	affected2, err := inserted.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(affected2) + int(affected1), nil
}

func (m *mysqlTaskStorage) taskExists(ctx context.Context, taskMetadataID string) (bool, error) {
	var count int32
	if err := m.db.QueryRowContext(ctx, countTaskId, taskMetadataID).Scan(&count); err != nil {
		return false, err
	}
	return count != 0, nil
}

func (m *mysqlTaskStorage) getTriggerTimes(ctx context.Context, taskMetadataID string) (uint64, error) {
	var triggerTimes uint64
	err := m.db.QueryRowContext(ctx, getTriggerTimes, taskMetadataID).Scan(&triggerTimes)
	if err != nil {
		return 0, err
	}
	return triggerTimes, nil
}

func buildWhereClause(c *conditions) string {
	var clauseBuilder strings.Builder

	for code := range whereConditionCodes {
		if cond, ok := (*c)[code]; ok {
			clauseBuilder.WriteString(" AND ")
			clauseBuilder.WriteString(cond.sql())
		}
	}

	return clauseBuilder.String()
}

func buildLimitClause(c *conditions) string {
	if cond, ok := (*c)[CondLimit]; ok {
		return cond.sql()
	}
	return ""
}

func buildOrderByClause(c *conditions) string {
	if cond, ok := (*c)[CondOrderByDesc]; ok {
		return cond.sql()
	}
	return " order by task_id"
}

func removeDuplicateAsyncTasks(err error, tasks []task.AsyncTask) ([]task.AsyncTask, error) {
	var me *mysql.MySQLError
	if ok := errors.As(err, &me); !ok {
		return nil, err
	}
	if me.Number != moerr.ER_DUP_ENTRY {
		return nil, err
	}
	b := tasks[:0]
	for _, t := range tasks {
		if !strings.Contains(me.Message, t.Metadata.ID) {
			b = append(b, t)
		}
	}
	return b, nil
}

func removeDuplicateCronTasks(err error, tasks []task.CronTask) ([]task.CronTask, error) {
	var me *mysql.MySQLError
	if ok := errors.As(err, &me); !ok {
		return nil, err
	}
	if me.Number != moerr.ER_DUP_ENTRY {
		return nil, err
	}
	b := tasks[:0]
	for _, t := range tasks {
		if !strings.Contains(me.Message, t.Metadata.ID) {
			b = append(b, t)
		}
	}
	return b, nil
}

func removeDuplicateDaemonTasks(err error, tasks []task.DaemonTask) ([]task.DaemonTask, error) {
	var me *mysql.MySQLError
	if ok := errors.As(err, &me); !ok {
		return nil, err
	}
	if me.Number != moerr.ER_DUP_ENTRY {
		return nil, err
	}
	b := tasks[:0]
	for _, t := range tasks {
		if !strings.Contains(me.Message, t.Metadata.ID) {
			b = append(b, t)
		}
	}
	return b, nil
}

func (m *mysqlTaskStorage) AddDaemonTask(ctx context.Context, tasks ...task.DaemonTask) (int, error) {
	if taskFrameworkDisabled() {
		return 0, nil
	}
	if len(tasks) == 0 {
		return 0, nil
	}

	sqlStr := insertDaemonTask
	values := make([]any, 0)
	for _, t := range tasks {
		sqlStr += "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),"

		j, err := json.Marshal(t.Metadata.Options)
		if err != nil {
			return 0, err
		}
		details, err := t.Details.Marshal()
		if err != nil {
			return 0, err
		}

		values = append(values,
			t.Metadata.ID,
			t.Metadata.Executor,
			t.Metadata.Context,
			string(j),
			t.Details.AccountID,
			t.Details.Account,
			t.TaskType.String(),
			t.TaskStatus,
			t.CreateAt,
			t.UpdateAt,
			details,
		)
	}
	if sqlStr == insertDaemonTask {
		return 0, nil
	}
	exec, err := m.db.ExecContext(ctx, sqlStr[:len(sqlStr)-1], values...)
	if err != nil {
		dup, err := removeDuplicateDaemonTasks(err, tasks)
		if err != nil {
			return 0, err
		}
		add, err := m.AddDaemonTask(ctx, dup...)
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

func (m *mysqlTaskStorage) UpdateDaemonTask(ctx context.Context, tasks []task.DaemonTask, condition ...Condition) (int, error) {
	if taskFrameworkDisabled() {
		return 0, nil
	}

	if len(tasks) == 0 {
		return 0, nil
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}

	c := newConditions(condition...)
	updateSql := updateDaemonTask + buildDaemonTaskWhereClause(c)
	prepare, err := tx.PrepareContext(ctx, updateSql)
	if err != nil {
		return 0, err
	}
	defer prepare.Close()
	n := 0
	for _, t := range tasks {
		err := func() error {
			j, err := json.Marshal(t.Metadata.Options)
			if err != nil {
				return err
			}
			details, err := t.Details.Marshal()
			if err != nil {
				return err
			}

			var lastHeartbeat, updateAt, endAt, lastRun any
			if !t.LastHeartbeat.IsZero() {
				lastHeartbeat = t.LastHeartbeat
			}
			if !t.UpdateAt.IsZero() {
				updateAt = t.UpdateAt
			}
			if !t.EndAt.IsZero() {
				endAt = t.EndAt
			}
			if !t.LastRun.IsZero() {
				lastRun = t.LastRun
			}

			exec, err := prepare.ExecContext(ctx,
				t.Metadata.Executor,
				t.Metadata.Context,
				string(j),
				t.TaskType.String(),
				t.TaskStatus,
				t.TaskRunner,
				lastHeartbeat,
				updateAt,
				endAt,
				lastRun,
				details,
				t.ID,
			)
			if err != nil {
				return err
			}
			affected, err := exec.RowsAffected()
			if err != nil {
				return nil
			}
			n += int(affected)
			return nil
		}()
		if err != nil {
			if e := tx.Rollback(); e != nil {
				return 0, errors.Join(e, err)
			}
			return 0, err
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return n, nil
}

func (m *mysqlTaskStorage) DeleteDaemonTask(ctx context.Context, condition ...Condition) (int, error) {
	if taskFrameworkDisabled() {
		return 0, nil
	}

	c := newConditions(condition...)
	deleteSql := deleteDaemonTask + buildDaemonTaskWhereClause(c)
	exec, err := m.db.ExecContext(ctx, deleteSql)
	if err != nil {
		return 0, err
	}
	affected, err := exec.RowsAffected()
	if err != nil {
		panic(err)
	}
	return int(affected), nil
}

func (m *mysqlTaskStorage) QueryDaemonTask(ctx context.Context, condition ...Condition) ([]task.DaemonTask, error) {
	if taskFrameworkDisabled() {
		return nil, nil
	}

	c := newConditions(condition...)
	query := selectDaemonTask +
		buildDaemonTaskWhereClause(c) +
		buildOrderByClause(c) +
		buildLimitClause(c)

	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	tasks := make([]task.DaemonTask, 0)
	for rows.Next() {
		var t task.DaemonTask
		var options, taskType string
		var runner sql.NullString
		var lastHeartbeat, createAt, updateAt, endAt, lastRun sql.NullTime
		if err := rows.Scan(
			&t.ID,
			&t.Metadata.ID,
			&t.Metadata.Executor,
			&t.Metadata.Context,
			&options,
			&t.AccountID,
			&t.Account,
			&taskType,
			&runner,
			&t.TaskStatus,
			&lastHeartbeat,
			&createAt,
			&updateAt,
			&endAt,
			&lastRun,
			&t.Details,
		); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(options), &t.Metadata.Options); err != nil {
			return nil, err
		}

		typ, ok := task.TaskType_value[taskType]
		if !ok {
			typ = int32(task.TaskType_TypeUnknown)
		}
		t.TaskType = task.TaskType(typ)

		t.TaskRunner = runner.String
		t.LastHeartbeat = lastHeartbeat.Time
		t.CreateAt = createAt.Time
		t.UpdateAt = updateAt.Time
		t.EndAt = endAt.Time
		t.LastRun = lastRun.Time

		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return tasks, err
	}
	return tasks, nil
}

func (m *mysqlTaskStorage) HeartbeatDaemonTask(ctx context.Context, tasks []task.DaemonTask) (int, error) {
	if taskFrameworkDisabled() {
		return 0, nil
	}

	if len(tasks) == 0 {
		return 0, nil
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}

	prepare, err := tx.PrepareContext(ctx, heartbeatDaemonTask)
	if err != nil {
		return 0, err
	}
	defer prepare.Close()
	n := 0
	for _, t := range tasks {
		err := func() error {
			var lastHeartbeat time.Time
			if !t.LastHeartbeat.IsZero() {
				lastHeartbeat = t.LastHeartbeat
			}

			exec, err := prepare.ExecContext(ctx,
				lastHeartbeat,
				t.ID,
			)
			if err != nil {
				return err
			}
			affected, err := exec.RowsAffected()
			if err != nil {
				return nil
			}
			n += int(affected)
			return nil
		}()
		if err != nil {
			if e := tx.Rollback(); e != nil {
				return 0, errors.Join(e, err)
			}
			return 0, err
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return n, nil
}

func buildDaemonTaskWhereClause(c *conditions) string {
	var clauseBuilder strings.Builder

	for cond := range daemonWhereConditionCodes {
		if cond, ok := (*c)[cond]; ok {
			clauseBuilder.WriteString(" AND ")
			clauseBuilder.WriteString(cond.sql())
		}
	}

	return clauseBuilder.String()
}
