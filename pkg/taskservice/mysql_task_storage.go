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
	"fmt"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

var (
	insertAsyncTask = `insert into %s.sys_async_task(
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

	updateAsyncTask = `update %s.sys_async_task set 
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
						from %s.sys_async_task where 1=1`

	insertCronTask = `insert into %s.sys_cron_task (
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
						from %s.sys_cron_task where 1=1`

	updateCronTask = `update %s.sys_cron_task set 
							task_metadata_executor=?,
    						task_metadata_context=?,
    						task_metadata_option=?,
    						cron_expr=?,
    						next_time=?,
    						trigger_times=?,
    						create_at=?,
    						update_at=? where cron_task_id=?`

	countTaskId = `select count(task_metadata_id) from %s.sys_async_task where task_metadata_id=?`

	getTriggerTimes = `select trigger_times from %s.sys_cron_task where task_metadata_id=?`

	deleteAsyncTask = `delete from %s.sys_async_task where 1=1`

	insertDaemonTask = `insert into %s.sys_daemon_task (
                      task_metadata_id,
							task_metadata_executor,
							task_metadata_context,
                           task_metadata_option,
                           account_id,
                           account,
                           task_type,
                           task_status,
                           create_at,
                           update_at,
                           details
                    ) values `

	updateDaemonTask = `update %s.sys_daemon_task set
							task_metadata_executor=?,
							task_metadata_context=?,
							task_metadata_option=?,
							task_type=?,
							task_status=?,
							task_runner=?,
							last_heartbeat=?,
							update_at=?,
							end_at=?,
                            last_run=?,
                            details=? where task_id=?`

	heartbeatDaemonTask = `update %s.sys_daemon_task set
							last_heartbeat=? where task_id=?`

	deleteDaemonTask = `delete from %s.sys_daemon_task where 1=1`

	selectDaemonTask = `select
							task_id,
							task_metadata_id,
							task_metadata_executor,
							task_metadata_context,
							task_metadata_option,
							account_id,
							account,
							task_type,
							task_runner,
							task_status,
							last_heartbeat,
							create_at,
							update_at,
							end_at,
							last_run,
							details
						from %s.sys_daemon_task where 1=1`
)

var (
	forceNewConn = "async_task_force_new_connection"
)

type mysqlTaskStorage struct {
	dsn          string
	dbname       string
	db           *sql.DB
	forceNewConn bool
}

func NewMysqlTaskStorage(dsn, dbname string) (TaskStorage, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(1)

	_, ok := os.LookupEnv(forceNewConn)
	return &mysqlTaskStorage{
		dsn:          dsn,
		db:           db,
		dbname:       dbname,
		forceNewConn: ok,
	}, nil
}

func (m *mysqlTaskStorage) Close() error {
	return m.db.Close()
}

func (m *mysqlTaskStorage) AddAsyncTask(ctx context.Context, tasks ...task.AsyncTask) (int, error) {
	if taskFrameworkDisabled() {
		return 0, nil
	}

	if len(tasks) == 0 {
		return 0, nil
	}

	db, release, err := m.getDB()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = release()
	}()

	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = conn.Close()
	}()

	sqlStr := fmt.Sprintf(insertAsyncTask, m.dbname)
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

	if sqlStr == fmt.Sprintf(insertAsyncTask, m.dbname) {
		return 0, nil
	}
	sqlStr = sqlStr[0 : len(sqlStr)-1]
	stmt, err := conn.PrepareContext(ctx, sqlStr)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	exec, err := stmt.ExecContext(ctx, vals...)
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

	db, release, err := m.getDB()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = release()
	}()

	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = conn.Close()
	}()

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return 0, err
	}

	c := newConditions(condition...)
	updateSql := fmt.Sprintf(updateAsyncTask, m.dbname) + buildWhereClause(c)
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

			prepare, err := tx.PrepareContext(ctx, updateSql)
			if err != nil {
				return err
			}
			defer prepare.Close()

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

	db, release, err := m.getDB()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = release()
	}()

	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = conn.Close()
	}()

	c := newConditions(condition...)
	deleteSql := fmt.Sprintf(deleteAsyncTask, m.dbname) + buildWhereClause(c)
	exec, err := conn.ExecContext(ctx, deleteSql)
	if err != nil {
		return 0, err
	}
	affected, err := exec.RowsAffected()
	if err != nil {
		panic(err)
	}
	return int(affected), nil
}

func (m *mysqlTaskStorage) QueryAsyncTask(ctx context.Context, condition ...Condition) ([]task.AsyncTask, error) {
	if taskFrameworkDisabled() {
		return nil, nil
	}

	db, release, err := m.getDB()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = release()
	}()

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()

	c := newConditions(condition...)

	query := fmt.Sprintf(selectAsyncTask, m.dbname) + buildWhereClause(c)
	query += buildOrderByClause(c)
	query += buildLimitClause(c)

	rows, err := conn.QueryContext(ctx, query)
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

			msg, err := msgOption.Value()
			if err != nil {
				return nil, err
			}
			t.ExecuteResult.Error = msg.(string)
		}

		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return tasks, err
	}
	return tasks, nil
}

func (m *mysqlTaskStorage) AddCronTask(ctx context.Context, cronTask ...task.CronTask) (int, error) {
	if taskFrameworkDisabled() {
		return 0, nil
	}

	if len(cronTask) == 0 {
		return 0, nil
	}

	db, release, err := m.getDB()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = release()
	}()

	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = conn.Close()
	}()

	sqlStr := fmt.Sprintf(insertCronTask, m.dbname)
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
	sqlStr = sqlStr[0 : len(sqlStr)-1]
	stmt, err := conn.PrepareContext(ctx, sqlStr)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
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

func (m *mysqlTaskStorage) QueryCronTask(ctx context.Context, condition ...Condition) (tasks []task.CronTask, err error) {
	if taskFrameworkDisabled() {
		return nil, nil
	}

	db, release, err := m.getDB()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = release()
	}()

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()

	c := newConditions(condition...)
	query := fmt.Sprintf(selectCronTask, m.dbname) + buildWhereClause(c)
	rows, err := conn.QueryContext(ctx, query)
	defer func(rows *sql.Rows) {
		if rows == nil {
			return
		}
		_ = rows.Close()
	}(rows)
	if err != nil {
		return nil, err
	}
	defer func() {
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

	db, release, err := m.getDB()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = release()
	}()

	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = conn.Close()
	}()

	ok, err := m.taskExists(ctx, conn, asyncTask.Metadata.ID)
	if err != nil || ok {
		return 0, err
	}

	triggerTimes, err := m.getTriggerTimes(ctx, conn, cronTask.Metadata.ID)
	if errors.Is(err, sql.ErrNoRows) || triggerTimes != cronTask.TriggerTimes-1 {
		return 0, moerr.NewInternalError(ctx, "cron task trigger times not match")
	}

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer func(tx *sql.Tx) {
		_ = tx.Rollback()
	}(tx)

	preInsert, err := tx.Prepare(fmt.Sprintf(insertAsyncTask, m.dbname) + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return 0, err
	}
	defer preInsert.Close()

	preUpdate, err := tx.Prepare(fmt.Sprintf(updateCronTask, m.dbname))
	if err != nil {
		return 0, err
	}
	defer preUpdate.Close()

	j, err := json.Marshal(asyncTask.Metadata.Options)
	if err != nil {
		return 0, err
	}
	inserted, err := preInsert.Exec(
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

	updated, err := preUpdate.Exec(
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

func (m *mysqlTaskStorage) taskExists(ctx context.Context, conn *sql.Conn, taskMetadataID string) (bool, error) {
	var count int32
	if err := conn.QueryRowContext(ctx, fmt.Sprintf(countTaskId, m.dbname), taskMetadataID).Scan(&count); err != nil {
		return false, err
	}
	return count != 0, nil
}

func (m *mysqlTaskStorage) getTriggerTimes(ctx context.Context, conn *sql.Conn, taskMetadataID string) (uint64, error) {
	var triggerTimes uint64
	err := conn.QueryRowContext(ctx, fmt.Sprintf(getTriggerTimes, m.dbname), taskMetadataID).Scan(&triggerTimes)
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

func (m *mysqlTaskStorage) getDB() (*sql.DB, func() error, error) {
	if !m.forceNewConn {
		if err := m.useDB(m.db); err != nil {
			return nil, nil, err
		}
		return m.db, func() error { return nil }, nil
	}

	db, err := sql.Open("mysql", m.dsn)
	if err != nil {
		return nil, nil, err
	}

	if err = m.useDB(db); err != nil {
		return nil, nil, errors.Join(err, db.Close())
	}

	return db, db.Close, nil
}

func (m *mysqlTaskStorage) useDB(db *sql.DB) (err error) {
	if err := db.Ping(); err != nil {
		return errors.Join(err, ErrNotReady)
	}
	if _, err := db.Exec("use " + m.dbname); err != nil {
		return errors.Join(err, db.Close())
	}
	if _, err := db.Exec("set session disable_txn_trace=1"); err != nil {
		return errors.Join(err, db.Close())
	}
	return nil
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
	db, release, err := m.getDB()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = release()
	}()
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = conn.Close()
	}()
	sqlStr := fmt.Sprintf(insertDaemonTask, m.dbname)
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
	if sqlStr == fmt.Sprintf(insertDaemonTask, m.dbname) {
		return 0, nil
	}
	sqlStr = sqlStr[0 : len(sqlStr)-1]
	stmt, err := conn.PrepareContext(ctx, sqlStr)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	exec, err := stmt.Exec(values...)
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

	db, release, err := m.getDB()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = release()
	}()

	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = conn.Close()
	}()

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return 0, err
	}

	c := newConditions(condition...)
	updateSql := fmt.Sprintf(updateDaemonTask, m.dbname) + buildDaemonTaskWhereClause(c)
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

			prepare, err := tx.PrepareContext(ctx, updateSql)
			if err != nil {
				return err
			}
			defer prepare.Close()

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

	db, release, err := m.getDB()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = release()
	}()

	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = conn.Close()
	}()

	c := newConditions(condition...)
	deleteSql := fmt.Sprintf(deleteDaemonTask, m.dbname) + buildDaemonTaskWhereClause(c)
	exec, err := conn.ExecContext(ctx, deleteSql)
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

	db, release, err := m.getDB()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = release()
	}()

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()

	c := newConditions(condition...)
	query := fmt.Sprintf(selectDaemonTask, m.dbname) + buildDaemonTaskWhereClause(c)
	query += buildOrderByClause(c)
	query += buildLimitClause(c)

	rows, err := conn.QueryContext(ctx, query)
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

	db, release, err := m.getDB()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = release()
	}()

	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = conn.Close()
	}()

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return 0, err
	}

	update := fmt.Sprintf(heartbeatDaemonTask, m.dbname)
	n := 0
	for _, t := range tasks {
		err := func() error {
			prepare, err := tx.PrepareContext(ctx, update)
			if err != nil {
				return err
			}
			defer prepare.Close()

			var lastHeartbeat any
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
