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
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"go.uber.org/multierr"
)

var (
	createDatabase = `create database if not exists %s`
	createTables   = map[string]string{
		"sys_async_task": `create table if not exists %s.sys_async_task (
			task_id                     int primary key auto_increment,
			task_metadata_id            varchar(50) unique not null,
			task_metadata_executor      int,
			task_metadata_context       blob,
			task_metadata_option        varchar(1000),
			task_parent_id              varchar(50),
			task_status                 int,
			task_runner                 varchar(50),
			task_epoch                  int,
			last_heartbeat              bigint,
			result_code                 int null,
			error_msg                   varchar(1000) null,
			create_at                   bigint,
			end_at                      bigint)`,
		"sys_cron_task": `create table if not exists %s.sys_cron_task (
			cron_task_id				int primary key auto_increment,
    		task_metadata_id            varchar(50) unique not null,
			task_metadata_executor      int,
			task_metadata_context       blob,
			task_metadata_option 		varchar(1000),
			cron_expr					varchar(100) not null,
			next_time					bigint,
			trigger_times				int,
			create_at					bigint,
			update_at					bigint)`,
	}

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
						from %s.sys_async_task`

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
						from %s.sys_cron_task`

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

	deleteTask = `delete from %s.sys_async_task where `
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

func (m *mysqlTaskStorage) Add(ctx context.Context, tasks ...task.Task) (int, error) {
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

	c := conditions{}
	for _, cond := range condition {
		cond(&c)
	}
	where := buildWhereClause(c)

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return 0, err
	}

	var update string
	if where != "" {
		update = fmt.Sprintf(updateAsyncTask, m.dbname) + " and " + where
	} else {
		update = fmt.Sprintf(updateAsyncTask, m.dbname)
	}
	n := 0
	for _, t := range tasks {
		err := func() error {
			execResult := &task.ExecuteResult{}
			if t.ExecuteResult != nil {
				execResult.Code = t.ExecuteResult.Code
				execResult.Error = t.ExecuteResult.Error
			}

			j, err := json.Marshal(t.Metadata.Options)
			if err != nil {
				return err
			}

			exec, err := tx.ExecContext(ctx, update,
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
				return 0, e
			}
			return 0, err
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return n, nil
}

func (m *mysqlTaskStorage) Delete(ctx context.Context, condition ...Condition) (int, error) {
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

	c := conditions{}
	for _, cond := range condition {
		cond(&c)
	}
	where := buildWhereClause(c)

	exec, err := conn.ExecContext(ctx, fmt.Sprintf(deleteTask, m.dbname)+where)
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

	c := conditions{}
	for _, cond := range condition {
		cond(&c)
	}

	where := buildWhereClause(c)
	var query string
	if where != "" {
		query = fmt.Sprintf(selectAsyncTask, m.dbname) + " where " + where
	} else {
		query = fmt.Sprintf(selectAsyncTask, m.dbname)
	}
	query += buildOrderByCause(c)
	query += buildLimitClause(c)

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

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
	if sqlStr == fmt.Sprintf(insertCronTask, m.dbname) {
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

	rows, err := conn.QueryContext(ctx, fmt.Sprintf(selectCronTask, m.dbname))
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
	if taskFrameworkDisabled() {
		return 0, nil
	}

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
	stmt, err := tx.Prepare(fmt.Sprintf(insertAsyncTask, m.dbname) + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
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
	exec, err := tx.Exec(fmt.Sprintf(updateCronTask, m.dbname),
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

	if c.hasTaskExecutorCond {
		if clause != "" {
			clause += " AND "
		}
		clause += fmt.Sprintf("task_metadata_executor%s%d", OpName[c.taskExecutorOp], c.taskExecutor)
	}

	return clause
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
		return nil, nil, multierr.Append(err, db.Close())
	}

	return db, func() error { return db.Close() }, nil
}

func (m *mysqlTaskStorage) useDB(db *sql.DB) error {
	if err := db.Ping(); err != nil {
		return errNotReady
	}
	for _, err := db.Exec("use " + m.dbname); err != nil; _, err = db.Exec("use " + m.dbname) {
		me, ok := err.(*mysql.MySQLError)
		if !ok || me.Number != moerr.ER_BAD_DB_ERROR {
			return err
		}
		if _, err = db.Exec(fmt.Sprintf(createDatabase, m.dbname)); err != nil {
			return multierr.Append(err, db.Close())
		}
	}
	rows, err := db.Query("show tables")
	if err != nil {
		return err
	}

	tables := make(map[string]struct{}, len(createTables))
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return err
		}
		tables[table] = struct{}{}
	}

	for table, createSql := range createTables {
		if _, ok := tables[table]; !ok {
			if _, err = db.Exec(fmt.Sprintf(createSql, m.dbname)); err != nil {
				return multierr.Append(err, db.Close())
			}
		}
	}
	return nil
}

func buildLimitClause(c conditions) string {
	if c.limit != 0 {
		return fmt.Sprintf(" limit %d", c.limit)
	}
	return ""
}

func buildOrderByCause(c conditions) string {
	if c.orderByDesc {
		return " order by task_id desc"
	}
	return " order by task_id"
}

func removeDuplicateTasks(err error, tasks []task.Task) ([]task.Task, error) {
	me, ok := err.(*mysql.MySQLError)
	if !ok {
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
	me, ok := err.(*mysql.MySQLError)
	if !ok {
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
