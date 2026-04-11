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

package frontend

import (
	"context"
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pbtask "github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

var (
	sqlTaskShowCols = []Column{
		newSQLTaskColumn("task_name", defines.MYSQL_TYPE_VARCHAR),
		newSQLTaskColumn("schedule", defines.MYSQL_TYPE_VARCHAR),
		newSQLTaskColumn("enabled", defines.MYSQL_TYPE_LONG),
		newSQLTaskColumn("gate_condition", defines.MYSQL_TYPE_VARCHAR),
		newSQLTaskColumn("retry_limit", defines.MYSQL_TYPE_LONG),
		newSQLTaskColumn("timeout", defines.MYSQL_TYPE_VARCHAR),
		newSQLTaskColumn("created_at", defines.MYSQL_TYPE_VARCHAR),
		newSQLTaskColumn("last_run_status", defines.MYSQL_TYPE_VARCHAR),
		newSQLTaskColumn("last_run_time", defines.MYSQL_TYPE_VARCHAR),
	}
	sqlTaskRunShowCols = []Column{
		newSQLTaskColumn("run_id", defines.MYSQL_TYPE_LONGLONG),
		newSQLTaskColumn("task_name", defines.MYSQL_TYPE_VARCHAR),
		newSQLTaskColumn("trigger_type", defines.MYSQL_TYPE_VARCHAR),
		newSQLTaskColumn("status", defines.MYSQL_TYPE_VARCHAR),
		newSQLTaskColumn("started_at", defines.MYSQL_TYPE_VARCHAR),
		newSQLTaskColumn("finished_at", defines.MYSQL_TYPE_VARCHAR),
		newSQLTaskColumn("duration", defines.MYSQL_TYPE_DOUBLE),
		newSQLTaskColumn("attempt", defines.MYSQL_TYPE_LONG),
		newSQLTaskColumn("rows_affected", defines.MYSQL_TYPE_LONGLONG),
		newSQLTaskColumn("error_message", defines.MYSQL_TYPE_VARCHAR),
	}
)

func handleCreateSQLTask(ctx context.Context, ses *Session, stmt *tree.CreateSQLTask) error {
	ts, err := getSQLTaskService(ctx, ses)
	if err != nil {
		return err
	}
	store := ts.GetStorage()
	existing, err := store.QuerySQLTask(ctx,
		taskservice.WithTaskName(taskservice.EQ, string(stmt.Name)),
		taskservice.WithAccountID(taskservice.EQ, ses.GetAccountId()),
	)
	if err != nil {
		return err
	}
	if len(existing) > 0 {
		if stmt.IfNotExists {
			return nil
		}
		return moerr.NewInternalErrorf(ctx, "sql task %s already exists", string(stmt.Name))
	}

	timeoutSeconds, err := taskservice.ParseSQLTaskTimeout(stmt.Timeout)
	if err != nil {
		return err
	}
	now := time.Now()
	sqlTask, err := buildSQLTaskFromStmt(ses, stmt, timeoutSeconds, now)
	if err != nil {
		return err
	}
	_, err = store.AddSQLTask(ctx, sqlTask)
	return err
}

func handleAlterSQLTask(ctx context.Context, ses *Session, stmt *tree.AlterSQLTask) error {
	ts, err := getSQLTaskService(ctx, ses)
	if err != nil {
		return err
	}
	store := ts.GetStorage()
	sqlTask, err := getSQLTaskByName(ctx, store, string(stmt.Name), ses.GetAccountId())
	if err != nil {
		return err
	}

	now := time.Now()
	switch stmt.Action {
	case tree.AlterTaskSuspend:
		sqlTask.Enabled = false
	case tree.AlterTaskResume:
		sqlTask.Enabled = true
		nextFire, err := taskservice.NextSQLTaskFireTime(newSQLTaskCronParser(), sqlTask.CronExpr, sqlTask.Timezone, now)
		if err != nil {
			return err
		}
		sqlTask.NextFireTime = nextFire
	case tree.AlterTaskSetSchedule:
		timezone := normalizeSQLTaskTimezone(stmt.Timezone)
		nextFire, err := taskservice.NextSQLTaskFireTime(newSQLTaskCronParser(), stmt.CronExpr, timezone, now)
		if err != nil {
			return err
		}
		sqlTask.CronExpr = stmt.CronExpr
		sqlTask.Timezone = timezone
		sqlTask.TriggerCount = 0
		sqlTask.NextFireTime = nextFire
	case tree.AlterTaskSetWhen:
		sqlTask.GateCondition = stmt.GateCondition
	case tree.AlterTaskSetRetry:
		sqlTask.RetryLimit = int(stmt.RetryLimit)
	case tree.AlterTaskSetTimeout:
		timeoutSeconds, err := taskservice.ParseSQLTaskTimeout(stmt.Timeout)
		if err != nil {
			return err
		}
		sqlTask.TimeoutSeconds = timeoutSeconds
	}
	sqlTask.UpdatedAt = now
	_, err = store.UpdateSQLTask(ctx, []taskservice.SQLTask{sqlTask}, taskservice.WithTaskIDCond(taskservice.EQ, sqlTask.TaskID))
	return err
}

func handleDropSQLTask(ctx context.Context, ses *Session, stmt *tree.DropSQLTask) error {
	ts, err := getSQLTaskService(ctx, ses)
	if err != nil {
		return err
	}
	store := ts.GetStorage()
	tasks, err := store.QuerySQLTask(ctx,
		taskservice.WithTaskName(taskservice.EQ, string(stmt.Name)),
		taskservice.WithAccountID(taskservice.EQ, ses.GetAccountId()),
	)
	if err != nil {
		return err
	}
	if len(tasks) == 0 && stmt.IfExists {
		return nil
	}
	if len(tasks) != 1 {
		return moerr.NewInternalErrorf(ctx, "sql task %s not found", string(stmt.Name))
	}
	sqlTask := tasks[0]
	_, err = store.DeleteSQLTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, sqlTask.TaskID))
	return err
}

func handleExecuteSQLTask(ctx context.Context, ses *Session, stmt *tree.ExecuteSQLTask) error {
	ts, err := getSQLTaskService(ctx, ses)
	if err != nil {
		return err
	}
	sqlTask, err := getSQLTaskByName(ctx, ts.GetStorage(), string(stmt.Name), ses.GetAccountId())
	if err != nil {
		return err
	}
	spec := &pbtask.SQLTaskContext{
		TaskId:         sqlTask.TaskID,
		TaskName:       sqlTask.TaskName,
		AccountId:      sqlTask.AccountID,
		DatabaseName:   sqlTask.DatabaseName,
		SQLBody:        sqlTask.SQLBody,
		GateCondition:  sqlTask.GateCondition,
		RetryLimit:     uint32(sqlTask.RetryLimit),
		TimeoutSeconds: uint32(sqlTask.TimeoutSeconds),
		Creator:        sqlTask.Creator,
		CreatorUserId:  sqlTask.CreatorUserID,
		CreatorRoleId:  sqlTask.CreatorRoleID,
		TriggerType:    taskservice.SQLTaskTriggerManual,
		ScheduledAt:    time.Now().UnixMilli(),
	}
	executor := taskservice.NewSQLTaskExecutor(func() ie.InternalExecutor {
		return NewInternalExecutor(ses.GetService())
	}, ts, ses.GetService())
	if err := executor.ExecuteContext(detachSQLTaskExecuteContext(ctx), spec, true); err != nil {
		if err == taskservice.ErrSQLTaskOverlap {
			return moerr.NewInternalError(ctx, "sql task is already running")
		}
		return err
	}
	return nil
}

func handleShowSQLTasks(ctx context.Context, ses *Session, _ *ExecCtx, _ *tree.ShowSQLTasks) error {
	ts, err := getSQLTaskService(ctx, ses)
	if err != nil {
		return err
	}
	tasks, err := ts.GetStorage().QuerySQLTask(ctx, taskservice.WithAccountID(taskservice.EQ, ses.GetAccountId()))
	if err != nil {
		return err
	}
	runs, err := ts.GetStorage().QuerySQLTaskRun(ctx, taskservice.WithAccountID(taskservice.EQ, ses.GetAccountId()))
	if err != nil {
		return err
	}
	lastRun := make(map[uint64]taskservice.SQLTaskRun, len(runs))
	for _, run := range runs {
		lastRun[run.TaskID] = run
	}

	mrs := ses.GetMysqlResultSet()
	for _, col := range sqlTaskShowCols {
		mrs.AddColumn(col)
	}
	for _, sqlTask := range tasks {
		run := lastRun[sqlTask.TaskID]
		row := []any{
			sqlTask.TaskName,
			taskservice.BuildSQLTaskCronSpec(sqlTask.CronExpr, sqlTask.Timezone),
			boolToInt(sqlTask.Enabled),
			sqlTask.GateCondition,
			sqlTask.RetryLimit,
			formatSQLTaskTimeout(sqlTask.TimeoutSeconds),
			sqlTask.CreatedAt.String(),
			run.Status,
			formatNullableTime(run.StartedAt),
		}
		mrs.AddRow(row)
	}
	return trySaveQueryResult(ctx, ses, mrs)
}

func handleShowSQLTaskRuns(ctx context.Context, ses *Session, _ *ExecCtx, stmt *tree.ShowSQLTaskRuns) error {
	ts, err := getSQLTaskService(ctx, ses)
	if err != nil {
		return err
	}
	conds := []taskservice.Condition{
		taskservice.WithAccountID(taskservice.EQ, ses.GetAccountId()),
	}
	if stmt.HasTask {
		conds = append(conds, taskservice.WithTaskName(taskservice.EQ, string(stmt.TaskName)))
	}
	if stmt.HasLimit {
		conds = append(conds, taskservice.WithLimitCond(int(stmt.Limit)))
	}
	runs, err := ts.GetStorage().QuerySQLTaskRun(ctx, conds...)
	if err != nil {
		return err
	}

	mrs := ses.GetMysqlResultSet()
	for _, col := range sqlTaskRunShowCols {
		mrs.AddColumn(col)
	}
	for _, run := range runs {
		mrs.AddRow([]any{
			run.RunID,
			run.TaskName,
			run.TriggerType,
			run.Status,
			formatNullableTime(run.StartedAt),
			formatNullableTime(run.FinishedAt),
			run.DurationSeconds,
			run.AttemptNumber,
			run.RowsAffected,
			run.ErrorMessage,
		})
	}
	return trySaveQueryResult(ctx, ses, mrs)
}

func detachSQLTaskExecuteContext(ctx context.Context) context.Context {
	return context.WithoutCancel(ctx)
}

func buildSQLTaskFromStmt(ses *Session, stmt *tree.CreateSQLTask, timeoutSeconds int, now time.Time) (taskservice.SQLTask, error) {
	tenant := ses.GetTenantInfo()
	timezone := normalizeSQLTaskTimezone(stmt.Timezone)
	nextFire, err := taskservice.NextSQLTaskFireTime(newSQLTaskCronParser(), stmt.CronExpr, timezone, now)
	if err != nil {
		return taskservice.SQLTask{}, err
	}
	sqlTask := taskservice.SQLTask{
		TaskName:       string(stmt.Name),
		AccountID:      ses.GetAccountId(),
		DatabaseName:   ses.GetDatabaseName(),
		CronExpr:       stmt.CronExpr,
		Timezone:       timezone,
		SQLBody:        stmt.SQLBody,
		GateCondition:  stmt.GateCondition,
		RetryLimit:     int(stmt.RetryLimit),
		TimeoutSeconds: timeoutSeconds,
		Enabled:        true,
		NextFireTime:   nextFire,
		TriggerCount:   0,
		Creator:        ses.GetUserName(),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if tenant != nil {
		sqlTask.CreatorUserID = tenant.GetUserID()
		sqlTask.CreatorRoleID = tenant.GetDefaultRoleID()
	}
	return sqlTask, nil
}

func normalizeSQLTaskTimezone(timezone string) string {
	if strings.TrimSpace(timezone) == "" {
		return "UTC"
	}
	return timezone
}

func getSQLTaskService(ctx context.Context, ses *Session) (taskservice.TaskService, error) {
	ts := getPu(ses.GetService()).TaskService
	if ts == nil {
		return nil, moerr.NewInternalError(ctx, "task service not ready yet, please try again later.")
	}
	return ts, nil
}

func getSQLTaskByName(ctx context.Context, store taskservice.TaskStorage, name string, accountID uint32) (taskservice.SQLTask, error) {
	tasks, err := store.QuerySQLTask(ctx,
		taskservice.WithTaskName(taskservice.EQ, name),
		taskservice.WithAccountID(taskservice.EQ, accountID),
	)
	if err != nil {
		return taskservice.SQLTask{}, err
	}
	if len(tasks) != 1 {
		return taskservice.SQLTask{}, moerr.NewInternalErrorf(ctx, "sql task %s not found", name)
	}
	return tasks[0], nil
}

func newSQLTaskCronParser() cron.Parser {
	return cron.NewParser(
		cron.Second |
			cron.Minute |
			cron.Hour |
			cron.Dom |
			cron.Month |
			cron.Dow |
			cron.Descriptor,
	)
}

func newSQLTaskColumn(name string, typ defines.MysqlType) Column {
	col := &MysqlColumn{}
	col.SetName(name)
	col.SetColumnType(typ)
	return col
}

func formatSQLTaskTimeout(seconds int) string {
	if seconds <= 0 {
		return ""
	}
	return (time.Duration(seconds) * time.Second).String()
}

func formatNullableTime(ts time.Time) string {
	if ts.IsZero() {
		return ""
	}
	return ts.String()
}

func boolToInt(v bool) int8 {
	if v {
		return 1
	}
	return 0
}
