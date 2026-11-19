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
	"errors"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	pbtask "github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

var (
	sqlTaskSyncCommitFailed = logutil.Event{
		Name: "task.sql.sync.commit.failed", Message: "sql task sync commit timestamp failed",
	}
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
		newSQLTaskColumn("runner_cn", defines.MYSQL_TYPE_VARCHAR),
	}
)

func handleCreateSQLTask(ctx context.Context, ses *Session, stmt *tree.CreateSQLTask) error {
	if ses.GetDatabaseName() == "" {
		return moerr.NewNoDB(ctx)
	}
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
	if _, err = store.AddSQLTask(ctx, sqlTask); err != nil {
		return err
	}
	warnSQLTaskVisibility(ctx, ses)
	return nil
}

func handleAlterSQLTask(ctx context.Context, ses *Session, stmt *tree.AlterSQLTask) error {
	ts, err := getSQLTaskService(ctx, ses)
	if err != nil {
		return err
	}
	store := ts.GetStorage()
	if stmt.Action == tree.AlterTaskResetRun {
		if stmt.RunID <= 0 {
			return moerr.NewInvalidInputNoCtx("run ID must be positive")
		}
		changed, err := taskservice.ForceRecoverSQLTaskRun(ctx, store, ses.GetAccountId(), string(stmt.Name), uint64(stmt.RunID), ses.GetUserName())
		if err != nil {
			return err
		}
		if changed {
			ses.AppendWarningDiagnostic(moerr.ER_UNKNOWN_ERROR, "Run marked UNKNOWN; the current task with this name, if any, was suspended. This does not stop SQL or undo its effects; isolate the old executor before running it again")
			warnSQLTaskVisibility(ctx, ses)
		} else {
			ses.AppendWarningDiagnostic(moerr.ER_UNKNOWN_ERROR, "Run is already terminal; no state changed")
		}
		return nil
	}
	sqlTask, err := getSQLTaskByName(ctx, store, string(stmt.Name), ses.GetAccountId())
	if err != nil {
		return err
	}

	now := time.Now()
	observed := sqlTask
	var action taskservice.SQLTaskAlterAction
	switch stmt.Action {
	case tree.AlterTaskSuspend:
		action = taskservice.SQLTaskSuspend
		sqlTask.Enabled = false
	case tree.AlterTaskResume:
		action = taskservice.SQLTaskResume
		sqlTask.Enabled = true
		nextFire, err := taskservice.NextSQLTaskFireTime(newSQLTaskCronParser(), sqlTask.CronExpr, sqlTask.Timezone, now)
		if err != nil {
			return err
		}
		sqlTask.NextFireTime = nextFire
	case tree.AlterTaskSetSchedule:
		action = taskservice.SQLTaskSetSchedule
		timezone := normalizeSQLTaskTimezone(stmt.Timezone)
		nextFire, err := taskservice.NextSQLTaskFireTime(newSQLTaskCronParser(), stmt.CronExpr, timezone, now)
		if err != nil {
			return err
		}
		sqlTask.CronExpr = stmt.CronExpr
		sqlTask.Timezone = timezone
		sqlTask.NextFireTime = nextFire
	case tree.AlterTaskSetWhen:
		action = taskservice.SQLTaskSetGate
		sqlTask.GateCondition = stmt.GateCondition
	case tree.AlterTaskSetRetry:
		action = taskservice.SQLTaskSetRetry
		sqlTask.RetryLimit = int(stmt.RetryLimit)
	case tree.AlterTaskSetTimeout:
		action = taskservice.SQLTaskSetTimeout
		timeoutSeconds, err := taskservice.ParseSQLTaskTimeout(stmt.Timeout)
		if err != nil {
			return err
		}
		sqlTask.TimeoutSeconds = timeoutSeconds
	default:
		return moerr.NewInvalidInputNoCtx("unknown sql task alter action")
	}
	sqlTask.UpdatedAt = now
	affected, err := store.AlterSQLTask(ctx, observed, sqlTask, action)
	if err != nil {
		return err
	}
	if affected == 0 {
		return moerr.NewInvalidStateNoCtx("sql task changed or was removed during alter; retry the operation")
	}
	warnSQLTaskVisibility(ctx, ses)
	return nil
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
	if _, err = store.DeleteSQLTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, sqlTask.TaskID)); err != nil {
		return err
	}
	warnSQLTaskVisibility(ctx, ses)
	return nil
}

func handleExecuteSQLTask(ctx context.Context, ses *Session, stmt *tree.ExecuteSQLTask) error {
	ts, err := getSQLTaskService(ctx, ses)
	if err != nil {
		return err
	}
	ctx, releaseAdmission, err := taskservice.BeginSQLTaskManualAdmission(ctx, ts, ses.GetAccountId(), string(stmt.Name))
	if err != nil {
		return err
	}
	defer releaseAdmission()
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
	executionContext := detachSQLTaskExecuteContext(ctx)
	executionErr := executor.ExecuteContext(executionContext, spec, true)
	var pending *taskservice.SQLTaskMetadataPendingError
	if errors.As(executionErr, &pending) {
		if pending.Cause != nil {
			return executionErr
		}
		ses.AppendWarningDiagnostic(moerr.ER_UNKNOWN_ERROR, sqlTaskPendingWarning(pending))
		return nil
	}
	// Completion metadata may commit on another CN, including on body failure.
	// Fence the caller's next snapshot before returning the original result.
	visibilityErr := syncSQLTaskMetadataCommitTimestamp(executionContext, ses)
	if err := executionErr; err != nil {
		if err == taskservice.ErrSQLTaskOverlap {
			return moerr.NewInternalError(ctx, "sql task is already running")
		}
		if visibilityErr != nil {
			return errutil.Wrap(err, "task metadata visibility could not be confirmed")
		}
		return err
	}
	if visibilityErr != nil {
		ses.AppendWarningDiagnostic(moerr.ER_UNKNOWN_ERROR, "SQL task completed, but its metadata may not yet be visible on this CN")
	}
	return nil
}

func sqlTaskPendingWarning(pending *taskservice.SQLTaskMetadataPendingError) string {
	if pending.Status == taskservice.SQLTaskStatusSkipped {
		return "SQL task skipped because its condition was false; " + pending.Error() + "; SQL body was not executed"
	}
	return "SQL task succeeded; " + pending.Error() + "; do not repeat the SQL body"
}

func handleShowSQLTasks(ctx context.Context, ses *Session, _ *ExecCtx, _ *tree.ShowSQLTasks) error {
	ts, err := getSQLTaskService(ctx, ses)
	if err != nil {
		return err
	}
	store := ts.GetStorage()
	tasks, err := store.QuerySQLTask(ctx, taskservice.WithAccountID(taskservice.EQ, ses.GetAccountId()))
	if err != nil {
		return err
	}
	taskIDs := make([]uint64, 0, len(tasks))
	for _, sqlTask := range tasks {
		taskIDs = append(taskIDs, sqlTask.TaskID)
	}
	runs, err := store.QueryLatestSQLTaskRun(ctx, ses.GetAccountId(), taskIDs)
	if err != nil {
		return err
	}
	lastRun := make(map[uint64]taskservice.SQLTaskRun, len(tasks))
	for _, run := range runs {
		prev, exists := lastRun[run.TaskID]
		if !exists || run.RunID > prev.RunID {
			lastRun[run.TaskID] = run
		}
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
			int32(boolToInt(sqlTask.Enabled)),
			sqlTask.GateCondition,
			int32(sqlTask.RetryLimit),
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
			int64(run.RunID),
			run.TaskName,
			run.TriggerType,
			run.Status,
			formatNullableTime(run.StartedAt),
			formatNullableTime(run.FinishedAt),
			run.DurationSeconds,
			int32(run.AttemptNumber),
			run.RowsAffected,
			run.ErrorMessage,
			run.RunnerCN,
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
	ts := getPu(ses.GetService()).GetTaskService()
	if ts == nil {
		return nil, moerr.NewInternalError(ctx, "task service not ready yet, please try again later.")
	}
	return ts, nil
}

// SQL task metadata may be written through a different CN. Bound discovery,
// the entire fanout and the local logtail wait with a single budget.
func syncSQLTaskMetadataCommitTimestamp(ctx context.Context, ses *Session) error {
	if ses == nil || ses.proc == nil || ses.proc.Base == nil {
		return nil
	}
	qc := ses.proc.Base.QueryClient
	txnClient := ses.proc.Base.TxnClient
	if qc == nil || txnClient == nil {
		return nil
	}
	ctx, cancel := context.WithTimeoutCause(ctx, 5*time.Second, moerr.CauseSyncLatestCommitT)
	defer cancel()
	cluster, err := clusterservice.GetMOClusterWithContext(ctx, qc.ServiceID())
	if err != nil {
		return err
	}

	addresses := make([]string, 0, 4)
	err = clusterservice.GetCNServiceWithoutWorkingStateWithContext(ctx, cluster,
		clusterservice.NewSelectAll(),
		func(cn metadata.CNService) bool {
			if cn.QueryAddress != "" {
				addresses = append(addresses, cn.QueryAddress)
			}
			return true
		},
	)
	if err != nil {
		return err
	}

	var (
		maxCommitTS timestamp.Timestamp
		hasTS       bool
		incomplete  bool
	)
	for _, addr := range addresses {
		if err := ctx.Err(); err != nil {
			return err
		}
		req := qc.NewRequest(querypb.CmdMethod_GetCommit)
		resp, err := qc.SendMessage(ctx, addr, req)
		if err != nil {
			if resp != nil {
				qc.Release(resp)
			}
			incomplete = true
			v2.SQLTaskSyncCommitFailureCounter.Inc()
			sqlTaskSyncCommitFailed.WarnLazy(func() []zap.Field {
				return []zap.Field{zap.String("address", addr), zap.Error(err)}
			})
			continue
		}
		if resp == nil {
			incomplete = true
			continue
		}
		if commitResp := resp.GetCommit; commitResp != nil {
			if !hasTS || maxCommitTS.Less(commitResp.CurrentCommitTS) {
				maxCommitTS = commitResp.CurrentCommitTS
				hasTS = true
			}
		} else {
			incomplete = true
		}
		qc.Release(resp)
	}

	if hasTS {
		syncer, ok := txnClient.(interface {
			SyncLatestCommitTSContext(context.Context, timestamp.Timestamp) error
		})
		if !ok {
			return moerr.NewInternalErrorNoCtx("context-aware SQL task visibility sync is unavailable")
		}
		if err := syncer.SyncLatestCommitTSContext(ctx, maxCommitTS); err != nil {
			return err
		}
	}
	if incomplete || !hasTS {
		return moerr.NewInternalErrorNoCtx("SQL task commit timestamp fanout was incomplete")
	}
	return nil
}

func warnSQLTaskVisibility(ctx context.Context, ses *Session) {
	if err := syncSQLTaskMetadataCommitTimestamp(ctx, ses); err != nil {
		ses.AppendWarningDiagnostic(moerr.ER_UNKNOWN_ERROR, "SQL task metadata changed, but may not yet be visible on this CN")
	}
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
