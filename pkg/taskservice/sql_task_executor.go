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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	mysqlparser "github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

const sqlTaskGateTimeout = 30 * time.Second

type SQLTaskExecutor struct {
	ieFactory   func() ie.InternalExecutor
	taskService TaskService
	runnerCN    string
}

func NewSQLTaskExecutor(
	ieFactory func() ie.InternalExecutor,
	taskService TaskService,
	runnerCN string,
) *SQLTaskExecutor {
	return &SQLTaskExecutor{
		ieFactory:   ieFactory,
		taskService: taskService,
		runnerCN:    runnerCN,
	}
}

func (e *SQLTaskExecutor) TaskExecutor() TaskExecutor {
	return func(ctx context.Context, value task.Task) error {
		if disposition, ok := ctx.Value(sqlTaskDispositionContextKey{}).(*sqlTaskRunnerDisposition); ok {
			disposition.handled = true
		}
		asyncTask, ok := value.(*task.AsyncTask)
		if !ok {
			return moerr.NewInternalErrorNoCtx("invalid sql task payload")
		}
		service := sqlTaskServiceOwner(e.taskService)
		if service == nil {
			deferSQLTask(ctx)
			return ErrNotReady
		}
		claim := SQLTaskClaim{ID: asyncTask.ID, Runner: asyncTask.TaskRunner, Epoch: asyncTask.Epoch, ParentTaskID: asyncTask.ParentTaskID, MetadataID: asyncTask.Metadata.ID}
		guardKey := sqlTaskGuardKey{kind: 1, value: claim.ParentTaskID}
		if guardKey.value == "" || len(guardKey.value) > 50 {
			guardKey = sqlTaskGuardKey{kind: 2, value: strconv.FormatUint(claim.ID, 10)}
		}
		guard := service.sqlRecovery.tryGuard(guardKey, 50)
		if guard == nil {
			deferSQLTask(ctx)
			return nil // defer the unclaimed firing
		}
		defer service.sqlRecovery.releaseGuard(guard)
		spec := new(task.SQLTaskContext)
		if err := spec.Unmarshal(asyncTask.Metadata.Context); err != nil {
			if !service.finishUnclaimedSQLTask(ctx, claim, err, 0) {
				deferSQLTask(ctx)
			}
			return nil
		}
		if spec.TriggerType != SQLTaskTriggerScheduled {
			if !service.finishUnclaimedSQLTask(ctx, claim, moerr.NewInvalidStateNoCtx("invalid scheduled SQL task trigger"), 0) {
				deferSQLTask(ctx)
			}
			return nil
		}
		ctx = context.WithValue(ctx, sqlTaskScheduledGuardContextKey{}, guard)
		err := e.executeContext(ctx, spec, true, &claim)
		if err == ErrSQLTaskStaleFiring || err == ErrSQLTaskNotFound || err == ErrSQLTaskOverlap {
			var cleanupTaskID uint64
			if err == ErrSQLTaskStaleFiring && claim.ParentTaskID == fmt.Sprintf("sql-task:%d", spec.TaskId) {
				cleanupTaskID = spec.TaskId
			}
			if !service.finishUnclaimedSQLTask(ctx, claim, nil, cleanupTaskID) {
				deferSQLTask(ctx)
			}
			return nil
		}
		if err == ErrSQLTaskClaimed || err == ErrSQLTaskRecoveryCapacity {
			deferSQLTask(ctx)
			return nil
		}
		if err != nil {
			if disposition, ok := ctx.Value(sqlTaskDispositionContextKey{}).(*sqlTaskRunnerDisposition); ok && !disposition.owned {
				disposition.deferred = true
				disposition.failed = ctx.Err() == nil && err != errSQLTaskStoreBusy
			}
		}
		return err
	}
}

type sqlTaskDispositionContextKey struct{}
type sqlTaskScheduledGuardContextKey struct{}
type sqlTaskRunnerDisposition struct{ handled, owned, deferred, failed bool }

func deferSQLTask(ctx context.Context) {
	if disposition, ok := ctx.Value(sqlTaskDispositionContextKey{}).(*sqlTaskRunnerDisposition); ok {
		disposition.deferred = true
	}
}

func deferFailedSQLTask(ctx context.Context) {
	if disposition, ok := ctx.Value(sqlTaskDispositionContextKey{}).(*sqlTaskRunnerDisposition); ok && !disposition.owned {
		disposition.deferred = true
		disposition.failed = ctx.Err() == nil
	}
}

func (e *SQLTaskExecutor) ExecuteContext(ctx context.Context, spec *task.SQLTaskContext, overlapAsError bool) error {
	return e.executeContext(ctx, spec, overlapAsError, nil)
}

func (e *SQLTaskExecutor) executeContext(ctx context.Context, spec *task.SQLTaskContext, overlapAsError bool, claim *SQLTaskClaim) error {
	if spec == nil {
		return moerr.NewInternalErrorNoCtx("sql task context is nil")
	}
	// A known result must always fit in its bounded recovery receipt.
	if len(e.runnerCN) > sqlTaskRunnerMaxBytes || claim != nil &&
		(len(claim.Runner) > sqlTaskAsyncRunnerMaxBytes || len(claim.ParentTaskID) > 50 || len(claim.MetadataID) > 50) {
		return moerr.NewInvalidInputNoCtx("SQL task execution identifier is too long")
	}
	if spec.TriggerType == SQLTaskTriggerScheduled {
		ctx = context.WithValue(ctx, defines.SQLTaskBackgroundKey{}, true)
	}
	service := sqlTaskServiceOwner(e.taskService)
	if service == nil {
		return ErrNotReady
	}
	recovery := service.sqlRecovery
	ctx, cancelExecution := context.WithCancel(ctx)
	stopExecution := context.AfterFunc(recovery.ctx, cancelExecution)
	defer func() { stopExecution(); cancelExecution() }()
	class := sqlTaskManualReceipt
	if spec.TriggerType == SQLTaskTriggerScheduled {
		class = sqlTaskScheduledReceipt
	}
	var manualGuard *sqlTaskManualGuard
	if class == sqlTaskManualReceipt {
		manualGuard, _ = ctx.Value(sqlTaskManualGuardContextKey{}).(*sqlTaskManualGuard)
		if manualGuard == nil || manualGuard.owner != recovery || manualGuard.guard.key.account != spec.AccountId || manualGuard.guard.key.value != spec.TaskName {
			var release func()
			var err error
			ctx, release, err = BeginSQLTaskManualAdmission(ctx, e.taskService, spec.AccountId, spec.TaskName)
			if err != nil {
				return err
			}
			defer release()
			manualGuard = ctx.Value(sqlTaskManualGuardContextKey{}).(*sqlTaskManualGuard)
		}
	}

	sqlTask := SQLTask{
		TaskID:         spec.TaskId,
		TaskName:       spec.TaskName,
		AccountID:      spec.AccountId,
		DatabaseName:   spec.DatabaseName,
		SQLBody:        spec.SQLBody,
		GateCondition:  spec.GateCondition,
		RetryLimit:     int(spec.RetryLimit),
		TimeoutSeconds: int(spec.TimeoutSeconds),
		Creator:        spec.Creator,
		CreatorUserID:  spec.CreatorUserId,
		CreatorRoleID:  spec.CreatorRoleId,
	}
	ctx = attachSQLTaskContext(ctx, sqlTask)

	var nextRun *SQLTaskRun
	var credit *sqlTaskCredit
	for attempt := 1; attempt <= sqlTask.RetryLimit+1; attempt++ {
		run := SQLTaskRun{
			TaskID:        sqlTask.TaskID,
			TaskName:      sqlTask.TaskName,
			AccountID:     sqlTask.AccountID,
			ScheduledAt:   time.UnixMilli(spec.ScheduledAt),
			StartedAt:     time.Now(),
			Status:        SQLTaskStatusRunning,
			TriggerType:   spec.TriggerType,
			AttemptNumber: attempt,
			GateResult:    true,
			RunnerCN:      e.runnerCN,
		}
		if nextRun != nil {
			run = *nextRun
		} else {
			admission := &SQLTaskRunAdmission{TriggerSequence: spec.TriggerCount, Reserve: func(account uint32, name string) bool {
				credit = recovery.reserve(class, account, name)
				return credit != nil
			}}
			if claim != nil {
				admission.AsyncTaskID, admission.RunnerCN, admission.Epoch = claim.ID, claim.Runner, claim.Epoch
				admission.ParentTaskID, admission.MetadataID = claim.ParentTaskID, claim.MetadataID
			}
			admitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			runID, err := service.store.AcquireSQLTaskRun(admitCtx, sqlTask, run, admission)
			cancel()
			if manualGuard != nil {
				manualGuard.owner.releaseGuard(manualGuard.guard)
				manualGuard = nil
			}
			run.RunID = runID
			if err != nil {
				if runID != 0 && credit != nil {
					freezeSQLTaskRun(&run, moerr.NewInternalErrorNoCtx("admission outcome was uncertain; SQL did not start"), false)
					run.Status = SQLTaskStatusFailed
					if pending := e.publishSQLTaskResult(service, credit, claim, run, SQLTaskReceiptAdmission, err); pending != nil {
						return pending
					}
					return err
				}
				recovery.release(credit)
				if err == ErrSQLTaskOverlap && !overlapAsError {
					return nil
				}
				return err
			}
			if runID == 0 || credit == nil {
				recovery.release(credit)
				return ErrNotReady
			}
			if disposition, ok := ctx.Value(sqlTaskDispositionContextKey{}).(*sqlTaskRunnerDisposition); ok {
				disposition.owned = true
			}
			if guard, ok := ctx.Value(sqlTaskScheduledGuardContextKey{}).(*sqlTaskGuard); ok {
				recovery.releaseGuard(guard)
			}
		}
		if err := ctx.Err(); err != nil {
			run.Status = SQLTaskStatusTimeout
			freezeSQLTaskRun(&run, err, true)
			if pending := e.publishSQLTaskResult(service, credit, claim, run, SQLTaskReceiptTerminal, err); pending != nil {
				return pending
			}
			return err
		}
		allowed, err := e.evaluateGate(ctx, sqlTask)
		if err == nil && !allowed {
			run.Status = SQLTaskStatusSkipped
			run.GateResult = false
			freezeSQLTaskRun(&run, nil, false)
			return e.publishSQLTaskResult(service, credit, claim, run, SQLTaskReceiptTerminal, nil)
		}
		if err != nil {
			run.GateResult = false
		} else if err = ctx.Err(); err == nil {
			run.RowsAffected, err = e.executeStatements(ctx, sqlTask)
		}
		if err == nil {
			run.Status = SQLTaskStatusSuccess
			freezeSQLTaskRun(&run, nil, false)
			return e.publishSQLTaskResult(service, credit, claim, run, SQLTaskReceiptTerminal, nil)
		}
		if unknown := moerr.FindTxnUnknown(err); unknown != nil {
			run.ErrorCode, run.ErrorMessage = int(unknown.ErrorCode()), util.Abbreviate(err.Error(), 4093)
			if pending := e.publishSQLTaskResult(service, credit, claim, run, SQLTaskReceiptUnknown, err); pending != nil {
				return sqlTaskUnknownCommitResult(pending, unknown)
			}
			return sqlTaskUnknownCommitResult(err, unknown)
		}
		timedOut := ctx.Err() != nil || strings.Contains(strings.ToLower(err.Error()), "deadline exceeded")
		run.Status = SQLTaskStatusFailed
		freezeSQLTaskRun(&run, err, timedOut)
		if attempt <= sqlTask.RetryLimit && ctx.Err() == nil {
			next := run
			next.RunID, next.Status, next.AttemptNumber = 0, SQLTaskStatusRunning, attempt+1
			next.StartedAt, next.FinishedAt = time.Now(), time.Time{}
			next.DurationSeconds, next.RowsAffected, next.ErrorCode, next.ErrorMessage, next.GateResult = 0, 0, 0, "", true
			result, transitionErr := retrySQLTaskTransition(ctx, service.store, SQLTaskRetryRequest{Previous: run, Next: next, Claim: claim})
			if transitionErr == nil && result.Continued {
				next.RunID = result.RunID
				nextRun = &next
				continue
			}
			if transitionErr == nil {
				recovery.release(credit)
				reason := moerr.NewInvalidStateNoCtx("SQL task retry was not executed because ownership changed")
				service.rt.Logger().WarnEvent(sqlTaskRetryIncomplete, zap.Uint64("run-id", run.RunID), zap.Error(reason))
				return errors.Join(err, reason)
			}
			transitionErr = errors.Join(moerr.NewInternalErrorNoCtx("SQL task retry was not executed"), transitionErr)
			service.rt.Logger().WarnEvent(sqlTaskRetryIncomplete, zap.Uint64("run-id", run.RunID),
				zap.String("error", util.Abbreviate(transitionErr.Error(), 997)))
			cause := errors.Join(err, transitionErr)
			if result.CommitAttempted && result.RunID != 0 {
				next.RunID, next.Status = result.RunID, SQLTaskStatusFailed
				freezeSQLTaskRun(&next, moerr.NewInternalErrorNoCtx("retry admission was uncertain; SQL did not start"), false)
				receipt := &sqlTaskReceipt{SQLTaskReceiptRequest: SQLTaskReceiptRequest{Kind: SQLTaskReceiptRetryAdmission, Run: run, NextRun: next, EndAt: time.Now().UnixMilli()}, credit: credit}
				if claim != nil {
					receipt.Claim = *claim
				}
				queued := recovery.enqueue(receipt)
				if !queued {
					service.abandonSQLTaskReceipt(receipt)
				}
				return &SQLTaskMetadataPendingError{RunID: result.RunID, Recovering: queued, Cause: cause}
			}
			if pending := e.publishSQLTaskResult(service, credit, claim, run, SQLTaskReceiptTerminal, cause); pending != nil {
				return pending
			}
			return cause
		}
		if pending := e.publishSQLTaskResult(service, credit, claim, run, SQLTaskReceiptTerminal, err); pending != nil {
			return pending
		}
		return err
	}
	return nil
}

// SQLTaskMetadataPendingError reports a known business result whose metadata
// has not yet been confirmed. Cause is nil only when the SQL body succeeded.
type SQLTaskMetadataPendingError struct {
	RunID      uint64
	Status     string
	Recovering bool
	Cause      error
}

func (e *SQLTaskMetadataPendingError) Error() string {
	message := fmt.Sprintf("SQL task run %d metadata is pending", e.RunID)
	if e.Recovering {
		message += "; the running task service will retry when storage recovers"
	} else {
		message += "; automatic recovery could not be scheduled"
	}
	if e.Cause != nil {
		return e.Cause.Error() + "; " + message
	}
	return message
}

func (e *SQLTaskMetadataPendingError) Unwrap() error { return e.Cause }

// publishSQLTaskResult returns nil once the frozen metadata result is known to
// be durable. Otherwise it transfers its reserved credit to the service worker.
func (e *SQLTaskExecutor) publishSQLTaskResult(service *taskService, credit *sqlTaskCredit, claim *SQLTaskClaim, run SQLTaskRun, kind SQLTaskReceiptKind, execErr error) error {
	recovery := service.sqlRecovery
	receipt := &sqlTaskReceipt{SQLTaskReceiptRequest: SQLTaskReceiptRequest{Kind: kind, Run: run, EndAt: time.Now().UnixMilli()}, credit: credit}
	if claim != nil {
		receipt.Claim = *claim
	}
	guard := recovery.tryGuard(sqlTaskGuardKey{account: run.AccountID, value: run.TaskName}, 512)
	if guard != nil {
		ctx, cancel := context.WithTimeout(recovery.ctx, 5*time.Second)
		resolved, err := e.taskService.GetStorage().PublishSQLTaskReceipt(ctx, receipt.SQLTaskReceiptRequest)
		cancel()
		recovery.releaseGuard(guard)
		if err == nil && resolved {
			recovery.release(credit)
			return nil
		}
	}
	queued := recovery.enqueue(receipt)
	if !queued {
		service.abandonSQLTaskReceipt(receipt)
	}
	return &SQLTaskMetadataPendingError{RunID: run.RunID, Status: run.Status, Recovering: queued, Cause: execErr}
}

// Keep the unknown-commit identity visible to the SQL protocol even when an
// earlier joined cleanup error has a different MatrixOne error code.
func sqlTaskUnknownCommitResult(err error, unknown *moerr.Error) error {
	var identity *moerr.Error
	if errors.As(err, &identity) && identity != nil && identity.ErrorCode() == moerr.ErrTxnUnknown {
		return err
	}
	return &sqlTaskUnknownCommitError{error: err, unknown: unknown}
}

type sqlTaskUnknownCommitError struct {
	error
	unknown *moerr.Error
}

func (e *sqlTaskUnknownCommitError) Unwrap() error { return e.error }

func (e *sqlTaskUnknownCommitError) As(target any) bool {
	if out, ok := target.(**moerr.Error); ok {
		*out = e.unknown
		return true
	}
	return false
}

func attachSQLTaskContext(ctx context.Context, sqlTask SQLTask) context.Context {
	return defines.AttachAccount(ctx, sqlTask.AccountID, sqlTask.CreatorUserID, sqlTask.CreatorRoleID)
}

func (e *SQLTaskExecutor) evaluateGate(ctx context.Context, sqlTask SQLTask) (bool, error) {
	if strings.TrimSpace(sqlTask.GateCondition) == "" {
		return true, nil
	}

	gateCtx, cancel := context.WithTimeoutCause(ctx, sqlTaskGateTimeout, moerr.CauseInternalExecutorQuery)
	defer cancel()

	query := fmt.Sprintf("select (%s) as gate_result", sqlTask.GateCondition)
	res := e.ieFactory().Query(gateCtx, query, DefinerOpts(sqlTask))
	if res.Error() != nil {
		return false, res.Error()
	}
	if res.RowCount() == 0 || res.ColumnCount() == 0 {
		return false, nil
	}
	value, err := res.Value(gateCtx, 0, 0)
	if err != nil {
		return false, err
	}
	return toBool(value)
}

func (e *SQLTaskExecutor) executeStatements(ctx context.Context, sqlTask SQLTask) (int64, error) {
	body := strings.TrimSpace(sqlTask.SQLBody)
	if body == "" {
		return 0, nil
	}

	runCtx := ctx
	cancel := func() {}
	if sqlTask.TimeoutSeconds > 0 {
		runCtx, cancel = context.WithTimeoutCause(ctx, time.Duration(sqlTask.TimeoutSeconds)*time.Second, moerr.CauseInternalExecutorExec)
	}
	defer cancel()

	stmts, err := mysqlparser.Parse(runCtx, body, 1)
	if err != nil {
		return 0, err
	}
	exec := e.ieFactory()
	opts := DefinerOpts(sqlTask)
	rowsAffected := int64(0)
	for _, stmt := range stmts {
		sql := tree.StringWithOpts(stmt, dialect.MYSQL, tree.WithSingleQuoteString())
		status, err := execSQLTaskStatement(runCtx, exec, sql, opts)
		if err != nil {
			return rowsAffected, err
		}
		rowsAffected += int64(status.AffectedRows)
	}
	return rowsAffected, nil
}

func execSQLTaskStatement(ctx context.Context, exec ie.InternalExecutor, sql string, opts ie.SessionOverrideOptions) (ie.InternalExecStatus, error) {
	if execWithStatus, ok := exec.(ie.InternalExecutorWithStatus); ok {
		return execWithStatus.ExecWithStatus(ctx, sql, opts)
	}
	return ie.InternalExecStatus{}, exec.Exec(ctx, sql, opts)
}

func freezeSQLTaskRun(run *SQLTaskRun, execErr error, timedOut bool) {
	now := time.Now()
	run.FinishedAt = now.Truncate(time.Second)
	run.DurationSeconds = now.Sub(run.StartedAt).Seconds()
	if execErr != nil {
		run.ErrorMessage = util.Abbreviate(execErr.Error(), 4093)
		var moErr *moerr.Error
		if errors.As(execErr, &moErr) {
			run.ErrorCode = int(moErr.ErrorCode())
		}
	}
	if timedOut {
		run.Status = SQLTaskStatusTimeout
	}
}

func toBool(value any) (bool, error) {
	switch v := value.(type) {
	case nil:
		return false, nil
	case bool:
		return v, nil
	case int8:
		return v != 0, nil
	case int16:
		return v != 0, nil
	case int32:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case int:
		return v != 0, nil
	case uint8:
		return v != 0, nil
	case uint16:
		return v != 0, nil
	case uint32:
		return v != 0, nil
	case uint64:
		return v != 0, nil
	case uint:
		return v != 0, nil
	case float32:
		return v != 0, nil
	case float64:
		return v != 0, nil
	case string:
		lower := strings.ToLower(strings.TrimSpace(v))
		switch lower {
		case "", "0", "false", "f", "no", "n":
			return false, nil
		case "1", "true", "t", "yes", "y":
			return true, nil
		default:
			i, err := strconv.ParseFloat(lower, 64)
			if err != nil {
				return false, err
			}
			return i != 0, nil
		}
	case []byte:
		return toBool(string(v))
	default:
		return false, moerr.NewInternalErrorNoCtxf("unsupported gate result type %T", value)
	}
}
