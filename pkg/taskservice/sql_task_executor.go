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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
		asyncTask, ok := value.(*task.AsyncTask)
		if !ok {
			return moerr.NewInternalErrorNoCtx("invalid sql task payload")
		}
		spec := new(task.SQLTaskContext)
		if err := spec.Unmarshal(asyncTask.Metadata.Context); err != nil {
			return err
		}
		err := e.ExecuteContext(ctx, spec, false)
		if err == ErrSQLTaskOverlap {
			return nil
		}
		return err
	}
}

func (e *SQLTaskExecutor) ExecuteContext(ctx context.Context, spec *task.SQLTaskContext, overlapAsError bool) error {
	if spec == nil {
		return moerr.NewInternalErrorNoCtx("sql task context is nil")
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

	var lastErr error
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

		runID, err := e.taskService.GetStorage().AcquireSQLTaskRun(ctx, sqlTask, run)
		if err != nil {
			if err == ErrSQLTaskOverlap && !overlapAsError {
				return nil
			}
			return err
		}
		run.RunID = runID

		allowed, err := e.evaluateGate(ctx, sqlTask)
		if err != nil {
			run.Status = SQLTaskStatusFailed
			run.GateResult = false
			if _, completeErr := e.finishRun(ctx, &run, err, false); completeErr != nil {
				return completeErr
			}
			lastErr = err
		} else if !allowed {
			run.Status = SQLTaskStatusSkipped
			run.GateResult = false
			if _, completeErr := e.finishRun(ctx, &run, nil, false); completeErr != nil {
				return completeErr
			}
			return nil
		} else {
			err = e.executeStatements(ctx, sqlTask)
			if err == nil {
				run.Status = SQLTaskStatusSuccess
				if _, completeErr := e.finishRun(ctx, &run, nil, false); completeErr != nil {
					return completeErr
				}
				return nil
			}

			timedOut := ctx.Err() != nil || strings.Contains(strings.ToLower(err.Error()), "deadline exceeded")
			if timedOut {
				run.Status = SQLTaskStatusTimeout
			} else {
				run.Status = SQLTaskStatusFailed
			}
			if _, completeErr := e.finishRun(ctx, &run, err, timedOut); completeErr != nil {
				return completeErr
			}
			lastErr = err
		}

		if attempt > sqlTask.RetryLimit {
			break
		}
	}
	return lastErr
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

func (e *SQLTaskExecutor) executeStatements(ctx context.Context, sqlTask SQLTask) error {
	body := strings.TrimSpace(sqlTask.SQLBody)
	if body == "" {
		return nil
	}

	runCtx := ctx
	cancel := func() {}
	if sqlTask.TimeoutSeconds > 0 {
		runCtx, cancel = context.WithTimeoutCause(ctx, time.Duration(sqlTask.TimeoutSeconds)*time.Second, moerr.CauseInternalExecutorExec)
	}
	defer cancel()

	stmts, err := mysqlparser.Parse(runCtx, body, 1)
	if err != nil {
		return err
	}
	exec := e.ieFactory()
	opts := DefinerOpts(sqlTask)
	for _, stmt := range stmts {
		sql := tree.String(stmt, dialect.MYSQL)
		if err := exec.Exec(runCtx, sql, opts); err != nil {
			return err
		}
	}
	return nil
}

func (e *SQLTaskExecutor) finishRun(ctx context.Context, run *SQLTaskRun, execErr error, timedOut bool) (int, error) {
	run.FinishedAt = time.Now()
	run.DurationSeconds = run.FinishedAt.Sub(run.StartedAt).Seconds()
	if execErr != nil {
		run.ErrorMessage = execErr.Error()
		if moErr, ok := execErr.(*moerr.Error); ok {
			run.ErrorCode = int(moErr.ErrorCode())
		}
	}
	if timedOut {
		run.Status = SQLTaskStatusTimeout
	}
	return e.taskService.GetStorage().CompleteSQLTaskRun(ctx, *run)
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
