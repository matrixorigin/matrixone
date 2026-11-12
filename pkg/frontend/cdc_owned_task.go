// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

type cdcCatalogStateConflict struct{ cause error }

func (e *cdcCatalogStateConflict) Error() string { return e.cause.Error() }
func (e *cdcCatalogStateConflict) Unwrap() error { return e.cause }

// A failed catalog operation says nothing about the validity of CDC metadata.
// Retain its underlying error while allowing startup to recover after the
// store becomes available again.
type cdcCatalogAccessError struct{ cause error }

func (e *cdcCatalogAccessError) Error() string { return e.cause.Error() }
func (e *cdcCatalogAccessError) Unwrap() error { return e.cause }

func cdcStartupRetryable(err error) bool {
	var storeErr *taskservice.CDCTaskStorageError
	if errors.As(err, &storeErr) && storeErr.CommitUnknown {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	retryable, classified := cdc.ClassifyRetryableError(err)
	if classified {
		return retryable
	}
	var accessErr *cdcCatalogAccessError
	return errors.As(err, &accessErr) || errors.As(err, &storeErr)
}

func (exec *CDCTaskExecutor) catalogState(ctx context.Context) (string, error) {
	if exec.ie == nil {
		return "", moerr.NewInternalError(ctx, "CDC catalog executor is unavailable")
	}
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	result := exec.ie.Query(ctx, cdc.CDCSQLBuilder.GetTaskStateSQL(exec.taskOwner.Key.AccountId, exec.taskOwner.Key.TaskId), ie.SessionOverrideOptions{})
	if result == nil {
		return "", moerr.NewInternalError(ctx, "CDC catalog state query returned no result")
	}
	if err := result.Error(); err != nil {
		return "", &cdcCatalogAccessError{cause: err}
	}
	if result.RowCount() != 1 {
		return "", moerr.NewInternalErrorf(ctx, "CDC catalog state query returned %d rows", result.RowCount())
	}
	return result.GetString(ctx, 0, 0)
}

// admitStart derives recovery from the durable catalog marker. A failed task
// can only be reopened by an explicit restart claim or local restart request.
func (exec *CDCTaskExecutor) admitStart(ctx context.Context, generation uint64, claimedRestart bool) (restart, terminal bool, err error) {
	_, localRestart := exec.restartCatalogStateForGeneration(generation)
	expectedDaemon := task.TaskStatus_Running
	if localRestart {
		expectedDaemon = task.TaskStatus_RestartRequested
	}
	state, err := exec.catalogState(ctx)
	if err != nil {
		return false, false, err
	}
	if state == cdc.CDCState_Failed && !claimedRestart && !localRestart {
		err = exec.updateOwnedCatalog(ctx, expectedDaemon,
			taskservice.CDCTaskTransition{Status: task.TaskStatus_Error}, state, state, nil, false)
		if err != nil {
			return false, false, err
		}
		return false, true, moerr.NewInternalError(ctx, "CDC task is failed; an explicit restart is required")
	}
	if state != cdc.CDCState_Running && state != cdc.CDCState_Restarting && state != cdc.CDCState_Failed {
		return false, false, moerr.NewInternalErrorf(ctx, "CDC task cannot start from catalog state %s", state)
	}
	if (claimedRestart || localRestart) && state != cdc.CDCState_Restarting {
		err = exec.updateOwnedCatalog(ctx, expectedDaemon,
			taskservice.CDCTaskTransition{Status: expectedDaemon}, state, cdc.CDCState_Restarting, nil, true)
		if err != nil {
			return false, false, err
		}
		state = cdc.CDCState_Restarting
	}
	return state == cdc.CDCState_Restarting, false, nil
}

// updateOwnedCatalog changes an exact catalog state and daemon lease together.
// A nil message preserves the existing user-facing diagnostic.
func (exec *CDCTaskExecutor) updateOwnedCatalog(
	ctx context.Context,
	expectedDaemon task.TaskStatus,
	targetDaemon taskservice.CDCTaskTransition,
	from, to string,
	message *string,
	acceptTarget bool,
) error {
	if exec.taskService == nil {
		return moerr.NewInternalError(ctx, "CDC task service is unavailable")
	}
	owner := exec.taskOwner
	owner.Status = expectedDaemon
	return exec.taskService.UpdateOwnedCDCTask(ctx, owner, targetDaemon, func(ctx context.Context, tx taskservice.SqlExecutor) error {
		fault.TriggerFault(cdcStateTransitionFaultPoint(from, to))
		query := "update mo_catalog.mo_cdc_task set state=?"
		args := []any{to}
		if message != nil {
			value := *message
			if len(value) > cdc.CDCWatermarkErrMsgMaxLen {
				value = value[:cdc.CDCWatermarkErrMsgMaxLen]
			}
			query += ", err_msg=?"
			args = append(args, value)
		}
		query += " where account_id=? and task_id=? and state=?"
		args = append(args, owner.Key.AccountId, owner.Key.TaskId, from)
		result, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return &taskservice.CDCTaskStorageError{Err: err}
		}
		matched, err := result.RowsAffected()
		if err != nil {
			return &taskservice.CDCTaskStorageError{Err: err}
		}
		if matched == 0 && acceptTarget && from != to {
			// Recheck and lock an already-published target in the same
			// transaction as the lease guard, including after a lost ACK.
			result, err = tx.ExecContext(ctx,
				"update mo_catalog.mo_cdc_task set state=state where account_id=? and task_id=? and state=?",
				owner.Key.AccountId, owner.Key.TaskId, to)
			if err != nil {
				return &taskservice.CDCTaskStorageError{Err: err}
			}
			matched, err = result.RowsAffected()
			if err != nil {
				return &taskservice.CDCTaskStorageError{Err: err}
			}
		}
		if matched != 1 {
			return &cdcCatalogStateConflict{cause: moerr.NewInternalErrorf(ctx, "CDC catalog state no longer matches %s", from)}
		}
		return nil
	})
}

// CompleteRestart publishes the in-process restart only if no later control
// request has changed either the catalog state or the daemon lease.
func (exec *CDCTaskExecutor) CompleteRestart(ctx context.Context) error {
	if err := exec.updateOwnedCatalog(ctx, task.TaskStatus_RestartRequested,
		taskservice.CDCTaskTransition{Status: task.TaskStatus_Running, StartedAt: time.Now()},
		cdc.CDCState_Running, cdc.CDCState_Running, nil, false); err != nil {
		return err
	}
	attempt := exec.activeStart()
	if attempt == nil || !exec.isCurrentStartAttempt(attempt) || attempt.restartOwner.Load() != cdcRestartOwnerCompleted {
		return moerr.NewInternalError(ctx, "CDC restart was superseded before activation")
	}
	attempt.ready.Store(true)
	return nil
}

// ReleaseFailedStart keeps the persisted restart admission after its local
// resources have drained, without changing a successor or a newer control.
func (exec *CDCTaskExecutor) ReleaseFailedStart(ctx context.Context, cause error) error {
	if cause == nil {
		return moerr.NewInternalError(ctx, "CDC failed-start release requires a cause")
	}
	var lostOwner *taskservice.CDCTaskOwnerMismatch
	var conflict *cdcCatalogStateConflict
	if errors.As(cause, &lostOwner) || errors.As(cause, &conflict) {
		return nil
	}
	state, err := exec.catalogState(ctx)
	if err != nil {
		return err
	}
	if state == cdc.CDCState_Failed {
		// Terminal startup failure already published Error with the catalog.
		return nil
	}
	retryable := cdcStartupRetryable(cause)
	if state == cdc.CDCState_Running && errors.Is(cause, context.Canceled) && !retryable {
		// Shutdown leaves an ordinary running task for lease recovery.
		return nil
	}
	if !retryable && !errors.Is(cause, context.Canceled) {
		if state != cdc.CDCState_Running && state != cdc.CDCState_Restarting {
			return moerr.NewInternalErrorf(ctx, "CDC failure cannot be published from state %s", state)
		}
		message := cause.Error()
		return exec.updateOwnedCatalog(ctx, task.TaskStatus_Running,
			taskservice.CDCTaskTransition{Status: task.TaskStatus_Error}, state, cdc.CDCState_Failed, &message, false)
	}
	if retryable && state == cdc.CDCState_Running {
		message := cause.Error()
		return exec.updateOwnedCatalog(ctx, task.TaskStatus_Running,
			taskservice.CDCTaskTransition{Status: task.TaskStatus_RestartRequested, ReleaseLease: true},
			state, cdc.CDCState_Restarting, &message, false)
	}
	if state != cdc.CDCState_Restarting {
		return moerr.NewInternalErrorf(ctx, "CDC restart cannot be released from state %s", state)
	}
	return exec.updateOwnedCatalog(ctx, task.TaskStatus_Running,
		taskservice.CDCTaskTransition{Status: task.TaskStatus_RestartRequested, ReleaseLease: true},
		state, cdc.CDCState_Restarting, nil, false)
}
