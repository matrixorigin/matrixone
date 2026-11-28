// Copyright 2026 Matrix Origin
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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const maxExecutionTime = "max_execution_time"

// startMaxExecutionTimer scopes the deadline to one top-level client SELECT.
// The caller stops it before transaction finalization and the final response.
func startMaxExecutionTimer(ses *Session, execCtx *ExecCtx) (func(error) error, error) {
	stmt := execCtx.stmt
	switch stmt.(type) {
	case *tree.Select, *tree.Execute:
	default:
		return nil, nil
	}
	if execCtx.proc == nil || !isTopLevelClientStatement(ses, execCtx, execCtx.input) ||
		!ses.GetFromRealUser() || ses.GetIsInternal() {
		return nil, nil
	}

	value, err := ses.GetSessionSysVar(maxExecutionTime)
	if err != nil {
		return nil, err
	}
	milliseconds, ok := value.(int64)
	if !ok {
		return nil, moerr.NewInternalErrorf(execCtx.reqCtx,
			"system variable %s has unexpected type %T", maxExecutionTime, value)
	}
	if milliseconds == 0 {
		return nil, nil
	}

	// COM_STMT_EXECUTE already carries the inner AST. SQL EXECUTE resolves
	// the saved AST here and leaves missing statements to the normal path.
	if execute, ok := stmt.(*tree.Execute); ok {
		prepared, err := ses.GetPrepareStmt(execCtx.reqCtx, string(execute.Name))
		if err != nil {
			return nil, nil
		}
		stmt = prepared.PrepareStmt
	}
	if !isReadOnlySelectForTimeout(stmt) {
		return nil, nil
	}

	parentCtx := execCtx.reqCtx
	proc := execCtx.proc
	previousTopCtx := proc.GetTopContext()
	timeoutErr := moerr.NewQueryTimeout(parentCtx)
	timeoutCtx, cancel := context.WithTimeoutCause(parentCtx, time.Duration(milliseconds)*time.Millisecond, timeoutErr)
	execCtx.reqCtx = timeoutCtx
	execCtx.txnAdmissionCtx = timeoutCtx
	proc.ReplaceTopCtx(timeoutCtx)
	proc.ResetQueryContext()

	return func(stmtErr error) error {
		cancel()
		execCtx.reqCtx = parentCtx
		execCtx.txnAdmissionCtx = nil
		proc.ReplaceTopCtx(previousTopCtx)
		proc.ResetQueryContext()
		// Keep the original error if it requires a full transaction rollback.
		if context.Cause(timeoutCtx) == timeoutErr && !isErrorRollbackWholeTxn(stmtErr) {
			return timeoutErr
		}
		return stmtErr
	}, nil
}

func isReadOnlySelectForTimeout(stmt tree.Statement) bool {
	sel, ok := stmt.(*tree.Select)
	return ok && !selectHasWriteIntent(sel)
}

// Like the planner, look through parentheses and set-operation branches for
// SELECTs with explicit row locks.
func selectHasWriteIntent(stmt tree.SelectStatement) bool {
	switch sel := stmt.(type) {
	case *tree.Select:
		return sel.HasLockingRead || sel.IsPerform || sel.Ep != nil ||
			(sel.SelectLockInfo != nil && sel.SelectLockInfo.LockType != tree.SelectLockNone) ||
			selectHasWriteIntent(sel.Select)
	case *tree.ParenSelect:
		return selectHasWriteIntent(sel.Select)
	case *tree.UnionClause:
		return selectHasWriteIntent(sel.Left) || selectHasWriteIntent(sel.Right)
	default:
		return false
	}
}
