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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const maxExecutionTime = "max_execution_time"

// executeStmtWithMaxExecutionTime applies MySQL's session max_execution_time
// to read-only SELECT statements. The timeout context is statement-scoped so
// an expired statement cannot poison the next statement on the connection.
func executeStmtWithMaxExecutionTime(ses *Session, execCtx *ExecCtx) (err error) {
	finish, err := startMaxExecutionTimer(ses, execCtx)
	if err != nil {
		return err
	}
	defer func() {
		err = finish(err)
	}()

	return executeStmtWithTxn(ses, nil, execCtx)
}

// startMaxExecutionTimer installs a deadline on both ExecCtx and the session's
// reusable process. The returned function must be called after the statement;
// it restores the parent context and converts this timer's expiry to MySQL's
// query-timeout error without masking cancellation from an ancestor.
func startMaxExecutionTimer(
	ses *Session,
	execCtx *ExecCtx,
) (func(error) error, error) {
	identity := func(err error) error { return err }
	if ses == nil || execCtx == nil || execCtx.proc == nil {
		return identity, nil
	}
	// MySQL applies max_execution_time only to top-level client SELECTs. Do not
	// let a global/session value bound internal work synthesized for another
	// statement, session migration, or internal-executor traffic.
	if !isTopLevelClientStatement(ses, execCtx, execCtx.input) || ses.GetIsInternal() {
		return identity, nil
	}

	applies, err := maxExecutionTimeApplies(execCtx.reqCtx, ses, execCtx.stmt)
	if err != nil || !applies {
		return identity, err
	}

	value, err := ses.GetSessionSysVar(maxExecutionTime)
	if err != nil {
		return identity, err
	}
	milliseconds, ok := value.(int64)
	if !ok {
		return identity, moerr.NewInternalErrorf(
			execCtx.reqCtx,
			"system variable %s has unexpected type %T",
			maxExecutionTime,
			value,
		)
	}
	if milliseconds == 0 {
		return identity, nil
	}

	parentCtx := execCtx.reqCtx
	timeoutErr := moerr.NewQueryTimeout(parentCtx)
	timeoutCtx, cancel := context.WithTimeoutCause(
		parentCtx,
		time.Duration(milliseconds)*time.Millisecond,
		timeoutErr,
	)
	execCtx.reqCtx = timeoutCtx
	execCtx.proc.ReplaceTopCtx(timeoutCtx)

	return func(stmtErr error) error {
		cause := context.Cause(timeoutCtx)
		cancel()
		execCtx.reqCtx = parentCtx
		execCtx.proc.ReplaceTopCtx(parentCtx)
		if moerr.IsMoErrCode(cause, moerr.ErrQueryTimeout) {
			return timeoutErr
		}
		return stmtErr
	}, nil
}

func maxExecutionTimeApplies(
	ctx context.Context,
	ses *Session,
	stmt tree.Statement,
) (bool, error) {
	switch stmt := stmt.(type) {
	case *tree.Select:
		// Locking reads are not read-only SELECT statements.
		return stmt.SelectLockInfo == nil ||
			stmt.SelectLockInfo.LockType == tree.SelectLockNone, nil
	case *tree.Execute:
		prepared, err := ses.GetPrepareStmt(ctx, string(stmt.Name))
		if err != nil {
			return false, err
		}
		return maxExecutionTimeApplies(ctx, ses, prepared.PrepareStmt)
	default:
		return false, nil
	}
}
