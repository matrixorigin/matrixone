// Copyright 2021 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// CmdExecutor handle the command from the client
type CmdExecutor interface {
	SetSession(*Session)

	GetSession() *Session

	// ExecRequest execute the request and get the response
	ExecRequest(context.Context, *Session, *Request) (*Response, error)

	//SetCancelFunc saves a cancel function for active request.
	SetCancelFunc(context.CancelFunc)

	// CancelRequest cancels the active request
	CancelRequest()

	Close()
}

type CmdExecutorImpl struct {
	CmdExecutor
}

// UserInput
// normally, just use the sql.
// for some special statement, like 'set_var', we need to use the stmt.
// if the stmt is not nil, we neglect the sql.
type UserInput struct {
	sql  string
	stmt tree.Statement
}

func (in *UserInput) GetSql() string {
	return in.sql
}

// GetStmt if the stmt is not nil, we neglect the sql.
func (in *UserInput) GetStmt() tree.Statement {
	return in.stmt
}

type doComQueryFunc func(context.Context, *UserInput) error

type stmtExecStatus int

const (
	stmtExecSuccess stmtExecStatus = iota
	stmtExecFail
)

// StmtExecutor represents the single statement execution.
// it is also independent of the protocol
type StmtExecutor interface {
	ComputationWrapper

	// GetStatus returns the execution status
	GetStatus() stmtExecStatus

	// SetStatus sets the execution status
	SetStatus(err error)

	// Setup does preparation
	Setup(ctx context.Context, ses *Session) error

	// VerifyPrivilege ensures the user can execute this statement
	VerifyPrivilege(ctx context.Context, ses *Session) error

	// VerifyTxn checks the restriction of the transaction
	VerifyTxn(ctx context.Context, ses *Session) error

	// ResponseBeforeExec responses the client before the execution starts
	ResponseBeforeExec(ctx context.Context, ses *Session) error

	// ExecuteImpl runs the concrete logic of the statement. every statement has its implementation
	ExecuteImpl(ctx context.Context, ses *Session) error

	// ResponseAfterExec responses the client after the execution ends
	ResponseAfterExec(ctx context.Context, ses *Session) error

	// CommitOrRollbackTxn commits or rollbacks the transaction based on the status
	CommitOrRollbackTxn(ctx context.Context, ses *Session) error

	// Close does clean
	Close(ctx context.Context, ses *Session) error
}

var _ StmtExecutor = &baseStmtExecutor{}
var _ StmtExecutor = &statusStmtExecutor{}
var _ StmtExecutor = &resultSetStmtExecutor{}

// Execute runs the statement executor
func Execute(ctx context.Context, ses *Session, proc *process.Process, stmtExec StmtExecutor, beginInstant time.Time, envStmt, sqlType string, useEnv bool) error {
	var err, err2 error
	var cmpBegin, runBegin time.Time
	ctx = RecordStatement(ctx, ses, proc, stmtExec, beginInstant, envStmt, sqlType, useEnv)
	err = stmtExec.Setup(ctx, ses)
	if err != nil {
		goto handleRet
	}

	err = stmtExec.VerifyPrivilege(ctx, ses)
	if err != nil {
		goto handleRet
	}

	err = stmtExec.VerifyTxn(ctx, ses)
	if err != nil {
		goto handleRet
	}

	if err = stmtExec.SetDatabaseName(ses.GetDatabaseName()); err != nil {
		goto handleRet
	}

	cmpBegin = time.Now()

	//TODO: selfhandle statements do not need to compile
	if _, err = stmtExec.Compile(ctx, ses, ses.GetOutputCallback()); err != nil {
		goto handleRet
	}

	// only log if time of compile is longer than 1s
	if time.Since(cmpBegin) > time.Second {
		logInfof(ses.GetDebugString(), "time of Exec.Build : %s", time.Since(cmpBegin).String())
	}

	err = stmtExec.ResponseBeforeExec(ctx, ses)
	if err != nil {
		goto handleRet
	}

	runBegin = time.Now()

	err = stmtExec.ExecuteImpl(ctx, ses)
	if err != nil {
		goto handleRet
	}

	// only log if time of run is longer than 1s
	if time.Since(runBegin) > time.Second {
		logInfof(ses.GetDebugString(), "time of Exec.Run : %s", time.Since(runBegin).String())
	}

	_ = stmtExec.RecordExecPlan(ctx)

handleRet:
	stmtExec.SetStatus(err)
	err2 = stmtExec.CommitOrRollbackTxn(ctx, ses)
	if err2 != nil {
		return err2
	}

	err2 = stmtExec.ResponseAfterExec(ctx, ses)
	if err2 != nil {
		return err2
	}

	err2 = stmtExec.Close(ctx, ses)
	if err2 != nil {
		return err2
	}
	return err
}

// baseStmtExecutor the base class for the statement execution
type baseStmtExecutor struct {
	ComputationWrapper
	tenantName string
	status     stmtExecStatus
	err        error
}

func (bse *baseStmtExecutor) GetStatus() stmtExecStatus {
	return bse.status
}

func (bse *baseStmtExecutor) SetStatus(err error) {
	bse.err = err
	bse.status = stmtExecSuccess
	if err != nil {
		bse.status = stmtExecFail
	}

}

func (bse *baseStmtExecutor) CommitOrRollbackTxn(ctx context.Context, ses *Session) error {
	var txnErr error
	stmt := bse.GetAst()
	tenant := bse.tenantName
	incStatementCounter(tenant, stmt)
	if bse.GetStatus() == stmtExecSuccess {
		txnErr = ses.TxnCommitSingleStatement(stmt)
		if txnErr != nil {
			incTransactionErrorsCounter(tenant, metric.SQLTypeCommit)
			logStatementStatus(ctx, ses, stmt, fail, txnErr)
			return txnErr
		}
		logStatementStatus(ctx, ses, stmt, success, nil)
	} else {
		incStatementErrorsCounter(tenant, stmt)
		/*
			Cases    | set Autocommit = 1/0 | BEGIN statement |
			---------------------------------------------------
			Case1      1                       Yes
			Case2      1                       No
			Case3      0                       Yes
			Case4      0                       No
			---------------------------------------------------
			update error message in Case1,Case3,Case4.
		*/
		if ses.InMultiStmtTransactionMode() && ses.InActiveTransaction() {
			ses.SetOptionBits(OPTION_ATTACH_ABORT_TRANSACTION_ERROR)
		}
		logError(ses.GetDebugString(), bse.err.Error())
		txnErr = ses.TxnRollbackSingleStatement(stmt)
		if txnErr != nil {
			incTransactionErrorsCounter(tenant, metric.SQLTypeRollback)
			logStatementStatus(ctx, ses, stmt, fail, txnErr)
			return txnErr
		}
		logStatementStatus(ctx, ses, stmt, fail, bse.err)
	}
	return nil
}

func (bse *baseStmtExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return bse.Run(0)
}

func (bse *baseStmtExecutor) Setup(ctx context.Context, ses *Session) error {
	ses.SetMysqlResultSet(&MysqlResultSet{})
	return nil
}

func (bse *baseStmtExecutor) Close(ctx context.Context, ses *Session) error {
	ses.SetMysqlResultSet(nil)
	return nil
}

func (bse *baseStmtExecutor) VerifyPrivilege(ctx context.Context, ses *Session) error {
	var err error
	bse.tenantName = sysAccountName
	//skip PREPARE statement here
	if ses.GetTenantInfo() != nil && !IsPrepareStatement(bse.GetAst()) {
		bse.tenantName = ses.GetTenantInfo().GetTenant()
		err = authenticateUserCanExecuteStatement(ctx, ses, bse.GetAst())
		if err != nil {
			return err
		}
	}
	return err
}

func (bse *baseStmtExecutor) VerifyTxn(ctx context.Context, ses *Session) error {
	var err error
	var can bool
	/*
			if it is in an active or multi-statement transaction, we check the type of the statement.
			Then we decide that if we can execute the statement.

		If we check the active transaction, it will generate the case below.
		case:
		set autocommit = 0;  <- no active transaction
		                     <- no active transaction
		drop table test1;    <- no active transaction, no error
		                     <- has active transaction
		drop table test1;    <- has active transaction, error
		                     <- has active transaction
	*/
	if ses.InActiveTransaction() {
		stmt := bse.GetAst()
		can, err = statementCanBeExecutedInUncommittedTransaction(ses, stmt)
		if err != nil {
			return err
		}
		if !can {
			//is ddl statement
			if IsAdministrativeStatement(stmt) {
				return moerr.NewInternalError(ctx, administrativeCommandIsUnsupportedInTxnErrorInfo())
			} else if IsParameterModificationStatement(stmt) {
				return moerr.NewInternalError(ctx, parameterModificationInTxnErrorInfo())
			} else {
				return moerr.NewInternalError(ctx, unclassifiedStatementInUncommittedTxnErrorInfo())
			}
		}
	}
	return err
}

func (bse *baseStmtExecutor) ResponseBeforeExec(ctx context.Context, ses *Session) error {
	return nil
}

func (bse *baseStmtExecutor) ResponseAfterExec(ctx context.Context, ses *Session) error {
	var err, retErr error
	if bse.GetStatus() == stmtExecSuccess {
		resp := NewOkResponse(bse.GetAffectedRows(), 0, 0, 0, int(COM_QUERY), "")
		if err = ses.GetMysqlProtocol().SendResponse(ctx, resp); err != nil {
			retErr = moerr.NewInternalError(ctx, "routine send response failed. error:%v ", err)
			logStatementStatus(ctx, ses, bse.GetAst(), fail, retErr)
			return retErr
		}
	}
	return nil
}
