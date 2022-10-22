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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"sync"
	"time"
)

// CmdExecutor handle the command from the client
type CmdExecutor interface {
	PrepareSessionBeforeExecRequest(*Session)

	// ExecRequest execute the request and get the response
	ExecRequest(context.Context, *Request) (*Response, error)

	Close()
}

type CmdExecutorImpl struct {
	CmdExecutor
}

type doComQueryFunc func(context.Context, string) error

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
	SetStatus(status stmtExecStatus)

	// Prepare setups something
	Prepare(ctx context.Context, ses *Session, proc *process.Process, beginInstant time.Time) error

	// Close cleans the side effect
	Close(ctx context.Context, ses *Session) error

	// VerifyPrivilege ensures the user can execute this statement
	VerifyPrivilege(ctx context.Context, ses *Session) error

	// VerifyTxnRestriction checks the restriction due to the transaction semantic
	VerifyTxnRestriction(ctx context.Context, ses *Session) error

	// ResponseBefore responses the client before the execution starts
	ResponseBefore(ctx context.Context, ses *Session) error

	// ResponseAfter responses the client after the execution ends
	ResponseAfter(ctx context.Context, ses *Session) error

	// ExecuteImpl runs concrete logic. every statement has its implementation
	ExecuteImpl(ctx context.Context, ses *Session) error

	// RecordPlan saves the plan into the log
	RecordPlan(ctx context.Context, ses *Session) error

	// CommitOrRollbackTxn commits or rollbacks the transaction based on the status
	CommitOrRollbackTxn(ctx context.Context, ses *Session) error
}

var _ StmtExecutor = &baseStmtExecutor{}
var _ StmtExecutor = &statusStmtExecutor{}
var _ StmtExecutor = &resultSetStmtExecutor{}

// baseStmtExecutor the base class for the statement execution
type baseStmtExecutor struct {
	*TxnComputationWrapper
	mu sync.Mutex
	// the ctx will be updated in Prepare
	updatedCtx context.Context

	tenantName string

	status stmtExecStatus
	err    error
	proc   *process.Process
}

func (bse *baseStmtExecutor) GetStatus() stmtExecStatus {
	return bse.status
}

func (bse *baseStmtExecutor) SetStatus(status stmtExecStatus) {
	bse.status = status
}

func (bse *baseStmtExecutor) RecordPlan(ctx context.Context, ses *Session) error {
	_ = bse.RecordExecPlan(bse.updatedCtx)
	return nil
}

func (bse *baseStmtExecutor) CommitOrRollbackTxn(ctx context.Context, ses *Session) error {
	var txnErr error
	stmt := bse.stmt
	requestCtx := bse.updatedCtx
	tenant := bse.tenantName
	incStatementCounter(tenant, stmt)
	if bse.GetStatus() == stmtExecSuccess {
		txnErr = ses.TxnCommitSingleStatement(stmt)
		if txnErr != nil {
			incTransactionErrorsCounter(tenant, metric.SQLTypeCommit)
			trace.EndStatement(requestCtx, txnErr)
			logStatementStatus(requestCtx, ses, stmt, fail, txnErr)
			return txnErr
		}
		trace.EndStatement(requestCtx, nil)
		logStatementStatus(requestCtx, ses, stmt, success, nil)
	} else {
		incStatementErrorsCounter(tenant, stmt)
		trace.EndStatement(requestCtx, bse.err)
		logutil.Error(bse.err.Error())
		txnErr = ses.TxnRollbackSingleStatement(stmt)
		if txnErr != nil {
			incTransactionErrorsCounter(tenant, metric.SQLTypeRollback)
			logStatementStatus(requestCtx, ses, stmt, fail, txnErr)
			return txnErr
		}
		logStatementStatus(requestCtx, ses, stmt, fail, bse.err)
	}
	return nil
}

func (bse *baseStmtExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	if bse.compile != nil {
		var runner ComputationRunner
		runner = bse.compile
		return runner.Run(0)
	}
	return nil
}

func (bse *baseStmtExecutor) Prepare(ctx context.Context, ses *Session, proc *process.Process, beginInstant time.Time) error {
	ses.SetMysqlResultSet(&MysqlResultSet{})
	bse.proc = proc
	bse.updatedCtx = RecordStatement(ctx, ses, proc, bse, beginInstant)
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
	if ses.GetTenantInfo() != nil && !IsPrepareStatement(bse.stmt) {
		bse.tenantName = ses.GetTenantInfo().GetTenant()
		err = authenticatePrivilegeOfStatement(bse.updatedCtx, ses, bse.stmt)
		if err != nil {
			return err
		}
	}
	return err
}

func (bse *baseStmtExecutor) VerifyTxnRestriction(ctx context.Context, ses *Session) error {
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
		stmt := bse.stmt
		can, err = StatementCanBeExecutedInUncommittedTransaction(ses, stmt)
		if err != nil {
			return err
		}
		if !can {
			//is ddl statement
			if IsDDL(stmt) {
				return errorOnlyCreateStatement
			} else if IsAdministrativeStatement(stmt) {
				return errorAdministrativeStatement
			} else if IsParameterModificationStatement(stmt) {
				return errorParameterModificationInTxn
			} else {
				return errorUnclassifiedStatement
			}
		}
	}
	return err
}

func (bse *baseStmtExecutor) ResponseBefore(ctx context.Context, ses *Session) error {
	return nil
}

func (bse *baseStmtExecutor) ResponseAfter(ctx context.Context, ses *Session) error {
	var err, retErr error
	if bse.GetStatus() == stmtExecSuccess {
		resp := NewOkResponse(bse.GetAffectedRows(), 0, 0, 0, int(COM_QUERY), "")
		if err = ses.GetMysqlProtocol().SendResponse(resp); err != nil {
			trace.EndStatement(bse.updatedCtx, err)
			retErr = moerr.NewInternalError("routine send response failed. error:%v ", err)
			logStatementStatus(bse.updatedCtx, ses, bse.stmt, fail, retErr)
			return retErr
		}
	}
	return nil
}

// Execute runs the execution framework
func Execute(ctx context.Context, ses *Session, proc *process.Process, stmtExec StmtExecutor, beginInstant time.Time) error {
	var err error
	var cmpBegin, runBegin time.Time
	pu := ses.GetParameterUnit()
	err = stmtExec.Prepare(ctx, ses, proc, beginInstant)
	if err != nil {
		goto handleRet
	}

	err = stmtExec.VerifyPrivilege(ctx, ses)
	if err != nil {
		goto handleRet
	}

	err = stmtExec.VerifyTxnRestriction(ctx, ses)
	if err != nil {
		goto handleRet
	}

	ses.GetTxnCompileCtx().SetQueryType(TXN_DEFAULT)

	if err = stmtExec.SetDatabaseName(ses.GetDatabaseName()); err != nil {
		goto handleRet
	}

	cmpBegin = time.Now()

	if _, err = stmtExec.Compile(ctx, ses, ses.GetOutputCallback()); err != nil {
		goto handleRet
	}

	if !pu.SV.DisableRecordTimeElapsedOfSqlRequest {
		logutil.Infof("time of Exec.Build : %s", time.Since(cmpBegin).String())
	}

	err = stmtExec.ResponseBefore(ctx, ses)
	if err != nil {
		goto handleRet
	}

	runBegin = time.Now()

	err = stmtExec.ExecuteImpl(ctx, ses)
	if err != nil {
		goto handleRet
	}

	err = stmtExec.CommitOrRollbackTxn(ctx, ses)
	if err != nil {
		goto handleRet
	}

	err = stmtExec.ResponseAfter(ctx, ses)
	if err != nil {
		goto handleRet
	}

	if !pu.SV.DisableRecordTimeElapsedOfSqlRequest {
		logutil.Infof("time of Exec.Run : %s", time.Since(runBegin).String())
	}

	_ = stmtExec.RecordExecPlan(ctx)

handleRet:
	stmtExec.SetStatus(stmtExecSuccess)
	if err != nil {
		stmtExec.SetStatus(stmtExecFail)
	}

	err = stmtExec.Close(ctx, ses)
	if err != nil {
		return err
	}
	return nil
}

// statusStmtExecutor represents the execution without outputting result set to the client
type statusStmtExecutor struct {
	*baseStmtExecutor
}

// resultSetStmtExecutor represents the execution outputting result set to the client
type resultSetStmtExecutor struct {
	*baseStmtExecutor
}

func (rsse *resultSetStmtExecutor) ResponseBefore(ctx context.Context, ses *Session) error {
	var err error
	var columns []interface{}
	proto := ses.GetMysqlProtocol()
	err = rsse.baseStmtExecutor.ResponseBefore(ctx, ses)
	if err != nil {
		return err
	}

	columns, err = rsse.GetColumns()
	if err != nil {
		logutil.Errorf("GetColumns from Computation handler failed. error: %v", err)
		return err
	}
	/*
		Step 1 : send column count and column definition.
	*/
	//send column count
	colCnt := uint64(len(columns))
	err = proto.SendColumnCountPacket(colCnt)
	if err != nil {
		return err
	}
	//send columns
	//column_count * Protocol::ColumnDefinition packets
	cmd := ses.GetCmd()
	mrs := ses.GetMysqlResultSet()
	for _, c := range columns {
		mysqlc := c.(Column)
		mrs.AddColumn(mysqlc)

		/*
			mysql COM_QUERY response: send the column definition per column
		*/
		err = proto.SendColumnDefinitionPacket(mysqlc, cmd)
		if err != nil {
			return err
		}
	}

	/*
		mysql COM_QUERY response: End after the column has been sent.
		send EOF packet
	*/
	err = proto.SendEOFPacketIf(0, 0)
	if err != nil {
		return err
	}
	return nil
}

func (rsse *resultSetStmtExecutor) ResponseAfter(ctx context.Context, ses *Session) error {
	/*
		Step 3: Say goodbye
		mysql COM_QUERY response: End after the data row has been sent.
		After all row data has been sent, it sends the EOF or OK packet.
	*/
	proto := ses.GetMysqlProtocol()
	return proto.sendEOFOrOkPacket(0, 0)
}
