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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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

// StmtExecutor represents the single statement execution.
// it is also independent of the protocol
type StmtExecutor interface {
	ComputationWrapper
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

	// Execute runs the execution framework
	Execute(ctx context.Context, ses *Session, proc *process.Process, beginInstant time.Time) error
}

var _ StmtExecutor = &baseStmtExecutor{}
var _ StmtExecutor = &statusStmtExecutor{}
var _ StmtExecutor = &resultSetStmtExecutor{}

// baseStmtExecutor the base class for the statement execution
type baseStmtExecutor struct {
	mu sync.Mutex
	*TxnComputationWrapper

	// the ctx will be updated in Prepare
	updatedCtx context.Context

	tenantName string
}

func (bse *baseStmtExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	//TODO implement me
	panic("implement me")
}

func (bse *baseStmtExecutor) Prepare(ctx context.Context, ses *Session, proc *process.Process, beginInstant time.Time) error {
	ses.SetMysqlResultSet(&MysqlResultSet{})
	bse.updatedCtx = RecordStatement(ctx, ses, proc, bse, beginInstant)
	return nil
}

func (bse *baseStmtExecutor) Close(ctx context.Context, ses *Session) error {
	//TODO: commit or rollback
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
	return nil
}

func (bse *baseStmtExecutor) Execute(ctx context.Context, ses *Session, proc *process.Process, beginInstant time.Time) error {
	var err error
	err = bse.Prepare(ctx, ses, proc, beginInstant)
	if err != nil {
		goto handleRet
	}

	err = bse.VerifyPrivilege(ctx, ses)
	if err != nil {
		goto handleRet
	}

	err = bse.VerifyTxnRestriction(ctx, ses)
	if err != nil {
		goto handleRet
	}

	err = bse.ResponseBefore(ctx, ses)
	if err != nil {
		goto handleRet
	}

	err = bse.ExecuteImpl(ctx, ses)
	if err != nil {
		goto handleRet
	}

	err = bse.ResponseAfter(ctx, ses)
	if err != nil {
		goto handleRet
	}

handleRet:
	err = bse.Close(ctx, ses)
	if err != nil {
		return err
	}
	return nil
}

// statusStmtExecutor represents the execution without outputting result set to the client
type statusStmtExecutor struct {
	father *baseStmtExecutor
}

func (sse *statusStmtExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	//TODO implement me
	return nil
}

func (sse *statusStmtExecutor) Run(ts uint64) (err error) {
	return sse.father.Run(ts)
}

func (sse *statusStmtExecutor) GetAst() tree.Statement {
	return sse.father.GetAst()
}

func (sse *statusStmtExecutor) SetDatabaseName(db string) error {
	return sse.father.SetDatabaseName(db)
}

func (sse *statusStmtExecutor) GetColumns() ([]interface{}, error) {
	return sse.father.GetColumns()
}

func (sse *statusStmtExecutor) GetAffectedRows() uint64 {
	return sse.father.GetAffectedRows()
}

func (sse *statusStmtExecutor) Compile(requestCtx context.Context, u interface{}, fill func(interface{}, *batch.Batch) error) (interface{}, error) {
	return sse.father.Compile(requestCtx, u, fill)
}

func (sse *statusStmtExecutor) GetUUID() []byte {
	return sse.father.GetUUID()
}

func (sse *statusStmtExecutor) RecordExecPlan(ctx context.Context) error {
	return sse.father.RecordExecPlan(ctx)
}

func (sse *statusStmtExecutor) GetLoadTag() bool {
	return sse.father.GetLoadTag()
}

func (sse *statusStmtExecutor) Prepare(ctx context.Context, ses *Session, proc *process.Process, beginInstant time.Time) error {
	return sse.father.Prepare(ctx, ses, proc, beginInstant)
}

func (sse *statusStmtExecutor) Close(ctx context.Context, ses *Session) error {
	//TODO:
	return sse.father.Close(ctx, ses)
}

func (sse *statusStmtExecutor) VerifyPrivilege(ctx context.Context, ses *Session) error {
	return sse.father.VerifyPrivilege(ctx, ses)
}

func (sse *statusStmtExecutor) VerifyTxnRestriction(ctx context.Context, ses *Session) error {
	return sse.father.VerifyTxnRestriction(ctx, ses)
}

func (sse *statusStmtExecutor) ResponseBefore(ctx context.Context, ses *Session) error {
	var err error
	err = sse.father.ResponseBefore(ctx, ses)
	if err != nil {
		return err
	}
	//TODO:
	return err
}

func (sse *statusStmtExecutor) ResponseAfter(ctx context.Context, ses *Session) error {
	//TODO: success or fail
	return sse.father.ResponseAfter(ctx, ses)
}

func (sse *statusStmtExecutor) Execute(ctx context.Context, ses *Session, proc *process.Process, beginInstant time.Time) error {
	return sse.father.Execute(ctx, ses, proc, beginInstant)
}

// resultSetStmtExecutor represents the execution outputting result set to the client
type resultSetStmtExecutor struct {
	father *baseStmtExecutor
}

func (rsse *resultSetStmtExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	//TODO:
	return nil
}

func (rsse *resultSetStmtExecutor) Run(ts uint64) (err error) {
	return rsse.father.Run(ts)
}

func (rsse *resultSetStmtExecutor) GetAst() tree.Statement {
	return rsse.father.GetAst()
}

func (rsse *resultSetStmtExecutor) SetDatabaseName(db string) error {
	return rsse.father.SetDatabaseName(db)
}

func (rsse *resultSetStmtExecutor) GetColumns() ([]interface{}, error) {
	return rsse.father.GetColumns()
}

func (rsse *resultSetStmtExecutor) GetAffectedRows() uint64 {
	return rsse.father.GetAffectedRows()
}

func (rsse *resultSetStmtExecutor) Compile(requestCtx context.Context, u interface{}, fill func(interface{}, *batch.Batch) error) (interface{}, error) {
	return rsse.father.Compile(requestCtx, u, fill)
}

func (rsse *resultSetStmtExecutor) GetUUID() []byte {
	return rsse.father.GetUUID()
}

func (rsse *resultSetStmtExecutor) RecordExecPlan(ctx context.Context) error {
	return rsse.father.RecordExecPlan(ctx)
}

func (rsse *resultSetStmtExecutor) GetLoadTag() bool {
	return rsse.father.GetLoadTag()
}

func (rsse *resultSetStmtExecutor) Prepare(ctx context.Context, ses *Session, proc *process.Process, beginInstant time.Time) error {
	return rsse.father.Prepare(ctx, ses, proc, beginInstant)
}

func (rsse *resultSetStmtExecutor) Close(ctx context.Context, ses *Session) error {
	//TODO:
	return rsse.father.Close(ctx, ses)
}

func (rsse *resultSetStmtExecutor) VerifyPrivilege(ctx context.Context, ses *Session) error {
	return rsse.father.VerifyPrivilege(ctx, ses)
}

func (rsse *resultSetStmtExecutor) VerifyTxnRestriction(ctx context.Context, ses *Session) error {
	return rsse.father.VerifyTxnRestriction(ctx, ses)
}

func (rsse *resultSetStmtExecutor) ResponseBefore(ctx context.Context, ses *Session) error {
	var err error
	err = rsse.father.ResponseBefore(ctx, ses)
	if err != nil {
		return err
	}
	//TODO:
	return nil
}

func (rsse *resultSetStmtExecutor) ResponseAfter(ctx context.Context, ses *Session) error {
	//TODO implement me
	return rsse.father.ResponseAfter(ctx, ses)
}

func (rsse *resultSetStmtExecutor) Execute(ctx context.Context, ses *Session, proc *process.Process, beginInstant time.Time) error {
	return rsse.father.Execute(ctx, ses, proc, beginInstant)
}
