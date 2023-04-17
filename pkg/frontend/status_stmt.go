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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// statusStmtExecutor represents the execution without outputting result set to the client
type statusStmtExecutor struct {
	*baseStmtExecutor
}

type BeginTxnExecutor struct {
	*statusStmtExecutor
	bt *tree.BeginTransaction
}

func (bte *BeginTxnExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	err := ses.TxnBegin()
	if err != nil {
		return err
	}
	RecordStatementTxnID(ctx, ses)
	return err
}

type CommitTxnExecutor struct {
	*statusStmtExecutor
	ct *tree.CommitTransaction
}

func (cte *CommitTxnExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return ses.TxnCommit()
}

type RollbackTxnExecutor struct {
	*statusStmtExecutor
	rt *tree.RollbackTransaction
}

func (rte *RollbackTxnExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return ses.TxnRollback()
}

type SetRoleExecutor struct {
	*statusStmtExecutor
	sr *tree.SetRole
}

func (sre *SetRoleExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return doSwitchRole(ctx, ses, sre.sr)
}

type UseExecutor struct {
	*statusStmtExecutor
	u *tree.Use
}

func (ue *UseExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	v, err := ses.GetGlobalVar("lower_case_table_names")
	if err != nil {
		return err
	}
	ue.u.Name.SetConfig(v.(int64))
	return doUse(ctx, ses, ue.u.Name.Compare())
}

type DropDatabaseExecutor struct {
	*statusStmtExecutor
	dd *tree.DropDatabase
}

func (dde *DropDatabaseExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	// if the droped database is the same as the one in use, database must be reseted to empty.
	if string(dde.dd.Name) == ses.GetDatabaseName() {
		ses.SetDatabaseName("")
	}
	return dde.statusStmtExecutor.ExecuteImpl(ctx, ses)
}

type PrepareStmtExecutor struct {
	*statusStmtExecutor
	ps          *tree.PrepareStmt
	prepareStmt *PrepareStmt
}

func (pse *PrepareStmtExecutor) ResponseAfterExec(ctx context.Context, ses *Session) error {
	var err2, retErr error
	if ses.GetCmd() == COM_STMT_PREPARE {
		if err2 = ses.GetMysqlProtocol().SendPrepareResponse(ctx, pse.prepareStmt); err2 != nil {
			retErr = moerr.NewInternalError(ctx, "routine send response failed. error:%v ", err2)
			logStatementStatus(ctx, ses, pse.GetAst(), fail, retErr)
			return retErr
		}
	} else {
		resp := NewOkResponse(pse.GetAffectedRows(), 0, 0, 0, int(COM_QUERY), "")
		if err2 = ses.GetMysqlProtocol().SendResponse(ctx, resp); err2 != nil {
			retErr = moerr.NewInternalError(ctx, "routine send response failed. error:%v ", err2)
			logStatementStatus(ctx, ses, pse.GetAst(), fail, retErr)
			return retErr
		}
	}
	return nil
}

func (pse *PrepareStmtExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	var err error
	pse.prepareStmt, err = doPrepareStmt(ctx, ses, pse.ps)
	if err != nil {
		return err
	}
	return authenticateUserCanExecutePrepareOrExecute(ctx, ses, pse.prepareStmt.PrepareStmt, pse.prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
}

type PrepareStringExecutor struct {
	*statusStmtExecutor
	ps          *tree.PrepareString
	prepareStmt *PrepareStmt
}

func (pse *PrepareStringExecutor) ResponseAfterExec(ctx context.Context, ses *Session) error {
	var err2, retErr error
	if ses.GetCmd() == COM_STMT_PREPARE {
		if err2 = ses.GetMysqlProtocol().SendPrepareResponse(ctx, pse.prepareStmt); err2 != nil {
			retErr = moerr.NewInternalError(ctx, "routine send response failed. error:%v ", err2)
			logStatementStatus(ctx, ses, pse.GetAst(), fail, retErr)
			return retErr
		}
	} else {
		resp := NewOkResponse(pse.GetAffectedRows(), 0, 0, 0, int(COM_QUERY), "")
		if err2 = ses.GetMysqlProtocol().SendResponse(ctx, resp); err2 != nil {
			retErr = moerr.NewInternalError(ctx, "routine send response failed. error:%v ", err2)
			logStatementStatus(ctx, ses, pse.GetAst(), fail, retErr)
			return retErr
		}
	}
	return nil
}

func (pse *PrepareStringExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	var err error
	pse.prepareStmt, err = doPrepareString(ctx, ses, pse.ps)
	if err != nil {
		return err
	}
	return authenticateUserCanExecutePrepareOrExecute(ctx, ses, pse.prepareStmt.PrepareStmt, pse.prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
}

// TODO: DeallocateExecutor has no response like QUIT COMMAND ?
type DeallocateExecutor struct {
	*statusStmtExecutor
	d *tree.Deallocate
}

func (de *DeallocateExecutor) ResponseAfterExec(ctx context.Context, ses *Session) error {
	var err2, retErr error
	//we will not send response in COM_STMT_CLOSE command
	if ses.GetCmd() != COM_STMT_CLOSE {
		resp := NewOkResponse(de.GetAffectedRows(), 0, 0, 0, int(COM_QUERY), "")
		if err2 = ses.GetMysqlProtocol().SendResponse(ctx, resp); err2 != nil {
			retErr = moerr.NewInternalError(ctx, "routine send response failed. error:%v ", err2)
			logStatementStatus(ctx, ses, de.GetAst(), fail, retErr)
			return retErr
		}
	}
	return nil
}

func (de *DeallocateExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return doDeallocate(ctx, ses, de.d)
}

type ExecuteExecutor struct {
	*baseStmtExecutor
	actualStmtExec StmtExecutor
	ses            *Session
	proc           *process.Process
	e              *tree.Execute
}

func (ee *ExecuteExecutor) Compile(requestCtx context.Context, u interface{}, fill func(interface{}, *batch.Batch) error) (interface{}, error) {
	var err error
	var ret interface{}
	ret, err = ee.baseStmtExecutor.Compile(requestCtx, u, fill)
	if err != nil {
		return nil, err
	}

	ee.actualStmtExec, err = getStmtExecutor(ee.ses, ee.proc, ee.baseStmtExecutor, ee.GetAst())
	if err != nil {
		return nil, err
	}
	return ret, err
}

func (ee *ExecuteExecutor) ResponseBeforeExec(ctx context.Context, ses *Session) error {
	return ee.actualStmtExec.ResponseBeforeExec(ctx, ses)
}

func (ee *ExecuteExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return ee.actualStmtExec.ExecuteImpl(ctx, ses)
}

func (ee *ExecuteExecutor) ResponseAfterExec(ctx context.Context, ses *Session) error {
	return ee.actualStmtExec.ResponseAfterExec(ctx, ses)
}

func (ee *ExecuteExecutor) CommitOrRollbackTxn(ctx context.Context, ses *Session) error {
	return ee.actualStmtExec.CommitOrRollbackTxn(ctx, ses)
}

func (ee *ExecuteExecutor) Close(ctx context.Context, ses *Session) error {
	return ee.actualStmtExec.Close(ctx, ses)
}

type SetVarExecutor struct {
	*statusStmtExecutor
	sv *tree.SetVar
}

func (sve *SetVarExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return doSetVar(ctx, ses, sve.sv)
}

type DeleteExecutor struct {
	*statusStmtExecutor
	d *tree.Delete
}

func (de *DeleteExecutor) Setup(ctx context.Context, ses *Session) error {
	err := de.baseStmtExecutor.Setup(ctx, ses)
	if err != nil {
		return err
	}
	return nil
}

type UpdateExecutor struct {
	*statusStmtExecutor
	u *tree.Update
}

func (de *UpdateExecutor) Setup(ctx context.Context, ses *Session) error {
	err := de.baseStmtExecutor.Setup(ctx, ses)
	if err != nil {
		return err
	}
	return nil
}

type DropPublicationExecutor struct {
	*statusStmtExecutor
	dp *tree.DropPublication
}

func (dpe *DropPublicationExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return doDropPublication(ctx, ses, dpe.dp)
}

type AlterPublicationExecutor struct {
	*statusStmtExecutor
	ap *tree.AlterPublication
}

func (ape *AlterPublicationExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return doAlterPublication(ctx, ses, ape.ap)
}

type CreatePublicationExecutor struct {
	*statusStmtExecutor
	cp *tree.CreatePublication
}

func (cpe *CreatePublicationExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return doCreatePublication(ctx, ses, cpe.cp)
}

type CreateAccountExecutor struct {
	*statusStmtExecutor
	ca *tree.CreateAccount
}

func (cae *CreateAccountExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return InitGeneralTenant(ctx, ses, cae.ca)
}

type DropAccountExecutor struct {
	*statusStmtExecutor
	da *tree.DropAccount
}

func (dae *DropAccountExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return doDropAccount(ctx, ses, dae.da)
}

type AlterAccountExecutor struct {
	*statusStmtExecutor
	aa *tree.AlterAccount
}

type CreateUserExecutor struct {
	*statusStmtExecutor
	cu *tree.CreateUser
}

func (cue *CreateUserExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	tenant := ses.GetTenantInfo()
	return InitUser(ctx, ses, tenant, cue.cu)
}

type DropUserExecutor struct {
	*statusStmtExecutor
	du *tree.DropUser
}

func (due *DropUserExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return doDropUser(ctx, ses, due.du)
}

type AlterUserExecutor struct {
	*statusStmtExecutor
	au *tree.AlterUser
}

type CreateRoleExecutor struct {
	*statusStmtExecutor
	cr *tree.CreateRole
}

func (cre *CreateRoleExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	tenant := ses.GetTenantInfo()

	//step1 : create the role
	return InitRole(ctx, ses, tenant, cre.cr)
}

type DropRoleExecutor struct {
	*statusStmtExecutor
	dr *tree.DropRole
}

func (dre *DropRoleExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return doDropRole(ctx, ses, dre.dr)
}

type GrantExecutor struct {
	*statusStmtExecutor
	g *tree.Grant
}

func (ge *GrantExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	switch ge.g.Typ {
	case tree.GrantTypeRole:
		return doGrantRole(ctx, ses, &ge.g.GrantRole)
	case tree.GrantTypePrivilege:
		return doGrantPrivilege(ctx, ses, &ge.g.GrantPrivilege)
	}
	return moerr.NewInternalError(ctx, "no such grant type %v", ge.g.Typ)
}

type RevokeExecutor struct {
	*statusStmtExecutor
	r *tree.Revoke
}

func (re *RevokeExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	switch re.r.Typ {
	case tree.RevokeTypeRole:
		return doRevokeRole(ctx, ses, &re.r.RevokeRole)
	case tree.RevokeTypePrivilege:
		return doRevokePrivilege(ctx, ses, &re.r.RevokePrivilege)
	}
	return moerr.NewInternalError(ctx, "no such revoke type %v", re.r.Typ)
}

type CreateTableExecutor struct {
	*statusStmtExecutor
	ct *tree.CreateTable
}

type DropTableExecutor struct {
	*statusStmtExecutor
	dt *tree.DropTable
}

type CreateDatabaseExecutor struct {
	*statusStmtExecutor
	cd *tree.CreateDatabase
}

type CreateIndexExecutor struct {
	*statusStmtExecutor
	ci *tree.CreateIndex
}

type DropIndexExecutor struct {
	*statusStmtExecutor
	di *tree.DropIndex
}

type CreateViewExecutor struct {
	*statusStmtExecutor
	cv *tree.CreateView
}

type AlterViewExecutor struct {
	*statusStmtExecutor
	av *tree.AlterView
}

type CreateSequenceExecutor struct {
	*statusStmtExecutor
	cs *tree.CreateSequence
}

type DropSequenceExecutor struct {
	*statusStmtExecutor
	ds *tree.DropSequence
}

type DropViewExecutor struct {
	*statusStmtExecutor
	dv *tree.DropView
}

type AlterTableExecutor struct {
	*statusStmtExecutor
	at *tree.AlterTable
}

type InsertExecutor struct {
	*statusStmtExecutor
	i *tree.Insert
}

func (ie *InsertExecutor) ResponseAfterExec(ctx context.Context, ses *Session) error {
	var err, retErr error
	if ie.GetStatus() == stmtExecSuccess {
		resp := NewOkResponse(ie.GetAffectedRows(), 0, 0, 0, int(COM_QUERY), "")
		resp.lastInsertId = 1
		if err = ses.GetMysqlProtocol().SendResponse(ctx, resp); err != nil {
			retErr = moerr.NewInternalError(ctx, "routine send response failed. error:%v ", err)
			logStatementStatus(ctx, ses, ie.GetAst(), fail, retErr)
			return retErr
		}
	}
	return nil
}

type LoadExecutor struct {
	*statusStmtExecutor
	l *tree.Load
}

func (le *LoadExecutor) CommitOrRollbackTxn(ctx context.Context, ses *Session) error {
	stmt := le.GetAst()
	tenant := le.tenantName
	incStatementCounter(tenant, stmt)
	if le.GetStatus() == stmtExecSuccess {
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
		logutil.Error(le.err.Error())
		logStatementStatus(ctx, ses, stmt, fail, le.err)
	}
	return nil
}

type SetDefaultRoleExecutor struct {
	*statusStmtExecutor
	sdr *tree.SetDefaultRole
}

type SetPasswordExecutor struct {
	*statusStmtExecutor
	sp *tree.SetPassword
}

type TruncateTableExecutor struct {
	*statusStmtExecutor
	tt *tree.TruncateTable
}
