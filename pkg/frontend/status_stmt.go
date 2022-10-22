package frontend

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

type BeginTxnExecutor struct {
	*statusStmtExecutor
	bt *tree.BeginTransaction
}

func (bte *BeginTxnExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	err := ses.TxnBegin()
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
	return doUse(ctx, ses, ue.u.Name)
}

type DropDatabaseExecutor struct {
	*statusStmtExecutor
	dd *tree.DropDatabase
}

func (dde *DropDatabaseExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	// if the droped database is the same as the one in use, database must be reseted to empty.
	if string(dde.dd.Name) == ses.GetDatabaseName() {
		ses.SetUserName("")
	}
	return dde.statusStmtExecutor.ExecuteImpl(ctx, ses)
}

type ImportExecutor struct {
	*statusStmtExecutor
	i      *tree.Import
	result *LoadResult
}

func (ie *ImportExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	var err error
	ie.result, err = doLoadData(ctx, ses, ie.proc, ie.i)
	return err
}

func (ie *ImportExecutor) ResponseAfter(ctx context.Context, ses *Session) error {
	var err error
	result := ie.result
	info := moerr.NewLoadInfo(result.Records, result.Deleted, result.Skipped, result.Warnings, result.WriteTimeout).Error()
	resp := NewOkResponse(result.Records, 0, uint16(result.Warnings), 0, int(COM_QUERY), info)
	if err = ses.GetMysqlProtocol().SendResponse(resp); err != nil {
		return moerr.NewInternalError("routine send response failed. error:%v ", err)
	}
	return nil
}

type PrepareStmtExecutor struct {
	*statusStmtExecutor
	ps          *tree.PrepareStmt
	prepareStmt *PrepareStmt
}

func (pse *PrepareStmtExecutor) ResponseAfter(ctx context.Context, ses *Session) error {
	var err2, retErr error
	if err2 = ses.GetMysqlProtocol().SendPrepareResponse(pse.prepareStmt); err2 != nil {
		trace.EndStatement(ctx, err2)
		retErr = moerr.NewInternalError("routine send response failed. error:%v ", err2)
		logStatementStatus(ctx, ses, pse.stmt, fail, retErr)
		return retErr
	}
	return nil
}

func (pse *PrepareStmtExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	var err error
	pse.prepareStmt, err = doPrepareStmt(ctx, ses, pse.ps)
	if err != nil {
		return err
	}
	return authenticatePrivilegeOfPrepareOrExecute(ctx, ses, pse.prepareStmt.PrepareStmt, pse.prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
}

type PrepareStringExecutor struct {
	*statusStmtExecutor
	ps          *tree.PrepareString
	prepareStmt *PrepareStmt
}

func (pse *PrepareStringExecutor) ResponseAfter(ctx context.Context, ses *Session) error {
	var err2, retErr error
	if err2 = ses.GetMysqlProtocol().SendPrepareResponse(pse.prepareStmt); err2 != nil {
		trace.EndStatement(ctx, err2)
		retErr = moerr.NewInternalError("routine send response failed. error:%v ", err2)
		logStatementStatus(ctx, ses, pse.stmt, fail, retErr)
		return retErr
	}
	return nil
}

func (pse *PrepareStringExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	var err error
	pse.prepareStmt, err = doPrepareString(ctx, ses, pse.ps)
	if err != nil {
		return err
	}
	return authenticatePrivilegeOfPrepareOrExecute(ctx, ses, pse.prepareStmt.PrepareStmt, pse.prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
}

// TODO: DeallocateExecutor has no response like QUIT COMMAND ?
type DeallocateExecutor struct {
	*statusStmtExecutor
	d *tree.Deallocate
}

func (de *DeallocateExecutor) ExecuteImpl(ctx context.Context, ses *Session) error {
	return doDeallocate(ctx, ses, de.d)
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

type UpdateExecutor struct {
	*statusStmtExecutor
	u *tree.Update
}

type CreateAccountExecutor struct {
	*statusStmtExecutor
	ca *tree.CreateAccount
}

type DropAccountExecutor struct {
	*statusStmtExecutor
	da *tree.DropAccount
}

type AlterAccountExecutor struct {
	*statusStmtExecutor
	aa *tree.AlterAccount
}

type CreateUserExecutor struct {
	*statusStmtExecutor
	cu *tree.CreateUser
}

type DropUserExecutor struct {
	*statusStmtExecutor
	du *tree.DropUser
}

type AlterUserExecutor struct {
	*statusStmtExecutor
	au *tree.AlterUser
}

type CreateRoleExecutor struct {
	*statusStmtExecutor
	cr *tree.CreateRole
}

type DropRoleExecutor struct {
	*statusStmtExecutor
	dr *tree.DropRole
}

type GrantExecutor struct {
	*statusStmtExecutor
	g *tree.Grant
}

type RevokeExecutor struct {
	*statusStmtExecutor
	r *tree.Revoke
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

type DropViewExecutor struct {
	*statusStmtExecutor
	dv *tree.DropView
}

type InsertExecutor struct {
	*statusStmtExecutor
	i *tree.Insert
}

type LoadExecutor struct {
	*statusStmtExecutor
	l *tree.Load
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
