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
)

func execInFrontend(requestCtx context.Context,
	ses *Session,
	execCtx *ExecCtx,
) (err error) {
	//check transaction states
	switch st := execCtx.stmt.(type) {
	case *tree.BeginTransaction:
		err = ses.GetTxnHandler().TxnBegin()
		if err != nil {
			return
		}
		RecordStatementTxnID(requestCtx, ses)
	case *tree.CommitTransaction:
		err = ses.GetTxnHandler().TxnCommit()
		if err != nil {
			return
		}
	case *tree.RollbackTransaction:
		err = ses.GetTxnHandler().TxnRollback()
		if err != nil {
			return
		}
	case *tree.SetRole:

		ses.InvalidatePrivilegeCache()
		//switch role
		err = handleSwitchRole(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.Use:

		var v interface{}
		v, err = ses.GetGlobalVar("lower_case_table_names")
		if err != nil {
			return
		}
		st.Name.SetConfig(v.(int64))
		//use database
		err = handleChangeDB(requestCtx, ses, st.Name.Compare())
		if err != nil {
			return
		}
		err = changeVersion(requestCtx, ses, st.Name.Compare())
		if err != nil {
			return
		}
	case *tree.MoDump:

		//dump
		err = handleDump(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.PrepareStmt:
		_, ses.proc.TxnOperator, err = ses.GetTxnHandler().GetTxn()
		if err != nil {
			return
		}
		execCtx.prepareStmt, err = handlePrepareStmt(requestCtx, ses, st, execCtx.sqlOfStmt)
		if err != nil {
			return
		}
		err = authenticateUserCanExecutePrepareOrExecute(requestCtx, ses, execCtx.prepareStmt.PrepareStmt, execCtx.prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
		if err != nil {
			ses.RemovePrepareStmt(execCtx.prepareStmt.Name)
			return
		}
	case *tree.PrepareString:
		_, ses.proc.TxnOperator, err = ses.GetTxnHandler().GetTxn()
		if err != nil {
			return
		}
		execCtx.prepareStmt, err = handlePrepareString(requestCtx, ses, st)
		if err != nil {
			return
		}
		err = authenticateUserCanExecutePrepareOrExecute(requestCtx, ses, execCtx.prepareStmt.PrepareStmt, execCtx.prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
		if err != nil {
			ses.RemovePrepareStmt(execCtx.prepareStmt.Name)
			return
		}
	case *tree.CreateConnector:

		err = handleCreateConnector(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.PauseDaemonTask:

		err = handlePauseDaemonTask(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.CancelDaemonTask:

		err = handleCancelDaemonTask(requestCtx, ses, st.TaskID)
		if err != nil {
			return
		}
	case *tree.ResumeDaemonTask:

		err = handleResumeDaemonTask(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.DropConnector:

		err = handleDropConnector(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.ShowConnectors:

		if err = handleShowConnectors(requestCtx, ses, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.Deallocate:

		err = handleDeallocate(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.Reset:

		err = handleReset(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.SetVar:

		err = handleSetVar(requestCtx, ses, st, execCtx.sqlOfStmt)
		if err != nil {
			return
		}
	case *tree.ShowVariables:

		err = handleShowVariables(ses, st, execCtx.proc, execCtx.isLastStmt)
		if err != nil {
			return
		}
	case *tree.ShowErrors, *tree.ShowWarnings:

		err = handleShowErrors(ses, execCtx.isLastStmt)
		if err != nil {
			return
		}
	case *tree.AnalyzeStmt:

		if err = handleAnalyzeStmt(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.ExplainStmt:

		if err = handleExplainStmt(requestCtx, ses, st); err != nil {
			return
		}
	case *InternalCmdFieldList:

		if err = handleCmdFieldList(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CreatePublication:

		if err = handleCreatePublication(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.AlterPublication:

		if err = handleAlterPublication(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropPublication:

		if err = handleDropPublication(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.ShowSubscriptions:

		if err = handleShowSubscriptions(requestCtx, ses, st, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.CreateStage:

		if err = handleCreateStage(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropStage:

		if err = handleDropStage(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.AlterStage:

		if err = handleAlterStage(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CreateAccount:

		ses.InvalidatePrivilegeCache()
		if err = handleCreateAccount(requestCtx, ses, st, execCtx.proc); err != nil {
			return
		}
	case *tree.DropAccount:

		ses.InvalidatePrivilegeCache()
		if err = handleDropAccount(requestCtx, ses, st, execCtx.proc); err != nil {
			return
		}
	case *tree.AlterAccount:
		ses.InvalidatePrivilegeCache()

		if err = handleAlterAccount(requestCtx, ses, st, execCtx.proc); err != nil {
			return
		}
	case *tree.AlterDataBaseConfig:
		ses.InvalidatePrivilegeCache()

		if st.IsAccountLevel {
			if err = handleAlterAccountConfig(requestCtx, ses, st); err != nil {
				return
			}
		} else {
			if err = handleAlterDataBaseConfig(requestCtx, ses, st); err != nil {
				return
			}
		}
	case *tree.CreateUser:

		ses.InvalidatePrivilegeCache()
		if err = handleCreateUser(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropUser:

		ses.InvalidatePrivilegeCache()
		if err = handleDropUser(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.AlterUser: //TODO

		ses.InvalidatePrivilegeCache()
		if err = handleAlterUser(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CreateRole:

		ses.InvalidatePrivilegeCache()
		if err = handleCreateRole(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropRole:

		ses.InvalidatePrivilegeCache()
		if err = handleDropRole(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CreateFunction:

		if err = st.Valid(); err != nil {
			return err
		}
		if err = handleCreateFunction(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropFunction:

		if err = handleDropFunction(requestCtx, ses, st, execCtx.proc); err != nil {
			return
		}
	case *tree.CreateProcedure:

		if err = handleCreateProcedure(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropProcedure:

		if err = handleDropProcedure(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CallStmt:

		if err = handleCallProcedure(requestCtx, ses, st, execCtx.proc); err != nil {
			return
		}
	case *tree.Grant:

		ses.InvalidatePrivilegeCache()
		switch st.Typ {
		case tree.GrantTypeRole:
			if err = handleGrantRole(requestCtx, ses, &st.GrantRole); err != nil {
				return
			}
		case tree.GrantTypePrivilege:
			if err = handleGrantPrivilege(requestCtx, ses, &st.GrantPrivilege); err != nil {
				return
			}
		}
	case *tree.Revoke:

		ses.InvalidatePrivilegeCache()
		switch st.Typ {
		case tree.RevokeTypeRole:
			if err = handleRevokeRole(requestCtx, ses, &st.RevokeRole); err != nil {
				return
			}
		case tree.RevokeTypePrivilege:
			if err = handleRevokePrivilege(requestCtx, ses, &st.RevokePrivilege); err != nil {
				return
			}
		}
	case *tree.Kill:

		ses.InvalidatePrivilegeCache()
		if err = handleKill(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.ShowAccounts:

		if err = handleShowAccounts(requestCtx, ses, st, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.ShowCollation:

		if err = handleShowCollation(ses, st, execCtx.proc, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.ShowBackendServers:

		if err = handleShowBackendServers(requestCtx, ses, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.SetTransaction:

		//TODO: handle set transaction
	case *tree.LockTableStmt:

	case *tree.UnLockTableStmt:

	case *tree.BackupStart:

		if err = handleStartBackup(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.EmptyStmt:

		if err = handleEmptyStmt(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CreateSnapShot:
		//TODO: invalidate privilege cache
		if err = handleCreateSnapshot(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropSnapShot:
		//TODO: invalidate privilege cache
		if err = handleDropSnapshot(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.RestoreSnapShot:
		//TODO: invalidate privilege cache
		if err = handleRestoreSnapshot(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.UpgradeStatement:
		//TODO: invalidate privilege cache
		if err = handleExecUpgrade(requestCtx, ses, st); err != nil {
			return
		}
	}
	return
}
