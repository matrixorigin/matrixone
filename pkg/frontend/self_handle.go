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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func execInFrontend(ses *Session, execCtx *ExecCtx) (err error) {
	//check transaction states
	switch st := execCtx.stmt.(type) {
	case *tree.BeginTransaction:
		RecordStatementTxnID(execCtx.reqCtx, ses)
	case *tree.CommitTransaction:
	case *tree.RollbackTransaction:
	case *tree.SetRole:

		ses.InvalidatePrivilegeCache()
		//switch role
		err = handleSwitchRole(ses, execCtx, st)
		if err != nil {
			return
		}
	case *tree.Use:

		var v interface{}
		v, err = ses.GetGlobalVar(execCtx.reqCtx, "lower_case_table_names")
		if err != nil {
			return
		}
		st.Name.SetConfig(v.(int64))
		//use database
		err = handleChangeDB(ses, execCtx, st.Name.Compare())
		if err != nil {
			return
		}
		err = changeVersion(execCtx.reqCtx, ses, st.Name.Compare())
		if err != nil {
			return
		}
	case *tree.MoDump:

		//dump
		err = handleDump(ses, execCtx, st)
		if err != nil {
			return
		}
	case *tree.PrepareStmt:

		execCtx.prepareStmt, err = handlePrepareStmt(ses, execCtx, st)
		if err != nil {
			return
		}
		err = authenticateUserCanExecutePrepareOrExecute(execCtx.reqCtx, ses, execCtx.prepareStmt.PrepareStmt, execCtx.prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
		if err != nil {
			ses.RemovePrepareStmt(execCtx.prepareStmt.Name)
			return
		}
	case *tree.PrepareString:
		execCtx.prepareStmt, err = handlePrepareString(ses, execCtx, st)
		if err != nil {
			return
		}
		err = authenticateUserCanExecutePrepareOrExecute(execCtx.reqCtx, ses, execCtx.prepareStmt.PrepareStmt, execCtx.prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
		if err != nil {
			ses.RemovePrepareStmt(execCtx.prepareStmt.Name)
			return
		}
	case *tree.CreateConnector:

		err = handleCreateConnector(execCtx.reqCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.PauseDaemonTask:

		err = handlePauseDaemonTask(execCtx.reqCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.CancelDaemonTask:

		err = handleCancelDaemonTask(execCtx.reqCtx, ses, st.TaskID)
		if err != nil {
			return
		}
	case *tree.ResumeDaemonTask:

		err = handleResumeDaemonTask(execCtx.reqCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.DropConnector:

		err = handleDropConnector(execCtx.reqCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.ShowConnectors:

		if err = handleShowConnectors(execCtx.reqCtx, ses); err != nil {
			return
		}
	case *tree.Deallocate:

		err = handleDeallocate(ses, execCtx, st)
		if err != nil {
			return
		}
	case *tree.Reset:

		err = handleReset(ses, execCtx, st)
		if err != nil {
			return
		}
	case *tree.SetVar:

		err = handleSetVar(ses, execCtx, st, execCtx.sqlOfStmt)
		if err != nil {
			return
		}
	case *tree.ShowVariables:

		err = handleShowVariables(ses, execCtx, st)
		if err != nil {
			return
		}
	case *tree.ShowErrors, *tree.ShowWarnings:

		err = handleShowErrors(ses)
		if err != nil {
			return
		}
	case *tree.AnalyzeStmt:

		if err = handleAnalyzeStmt(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ExplainStmt:

		if err = handleExplainStmt(ses, execCtx, st); err != nil {
			return
		}
	case *InternalCmdFieldList:

		if err = handleCmdFieldList(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreatePublication:

		if err = handleCreatePublication(ses, execCtx, st); err != nil {
			return
		}
	case *tree.AlterPublication:

		if err = handleAlterPublication(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropPublication:

		if err = handleDropPublication(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ShowSubscriptions:

		if err = handleShowSubscriptions(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreateStage:

		if err = handleCreateStage(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropStage:

		if err = handleDropStage(ses, execCtx, st); err != nil {
			return
		}
	case *tree.AlterStage:

		if err = handleAlterStage(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreateAccount:

		ses.InvalidatePrivilegeCache()
		if err = handleCreateAccount(ses, execCtx, st, execCtx.proc); err != nil {
			return
		}
	case *tree.DropAccount:

		ses.InvalidatePrivilegeCache()
		if err = handleDropAccount(ses, execCtx, st, execCtx.proc); err != nil {
			return
		}
	case *tree.AlterAccount:
		ses.InvalidatePrivilegeCache()

		if err = handleAlterAccount(ses, execCtx, st, execCtx.proc); err != nil {
			return
		}
	case *tree.AlterDataBaseConfig:
		ses.InvalidatePrivilegeCache()

		if st.IsAccountLevel {
			if err = handleAlterAccountConfig(ses, execCtx, st); err != nil {
				return
			}
		} else {
			if err = handleAlterDataBaseConfig(ses, execCtx, st); err != nil {
				return
			}
		}
	case *tree.CreateUser:

		ses.InvalidatePrivilegeCache()
		if err = handleCreateUser(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropUser:

		ses.InvalidatePrivilegeCache()
		if err = handleDropUser(ses, execCtx, st); err != nil {
			return
		}
	case *tree.AlterUser: //TODO

		ses.InvalidatePrivilegeCache()
		if err = handleAlterUser(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreateRole:

		ses.InvalidatePrivilegeCache()
		if err = handleCreateRole(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropRole:

		ses.InvalidatePrivilegeCache()
		if err = handleDropRole(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreateFunction:

		if err = st.Valid(); err != nil {
			return err
		}
		if err = handleCreateFunction(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropFunction:

		if err = handleDropFunction(ses, execCtx, st, execCtx.proc); err != nil {
			return
		}
	case *tree.CreateProcedure:

		if err = handleCreateProcedure(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropProcedure:

		if err = handleDropProcedure(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CallStmt:

		if err = handleCallProcedure(ses, execCtx, st, execCtx.proc); err != nil {
			return
		}
	case *tree.Grant:

		ses.InvalidatePrivilegeCache()
		switch st.Typ {
		case tree.GrantTypeRole:
			if err = handleGrantRole(ses, execCtx, &st.GrantRole); err != nil {
				return
			}
		case tree.GrantTypePrivilege:
			if err = handleGrantPrivilege(ses, execCtx, &st.GrantPrivilege); err != nil {
				return
			}
		}
	case *tree.Revoke:

		ses.InvalidatePrivilegeCache()
		switch st.Typ {
		case tree.RevokeTypeRole:
			if err = handleRevokeRole(ses, execCtx, &st.RevokeRole); err != nil {
				return
			}
		case tree.RevokeTypePrivilege:
			if err = handleRevokePrivilege(ses, execCtx, &st.RevokePrivilege); err != nil {
				return
			}
		}
	case *tree.Kill:

		ses.InvalidatePrivilegeCache()
		if err = handleKill(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ShowAccounts:

		if err = handleShowAccounts(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ShowCollation:

		if err = handleShowCollation(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ShowBackendServers:
		if err = handleShowBackendServers(ses, execCtx); err != nil {
			return
		}
	case *tree.SetTransaction:

		//TODO: handle set transaction
	case *tree.LockTableStmt:

	case *tree.UnLockTableStmt:

	case *tree.BackupStart:

		if err = handleStartBackup(ses, execCtx, st); err != nil {
			return
		}
	case *tree.EmptyStmt:

		if err = handleEmptyStmt(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreateSnapShot:
		//TODO: invalidate privilege cache
		if err = handleCreateSnapshot(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropSnapShot:
		//TODO: invalidate privilege cache
		if err = handleDropSnapshot(ses, execCtx, st); err != nil {
			return
		}
	case *tree.RestoreSnapShot:
		//TODO: invalidate privilege cache
		if err = handleRestoreSnapshot(ses, execCtx, st); err != nil {
			return
		}
	case *tree.UpgradeStatement:
		//TODO: invalidate privilege cache
		if err = handleExecUpgrade(ses, execCtx, st); err != nil {
			return
		}
	}
	return
}
