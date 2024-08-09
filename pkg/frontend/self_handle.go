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
	ses.EnterFPrint(9)
	defer ses.ExitFPrint(9)
	//check transaction states
	switch st := execCtx.stmt.(type) {
	case *tree.BeginTransaction:
		ses.EnterFPrint(10)
		defer ses.ExitFPrint(10)
		RecordStatementTxnID(execCtx.reqCtx, ses)
	case *tree.CommitTransaction:
	case *tree.RollbackTransaction:
	case *tree.SetRole:
		ses.EnterFPrint(11)
		defer ses.ExitFPrint(11)
		ses.InvalidatePrivilegeCache()
		//switch role
		err = handleSwitchRole(ses, execCtx, st)
		if err != nil {
			return
		}
	case *tree.Use:
		ses.EnterFPrint(12)
		defer ses.ExitFPrint(12)
		var uniqueCheckOnAuto string
		dbName := st.Name.Compare()
		//use database
		err = handleChangeDB(ses, execCtx, dbName)
		if err != nil {
			return
		}
		err = changeVersion(execCtx.reqCtx, ses, dbName)
		if err != nil {
			return
		}
		uniqueCheckOnAuto, err = GetUniqueCheckOnAutoIncr(execCtx.reqCtx, ses, dbName)
		if err != nil {
			return
		}
		ses.SetConfig(dbName, "unique_check_on_autoincr", uniqueCheckOnAuto)
	case *tree.MoDump:

		//dump
		err = handleDump(ses, execCtx, st)
		if err != nil {
			return
		}
	case *tree.PrepareStmt:
		ses.EnterFPrint(13)
		defer ses.ExitFPrint(13)
		execCtx.prepareStmt, err = handlePrepareStmt(ses, execCtx, st, execCtx.sqlOfStmt)
		if err != nil {
			return
		}
		err = authenticateUserCanExecutePrepareOrExecute(execCtx.reqCtx, ses, execCtx.prepareStmt.PrepareStmt, execCtx.prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
		if err != nil {
			ses.RemovePrepareStmt(execCtx.prepareStmt.Name)
			return
		}
	case *tree.PrepareString:
		ses.EnterFPrint(14)
		defer ses.ExitFPrint(14)
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
		ses.EnterFPrint(15)
		defer ses.ExitFPrint(15)
		err = handleCreateConnector(execCtx.reqCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.PauseDaemonTask:
		ses.EnterFPrint(16)
		defer ses.ExitFPrint(16)
		err = handlePauseDaemonTask(execCtx.reqCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.CancelDaemonTask:
		ses.EnterFPrint(17)
		defer ses.ExitFPrint(17)
		err = handleCancelDaemonTask(execCtx.reqCtx, ses, st.TaskID)
		if err != nil {
			return
		}
	case *tree.ResumeDaemonTask:
		ses.EnterFPrint(18)
		defer ses.ExitFPrint(18)
		err = handleResumeDaemonTask(execCtx.reqCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.DropConnector:
		ses.EnterFPrint(19)
		defer ses.ExitFPrint(19)
		err = handleDropConnector(execCtx.reqCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.ShowConnectors:
		ses.EnterFPrint(20)
		defer ses.ExitFPrint(20)
		if err = handleShowConnectors(execCtx.reqCtx, ses); err != nil {
			return
		}
	case *tree.Deallocate:
		ses.EnterFPrint(21)
		defer ses.ExitFPrint(21)
		err = handleDeallocate(ses, execCtx, st)
		if err != nil {
			return
		}
	case *tree.Reset:
		ses.EnterFPrint(22)
		defer ses.ExitFPrint(22)
		err = handleReset(ses, execCtx, st)
		if err != nil {
			return
		}
	case *tree.SetVar:
		ses.EnterFPrint(23)
		defer ses.ExitFPrint(23)
		err = handleSetVar(ses, execCtx, st, execCtx.sqlOfStmt)
		if err != nil {
			return
		}
	case *tree.ShowVariables:
		ses.EnterFPrint(24)
		defer ses.ExitFPrint(24)
		err = handleShowVariables(ses, execCtx, st)
		if err != nil {
			return
		}
	case *tree.ShowErrors, *tree.ShowWarnings:
		ses.EnterFPrint(25)
		defer ses.ExitFPrint(25)
		err = handleShowErrors(ses, execCtx)
		if err != nil {
			return
		}
	case *tree.AnalyzeStmt:
		ses.EnterFPrint(26)
		defer ses.ExitFPrint(26)
		if err = handleAnalyzeStmt(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ExplainStmt:
		ses.EnterFPrint(27)
		defer ses.ExitFPrint(27)
		if err = handleExplainStmt(ses, execCtx, st); err != nil {
			return
		}
	case *InternalCmdFieldList:
		ses.EnterFPrint(28)
		defer ses.ExitFPrint(28)
		if err = handleCmdFieldList(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreatePublication:
		ses.EnterFPrint(29)
		defer ses.ExitFPrint(29)
		if err = handleCreatePublication(ses, execCtx, st); err != nil {
			return
		}
	case *tree.AlterPublication:
		ses.EnterFPrint(30)
		defer ses.ExitFPrint(30)
		if err = handleAlterPublication(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropPublication:
		ses.EnterFPrint(31)
		defer ses.ExitFPrint(31)
		if err = handleDropPublication(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ShowPublications:
		//ses.EnterFPrint(32)
		//defer ses.ExitFPrint(32)
		if err = handleShowPublications(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ShowSubscriptions:
		ses.EnterFPrint(32)
		defer ses.ExitFPrint(32)
		if err = handleShowSubscriptions(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreateStage:
		ses.EnterFPrint(33)
		defer ses.ExitFPrint(33)
		if err = handleCreateStage(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropStage:
		ses.EnterFPrint(34)
		defer ses.ExitFPrint(34)
		if err = handleDropStage(ses, execCtx, st); err != nil {
			return
		}
	case *tree.AlterStage:
		ses.EnterFPrint(35)
		defer ses.ExitFPrint(35)
		if err = handleAlterStage(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreateAccount:
		ses.EnterFPrint(36)
		defer ses.ExitFPrint(36)
		ses.InvalidatePrivilegeCache()
		if err = handleCreateAccount(ses, execCtx, st, execCtx.proc); err != nil {
			return
		}
	case *tree.DropAccount:
		ses.EnterFPrint(37)
		defer ses.ExitFPrint(37)
		ses.InvalidatePrivilegeCache()
		if err = handleDropAccount(ses, execCtx, st, execCtx.proc); err != nil {
			return
		}
	case *tree.AlterAccount:
		ses.InvalidatePrivilegeCache()
		ses.EnterFPrint(38)
		defer ses.ExitFPrint(38)
		if err = handleAlterAccount(ses, execCtx, st, execCtx.proc); err != nil {
			return
		}
	case *tree.AlterDataBaseConfig:
		ses.InvalidatePrivilegeCache()
		ses.EnterFPrint(39)
		defer ses.ExitFPrint(39)
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
		ses.EnterFPrint(40)
		defer ses.ExitFPrint(40)
		ses.InvalidatePrivilegeCache()
		if err = handleCreateUser(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropUser:
		ses.EnterFPrint(41)
		defer ses.ExitFPrint(41)
		ses.InvalidatePrivilegeCache()
		if err = handleDropUser(ses, execCtx, st); err != nil {
			return
		}
	case *tree.AlterUser: //TODO
		ses.EnterFPrint(42)
		defer ses.ExitFPrint(42)
		ses.InvalidatePrivilegeCache()
		if err = handleAlterUser(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreateRole:
		ses.EnterFPrint(43)
		defer ses.ExitFPrint(43)
		ses.InvalidatePrivilegeCache()
		if err = handleCreateRole(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropRole:
		ses.EnterFPrint(44)
		defer ses.ExitFPrint(44)
		ses.InvalidatePrivilegeCache()
		if err = handleDropRole(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreateFunction:
		ses.EnterFPrint(45)
		defer ses.ExitFPrint(45)
		if err = st.Valid(); err != nil {
			return err
		}
		if err = handleCreateFunction(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropFunction:
		ses.EnterFPrint(46)
		defer ses.ExitFPrint(46)
		if err = handleDropFunction(ses, execCtx, st, execCtx.proc); err != nil {
			return
		}
	case *tree.CreateProcedure:
		ses.EnterFPrint(47)
		defer ses.ExitFPrint(47)
		if err = handleCreateProcedure(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropProcedure:
		ses.EnterFPrint(48)
		defer ses.ExitFPrint(48)
		if err = handleDropProcedure(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CallStmt:
		ses.EnterFPrint(49)
		defer ses.ExitFPrint(49)
		if err = handleCallProcedure(ses, execCtx, st); err != nil {
			return
		}
	case *tree.Grant:
		ses.EnterFPrint(50)
		defer ses.ExitFPrint(50)
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
		ses.EnterFPrint(51)
		defer ses.ExitFPrint(51)
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
		ses.EnterFPrint(52)
		defer ses.ExitFPrint(52)
		ses.InvalidatePrivilegeCache()
		if err = handleKill(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ShowAccounts:
		ses.EnterFPrint(53)
		defer ses.ExitFPrint(53)
		if err = handleShowAccounts(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ShowCollation:
		ses.EnterFPrint(54)
		defer ses.ExitFPrint(54)
		if err = handleShowCollation(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ShowBackendServers:
		ses.EnterFPrint(55)
		defer ses.ExitFPrint(55)
		if err = handleShowBackendServers(ses, execCtx); err != nil {
			return
		}
	case *tree.SetTransaction:
		ses.EnterFPrint(56)
		defer ses.ExitFPrint(56)
		//TODO: handle set transaction
	case *tree.LockTableStmt:

	case *tree.UnLockTableStmt:

	case *tree.BackupStart:
		ses.EnterFPrint(57)
		defer ses.ExitFPrint(57)
		if err = handleStartBackup(ses, execCtx, st); err != nil {
			return
		}
	case *tree.EmptyStmt:

		if err = handleEmptyStmt(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreateSnapShot:
		ses.EnterFPrint(58)
		defer ses.ExitFPrint(58)
		//TODO: invalidate privilege cache
		if err = handleCreateSnapshot(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropSnapShot:
		ses.EnterFPrint(59)
		defer ses.ExitFPrint(59)
		//TODO: invalidate privilege cache
		if err = handleDropSnapshot(ses, execCtx, st); err != nil {
			return
		}
	case *tree.RestoreSnapShot:
		ses.EnterFPrint(60)
		defer ses.ExitFPrint(60)
		//TODO: invalidate privilege cache
		if err = handleRestoreSnapshot(ses, execCtx, st); err != nil {
			return
		}
	case *tree.UpgradeStatement:
		ses.EnterFPrint(61)
		defer ses.ExitFPrint(61)
		//TODO: invalidate privilege cache
		if err = handleExecUpgrade(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreatePitr:
		ses.EnterFPrint(120)
		defer ses.ExitFPrint(120)
		//TODO: invalidate privilege cache
		if err = handleCreatePitr(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropPitr:
		ses.EnterFPrint(121)
		defer ses.ExitFPrint(121)
		//TODO: invalidate privilege cache
		if err = handleDropPitr(ses, execCtx, st); err != nil {
			return
		}
	case *tree.AlterPitr:
		ses.EnterFPrint(122)
		defer ses.ExitFPrint(122)
		//TODO: invalidate privilege cache
		if err = handleAlterPitr(ses, execCtx, st); err != nil {
			return
		}
	case *tree.RestorePitr:
		ses.EnterFPrint(123)
		defer ses.ExitFPrint(123)
		//TODO: invalidate privilege cache
		if err = handleRestorePitr(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreateCDC:
	case *tree.PauseCDC:
	case *tree.DropCDC:
	case *tree.RestartCDC:
	case *tree.ResumeCDC:
	case *tree.ShowCDC:
	}
	return
}
