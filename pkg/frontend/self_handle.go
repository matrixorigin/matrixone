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
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
)

func execInFrontend(ses *Session, execCtx *ExecCtx) (stats statistic.StatsArray, err error) {
	finishRunSQL := enterFrontendRunSQL(ses, execCtx)
	defer finishRunSQL()
	ses.EnterFPrint(FPExecInFrontEnd)
	defer ses.ExitFPrint(FPExecInFrontEnd)
	//check transaction states
	switch st := execCtx.stmt.(type) {
	case *tree.BeginTransaction:
		ses.EnterFPrint(FPBeginTxn)
		defer ses.ExitFPrint(FPBeginTxn)
		RecordStatementTxnID(execCtx.reqCtx, ses)
	case *tree.CommitTransaction:
	case *tree.RollbackTransaction:
	case *tree.SavePoint, *tree.ReleaseSavePoint, *tree.RollbackToSavePoint:
	case *tree.SetRole:
		ses.EnterFPrint(FPSetRole)
		defer ses.ExitFPrint(FPSetRole)
		ses.InvalidatePrivilegeCache()
		//switch role
		err = handleSwitchRole(ses, execCtx, st)
		if err != nil {
			return
		}
	case *tree.Use:
		ses.EnterFPrint(FPUse)
		defer ses.ExitFPrint(FPUse)
		dbName := st.Name.Compare()
		//use database
		err = handleChangeDB(ses, execCtx, dbName)
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
		ses.EnterFPrint(FPPrepareStmt)
		defer ses.ExitFPrint(FPPrepareStmt)
		execCtx.prepareStmt, err = handlePrepareStmt(ses, execCtx, st, execCtx.sqlOfStmt)
		if err != nil {
			return
		}
		_, err = authenticateUserCanExecutePrepareOrExecute(execCtx.reqCtx, ses, execCtx.prepareStmt.PrepareStmt, execCtx.prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
		if err != nil {
			ses.RemovePrepareStmt(execCtx.prepareStmt.Name)
			return
		}
	case *tree.PrepareString, *tree.PrepareVar:
		ses.EnterFPrint(FPPrepareString)
		defer ses.ExitFPrint(FPPrepareString)
		switch st := st.(type) {
		case *tree.PrepareString:
			execCtx.prepareStmt, err = handlePrepareString(ses, execCtx, st)
		case *tree.PrepareVar:
			execCtx.prepareStmt, err = handlePrepareVar(ses, execCtx, st)
		}
		if err != nil {
			return
		}
		_, err = authenticateUserCanExecutePrepareOrExecute(execCtx.reqCtx, ses, execCtx.prepareStmt.PrepareStmt, execCtx.prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
		if err != nil {
			ses.RemovePrepareStmt(execCtx.prepareStmt.Name)
			return
		}
	case *tree.CreateConnector:
		ses.EnterFPrint(FPCreateConnector)
		defer ses.ExitFPrint(FPCreateConnector)
		err = handleCreateConnector(execCtx.reqCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.PauseDaemonTask:
		ses.EnterFPrint(FPPauseDaemonTask)
		defer ses.ExitFPrint(FPPauseDaemonTask)
		err = handlePauseDaemonTask(execCtx.reqCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.CancelDaemonTask:
		ses.EnterFPrint(FPCancelDaemonTask)
		defer ses.ExitFPrint(FPCancelDaemonTask)
		err = handleCancelDaemonTask(execCtx.reqCtx, ses, st.TaskID)
		if err != nil {
			return
		}
	case *tree.ResumeDaemonTask:
		ses.EnterFPrint(FPResumeDaemonTask)
		defer ses.ExitFPrint(FPResumeDaemonTask)
		err = handleResumeDaemonTask(execCtx.reqCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.DropConnector:
		ses.EnterFPrint(FPDropConnector)
		defer ses.ExitFPrint(FPDropConnector)
		err = handleDropConnector(execCtx.reqCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.ShowConnectors:
		ses.EnterFPrint(FPShowConnectors)
		defer ses.ExitFPrint(FPShowConnectors)
		if err = handleShowConnectors(execCtx.reqCtx, ses); err != nil {
			return
		}
	case *tree.Deallocate:
		ses.EnterFPrint(FPDeallocate)
		defer ses.ExitFPrint(FPDeallocate)
		err = handleDeallocate(ses, execCtx, st)
		if err != nil {
			return
		}
	case *tree.Reset:
		ses.EnterFPrint(FPReset)
		defer ses.ExitFPrint(FPReset)
		err = handleReset(ses, execCtx, st)
		if err != nil {
			return
		}
	case *tree.SetVar:
		ses.EnterFPrint(FPSetVar)
		defer ses.ExitFPrint(FPSetVar)
		err = handleSetVar(ses, execCtx, st, execCtx.sqlOfStmt)
		if err != nil {
			return
		}
	case *tree.ShowVariables:
		ses.EnterFPrint(FPShowVariables)
		defer ses.ExitFPrint(FPShowVariables)
		err = handleShowVariables(ses, execCtx, st)
		if err != nil {
			return
		}
	case *tree.ShowErrors, *tree.ShowWarnings:
		ses.EnterFPrint(FPShowErrors)
		defer ses.ExitFPrint(FPShowErrors)
		err = handleShowErrors(ses, execCtx)
		if err != nil {
			return
		}
	case *tree.AnalyzeStmt:
		ses.EnterFPrint(FPAnalyzeStmt)
		defer ses.ExitFPrint(FPAnalyzeStmt)
		if err = handleAnalyzeStmt(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ExplainStmt:
		ses.EnterFPrint(FPExplainStmt)
		defer ses.ExitFPrint(FPExplainStmt)
		if err = handleExplainStmt(ses, execCtx, st); err != nil {
			return
		}
	case *InternalCmdFieldList:
		ses.EnterFPrint(FPInternalCmdFieldList)
		defer ses.ExitFPrint(FPInternalCmdFieldList)
		if err = handleCmdFieldList(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreatePublication:
		ses.EnterFPrint(FPCreatePublication)
		defer ses.ExitFPrint(FPCreatePublication)
		if err = handleCreatePublication(ses, execCtx, st); err != nil {
			return
		}
	case *tree.AlterPublication:
		ses.EnterFPrint(FPAlterPublication)
		defer ses.ExitFPrint(FPAlterPublication)
		if err = handleAlterPublication(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropPublication:
		ses.EnterFPrint(FPDropPublication)
		defer ses.ExitFPrint(FPDropPublication)
		if err = handleDropPublication(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ShowPublications:
		ses.EnterFPrint(FPShowPublications)
		defer ses.ExitFPrint(FPShowPublications)
		if err = handleShowPublications(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ShowSubscriptions:
		ses.EnterFPrint(FPShowSubscriptions)
		defer ses.ExitFPrint(FPShowSubscriptions)
		if err = handleShowSubscriptions(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreateStage:
		ses.EnterFPrint(FPCreateStage)
		defer ses.ExitFPrint(FPCreateStage)
		if err = handleCreateStage(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropStage:
		ses.EnterFPrint(FPDropStage)
		defer ses.ExitFPrint(FPDropStage)
		if err = handleDropStage(ses, execCtx, st); err != nil {
			return
		}
	case *tree.AlterStage:
		ses.EnterFPrint(FPAlterStage)
		defer ses.ExitFPrint(FPAlterStage)
		if err = handleAlterStage(ses, execCtx, st); err != nil {
			return
		}
	case *tree.RemoveStageFiles:
		ses.EnterFPrint(FPRemoveStageFiles)
		defer ses.ExitFPrint(FPRemoveStageFiles)
		if err = handleRemoveStageFiles(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreateAccount:
		ses.EnterFPrint(FPCreateAccount)
		defer ses.ExitFPrint(FPCreateAccount)
		ses.InvalidatePrivilegeCache()
		if err = handleCreateAccount(ses, execCtx, st, execCtx.proc); err != nil {
			return
		}
	case *tree.DropAccount:
		ses.EnterFPrint(FPDropAccount)
		defer ses.ExitFPrint(FPDropAccount)
		ses.InvalidatePrivilegeCache()
		if err = handleDropAccount(ses, execCtx, st, execCtx.proc); err != nil {
			return
		}
	case *tree.AlterAccount:
		ses.InvalidatePrivilegeCache()
		ses.EnterFPrint(FPAlterAccount)
		defer ses.ExitFPrint(FPAlterAccount)
		if err = handleAlterAccount(ses, execCtx, st, execCtx.proc); err != nil {
			return
		}
	case *tree.AlterDataBaseConfig:
		ses.InvalidatePrivilegeCache()
		ses.EnterFPrint(FPAlterDataBaseConfig)
		defer ses.ExitFPrint(FPAlterDataBaseConfig)
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
		ses.EnterFPrint(FPCreateUser)
		defer ses.ExitFPrint(FPCreateUser)
		ses.InvalidatePrivilegeCache()
		if err = handleCreateUser(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropUser:
		ses.EnterFPrint(FPDropUser)
		defer ses.ExitFPrint(FPDropUser)
		ses.InvalidatePrivilegeCache()
		if err = handleDropUser(ses, execCtx, st); err != nil {
			return
		}
	case *tree.AlterUser: //TODO
		ses.EnterFPrint(FPAlterUser)
		defer ses.ExitFPrint(FPAlterUser)
		ses.InvalidatePrivilegeCache()
		if err = handleAlterUser(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreateRole:
		ses.EnterFPrint(FPCreateRole)
		defer ses.ExitFPrint(FPCreateRole)
		ses.InvalidatePrivilegeCache()
		if err = handleCreateRole(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropRole:
		ses.EnterFPrint(FPDropRole)
		defer ses.ExitFPrint(FPDropRole)
		ses.InvalidatePrivilegeCache()
		if err = handleDropRole(ses, execCtx, st); err != nil {
			return
		}
	case *tree.AlterRole:
		ses.EnterFPrint(FPAlterRole)
		defer ses.ExitFPrint(FPAlterRole)
		ses.InvalidatePrivilegeCache()
		if err = handleAlterRole(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreateFunction:
		ses.EnterFPrint(FPCreateFunction)
		defer ses.ExitFPrint(FPCreateFunction)
		if err = st.Valid(); err != nil {
			return
		}
		if err = handleCreateFunction(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropFunction:
		ses.EnterFPrint(FPDropFunction)
		defer ses.ExitFPrint(FPDropFunction)
		if err = handleDropFunction(ses, execCtx, st, execCtx.proc); err != nil {
			return
		}
	case *tree.CreateProcedure:
		ses.EnterFPrint(FPCreateProcedure)
		defer ses.ExitFPrint(FPCreateProcedure)
		if err = handleCreateProcedure(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropProcedure:
		ses.EnterFPrint(FPDropProcedure)
		defer ses.ExitFPrint(FPDropProcedure)
		if err = handleDropProcedure(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CallStmt:
		ses.EnterFPrint(FPCallStmt)
		defer ses.ExitFPrint(FPCallStmt)
		if err = handleCallProcedure(ses, execCtx, st, false); err != nil {
			return
		}
	case *tree.Grant:
		ses.EnterFPrint(FPGrant)
		defer ses.ExitFPrint(FPGrant)
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
		ses.EnterFPrint(FPRevoke)
		defer ses.ExitFPrint(FPRevoke)
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
		ses.EnterFPrint(FPKill)
		defer ses.ExitFPrint(FPKill)
		ses.InvalidatePrivilegeCache()
		if err = handleKill(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ShowAccounts:
		ses.EnterFPrint(FPShowAccounts)
		defer ses.ExitFPrint(FPShowAccounts)
		if err = handleShowAccounts(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ShowCollation:
		ses.EnterFPrint(FPShowCollation)
		defer ses.ExitFPrint(FPShowCollation)
		if err = handleShowCollation(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ShowBackendServers:
		ses.EnterFPrint(FPShowBackendServers)
		defer ses.ExitFPrint(FPShowBackendServers)
		if err = handleShowBackendServers(ses, execCtx); err != nil {
			return
		}
	case *tree.SetTransaction:
		ses.EnterFPrint(FPSetTransaction)
		defer ses.ExitFPrint(FPSetTransaction)
		//TODO: handle set transaction
	case *tree.LockTableStmt:

	case *tree.UnLockTableStmt:

	case *tree.BackupStart:
		ses.EnterFPrint(FPBackupStart)
		defer ses.ExitFPrint(FPBackupStart)
		if err = handleStartBackup(ses, execCtx, st); err != nil {
			return
		}
	case *tree.EmptyStmt:

		if err = handleEmptyStmt(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreateSnapShot:
		ses.EnterFPrint(FPCreateSnapShot)
		defer ses.ExitFPrint(FPCreateSnapShot)
		//TODO: invalidate privilege cache
		if err = handleCreateSnapshot(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropSnapShot:
		ses.EnterFPrint(FPDropSnapShot)
		defer ses.ExitFPrint(FPDropSnapShot)
		//TODO: invalidate privilege cache
		if err = handleDropSnapshot(ses, execCtx, st); err != nil {
			return
		}
	case *tree.RestoreSnapShot:
		ses.EnterFPrint(FPRestoreSnapShot)
		defer ses.ExitFPrint(FPRestoreSnapShot)
		//TODO: invalidate privilege cache
		stats, err = handleRestoreSnapshot(ses, execCtx, st)
		if err != nil {
			return
		}
	case *tree.UpgradeStatement:
		ses.EnterFPrint(FPUpgradeStatement)
		defer ses.ExitFPrint(FPUpgradeStatement)
		//TODO: invalidate privilege cache
		if err = handleExecUpgrade(ses, execCtx, st); err != nil {
			return
		}
	case *tree.CreatePitr:
		ses.EnterFPrint(FPCreatePitr)
		defer ses.ExitFPrint(FPCreatePitr)
		//TODO: invalidate privilege cache
		if err = handleCreatePitr(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropPitr:
		ses.EnterFPrint(FPDropPitr)
		defer ses.ExitFPrint(FPDropPitr)
		//TODO: invalidate privilege cache
		if err = handleDropPitr(ses, execCtx, st); err != nil {
			return
		}
	case *tree.AlterPitr:
		ses.EnterFPrint(FPAlterPitr)
		defer ses.ExitFPrint(FPAlterPitr)
		//TODO: invalidate privilege cache
		if err = handleAlterPitr(ses, execCtx, st); err != nil {
			return
		}
	case *tree.RestorePitr:
		ses.EnterFPrint(FPRestorePitr)
		defer ses.ExitFPrint(FPRestorePitr)
		//TODO: invalidate privilege cache
		stats, err = handleRestorePitr(ses, execCtx, st)
		if err != nil {
			return
		}
	case *tree.ShowRecoveryWindow:
		ses.EnterFPrint(FPShowRecoveryWindow)
		defer ses.ExitFPrint(FPShowRecoveryWindow)
		if err = handleShowRecoveryWindow(ses, execCtx, st); err != nil {
			return
		}
	case *tree.SetConnectionID:
		ses.EnterFPrint(FPSetConnectionID)
		defer ses.ExitFPrint(FPSetConnectionID)
		ses.SetConnectionID(st.ConnectionID)
	case *tree.CreateCDC:
		ses.EnterFPrint(FPCreateCDC)
		defer ses.ExitFPrint(FPCreateCDC)
		if err = handleCreateCdc(ses, execCtx, st); err != nil {
			return
		}
	case *tree.PauseCDC:
		ses.EnterFPrint(FPPauseCDC)
		defer ses.ExitFPrint(FPPauseCDC)
		if err = handlePauseCdc(ses, execCtx, st); err != nil {
			return
		}
	case *tree.DropCDC:
		ses.EnterFPrint(FPDropCDC)
		defer ses.ExitFPrint(FPDropCDC)
		if err = handleDropCdc(ses, execCtx, st); err != nil {
			return
		}
	case *tree.RestartCDC:
		ses.EnterFPrint(FPRestartCDC)
		defer ses.ExitFPrint(FPRestartCDC)
		if err = handleRestartCdc(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ResumeCDC:
		ses.EnterFPrint(FPResumeCDC)
		defer ses.ExitFPrint(FPResumeCDC)
		if err = handleResumeCdc(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ShowCDC:
		ses.EnterFPrint(FPShowCDC)
		defer ses.ExitFPrint(FPShowCDC)
		if err = handleShowCdc(ses, execCtx, st); err != nil {
			return
		}
	case *tree.ShowLogserviceReplicas:
		if err = handleShowLogserviceReplicas(execCtx, ses); err != nil {
			return
		}
	case *tree.ShowLogserviceStores:
		if err = handleShowLogserviceStores(execCtx, ses); err != nil {
			return
		}
	case *tree.ShowLogserviceSettings:
		if err = handleShowLogserviceSettings(execCtx, ses); err != nil {
			return
		}
	case *tree.SetLogserviceSettings:
		if err = handleSetLogserviceSettings(execCtx, ses, st); err != nil {
			return
		}
	case *tree.CloneDatabase:
		ses.EnterFPrint(FPCloneDatabase)
		defer ses.ExitFPrint(FPCloneDatabase)
		if _, err = handleCloneDatabase(execCtx, ses, nil, st); err != nil {
			return
		}

	case *tree.CloneTable:
		ses.EnterFPrint(FPCloneTable)
		defer ses.ExitFPrint(FPCloneTable)
		if _, err = handleCloneTable(execCtx, ses, st, nil); err != nil {
			return
		}

	case *tree.DataBranchDiff,
		*tree.DataBranchMerge,
		*tree.DataBranchCreateTable,
		*tree.DataBranchDeleteTable,
		*tree.DataBranchDeleteDatabase,
		*tree.DataBranchCreateDatabase:

		ses.EnterFPrint(FPDataBranch)
		defer ses.ExitFPrint(FPDataBranch)
		if err = handleDataBranch(execCtx, ses, st); err != nil {
			return
		}
	}
	return
}

func enterFrontendRunSQL(ses *Session, execCtx *ExecCtx) func() {
	if ses == nil || execCtx == nil {
		return func() {}
	}
	txnHandler := ses.GetTxnHandler()
	if txnHandler == nil {
		return func() {}
	}
	txnOp := txnHandler.GetTxn()
	if txnOp == nil {
		return func() {}
	}
	sqlText := execCtx.sqlOfStmt
	if sqlText == "" {
		sqlText = ses.GetSql()
	}
	ctx := execCtx.reqCtx
	if ctx == nil {
		ctx = context.Background()
	}
	_, cancel := context.WithCancel(ctx)
	token := txnOp.EnterRunSqlWithTokenAndSQL(cancel, sqlText)
	if token != 0 {
		ses.pushRunSQLToken(token)
	}
	return func() {
		txnOp.ExitRunSqlWithToken(token)
		if token != 0 {
			ses.popRunSQLToken()
		}
		cancel()
	}
}
