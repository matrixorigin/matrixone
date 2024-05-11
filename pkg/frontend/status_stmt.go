// Copyright 2021 - 2024 Matrix Origin
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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// executeStatusStmt run the statement that responses status t
func executeStatusStmt(requestCtx context.Context, ses *Session, execCtx *ExecCtx) (err error) {
	var loadLocalErrGroup *errgroup.Group
	var columns []interface{}

	mrs := ses.GetMysqlResultSet()
	ep := ses.GetExportConfig()
	switch st := execCtx.stmt.(type) {
	case *tree.Select:
		if ep.needExportToFile() {

			columns, err = execCtx.cw.GetColumns()
			if err != nil {
				logError(ses, ses.GetDebugString(),
					"Failed to get columns from computation handler",
					zap.Error(err))
				return
			}
			for _, c := range columns {
				mysqlc := c.(Column)
				mrs.AddColumn(mysqlc)
			}

			// open new file
			ep.DefaultBufSize = getGlobalPu().SV.ExportDataDefaultFlushSize
			initExportFileParam(ep, mrs)
			if err = openNewFile(requestCtx, ep, mrs); err != nil {
				return
			}

			runBegin := time.Now()
			/*
				Start pipeline
				Producing the data row and sending the data row
			*/
			// todo: add trace
			if _, err = execCtx.runner.Run(0); err != nil {
				return
			}

			// only log if run time is longer than 1s
			if time.Since(runBegin) > time.Second {
				logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Run : %s", time.Since(runBegin).String()))
			}

			oq := NewOutputQueue(ses.GetRequestContext(), ses, 0, nil, nil)
			if err = exportAllData(oq); err != nil {
				return
			}
			if err = ep.Writer.Flush(); err != nil {
				return
			}
			if err = ep.File.Close(); err != nil {
				return
			}

		} else {
			return moerr.NewInternalError(requestCtx, "select without it generates the result rows")
		}
	case *tree.CreateTable:
		runBegin := time.Now()
		if execCtx.runResult, err = execCtx.runner.Run(0); err != nil {
			return
		}
		// only log if run time is longer than 1s
		if time.Since(runBegin) > time.Second {
			logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Run : %s", time.Since(runBegin).String()))
		}

		// execute insert sql if this is a `create table as select` stmt
		if st.IsAsSelect {
			if txw, ok := execCtx.cw.(*TxnComputationWrapper); ok {
				insertSql := txw.plan.GetDdl().GetDefinition().(*plan.DataDefinition_CreateTable).CreateTable.CreateAsSelectSql
				ses.createAsSelectSql = insertSql
			}
			return
		}

		// Start the dynamic table daemon task
		if st.IsDynamicTable {
			if err = handleCreateDynamicTable(requestCtx, ses, st); err != nil {
				return
			}
		}
	default:
		//change privilege
		switch execCtx.stmt.(type) {
		case *tree.DropTable, *tree.DropDatabase, *tree.DropIndex, *tree.DropView, *tree.DropSequence,
			*tree.CreateUser, *tree.DropUser, *tree.AlterUser,
			*tree.CreateRole, *tree.DropRole,
			*tree.Revoke, *tree.Grant,
			*tree.SetDefaultRole, *tree.SetRole:
			ses.InvalidatePrivilegeCache()
		}
		runBegin := time.Now()
		/*
			Step 1: Start
		*/

		if st, ok := execCtx.stmt.(*tree.Load); ok {
			if st.Local {
				loadLocalErrGroup = new(errgroup.Group)
				loadLocalErrGroup.Go(func() error {
					return processLoadLocal(execCtx.proc.Ctx, ses, st.Param, execCtx.loadLocalWriter)
				})
			}
		}

		if execCtx.runResult, err = execCtx.runner.Run(0); err != nil {
			if loadLocalErrGroup != nil { // release resources
				err2 := execCtx.proc.LoadLocalReader.Close()
				if err2 != nil {
					logError(ses, ses.GetDebugString(),
						"processLoadLocal goroutine failed",
						zap.Error(err2))
				}
				err2 = loadLocalErrGroup.Wait() // executor failed, but processLoadLocal is still running, wait for it
				if err2 != nil {
					logError(ses, ses.GetDebugString(),
						"processLoadLocal goroutine failed",
						zap.Error(err2))
				}
			}
			return
		}

		if loadLocalErrGroup != nil {
			if err = loadLocalErrGroup.Wait(); err != nil { //executor success, but processLoadLocal goroutine failed
				return
			}
		}

		// only log if run time is longer than 1s
		if time.Since(runBegin) > time.Second {
			logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Run : %s", time.Since(runBegin).String()))
		}

		echoTime := time.Now()

		logDebug(ses, ses.GetDebugString(), fmt.Sprintf("time of SendResponse %s", time.Since(echoTime).String()))
	}

	return
}

func respStatus(requestCtx context.Context,
	ses *Session,
	execCtx *ExecCtx) (err error) {
	var rspLen uint64
	if execCtx.runResult != nil {
		rspLen = execCtx.runResult.AffectRows
	}

	switch st := execCtx.stmt.(type) {
	case *tree.Select:
		//select ... into ...
		if len(execCtx.proc.SessionInfo.SeqAddValues) != 0 {
			ses.AddSeqValues(execCtx.proc)
		}
		ses.SetSeqLastValue(execCtx.proc)

		resp := setResponse(ses, execCtx.isLastStmt, rspLen)
		if err2 := ses.GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
			err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
			logStatementStatus(requestCtx, ses, execCtx.stmt, fail, err)
			return err
		}
	case *tree.PrepareStmt, *tree.PrepareString:
		if ses.GetCmd() == COM_STMT_PREPARE {
			if err2 := ses.GetMysqlProtocol().SendPrepareResponse(requestCtx, execCtx.prepareStmt); err2 != nil {
				err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
				logStatementStatus(requestCtx, ses, execCtx.stmt, fail, err)
				return err
			}
		} else {
			resp := setResponse(ses, execCtx.isLastStmt, rspLen)
			if err2 := ses.GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
				err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
				logStatementStatus(requestCtx, ses, execCtx.stmt, fail, err)
				return err
			}
		}

	case *tree.Deallocate:
		//we will not send response in COM_STMT_CLOSE command
		if ses.GetCmd() != COM_STMT_CLOSE {
			resp := setResponse(ses, execCtx.isLastStmt, rspLen)
			if err2 := ses.GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
				err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
				logStatementStatus(requestCtx, ses, execCtx.stmt, fail, err)
				return err
			}
		}
	case *tree.CreateTable:
		// skip create table as select
		if st.IsAsSelect {
			return nil
		}
		resp := setResponse(ses, execCtx.isLastStmt, rspLen)
		if len(execCtx.proc.SessionInfo.SeqDeleteKeys) != 0 {
			ses.DeleteSeqValues(execCtx.proc)
		}
		_ = doGrantPrivilegeImplicitly(requestCtx, ses, st)
		if err2 := ses.GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
			err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
			logStatementStatus(requestCtx, ses, execCtx.stmt, fail, err)
			return err
		}
	default:
		resp := setResponse(ses, execCtx.isLastStmt, rspLen)

		if len(execCtx.proc.SessionInfo.SeqDeleteKeys) != 0 {
			ses.DeleteSeqValues(execCtx.proc)
		}

		switch st := execCtx.stmt.(type) {
		case *tree.Insert:
			resp.lastInsertId = execCtx.proc.GetLastInsertID()
			if execCtx.proc.GetLastInsertID() != 0 {
				ses.SetLastInsertID(execCtx.proc.GetLastInsertID())
			}
		case *tree.CreateTable:
			_ = doGrantPrivilegeImplicitly(requestCtx, ses, st)
		case *tree.DropTable:
			// handle dynamic table drop, cancel all the running daemon task
			_ = handleDropDynamicTable(requestCtx, ses, st)
			_ = doRevokePrivilegeImplicitly(requestCtx, ses, st)
		case *tree.CreateDatabase:
			_ = insertRecordToMoMysqlCompatibilityMode(requestCtx, ses, execCtx.stmt)
			_ = doGrantPrivilegeImplicitly(requestCtx, ses, st)
		case *tree.DropDatabase:
			_ = deleteRecordToMoMysqlCompatbilityMode(requestCtx, ses, execCtx.stmt)
			_ = doRevokePrivilegeImplicitly(requestCtx, ses, st)
			err = doDropFunctionWithDB(requestCtx, ses, execCtx.stmt, func(path string) error {
				return execCtx.proc.FileService.Delete(requestCtx, path)
			})
		}

		if err2 := ses.GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
			err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
			logStatementStatus(requestCtx, ses, execCtx.stmt, fail, err)
			return err
		}
	}
	return
}
