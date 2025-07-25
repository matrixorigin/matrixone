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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// executeStatusStmt run the statement that responses status t
func executeStatusStmt(ses *Session, execCtx *ExecCtx) (err error) {
	var loadLocalErrGroup *errgroup.Group
	var columns []interface{}

	mrs := ses.GetMysqlResultSet()
	ep := ses.GetExportConfig()
	switch st := execCtx.stmt.(type) {
	case *tree.Select:
		if ep.needExportToFile() {
			defer ep.Close()
			columns, err = execCtx.cw.GetColumns(execCtx.reqCtx)
			if err != nil {
				ses.Error(execCtx.reqCtx,
					"Failed to get columns from computation handler",
					zap.Error(err))
				return
			}
			for _, c := range columns {
				mysqlc := c.(Column)
				mrs.AddColumn(mysqlc)
			}

			// open new file
			ep.DefaultBufSize = getPu(ses.GetService()).SV.ExportDataDefaultFlushSize
			initExportFileParam(ep, mrs)
			if err = openNewFile(execCtx.reqCtx, ep, mrs); err != nil {
				return
			}

			ep.init()
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
				ses.Infof(execCtx.reqCtx, "time of Exec.Run : %s", time.Since(runBegin).String())
			}

			if err = exportAllDataFromBatches(ep); err != nil {
				return
			}

			if err = Close(ep); err != nil {
				return
			}

		} else {
			return moerr.NewInternalError(execCtx.reqCtx, "select without it generates the result rows")
		}
	case *tree.CreateTable:
		runBegin := time.Now()
		if execCtx.runResult, err = execCtx.runner.Run(0); err != nil {
			return
		}
		// only log if run time is longer than 1s
		if time.Since(runBegin) > time.Second {
			ses.Infof(execCtx.reqCtx, "time of Exec.Run : %s", time.Since(runBegin).String())
		}

		// execute insert sql if this is a `create table as select` stmt
		if st.IsAsSelect {
			insertSql := execCtx.cw.Plan().GetDdl().GetDefinition().(*plan.DataDefinition_CreateTable).CreateTable.CreateAsSelectSql
			ses.createAsSelectSql = insertSql
			return
		}

		// Start the dynamic table daemon task
		if st.IsDynamicTable {
			if err = handleCreateDynamicTable(execCtx.reqCtx, ses, st); err != nil {
				return
			}
		}

		// grant privilege implicitly
		// must execute after run to get table id
		err = doGrantPrivilegeImplicitly(execCtx.reqCtx, ses, st)
		if err != nil {
			return
		}

	default:
		//change privilege
		switch execCtx.stmt.(type) {
		case *tree.DropTable, *tree.DropDatabase:
			ses.InvalidatePrivilegeCache()
			// must execute before run to get database id or table id
			doRevokePrivilegeImplicitly(execCtx.reqCtx, ses, st)

		case *tree.DropIndex, *tree.DropView, *tree.DropSequence,
			*tree.CreateUser, *tree.DropUser, *tree.AlterUser,
			*tree.CreateRole, *tree.DropRole,
			*tree.Revoke, *tree.Grant,
			*tree.SetDefaultRole, *tree.SetRole:
			ses.InvalidatePrivilegeCache()
		}
		runBegin := time.Now()
		if st, ok := execCtx.stmt.(*tree.Load); ok {
			if st.Local {
				loadLocalErrGroup = new(errgroup.Group)
				loadLocalErrGroup.Go(func() error {
					return processLoadLocal(ses, execCtx, st.Param, execCtx.loadLocalWriter, execCtx.proc.GetLoadLocalReader())
				})
			}
		}

		if execCtx.runResult, err = execCtx.runner.Run(0); err != nil {
			if loadLocalErrGroup != nil { // release resources
				err2 := execCtx.proc.Base.LoadLocalReader.Close()
				if err2 != nil {
					ses.Error(execCtx.reqCtx,
						"processLoadLocal goroutine failed",
						zap.Error(err2))
				}
				err2 = loadLocalErrGroup.Wait() // executor failed, but processLoadLocal is still running, wait for it
				if err2 != nil {
					ses.Error(execCtx.reqCtx,
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
			ses.Infof(execCtx.reqCtx, "time of Exec.Run : %s", time.Since(runBegin).String())
		}
		switch execCtx.stmt.(type) {
		case *tree.CreateDatabase:
			// must execute after run to get database id
			err = doGrantPrivilegeImplicitly(execCtx.reqCtx, ses, st)
			if err != nil {
				return
			}
		}
	}

	return
}

func (resper *MysqlResp) respStatus(ses *Session,
	execCtx *ExecCtx) (err error) {
	ses.EnterFPrint(FPRespStatus)
	defer ses.ExitFPrint(FPRespStatus)
	if execCtx.inMigration {
		return nil
	}
	var rspLen uint64
	if execCtx.runResult != nil {
		rspLen = execCtx.runResult.AffectRows
	}

	switch st := execCtx.stmt.(type) {
	case *tree.Select:
		//select ... into ...
		if len(execCtx.proc.GetSessionInfo().SeqAddValues) != 0 {
			ses.AddSeqValues(execCtx.proc)
		}
		ses.SetSeqLastValue(execCtx.proc)

		res := setResponse(ses, execCtx.isLastStmt, rspLen)
		if err2 := resper.mysqlRrWr.WriteResponse(execCtx.reqCtx, res); err2 != nil {
			err = moerr.NewInternalErrorf(execCtx.reqCtx, "routine send response failed. error:%v ", err2)
			logStatementStatus(execCtx.reqCtx, ses, execCtx.stmt, fail, err)
			return err
		}
	case *tree.PrepareStmt, *tree.PrepareString:
		if ses.GetCmd() == COM_STMT_PREPARE {
			if err2 := resper.mysqlRrWr.WritePrepareResponse(execCtx.reqCtx, execCtx.prepareStmt); err2 != nil {
				err = moerr.NewInternalErrorf(execCtx.reqCtx, "routine send response failed. error:%v ", err2)
				logStatementStatus(execCtx.reqCtx, ses, execCtx.stmt, fail, err)
				return err
			}
		} else {
			res := setResponse(ses, execCtx.isLastStmt, rspLen)
			if err2 := resper.mysqlRrWr.WriteResponse(execCtx.reqCtx, res); err2 != nil {
				err = moerr.NewInternalErrorf(execCtx.reqCtx, "routine send response failed. error:%v ", err2)
				logStatementStatus(execCtx.reqCtx, ses, execCtx.stmt, fail, err)
				return err
			}
		}

	case *tree.Deallocate:
		//we will not send response in COM_STMT_CLOSE command
		if ses.GetCmd() != COM_STMT_CLOSE {
			res := setResponse(ses, execCtx.isLastStmt, rspLen)
			if err2 := resper.mysqlRrWr.WriteResponse(execCtx.reqCtx, res); err2 != nil {
				err = moerr.NewInternalErrorf(execCtx.reqCtx, "routine send response failed. error:%v ", err2)
				logStatementStatus(execCtx.reqCtx, ses, execCtx.stmt, fail, err)
				return err
			}
		}
	case *tree.CreateTable:
		// skip create table as select
		if st.IsAsSelect {
			return nil
		}
		res := setResponse(ses, execCtx.isLastStmt, rspLen)
		if len(execCtx.proc.GetSessionInfo().SeqDeleteKeys) != 0 {
			ses.DeleteSeqValues(execCtx.proc)
		}
		if err2 := resper.mysqlRrWr.WriteResponse(execCtx.reqCtx, res); err2 != nil {
			err = moerr.NewInternalErrorf(execCtx.reqCtx, "routine send response failed. error:%v ", err2)
			logStatementStatus(execCtx.reqCtx, ses, execCtx.stmt, fail, err)
			return err
		}
	case *InternalCmdFieldList:
		if err2 := resper.mysqlRrWr.WriteEOFOrOK(0, ses.GetTxnHandler().GetServerStatus()); err2 != nil {
			err = moerr.NewInternalErrorf(execCtx.reqCtx, "routine send response failed. error:%v ", err2)
			logStatementStatus(execCtx.reqCtx, ses, execCtx.stmt, fail, err)
			return err
		}
	default:
		res := setResponse(ses, execCtx.isLastStmt, rspLen)

		if len(execCtx.proc.GetSessionInfo().SeqDeleteKeys) != 0 {
			ses.DeleteSeqValues(execCtx.proc)
		}

		isIssue3482 := false
		localFileName := ""
		switch st := execCtx.stmt.(type) {
		case *tree.Insert:
			res.lastInsertId = execCtx.proc.GetLastInsertID()
			if execCtx.proc.GetLastInsertID() != 0 {
				ses.SetLastInsertID(execCtx.proc.GetLastInsertID())
			}
		case *tree.DropTable:
			// handle dynamic table drop, cancel all the running daemon task
			_ = handleDropDynamicTable(execCtx.reqCtx, ses, st)
		case *tree.CreateDatabase:
			_ = insertRecordToMoMysqlCompatibilityMode(execCtx.reqCtx, ses, execCtx.stmt)
		case *tree.DropDatabase:
			_ = deleteRecordToMoMysqlCompatbilityMode(execCtx.reqCtx, ses, execCtx.stmt)
			err = doDropFunctionWithDB(execCtx.reqCtx, ses, execCtx.stmt, func(path string) error {
				return execCtx.proc.Base.FileService.Delete(execCtx.reqCtx, path)
			})
			if err != nil {
				return err
			}
			err = doDropProcedureWithDB(execCtx.reqCtx, ses, execCtx.stmt)

		case *tree.Load:
			if st.Local && execCtx.isIssue3482 {
				isIssue3482 = true
				localFileName = st.Param.Filepath
			}
		}

		if err2 := resper.mysqlRrWr.WriteResponse(execCtx.reqCtx, res); err2 != nil {
			if isIssue3482 {
				err = moerr.NewInternalErrorf(execCtx.reqCtx, "routine send response failed. local local '%s' response error:%v ", localFileName, err2)
			} else {
				err = moerr.NewInternalErrorf(execCtx.reqCtx, "routine send response failed. error:%v ", err2)
			}

			logStatementStatus(execCtx.reqCtx, ses, execCtx.stmt, fail, err)
			return err
		}

		if isIssue3482 {
			ses.Infof(execCtx.reqCtx, "local local '%s' response ok", localFileName)
		}
	}
	return
}
