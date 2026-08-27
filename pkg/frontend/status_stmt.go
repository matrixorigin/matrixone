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

	"github.com/matrixorigin/matrixone/pkg/catalog"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func isPerformStatement(stmt tree.Statement) bool {
	selectStmt, ok := stmt.(*tree.Select)
	return ok && selectStmt.IsPerform
}

// executeStatusStmt run the statement that responses status t
func executeStatusStmt(ses *Session, execCtx *ExecCtx) (err error) {
	var loadLocalErrGroup *errgroup.Group
	var columns []interface{}
	execCtx.persistentDropTableTargets = nil

	mrs := ses.GetMysqlResultSet()
	ep := ses.GetExportConfig()
	switch st := execCtx.stmt.(type) {
	case *tree.Select:
		if st.IsPerform && len(st.IntoVars) > 0 {
			return moerr.NewSyntaxError(execCtx.reqCtx, tree.PerformIntoClauseMessage)
		}
		if st.IsPerform {
			queryResultFinalized := false
			defer func() {
				if !queryResultFinalized {
					resetQueryResultState(ses)
				}
			}()
			ses.rs = &plan.ResultColDef{
				ResultCols: plan2.GetResultColumnsFromPlan(execCtx.cw.Plan()),
			}
			freezeResultMetadata(execCtx.runner)
			runBegin := time.Now()
			if execCtx.runResult, err = execCtx.runner.Run(0); err != nil {
				return
			}
			if err = finalizePerformQueryResult(execCtx); err != nil {
				return
			}
			queryResultFinalized = true
			if execCtx.runResult != nil {
				execCtx.runResult.AffectRows = 0
			}
			if time.Since(runBegin) > time.Second {
				ses.Infof(execCtx.reqCtx, "time of Exec.Run : %s", time.Since(runBegin).String())
			}
			return
		}
		if len(st.IntoVars) > 0 {
			if err = validateSelectIntoArity(execCtx.reqCtx, execCtx.cw.Plan(), len(st.IntoVars)); err != nil {
				return
			}
			runBegin := time.Now()
			if execCtx.runResult, err = execCtx.runner.Run(0); err != nil {
				return
			}
			if execCtx.selectInto == nil {
				return moerr.NewInternalError(execCtx.reqCtx, "SELECT INTO user-variable collector is not initialized")
			}
			if err = execCtx.selectInto.apply(execCtx.reqCtx, ses, execCtx.sqlOfStmt); err != nil {
				return
			}
			appendSelectIntoDeprecatedWarning(ses, st.DeprecatedInto)
			if time.Since(runBegin) > time.Second {
				ses.Infof(execCtx.reqCtx, "time of Exec.Run : %s", time.Since(runBegin).String())
			}
			return
		}
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
			freezeResultMetadata(execCtx.runner)

			// open new file
			ep.DefaultBufSize = getPu(ses.GetService()).SV.ExportDataDefaultFlushSize
			initExportFileParam(ep, mrs)
			ep.mrs = mrs
			ep.ctx = execCtx.reqCtx
			ep.service = ses.GetService()
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
			// Keep runResult so ROW_COUNT()/the OK packet report this statement's
			// affected rows instead of leaking the previous statement's value.
			if execCtx.runResult, err = execCtx.runner.Run(0); err != nil {
				return
			}

			// only log if run time is longer than 1s
			if time.Since(runBegin) > time.Second {
				ses.Infof(execCtx.reqCtx, "time of Exec.Run : %s", time.Since(runBegin).String())
			}

			if err = exportAllDataFromBatches(ep); err != nil {
				return
			}

			// For parquet format, file is written in exportAllDataFromBatches
			// No need to close pipe-based writer
			if ep.getExportFormat() != "parquet" {
				if err = Close(ep); err != nil {
					return
				}
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

		// grant privilege implicitly
		// must execute after run to get table id
		err = doGrantPrivilegeImplicitly(execCtx.reqCtx, ses, st)
		if err != nil {
			return
		}

	default:
		//change privilege
		switch st := execCtx.stmt.(type) {
		case *tree.DropTable:
			execCtx.persistentDropTableTargets = capturePersistentDropTableTargets(ses, st)
			ses.InvalidatePrivilegeCache()
			// must execute before run to get database id or table id
			if err = doRevokePrivilegeImplicitly(execCtx.reqCtx, ses, st, execCtx.persistentDropTableTargets); err != nil {
				return
			}

		case *tree.DropDatabase:
			ses.InvalidatePrivilegeCache()
			// must execute before run to get database id or table id
			if err = doRevokePrivilegeImplicitly(execCtx.reqCtx, ses, st, nil); err != nil {
				return
			}

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

// capturePersistentDropTableTargets classifies every DROP TABLE target while
// the session's temporary aliases still exist. Both the pre-execution
// ownership revoke and the post-execution dynamic-table cleanup must consume
// this same snapshot: dropTableSingle removes temporary aliases as it runs.
func capturePersistentDropTableTargets(ses *Session, st *tree.DropTable) tree.TableNames {
	if st == nil || st.Temporary {
		return nil
	}

	targets := make(tree.TableNames, 0, len(st.Names))
	for _, name := range st.Names {
		if name == nil {
			continue
		}
		dbName := string(name.SchemaName)
		if dbName == "" {
			dbName = ses.GetDatabaseName()
		}
		if _, isTemporary := ses.GetTempTable(dbName, string(name.ObjectName)); isTemporary {
			continue
		}
		targets = append(targets, name)
	}
	return targets
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

	switch execCtx.stmt.(type) {
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
		if len(execCtx.proc.GetSessionInfo().SeqAddValues) != 0 {
			ses.AddSeqValues(execCtx.proc)
		}
		ses.SetSeqLastValue(execCtx.proc)

		isIssue3482 := false
		localFileName := ""
		switch st := execCtx.stmt.(type) {
		case *tree.Insert:
			res.lastInsertId = execCtx.proc.GetStatementLastInsertID()
			if res.lastInsertId != 0 {
				ses.SetLastInsertID(res.lastInsertId)
			}
		case *tree.MultiInsert:
			// A multi-table INSERT has one PRE_INSERT per target, each publishing
			// its generated key through the same statement-wide coordinator, which
			// keeps the numerically smallest non-zero value. That rule is correct
			// for the parallel scopes of ONE table, where the smallest really is
			// the first generated; across targets the counters are unrelated, so
			// the smallest identifies neither the first target nor the first
			// generated row and would change meaning with the targets' counters
			// rather than with the statement. Report an insert id only when a
			// single target can generate one; otherwise the statement is
			// ambiguous and reports none.
			if multiInsertHasUniqueAutoIncrTarget(execCtx) {
				res.lastInsertId = execCtx.proc.GetStatementLastInsertID()
				if res.lastInsertId != 0 {
					ses.SetLastInsertID(res.lastInsertId)
				}
			} else {
				// Declining to report the ambiguous value is not enough. The
				// targets' PRE_INSERTs published through
				// SetStatementLastInsertIDIfEarlier, which writes the
				// session-visible LastInsertID as well as the statement one,
				// and doComQuery reuses this process for the next statement of
				// the same COM_QUERY while resetting only the statement value.
				// Left alone, the suppressed cross-table minimum would answer
				// that statement's LAST_INSERT_ID(). Put the session's value
				// back, which is what this statement left visible.
				execCtx.proc.SetLastInsertID(ses.GetLastInsertID())
			}
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

// multiInsertHasUniqueAutoIncrTarget reports whether exactly one target of a
// multi-table INSERT can generate AUTO_INCREMENT values. Only then does
// LAST_INSERT_ID() have the single-insert meaning: the first value generated by
// that target.
func multiInsertHasUniqueAutoIncrTarget(execCtx *ExecCtx) bool {
	if execCtx == nil || execCtx.cw == nil {
		return false
	}
	p := execCtx.cw.Plan()
	if p == nil || p.GetQuery() == nil {
		return false
	}
	targets := 0
	for _, node := range p.GetQuery().Nodes {
		if node.GetNodeType() != plan.Node_PRE_INSERT || node.PreInsertCtx == nil {
			continue
		}
		def := node.PreInsertCtx.TableDef
		if def == nil {
			continue
		}
		for _, col := range def.Cols {
			// The fake primary key a PK-less table carries is itself
			// auto-increment, but it is hidden and never surfaces through
			// LAST_INSERT_ID(); counting it would make every PK-less target look
			// like an auto-increment one.
			if col == nil || !col.Typ.AutoIncr || col.Hidden ||
				col.Name == catalog.FakePrimaryKeyColName {
				continue
			}
			targets++
			break
		}
	}
	return targets == 1
}
