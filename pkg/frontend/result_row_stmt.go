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
	"bufio"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/explain"
)

// executeResultRowStmt run the statemet that responses result rows
func executeResultRowStmt(ses *Session, execCtx *ExecCtx) (err error) {
	var columns []interface{}
	var colDefs []*plan2.ColDef
	ses.EnterFPrint(FPResultRowStmt)
	defer ses.ExitFPrint(FPResultRowStmt)
	switch statement := execCtx.stmt.(type) {
	case *tree.Select:

		columns, err = execCtx.cw.GetColumns(execCtx.reqCtx)
		if err != nil {
			ses.Error(execCtx.reqCtx,
				"Failed to get columns from computation handler",
				zap.Error(err))
			return
		}

		ses.rs = &plan.ResultColDef{ResultCols: plan2.GetResultColumnsFromPlan(execCtx.cw.Plan())}

		ses.EnterFPrint(FPResultRowStmtSelect1)
		defer ses.ExitFPrint(FPResultRowStmtSelect1)
		err = execCtx.resper.RespPreMeta(execCtx, columns)
		if err != nil {
			return
		}

		ses.EnterFPrint(FPResultRowStmtSelect2)
		defer ses.ExitFPrint(FPResultRowStmtSelect2)
		runBegin := time.Now()
		/*
			Step 2: Start pipeline
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

	case *tree.ExplainAnalyze:
		queryPlan := execCtx.cw.Plan()
		txnHaveDDL := false
		ws := ses.proc.GetTxnOperator().GetWorkspace()
		if ws != nil {
			txnHaveDDL = ws.GetHaveDDL()
		}
		explainColName := plan2.GetPlanTitle(queryPlan.GetQuery(), txnHaveDDL)
		colDefs, columns, err = GetExplainColumns(execCtx.reqCtx, explainColName)
		if err != nil {
			ses.Error(execCtx.reqCtx,
				"Failed to get columns from ExplainColumns handler",
				zap.Error(err))
			return
		}

		ses.rs = &plan.ResultColDef{ResultCols: colDefs}

		ses.EnterFPrint(FPResultRowStmtExplainAnalyze1)
		defer ses.ExitFPrint(FPResultRowStmtExplainAnalyze1)
		err = execCtx.resper.RespPreMeta(execCtx, columns)
		if err != nil {
			return
		}

		ses.EnterFPrint(FPResultRowStmtExplainAnalyze2)
		defer ses.ExitFPrint(FPResultRowStmtExplainAnalyze2)
		runBegin := time.Now()
		/*
			Step 1: Start
		*/
		if _, err = execCtx.runner.Run(0); err != nil {
			return
		}

		// only log if run time is longer than 1s
		if time.Since(runBegin) > time.Second {
			ses.Infof(execCtx.reqCtx, "time of Exec.Run : %s", time.Since(runBegin).String())
		}
		//----------------------------------------------------------------------------------------------------------------------
	case *tree.ExplainPhyPlan:
		queryPlan := execCtx.cw.Plan()
		txnHaveDDL := false
		ws := ses.proc.GetTxnOperator().GetWorkspace()
		if ws != nil {
			txnHaveDDL = ws.GetHaveDDL()
		}
		explainColName := plan2.GetPhyPlanTitle(queryPlan.GetQuery(), txnHaveDDL)
		colDefs, columns, err = GetExplainColumns(execCtx.reqCtx, explainColName)
		if err != nil {
			ses.Error(execCtx.reqCtx,
				"Failed to get columns from ExplainColumns handler",
				zap.Error(err))
			return
		}

		ses.rs = &plan.ResultColDef{ResultCols: colDefs}

		ses.EnterFPrint(FPResultRowStmtExplainAnalyze1)
		defer ses.ExitFPrint(FPResultRowStmtExplainAnalyze1)
		err = execCtx.resper.RespPreMeta(execCtx, columns)
		if err != nil {
			return
		}

		ses.EnterFPrint(FPResultRowStmtExplainAnalyze2)
		defer ses.ExitFPrint(FPResultRowStmtExplainAnalyze2)
		runBegin := time.Now()
		/*
			Step 1: Start
		*/
		if _, err = execCtx.runner.Run(0); err != nil {
			return
		}

		// only log if run time is longer than 1s
		if time.Since(runBegin) > time.Second {
			ses.Infof(execCtx.reqCtx, "time of Exec.Run : %s", time.Since(runBegin).String())
		}
		//----------------------------------------------------------------------------------------------------------------------
	default:
		columns, err = execCtx.cw.GetColumns(execCtx.reqCtx)
		if err != nil {
			ses.Error(execCtx.reqCtx,
				"Failed to get columns from computation handler",
				zap.Error(err))
			return
		}

		ses.rs = &plan.ResultColDef{ResultCols: plan2.GetResultColumnsFromPlan(execCtx.cw.Plan())}

		ses.EnterFPrint(FPResultRowStmtDefault1)
		defer ses.ExitFPrint(FPResultRowStmtDefault1)
		err = execCtx.resper.RespPreMeta(execCtx, columns)
		if err != nil {
			return
		}

		ses.EnterFPrint(FPResultRowStmtDefault2)
		defer ses.ExitFPrint(FPResultRowStmtDefault2)
		runBegin := time.Now()
		/*
			Step 2: Start pipeline
			Producing the data row and sending the data row
		*/
		// todo: add trace
		if _, err = execCtx.runner.Run(0); err != nil {
			return
		}

		switch ses.GetShowStmtType() {
		case ShowTableStatus:
			if err = handleShowTableStatus(ses, execCtx, statement.(*tree.ShowTableStatus)); err != nil {
				return
			}
		}

		// only log if run time is longer than 1s
		if time.Since(runBegin) > time.Second {
			ses.Infof(execCtx.reqCtx, "time of Exec.Run : %s", time.Since(runBegin).String())
		}
	}
	return
}

func (resper *MysqlResp) respColumnDefsWithoutFlush(ses *Session, execCtx *ExecCtx, columns []any) (err error) {
	if execCtx.inMigration {
		return nil
	}
	//!!!carefully to use
	//execCtx.proto.DisableAutoFlush()
	//defer execCtx.proto.EnableAutoFlush()

	mrs := ses.GetMysqlResultSet()
	/*
		Step 1 : send column count and column definition.
	*/
	//send column count
	colCnt := uint64(len(columns))
	err = resper.mysqlRrWr.WriteLengthEncodedNumber(colCnt)
	if err != nil {
		return
	}

	if execCtx.prepareColDef != nil && len(columns) != len(execCtx.prepareColDef) {
		execCtx.prepareColDef = nil
	}

	//send columns
	//column_count * Protocol::ColumnDefinition packets
	cmd := ses.GetCmd()
	for i, c := range columns {
		mysqlc := c.(Column)
		mrs.AddColumn(mysqlc)
		/*
			mysql COM_QUERY response: send the column definition per column
		*/
		if execCtx.prepareColDef == nil {
			err = resper.mysqlRrWr.WriteColumnDef(execCtx.reqCtx, mysqlc, int(cmd))
			if err != nil {
				return
			}
		} else {
			err = resper.mysqlRrWr.WriteColumnDefBytes(execCtx.prepareColDef[i])
			if err != nil {
				return
			}
		}
	}
	/*
		mysql COM_QUERY response: End after the column has been sent.
		send EOF packet
	*/
	err = resper.mysqlRrWr.WriteEOFIFAndNoFlush(0, ses.GetTxnHandler().GetServerStatus())
	if err != nil {
		return
	}
	return
}

func (resper *MysqlResp) respStreamResultRow(ses *Session,
	execCtx *ExecCtx) (err error) {
	ses.EnterFPrint(FPRespStreamResultRow)
	defer ses.ExitFPrint(FPRespStreamResultRow)
	if execCtx.inMigration {
		return nil
	}

	switch statement := execCtx.stmt.(type) {
	case *tree.Select:
		if len(execCtx.proc.GetSessionInfo().SeqAddValues) != 0 {
			ses.AddSeqValues(execCtx.proc)
		}
		ses.SetSeqLastValue(execCtx.proc)
		err2 := resper.mysqlRrWr.WriteEOFOrOK(0, checkMoreResultSet(ses.getStatusAfterTxnIsEnded(execCtx.reqCtx), execCtx.isLastStmt))
		if err2 != nil {
			err = moerr.NewInternalErrorf(execCtx.reqCtx, "routine send response failed. error:%v ", err2)
			logStatementStatus(execCtx.reqCtx, ses, execCtx.stmt, fail, err)
			return
		}

	case *tree.ExplainAnalyze:
		queryPlan := execCtx.cw.Plan()
		txnHaveDDL := false
		ws := ses.proc.GetTxnOperator().GetWorkspace()
		if ws != nil {
			txnHaveDDL = ws.GetHaveDDL()
		}
		explainColName := plan2.GetPlanTitle(queryPlan.GetQuery(), txnHaveDDL)
		//if it is the plan from the EXECUTE,
		// replace the plan by the plan generated by the PREPARE
		if len(execCtx.cw.ParamVals()) != 0 {
			queryPlan, err = plan2.FillValuesOfParamsInPlan(execCtx.reqCtx, queryPlan, execCtx.cw.ParamVals())
			if err != nil {
				return
			}
		}
		// generator query explain
		explainQuery := explain.NewExplainQueryImpl(queryPlan.GetQuery())

		// build explain data buffer
		buffer := explain.NewExplainDataBuffer()
		var option *explain.ExplainOptions
		option, err = getExplainOption(execCtx.reqCtx, statement.Options)
		if err != nil {
			return
		}

		err = explainQuery.ExplainPlan(execCtx.reqCtx, buffer, option)
		if err != nil {
			return
		}

		err = buildMoExplainQuery(execCtx, explainColName, buffer, ses, getDataFromPipeline)
		if err != nil {
			return
		}

		err = resper.mysqlRrWr.WriteEOFOrOK(0, checkMoreResultSet(ses.getStatusAfterTxnIsEnded(execCtx.reqCtx), execCtx.isLastStmt))
		if err != nil {
			return
		}
		//--------------------------------------------------------------------------------------------------------------
	case *tree.ExplainPhyPlan:
		queryPlan := execCtx.cw.Plan()
		txnHaveDDL := false
		ws := ses.proc.GetTxnOperator().GetWorkspace()
		if ws != nil {
			txnHaveDDL = ws.GetHaveDDL()
		}
		explainColName := plan2.GetPlanTitle(queryPlan.GetQuery(), txnHaveDDL)

		txnCompileWrapper := execCtx.cw.(*TxnComputationWrapper)
		reader := bufio.NewReader(txnCompileWrapper.explainBuffer)
		err = buildMoExplainPhyPlan(execCtx, explainColName, reader, ses, getDataFromPipeline)
		if err != nil {
			return
		}

		err = resper.mysqlRrWr.WriteEOFOrOK(0, checkMoreResultSet(ses.getStatusAfterTxnIsEnded(execCtx.reqCtx), execCtx.isLastStmt))
		if err != nil {
			return
		}

		//--------------------------------------------------------------------------------------------------------------
	default:
		err = resper.mysqlRrWr.WriteEOFOrOK(0, checkMoreResultSet(ses.getStatusAfterTxnIsEnded(execCtx.reqCtx), execCtx.isLastStmt))
		if err != nil {
			return
		}
	}

	return
}

func (resper *MysqlResp) respPrebuildResultRow(ses *Session,
	execCtx *ExecCtx) (err error) {
	ses.EnterFPrint(FPrespPrebuildResultRow)
	defer ses.ExitFPrint(FPrespPrebuildResultRow)
	if execCtx.inMigration {
		return nil
	}
	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	res := ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, execCtx.isLastStmt)
	if err := resper.mysqlRrWr.WriteResponse(execCtx.reqCtx, res); err != nil {
		return moerr.NewInternalErrorf(execCtx.reqCtx, "routine send response failed, error: %v ", err)
	}
	return err
}

func (resper *MysqlResp) respMixedResultRow(ses *Session,
	execCtx *ExecCtx) (err error) {
	ses.EnterFPrint(FPrespMixedResultRow)
	defer ses.ExitFPrint(FPrespMixedResultRow)
	if execCtx.inMigration {
		return nil
	}
	//!!!the columnDef has been sent after the compiling ends. It should not be sent here again.
	//only the result rows need to be sent.
	mrs := ses.GetMysqlResultSet()
	if err := ses.GetResponser().MysqlRrWr().WriteResultSetRow(mrs, mrs.GetRowCount()); err != nil {
		ses.Error(execCtx.reqCtx,
			"Failed to handle 'SHOW TABLE STATUS'",
			zap.Error(err))
		return err
	}

	err = resper.mysqlRrWr.WriteEOFOrOK(0, checkMoreResultSet(ses.getStatusAfterTxnIsEnded(execCtx.reqCtx), execCtx.isLastStmt))
	if err != nil {
		return
	}

	return err
}

func (resper *MysqlResp) respBySituation(ses *Session,
	execCtx *ExecCtx) (err error) {
	defer func() {
		execCtx.results = nil
	}()
	resp := NewGeneralOkResponse(COM_QUERY, ses.GetTxnHandler().GetServerStatus())
	if len(execCtx.results) == 0 {
		if err = resper.mysqlRrWr.WriteResponse(execCtx.reqCtx, resp); err != nil {
			return moerr.NewInternalErrorf(execCtx.reqCtx, "routine send response failed. error:%v ", err)
		}
	} else {
		for i, result := range execCtx.results {
			mer := NewMysqlExecutionResult(0, 0, 0, 0, result.(*MysqlResultSet))
			resp = ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, i == len(execCtx.results)-1)
			if err = resper.mysqlRrWr.WriteResponse(execCtx.reqCtx, resp); err != nil {
				return moerr.NewInternalErrorf(execCtx.reqCtx, "routine send response failed. error:%v ", err)
			}
		}
	}
	return
}
