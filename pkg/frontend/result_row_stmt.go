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

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/explain"
)

// executeResultRowStmt run the statemet that responses result rows
func executeResultRowStmt(requestCtx context.Context, ses *Session, execCtx *ExecCtx) (err error) {
	var columns []interface{}

	mrs := ses.GetMysqlResultSet()

	switch statement := execCtx.stmt.(type) {
	case *tree.Select:

		columns, err = execCtx.cw.GetColumns()
		if err != nil {
			logError(ses, ses.GetDebugString(),
				"Failed to get columns from computation handler",
				zap.Error(err))
			return
		}
		if c, ok := execCtx.cw.(*TxnComputationWrapper); ok {
			ses.rs = &plan.ResultColDef{ResultCols: plan2.GetResultColumnsFromPlan(c.plan)}
		}
		/*
			Step 1 : send column count and column definition.
		*/
		//send column count
		colCnt := uint64(len(columns))
		err = execCtx.proto.SendColumnCountPacket(colCnt)
		if err != nil {
			return
		}
		//send columns
		//column_count * Protocol::ColumnDefinition packets
		cmd := ses.GetCmd()
		for _, c := range columns {
			mysqlc := c.(Column)
			mrs.AddColumn(mysqlc)
			/*
				mysql COM_QUERY response: send the column definition per column
			*/
			err = execCtx.proto.SendColumnDefinitionPacket(requestCtx, mysqlc, int(cmd))
			if err != nil {
				return
			}
		}

		/*
			mysql COM_QUERY response: End after the column has been sent.
			send EOF packet
		*/
		err = execCtx.proto.SendEOFPacketIf(0, ses.GetTxnHandler().GetServerStatus())
		if err != nil {
			return
		}

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
			logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Run : %s", time.Since(runBegin).String()))
		}

	case *tree.ExplainAnalyze:
		explainColName := "QUERY PLAN"
		columns, err = GetExplainColumns(requestCtx, explainColName)
		if err != nil {
			logError(ses, ses.GetDebugString(),
				"Failed to get columns from ExplainColumns handler",
				zap.Error(err))
			return
		}
		/*
			Step 1 : send column count and column definition.
		*/
		//send column count
		colCnt := uint64(len(columns))
		err = execCtx.proto.SendColumnCountPacket(colCnt)
		if err != nil {
			return
		}
		//send columns
		//column_count * Protocol::ColumnDefinition packets
		cmd := ses.GetCmd()
		for _, c := range columns {
			mysqlc := c.(Column)
			mrs.AddColumn(mysqlc)
			/*
				mysql COM_QUERY response: send the column definition per column
			*/
			err = execCtx.proto.SendColumnDefinitionPacket(requestCtx, mysqlc, int(cmd))
			if err != nil {
				return
			}
		}
		/*
			mysql COM_QUERY response: End after the column has been sent.
			send EOF packet
		*/
		err = execCtx.proto.SendEOFPacketIf(0, ses.GetTxnHandler().GetServerStatus())
		if err != nil {
			return
		}

		runBegin := time.Now()
		/*
			Step 1: Start
		*/
		if _, err = execCtx.runner.Run(0); err != nil {
			return
		}

		// only log if run time is longer than 1s
		if time.Since(runBegin) > time.Second {
			logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Run : %s", time.Since(runBegin).String()))
		}

	default:
		columns, err = execCtx.cw.GetColumns()
		if err != nil {
			logError(ses, ses.GetDebugString(),
				"Failed to get columns from computation handler",
				zap.Error(err))
			return
		}
		if c, ok := execCtx.cw.(*TxnComputationWrapper); ok {
			ses.rs = &plan.ResultColDef{ResultCols: plan2.GetResultColumnsFromPlan(c.plan)}
		}
		/*
			Step 1 : send column count and column definition.
		*/
		//send column count
		colCnt := uint64(len(columns))
		err = execCtx.proto.SendColumnCountPacket(colCnt)
		if err != nil {
			return
		}
		//send columns
		//column_count * Protocol::ColumnDefinition packets
		cmd := ses.GetCmd()
		for _, c := range columns {
			mysqlc := c.(Column)
			mrs.AddColumn(mysqlc)
			/*
				mysql COM_QUERY response: send the column definition per column
			*/
			err = execCtx.proto.SendColumnDefinitionPacket(requestCtx, mysqlc, int(cmd))
			if err != nil {
				return
			}
		}

		/*
			mysql COM_QUERY response: End after the column has been sent.
			send EOF packet
		*/
		err = execCtx.proto.SendEOFPacketIf(0, ses.GetTxnHandler().GetServerStatus())
		if err != nil {
			return
		}

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
			if err = handleShowTableStatus(ses, statement.(*tree.ShowTableStatus), execCtx.proc); err != nil {
				return
			}
		}

		// only log if run time is longer than 1s
		if time.Since(runBegin) > time.Second {
			logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Run : %s", time.Since(runBegin).String()))
		}
	}
	return
}

func respStreamResultRow(requestCtx context.Context,
	ses *Session,
	execCtx *ExecCtx) (err error) {
	switch statement := execCtx.stmt.(type) {
	case *tree.Select:
		if len(execCtx.proc.SessionInfo.SeqAddValues) != 0 {
			ses.AddSeqValues(execCtx.proc)
		}
		ses.SetSeqLastValue(execCtx.proc)

		err2 := execCtx.proto.sendEOFOrOkPacket(0, ses.GetTxnHandler().GetServerStatus())
		if err2 != nil {
			err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
			logStatementStatus(requestCtx, ses, execCtx.stmt, fail, err)
			return
		}

	case *tree.ExplainAnalyze:
		explainColName := "QUERY PLAN"
		if cwft, ok := execCtx.cw.(*TxnComputationWrapper); ok {
			queryPlan := cwft.plan
			//if it is the plan from the EXECUTE,
			// replace the plan by the plan generated by the PREPARE
			if len(cwft.paramVals) != 0 {
				queryPlan, err = plan2.FillValuesOfParamsInPlan(requestCtx, queryPlan, cwft.paramVals)
				if err != nil {
					return
				}
			}
			// generator query explain
			explainQuery := explain.NewExplainQueryImpl(queryPlan.GetQuery())

			// build explain data buffer
			buffer := explain.NewExplainDataBuffer()
			var option *explain.ExplainOptions
			option, err = getExplainOption(requestCtx, statement.Options)
			if err != nil {
				return
			}

			err = explainQuery.ExplainPlan(requestCtx, buffer, option)
			if err != nil {
				return
			}

			err = buildMoExplainQuery(explainColName, buffer, ses, getDataFromPipeline)
			if err != nil {
				return
			}

			err = execCtx.proto.sendEOFOrOkPacket(0, ses.GetTxnHandler().GetServerStatus())
			if err != nil {
				return
			}
		}
	default:
		err = execCtx.proto.sendEOFOrOkPacket(0, ses.GetTxnHandler().GetServerStatus())
		if err != nil {
			return
		}
	}

	return
}

func respPrebuildResultRow(requestCtx context.Context,
	ses *Session,
	execCtx *ExecCtx) (err error) {
	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, execCtx.isLastStmt)
	if err := execCtx.proto.SendResponse(ses.GetRequestContext(), resp); err != nil {
		return moerr.NewInternalError(ses.GetRequestContext(), "routine send response failed, error: %v ", err)
	}
	return err
}

func respMixedResultRow(requestCtx context.Context,
	ses *Session,
	execCtx *ExecCtx) (err error) {
	mrs := ses.GetMysqlResultSet()
	if err := ses.GetMysqlProtocol().SendResultSetTextBatchRowSpeedup(mrs, mrs.GetRowCount()); err != nil {
		logError(ses, ses.GetDebugString(),
			"Failed to handle 'SHOW TABLE STATUS'",
			zap.Error(err))
		return err
	}
	err = execCtx.proto.sendEOFOrOkPacket(0, ses.GetTxnHandler().GetServerStatus())
	if err != nil {
		return
	}

	return err
}
