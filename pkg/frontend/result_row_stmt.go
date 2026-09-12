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
	"context"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/explain"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
)

func GetExplainColumn(ctx context.Context, explainColName string) ([]*plan2.ColDef, []interface{}, error) {
	cols := []*plan2.ColDef{
		{
			Typ:        plan2.Type{Id: int32(types.T_varchar)},
			Name:       strings.ToLower(explainColName),
			OriginName: explainColName,
		},
	}
	columns := make([]interface{}, len(cols))
	var err error = nil
	for i, col := range cols {
		c, err := colDef2MysqlColumn(ctx, col)
		if err != nil {
			return nil, nil, err
		}
		columns[i] = c
	}
	return cols, columns, err
}

func getPreparedResultColumns(stmt *PrepareStmt, txnHaveDDL bool) []*plan2.ColDef {
	return getPreparedResultColumnsFromPlanWithGroupConcatMaxLen(
		stmt.PrepareStmt, stmt.PreparePlan, txnHaveDDL, stmt.groupConcatMaxLenFloor)
}

func getPreparedResultColumnsFromPlanWithGroupConcatMaxLen(
	stmt tree.Statement, preparedPlan *plan2.Plan, txnHaveDDL bool, groupConcatMaxLenFloor uint64,
) []*plan2.ColDef {
	plan := preparedPlan.GetDcl().GetPrepare().GetPlan()
	return getPreparedResultColumnsForWithGroupConcatMaxLen(
		stmt, plan, txnHaveDDL, groupConcatMaxLenFloor)
}

func getPreparedResultColumnsFor(stmt tree.Statement, plan *plan.Plan, txnHaveDDL bool) []*plan2.ColDef {
	return getPreparedResultColumnsForWithGroupConcatMaxLen(stmt, plan, txnHaveDDL, 0)
}

func getPreparedResultColumnsForWithGroupConcatMaxLen(
	stmt tree.Statement, preparedPlan *plan.Plan, txnHaveDDL bool, groupConcatMaxLenFloor uint64,
) []*plan2.ColDef {
	if isPerformStatement(stmt) {
		return nil
	}
	if query := preparedPlan.GetQuery(); query != nil {
		var title string
		switch stmt.(type) {
		case *tree.ExplainStmt, *tree.ExplainAnalyze:
			title = plan2.GetPlanTitle(query, txnHaveDDL)
		case *tree.ExplainPhyPlan:
			title = plan2.GetPhyPlanTitle(query, txnHaveDDL)
		}
		if title != "" {
			return []*plan2.ColDef{{
				Typ:        plan2.Type{Id: int32(types.T_varchar)},
				Name:       title,
				OriginName: title,
			}}
		}
	}
	columns := plan2.GetResultColumnsFromPlan(preparedPlan)
	overlayPreparedGroupConcatResultMetadata(
		preparedPlan.GetQuery(), columns, groupConcatMaxLenFloor)
	return columns
}

const preparedGroupConcatVarcharMaxLen = 512

type preparedGroupConcatResultColumnRef struct {
	nodeID int32
	colPos int32
}

func preparedPlanContainsGroupConcat(preparedPlan *plan.Plan) bool {
	if preparedPlan == nil || preparedPlan.GetQuery() == nil {
		return false
	}
	for _, node := range preparedPlan.GetQuery().Nodes {
		if node == nil {
			continue
		}
		for _, expr := range node.AggList {
			if preparedExprContainsGroupConcat(expr) {
				return true
			}
		}
		for _, expr := range node.WinSpecList {
			if preparedExprContainsGroupConcat(expr) {
				return true
			}
		}
	}
	return false
}

func preparedExprContainsGroupConcat(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if fn.GetFunc() != nil && fn.GetFunc().GetObjName() == plan2.NameGroupConcat {
			return true
		}
		for _, arg := range fn.Args {
			if preparedExprContainsGroupConcat(arg) {
				return true
			}
		}
	}
	if window := expr.GetW(); window != nil {
		if preparedExprContainsGroupConcat(window.WindowFunc) {
			return true
		}
		for _, arg := range window.PartitionBy {
			if preparedExprContainsGroupConcat(arg) {
				return true
			}
		}
		for _, order := range window.OrderBy {
			if order != nil && preparedExprContainsGroupConcat(order.Expr) {
				return true
			}
		}
	}
	return false
}

// overlayPreparedGroupConcatResultMetadata applies the MySQL result-type rule
// that depends on group_concat_max_len to prepared result metadata only. The
// execution plan keeps the aggregate's engine type (T_text/T_blob); changing it
// here would change vector allocation and aggregate execution semantics.
func overlayPreparedGroupConcatResultMetadata(
	query *plan.Query, columns []*plan2.ColDef, groupConcatMaxLen uint64,
) {
	if query == nil || groupConcatMaxLen == 0 || len(query.Steps) == 0 {
		return
	}
	step := len(query.Steps) - 1
	if query.HasReturning {
		if query.ReturningStep < 0 || int(query.ReturningStep) >= len(query.Steps) {
			return
		}
		step = int(query.ReturningStep)
	}
	rootID := query.Steps[step]
	if rootID < 0 || int(rootID) >= len(query.Nodes) {
		return
	}
	root := query.Nodes[rootID]
	if root == nil || len(root.ProjectList) != len(columns) {
		return
	}

	for idx, expr := range root.ProjectList {
		if !isDirectPreparedGroupConcatResult(query, rootID, expr, make(map[preparedGroupConcatResultColumnRef]struct{})) {
			continue
		}
		col := columns[idx]
		if col == nil {
			continue
		}
		isBinary := col.Typ.Id == int32(types.T_blob) ||
			col.Typ.Charset == uint32(types.CharsetBinary)
		if groupConcatMaxLen <= preparedGroupConcatVarcharMaxLen {
			if isBinary {
				col.Typ.Id = int32(types.T_varbinary)
				col.Typ.Charset = uint32(types.CharsetBinary)
			} else {
				col.Typ.Id = int32(types.T_varchar)
			}
			col.Typ.Width = int32(groupConcatMaxLen)
		} else if isBinary {
			// MySQL exposes a binary GROUP_CONCAT as BLOB above the VARCHAR
			// threshold. Keep the binary OID so colDef2MysqlColumn emits both
			// the binary charset and BINARY_FLAG.
			col.Typ.Id = int32(types.T_blob)
			col.Typ.Charset = uint32(types.CharsetBinary)
		} else {
			col.Typ.Id = int32(types.T_text)
			col.Typ.Width = types.MaxLongTextLen
		}
	}
}

func isDirectPreparedGroupConcatResult(
	query *plan.Query, nodeID int32, expr *plan.Expr, seen map[preparedGroupConcatResultColumnRef]struct{},
) bool {
	if expr == nil {
		return false
	}
	if query == nil || nodeID < 0 || int(nodeID) >= len(query.Nodes) {
		return false
	}
	node := query.Nodes[nodeID]
	if node == nil {
		return false
	}
	switch node.NodeType {
	case plan.Node_UNION, plan.Node_UNION_ALL,
		plan.Node_INTERSECT, plan.Node_INTERSECT_ALL,
		plan.Node_MINUS, plan.Node_MINUS_ALL:
		// Set-operation output types are common to both branches. Following
		// only the left projection could narrow a result whose right branch
		// still produces a wider value.
		return false
	}
	if fn := expr.GetF(); fn != nil && fn.GetFunc() != nil {
		return fn.GetFunc().GetObjName() == plan2.NameGroupConcat
	}
	col := expr.GetCol()
	if col == nil {
		return false
	}
	switch {
	case col.RelPos == -2:
		if node.NodeType != plan.Node_AGG {
			return false
		}
		aggPos := col.ColPos - int32(len(node.GroupBy))
		if aggPos < 0 || int(aggPos) >= len(node.AggList) {
			return false
		}
		return isDirectPreparedGroupConcatResult(
			query, nodeID, node.AggList[aggPos], seen)
	case col.RelPos >= 0:
		if int(col.RelPos) >= len(node.Children) {
			return false
		}
		childID := node.Children[col.RelPos]
		if childID < 0 || int(childID) >= len(query.Nodes) {
			return false
		}
		child := query.Nodes[childID]
		if child == nil || col.ColPos < 0 || int(col.ColPos) >= len(child.ProjectList) {
			return false
		}
		ref := preparedGroupConcatResultColumnRef{nodeID: childID, colPos: col.ColPos}
		if _, ok := seen[ref]; ok {
			return false
		}
		seen[ref] = struct{}{}
		return isDirectPreparedGroupConcatResult(
			query, childID, child.ProjectList[col.ColPos], seen)
	default:
		return false
	}
}

func sessionTxnHaveDDL(ses FeSession) bool {
	if ses == nil || ses.GetProc() == nil {
		return false
	}
	txnOperator := ses.GetProc().GetTxnOperator()
	if txnOperator == nil {
		return false
	}
	workspace := txnOperator.GetWorkspace()
	return workspace != nil && workspace.GetHaveDDL()
}

func getSelectColumnsAndResultColumns(ctx context.Context, cw ComputationWrapper) ([]interface{}, []*plan2.ColDef, error) {
	if txnCW, ok := cw.(*TxnComputationWrapper); ok {
		if _, ok = txnCW.GetAst().(*tree.Select); ok {
			return txnCW.getColumnsWithResultColumns(ctx)
		}
	}

	columns, err := cw.GetColumns(ctx)
	if err != nil {
		return nil, nil, err
	}
	return columns, plan2.GetResultColumnsFromPlan(cw.Plan()), nil
}

type resultMetadataFreezer interface {
	FreezeResultMetadata()
}

func freezeResultMetadata(runner ComputationRunner) {
	if freezer, ok := runner.(resultMetadataFreezer); ok {
		freezer.FreezeResultMetadata()
	}
}

// executeResultRowStmt run the statemet that responses result rows
func executeResultRowStmt(ses *Session, execCtx *ExecCtx) (err error) {
	var columns []interface{}
	var colDefs []*plan2.ColDef
	ses.EnterFPrint(FPResultRowStmt)
	defer ses.ExitFPrint(FPResultRowStmt)
	if execCtx.stmt.StmtKind().RespType() == tree.RESP_DEFERRED_RESULT_ROW {
		if execCtx.returning == nil || execCtx.returning.spool == nil {
			return moerr.NewInternalError(execCtx.reqCtx, "DML RETURNING spool is not initialized")
		}
		if execCtx.runResult, err = execCtx.runner.Run(0); err != nil {
			return err
		}
		// RETURNING rows are attempt-spooled and no metadata has reached the
		// client yet. Sync a definition-retried generation first, then derive
		// metadata from the plan that actually produced the committed spool.
		if txnCw, ok := execCtx.cw.(*TxnComputationWrapper); ok {
			if runningCompile, ok := execCtx.runner.(Compile); ok {
				txnCw.syncCompileExecution(runningCompile)
			}
		}
		columns, err = execCtx.cw.GetColumns(execCtx.reqCtx)
		if err != nil {
			return err
		}
		colDefs = plan2.GetResultColumnsFromPlan(execCtx.cw.Plan())
		if len(columns) != len(colDefs) {
			return moerr.NewInternalError(execCtx.reqCtx, "DML RETURNING metadata does not match projection")
		}
		ses.rs = &plan.ResultColDef{ResultCols: colDefs}
		execCtx.returning.columns = columns
		execCtx.returning.affectedRows = execCtx.runResult.AffectRows
		if got := execCtx.returning.spool.RowCount(); got != execCtx.runResult.AffectRows {
			return moerr.NewInternalErrorf(execCtx.reqCtx,
				"DML RETURNING row count %d does not match affected rows %d", got, execCtx.runResult.AffectRows)
		}
		if canSaveQueryResult(execCtx.reqCtx, ses) {
			saver := &QueryResult{}
			execCtx.returning.stagedSaver = saver
			if err = execCtx.returning.spool.Replay(execCtx.reqCtx, func(bat *batch.Batch, crs *perfcounter.CounterSet) error {
				return saver.Stage(execCtx, crs, bat)
			}); err != nil {
				return err
			}
			if err = saver.FinishStage(execCtx); err != nil {
				return err
			}
		}
		return nil
	}
	switch statement := execCtx.stmt.(type) {
	case *tree.Select:

		columns, colDefs, err = getSelectColumnsAndResultColumns(execCtx.reqCtx, execCtx.cw)
		if err != nil {
			ses.Error(execCtx.reqCtx,
				"Failed to get columns from computation handler",
				zap.Error(err))
			return
		}

		ses.rs = &plan.ResultColDef{ResultCols: colDefs}

		ses.EnterFPrint(FPResultRowStmtSelect1)
		defer ses.ExitFPrint(FPResultRowStmtSelect1)
		freezeResultMetadata(execCtx.runner)
		cursorExecute := execCtx.input != nil && execCtx.input.isCursorExecute
		if cursorExecute {
			// A cursor must retain its metadata before the pipeline starts so
			// captured batches can be decoded, but its execute terminator must
			// not be sent until all batches have materialized successfully.
			if resper, ok := execCtx.resper.(*MysqlResp); ok {
				resper.setPreparedCursorColumns(execCtx, columns)
			} else {
				return moerr.NewInternalError(execCtx.reqCtx, "prepared cursor requires MySQL response writer")
			}
		} else {
			err = execCtx.resper.RespPreMeta(execCtx, columns)
			if err != nil {
				return
			}
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
		// Cursor metadata is retained above for decoding, but its wire response
		// is emitted by respStreamResultRow after transaction finalization. This
		// prevents a later autocommit commit error from following a successful
		// cursor response on the same connection.

		// only log if run time is longer than 1s
		if time.Since(runBegin) > time.Second {
			ses.Infof(execCtx.reqCtx, "time of Exec.Run : %s", time.Since(runBegin).String())
		}

	case *tree.ExplainAnalyze, *tree.ExplainPhyPlan:
		query := execCtx.cw.Plan().GetQuery()
		var reqCtx = execCtx.reqCtx
		var txnHaveDDL bool
		ws := ses.proc.GetTxnOperator().GetWorkspace()
		if ws != nil {
			txnHaveDDL = ws.GetHaveDDL()
		}

		var explainColName string
		if _, ok := statement.(*tree.ExplainAnalyze); ok {
			explainColName = plan2.GetPlanTitle(query, txnHaveDDL)
		} else {
			explainColName = plan2.GetPhyPlanTitle(query, txnHaveDDL)
		}

		colDefs, columns, err = GetExplainColumn(reqCtx, explainColName)
		if err != nil {
			ses.Error(execCtx.reqCtx,
				"Failed to get columns from ExplainColumns handler",
				zap.Error(err))
			return
		}

		ses.rs = &plan.ResultColDef{
			ResultCols: colDefs,
		}

		ses.EnterFPrint(FPResultRowStmtExplainAnalyze1)
		defer ses.ExitFPrint(FPResultRowStmtExplainAnalyze1)
		freezeResultMetadata(execCtx.runner)
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
		freezeResultMetadata(execCtx.runner)
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
	resper.setPreparedCursorColumns(execCtx, columns)
	if err = resper.writeColumnDefs(ses, execCtx, columns); err != nil {
		return err
	}
	/*
		mysql COM_QUERY response: End after the column has been sent.
		send EOF packet
	*/
	return resper.mysqlRrWr.WriteEOFIFAndNoFlush(0, ses.GetTxnHandler().GetServerStatus())
}

// respCursorColumnDefs writes the complete COM_STMT_EXECUTE cursor response
// after the pipeline has successfully materialized the result and transaction
// finalization has succeeded. A server cursor response has one and only one
// execute terminator: the EOF/OK packet following the column definitions,
// carrying SERVER_STATUS_CURSOR_EXISTS. Delaying this packet prevents a failed
// decode, limit check, pipeline, or commit operation from advertising a cursor
// that cannot be fetched.
func (resper *MysqlResp) respCursorColumnDefs(ses *Session, execCtx *ExecCtx, columns []any) error {
	if execCtx.inMigration {
		return nil
	}
	resper.setPreparedCursorColumns(execCtx, columns)
	if err := resper.writeColumnDefs(ses, execCtx, columns); err != nil {
		return err
	}
	status := cursorExecuteStatus(checkMoreResultSet(ses.getStatusAfterTxnIsEnded(), execCtx.isLastStmt))
	return resper.mysqlRrWr.WriteEOFOrOK(0, status)
}

func (resper *MysqlResp) setPreparedCursorColumns(execCtx *ExecCtx, columns []any) {
	if execCtx == nil || execCtx.input == nil || !execCtx.input.isCursorExecute ||
		execCtx.prepareStmt == nil || execCtx.prepareStmt.cursor == nil {
		return
	}
	if execCtx.prepareStmt.cursor.result == nil {
		execCtx.prepareStmt.cursor.result = &MysqlResultSet{}
	}
	cursorColumns := make([]Column, 0, len(columns))
	for _, column := range columns {
		cursorColumns = append(cursorColumns, column.(Column))
	}
	execCtx.prepareStmt.cursor.result.Columns = cursorColumns
}

func (resper *MysqlResp) writeColumnDefs(ses *Session, execCtx *ExecCtx, columns []any) (err error) {
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
		if execCtx.input != nil && execCtx.input.isCursorExecute {
			// The execute pipeline has already materialized the retained rows. Emit
			// the column definitions and the sole cursor terminator only now: this
			// callback runs after executeStmtWithWorkspace has finalized the
			// transaction. Fetch owns the next protocol packet, including
			// LAST_ROW_SENT for an empty result.
			if execCtx.prepareStmt == nil || execCtx.prepareStmt.cursor == nil ||
				execCtx.prepareStmt.cursor.result == nil || len(execCtx.prepareStmt.cursor.result.Columns) == 0 {
				err = moerr.NewInternalError(execCtx.reqCtx, "prepared cursor result metadata is missing")
				return
			}
			columns := make([]any, len(execCtx.prepareStmt.cursor.result.Columns))
			for i, column := range execCtx.prepareStmt.cursor.result.Columns {
				columns[i] = column
			}
			if err = resper.respCursorColumnDefs(ses, execCtx, columns); err != nil {
				return
			}
			return nil
		}
		status := checkMoreResultSet(ses.getStatusAfterTxnIsEnded(), execCtx.isLastStmt)
		err2 := resper.mysqlRrWr.WriteEOFOrOK(0, status)
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
		appendSchedulingExplain(buffer, schedulingTraceForExplain(ses, execCtx.cw))

		err = buildMoExplainQuery(execCtx, explainColName, buffer, ses, getDataFromPipeline)
		if err != nil {
			return
		}

		err = resper.mysqlRrWr.WriteEOFOrOK(0, checkMoreResultSet(ses.getStatusAfterTxnIsEnded(), execCtx.isLastStmt))
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
		err = buildMoExplainPhyPlan(
			execCtx,
			explainColName,
			reader,
			ses,
			getDataFromPipeline,
			schedulingTraceForExplain(ses, execCtx.cw),
		)
		if err != nil {
			return
		}

		err = resper.mysqlRrWr.WriteEOFOrOK(0, checkMoreResultSet(ses.getStatusAfterTxnIsEnded(), execCtx.isLastStmt))
		if err != nil {
			return
		}

		//--------------------------------------------------------------------------------------------------------------
	default:
		err = resper.mysqlRrWr.WriteEOFOrOK(0, checkMoreResultSet(ses.getStatusAfterTxnIsEnded(), execCtx.isLastStmt))
		if err != nil {
			return
		}
	}

	return
}

func cursorExecuteStatus(status uint16) uint16 {
	status &^= SERVER_STATUS_CURSOR_EXISTS | SERVER_STATUS_LAST_ROW_SENT
	return status | SERVER_STATUS_CURSOR_EXISTS
}

func schedulingTraceFromComputationWrapper(cw ComputationWrapper) schedule.Trace {
	if provider, ok := cw.(interface{ SchedulingTrace() schedule.Trace }); ok {
		return provider.SchedulingTrace()
	}
	return schedule.Trace{}
}

func schedulingTraceForExplain(ses *Session, cw ComputationWrapper) schedule.Trace {
	if !explainSchedulingEnabled(ses) {
		return schedule.Trace{}
	}
	return schedulingTraceFromComputationWrapper(cw)
}

func (resper *MysqlResp) respPrebuildResultRow(ses *Session,
	execCtx *ExecCtx) (err error) {
	ses.EnterFPrint(FPrespPrebuildResultRow)
	defer ses.ExitFPrint(FPrespPrebuildResultRow)
	if execCtx.inMigration {
		return nil
	}
	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	res := ses.SetNewResponse(ResultResponse, 0, int(ses.GetCmd()), mer, execCtx.isLastStmt)
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

	err = resper.mysqlRrWr.WriteEOFOrOK(0, checkMoreResultSet(ses.getStatusAfterTxnIsEnded(), execCtx.isLastStmt))
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
	if len(execCtx.results) == 0 {
		var affectedRows uint64
		if execCtx.runResult != nil {
			affectedRows = execCtx.runResult.AffectRows
		}
		resp := setResponse(ses, execCtx.isLastStmt, affectedRows)
		if err = resper.mysqlRrWr.WriteResponse(execCtx.reqCtx, resp); err != nil {
			return moerr.NewInternalErrorf(execCtx.reqCtx, "routine send response failed. error:%v ", err)
		}
	} else {
		_, isCall := execCtx.stmt.(*tree.CallStmt)
		cmd := int(COM_QUERY)
		if execCtx.input != nil && execCtx.input.isBinaryProtExecute {
			cmd = int(COM_STMT_EXECUTE)
		}
		for i, result := range execCtx.results {
			mer := NewMysqlExecutionResult(0, 0, 0, 0, result.(*MysqlResultSet))
			isLastResult := i == len(execCtx.results)-1 && execCtx.isLastStmt && !isCall
			resp := ses.SetNewResponse(ResultResponse, 0, cmd, mer, isLastResult)
			if err = resper.mysqlRrWr.WriteResponse(execCtx.reqCtx, resp); err != nil {
				return moerr.NewInternalErrorf(execCtx.reqCtx, "routine send response failed. error:%v ", err)
			}
		}
		if isCall {
			var affectedRows uint64
			if execCtx.runResult != nil {
				affectedRows = execCtx.runResult.AffectRows
			}
			resp := setResponse(ses, execCtx.isLastStmt, affectedRows)
			if err = resper.mysqlRrWr.WriteResponse(execCtx.reqCtx, resp); err != nil {
				return moerr.NewInternalErrorf(execCtx.reqCtx, "routine send response failed. error:%v ", err)
			}
		}
	}
	return
}
