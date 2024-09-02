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
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	util2 "github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/mohae/deepcopy"
)

var (
	_ ComputationWrapper = &TxnComputationWrapper{}
)

type TxnComputationWrapper struct {
	stmt      tree.Statement
	plan      *plan2.Plan
	proc      *process.Process
	ses       FeSession
	compile   *compile.Compile
	runResult *util2.RunResult

	ifIsExeccute bool
	uuid         uuid.UUID
	//holds values of params in the PREPARE
	paramVals []any
}

func InitTxnComputationWrapper(
	ses FeSession,
	stmt tree.Statement,
	proc *process.Process,
) *TxnComputationWrapper {
	uuid, _ := uuid.NewV7()
	return &TxnComputationWrapper{
		stmt: stmt,
		proc: proc,
		ses:  ses,
		uuid: uuid,
	}
}

func (cwft *TxnComputationWrapper) Plan() *plan.Plan {
	return cwft.plan
}

func (cwft *TxnComputationWrapper) ResetPlanAndStmt(stmt tree.Statement) {
	cwft.plan = nil
	cwft.freeStmt()
	cwft.stmt = stmt
}

func (cwft *TxnComputationWrapper) GetAst() tree.Statement {
	return cwft.stmt
}

func (cwft *TxnComputationWrapper) Free() {
	cwft.freeStmt()
	cwft.Clear()
}

func (cwft *TxnComputationWrapper) freeStmt() {
	if cwft.stmt != nil {
		if !cwft.ifIsExeccute {
			cwft.stmt.Free()
			cwft.stmt = nil
		}
	}
}

func (cwft *TxnComputationWrapper) Clear() {
	cwft.plan = nil
	cwft.proc = nil
	cwft.ses = nil
	cwft.compile = nil
	cwft.runResult = nil
	cwft.paramVals = nil
}

func (cwft *TxnComputationWrapper) ParamVals() []any {
	return cwft.paramVals
}

func (cwft *TxnComputationWrapper) GetProcess() *process.Process {
	return cwft.proc
}

func (cwft *TxnComputationWrapper) GetColumns(ctx context.Context) ([]interface{}, error) {
	var err error
	cols := plan2.GetResultColumnsFromPlan(cwft.plan)
	switch cwft.GetAst().(type) {
	case *tree.ShowColumns:
		if len(cols) == 7 {
			cols = []*plan2.ColDef{
				{Typ: plan2.Type{Id: int32(types.T_char)}, Name: "Field"},
				{Typ: plan2.Type{Id: int32(types.T_char)}, Name: "Type"},
				{Typ: plan2.Type{Id: int32(types.T_char)}, Name: "Null"},
				{Typ: plan2.Type{Id: int32(types.T_char)}, Name: "Key"},
				{Typ: plan2.Type{Id: int32(types.T_char)}, Name: "Default"},
				{Typ: plan2.Type{Id: int32(types.T_char)}, Name: "Extra"},
				{Typ: plan2.Type{Id: int32(types.T_char)}, Name: "Comment"},
			}
		} else {
			cols = []*plan2.ColDef{
				{Typ: plan2.Type{Id: int32(types.T_char)}, Name: "Field"},
				{Typ: plan2.Type{Id: int32(types.T_char)}, Name: "Type"},
				{Typ: plan2.Type{Id: int32(types.T_char)}, Name: "Collation"},
				{Typ: plan2.Type{Id: int32(types.T_char)}, Name: "Null"},
				{Typ: plan2.Type{Id: int32(types.T_char)}, Name: "Key"},
				{Typ: plan2.Type{Id: int32(types.T_char)}, Name: "Default"},
				{Typ: plan2.Type{Id: int32(types.T_char)}, Name: "Extra"},
				{Typ: plan2.Type{Id: int32(types.T_char)}, Name: "Privileges"},
				{Typ: plan2.Type{Id: int32(types.T_char)}, Name: "Comment"},
			}
		}
	}
	columns := make([]interface{}, len(cols))
	for i, col := range cols {
		c, err := colDef2MysqlColumn(ctx, col)
		if err != nil {
			return nil, err
		}
		columns[i] = c
	}
	return columns, err
}

func (cwft *TxnComputationWrapper) GetServerStatus() uint16 {
	return uint16(cwft.ses.GetTxnHandler().GetServerStatus())
}

func checkResultQueryPrivilege(proc *process.Process, p *plan.Plan, reqCtx context.Context, sid string, ses *Session) error {
	var ids []string
	var err error
	if ids, err = isResultQuery(proc, p); err != nil || ids == nil {
		return err
	}
	return checkPrivilege(sid, ids, reqCtx, ses)
}

func (cwft *TxnComputationWrapper) Compile(any any, fill func(*batch.Batch) error) (interface{}, error) {
	var originSQL string
	var span trace.Span
	execCtx := any.(*ExecCtx)
	execCtx.reqCtx, span = trace.Start(execCtx.reqCtx, "TxnComputationWrapper.Compile",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End(trace.WithStatementExtra(cwft.ses.GetTxnId(), cwft.ses.GetStmtId(), cwft.ses.GetSqlOfStmt()))

	var err error
	defer RecordStatementTxnID(execCtx.reqCtx, cwft.ses)
	if cwft.ses.GetTxnHandler().HasTempEngine() {
		updateTempStorageInCtx(execCtx, cwft.proc, cwft.ses.GetTxnHandler().GetTempStorage())
	}

	cacheHit := cwft.plan != nil
	if !cacheHit {
		cwft.plan, err = buildPlan(execCtx.reqCtx, cwft.ses, cwft.ses.GetTxnCompileCtx(), cwft.stmt)
	} else if cwft.ses != nil && cwft.ses.GetTenantInfo() != nil && !cwft.ses.IsBackgroundSession() {
		var accId uint32
		accId, err = defines.GetAccountId(execCtx.reqCtx)
		if err != nil {
			return nil, err
		}
		cwft.ses.SetAccountId(accId)
		err = authenticateCanExecuteStatementAndPlan(execCtx.reqCtx, cwft.ses.(*Session), cwft.stmt, cwft.plan)
	}
	if err != nil {
		return nil, err
	}
	if !cwft.ses.IsBackgroundSession() {
		cwft.ses.SetPlan(cwft.plan)
		if err := checkResultQueryPrivilege(cwft.proc, cwft.plan, execCtx.reqCtx, cwft.ses.GetService(), cwft.ses.(*Session)); err != nil {
			return nil, err
		}
	}

	if _, ok := cwft.stmt.(*tree.Execute); ok {
		executePlan := cwft.plan.GetDcl().GetExecute()
		retComp, plan, stmt, sql, err := initExecuteStmtParam(execCtx.reqCtx, cwft.ses.(*Session), cwft, executePlan)
		if err != nil {
			return nil, err
		}
		if err := checkResultQueryPrivilege(cwft.proc, plan, execCtx.reqCtx, cwft.ses.GetService(), cwft.ses.(*Session)); err != nil {
			return nil, err
		}
		originSQL = sql
		cwft.plan = plan

		cwft.stmt.Free()
		// reset plan & stmt
		cwft.stmt = stmt
		cwft.ifIsExeccute = true
		// reset some special stmt for execute statement
		switch cwft.stmt.(type) {
		case *tree.ShowTableStatus:
			cwft.ses.SetShowStmtType(ShowTableStatus)
			cwft.ses.SetData(nil)
		case *tree.SetVar, *tree.ShowVariables, *tree.ShowErrors, *tree.ShowWarnings,
			*tree.CreateAccount, *tree.AlterAccount, *tree.DropAccount:
			return nil, nil
		}

		if retComp == nil {
			cwft.compile, err = createCompile(execCtx, cwft.ses, cwft.proc, cwft.ses.GetSql(), cwft.stmt, cwft.plan, fill, false)
			if err != nil {
				return nil, err
			}
			cwft.compile.SetOriginSQL(originSQL)
		} else {
			// retComp
			cwft.proc.ReplaceTopCtx(execCtx.reqCtx)
			retComp.Reset(cwft.proc, getStatementStartAt(execCtx.reqCtx), fill, cwft.ses.GetSql())
			cwft.compile = retComp
		}

		//check privilege
		/* prepare not need check privilege
		   err = authenticateUserCanExecutePrepareOrExecute(requestCtx, cwft.ses, prepareStmt.PrepareStmt, newPlan)
		   if err != nil {
		   	return nil, err
		   }
		*/
	} else {
		cwft.compile, err = createCompile(execCtx, cwft.ses, cwft.proc, execCtx.sqlOfStmt, cwft.stmt, cwft.plan, fill, false)
		if err != nil {
			return nil, err
		}
	}

	return cwft.compile, err
}

func updateTempStorageInCtx(execCtx *ExecCtx, proc *process.Process, tempStorage *memorystorage.Storage) {
	if execCtx != nil && execCtx.reqCtx != nil {
		execCtx.reqCtx = attachValue(execCtx.reqCtx, defines.TemporaryTN{}, tempStorage)
		proc.ReplaceTopCtx(execCtx.reqCtx)
	}
}

func (cwft *TxnComputationWrapper) RecordExecPlan(ctx context.Context) error {
	if stm := cwft.ses.GetStmtInfo(); stm != nil {
		waitActiveCost := time.Duration(0)
		if handler := cwft.ses.GetTxnHandler(); handler.InActiveTxn() {
			txn := handler.GetTxn()
			if txn != nil {
				waitActiveCost = txn.GetWaitActiveCost()
			}
		}
		stm.SetSerializableExecPlan(NewJsonPlanHandler(ctx, stm, cwft.ses, cwft.plan, WithWaitActiveCost(waitActiveCost)))
	}
	return nil
}

func (cwft *TxnComputationWrapper) GetUUID() []byte {
	return cwft.uuid[:]
}

func (cwft *TxnComputationWrapper) Run(ts uint64) (*util2.RunResult, error) {
	runResult, err := cwft.compile.Run(ts)
	cwft.compile.Release()
	cwft.runResult = runResult
	cwft.compile = nil
	return runResult, err
}

func (cwft *TxnComputationWrapper) GetLoadTag() bool {
	return cwft.plan.GetQuery().GetLoadTag()
}

func appendStatementAt(ctx context.Context, value time.Time) context.Context {
	return context.WithValue(ctx, defines.StartTS{}, value)
}

func getStatementStartAt(ctx context.Context) time.Time {
	v := ctx.Value(defines.StartTS{})
	if v == nil {
		return time.Now()
	}
	return v.(time.Time)
}

// initExecuteStmtParam replaces the plan of the EXECUTE by the plan generated by
// the PREPARE and setups the params for the plan.
func initExecuteStmtParam(reqCtx context.Context, ses *Session, cwft *TxnComputationWrapper, execPlan *plan.Execute) (*compile.Compile, *plan.Plan, tree.Statement, string, error) {
	stmtName := execPlan.GetName()
	prepareStmt, err := ses.GetPrepareStmt(reqCtx, stmtName)
	if err != nil {
		return nil, nil, nil, "", err
	}
	originSQL := prepareStmt.Sql
	preparePlan := prepareStmt.PreparePlan.GetDcl().GetPrepare()

	// TODO check if schema change, obj.Obj is zero all the time in 0.6
	for _, obj := range preparePlan.GetSchemas() {
		change, err := ses.txnCompileCtx.checkTableDefChange(obj.SchemaName, obj.ObjName, uint64(obj.Obj), uint32(obj.Server))
		if err != nil {
			return nil, nil, nil, originSQL, moerr.NewInternalError(reqCtx, "table '%s' in prepare statement '%s' does not exist anymore", obj.ObjName, stmtName)
		}
		if change {
			return nil, nil, nil, originSQL, moerr.NewInternalError(reqCtx, "table '%s' has been changed, please reset prepare statement '%s'", obj.ObjName, stmtName)
		}
	}

	// The default count is 1. Setting it to 2 ensures that memory will not be reclaimed.
	//  Convenient to reuse memory next time
	if prepareStmt.InsertBat != nil {
		prepareStmt.InsertBat.SetCnt(1000) // we will make sure :  when retry in lock error, we will not clean up this batch
		cwft.proc.SetPrepareBatch(prepareStmt.InsertBat)
		cwft.proc.SetPrepareExprList(prepareStmt.exprList)
	}
	numParams := len(preparePlan.ParamTypes)
	if prepareStmt.params != nil && prepareStmt.params.Length() > 0 { // use binary protocol
		if prepareStmt.params.Length() != numParams {
			return nil, nil, nil, originSQL, moerr.NewInvalidInput(reqCtx, "Incorrect arguments to EXECUTE")
		}
		cwft.proc.SetPrepareParams(prepareStmt.params)
	} else if len(execPlan.Args) > 0 {
		if len(execPlan.Args) != numParams {
			return nil, nil, nil, originSQL, moerr.NewInvalidInput(reqCtx, "Incorrect arguments to EXECUTE")
		}
		params := cwft.proc.GetVector(types.T_text.ToType())
		paramVals := make([]any, numParams)
		for i, arg := range execPlan.Args {
			exprImpl := arg.Expr.(*plan.Expr_V)
			param, err := cwft.proc.GetResolveVariableFunc()(exprImpl.V.Name, exprImpl.V.System, exprImpl.V.Global)
			if err != nil {
				return nil, nil, nil, originSQL, err
			}
			if param == nil {
				return nil, nil, nil, originSQL, moerr.NewInvalidInput(reqCtx, "Incorrect arguments to EXECUTE")
			}
			err = util.AppendAnyToStringVector(cwft.proc, param, params)
			if err != nil {
				return nil, nil, nil, originSQL, err
			}
			paramVals[i] = param
		}
		cwft.proc.SetPrepareParams(params)
		cwft.paramVals = paramVals
	} else {
		if numParams > 0 {
			return nil, nil, nil, originSQL, moerr.NewInvalidInput(reqCtx, "Incorrect arguments to EXECUTE")
		}
	}
	return prepareStmt.compile, preparePlan.Plan, prepareStmt.PrepareStmt, originSQL, nil
}

func createCompile(
	execCtx *ExecCtx,
	ses FeSession,
	proc *process.Process,
	originSQL string,
	stmt tree.Statement,
	plan *plan2.Plan,
	fill func(*batch.Batch) error,
	isPrepare bool,
) (retCompile *compile.Compile, err error) {

	addr := ""
	if len(getGlobalPu().ClusterNodes) > 0 {
		addr = getGlobalPu().ClusterNodes[0].Addr
	}
	proc.ReplaceTopCtx(execCtx.reqCtx)
	proc.Base.FileService = getGlobalPu().FileService

	var tenant string
	tInfo := ses.GetTenantInfo()
	if tInfo != nil {
		tenant = tInfo.GetTenant()
	}

	stats := statistic.StatsInfoFromContext(execCtx.reqCtx)
	stats.CompileStart()
	defer stats.CompileEnd()
	defer func() {
		if err != nil && retCompile != nil {
			retCompile.SetIsPrepare(false)
			retCompile.Release()
			retCompile = nil
		}
	}()
	retCompile = compile.NewCompile(
		addr,
		ses.GetDatabaseName(),
		ses.GetSql(),
		tenant,
		ses.GetUserName(),
		ses.GetTxnHandler().GetStorage(),
		proc,
		stmt,
		ses.GetIsInternal(),
		deepcopy.Copy(ses.getCNLabels()).(map[string]string),
		getStatementStartAt(execCtx.reqCtx),
	)
	retCompile.SetIsPrepare(isPrepare)
	retCompile.SetBuildPlanFunc(func(ctx context.Context) (*plan2.Plan, error) {
		plan, err := buildPlan(ctx, ses, ses.GetTxnCompileCtx(), stmt)
		if err != nil {
			return nil, err
		}
		if plan.IsPrepare {
			_, _, err = plan2.ResetPreparePlan(ses.GetTxnCompileCtx(), plan)
		}
		return plan, err
	})

	if _, ok := stmt.(*tree.ExplainAnalyze); ok {
		fill = func(bat *batch.Batch) error { return nil }
	}
	err = retCompile.Compile(execCtx.reqCtx, plan, fill)
	if err != nil {
		return
	}
	// check if it is necessary to initialize the temporary engine
	if !ses.GetTxnHandler().HasTempEngine() && retCompile.NeedInitTempEngine() {
		// 0. init memory-non-dist storage
		err = ses.GetTxnHandler().CreateTempStorage(runtime.ServiceRuntime(ses.GetService()).Clock())
		if err != nil {
			return
		}

		// temporary storage is passed through Ctx
		updateTempStorageInCtx(execCtx, proc, ses.GetTxnHandler().GetTempStorage())

		// 1. init memory-non-dist engine
		ses.GetTxnHandler().CreateTempEngine()
		tempEngine := ses.GetTxnHandler().GetTempEngine()

		// 2. bind the temporary engine to the session and txnHandler
		retCompile.SetTempEngine(tempEngine, ses.GetTxnHandler().GetTempStorage())

		// 3. init temp-db to store temporary relations
		txnOp2 := ses.GetTxnHandler().GetTxn()
		err = tempEngine.Create(execCtx.reqCtx, defines.TEMPORARY_DBNAME, txnOp2)
		if err != nil {
			return
		}
	}
	retCompile.SetOriginSQL(originSQL)
	return
}
