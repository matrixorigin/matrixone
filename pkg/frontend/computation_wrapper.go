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
	"github.com/mohae/deepcopy"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	txnTrace "github.com/matrixorigin/matrixone/pkg/txn/trace"
	util2 "github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	_ ComputationWrapper = &TxnComputationWrapper{}
	_ ComputationWrapper = &NullComputationWrapper{}
)

type NullComputationWrapper struct {
	*TxnComputationWrapper
}

func InitNullComputationWrapper(ses *Session, stmt tree.Statement, proc *process.Process) *NullComputationWrapper {
	return &NullComputationWrapper{
		TxnComputationWrapper: InitTxnComputationWrapper(ses, stmt, proc),
	}
}

func (ncw *NullComputationWrapper) GetAst() tree.Statement {
	return ncw.stmt
}

func (ncw *NullComputationWrapper) GetColumns(context.Context) ([]interface{}, error) {
	return []interface{}{}, nil
}

func (ncw *NullComputationWrapper) Compile(any any, fill func(*batch.Batch) error) (interface{}, error) {
	return nil, nil
}

func (ncw *NullComputationWrapper) RecordExecPlan(ctx context.Context) error {
	return nil
}

func (ncw *NullComputationWrapper) GetUUID() []byte {
	return ncw.uuid[:]
}

func (ncw *NullComputationWrapper) Run(ts uint64) (*util2.RunResult, error) {
	return nil, nil
}

func (ncw *NullComputationWrapper) GetLoadTag() bool {
	return false
}
func (ncw *NullComputationWrapper) Clear() {

}
func (ncw *NullComputationWrapper) Plan() *plan.Plan {
	return nil
}
func (ncw *NullComputationWrapper) ResetPlanAndStmt(tree.Statement) {

}

func (ncw *NullComputationWrapper) Free() {
	ncw.Clear()
}

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

func InitTxnComputationWrapper(ses FeSession, stmt tree.Statement, proc *process.Process) *TxnComputationWrapper {
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
	cwft.stmt = stmt
}

func (cwft *TxnComputationWrapper) GetAst() tree.Statement {
	return cwft.stmt
}

func (cwft *TxnComputationWrapper) Free() {
	if cwft.stmt != nil {
		if !cwft.ifIsExeccute {
			cwft.stmt.Free()
			cwft.stmt = nil
		}
	}
	cwft.Clear()
}

func (cwft *TxnComputationWrapper) Clear() {
	cwft.plan = nil
	cwft.proc = nil
	cwft.ses = nil
	cwft.compile = nil
	cwft.runResult = nil
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
		c := new(MysqlColumn)
		c.SetName(col.Name)
		c.SetOrgName(col.Name)
		c.SetTable(col.TblName)
		c.SetOrgTable(col.TblName)
		c.SetAutoIncr(col.Typ.AutoIncr)
		c.SetSchema(col.DbName)
		err = convertEngineTypeToMysqlType(ctx, types.T(col.Typ.Id), c)
		if err != nil {
			return nil, err
		}
		setColFlag(c)
		setColLength(c, col.Typ.Width)
		setCharacter(c)

		// For binary/varbinary with mysql_type_varchar.Change the charset.
		if types.T(col.Typ.Id) == types.T_binary || types.T(col.Typ.Id) == types.T_varbinary {
			c.SetCharset(0x3f)
		}

		c.SetDecimal(col.Typ.Scale)
		convertMysqlTextTypeToBlobType(c)
		columns[i] = c
	}
	return columns, err
}

func (cwft *TxnComputationWrapper) GetClock() clock.Clock {
	rt := runtime.ProcessLevelRuntime()
	return rt.Clock()
}

func (cwft *TxnComputationWrapper) GetServerStatus() uint16 {
	return uint16(cwft.ses.GetTxnHandler().GetServerStatus())
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
		if ids := isResultQuery(cwft.plan); ids != nil {
			if err = checkPrivilege(ids, execCtx.reqCtx, cwft.ses.(*Session)); err != nil {
				return nil, err
			}
		}
	}

	if _, ok := cwft.stmt.(*tree.Execute); ok {
		executePlan := cwft.plan.GetDcl().GetExecute()
		plan, stmt, sql, err := replacePlan(execCtx.reqCtx, cwft.ses.(*Session), cwft, executePlan)
		if err != nil {
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

		//check privilege
		/* prepare not need check privilege
		   err = authenticateUserCanExecutePrepareOrExecute(requestCtx, cwft.ses, prepareStmt.PrepareStmt, newPlan)
		   if err != nil {
		   	return nil, err
		   }
		*/
	}

	addr := ""
	if len(getGlobalPu().ClusterNodes) > 0 {
		addr = getGlobalPu().ClusterNodes[0].Addr
	}
	cwft.proc.Ctx = execCtx.reqCtx
	cwft.proc.FileService = getGlobalPu().FileService

	var tenant string
	tInfo := cwft.ses.GetTenantInfo()
	if tInfo != nil {
		tenant = tInfo.GetTenant()
	}

	stats := statistic.StatsInfoFromContext(execCtx.reqCtx)
	stats.CompileStart()
	defer stats.CompileEnd()
	cwft.compile = compile.NewCompile(
		addr,
		cwft.ses.GetDatabaseName(),
		cwft.ses.GetSql(),
		tenant,
		cwft.ses.GetUserName(),
		execCtx.reqCtx,
		cwft.ses.GetTxnHandler().GetStorage(),
		cwft.proc,
		cwft.stmt,
		cwft.ses.GetIsInternal(),
		deepcopy.Copy(cwft.ses.getCNLabels()).(map[string]string),
		getStatementStartAt(execCtx.reqCtx),
	)
	defer func() {
		if err != nil {
			cwft.compile.Release()
		}
	}()
	cwft.compile.SetBuildPlanFunc(func() (*plan2.Plan, error) {
		plan, err := buildPlan(execCtx.reqCtx, cwft.ses, cwft.ses.GetTxnCompileCtx(), cwft.stmt)
		if err != nil {
			return nil, err
		}
		if plan.IsPrepare {
			_, _, err = plan2.ResetPreparePlan(cwft.ses.GetTxnCompileCtx(), plan)
		}
		return plan, err
	})

	if _, ok := cwft.stmt.(*tree.ExplainAnalyze); ok {
		fill = func(bat *batch.Batch) error { return nil }
	}
	err = cwft.compile.Compile(execCtx.reqCtx, cwft.plan, fill)
	if err != nil {
		return nil, err
	}
	// check if it is necessary to initialize the temporary engine
	if !cwft.ses.GetTxnHandler().HasTempEngine() && cwft.compile.NeedInitTempEngine() {
		// 0. init memory-non-dist storage
		err = cwft.ses.GetTxnHandler().CreateTempStorage(cwft.GetClock())
		if err != nil {
			return nil, err
		}

		// temporary storage is passed through Ctx
		updateTempStorageInCtx(execCtx, cwft.proc, cwft.ses.GetTxnHandler().GetTempStorage())

		// 1. init memory-non-dist engine
		cwft.ses.GetTxnHandler().CreateTempEngine()
		tempEngine := cwft.ses.GetTxnHandler().GetTempEngine()

		// 2. bind the temporary engine to the session and txnHandler
		cwft.compile.SetTempEngine(tempEngine, cwft.ses.GetTxnHandler().GetTempStorage())

		// 3. init temp-db to store temporary relations
		txnOp2 := cwft.ses.GetTxnHandler().GetTxn()
		err = tempEngine.Create(execCtx.reqCtx, defines.TEMPORARY_DBNAME, txnOp2)
		if err != nil {
			return nil, err
		}
	}
	cwft.compile.SetOriginSQL(originSQL)
	return cwft.compile, err
}

func updateTempStorageInCtx(execCtx *ExecCtx, proc *process.Process, tempStorage *memorystorage.Storage) {
	if execCtx != nil && execCtx.reqCtx != nil {
		execCtx.reqCtx = attachValue(execCtx.reqCtx, defines.TemporaryTN{}, tempStorage)
	}
	if proc != nil && proc.Ctx != nil {
		proc.Ctx = attachValue(proc.Ctx, defines.TemporaryTN{}, tempStorage)
	}
}

func (cwft *TxnComputationWrapper) RecordExecPlan(ctx context.Context) error {
	if stm := motrace.StatementFromContext(ctx); stm != nil {
		waitActiveCost := time.Duration(0)
		if handler := cwft.ses.GetTxnHandler(); handler.InActiveTxn() {
			txn := handler.GetTxn()
			if txn != nil {
				waitActiveCost = txn.GetWaitActiveCost()
			}
		}
		stm.SetSerializableExecPlan(NewJsonPlanHandler(ctx, stm, cwft.plan, WithWaitActiveCost(waitActiveCost)))
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

// replacePlan replaces the plan of the EXECUTE by the plan generated by
// the PREPARE and setups the params for the plan.
func replacePlan(reqCtx context.Context, ses *Session, cwft *TxnComputationWrapper, execPlan *plan.Execute) (*plan.Plan, tree.Statement, string, error) {
	originSQL := ""
	stmtName := execPlan.GetName()
	prepareStmt, err := ses.GetPrepareStmt(reqCtx, stmtName)
	if err != nil {
		return nil, nil, originSQL, err
	}
	if txnTrace.GetService().Enabled(txnTrace.FeatureTraceTxn) {
		originSQL = tree.String(prepareStmt.PrepareStmt, dialect.MYSQL)
	}
	preparePlan := prepareStmt.PreparePlan.GetDcl().GetPrepare()

	// TODO check if schema change, obj.Obj is zero all the time in 0.6
	for _, obj := range preparePlan.GetSchemas() {
		newObj, newTableDef := ses.txnCompileCtx.Resolve(obj.SchemaName, obj.ObjName, plan2.Snapshot{TS: &timestamp.Timestamp{}})
		if newObj == nil {
			return nil, nil, originSQL, moerr.NewInternalError(reqCtx, "table '%s' in prepare statement '%s' does not exist anymore", obj.ObjName, stmtName)
		}
		if newObj.Obj != obj.Obj || newTableDef.Version != uint32(obj.Server) {
			return nil, nil, originSQL, moerr.NewInternalError(reqCtx, "table '%s' has been changed, please reset prepare statement '%s'", obj.ObjName, stmtName)
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
			return nil, nil, originSQL, moerr.NewInvalidInput(reqCtx, "Incorrect arguments to EXECUTE")
		}
		cwft.proc.SetPrepareParams(prepareStmt.params)
	} else if len(execPlan.Args) > 0 {
		if len(execPlan.Args) != numParams {
			return nil, nil, originSQL, moerr.NewInvalidInput(reqCtx, "Incorrect arguments to EXECUTE")
		}
		params := cwft.proc.GetVector(types.T_text.ToType())
		paramVals := make([]any, numParams)
		for i, arg := range execPlan.Args {
			exprImpl := arg.Expr.(*plan.Expr_V)
			param, err := cwft.proc.GetResolveVariableFunc()(exprImpl.V.Name, exprImpl.V.System, exprImpl.V.Global)
			if err != nil {
				return nil, nil, originSQL, err
			}
			if param == nil {
				return nil, nil, originSQL, moerr.NewInvalidInput(reqCtx, "Incorrect arguments to EXECUTE")
			}
			err = util.AppendAnyToStringVector(cwft.proc, param, params)
			if err != nil {
				return nil, nil, originSQL, err
			}
			paramVals[i] = param
		}
		cwft.proc.SetPrepareParams(params)
		cwft.paramVals = paramVals
	} else {
		if numParams > 0 {
			return nil, nil, originSQL, moerr.NewInvalidInput(reqCtx, "Incorrect arguments to EXECUTE")
		}
	}
	return preparePlan.Plan, prepareStmt.PrepareStmt, originSQL, nil
}
