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
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	txnTrace "github.com/matrixorigin/matrixone/pkg/txn/trace"
	util2 "github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/mohae/deepcopy"
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

func (ncw *NullComputationWrapper) GetColumns() ([]interface{}, error) {
	return []interface{}{}, nil
}

func (ncw *NullComputationWrapper) Compile(requestCtx context.Context, u interface{}, fill func(interface{}, *batch.Batch) error) (interface{}, error) {
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

type TxnComputationWrapper struct {
	stmt      tree.Statement
	plan      *plan2.Plan
	proc      *process.Process
	ses       *Session
	compile   *compile.Compile
	runResult *util2.RunResult

	uuid uuid.UUID
	//holds values of params in the PREPARE
	paramVals []any
}

func InitTxnComputationWrapper(ses *Session, stmt tree.Statement, proc *process.Process) *TxnComputationWrapper {
	uuid, _ := uuid.NewV7()
	return &TxnComputationWrapper{
		stmt: stmt,
		proc: proc,
		ses:  ses,
		uuid: uuid,
	}
}

func (cwft *TxnComputationWrapper) GetAst() tree.Statement {
	return cwft.stmt
}

func (cwft *TxnComputationWrapper) Free() {
	if cwft.stmt != nil {
		if !strings.HasPrefix(cwft.ses.sql, "execute ") {
			cwft.stmt.Free()
			cwft.stmt = nil
		}
	}
	cwft.plan = nil
	cwft.proc = nil
	cwft.ses = nil
	cwft.compile = nil
	cwft.runResult = nil
}

func (cwft *TxnComputationWrapper) GetProcess() *process.Process {
	return cwft.proc
}

func (cwft *TxnComputationWrapper) GetColumns() ([]interface{}, error) {
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
		c.SetTable(col.Typ.Table)
		c.SetOrgTable(col.Typ.Table)
		c.SetAutoIncr(col.Typ.AutoIncr)
		c.SetSchema(cwft.ses.GetTxnCompileCtx().DefaultDatabase())
		err = convertEngineTypeToMysqlType(cwft.ses.requestCtx, types.T(col.Typ.Id), c)
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
	return cwft.ses.GetServerStatus()
}

func (cwft *TxnComputationWrapper) Compile(requestCtx context.Context, u interface{}, fill func(interface{}, *batch.Batch) error) (interface{}, error) {
	var originSQL string
	var span trace.Span
	requestCtx, span = trace.Start(requestCtx, "TxnComputationWrapper.Compile",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End(trace.WithStatementExtra(cwft.ses.GetTxnId(), cwft.ses.GetStmtId(), cwft.ses.GetSqlOfStmt()))

	stats := statistic.StatsInfoFromContext(requestCtx)
	stats.CompileStart()
	defer stats.CompileEnd()

	var err error
	defer RecordStatementTxnID(requestCtx, cwft.ses)
	if cwft.ses.IfInitedTempEngine() {
		requestCtx = context.WithValue(requestCtx, defines.TemporaryTN{}, cwft.ses.GetTempTableStorage())
		cwft.ses.SetRequestContext(requestCtx)
		cwft.proc.Ctx = context.WithValue(cwft.proc.Ctx, defines.TemporaryTN{}, cwft.ses.GetTempTableStorage())
		cwft.ses.GetTxnHandler().AttachTempStorageToTxnCtx()
	}

	txnHandler := cwft.ses.GetTxnHandler()
	var txnCtx context.Context
	txnCtx, cwft.proc.TxnOperator, err = txnHandler.GetTxn()
	if err != nil {
		return nil, err
	}

	txnCtx = statistic.EnsureStatsInfoCanBeFound(txnCtx, requestCtx)

	// Increase the statement ID and update snapshot TS before build plan, because the
	// snapshot TS is used when build plan.
	// NB: In internal executor, we should also do the same action, which is increasing
	// statement ID and updating snapshot TS.
	// See `func (exec *txnExecutor) Exec(sql string)` for details.
	txnOp := cwft.proc.TxnOperator
	cwft.ses.SetTxnId(txnOp.Txn().ID)
	//non derived statement
	if txnOp != nil && !cwft.ses.IsDerivedStmt() {
		//startStatement has been called
		ok, _ := cwft.ses.GetTxnHandler().calledStartStmt()
		if !ok {
			txnOp.GetWorkspace().StartStatement()
			cwft.ses.GetTxnHandler().enableStartStmt(txnOp.Txn().ID)
		}

		//increase statement id
		err = txnOp.GetWorkspace().IncrStatementID(requestCtx, false)
		if err != nil {
			return nil, err
		}
		cwft.ses.GetTxnHandler().enableIncrStmt(txnOp.Txn().ID)
	}

	cacheHit := cwft.plan != nil
	if !cacheHit {
		cwft.plan, err = buildPlan(requestCtx, cwft.ses, cwft.ses.GetTxnCompileCtx(), cwft.stmt)
	} else if cwft.ses != nil && cwft.ses.GetTenantInfo() != nil {
		cwft.ses.accountId, err = defines.GetAccountId(requestCtx)
		if err != nil {
			return nil, err
		}
		err = authenticateCanExecuteStatementAndPlan(requestCtx, cwft.ses, cwft.stmt, cwft.plan)
	}
	if err != nil {
		return nil, err
	}
	cwft.ses.p = cwft.plan
	if ids := isResultQuery(cwft.plan); ids != nil {
		if err = checkPrivilege(ids, requestCtx, cwft.ses); err != nil {
			return nil, err
		}
	}
	if _, ok := cwft.stmt.(*tree.Execute); ok {
		executePlan := cwft.plan.GetDcl().GetExecute()
		plan, stmt, sql, err := replacePlan(requestCtx, cwft.ses, cwft, executePlan)
		if err != nil {
			return nil, err
		}
		originSQL = sql
		cwft.plan = plan

		// reset plan & stmt
		cwft.stmt = stmt
		// reset some special stmt for execute statement
		switch cwft.stmt.(type) {
		case *tree.ShowTableStatus:
			cwft.ses.showStmtType = ShowTableStatus
			cwft.ses.SetData(nil)
		case *tree.SetVar, *tree.ShowVariables, *tree.ShowErrors, *tree.ShowWarnings:
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
	if len(cwft.ses.GetParameterUnit().ClusterNodes) > 0 {
		addr = cwft.ses.GetParameterUnit().ClusterNodes[0].Addr
	}
	cwft.proc.Ctx = txnCtx
	cwft.proc.FileService = cwft.ses.GetParameterUnit().FileService

	var tenant string
	tInfo := cwft.ses.GetTenantInfo()
	if tInfo != nil {
		tenant = tInfo.GetTenant()
	}
	cwft.compile = compile.NewCompile(
		addr,
		cwft.ses.GetDatabaseName(),
		cwft.ses.GetSql(),
		tenant,
		cwft.ses.GetUserName(),
		txnCtx,
		cwft.ses.GetStorage(),
		cwft.proc,
		cwft.stmt,
		cwft.ses.isInternal,
		deepcopy.Copy(cwft.ses.getCNLabels()).(map[string]string),
		getStatementStartAt(requestCtx),
	)
	defer func() {
		if err != nil {
			cwft.compile.Release()
		}
	}()
	cwft.compile.SetBuildPlanFunc(func() (*plan2.Plan, error) {
		plan, err := buildPlan(requestCtx, cwft.ses, cwft.ses.GetTxnCompileCtx(), cwft.stmt)
		if err != nil {
			return nil, err
		}
		if plan.IsPrepare {
			_, _, err = plan2.ResetPreparePlan(cwft.ses.GetTxnCompileCtx(), plan)
		}
		return plan, err
	})

	if _, ok := cwft.stmt.(*tree.ExplainAnalyze); ok {
		fill = func(obj interface{}, bat *batch.Batch) error { return nil }
	}
	err = cwft.compile.Compile(txnCtx, cwft.plan, cwft.ses, fill)
	if err != nil {
		return nil, err
	}
	// check if it is necessary to initialize the temporary engine
	if cwft.compile.NeedInitTempEngine(cwft.ses.IfInitedTempEngine()) {
		// 0. init memory-non-dist storage
		var tnStore *metadata.TNService
		tnStore, err = cwft.ses.SetTempTableStorage(cwft.GetClock())
		if err != nil {
			return nil, err
		}

		// temporary storage is passed through Ctx
		requestCtx = context.WithValue(requestCtx, defines.TemporaryTN{}, cwft.ses.GetTempTableStorage())

		// 1. init memory-non-dist engine
		tempEngine := memoryengine.New(
			requestCtx,
			memoryengine.NewDefaultShardPolicy(
				mpool.MustNewZeroNoFixed(),
			),
			memoryengine.RandomIDGenerator,
			clusterservice.NewMOCluster(
				nil,
				0,
				clusterservice.WithDisableRefresh(),
				clusterservice.WithServices(nil, []metadata.TNService{
					*tnStore,
				})),
		)

		// 2. bind the temporary engine to the session and txnHandler
		_ = cwft.ses.SetTempEngine(requestCtx, tempEngine)
		cwft.compile.SetTempEngine(requestCtx, tempEngine)
		txnHandler.SetTempEngine(tempEngine)
		cwft.ses.GetTxnHandler().AttachTempStorageToTxnCtx()

		// 3. init temp-db to store temporary relations
		err = tempEngine.Create(requestCtx, defines.TEMPORARY_DBNAME, cwft.ses.txnHandler.txnOperator)
		if err != nil {
			return nil, err
		}

		cwft.ses.EnableInitTempEngine()
	}
	cwft.compile.SetOriginSQL(originSQL)
	return cwft.compile, err
}

func (cwft *TxnComputationWrapper) RecordExecPlan(ctx context.Context) error {
	if stm := motrace.StatementFromContext(ctx); stm != nil {
		stm.SetSerializableExecPlan(NewJsonPlanHandler(ctx, stm, cwft.plan))
	}
	return nil
}

func (cwft *TxnComputationWrapper) GetUUID() []byte {
	return cwft.uuid[:]
}

func (cwft *TxnComputationWrapper) Run(ts uint64) (*util2.RunResult, error) {
	logDebug(cwft.ses, cwft.ses.GetDebugString(), "compile.Run begin")
	defer func() {
		logDebug(cwft.ses, cwft.ses.GetDebugString(), "compile.Run end")
	}()
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
func replacePlan(requestCtx context.Context, ses *Session, cwft *TxnComputationWrapper, execPlan *plan.Execute) (*plan.Plan, tree.Statement, string, error) {
	originSQL := ""
	stmtName := execPlan.GetName()
	prepareStmt, err := ses.GetPrepareStmt(stmtName)
	if err != nil {
		return nil, nil, originSQL, err
	}
	if txnTrace.GetService().Enabled(txnTrace.FeatureTraceTxn) {
		originSQL = tree.String(prepareStmt.PrepareStmt, dialect.MYSQL)
	}
	preparePlan := prepareStmt.PreparePlan.GetDcl().GetPrepare()

	// TODO check if schema change, obj.Obj is zero all the time in 0.6
	for _, obj := range preparePlan.GetSchemas() {
		newObj, newTableDef := ses.txnCompileCtx.Resolve(obj.SchemaName, obj.ObjName)
		if newObj == nil {
			return nil, nil, originSQL, moerr.NewInternalError(requestCtx, "table '%s' in prepare statement '%s' does not exist anymore", obj.ObjName, stmtName)
		}
		if newObj.Obj != obj.Obj || newTableDef.Version != uint32(obj.Server) {
			return nil, nil, originSQL, moerr.NewInternalError(requestCtx, "table '%s' has been changed, please reset prepare statement '%s'", obj.ObjName, stmtName)
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
			return nil, nil, originSQL, moerr.NewInvalidInput(requestCtx, "Incorrect arguments to EXECUTE")
		}
		cwft.proc.SetPrepareParams(prepareStmt.params)
	} else if len(execPlan.Args) > 0 {
		if len(execPlan.Args) != numParams {
			return nil, nil, originSQL, moerr.NewInvalidInput(requestCtx, "Incorrect arguments to EXECUTE")
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
				return nil, nil, originSQL, moerr.NewInvalidInput(requestCtx, "Incorrect arguments to EXECUTE")
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
			return nil, nil, originSQL, moerr.NewInvalidInput(requestCtx, "Incorrect arguments to EXECUTE")
		}
	}
	return preparePlan.Plan, prepareStmt.PrepareStmt, originSQL, nil
}
