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
	"bytes"
	"context"
	"maps"
	"slices"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/mohae/deepcopy"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	util2 "github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	_ ComputationWrapper = &TxnComputationWrapper{}
)

type Compile interface {
	Run(uint64) (*util2.RunResult, error)
	GetPlan() *plan.Plan
	Release()
	SetOriginSQL(string)
}

type TxnComputationWrapper struct {
	stmt      tree.Statement
	plan      *plan2.Plan
	proc      *process.Process
	ses       FeSession
	compile   Compile
	runResult *util2.RunResult

	ifIsExeccute bool
	// stmtBorrowed is true when another long-lived owner, such as PrepareStmt or
	// the session plan cache, retains stmt. This wrapper must then not return it
	// to the AST pool. The zero value intentionally means owned so ordinary
	// wrappers preserve their existing lifecycle.
	stmtBorrowed bool
	uuid         uuid.UUID
	//holds values of params in the PREPARE
	paramVals []any
	// preparedParamBindingTypes carries the current execution's dependency-only
	// runtime domains into AST-only SET expression evaluation.
	preparedParamBindingTypes []types.Type

	explainBuffer *bytes.Buffer
	binaryPrepare bool
	prepareName   string

	schedulingTrace schedule.TraceRecorder

	// remapDb is the effective database remap for this statement only. A COM_QUERY
	// can contain statements with different inline overrides, so this metadata
	// must travel with the wrapper rather than live at request scope.
	remapDb map[string]string

	// schedulingSQL preserves the raw per-statement fragment, including
	// optimizer comments. sqlOfStmt is intentionally sanitized for logging and
	// therefore cannot carry statement-scoped scheduling intent.
	schedulingSQL string

	// Prepared SQL keeps the lexical mode from PREPARE time. An empty value is
	// a valid mode, so prepared execution tracks its presence separately.
	preparedSchedulingSQLMode    string
	hasPreparedSchedulingSQLMode bool
	preparedSchedulingSQL        string

	// protocolVersion is captured when plan is built. The session plan cache
	// uses it instead of the version observed later when execution completes.
	protocolVersion int64
	// runtimeDecimalParamPositions identifies the parameters materialized while
	// rebuilding this execution's parameter-sensitive DECIMAL plan.
	runtimeDecimalParamPositions []int32
}

func InitTxnComputationWrapper(
	ses FeSession,
	stmt tree.Statement,
	proc *process.Process,
) *TxnComputationWrapper {
	u, _ := util2.FastUuid()
	uuid := uuid.UUID(u)
	return &TxnComputationWrapper{
		stmt: stmt,
		proc: proc,
		ses:  ses,
		uuid: uuid,
	}
}

func (cwft *TxnComputationWrapper) BinaryExecute() (bool, string) {
	return cwft.binaryPrepare, cwft.prepareName
}

func (cwft *TxnComputationWrapper) SetRemapDb(remapDb map[string]string) {
	cwft.remapDb = maps.Clone(remapDb)
}

func (cwft *TxnComputationWrapper) GetRemapDb() map[string]string {
	return cwft.remapDb
}

func (cwft *TxnComputationWrapper) SetSchedulingSQL(sql string) {
	cwft.schedulingSQL = sql
}

func (cwft *TxnComputationWrapper) SchedulingSQL() string {
	return cwft.schedulingSQL
}

func (cwft *TxnComputationWrapper) schedulingSQLOr(fallback string) string {
	if cwft.schedulingSQL != "" {
		return cwft.schedulingSQL
	}
	return fallback
}

func (cwft *TxnComputationWrapper) querySchedulingIntentForPreparedStatement(
	sql string,
) schedule.SchedulingIntent {
	if cwft.hasPreparedSchedulingSQLMode {
		return querySchedulingIntentForStatementWithSQLMode(
			cwft.ses, sql, cwft.preparedSchedulingSQLMode)
	}
	return querySchedulingIntentForStatement(cwft.ses, sql)
}

func (cwft *TxnComputationWrapper) Plan() *plan.Plan {
	return cwft.plan
}

func (cwft *TxnComputationWrapper) ResetPlanAndStmt(stmt tree.Statement) {
	cwft.plan = nil
	cwft.freeStmt()
	cwft.stmt = stmt
	cwft.stmtBorrowed = false
}

func (cwft *TxnComputationWrapper) GetAst() tree.Statement {
	return cwft.stmt
}

func (cwft *TxnComputationWrapper) Free() {
	cwft.freeStmt()
	cwft.Clear()
}

func (cwft *TxnComputationWrapper) freeStmt() {
	if cwft.stmt != nil && !cwft.stmtBorrowed {
		cwft.stmt.Free()
	}
	cwft.stmt = nil
	cwft.stmtBorrowed = false
}

func (cwft *TxnComputationWrapper) Clear() {
	cwft.plan = nil
	cwft.proc = nil
	cwft.ses = nil
	cwft.compile = nil
	cwft.runResult = nil
	cwft.paramVals = nil
	cwft.prepareName = ""
	cwft.binaryPrepare = false
	cwft.remapDb = nil
	cwft.schedulingSQL = ""
	cwft.preparedSchedulingSQLMode = ""
	cwft.hasPreparedSchedulingSQLMode = false
	cwft.preparedSchedulingSQL = ""
	cwft.schedulingTrace.Reset()
	cwft.runtimeDecimalParamPositions = nil
}

func (cwft *TxnComputationWrapper) ParamVals() []any {
	return cwft.paramVals
}

func (cwft *TxnComputationWrapper) GetProcess() *process.Process {
	return cwft.proc
}

func columnsToMysqlColumns(ctx context.Context, cols []*plan2.ColDef) ([]interface{}, error) {
	columns := make([]interface{}, len(cols))
	for i, col := range cols {
		c, err := colDef2MysqlColumn(ctx, col)
		if err != nil {
			return nil, err
		}
		columns[i] = c
	}
	return columns, nil
}

func (cwft *TxnComputationWrapper) getColumnsWithResultColumns(ctx context.Context) ([]interface{}, []*plan2.ColDef, error) {
	cols := plan2.GetResultColumnsFromPlan(cwft.plan)
	columns, err := columnsToMysqlColumns(ctx, cols)
	return columns, cols, err
}

func (cwft *TxnComputationWrapper) GetColumns(ctx context.Context) ([]interface{}, error) {
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
	return columnsToMysqlColumns(ctx, cols)
}

func (cwft *TxnComputationWrapper) GetServerStatus() uint16 {
	return uint16(cwft.ses.GetTxnHandler().GetServerStatus())
}

func checkResultQueryPrivilege(proc *process.Process, p *plan.Plan, reqCtx context.Context, sid string, ses *Session) (statistic.StatsArray, error) {
	var ids []string
	var err error
	var stats statistic.StatsArray
	stats.Reset()

	if ids, err = isResultQuery(proc, p); err != nil || ids == nil {
		return stats, err
	}
	return checkPrivilege(sid, ids, reqCtx, ses)
}

func preparedStatementOwner(ctx context.Context, ses FeSession) (*Session, error) {
	if owner, ok := ses.(*Session); ok {
		return owner, nil
	}
	if backSes, ok := ses.(*backSession); ok && backSes.upstream != nil {
		return backSes.upstream, nil
	}
	return nil, moerr.NewInternalError(ctx, "prepared statement session has no client owner")
}

// Compile build logical plan and then build physical plan `Compile` object
func (cwft *TxnComputationWrapper) Compile(any any, fill func(*batch.Batch, *perfcounter.CounterSet) error) (_ interface{}, err error) {
	var originSQL string
	var span trace.Span

	execCtx := any.(*ExecCtx)
	execCtx.reqCtx, span = trace.Start(execCtx.reqCtx, "TxnComputationWrapper.Compile",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End(trace.WithStatementExtra(cwft.ses.GetTxnId(), cwft.ses.GetStmtId(), cwft.ses.GetSqlOfStmt()))
	defer func() {
		if err != nil {
			cwft.recordSchedulingTraceOnCompileError(execCtx.reqCtx)
		}
	}()

	defer RecordStatementTxnID(execCtx.reqCtx, cwft.ses)
	stats := statistic.StatsInfoFromContext(execCtx.reqCtx)

	cacheHit := cwft.plan != nil
	if !cacheHit {
		cwft.protocolVersion = currentProtocolVersion(cwft.proc)
		buildCtx := execCtx.reqCtx
		compilerCtx := cwft.ses.GetTxnCompileCtx()
		var restoreCompilerCtx context.Context
		if execCtx.input.isPreparedExpr() {
			buildCtx = plan2.WithPrepareRuntimeParams(buildCtx, cwft.runtimeDecimalParamPositions...)
			restoreCompilerCtx = compilerCtx.GetContext()
			compilerCtx.SetContext(plan2.WithPrepareRuntimeParams(restoreCompilerCtx, cwft.runtimeDecimalParamPositions...))
			defer compilerCtx.SetContext(restoreCompilerCtx)
		}
		cwft.plan, err = buildPlanWithPrepareMode(
			buildCtx,
			cwft.ses,
			compilerCtx,
			cwft.stmt,
			execCtx.input.isPreparedExpr(),
		)
		if err != nil {
			return nil, err
		}
	}
	if cwft.ses != nil && cwft.ses.GetTenantInfo() != nil && !cwft.ses.IsBackgroundSession() {
		var accId uint32
		accId, err = defines.GetAccountId(execCtx.reqCtx)
		if err != nil {
			return nil, err
		}
		cwft.ses.SetAccountId(accId)

		// the content of prepare sql don't need to authenticate when execute stmt
		if !execCtx.input.isBinaryProtExecute {
			authStats, err := authenticateCanExecuteStatementAndPlan(execCtx.reqCtx, cwft.ses.(*Session), cwft.stmt, cwft.plan)
			if err != nil {
				return nil, err
			}
			// record permission statistics.
			stats.PermissionAuth.Add(&authStats)
		}
	}

	if !cwft.ses.IsBackgroundSession() {
		cwft.ses.SetPlan(cwft.plan)
		authStats, err := checkResultQueryPrivilege(cwft.proc, cwft.plan, execCtx.reqCtx, cwft.ses.GetService(), cwft.ses.(*Session))
		if err != nil {
			return nil, err
		}
		stats.PermissionAuth.Add(&authStats)
	}

	if _, isTextProtExecute := cwft.stmt.(*tree.Execute); isTextProtExecute || execCtx.input.isBinaryProtExecute {
		owner, ownerErr := preparedStatementOwner(execCtx.reqCtx, cwft.ses)
		if ownerErr != nil {
			return nil, ownerErr
		}
		var retComp *compile.Compile
		var plan *plan.Plan
		var stmt tree.Statement
		var sql string
		var stmtOwned bool
		if isTextProtExecute {
			executePlan := cwft.plan.GetDcl().GetExecute()
			retComp, plan, stmt, sql, stmtOwned, err = initExecuteStmtParamInSession(
				execCtx, owner, cwft.ses, cwft, executePlan, executePlan.GetName())
			if err != nil {
				return nil, err
			}
			if stmtOwned {
				cwft.freeStmt()
				cwft.stmt = stmt
				cwft.stmtBorrowed = false
			}
			if !cwft.ses.IsBackgroundSession() {
				authStats, err := authenticatePreparedDDLOwnerStatement(execCtx.reqCtx, owner, stmt, plan)
				if err != nil {
					return nil, err
				}
				stats.PermissionAuth.Add(&authStats)
				authStats, err = checkResultQueryPrivilege(cwft.proc, plan, execCtx.reqCtx, cwft.ses.GetService(), owner)
				if err != nil {
					return nil, err
				}
				stats.PermissionAuth.Add(&authStats)
			}

			cwft.plan = plan
			if !stmtOwned {
				cwft.freeStmt()
				cwft.stmt = stmt
				cwft.stmtBorrowed = true
			}
		} else {
			// binary protocol execute
			retComp, plan, stmt, sql, stmtOwned, err = initExecuteStmtParamInSession(
				execCtx, owner, cwft.ses, cwft, nil, execCtx.input.stmtName)
			if err != nil {
				return nil, err
			}
			if plan != nil {
				cwft.plan = plan
			}
			if stmt != nil && stmtOwned {
				cwft.stmt = stmt
				cwft.stmtBorrowed = false
			}
			if !cwft.ses.IsBackgroundSession() {
				authStats, err := authenticatePreparedDDLOwnerStatement(
					execCtx.reqCtx, owner, stmt, cwft.plan)
				if err != nil {
					return nil, err
				}
				stats.PermissionAuth.Add(&authStats)
			}
			if stmt != nil && !stmtOwned {
				cwft.stmt = stmt
				cwft.stmtBorrowed = true
			}
		}
		refreshProcessStmtProfileForPreparedStmt(cwft.proc, stmt)
		originSQL = sql
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
			var schedulingSQLMode *string
			if cwft.hasPreparedSchedulingSQLMode {
				schedulingSQLMode = &cwft.preparedSchedulingSQLMode
			}
			cwft.compile, err = createCompile(
				execCtx,
				cwft.ses,
				cwft.proc,
				cwft.ses.GetSql(),
				originSQL,
				schedulingSQLMode,
				cwft.stmt,
				cwft.plan,
				fill,
				false,
				&cwft.schedulingTrace,
				cwft.runtimeDecimalParamPositions,
			)
			if err != nil {
				return nil, err
			}
			cwft.compile.SetOriginSQL(originSQL)
		} else {
			// retComp
			cwft.proc.ReplaceTopCtx(execCtx.reqCtx)
			// originSQL is the prepared statement text here; the wrapper carries
			// the outer EXECUTE fragment, which cannot contain the inner hint.
			retComp.SetQuerySchedulingIntent(cwft.querySchedulingIntentForPreparedStatement(originSQL))
			retComp.SetSchedulingTraceRecorder(&cwft.schedulingTrace)
			if err = retComp.Reset(
				cwft.proc,
				getStatementStartAt(execCtx.reqCtx),
				compileOutputCallback(cwft.stmt, fill),
				cwft.ses.GetSql(),
			); err != nil {
				return nil, err
			}
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
		cwft.compile, err = createCompile(
			execCtx,
			cwft.ses,
			cwft.proc,
			execCtx.sqlOfStmt,
			cwft.schedulingSQLOr(execCtx.sqlOfStmt),
			nil,
			cwft.stmt,
			cwft.plan,
			fill,
			false,
			&cwft.schedulingTrace,
			nil,
		)
		if err != nil {
			return nil, err
		}
	}

	return cwft.compile, err
}

func authenticatePreparedDDLOwnerStatement(reqCtx context.Context, ses *Session, stmt tree.Statement, p *plan.Plan) (statistic.StatsArray, error) {
	var stats statistic.StatsArray
	stats.Reset()
	switch stmt.(type) {
	case *tree.CreateDatabase, *tree.CreateTable:
		return authenticateUserCanExecutePrepareOrExecute(reqCtx, ses, stmt, p)
	default:
		return stats, nil
	}
}

func (cwft *TxnComputationWrapper) RecordExecPlan(ctx context.Context, phyPlan *models.PhyPlan) error {
	if stm := cwft.ses.GetStmtInfo(); stm != nil {
		waitActiveCost := time.Duration(0)
		if handler := cwft.ses.GetTxnHandler(); handler.InActiveTxn() {
			txn := handler.GetTxn()
			if txn != nil {
				waitActiveCost = txn.GetWaitActiveCost()
			}
		}
		opts := []marshalPlanOptions{
			WithWaitActiveCost(waitActiveCost),
			withSchedulingTraceRecorder(&cwft.schedulingTrace),
		}
		handler := NewJsonPlanHandler(ctx, stm, cwft.ses, cwft.plan, phyPlan, opts...)
		if handler.persistSchedulingTrace {
			stm.DisableAgg()
		}
		stm.SetSerializableExecPlan(handler)
	}
	return nil
}

func (cwft *TxnComputationWrapper) SchedulingTrace() schedule.Trace {
	return cwft.schedulingTrace.Snapshot()
}

func (cwft *TxnComputationWrapper) recordSchedulingTraceOnCompileError(ctx context.Context) {
	if cwft.ses == nil {
		return
	}
	traceSnapshot := cwft.schedulingTrace.SnapshotForExport(false)
	if traceSnapshot.Empty() {
		return
	}
	if stm := cwft.ses.GetStmtInfo(); stm != nil {
		stm.DisableAgg()
		stm.SetSerializableExecPlan(newSchedulingTracePlanHandler(ctx, traceSnapshot))
	}
}

// RecordCompoundStmt Check if it is a compound statement, What is a compound statement?
func (cwft *TxnComputationWrapper) RecordCompoundStmt(ctx context.Context, statsBytes statistic.StatsArray) error {
	if stm := cwft.ses.GetStmtInfo(); stm != nil {
		// Check if it is a compound statement, What is a compound statement?
		jsonHandle := &jsonPlanHandler{
			jsonBytes:  sqlQueryIgnoreExecPlan,
			statsBytes: statsBytes,
		}
		stm.SetSerializableExecPlan(jsonHandle)
	}
	return nil
}

// StatsCompositeSubStmtResource returns the legacy plan-statistics projection
// for a composite child statement. The caller owns the returned value; it is
// deliberately not merged into the authoritative resource root.
func (cwft *TxnComputationWrapper) StatsCompositeSubStmtResource(ctx context.Context) (statsByte statistic.StatsArray) {
	waitActiveCost := time.Duration(0)
	if handler := cwft.ses.GetTxnHandler(); handler.InActiveTxn() {
		if txn := handler.GetTxn(); txn != nil {
			waitActiveCost = txn.GetWaitActiveCost()
		}
	}
	h := NewMarshalPlanHandlerCompositeSubStmt(ctx, cwft.plan, WithWaitActiveCost(waitActiveCost))
	statsByte, _ = h.Stats(ctx, cwft.ses)
	return statsByte
}

func (cwft *TxnComputationWrapper) SetExplainBuffer(buf *bytes.Buffer) {
	cwft.explainBuffer = buf
}

func (cwft *TxnComputationWrapper) GetUUID() []byte {
	return cwft.uuid[:]
}

func (cwft *TxnComputationWrapper) Run(ts uint64) (*util2.RunResult, error) {
	runningCompile := cwft.compile
	defer func() {
		runningCompile.Release()
		cwft.compile = nil
	}()

	runResult, err := runningCompile.Run(ts)
	// Sync the latest plan after Run (it may have changed due to retry)
	cwft.plan = runningCompile.GetPlan()
	cwft.runResult = runResult
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

func CheckTableDefChange(catalogCache *cache.CatalogCache, tblKey *cache.TableChangeQuery) bool {
	if catalogCache == nil {
		return false
	}
	return catalogCache.HasNewerVersion(tblKey)
}

func preparePlanNeedsRebuild(schemaChanged, modeMismatch, protocolMismatch bool) bool {
	return schemaChanged || modeMismatch || protocolMismatch
}

func rebuildPreparePlan(
	execCtx *ExecCtx,
	executionSes FeSession,
	prepareStmt *PrepareStmt,
	buildFn func(context.Context, FeSession, plan2.CompilerContext, tree.Statement) (*plan2.Plan, error),
) (*plan2.Plan, error) {
	innerStmt, owned, err := freshPreparedCloneStatement(execCtx.reqCtx, prepareStmt)
	if err != nil {
		return nil, err
	}
	if owned {
		defer innerStmt.Free()
	}
	originPrepareStmt := &tree.PrepareStmt{
		Name: tree.Identifier(prepareStmt.Name),
		Stmt: innerStmt,
	}
	var newPlan *plan2.Plan
	err = execCtx.withRootSQL(prepareStmt.Sql, func() (err error) {
		compilerCtx := executionSes.GetTxnCompileCtx()
		currentDatabase := compilerCtx.GetDatabase()
		compilerCtx.SetDatabase(prepareStmt.defaultDatabase)
		defer compilerCtx.SetDatabase(currentDatabase)
		newPlan, err = buildFn(execCtx.reqCtx, executionSes, compilerCtx, originPrepareStmt)
		return err
	})
	return newPlan, err
}

// initExecuteStmtParam replaces the plan of the EXECUTE by the plan generated by
// the PREPARE and setups the params for the plan.
func initExecuteStmtParam(execCtx *ExecCtx, ses *Session, cwft *TxnComputationWrapper, execPlan *plan.Execute, stmtName string) (*compile.Compile, *plan.Plan, tree.Statement, string, bool, error) {
	return initExecuteStmtParamInSession(execCtx, ses, ses, cwft, execPlan, stmtName)
}

func initExecuteStmtParamInSession(
	execCtx *ExecCtx,
	owner *Session,
	executionSes FeSession,
	cwft *TxnComputationWrapper,
	execPlan *plan.Execute,
	stmtName string,
) (*compile.Compile, *plan.Plan, tree.Statement, string, bool, error) {
	return initExecuteStmtParamWithResolverInSession(
		execCtx,
		owner,
		executionSes,
		cwft,
		execPlan,
		stmtName,
		executionSes.GetTxnCompileCtx().Resolve,
	)
}

type preparedSchemaResolver func(
	databaseName string,
	tableName string,
	snapshot *plan.Snapshot,
) (*plan.ObjectRef, *plan.TableDef, error)

func initExecuteStmtParamWithResolver(
	execCtx *ExecCtx,
	ses *Session,
	cwft *TxnComputationWrapper,
	execPlan *plan.Execute,
	stmtName string,
	resolve preparedSchemaResolver,
) (*compile.Compile, *plan.Plan, tree.Statement, string, bool, error) {
	return initExecuteStmtParamWithResolverInSession(execCtx, ses, ses, cwft, execPlan, stmtName, resolve)
}

func binaryProtocolPrepareParamKind(
	mysqlType defines.MysqlType,
	isUnsigned bool,
	value []byte,
) vector.PrepareParamKind {
	switch mysqlType {
	case defines.MYSQL_TYPE_TINY:
		// The binary protocol has no usable Boolean parameter type. Go's
		// database/sql MySQL driver sends bool values as signed TINY 0/1, while
		// other clients can use TINY for integers. Preserve unsigned and other
		// TINY values as integers and restore the driver's bool values.
		if !isUnsigned && (bytes.Equal(value, []byte("0")) || bytes.Equal(value, []byte("1"))) {
			return vector.PrepareParamBoolean
		}
		return vector.PrepareParamInteger
	case defines.MYSQL_TYPE_SHORT, defines.MYSQL_TYPE_INT24,
		defines.MYSQL_TYPE_LONG, defines.MYSQL_TYPE_LONGLONG, defines.MYSQL_TYPE_BIT,
		defines.MYSQL_TYPE_YEAR:
		return vector.PrepareParamInteger
	case defines.MYSQL_TYPE_FLOAT, defines.MYSQL_TYPE_DOUBLE:
		return vector.PrepareParamFloat
	case defines.MYSQL_TYPE_DECIMAL, defines.MYSQL_TYPE_NEWDECIMAL:
		return vector.PrepareParamDecimal
	default:
		return vector.PrepareParamNone
	}
}

const preparedNumericTextBindingCharset = uint8(255)

const (
	preparedNumericTextPrefix int32 = 2
	preparedNumericTextFloat  int32 = 3
	preparedNumericWide       int32 = 4
	preparedNumericFallback   int32 = 5
	preparedNumericPrefixMax  int32 = 6
	preparedNumericApprox     int32 = 7
	preparedNumericExact      int32 = 8
)

func preparedParamBindingType(kind vector.PrepareParamKind, value []byte) types.Type {
	switch kind {
	case vector.PrepareParamInteger:
		if len(value) > 0 && value[0] == '-' {
			return types.T_int64.ToType()
		}
		return types.T_uint64.ToType()
	case vector.PrepareParamFloat:
		return types.T_float64.ToType()
	case vector.PrepareParamDecimal:
		value = normalizePreparedDecimalPayload(value)
		width, scale := preparedNativeDecimalDomain(value)
		return preparedDecimalBindingType(width, scale, true, false)
	case vector.PrepareParamBoolean:
		return types.T_bool.ToType()
	}

	width, scale, full, exponent := preparedNumericTextDomain(value)
	return preparedDecimalBindingType(width, scale, full, exponent)
}

func preparedDecimalBindingType(width, scale int32, full, exponent bool) types.Type {
	binding := types.T_text.ToType()
	binding.Charset = preparedNumericTextBindingCharset
	integral := max(width-scale, 0)
	switch {
	case full && integral > 76:
		binding.Size = preparedNumericTextFloat
	case integral <= 35:
		binding.Size = preparedNumericTextPrefix
		binding.Width, binding.Scale = 65, 30
	case integral <= 67 && scale <= 9:
		binding.Size = preparedNumericWide
		binding.Width, binding.Scale = 76, 9
	case full && integral > 67 && integral <= 76 && scale <= 9:
		// A fixed exact DECIMAL domain cannot retain both 68+ integral digits
		// and an arbitrary fractional peer. Use an explicit numeric
		// approximation instead of silently falling back to lexical TEXT.
		binding.Size = preparedNumericApprox
	case full && width <= 76:
		// The payload is exactly representable by Decimal256, but does not fit
		// either stable MySQL-compatible envelope above. Retain its physical
		// domain and include width/scale in the prepared generation key.
		binding.Size = preparedNumericExact
		binding.Width, binding.Scale = max(width, 1), scale
	case full:
		// The complete payload is numeric but its combined integral and scale
		// domain exceeds Decimal256. Keep numeric ordering through an explicit
		// approximation instead of making the result depend on lexical spelling.
		binding.Size = preparedNumericTextFloat
	case !full && integral > 76:
		binding.Size = preparedNumericPrefixMax
		binding.Width, binding.Scale = 74, 9
	default:
		binding.Size = preparedNumericFallback
	}
	return binding
}

// normalizePreparedDecimalPayload applies the MySQL numeric lexical rules that
// types.ParseDecimal256 does not yet accept directly. It is allocation-free for
// already canonical payloads, which are the common binary-protocol case.
func normalizePreparedDecimalPayload(value []byte) []byte {
	start := 0
	for start < len(value) && isPreparedNumericSpace(value[start]) {
		start++
	}
	value = value[start:]
	end := preparedDecimalPrefixEnd(value)
	if end == 0 {
		return []byte{'0'}
	}
	normalized := value[:end]
	for i, ch := range normalized {
		if ch == 'E' {
			copyValue := append([]byte(nil), normalized...)
			copyValue[i] = 'e'
			return copyValue
		}
	}
	return normalized
}

func preparedDecimalPrefixEnd(value []byte) int {
	i := 0
	if i < len(value) && (value[i] == '+' || value[i] == '-') {
		i++
	}
	digits := 0
	for i < len(value) && value[i] >= '0' && value[i] <= '9' {
		i++
		digits++
	}
	if i < len(value) && value[i] == '.' {
		i++
		for i < len(value) && value[i] >= '0' && value[i] <= '9' {
			i++
			digits++
		}
	}
	if digits == 0 {
		return 0
	}
	mantissaEnd := i
	if i < len(value) && (value[i] == 'e' || value[i] == 'E') {
		i++
		if i < len(value) && (value[i] == '+' || value[i] == '-') {
			i++
		}
		exponentStart := i
		for i < len(value) && value[i] >= '0' && value[i] <= '9' {
			i++
		}
		if i == exponentStart {
			return mantissaEnd
		}
	}
	return i
}

// preparedNativeDecimalDomain preserves the lexical scale carried by a native
// DECIMAL/NEWDECIMAL payload. Unlike generic text numeric-prefix conversion,
// trailing fractional zeroes are part of the client's exact DECIMAL domain.
func preparedNativeDecimalDomain(value []byte) (width, scale int32) {
	const capDigits = int64(77)
	i := 0
	if i < len(value) && (value[i] == '+' || value[i] == '-') {
		i++
	}
	digits := int64(0)
	firstNonZero := int64(-1)
	for i < len(value) && value[i] >= '0' && value[i] <= '9' {
		if value[i] != '0' && firstNonZero < 0 {
			firstNonZero = digits
		}
		digits = min(digits+1, capDigits)
		i++
	}
	decimalPos := digits
	if i < len(value) && value[i] == '.' {
		i++
		for i < len(value) && value[i] >= '0' && value[i] <= '9' {
			if value[i] != '0' && firstNonZero < 0 {
				firstNonZero = digits
			}
			digits = min(digits+1, capDigits)
			i++
		}
	}
	exponent := int64(0)
	if i < len(value) && (value[i] == 'e' || value[i] == 'E') {
		i++
		negative := false
		if i < len(value) && (value[i] == '+' || value[i] == '-') {
			negative = value[i] == '-'
			i++
		}
		for i < len(value) && value[i] >= '0' && value[i] <= '9' {
			exponent = min(exponent*10+int64(value[i]-'0'), capDigits)
			i++
		}
		if negative {
			exponent = -exponent
		}
	}
	integral := int64(0)
	if firstNonZero >= 0 {
		integral = max(decimalPos+exponent-firstNonZero, 0)
	}
	scale64 := max(digits-decimalPos-exponent, 0)
	return int32(max(min(integral+scale64, capDigits), 1)), int32(min(scale64, capDigits))
}

// preparedNumericTextDomain scans only the numeric prefix and caps all counts
// at 77. It never expands the exponent or allocates proportionally to its
// numeric value.
func preparedNumericTextDomain(value []byte) (width, scale int32, full, exponent bool) {
	const capDigits = int64(77)
	i := 0
	for i < len(value) && isPreparedNumericSpace(value[i]) {
		i++
	}
	if i < len(value) && (value[i] == '+' || value[i] == '-') {
		i++
	}
	decimalPos := int64(0)
	digitPos := int64(0)
	firstNonZero, lastNonZero := int64(-1), int64(-1)
	for i < len(value) && value[i] >= '0' && value[i] <= '9' {
		if value[i] != '0' {
			if firstNonZero < 0 {
				firstNonZero = digitPos
			}
			lastNonZero = digitPos
		}
		digitPos++
		i++
	}
	decimalPos = digitPos
	hasDigits := digitPos > 0
	if i < len(value) && value[i] == '.' {
		i++
		for i < len(value) && value[i] >= '0' && value[i] <= '9' {
			if value[i] != '0' {
				if firstNonZero < 0 {
					firstNonZero = digitPos
				}
				lastNonZero = digitPos
			}
			digitPos++
			i++
		}
		hasDigits = digitPos > 0
	}
	exp := int64(0)
	if hasDigits && i < len(value) && (value[i] == 'e' || value[i] == 'E') {
		exponent = true
		expAt := i
		i++
		negative := false
		if i < len(value) && (value[i] == '+' || value[i] == '-') {
			negative = value[i] == '-'
			i++
		}
		expDigits := 0
		for i < len(value) && value[i] >= '0' && value[i] <= '9' {
			exp = min(exp*10+int64(value[i]-'0'), capDigits)
			expDigits++
			i++
		}
		if expDigits == 0 {
			i = expAt
			exponent = false
			exp = 0
		} else if negative {
			exp = -exp
		}
	}
	for i < len(value) && isPreparedNumericSpace(value[i]) {
		i++
	}
	full = hasDigits && i == len(value)
	if !hasDigits {
		return 1, 0, false, false
	}
	// Zero has no precision domain: spelling it with leading/trailing zeroes or
	// an exponent must not promote an otherwise exact prepared value to DOUBLE.
	if firstNonZero < 0 {
		return 1, 0, full, exponent
	}
	integral := max(decimalPos-firstNonZero+exp, 0)
	scale64 := max(lastNonZero+1-decimalPos-exp, 0)
	width64 := min(integral+scale64, capDigits)
	return int32(max(width64, 1)), int32(min(scale64, capDigits)), full, exponent
}

func isPreparedNumericSpace(ch byte) bool {
	switch ch {
	case ' ', '\t', '\n', '\v', '\f', '\r':
		return true
	default:
		return false
	}
}

func preparedParamBindingTypes(
	params *vector.Vector,
	kinds []vector.PrepareParamKind,
	dependencies []bool,
	count int,
) []types.Type {
	if len(dependencies) == 0 {
		return nil
	}
	var bindingTypes []types.Type
	for i := 0; i < count; i++ {
		if i >= len(dependencies) || !dependencies[i] {
			continue
		}
		var value []byte
		if !params.IsNull(uint64(i)) {
			value = params.GetRawBytesAt(i)
		}
		var kind vector.PrepareParamKind
		if i < len(kinds) {
			kind = kinds[i]
		}
		bindingType := preparedParamBindingType(kind, value)
		if bindingType.Oid == types.T_any {
			continue
		}
		if bindingTypes == nil {
			bindingTypes = make([]types.Type, count)
		}
		bindingTypes[i] = bindingType
	}
	return bindingTypes
}

func preparedParamBindingTypesEqualAtDependencies(left, right []types.Type, dependencies []bool, count int) bool {
	for i := 0; i < count; i++ {
		if i >= len(dependencies) || !dependencies[i] {
			continue
		}
		var leftType, rightType types.Type
		if i < len(left) {
			leftType = left[i]
		}
		if i < len(right) {
			rightType = right[i]
		}
		if !preparedParamBindingCategoryEqual(leftType, rightType) {
			return false
		}
	}
	return true
}

func preparedParamBindingCategoryEqual(left, right types.Type) bool {
	if left.Oid == 0 && isStablePreparedDecimalBinding(right) ||
		right.Oid == 0 && isStablePreparedDecimalBinding(left) {
		return true
	}
	if left.Oid != right.Oid {
		return false
	}
	if left.Oid == types.T_text && left.Charset == preparedNumericTextBindingCharset &&
		right.Charset == preparedNumericTextBindingCharset {
		return left.Size == right.Size &&
			(left.Size != preparedNumericExact || left.Width == right.Width && left.Scale == right.Scale)
	}
	if left.Oid.IsDecimal() && right.Oid.IsDecimal() {
		return true
	}
	return left.Eq(right)
}

func isStablePreparedDecimalBinding(typ types.Type) bool {
	return typ.Oid == types.T_text && typ.Charset == preparedNumericTextBindingCharset &&
		typ.Size == preparedNumericTextPrefix
}

func clonePreparedParamBindingTypes(bindingTypes []types.Type) []types.Type {
	if len(bindingTypes) == 0 {
		return nil
	}
	return append([]types.Type(nil), bindingTypes...)
}

func preparedParamBindingTypesAtDependencies(bindingTypes []types.Type, dependencies []bool) []types.Type {
	if len(bindingTypes) == 0 || len(dependencies) == 0 {
		return nil
	}
	masked := make([]types.Type, len(bindingTypes))
	for i := range bindingTypes {
		if i < len(dependencies) && dependencies[i] {
			masked[i] = bindingTypes[i]
		}
	}
	return masked
}

type preparedExecuteParamState struct {
	params       *vector.Vector
	paramVals    []any
	paramIsBin   []bool
	paramKinds   []vector.PrepareParamKind
	paramTypes   []byte
	bindingTypes []types.Type
	owned        bool
}

func (state *preparedExecuteParamState) bindingTypesFor(dependencies []bool, count int) []types.Type {
	if state == nil || state.params == nil {
		return nil
	}
	bindingTypes := preparedParamBindingTypes(state.params, state.paramKinds, dependencies, count)
	for i := 0; i < count && i*2+1 < len(state.paramTypes); i++ {
		if bindingTypes == nil || i >= len(state.paramKinds) ||
			state.paramKinds[i] != vector.PrepareParamInteger {
			continue
		}
		if state.paramTypes[i*2+1]&0x80 != 0 {
			bindingTypes[i] = types.T_uint64.ToType()
		} else {
			bindingTypes[i] = types.T_int64.ToType()
		}
	}
	return bindingTypes
}

func (state *preparedExecuteParamState) apply(proc *process.Process) {
	if state == nil || state.params == nil {
		return
	}
	if state.owned {
		proc.SetOwnedPrepareParamsWithMeta(state.params, state.paramIsBin, state.paramKinds)
		state.owned = false
		return
	}
	proc.SetPrepareParamsWithMeta(state.params, state.paramIsBin, state.paramKinds)
}

func (state *preparedExecuteParamState) release(proc *process.Process) {
	if state == nil || !state.owned || state.params == nil {
		return
	}
	state.params.Free(proc.Mp())
	state.params = nil
	state.owned = false
}

func initPreparedExecuteParams(
	reqCtx context.Context,
	prepareStmt *PrepareStmt,
	execPlan *plan.Execute,
	cwft *TxnComputationWrapper,
	dependencies []bool,
	numParams int,
) (*preparedExecuteParamState, error) {
	if prepareStmt.params != nil && prepareStmt.params.Length() > 0 { // binary protocol
		if prepareStmt.params.Length() != numParams {
			return nil, moerr.NewInvalidInput(reqCtx, "Incorrect arguments to EXECUTE")
		}
		paramCount := prepareStmt.params.Length()
		var kinds []vector.PrepareParamKind
		for i := 0; i < paramCount && i*2+1 < len(prepareStmt.ParamTypes); i++ {
			mysqlType := defines.MysqlType(prepareStmt.ParamTypes[i*2])
			isUnsigned := prepareStmt.ParamTypes[i*2+1]&0x80 != 0
			kind := binaryProtocolPrepareParamKind(
				mysqlType, isUnsigned, prepareStmt.params.GetRawBytesAt(i))
			if kind != vector.PrepareParamNone {
				if kinds == nil {
					kinds = make([]vector.PrepareParamKind, paramCount)
				}
				kinds[i] = kind
			}
		}
		bindingTypes := preparedParamBindingTypes(prepareStmt.params, kinds, dependencies, numParams)
		for i := 0; i < paramCount && i*2+1 < len(prepareStmt.ParamTypes); i++ {
			if i >= len(kinds) || kinds[i] != vector.PrepareParamInteger || bindingTypes == nil {
				continue
			}
			if prepareStmt.ParamTypes[i*2+1]&0x80 != 0 {
				bindingTypes[i] = types.T_uint64.ToType()
			} else {
				bindingTypes[i] = types.T_int64.ToType()
			}
		}
		return &preparedExecuteParamState{
			params:       prepareStmt.params,
			paramVals:    preparedParamValues(prepareStmt.params, nil),
			paramKinds:   kinds,
			paramTypes:   prepareStmt.ParamTypes,
			bindingTypes: bindingTypes,
		}, nil
	} else if execPlan != nil && len(execPlan.Args) > 0 {
		if len(execPlan.Args) != numParams {
			return nil, moerr.NewInvalidInput(reqCtx, "Incorrect arguments to EXECUTE")
		}
		params, paramVals, paramIsBin, paramKinds, err := buildExecuteUserParams(cwft.proc, execPlan.Args)
		if err != nil {
			return nil, err
		}
		return &preparedExecuteParamState{
			params:       params,
			paramVals:    paramVals,
			paramIsBin:   paramIsBin,
			paramKinds:   paramKinds,
			bindingTypes: preparedParamBindingTypes(params, paramKinds, dependencies, numParams),
			owned:        true,
		}, nil
	} else if numParams > 0 {
		return nil, moerr.NewInvalidInput(reqCtx, "Incorrect arguments to EXECUTE")
	}
	return &preparedExecuteParamState{}, nil
}

func initExecuteStmtParamWithResolverInSession(
	execCtx *ExecCtx,
	owner *Session,
	executionSes FeSession,
	cwft *TxnComputationWrapper,
	execPlan *plan.Execute,
	stmtName string,
	resolve preparedSchemaResolver,
) (*compile.Compile, *plan.Plan, tree.Statement, string, bool, error) {
	reqCtx := execCtx.reqCtx
	if execPlan != nil { // binary protocol, don't have to buildplan, execPlan is nil
		stmtName = execPlan.GetName()
	}
	prepareStmt, err := owner.GetPrepareStmt(reqCtx, stmtName)
	if err != nil {
		return nil, nil, nil, "", false, err
	}
	originSQL := prepareStmt.Sql
	preparePlan := prepareStmt.PreparePlan.GetDcl().GetPrepare()
	if !prepareStmt.paramBindingDependenciesSet {
		prepareStmt.paramBindingDependencies = plan2.PreparedParamCommonTypeDependencies(
			preparePlan.Plan, len(preparePlan.ParamTypes))
		prepareStmt.paramBindingDependenciesSet = true
	}
	paramState, err := initPreparedExecuteParams(
		reqCtx, prepareStmt, execPlan, cwft, prepareStmt.paramBindingDependencies, len(preparePlan.ParamTypes))
	if err != nil {
		return nil, nil, nil, originSQL, false, err
	}
	defer paramState.release(cwft.proc)
	paramBindingTypes := paramState.bindingTypes
	currentNativeMode := owner.sqlModeHasMatrixOneNative()
	currentOnlyFullGroupBy := owner.sqlModeHasOnlyFullGroupBy()

	// TODO check if schema change, obj.Obj is zero all the time in 0.6
	eng := cwft.proc.Base.SessionInfo.StorageEngine
	catalogCache := eng.(*disttae.Engine).GetLatestCatalogCache()

	currentTempTableVersion := owner.GetTempTableVersion()
	currentDDLVersion := owner.getDDLVersion()
	change := prepareStmt.tempTableVersion != currentTempTableVersion ||
		prepareStmt.ddlVersion != currentDDLVersion
	var preparedMetadataTS timestamp.Timestamp
	if catalogCache != nil {
		preparedMetadataTS = catalogCache.GetPreparedMetadataTS()
	}
	validateSubscriptions := preparedSubscriptionsNeedValidation(
		preparedMetadataTS, prepareStmt.Ts, prepareStmt.preparedMetadataCheckTS)
	validateNamedSnapshots := preparedNamedSnapshotsNeedValidation(
		preparePlan.GetSchemas(), preparedMetadataTS, prepareStmt.Ts, prepareStmt.preparedMetadataCheckTS)
	if validateNamedSnapshots {
		change = true
	}
	for _, obj := range preparePlan.GetSchemas() {
		if obj.GetSubscriptionName() != "" && validateSubscriptions {
			subscriptionChanged, err := preparedSubscriptionSchemaChanged(resolve, obj)
			if err != nil {
				return nil, nil, nil, "", false, err
			}
			if subscriptionChanged {
				change = true
				break
			}
		}
		// A historical dependency is immutable at its captured snapshot. Newer
		// versions of the current object must not invalidate that plan.
		if plan2.IsSnapshotValid(obj.GetSnapshot()) {
			continue
		}
		accountId := prepareSchemaAccountID(owner.GetAccountId(), obj)
		tblKey := &cache.TableChangeQuery{
			AccountId:    accountId,
			DatabaseId:   uint64(obj.Db),
			DatabaseName: obj.SchemaName,
			Name:         obj.ObjName,
			Version:      uint32(obj.Server),
			TableId:      uint64(obj.Obj),
			Ts:           prepareStmt.Ts,
		}

		if CheckTableDefChange(catalogCache, tblKey) {
			change = true
			break
		}
	}
	if !change && validateSubscriptions {
		prepareStmt.preparedMetadataCheckTS = preparedMetadataTS
	}

	// These DDL plans cache catalog state that is not represented by a table
	// schema version. CREATE PITR stores account/database/table IDs, while DROP
	// DATABASE validates publication rows. Refresh them on every EXECUTE so a
	// catalog change between PREPARE and EXECUTE cannot bypass validation or
	// persist a stale object ID.
	if preparedDDLNeedsCatalogRefresh(prepareStmt.PrepareStmt) {
		change = true
	}

	// FK-sensitive plans also depend on the current foreign_key_checks session
	// value, which does not invalidate prepared statements. Rebuild them for
	// every EXECUTE so both enabled->disabled and disabled->enabled transitions
	// observe the current setting.
	fkSensitive := shouldRebuildPreparePlan(false, preparePlan.Plan)
	modeMismatch := prepareStmt.NativeMode != currentNativeMode ||
		prepareStmt.onlyFullGroupBySet && prepareStmt.OnlyFullGroupBy != currentOnlyFullGroupBy
	protocolVersion := currentProtocolVersion(cwft.proc)
	protocolMismatch := prepareStmt.protocolVersion != 0 &&
		prepareStmt.protocolVersion != protocolVersion
	paramBindingMismatch := !preparedParamBindingTypesEqualAtDependencies(
		prepareStmt.paramBindingTypes, paramBindingTypes,
		prepareStmt.paramBindingDependencies, len(preparePlan.ParamTypes))
	needRebuild := preparePlanNeedsRebuild(change, modeMismatch, protocolMismatch) ||
		fkSensitive || paramBindingMismatch

	// Rebuild the plan when catalog schema, session temporary-table name
	// resolution, FK-check state, protocol, compatibility mode, or runtime
	// parameter category changed.
	if needRebuild {
		compilerCtx := executionSes.GetTxnCompileCtx()
		rebuildWithBindingTypes := func(bindingTypes []types.Type, dependencies []bool) (*plan.Plan, error) {
			compilerCtx.setPreparedParamBindingTypes(preparedParamBindingTypesAtDependencies(
				bindingTypes, dependencies))
			defer compilerCtx.setPreparedParamBindingTypes(nil)
			return rebuildPreparePlan(execCtx, executionSes, prepareStmt, buildPlan)
		}
		newPlan, err := rebuildWithBindingTypes(paramBindingTypes, prepareStmt.paramBindingDependencies)
		if err != nil {
			return nil, nil, nil, "", false, err
		}
		newPreparePlan := newPlan.GetDcl().GetPrepare()
		newDependencies := plan2.PreparedParamCommonTypeDependencies(
			newPreparePlan.Plan, len(newPreparePlan.ParamTypes))
		convergedBindingTypes := paramState.bindingTypesFor(
			newDependencies, len(newPreparePlan.ParamTypes))
		if !preparedParamBindingTypesEqualAtDependencies(
			paramBindingTypes, convergedBindingTypes, newDependencies, len(newPreparePlan.ParamTypes)) {
			newPlan, err = rebuildWithBindingTypes(convergedBindingTypes, newDependencies)
			if err != nil {
				return nil, nil, nil, "", false, err
			}
			newPreparePlan = newPlan.GetDcl().GetPrepare()
			finalDependencies := plan2.PreparedParamCommonTypeDependencies(
				newPreparePlan.Plan, len(newPreparePlan.ParamTypes))
			finalBindingTypes := paramState.bindingTypesFor(
				finalDependencies, len(newPreparePlan.ParamTypes))
			if !slices.Equal(newDependencies, finalDependencies) ||
				!preparedParamBindingTypesEqualAtDependencies(
					convergedBindingTypes, finalBindingTypes, finalDependencies, len(newPreparePlan.ParamTypes)) {
				return nil, nil, nil, "", false, moerr.NewInternalError(
					reqCtx, "prepared parameter dependencies did not converge after schema rebuild")
			}
			newDependencies = finalDependencies
			convergedBindingTypes = finalBindingTypes
		}
		paramBindingTypes = convergedBindingTypes
		prepareTs := currentTxnSnapshotTSForProcess(cwft.proc)
		var txnHaveDDL bool
		switch prepareStmt.PrepareStmt.(type) {
		case *tree.ExplainStmt, *tree.ExplainAnalyze, *tree.ExplainPhyPlan:
			txnHaveDDL = sessionTxnHaveDDL(executionSes)
		}
		columns := getPreparedResultColumnsFromPlan(
			prepareStmt.PrepareStmt, newPlan, txnHaveDDL)
		resper := execCtx.resper
		if executionSes.IsBackgroundSession() {
			resper = owner.GetResponser()
		}
		newColDefData, err := resper.MysqlRrWr().MakeColumnDefData(reqCtx, columns)
		if err != nil {
			return nil, nil, nil, "", false, err
		}

		preparePlan = newPreparePlan
		prepareStmt.PreparePlan = newPlan
		prepareStmt.exactDecimalParamPositions, err = plan2.ExactDecimalComparisonParamPositions(
			reqCtx,
			preparePlan.Plan,
		)
		if err != nil {
			return nil, nil, nil, "", false, err
		}
		prepareStmt.exactDecimalParamPositions = excludePreparedParamDependencies(
			prepareStmt.exactDecimalParamPositions, newDependencies)
		prepareStmt.exactDecimalComparisonParams = len(prepareStmt.exactDecimalParamPositions) > 0
		prepareStmt.exactDecimalComparisonParamsSet = true
		prepareStmt.ColDefData = newColDefData
		if execCtx.input != nil && execCtx.input.isBinaryProtExecute {
			execCtx.prepareColDef = newColDefData
		}
		prepareStmt.NativeMode = currentNativeMode
		prepareStmt.OnlyFullGroupBy = currentOnlyFullGroupBy
		prepareStmt.onlyFullGroupBySet = true
		prepareStmt.Ts = prepareTs
		prepareStmt.tempTableVersion = currentTempTableVersion
		prepareStmt.ddlVersion = currentDDLVersion
		// The rebuilt plan has incorporated the metadata visible through this
		// high-watermark. A later logtail event will advance it again.
		prepareStmt.preparedMetadataCheckTS = preparedMetadataTS
		prepareStmt.protocolVersion = protocolVersion
		prepareStmt.paramBindingTypes = clonePreparedParamBindingTypes(paramBindingTypes)
		prepareStmt.paramBindingDependencies = newDependencies
		prepareStmt.paramBindingDependenciesSet = true
	}

	// Recreate the cached compile only when a plan dependency changed.
	// Otherwise the cached compile is reused as-is: Compile.Reset clears
	// the per-execution state, including the pipeline edges' terminal state
	// (see Scope.resetForReuse), so reuse is safe and avoids the
	// per-execution recompilation overhead that regressed TPCC. A nil cache
	// means the statement is not eligible for prepare-time compile (e.g. AP
	// query); recompiling would fail with ErrCantCompileForPrepare on every
	// execution, so leave it to the regular compile path (isPrepare=false).
	// See: https://github.com/matrixorigin/matrixone/issues/25614
	if needRebuild && prepareStmt.compile != nil {
		prepareStmt.compile.FreeOperator()
		prepareStmt.compile.SetIsPrepare(false)
		prepareStmt.compile.Release()
		prepareStmt.compile = nil

		executionIntent := querySchedulingIntentForStatementWithSQLMode(
			owner, originSQL, prepareStmt.schedulingSQLMode)
		if !executionSes.IsBackgroundSession() {
			if _, ok := preparePlan.Plan.Plan.(*plan.Plan_Query); ok &&
				!prepareStmt.exactDecimalComparisonParams &&
				shouldCachePrepareCompile(preparePlan.Plan) && !executionIntent.Explicit {
				// Prepare-time compiles are cached and must not retain a statement-owned trace.
				// The execution path attaches the current wrapper trace after cache retrieval.
				comp, err := createCompile(execCtx, executionSes, cwft.proc, originSQL, originSQL, &prepareStmt.schedulingSQLMode, prepareStmt.PrepareStmt, preparePlan.Plan, owner.GetOutputCallback(execCtx), true, nil, nil)
				if err != nil {
					if !moerr.IsMoErrCode(err, moerr.ErrCantCompileForPrepare) {
						return nil, nil, nil, "", false, err
					}
				}
				// do not save ap query now()
				if comp != nil && !comp.IsTpQuery() {
					comp.SetIsPrepare(false)
					comp.Release()
					comp = nil
				}
				prepareStmt.compile = comp
			}
		}
	}
	// Replanning uses the same process and may run internal SQL that replaces
	// its parameter slot. Install this execution's parameters only after the
	// new plan and cached compile generation are complete.
	paramState.apply(cwft.proc)
	cwft.paramVals = paramState.paramVals
	cwft.preparedParamBindingTypes = preparedParamBindingTypesAtDependencies(
		paramBindingTypes, prepareStmt.paramBindingDependencies)
	cwft.proc.SetPreparedParamBindingTypes(cwft.preparedParamBindingTypes)
	executionPlan := preparePlan.Plan
	if !prepareStmt.exactDecimalComparisonParamsSet {
		prepareStmt.exactDecimalParamPositions, err = plan2.ExactDecimalComparisonParamPositions(
			reqCtx,
			executionPlan,
		)
		if err != nil {
			return nil, nil, nil, originSQL, false, err
		}
		prepareStmt.exactDecimalParamPositions = excludePreparedParamDependencies(
			prepareStmt.exactDecimalParamPositions, prepareStmt.paramBindingDependencies)
		prepareStmt.exactDecimalComparisonParams = len(prepareStmt.exactDecimalParamPositions) > 0
		prepareStmt.exactDecimalComparisonParamsSet = true
	}
	if prepareStmt.exactDecimalComparisonParams {
		// Parameter source kinds participate in DECIMAL coercion. Rebuild from
		// the saved AST after installing the runtime values so every derived
		// predicate (including index prefix filters) and every constant vector is
		// produced in one consistent domain. Rewriting an already optimized plan
		// leaves stale index conditions and can mix DECIMAL physical widths.
		compilerCtx := executionSes.GetTxnCompileCtx()
		compilerCtx.setPreparedParamBindingTypes(cwft.preparedParamBindingTypes)
		defer compilerCtx.setPreparedParamBindingTypes(nil)
		var rebuilt *plan2.Plan
		rebuilt, err = rebuildPreparePlan(
			execCtx,
			executionSes,
			prepareStmt,
			func(ctx context.Context, ses FeSession, compilerCtx plan2.CompilerContext, stmt tree.Statement) (*plan2.Plan, error) {
				originalCtx := compilerCtx.GetContext()
				compilerCtx.SetContext(plan2.WithPrepareRuntimeParams(
					originalCtx, prepareStmt.exactDecimalParamPositions...))
				defer compilerCtx.SetContext(originalCtx)
				return buildPlan(
					plan2.WithPrepareRuntimeParams(ctx, prepareStmt.exactDecimalParamPositions...),
					ses,
					compilerCtx,
					stmt,
				)
			},
		)
		if err != nil {
			return nil, nil, nil, originSQL, false, err
		}
		executionPlan = rebuilt.GetDcl().GetPrepare().Plan
		cwft.runtimeDecimalParamPositions = slices.Clone(prepareStmt.exactDecimalParamPositions)
	}
	// A cached prepared Compile already owns a materialized worker topology.
	// Explicit scheduling intent must be evaluated for this execution, so it
	// cannot reuse a topology compiled under the prepare-time defaults. Keep a
	// default cached topology dormant, though: prepared compiles already coexist
	// with other statement compiles on the session process, and it may become
	// reusable if a session-level scheduling override is later cleared.
	cwft.preparedSchedulingSQLMode = prepareStmt.schedulingSQLMode
	cwft.hasPreparedSchedulingSQLMode = true
	cwft.preparedSchedulingSQL = originSQL
	retComp := prepareStmt.compile
	if prepareStmt.exactDecimalComparisonParams {
		retComp = nil
	}
	if executionSes.IsBackgroundSession() {
		// A cached compile owns pipelines tied to the client process used at
		// PREPARE time. A procedure executes with a distinct background process.
		retComp = nil
	}
	if retComp != nil && querySchedulingIntentForStatementWithSQLMode(
		owner, originSQL, prepareStmt.schedulingSQLMode).Explicit {
		retComp = nil
	}
	executionStmt, owned, err := freshPreparedCloneStatement(reqCtx, prepareStmt)
	if err != nil {
		return nil, nil, nil, "", false, err
	}
	return retComp, executionPlan, executionStmt, originSQL, owned, nil
}

func excludePreparedParamDependencies(positions []int32, dependencies []bool) []int32 {
	if len(positions) == 0 || len(dependencies) == 0 {
		return positions
	}
	filtered := positions[:0]
	for _, pos := range positions {
		if pos >= 0 && int(pos) < len(dependencies) && dependencies[pos] {
			continue
		}
		filtered = append(filtered, pos)
	}
	return filtered
}

func prepareSchemaAccountID(currentAccountID uint32, obj *plan.ObjectRef) uint32 {
	if obj.GetPubInfo() != nil {
		return uint32(obj.GetPubInfo().GetTenantId())
	}
	if ShouldSwitchToSysAccount(obj.SchemaName, obj.ObjName) {
		return uint32(sysAccountID)
	}
	return currentAccountID
}

func currentTxnSnapshotTS(ses *Session) timestamp.Timestamp {
	if ses == nil || ses.GetProc() == nil {
		return timestamp.Timestamp{}
	}
	return currentTxnSnapshotTSForProcess(ses.GetProc())
}

func currentTxnSnapshotTSForProcess(proc *process.Process) timestamp.Timestamp {
	if proc == nil {
		return timestamp.Timestamp{}
	}
	txnOperator := proc.GetTxnOperator()
	if txnOperator == nil {
		return timestamp.Timestamp{}
	}
	return txnOperator.SnapshotTS()
}

func preparedSubscriptionsNeedValidation(
	metadataTS timestamp.Timestamp,
	prepareTS timestamp.Timestamp,
	checkedTS timestamp.Timestamp,
) bool {
	return metadataTS.Greater(checkedTS) && metadataTS.Greater(prepareTS)
}

func preparedNamedSnapshotsNeedValidation(
	schemas []*plan.ObjectRef,
	metadataTS timestamp.Timestamp,
	prepareTS timestamp.Timestamp,
	checkedTS timestamp.Timestamp,
) bool {
	if !preparedSubscriptionsNeedValidation(metadataTS, prepareTS, checkedTS) {
		return false
	}
	for _, schema := range schemas {
		if schema.GetSnapshot().GetExtraInfo().GetName() != "" {
			return true
		}
	}
	return false
}

func preparedSubscriptionSchemaChanged(resolve preparedSchemaResolver, expected *plan.ObjectRef) (bool, error) {
	if expected.GetPubInfo() == nil {
		return true, nil
	}
	currentRef, currentDef, err := resolve(
		expected.GetSubscriptionName(),
		expected.GetObjName(),
		nil,
	)
	if err != nil {
		return false, err
	}
	if currentRef == nil || currentDef == nil || currentRef.GetPubInfo() == nil {
		return true, nil
	}
	expectedTenant := expected.GetPubInfo().GetTenantId()
	if plan2.IsSnapshotValid(expected.GetSnapshot()) {
		if currentRef.GetSubscriptionName() != expected.GetSubscriptionName() ||
			currentRef.GetPubInfo().GetTenantId() != expectedTenant {
			return true, nil
		}
		currentRef, currentDef, err = resolve(
			expected.GetSubscriptionName(),
			expected.GetObjName(),
			expected.GetSnapshot(),
		)
		if err != nil {
			return false, err
		}
		if currentRef == nil || currentDef == nil || currentRef.GetPubInfo() == nil {
			return true, nil
		}
	}
	return currentRef.GetSubscriptionName() != expected.GetSubscriptionName() ||
		currentRef.GetPubInfo().GetTenantId() != expectedTenant ||
		currentRef.GetSchemaName() != expected.GetSchemaName() ||
		currentRef.GetObjName() != expected.GetObjName() ||
		currentRef.GetObj() != expected.GetObj() ||
		currentDef.GetDbId() != uint64(expected.GetDb()) ||
		currentDef.GetTblId() != uint64(expected.GetObj()) ||
		currentDef.GetVersion() != uint32(expected.GetServer()), nil
}

func preparedDDLNeedsCatalogRefresh(stmt tree.Statement) bool {
	switch ddl := stmt.(type) {
	case *tree.CreateDatabase:
		return ddl.SubscriptionOption != nil
	case *tree.CreatePitr, *tree.DropDatabase, *tree.CloneTable:
		return true
	default:
		return false
	}
}

func preparedParamValues(params *vector.Vector, paramIsBin []bool) []any {
	if params == nil || params.Length() == 0 {
		return nil
	}
	values := make([]any, params.Length())
	for i := range values {
		if params.IsNull(uint64(i)) {
			continue
		}
		isBin := false
		if i < len(paramIsBin) {
			isBin = paramIsBin[i]
		}
		values[i] = plan2.ParamValue{Value: string(params.GetRawBytesAt(i)), IsBin: isBin}
	}
	return values
}

func buildExecuteUserParams(
	proc *process.Process,
	args []*plan.Expr,
) (
	params *vector.Vector,
	paramVals []any,
	paramIsBin []bool,
	paramKinds []vector.PrepareParamKind,
	err error,
) {
	params = vector.NewVec(types.T_text.ToType())
	defer func() {
		if err != nil {
			params.Free(proc.Mp())
		}
	}()
	paramVals = make([]any, len(args))
	paramIsBin = make([]bool, len(args))
	paramKinds = make([]vector.PrepareParamKind, len(args))
	for i, arg := range args {
		exprImpl := arg.Expr.(*plan.Expr_V)
		var param any
		param, err = proc.GetResolveVariableFunc()(exprImpl.V.Name, exprImpl.V.System, exprImpl.V.Global)
		if err != nil {
			return
		}
		resolveIsBin := proc.GetResolveVariableIsBinFunc()
		if resolveIsBin != nil {
			paramIsBin[i], err = resolveIsBin(exprImpl.V.Name, exprImpl.V.System, exprImpl.V.Global)
			if err != nil {
				return
			}
		}
		resolveKind := proc.GetResolveVariablePrepareParamKindFunc()
		if resolveKind != nil {
			paramKinds[i], err = resolveKind(exprImpl.V.Name, exprImpl.V.System, exprImpl.V.Global)
			if err != nil {
				return
			}
		} else {
			paramKinds[i] = prepareParamKindFromValue(param)
		}
		err = util.AppendAnyToStringVector(proc, param, params)
		if err != nil {
			return
		}
		paramVals[i] = plan2.ParamValue{
			Value:            param,
			IsBin:            paramIsBin[i],
			PrepareParamKind: paramKinds[i],
		}
	}
	return
}

func shouldCachePrepareCompile(p *plan.Plan) bool {
	if p == nil {
		return true
	}
	query := p.GetQuery()
	if query == nil {
		return true
	}
	for _, node := range query.GetNodes() {
		if node != nil && node.GetExternScan() != nil && node.GetExternScan().GetIcebergScan() != nil {
			// Iceberg tasks are resolved from an external snapshot while the
			// pipeline is compiled. That snapshot is not covered by MatrixOne's
			// schema-change timestamp, so a cached Compile would keep scanning the
			// old snapshot across EXECUTE calls. If Iceberg planning moves to an
			// execution-time operator in the future this restriction can be
			// revisited without weakening the generic prepared-statement cache.
			return false
		}
	}
	return !query.GetHasForeignKeyAction()
}

func shouldRebuildPreparePlan(schemaChanged bool, p *plan.Plan) bool {
	if schemaChanged || p == nil {
		return schemaChanged
	}
	query := p.GetQuery()
	return query != nil && query.GetHasForeignKeyAction()
}

func createCompile(
	execCtx *ExecCtx,
	ses FeSession,
	proc *process.Process,
	originSQL string,
	schedulingSQL string,
	schedulingSQLMode *string,
	stmt tree.Statement,
	plan *plan2.Plan,
	fill func(*batch.Batch, *perfcounter.CounterSet) error,
	isPrepare bool,
	schedulingTrace *schedule.TraceRecorder,
	runtimeDecimalParamPositions []int32,
) (retCompile *compile.Compile, err error) {

	addr := currentCNPipelineAddress(ses)
	pu := getPu(ses.GetService())
	proc.ReplaceTopCtx(execCtx.reqCtx)
	proc.Base.FileService = pu.FileService

	var tenant string
	tInfo := ses.GetTenantInfo()
	if tInfo != nil {
		tenant = tInfo.GetTenant()
	}

	stats := statistic.StatsInfoFromContext(execCtx.reqCtx)
	stats.CompileStart()
	var compileIOStart int64
	if stats != nil {
		compileIOStart = atomic.LoadInt64(&stats.IOAccessTimeConsumption)
	}
	crs := new(perfcounter.CounterSet)
	execCtx.reqCtx = perfcounter.AttachCompilePlanMarkKey(execCtx.reqCtx, crs)
	defer func() {
		if stats != nil {
			compileIO := atomic.LoadInt64(&stats.IOAccessTimeConsumption) - compileIOStart
			stats.AddCompileIOConsumption(time.Duration(compileIO))
		}
		stats.AddCompileS3Request(statistic.S3Request{
			List:      crs.FileService.S3.List.Load(),
			Head:      crs.FileService.S3.Head.Load(),
			Put:       crs.FileService.S3.Put.Load(),
			Get:       crs.FileService.S3.Get.Load(),
			Delete:    crs.FileService.S3.Delete.Load(),
			DeleteMul: crs.FileService.S3.DeleteMulti.Load(),
		})
		stats.CompileEnd()
	}()

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
	if schedulingSQL == "" {
		schedulingSQL = originSQL
	}
	if schedulingSQLMode != nil {
		retCompile.SetQuerySchedulingIntent(querySchedulingIntentForStatementWithSQLMode(
			ses, schedulingSQL, *schedulingSQLMode))
	} else {
		retCompile.SetQuerySchedulingIntent(querySchedulingIntentForStatement(ses, schedulingSQL))
	}
	if resourceAttemptOwnerEligible(ses) {
		retCompile.SetResourceAttemptOwnerEligible()
	}
	retCompile.SetSchedulingTraceRecorder(schedulingTrace)
	forcePrepare := execCtx.input.isPreparedExpr()
	retryRuntimePositions := slices.Clone(runtimeDecimalParamPositions)
	retryBindingTypes := clonePreparedParamBindingTypes(proc.GetPreparedParamBindingTypes())
	retCompile.SetBuildPlanFunc(func(ctx context.Context) (*plan2.Plan, error) {
		return buildPlanForCompileRetry(
			ctx, ses, ses.GetTxnCompileCtx(), stmt, forcePrepare, retryRuntimePositions, retryBindingTypes)
	})

	err = retCompile.Compile(execCtx.reqCtx, plan, compileOutputCallback(stmt, fill))
	if err != nil {
		return
	}
	retCompile.SetOriginSQL(originSQL)
	return
}

// EXPLAIN ANALYZE and EXPLAIN PHYPLAN execute the inner query only to collect
// runtime data. Their result rows are constructed by the frontend after the
// pipeline finishes, so inner-query batches must never reach the client output
// callback. Apply the same rule both when compiling a fresh pipeline and when
// resetting a cached prepared pipeline for another execution.
func compileOutputCallback(
	stmt tree.Statement,
	fill func(*batch.Batch, *perfcounter.CounterSet) error,
) func(*batch.Batch, *perfcounter.CounterSet) error {
	switch stmt.(type) {
	case *tree.ExplainAnalyze, *tree.ExplainPhyPlan:
		return func(*batch.Batch, *perfcounter.CounterSet) error { return nil }
	default:
		return fill
	}
}

func buildPlanForCompileRetry(
	ctx context.Context,
	ses FeSession,
	compilerContext plan2.CompilerContext,
	stmt tree.Statement,
	forcePrepare bool,
	runtimeDecimalParamPositions []int32,
	preparedParamBindingTypes []types.Type,
) (*plan2.Plan, error) {
	if txnCtx, ok := compilerContext.(*TxnCompilerContext); ok {
		txnCtx.setPreparedParamBindingTypes(preparedParamBindingTypes)
		defer txnCtx.setPreparedParamBindingTypes(nil)
	}
	if len(runtimeDecimalParamPositions) > 0 {
		ctx = plan2.WithPrepareRuntimeParams(ctx, runtimeDecimalParamPositions...)
		originalCtx := compilerContext.GetContext()
		compilerContext.SetContext(plan2.WithPrepareRuntimeParams(originalCtx, runtimeDecimalParamPositions...))
		defer compilerContext.SetContext(originalCtx)
	}
	// No permission verification is required when retry execute buildPlan.
	retryPlan, err := buildPlanWithPrepareMode(
		ctx, ses, compilerContext, stmt, forcePrepare)
	if err != nil {
		return nil, err
	}
	// Forced SET-expression plans were already normalized from the parser's
	// global one-based ordinals. Generic prepared plans retain the existing
	// compacting normalization path.
	if retryPlan.IsPrepare && !forcePrepare {
		_, _, err = plan2.ResetPreparePlan(compilerContext, retryPlan)
	}
	return retryPlan, err
}

func querySchedulingIntent(ses FeSession) schedule.SchedulingIntent {
	intent := schedule.SchedulingIntent{
		PoolFallback:      schedule.PoolFallbackLegacyCompatible,
		EmptyWorkerPolicy: schedule.EmptyWorkerLocalFallback,
		CurrentCNPolicy:   schedule.CurrentCNAllowed,
		WorkerSet: schedule.WorkerSetPolicy{
			Mode: schedule.WorkerSetAll,
		},
	}
	if ses == nil {
		return intent
	}
	if value, err := ses.GetSessionSysVar(queryMaxWorkers); err == nil {
		var maxWorkers int
		switch value := value.(type) {
		case int64:
			maxWorkers = int(value)
		case uint64:
			maxWorkers = int(value)
		case int:
			maxWorkers = value
		}
		if maxWorkers > 0 {
			intent.Explicit = true
			intent.WorkerSet.Mode = schedule.WorkerSetMax
			intent.WorkerSet.MaxWorkers = maxWorkers
		}
	}
	if value, err := ses.GetSessionSysVar(queryPoolStrict); err == nil {
		if boolType, ok := gSysVarsDefs[queryPoolStrict].Type.(SystemVariableBoolType); ok && boolType.IsTrue(value) {
			intent.Explicit = true
			intent.PoolFallback = schedule.PoolFallbackStrict
			intent.EmptyWorkerPolicy = schedule.EmptyWorkerFail
		}
	}
	return intent
}

// Only the client statement owns retry-attempt cardinality. Back-exec SQL is
// derived work under that statement's resource root and contributes resources,
// but it must not claim the root's single attempt owner.
func resourceAttemptOwnerEligible(ses FeSession) bool {
	_, isBackExec := ses.(*backSession)
	return !isBackExec && !ses.IsDerivedStmt()
}

func currentCNPipelineAddress(ses FeSession) string {
	if ses == nil {
		return ""
	}
	pu := getPu(ses.GetService())
	if len(pu.ClusterNodes) == 0 {
		return ""
	}
	return pu.ClusterNodes[0].Addr
}
