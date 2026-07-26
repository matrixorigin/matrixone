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
	"strconv"
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
	uuid         uuid.UUID
	//holds values of params in the PREPARE
	paramVals []any

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
	cwft.prepareName = ""
	cwft.binaryPrepare = false
	cwft.remapDb = nil
	cwft.schedulingSQL = ""
	cwft.preparedSchedulingSQLMode = ""
	cwft.hasPreparedSchedulingSQLMode = false
	cwft.preparedSchedulingSQL = ""
	cwft.schedulingTrace.Reset()
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
		cwft.plan, err = buildPlan(execCtx.reqCtx, cwft.ses, cwft.ses.GetTxnCompileCtx(), cwft.stmt)
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
		var retComp *compile.Compile
		var plan *plan.Plan
		var stmt tree.Statement
		var sql string
		if isTextProtExecute {
			executePlan := cwft.plan.GetDcl().GetExecute()
			retComp, plan, stmt, sql, err = initExecuteStmtParam(execCtx, cwft.ses.(*Session), cwft, executePlan, executePlan.GetName())
			if err != nil {
				return nil, err
			}
			authStats, err := authenticatePreparedDDLOwnerStatement(execCtx.reqCtx, cwft.ses.(*Session), stmt, plan)
			if err != nil {
				return nil, err
			}
			stats.PermissionAuth.Add(&authStats)
			authStats, err = checkResultQueryPrivilege(cwft.proc, plan, execCtx.reqCtx, cwft.ses.GetService(), cwft.ses.(*Session))
			if err != nil {
				return nil, err
			}
			stats.PermissionAuth.Add(&authStats)

			cwft.plan = plan
			cwft.stmt.Free()
			// reset plan & stmt
			cwft.stmt = stmt
		} else {
			// binary protocol execute
			retComp, plan, stmt, sql, err = initExecuteStmtParam(execCtx, cwft.ses.(*Session), cwft, nil, execCtx.input.stmtName)
			if err != nil {
				return nil, err
			}
			if plan != nil {
				cwft.plan = plan
			}
			if stmt != nil {
				cwft.stmt = stmt
			}
			authStats, err := authenticatePreparedDDLOwnerStatement(execCtx.reqCtx, cwft.ses.(*Session), cwft.stmt, cwft.plan)
			if err != nil {
				return nil, err
			}
			stats.PermissionAuth.Add(&authStats)
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
			if planNeedsRuntimeTypedComparison(cwft.plan, cwft.paramVals, cwft.ses.(*Session)) {
				compilerContext := cwft.ses.GetTxnCompileCtx()
				originalContext := compilerContext.GetContext()
				parameterContext := plan2.AttachPrepareParamValues(originalContext, cwft.paramVals)
				compilerContext.SetContext(parameterContext)
				cwft.plan, err = buildPlan(parameterContext, cwft.ses, compilerContext, cwft.stmt)
				compilerContext.SetContext(originalContext)
				if err != nil {
					return nil, err
				}
			}
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
			if err = retComp.Reset(cwft.proc, getStatementStartAt(execCtx.reqCtx), fill, cwft.ses.GetSql()); err != nil {
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
	return catalogCache.HasNewerVersion(tblKey)
}

func preparePlanNeedsRebuild(schemaChanged, modeMismatch bool) bool {
	return schemaChanged || modeMismatch
}

func rebuildPreparePlan(
	execCtx *ExecCtx,
	ses *Session,
	prepareStmt *PrepareStmt,
	buildFn func(context.Context, FeSession, plan2.CompilerContext, tree.Statement) (*plan2.Plan, error),
) (*plan2.Plan, error) {
	originPrepareStmt := &tree.PrepareStmt{
		Name: tree.Identifier(prepareStmt.Name),
		Stmt: prepareStmt.PrepareStmt,
	}
	var newPlan *plan2.Plan
	err := execCtx.withRootSQL(prepareStmt.Sql, func() (err error) {
		compilerCtx := ses.GetTxnCompileCtx()
		currentDatabase := compilerCtx.GetDatabase()
		compilerCtx.SetDatabase(prepareStmt.defaultDatabase)
		defer compilerCtx.SetDatabase(currentDatabase)
		newPlan, err = buildFn(execCtx.reqCtx, ses, compilerCtx, originPrepareStmt)
		return err
	})
	return newPlan, err
}

// initExecuteStmtParam replaces the plan of the EXECUTE by the plan generated by
// the PREPARE and setups the params for the plan.
func initExecuteStmtParam(execCtx *ExecCtx, ses *Session, cwft *TxnComputationWrapper, execPlan *plan.Execute, stmtName string) (*compile.Compile, *plan.Plan, tree.Statement, string, error) {
	reqCtx := execCtx.reqCtx
	if execPlan != nil { // binary protocol, don't have to buildplan, execPlan is nil
		stmtName = execPlan.GetName()
	}
	prepareStmt, err := ses.GetPrepareStmt(reqCtx, stmtName)
	if err != nil {
		return nil, nil, nil, "", err
	}
	originSQL := prepareStmt.Sql
	preparePlan := prepareStmt.PreparePlan.GetDcl().GetPrepare()
	currentNativeMode := ses.sqlModeHasMatrixOneNative()

	// TODO check if schema change, obj.Obj is zero all the time in 0.6
	eng := ses.proc.Base.SessionInfo.StorageEngine
	catalogCache := eng.(*disttae.Engine).GetLatestCatalogCache()

	currentTempTableVersion := ses.GetTempTableVersion()
	change := prepareStmt.tempTableVersion != currentTempTableVersion
	for _, obj := range preparePlan.GetSchemas() {
		accountId := ses.GetAccountId()
		if ShouldSwitchToSysAccount(obj.SchemaName, obj.ObjName) {
			accountId = uint32(sysAccountID)
		}
		tblKey := &cache.TableChangeQuery{
			AccountId:  accountId,
			DatabaseId: uint64(obj.Db),
			Name:       obj.ObjName,
			Version:    uint32(obj.Server),
			TableId:    uint64(obj.Obj),
			Ts:         prepareStmt.Ts,
		}

		if CheckTableDefChange(catalogCache, tblKey) {
			change = true
			break
		}
	}

	modeMismatch := prepareStmt.NativeMode != currentNativeMode
	needRebuild := preparePlanNeedsRebuild(change, modeMismatch)

	// Rebuild the plan when catalog schema, session temporary-table name
	// resolution, or the session's compatibility mode changed.
	if needRebuild {
		newPlan, err := rebuildPreparePlan(execCtx, ses, prepareStmt, buildPlan)
		if err != nil {
			return nil, nil, nil, "", err
		}
		newPreparePlan := newPlan.GetDcl().GetPrepare()
		columns := plan2.GetResultColumnsFromPlan(newPreparePlan.Plan)
		newColDefData, err := execCtx.resper.MysqlRrWr().MakeColumnDefData(reqCtx, columns)
		if err != nil {
			return nil, nil, nil, "", err
		}

		preparePlan = newPreparePlan
		prepareStmt.PreparePlan = newPlan
		prepareStmt.ColDefData = newColDefData
		if execCtx.input != nil && execCtx.input.isBinaryProtExecute {
			execCtx.prepareColDef = newColDefData
		}
		prepareStmt.NativeMode = currentNativeMode
		prepareStmt.Ts = timestamp.Timestamp{PhysicalTime: time.Now().Unix()}
		prepareStmt.tempTableVersion = currentTempTableVersion
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
			ses, originSQL, prepareStmt.schedulingSQLMode)
		if _, ok := preparePlan.Plan.Plan.(*plan.Plan_Query); ok &&
			shouldCachePrepareCompile(preparePlan.Plan) && !executionIntent.Explicit {
			// Prepare-time compiles are cached and must not retain a statement-owned trace.
			// The execution path attaches the current wrapper trace after cache retrieval.
			comp, err := createCompile(execCtx, ses, ses.proc, originSQL, originSQL, &prepareStmt.schedulingSQLMode, prepareStmt.PrepareStmt, preparePlan.Plan, ses.GetOutputCallback(execCtx), true, nil)
			if err != nil {
				if !moerr.IsMoErrCode(err, moerr.ErrCantCompileForPrepare) {
					return nil, nil, nil, "", err
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
	numParams := len(preparePlan.ParamTypes)
	cwft.paramVals = nil
	if prepareStmt.params != nil && prepareStmt.params.Length() > 0 { // use binary protocol
		if prepareStmt.params.Length() != numParams {
			return nil, nil, nil, originSQL, moerr.NewInvalidInput(reqCtx, "Incorrect arguments to EXECUTE")
		}
		runtimeTypes := make([]types.T, numParams)
		for i := range runtimeTypes {
			runtimeTypes[i] = preparedMysqlParamType(prepareStmt.ParamTypes, i).Oid
		}
		cwft.proc.SetPrepareParamsWithTypes(prepareStmt.params, nil, runtimeTypes)
		cwft.paramVals, err = preparedParamValues(cwft.proc, prepareStmt.ParamTypes)
		if err != nil {
			return nil, nil, nil, originSQL, err
		}
	} else if execPlan != nil && len(execPlan.Args) > 0 {
		if len(execPlan.Args) != numParams {
			return nil, nil, nil, originSQL, moerr.NewInvalidInput(reqCtx, "Incorrect arguments to EXECUTE")
		}
		params, paramVals, paramIsBin, err := buildExecuteUserParams(ses, cwft.proc, execPlan.Args)
		if err != nil {
			return nil, nil, nil, originSQL, err
		}
		runtimeTypes := make([]types.T, len(paramVals))
		for i, value := range paramVals {
			if param, ok := value.(plan2.ParamValue); ok {
				runtimeTypes[i] = param.Typ.Oid
			}
		}
		cwft.proc.SetOwnedPrepareParamsWithTypes(params, paramIsBin, runtimeTypes)
		cwft.paramVals = paramVals
	} else {
		if numParams > 0 {
			return nil, nil, nil, originSQL, moerr.NewInvalidInput(reqCtx, "Incorrect arguments to EXECUTE")
		}
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
	if retComp != nil && querySchedulingIntentForStatementWithSQLMode(
		ses, originSQL, prepareStmt.schedulingSQLMode).Explicit {
		retComp = nil
	}
	return retComp, preparePlan.Plan, prepareStmt.PrepareStmt, originSQL, nil
}

func preparedParamValues(proc *process.Process, paramTypes []byte) ([]any, error) {
	params := proc.GetPrepareParams()
	if params == nil || params.Length() == 0 {
		return nil, nil
	}
	values := make([]any, params.Length())
	for i := range values {
		if params.IsNull(uint64(i)) {
			continue
		}
		raw, err := proc.GetPrepareParamsAt(i)
		if err != nil {
			return nil, err
		}
		value, numericString, err := decodePreparedParamValue(raw, paramTypes, i)
		if err != nil {
			return nil, err
		}
		values[i] = plan2.ParamValue{
			Value:         value,
			IsBin:         proc.GetPrepareParamIsBin(i),
			NumericString: numericString,
			Typ:           preparedMysqlParamType(paramTypes, i),
		}
	}
	return values, nil
}

func preparedMysqlParamType(paramTypes []byte, index int) types.Type {
	if index*2+1 >= len(paramTypes) {
		return types.T_text.ToType()
	}
	mysqlType := defines.MysqlType(paramTypes[index*2])
	unsigned := paramTypes[index*2+1]&0x80 != 0
	switch mysqlType {
	case defines.MYSQL_TYPE_BIT:
		return types.T_bit.ToType()
	case defines.MYSQL_TYPE_TINY, defines.MYSQL_TYPE_SHORT, defines.MYSQL_TYPE_YEAR,
		defines.MYSQL_TYPE_INT24, defines.MYSQL_TYPE_LONG, defines.MYSQL_TYPE_LONGLONG:
		if unsigned {
			return types.T_uint64.ToType()
		}
		return types.T_int64.ToType()
	case defines.MYSQL_TYPE_FLOAT:
		return types.T_float32.ToType()
	case defines.MYSQL_TYPE_DOUBLE:
		return types.T_float64.ToType()
	case defines.MYSQL_TYPE_DECIMAL, defines.MYSQL_TYPE_NEWDECIMAL:
		return types.T_decimal128.ToType()
	default:
		return types.T_text.ToType()
	}
}

func decodePreparedParamValue(raw []byte, paramTypes []byte, index int) (any, bool, error) {
	if index*2+1 >= len(paramTypes) {
		return string(raw), false, nil
	}
	mysqlType := defines.MysqlType(paramTypes[index*2])
	unsigned := paramTypes[index*2+1]&0x80 != 0
	text := string(raw)
	switch mysqlType {
	case defines.MYSQL_TYPE_BIT, defines.MYSQL_TYPE_TINY, defines.MYSQL_TYPE_SHORT,
		defines.MYSQL_TYPE_YEAR, defines.MYSQL_TYPE_INT24, defines.MYSQL_TYPE_LONG,
		defines.MYSQL_TYPE_LONGLONG:
		if unsigned || mysqlType == defines.MYSQL_TYPE_BIT {
			value, err := strconv.ParseUint(text, 10, 64)
			return value, false, err
		}
		value, err := strconv.ParseInt(text, 10, 64)
		return value, false, err
	case defines.MYSQL_TYPE_FLOAT:
		value, err := strconv.ParseFloat(text, 32)
		return float32(value), false, err
	case defines.MYSQL_TYPE_DOUBLE:
		value, err := strconv.ParseFloat(text, 64)
		return value, false, err
	case defines.MYSQL_TYPE_DECIMAL, defines.MYSQL_TYPE_NEWDECIMAL:
		return text, true, nil
	default:
		return text, false, nil
	}
}

func buildExecuteUserParams(
	ses *Session,
	proc *process.Process,
	args []*plan.Expr,
) (params *vector.Vector, paramVals []any, paramIsBin []bool, err error) {
	params = vector.NewVec(types.T_text.ToType())
	defer func() {
		if err != nil {
			params.Free(proc.Mp())
		}
	}()
	paramVals = make([]any, len(args))
	paramIsBin = make([]bool, len(args))
	for i, arg := range args {
		exprImpl := arg.Expr.(*plan.Expr_V)
		var param any
		param, err = proc.GetResolveVariableFunc()(exprImpl.V.Name, exprImpl.V.System, exprImpl.V.Global)
		if err != nil {
			return
		}
		err = util.AppendAnyToStringVector(proc, param, params)
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
		numericString := false
		paramType := inferUserDefinedVarType(param, false)
		if !exprImpl.V.System {
			_, resolvedType, getErr := ses.txnCompileCtx.ResolveVariableWithType(
				exprImpl.V.Name, false, exprImpl.V.Global,
			)
			if getErr == nil {
				paramType = resolvedType
			}
			variable, getErr := ses.GetUserDefinedVar(exprImpl.V.Name)
			if getErr == nil && resolvedType == variable.Typ {
				numericString = variable.NumericString
			}
		}
		paramVals[i] = plan2.ParamValue{
			Value:         param,
			IsBin:         paramIsBin[i],
			NumericString: numericString,
			Typ:           paramType,
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
	projectedParams := projectedRuntimeTypedParams(query)
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
		for _, expr := range preparedRuntimeTypedExprs(node) {
			if hasRuntimeTypedComparison(expr, node, projectedParams) {
				return false
			}
		}
	}
	return !query.GetHasForeignKeyAction()
}

func planNeedsRuntimeTypedComparison(p *plan.Plan, paramVals []any, sessions ...*Session) bool {
	if p == nil || p.GetQuery() == nil {
		return false
	}
	query := p.GetQuery()
	projectedParams := projectedRuntimeTypedParams(query)
	for _, node := range query.GetNodes() {
		for _, expr := range preparedRuntimeTypedExprs(node) {
			if hasRuntimeNumericComparison(expr, node, paramVals, projectedParams, sessions...) {
				return true
			}
		}
	}
	return false
}

func hasRuntimeNumericComparison(
	expr *plan.Expr,
	node *plan.Node,
	paramVals []any,
	projectedParams *projectedParamDependencies,
	sessions ...*Session,
) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	switch fn.Func.ObjName {
	case "=", "<=>", "<", "<=", ">", ">=", "<>", "between", "in_range",
		"+", "-", "*", "/", "%", "mod", "div":
		if containsRuntimeNumericParam(expr, node, paramVals, projectedParams, sessions...) {
			return true
		}
	}
	for _, arg := range fn.Args {
		if hasRuntimeNumericComparison(arg, node, paramVals, projectedParams, sessions...) {
			return true
		}
	}
	return false
}

func containsRuntimeNumericParam(
	expr *plan.Expr,
	node *plan.Node,
	paramVals []any,
	projectedParams *projectedParamDependencies,
	sessions ...*Session,
) bool {
	if expr == nil {
		return false
	}
	if param := expr.GetP(); param != nil {
		pos := int(param.Pos)
		return pos >= 0 && pos < len(paramVals) && isRuntimeNumericParam(paramVals[pos])
	}
	if variable := expr.GetV(); variable != nil && !variable.System && len(sessions) > 0 && sessions[0] != nil {
		userVar, err := sessions[0].GetUserDefinedVar(variable.Name)
		return err == nil && userVar.Typ.IsNumeric()
	}
	if col := expr.GetCol(); col != nil {
		for pos := range projectedParams.columnPositions(node, col) {
			idx := int(pos)
			if idx >= 0 && idx < len(paramVals) && isRuntimeNumericParam(paramVals[idx]) {
				return true
			}
		}
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if containsRuntimeNumericParam(arg, node, paramVals, projectedParams, sessions...) {
				return true
			}
		}
	}
	return false
}

func isRuntimeNumericParam(value any) bool {
	if param, ok := value.(plan2.ParamValue); ok {
		if param.Typ.IsNumeric() || param.NumericString {
			return true
		}
		value = param.Value
	}
	switch value.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return true
	}
	return false
}

type projectedParamDependencies struct {
	outputs map[[2]int32]map[int32]struct{}
}

func projectedRuntimeTypedParams(query *plan.Query) *projectedParamDependencies {
	result := &projectedParamDependencies{
		outputs: make(map[[2]int32]map[int32]struct{}),
	}
	if query == nil {
		return result
	}

	// Optimized plans address a JOIN input column by child ordinal, not by the
	// binder's original binding tag. Propagate dependencies through the node
	// DAG so derived tables, CTEs, and projection chains retain parameter types.
	for changed := true; changed; {
		changed = false
		for _, node := range query.GetNodes() {
			if node == nil {
				continue
			}
			outputExprs := make(map[int32]*plan.Expr, len(node.GetProjectList())+
				len(node.GetAggList())+len(node.GetWinSpecList()))
			for colPos, expr := range node.GetProjectList() {
				outputExprs[int32(colPos)] = expr
			}
			if node.GetNodeType() == plan.Node_AGG {
				for i, expr := range node.GetAggList() {
					outputExprs[int32(len(node.GetGroupBy())+i)] = expr
				}
			}
			if node.GetNodeType() == plan.Node_WINDOW {
				for i, expr := range node.GetWinSpecList() {
					outputExprs[int32(len(node.GetProjectList())+i)] = expr
				}
			}
			for colPos, expr := range outputExprs {
				positions := make(map[int32]struct{})
				result.collectExprPositions(expr, node, positions)
				if len(positions) == 0 {
					continue
				}
				key := [2]int32{node.GetNodeId(), colPos}
				if result.outputs[key] == nil {
					result.outputs[key] = make(map[int32]struct{})
				}
				for pos := range positions {
					if _, ok := result.outputs[key][pos]; !ok {
						result.outputs[key][pos] = struct{}{}
						changed = true
					}
				}
			}
		}
	}
	return result
}

func (d *projectedParamDependencies) collectExprPositions(
	expr *plan.Expr,
	node *plan.Node,
	positions map[int32]struct{},
) {
	if expr == nil {
		return
	}
	if param := expr.GetP(); param != nil {
		positions[param.Pos] = struct{}{}
		return
	}
	if col := expr.GetCol(); col != nil {
		for pos := range d.columnPositions(node, col) {
			positions[pos] = struct{}{}
		}
		return
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			d.collectExprPositions(arg, node, positions)
		}
	}
}

func (d *projectedParamDependencies) columnPositions(
	node *plan.Node,
	col *plan.ColRef,
) map[int32]struct{} {
	if node == nil || col == nil || len(node.GetChildren()) == 0 {
		return nil
	}
	childIndex := int(col.RelPos)
	if childIndex < 0 || childIndex >= len(node.GetChildren()) {
		if len(node.GetChildren()) != 1 {
			return nil
		}
		childIndex = 0
	}
	childID := node.GetChildren()[childIndex]
	return d.outputs[[2]int32{childID, col.ColPos}]
}

func preparedRuntimeTypedExprs(node *plan.Node) []*plan.Expr {
	if node == nil {
		return nil
	}
	exprs := make([]*plan.Expr, 0,
		len(node.GetProjectList())+len(node.GetFilterList())+len(node.GetOnList())+
			len(node.GetGroupBy())+len(node.GetAggList())+len(node.GetWinSpecList()))
	exprs = append(exprs, node.GetProjectList()...)
	exprs = append(exprs, node.GetFilterList()...)
	exprs = append(exprs, node.GetOnList()...)
	exprs = append(exprs, node.GetGroupBy()...)
	exprs = append(exprs, node.GetAggList()...)
	exprs = append(exprs, node.GetWinSpecList()...)
	for _, orderBy := range node.GetOrderBy() {
		exprs = append(exprs, orderBy.GetExpr())
	}
	return exprs
}

func hasRuntimeTypedComparison(
	expr *plan.Expr,
	node *plan.Node,
	projectedParams *projectedParamDependencies,
) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	switch fn.Func.ObjName {
	case "=", "<=>", "<", "<=", ">", ">=", "<>", "between", "in_range",
		"+", "-", "*", "/", "%", "mod", "div":
		if containsPreparedParam(expr, node, projectedParams) {
			return true
		}
	}
	for _, arg := range fn.Args {
		if hasRuntimeTypedComparison(arg, node, projectedParams) {
			return true
		}
	}
	return false
}

func containsPreparedParam(
	expr *plan.Expr,
	node *plan.Node,
	projectedParams *projectedParamDependencies,
) bool {
	if expr == nil {
		return false
	}
	if expr.GetP() != nil {
		return true
	}
	if variable := expr.GetV(); variable != nil && !variable.System {
		return true
	}
	if col := expr.GetCol(); col != nil {
		return len(projectedParams.columnPositions(node, col)) > 0
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if containsPreparedParam(arg, node, projectedParams) {
				return true
			}
		}
	}
	return false
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
	retCompile.SetBuildPlanFunc(func(ctx context.Context) (*plan2.Plan, error) {
		// No permission verification is required when retry execute buildPlan
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
		fill = func(bat *batch.Batch, crs *perfcounter.CounterSet) error { return nil }
	}

	if _, ok := stmt.(*tree.ExplainPhyPlan); ok {
		fill = func(bat *batch.Batch, crs *perfcounter.CounterSet) error { return nil }
	}

	err = retCompile.Compile(execCtx.reqCtx, plan, fill)
	if err != nil {
		return
	}
	retCompile.SetOriginSQL(originSQL)
	return
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
