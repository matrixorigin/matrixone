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
		refreshProcessDivByZeroProfileForPreparedStmt(cwft.proc, stmt)
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
	if catalogCache == nil {
		return false
	}
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
	restorePreparedCloneNames(prepareStmt)
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
	return initExecuteStmtParamWithResolver(
		execCtx,
		ses,
		cwft,
		execPlan,
		stmtName,
		ses.GetTxnCompileCtx().Resolve,
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
) (*compile.Compile, *plan.Plan, tree.Statement, string, error) {
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
	currentDDLVersion := ses.getDDLVersion()
	change := prepareStmt.tempTableVersion != currentTempTableVersion ||
		prepareStmt.ddlVersion != currentDDLVersion
	for _, obj := range preparePlan.GetSchemas() {
		if obj.GetSubscriptionName() != "" {
			subscriptionChanged, err := preparedSubscriptionSchemaChanged(resolve, obj)
			if err != nil {
				return nil, nil, nil, "", err
			}
			if subscriptionChanged {
				change = true
				break
			}
		}
		accountId := prepareSchemaAccountID(ses.GetAccountId(), obj)
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

	// These DDL plans cache catalog state that is not represented by a table
	// schema version. CREATE PITR stores account/database/table IDs, while DROP
	// DATABASE validates publication rows. Refresh them on every EXECUTE so a
	// catalog change between PREPARE and EXECUTE cannot bypass validation or
	// persist a stale object ID.
	if preparedDDLNeedsCatalogRefresh(prepareStmt.PrepareStmt) {
		change = true
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
		prepareTs := currentTxnSnapshotTS(ses)
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
		prepareStmt.Ts = prepareTs
		prepareStmt.tempTableVersion = currentTempTableVersion
		prepareStmt.ddlVersion = currentDDLVersion
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
		cwft.proc.SetPrepareParams(prepareStmt.params)
		cwft.paramVals, err = preparedParamValues(cwft.proc)
		if err != nil {
			return nil, nil, nil, originSQL, err
		}
	} else if execPlan != nil && len(execPlan.Args) > 0 {
		if len(execPlan.Args) != numParams {
			return nil, nil, nil, originSQL, moerr.NewInvalidInput(reqCtx, "Incorrect arguments to EXECUTE")
		}
		params, paramVals, paramIsBin, err := buildExecuteUserParams(cwft.proc, execPlan.Args)
		if err != nil {
			return nil, nil, nil, originSQL, err
		}
		cwft.proc.SetOwnedPrepareParamsWithIsBin(params, paramIsBin)
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
	restorePreparedCloneNames(prepareStmt)
	return retComp, preparePlan.Plan, prepareStmt.PrepareStmt, originSQL, nil
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
	txnOperator := ses.GetProc().GetTxnOperator()
	if txnOperator == nil {
		return timestamp.Timestamp{}
	}
	return txnOperator.SnapshotTS()
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
	return currentRef.GetSubscriptionName() != expected.GetSubscriptionName() ||
		currentRef.GetPubInfo().GetTenantId() != expectedTenant ||
		currentRef.GetSchemaName() != expected.GetSchemaName() ||
		currentRef.GetObjName() != expected.GetObjName() ||
		currentRef.GetObj() != expected.GetObj() ||
		currentDef.GetDbId() != uint64(expected.GetDb()) ||
		currentDef.GetTblId() != uint64(expected.GetObj()) ||
		currentDef.GetVersion() != uint32(expected.GetServer()), nil
}

func restorePreparedCloneNames(prepareStmt *PrepareStmt) {
	if prepareStmt == nil || !prepareStmt.hasCloneSource {
		return
	}
	clone, ok := prepareStmt.PrepareStmt.(*tree.CloneTable)
	if !ok {
		return
	}
	clone.SrcTable.SchemaName = tree.Identifier(prepareStmt.cloneSourceDatabase)
	clone.SrcTable.ObjectName = tree.Identifier(prepareStmt.cloneSourceTable)
	clone.CreateTable.Table.SchemaName = tree.Identifier(prepareStmt.cloneTargetDatabase)
	clone.CreateTable.Table.ObjectName = tree.Identifier(prepareStmt.cloneTargetTable)
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

func preparedParamValues(proc *process.Process) ([]any, error) {
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
		values[i] = plan2.ParamValue{Value: string(raw), IsBin: proc.GetPrepareParamIsBin(i)}
	}
	return values, nil
}

func buildExecuteUserParams(
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
		paramVals[i] = plan2.ParamValue{Value: param, IsBin: paramIsBin[i]}
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
