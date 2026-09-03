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
	"fmt"
	"maps"
	"slices"
	"strings"
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
	PlanGenerationRebuilt() bool
	PlanSnapshotTS() (timestamp.Timestamp, bool)
	Release()
	SetOriginSQL(string)
}

type retiredRuntimeCompile struct {
	owner   *PrepareStmt
	compile *compile.Compile
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
	// runtimeDirectResultSpecialization records that this execution specialized
	// a direct projected binary parameter. Compile retry must replay the same
	// admission without rescanning the prepared plan.
	runtimeDirectResultSpecialization bool
	// runtimeCacheTarget/runtimeCacheKey/runtimeCachePlan stage a candidate
	// specialization outside the live PrepareStmt cache. The candidate is
	// installed only after its Compile succeeds, so a failed replacement leaves
	// the preceding category and Compile intact.
	runtimeCacheTarget          *PrepareStmt
	runtimeCacheKey             string
	runtimeCachePlan            *plan.Plan
	runtimeCacheRetiredCompiles []retiredRuntimeCompile

	explainBuffer *bytes.Buffer
	binaryPrepare bool
	prepareName   string
	// preparedStmt is the cache owner selected for this EXECUTE. It lets Run
	// invalidate a stale prepared physical topology discovered by an internal
	// definition-change retry.
	preparedStmt *PrepareStmt

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

	// protocolVersion and optimizerStatsVersions are captured when the plan is
	// built. The session plan cache uses them instead of values observed later
	// when execution completes.
	protocolVersion        int64
	optimizerStatsVersions map[optimizerStatsTableKey]uint64

	// A reusable logical plan and its generation snapshot are one immutable
	// binding. cachedPlan* identifies the session-cache slot so a definition
	// retry can atomically publish its replacement generation.
	planSnapshotTS       timestamp.Timestamp
	hasPlanSnapshotTS    bool
	planGenerationReused bool
	cachedPlanSQL        string
	cachedPlanIndex      int
	cachedPlanGeneration *plan.Plan
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

func (cwft *TxnComputationWrapper) PlanSnapshotTS() (timestamp.Timestamp, bool) {
	if !cwft.hasPlanSnapshotTS {
		return timestamp.Timestamp{}, false
	}
	return cwft.planSnapshotTS, true
}

func (cwft *TxnComputationWrapper) setPlanSnapshotTS(ts timestamp.Timestamp) {
	cwft.planSnapshotTS = ts
	cwft.hasPlanSnapshotTS = true
}

func (cwft *TxnComputationWrapper) ResetPlanAndStmt(stmt tree.Statement) {
	cwft.plan = nil
	cwft.planSnapshotTS = timestamp.Timestamp{}
	cwft.hasPlanSnapshotTS = false
	cwft.planGenerationReused = false
	cwft.cachedPlanSQL = ""
	cwft.cachedPlanIndex = 0
	cwft.cachedPlanGeneration = nil
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
	cwft.releaseRuntimeCacheRetiredCompiles()
	cwft.plan = nil
	cwft.proc = nil
	cwft.ses = nil
	cwft.compile = nil
	cwft.runResult = nil
	cwft.paramVals = nil
	cwft.runtimeDirectResultSpecialization = false
	cwft.prepareName = ""
	cwft.binaryPrepare = false
	cwft.preparedStmt = nil
	cwft.remapDb = nil
	cwft.schedulingSQL = ""
	cwft.preparedSchedulingSQLMode = ""
	cwft.hasPreparedSchedulingSQLMode = false
	cwft.preparedSchedulingSQL = ""
	cwft.optimizerStatsVersions = nil
	cwft.planSnapshotTS = timestamp.Timestamp{}
	cwft.hasPlanSnapshotTS = false
	cwft.planGenerationReused = false
	cwft.cachedPlanSQL = ""
	cwft.cachedPlanIndex = 0
	cwft.cachedPlanGeneration = nil
	cwft.schedulingTrace.Reset()
}

func (cwft *TxnComputationWrapper) recordOptimizerStatsVersion(key optimizerStatsTableKey, version uint64) {
	if cwft.optimizerStatsVersions == nil {
		cwft.optimizerStatsVersions = make(map[optimizerStatsTableKey]uint64)
	}
	// Keep the first observed version. If publication happens between repeated
	// reads, admission against the newer current version will reject the plan.
	if _, exists := cwft.optimizerStatsVersions[key]; !exists {
		cwft.optimizerStatsVersions[key] = version
	}
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

	var preparedExprRetry *preparedExecutionRetry
	if execCtx.input.isPreparedExpr() {
		preparedExprRetry = newPreparedExecutionRetry(
			execCtx.input.preparedParamVals, execCtx.input.preparedBinaryExecute)
	}
	cacheHit := cwft.plan != nil
	if !cacheHit {
		cwft.protocolVersion = currentProtocolVersion(cwft.proc)
		clear(cwft.optimizerStatsVersions)
		cwft.plan, err = buildPlanWithPrepareMode(
			execCtx.reqCtx,
			cwft.ses,
			cwft.ses.GetTxnCompileCtx(),
			cwft.stmt,
			execCtx.input.isPreparedExpr(),
		)
		if err != nil {
			return nil, err
		}
		if preparedExprRetry != nil {
			runtimePlan, _, specializationErr := plan2.FillValuesOfParamsInPlanWithSpecialization(
				execCtx.reqCtx, cwft.plan, preparedExprRetry.paramVals)
			if specializationErr != nil {
				return nil, specializationErr
			}
			cwft.plan = runtimePlan
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
		cwft.discardRuntimeCacheCandidate()
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
				// Prepared plans are cached across privilege-cache refreshes. Recheck
				// the resolved statement and plan at execution time so a revoke cannot
				// leave an existing PREPARE/EXECUTE handle authorized.
				authStats, err := authenticateUserCanExecutePrepareOrExecute(
					execCtx.reqCtx, owner, stmt, plan, execCtx.effectiveTxnDefaultDatabase,
				)
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
				// Binary prepared execution follows the same execute-time privilege
				// check as text EXECUTE. Do not rely on authorization captured while
				// the statement was prepared.
				authStats, err := authenticateUserCanExecutePrepareOrExecute(
					execCtx.reqCtx, owner, stmt, cwft.plan, execCtx.effectiveTxnDefaultDatabase,
				)
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
			*tree.CreateAccount, *tree.AlterAccount, *tree.DropAccount, *tree.AnalyzeStmt:
			return nil, nil
		}

		if retComp == nil {
			var schedulingSQLMode *string
			if cwft.hasPreparedSchedulingSQLMode {
				schedulingSQLMode = &cwft.preparedSchedulingSQLMode
			}
			preparedRetry := newPreparedExecutionRetry(
				cwft.paramVals,
				execCtx.input != nil && execCtx.input.isBinaryProtExecute,
				cwft.runtimeDirectResultSpecialization,
			)
			var planSnapshotTS *timestamp.Timestamp
			if cwft.preparedStmt != nil {
				planSnapshotTS = &cwft.preparedStmt.Ts
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
				planSnapshotTS,
				cwft.planGenerationReused,
				fill,
				false,
				&cwft.schedulingTrace,
				preparedRetry,
			)
			if err != nil {
				cwft.completeRuntimeCacheCandidate(nil, err)
				return nil, err
			}
			cwft.compile.SetOriginSQL(originSQL)
			if cwft.runtimeCacheTarget != nil {
				runtimeCompile, ok := cwft.compile.(*compile.Compile)
				if !ok || !cwft.completeRuntimeCacheCandidate(runtimeCompile, nil) {
					cwft.discardRuntimeCacheCandidate()
				}
			}
		} else {
			// retComp
			cwft.proc.ReplaceTopCtx(execCtx.reqCtx)
			retComp.SetBuildPlanFunc(preparedExecutionBuildPlanFunc(
				cwft.ses,
				cwft.stmt,
				execCtx.input.isPreparedExpr(),
				newPreparedExecutionRetry(
					cwft.paramVals,
					execCtx.input != nil && execCtx.input.isBinaryProtExecute,
					cwft.runtimeDirectResultSpecialization,
				),
			))
			retComp.SetPlanGenerationReused(cwft.planGenerationReused)
			// originSQL is the prepared statement text here; the wrapper carries
			// the outer EXECUTE fragment, which cannot contain the inner hint.
			retComp.SetQuerySchedulingIntent(cwft.querySchedulingIntentForPreparedStatement(originSQL))
			retComp.SetSchedulingTraceRecorder(&cwft.schedulingTrace)
			if err = retComp.Reset(
				cwft.proc,
				getStatementStartAt(execCtx.reqCtx),
				compileOutputCallback(execCtx, cwft.ses, cwft.stmt, fill),
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
		var planSnapshotTS *timestamp.Timestamp
		if cwft.hasPlanSnapshotTS {
			planSnapshotTS = &cwft.planSnapshotTS
		}
		cwft.compile, err = createCompile(
			execCtx,
			cwft.ses,
			cwft.proc,
			execCtx.sqlOfStmt,
			cwft.schedulingSQLOr(execCtx.sqlOfStmt),
			nil,
			cwft.stmt,
			cwft.plan,
			planSnapshotTS,
			cwft.planGenerationReused,
			fill,
			false,
			&cwft.schedulingTrace,
			preparedExprRetry,
		)
		if err != nil {
			return nil, err
		}
	}

	return cwft.compile, err
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
	cwft.completeCompileExecution(runningCompile, err)
	cwft.runResult = runResult
	return runResult, err
}

// completeCompileExecution publishes or invalidates reusable frontend state at
// the actual terminal owner of Compile.Run. Production executes the returned
// *compile.Compile directly, so executeStmt/executeStmtInBack must call this
// before their one Release; TxnComputationWrapper.Run keeps the same contract
// for direct callers and tests.
func (cwft *TxnComputationWrapper) completeCompileExecution(
	runningCompile Compile,
	runErr error,
) {
	cwft.syncCompileExecution(runningCompile)
	if !runningCompile.PlanGenerationRebuilt() {
		return
	}

	if cwft.preparedStmt != nil {
		invalidatedCompile := cwft.preparedStmt.invalidateCachedCompile()
		if invalidatedCompile != nil && runningCompile != invalidatedCompile {
			invalidatedCompile.Release()
		}
	}
	if cwft.cachedPlanSQL != "" {
		if ses, ok := cwft.ses.(*Session); ok {
			updated := false
			if runErr == nil && cwft.hasPlanSnapshotTS {
				updated = ses.updateCachedPlanGeneration(
					cwft.cachedPlanSQL,
					cwft.cachedPlanIndex,
					cwft.cachedPlanGeneration,
					cwft.plan,
					cwft.planSnapshotTS,
					cwft.optimizerStatsVersions,
				)
			}
			if !updated {
				ses.invalidateCachedPlanGeneration(
					cwft.cachedPlanSQL,
					cwft.cachedPlanIndex,
					cwft.cachedPlanGeneration,
				)
			}
		}
	}
}

func (cwft *TxnComputationWrapper) syncCompileExecution(runningCompile Compile) {
	// Sync the latest plan generation after Run (it may have changed on retry).
	cwft.plan = runningCompile.GetPlan()
	cwft.planSnapshotTS, cwft.hasPlanSnapshotTS = runningCompile.PlanSnapshotTS()
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

func applyBinaryDirectResultDecimalTypes(
	ctx context.Context,
	paramVals []any,
	paramTypes []byte,
	positions []int32,
) error {
	for _, position := range positions {
		if position < 0 || int(position) >= len(paramVals) || int(position)*2+1 >= len(paramTypes) {
			continue
		}
		param, ok := paramVals[position].(plan2.ParamValue)
		if !ok || param.Value == nil {
			continue
		}
		mysqlType := defines.MysqlType(paramTypes[position*2])
		if mysqlType != defines.MYSQL_TYPE_DECIMAL && mysqlType != defines.MYSQL_TYPE_NEWDECIMAL {
			continue
		}
		if !param.HasDirectResultType {
			return invalidBinaryDecimalParameter(ctx, param.Value)
		}
		param.RuntimeType = param.DirectResultType
		param.HasRuntimeType = true
		paramVals[position] = param
	}
	return nil
}

func filterBinaryNumericPrefixCandidates(
	preparePlan *plan2.Plan,
	fixedIntegerPositions []int32,
	paramVals []any,
	paramTypes []byte,
) bool {
	anyRelevant := false
	for i := range paramVals {
		param, ok := paramVals[i].(plan2.ParamValue)
		if !ok {
			continue
		}
		mysqlTypeEligible := i*2+1 < len(paramTypes) &&
			binaryProtocolMayNeedNumericPrefix(defines.MysqlType(paramTypes[i*2]))
		_, fixedInteger := slices.BinarySearch(fixedIntegerPositions, int32(i))
		if !mysqlTypeEligible || fixedInteger {
			// Numeric-prefix admission is position-local. A text-capable marker
			// elsewhere in the plan must not reclassify BLOB values or parameters
			// with a fixed unsigned-integer contract such as LIMIT/OFFSET.
			param.EnableNumericPrefix = false
			param.RetainParamRef = false
			paramVals[i] = param
			continue
		}
		candidates := append([]any(nil), paramVals...)
		for candidatePos, value := range candidates {
			candidate, candidateOK := value.(plan2.ParamValue)
			if candidateOK {
				candidate.EnableNumericPrefix = candidatePos == i
				candidates[candidatePos] = candidate
			}
		}
		relevant := plan2.PreparedPlanNeedsNumericPrefixSpecialization(preparePlan, candidates)
		if !relevant && preparedPositionHasStaticExactNumericPeer(preparePlan, i) && i*2+1 < len(paramTypes) {
			mysqlType := defines.MysqlType(paramTypes[i*2])
			if mysqlType == defines.MYSQL_TYPE_VARCHAR || mysqlType == defines.MYSQL_TYPE_VAR_STRING ||
				mysqlType == defines.MYSQL_TYPE_STRING {
				param.PrepareParamKind = vector.PrepareParamDecimal
				param.RuntimeType = types.T_text.ToType()
				param.HasRuntimeType = true
				candidates[i] = param
				relevant = plan2.PreparedPlanNeedsNumericPrefixSpecialization(preparePlan, candidates)
			}
		}
		param.EnableNumericPrefix = relevant
		param.RetainParamRef = true
		if relevant {
			paramVals[i] = param
			anyRelevant = true
		} else {
			paramVals[i] = param
		}
	}
	return anyRelevant
}

// preparedFixedIntegerParamPositions returns static execute-time metadata for
// one prepared-plan generation. LIMIT/OFFSET and LAG/LEAD offsets have the
// same fixed unsigned-integer contract, so retain one sorted position list for
// all binary execute-time consumers.
func preparedFixedIntegerParamPositions(preparePlan *plan2.Plan) ([]int32, bool, bool) {
	paginationPositions := plan2.PreparedPaginationParamPositions(preparePlan)
	lagLeadPositions := plan2.PreparedLagLeadParamPositions(preparePlan)
	fixedIntegerPositions := append(paginationPositions, lagLeadPositions...)
	slices.Sort(fixedIntegerPositions)
	return fixedIntegerPositions, len(paginationPositions) > 0, len(lagLeadPositions) > 0
}

func (prepareStmt *PrepareStmt) refreshFixedIntegerParamPositions(preparePlan *plan2.Plan) {
	prepareStmt.fixedIntegerParamPositions,
		prepareStmt.hasPaginationParams,
		prepareStmt.hasLagLeadParams = preparedFixedIntegerParamPositions(preparePlan)
}

func preparedPositionHasStaticExactNumericPeer(preparePlan *plan2.Plan, position int) bool {
	found := false
	_ = plan.VisitExpressionsInOwner(preparePlan, func(expr *plan.Expr) error {
		fn := expr.GetF()
		if found || fn == nil {
			return nil
		}
		containsPosition := false
		hasExactEnvelope := false
		hasExactSibling := false
		for _, arg := range fn.Args {
			argContainsPosition := exprContainsPreparedPosition(arg, position)
			if argContainsPosition {
				containsPosition = true
				_ = plan.VisitExprTree(arg, func(candidate *plan.Expr) error {
					candidateType := types.T(candidate.Typ.Id)
					if (candidateType.IsInteger() || candidateType.IsDecimal()) &&
						exprContainsPreparedPosition(candidate, position) {
						hasExactEnvelope = true
					}
					return nil
				})
			} else {
				argType := types.T(arg.Typ.Id)
				hasExactSibling = hasExactSibling || argType.IsInteger() || argType.IsDecimal()
			}
		}
		isPrefixFilter := fn.Func != nil && (fn.Func.ObjName == "prefix_eq" || fn.Func.ObjName == "prefix_in" ||
			fn.Func.ObjName == "prefix_between" || fn.Func.ObjName == "prefix_in_range")
		found = containsPosition && (hasExactEnvelope || (!isPrefixFilter && hasExactSibling))
		return nil
	})
	return found
}

func exprContainsPreparedPosition(expr *plan.Expr, position int) bool {
	found := false
	_ = plan.VisitExprTree(expr, func(candidate *plan.Expr) error {
		if param := candidate.GetP(); param != nil && int(param.Pos) == position {
			found = true
		}
		return nil
	})
	return found
}

func binaryProtocolMayNeedNumericPrefix(mysqlType defines.MysqlType) bool {
	switch mysqlType {
	case defines.MYSQL_TYPE_DECIMAL, defines.MYSQL_TYPE_NEWDECIMAL, defines.MYSQL_TYPE_NULL,
		defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_STRING:
		return true
	default:
		return false
	}
}

func binaryProtocolPrepareParamType(
	mysqlType defines.MysqlType,
	isUnsigned bool,
	value []byte,
) (types.Type, bool) {
	runtimeType, _, _, _, ok := binaryProtocolPrepareParamDomains(mysqlType, isUnsigned, string(value))
	return runtimeType, ok
}

// binaryProtocolPrepareParamCategoryType classifies a packet from protocol
// metadata only. It intentionally does not inspect the value: callers that
// merely choose a text-vs-numeric specialization must not copy or scan a large
// DECIMAL payload before preparedParamValues performs the single exact scan.
func binaryProtocolPrepareParamCategoryType(
	mysqlType defines.MysqlType,
	isUnsigned bool,
) (types.Type, bool) {
	if mysqlType == defines.MYSQL_TYPE_DECIMAL || mysqlType == defines.MYSQL_TYPE_NEWDECIMAL {
		return types.T_decimal256.ToType(), true
	}
	runtimeType, _, _, _, ok := binaryProtocolPrepareParamDomains(mysqlType, isUnsigned, "")
	return runtimeType, ok
}

func binaryProtocolPrepareParamDomains(
	mysqlType defines.MysqlType,
	isUnsigned bool,
	value string,
) (
	runtimeType, directResultType types.Type,
	materializedValue string,
	hasDirectResultType, ok bool,
) {
	signed := func(signedType, unsignedType types.T) types.Type {
		if isUnsigned {
			return unsignedType.ToType()
		}
		return signedType.ToType()
	}
	switch mysqlType {
	case defines.MYSQL_TYPE_TINY:
		return signed(types.T_int8, types.T_uint8), types.Type{}, "", false, true
	case defines.MYSQL_TYPE_SHORT:
		return signed(types.T_int16, types.T_uint16), types.Type{}, "", false, true
	case defines.MYSQL_TYPE_INT24, defines.MYSQL_TYPE_LONG:
		return signed(types.T_int32, types.T_uint32), types.Type{}, "", false, true
	case defines.MYSQL_TYPE_LONGLONG:
		return signed(types.T_int64, types.T_uint64), types.Type{}, "", false, true
	case defines.MYSQL_TYPE_BIT:
		return signed(types.T_bit, types.T_uint64), types.Type{}, "", false, true
	case defines.MYSQL_TYPE_YEAR:
		return types.T_year.ToType(), types.Type{}, "", false, true
	case defines.MYSQL_TYPE_FLOAT:
		return types.T_float32.ToType(), types.Type{}, "", false, true
	case defines.MYSQL_TYPE_DOUBLE:
		return types.T_float64.ToType(), types.Type{}, "", false, true
	case defines.MYSQL_TYPE_DECIMAL, defines.MYSQL_TYPE_NEWDECIMAL:
		normalized, visible, canonical, valid := plan2.PreparedDecimalRuntimeDomains(value)
		return normalized, visible, canonical, valid, valid
	case defines.MYSQL_TYPE_NULL:
		// Keep NULL on the prepared plan's original domain.  The next execute
		// packet may carry a concrete type and will specialize it then.
		return types.Type{}, types.Type{}, "", false, false
	default:
		return types.T_text.ToType(), types.Type{}, "", false, true
	}
}

func invalidBinaryDecimalParameter(ctx context.Context, value any) error {
	length := 0
	switch value := value.(type) {
	case string:
		length = len(value)
	case []byte:
		length = len(value)
	}
	return moerr.NewInvalidInputf(
		ctx, "binary DECIMAL parameter (%d bytes) exceeds DECIMAL(76) or has invalid syntax", length)
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
	cwft.preparedStmt = prepareStmt
	// Carry the binding database through execute-time authorization, lifecycle
	// admission, and ownership cleanup. All three must address the same object.
	execCtx.effectiveTxnDefaultDatabase = prepareStmt.defaultDatabase
	originSQL := prepareStmt.Sql
	preparePlan := prepareStmt.PreparePlan.GetDcl().GetPrepare()
	executionPlan := preparePlan.Plan
	currentNativeMode := owner.sqlModeHasMatrixOneNative()
	currentOnlyFullGroupBy := owner.sqlModeHasOnlyFullGroupBy()
	currentBoolSumAvg := owner.sqlModeHasEnableBoolSumAvg()

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
		prepareStmt.sqlModeFlagsSet && (prepareStmt.OnlyFullGroupBy != currentOnlyFullGroupBy ||
			prepareStmt.BoolSumAvg != currentBoolSumAvg)
	protocolVersion := currentProtocolVersion(cwft.proc)
	protocolMismatch := prepareStmt.protocolVersion != 0 &&
		prepareStmt.protocolVersion != protocolVersion
	needRebuild := prepareStmt.needsRebuild ||
		preparePlanNeedsRebuild(change, modeMismatch, protocolMismatch) || fkSensitive ||
		!reusablePlanGenerationSupported(cwft.proc)
	cwft.planGenerationReused = !needRebuild

	// Rebuild the plan when catalog schema, session temporary-table name
	// resolution, FK-check state, protocol, or compatibility mode changed. The
	// rollout gate also forces a current-transaction plan until every lock owner
	// understands the plan-generation snapshot wire contract.
	if needRebuild {
		newPlan, err := rebuildPreparePlan(execCtx, executionSes, prepareStmt, buildPlan)
		if err != nil {
			return nil, nil, nil, "", false, err
		}
		prepareTs := currentTxnSnapshotTSForProcess(cwft.proc)
		newPreparePlan := newPlan.GetDcl().GetPrepare()
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
		executionPlan = preparePlan.Plan
		prepareStmt.PreparePlan = newPlan
		prepareStmt.directResultParamPositions = plan2.PreparedPlanDirectResultParamPositions(executionPlan)
		prepareStmt.directResultParamPositionsSet = true
		prepareStmt.jsonComparisonParamPositions =
			plan2.PreparedJSONComparisonParamPositions(executionPlan)
		prepareStmt.refreshNumericPrefixConsumer(
			newPreparePlan.Plan, len(newPreparePlan.ParamTypes))
		prepareStmt.numericOverloadParamPositions = plan2.PreparedPlanNumericFallbackParamPositions(
			newPreparePlan.Plan)
		prepareStmt.refreshFixedIntegerParamPositions(newPreparePlan.Plan)
		prepareStmt.ColDefData = newColDefData
		if execCtx.input != nil && execCtx.input.isBinaryProtExecute {
			execCtx.prepareColDef = newColDefData
		}
		prepareStmt.NativeMode = currentNativeMode
		prepareStmt.OnlyFullGroupBy = currentOnlyFullGroupBy
		prepareStmt.BoolSumAvg = currentBoolSumAvg
		prepareStmt.sqlModeFlagsSet = true
		prepareStmt.Ts = prepareTs
		prepareStmt.tempTableVersion = currentTempTableVersion
		prepareStmt.ddlVersion = currentDDLVersion
		// The rebuilt plan has incorporated the metadata visible through this
		// high-watermark. A later logtail event will advance it again.
		prepareStmt.preparedMetadataCheckTS = preparedMetadataTS
		prepareStmt.protocolVersion = protocolVersion
		prepareStmt.needsRebuild = false
	}

	// Recreate the cached compile only when a plan dependency changed or an
	// execution-time retry proved that its physical topology was stale.
	// Otherwise the cached compile is reused as-is: Compile.Reset clears
	// the per-execution state, including the pipeline edges' terminal state
	// (see Scope.resetForReuse), so reuse is safe and avoids the
	// per-execution recompilation overhead that regressed TPCC. A plain nil
	// cache means the statement is not eligible for prepare-time compile (e.g.
	// AP query); compileNeedsRebuild distinguishes a released stale cache that
	// should be recreated once scheduling permits it.
	// See: https://github.com/matrixorigin/matrixone/issues/25614
	if needRebuild {
		prepareStmt.clearRuntimeSpecializationCache()
	}
	if needRebuild && prepareStmt.compile != nil {
		prepareStmt.compile.FreeOperator()
		prepareStmt.compile.SetIsPrepare(false)
		prepareStmt.compile.Release()
		prepareStmt.compile = nil
		prepareStmt.compileNeedsRebuild = true
	}

	if prepareStmt.compileNeedsRebuild {
		executionIntent := querySchedulingIntentForStatementWithSQLMode(
			owner, originSQL, prepareStmt.schedulingSQLMode)
		if !executionSes.IsBackgroundSession() && !executionIntent.Explicit {
			if _, ok := preparePlan.Plan.Plan.(*plan.Plan_Query); ok &&
				shouldCachePrepareCompile(preparePlan.Plan) {
				// Prepare-time compiles are cached and must not retain a statement-owned trace.
				// The execution path attaches the current wrapper trace after cache retrieval.
				comp, err := createCompile(execCtx, executionSes, cwft.proc, originSQL, originSQL, &prepareStmt.schedulingSQLMode, prepareStmt.PrepareStmt, preparePlan.Plan, &prepareStmt.Ts, cwft.planGenerationReused, owner.GetOutputCallback(execCtx), true, nil, nil)
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
			prepareStmt.compileNeedsRebuild = false
		}
	}
	// Decide from the plan shape once per prepared-plan generation. This keeps
	// ordinary write-only statements on their cached compile while allowing
	// domain-sensitive predicates/expressions to be rebound for each binary
	// execution.
	if prepareStmt.runtimeSpecializationPlan != prepareStmt.PreparePlan {
		prepareStmt.runtimeSpecializationNeeded = plan2.PreparedPlanNeedsRuntimeSpecialization(preparePlan.Plan)
		prepareStmt.runtimeSpecializationPlan = prepareStmt.PreparePlan
	}
	needsRuntimeSpecialization := prepareStmt.runtimeSpecializationNeeded ||
		(executionPlan != nil && executionPlan.GetDdl() != nil)
	numParams := len(preparePlan.ParamTypes)
	prepareStmt.refreshNumericPrefixConsumer(executionPlan, numParams)
	binaryExecute := execCtx.input != nil && execCtx.input.isBinaryProtExecute
	binaryLiteralPlan := binaryExecute &&
		(executionPlan.GetDdl() != nil || executionPlan.GetDcl().GetSetVariables() != nil)
	preparedExplain := false
	switch prepareStmt.PrepareStmt.(type) {
	case *tree.ExplainStmt, *tree.ExplainAnalyze, *tree.ExplainPhyPlan:
		preparedExplain = true
	}
	runtimeNumericPrefixCandidate := false
	// The planner records deferred ABS overloads explicitly on the prepared
	// plan.  Carry this bounded metadata into execution instead of walking every
	// expression tree for each EXECUTE.
	runtimeNumericOverloadCandidate := len(prepareStmt.numericOverloadParamPositions) > 0 &&
		executionPlan.GetQuery() != nil
	runtimeDirectResultCandidate := false
	runtimeTextComparisonSpecialization := false
	directResultPositions := prepareStmt.directResultParamPositions
	runtimeDirectResultPositions := make([]int32, 0, len(directResultPositions))
	needsRuntimeParamVals := !binaryExecute || binaryLiteralPlan ||
		prepareStmt.hasPaginationParams || prepareStmt.hasLagLeadParams || preparedExplain ||
		runtimeNumericOverloadCandidate
	cwft.paramVals = nil
	cwft.runtimeDirectResultSpecialization = false
	if prepareStmt.params != nil && prepareStmt.params.Length() > 0 { // use binary protocol
		if prepareStmt.params.Length() != numParams {
			return nil, nil, nil, originSQL, false, moerr.NewInvalidInput(reqCtx, "Incorrect arguments to EXECUTE")
		}
		paramCount := prepareStmt.params.Length()
		if err = prepareStmt.params.SetStringSource(types.StringSourceCOMStmt); err != nil {
			return nil, nil, nil, originSQL, false, err
		}
		runtimeParamTypes := binaryProtocolRuntimeParamTypes(prepareStmt.ParamTypes, prepareStmt.params)
		// A text-comparison rewrite is impossible when every current packet has a
		// numeric (or NULL) domain. Guard the plan walk before invoking it: TPCC
		// binds only numeric parameters and executes this path for every statement.
		if runtimeParamTypesContainText(runtimeParamTypes) &&
			plan2.PreparedPlanNeedsRuntimeTextComparisonSpecialization(executionPlan, runtimeParamTypes) {
			runtimeTextComparisonSpecialization = true
			needsRuntimeSpecialization = true
		}
		if cap(prepareStmt.paramKinds) < paramCount {
			prepareStmt.paramKinds = make([]vector.PrepareParamKind, paramCount)
		} else {
			prepareStmt.paramKinds = prepareStmt.paramKinds[:paramCount]
			clear(prepareStmt.paramKinds)
		}
		hasParamKind := false
		hasConcreteType := false
		if cap(prepareStmt.paramConcreteTypes) < paramCount {
			prepareStmt.paramConcreteTypes = make([]types.T, paramCount)
		} else {
			prepareStmt.paramConcreteTypes = prepareStmt.paramConcreteTypes[:paramCount]
			clear(prepareStmt.paramConcreteTypes)
		}
		directPositionIndex := 0
		for i := 0; i < paramCount && i*2+1 < len(prepareStmt.ParamTypes); i++ {
			mysqlType := defines.MysqlType(prepareStmt.ParamTypes[i*2])
			isUnsigned := prepareStmt.ParamTypes[i*2+1]&0x80 != 0
			kind := binaryProtocolPrepareParamKind(
				mysqlType, isUnsigned, prepareStmt.params.GetRawBytesAt(i))
			prepareStmt.paramKinds[i] = kind
			if _, relevant := slices.BinarySearch(
				prepareStmt.jsonComparisonParamPositions, int32(i)); relevant {
				concreteType := runtimeParamTypes[i].Oid
				if expectedKind, supported := vector.PrepareParamKindForType(concreteType); supported && expectedKind == kind {
					prepareStmt.paramConcreteTypes[i] = concreteType
					hasConcreteType = true
				}
			}
			for directPositionIndex < len(directResultPositions) &&
				directResultPositions[directPositionIndex] < int32(i) {
				directPositionIndex++
			}
			if directPositionIndex < len(directResultPositions) &&
				directResultPositions[directPositionIndex] == int32(i) &&
				kind != vector.PrepareParamNone && !prepareStmt.params.IsNull(uint64(i)) {
				runtimeDirectResultCandidate = true
				runtimeDirectResultPositions = append(runtimeDirectResultPositions, int32(i))
			}
			if binaryProtocolMayNeedNumericPrefix(mysqlType) {
				runtimeNumericPrefixCandidate = runtimeNumericPrefixCandidate || prepareStmt.numericPrefixConsumer
			}
			hasParamKind = hasParamKind || kind != vector.PrepareParamNone
		}
		if hasConcreteType {
			prepareStmt.paramMetadata = cwft.proc.SetPrepareParamsWithReusableTypedMeta(
				prepareStmt.params, nil, prepareStmt.paramKinds,
				prepareStmt.paramConcreteTypes, prepareStmt.paramMetadata)
		} else if hasParamKind {
			prepareStmt.paramMetadata = cwft.proc.SetPrepareParamsWithReusableMeta(
				prepareStmt.params, nil, prepareStmt.paramKinds, prepareStmt.paramMetadata)
		} else {
			cwft.proc.SetPrepareParams(prepareStmt.params)
		}
		needsRuntimeParamVals = needsRuntimeParamVals || needsRuntimeSpecialization ||
			runtimeNumericPrefixCandidate || runtimeNumericOverloadCandidate || runtimeDirectResultCandidate
		if needsRuntimeParamVals {
			cwft.paramVals, err = preparedParamValues(cwft.proc, prepareStmt.ParamTypes)
			if err != nil {
				return nil, nil, nil, originSQL, false, err
			}
			if runtimeDirectResultCandidate {
				if err = applyBinaryDirectResultDecimalTypes(
					reqCtx, cwft.paramVals, prepareStmt.ParamTypes, runtimeDirectResultPositions); err != nil {
					return nil, nil, nil, originSQL, false, err
				}
			}
			if runtimeNumericPrefixCandidate && executionPlan.GetQuery() != nil {
				runtimeNumericPrefixCandidate = filterBinaryNumericPrefixCandidates(
					executionPlan, prepareStmt.fixedIntegerParamPositions,
					cwft.paramVals, prepareStmt.ParamTypes)
			}
			if runtimeDirectResultCandidate && !runtimeNumericPrefixCandidate &&
				!runtimeNumericOverloadCandidate && !needsRuntimeSpecialization {
				restrictPreparedRuntimeTypesToDirectResults(cwft.paramVals, runtimeDirectResultPositions)
			} else if runtimeDirectResultCandidate {
				retainPreparedRuntimeParamRefs(cwft.paramVals)
			}
			cwft.runtimeDirectResultSpecialization = runtimeDirectResultCandidate
		}
		// Text-vs-numeric comparisons use a plan-local DOUBLE conversion and
		// warning semantics. Do not put that plan in the numeric-prefix cache:
		// replacing an older cached compile would release operators that share
		// this execution process before the new plan runs.
		if runtimeTextComparisonSpecialization {
			runtimeNumericPrefixCandidate = false
		}
	} else if execPlan != nil && len(execPlan.Args) > 0 {
		if len(execPlan.Args) != numParams {
			return nil, nil, nil, originSQL, false, moerr.NewInvalidInput(reqCtx, "Incorrect arguments to EXECUTE")
		}
		params, paramVals, paramIsBin, paramKinds, paramTypes, err := buildExecuteUserParams(
			cwft.proc, execPlan.Args, prepareStmt.jsonComparisonParamPositions)
		if err != nil {
			return nil, nil, nil, originSQL, false, err
		}
		if err = params.SetStringSource(types.StringSourceSQLPrepare); err != nil {
			params.Free(cwft.proc.Mp())
			return nil, nil, nil, originSQL, false, err
		}
		if paramTypes != nil {
			cwft.proc.SetOwnedPrepareParamsWithTypedMeta(
				params, paramIsBin, paramKinds, paramTypes)
		} else {
			cwft.proc.SetOwnedPrepareParamsWithMeta(params, paramIsBin, paramKinds)
		}
		cwft.paramVals = paramVals
	} else {
		if numParams > 0 {
			return nil, nil, nil, originSQL, false, moerr.NewInvalidInput(reqCtx, "Incorrect arguments to EXECUTE")
		}
	}
	if !binaryExecute && executionPlan.GetQuery() != nil {
		// SQL EXECUTE values are already decoded as ParamValue.  The prepared
		// plan's cached prefix-consumer bit is sufficient to decide whether the
		// numeric-prefix path can apply; avoid another full plan walk here.
		runtimeNumericPrefixCandidate = prepareStmt.numericPrefixConsumer &&
			preparedParamValuesEnableNumericPrefix(cwft.paramVals)
		if runtimeTypes := preparedRuntimeTextComparisonTypes(cwft.paramVals); runtimeTypes != nil {
			runtimeTextComparisonSpecialization = plan2.PreparedPlanNeedsRuntimeTextComparisonSpecialization(
				executionPlan, runtimeTypes)
		}
	}
	// Static binary specialization, numeric-prefix conversion, and deferred
	// overload binding all materialize every ParamRef in the copied plan. Keep
	// provenance for every parameter before caching so a same-category hit reads
	// the current Process vector instead of retaining the first execution's value.
	// Pagination, window offsets, and EXPLAIN remain value-driven per execution.
	stableRuntimeSpecializationCandidate := binaryExecute &&
		prepareStmt.runtimeSpecializationNeeded && !runtimeTextComparisonSpecialization &&
		!prepareStmt.hasPaginationParams && !prepareStmt.hasLagLeadParams && !preparedExplain
	if runtimeNumericOverloadCandidate || runtimeNumericPrefixCandidate ||
		stableRuntimeSpecializationCandidate {
		retainPreparedRuntimeParamRefs(cwft.paramVals)
	}
	if err := plan2.ValidatePreparedLagLeadParams(reqCtx, preparePlan.Plan, cwft.paramVals); err != nil {
		return nil, nil, nil, originSQL, false, err
	}
	if prepareStmt.hasPaginationParams {
		if err := plan2.ValidatePreparedPaginationParams(reqCtx, preparePlan.Plan, cwft.paramVals); err != nil {
			return nil, nil, nil, originSQL, false, err
		}
	}
	if prepareStmt.hasPaginationParams || prepareStmt.hasLagLeadParams {
		if err := normalizePreparedOffsetBooleans(
			cwft.proc, prepareStmt.fixedIntegerParamPositions, cwft.paramVals); err != nil {
			return nil, nil, nil, originSQL, false, err
		}
	}

	// Static binary specialization, numeric-prefix consumers, and direct numeric
	// result markers enter the same bounded one-category cache. The copied plan
	// keeps typed ParamRefs, so values in a stable runtime domain reuse its compile
	// while a domain switch replaces at most one old category.
	runtimePlan, runtimeSpecialized, runtimePlanApplied := executionPlan, false, false
	var cachedRuntimeCompile *compile.Compile
	runtimeCacheKey := ""
	runtimeCategoryCandidate := runtimeNumericPrefixCandidate || runtimeNumericOverloadCandidate ||
		stableRuntimeSpecializationCandidate
	runtimeSpecializationCandidate := runtimeCategoryCandidate || runtimeDirectResultCandidate
	cacheableRuntimeQuery := executionPlan.GetQuery() != nil && !runtimeTextComparisonSpecialization &&
		(runtimeDirectResultCandidate ||
			(runtimeCategoryCandidate && preparedRuntimeCacheSupports(cwft.paramVals)))
	if cacheableRuntimeQuery {
		if runtimeCategoryCandidate {
			runtimeCacheKey = preparedRuntimeSemanticKey(cwft.paramVals)
		} else {
			runtimeCacheKey = preparedDirectResultSemanticKey(cwft.paramVals, runtimeDirectResultPositions)
		}
		if runtimeCacheKey != "" && runtimeCacheKey == prepareStmt.runtimeSpecializationKey &&
			prepareStmt.runtimePlan != nil && prepareStmt.runtimeCompile != nil {
			runtimePlan = prepareStmt.runtimePlan
			runtimePlanApplied = true
			cachedRuntimeCompile = prepareStmt.runtimeCompile
		}
	}
	if cachedRuntimeCompile == nil &&
		(!binaryExecute || runtimeSpecializationCandidate || binaryLiteralPlan ||
			prepareStmt.hasPaginationParams || needsRuntimeSpecialization) {
		var laterRuntimeSpecialized bool
		runtimePlan, laterRuntimeSpecialized, runtimePlanApplied, err = specializePreparedExecutionPlan(
			reqCtx, executionPlan, cwft.paramVals, binaryExecute,
			runtimeNumericOverloadCandidate, runtimeDirectResultCandidate, needsRuntimeSpecialization,
			runtimeDirectResultPositions, true, runtimeTextComparisonSpecialization)
		runtimeSpecialized = runtimeSpecialized || laterRuntimeSpecialized
		if err == nil && cacheableRuntimeQuery && laterRuntimeSpecialized && runtimePlanApplied {
			err = plan2.RestorePreparedRuntimeParamRefs(reqCtx, runtimePlan)
			if err == nil {
				cwft.runtimeCacheTarget = prepareStmt
				cwft.runtimeCacheKey = runtimeCacheKey
				cwft.runtimeCachePlan = runtimePlan
			}
		}
	}
	if err != nil {
		return nil, nil, nil, originSQL, false, err
	}
	if runtimePlanApplied {
		executionPlan = runtimePlan
		if binaryExecute {
			columns := getPreparedResultColumnsFor(
				prepareStmt.PrepareStmt, runtimePlan, sessionTxnHaveDDL(executionSes))
			resper := execCtx.resper
			if executionSes.IsBackgroundSession() {
				resper = owner.GetResponser()
			}
			colDefData, metadataErr := resper.MysqlRrWr().MakeColumnDefData(reqCtx, columns)
			if metadataErr != nil {
				return nil, nil, nil, originSQL, false, metadataErr
			}
			execCtx.prepareColDef = colDefData
		}
	}

	// A cached prepared Compile already owns a materialized worker topology.
	// Explicit scheduling or Sirius intent must be evaluated for this execution,
	// so neither can reuse a native topology compiled under prepare-time defaults.
	// Keep that cached topology dormant: prepared compiles already coexist with
	// other statement compiles on the session process, and the ordinary scheduling
	// cache may become reusable if a session-level override is later cleared.
	cwft.preparedSchedulingSQLMode = prepareStmt.schedulingSQLMode
	cwft.hasPreparedSchedulingSQLMode = true
	cwft.preparedSchedulingSQL = originSQL
	retComp := prepareStmt.compile
	if cachedRuntimeCompile != nil {
		retComp = cachedRuntimeCompile
	} else if runtimeSpecialized || prepareStmt.hasPaginationParams {
		// The cached compile was built from the prepare-time parameter types and
		// cannot execute a plan whose overloads, result metadata, or pagination
		// values must be rebound for this execution.
		retComp = nil
	}
	if executionSes.IsBackgroundSession() {
		// A cached compile owns pipelines tied to the client process used at
		// PREPARE time. A procedure executes with a distinct background process.
		retComp = nil
	}
	if retComp != nil {
		executionIntent := querySchedulingIntentForStatementWithSQLMode(
			owner, originSQL, prepareStmt.schedulingSQLMode)
		if executionIntent.Explicit || siriusStatementSelected(originSQL, prepareStmt.PrepareStmt) {
			retComp = nil
		}
	}
	executionStmt, owned, err := freshPreparedCloneStatement(reqCtx, prepareStmt)
	if err != nil {
		return nil, nil, nil, "", false, err
	}
	return retComp, executionPlan, executionStmt, originSQL, owned, nil
}

func (cwft *TxnComputationWrapper) discardRuntimeCacheCandidate() {
	cwft.runtimeCacheTarget = nil
	cwft.runtimeCacheKey = ""
	cwft.runtimeCachePlan = nil
}

func (cwft *TxnComputationWrapper) completeRuntimeCacheCandidate(
	runtimeCompile *compile.Compile,
	compileErr error,
) bool {
	if compileErr != nil {
		cwft.discardRuntimeCacheCandidate()
		return false
	}
	return cwft.installRuntimeCacheCandidate(runtimeCompile)
}

func (cwft *TxnComputationWrapper) installRuntimeCacheCandidate(runtimeCompile *compile.Compile) bool {
	if cwft.runtimeCacheTarget == nil || cwft.runtimeCacheKey == "" ||
		cwft.runtimeCachePlan == nil || runtimeCompile == nil {
		return false
	}
	retiredCompile := cwft.runtimeCacheTarget.installRuntimeSpecializationCache(
		cwft.runtimeCacheKey, cwft.runtimeCachePlan, runtimeCompile)
	if retiredCompile != nil {
		// NewCompile has already installed runtimeCompile's execution state on the
		// shared session Process. Releasing the displaced compile here would call
		// Process.Free and erase that state before runtimeCompile can run. Keep the
		// old topology alive until this statement wrapper has fully finished.
		cwft.runtimeCacheRetiredCompiles = append(cwft.runtimeCacheRetiredCompiles, retiredRuntimeCompile{
			owner: cwft.runtimeCacheTarget, compile: retiredCompile,
		})
	}
	cwft.discardRuntimeCacheCandidate()
	return true
}

func (cwft *TxnComputationWrapper) releaseRuntimeCacheRetiredCompiles() {
	for _, retired := range cwft.runtimeCacheRetiredCompiles {
		retired.owner.releaseRuntimeCompile(retired.compile)
	}
	cwft.runtimeCacheRetiredCompiles = nil
}

func retainPreparedRuntimeParamRefs(paramVals []any) {
	for i, value := range paramVals {
		param, ok := value.(plan2.ParamValue)
		if !ok {
			continue
		}
		param.RetainParamRef = true
		paramVals[i] = param
	}
}

func preparedParamValuesEnableNumericPrefix(paramVals []any) bool {
	for _, value := range paramVals {
		param, ok := value.(plan2.ParamValue)
		if ok && param.EnableNumericPrefix {
			return true
		}
	}
	return false
}

func restrictPreparedRuntimeTypesToDirectResults(paramVals []any, positions []int32) {
	positionIndex := 0
	for i, value := range paramVals {
		for positionIndex < len(positions) && positions[positionIndex] < int32(i) {
			positionIndex++
		}
		direct := positionIndex < len(positions) && positions[positionIndex] == int32(i)
		param, ok := value.(plan2.ParamValue)
		if !ok {
			continue
		}
		param.RetainParamRef = true
		if !direct {
			// The process still carries protocol source-kind metadata. Suppress it
			// only in the isolated plan rewrite so an unrelated numeric marker in
			// ABS(?) or another expression cannot expand direct-result admission.
			param.IsBinaryProtocol = false
			param.RuntimeType = types.Type{}
			param.HasRuntimeType = false
			param.EnableNumericPrefix = false
		}
		paramVals[i] = param
	}
}

// preparedDirectResultRuntimePositions returns direct-result positions whose
// execute packet carries a concrete runtime domain. A prepared plan may expose
// several markers directly (for example, a numeric result beside a text
// result), but only concrete numeric packet domains need execute-time
// rebinding. Leaving the other direct markers as ParamRefs preserves their
// prepare-time charset/collation and avoids invalidating the cached compile
// for an unrelated sibling.
func preparedDirectResultRuntimePositions(paramVals []any, positions []int32) []int32 {
	if len(paramVals) == 0 || len(positions) == 0 {
		return nil
	}
	result := make([]int32, 0, len(positions))
	for _, position := range positions {
		if position < 0 || int(position) >= len(paramVals) {
			continue
		}
		param, ok := paramVals[position].(plan2.ParamValue)
		if !ok || param.Value == nil || !param.HasRuntimeType {
			continue
		}
		result = append(result, position)
	}
	return result
}

func preparedDirectResultSemanticKey(paramVals []any, positions []int32) string {
	if len(paramVals) == 0 || len(positions) == 0 {
		return ""
	}
	var key strings.Builder
	key.WriteString("direct;")
	for _, position := range positions {
		if position < 0 || int(position) >= len(paramVals) {
			return ""
		}
		param, ok := paramVals[position].(plan2.ParamValue)
		if !ok {
			return ""
		}
		runtimeType := types.T_text.ToType()
		if param.HasRuntimeType {
			runtimeType = param.RuntimeType
		}
		fmt.Fprintf(&key, "%d:%d:%d:%d:%d;", position, param.PrepareParamKind,
			runtimeType.Oid, runtimeType.Width, runtimeType.Scale)
	}
	return key.String()
}

func preparedRuntimeCacheSupports(paramVals []any) bool {
	if len(paramVals) == 0 {
		return false
	}
	for _, value := range paramVals {
		param, ok := value.(plan2.ParamValue)
		if !ok || (param.Value == nil && !param.HasRuntimeType) {
			// NULL has no stable physical category and the compile setup may
			// detach its empty parameter vector. Rebuild this rare category.
			return false
		}
	}
	return true
}

func preparedRuntimeSemanticKey(paramVals []any) string {
	if len(paramVals) == 0 {
		return ""
	}
	var key strings.Builder
	for i, value := range paramVals {
		param, ok := value.(plan2.ParamValue)
		if !ok {
			return ""
		}
		runtimeType := param.RuntimeType
		if !param.HasRuntimeType || runtimeType.Oid == types.T_text {
			runtimeType = plan2.PreparedNumericPrefixTypeFromString(fmt.Sprintf("%v", param.Value))
		}
		fmt.Fprintf(&key, "%d:%d:%d:%d:%d;", i, param.PrepareParamKind,
			runtimeType.Oid, runtimeType.Width, runtimeType.Scale)
		if param.HasSourceType {
			// SQL EXECUTE arithmetic specializes from the user variable's logical
			// type. Keep that dependency in the cache identity without replacing
			// the value-derived domain above: comparison specialization still
			// relies on the latter to separate values such as 200 and 10.
			fmt.Fprintf(&key, "source:%d:%d:%d;",
				param.SourceType.Oid, param.SourceType.Width, param.SourceType.Scale)
		}
	}
	return key.String()
}

func preparedPlanHasNumericPrefixConsumer(preparePlan *plan2.Plan, paramCount int) bool {
	return preparePlan != nil && preparePlan.GetQuery() != nil && paramCount > 0 &&
		preparedPlanHasStaticExactNumericPeer(preparePlan) &&
		preparedPlanAdmitsPotentialDecimal(preparePlan, paramCount)
}

func (prepareStmt *PrepareStmt) refreshNumericPrefixConsumer(
	preparePlan *plan2.Plan,
	paramCount int,
) {
	if preparePlan == nil {
		prepareStmt.numericPrefixConsumerPlan = nil
		prepareStmt.numericPrefixConsumer = false
		return
	}
	if prepareStmt.numericPrefixConsumerPlan == preparePlan {
		return
	}
	prepareStmt.numericPrefixConsumer = preparedPlanHasNumericPrefixConsumer(preparePlan, paramCount)
	prepareStmt.numericPrefixConsumerPlan = preparePlan
}

func preparedPlanAdmitsPotentialDecimal(preparePlan *plan2.Plan, paramCount int) bool {
	values := make([]any, paramCount)
	for candidate := 0; candidate < paramCount; candidate++ {
		for i := range values {
			values[i] = plan2.ParamValue{EnableNumericPrefix: true}
		}
		values[candidate] = plan2.ParamValue{
			Value:               "0.0",
			PrepareParamKind:    vector.PrepareParamDecimal,
			RuntimeType:         types.New(types.T_decimal64, 2, 1),
			HasRuntimeType:      true,
			EnableNumericPrefix: true,
		}
		if plan2.PreparedPlanNeedsNumericPrefixSpecialization(preparePlan, values) {
			return true
		}
	}
	return false
}

func preparedPlanHasStaticExactNumericPeer(preparePlan *plan2.Plan) bool {
	found := false
	_ = plan.VisitExpressionsInOwner(preparePlan, func(expr *plan.Expr) error {
		fn := expr.GetF()
		if found || fn == nil {
			return nil
		}
		hasParam := false
		hasStaticExact := false
		for _, arg := range fn.Args {
			argHasParam := false
			argHasStaticExact := false
			_ = plan.VisitExprTree(arg, func(candidate *plan.Expr) error {
				argHasParam = argHasParam || candidate.GetP() != nil
				candidateType := types.T(candidate.Typ.Id)
				isStaticValue := candidate.GetCol() != nil ||
					(candidate.GetLit() != nil && candidate.GetLit().GetSrc() == nil)
				argHasStaticExact = argHasStaticExact || (isStaticValue &&
					(candidateType.IsInteger() || candidateType.IsDecimal() || candidateType == types.T_bit))
				return nil
			})
			hasParam = hasParam || argHasParam
			hasStaticExact = hasStaticExact || argHasStaticExact
		}
		isPrefixFilter := fn.Func != nil && (fn.Func.ObjName == "prefix_eq" || fn.Func.ObjName == "prefix_in" ||
			fn.Func.ObjName == "prefix_between" || fn.Func.ObjName == "prefix_in_range")
		found = hasParam && (hasStaticExact || isPrefixFilter)
		return nil
	})
	return found
}

func specializePreparedExecutionPlan(
	ctx context.Context,
	executionPlan *plan2.Plan,
	paramVals []any,
	binaryExecute bool,
	forceNumericOverload bool,
	directResultSpecialization bool,
	forceSpecialization bool,
	directResultPositions []int32,
	textComparisonChecked bool,
	needsTextComparison bool,
) (*plan2.Plan, bool, bool, error) {
	if len(paramVals) == 0 || executionPlan == nil ||
		(executionPlan.GetQuery() == nil && executionPlan.GetDdl() == nil &&
			executionPlan.GetDcl().GetSetVariables() == nil) {
		return executionPlan, false, false, nil
	}
	binaryLiteralPlan := binaryExecute &&
		(executionPlan.GetDdl() != nil || executionPlan.GetDcl().GetSetVariables() != nil)
	needsNumericPrefix := !forceNumericOverload &&
		plan2.PreparedPlanNeedsNumericPrefixSpecialization(executionPlan, paramVals)
	needsRuntimeSpecialization := forceSpecialization ||
		plan2.PreparedPlanNeedsRuntimeSpecialization(executionPlan)
	if !textComparisonChecked {
		if runtimeTypes := preparedRuntimeTextComparisonTypes(paramVals); runtimeTypes != nil {
			needsTextComparison = plan2.PreparedPlanNeedsRuntimeTextComparisonSpecialization(
				executionPlan, runtimeTypes)
		}
	}
	if !forceNumericOverload && !needsNumericPrefix && !directResultSpecialization && !binaryLiteralPlan &&
		!plan2.PreparedPlanHasPaginationParams(executionPlan) && !needsRuntimeSpecialization &&
		!needsTextComparison {
		return executionPlan, false, false, nil
	}

	var runtimePlan *plan2.Plan
	var specialized bool
	var err error
	if forceNumericOverload {
		runtimePlan, specialized, err = plan2.FillValuesOfParamsInPlanWithPreparedNumericOverload(
			ctx, executionPlan, paramVals)
	} else if directResultSpecialization && !needsNumericPrefix && !forceSpecialization {
		positions := plan2.PreparedPlanDirectResultParamPositions(executionPlan)
		if directResultPositions != nil {
			positions = directResultPositions
		}
		positions = preparedDirectResultRuntimePositions(paramVals, positions)
		runtimePlan, specialized, err = plan2.FillValuesOfParamsInPlanWithSpecializationAtPositions(
			ctx, executionPlan, paramVals, positions)
	} else {
		runtimePlan, specialized, err = plan2.FillValuesOfParamsInPlanWithSpecializationPreservingDMLWrites(
			ctx, executionPlan, paramVals)
	}
	if err != nil {
		return nil, false, false, err
	}
	// Binary DDL and SET plans need literal materialization even when no
	// overload or result domain changed. Query plans use the copy only when
	// specialization changed execution semantics.
	if runtimePlan != nil && (specialized || binaryLiteralPlan) {
		return runtimePlan, specialized, true, nil
	}
	return executionPlan, false, false, nil
}

func preparedRuntimeTextComparisonTypes(paramVals []any) []types.Type {
	var runtimeTypes []types.Type
	for i, value := range paramVals {
		param, ok := value.(plan2.ParamValue)
		if !ok {
			continue
		}
		var runtimeType types.Type
		if param.IsBinaryProtocol {
			if param.HasRuntimeType {
				switch param.RuntimeType.Oid {
				case types.T_char, types.T_varchar, types.T_text:
					runtimeType = param.RuntimeType
				}
			} else if param.Value != nil {
				runtimeType = types.T_text.ToType()
			}
		} else if param.HasSourceType {
			switch param.SourceType.Oid {
			case types.T_char, types.T_varchar, types.T_text:
				runtimeType = types.T_text.ToType()
			}
		}
		if runtimeType.Oid != types.T_any {
			if runtimeTypes == nil {
				runtimeTypes = make([]types.Type, len(paramVals))
			}
			runtimeTypes[i] = runtimeType
		}
	}
	return runtimeTypes
}

// preparedExecutionRetry is an immutable snapshot of the execution-time
// binding needed when Compile rebuilds a plan after a definition change. The
// slice must not alias TxnComputationWrapper.paramVals because that field is
// replaced on the next execution of the cached prepared statement.
type preparedExecutionRetry struct {
	paramVals                  []any
	binaryExecute              bool
	directResultSpecialization bool
}

func newPreparedExecutionRetry(
	paramVals []any,
	binaryExecute bool,
	directResultSpecialization ...bool,
) *preparedExecutionRetry {
	if len(paramVals) == 0 {
		return nil
	}
	retry := &preparedExecutionRetry{
		paramVals:     append([]any(nil), paramVals...),
		binaryExecute: binaryExecute,
	}
	if len(directResultSpecialization) > 0 {
		retry.directResultSpecialization = directResultSpecialization[0]
	}
	return retry
}

func normalizePreparedOffsetBooleans(proc *process.Process, fixedIntegerPositions []int32, paramVals []any) error {
	params := proc.GetPrepareParams()
	if params == nil {
		return nil
	}
	for _, position := range fixedIntegerPositions {
		if position < 0 || int(position) >= len(paramVals) {
			continue
		}
		param, ok := paramVals[position].(plan2.ParamValue)
		if !ok || param.PrepareParamKind != vector.PrepareParamBoolean {
			continue
		}
		value, ok := param.Value.(bool)
		if !ok {
			continue
		}
		encoded := "0"
		if value {
			encoded = "1"
		}
		if err := vector.SetStringAt(params, int(position), encoded, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
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

func preparedParamValues(proc *process.Process, paramTypes []byte) ([]any, error) {
	params := proc.GetPrepareParams()
	if params == nil || params.Length() == 0 {
		return nil, nil
	}
	values := make([]any, params.Length())
	for i := range values {
		paramValue := plan2.ParamValue{
			IsBin:               proc.GetPrepareParamIsBin(i),
			IsBinaryProtocol:    true,
			PrepareParamKind:    proc.GetPrepareParamKind(i),
			EnableNumericPrefix: currentProtocolVersion(proc) >= defines.MORPCVersion30,
		}
		if params.IsNull(uint64(i)) {
			// NULL has no runtime value type, but it must retain its parameter
			// position, protocol source, and negotiated capabilities. A bare nil
			// would make execute-time common-type rebinding forget that this is the
			// same eligible marker used by a preceding non-NULL execution.
			values[i] = paramValue
			continue
		}
		raw, err := proc.GetPrepareParamsAt(i)
		if err != nil {
			return nil, err
		}
		paramValue.Value = string(raw)
		if i*2+1 < len(paramTypes) {
			mysqlType := defines.MysqlType(paramTypes[i*2])
			isUnsigned := paramTypes[i*2+1]&0x80 != 0
			// The MySQL binary protocol represents Go bool values as signed
			// MYSQL_TYPE_TINY 0/1.  Keep the protocol type helper numeric for
			// ordinary TINYINT callers, but restore the Boolean semantic kind
			// before constructing the execute-time literal.  Otherwise JSON
			// functions receive an integer 0/1 and change the stored JSON type.
			if paramValue.PrepareParamKind == vector.PrepareParamBoolean {
				paramValue.RuntimeType = types.T_bool.ToType()
				paramValue.HasRuntimeType = true
			} else if runtimeType, directResultType, materializedValue, hasDirectResultType, ok :=
				binaryProtocolPrepareParamDomains(mysqlType, isUnsigned, paramValue.Value.(string)); ok {
				if runtimeType.Oid != types.T_text {
					paramValue.RuntimeType = runtimeType
					paramValue.HasRuntimeType = true
				}
				paramValue.DirectResultType = directResultType
				paramValue.HasDirectResultType = hasDirectResultType
				paramValue.MaterializedValue = materializedValue
				if materializedValue != "" {
					// The restored cached plan executes a typed ParamRef against this
					// vector. Keep the raw packet spelling only in ParamValue provenance;
					// otherwise the executor reparses input-sized leading zeroes.
					if err = vector.SetStringAt(params, i, materializedValue, proc.Mp()); err != nil {
						return nil, err
					}
				}
			} else if mysqlType == defines.MYSQL_TYPE_DECIMAL || mysqlType == defines.MYSQL_TYPE_NEWDECIMAL {
				return nil, invalidBinaryDecimalParameter(proc.Ctx, paramValue.Value)
			}
		}
		// COM_STMT_EXECUTE values are binary-protocol values even when their
		// declared MySQL type is VAR_STRING. Keep this provenance separate from
		// RuntimeType so text values can safely participate in numeric overload
		// inference without changing direct string result metadata.
		values[i] = paramValue
	}
	return values, nil
}

func binaryProtocolRuntimeParamTypes(paramTypes []byte, params *vector.Vector) []types.Type {
	if params == nil || params.Length() == 0 {
		return nil
	}
	runtimeTypes := make([]types.Type, params.Length())
	for i := range runtimeTypes {
		if params.IsNull(uint64(i)) || i*2+1 >= len(paramTypes) {
			continue
		}
		mysqlType := defines.MysqlType(paramTypes[i*2])
		isUnsigned := paramTypes[i*2+1]&0x80 != 0
		if runtimeType, ok := binaryProtocolPrepareParamCategoryType(mysqlType, isUnsigned); ok {
			runtimeTypes[i] = runtimeType
		}
	}
	return runtimeTypes
}

func runtimeParamTypesContainText(runtimeTypes []types.Type) bool {
	for _, runtimeType := range runtimeTypes {
		switch runtimeType.Oid {
		case types.T_char, types.T_varchar, types.T_text:
			return true
		}
	}
	return false
}

// executeUserParamConcreteType returns the assignment-time SQL type carried by
// the EXECUTE ... USING expression when that width changes JSON comparison
// semantics. The Go value is only a compatibility fallback for callers which
// cannot provide binder type metadata (for example, legacy stored-procedure
// helpers); inferUserDefinedVarType deliberately widens Go integers and must
// not replace a real SQL TINYINT/SMALLINT/INT type.
func executeUserParamConcreteType(
	proc *process.Process,
	arg *plan.Expr,
	param any,
	kind vector.PrepareParamKind,
	position int,
) (types.T, error) {
	if arg != nil {
		concreteType := types.T(arg.Typ.Id)
		if expectedKind, supported := vector.PrepareParamKindForType(concreteType); supported {
			if expectedKind != kind {
				return types.T_any, moerr.NewInternalErrorf(
					proc.Ctx,
					"EXECUTE parameter type %s does not match kind %d at parameter %d",
					concreteType.String(), kind, position)
			}
			return concreteType, nil
		}
	}

	// Preserve the pre-existing conservative behavior for untyped callers. In
	// particular, arbitrary Go integer widths are intentionally normalized to
	// BIGINT/UBIGINT rather than treated as proof of an SQL assignment type.
	concreteType := types.T(inferUserDefinedVarType(param).Id)
	if expectedKind, supported := vector.PrepareParamKindForType(concreteType); supported && expectedKind == kind {
		return concreteType, nil
	}
	return types.T_any, nil
}

func buildExecuteUserParams(
	proc *process.Process,
	args []*plan.Expr,
	typedPositions []int32,
) (
	params *vector.Vector,
	paramVals []any,
	paramIsBin []bool,
	paramKinds []vector.PrepareParamKind,
	paramTypes []types.T,
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
		if _, relevant := slices.BinarySearch(typedPositions, int32(i)); relevant {
			var concreteType types.T
			concreteType, err = executeUserParamConcreteType(proc, arg, param, paramKinds[i], i)
			if err != nil {
				return
			}
			if concreteType != types.T_any {
				if paramTypes == nil {
					paramTypes = make([]types.T, len(args))
				}
				paramTypes[i] = concreteType
			}
		}
		err = util.AppendAnyToStringVector(proc, param, params)
		if err != nil {
			return
		}
		paramValue := plan2.ParamValue{
			Value:               param,
			IsBin:               paramIsBin[i],
			PrepareParamKind:    paramKinds[i],
			EnableNumericPrefix: currentProtocolVersion(proc) >= defines.MORPCVersion30,
		}
		if paramIsBin[i] {
			// User variables assigned from binary literals retain a binary SQL
			// result domain even when the EXECUTE argument itself is untyped.
			paramValue.SourceType = types.T_varbinary.ToType()
			paramValue.HasSourceType = true
		} else if arg.Typ.Id != 0 {
			sourceOID := types.T(arg.Typ.Id)
			if arg.Typ.Charset == uint32(types.CharsetBinary) {
				switch sourceOID {
				case types.T_char:
					sourceOID = types.T_binary
				case types.T_varchar:
					sourceOID = types.T_varbinary
				case types.T_text:
					sourceOID = types.T_blob
				}
			}
			paramValue.SourceType = types.NewWithCharset(
				sourceOID, arg.Typ.Width, arg.Typ.Scale, uint8(arg.Typ.Charset))
			paramValue.HasSourceType = true
		}
		paramVals[i] = paramValue
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
	planSnapshotTS *timestamp.Timestamp,
	planGenerationReused bool,
	fill func(*batch.Batch, *perfcounter.CounterSet) error,
	isPrepare bool,
	schedulingTrace *schedule.TraceRecorder,
	preparedRetry *preparedExecutionRetry,
) (retCompile *compile.Compile, err error) {

	addr := currentCNPipelineAddress(ses)
	pu := getPu(ses.GetService())
	if schedulingSQL == "" {
		schedulingSQL = originSQL
	}
	crs := new(perfcounter.CounterSet)
	var compileCtx context.Context
	execCtx.reqCtx, compileCtx = compileStatementContexts(
		execCtx.reqCtx, schedulingSQL, stmt, crs)
	proc.ReplaceTopCtx(compileCtx)
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
	if planSnapshotTS != nil {
		retCompile.SetPlanSnapshotTS(*planSnapshotTS)
		retCompile.SetPlanGenerationReused(planGenerationReused)
	} else if planGenerationReused {
		return nil, moerr.NewInternalError(execCtx.reqCtx,
			"reused plan generation is missing its snapshot binding")
	}
	forcePrepare := execCtx.input.isPreparedExpr()
	retCompile.SetBuildPlanFunc(preparedExecutionBuildPlanFunc(
		ses, stmt, forcePrepare, preparedRetry))

	err = retCompile.Compile(compileCtx, plan, compileOutputCallback(execCtx, ses, stmt, fill))
	if err != nil {
		return
	}
	retCompile.SetOriginSQL(originSQL)
	return
}

func compileStatementContexts(
	ctx context.Context,
	sql string,
	stmt tree.Statement,
	crs *perfcounter.CounterSet,
) (requestCtx, compileCtx context.Context) {
	requestCtx = perfcounter.AttachCompilePlanMarkKey(ctx, crs)
	if siriusStatementSelected(sql, stmt) {
		return requestCtx, compile.WithSiriusOffload(requestCtx)
	}
	return requestCtx, requestCtx
}

func siriusStatementSelected(sql string, stmt tree.Statement) bool {
	selected, _ := isSidecarQuery(sql)
	return selected && !isPerformStatement(stmt)
}

// EXPLAIN ANALYZE and EXPLAIN PHYPLAN execute the inner query only to collect
// runtime data. Their result rows are constructed by the frontend after the
// pipeline finishes, so inner-query batches must never reach the client output
// callback. Apply the same rule both when compiling a fresh pipeline and when
// resetting a cached prepared pipeline for another execution.
func compileOutputCallback(
	execCtx *ExecCtx,
	ses FeSession,
	stmt tree.Statement,
	fill func(*batch.Batch, *perfcounter.CounterSet) error,
) func(*batch.Batch, *perfcounter.CounterSet) error {
	switch stmt.(type) {
	case *tree.ExplainAnalyze, *tree.ExplainPhyPlan:
		return func(*batch.Batch, *perfcounter.CounterSet) error { return nil }
	default:
		return selectIntoUserVariablesOutputCallback(execCtx, ses, stmt, fill)
	}
}

func buildPlanForCompileRetry(
	ctx context.Context,
	ses FeSession,
	compilerContext plan2.CompilerContext,
	stmt tree.Statement,
	forcePrepare bool,
	preparedRetry *preparedExecutionRetry,
) (*plan2.Plan, error) {
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
	if err != nil {
		return nil, err
	}
	if preparedRetry == nil {
		return retryPlan, nil
	}
	if forcePrepare {
		runtimePlan, _, err := plan2.FillValuesOfParamsInPlanWithSpecialization(
			ctx, retryPlan, preparedRetry.paramVals)
		return runtimePlan, err
	}
	runtimePlan, _, applied, err := specializePreparedExecutionPlan(
		ctx, retryPlan, preparedRetry.paramVals, preparedRetry.binaryExecute,
		len(plan2.PreparedPlanNumericFallbackParamPositions(retryPlan)) > 0,
		preparedRetry.directResultSpecialization, false, nil, false, false)
	if err != nil {
		return nil, err
	}
	if applied {
		return runtimePlan, nil
	}
	return retryPlan, nil
}

func preparedExecutionBuildPlanFunc(
	ses FeSession,
	stmt tree.Statement,
	forcePrepare bool,
	preparedRetry *preparedExecutionRetry,
) func(context.Context) (*plan2.Plan, error) {
	return func(ctx context.Context) (*plan2.Plan, error) {
		return buildPlanForCompileRetry(
			ctx, ses, ses.GetTxnCompileCtx(), stmt, forcePrepare, preparedRetry)
	}
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
