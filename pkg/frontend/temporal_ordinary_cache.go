// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/constant"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// The candidate exists only inside Compile while the original scope is live.
type temporalCacheCandidate struct {
	raw, execution *pbplan.Plan
	original       *pbplan.Query
	proof          plan.TemporalCacheProof
}

// This one-shot seal belongs to the current wrapper until terminal publication.
// No Compile reference survives its terminal call or enters the session LRU.
type temporalCacheSeal struct {
	raw      *pbplan.Plan
	proof    plan.TemporalCacheProof
	owner    *ExecCtx
	running  *compile.Compile
	terminal bool
}

func temporalCacheRequestEligible(execCtx *ExecCtx, ses *Session, single bool) bool {
	if execCtx == nil || execCtx.reqCtx == nil || context.Cause(execCtx.reqCtx) != nil || ses == nil || ses.GetCmd() != COM_QUERY || !temporalCacheRawInputEligible(execCtx.input, sessionSQLModeForParser(ses)) || ses.IsBackgroundSession() || ses.GetIsInternal() || !isTopLevelClientStatement(ses, execCtx, execCtx.input) || execCtx.input.isInternalSQLSource(ses) || execCtx.input.isPreparedExpr() || execCtx.input.isBinaryProtExecute || !execCtx.input.canUsePlanCache() || single && !execCtx.singleStatementQuery {
		return false
	}
	return true
}

func temporalCacheRawInputEligible(input *UserInput, sqlMode string) bool {
	return input != nil && input.getStmt() == nil && input.getSql() != "" && !hasSchedulingOptimizerHint(input.getSql()) && statementSQLSource(input.getSql(), sqlMode) == constant.ExternSql
}

// Only the raw, ordinary SELECT owner can receive this optional qualification.
func temporalCacheStatementEligible(stmt tree.Statement) bool {
	selectStmt, ok := stmt.(*tree.Select)
	if !ok || selectStmt == nil || selectStmt.IsPerform || selectStmt.RewriteOption != nil || selectStmt.TimeWindow != nil || len(selectStmt.OrderBy) != 0 || selectStmt.Limit != nil || selectStmt.RankOption != nil || selectStmt.With != nil || selectStmt.Ep != nil || len(selectStmt.IntoVars) != 0 || selectStmt.DeprecatedInto || selectStmt.SelectLockInfo != nil {
		return false
	}
	clause, ok := selectStmt.Select.(*tree.SelectClause)
	return ok && clause != nil && !clause.Distinct && clause.Option == 0 && clause.IntoExport == nil && len(clause.IntoVars) == 0 && clause.GroupBy == nil && clause.Having == nil && len(clause.Windows) == 0
}

func temporalCacheEnvironment(ctx context.Context, ses *Session, p *pbplan.Plan) bool {
	if ctx == nil || ses == nil || p == nil || p.GetQuery() == nil || context.Cause(ctx) != nil {
		return false
	}
	if v := ctx.Value(defines.OptimizerHints{}); v != nil {
		s, ok := v.(string)
		if !ok || s != "" {
			return false
		}
	}
	rt := runtime.ServiceRuntime(ses.GetService())
	if rt == nil {
		return false
	}
	if v, exists := rt.GetGlobalVariables("optimizer_hints"); exists {
		s, ok := v.(string)
		if !ok || s != "" {
			return false
		}
	}
	value, err := ses.GetSessionSysVar("max_dop")
	dop, ok := value.(int64)
	return err == nil && ok && dop == p.GetQuery().MaxDop
}

// Normal root pop restores the parent frame before this gate. Requiring the
// completed frame to remain current would incorrectly reject every root.
func (tcc *TxnCompilerContext) trySealTemporalScope(scope *plan.TemporalPlanningScope, q *pbplan.Query, owner *ExecCtx, op client.TxnOperator) bool {
	if tcc == nil || scope == nil || owner == nil || op == nil || !scope.MatchesQuery(q) {
		return false
	}
	frame, ok := scope.NativeOwner().(*temporalScopeFrame)
	if !ok || frame == nil || frame.owner != tcc || !tcc.temporal.mu.TryLock() {
		return false
	}
	defer tcc.temporal.mu.Unlock()
	binding := frame.binding.Load()
	return !tcc.temporal.contended.Load() && frame.completed && frame.valid && frame.retired.Load() && frame.active.Load() == 0 && binding != nil && binding.execution == owner && binding.op == op && tcc.execCtx == owner && scope.MatchesQuery(q)
}

func (cwft *TxnComputationWrapper) temporalCacheOwnerCurrent(execCtx *ExecCtx, ses *Session, original *pbplan.Query) bool {
	op := ses.GetTxnHandler().GetTxn()
	return op != nil && cwft.proc != nil && cwft.proc.GetTxnOperator() == op && cwft.temporalScope.ValidateTemporalCacheRead(execCtx.reqCtx, original) && ses.GetTxnCompileCtx().trySealTemporalScope(cwft.temporalScope, original, execCtx, op) && cwft.proc.GetTxnOperator() == op
}

func (cwft *TxnComputationWrapper) prepareTemporalCacheCandidate(execCtx *ExecCtx) *temporalCacheCandidate {
	ses, ok := cwft.ses.(*Session)
	if !ok || !temporalCacheRequestEligible(execCtx, ses, true) || !temporalCacheStatementEligible(cwft.stmt) || !temporalCacheEnvironment(execCtx.reqCtx, ses, cwft.plan) {
		return nil
	}
	scope := cwft.temporalScope
	original := cwft.plan.GetQuery()
	proof, ok := scope.TemporalCacheProof(original)
	if !ok {
		return nil
	}
	if !cwft.temporalCacheOwnerCurrent(execCtx, ses, original) || !plan.AuditTemporalCacheRaw(execCtx.reqCtx, cwft.plan, proof, cwft.proc) {
		return nil
	}
	raw, ok := plan.CloneTemporalCachePlan(execCtx.reqCtx, cwft.plan)
	if !ok || !plan.AuditTemporalCacheRaw(execCtx.reqCtx, raw, proof, cwft.proc) || !cwft.temporalCacheOwnerCurrent(execCtx, ses, original) {
		return nil
	}
	execution, ok := plan.CloneTemporalCachePlan(execCtx.reqCtx, raw)
	if !ok || !cwft.temporalCacheOwnerCurrent(execCtx, ses, original) {
		return nil
	}
	cwft.plan = execution
	return &temporalCacheCandidate{raw: raw, execution: execution, original: original, proof: proof}
}

func (cwft *TxnComputationWrapper) sealTemporalCacheCandidate(execCtx *ExecCtx, candidate *temporalCacheCandidate) {
	if candidate == nil || cwft.plan != candidate.execution {
		return
	}
	ses, ok := cwft.ses.(*Session)
	if !ok || !temporalCacheRequestEligible(execCtx, ses, true) || !temporalCacheStatementEligible(cwft.stmt) || !temporalCacheEnvironment(execCtx.reqCtx, ses, candidate.raw) {
		return
	}
	actual, ok := cwft.compile.(*compile.Compile)
	if !ok || actual == nil || !cwft.temporalCacheOwnerCurrent(execCtx, ses, candidate.original) {
		return
	}
	cwft.temporalCacheSeal = &temporalCacheSeal{raw: candidate.raw, proof: candidate.proof, owner: execCtx, running: actual}
}

func (cwft *TxnComputationWrapper) finishTemporalCacheExecution(running Compile, runErr error) {
	seal := cwft.temporalCacheSeal
	if seal == nil {
		return
	}
	// Compare before clearing, and always drop the pool-bound pointer now.
	same := seal.running != nil && running == seal.running
	seal.running = nil
	ses, ok := cwft.ses.(*Session)
	if !same || !ok || runErr != nil || running.PlanGenerationRebuilt() || !cwft.hasPlanSnapshotTS || cwft.protocolVersion != currentProtocolVersion(cwft.proc) || seal.owner == nil || seal.owner.cw != cwft || ses.GetTxnCompileCtx().execCtx != seal.owner || !temporalCacheRequestEligible(seal.owner, ses, true) || !temporalCacheStatementEligible(cwft.stmt) {
		cwft.temporalCacheSeal = nil
		return
	}
	seal.terminal = true
}

func (cwft *TxnComputationWrapper) temporalCachePublication(execCtx *ExecCtx) *temporalCacheSeal {
	seal := cwft.temporalCacheSeal
	ses, ok := cwft.ses.(*Session)
	if !ok || execCtx == nil || execCtx.proc != cwft.proc || seal == nil || !seal.terminal || seal.running != nil || seal.owner != execCtx || !temporalCacheRequestEligible(execCtx, ses, true) || !temporalCacheStatementEligible(cwft.stmt) || !temporalCacheEnvironment(execCtx.reqCtx, ses, seal.raw) || !plan.AuditTemporalCacheRaw(execCtx.reqCtx, seal.raw, seal.proof, execCtx.proc) {
		return nil
	}
	return seal
}

// Avoid generated oneof getters before the optional preflight has run.
func temporalPlanReplanState(p *pbplan.Plan) (needsReplan, valid bool) {
	if p == nil {
		return false, false
	}
	if query, ok := p.Plan.(*pbplan.Plan_Query); ok {
		if query == nil || query.Query == nil {
			return false, false
		}
		return query.Query.TemporalIndexNeedsReplan, true
	}
	return false, true
}

// Lookup keeps the ordinary owned AST and exact expected raw generation. The
// executable clone is delayed until ordinary checkModify has passed.
func temporalCachedPlanForExecution(ses *Session, execCtx *ExecCtx) (*cachedPlan, error) {
	if cause := context.Cause(execCtx.reqCtx); cause != nil {
		return nil, cause
	}
	cached := cachedPlanForInput(ses, execCtx.input)
	if cached == nil {
		return nil, context.Cause(execCtx.reqCtx)
	}
	special := false
	for _, p := range cached.plans {
		needsReplan, valid := temporalPlanReplanState(p)
		if !valid {
			return nil, context.Cause(execCtx.reqCtx)
		}
		special = special || needsReplan
	}
	if !special && !cached.temporal {
		return cached, context.Cause(execCtx.reqCtx)
	}
	if !cached.temporal || len(cached.plans) != 1 || len(cached.stmts) != 1 || !special || !temporalCacheRequestEligible(execCtx, ses, false) || !temporalCacheEnvironment(execCtx.reqCtx, ses, cached.plans[0]) {
		if cause := context.Cause(execCtx.reqCtx); cause != nil {
			return nil, cause
		}
		ses.removeCachedPlan(execCtx.input.getHash())
		return nil, context.Cause(execCtx.reqCtx)
	}
	if !temporalCacheStatementEligible(cached.stmts[0]) {
		if cause := context.Cause(execCtx.reqCtx); cause != nil {
			return nil, cause
		}
		ses.removeCachedPlan(execCtx.input.getHash())
		return nil, context.Cause(execCtx.reqCtx)
	}
	return cached, context.Cause(execCtx.reqCtx)
}

// pendingTemporalGenerationCurrent is read-only: getCachedPlan may evict an
// invalid entry and free the AST that this wrapper still borrows.
func (cwft *TxnComputationWrapper) pendingTemporalGenerationCurrent(ses *Session) bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if ses.planCache == nil {
		return false
	}
	element := ses.planCache.cachePool[cwft.cachedPlanSQL]
	if element == nil {
		return false
	}
	cached := element.Value.(*cachedPlan)
	return !cached.invalid && cached.temporal && len(cached.plans) == 1 &&
		len(cached.stmts) == 1 && len(cached.planSnapshotTS) == 1 &&
		cwft.cachedPlanIndex == 0 && cached.plans[0] == cwft.cachedPlanGeneration &&
		cached.stmts[0] == cwft.stmt && proto.Equal(&cached.planSnapshotTS[0], &cwft.planSnapshotTS) &&
		cached.protocolVersion == cwft.protocolVersion
}

// Only a Session-published, non-escaping raw generation reaches this shortcut.
// Publication audited and bounded the complete protobuf graph; all execution
// consumers receive a separate clone. The public plan clone keeps its preflight.
func clonePrivateTemporalGeneration(ctx context.Context, raw *pbplan.Plan) (*pbplan.Plan, error) {
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	cloned := proto.Clone(raw).(*pbplan.Plan)
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	return cloned, nil
}

func (cwft *TxnComputationWrapper) admitTemporalCachedExecution(execCtx *ExecCtx) (bool, error) {
	if cause := context.Cause(execCtx.reqCtx); cause != nil {
		return false, cause
	}
	ses, ok := cwft.ses.(*Session)
	if !ok || execCtx.ses != ses || execCtx.cw != cwft || execCtx.proc != cwft.proc ||
		execCtx.input == nil || cwft.cachedPlanSQL != execCtx.input.getHash() ||
		len(execCtx.cws) != 1 || execCtx.cws[0] != cwft ||
		ses.GetTxnCompileCtx().execCtx != execCtx {
		return false, moerr.NewInternalError(execCtx.reqCtx, "temporal cache execution owner mismatch")
	}
	if !cwft.pendingTemporalGenerationCurrent(ses) ||
		!temporalCacheRequestEligible(execCtx, ses, true) ||
		!temporalCacheStatementEligible(cwft.stmt) {
		return false, context.Cause(execCtx.reqCtx)
	}
	op := ses.GetTxnHandler().GetTxn()
	if op == nil || cwft.proc == nil || cwft.proc.GetTxnOperator() != op {
		return false, moerr.NewInternalError(execCtx.reqCtx, "temporal cache transaction owner mismatch")
	}
	raw := cwft.cachedPlanGeneration
	changed, err := checkModify(raw, ses.GetTxnCompileCtx().Resolve)
	if err != nil {
		return false, err
	}
	if !changed {
		changed, err = plan.CachedPlanStatsChanged(raw, ses.GetTxnCompileCtx())
		if err != nil {
			return false, err
		}
	}
	if cause := context.Cause(execCtx.reqCtx); cause != nil {
		return false, cause
	}
	if ses.GetTxnHandler().GetTxn() != op || cwft.proc.GetTxnOperator() != op {
		return false, moerr.NewInternalError(execCtx.reqCtx, "temporal cache transaction owner mismatch")
	}
	if changed || !temporalCacheEnvironment(execCtx.reqCtx, ses, raw) ||
		!cwft.pendingTemporalGenerationCurrent(ses) {
		return false, context.Cause(execCtx.reqCtx)
	}
	execution, err := clonePrivateTemporalGeneration(execCtx.reqCtx, raw)
	if err != nil {
		return false, err
	}
	cwft.plan = execution
	cwft.hasPlanSnapshotTS = true
	cwft.planGenerationReused = true
	return true, nil
}

func (seal *temporalCacheSeal) permits(raw *pbplan.Plan) bool {
	needsReplan, valid := temporalPlanReplanState(raw)
	return valid && needsReplan && seal != nil && seal.terminal && seal.running == nil && seal.raw == raw
}
