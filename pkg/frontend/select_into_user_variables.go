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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// selectIntoUserVariables collects the result of SELECT ... INTO @var.  The
// output callback can receive several batches, so assignments are delayed
// until the complete result is known.  This also keeps existing variables
// unchanged for a zero-row query, as MySQL does.
type selectIntoUserVariables struct {
	mu        sync.Mutex
	vars      []*tree.VarExpr
	row       []any
	rowIsBin  []bool
	rowType   []plan2.Type
	rowDomain []types.RuntimeStringDomain
	rowCount  uint64
}

// scalarNumericValueKindCapture retains the companion BOOL for the single
// visible value produced by an internal SET expression SELECT. It deliberately
// stores only the scalar snapshot; vectors remain owned by the result batch.
type scalarNumericValueKindCapture struct {
	mu        sync.Mutex
	value     bool
	available bool
}

func (capture *scalarNumericValueKindCapture) reset() {
	if capture == nil {
		return
	}
	capture.mu.Lock()
	capture.value = false
	capture.available = false
	capture.mu.Unlock()
}

func (capture *scalarNumericValueKindCapture) captureBatch(
	ctx context.Context, query *plan.Query, bat *batch.Batch,
) error {
	if bat == nil || query == nil {
		return nil
	}
	if query.StmtType != plan.Query_SELECT || query.HasReturning || len(query.Headings) != 1 {
		return moerr.NewWrongNumberOfColumnsInSelect(ctx)
	}
	if err := plan2.ValidateSelectIntoKindLayout(query); err != nil {
		return moerr.NewInternalErrorf(ctx, "invalid scalar numeric kind layout: %v", err)
	}
	step := query.MainOutputStep()
	if step < 0 || int(step) >= len(query.Steps) {
		return moerr.NewWrongNumberOfColumnsInSelect(ctx)
	}
	nodeID := query.Steps[step]
	if nodeID < 0 || int(nodeID) >= len(query.Nodes) || query.Nodes[nodeID] == nil {
		return moerr.NewWrongNumberOfColumnsInSelect(ctx)
	}
	visible := len(query.Headings)
	projectList := query.Nodes[nodeID].ProjectList
	if visible > len(projectList) || len(bat.Vecs) != len(projectList) {
		return moerr.NewWrongNumberOfColumnsInSelect(ctx)
	}
	kindLayout := query.IntoUserVariableKindColumns
	if len(kindLayout) != 0 && len(kindLayout) != visible {
		return moerr.NewWrongNumberOfColumnsInSelect(ctx)
	}
	passenger := int32(-1)
	if len(kindLayout) != 0 {
		passenger = kindLayout[0]
	}
	if passenger >= 0 {
		if passenger < int32(visible) || int(passenger) >= len(bat.Vecs) ||
			bat.Vecs[passenger] == nil || bat.Vecs[passenger].GetType().Oid != types.T_bool {
			return moerr.NewWrongNumberOfColumnsInSelect(ctx)
		}
	}
	if bat.RowCount() == 0 {
		return nil
	}
	if bat.Vecs[0] == nil {
		return moerr.NewWrongNumberOfColumnsInSelect(ctx)
	}
	valueIsNull := bat.Vecs[0].IsNull(0)
	value := false
	available := passenger >= 0
	if available && !valueIsNull {
		kindVector := bat.Vecs[passenger]
		if !kindVector.IsConstNull() && !kindVector.GetNulls().Contains(0) {
			value = vector.GetFixedAtNoTypeCheck[bool](kindVector, 0)
		}
	}
	capture.mu.Lock()
	capture.value = value && !valueIsNull
	capture.available = available
	capture.mu.Unlock()
	return nil
}

func (capture *scalarNumericValueKindCapture) snapshot() (bool, bool) {
	if capture == nil {
		return false, false
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	return capture.value, capture.available
}

func newSelectIntoUserVariables(vars []*tree.VarExpr) *selectIntoUserVariables {
	return &selectIntoUserVariables{vars: vars}
}

// validateSelectIntoArity checks the result shape before execution.  A query
// that produces no rows may not invoke the output callback, so validating only
// in capture would let a mismatched SELECT ... INTO silently succeed.
func validateSelectIntoArity(ctx context.Context, p *plan.Plan, variableCount int) error {
	if p == nil {
		return nil
	}
	if query := p.GetQuery(); query != nil {
		if err := plan2.ValidateSelectIntoKindLayout(query); err != nil {
			return moerr.NewInternalErrorf(ctx, "invalid SELECT INTO result kind layout: %v", err)
		}
	}
	if len(plan2.GetResultColumnsFromPlan(p)) != variableCount {
		return moerr.NewWrongNumberOfColumnsInSelect(ctx)
	}
	return nil
}

// activeSelectIntoPlan returns the plan generation that produced the current
// execution result. A prepared execution can rebuild its plan inside Run; the
// wrapper's published plan is synchronized only after the statement handler
// returns, so validation immediately after Run must consult the active compile.
func activeSelectIntoPlan(execCtx *ExecCtx) *plan.Plan {
	if execCtx == nil || execCtx.cw == nil {
		return nil
	}
	if wrapper, ok := execCtx.cw.(*TxnComputationWrapper); ok && wrapper.compile != nil {
		return wrapper.compile.GetPlan()
	}
	return execCtx.cw.Plan()
}

func selectIntoUserVariablesOutputCallback(
	execCtx *ExecCtx,
	ses FeSession,
	stmt tree.Statement,
	fill func(*batch.Batch, *perfcounter.CounterSet) error,
	currentPlan func() *plan.Plan,
) func(*batch.Batch, *perfcounter.CounterSet) error {
	if capture := execCtx.numericValueKindCapture; capture != nil {
		return func(bat *batch.Batch, counters *perfcounter.CounterSet) error {
			if bat == nil {
				return fill(nil, counters)
			}
			if currentPlan == nil {
				return moerr.NewInternalError(execCtx.reqCtx, "scalar numeric kind capture has no active plan")
			}
			current := currentPlan()
			if current == nil || current.GetQuery() == nil {
				return moerr.NewInternalError(execCtx.reqCtx, "scalar numeric kind capture has no active query plan")
			}
			query := current.GetQuery()
			if err := capture.captureBatch(execCtx.reqCtx, query, bat); err != nil {
				return err
			}
			visibleBatch := *bat
			visibleBatch.Vecs = bat.Vecs[:len(query.Headings)]
			if len(bat.Attrs) >= len(query.Headings) {
				visibleBatch.Attrs = bat.Attrs[:len(query.Headings)]
			}
			return fill(&visibleBatch, counters)
		}
	}
	selectStmt, ok := stmt.(*tree.Select)
	if !ok || len(selectStmt.IntoVars) == 0 {
		return fill
	}
	// ExecCtx is reused for every statement in a COM_QUERY request.  A
	// collector belongs to exactly one SELECT ... INTO statement; retaining it
	// across statement generation leaks rowCount and assignments into the next
	// statement.
	collector := newSelectIntoUserVariables(selectStmt.IntoVars)
	execCtx.selectInto = collector
	return func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
		var kindLayout []int32
		if currentPlan != nil {
			if current := currentPlan(); current != nil && current.GetQuery() != nil {
				query := current.GetQuery()
				if err := plan2.ValidateSelectIntoKindLayout(query); err != nil {
					return moerr.NewInternalErrorf(execCtx.reqCtx, "invalid SELECT INTO result kind layout: %v", err)
				}
				kindLayout = query.IntoUserVariableKindColumns
			}
		}
		return collector.capture(execCtx.reqCtx, ses, bat, kindLayout)
	}
}

func (collector *selectIntoUserVariables) capture(ctx context.Context, ses FeSession, bat *batch.Batch, kindLayouts ...[]int32) error {
	if bat == nil {
		return nil
	}
	visibleColumns := len(collector.vars)
	var kindLayout []int32
	if len(kindLayouts) > 0 {
		kindLayout = kindLayouts[0]
	}
	if len(kindLayout) != 0 && len(kindLayout) != visibleColumns {
		return moerr.NewWrongNumberOfColumnsInSelect(ctx)
	}
	expectedWidth := visibleColumns
	usedPassengers := make([]bool, len(bat.Vecs))
	for _, passenger := range kindLayout {
		if passenger == -1 {
			continue
		}
		if passenger < int32(visibleColumns) || int(passenger) >= len(bat.Vecs) {
			return moerr.NewWrongNumberOfColumnsInSelect(ctx)
		}
		if bat.Vecs[passenger] == nil || bat.Vecs[passenger].GetType().Oid != types.T_bool {
			return moerr.NewWrongNumberOfColumnsInSelect(ctx)
		}
		usedPassengers[passenger] = true
		if int(passenger)+1 > expectedWidth {
			expectedWidth = int(passenger) + 1
		}
	}
	if len(bat.Vecs) != expectedWidth {
		return moerr.NewWrongNumberOfColumnsInSelect(ctx)
	}
	for physical := visibleColumns; physical < len(bat.Vecs); physical++ {
		if !usedPassengers[physical] {
			return moerr.NewWrongNumberOfColumnsInSelect(ctx)
		}
	}
	if bat.RowCount() == 0 {
		return nil
	}

	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.rowCount+uint64(bat.RowCount()) > 1 {
		return moerr.NewTooManyRows(ctx)
	}
	if collector.rowCount == 0 {
		collector.row = make([]any, len(collector.vars))
		collector.rowIsBin = make([]bool, len(collector.vars))
		collector.rowType = make([]plan2.Type, len(collector.vars))
		collector.rowDomain = make([]types.RuntimeStringDomain, len(collector.vars))
		visibleBatch := *bat
		visibleBatch.Vecs = bat.Vecs[:visibleColumns]
		if err := extractRowFromEveryVector(ctx, ses, &visibleBatch, 0, collector.row, false); err != nil {
			return err
		}
		for i, vec := range visibleBatch.Vecs {
			collector.rowIsBin[i] = vec.GetIsBin()
			if collector.row[i] != nil && len(kindLayout) != 0 && kindLayout[i] >= 0 {
				kindVector := bat.Vecs[kindLayout[i]]
				if !kindVector.IsConstNull() && !kindVector.GetNulls().Contains(0) {
					collector.rowIsBin[i] = collector.rowIsBin[i] || vector.GetFixedAtNoTypeCheck[bool](kindVector, 0)
				}
			}
			collector.rowDomain[i] = vec.GetRuntimeStringDomainAt(0)
			collector.row[i], collector.rowType[i] = selectIntoUserVariableValueAndType(collector.row[i], *vec.GetType())
		}
	}
	collector.rowCount += uint64(bat.RowCount())
	return nil
}

func (collector *selectIntoUserVariables) resetAttempt() {
	if collector == nil {
		return
	}
	collector.mu.Lock()
	collector.row = nil
	collector.rowIsBin = nil
	collector.rowType = nil
	collector.rowDomain = nil
	collector.rowCount = 0
	collector.mu.Unlock()
}

type selectIntoAttemptState uint8

const (
	selectIntoAttemptIdle selectIntoAttemptState = iota
	selectIntoAttemptWriting
	selectIntoAttemptSealed
)

// selectIntoResultSink stages callback writes under Compile's retry generation.
// A failed attempt can have delivered a batch before a later operator requests
// a retry, so its collector/capture must be discarded before the next attempt
// begins.
type selectIntoResultSink struct {
	mu         sync.Mutex
	write      func(*batch.Batch, *perfcounter.CounterSet) error
	collector  *selectIntoUserVariables
	capture    *scalarNumericValueKindCapture
	session    FeSession
	state      selectIntoAttemptState
	generation uint64
}

func newSelectIntoResultSink(
	execCtx *ExecCtx,
	stmt tree.Statement,
	write func(*batch.Batch, *perfcounter.CounterSet) error,
) compile.ResultSink {
	selectStmt, isSelect := stmt.(*tree.Select)
	needsAttemptScope := isSelect && len(selectStmt.IntoVars) != 0
	if execCtx != nil && execCtx.numericValueKindCapture != nil {
		needsAttemptScope = true
	}
	if execCtx == nil || !needsAttemptScope {
		return nil
	}
	return &selectIntoResultSink{
		write:     write,
		collector: execCtx.selectInto,
		capture:   execCtx.numericValueKindCapture,
		session:   execCtx.ses,
	}
}

func (sink *selectIntoResultSink) resetAttemptLocked() {
	sink.collector.resetAttempt()
	sink.capture.reset()
	// Internal SET evaluation also retains raw output batches on the session.
	// Clear them with the paired BOOL snapshot so a failed generation cannot
	// leave a value row from one attempt beside a kind bit from another.
	if sink.capture != nil && sink.session != nil {
		if cleaner, ok := sink.session.(interface{ ClearResultBatches() }); ok {
			cleaner.ClearResultBatches()
		}
	}
}

func (sink *selectIntoResultSink) BeginAttempt(_ context.Context, generation uint64, _ *process.Process) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.state == selectIntoAttemptWriting {
		return moerr.NewInternalErrorNoCtx("SELECT INTO output attempt already active")
	}
	sink.resetAttemptLocked()
	sink.generation = generation
	sink.state = selectIntoAttemptWriting
	return nil
}

func (sink *selectIntoResultSink) Write(generation uint64, bat *batch.Batch, counters *perfcounter.CounterSet) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.state != selectIntoAttemptWriting || generation != sink.generation {
		return moerr.NewInternalErrorNoCtxf(
			"SELECT INTO output generation mismatch: got %d, active %d", generation, sink.generation)
	}
	return sink.write(bat, counters)
}

func (sink *selectIntoResultSink) SealAttempt(generation uint64) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.state != selectIntoAttemptWriting || generation != sink.generation {
		return moerr.NewInternalErrorNoCtxf(
			"SELECT INTO output seal generation mismatch: got %d, active %d", generation, sink.generation)
	}
	sink.state = selectIntoAttemptSealed
	return nil
}

func (sink *selectIntoResultSink) AbortAttempt(generation uint64, _ error) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.state != selectIntoAttemptWriting || generation != sink.generation {
		return moerr.NewInternalErrorNoCtxf(
			"SELECT INTO output abort generation mismatch: got %d, active %d", generation, sink.generation)
	}
	sink.resetAttemptLocked()
	sink.state = selectIntoAttemptIdle
	return nil
}

func selectIntoUserVariableValueAndType(value any, typ types.Type) (any, plan2.Type) {
	switch typ.Oid {
	case types.T_geometry, types.T_geometry32:
		// extractRowFromEveryVector converts geometry payloads to display text.
		// Persist that text as a string user variable instead of retaining the
		// geometry OID, otherwise a later @var read would treat WKT bytes as the
		// internal geometry payload.
		return value, plan2.Type{Id: int32(types.T_varchar)}
	default:
		return value, plan2.MakePlan2Type(&typ)
	}
}

func (collector *selectIntoUserVariables) apply(ctx context.Context, ses FeSession, sql string) error {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.rowCount == 0 {
		appendSelectIntoNoDataWarning(ses)
		return nil
	}
	if collector.rowCount > 1 {
		return moerr.NewTooManyRows(ctx)
	}
	for i, variable := range collector.vars {
		var isBin bool
		if i < len(collector.rowIsBin) {
			isBin = collector.rowIsBin[i]
		}
		var typ plan2.Type
		if i < len(collector.rowType) {
			typ = collector.rowType[i]
		}
		runtimeDomain := types.RuntimeStringInherit
		if i < len(collector.rowDomain) {
			runtimeDomain = collector.rowDomain[i]
		}
		if err := setUserDefinedVarWithTypeAndDomain(
			ses, variable.Name, collector.row[i], sql, isBin, typ, runtimeDomain); err != nil {
			return err
		}
	}
	return nil
}

func setUserDefinedVarWithIsBin(ses FeSession, name string, value interface{}, sql string, isBin bool) error {
	return setUserDefinedVarWithType(ses, name, value, sql, isBin, plan2.Type{})
}

func setUserDefinedVarWithType(ses FeSession, name string, value interface{}, sql string, isBin bool, typ plan2.Type) error {
	return setUserDefinedVarWithTypeAndDomain(
		ses, name, value, sql, isBin, typ, types.RuntimeStringInherit)
}

func setUserDefinedVarWithTypeAndDomain(
	ses FeSession,
	name string,
	value interface{},
	sql string,
	isBin bool,
	typ plan2.Type,
	runtimeDomain types.RuntimeStringDomain,
) error {
	switch session := ses.(type) {
	case *Session:
		return session.setUserDefinedVarWithTypeAndKindAndReplayability(
			name, value, sql, isBin, typ, prepareParamKindFromType(types.T(typ.Id)), false, runtimeDomain)
	case *backSession:
		if session.upstream == nil {
			return moerr.NewInternalError(context.Background(), "do not support set user defined var in background exec")
		}
		return setUserDefinedVarWithTypeAndDomain(
			session.upstream, name, value, sql, isBin, typ, runtimeDomain)
	default:
		if isBin || runtimeDomain != types.RuntimeStringInherit {
			return moerr.NewInternalError(context.Background(), "do not support binary user defined var assignment")
		}
		return ses.SetUserDefinedVar(name, value, sql)
	}
}

func appendSelectIntoNoDataWarning(ses FeSession) {
	switch session := ses.(type) {
	case *Session:
		session.appendWarningDiagnostic(moerr.ER_SP_FETCH_NO_DATA, "No data - zero rows fetched, selected, or processed")
	case *backSession:
		if session.upstream != nil {
			appendSelectIntoNoDataWarning(session.upstream)
		}
	}
}

func appendSelectIntoDeprecatedWarning(ses FeSession, deprecated bool) {
	if !deprecated {
		return
	}
	const msg = "The INTO clause is deprecated inside query blocks of query expressions and will be removed in a future release. Please move the INTO clause to the end of statement instead."
	switch session := ses.(type) {
	case *Session:
		session.appendWarningDiagnostic(moerr.ER_WARN_DEPRECATED_INNER_INTO, msg)
	case *backSession:
		if session.upstream != nil {
			appendSelectIntoDeprecatedWarning(session.upstream, deprecated)
		}
	}
}
