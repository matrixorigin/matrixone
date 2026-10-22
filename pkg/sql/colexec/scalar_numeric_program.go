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

package colexec

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// scalarNumericProgramState owns one source executor and exposes its value and
// branch-produced kind as sibling outputs. The kind never evaluates a second
// copy of the expression tree.
type scalarNumericProgramState struct {
	definition *plan.Expr
	value      ExpressionExecutor
	kindSource ExpressionExecutor
	selection  *vector.AllocationAccountSelection
	mp         *mpool.MPool
	refs       int
	building   bool
	evaluated  bool
	valueRows  *vector.Vector
	kindRows   *vector.Vector
	rowCount   int
	batches    []*batch.Batch
	selectList []bool
	nilSelect  bool
}

type scalarNumericProgramRefExecutor struct {
	state *scalarNumericProgramState
	slot  int
}

func (expr *scalarNumericProgramRefExecutor) Eval(
	proc *process.Process, batches []*batch.Batch, selectList []bool,
) (*vector.Vector, error) {
	rowCount := expressionRowCount(batches)
	if expr.state.evaluated {
		if !expr.state.sameInvocation(batches, rowCount, selectList) {
			return nil, moerr.NewInvalidInputNoCtx("scalar numeric program outputs were requested with different row masks")
		}
	} else {
		value, err := expr.state.value.Eval(proc, batches, selectList)
		if err != nil {
			return nil, err
		}
		var kind *vector.Vector
		if expr.state.kindSource != nil {
			kind, err = expr.state.kindSource.Eval(proc, batches, selectList)
			if err != nil {
				return nil, err
			}
			if kind == nil || kind.GetType().Oid != types.T_bool ||
				(!kind.IsConst() && kind.Length() < rowCount) {
				return nil, moerr.NewInvalidInputNoCtx("scalar numeric program kind input is not row-aligned BOOL")
			}
		} else {
			kind, err = expr.state.makeKindRows(rowCount)
			if err != nil {
				return nil, err
			}
		}
		expr.state.valueRows = value
		expr.state.kindRows = kind
		expr.state.captureInvocation(batches, rowCount, selectList)
		expr.state.evaluated = true
	}
	if expr.slot == 0 {
		return expr.state.valueRows, nil
	}
	if expr.slot == 1 {
		return expr.state.kindRows, nil
	}
	return nil, moerr.NewInvalidInputNoCtx("scalar numeric program output slot is out of range")
}

func (expr *scalarNumericProgramRefExecutor) EvalWithoutResultReusing(
	proc *process.Process, batches []*batch.Batch, selectList []bool,
) (*vector.Vector, error) {
	return expr.Eval(proc, batches, selectList)
}

func (expr *scalarNumericProgramRefExecutor) ResetForNextQuery() {}

func (expr *scalarNumericProgramRefExecutor) Free() {
	if expr.state == nil {
		return
	}
	expr.state.refs--
	if expr.state.refs == 0 {
		if expr.state.value != nil {
			expr.state.value.Free()
			expr.state.value = nil
		}
		hasKindSource := expr.state.kindSource != nil
		if expr.state.kindSource != nil {
			expr.state.kindSource.Free()
			expr.state.kindSource = nil
		}
		if expr.state.kindRows != nil && !hasKindSource {
			// The synthesized selector result belongs to this state. A BOOL
			// passenger borrowed from the input batch does not.
			expr.state.kindRows.Free(expr.state.mp)
		}
		expr.state.kindRows = nil
		expr.state.resetEvaluation()
	}
	expr.state = nil
}

func (expr *scalarNumericProgramRefExecutor) IsColumnExpr() bool { return false }
func (expr *scalarNumericProgramRefExecutor) TypeName() string {
	return "scalar numeric program output"
}

func (expr *scalarNumericProgramRefExecutor) numericValueKindAt(row int) bool {
	if expr.slot == 1 && expr.state.kindRows != nil && row >= 0 && row < expr.state.kindRows.Length() {
		return vector.MustFixedColWithTypeCheck[bool](expr.state.kindRows)[row]
	}
	if expr.slot == 0 && expr.state.evaluated {
		return numericValueKindAt(expr.state.value, row)
	}
	return false
}

// scalarNumericProgramZoneMapConstant admits only a complete CAST9 lexical
// owner whose original source is row-independent and safe to evaluate once.
// Private refs outside that owner remain unsupported by metadata evaluation.
func scalarNumericProgramZoneMapConstant(expr *plan.Expr) bool {
	hasOwner := false
	if !scalarNumericProgramZoneMapConstantExpr(expr, &hasOwner) || !hasOwner {
		return false
	}
	return types.T(expr.Typ.Id).ToType().IsNumeric() && types.T(expr.Typ.Id) != types.T_decimal256
}

func scalarNumericProgramZoneMapConstantExpr(expr *plan.Expr, hasOwner *bool) bool {
	if expr == nil {
		return false
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Lit, *plan.Expr_T, *plan.Expr_P, *plan.Expr_V:
		return true
	case *plan.Expr_Fold:
		return e.Fold != nil && e.Fold.IsConst
	case *plan.Expr_Col, *plan.Expr_Sub, *plan.Expr_Vec:
		return false
	case *plan.Expr_List:
		if e.List == nil {
			return false
		}
		for _, item := range e.List.List {
			if !scalarNumericProgramZoneMapConstantExpr(item, hasOwner) {
				return false
			}
		}
		return true
	case *plan.Expr_F:
		if e.F == nil || e.F.Func == nil {
			return false
		}
		fid, overload := function.DecodeOverloadID(e.F.Func.GetObj())
		if fid == function.CAST && overload == function.NumericValueCastOverload {
			source, sourceKind, ok := function.NumericValueProgramCastOwner(expr)
			if !ok || !scalarNumericProgramZoneMapConstantExpr(source, hasOwner) {
				return false
			}
			if sourceKind != nil && !scalarNumericProgramZoneMapConstantExpr(sourceKind, hasOwner) {
				return false
			}
			*hasOwner = true
			return true
		}
		if fid == function.SCALAR_PROGRAM || fid == function.SCALAR_PROGRAM_REF {
			return false
		}
		registered, ok := function.GetFunctionByIdWithoutError(e.F.Func.GetObj())
		if !ok || registered.CannotFold() || registered.IsRealTimeRelated() || registered.IsAgg() || registered.IsWin() {
			return false
		}
		for _, arg := range e.F.Args {
			if !scalarNumericProgramZoneMapConstantExpr(arg, hasOwner) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func scalarNumericProgramZoneMap(proc *process.Process, expr *plan.Expr, previous objectio.ZoneMap) objectio.ZoneMap {
	typ := types.T(expr.Typ.Id)
	if len(previous) != index.ZMSize || previous.GetType() != typ || previous.GetScale() != expr.Typ.Scale {
		previous = index.NewZM(typ, expr.Typ.Scale)
	} else {
		previous.Reset()
	}
	executor, err := NewExpressionExecutor(proc, expr)
	if err != nil {
		return previous
	}
	defer executor.Free()
	value, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil || value == nil || value.GetType().Oid != typ || value.GetType().Scale != expr.Typ.Scale ||
		value.Length() == 0 || value.IsConstNull() {
		return previous
	}
	if err := index.BatchUpdateZM(previous, value); err != nil {
		previous.Reset()
	}
	return previous
}

func (state *scalarNumericProgramState) makeKindRows(rowCount int) (*vector.Vector, error) {
	if state.kindRows == nil {
		vec, err := newExpressionOffHeapVector(types.T_bool.ToType(), state.selection)
		if err != nil {
			return nil, err
		}
		state.kindRows = vec
	}
	state.kindRows.ResetWithSameType()
	if err := state.kindRows.PreExtend(rowCount, state.mp); err != nil {
		return nil, err
	}
	provider, _ := state.value.(numericValueKindProvider)
	for row := 0; row < rowCount; row++ {
		kind := false
		if provider != nil {
			kind = provider.numericValueKindAt(row)
		}
		if err := vector.AppendFixed(state.kindRows, kind, false, state.mp); err != nil {
			return nil, err
		}
	}
	state.kindRows.SetLength(rowCount)
	return state.kindRows, nil
}

func (state *scalarNumericProgramState) resetEvaluation() {
	state.evaluated = false
	state.valueRows = nil
	state.rowCount = 0
	state.batches = nil
	state.selectList = nil
	state.nilSelect = false
}

func (state *scalarNumericProgramState) captureInvocation(
	batches []*batch.Batch, rowCount int, selectList []bool,
) {
	state.rowCount = rowCount
	state.batches = append(state.batches[:0], batches...)
	state.nilSelect = selectList == nil
	if selectList == nil {
		state.selectList = state.selectList[:0]
	} else {
		state.selectList = append(state.selectList[:0], selectList...)
	}
}

func (state *scalarNumericProgramState) sameInvocation(
	batches []*batch.Batch, rowCount int, selectList []bool,
) bool {
	if state.rowCount != rowCount || state.nilSelect != (selectList == nil) || len(state.batches) != len(batches) {
		return false
	}
	for i := range batches {
		if state.batches[i] != batches[i] {
			return false
		}
	}
	if selectList == nil {
		return true
	}
	if len(state.selectList) != len(selectList) {
		return false
	}
	for i := range selectList {
		if state.selectList[i] != selectList[i] {
			return false
		}
	}
	return true
}

func (state *scalarNumericProgramState) resetForNextQuery() {
	if state.value != nil {
		state.value.ResetForNextQuery()
	}
	if state.kindSource != nil {
		state.kindSource.ResetForNextQuery()
	}
	state.resetEvaluation()
}

func scalarNumericProgramScope(expr *plan.Expr) (int64, bool) {
	if expr == nil || expr.GetLit() == nil || expr.GetLit().Isnull {
		return 0, false
	}
	value, ok := expr.GetLit().GetValue().(*plan.Literal_I64Val)
	if !ok || value == nil || value.I64Val <= 0 {
		return 0, false
	}
	return value.I64Val, true
}

func scalarNumericProgramSlot(expr *plan.Expr) (int, bool) {
	if expr == nil || expr.GetLit() == nil || expr.GetLit().Isnull {
		return 0, false
	}
	value, ok := expr.GetLit().GetValue().(*plan.Literal_I64Val)
	if !ok || value == nil || value.I64Val < 0 || value.I64Val > 1 {
		return 0, false
	}
	return int(value.I64Val), true
}

func numericProgramDefinitionsEqual(left, right *plan.Expr) bool {
	if left == nil || right == nil {
		return false
	}
	a, errA := left.Marshal()
	b, errB := right.Marshal()
	return errA == nil && errB == nil && bytes.Equal(a, b)
}

func newScalarNumericProgramRefExecutor(
	proc *process.Process,
	planExpr *plan.Expr,
	selection *vector.AllocationAccountSelection,
	buildCtx *expressionExecutorBuildContext,
	fid int32,
) (ExpressionExecutor, error) {
	f := planExpr.GetF()
	if f == nil {
		return nil, moerr.NewInvalidInputNoCtx("invalid scalar numeric program reference")
	}
	args := f.GetArgs()
	var definition *plan.Expr
	var scopeExpr, slotExpr *plan.Expr
	if len(args) == 3 {
		definition, scopeExpr, slotExpr = args[0], args[1], args[2]
	} else if len(args) == 2 && fid == function.SCALAR_PROGRAM_REF {
		scopeExpr, slotExpr = args[0], args[1]
	} else {
		return nil, moerr.NewInvalidInputNoCtx("invalid scalar numeric program reference arity")
	}
	scope, ok := scalarNumericProgramScope(scopeExpr)
	if !ok {
		return nil, moerr.NewInvalidInputNoCtx("invalid scalar numeric program scope")
	}
	slot, ok := scalarNumericProgramSlot(slotExpr)
	if !ok {
		return nil, moerr.NewInvalidInputNoCtx("invalid scalar numeric program output slot")
	}
	if (definition != nil && slot != 0) || (definition == nil && slot != 1) {
		return nil, moerr.NewInvalidInputNoCtx("scalar numeric program definition is only permitted on slot zero")
	}
	if buildCtx.numericPrograms == nil {
		buildCtx.numericPrograms = make(map[int64]*scalarNumericProgramState)
	}
	state := buildCtx.numericPrograms[scope]
	if definition != nil {
		defFn := definition.GetF()
		if defFn == nil || defFn.Func == nil {
			return nil, moerr.NewInvalidInputNoCtx("scalar numeric program definition is missing")
		}
		defID, _ := function.DecodeOverloadID(defFn.Func.GetObj())
		defArgs := defFn.GetArgs()
		defScope, hasScope := scalarNumericProgramScopeAt(defArgs, 0)
		if defID != function.SCALAR_PROGRAM || (len(defArgs) != 2 && len(defArgs) != 3) || !hasScope || defScope != scope || defArgs[1] == nil {
			return nil, moerr.NewInvalidInputNoCtx("invalid scalar numeric program definition")
		}
		if len(defArgs) == 3 && (defArgs[2] == nil || types.T(defArgs[2].Typ.Id) != types.T_bool) {
			return nil, moerr.NewInvalidInputNoCtx("scalar numeric program kind source must be BOOL")
		}
		if !samePlanType(defArgs[1].Typ, planExpr.Typ) {
			return nil, moerr.NewInvalidInputNoCtx("scalar numeric program value output type mismatch")
		}
		if state != nil {
			if state.building || !numericProgramDefinitionsEqual(state.definition, definition) {
				return nil, moerr.NewInvalidInputNoCtx("conflicting scalar numeric program definition")
			}
		} else {
			state = &scalarNumericProgramState{
				definition: definition,
				building:   true,
				selection:  selection,
				mp:         proc.Mp(),
			}
			buildCtx.numericPrograms[scope] = state
			buildCtx.programStates = append(buildCtx.programStates, state)
			buildCtx.numericKindDemandDepth++
			valueExecutor, err := newExpressionExecutorWithAllocation(proc, defArgs[1], selection, buildCtx)
			buildCtx.numericKindDemandDepth--
			if err != nil {
				delete(buildCtx.numericPrograms, scope)
				return nil, err
			}
			state.value = valueExecutor
			if len(defArgs) == 3 {
				kindExecutor, err := newExpressionExecutorWithAllocation(proc, defArgs[2], selection, buildCtx)
				if err != nil {
					state.value.Free()
					state.value = nil
					delete(buildCtx.numericPrograms, scope)
					return nil, err
				}
				state.kindSource = kindExecutor
			}
			state.building = false
		}
	} else if state == nil || state.building {
		return nil, moerr.NewInvalidInputNoCtx("scalar numeric program reference has no lexical definition")
	}
	if state == nil || state.building || state.value == nil {
		return nil, moerr.NewInvalidInputNoCtx("incomplete scalar numeric program")
	}
	wantType := types.T(planExpr.Typ.Id)
	if slot == 1 && wantType != types.T_bool {
		return nil, moerr.NewInvalidInputNoCtx("scalar numeric program kind output must be BOOL")
	}
	state.refs++
	return &scalarNumericProgramRefExecutor{state: state, slot: slot}, nil
}

// ProjectionExpressionProgramGroup coordinates program outputs across sibling
// projection expressions. It does not own executors or vectors; each output
// reference retains the program state until its own Free call.
type ProjectionExpressionProgramGroup struct {
	programs []*scalarNumericProgramState
}

// NewProjectionExpressionExecutorsWithAllocation builds projection roots with
// independent memo state and a shared lexical-program registry. This lets a
// raw output and its ordinary BOOL passenger reuse one producer evaluation.
func NewProjectionExpressionExecutorsWithAllocation(
	proc *process.Process,
	exprs []*plan.Expr,
	selection *vector.AllocationAccountSelection,
) ([]ExpressionExecutor, *ProjectionExpressionProgramGroup, error) {
	if err := plan.RequireLegacyCollations(exprs); err != nil {
		return nil, nil, err
	}
	executors := make([]ExpressionExecutor, len(exprs))
	group := &ProjectionExpressionProgramGroup{}
	sharedPrograms := make(map[int64]*scalarNumericProgramState)
	knownPrograms := make(map[*scalarNumericProgramState]struct{})
	for i, expr := range exprs {
		buildCtx := &expressionExecutorBuildContext{numericPrograms: sharedPrograms}
		executor, err := newExpressionExecutorWithAllocation(proc, expr, selection, buildCtx)
		if err != nil {
			for j := 0; j < i; j++ {
				executors[j].Free()
			}
			return nil, nil, err
		}
		for _, state := range buildCtx.programStates {
			if _, exists := knownPrograms[state]; !exists {
				knownPrograms[state] = struct{}{}
				group.programs = append(group.programs, state)
			}
		}
		// Program lifetime and batch boundaries belong to the group. Root
		// wrappers continue to own only their local memo states.
		buildCtx.programStates = nil
		executors[i] = expressionExecutorBuildContextRoot(executor, buildCtx)
	}
	return executors, group, nil
}

// BeginBatch clears the paired-output cache once before the projection emits
// any of its sibling columns for this input batch.
func (group *ProjectionExpressionProgramGroup) BeginBatch() {
	if group == nil {
		return
	}
	for _, state := range group.programs {
		state.resetEvaluation()
	}
}

// ResetForNextQuery resets paired source executors once after projection roots
// have reset their own memo state.
func (group *ProjectionExpressionProgramGroup) ResetForNextQuery() {
	if group == nil {
		return
	}
	for _, state := range group.programs {
		state.resetForNextQuery()
	}
}

func scalarNumericProgramScopeAt(args []*plan.Expr, index int) (int64, bool) {
	if index < 0 || index >= len(args) {
		return 0, false
	}
	return scalarNumericProgramScope(args[index])
}

func samePlanType(left, right plan.Type) bool {
	leftType, leftErr := types.TypeFromPlan(left)
	rightType, rightErr := types.TypeFromPlan(right)
	return leftErr == nil && rightErr == nil && leftType == rightType
}

func numericValueKindAt(executor ExpressionExecutor, row int) bool {
	provider, ok := executor.(numericValueKindProvider)
	return ok && provider.numericValueKindAt(row)
}
