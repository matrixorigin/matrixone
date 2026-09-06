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

package colexec

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	util2 "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/datalink"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	constBType          = types.T_bool.ToType()
	constI8Type         = types.T_int8.ToType()
	constI16Type        = types.T_int16.ToType()
	constI32Type        = types.T_int32.ToType()
	constI64Type        = types.T_int64.ToType()
	constU8Type         = types.T_uint8.ToType()
	constU16Type        = types.T_uint16.ToType()
	constU32Type        = types.T_uint32.ToType()
	constU64Type        = types.T_uint64.ToType()
	constFType          = types.T_float32.ToType()
	constDType          = types.T_float64.ToType()
	constSType          = types.T_varchar.ToType()
	constBinType        = types.T_varbinary.ToType()
	constDateType       = types.T_date.ToType()
	constTimeType       = types.T_time.ToType()
	constDatetimeType   = types.T_datetime.ToType()
	constEnumType       = types.T_enum.ToType()
	constTimestampTypes = []types.Type{
		types.New(types.T_timestamp, 0, 0),
		types.New(types.T_timestamp, 0, 1),
		types.New(types.T_timestamp, 0, 2),
		types.New(types.T_timestamp, 0, 3),
		types.New(types.T_timestamp, 0, 4),
		types.New(types.T_timestamp, 0, 5),
		types.New(types.T_timestamp, 0, 6),
	}
	//No need to add T_array here, as Array is cast from varchar.
)

// ExpressionExecutor
// generated from plan.Expr, can evaluate the result from vectors directly.
type ExpressionExecutor interface {
	// Eval evaluates the expression and returns the result vector.
	// The result is a read-only vector that is only valid until the next call to Eval.
	Eval(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error)

	// EvalWithoutResultReusing will be removed in the future.
	// It's used to evaluate the expression,
	// and the result's ownership is transferred to the caller except for the column expression.
	EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error)

	// ResetForNextQuery resets the expression to its initial state for a same expression.
	// this is useful to a prepare statement.
	ResetForNextQuery()

	// Free closes the expression and releases all resources.
	Free()

	// IsColumnExpr returns true if the expression is a column expression.
	IsColumnExpr() bool

	TypeName() string
}

type memoExpressionState struct {
	executor ExpressionExecutor
	cached   *vector.Vector
	refs     int
}

type memoExpressionExecutor struct {
	state *memoExpressionState
}

func (expr *memoExpressionExecutor) Eval(
	proc *process.Process, batches []*batch.Batch, selectList []bool,
) (*vector.Vector, error) {
	if expr.state.cached != nil {
		return expr.state.cached, nil
	}
	result, err := expr.state.executor.Eval(proc, batches, selectList)
	if err == nil {
		expr.state.cached = result
	}
	return result, err
}

func (expr *memoExpressionExecutor) EvalWithoutResultReusing(
	proc *process.Process, batches []*batch.Batch, selectList []bool,
) (*vector.Vector, error) {
	return expr.Eval(proc, batches, selectList)
}

func (expr *memoExpressionExecutor) ResetForNextQuery() {}

func (expr *memoExpressionExecutor) Free() {
	expr.state.refs--
	if expr.state.refs == 0 {
		expr.state.executor.Free()
		expr.state.executor = nil
		expr.state.cached = nil
	}
}

func (expr *memoExpressionExecutor) IsColumnExpr() bool { return false }
func (expr *memoExpressionExecutor) TypeName() string   { return "memo expression" }

type memoRootExpressionExecutor struct {
	executor ExpressionExecutor
	states   []*memoExpressionState
}

func (expr *memoRootExpressionExecutor) resetCachedValues() {
	for _, state := range expr.states {
		state.cached = nil
	}
}

func (expr *memoRootExpressionExecutor) Eval(
	proc *process.Process, batches []*batch.Batch, selectList []bool,
) (*vector.Vector, error) {
	expr.resetCachedValues()
	return expr.executor.Eval(proc, batches, selectList)
}

func (expr *memoRootExpressionExecutor) EvalWithoutResultReusing(
	proc *process.Process, batches []*batch.Batch, selectList []bool,
) (*vector.Vector, error) {
	expr.resetCachedValues()
	return expr.executor.EvalWithoutResultReusing(proc, batches, selectList)
}

func (expr *memoRootExpressionExecutor) ResetForNextQuery() {
	expr.executor.ResetForNextQuery()
	for _, state := range expr.states {
		state.executor.ResetForNextQuery()
	}
}

func (expr *memoRootExpressionExecutor) Free()              { expr.executor.Free() }
func (expr *memoRootExpressionExecutor) IsColumnExpr() bool { return expr.executor.IsColumnExpr() }
func (expr *memoRootExpressionExecutor) TypeName() string   { return expr.executor.TypeName() }

type expressionExecutorBuildContext struct {
	memos  map[int32]*memoExpressionState
	states []*memoExpressionState
}

func NewExpressionExecutorsFromPlanExpressions(proc *process.Process, planExprs []*plan.Expr) (executors []ExpressionExecutor, err error) {
	return NewExpressionExecutorsFromPlanExpressionsWithAllocation(proc, planExprs, nil)
}

// NewExpressionExecutorsFromPlanExpressionsWithAllocation builds a complete
// expression tree whose owned MPool vectors use one immutable allocation
// selection. Borrowed input vectors remain owned and charged by their source.
func NewExpressionExecutorsFromPlanExpressionsWithAllocation(
	proc *process.Process,
	planExprs []*plan.Expr,
	selection *vector.AllocationAccountSelection,
) (executors []ExpressionExecutor, err error) {
	executors = make([]ExpressionExecutor, len(planExprs))
	for i := range executors {
		executors[i], err = NewExpressionExecutorWithAllocation(proc, planExprs[i], selection)
		if err != nil {
			for j := 0; j < i; j++ {
				executors[j].Free()
			}
			return nil, err
		}
	}
	return executors, err
}

func NewExpressionExecutor(proc *process.Process, planExpr *plan.Expr) (ExpressionExecutor, error) {
	return NewExpressionExecutorWithAllocation(proc, planExpr, nil)
}

func NewExpressionExecutorWithAllocation(
	proc *process.Process,
	planExpr *plan.Expr,
	selection *vector.AllocationAccountSelection,
) (ExpressionExecutor, error) {
	buildCtx := &expressionExecutorBuildContext{}
	executor, err := newExpressionExecutorWithAllocation(proc, planExpr, selection, buildCtx)
	if err != nil || len(buildCtx.states) == 0 {
		return executor, err
	}
	return &memoRootExpressionExecutor{executor: executor, states: buildCtx.states}, nil
}

func newExpressionExecutorWithAllocation(
	proc *process.Process,
	planExpr *plan.Expr,
	selection *vector.AllocationAccountSelection,
	buildCtx *expressionExecutorBuildContext,
) (ExpressionExecutor, error) {
	if planExpr.AuxId < 0 {
		if buildCtx.memos == nil {
			buildCtx.memos = make(map[int32]*memoExpressionState)
		}
		if state, ok := buildCtx.memos[planExpr.AuxId]; ok {
			state.refs++
			return &memoExpressionExecutor{state: state}, nil
		}
		withoutMemo := *planExpr
		withoutMemo.AuxId = 0
		executor, err := newExpressionExecutorWithAllocation(proc, &withoutMemo, selection, buildCtx)
		if err != nil {
			return nil, err
		}
		state := &memoExpressionState{executor: executor, refs: 1}
		buildCtx.memos[planExpr.AuxId] = state
		buildCtx.states = append(buildCtx.states, state)
		return &memoExpressionExecutor{state: state}, nil
	}
	switch t := planExpr.Expr.(type) {
	case *plan.Expr_Lit:
		typ := types.NewWithCharset(
			types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale, uint8(planExpr.Typ.Charset),
		)
		vec, err := generateConstExpressionExecutor(proc, typ, t.Lit, selection)
		if err != nil {
			return nil, err
		}
		return NewFixedVectorExpressionExecutor(proc.Mp(), false, vec), nil

	case *plan.Expr_T:
		typ := types.NewWithCharset(
			types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale, uint8(planExpr.Typ.Charset),
		)
		vec, err := newExpressionConstNull(typ, 1, selection)
		if err != nil {
			return nil, err
		}
		return NewFixedVectorExpressionExecutor(proc.Mp(), false, vec), nil

	case *plan.Expr_Col:
		typ := types.NewWithCharset(
			types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale, uint8(planExpr.Typ.Charset),
		)
		ce := NewColumnExpressionExecutor()
		*ce = ColumnExpressionExecutor{
			mp:         proc.Mp(),
			relIndex:   int(t.Col.RelPos),
			colIndex:   int(t.Col.ColPos),
			typ:        typ,
			allocation: selection,
		}
		// [issue#19574]
		// if < 0, it's special for agg or others.
		if ce.relIndex < 0 {
			ce.relIndex = 0
		}
		return ce, nil

	case *plan.Expr_P:
		typ := types.NewWithCharset(
			types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale, uint8(planExpr.Typ.Charset),
		)
		executor := NewParamExpressionExecutor(proc.Mp(), int(t.P.Pos), typ)
		executor.allocation = selection
		return executor, nil

	case *plan.Expr_V:
		typ := types.NewWithCharset(
			types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale, uint8(planExpr.Typ.Charset),
		)
		ve := NewVarExpressionExecutor()
		*ve = VarExpressionExecutor{
			mp:         proc.Mp(),
			name:       t.V.Name,
			system:     t.V.System,
			global:     t.V.Global,
			typ:        typ,
			allocation: selection,
		}
		return ve, nil

	case *plan.Expr_Vec:
		vec := vector.NewVec(types.T_any.ToType())
		err := vec.UnmarshalBinary(t.Vec.Data)
		if err != nil {
			vec.Free(proc.Mp())
			return nil, err
		}
		// The stable Vector payload does not carry runtime provenance. LiteralVec
		// explicitly records its uniform owner because the same container is used
		// for SQL constants and runtime-filter payloads.
		rawSource := t.Vec.GetStringSource()
		if rawSource > uint32(types.StringSourceCOMStmt) {
			vec.Free(proc.Mp())
			return nil, moerr.NewInvalidInputf(proc.Ctx, "invalid literal vector string source %d", rawSource)
		}
		source := types.StringSource(rawSource)
		if err := vec.SetStringSource(source); err != nil {
			vec.Free(proc.Mp())
			return nil, err
		}
		return NewFixedVectorExpressionExecutor(proc.Mp(), true, vec), nil

	case *plan.Expr_List:
		executor := NewListExpressionExecutor()
		resultVecTyp := t.List.List[0].GetTyp()
		typ := types.NewWithCharset(
			types.T(resultVecTyp.Id), resultVecTyp.Width, resultVecTyp.Scale, uint8(resultVecTyp.Charset),
		)
		if err := executor.init(proc, typ, len(t.List.List), selection); err != nil {
			executor.Free()
			return nil, err
		}
		for i := range executor.parameterExecutor {
			subExecutor, paramErr := newExpressionExecutorWithAllocation(proc, t.List.List[i], selection, buildCtx)
			if paramErr != nil {
				executor.Free()
				return nil, paramErr
			}
			executor.SetParameter(i, subExecutor)
		}
		return executor, nil

	case *plan.Expr_F:
		overloadID := t.F.GetFunc().GetObj()
		overload, err := function.GetFunctionById(proc.Ctx, overloadID)
		if err != nil {
			return nil, err
		}

		executor := NewFunctionExpressionExecutor()
		{
			// init function folding status.
			executor.folded.reset(proc.Mp())
		}
		{
			// init function information for evaluation.
			executor.overloadID = overloadID
			// String-to-numeric casts can emit one warning for every logical
			// output row. Do not fold ordinary text parameters, but retain the
			// cast information so doFold can safely fold parameters whose
			// protocol metadata proves that they originated as integers.
			executor.stringToNumericCast = !overload.CannotFold() && isStringToNumericCast(planExpr)
			executor.volatile = overload.CannotFold() || executor.stringToNumericCast
			executor.timeDependent = overload.IsRealTimeRelated()
			executor.fid, _ = function.DecodeOverloadID(overloadID)
			executor.evalFn, executor.resetFn, executor.freeFn, executor.retainedBytesFn = overload.GetExecuteMethod()
		}
		typ := types.NewWithCharset(
			types.T(planExpr.Typ.Id), planExpr.Typ.Width, planExpr.Typ.Scale, uint8(planExpr.Typ.Charset),
		)

		if err = executor.init(proc, len(t.F.Args), typ, selection); err != nil {
			executor.Free()
			return nil, err
		}

		for i := range executor.parameterExecutor {
			subExecutor, paramErr := newExpressionExecutorWithAllocation(proc, t.F.Args[i], selection, buildCtx)
			if paramErr != nil {
				executor.Free()
				return nil, paramErr
			}
			executor.SetParameter(i, subExecutor)
		}
		return executor, nil
	}

	return nil, moerr.NewNYI(proc.Ctx, fmt.Sprintf("unsupported expression executor for %v now", planExpr))
}

func isStringToNumericCast(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	f := expr.GetF()
	if f == nil || f.Func == nil || len(f.Args) == 0 {
		return false
	}
	switch f.Func.GetObjName() {
	case "cast", "cast_strict", "cast_assign", "cast_ignore":
	default:
		return false
	}
	source := types.T(f.Args[0].Typ.Id)
	target := types.T(expr.Typ.Id)
	return source.IsMySQLString() && target.ToType().IsNumeric()
}

func newExpressionOffHeapVector(
	typ types.Type,
	selection *vector.AllocationAccountSelection,
) (*vector.Vector, error) {
	if selection == nil {
		return vector.NewOffHeapVecWithType(typ), nil
	}
	return vector.NewOffHeapVecWithTypeAndAllocation(typ, selection)
}

func newExpressionConstNull(
	typ types.Type,
	length int,
	selection *vector.AllocationAccountSelection,
) (*vector.Vector, error) {
	if selection == nil {
		return vector.NewConstNull(typ, length, nil), nil
	}
	return vector.NewConstNullWithAllocation(typ, length, selection)
}

func newExpressionConstFixed[T any](
	typ types.Type,
	value T,
	length int,
	mp *mpool.MPool,
	selection *vector.AllocationAccountSelection,
) (*vector.Vector, error) {
	if selection == nil {
		return vector.NewConstFixed(typ, value, length, mp)
	}
	return vector.NewConstFixedWithAllocation(typ, value, length, mp, selection)
}

func newExpressionConstBytes(
	typ types.Type,
	value []byte,
	length int,
	mp *mpool.MPool,
	selection *vector.AllocationAccountSelection,
) (*vector.Vector, error) {
	if selection == nil {
		return vector.NewConstBytes(typ, value, length, mp)
	}
	return vector.NewConstBytesWithAllocation(typ, value, length, mp, selection)
}

func newExpressionConstArray[T types.ArrayElement](
	typ types.Type,
	value []T,
	length int,
	mp *mpool.MPool,
	selection *vector.AllocationAccountSelection,
) (*vector.Vector, error) {
	if selection == nil {
		return vector.NewConstArray(typ, value, length, mp)
	}
	return vector.NewConstArrayWithAllocation(typ, value, length, mp, selection)
}

// FixedVectorExpressionExecutor
// the content of its vector is fixed.
// e.g.
//
//	ConstVector [1, 1, 1, 1, 1]
//	ConstVector [null, null, null]
//	ListVector  ["1", "2", "3", null, "5"]
type FixedVectorExpressionExecutor struct {
	m *mpool.MPool

	noNeedToSetLength bool
	resultVector      *vector.Vector
}

type FunctionExpressionExecutor struct {
	m          *mpool.MPool
	allocation *vector.AllocationAccountSelection
	// resultType is the declared function return type. Some built-ins refine
	// result metadata (for example temporal scale or decimal width/scale) at
	// runtime, so reusable result vectors must start each evaluation from this
	// stable type before the function applies the current runtime metadata.
	resultType types.Type
	functionInformationForEval
	folded      functionFolding
	selectList1 []bool
	selectList2 []bool
	selectList  function.FunctionSelectList

	// A function implementation cannot be required to interpret selectList
	// correctly: many built-ins predate it, and evaluating them on masked rows
	// can still raise errors or perform side effects. For a partial selection we
	// therefore compact row-aligned parameters, evaluate only selected rows, and
	// scatter the result back to the original row positions. These buffers are
	// allocated lazily and reused across batches.
	selectedRows             []int64
	selectedParameterResults []*vector.Vector
	selectedParameterVectors []*vector.Vector
	selectedResult           vector.FunctionResultWrapper
	selectedNullResult       *vector.Vector

	resultVector vector.FunctionResultWrapper
	// parameters related
	parameterResults         []*vector.Vector
	parameterExecutor        []ExpressionExecutor
	flowControlKind          vector.PrepareParamKind
	flowControlKindSeen      bool
	flowControlKinds         []vector.PrepareParamKind
	flowControlStringDomains []types.RuntimeStringDomain
	flowControlStringSources []types.StringSource
	iffNullResults           [2]*vector.Vector
}

type ColumnExpressionExecutor struct {
	mp         *mpool.MPool
	allocation *vector.AllocationAccountSelection
	relIndex   int
	colIndex   int

	// result type.
	typ types.Type
	// we should new and cache a null vector here.
	// because we need to change its type when doing the execution for const null vector.
	// but other process may using its type at the same time.
	nullVecCache *vector.Vector
}

func (expr *ColumnExpressionExecutor) GetRelIndex() int {
	return expr.relIndex
}

func (expr *ColumnExpressionExecutor) GetColIndex() int {
	return expr.colIndex
}

type ParamExpressionExecutor struct {
	mp         *mpool.MPool
	allocation *vector.AllocationAccountSelection
	null       *vector.Vector
	// maskedNull is separate from null/vec because it is not a resolved
	// parameter value and must never participate in the folded-value cache.
	maskedNull *vector.Vector
	vec        *vector.Vector
	pos        int
	typ        types.Type

	folded     bool
	foldedNull bool
}

func (expr *ParamExpressionExecutor) Eval(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	// Scalar values stay physically constant, but their logical cardinality
	// must follow the input batch for downstream materialization and shuffle.
	rowCount := expressionRowCount(batches)
	if noRowsSelected(selectList, rowCount) {
		if expr.maskedNull == nil {
			var err error
			expr.maskedNull, err = newExpressionConstNull(expr.typ, 1, expr.allocation)
			if err != nil {
				return nil, err
			}
		}
		expr.maskedNull.SetLength(rowCount)
		return expr.maskedNull, nil
	}
	if expr.folded {
		if expr.foldedNull {
			expr.null.SetLength(rowCount)
			return expr.null, nil
		}
		expr.vec.SetLength(rowCount)
		return expr.vec, nil
	}

	val, err := proc.GetPrepareParamsAt(expr.pos)
	if err != nil {
		return nil, err
	}

	if val == nil {
		if expr.null == nil {
			expr.null, err = newExpressionConstNull(expr.typ, 1, expr.allocation)
			if err != nil {
				return nil, err
			}
		}
		expr.folded = true
		expr.foldedNull = true
		if params := proc.GetPrepareParams(); params != nil {
			if err = expr.null.SetStringSource(params.GetStringSourceAt(expr.pos)); err != nil {
				return nil, err
			}
		}
		expr.null.SetLength(rowCount)
		return expr.null, nil
	}

	if expr.vec == nil {
		expr.vec, err = newExpressionConstBytes(
			expr.typ, val, 1, proc.Mp(), expr.allocation,
		)
	} else {
		err = vector.SetConstBytes(expr.vec, val, 1, proc.GetMPool())
	}
	if err == nil {
		expr.vec.SetIsBin(proc.GetPrepareParamIsBin(expr.pos))
		expr.vec.SetIsBinaryString(proc.GetPrepareParamIsBinaryString(expr.pos))
		expr.vec.SetPrepareParamKind(proc.GetPrepareParamKind(expr.pos))
		if params := proc.GetPrepareParams(); params != nil {
			err = expr.vec.SetStringSource(params.GetStringSourceAt(expr.pos))
		}
		if err != nil {
			return nil, err
		}
		expr.vec.SetPrepareParamType(proc.GetPrepareParamType(expr.pos))
		expr.folded = true
		expr.foldedNull = false
		expr.vec.SetLength(rowCount)
	}
	return expr.vec, err
}

func (expr *ParamExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches, nil)
	if err != nil {
		return nil, err
	}
	if vec == expr.null {
		expr.null = nil
	} else {
		expr.vec = nil
	}
	expr.folded = false
	expr.foldedNull = false
	return vec, nil
}

func (expr *ParamExpressionExecutor) Free() {
	if expr == nil {
		return
	}

	if expr.vec != nil {
		expr.vec.Free(expr.mp)
		expr.vec = nil
	}
	if expr.null != nil {
		expr.null.Free(expr.mp)
		expr.null = nil
	}
	if expr.maskedNull != nil {
		expr.maskedNull.Free(expr.mp)
		expr.maskedNull = nil
	}
	reuse.Free[ParamExpressionExecutor](expr, nil)
}

func (expr *ParamExpressionExecutor) IsColumnExpr() bool {
	return false
}

type VarExpressionExecutor struct {
	mp         *mpool.MPool
	allocation *vector.AllocationAccountSelection
	null       *vector.Vector
	// maskedNull lets a skipped variable avoid the resolver without changing
	// the value cache used by a later selected evaluation.
	maskedNull *vector.Vector
	vec        *vector.Vector

	name   string
	system bool
	global bool
	typ    types.Type
}

func (expr *VarExpressionExecutor) Eval(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	// Scalar values stay physically constant, but their logical cardinality
	// must follow the input batch for downstream materialization and shuffle.
	rowCount := expressionRowCount(batches)
	if noRowsSelected(selectList, rowCount) {
		if expr.maskedNull == nil {
			var err error
			expr.maskedNull, err = newExpressionConstNull(expr.typ, 1, expr.allocation)
			if err != nil {
				return nil, err
			}
		}
		expr.maskedNull.SetLength(rowCount)
		return expr.maskedNull, nil
	}
	resolveVariableFunc := proc.GetResolveVariableFunc()
	if resolveVariableFunc == nil {
		return nil, moerr.NewInternalErrorf(proc.Ctx, "resolve variable function is not set for variable %s", expr.name)
	}
	val, err := resolveVariableFunc(expr.name, expr.system, expr.global)
	if err != nil {
		return nil, err
	}
	isBin := false
	if resolveIsBin := proc.GetResolveVariableIsBinFunc(); resolveIsBin != nil {
		isBin, err = resolveIsBin(expr.name, expr.system, expr.global)
		if err != nil {
			return nil, err
		}
	}
	prepareParamKind := vector.PrepareParamNone
	if resolveKind := proc.GetResolveVariablePrepareParamKindFunc(); resolveKind != nil {
		prepareParamKind, err = resolveKind(expr.name, expr.system, expr.global)
		if err != nil {
			return nil, err
		}
	}

	if val == nil {
		if expr.null == nil {
			expr.null, err = util.GenVectorByVarValueWithAllocation(
				proc, expr.typ, nil, expr.allocation,
			)
		}
		if err == nil {
			expr.null.SetIsBin(isBin)
			expr.null.SetPrepareParamKind(prepareParamKind)
			err = expr.null.SetStringSource(types.StringSourceUserVariable)
			expr.null.SetLength(rowCount)
		}
		return expr.null, err
	}

	if expr.vec == nil {
		expr.vec, err = util.GenVectorByVarValueWithAllocation(
			proc, expr.typ, val, expr.allocation,
		)
	} else if !expr.typ.IsVarlen() || expr.typ.Oid == types.T_json || expr.typ.Oid.IsArrayRelate() {
		// Fixed-width user-variable values (including DECIMAL), JSON values, and
		// array/vector values need typed reconstruction on every evaluation.
		// Reusing the generic varlena update path would either reinterpret a
		// fixed vector's backing memory as a Varlena, skip JSON's binary
		// encoding, or replace array element bytes with display text such as
		// "[1 2 3]".
		expr.vec.Free(expr.mp)
		expr.vec, err = util.GenVectorByVarValueWithAllocation(
			proc, expr.typ, val, expr.allocation,
		)
	} else {
		switch v := val.(type) {
		case []byte:
			err = vector.SetConstBytes(expr.vec, v, 1, proc.GetMPool())
		case string:
			err = vector.SetConstBytes(expr.vec, util2.UnsafeStringToBytes(v), 1, proc.GetMPool())
		default:
			err = vector.SetConstBytes(expr.vec, util2.UnsafeStringToBytes(fmt.Sprintf("%v", v)), 1, proc.GetMPool())
		}
	}
	if err == nil {
		expr.vec.SetIsBin(isBin)
		expr.vec.SetPrepareParamKind(prepareParamKind)
		err = expr.vec.SetStringSource(types.StringSourceUserVariable)
		expr.vec.SetLength(rowCount)
	}
	return expr.vec, err
}

func (expr *VarExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches, nil)
	if err != nil {
		return nil, err
	}
	if vec == expr.null {
		expr.null = nil
		return vec, nil
	}
	expr.vec = nil
	return vec, nil
}

func (expr *VarExpressionExecutor) Free() {
	if expr == nil {
		return
	}
	if expr.vec != nil {
		expr.vec.Free(expr.mp)
		expr.vec = nil
	}
	if expr.null != nil {
		expr.null.Free(expr.mp)
		expr.null = nil
	}
	if expr.maskedNull != nil {
		expr.maskedNull.Free(expr.mp)
		expr.maskedNull = nil
	}
	reuse.Free[VarExpressionExecutor](expr, nil)
}

func (expr *VarExpressionExecutor) IsColumnExpr() bool {
	return false
}

type ListExpressionExecutor struct {
	mp         *mpool.MPool
	allocation *vector.AllocationAccountSelection

	typ               types.Type
	resultVector      *vector.Vector
	parameterExecutor []ExpressionExecutor
}

func (expr *ListExpressionExecutor) Eval(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error) {

	if expr.resultVector == nil {
		var err error
		expr.resultVector, err = newExpressionOffHeapVector(expr.typ, expr.allocation)
		if err != nil {
			return nil, err
		}
	} else {
		expr.resultVector.CleanOnlyData()
	}
	if err := expr.resultVector.PreExtend(len(expr.parameterExecutor), proc.Mp()); err != nil {
		return nil, err
	}
	for i := range expr.parameterExecutor {
		vec, err := expr.parameterExecutor[i].Eval(proc, batches, selectList)
		if err != nil {
			return nil, err
		}
		err = expr.resultVector.UnionOne(vec, 0, expr.mp)
		if err != nil {
			return nil, err
		}
	}
	expr.resultVector.SetLength(len(expr.parameterExecutor))
	return expr.resultVector, nil
}

func (expr *ListExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches, nil)
	if err != nil {
		return nil, err
	}
	expr.resultVector = nil
	return vec, nil
}

func (expr *ListExpressionExecutor) Free() {
	if expr == nil {
		return
	}
	for _, e := range expr.parameterExecutor {
		if e != nil {
			e.Free()
		}
	}
	if expr.resultVector != nil {
		expr.resultVector.Free(expr.mp)
		expr.resultVector = nil
	}
	reuse.Free[ListExpressionExecutor](expr, nil)
}

func (expr *ListExpressionExecutor) IsColumnExpr() bool {
	return false
}

func (expr *ListExpressionExecutor) Init(proc *process.Process, typ types.Type, parameterNum int) {
	if err := expr.init(proc, typ, parameterNum, nil); err != nil {
		panic(err)
	}
}

func (expr *ListExpressionExecutor) init(
	proc *process.Process,
	typ types.Type,
	parameterNum int,
	selection *vector.AllocationAccountSelection,
) error {
	m := proc.Mp()

	expr.typ = typ
	expr.mp = m
	expr.allocation = selection
	expr.parameterExecutor = make([]ExpressionExecutor, parameterNum)
	var err error
	expr.resultVector, err = newExpressionOffHeapVector(typ, selection)
	return err
}

func (expr *ListExpressionExecutor) SetParameter(index int, executor ExpressionExecutor) {
	expr.parameterExecutor[index] = executor
}

func (expr *ListExpressionExecutor) ResetForNextQuery() {
	for _, e := range expr.parameterExecutor {
		e.ResetForNextQuery()
	}
}

func (expr *FunctionExpressionExecutor) Init(
	proc *process.Process,
	parameterNum int,
	retType types.Type) (err error) {
	return expr.init(proc, parameterNum, retType, nil)
}

func (expr *FunctionExpressionExecutor) init(
	proc *process.Process,
	parameterNum int,
	retType types.Type,
	selection *vector.AllocationAccountSelection,
) (err error) {
	m := proc.Mp()

	expr.m = m
	expr.allocation = selection
	expr.resultType = retType
	expr.parameterResults = make([]*vector.Vector, parameterNum)
	expr.parameterExecutor = make([]ExpressionExecutor, parameterNum)

	expr.resultVector, err = vector.NewFunctionResultWrapperWithAllocation(retType, m, selection)
	return err
}

func (expr *FunctionExpressionExecutor) resetResultType(result vector.FunctionResultWrapper) {
	if result == nil {
		return
	}
	if vec := result.GetResultVector(); vec != nil {
		vec.SetType(expr.resultType)
		vec.SetIsBin(false)
	}
}

func expressionRowCount(batches []*batch.Batch) int {
	if len(batches) > 0 {
		return batches[0].RowCount()
	}
	return 1
}

func (expr *FunctionExpressionExecutor) EvalIff(proc *process.Process, batches []*batch.Batch, selectList []bool) (err error) {
	expr.resetFlowControlPrepareParamKind()
	expr.parameterResults[0], err = expr.parameterExecutor[0].Eval(proc, batches, selectList)
	if err != nil {
		return err
	}
	rowCount := expressionRowCount(batches)
	if len(expr.selectList1) < rowCount {
		expr.selectList1 = make([]bool, rowCount)
		expr.selectList2 = make([]bool, rowCount)
	}

	trueBranch := expr.selectList1[:rowCount]
	falseBranch := expr.selectList2[:rowCount]
	mode := function.CompatibilityModeFromProcess(proc)

	for i := 0; i < rowCount; i++ {
		if selectList != nil {
			trueBranch[i] = selectList[i]
			falseBranch[i] = selectList[i]
		} else {
			trueBranch[i] = true
			falseBranch[i] = true
		}
		if !trueBranch[i] {
			continue
		}
		truth, err := function.IffConditionTruthyAt(expr.parameterResults[0], uint64(i), mode)
		if err != nil {
			return err
		}
		if truth {
			falseBranch[i] = false
		} else {
			trueBranch[i] = false
		}
	}

	if hasSelectedRows(trueBranch) {
		expr.parameterResults[1], err = expr.parameterExecutor[1].Eval(proc, batches, trueBranch)
		if err != nil {
			return err
		}
		expr.observeFlowControlPrepareParamKind(expr.parameterResults[1], expr.parameterExecutor[1], trueBranch)
	} else {
		expr.parameterResults[1], err = expr.iffNullResult(0, rowCount)
		if err != nil {
			return err
		}
	}
	if hasSelectedRows(falseBranch) {
		expr.parameterResults[2], err = expr.parameterExecutor[2].Eval(proc, batches, falseBranch)
		if err != nil {
			return err
		}
		expr.observeFlowControlPrepareParamKind(expr.parameterResults[2], expr.parameterExecutor[2], falseBranch)
		return nil
	}
	expr.parameterResults[2], err = expr.iffNullResult(1, rowCount)
	return err
}

func hasSelectedRows(selectList []bool) bool {
	for _, selected := range selectList {
		if selected {
			return true
		}
	}
	return false
}

func (expr *FunctionExpressionExecutor) resetFlowControlPrepareParamKind() {
	expr.flowControlKind = vector.PrepareParamNone
	expr.flowControlKindSeen = false
	if expr.flowControlKinds != nil {
		expr.flowControlKinds = expr.flowControlKinds[:0]
	}
	if expr.flowControlStringDomains != nil {
		expr.flowControlStringDomains = expr.flowControlStringDomains[:0]
	}
	if expr.flowControlStringSources != nil {
		expr.flowControlStringSources = expr.flowControlStringSources[:0]
	}
}

func (expr *FunctionExpressionExecutor) ensureFlowControlPrepareParamRows(rows int) {
	if rows <= len(expr.flowControlKinds) {
		return
	}
	old := len(expr.flowControlKinds)
	if rows <= cap(expr.flowControlKinds) {
		expr.flowControlKinds = expr.flowControlKinds[:rows]
		clear(expr.flowControlKinds[old:])
		return
	}
	expr.flowControlKinds = append(expr.flowControlKinds, make([]vector.PrepareParamKind, rows-old)...)
}

func (expr *FunctionExpressionExecutor) ensureFlowControlBinaryStringRows(rows int) {
	if rows <= len(expr.flowControlStringDomains) {
		return
	}
	old := len(expr.flowControlStringDomains)
	if rows <= cap(expr.flowControlStringDomains) {
		expr.flowControlStringDomains = expr.flowControlStringDomains[:rows]
		clear(expr.flowControlStringDomains[old:])
		return
	}
	expr.flowControlStringDomains = append(expr.flowControlStringDomains, make([]types.RuntimeStringDomain, rows-old)...)
}

func (expr *FunctionExpressionExecutor) ensureFlowControlStringSourceRows(rows int) {
	if rows <= len(expr.flowControlStringSources) {
		return
	}
	old := len(expr.flowControlStringSources)
	if rows <= cap(expr.flowControlStringSources) {
		expr.flowControlStringSources = expr.flowControlStringSources[:rows]
		clear(expr.flowControlStringSources[old:])
		return
	}
	expr.flowControlStringSources = append(
		expr.flowControlStringSources, make([]types.StringSource, rows-old)...)
}

// observeFlowControlPrepareParamKind inspects only rows that can reach one
// IF/CASE/COALESCE arm. Column executors intentionally return their full input
// vector even under a row mask, so checking value.AllNull() would incorrectly
// observe non-NULL values from inactive rows.
func (expr *FunctionExpressionExecutor) observeFlowControlPrepareParamKind(
	value *vector.Vector,
	executor ExpressionExecutor,
	selection []bool,
) {
	value = flowControlSelectedValueSource(value, executor)
	if value == nil || value.Length() == 0 {
		return
	}
	resultDomain := types.StaticStringDomain(expr.resultType)
	if !value.HasNull() && !value.HasBinaryStringMetadata() &&
		!value.HasPrepareParamKind() && len(value.GetPrepareParamKinds()) == 0 &&
		!value.HasStringSourceMetadata() &&
		types.StaticStringDomain(*value.GetType()) == resultDomain &&
		len(expr.flowControlKinds) == 0 &&
		(!expr.flowControlKindSeen || expr.flowControlKind == vector.PrepareParamNone) {
		expr.flowControlKind = vector.PrepareParamNone
		expr.flowControlKindSeen = true
		return
	}
	for row, selected := range selection {
		if selected && (value.IsConst() || row < value.Length()) {
			// Source follows selected-value only for COALESCE. IF/CASE (including
			// IFNULL rewritten to CASE) are common-domain expressions and own it.
			// Runtime domain and conversion kind remain independent axes.
			if expr.fid == function.COALESCE {
				source := value.GetStringSourceAt(row)
				if source != types.StringSourceExpression || len(expr.flowControlStringSources) != 0 {
					expr.ensureFlowControlStringSourceRows(len(selection))
					expr.flowControlStringSources[row] = source
				}
			}
			if value.IsNull(uint64(row)) {
				continue
			}
			domain := value.GetRuntimeStringDomainAt(row)
			if domain == types.RuntimeStringInherit {
				switch staticDomain := types.StaticStringDomain(*value.GetType()); {
				case staticDomain == types.StringDomainText && resultDomain == types.StringDomainBinary:
					domain = types.RuntimeStringText
				case staticDomain == types.StringDomainBinary && resultDomain == types.StringDomainText:
					domain = types.RuntimeStringBinary
				}
			} else if (domain == types.RuntimeStringText && resultDomain == types.StringDomainText) ||
				(domain == types.RuntimeStringBinary && resultDomain == types.StringDomainBinary) {
				domain = types.RuntimeStringInherit
			}
			if domain != types.RuntimeStringInherit || len(expr.flowControlStringDomains) != 0 {
				expr.ensureFlowControlBinaryStringRows(len(selection))
				expr.flowControlStringDomains[row] = domain
			}
			kind := value.GetPrepareParamKindAt(row)
			if !expr.flowControlKindSeen {
				expr.flowControlKind = kind
				expr.flowControlKindSeen = true
			} else if len(expr.flowControlKinds) == 0 && expr.flowControlKind != kind {
				expr.ensureFlowControlPrepareParamRows(len(selection))
				for i := range expr.flowControlKinds {
					expr.flowControlKinds[i] = expr.flowControlKind
				}
				expr.flowControlKind = vector.PrepareParamNone
			}
			if len(expr.flowControlKinds) > 0 {
				expr.flowControlKinds[row] = kind
			}
		}
	}
}

// flowControlSelectedValueSource unwraps only binder-inserted casts. Explicit
// CAST uses overload 1 and remains a semantic boundary. An implicit cast has
// already evaluated its source, so this does not execute an expression twice.
func flowControlSelectedValueSource(value *vector.Vector, executor ExpressionExecutor) *vector.Vector {
	for {
		fn, ok := executor.(*FunctionExpressionExecutor)
		if !ok || fn.fid != function.CAST || len(fn.parameterResults) == 0 ||
			len(fn.parameterExecutor) == 0 || fn.parameterResults[0] == nil {
			return value
		}
		_, overload := function.DecodeOverloadID(fn.overloadID)
		if overload != 0 {
			return value
		}
		value = fn.parameterResults[0]
		executor = fn.parameterExecutor[0]
	}
}

func (expr *FunctionExpressionExecutor) applyFlowControlPrepareParamKinds(
	result *vector.Vector,
	rows int,
	mp *mpool.MPool,
) error {
	if result == nil || rows <= 0 {
		return nil
	}
	if len(expr.flowControlStringDomains) != 0 {
		expr.ensureFlowControlBinaryStringRows(rows)
		if err := result.SetRuntimeStringDomainsWithMP(expr.flowControlStringDomains[:rows], mp); err != nil {
			return err
		}
	}
	if len(expr.flowControlStringSources) != 0 {
		expr.ensureFlowControlStringSourceRows(rows)
		if err := result.SetStringSourcesWithMP(expr.flowControlStringSources[:rows], mp); err != nil {
			return err
		}
	}
	if len(expr.flowControlKinds) == 0 {
		if expr.flowControlKindSeen {
			result.SetPrepareParamKind(expr.flowControlKind)
		}
		return nil
	}
	expr.ensureFlowControlPrepareParamRows(rows)
	if err := result.SetPrepareParamKindsWithMP(expr.flowControlKinds[:rows], mp); err != nil {
		return err
	}
	return nil
}

func (expr *FunctionExpressionExecutor) isImplicitCast() bool {
	if expr.fid != function.CAST {
		return false
	}
	_, overload := function.DecodeOverloadID(expr.overloadID)
	return overload == 0
}

func applyTransparentStringSource(
	result *vector.Vector,
	source *vector.Vector,
	rows int,
	mp *mpool.MPool,
) error {
	if result == nil || source == nil || rows <= 0 {
		return nil
	}
	if source.GetStringSources() == nil {
		return result.SetStringSource(source.GetStringSource())
	}
	sources := make([]types.StringSource, rows)
	for row := range sources {
		sources[row] = source.GetStringSourceAt(row)
	}
	return result.SetStringSourcesWithMP(sources, mp)
}

func (expr *FunctionExpressionExecutor) getFlowControlPrepareParamKind() vector.PrepareParamKind {
	if !expr.flowControlKindSeen {
		return vector.PrepareParamNone
	}
	return expr.flowControlKind
}

func (expr *FunctionExpressionExecutor) iffNullResult(index, length int) (*vector.Vector, error) {
	typ := expr.resultType
	result := expr.iffNullResults[index]
	if result == nil || *result.GetType() != typ {
		if result != nil {
			result.Free(expr.m)
			expr.iffNullResults[index] = nil
		}
		var err error
		result, err = newExpressionConstNull(typ, length, expr.allocation)
		if err != nil {
			return nil, err
		}
		expr.iffNullResults[index] = result
	} else {
		result.SetLength(length)
	}
	return result, nil
}

func (expr *FunctionExpressionExecutor) EvalCase(proc *process.Process, batches []*batch.Batch, selectList []bool) (err error) {
	expr.resetFlowControlPrepareParamKind()
	rowCount := expressionRowCount(batches)
	if len(expr.selectList1) < rowCount {
		expr.selectList1 = make([]bool, rowCount)
		expr.selectList2 = make([]bool, rowCount)
	}
	remaining := expr.selectList1[:rowCount]
	selectedBranch := expr.selectList2[:rowCount]
	if selectList != nil {
		copy(remaining, selectList)
	} else {
		for i := range remaining {
			remaining[i] = true
		}
	}
	for i := 0; i < len(expr.parameterExecutor); i += 2 {
		expr.parameterResults[i], err = expr.parameterExecutor[i].Eval(proc, batches, remaining)
		if err != nil {
			return err
		}
		if i != len(expr.parameterExecutor)-1 {
			bs := vector.GenerateFunctionFixedTypeParameter[bool](expr.parameterResults[i])

			for j := 0; j < rowCount; j++ {
				b, null := bs.GetValue(uint64(j))
				if remaining[j] && !null && b {
					remaining[j] = false
					selectedBranch[j] = true
				} else {
					selectedBranch[j] = false
				}
			}
			expr.parameterResults[i+1], err = expr.parameterExecutor[i+1].Eval(proc, batches, selectedBranch)
			if err != nil {
				return err
			}
			expr.observeFlowControlPrepareParamKind(
				expr.parameterResults[i+1], expr.parameterExecutor[i+1], selectedBranch)
		} else {
			expr.observeFlowControlPrepareParamKind(
				expr.parameterResults[i], expr.parameterExecutor[i], remaining)
		}
	}
	return err
}

func (expr *FunctionExpressionExecutor) EvalCoalesce(proc *process.Process, batches []*batch.Batch, selectList []bool) (err error) {
	expr.resetFlowControlPrepareParamKind()
	rowCount := expressionRowCount(batches)
	if len(expr.selectList1) < rowCount {
		expr.selectList1 = make([]bool, rowCount)
	}
	remaining := expr.selectList1[:rowCount]
	if selectList != nil {
		for i := range remaining {
			remaining[i] = i < len(selectList) && selectList[i]
		}
	} else {
		for i := range remaining {
			remaining[i] = true
		}
	}

	for i := range expr.parameterExecutor {
		expr.parameterResults[i], err = expr.parameterExecutor[i].Eval(proc, batches, remaining)
		if err != nil {
			return err
		}
		expr.observeFlowControlPrepareParamKind(
			expr.parameterResults[i], expr.parameterExecutor[i], remaining)
		for row := range remaining {
			if remaining[row] && !expr.parameterResults[i].IsNull(uint64(row)) {
				remaining[row] = false
			}
		}
	}
	return nil
}

func noRowsSelected(selectList []bool, rowCount int) bool {
	if selectList == nil {
		return false
	}
	if len(selectList) < rowCount {
		return false
	}
	for i := 0; i < rowCount; i++ {
		if selectList[i] {
			return false
		}
	}
	return true
}

func (expr *FunctionExpressionExecutor) makeNullResult(rowCount int) (*vector.Vector, error) {
	expr.resetResultType(expr.resultVector)
	if err := expr.resultVector.PreExtendAndReset(rowCount); err != nil {
		return nil, err
	}
	result := expr.resultVector.GetResultVector()
	result.SetAllNulls(rowCount)
	result.SetLength(rowCount)
	return result, nil
}

func (expr *FunctionExpressionExecutor) evalSelectedRows(
	proc *process.Process,
	rowCount int,
	selectList []bool,
) (*vector.Vector, error) {
	expr.selectedRows = expr.selectedRows[:0]
	for row := 0; row < rowCount; row++ {
		if selectList[row] {
			expr.selectedRows = append(expr.selectedRows, int64(row))
		}
	}

	selectedCount := len(expr.selectedRows)
	if len(expr.selectedParameterResults) == 0 && len(expr.parameterResults) > 0 {
		expr.selectedParameterResults = make([]*vector.Vector, len(expr.parameterResults))
		expr.selectedParameterVectors = make([]*vector.Vector, len(expr.parameterResults))
	}
	for i, parameter := range expr.parameterResults {
		// Constants, folded vectors, and list/vector literals are not row-aligned.
		// They must be passed through unchanged; only column and non-folded
		// function results map one-to-one to the input batch rows.
		rowAligned := false
		switch executor := expr.parameterExecutor[i].(type) {
		case *ColumnExpressionExecutor:
			rowAligned = true
		case *FunctionExpressionExecutor:
			rowAligned = !executor.folded.canFold
		}
		if rowAligned && !parameter.IsConst() {
			selected := expr.selectedParameterVectors[i]
			if selected == nil {
				var err error
				selected, err = newExpressionOffHeapVector(
					*parameter.GetType(), expr.allocation,
				)
				if err != nil {
					return nil, err
				}
				expr.selectedParameterVectors[i] = selected
			} else {
				selected.Reset(*parameter.GetType())
			}
			selected.SetIsBin(parameter.GetIsBin())
			if err := selected.Union(parameter, expr.selectedRows, proc.Mp()); err != nil {
				return nil, err
			}
			expr.selectedParameterResults[i] = selected
			continue
		}
		expr.selectedParameterResults[i] = parameter
	}

	expr.resetResultType(expr.resultVector)
	if err := expr.resultVector.PreExtendAndReset(rowCount); err != nil {
		return nil, err
	}
	if expr.selectedResult == nil {
		var err error
		expr.selectedResult, err = vector.NewFunctionResultWrapperWithAllocation(
			expr.resultType, expr.m, expr.allocation,
		)
		if err != nil {
			return nil, err
		}
	}
	expr.resetResultType(expr.selectedResult)
	if err := expr.selectedResult.PreExtendAndReset(selectedCount); err != nil {
		return nil, err
	}
	if err := expr.evalFn(
		expr.selectedParameterResults, expr.selectedResult, proc, selectedCount, nil); err != nil {
		return nil, err
	}
	if expr.isImplicitCast() && len(expr.selectedParameterResults) > 0 {
		if err := applyTransparentStringSource(
			expr.selectedResult.GetResultVector(), expr.selectedParameterResults[0], selectedCount, proc.Mp()); err != nil {
			return nil, err
		}
	}

	selectedResult := expr.selectedResult.GetResultVector()
	runtimeType := *selectedResult.GetType()
	runtimeIsBin := selectedResult.GetIsBin()
	runtimePrepareParamKind := selectedResult.GetPrepareParamKind()
	runtimePreparedJSONComparisonParam := selectedResult.IsPreparedJSONComparisonParam()
	runtimePrepareParamType := selectedResult.GetPrepareParamType()
	if expr.fid == function.IFF || expr.fid == function.CASE || expr.fid == function.COALESCE {
		runtimePrepareParamKind = expr.getFlowControlPrepareParamKind()
	}

	result := expr.resultVector.GetResultVector()
	result.SetType(runtimeType)
	result.SetIsBin(runtimeIsBin)
	result.ResetWithSameType()
	if expr.selectedNullResult == nil {
		var err error
		expr.selectedNullResult, err = newExpressionConstNull(
			runtimeType, 1, expr.allocation,
		)
		if err != nil {
			return nil, err
		}
	} else {
		expr.selectedNullResult.SetType(runtimeType)
		expr.selectedNullResult.SetLength(1)
	}
	expr.selectedNullResult.SetIsBin(runtimeIsBin)
	selectedRow := int64(0)
	for row := 0; row < rowCount; row++ {
		if selectList[row] {
			if err := result.UnionOne(selectedResult, selectedRow, proc.Mp()); err != nil {
				return nil, err
			}
			selectedRow++
		} else if err := result.UnionOne(expr.selectedNullResult, 0, proc.Mp()); err != nil {
			return nil, err
		}
	}
	if expr.fid == function.IFF || expr.fid == function.CASE || expr.fid == function.COALESCE {
		if err := expr.applyFlowControlPrepareParamKinds(result, rowCount, proc.Mp()); err != nil {
			return nil, err
		}
	} else {
		// A selected function result may already carry exact row provenance
		// from a type-preserving materialization. Keep that sidecar; the scalar
		// summary is only the compatibility fallback for uniform results.
		if len(result.GetPrepareParamKinds()) == 0 {
			result.SetPrepareParamKind(runtimePrepareParamKind)
		}
		if runtimePreparedJSONComparisonParam {
			result.SetPrepareParamType(runtimePrepareParamType)
			result.SetPreparedJSONComparisonParam()
		}
	}
	return result, nil
}

func (expr *FunctionExpressionExecutor) Eval(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	if len(batches) == 0 {
		batches = []*batch.Batch{batch.EmptyForConstFoldBatch}
	}
	rowCount := expressionRowCount(batches)
	if !expr.folded.canFold && noRowsSelected(selectList, rowCount) {
		return expr.makeNullResult(rowCount)
	}
	if expr.folded.needFoldingCheck {
		if err := expr.doFold(proc, proc.GetBaseProcessRunningStatus()); err != nil {
			return nil, err
		}
	}
	if expr.folded.canFold {
		if len(batches) > 0 {
			return expr.getFoldedVector(batches[0].RowCount()), nil
		}
		return expr.getFoldedVector(1), nil
	}

	var err error
	if expr.fid == function.IFF {
		err = expr.EvalIff(proc, batches, selectList)
		if err != nil {
			return nil, err
		}
	} else if expr.fid == function.CASE {
		err = expr.EvalCase(proc, batches, selectList)
		if err != nil {
			return nil, err
		}
	} else if expr.fid == function.COALESCE {
		err = expr.EvalCoalesce(proc, batches, selectList)
		if err != nil {
			return nil, err
		}
	} else {
		for i := range expr.parameterExecutor {
			expr.parameterResults[i], err = expr.parameterExecutor[i].Eval(proc, batches, selectList)
			if err != nil {
				return nil, err
			}
		}
	}

	if selectList != nil {
		selectedCount := 0
		for row := 0; row < rowCount; row++ {
			if selectList[row] {
				selectedCount++
			}
		}
		if selectedCount < rowCount {
			return expr.evalSelectedRows(proc, rowCount, selectList)
		}
	}

	expr.resetResultType(expr.resultVector)
	if err = expr.resultVector.PreExtendAndReset(rowCount); err != nil {
		return nil, err
	}
	if selectList != nil && len(expr.selectList.SelectList) < rowCount {
		expr.selectList.SelectList = make([]bool, rowCount)
	}
	if selectList == nil {
		expr.selectList.AnyNull = false
		expr.selectList.AllNull = false
		for i := range expr.selectList.SelectList {
			expr.selectList.SelectList[i] = true
		}
	} else {
		expr.selectList.AllNull = true
		expr.selectList.AnyNull = false
		for i := range selectList {
			expr.selectList.SelectList[i] = selectList[i]
			if selectList[i] {
				expr.selectList.AllNull = false
			} else {
				expr.selectList.AnyNull = true
			}
		}
	}

	if err = expr.evalFn(
		expr.parameterResults, expr.resultVector, proc, rowCount, &expr.selectList); err != nil {
		return nil, err
	}
	if expr.isImplicitCast() && len(expr.parameterResults) > 0 {
		if err := applyTransparentStringSource(
			expr.resultVector.GetResultVector(), expr.parameterResults[0], rowCount, proc.Mp()); err != nil {
			return nil, err
		}
	}
	if expr.fid == function.IFF || expr.fid == function.CASE || expr.fid == function.COALESCE {
		if err := expr.applyFlowControlPrepareParamKinds(
			expr.resultVector.GetResultVector(), rowCount, proc.Mp()); err != nil {
			return nil, err
		}
	}

	return expr.resultVector.GetResultVector(), nil
}

func (expr *FunctionExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches, nil)
	if err != nil {
		return nil, err
	}
	if expr.folded.canFold {
		return vec.Dup(proc.Mp())
	}
	expr.resultVector.SetResultVector(nil)
	return vec, nil
}

func (expr *FunctionExpressionExecutor) Free() {
	if expr == nil {
		return
	}
	if expr.resultVector != nil {
		expr.resultVector.Free()
		expr.resultVector = nil
	}
	expr.freeIffNullResults()
	if expr.selectedResult != nil {
		expr.selectedResult.Free()
		expr.selectedResult = nil
	}
	if expr.selectedNullResult != nil {
		expr.selectedNullResult.Free(expr.m)
		expr.selectedNullResult = nil
	}
	for _, parameter := range expr.selectedParameterVectors {
		if parameter != nil {
			parameter.Free(expr.m)
		}
	}

	for _, p := range expr.parameterExecutor {
		if p != nil {
			p.Free()
		}
	}
	if expr.freeFn != nil {
		_ = expr.freeFn()
		expr.freeFn = nil
	}
	reuse.Free[FunctionExpressionExecutor](expr, nil)
}

func (expr *FunctionExpressionExecutor) SetParameter(index int, executor ExpressionExecutor) {
	expr.parameterExecutor[index] = executor
}

func (expr *FunctionExpressionExecutor) IsColumnExpr() bool {
	return false
}

func (expr *ColumnExpressionExecutor) Eval(_ *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {

	relIndex := expr.relIndex
	// XXX it's a bad hack here. root cause is pipeline set a wrong relation index here.
	if len(batches) == 1 {
		relIndex = 0
	}

	if relIndex >= len(batches) {
		return nil, moerr.NewInternalErrorNoCtxf(
			"column expression eval: relIndex %d out of range, batches length %d",
			relIndex, len(batches))
	}

	vec := batches[relIndex].Vecs[expr.colIndex]
	// A grouping-set sentinel has no value payload and therefore also satisfies
	// IsConstNull, but its grouping bitmap is semantically distinct from SQL
	// NULL. Preserve it instead of normalizing it into the ordinary NULL cache.
	if vec.IsConstNull() && !vec.IsGrouping() {
		var err error
		vec, err = expr.getConstNullVec(expr.typ, vec.Length(), vec.GetStringSourceAt(0))
		if err != nil {
			return nil, err
		}
	}
	return vec, nil
}

func (expr *ColumnExpressionExecutor) getConstNullVec(
	typ types.Type,
	length int,
	source types.StringSource,
) (*vector.Vector, error) {
	if expr.nullVecCache != nil {
		expr.nullVecCache.SetType(typ)
		expr.nullVecCache.SetLength(length)
	} else {
		var err error
		expr.nullVecCache, err = newExpressionConstNull(typ, length, expr.allocation)
		if err != nil {
			return nil, err
		}
	}
	if err := expr.nullVecCache.SetStringSource(source); err != nil {
		return nil, err
	}
	return expr.nullVecCache, nil
}

func (expr *ColumnExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches, nil)
	if vec == expr.nullVecCache {
		expr.nullVecCache = nil
	}
	return vec, err
}

func (expr *ColumnExpressionExecutor) Free() {
	if expr == nil {
		return
	}
	if expr.nullVecCache != nil {
		expr.nullVecCache.Free(expr.mp)
		expr.nullVecCache = nil
	}
	reuse.Free[ColumnExpressionExecutor](expr, nil)
}

func (expr *ColumnExpressionExecutor) IsColumnExpr() bool {
	return true
}

func (expr *FixedVectorExpressionExecutor) Eval(_ *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	if !expr.noNeedToSetLength {
		expr.resultVector.SetLength(batches[0].RowCount())
	}
	return expr.resultVector, nil
}

func (expr *FixedVectorExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches, nil)
	if err != nil {
		return nil, err
	}
	return vec.Dup(proc.Mp())
}

func (expr *FixedVectorExpressionExecutor) Free() {
	if expr == nil {
		return
	}
	defer reuse.Free[FixedVectorExpressionExecutor](expr, nil)
	if expr.resultVector == nil {
		return
	}
	expr.resultVector.Free(expr.m)
	expr.resultVector = nil
}

func (expr *FixedVectorExpressionExecutor) IsColumnExpr() bool {
	return false
}

func generateConstExpressionExecutor(
	proc *process.Process,
	typ types.Type,
	con *plan.Literal,
	selection *vector.AllocationAccountSelection,
) (vec *vector.Vector, err error) {
	if con.GetIsnull() {
		vec, err = newExpressionConstNull(typ, 1, selection)
	} else {
		switch val := con.GetValue().(type) {
		case *plan.Literal_Bval:
			vec, err = newExpressionConstFixed(constBType, val.Bval, 1, proc.Mp(), selection)
		case *plan.Literal_I8Val:
			vec, err = newExpressionConstFixed(constI8Type, int8(val.I8Val), 1, proc.Mp(), selection)
		case *plan.Literal_I16Val:
			vec, err = newExpressionConstFixed(constI16Type, int16(val.I16Val), 1, proc.Mp(), selection)
		case *plan.Literal_I32Val:
			if typ.Oid == types.T_year {
				vec, err = newExpressionConstFixed(typ, types.MoYear(val.I32Val), 1, proc.Mp(), selection)
			} else {
				vec, err = newExpressionConstFixed(constI32Type, val.I32Val, 1, proc.Mp(), selection)
			}
		case *plan.Literal_I64Val:
			vec, err = newExpressionConstFixed(constI64Type, val.I64Val, 1, proc.Mp(), selection)
		case *plan.Literal_U8Val:
			vec, err = newExpressionConstFixed(constU8Type, uint8(val.U8Val), 1, proc.Mp(), selection)
		case *plan.Literal_U16Val:
			vec, err = newExpressionConstFixed(constU16Type, uint16(val.U16Val), 1, proc.Mp(), selection)
		case *plan.Literal_U32Val:
			vec, err = newExpressionConstFixed(constU32Type, val.U32Val, 1, proc.Mp(), selection)
		case *plan.Literal_U64Val:
			if typ.Oid == types.T_bit {
				vec, err = newExpressionConstFixed(typ, val.U64Val, 1, proc.Mp(), selection)
			} else {
				vec, err = newExpressionConstFixed(constU64Type, val.U64Val, 1, proc.Mp(), selection)
			}
		case *plan.Literal_Fval:
			vec, err = newExpressionConstFixed(constFType, val.Fval, 1, proc.Mp(), selection)
		case *plan.Literal_Dval:
			vec, err = newExpressionConstFixed(constDType, val.Dval, 1, proc.Mp(), selection)
		case *plan.Literal_Dateval:
			vec, err = newExpressionConstFixed(constDateType, types.Date(val.Dateval), 1, proc.Mp(), selection)
		case *plan.Literal_Timeval:
			vec, err = newExpressionConstFixed(typ, types.Time(val.Timeval), 1, proc.Mp(), selection)
		case *plan.Literal_Datetimeval:
			vec, err = newExpressionConstFixed(typ, types.Datetime(val.Datetimeval), 1, proc.Mp(), selection)
		case *plan.Literal_Decimal64Val:
			cd64 := val.Decimal64Val
			d64 := types.Decimal64(cd64.A)
			vec, err = newExpressionConstFixed(typ, d64, 1, proc.Mp(), selection)
		case *plan.Literal_Decimal128Val:
			cd128 := val.Decimal128Val
			d128 := types.Decimal128{B0_63: uint64(cd128.A), B64_127: uint64(cd128.B)}
			vec, err = newExpressionConstFixed(typ, d128, 1, proc.Mp(), selection)
		case *plan.Literal_Timestampval:
			scale := typ.Scale
			if scale < 0 || scale > 6 {
				return nil, moerr.NewErrTooBigPrecision(proc.Ctx, int64(scale), "TIMESTAMP", 6)
			}
			vec, err = newExpressionConstFixed(constTimestampTypes[scale], types.Timestamp(val.Timestampval), 1, proc.Mp(), selection)
		case *plan.Literal_Sval:
			sval := val.Sval
			// Distinguish binary with non-binary string.
			if typ.Oid == types.T_binary || typ.Oid == types.T_varbinary || typ.Oid == types.T_blob {
				vec, err = newExpressionConstBytes(constBinType, []byte(sval), 1, proc.Mp(), selection)
			} else if typ.Oid == types.T_geometry {
				vec, err = newExpressionConstBytes(typ, []byte(sval), 1, proc.Mp(), selection)
			} else if typ.Oid == types.T_array_float32 {
				array, err1 := types.StringToArray[float32](sval)
				if err1 != nil {
					return nil, err1
				}
				vec, err = newExpressionConstArray(typ, array, 1, proc.Mp(), selection)
			} else if typ.Oid == types.T_array_float64 {
				array, err1 := types.StringToArray[float64](sval)
				if err1 != nil {
					return nil, err1
				}
				vec, err = newExpressionConstArray(typ, array, 1, proc.Mp(), selection)
			} else if typ.Oid == types.T_datalink {
				_, _, err1 := datalink.ParseDatalink(sval, proc)
				if err1 != nil {
					return nil, err1
				}
				vec, err = newExpressionConstBytes(constBinType, []byte(sval), 1, proc.Mp(), selection)
			} else if typ.Oid.IsMySQLString() {
				// String literals use the executor's canonical VARCHAR container,
				// but must retain the plan charset. Raw HEX/BIT literals are
				// VARCHAR-shaped with CharsetBinary, while an empty SQL string is
				// plan-typed CHAR and still materializes as VARCHAR for consumers.
				constStringType := constSType
				constStringType.Charset = typ.Charset
				vec, err = newExpressionConstBytes(constStringType, []byte(sval), 1, proc.Mp(), selection)
			} else {
				vec, err = newExpressionConstBytes(constSType, []byte(sval), 1, proc.Mp(), selection)
			}
		case *plan.Literal_Defaultval:
			defaultVal := val.Defaultval
			vec, err = newExpressionConstFixed(constBType, defaultVal, 1, proc.Mp(), selection)
		case *plan.Literal_EnumVal:
			vec, err = newExpressionConstFixed(constEnumType, types.Enum(val.EnumVal), 1, proc.Mp(), selection)
		case *plan.Literal_VecVal:
			switch typ.Oid {
			case types.T_array_float32:
				vec, err = newExpressionConstArray(typ, types.BytesToArray[float32]([]byte(val.VecVal)), 1, proc.Mp(), selection)
			case types.T_array_float64:
				vec, err = newExpressionConstArray(typ, types.BytesToArray[float64]([]byte(val.VecVal)), 1, proc.Mp(), selection)
			case types.T_array_bf16:
				vec, err = newExpressionConstArray(typ, types.BytesToArray[types.BF16]([]byte(val.VecVal)), 1, proc.Mp(), selection)
			case types.T_array_float16:
				vec, err = newExpressionConstArray(typ, types.BytesToArray[types.Float16]([]byte(val.VecVal)), 1, proc.Mp(), selection)
			case types.T_array_int8:
				vec, err = newExpressionConstArray(typ, types.BytesToArray[int8]([]byte(val.VecVal)), 1, proc.Mp(), selection)
			case types.T_array_uint8:
				vec, err = newExpressionConstArray(typ, types.BytesToArray[uint8]([]byte(val.VecVal)), 1, proc.Mp(), selection)
			}
		default:
			return nil, moerr.NewNYI(proc.Ctx, fmt.Sprintf("const expression %v", con.GetValue()))
		}
		if err == nil {
			source, sourceErr := DecodeLiteralStringSource(con)
			if sourceErr != nil {
				vec.Free(proc.Mp())
				return nil, sourceErr
			}
			if err = vec.SetStringSource(source); err != nil {
				vec.Free(proc.Mp())
				return nil, err
			}
			vec.SetIsBin(con.IsBin)
			if typ.Oid.IsMySQLString() {
				domain := types.RuntimeStringInherit
				effectiveDomain := types.StaticStringDomain(typ)
				switch con.LiteralForm {
				case plan.StringLiteralForm_STRING_LITERAL_TEXT:
					effectiveDomain = types.StringDomainText
				case plan.StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER,
					plan.StringLiteralForm_STRING_LITERAL_HEX,
					plan.StringLiteralForm_STRING_LITERAL_BIT:
					effectiveDomain = types.StringDomainBinary
				}
				if effectiveDomain != types.StaticStringDomain(typ) {
					if effectiveDomain == types.StringDomainBinary {
						domain = types.RuntimeStringBinary
					} else {
						domain = types.RuntimeStringText
					}
				}
				if err = vec.SetRuntimeStringDomainWithMP(domain, proc.Mp()); err != nil {
					vec.Free(proc.Mp())
					return nil, err
				}
			}
		}
	}
	if err == nil && con.GetIsnull() {
		var source types.StringSource
		source, err = DecodeLiteralStringSource(con)
		if err == nil {
			err = vec.SetStringSource(source)
		}
	}
	return vec, err
}

// DecodeLiteralStringSource validates the protobuf-width encoding before
// narrowing it to the runtime enum. Scalar literal consumers must share this
// boundary so malformed plans cannot depend on the chosen execution path.
func DecodeLiteralStringSource(literal *plan.Literal) (types.StringSource, error) {
	if literal == nil || literal.GetStringSource() == 0 {
		return types.StringSourceLiteral, nil
	}
	rawSource := literal.GetStringSource()
	if rawSource > uint32(types.StringSourceCOMStmt)+1 {
		return types.StringSourceExpression,
			moerr.NewInvalidInputNoCtxf("invalid literal string source %d", rawSource)
	}
	source := types.StringSource(rawSource - 1)
	if !source.Valid() {
		return types.StringSourceExpression,
			moerr.NewInvalidInputNoCtxf("invalid literal string source %d", literal.GetStringSource())
	}
	return source, nil
}

func GenerateConstListExpressionExecutor(proc *process.Process, exprs []*plan.Expr) (*vector.Vector, error) {
	lenList := len(exprs)
	vec, err := proc.AllocVectorOfRows(types.NewWithCharset(
		types.T(exprs[0].Typ.Id), exprs[0].Typ.Width, exprs[0].Typ.Scale, uint8(exprs[0].Typ.Charset),
	), lenList, nil)
	if err != nil {
		return nil, err
	}
	sources := make([]types.StringSource, lenList)
	for i := 0; i < lenList; i++ {
		expr := exprs[i]
		t := expr.GetLit()
		if t == nil {
			return nil, moerr.NewInternalError(proc.Ctx, "args in list must be constant")
		}
		sources[i], err = DecodeLiteralStringSource(t)
		if err != nil {
			vec.Free(proc.Mp())
			return nil, err
		}
		if t.GetIsnull() {
			vec.GetNulls().Set(uint64(i))
		} else {
			switch val := t.GetValue().(type) {
			case *plan.Literal_Bval:
				veccol := vector.MustFixedColNoTypeCheck[bool](vec)
				veccol[i] = val.Bval
			case *plan.Literal_I8Val:
				veccol := vector.MustFixedColNoTypeCheck[int8](vec)
				veccol[i] = int8(val.I8Val)
			case *plan.Literal_I16Val:
				veccol := vector.MustFixedColNoTypeCheck[int16](vec)
				veccol[i] = int16(val.I16Val)
			case *plan.Literal_I32Val:
				veccol := vector.MustFixedColNoTypeCheck[int32](vec)
				veccol[i] = val.I32Val
			case *plan.Literal_I64Val:
				veccol := vector.MustFixedColNoTypeCheck[int64](vec)
				veccol[i] = val.I64Val
			case *plan.Literal_U8Val:
				veccol := vector.MustFixedColNoTypeCheck[uint8](vec)
				veccol[i] = uint8(val.U8Val)
			case *plan.Literal_U16Val:
				veccol := vector.MustFixedColNoTypeCheck[uint16](vec)
				veccol[i] = uint16(val.U16Val)
			case *plan.Literal_U32Val:
				veccol := vector.MustFixedColNoTypeCheck[uint32](vec)
				veccol[i] = val.U32Val
			case *plan.Literal_U64Val:
				veccol := vector.MustFixedColNoTypeCheck[uint64](vec)
				veccol[i] = val.U64Val
			case *plan.Literal_Fval:
				veccol := vector.MustFixedColNoTypeCheck[float32](vec)
				veccol[i] = val.Fval
			case *plan.Literal_Dval:
				veccol := vector.MustFixedColNoTypeCheck[float64](vec)
				veccol[i] = val.Dval
			case *plan.Literal_Dateval:
				veccol := vector.MustFixedColNoTypeCheck[types.Date](vec)
				veccol[i] = types.Date(val.Dateval)
			case *plan.Literal_Timeval:
				veccol := vector.MustFixedColNoTypeCheck[types.Time](vec)
				veccol[i] = types.Time(val.Timeval)
			case *plan.Literal_Datetimeval:
				veccol := vector.MustFixedColNoTypeCheck[types.Datetime](vec)
				veccol[i] = types.Datetime(val.Datetimeval)
			case *plan.Literal_Decimal64Val:
				cd64 := val.Decimal64Val
				d64 := types.Decimal64(cd64.A)
				veccol := vector.MustFixedColNoTypeCheck[types.Decimal64](vec)
				veccol[i] = d64
			case *plan.Literal_Decimal128Val:
				cd128 := val.Decimal128Val
				d128 := types.Decimal128{B0_63: uint64(cd128.A), B64_127: uint64(cd128.B)}
				veccol := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
				veccol[i] = d128
			case *plan.Literal_Timestampval:
				scale := expr.Typ.Scale
				if scale < 0 || scale > 6 {
					return nil, moerr.NewErrTooBigPrecision(proc.Ctx, int64(scale), "TIMESTAMP", 6)
				}
				veccol := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)
				veccol[i] = types.Timestamp(val.Timestampval)
			case *plan.Literal_Sval:
				sval := val.Sval
				if expr.Typ.Id == int32(types.T_geometry) {
					err = vector.SetBytesAt(vec, i, []byte(sval), proc.Mp())
				} else {
					err = vector.SetStringAt(vec, i, sval, proc.Mp())
				}
				if err != nil {
					return nil, err
				}
			case *plan.Literal_Defaultval:
				defaultVal := val.Defaultval
				veccol := vector.MustFixedColNoTypeCheck[bool](vec)
				veccol[i] = defaultVal
			case *plan.Literal_EnumVal:
				veccol := vector.MustFixedColNoTypeCheck[types.Enum](vec)
				veccol[i] = types.Enum(val.EnumVal)
			case *plan.Literal_VecVal:
				sval := val.VecVal
				err = vector.SetStringAt(vec, i, sval, proc.Mp())
				if err != nil {
					return nil, err
				}
			default:
				return nil, moerr.NewNYI(proc.Ctx, fmt.Sprintf("const expression %v", t.GetValue()))
			}
			vec.SetIsBin(t.IsBin)
		}
	}
	if err := vec.SetStringSourcesWithMP(sources, proc.Mp()); err != nil {
		vec.Free(proc.Mp())
		return nil, err
	}
	return vec, nil
}

func NewJoinBatch(bat *batch.Batch, mp *mpool.MPool) (*batch.Batch,
	[]func(*vector.Vector, *vector.Vector, int64, int) error) {
	rbat := batch.NewOffHeapWithSize(bat.VectorCount())
	cfs := make([]func(*vector.Vector, *vector.Vector, int64, int) error, bat.VectorCount())
	for i, vec := range bat.Vecs {
		typ := *vec.GetType()
		rbat.Vecs[i] = vector.NewConstNull(typ, 0, nil)
		rbat.Vecs[i].SetOffHeap(true)
		cfs[i] = vector.GetConstSetFunction(typ, mp)
	}
	return rbat, cfs
}

func SetJoinBatchValues(joinBat, bat *batch.Batch, sel int64, length int,
	cfs []func(*vector.Vector, *vector.Vector, int64, int) error) error {
	for i, vec := range bat.Vecs {
		if err := cfs[i](joinBat.Vecs[i], vec, sel, length); err != nil {
			return err
		}
	}
	joinBat.SetRowCount(length)
	return nil
}

func getConstZM(
	ctx context.Context,
	expr *plan.Expr,
	proc *process.Process,
) (zm index.ZM, err error) {
	c := expr.GetLit()
	typ := expr.Typ
	if c.GetIsnull() {
		zm = index.NewZM(types.T(typ.Id), typ.Scale)
		return
	}
	switch val := c.GetValue().(type) {
	case *plan.Literal_Bval:
		zm = index.NewZM(constBType.Oid, 0)
		v := val.Bval
		index.UpdateZM(zm, types.EncodeBool(&v))
	case *plan.Literal_I8Val:
		zm = index.NewZM(constI8Type.Oid, 0)
		v := int8(val.I8Val)
		index.UpdateZM(zm, types.EncodeInt8(&v))
	case *plan.Literal_I16Val:
		zm = index.NewZM(constI16Type.Oid, 0)
		v := int16(val.I16Val)
		index.UpdateZM(zm, types.EncodeInt16(&v))
	case *plan.Literal_I32Val:
		zm = index.NewZM(constI32Type.Oid, 0)
		v := val.I32Val
		index.UpdateZM(zm, types.EncodeInt32(&v))
	case *plan.Literal_I64Val:
		zm = index.NewZM(constI64Type.Oid, 0)
		v := val.I64Val
		index.UpdateZM(zm, types.EncodeInt64(&v))
	case *plan.Literal_U8Val:
		zm = index.NewZM(constU8Type.Oid, 0)
		v := uint8(val.U8Val)
		index.UpdateZM(zm, types.EncodeUint8(&v))
	case *plan.Literal_U16Val:
		zm = index.NewZM(constU16Type.Oid, 0)
		v := uint16(val.U16Val)
		index.UpdateZM(zm, types.EncodeUint16(&v))
	case *plan.Literal_U32Val:
		zm = index.NewZM(constU32Type.Oid, 0)
		v := val.U32Val
		index.UpdateZM(zm, types.EncodeUint32(&v))
	case *plan.Literal_U64Val:
		zm = index.NewZM(constU64Type.Oid, 0)
		v := val.U64Val
		index.UpdateZM(zm, types.EncodeUint64(&v))
	case *plan.Literal_Fval:
		zm = index.NewZM(constFType.Oid, 0)
		v := val.Fval
		index.UpdateZM(zm, types.EncodeFloat32(&v))
	case *plan.Literal_Dval:
		zm = index.NewZM(constDType.Oid, 0)
		v := val.Dval
		index.UpdateZM(zm, types.EncodeFloat64(&v))
	case *plan.Literal_Dateval:
		zm = index.NewZM(constDateType.Oid, 0)
		v := val.Dateval
		index.UpdateZM(zm, types.EncodeInt32(&v))
	case *plan.Literal_Timeval:
		zm = index.NewZM(constTimeType.Oid, 0)
		v := val.Timeval
		index.UpdateZM(zm, types.EncodeInt64(&v))
	case *plan.Literal_Datetimeval:
		zm = index.NewZM(constDatetimeType.Oid, 0)
		v := val.Datetimeval
		index.UpdateZM(zm, types.EncodeInt64(&v))
	case *plan.Literal_Decimal64Val:
		v := val.Decimal64Val
		zm = index.NewZM(types.T_decimal64, typ.Scale)
		d64 := types.Decimal64(v.A)
		index.UpdateZM(zm, types.EncodeDecimal64(&d64))
	case *plan.Literal_Decimal128Val:
		v := val.Decimal128Val
		zm = index.NewZM(types.T_decimal128, typ.Scale)
		d128 := types.Decimal128{B0_63: uint64(v.A), B64_127: uint64(v.B)}
		index.UpdateZM(zm, types.EncodeDecimal128(&d128))
	case *plan.Literal_Timestampval:
		v := val.Timestampval
		scale := typ.Scale
		if scale < 0 || scale > 6 {
			err = moerr.NewErrTooBigPrecision(proc.Ctx, int64(scale), "TIMESTAMP", 6)
			return
		}
		zm = index.NewZM(constTimestampTypes[0].Oid, scale)
		index.UpdateZM(zm, types.EncodeInt64(&v))
	case *plan.Literal_Sval:
		zm = index.NewZM(constSType.Oid, 0)
		v := val.Sval
		index.UpdateZM(zm, []byte(v))
	case *plan.Literal_Defaultval:
		zm = index.NewZM(constBType.Oid, 0)
		v := val.Defaultval
		index.UpdateZM(zm, types.EncodeBool(&v))
	case *plan.Literal_EnumVal:
		zm = index.NewZM(constEnumType.Oid, 0)
		v := types.Enum(val.EnumVal)
		index.UpdateZM(zm, types.EncodeEnum(&v))
	default:
		err = moerr.NewNYI(ctx, fmt.Sprintf("const expression %v", c.GetValue()))
	}
	return
}

func EvaluateFilterByZoneMap(
	ctx context.Context, // why we need a context here, to escape trace?
	proc *process.Process,
	expr *plan.Expr,
	meta objectio.ColumnMetaFetcher,
	columnMap map[int]int,
	zms []objectio.ZoneMap,
	vecs []*vector.Vector) (selected bool) {
	if expr == nil {
		selected = true
		return
	}

	if len(columnMap) == 0 {
		vec, free, err := GetReadonlyResultFromNoColumnExpression(proc, expr)
		if err != nil {
			return true
		}
		cols := vector.MustFixedColWithTypeCheck[bool](vec)
		for _, isNeed := range cols {
			if isNeed {
				free()
				return true
			}
		}
		free()
		return false
	}

	zm := GetExprZoneMap(ctx, proc, expr, meta, columnMap, zms, vecs)
	if !zm.IsInited() || zm.GetType() != types.T_bool {
		// Unknown zonemap results are not proof that the block cannot match.
		selected = true
	} else {
		selected = types.DecodeBool(zm.GetMaxBuf())
	}

	// clean the vector.
	for i := range vecs {
		if vecs[i] != nil {
			vecs[i].Free(proc.Mp())
			vecs[i] = nil
		}
	}
	return
}

// zoneMapInVector decodes an IN / prefix_in payload for zone-map pruning and
// reports whether it may be used to prune.
//
// What each consumer needs differs:
//   - ZM.PrefixIn always binary-searches the physical varlena slots and never
//     consults the null bitmap, so it needs the physical order to be ascending.
//   - ZM.AnyIn binary-searches too, except when the payload carries NULLs, where
//     it falls back to anyInNullableVec -- a linear scan that ignores order.
//
// An out-of-order payload makes the search probe the wrong element and silently
// drop blocks that hold matching rows, so it must not prune at all: keeping a
// block is always safe.
//
// Normalizing here is not an option. EvaluateFilterByZoneMap frees its vector
// cache on every call, and disttae calls it once per object and again for each
// block, so sorting a private copy would clone and sort the payload per block.
// Decoding is therefore zero-copy and the payload is only ever inspected.
func zoneMapInVector(data []byte, prefixSearch bool) (*vector.Vector, bool) {
	vec := vector.NewVec(types.T_any.ToType())
	if err := vec.UnmarshalBinary(data); err != nil {
		return nil, false
	}
	if vec.IsConst() {
		return vec, true
	}
	if !prefixSearch && vec.GetNulls().Any() {
		// AnyIn scans linearly for these, so order does not matter.
		return vec, true
	}
	if zoneMapInVectorOrderIsKnown(vec, prefixSearch) {
		return vec, true
	}
	return nil, false
}

// zoneMapInVectorOrderIsKnown reports whether the payload's ascending order can be
// relied on. It is not "is this sorted" -- an unflagged fixed-width payload may
// well be ordered, but nothing here can establish that, and the caller must treat
// unknown exactly as it treats unsorted.
//
// The sorted flag is authoritative when set: InplaceSortAndCompact sets it
// unconditionally and it is marshalled with the payload, so a folded IN list that
// went through it carries it. It is not a universal invariant, though: constant
// folding marshals a function result verbatim, and deliberately leaves a nullable
// IN list unsorted to keep its null bitmap aligned with its values (both in
// pkg/sql/plan/rule/constant_fold.go), so an unordered payload arrives with no flag.
//
// For byte-string varlen types the order is verified directly, in the byte order
// AnyIn and PrefixIn search, with a NULL slot's empty payload sorting first exactly
// as they see it. Array payloads use value comparators and are handled separately.
// A prefix search needs one condition more than order -- see the walk below --
// which is why the flag does not short-circuit it. Fixed-width types would need a
// per-type comparator, so absent the flag their order is unknown and the caller
// fails open.
//
// Failing open only costs pruning. Trusting an unverified order costs rows:
// needles [30,10] against a block zonemap [5,15] make AnyIn's binary search probe
// 30, answer false, and drop a block holding the matching needle 10.
func zoneMapInVectorOrderIsKnown(vec *vector.Vector, prefixSearch bool) bool {
	oid := vec.GetType().Oid
	if oid.IsArrayRelate() {
		// PrefixIn compares physical bytes and is not defined for array values.
		// AnyIn supports float32/float64 arrays with ArrayCompare, so only the
		// comparator-consistent flag produced by InplaceSort or
		// InplaceSortAndCompact proves
		// their order. Narrow arrays currently fail open in AnyIn and stay
		// conservative here regardless of their metadata.
		if prefixSearch {
			return false
		}
		if vec.Length() < 2 {
			return true
		}
		switch oid {
		case types.T_array_float32, types.T_array_float64:
			return vec.GetSorted()
		default:
			return false
		}
	}
	if vec.Length() < 2 {
		return true
	}
	if !vec.GetType().IsVarlen() {
		// Fixed-width order can only come from the flag. PrefixIn never reaches
		// these -- it reads varlena slots directly.
		return vec.GetSorted()
	}
	switch oid {
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob,
		types.T_text, types.T_datalink:
	default:
		return false
	}

	// The flag alone is not enough for a prefix search, so this walks even when it
	// is set: PrefixIn's search predicate is non-monotonic whenever one needle is a
	// proper byte-prefix of another, and InplaceSortAndCompact will happily flag
	// such a payload. Ascending ["a","ab"] against a zone map ["az","c"] makes the
	// predicate read [true,false]; sort.Search runs off the end and prunes a block
	// that "a" matches. Checking adjacent pairs suffices: if one needle is a proper
	// prefix of a later one, every needle between them carries that prefix too.
	// This is a guard, not the fix -- #27817 tracks PrefixIn itself, and closing it
	// makes this branch removable.
	checkOrder := !vec.GetSorted()
	if !checkOrder && !prefixSearch {
		// Flagged, and no prefix search to second-guess the flag: nothing to verify.
		// Without this the walk below runs to completion doing nothing, once per
		// zone map, for every flagged varlen IN payload -- which is what
		// ConstructInExpr now publishes on the transfer path.
		return true
	}
	col, area := vector.MustVarlenaRawData(vec)
	prev := col[0].GetByteSlice(area)
	for i := 1; i < len(col); i++ {
		cur := col[i].GetByteSlice(area)
		if checkOrder && bytes.Compare(prev, cur) > 0 {
			return false
		}
		if prefixSearch && len(prev) < len(cur) && bytes.HasPrefix(cur, prev) {
			return false
		}
		prev = cur
	}
	return true
}

func GetExprZoneMap(
	ctx context.Context,
	proc *process.Process,
	expr *plan.Expr,
	meta objectio.ColumnMetaFetcher,
	columnMap map[int]int,
	zms []objectio.ZoneMap,
	vecs []*vector.Vector) (v objectio.ZoneMap) {
	var err error

	switch t := expr.Expr.(type) {
	case *plan.Expr_Lit:
		if zms[expr.AuxId] == nil {
			if zms[expr.AuxId], err = getConstZM(ctx, expr, proc); err != nil {
				zms[expr.AuxId] = objectio.NewZM(types.T_bool, 0)
			}
		}

	case *plan.Expr_Col:
		zms[expr.AuxId] = meta.MustGetColumn(uint16(columnMap[int(t.Col.ColPos)])).ZoneMap()

	case *plan.Expr_F:
		id := t.F.GetFunc().GetObj()
		if overload, errGetFunc := function.GetFunctionById(ctx, id); errGetFunc != nil {
			zms[expr.AuxId].Reset()

		} else {
			args := t.F.Args

			// Some expressions need to be handled specifically
			switch t.F.Func.ObjName {
			case "isnull", "is_null":
				switch exprImpl := args[0].Expr.(type) {
				case *plan.Expr_Col:
					nullCnt := meta.MustGetColumn(uint16(columnMap[int(exprImpl.Col.ColPos)])).NullCnt()
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], nullCnt > 0)
					return zms[expr.AuxId]
				default:
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}
			case "isnotnull", "is_not_null":
				switch exprImpl := args[0].Expr.(type) {
				case *plan.Expr_Col:
					zm := meta.MustGetColumn(uint16(columnMap[int(exprImpl.Col.ColPos)])).ZoneMap()
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], zm.IsInited())
					return zms[expr.AuxId]
				default:
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}
			case "in":
				if list := args[1].GetList(); list != nil {
					return foldNativeInListZoneMap(ctx, proc, meta, columnMap, zms, vecs, args, expr.AuxId, false)
				}
				rid := args[1].AuxId
				if vecs[rid] == nil {
					if data, ok := args[1].Expr.(*plan.Expr_Vec); ok {
						vec, decoded := zoneMapInVector(data.Vec.Data, false)
						if !decoded {
							zms[expr.AuxId].Reset()
							vecs[rid] = vector.NewConstNull(types.T_any.ToType(), math.MaxInt, proc.Mp())
							return zms[expr.AuxId]
						}
						vecs[rid] = vec
					} else {
						zms[expr.AuxId].Reset()
						vecs[rid] = vector.NewConstNull(types.T_any.ToType(), math.MaxInt, proc.Mp())
						return zms[expr.AuxId]
					}
				}

				if vecs[rid].IsConstNull() && vecs[rid].Length() == math.MaxInt {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}

				lhs := GetExprZoneMap(ctx, proc, args[0], meta, columnMap, zms, vecs)
				if !lhs.IsInited() {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}

				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], lhs.AnyIn(vecs[rid]))
				return zms[expr.AuxId]

			case "not_in":
				if list := args[1].GetList(); list != nil {
					return foldNativeInListZoneMap(ctx, proc, meta, columnMap, zms, vecs, args, expr.AuxId, true)
				}
				zms[expr.AuxId].Reset()
				return zms[expr.AuxId]

			case "prefix_eq":
				lhs := GetExprZoneMap(ctx, proc, args[0], meta, columnMap, zms, vecs)
				if !lhs.IsInited() {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}

				s := []byte(args[1].GetLit().GetSval())

				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], lhs.PrefixEq(s))
				return zms[expr.AuxId]

			case "prefix_between":
				lhs := GetExprZoneMap(ctx, proc, args[0], meta, columnMap, zms, vecs)
				if !lhs.IsInited() {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}

				lb := []byte(args[1].GetLit().GetSval())
				ub := []byte(args[2].GetLit().GetSval())

				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], lhs.PrefixBetween(lb, ub))
				return zms[expr.AuxId]

			case "prefix_in_range":
				lhs := GetExprZoneMap(ctx, proc, args[0], meta, columnMap, zms, vecs)
				if !lhs.IsInited() {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}

				lb := []byte(args[1].GetLit().GetSval())
				ub := []byte(args[2].GetLit().GetSval())

				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], lhs.PrefixBetween(lb, ub))
				return zms[expr.AuxId]

			case "prefix_in":
				rid := args[1].AuxId
				if vecs[rid] == nil {
					if data, ok := args[1].Expr.(*plan.Expr_Vec); ok {
						vec, decoded := zoneMapInVector(data.Vec.Data, true)
						if !decoded {
							zms[expr.AuxId].Reset()
							vecs[rid] = vector.NewConstNull(types.T_any.ToType(), math.MaxInt, proc.Mp())
							return zms[expr.AuxId]
						}
						vecs[rid] = vec
					} else {
						zms[expr.AuxId].Reset()
						vecs[rid] = vector.NewConstNull(types.T_any.ToType(), math.MaxInt, proc.Mp())
						return zms[expr.AuxId]
					}
				}

				if vecs[rid].IsConstNull() && vecs[rid].Length() == math.MaxInt {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}

				lhs := GetExprZoneMap(ctx, proc, args[0], meta, columnMap, zms, vecs)
				if !lhs.IsInited() {
					zms[expr.AuxId].Reset()
					return zms[expr.AuxId]
				}

				zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], lhs.PrefixIn(vecs[rid]))
				return zms[expr.AuxId]
			}

			f := func() bool {
				for _, arg := range args {
					zms[arg.AuxId] = GetExprZoneMap(ctx, proc, arg, meta, columnMap, zms, vecs)
					if !zms[arg.AuxId].IsInited() {
						zms[expr.AuxId].Reset()
						return true
					}
				}
				return false
			}

			var res, ok bool
			switch t.F.Func.ObjName {
			case ">":
				if hasConstNullArg(args) {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], false)
					return zms[expr.AuxId]
				}
				if f() {
					return zms[expr.AuxId]
				}
				if foldTemporalComparisonZoneMap(proc, args, zms, expr.AuxId, ">") {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].AnyGT(zms[args[1].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case "<":
				if hasConstNullArg(args) {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], false)
					return zms[expr.AuxId]
				}
				if f() {
					return zms[expr.AuxId]
				}
				if foldTemporalComparisonZoneMap(proc, args, zms, expr.AuxId, "<") {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].AnyLT(zms[args[1].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case ">=":
				if hasConstNullArg(args) {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], false)
					return zms[expr.AuxId]
				}
				if f() {
					return zms[expr.AuxId]
				}
				if foldTemporalComparisonZoneMap(proc, args, zms, expr.AuxId, ">=") {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].AnyGE(zms[args[1].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case "<=":
				if hasConstNullArg(args) {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], false)
					return zms[expr.AuxId]
				}
				if f() {
					return zms[expr.AuxId]
				}
				if foldTemporalComparisonZoneMap(proc, args, zms, expr.AuxId, "<=") {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].AnyLE(zms[args[1].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case "=":
				if hasConstNullArg(args) {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], false)
					return zms[expr.AuxId]
				}
				if f() {
					return zms[expr.AuxId]
				}
				if foldTemporalComparisonZoneMap(proc, args, zms, expr.AuxId, "=") {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].Intersect(zms[args[1].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case "!=", "<>":
				if hasConstNullArg(args) {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], false)
					return zms[expr.AuxId]
				}
				if f() {
					return zms[expr.AuxId]
				}
				if foldTemporalComparisonZoneMap(proc, args, zms, expr.AuxId, "!=") {
					return zms[expr.AuxId]
				}
				if res, ok = anyNotEqualZoneMap(zms[args[0].AuxId], zms[args[1].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case "between":
				if hasConstNullArg(args) {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], false)
					return zms[expr.AuxId]
				}
				if f() {
					return zms[expr.AuxId]
				}
				if foldTemporalBetweenZoneMap(proc, args, zms, expr.AuxId) {
					return zms[expr.AuxId]
				}
				if res, ok = zms[args[0].AuxId].AnyBetween(zms[args[1].AuxId], zms[args[2].AuxId]); !ok {
					zms[expr.AuxId].Reset()
				} else {
					zms[expr.AuxId] = index.SetBool(zms[expr.AuxId], res)
				}

			case "and":
				if hasResult := foldAndZoneMap(ctx, proc, meta, columnMap, zms, vecs, args, expr.AuxId); hasResult {
					return zms[expr.AuxId]
				}

			case "or":
				if hasResult := foldOrZoneMap(ctx, proc, meta, columnMap, zms, vecs, args, expr.AuxId); hasResult {
					return zms[expr.AuxId]
				}

			case "+":
				if f() {
					return zms[expr.AuxId]
				}
				zms[expr.AuxId] = index.ZMPlus(zms[args[0].AuxId], zms[args[1].AuxId], zms[expr.AuxId])

			case "-":
				if f() {
					return zms[expr.AuxId]
				}
				zms[expr.AuxId] = index.ZMMinus(zms[args[0].AuxId], zms[args[1].AuxId], zms[expr.AuxId])

			case "*":
				if f() {
					return zms[expr.AuxId]
				}
				zms[expr.AuxId] = index.ZMMulti(zms[args[0].AuxId], zms[args[1].AuxId], zms[expr.AuxId])

			default:
				ivecs := make([]*vector.Vector, len(args))
				if isAllConst(args) { // constant fold
					defer func() {
						for _, v := range ivecs {
							if v != nil {
								v.Free(proc.Mp())
							}
						}
					}()
					for i, arg := range args {
						if vecs[arg.AuxId] != nil {
							vecs[arg.AuxId].Free(proc.Mp())
						}
						if vecs[arg.AuxId], _, err = GetReadonlyResultFromNoColumnExpression(proc, arg); err != nil {
							zms[expr.AuxId].Reset()
							return zms[expr.AuxId]
						}
						if ivecs[i], err = vecs[arg.AuxId].Dup(proc.Mp()); err != nil {
							zms[expr.AuxId].Reset()
							return zms[expr.AuxId]
						}
					}
				} else {
					if f() {
						return zms[expr.AuxId]
					}
					for i, arg := range args {
						if vecs[arg.AuxId] != nil {
							vecs[arg.AuxId].Free(proc.Mp())
						}
						if vecs[arg.AuxId], err = index.ZMToVector(zms[arg.AuxId], vecs[arg.AuxId], proc.Mp()); err != nil {
							zms[expr.AuxId].Reset()
							return zms[expr.AuxId]
						}
						ivecs[i] = vecs[arg.AuxId]
					}
				}
				fn, _, fnFree, _ := overload.GetExecuteMethod()
				typ := types.NewWithCharset(
					types.T(expr.Typ.Id), expr.Typ.Width, expr.Typ.Scale, uint8(expr.Typ.Charset),
				)

				result := vector.NewFunctionResultWrapper(typ, proc.Mp())
				if err = result.PreExtendAndReset(2); err != nil {
					zms[expr.AuxId].Reset()
					result.Free()
					if fnFree != nil {
						// NOTE: fnFree is only applicable for serial and serial_full.
						// if fnFree is not nil, then make sure to call it after fn() is done.
						_ = fnFree()
					}
					return zms[expr.AuxId]
				}
				if err = fn(ivecs, result, proc, 2, nil); err != nil {
					zms[expr.AuxId].Reset()
					result.Free()
					if fnFree != nil {
						// NOTE: fnFree is only applicable for serial and serial_full.
						// if fnFree is not nil, then make sure to call it after fn() is done.
						_ = fnFree()
					}
					return zms[expr.AuxId]
				}
				if fnFree != nil {
					// NOTE: fnFree is only applicable for serial and serial_full.
					// if fnFree is not nil, then make sure to call it after fn() is done.
					_ = fnFree()
				}
				zms[expr.AuxId] = index.VectorToZM(result.GetResultVector(), zms[expr.AuxId])
				result.Free()
			}
		}

	default:
		zms[expr.AuxId].Reset()
	}

	return zms[expr.AuxId]
}

func hasConstNullArg(args []*plan.Expr) bool {
	for _, arg := range args {
		if isConstNullExpr(arg) {
			return true
		}
	}
	return false
}

func isConstNullExpr(expr *plan.Expr) bool {
	if lit := expr.GetLit(); lit != nil {
		return lit.GetIsnull()
	}
	if f := expr.GetF(); f != nil && f.Func.GetObjName() == "cast" && len(f.Args) >= 1 {
		return isConstNullExpr(f.Args[0])
	}
	return false
}

func foldNativeInListZoneMap(
	ctx context.Context,
	proc *process.Process,
	meta objectio.ColumnMetaFetcher,
	columnMap map[int]int,
	zms []objectio.ZoneMap,
	vecs []*vector.Vector,
	args []*plan.Expr,
	auxID int32,
	notIn bool,
) objectio.ZoneMap {
	list := args[1].GetList()
	if list == nil {
		zms[auxID].Reset()
		return zms[auxID]
	}

	for _, item := range list.List {
		if isConstNullExpr(item) {
			if notIn {
				zms[auxID] = index.SetBool(zms[auxID], false)
				return zms[auxID]
			}
			continue
		}
	}
	if notIn {
		zms[auxID].Reset()
		return zms[auxID]
	}

	lhs := GetExprZoneMap(ctx, proc, args[0], meta, columnMap, zms, vecs)
	if !lhs.IsInited() {
		zms[auxID].Reset()
		return zms[auxID]
	}

	hasUnknown := false
	for _, item := range list.List {
		rhs, isNull, ok := getConstExprZoneMap(proc, item)
		if isNull {
			continue
		}
		if !ok {
			hasUnknown = true
			continue
		}
		if res, ok := lhs.Intersect(rhs); !ok {
			hasUnknown = true
		} else if res {
			zms[auxID] = index.SetBool(zms[auxID], true)
			return zms[auxID]
		}
	}

	if hasUnknown {
		zms[auxID].Reset()
		return zms[auxID]
	}
	zms[auxID] = index.SetBool(zms[auxID], false)
	return zms[auxID]
}

func getConstExprZoneMap(proc *process.Process, expr *plan.Expr) (zm objectio.ZoneMap, isNull bool, ok bool) {
	vec, free, err := GetReadonlyResultFromNoColumnExpression(proc, expr)
	if err != nil {
		return nil, false, false
	}
	defer free()

	if vec.IsConstNull() || vec.GetNulls().Contains(0) {
		return objectio.NewZM(vec.GetType().Oid, vec.GetType().Scale), true, true
	}

	zm = objectio.NewZM(vec.GetType().Oid, vec.GetType().Scale)
	if err := index.BatchUpdateZM(zm, vec); err != nil || !zm.IsInited() {
		return nil, false, false
	}
	return zm, false, true
}

func anyNotEqualZoneMap(lhs, rhs objectio.ZoneMap) (bool, bool) {
	intersects, ok := lhs.Intersect(rhs)
	if !ok {
		return false, false
	}
	if !intersects {
		return true, true
	}
	if isSingleValueZoneMap(lhs) && isSingleValueZoneMap(rhs) && lhs.CompareMin(rhs) == 0 {
		return false, true
	}
	return true, true
}

// foldTemporalComparisonZoneMap evaluates a mixed DATETIME/TIMESTAMP
// comparison against min/max metadata without changing the logical predicate.
// DATETIME is interpreted in the session time zone, exactly like the execution
// comparator.  If that conversion is not order-preserving over a zonemap range
// (for example, the range intersects a DST fold or gap), the result is reset to
// unknown so the block is conservatively retained for residual evaluation.
func foldTemporalComparisonZoneMap(
	proc *process.Process,
	args []*plan.Expr,
	zms []objectio.ZoneMap,
	auxID int32,
	op string,
) bool {
	lhs := zms[args[0].AuxId]
	rhs := zms[args[1].AuxId]
	if !lhs.IsInited() || !rhs.IsInited() ||
		!isDatetimeTimestampZoneMapPair(lhs, rhs) {
		return false
	}

	result, ok := temporalZoneMapComparison(lhs, rhs, proc.GetSessionInfo().TimeZone, op)
	if !ok {
		zms[auxID].Reset()
		return true
	}
	zms[auxID] = index.SetBool(zms[auxID], result)
	return true
}

func temporalZoneMapComparison(
	lhs, rhs objectio.ZoneMap,
	zone *time.Location,
	op string,
) (bool, bool) {
	if !lhs.IsInited() || !rhs.IsInited() {
		return false, false
	}
	if lhs.GetType() == rhs.GetType() {
		switch op {
		case ">":
			return lhs.AnyGT(rhs)
		case ">=":
			return lhs.AnyGE(rhs)
		case "<":
			return lhs.AnyLT(rhs)
		case "<=":
			return lhs.AnyLE(rhs)
		case "=":
			return lhs.Intersect(rhs)
		case "!=":
			return anyNotEqualZoneMap(lhs, rhs)
		default:
			return false, false
		}
	}
	if !isDatetimeTimestampZoneMapPair(lhs, rhs) {
		return false, false
	}

	timestampScale := lhs.GetScale()
	if rhs.GetType() == types.T_timestamp {
		timestampScale = rhs.GetScale()
	}
	lhsTimestamp, lhsOK := temporalZoneMapAsTimestampRange(lhs, zone, timestampScale)
	rhsTimestamp, rhsOK := temporalZoneMapAsTimestampRange(rhs, zone, timestampScale)
	if !lhsOK || !rhsOK {
		return false, false
	}
	switch op {
	case ">":
		return lhsTimestamp.max > rhsTimestamp.min, true
	case ">=":
		return lhsTimestamp.max >= rhsTimestamp.min, true
	case "<":
		return lhsTimestamp.min < rhsTimestamp.max, true
	case "<=":
		return lhsTimestamp.min <= rhsTimestamp.max, true
	case "=":
		return lhsTimestamp.max >= rhsTimestamp.min && lhsTimestamp.min <= rhsTimestamp.max, true
	case "!=":
		return lhsTimestamp.min != lhsTimestamp.max ||
			rhsTimestamp.min != rhsTimestamp.max ||
			lhsTimestamp.min != rhsTimestamp.min, true
	default:
		return false, false
	}
}

func foldTemporalBetweenZoneMap(
	proc *process.Process,
	args []*plan.Expr,
	zms []objectio.ZoneMap,
	auxID int32,
) bool {
	if len(args) != 3 {
		return false
	}
	hasDatetime, hasTimestamp := false, false
	for _, arg := range args {
		zm := zms[arg.AuxId]
		if !zm.IsInited() {
			return false
		}
		switch zm.GetType() {
		case types.T_datetime:
			hasDatetime = true
		case types.T_timestamp:
			hasTimestamp = true
		default:
			return false
		}
	}
	if !hasDatetime || !hasTimestamp {
		return false
	}

	zone := proc.GetSessionInfo().TimeZone
	valueZM := zms[args[0].AuxId]
	lowerZM := zms[args[1].AuxId]
	upperZM := zms[args[2].AuxId]
	if valueZM.GetType() == types.T_datetime &&
		lowerZM.GetType() == types.T_timestamp &&
		upperZM.GetType() == types.T_timestamp {
		timestampScale := lowerZM.GetScale()
		valueRange, valueOK := temporalZoneMapAsTimestampRange(valueZM, zone, timestampScale)
		lowerRange, lowerOK := temporalZoneMapAsTimestampRange(lowerZM, zone, timestampScale)
		upperRange, upperOK := temporalZoneMapAsTimestampRange(upperZM, zone, timestampScale)
		if !valueOK || !lowerOK || !upperOK {
			zms[auxID].Reset()
		} else {
			result := valueRange.max >= lowerRange.min && valueRange.min <= upperRange.max
			zms[auxID] = index.SetBool(zms[auxID], result)
		}
		return true
	}

	lowerResult, lowerOK := temporalZoneMapComparison(
		valueZM, lowerZM, zone, ">=",
	)
	upperResult, upperOK := temporalZoneMapComparison(
		valueZM, upperZM, zone, "<=",
	)
	if lowerOK && !lowerResult || upperOK && !upperResult {
		zms[auxID] = index.SetBool(zms[auxID], false)
	} else if lowerOK && upperOK {
		zms[auxID] = index.SetBool(zms[auxID], true)
	} else {
		zms[auxID].Reset()
	}
	return true
}

func isDatetimeTimestampZoneMapPair(lhs, rhs objectio.ZoneMap) bool {
	return lhs.GetType() == types.T_datetime && rhs.GetType() == types.T_timestamp ||
		lhs.GetType() == types.T_timestamp && rhs.GetType() == types.T_datetime
}

type temporalTimestampRange struct {
	min types.Timestamp
	max types.Timestamp
}

func temporalZoneMapAsTimestampRange(
	zm objectio.ZoneMap,
	zone *time.Location,
	timestampScale int32,
) (temporalTimestampRange, bool) {
	if zm.GetType() == types.T_timestamp {
		minValue, minOK := zm.GetMin().(types.Timestamp)
		maxValue, maxOK := zm.GetMax().(types.Timestamp)
		return temporalTimestampRange{min: minValue, max: maxValue}, minOK && maxOK
	}
	if zm.GetType() != types.T_datetime {
		return temporalTimestampRange{}, false
	}

	minValue, minOK := zm.GetMin().(types.Datetime)
	maxValue, maxOK := zm.GetMax().(types.Datetime)
	if !minOK || !maxOK {
		return temporalTimestampRange{}, false
	}

	var minTimestamp, maxTimestamp types.Timestamp
	if isSingleValueZoneMap(zm) {
		minTimestamp = minValue.ToTimestamp(zone).TruncateToScale(timestampScale)
		maxTimestamp = minTimestamp
	} else {
		var ok bool
		minTimestamp, maxTimestamp, ok = types.DatetimeRangeToTimestampRange(minValue, maxValue, zone)
		if !ok {
			return temporalTimestampRange{}, false
		}
		minTimestamp = minTimestamp.TruncateToScale(timestampScale)
		maxTimestamp = maxTimestamp.TruncateToScale(timestampScale)
	}
	return temporalTimestampRange{min: minTimestamp, max: maxTimestamp}, true
}

func isSingleValueZoneMap(zm objectio.ZoneMap) bool {
	if !zm.IsInited() {
		return false
	}
	return compute.Compare(zm.GetMinBuf(), zm.GetMaxBuf(), zm.GetType(), zm.GetScale(), zm.GetScale()) == 0
}

func foldAndZoneMap(
	ctx context.Context,
	proc *process.Process,
	meta objectio.ColumnMetaFetcher,
	columnMap map[int]int,
	zms []objectio.ZoneMap,
	vecs []*vector.Vector,
	args []*plan.Expr,
	auxID int32,
) bool {
	hasKnown := false
	hasUnknown := false

	for _, arg := range args {
		zms[arg.AuxId] = GetExprZoneMap(ctx, proc, arg, meta, columnMap, zms, vecs)
		if !zms[arg.AuxId].IsInited() || zms[arg.AuxId].GetType() != types.T_bool {
			hasUnknown = true
			continue
		}
		hasKnown = true
		if !types.DecodeBool(zms[arg.AuxId].GetMaxBuf()) {
			zms[auxID] = index.SetBool(zms[auxID], false)
			return true
		}
	}

	if hasUnknown || !hasKnown {
		zms[auxID].Reset()
		return false
	}
	zms[auxID] = index.SetBool(zms[auxID], true)
	return true
}

func foldOrZoneMap(
	ctx context.Context,
	proc *process.Process,
	meta objectio.ColumnMetaFetcher,
	columnMap map[int]int,
	zms []objectio.ZoneMap,
	vecs []*vector.Vector,
	args []*plan.Expr,
	auxID int32,
) bool {
	hasKnown := false
	hasUnknown := false

	for _, arg := range args {
		zms[arg.AuxId] = GetExprZoneMap(ctx, proc, arg, meta, columnMap, zms, vecs)
		if !zms[arg.AuxId].IsInited() || zms[arg.AuxId].GetType() != types.T_bool {
			hasUnknown = true
			continue
		}
		hasKnown = true
		if types.DecodeBool(zms[arg.AuxId].GetMaxBuf()) {
			zms[auxID] = index.SetBool(zms[auxID], true)
			return true
		}
	}

	if hasUnknown || !hasKnown {
		zms[auxID].Reset()
		return false
	}
	zms[auxID] = index.SetBool(zms[auxID], false)
	return true
}

// RewriteFilterExprList will convert an expression list to be an AndExpr
func RewriteFilterExprList(list []*plan.Expr) *plan.Expr {
	l := len(list)
	if l == 0 {
		return nil
	} else if l == 1 {
		return list[0]
	} else {
		left := list[0]
		right := RewriteFilterExprList(list[1:])
		return &plan.Expr{
			Typ:  left.Typ,
			Expr: makeAndExpr(left, right),
		}
	}
}

func SplitAndExprs(list []*plan.Expr) []*plan.Expr {
	exprs := make([]*plan.Expr, 0, len(list))
	for i := range list {
		exprs = append(exprs, splitAndExpr(list[i])...)
	}
	return exprs
}

func splitAndExpr(expr *plan.Expr) []*plan.Expr {
	if expr == nil {
		return nil
	}
	exprs := make([]*plan.Expr, 0, 1)
	if e, ok := expr.Expr.(*plan.Expr_F); ok {
		fid, _ := function.DecodeOverloadID(e.F.Func.GetObj())
		if fid == function.AND {
			exprs = append(exprs, splitAndExpr(e.F.Args[0])...)
			exprs = append(exprs, splitAndExpr(e.F.Args[1])...)
			return exprs
		}
	}
	exprs = append(exprs, expr)
	return exprs
}

func makeAndExpr(left, right *plan.Expr) *plan.Expr_F {
	return &plan.Expr_F{
		F: &plan.Function{
			Func: &plan.ObjectRef{
				Obj:     function.AndFunctionEncodedID,
				ObjName: function.AndFunctionName,
			},
			Args: []*plan.Expr{left, right},
		},
	}
}

func isAllConst(exprs []*plan.Expr) bool {
	for _, expr := range exprs {
		if !isConst(expr) {
			return false
		}
	}
	return true
}

func isConst(expr *plan.Expr) bool {
	switch t := expr.Expr.(type) {
	case *plan.Expr_Col:
		return false
	case *plan.Expr_F:
		return isAllConst(t.F.Args)
	default:
		return true
	}
}

type ExprEvalVector struct {
	Executor []ExpressionExecutor
	Vec      []*vector.Vector
	Typ      []types.Type
}

func MakeEvalVector(proc *process.Process, expressions []*plan.Expr) (ev ExprEvalVector, err error) {
	return MakeEvalVectorWithAllocation(proc, expressions, nil)
}

// MakeEvalVectorWithAllocation builds an evaluation vector whose expression-
// owned result storage has explicit physical allocation provenance. Borrowed
// column vectors retain their source ownership.
func MakeEvalVectorWithAllocation(
	proc *process.Process,
	expressions []*plan.Expr,
	selection *vector.AllocationAccountSelection,
) (ev ExprEvalVector, err error) {
	if len(expressions) == 0 {
		return
	}

	ev.Executor, err = NewExpressionExecutorsFromPlanExpressionsWithAllocation(
		proc,
		expressions,
		selection,
	)
	if err != nil {
		return
	}
	ev.Vec = make([]*vector.Vector, len(ev.Executor))
	ev.Typ = make([]types.Type, len(ev.Executor))
	for i, expr := range expressions {
		ev.Typ[i] = types.NewWithCharset(
			types.T(expr.Typ.Id), expr.Typ.Width, expr.Typ.Scale, uint8(expr.Typ.Charset),
		)
	}
	return
}

func (ev *ExprEvalVector) Free() {
	for i := range ev.Executor {
		if ev.Executor[i] != nil {
			ev.Executor[i].Free()
		}
	}
	ev.Executor = nil
}

func (ev *ExprEvalVector) ResetForNextQuery() {
	for i := range ev.Executor {
		if ev.Executor[i] != nil {
			ev.Executor[i].ResetForNextQuery()
		}
	}
}
