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
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// The generic tree remains the sole owner of executors, constants and output.
// The wrapper borrows leaves only; it adds no intermediate vector or state cache.
type decimalMulSubExecutor struct {
	root     *FunctionExpressionExecutor
	x, y     *ColumnExpressionExecutor
	constant types.Decimal64
}

func decimalCall(e *plan.Expr, fid int32) *plan.Function {
	if e == nil || e.AuxId != 0 {
		return nil
	}
	f := e.GetF()
	if f == nil || f.Func == nil {
		return nil
	}
	actual, _ := function.DecodeOverloadID(f.Func.Obj)
	if actual != fid {
		return nil
	}
	return f
}

func decimalWidenedColumn(e *plan.Expr, executor ExpressionExecutor) *ColumnExpressionExecutor {
	f := decimalCall(e, function.CAST)
	cast, ok := executor.(*FunctionExpressionExecutor)
	if f == nil || !ok || f.Func.ObjName != "cast" || f.SyntaxExplicitCast || len(f.Args) != 2 || len(cast.parameterExecutor) != 2 || cast.overloadID != f.Func.Obj ||
		types.T(e.Typ.Id) != types.T_decimal128 || types.T(f.Args[0].Typ.Id) != types.T_decimal64 ||
		e.Typ.Scale != f.Args[0].Typ.Scale || f.Args[0].Typ.Width <= 0 || f.Args[0].Typ.Width > 18 ||
		f.Args[0].Typ.Scale < 0 || e.Typ.Width < f.Args[0].Typ.Width {
		return nil
	}
	if f.Args[0].GetCol() == nil || f.Args[0].AuxId != 0 {
		return nil
	}
	col, _ := cast.parameterExecutor[0].(*ColumnExpressionExecutor)
	return col
}

func decimalLiteral(e *plan.Expr) bool {
	if e == nil || e.AuxId != 0 {
		return false
	}
	if l := e.GetLit(); l != nil {
		oid := types.T(e.Typ.Id)
		return !l.Isnull && (oid.IsInteger() || oid == types.T_decimal64 || oid == types.T_decimal128)
	}
	f := decimalCall(e, function.CAST)
	if f == nil || f.Func.ObjName != "cast" || f.SyntaxExplicitCast || len(f.Args) != 2 ||
		types.T(e.Typ.Id) != types.T_decimal128 || f.Args[0].GetLit() == nil || !decimalLiteral(f.Args[0]) {
		return false
	}
	// Only domain-preserving literal casts can be skipped/revisited without
	// changing warning or precision-error behavior.
	source := f.Args[0].Typ
	integerDigits := e.Typ.Width - e.Typ.Scale
	switch oid := types.T(source.Id); {
	case oid.IsSignedInt():
		return e.Typ.Scale >= 0 && integerDigits >= 19
	case oid.IsUnsignedInt():
		return e.Typ.Scale >= 0 && integerDigits >= 20
	default:
		return source.Width > 0 && e.Typ.Scale >= source.Scale && integerDigits >= source.Width-source.Scale
	}
}

// FuseDecimalAggregateArgument specializes only a complete aggregate argument.
// Generic expression constructors never call it; ineligible roots retain their owner.
func FuseDecimalAggregateArgument(e *plan.Expr, executor ExpressionExecutor) ExpressionExecutor {
	if root, ok := executor.(*FunctionExpressionExecutor); !ok || root == nil {
		return executor
	}
	if typed := prepareDecimalTyped(e, executor); typed != nil {
		return typed
	}
	if fused := fuseDecimalMulSubArgument(e, executor); fused != executor {
		return fused
	}
	if chain := prepareDecimalChain(e, executor); chain != nil {
		return chain
	}
	return executor
}

func fuseDecimalMulSubArgument(e *plan.Expr, executor ExpressionExecutor) ExpressionExecutor {
	root, ok := executor.(*FunctionExpressionExecutor)
	if !ok {
		return executor
	}
	f := decimalCall(e, function.MULTI)
	if f == nil || len(f.Args) != 2 || root.resultType.Oid != types.T_decimal128 || len(root.parameterExecutor) != 2 || root.overloadID != f.Func.Obj {
		return root
	}
	for _, subIndex := range []int{1, 0} {
		subPlan := f.Args[subIndex]
		subCall := decimalCall(subPlan, function.MINUS)
		sub, ok := root.parameterExecutor[subIndex].(*FunctionExpressionExecutor)
		if subCall == nil || !ok || len(subCall.Args) != 2 || len(sub.parameterExecutor) != 2 || sub.overloadID != subCall.Func.Obj || !decimalLiteral(subCall.Args[0]) ||
			types.T(subPlan.Typ.Id) != types.T_decimal128 ||
			subCall.Args[0].Typ.Scale > subCall.Args[1].Typ.Scale ||
			subPlan.Typ.Scale != subCall.Args[1].Typ.Scale {
			continue
		}
		x := decimalWidenedColumn(f.Args[1-subIndex], root.parameterExecutor[1-subIndex])
		y := decimalWidenedColumn(subCall.Args[1], sub.parameterExecutor[1])
		if x == nil || y == nil || x.typ.Scale+subPlan.Typ.Scale != root.resultType.Scale ||
			root.resultType.Scale > 12 || root.resultType.Scale < 0 {
			continue
		}
		constant, ok := decimalMulSubLiteralValue(subCall.Args[0], sub.parameterExecutor[0], subPlan.Typ.Scale)
		if !ok {
			continue
		}
		return &decimalMulSubExecutor{root: root, x: x, y: y, constant: constant}
	}
	return root
}

// Literal storage is already owned by the generic tree. Inspect only a bound
// numeric literal or its proven lossless cast; never run an expression here.
func decimalMulSubLiteralValue(planExpr *plan.Expr, executor ExpressionExecutor, scale int32) (types.Decimal64, bool) {
	if f := decimalCall(planExpr, function.CAST); f != nil {
		cast, ok := executor.(*FunctionExpressionExecutor)
		if !ok {
			return 0, false
		}
		executor = cast.parameterExecutor[0]
	}
	literal, ok := executor.(*FixedVectorExpressionExecutor)
	if !ok {
		return 0, false
	}
	v := literal.resultVector
	if v == nil || !v.IsConst() || v.IsConstNull() {
		return 0, false
	}
	var value int64
	sourceScale := int32(0)
	switch v.GetType().Oid {
	case types.T_int8:
		value = int64(vector.GetFixedAtNoTypeCheck[int8](v, 0))
	case types.T_int16:
		value = int64(vector.GetFixedAtNoTypeCheck[int16](v, 0))
	case types.T_int32:
		value = int64(vector.GetFixedAtNoTypeCheck[int32](v, 0))
	case types.T_int64:
		value = vector.GetFixedAtNoTypeCheck[int64](v, 0)
	case types.T_uint8:
		value = int64(vector.GetFixedAtNoTypeCheck[uint8](v, 0))
	case types.T_uint16:
		value = int64(vector.GetFixedAtNoTypeCheck[uint16](v, 0))
	case types.T_uint32:
		value = int64(vector.GetFixedAtNoTypeCheck[uint32](v, 0))
	case types.T_uint64:
		u := vector.GetFixedAtNoTypeCheck[uint64](v, 0)
		if u > math.MaxInt64 {
			return 0, false
		}
		value = int64(u)
	case types.T_decimal64:
		value = int64(vector.GetFixedAtNoTypeCheck[types.Decimal64](v, 0))
		sourceScale = v.GetType().Scale
	case types.T_decimal128:
		d := vector.GetFixedAtNoTypeCheck[types.Decimal128](v, 0)
		if d.B64_127 != uint64(int64(d.B0_63)>>63) {
			return 0, false
		}
		value = int64(d.B0_63)
		sourceScale = v.GetType().Scale
	default:
		return 0, false
	}
	if sourceScale > scale {
		return 0, false
	}
	for ; sourceScale < scale; sourceScale++ {
		if value > math.MaxInt64/10 || value < math.MinInt64/10 {
			return 0, false
		}
		value *= 10
	}
	return types.Decimal64(value), true
}

func decimalMulSubSource(v *vector.Vector, column *ColumnExpressionExecutor, n int) bool {
	if !v.GetGrouping().IsEmpty() {
		return false
	}
	// Generic column evaluation normalizes ordinary constant NULLs to the bound
	// column type without reading a value payload.
	if v.IsConstNull() {
		return true
	}
	t := v.GetType()
	return t.Oid == types.T_decimal64 && t.Scale == column.typ.Scale && t.Width <= column.typ.Width && v.Length() == n
}

func (e *decimalMulSubExecutor) Eval(proc *process.Process, batches []*batch.Batch, selected []bool) (*vector.Vector, error) {
	n := expressionRowCount(batches)
	if len(batches) == 0 || selected != nil {
		return e.root.Eval(proc, batches, selected)
	}
	x, err := e.x.Eval(proc, batches, nil)
	if err != nil {
		return nil, err
	}
	y, err := e.y.Eval(proc, batches, nil)
	if err != nil {
		return nil, err
	}
	if !decimalMulSubSource(x, e.x, n) || !decimalMulSubSource(y, e.y, n) {
		return e.root.Eval(proc, batches, selected)
	}
	e.root.resetResultType(e.root.resultVector)
	if err := e.root.resultVector.PreExtendAndReset(n); err != nil {
		return nil, err
	}
	out := e.root.resultVector.GetResultVector()
	out.SetLength(n)
	if !function.TryDecimalMulSub64(x, y, e.constant, out) {
		return e.root.Eval(proc, batches, selected)
	}
	return out, nil
}

func (e *decimalMulSubExecutor) EvalWithoutResultReusing(p *process.Process, b []*batch.Batch, s []bool) (*vector.Vector, error) {
	return e.root.EvalWithoutResultReusing(p, b, s)
}
func (e *decimalMulSubExecutor) ResetForNextQuery() { e.root.ResetForNextQuery() }
func (e *decimalMulSubExecutor) Free()              { e.root.Free() }
func (e *decimalMulSubExecutor) IsColumnExpr() bool { return false }
func (e *decimalMulSubExecutor) TypeName() string   { return "decimal multiply/subtract" }
