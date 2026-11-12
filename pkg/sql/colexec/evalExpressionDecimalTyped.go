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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type decimalTypedExecutor struct {
	root        *FunctionExpressionExecutor
	left, right *ColumnExpressionExecutor
	op          int32
}

func decimalTypedColumn(e *plan.Expr, executor ExpressionExecutor) *ColumnExpressionExecutor {
	if e == nil || e.AuxId != 0 {
		return nil
	}
	if e.GetCol() != nil && types.T(e.Typ.Id) == types.T_decimal64 {
		col, _ := executor.(*ColumnExpressionExecutor)
		return col
	}
	f := decimalCall(e, function.CAST)
	cast, ok := executor.(*FunctionExpressionExecutor)
	if f == nil || !ok || f.SyntaxExplicitCast || f.Func.ObjName != "cast" || len(f.Args) != 2 || len(cast.parameterExecutor) != 2 || cast.overloadID != f.Func.Obj {
		return nil
	}
	_, overload := function.DecodeOverloadID(f.Func.Obj)
	if overload != 0 {
		return nil
	}
	source := f.Args[0]
	target := types.T(e.Typ.Id)
	if source.GetCol() == nil || source.AuxId != 0 || types.T(source.Typ.Id) != types.T_decimal64 || (target != types.T_decimal128 && target != types.T_decimal256) || e.Typ.Scale != source.Typ.Scale || source.Typ.Width <= 0 || source.Typ.Width > 18 || e.Typ.Width < source.Typ.Width {
		return nil
	}
	width := source.Typ.Width
	if width == 18 {
		width = 19
	}
	if e.Typ.Width < width {
		return nil
	}
	col, _ := cast.parameterExecutor[0].(*ColumnExpressionExecutor)
	return col
}

func prepareDecimalTyped(e *plan.Expr, executor ExpressionExecutor) ExpressionExecutor {
	root, ok := executor.(*FunctionExpressionExecutor)
	if !ok || e == nil || e.AuxId != 0 || len(root.parameterExecutor) != 2 {
		return nil
	}
	f := e.GetF()
	if f == nil || f.Func == nil || len(f.Args) != 2 || root.overloadID != f.Func.Obj {
		return nil
	}
	op, _ := function.DecodeOverloadID(f.Func.Obj)
	if op != function.PLUS && op != function.MINUS && op != function.MULTI {
		return nil
	}
	a, b := decimalTypedColumn(f.Args[0], root.parameterExecutor[0]), decimalTypedColumn(f.Args[1], root.parameterExecutor[1])
	if a == nil || b == nil || a.typ.Width <= 0 || b.typ.Width <= 0 || a.typ.Scale < 0 || b.typ.Scale < 0 {
		return nil
	}
	// Native operands already reach the existing typed arithmetic kernel.
	// Replacing that route removes no conversion and adds chunk/broadcast work.
	if types.T(f.Args[0].Typ.Id) == a.typ.Oid && types.T(f.Args[1].Typ.Id) == b.typ.Oid {
		return nil
	}
	aw, bw := a.typ.Width, b.typ.Width
	// The legacy precision-18 carrier may retain a full signed-int64 value.
	if aw == 18 {
		aw = 19
	}
	if bw == 18 {
		bw = 19
	}
	precision := max(aw, bw) + 1
	if op == function.MULTI {
		precision = aw + bw
		if root.resultType.Scale != a.typ.Scale+b.typ.Scale || root.resultType.Scale > 12 {
			return nil
		}
	} else if a.typ.Scale != b.typ.Scale || root.resultType.Scale != a.typ.Scale {
		return nil
	}
	if precision > root.resultType.Width || !root.resultType.Oid.IsDecimal() {
		return nil
	}
	return &decimalTypedExecutor{root: root, left: a, right: b, op: op}
}

func (e *decimalTypedExecutor) Eval(p *process.Process, b []*batch.Batch, s []bool) (*vector.Vector, error) {
	if len(b) == 0 || s != nil {
		return e.root.Eval(p, b, s)
	}
	n := expressionRowCount(b)
	x, err := e.left.Eval(p, b, nil)
	if err != nil {
		return nil, err
	}
	y, err := e.right.Eval(p, b, nil)
	if err != nil {
		return nil, err
	}
	if !decimalMulSubSource(x, e.left, n) || !decimalMulSubSource(y, e.right, n) || x.IsConst() || y.IsConst() || x.HasNull() || y.HasNull() {
		return e.root.Eval(p, b, s)
	}
	e.root.resetResultType(e.root.resultVector)
	if err = e.root.resultVector.PreExtendAndReset(n); err != nil {
		return nil, err
	}
	out := e.root.resultVector.GetResultVector()
	out.SetLength(n)
	if !function.TryDecimal64Binary(e.op, x, y, out) {
		return e.root.Eval(p, b, s)
	}
	return out, nil
}
func (e *decimalTypedExecutor) EvalWithoutResultReusing(p *process.Process, b []*batch.Batch, s []bool) (*vector.Vector, error) {
	return e.root.EvalWithoutResultReusing(p, b, s)
}
func (e *decimalTypedExecutor) ResetForNextQuery() { e.root.ResetForNextQuery() }
func (e *decimalTypedExecutor) Free()              { e.root.Free() }
func (e *decimalTypedExecutor) IsColumnExpr() bool { return false }
func (e *decimalTypedExecutor) TypeName() string   { return "typed decimal arithmetic" }
