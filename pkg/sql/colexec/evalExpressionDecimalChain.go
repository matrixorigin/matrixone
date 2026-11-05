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

const decimalChainRows = 256

type decimalChainInput struct {
	column           *ColumnExpressionExecutor
	literal          types.Decimal64
	scale, precision int32
	constant         bool
	value            *vector.Vector
}
type decimalChainNode struct {
	left, right int
	op          int32
	typ         types.Type
	precision   int32
	scratch     *vector.Vector
}

// Negative references identify borrowed leaves; nonnegative references identify
// committed postorder nodes. This description is bounded at preparation.
type decimalChainExecutor struct {
	root   *FunctionExpressionExecutor
	inputs []decimalChainInput
	nodes  []decimalChainNode
	visits int
	casts  int
}

func (e *decimalChainExecutor) domain(ref int) (int32, int32) {
	if ref < 0 {
		v := e.inputs[-ref-1]
		return v.precision, v.scale
	}
	v := e.nodes[ref]
	return v.precision, v.typ.Scale
}
func (e *decimalChainExecutor) prepare(expr *plan.Expr, executor ExpressionExecutor, align int32) (int, bool) {
	e.visits++
	if e.visits > 32 {
		return 0, false
	}
	if expr == nil || expr.AuxId != 0 {
		return 0, false
	}
	// Bypass a cast only after proving its original precision boundary.
	// Scale-increasing column casts are admitted only under exact ADD/SUB alignment.
	if f := decimalCall(expr, function.CAST); f != nil {
		fn, ok := executor.(*FunctionExpressionExecutor)
		if ok && len(f.Args) == 2 && len(fn.parameterExecutor) == 2 && fn.overloadID == f.Func.Obj && !f.SyntaxExplicitCast {
			_, overload := function.DecodeOverloadID(f.Func.Obj)
			source := f.Args[0].Typ
			if overload == 0 && types.T(source.Id).IsDecimal() && types.T(expr.Typ.Id).IsDecimal() && expr.Typ.Scale >= source.Scale && expr.Typ.Width-expr.Typ.Scale >= source.Width-source.Scale && types.T(expr.Typ.Id).TypeLen() >= types.T(source.Id).TypeLen() {
				if expr.Typ.Scale > source.Scale && align < expr.Typ.Scale {
					return 0, false
				}
				ref, ok := e.prepare(f.Args[0], fn.parameterExecutor[0], align)
				if !ok {
					return 0, false
				}
				width, scale := e.domain(ref)
				if width-max(scale-expr.Typ.Scale, 0) > expr.Typ.Width {
					return 0, false
				}
				if expr.Typ.Id != source.Id || expr.Typ.Scale != source.Scale {
					e.casts++
				}
				return ref, true
			}
		}
	}
	if decimalLiteral(expr) {
		scale := expr.Typ.Scale
		if align >= 0 {
			scale = align
		}
		v, ok := decimalMulSubLiteralValue(expr, executor, scale)
		if !ok {
			return 0, false
		}
		signed := int64(v)
		magnitude := uint64(signed)
		if signed < 0 {
			magnitude = 0 - magnitude
		}
		precision := int32(1)
		for magnitude >= 10 {
			precision++
			magnitude /= 10
		}
		e.inputs = append(e.inputs, decimalChainInput{literal: v, precision: precision, scale: scale, constant: true})
		return -len(e.inputs), true
	}
	if col := decimalChainColumn(expr, executor); col != nil {
		if col.typ.Scale < 0 || col.typ.Width <= 0 || col.typ.Width > min(col.typ.Oid.ToType().Width, 65) || (align >= 0 && align < col.typ.Scale) {
			return 0, false
		}
		width := col.typ.Width
		if col.typ.Oid == types.T_decimal64 && width == 18 {
			width = 19
		}
		if col.typ.Oid == types.T_decimal128 && width == 38 {
			width = 39
		}
		scale := col.typ.Scale
		if align >= 0 {
			width += align - scale
			scale = align
		}
		e.inputs = append(e.inputs, decimalChainInput{column: col, precision: width, scale: scale})
		return -len(e.inputs), true
	}
	f := expr.GetF()
	fn, ok := executor.(*FunctionExpressionExecutor)
	if f == nil || f.Func == nil || !ok || len(f.Args) != 2 || len(fn.parameterExecutor) != 2 || fn.overloadID != f.Func.Obj || !types.T(expr.Typ.Id).IsDecimal() {
		return 0, false
	}
	op, _ := function.DecodeOverloadID(f.Func.Obj)
	if op != function.PLUS && op != function.MINUS && op != function.MULTI {
		return 0, false
	}
	childAlign := int32(-1)
	if op != function.MULTI {
		childAlign = expr.Typ.Scale
	}
	left, ok := e.prepare(f.Args[0], fn.parameterExecutor[0], childAlign)
	if !ok {
		return 0, false
	}
	right, ok := e.prepare(f.Args[1], fn.parameterExecutor[1], childAlign)
	if !ok {
		return 0, false
	}
	lw, ls := e.domain(left)
	rw, rs := e.domain(right)
	precision := max(lw, rw) + 1
	scale := ls
	if op == function.MULTI {
		precision = lw + rw
		scale = ls + rs
		if scale > 12 {
			return 0, false
		}
	} else if ls != rs {
		return 0, false
	}
	// The full declared domain must pass the original intermediate boundary,
	// even when a later operation could cancel or round its result.
	if scale != expr.Typ.Scale || precision > expr.Typ.Width || precision > 65 || (align >= 0 && align != scale) || len(e.nodes) >= 4 {
		return 0, false
	}
	oid := types.T_decimal64
	if precision > 18 {
		oid = types.T_decimal128
	}
	if precision > 38 {
		oid = types.T_decimal256
	}
	e.nodes = append(e.nodes, decimalChainNode{left: left, right: right, op: op, typ: types.New(oid, precision, scale), precision: precision})
	return len(e.nodes) - 1, true
}
func prepareDecimalChain(expr *plan.Expr, executor ExpressionExecutor) ExpressionExecutor {
	root, ok := executor.(*FunctionExpressionExecutor)
	if !ok || root == nil || expr == nil {
		return nil
	}
	f := expr.GetF()
	if f == nil || f.Func == nil || !types.T(expr.Typ.Id).IsDecimal() {
		return nil
	}
	op, _ := function.DecodeOverloadID(f.Func.Obj)
	if op != function.PLUS && op != function.MINUS && op != function.MULTI {
		return nil
	}
	e := &decimalChainExecutor{root: root}
	ref, ok := e.prepare(expr, executor, -1)
	if !ok || ref != len(e.nodes)-1 || len(e.nodes) < 1 || len(e.inputs) > 5 {
		return nil
	}
	if len(e.nodes) == 1 && e.casts == 0 {
		return nil
	}
	for _, input := range e.inputs {
		if !input.constant {
			return e
		}
	}
	return nil
}
func (e *decimalChainExecutor) releaseScratch() {
	for i := range e.nodes {
		if v := e.nodes[i].scratch; v != nil {
			v.Free(e.root.m)
			e.nodes[i].scratch = nil
		}
	}
	for i := range e.inputs {
		if e.inputs[i].constant && e.inputs[i].value != nil {
			e.inputs[i].value.Free(e.root.m)
			e.inputs[i].value = nil
		}
	}
}
func (e *decimalChainExecutor) allocateScratch(rows int) (err error) {
	defer func() {
		if err != nil {
			e.releaseScratch()
		}
	}()
	for i := range e.inputs {
		v := &e.inputs[i]
		if v.constant && v.value == nil {
			v.value, err = newExpressionConstFixed(types.New(types.T_decimal64, min(v.precision, 18), v.scale), v.literal, 1, e.root.m, e.root.allocation)
			if err != nil {
				return err
			}
		}
	}
	for i := 0; i < len(e.nodes)-1; i++ {
		v := &e.nodes[i]
		if v.scratch == nil {
			v.scratch, err = newExpressionOffHeapVector(v.typ, e.root.allocation)
			if err != nil {
				return err
			}
		}
		if err = v.scratch.PreExtend(rows, e.root.m); err != nil {
			return err
		}
		v.scratch.SetLength(rows)
	}
	return nil
}
func (e *decimalChainExecutor) operand(ref, start int) (*vector.Vector, int) {
	if ref >= 0 {
		return e.nodes[ref].scratch, 0
	}
	v := &e.inputs[-ref-1]
	if v.constant {
		return v.value, 0
	}
	return v.value, start
}
func (e *decimalChainExecutor) Eval(p *process.Process, b []*batch.Batch, selected []bool) (out *vector.Vector, err error) {
	if len(b) == 0 || selected != nil {
		return e.root.Eval(p, b, selected)
	}
	// Borrowed column vectors live only through this evaluation, including every
	// early fallback/error path. Constants and scratch have separate ownership.
	defer func() {
		for i := range e.inputs {
			if !e.inputs[i].constant {
				e.inputs[i].value = nil
			}
		}
	}()
	defer func() {
		if err != nil {
			e.releaseScratch()
		}
	}()
	n := expressionRowCount(b)
	for i := range e.inputs {
		v := &e.inputs[i]
		if v.constant {
			continue
		}
		v.value, err = v.column.Eval(p, b, nil)
		if err != nil {
			return nil, err
		}
		if !decimalChainSource(v.value, v.column, n) || v.value.IsConst() || v.value.HasNull() {
			return e.root.Eval(p, b, selected)
		}
	}
	if n == 0 {
		return e.root.Eval(p, b, selected)
	}
	if err = e.allocateScratch(min(n, decimalChainRows)); err != nil {
		return nil, err
	}
	e.root.resetResultType(e.root.resultVector)
	if err = e.root.resultVector.PreExtendAndReset(n); err != nil {
		return nil, err
	}
	out = e.root.resultVector.GetResultVector()
	out.SetLength(n)
	for start := 0; start < n; start += decimalChainRows {
		count := min(decimalChainRows, n-start)
		for i := range e.nodes {
			node := &e.nodes[i]
			x, xi := e.operand(node.left, start)
			y, yi := e.operand(node.right, start)
			dest, offset := node.scratch, 0
			if i == len(e.nodes)-1 {
				dest, offset = out, start
			}
			if err = function.DecimalArithmeticChunk(node.op, x, y, dest, xi, yi, offset, count); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}
func (e *decimalChainExecutor) EvalWithoutResultReusing(p *process.Process, b []*batch.Batch, s []bool) (*vector.Vector, error) {
	return e.root.EvalWithoutResultReusing(p, b, s)
}
func (e *decimalChainExecutor) ResetForNextQuery() { e.releaseScratch(); e.root.ResetForNextQuery() }
func (e *decimalChainExecutor) Free()              { e.releaseScratch(); e.root.Free() }
func (e *decimalChainExecutor) IsColumnExpr() bool { return false }
func (e *decimalChainExecutor) TypeName() string   { return "bounded decimal arithmetic" }

func decimalChainColumn(expr *plan.Expr, executor ExpressionExecutor) *ColumnExpressionExecutor {
	if expr == nil || expr.AuxId != 0 || expr.GetCol() == nil || !types.T(expr.Typ.Id).IsDecimal() {
		return nil
	}
	col, _ := executor.(*ColumnExpressionExecutor)
	return col
}
func decimalChainSource(v *vector.Vector, col *ColumnExpressionExecutor, n int) bool {
	if v == nil || v.HasGrouping() || v.Length() != n {
		return false
	}
	if v.IsConstNull() {
		return true
	}
	typ := v.GetType()
	return typ.Oid == col.typ.Oid && typ.Scale == col.typ.Scale && typ.Width <= col.typ.Width
}
