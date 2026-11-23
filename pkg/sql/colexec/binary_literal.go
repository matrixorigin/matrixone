// Copyright 2026 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// A value is a "binary literal" when it was spelled as a hex/bit literal, or
// comes from a binary-flagged prepared parameter or user variable. Vectors do
// not carry this fact: the producer of a value knows it and hands it to its
// consumer. Only leaf values qualify; a function result or a column reference
// is never a binary literal. The rules below are the single definition used by
// the expression executor (per kernel argument) and by the frontend (for a
// bound SET/SELECT ... INTO expression).

// LiteralIsBin reports whether a plan literal is a non-NULL binary literal.
func LiteralIsBin(lit *plan.Literal) bool {
	return lit != nil && !lit.GetIsnull() && lit.GetIsBin()
}

// paramTypeReadsTextTransport reports whether a parameter of this type is
// read directly from the TEXT transport. Other types go through a typed value
// or a conversion, whose result is not a binary literal.
func paramTypeReadsTextTransport(oid types.T) bool {
	return oid == types.T_any || oid.IsMySQLString()
}

// userVariableIsBin resolves the binary flag of a user variable's current
// value. A process without a resolver has no binary variables.
func userVariableIsBin(proc *process.Process, name string, system, global bool) (bool, error) {
	resolve := proc.GetResolveVariableIsBinFunc()
	if resolve == nil {
		return false, nil
	}
	return resolve(name, system, global)
}

// ExprIsBinLiteral reports whether the value of a bound expression is a
// binary literal under the current execution's prepared parameters and user
// variables. Without a process there are neither.
func ExprIsBinLiteral(proc *process.Process, expr *plan.Expr) (bool, error) {
	if expr == nil {
		return false, nil
	}
	if lit := expr.GetLit(); lit != nil {
		return LiteralIsBin(lit), nil
	}
	if proc == nil {
		return false, nil
	}
	switch t := expr.Expr.(type) {
	case *plan.Expr_P:
		if !paramTypeReadsTextTransport(types.T(expr.Typ.Id)) {
			return false, nil
		}
		pos := int(t.P.Pos)
		if !proc.GetPrepareParamIsBin(pos) {
			return false, nil
		}
		val, err := proc.GetPrepareParamsAt(pos)
		if err != nil {
			return false, err
		}
		return val != nil, nil
	case *plan.Expr_V:
		return userVariableIsBin(proc, t.V.Name, t.V.System, t.V.Global)
	default:
		return false, nil
	}
}

// isBinaryLiteralResult reports whether vec, just returned by executor, is
// a binary-literal leaf value. The identity check rejects placeholders the
// caller substituted (for example NULLs for skipped branches) and masked
// results.
func isBinaryLiteralResult(executor ExpressionExecutor, vec *vector.Vector) bool {
	if vec == nil {
		return false
	}
	for {
		switch source := executor.(type) {
		case *memoExpressionExecutor:
			executor = source.state.executor
		case *memoRootExpressionExecutor:
			executor = source.executor
		case *dependentScalarReference:
			executor = source.ExpressionExecutor
		case *dependentScalarExecutor:
			executor = source.ExpressionExecutor
		case *dependentValueReference:
			cell := &source.frame.cells[source.index]
			if vec != cell.value {
				return false
			}
			executor = cell.executor
		case *FixedVectorExpressionExecutor:
			return source.isBin && vec == source.resultVector
		case *ParamExpressionExecutor:
			return source.vecIsBin && vec == source.vec
		case *VarExpressionExecutor:
			return source.isBin && (vec == source.vec || vec == source.null)
		default:
			return false
		}
	}
}

// publishArgumentIsBin hands the kernel, through result, which of the current
// parameterResults are binary literals.
func (expr *FunctionExpressionExecutor) publishArgumentIsBin(result vector.FunctionResultWrapper) {
	found := false
	for i, executor := range expr.parameterExecutor {
		if i < len(expr.parameterResults) && isBinaryLiteralResult(executor, expr.parameterResults[i]) {
			found = true
			break
		}
	}
	if !found {
		result.SetArgumentIsBin(nil)
		return
	}
	if len(expr.argumentIsBin) != len(expr.parameterExecutor) {
		expr.argumentIsBin = make([]bool, len(expr.parameterExecutor))
	}
	for i, executor := range expr.parameterExecutor {
		expr.argumentIsBin[i] = i < len(expr.parameterResults) &&
			isBinaryLiteralResult(executor, expr.parameterResults[i])
	}
	result.SetArgumentIsBin(expr.argumentIsBin)
}
