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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// NumericFunctionResultArgs returns the argument positions whose numeric
// domains determine the function result. Conditions, comparison operands and
// precision arguments are intentionally excluded. Keep numeric context and
// exact-domain reconstruction on this single contract. Relational dependency
// discovery includes aggregates; scalar context seeding must leave aggregates
// to their own binder (which supplies the deferred numeric input envelope).
func NumericFunctionResultArgs(name string, argCount int, includeAggregates bool) ([]int, bool) {
	all := func() ([]int, bool) {
		if argCount == 0 {
			return nil, false
		}
		indexes := make([]int, argCount)
		for i := range indexes {
			indexes[i] = i
		}
		return indexes, true
	}

	switch name {
	case "+", "-", "*", "/", "%", "mod", "div":
		if argCount != 2 {
			return nil, false
		}
		return []int{0, 1}, true
	case "lag", "lead":
		if !includeAggregates || argCount < 1 || argCount > 3 {
			return nil, false
		}
		if argCount == 3 {
			return []int{0, 2}, true
		}
		return []int{0}, true
	case "nth_value":
		if !includeAggregates || argCount != 2 {
			return nil, false
		}
		return []int{0}, true
	case "sum", "min", "max", "avg", "first_value", "last_value":
		if !includeAggregates || argCount != 1 {
			return nil, false
		}
		return []int{0}, true
	case "unary_plus", "unary_minus", "abs", "ceil", "ceiling", "floor":
		if argCount != 1 {
			return nil, false
		}
		return []int{0}, true
	case "round":
		if argCount != 1 && argCount != 2 {
			return nil, false
		}
		return []int{0}, true
	case "truncate":
		if argCount != 2 {
			return nil, false
		}
		return []int{0}, true
	case "if":
		if argCount != 3 {
			return nil, false
		}
		return []int{1, 2}, true
	case "case":
		if argCount < 2 {
			return nil, false
		}
		indexes := make([]int, 0, (argCount+1)/2)
		for i := 1; i < argCount; i += 2 {
			indexes = append(indexes, i)
		}
		if last := argCount - 1; indexes[len(indexes)-1] != last {
			indexes = append(indexes, last)
		}
		return indexes, true
	case "coalesce", "greatest", "least":
		return all()
	case "ifnull":
		if argCount != 2 {
			return nil, false
		}
		return []int{0, 1}, true
	case "nullif":
		if argCount != 2 {
			return nil, false
		}
		return []int{0}, true
	default:
		return nil, false
	}
}

// IsExactNumericExpression keeps logical SQL numeric provenance independent
// of the physical FLOAT execution type. resolve follows a projection ColRef.
func IsExactNumericExpression(expr *plan.Expr, resolve func(*plan.Expr) bool) bool {
	if expr == nil || expr.GetP() != nil {
		return false
	}
	if lit := expr.GetLit(); lit != nil {
		if lit.Isnull {
			return true
		}
		if types.T(expr.Typ.Id).IsFloat() && lit.Src != nil {
			return IsExactNumericExpression(lit.Src, resolve)
		}
	}
	oid := types.T(expr.Typ.Id)
	if oid.IsInteger() || oid.IsDecimal() {
		return true
	}
	if !oid.IsFloat() {
		return false
	}
	if expr.GetCol() != nil {
		return resolve != nil && resolve(expr)
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	name := fn.Func.GetObjName()
	if name == "cast" {
		_, overload := DecodeOverloadID(fn.Func.GetObj())
		return overload == 0 && !fn.SyntaxExplicitCast && len(fn.Args) > 0 &&
			IsExactNumericExpression(fn.Args[0], resolve)
	}
	indexes, ok := NumericFunctionResultArgs(name, len(fn.Args), false)
	if !ok {
		return false
	}
	for _, index := range indexes {
		if !IsExactNumericExpression(fn.Args[index], resolve) {
			return false
		}
	}
	return true
}
