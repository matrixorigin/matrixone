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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// preparedPaginationTopBound reads only an already-bound, decimal integer
// marker (or uint64 literal), including its implicit integer cast chain. It
// never executes an expression, emits diagnostics, or mutates the cached plan.
// Values outside the resident Top policy, NULL, noncanonical encodings and
// unproved conversion domains are left to the existing runtime operators.
func preparedPaginationTopBound(proc *process.Process, expr *plan.Expr) (uint64, bool, bool) {
	if proc == nil || proc.Base == nil || expr == nil || types.T(expr.Typ.Id) != types.T_uint64 {
		return 0, false, false
	}
	if lit := expr.GetLit(); lit != nil {
		value, ok := lit.Value.(*plan.Literal_U64Val)
		if !ok || lit.Isnull || lit.Src != nil || value.U64Val > mergeTopResidentPlanThreshold {
			return 0, false, false
		}
		return value.U64Val, false, true
	}

	maxValue := mergeTopResidentPlanThreshold
	for depth := 0; depth < 4; depth++ {
		if param := expr.GetP(); param != nil {
			if param.Pos < 0 || proc.GetPrepareParams() == nil || types.T(expr.Typ.Id) == types.T_any {
				return 0, false, false
			}
			if !preparedPaginationSourceDomain(types.T(expr.Typ.Id), &maxValue, true) {
				return 0, false, false
			}
			pos := int(param.Pos)
			kind := proc.GetPrepareParamKind(pos)
			if (kind != vector.PrepareParamNone && kind != vector.PrepareParamInteger && kind != vector.PrepareParamBoolean) ||
				proc.GetPrepareParamIsBin(pos) || proc.GetPrepareParamIsBinaryString(pos) ||
				!preparedPaginationSourceDomain(proc.GetPrepareParamType(pos), &maxValue, true) {
				return 0, false, false
			}
			if kind == vector.PrepareParamBoolean {
				maxValue = min(maxValue, 1)
			}
			value, err := proc.GetPrepareParamsAt(pos)
			if err != nil || len(value) == 0 || len(value) > 20 || (maxValue == 1 && len(value) != 1) {
				return 0, false, false
			}
			var number uint64
			for _, digit := range value {
				if digit < '0' || digit > '9' {
					return 0, false, false
				}
				number = number*10 + uint64(digit-'0')
				if number > maxValue {
					return 0, false, false
				}
			}
			return number, true, true
		}
		fn := expr.GetF()
		if fn == nil || fn.Func == nil || fn.GetSyntaxExplicitCast() || len(fn.Args) != 2 ||
			fn.Args[0] == nil || fn.Args[1] == nil || fn.Args[1].GetT() == nil ||
			fn.Args[1].Typ.Id != expr.Typ.Id {
			return 0, false, false
		}
		id, _ := function.DecodeOverloadID(fn.Func.Obj)
		if id != function.CAST || !preparedPaginationSourceDomain(types.T(expr.Typ.Id), &maxValue, false) {
			return 0, false, false
		}
		expr = fn.Args[0]
	}
	return 0, false, false
}

func preparedPaginationSourceDomain(typ types.T, maxValue *uint64, allowText bool) bool {
	switch typ {
	case types.T_any, types.T_text, types.T_varchar:
		return allowText
	case types.T_bool:
		*maxValue = min(*maxValue, 1)
	case types.T_int8:
		*maxValue = min(*maxValue, 127)
	case types.T_uint8:
		*maxValue = min(*maxValue, 255)
	case types.T_int16, types.T_uint16, types.T_int32, types.T_uint32, types.T_int64, types.T_uint64:
		// Every value within the resident threshold fits these integer domains.
	default:
		return false
	}
	return true
}

func (c *Compile) preparedPaginationTop(node *plan.Node) (uint64, bool) {
	if c == nil || c.isPrepare || c.pn == nil || !c.pn.IsPrepare || c.proc == nil || node == nil {
		return 0, false
	}
	limit, limitParam, ok := preparedPaginationTopBound(c.proc, node.Limit)
	if !ok || limit == 0 {
		return 0, false
	}
	offset, offsetParam, ok := preparedPaginationTopBound(c.proc, node.Offset)
	if !ok || (!limitParam && !offsetParam) || offset > mergeTopResidentPlanThreshold-limit {
		return 0, false
	}
	if !plan2.PreparedPaginationTopInputSafe(c.pn.GetQuery(), node) {
		return 0, false
	}
	return limit + offset, true
}
