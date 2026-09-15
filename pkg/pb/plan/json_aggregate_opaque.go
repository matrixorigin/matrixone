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

package plan

const (
	jsonArrayAggFunctionID  int32 = 400
	jsonObjectAggFunctionID int32 = 401
	// Function.Obj reserves its high bit for DISTINCT. Keep the mask local to
	// this low-level package to avoid importing the SQL planner/function layer.
	jsonAggregateFunctionIDMask uint64 = 0x7fffffffffffffff

	jsonAggregateBitType       int32 = 11
	jsonAggregateBinaryType    int32 = 64
	jsonAggregateVarbinaryType int32 = 65
	jsonAggregateBlobType      int32 = 70
)

// RequiresJSONAggregateOpaqueValues reports whether an expression owner
// contains a BIT or binary-family value argument to JSON_ARRAYAGG or
// JSON_OBJECTAGG. It deliberately inspects only the value position; object
// key coercion remains governed by the existing planner and executor rules.
func RequiresJSONAggregateOpaqueValues(owner any) (bool, error) {
	required := false
	err := VisitExpressionsInOwner(owner, func(expr *Expr) error {
		return VisitExprTree(expr, func(current *Expr) error {
			fn := current.GetF()
			if fn == nil || fn.Func == nil {
				return nil
			}
			functionID := int32((uint64(fn.Func.Obj) & jsonAggregateFunctionIDMask) >> 32)
			valueIndex := -1
			switch functionID {
			case jsonArrayAggFunctionID:
				valueIndex = 0
			case jsonObjectAggFunctionID:
				valueIndex = 1
			default:
				return nil
			}
			if valueIndex >= 0 && valueIndex < len(fn.Args) &&
				isJSONAggregateOpaqueType(fn.Args[valueIndex]) {
				required = true
			}
			return nil
		})
	})
	return required, err
}

func isJSONAggregateOpaqueType(expr *Expr) bool {
	if expr == nil {
		return false
	}
	switch expr.Typ.Id {
	case jsonAggregateBitType,
		jsonAggregateBinaryType,
		jsonAggregateVarbinaryType,
		jsonAggregateBlobType:
		return true
	default:
		return false
	}
}
