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

package rule

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// MarkExactNumeric uses the existing sparse provenance allocation so ordinary
// expressions retain their resident size. It does not select a prepared marker.
func MarkExactNumeric(expr *plan.Expr) {
	if expr.PreparedNumeric == nil {
		expr.PreparedNumeric = &plan.PreparedNumericMetadata{ParamPos: -1}
	}
	expr.PreparedNumeric.ExactNumeric = true
}

// IsExactNumeric keeps SQL numeric provenance independent of the physical
// execution type. resolve follows projection references before writer binding;
// folders use nil because their children already carry their source domains.
func IsExactNumeric(expr *plan.Expr, resolve func(*plan.Expr) bool) bool {
	if expr == nil || expr.GetP() != nil {
		return false
	}
	// A target-aware implicit cast around a marker is provisional, not proof
	// of an exact source. Defer its domain until execute-time specialization.
	if fn := expr.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "cast" && len(fn.Args) > 0 {
		_, overload := function.DecodeOverloadID(fn.Func.Obj)
		if overload == 0 && !fn.SyntaxExplicitCast {
			return IsExactNumeric(fn.Args[0], resolve)
		}
	}
	oid := types.T(expr.Typ.Id)
	if oid.IsInteger() || oid.IsDecimal() {
		return true
	}
	if !oid.IsFloat() {
		return false
	}
	if expr.GetPreparedNumeric().GetExactNumeric() {
		return true
	}
	if expr.GetCol() != nil && resolve != nil {
		return resolve(expr)
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) == 0 {
		return false
	}
	switch fn.Func.ObjName {
	case "cast":
		_, overload := function.DecodeOverloadID(fn.Func.Obj)
		return overload == 0 && !fn.SyntaxExplicitCast && IsExactNumeric(fn.Args[0], resolve)
	case "/", "+", "-", "*", "%", "abs", "unary_minus", "unary_plus":
		for _, arg := range fn.Args {
			if !IsExactNumeric(arg, resolve) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
