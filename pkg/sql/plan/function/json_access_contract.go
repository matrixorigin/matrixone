// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

// ExpressionUsesJSONAccess follows typed generated dependencies to identify JSON
// access. It is a presence detector; planners still own the complete eligibility
// contract, while checkpoint export must retain the persisted semantic marker.
func ExpressionUsesJSONAccess(expr *plan.Expr, cols []*plan.ColDef, visiting map[int32]bool) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func != nil && (fn.Func.Obj == EncodeOverloadID(JSON_EXTRACT, 0) || fn.Func.Obj == EncodeOverloadID(JSON_UNQUOTE, 0)) {
			return true
		}
		for _, arg := range fn.Args {
			if ExpressionUsesJSONAccess(arg, cols, visiting) {
				return true
			}
		}
	}
	if ref := expr.GetCol(); ref != nil && ref.RelPos == 0 && ref.ColPos >= 0 && int(ref.ColPos) < len(cols) && !visiting[ref.ColPos] {
		col := cols[ref.ColPos]
		if col != nil && col.GeneratedCol != nil {
			visiting[ref.ColPos] = true
			found := ExpressionUsesJSONAccess(col.GeneratedCol.Expr, cols, visiting)
			delete(visiting, ref.ColPos)
			return found
		}
	}
	return false
}
