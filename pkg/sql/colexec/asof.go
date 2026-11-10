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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// HasMixedJSONNumericEquality identifies ASOF predicates whose eligibility
// cannot be represented by ordinary equality hash keys. It must not be used
// to admit these predicates as hash, shuffle or runtime-filter keys.
func HasMixedJSONNumericEquality(exprs ...*plan.Expr) bool {
	for _, expr := range exprs {
		f := expr.GetF()
		if f == nil || f.Func == nil || len(f.Args) != 2 {
			continue
		}
		id, _ := function.DecodeOverloadID(f.Func.Obj)
		if id == function.AND {
			if HasMixedJSONNumericEquality(f.Args...) {
				return true
			}
			continue
		}
		if id != function.EQUAL {
			continue
		}
		if function.IsJSONNumericComparison(types.T(f.Args[0].Typ.Id), types.T(f.Args[1].Typ.Id)) {
			return true
		}
		// A prepared adapter has JSON output but carries the SQL peer's runtime
		// domain. Conservatively use eligibility scanning before it is rebound.
		for _, arg := range f.Args {
			if adapter := arg.GetF(); adapter != nil && adapter.Func != nil {
				adapterID, _ := function.DecodeOverloadID(adapter.Func.Obj)
				if adapterID == function.INTERNAL_JSON_COMPARISON_PARAM {
					return true
				}
			}
		}
	}
	return false
}
