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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestMixedJSONNumericEquality(t *testing.T) {
	col := func(typ types.T) *plan.Expr {
		return &plan.Expr{Typ: typ.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{}}}
	}
	expr := func(id int32, args ...*plan.Expr) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(id, 0), ObjName: "deliberately_not_the_identity"}, Args: args}}}
	}
	mixed := expr(function.EQUAL, col(types.T_json), col(types.T_int64))
	adapter := expr(function.INTERNAL_JSON_COMPARISON_PARAM, col(types.T_any))
	adapter.Typ = types.T_json.ToType().PlanType()
	for _, tc := range []struct {
		name string
		e    *plan.Expr
		want bool
	}{
		{"nil", nil, false}, {"mixed", mixed, true}, {"reversed", expr(function.EQUAL, col(types.T_float32), col(types.T_json)), true},
		{"same_json", expr(function.EQUAL, col(types.T_json), col(types.T_json)), false},
		{"json_string", expr(function.EQUAL, col(types.T_json), col(types.T_varchar)), false},
		{"not_equality", expr(function.GREAT_EQUAL, col(types.T_json), col(types.T_int64)), false},
		{"nested_and", expr(function.AND, expr(function.EQUAL, col(types.T_int64), col(types.T_int64)), mixed), true},
		{"prepared", expr(function.EQUAL, col(types.T_json), adapter), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, HasMixedJSONNumericEquality(tc.e))
			if tc.e != nil {
				data, err := tc.e.Marshal()
				require.NoError(t, err)
				var decoded plan.Expr
				require.NoError(t, decoded.Unmarshal(data))
				require.Equal(t, tc.want, HasMixedJSONNumericEquality(&decoded))
			}
		})
	}
}
