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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func boolCmpTestBoolCol() *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "b"}},
	}
}

func boolCmpTestInt(v int64) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: v}}},
	}
}

// In MySQL TRUE is 1, so `b = 2` is false for every row. Only the literals 0
// and 1 may be rewritten to FALSE/TRUE next to a BOOL in a comparison.
func TestConvertValueIntoBoolComparisonKeepsOtherIntegers(t *testing.T) {
	for _, v := range []int64{2, -1, 100} {
		args := []*plan.Expr{boolCmpTestBoolCol(), boolCmpTestInt(v)}
		require.NoError(t, convertValueIntoBool("=", args, false))
		require.Equal(t, int32(types.T_int64), args[1].Typ.Id, "literal %d", v)
		require.Equal(t, v, args[1].GetLit().GetI64Val(), "literal %d", v)
	}
	for v, want := range map[int64]bool{0: false, 1: true} {
		args := []*plan.Expr{boolCmpTestInt(v), boolCmpTestBoolCol()}
		require.NoError(t, convertValueIntoBool("<>", args, false))
		require.Equal(t, int32(types.T_bool), args[0].Typ.Id, "literal %d", v)
		require.Equal(t, want, args[0].GetLit().GetBval(), "literal %d", v)
	}
}

// In AND/OR/NOT/XOR every non-zero integer is TRUE.
func TestConvertValueIntoBoolLogicTreatsNonZeroAsTrue(t *testing.T) {
	args := []*plan.Expr{boolCmpTestInt(2), boolCmpTestInt(0)}
	require.NoError(t, convertValueIntoBool("and", args, true))
	require.True(t, args[0].GetLit().GetBval())
	require.False(t, args[1].GetLit().GetBval())
	require.Equal(t, int32(types.T_bool), args[0].Typ.Id)
}
