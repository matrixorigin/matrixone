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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestDecimalModLiteralMetadata(t *testing.T) {
	for _, tc := range []struct {
		expression   string
		width, scale int32
	}{
		{"mod(cast(1.23 as decimal(3,2)),2)", 3, 2},
		{"cast(1.23 as decimal(3,2)) % -2", 3, 2},
		{"mod(cast(1.23 as decimal(3,2)),0)", 3, 2},
		{"mod(cast(1.23 as decimal(3,2)),1000)", 4, 2},
		{"mod(cast(1.23 as decimal(3,2)),cast(2 as signed))", 19, 2},
		{"mod(cast(1.23 as decimal(3,2)),cast(2 as decimal(12,0)))", 12, 2},
		{"mod(cast(1.23 as decimal(3,2)),-9223372036854775808)", 19, 2},
		{"mod(cast(1.23 as decimal(3,2)),18446744073709551615)", 20, 2},
		{"mod(1.234,2)", 4, 3},
		{"mod(2,1.234)", 4, 3},
		{"mod(0.5,2)", 1, 1},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			query, err := runOneExprStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "select "+tc.expression)
			require.NoError(t, err)
			expr := query.GetQuery().Nodes[query.GetQuery().Steps[0]].ProjectList[0]
			require.Equal(t, tc.width, expr.Typ.Width)
			require.Equal(t, tc.scale, expr.Typ.Scale)
		})
	}
}

func TestDecimalModLiteralPreservesExecutionCasts(t *testing.T) {
	decimal := types.New(types.T_decimal64, 3, 2)
	for _, name := range []string{"mod", "%"} {
		expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, []*planpb.Expr{
			decimalArithmeticTestColumn(decimal, 0), makePlan2Int64ConstExprWithType(2),
		})
		require.NoError(t, err)
		require.Equal(t, int32(3), expr.Typ.Width)
		require.Equal(t, int32(types.T_decimal128), expr.Typ.Id)
		args := expr.GetF().Args
		require.Equal(t, int32(types.T_decimal128), args[0].Typ.Id)
		require.Equal(t, int32(types.T_decimal128), args[1].Typ.Id)
		require.Equal(t, int32(38), args[0].Typ.Width)
		require.Equal(t, int32(38), args[1].Typ.Width)
	}
}
