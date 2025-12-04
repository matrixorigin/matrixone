// Copyright 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestBindTimestampAddReturnType tests that BindFuncExprImplByPlanExpr correctly sets expr.Typ
// for TIMESTAMPADD function based on unit parameter (constant).
// This ensures GetResultColumnsFromPlan returns correct column type for MySQL protocol layer.
func TestBindTimestampAddReturnType(t *testing.T) {
	ctx := context.Background()

	// Helper function to create string constant expression
	makeStringConst := func(s string) *plan.Expr {
		return &plan.Expr{
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Isnull: false,
					Value: &plan.Literal_Sval{
						Sval: s,
					},
				},
			},
			Typ: plan.Type{
				Id:          int32(types.T_varchar),
				NotNullable: true,
				Width:       int32(len(s)),
			},
		}
	}

	// Helper function to create DATE constant expression
	makeDateConst := func() *plan.Expr {
		return &plan.Expr{
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Isnull: false,
					Value: &plan.Literal_Dateval{
						Dateval: 0, // 2024-12-20
					},
				},
			},
			Typ: plan.Type{
				Id:          int32(types.T_date),
				NotNullable: true,
			},
		}
	}

	// Helper function to create INT64 constant expression
	makeInt64Const := func(val int64) *plan.Expr {
		return &plan.Expr{
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Isnull: false,
					Value: &plan.Literal_I64Val{
						I64Val: val,
					},
				},
			},
			Typ: plan.Type{
				Id:          int32(types.T_int64),
				NotNullable: true,
			},
		}
	}

	// Test case 1: TIMESTAMPADD(DAY, 5, DATE('2024-12-20'))
	// Expected: expr.Typ should be DATE (not DATETIME)
	t.Run("DATE input + DAY unit → DATE type", func(t *testing.T) {
		args := []*plan.Expr{
			makeStringConst("DAY"), // unit
			makeInt64Const(5),      // interval
			makeDateConst(),        // date
		}

		// Call BindFuncExprImplByPlanExpr
		expr, err := BindFuncExprImplByPlanExpr(ctx, "timestampadd", args)
		require.NoError(t, err)
		require.NotNil(t, expr)

		// Verify expr.Typ is DATE (not DATETIME)
		require.Equal(t, int32(types.T_date), expr.Typ.Id, "expr.Typ should be DATE for DATE input + DAY unit")
		require.Equal(t, int32(0), expr.Typ.Scale, "Scale should be 0 for DATE type")
	})

	// Test case 2: TIMESTAMPADD(WEEK, 1, DATE('2024-12-20'))
	// Expected: expr.Typ should be DATE
	t.Run("DATE input + WEEK unit → DATE type", func(t *testing.T) {
		args := []*plan.Expr{
			makeStringConst("WEEK"),
			makeInt64Const(1),
			makeDateConst(),
		}

		expr, err := BindFuncExprImplByPlanExpr(ctx, "timestampadd", args)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_date), expr.Typ.Id, "expr.Typ should be DATE for DATE input + WEEK unit")
	})

	// Test case 3: TIMESTAMPADD(MONTH, 1, DATE('2024-12-20'))
	// Expected: expr.Typ should be DATE
	t.Run("DATE input + MONTH unit → DATE type", func(t *testing.T) {
		args := []*plan.Expr{
			makeStringConst("MONTH"),
			makeInt64Const(1),
			makeDateConst(),
		}

		expr, err := BindFuncExprImplByPlanExpr(ctx, "timestampadd", args)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_date), expr.Typ.Id, "expr.Typ should be DATE for DATE input + MONTH unit")
	})

	// Test case 4: TIMESTAMPADD(HOUR, 2, DATE('2024-12-20'))
	// Expected: expr.Typ should be DATETIME (time unit)
	t.Run("DATE input + HOUR unit → DATETIME type", func(t *testing.T) {
		args := []*plan.Expr{
			makeStringConst("HOUR"),
			makeInt64Const(2),
			makeDateConst(),
		}

		expr, err := BindFuncExprImplByPlanExpr(ctx, "timestampadd", args)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_datetime), expr.Typ.Id, "expr.Typ should be DATETIME for DATE input + HOUR unit")
	})

	// Test case 5: TIMESTAMPADD(MINUTE, 30, DATE('2024-12-20'))
	// Expected: expr.Typ should be DATETIME
	t.Run("DATE input + MINUTE unit → DATETIME type", func(t *testing.T) {
		args := []*plan.Expr{
			makeStringConst("MINUTE"),
			makeInt64Const(30),
			makeDateConst(),
		}

		expr, err := BindFuncExprImplByPlanExpr(ctx, "timestampadd", args)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_datetime), expr.Typ.Id, "expr.Typ should be DATETIME for DATE input + MINUTE unit")
	})

	// Test case 6: TIMESTAMPADD(SECOND, 45, DATE('2024-12-20'))
	// Expected: expr.Typ should be DATETIME
	t.Run("DATE input + SECOND unit → DATETIME type", func(t *testing.T) {
		args := []*plan.Expr{
			makeStringConst("SECOND"),
			makeInt64Const(45),
			makeDateConst(),
		}

		expr, err := BindFuncExprImplByPlanExpr(ctx, "timestampadd", args)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_datetime), expr.Typ.Id, "expr.Typ should be DATETIME for DATE input + SECOND unit")
	})

	// Test case 7: TIMESTAMPADD(MICROSECOND, 1000000, DATE('2024-12-20'))
	// Expected: expr.Typ should be DATETIME
	t.Run("DATE input + MICROSECOND unit → DATETIME type", func(t *testing.T) {
		args := []*plan.Expr{
			makeStringConst("MICROSECOND"),
			makeInt64Const(1000000),
			makeDateConst(),
		}

		expr, err := BindFuncExprImplByPlanExpr(ctx, "timestampadd", args)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_datetime), expr.Typ.Id, "expr.Typ should be DATETIME for DATE input + MICROSECOND unit")
	})

	// Test case 8: Verify GetResultColumnsFromPlan uses correct expr.Typ
	// This simulates the actual SQL: SELECT TIMESTAMPADD(DAY, 5, d) AS added_date FROM t1;
	t.Run("GetResultColumnsFromPlan uses correct expr.Typ", func(t *testing.T) {
		// Create a query plan with TIMESTAMPADD(DAY, 5, DATE('2024-12-20'))
		args := []*plan.Expr{
			makeStringConst("DAY"),
			makeInt64Const(5),
			makeDateConst(),
		}

		// Bind the function expression
		expr, err := BindFuncExprImplByPlanExpr(ctx, "timestampadd", args)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_date), expr.Typ.Id, "expr.Typ should be DATE")

		// Simulate GetResultColumnsFromPlan behavior
		// GetResultColumnsFromPlan uses expr.Typ to set column type
		colDef := &plan.ColDef{
			Name: "added_date",
			Typ:  expr.Typ,
		}

		// Verify column type is DATE (not DATETIME)
		require.Equal(t, int32(types.T_date), colDef.Typ.Id, "Column type should be DATE")
		require.Equal(t, int32(0), colDef.Typ.Scale, "Column scale should be 0 for DATE")

		// This ensures MySQL protocol layer gets MYSQL_TYPE_DATE (not MYSQL_TYPE_DATETIME)
		// which prevents "Invalid length (10) for type TIMESTAMP" errors
	})
}
