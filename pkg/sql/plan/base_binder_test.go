// Copyright 2022 Matrix Origin
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

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// getIntervalExprArgs
// ============================================================================

func makeListExpr(list []*plan.Expr) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_List{
			List: &plan.ExprList{List: list},
		},
	}
}

func TestGetIntervalExprArgs_NilExpr(t *testing.T) {
	ctx := context.Background()
	expr1, expr2, err := getIntervalExprArgs(ctx, nil)
	require.Error(t, err)
	require.Nil(t, expr1)
	require.Nil(t, expr2)
}

func TestGetIntervalExprArgs_NotList(t *testing.T) {
	ctx := context.Background()
	expr := makePlan2Int64ConstExprWithType(42)
	expr1, expr2, err := getIntervalExprArgs(ctx, expr)
	require.Error(t, err)
	require.Nil(t, expr1)
	require.Nil(t, expr2)
}

func TestGetIntervalExprArgs_TooShort(t *testing.T) {
	ctx := context.Background()
	expr := makeListExpr([]*plan.Expr{makePlan2Int64ConstExprWithType(1)})
	expr1, expr2, err := getIntervalExprArgs(ctx, expr)
	require.Error(t, err)
	require.Nil(t, expr1)
	require.Nil(t, expr2)
}

func TestGetIntervalExprArgs_Valid(t *testing.T) {
	ctx := context.Background()
	first := makePlan2Int64ConstExprWithType(3)
	second := makePlan2StringConstExprWithType("day")
	expr := makeListExpr([]*plan.Expr{first, second})

	expr1, expr2, err := getIntervalExprArgs(ctx, expr)
	require.NoError(t, err)
	require.Equal(t, first, expr1)
	require.Equal(t, second, expr2)
}

// ============================================================================
// handleTupleIn
// ============================================================================

func TestHandleTupleIn_SingleTuple_EqualLength(t *testing.T) {
	ctx := context.Background()
	// (a,b) IN ((1,'x'))
	leftList := &plan.Expr_List{
		List: &plan.ExprList{
			List: []*plan.Expr{
				makePlan2Int64ConstExprWithType(1),
				makePlan2StringConstExprWithType("x"),
			},
		},
	}
	rightList := &plan.ExprList{
		List: []*plan.Expr{
			makeListExpr([]*plan.Expr{
				makePlan2Int64ConstExprWithType(1),
				makePlan2StringConstExprWithType("x"),
			}),
		},
	}

	result, err := handleTupleIn(ctx, "in", leftList, rightList)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestHandleTupleIn_SingleTuple_LengthMismatch(t *testing.T) {
	ctx := context.Background()
	// (a,b) IN ((1,))  -- mismatch
	leftList := &plan.Expr_List{
		List: &plan.ExprList{
			List: []*plan.Expr{
				makePlan2Int64ConstExprWithType(1),
				makePlan2StringConstExprWithType("x"),
			},
		},
	}
	rightList := &plan.ExprList{
		List: []*plan.Expr{
			makeListExpr([]*plan.Expr{
				makePlan2Int64ConstExprWithType(1),
			}),
		},
	}

	_, err := handleTupleIn(ctx, "in", leftList, rightList)
	require.Error(t, err)
}

func TestHandleTupleIn_NotIn(t *testing.T) {
	ctx := context.Background()
	// (a,b) NOT IN ((1,'x'))
	leftList := &plan.Expr_List{
		List: &plan.ExprList{
			List: []*plan.Expr{
				makePlan2Int64ConstExprWithType(1),
				makePlan2StringConstExprWithType("x"),
			},
		},
	}
	rightList := &plan.ExprList{
		List: []*plan.Expr{
			makeListExpr([]*plan.Expr{
				makePlan2Int64ConstExprWithType(1),
				makePlan2StringConstExprWithType("x"),
			}),
		},
	}

	result, err := handleTupleIn(ctx, "not_in", leftList, rightList)
	require.NoError(t, err)
	require.NotNil(t, result)
	// Should have outer "not" wrapping
}

func TestHandleTupleIn_MultipleTuples(t *testing.T) {
	ctx := context.Background()
	// (a) IN ((1),(2),(3))
	leftList := &plan.Expr_List{
		List: &plan.ExprList{
			List: []*plan.Expr{
				makePlan2Int64ConstExprWithType(0),
			},
		},
	}
	rightList := &plan.ExprList{
		List: []*plan.Expr{
			makeListExpr([]*plan.Expr{makePlan2Int64ConstExprWithType(1)}),
			makeListExpr([]*plan.Expr{makePlan2Int64ConstExprWithType(2)}),
			makeListExpr([]*plan.Expr{makePlan2Int64ConstExprWithType(3)}),
		},
	}

	result, err := handleTupleIn(ctx, "in", leftList, rightList)
	require.NoError(t, err)
	require.NotNil(t, result)
}

// ============================================================================
// appendCastBeforeExpr
// ============================================================================

func TestAppendCastBeforeExpr_IntToString(t *testing.T) {
	ctx := context.Background()
	expr := makePlan2Int64ConstExprWithType(42)

	result, err := appendCastBeforeExpr(ctx, expr, plan.Type{
		Id:          int32(types.T_varchar),
		NotNullable: false,
	})
	require.NoError(t, err)
	require.NotNil(t, result)

	// Result should be a cast function
	f := result.GetF()
	require.NotNil(t, f)
	require.Equal(t, "cast", f.Func.GetObjName())
	require.Equal(t, 2, len(f.Args))
}

func TestAppendCastBeforeExpr_StringToString(t *testing.T) {
	ctx := context.Background()
	expr := makePlan2StringConstExprWithType("hello")

	result, err := appendCastBeforeExpr(ctx, expr, plan.Type{
		Id:          int32(types.T_varchar),
		NotNullable: false,
	})
	require.NoError(t, err)
	require.NotNil(t, result)

	f := result.GetF()
	require.NotNil(t, f)
	require.Equal(t, "cast", f.Func.GetObjName())
}

func TestAppendCastBeforeExpr_HexToUint64(t *testing.T) {
	ctx := context.Background()
	// This tests the isBin path for hex literal overflow
	expr := makePlan2StringConstExprWithType("0xFFFFFFFFFFFFFFFF", true)

	result, err := appendCastBeforeExpr(ctx, expr, plan.Type{
		Id:          int32(types.T_float64),
		NotNullable: false,
	}, true, true) // isBin flags
	require.NoError(t, err)
	require.NotNil(t, result)
	// With isBin[0] && isBin[1], type should be changed to uint64
	require.Equal(t, int32(types.T_uint64), result.Typ.Id)
}
