// Copyright 2023 Matrix Origin
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

func TestRemapWindowClause(t *testing.T) {
	b := &QueryBuilder{
		compCtx: &MockCompilerContext{
			ctx: context.Background(),
		},
		nameByColRef:        make(map[[2]int32]string),
		optimizationHistory: []string{"test optimization history"},
	}

	t.Run("current window output maps to appended column", func(t *testing.T) {
		expr := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 7,
					ColPos: 1,
				},
			},
		}
		b.nameByColRef[[2]int32{7, 1}] = "rank_in_product"

		err := b.remapWindowClause(expr, 7, 1, 3, map[[2]int32][2]int32{}, nil)
		require.NoError(t, err)
		require.Equal(t, int32(-1), expr.GetCol().RelPos)
		require.Equal(t, int32(3), expr.GetCol().ColPos)
		require.Equal(t, "rank_in_product", expr.GetCol().Name)
	})

	t.Run("previous window output remaps through child projection", func(t *testing.T) {
		expr := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_decimal128)},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 7,
					ColPos: 0,
				},
			},
		}
		b.nameByColRef[[2]int32{7, 0}] = "product_total"

		colMap := map[[2]int32][2]int32{
			{7, 0}: {0, 2},
		}
		err := b.remapWindowClause(expr, 7, 1, 3, colMap, nil)
		require.NoError(t, err)
		require.Equal(t, int32(0), expr.GetCol().RelPos)
		require.Equal(t, int32(2), expr.GetCol().ColPos)
		require.Equal(t, "product_total", expr.GetCol().Name)
	})

	t.Run("function expression remaps current and previous window outputs", func(t *testing.T) {
		prevExpr := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_decimal128)},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 7,
					ColPos: 0,
				},
			},
		}
		currExpr := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 7,
					ColPos: 1,
				},
			},
		}
		b.nameByColRef[[2]int32{7, 0}] = "product_total"
		b.nameByColRef[[2]int32{7, 1}] = "rank_in_product"

		filterExpr := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: getFunctionObjRef(0, "and"),
					Args: []*plan.Expr{
						{
							Typ: plan.Type{Id: int32(types.T_bool)},
							Expr: &plan.Expr_F{
								F: &plan.Function{
									Func: getFunctionObjRef(0, ">"),
									Args: []*plan.Expr{
										prevExpr,
										{
											Typ: plan.Type{Id: int32(types.T_decimal128)},
											Expr: &plan.Expr_Lit{
												Lit: &plan.Literal{
													Value: &plan.Literal_Decimal128Val{
														Decimal128Val: &plan.Decimal128{
															A: 500,
															B: 0,
														},
													},
												},
											},
										},
									},
								},
							},
						},
						{
							Typ: plan.Type{Id: int32(types.T_bool)},
							Expr: &plan.Expr_F{
								F: &plan.Function{
									Func: getFunctionObjRef(0, "="),
									Args: []*plan.Expr{
										currExpr,
										{
											Typ: plan.Type{Id: int32(types.T_int64)},
											Expr: &plan.Expr_Lit{
												Lit: &plan.Literal{
													Value: &plan.Literal_I64Val{I64Val: 1},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		colMap := map[[2]int32][2]int32{
			{7, 0}: {0, 2},
		}
		err := b.remapWindowClause(filterExpr, 7, 1, 3, colMap, nil)
		require.NoError(t, err)
		require.Equal(t, int32(0), prevExpr.GetCol().RelPos)
		require.Equal(t, int32(2), prevExpr.GetCol().ColPos)
		require.Equal(t, "product_total", prevExpr.GetCol().Name)
		require.Equal(t, int32(-1), currExpr.GetCol().RelPos)
		require.Equal(t, int32(3), currExpr.GetCol().ColPos)
		require.Equal(t, "rank_in_product", currExpr.GetCol().Name)
	})

	t.Run("missing remap still returns error", func(t *testing.T) {
		expr := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_timestamp)},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 3,
					ColPos: 3,
					Name:   "test",
				},
			},
		}

		f := &Expr{
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: getFunctionObjRef(1, "n"),
					Args: []*Expr{expr},
				},
			},
			Typ: plan.Type{},
		}

		err := b.remapWindowClause(f, 1, 0, 1, map[[2]int32][2]int32{}, nil)
		t.Log(err)
		require.Error(t, err)
	})
}

func TestBuildWindowFilterOnNonProjectedColumns(t *testing.T) {
	mock := NewMockOptimizer(false)

	sqls := []string{
		`WITH ranked AS (
			SELECT
				a,
				b,
				SUM(a) OVER (PARTITION BY a) AS product_total,
				ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) AS rank_in_product
			FROM cte_test.t1
		)
		SELECT a FROM ranked WHERE rank_in_product = 1 ORDER BY a;`,
		`WITH ranked AS (
			SELECT
				a,
				b,
				SUM(a) OVER (PARTITION BY a) AS product_total,
				ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) AS rank_in_product
			FROM cte_test.t1
		)
		SELECT a FROM ranked WHERE product_total > 1 ORDER BY a;`,
	}

	for _, sql := range sqls {
		_, err := buildSingleStmt(mock, t, sql)
		require.NoError(t, err, sql)
	}
}
