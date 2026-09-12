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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestDetermineHashOnPKRequiresNonNullableJoinKeys(t *testing.T) {
	tests := []struct {
		name             string
		leftNotNullable  bool
		rightNotNullable bool
		wantHashOnPK     bool
	}{
		{
			name:             "both join keys are not nullable",
			leftNotNullable:  true,
			rightNotNullable: true,
			wantHashOnPK:     true,
		},
		{
			name:             "left join key is nullable",
			leftNotNullable:  false,
			rightNotNullable: true,
			wantHashOnPK:     true,
		},
		{
			name:             "right primary key join key is nullable",
			leftNotNullable:  true,
			rightNotNullable: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := buildHashOnPKTestBuilder(tt.leftNotNullable, tt.rightNotNullable)

			determineHashOnPK(2, builder)

			require.Equal(t, tt.wantHashOnPK, builder.qry.Nodes[2].Stats.HashmapStats.HashOnPK)
		})
	}
}

func TestDetermineHashOnPKAllowsUntaggedInternalScan(t *testing.T) {
	builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{{
		NodeType: plan.Node_TABLE_SCAN,
		TableDef: &plan.TableDef{
			Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
			Name2ColIndex: map[string]int32{"id": 0},
		},
	}}}}

	require.NotPanics(t, func() {
		require.Nil(t, determineHashOnPK(0, builder))
		require.Nil(t, findHashOnPKTable(0, 1, builder))
	})
}

func TestDetermineHashOnPKRequiresGroupingCompatiblePrimaryKey(t *testing.T) {
	tests := []struct {
		name         string
		typ          plan.Type
		wantHashOnPK bool
	}{
		{
			name:         "varchar control",
			typ:          plan.Type{Id: int32(types.T_varchar), Width: 8, NotNullable: true},
			wantHashOnPK: true,
		},
		{
			name: "float64 signed zero",
			typ:  plan.Type{Id: int32(types.T_float64), NotNullable: true},
		},
		{
			name: "char trailing spaces",
			typ:  plan.Type{Id: int32(types.T_char), Width: 8, NotNullable: true},
		},
		{
			name: "collated varchar",
			typ: plan.Type{
				Id: int32(types.T_varchar), Width: 8, Charset: uint32(types.CharsetUTF8), NotNullable: true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := buildHashOnPKTestBuilder(true, true)
			builder.qry.Nodes[0].TableDef.Cols[0].Typ = test.typ
			builder.qry.Nodes[1].TableDef.Cols[0].Typ = test.typ
			joinFn := builder.qry.Nodes[2].OnList[0].GetF()
			require.NotNil(t, joinFn)
			joinFn.Args[0].Typ = test.typ
			joinFn.Args[1].Typ = test.typ

			determineHashOnPK(2, builder)

			require.Equal(t, test.wantHashOnPK,
				builder.qry.Nodes[2].Stats.HashmapStats.HashOnPK)
		})
	}
}

func TestDetermineHashOnPKRequiresDirectColumnEquality(t *testing.T) {
	tests := []struct {
		name         string
		wrapRightKey bool
		wantHashOnPK bool
	}{
		{
			name:         "zero tag direct primary key control",
			wantHashOnPK: true,
		},
		{
			name:         "zero tag wrapped primary key is not a proof",
			wrapRightKey: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := buildHashOnPKTestBuilder(true, true)
			builder.qry.Nodes[1].BindingTags[0] = 0
			joinFn := builder.qry.Nodes[2].OnList[0].GetF()
			require.NotNil(t, joinFn)
			rightKey := joinFn.Args[1]
			rightKey.GetCol().RelPos = 0
			if test.wrapRightKey {
				joinFn.Args[1] = &plan.Expr{
					Typ: rightKey.Typ,
					Expr: &plan.Expr_F{F: &plan.Function{
						Func: getFunctionObjRef(0, "cast"),
						Args: []*plan.Expr{rightKey},
					}},
				}
			}

			determineHashOnPK(2, builder)

			require.Equal(t, test.wantHashOnPK,
				builder.qry.Nodes[2].Stats.HashmapStats.HashOnPK)
		})
	}
}

func TestDetermineHashOnPKRejectsCrossTypeDirectEquality(t *testing.T) {
	tests := []struct {
		name      string
		leftType  plan.Type
		rightType plan.Type
	}{
		{
			name:      "datetime probe to timestamp primary key",
			leftType:  plan.Type{Id: int32(types.T_datetime), NotNullable: true},
			rightType: plan.Type{Id: int32(types.T_timestamp), NotNullable: true},
		},
		{
			name:      "timestamp probe to datetime primary key",
			leftType:  plan.Type{Id: int32(types.T_timestamp), NotNullable: true},
			rightType: plan.Type{Id: int32(types.T_datetime), NotNullable: true},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := buildHashOnPKTestBuilder(true, true)
			builder.qry.Nodes[0].TableDef.Cols[0].Typ = test.leftType
			builder.qry.Nodes[1].TableDef.Cols[0].Typ = test.rightType
			joinFn := builder.qry.Nodes[2].OnList[0].GetF()
			joinFn.Args[0].Typ = test.leftType
			joinFn.Args[1].Typ = test.rightType

			determineHashOnPK(2, builder)

			require.False(t, builder.qry.Nodes[2].Stats.HashmapStats.HashOnPK,
				"session-time-zone conversion is not a storage-key uniqueness proof")
		})
	}
}

func TestDetermineHashOnPKRequiresTypeMetadataToMatchReferencedColumns(t *testing.T) {
	tests := []struct {
		name         string
		exprType     plan.Type
		wantHashOnPK bool
	}{
		{
			name:         "matching expression and schema types",
			exprType:     plan.Type{Id: int32(types.T_int64), NotNullable: true},
			wantHashOnPK: true,
		},
		{
			name:     "join expressions agree but disagree with schema",
			exprType: plan.Type{Id: int32(types.T_uint64), NotNullable: true},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := buildHashOnPKTestBuilder(true, true)
			joinFn := builder.qry.Nodes[2].OnList[0].GetF()
			require.NotNil(t, joinFn)
			joinFn.Args[0].Typ = test.exprType
			joinFn.Args[1].Typ = test.exprType

			determineHashOnPK(2, builder)

			require.Equal(t, test.wantHashOnPK,
				builder.qry.Nodes[2].Stats.HashmapStats.HashOnPK)
		})
	}
}

func buildHashOnPKTestBuilder(leftNotNullable bool, rightNotNullable bool) *QueryBuilder {
	leftType := plan.Type{Id: int32(types.T_int64), NotNullable: leftNotNullable}
	rightType := plan.Type{Id: int32(types.T_int64), NotNullable: rightNotNullable}

	leftExpr := GetColExpr(leftType, 1, 0)
	rightExpr := GetColExpr(rightType, 2, 0)
	eqExpr := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool), NotNullable: true},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(function.EncodeOverloadID(int32(function.EQUAL), 0), "="),
				Args: []*plan.Expr{leftExpr, rightExpr},
			},
		},
	}

	return &QueryBuilder{
		qry: &plan.Query{
			Nodes: []*plan.Node{
				{
					NodeType:    plan.Node_TABLE_SCAN,
					NodeId:      0,
					BindingTags: []int32{1},
					TableDef: &plan.TableDef{
						Name:          "left_t",
						Cols:          []*plan.ColDef{{Name: "l_col", Typ: leftType}},
						Name2ColIndex: map[string]int32{"l_col": 0},
					},
				},
				{
					NodeType:    plan.Node_TABLE_SCAN,
					NodeId:      1,
					BindingTags: []int32{2},
					TableDef: &plan.TableDef{
						Name:          "right_t",
						Cols:          []*plan.ColDef{{Name: "r_pk", Typ: rightType}},
						Name2ColIndex: map[string]int32{"r_pk": 0},
						Pkey:          &plan.PrimaryKeyDef{PkeyColName: "r_pk", Names: []string{"r_pk"}},
					},
				},
				{
					NodeType: plan.Node_JOIN,
					NodeId:   2,
					Stats: &plan.Stats{
						HashmapStats: &plan.HashMapStats{},
					},
					Children: []int32{0, 1},
					JoinType: plan.Node_INNER,
					OnList:   []*plan.Expr{eqExpr},
				},
			},
		},
	}
}

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

func TestSharedComputationOptimizerHint(t *testing.T) {
	builder := &QueryBuilder{}
	require.False(t, builder.sharedComputationDisabled())
	handleOptimizerHints("sharedComputation=1", builder)
	require.True(t, builder.sharedComputationDisabled())
	handleOptimizerHints("sharedComputation=0", builder)
	require.False(t, builder.sharedComputationDisabled())
}

func TestOuterAntiPlanningOptimizerHint(t *testing.T) {
	builder := &QueryBuilder{}
	handleOptimizerHints("outerAntiPlanning=1", builder)
	require.True(t, builder.outerAntiPlanningDisabled())

	handleOptimizerHints("outerAntiPlanning=0", builder)
	require.False(t, builder.outerAntiPlanningDisabled())
}

func TestRemapHavingClause(t *testing.T) {
	b := &QueryBuilder{
		compCtx: &MockCompilerContext{ctx: context.Background()},
		nameByColRef: map[[2]int32]string{
			{1, 0}: "group_key",
			{2, 2}: "kept_aggregate",
		},
	}

	t.Run("remaps compacted aggregate inside list", func(t *testing.T) {
		expr := &plan.Expr{Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
			GetColExpr(plan.Type{Id: int32(types.T_int64)}, 1, 0),
			GetColExpr(plan.Type{Id: int32(types.T_int64)}, 2, 2),
		}}}}

		err := b.remapHavingClause(expr, 1, 2, 1, 3, []int32{-1, -1, 0})
		require.NoError(t, err)
		require.Equal(t, int32(-1), expr.GetList().List[0].GetCol().RelPos)
		require.Equal(t, int32(0), expr.GetList().List[0].GetCol().ColPos)
		require.Equal(t, int32(-2), expr.GetList().List[1].GetCol().RelPos)
		require.Equal(t, int32(1), expr.GetList().List[1].GetCol().ColPos)
	})

	for _, tc := range []struct {
		name string
		expr *plan.Expr
	}{
		{
			name: "rejects out of range aggregate slot",
			expr: GetColExpr(plan.Type{Id: int32(types.T_int64)}, 2, 3),
		},
		{
			name: "rejects pruned aggregate slot",
			expr: GetColExpr(plan.Type{Id: int32(types.T_int64)}, 2, 0),
		},
		{
			name: "rejects invalid group slot",
			expr: GetColExpr(plan.Type{Id: int32(types.T_int64)}, 1, 1),
		},
		{
			name: "rejects unknown relation tag",
			expr: GetColExpr(plan.Type{Id: int32(types.T_int64)}, 3, 0),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := b.remapHavingClause(tc.expr, 1, 2, 1, 3, []int32{-1, -1, 0})
			require.Error(t, err)
		})
	}

	t.Run("rejects inconsistent aggregate position map", func(t *testing.T) {
		expr := GetColExpr(plan.Type{Id: int32(types.T_int64)}, 2, 2)
		err := b.remapHavingClause(expr, 1, 2, 1, 3, []int32{0})
		require.Error(t, err)
	})
}

func TestReplaceColumnsForExprTraversesEveryNestedExpressionContainer(t *testing.T) {
	const (
		oldTag = int32(7)
		newTag = int32(9)
	)
	oldCol := func() *plan.Expr {
		return GetColExpr(plan.Type{Id: int32(types.T_int64)}, oldTag, 0)
	}
	tests := []struct {
		name             string
		expr             *plan.Expr
		wantReplacements int
	}{
		{
			name: "literal source",
			expr: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_I64Val{I64Val: 1},
				Src:   oldCol(),
			}}},
			wantReplacements: 1,
		},
		{
			name: "expression list",
			expr: &plan.Expr{Expr: &plan.Expr_List{List: &plan.ExprList{
				List: []*plan.Expr{oldCol(), oldCol()},
			}}},
			wantReplacements: 2,
		},
		{
			name: "subquery child",
			expr: &plan.Expr{Expr: &plan.Expr_Sub{Sub: &plan.SubqueryRef{
				Child: oldCol(),
			}}},
			wantReplacements: 1,
		},
		{
			name: "window frame",
			expr: &plan.Expr{Expr: &plan.Expr_W{W: &plan.WindowSpec{
				Frame: &plan.FrameClause{
					Start: &plan.FrameBound{Val: oldCol()},
					End:   &plan.FrameBound{Val: oldCol()},
				},
			}}},
			wantReplacements: 2,
		},
	}

	projMap := map[[2]int32]*plan.Expr{
		{oldTag, 0}: GetColExpr(plan.Type{Id: int32(types.T_int64)}, newTag, 3),
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := replaceColumnsForExpr(test.expr, projMap)
			replacements := 0
			require.NoError(t, plan.VisitExprTree(got, func(expr *plan.Expr) error {
				if col := expr.GetCol(); col != nil {
					require.NotEqual(t, oldTag, col.RelPos)
					if col.RelPos == newTag && col.ColPos == 3 {
						replacements++
					}
				}
				return nil
			}))
			require.Equal(t, test.wantReplacements, replacements)
		})
	}
}

func TestAggregateDependsOnInputOrder(t *testing.T) {
	makeAgg := func(name string) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: name}}}}
	}

	for _, name := range []string{"group_concat", "json_arrayagg", "json_objectagg"} {
		t.Run(name, func(t *testing.T) {
			require.True(t, aggregateDependsOnInputOrder(makeAgg(name)))
		})
	}
	require.False(t, aggregateDependsOnInputOrder(makeAgg("sum")))
	require.False(t, aggregateDependsOnInputOrder(GetColExpr(plan.Type{Id: int32(types.T_int64)}, 1, 0)))
	require.False(t, aggregateDependsOnInputOrder(&plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{}}}))
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
