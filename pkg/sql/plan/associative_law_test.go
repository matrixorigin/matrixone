// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestOuterJoinPreservedSideAssociativity(t *testing.T) {
	t.Run("moves unique inner join below left join", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(true, false)

		root := builder.applyOuterJoinPreservedSideRule(4)

		require.Equal(t, int32(3), root)
		require.Equal(t, []int32{4, 1}, builder.qry.Nodes[3].Children)
		require.Equal(t, []int32{0, 2}, builder.qry.Nodes[4].Children)
	})

	t.Run("handles commuted upper inner join", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(true, true)

		root := builder.applyOuterJoinPreservedSideRule(4)

		require.Equal(t, int32(3), root)
		require.Equal(t, []int32{4, 1}, builder.qry.Nodes[3].Children)
		require.Equal(t, []int32{0, 2}, builder.qry.Nodes[4].Children)
	})

	t.Run("keeps non unique inner input above left join", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(false, false)

		root := builder.applyOuterJoinPreservedSideRule(4)

		require.Equal(t, int32(4), root)
		require.Equal(t, []int32{3, 2}, builder.qry.Nodes[4].Children)
		require.Equal(t, []int32{0, 1}, builder.qry.Nodes[3].Children)
	})

	t.Run("keeps condition that references nullable side", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(true, false)
		builder.qry.Nodes[4].OnList = []*planpb.Expr{associativityEqExpr(1, 2)}

		root := builder.applyOuterJoinPreservedSideRule(4)

		require.Equal(t, int32(4), root)
		require.Equal(t, []int32{3, 2}, builder.qry.Nodes[4].Children)
	})

	t.Run("keeps local limit boundary", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(true, false)
		builder.qry.Nodes[3].Limit = &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_uint64)},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: 1}}},
		}

		root := builder.applyOuterJoinPreservedSideRule(4)

		require.Equal(t, int32(4), root)
		require.Equal(t, []int32{3, 2}, builder.qry.Nodes[4].Children)
	})
}

func TestOuterJoinNullableSideAssociativity(t *testing.T) {
	t.Run("moves null rejecting inner join below left join", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(false, false)
		builder.qry.Nodes[4].OnList = []*planpb.Expr{associativityEqExpr(2, 3)}

		root := builder.applyOuterJoinNullableSideRule(4)

		require.Equal(t, int32(3), root)
		require.Equal(t, planpb.Node_INNER, builder.qry.Nodes[3].JoinType)
		require.Equal(t, []int32{0, 4}, builder.qry.Nodes[3].Children)
		require.Equal(t, []int32{1, 2}, builder.qry.Nodes[4].Children)
	})

	t.Run("handles commuted upper inner join", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(false, true)
		builder.qry.Nodes[4].OnList = []*planpb.Expr{associativityEqExpr(2, 3)}

		root := builder.applyOuterJoinNullableSideRule(4)

		require.Equal(t, int32(3), root)
		require.Equal(t, planpb.Node_INNER, builder.qry.Nodes[3].JoinType)
		require.Equal(t, []int32{0, 4}, builder.qry.Nodes[3].Children)
		require.Equal(t, []int32{1, 2}, builder.qry.Nodes[4].Children)
	})

	t.Run("keeps condition that references preserved side", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(false, false)

		root := builder.applyOuterJoinNullableSideRule(4)

		require.Equal(t, int32(4), root)
		require.Equal(t, planpb.Node_LEFT, builder.qry.Nodes[3].JoinType)
		require.Equal(t, []int32{3, 2}, builder.qry.Nodes[4].Children)
	})

	t.Run("keeps mixed nullable and preserved-side conditions", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(false, false)
		builder.qry.Nodes[4].OnList = []*planpb.Expr{
			associativityEqExpr(2, 3),
			associativityEqExpr(1, 3),
		}

		root := builder.applyOuterJoinNullableSideRule(4)

		require.Equal(t, int32(4), root)
		require.Equal(t, planpb.Node_LEFT, builder.qry.Nodes[3].JoinType)
	})

	t.Run("keeps upper join without null rejecting nullable column", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(false, false)
		builder.qry.Nodes[4].OnList = []*planpb.Expr{associativityEqExpr(3, 3)}

		root := builder.applyOuterJoinNullableSideRule(4)

		require.Equal(t, int32(4), root)
		require.Equal(t, planpb.Node_LEFT, builder.qry.Nodes[3].JoinType)
	})

	t.Run("keeps local limit boundary", func(t *testing.T) {
		builder := newOuterJoinAssociativityBuilder(false, false)
		builder.qry.Nodes[4].OnList = []*planpb.Expr{associativityEqExpr(2, 3)}
		builder.qry.Nodes[3].Limit = &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_uint64)},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: 1}}},
		}

		root := builder.applyOuterJoinNullableSideRule(4)

		require.Equal(t, int32(4), root)
		require.Equal(t, planpb.Node_LEFT, builder.qry.Nodes[3].JoinType)
	})
}

func newOuterJoinAssociativityBuilder(uniqueInner, commuteUpper bool) *QueryBuilder {
	intType := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	scan := func(id, tag int32, name string) *planpb.Node {
		return &planpb.Node{
			NodeType:    planpb.Node_TABLE_SCAN,
			NodeId:      id,
			BindingTags: []int32{tag},
			TableDef: &planpb.TableDef{
				Name:          name,
				Cols:          []*planpb.ColDef{{Name: "id", Typ: intType}},
				Name2ColIndex: map[string]int32{"id": 0},
			},
			Stats: associativityStats(100, 1),
		}
	}

	nodes := []*planpb.Node{
		scan(0, 1, "a"),
		scan(1, 2, "b"),
		scan(2, 3, "c"),
		{
			NodeType: planpb.Node_JOIN,
			NodeId:   3,
			JoinType: planpb.Node_LEFT,
			Children: []int32{0, 1},
			OnList:   []*planpb.Expr{associativityEqExpr(1, 2)},
			Stats:    associativityStats(100, 1),
		},
		{
			NodeType: planpb.Node_JOIN,
			NodeId:   4,
			JoinType: planpb.Node_INNER,
			Children: []int32{3, 2},
			OnList:   []*planpb.Expr{associativityEqExpr(1, 3)},
			Stats:    associativityStats(10, 0.1),
		},
	}
	if uniqueInner {
		nodes[2].TableDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}
	}
	if commuteUpper {
		nodes[4].Children[0], nodes[4].Children[1] = nodes[4].Children[1], nodes[4].Children[0]
	}

	return &QueryBuilder{qry: &planpb.Query{Nodes: nodes}}
}

func associativityEqExpr(leftTag, rightTag int32) *planpb.Expr {
	typ := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool), NotNullable: true},
		Ndv: 100,
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: getFunctionObjRef(function.EncodeOverloadID(int32(function.EQUAL), 0), "="),
			Args: []*planpb.Expr{GetColExpr(typ, leftTag, 0), GetColExpr(typ, rightTag, 0)},
		}},
	}
}

func associativityStats(outcnt, selectivity float64) *planpb.Stats {
	return &planpb.Stats{
		Outcnt:      outcnt,
		TableCnt:    100,
		Cost:        outcnt,
		Selectivity: selectivity,
		BlockNum:    1,
		HashmapStats: &planpb.HashMapStats{
			HashmapSize: 1,
		},
	}
}
