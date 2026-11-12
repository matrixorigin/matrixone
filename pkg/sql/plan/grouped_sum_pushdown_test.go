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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func groupedSumTestBuilder(t *testing.T) (*QueryBuilder, int32) {
	t.Helper()
	ctx := NewEmptyCompilerContext(newPlanTestProcess(t))
	b := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	keyType := plan.Type{Id: int32(types.T_int32), NotNullable: true}
	valueType := plan.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2}
	attrType := plan.Type{Id: int32(types.T_varchar), Width: 64}
	factTag, dimTag := b.genNewBindTag(), b.genNewBindTag()
	fact := b.appendNode(&plan.Node{NodeType: plan.Node_TABLE_SCAN,
		BindingTags: []int32{factTag}, Stats: &plan.Stats{Outcnt: 10000},
		TableDef: &plan.TableDef{Cols: []*plan.ColDef{
			{Name: "key", Typ: keyType}, {Name: "value", Typ: valueType},
		}}}, nil)
	dim := b.appendNode(&plan.Node{NodeType: plan.Node_TABLE_SCAN,
		BindingTags: []int32{dimTag}, Stats: &plan.Stats{Outcnt: 10},
		TableDef: &plan.TableDef{Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
			Name2ColIndex: map[string]int32{"id": 0, "label": 1},
			Cols:          []*plan.ColDef{{Name: "id", Typ: keyType}, {Name: "label", Typ: attrType}}}}, nil)
	on, err := BindFuncExprImplByPlanExpr(b.GetContext(), "=", []*plan.Expr{
		GetColExpr(keyType, factTag, 0), GetColExpr(keyType, dimTag, 0)})
	require.NoError(t, err)
	join := b.appendNode(&plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_INNER,
		Children: []int32{fact, dim}, OnList: []*plan.Expr{on}}, nil)
	sum, err := BindFuncExprImplByPlanExpr(b.GetContext(), "sum",
		[]*plan.Expr{GetColExpr(valueType, factTag, 1)})
	require.NoError(t, err)
	groupTag, sumTag := b.genNewBindTag(), b.genNewBindTag()
	agg := b.appendNode(&plan.Node{NodeType: plan.Node_AGG, Children: []int32{join},
		BindingTags: []int32{groupTag, sumTag}, GroupingFlag: []bool{true},
		GroupBy: []*plan.Expr{GetColExpr(attrType, dimTag, 1)}, AggList: []*plan.Expr{sum}}, nil)
	root := b.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{agg},
		BindingTags: []int32{b.genNewBindTag()}, ProjectList: []*plan.Expr{
			GetColExpr(attrType, groupTag, 0), GetColExpr(sum.Typ, sumTag, 0)}}, nil)
	return b, root
}

func TestGroupedSumPushdown(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		b, root := groupedSumTestBuilder(t)
		agg, join := b.qry.Nodes[3], b.qry.Nodes[2]
		oldType := agg.AggList[0].Typ
		if reverse {
			join.Children[0], join.Children[1] = join.Children[1], join.Children[0]
		}
		b.pushDownGroupedSums(root)
		restore := b.qry.Nodes[b.qry.Nodes[root].Children[0]]
		require.Equal(t, plan.Node_PROJECT, restore.NodeType)
		require.Equal(t, oldType, restore.ProjectList[1].Typ, "published SUM precision/scale must not widen")
		require.Equal(t, restore.BindingTags[0], b.qry.Nodes[root].ProjectList[1].GetCol().RelPos)
		require.Equal(t, plan.Node_AGG, agg.NodeType, "final grouping survives colliding dimension attributes")
		require.Len(t, agg.GroupBy, 1)
		partial := b.qry.Nodes[join.Children[0]]
		if reverse {
			partial = b.qry.Nodes[join.Children[1]]
		}
		require.Equal(t, plan.Node_AGG, partial.NodeType)
		require.Len(t, partial.GroupBy, 1, "group facts by the complete join key")
		require.Equal(t, oldType, partial.AggList[0].Typ)
		require.Equal(t, partial.BindingTags[1], agg.AggList[0].GetF().Args[0].GetCol().RelPos)
	}
}

func TestGroupedSumPushdownRejections(t *testing.T) {
	for _, scenario := range []string{"outer", "residual", "nonunique", "distinct", "fallible", "fact-filter", "nested-fact-filter", "dimension-block-filter", "floating", "rollup", "no-benefit", "disabled", "shared", "shared-step"} {
		t.Run(scenario, func(t *testing.T) {
			b, root := groupedSumTestBuilder(t)
			agg, join := b.qry.Nodes[3], b.qry.Nodes[2]
			switch scenario {
			case "outer":
				join.JoinType = plan.Node_LEFT
			case "residual":
				join.OnList = append(join.OnList, DeepCopyExpr(join.OnList[0]))
			case "nonunique":
				b.qry.Nodes[1].TableDef.Pkey = nil
			case "distinct":
				fn := agg.AggList[0].GetF()
				fn.Func.Obj = int64(uint64(fn.Func.Obj) | function.Distinct)
			case "fallible":
				arg := agg.AggList[0].GetF().Args[0]
				zero := &plan.Expr{Typ: arg.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{
					Value: &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{}}}}}
				division, err := BindFuncExprImplByPlanExpr(b.GetContext(), "/", []*plan.Expr{arg, zero})
				require.NoError(t, err)
				agg.AggList[0], err = BindFuncExprImplByPlanExpr(b.GetContext(), "sum", []*plan.Expr{division})
				require.NoError(t, err)
			case "fact-filter", "nested-fact-filter", "dimension-block-filter":
				scan := b.qry.Nodes[0]
				if scenario == "dimension-block-filter" {
					scan = b.qry.Nodes[1]
				}
				text := plan.Type{Id: int32(types.T_varchar), Width: 20}
				scan.TableDef.Cols = append(scan.TableDef.Cols, &plan.ColDef{Name: "s", Typ: text})
				cast, err := appendCastBeforeExpr(b.GetContext(), GetColExpr(text, scan.BindingTags[0], 2), plan.Type{Id: int32(types.T_int64)})
				require.NoError(t, err)
				predicate, err := BindFuncExprImplByPlanExpr(b.GetContext(), ">", []*plan.Expr{cast, makePlan2Int64ConstExprWithType(0)})
				require.NoError(t, err)
				if scenario == "dimension-block-filter" {
					scan.BlockFilterList = []*plan.Expr{predicate}
				} else {
					scan.FilterList = []*plan.Expr{predicate}
					if scenario == "nested-fact-filter" {
						join.Children[0] = b.appendNode(&plan.Node{NodeType: plan.Node_FILTER,
							Children: []int32{scan.NodeId}, Stats: DeepCopyStats(scan.Stats)}, nil)
					}
				}
				// This error is independent of the total DECIMAL SUM argument:
				// changing the build input must not make it newly observable.
			case "floating":
				var err error
				agg.AggList[0], err = BindFuncExprImplByPlanExpr(b.GetContext(), "sum", []*plan.Expr{
					GetColExpr(plan.Type{Id: int32(types.T_float64)}, b.qry.Nodes[0].BindingTags[0], 1)})
				require.NoError(t, err)
			case "rollup":
				agg.GroupingFlag[0] = false
			case "no-benefit":
				b.qry.Nodes[0].Stats.Outcnt = b.qry.Nodes[1].Stats.Outcnt
			case "disabled":
				b.optimizerHints = &OptimizerHints{aggPushDown: 1}
			case "shared":
				root = b.appendNode(&plan.Node{NodeType: plan.Node_UNION_ALL, Children: []int32{root, root}}, nil)
			case "shared-step":
				root = agg.NodeId
				b.qry.Steps = []int32{root, root}
			}
			count := len(b.qry.Nodes)
			require.Equal(t, root, b.pushDownGroupedSums(root))
			require.Len(t, b.qry.Nodes, count, "rejection must not publish partial nodes")
		})
	}
}
