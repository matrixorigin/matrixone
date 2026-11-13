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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func groupedOuterDomainTestBuilder(t *testing.T) (*QueryBuilder, int32) {
	t.Helper()
	b := NewQueryBuilder(plan.Query_SELECT, NewEmptyCompilerContext(newPlanTestProcess(t)), false, false)
	b.syntheticNDVCols = make(map[[2]int32]struct{})
	keyType := plan.Type{Id: int32(types.T_int64)}
	valType := plan.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2}
	bind := func(name string, args ...*plan.Expr) *plan.Expr {
		e, err := BindFuncExprImplByPlanExpr(b.GetContext(), name, args)
		require.NoError(t, err)
		return e
	}
	groups := make([]*plan.Node, 0, 3)
	for i := range 3 {
		tag := b.genNewBindTag()
		scan := b.appendNode(&plan.Node{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{tag}, Stats: &plan.Stats{Outcnt: 10000},
			TableDef: &plan.TableDef{Cols: []*plan.ColDef{
				{Name: "k", Typ: keyType}, {Name: "v", Typ: valType}}}}, nil)
		key := GetColExpr(keyType, tag, 0)
		key.Ndv = 100000
		b.syntheticNDVCols[[2]int32{tag, 0}] = struct{}{}
		group := b.appendNode(&plan.Node{NodeType: plan.Node_AGG, Children: []int32{scan},
			BindingTags: []int32{b.genNewBindTag(), b.genNewBindTag()}, GroupingFlag: []bool{true},
			GroupBy: []*plan.Expr{key}, AggList: []*plan.Expr{bind("sum", GetColExpr(valType, tag, 1))}}, nil)
		rows := float64(100)
		if i == 0 {
			rows = 10000
		}
		b.qry.Nodes[scan].Stats = &plan.Stats{Outcnt: rows}
		b.qry.Nodes[group].Stats = &plan.Stats{Outcnt: rows}
		groups = append(groups, b.qry.Nodes[group])
	}
	root := groups[0].NodeId
	predicates := make([]*plan.Expr, 0, len(groups)-1)
	for _, right := range groups[1:] {
		on := bind("=", GetColExpr(keyType, groups[0].BindingTags[0], 0), GetColExpr(keyType, right.BindingTags[0], 0))
		root = b.appendNode(&plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_LEFT,
			Children: []int32{root, right.NodeId}, OnList: []*plan.Expr{on}}, nil)
		predicates = append(predicates, bind("isnotnull", GetColExpr(right.AggList[0].Typ, right.BindingTags[1], 0)))
	}
	root = b.appendNode(&plan.Node{NodeType: plan.Node_FILTER, Children: []int32{root},
		FilterList: []*plan.Expr{bind("or", predicates[0], predicates[1])}}, nil)
	b.qry.Steps = []int32{root}
	return b, root
}

func TestGroupedOuterDomain(t *testing.T) {
	for _, scenario := range []string{"prune", "null-accepting", "mismatched-key", "rollup", "no-benefit", "limited", "shared", "volatile"} {
		t.Run(scenario, func(t *testing.T) {
			b, root := groupedOuterDomainTestBuilder(t)
			probe, right, filter := b.qry.Nodes[1], b.qry.Nodes[3], b.qry.Nodes[root]
			switch scenario {
			case "null-accepting":
				filter.FilterList = []*plan.Expr{makePlan2BoolConstExprWithType(true)}
			case "mismatched-key":
				b.qry.Nodes[7].OnList[0].GetF().Args[0].GetCol().RelPos = right.BindingTags[0]
			case "rollup":
				probe.GroupingFlag[0] = false
			case "no-benefit":
				probe.Stats.Outcnt = 1
			case "limited":
				right.Limit = makePlan2Int64ConstExprWithType(1)
			case "shared":
				b.qry.Steps = append(b.qry.Steps, root)
			case "volatile":
				filter.FilterList = append(filter.FilterList, &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)},
					Expr: &plan.Expr_V{V: &plan.VarRef{Name: "user_side_effect"}}})
			}
			before := len(b.qry.Nodes)
			b.pruneGroupedOuterDomains(root)
			if scenario != "prune" {
				require.Len(t, b.qry.Nodes, before, "rejected rule must not publish auxiliary scans")
				return
			}
			semi := b.qry.Nodes[probe.Children[0]]
			require.Equal(t, plan.Node_SEMI, semi.JoinType)
			require.EqualValues(t, 0, semi.Children[0])
			domain := b.qry.Nodes[semi.Children[1]]
			require.Equal(t, plan.Node_UNION_ALL, domain.NodeType)
			require.Len(t, filter.FilterList, 1, "the final predicate is authoritative")
			seen := make(map[int32]bool)
			for _, n := range b.qry.Nodes {
				for _, tag := range n.BindingTags {
					require.False(t, seen[tag], "clones must not share binding tags")
					seen[tag] = true
				}
			}
			for _, arm := range domain.Children {
				project := b.qry.Nodes[arm]
				scan := b.qry.Nodes[project.Children[0]]
				require.Equal(t, scan.BindingTags[0], project.ProjectList[0].GetCol().RelPos)
				require.Same(t, scan.TableDef, b.tag2Table[scan.BindingTags[0]])
			}
		})
	}
}

func TestGroupedOuterIntegerSumBound(t *testing.T) {
	for _, scenario := range []string{"bounded", "missing-key", "wide-suffix"} {
		t.Run(scenario, func(t *testing.T) {
			b, root := groupedOuterDomainTestBuilder(t)
			probe, scan := b.qry.Nodes[1], b.qry.Nodes[0]
			intType := plan.Type{Id: int32(types.T_int32)}
			scan.TableDef.Cols[1].Typ = intType
			scan.TableDef.Cols = append(scan.TableDef.Cols, &plan.ColDef{Name: "ticket", Typ: intType})
			scan.TableDef.Name2ColIndex = map[string]int32{"k": 0, "v": 1, "ticket": 2}
			scan.TableDef.Pkey = &plan.PrimaryKeyDef{PkeyColName: catalog.CPrimaryKeyColName, Names: []string{"k", "ticket"}}
			var err error
			probe.AggList[0], err = BindFuncExprImplByPlanExpr(b.GetContext(), "sum", []*plan.Expr{
				GetColExpr(intType, scan.BindingTags[0], 1)})
			require.NoError(t, err)
			if scenario == "missing-key" {
				scan.TableDef.Pkey = nil
			} else if scenario == "wide-suffix" {
				scan.TableDef.Cols[2].Typ.Id = int32(types.T_int64)
			}
			before := len(b.qry.Nodes)
			b.pruneGroupedOuterDomains(root)
			if scenario == "bounded" {
				require.Greater(t, len(b.qry.Nodes), before, "2^32 INT32 values fit INT64 in every partial-sum order")
			} else {
				require.Len(t, b.qry.Nodes, before, "sampled small row counts cannot prove SUM totality")
			}
		})
	}
}
