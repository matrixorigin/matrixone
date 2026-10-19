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
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestRollupDerivedFilterPushdownRespectsGroupingFlag(t *testing.T) {
	for _, test := range []struct {
		name       string
		predicate  string
		filterName string
	}{
		{name: "is null", predicate: "grp is null", filterName: "isnull"},
		{name: "is not null", predicate: "grp is not null", filterName: "isnotnull"},
	} {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			useLegacyGroupingSetPlan(t, mock)
			logicPlan, err := runOneStmt(mock, t, `
				select grp, total
				from (
					select n_comment as grp, sum(n_regionkey) as total
					from nation
					group by n_comment with rollup
				) as rolled
				where `+test.predicate)
			require.NoError(t, err)

			query := logicPlan.GetQuery()
			require.NotNil(t, query)

			branches := rollupAggBranches(query, 1)
			require.Len(t, branches, 2)
			for _, agg := range branches {
				require.Len(t, agg.GroupingFlag, 1)
				scan := onlyTableScanBelow(t, query, agg)
				if agg.GroupingFlag[0] {
					require.True(t, containsFilterFunction(scan.FilterList, test.filterName),
						"predicate on a real group key must reach the table scan; aggregate filters: %v", agg.FilterList)
					require.False(t, containsFilterFunction(agg.FilterList, test.filterName))
				} else {
					require.False(t, containsFilterFunction(scan.FilterList, test.filterName),
						"predicate on a synthetic group key has no base-row equivalent")
					require.True(t, containsFilterFunction(agg.FilterList, test.filterName),
						"predicate on a synthetic group key must remain above the aggregate")
				}
			}
		})
	}
}

func TestRollupComputedOutputFilterPushdown(t *testing.T) {
	for _, tc := range []struct {
		name, projection, keys string
		keyCount               int
	}{
		{"column-control", "n_regionkey", "n_regionkey", 1},
		{"arithmetic", "n_regionkey+1", "n_regionkey", 1},
		{"scalar", "abs(n_regionkey)", "n_regionkey", 1},
		{"cast", "cast(n_regionkey as varchar(20))", "n_regionkey", 1},
		{"mixed-active-keys", "n_regionkey+n_nationkey", "n_regionkey,n_nationkey", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			setGroupingSetTestProtocol(t, mock.CurrentContext(), defines.MORPCVersion49)
			// Exercise the supported static path at the current protocol. No
			// superseded shared-plan branch nodes participate in these assertions.
			setPlanTestGlobalVariable(t, mock.CurrentContext().GetProcess().GetService(), "optimizer_hints", "sharedComputation=1")
			p, err := runOneStmt(mock, t, "select grp,total from (select "+tc.projection+
				" grp,sum(n_nationkey) total from nation group by "+tc.keys+" with rollup) r where grp=2")
			require.NoError(t, err)
			query := p.GetQuery()
			var branches []*planpb.Node
			seen := make(map[int32]bool)
			var visit func(int32)
			visit = func(id int32) {
				if seen[id] {
					return
				}
				seen[id] = true
				node := query.Nodes[id]
				if node.NodeType == planpb.Node_AGG && len(node.GroupingFlag) == tc.keyCount {
					branches = append(branches, node)
				}
				for _, child := range node.Children {
					visit(child)
				}
			}
			for _, step := range query.Steps {
				visit(step)
			}
			require.Len(t, branches, tc.keyCount+1)
			for _, agg := range branches {
				allActive := true
				for _, active := range agg.GroupingFlag {
					allActive = allActive && active
				}
				scan := onlyTableScanBelow(t, query, agg)
				require.Equal(t, allActive, containsFilterFunction(scan.FilterList, "="), "scan flags %v", agg.GroupingFlag)
				require.Equal(t, !allActive, containsFilterFunction(agg.FilterList, "="), "aggregate flags %v", agg.GroupingFlag)
			}
		})
	}
}

func TestRollupMultiColumnFilterPushdownRespectsEachBranch(t *testing.T) {
	for _, test := range []struct {
		name            string
		predicate       string
		filterName      string
		canPushForFlags func([]bool) bool
	}{
		{
			name:            "first key",
			predicate:       "grp1 is null",
			filterName:      "isnull",
			canPushForFlags: func(flags []bool) bool { return flags[0] },
		},
		{
			name:            "second key",
			predicate:       "grp2 = 1",
			filterName:      "=",
			canPushForFlags: func(flags []bool) bool { return flags[1] },
		},
		{
			name:            "active and synthetic keys in one predicate",
			predicate:       "grp1 is null or grp2 = 1",
			filterName:      "or",
			canPushForFlags: func(flags []bool) bool { return flags[0] && flags[1] },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			useLegacyGroupingSetPlan(t, mock)
			logicPlan, err := runOneStmt(mock, t, `
				select grp1, grp2, total
				from (
					select n_comment as grp1, n_regionkey as grp2, sum(n_nationkey) as total
					from nation
					group by n_comment, n_regionkey with rollup
				) as rolled
				where `+test.predicate)
			require.NoError(t, err)

			query := logicPlan.GetQuery()
			require.NotNil(t, query)

			branches := rollupAggBranches(query, 2)
			require.Len(t, branches, 3)
			for _, agg := range branches {
				require.Len(t, agg.GroupingFlag, 2)
				scan := onlyTableScanBelow(t, query, agg)
				if test.canPushForFlags(agg.GroupingFlag) {
					require.True(t, containsFilterFunction(scan.FilterList, test.filterName),
						"predicate must reach a branch where all referenced group keys are real")
					require.False(t, containsFilterFunction(agg.FilterList, test.filterName))
				} else {
					require.False(t, containsFilterFunction(scan.FilterList, test.filterName),
						"predicate must not reach a branch with a referenced synthetic group key")
					require.True(t, containsFilterFunction(agg.FilterList, test.filterName))
				}
			}
		})
	}
}

func TestAggFilterPushdownConservativelyKeepsUnsafeGroupRefs(t *testing.T) {
	for _, test := range []struct {
		name    string
		makeRef func(planpb.Type, int32) *planpb.Expr
	}{
		{
			name: "out of range group position",
			makeRef: func(typ planpb.Type, groupTag int32) *planpb.Expr {
				return GetColExpr(typ, groupTag, 1)
			},
		},
		{
			name: "unreplaceable expression variant",
			makeRef: func(typ planpb.Type, groupTag int32) *planpb.Expr {
				return &planpb.Expr{
					Typ: typ,
					Expr: &planpb.Expr_Corr{Corr: &planpb.CorrColRef{
						RelPos: groupTag,
						ColPos: 0,
						Depth:  1,
					}},
				}
			},
		},
		{
			name: "group ref in unreplaceable list",
			makeRef: func(typ planpb.Type, groupTag int32) *planpb.Expr {
				return &planpb.Expr{
					Typ: typ,
					Expr: &planpb.Expr_List{List: &planpb.ExprList{
						List: []*planpb.Expr{GetColExpr(typ, groupTag, 0)},
					}},
				}
			},
		},
		{
			name: "group ref in unreplaceable window",
			makeRef: func(typ planpb.Type, groupTag int32) *planpb.Expr {
				return &planpb.Expr{
					Typ: typ,
					Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
						WindowFunc: GetColExpr(typ, groupTag, 0),
					}},
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, false)
			scanTag := builder.GenNewBindTag()
			groupTag := builder.GenNewBindTag()
			aggregateTag := builder.GenNewBindTag()
			intType := planpb.Type{Id: int32(types.T_int64)}

			filter := &planpb.Expr{
				Typ: planpb.Type{Id: int32(types.T_bool)},
				Expr: &planpb.Expr_F{F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "isnull"},
					Args: []*planpb.Expr{test.makeRef(intType, groupTag)},
				}},
			}
			builder.qry.Nodes = []*planpb.Node{
				{
					NodeType:    planpb.Node_TABLE_SCAN,
					BindingTags: []int32{scanTag},
				},
				{
					NodeType:     planpb.Node_AGG,
					Children:     []int32{0},
					GroupBy:      []*planpb.Expr{GetColExpr(intType, scanTag, 0)},
					GroupingFlag: []bool{true},
					BindingTags:  []int32{groupTag, aggregateTag},
				},
			}

			require.NotPanics(t, func() {
				_, cantPushdown := builder.pushdownFilters(1, []*planpb.Expr{filter}, false)
				require.Empty(t, cantPushdown)
			})
			require.Empty(t, builder.qry.Nodes[0].FilterList)
			require.Equal(t, []*planpb.Expr{filter}, builder.qry.Nodes[1].FilterList)
		})
	}
}

func TestAggFilterPushdownWithoutGroupingFlagsKeepsLegacyPath(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, `
		select grp
		from (select distinct n_comment as grp from nation) as distinct_rows
		where grp is null`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	var distinctAggs []*planpb.Node
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_AGG && len(node.GroupBy) == 1 && len(node.GroupingFlag) == 0 {
			distinctAggs = append(distinctAggs, node)
		}
	}
	require.Len(t, distinctAggs, 1)

	distinctAgg := distinctAggs[0]
	scan := onlyTableScanBelow(t, query, distinctAgg)
	require.True(t, containsFilterFunction(scan.FilterList, "isnull"))
	require.False(t, containsFilterFunction(distinctAgg.FilterList, "isnull"))
}

func rollupAggBranches(query *planpb.Query, groupCount int) []*planpb.Node {
	var branches []*planpb.Node
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_AGG && len(node.GroupingFlag) == groupCount {
			branches = append(branches, node)
		}
	}
	return branches
}

func onlyTableScanBelow(t *testing.T, query *planpb.Query, root *planpb.Node) *planpb.Node {
	t.Helper()

	var scans []*planpb.Node
	var visit func(int32)
	visit = func(nodeID int32) {
		node := query.Nodes[nodeID]
		if node.NodeType == planpb.Node_TABLE_SCAN {
			scans = append(scans, node)
			return
		}
		for _, childID := range node.Children {
			visit(childID)
		}
	}
	for _, childID := range root.Children {
		visit(childID)
	}
	require.Len(t, scans, 1)
	return scans[0]
}

func containsFilterFunction(filters []*planpb.Expr, name string) bool {
	for _, filter := range filters {
		if fn := filter.GetF(); fn != nil && fn.Func.GetObjName() == name {
			return true
		}
	}
	return false
}

func TestSimplifyActiveGroupingFilter(t *testing.T) {
	builder, _ := genBuilderAndCtx(t)
	const tag int32 = 7
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		expr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), name, args)
		require.NoError(t, err)
		return expr
	}
	for _, tc := range []struct {
		name     string
		flags    []bool
		typ      types.T
		value    func(*planpb.Expr) *planpb.Expr
		change   func(*planpb.Expr) *planpb.Expr
		simplify bool
	}{
		{name: "active", flags: []bool{true}, simplify: true},
		{name: "active-varchar", typ: types.T_varchar, flags: []bool{true}, simplify: true},
		{name: "scalar-active", flags: []bool{true}, simplify: true, value: func(key *planpb.Expr) *planpb.Expr { return bind("abs", key) }},
		{name: "constant-without-active-key", flags: []bool{true}, value: func(key *planpb.Expr) *planpb.Expr { return makePlan2Int64ConstExprWithType(1) }},
		{name: "scalar-foreign", flags: []bool{true}, value: func(key *planpb.Expr) *planpb.Expr {
			key.GetCol().RelPos++
			return bind("abs", key)
		}},
		{name: "nested-grouping", flags: []bool{true}, value: func(key *planpb.Expr) *planpb.Expr { return bind("+", bind("grouping", key), key) }},
		{name: "volatile", flags: []bool{true}, value: func(key *planpb.Expr) *planpb.Expr { return bind("+", bind("rand"), key) }},
		{name: "planner-placeholder", flags: []bool{true}, value: func(key *planpb.Expr) *planpb.Expr {
			expr := bind("abs", key)
			expr.GetF().Func = &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.FULLTEXT_MATCH, 0), ObjName: "fulltext_match"}
			return expr
		}},
		{name: "diagnostic-conversion", flags: []bool{true}, value: func(key *planpb.Expr) *planpb.Expr {
			expr, err := appendCastBeforeExprWithOverload(builder.GetContext(), key, planpb.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}, 1)
			require.NoError(t, err)
			return expr
		}},
		{name: "unknown-expression", flags: []bool{true}, value: func(key *planpb.Expr) *planpb.Expr {
			return &planpb.Expr{Typ: key.Typ, Expr: &planpb.Expr_Raw{Raw: &planpb.RawColRef{}}}
		}},
		{name: "inactive", flags: []bool{false}},
		{name: "dynamic-missing-flags"},
		{name: "invalid-flag-count", flags: []bool{true, true}},
		{name: "foreign-tag", flags: []bool{true}, change: func(e *planpb.Expr) *planpb.Expr {
			e.GetF().Args[0].GetF().Args[0].GetCol().RelPos++
			e.GetF().Args[2].GetCol().RelPos++
			return e
		}},
		{name: "invalid-position", flags: []bool{true, true}, change: func(e *planpb.Expr) *planpb.Expr {
			e.GetF().Args[0].GetF().Args[0].GetCol().ColPos = 1
			e.GetF().Args[2].GetCol().ColPos = 1
			return e
		}},
		{name: "mismatched-value", flags: []bool{true}, change: func(e *planpb.Expr) *planpb.Expr { e.GetF().Args[0].GetF().Args[0].GetCol().ColPos++; return e }},
		{name: "mismatched-metadata", flags: []bool{true}, change: func(e *planpb.Expr) *planpb.Expr { e.GetF().Args[0].GetF().Args[0].AuxId++; return e }},
		{name: "not-null-then", flags: []bool{true}, change: func(e *planpb.Expr) *planpb.Expr { e.GetF().Args[1] = makePlan2Int64ConstExprWithType(0); return e }},
		{name: "wrong-null-type", flags: []bool{true}, change: func(e *planpb.Expr) *planpb.Expr { e.GetF().Args[1].Typ.Width++; return e }},
		{name: "wrong-function-id", flags: []bool{true}, change: func(e *planpb.Expr) *planpb.Expr {
			e.GetF().Args[0].GetF().Func.Obj = function.EncodeOverloadID(function.ABS, 0)
			return e
		}},
		{name: "grouping-observer", flags: []bool{true}, change: func(e *planpb.Expr) *planpb.Expr {
			out, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "grouping", []*planpb.Expr{e})
			require.NoError(t, err)
			return out
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := planpb.Type{Id: int32(types.T_int64)}
			if tc.typ == types.T_varchar {
				typ = planpb.Type{Id: int32(types.T_varchar), Width: 152}
			}
			key := GetColExpr(typ, tag, 0)
			if tc.value != nil {
				key = tc.value(key)
				key.Typ.NotNullable = false
			}
			normalized, err := normalizeGroupingSetProjects(builder.GetContext(), []*planpb.Expr{key}, []*planpb.Expr{key}, true)
			require.NoError(t, err)
			require.Equal(t, key.Typ, normalized[0].Typ, "normalization preserves the output type and collation provenance")
			expr := normalized[0]
			if tc.change != nil {
				expr = tc.change(expr)
			}
			before := DeepCopyExpr(expr)
			got := simplifyActiveGroupingFilter(expr, tag, 1, tc.flags)
			if tc.simplify {
				require.True(t, exprStructuralEqual(key, got))
			} else {
				require.True(t, exprStructuralEqual(before, got), "unsafe normalization must remain")
			}
		})
	}
}
