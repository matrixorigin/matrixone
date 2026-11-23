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

package compile

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minusall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestCompileParallelUnionDistinct(t *testing.T) {
	for _, tc := range []struct {
		name   string
		edit   func(*Compile, *plan.Node, []*Scope)
		owners int
	}{
		{"eligible", nil, 4},
		{"budget caps owners", func(_ *Compile, n *plan.Node, _ []*Scope) { n.SpillMem = 256 << 20 }, 2},
		{"auto unchanged", func(_ *Compile, n *plan.Node, _ []*Scope) { n.SpillMem = 0 }, 1},
		{"small explicit unchanged", func(_ *Compile, n *plan.Node, _ []*Scope) { n.SpillMem = 255 << 20 }, 1},
		{"max dop", func(c *Compile, _ *plan.Node, _ []*Scope) {
			c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{MaxDop: 2}}}
		}, 2},
		{"one cpu", func(c *Compile, _ *plan.Node, _ []*Scope) { c.ncpu = 1 }, 1},
		{"prepared", func(c *Compile, _ *plan.Node, _ []*Scope) { c.isPrepare = true }, 1},
		{"remote input", func(_ *Compile, _ *plan.Node, s []*Scope) { s[0].NodeInfo.Addr = "other:18000" }, 1},
		{"variable key", func(_ *Compile, n *plan.Node, _ []*Scope) { n.ProjectList[0].Typ.Id = int32(types.T_varchar) }, 1},
		{"different physical type", func(_ *Compile, n *plan.Node, _ []*Scope) {
			n.PhysicalEqualityKeyList = plan2.DeepCopyExprList(n.ProjectList)
			n.PhysicalEqualityKeyList[0].Typ.Id = int32(types.T_float64)
		}, 1},
		{"physical nullable annotation", func(_ *Compile, n *plan.Node, _ []*Scope) {
			n.PhysicalEqualityKeyList = plan2.DeepCopyExprList(n.ProjectList)
			n.PhysicalEqualityKeyList[0].Typ.NotNullable = true
		}, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newDistinctSetTestCompile(t)
			t.Cleanup(c.proc.Free)
			c.isPrepare = false
			c.addr = "127.0.0.1:18000"
			c.ncpu = 16
			c.cnList = engine.Nodes{{Addr: c.addr, Mcpu: 16}}
			input := newDistinctSetTestScopes(c, 2)
			for _, s := range input {
				s.NodeInfo.Addr = c.addr
			}
			node := &plan.Node{NodeType: plan.Node_UNION, SpillMem: 512 << 20, Stats: &plan.Stats{Dop: 16},
				ProjectList: []*plan.Expr{plan2.GetColExpr(types.T_int64.ToType().PlanType(), 0, 0)}}
			if tc.edit != nil {
				tc.edit(c, node, input)
			}
			before := proto.Clone(node)
			result := c.compileUnion(node, input[:1], input[1:], false)
			t.Cleanup(func() {
				for _, s := range result {
					s.FreeOperator(c)
					s.release()
				}
			})
			require.True(t, proto.Equal(before, node))
			require.Len(t, result, 1)
			if tc.owners == 1 {
				require.IsType(t, &group.Group{}, result[0].RootOp)
				return
			}
			require.IsType(t, &merge.Merge{}, result[0].RootOp)
			require.Len(t, result[0].PreScopes, tc.owners)
			var thresholdSum int64
			for i, owner := range result[0].PreScopes {
				g := owner.RootOp.GetOperatorBase().GetChildren(0).(*group.Group)
				thresholdSum += g.SpillMem
				require.GreaterOrEqual(t, g.SpillMem, int64(128<<20))
				require.True(t, g.IsFirst)
				require.Len(t, owner.Proc.Reg.MergeReceivers, 2)
				if i != 0 {
					require.Empty(t, owner.PreScopes)
					continue
				}
				require.Len(t, owner.PreScopes, 2)
				for _, feeder := range owner.PreScopes {
					router := feeder.RootOp.GetOperatorBase().GetChildren(0).(*shuffle.Shuffle)
					require.Equal(t, []int32{0}, router.ShuffleKeyColumns)
					require.True(t, router.DrainAllBuckets)
					require.Equal(t, int32(tc.owners), router.BucketNum)
					require.Len(t, feeder.PreScopes, 1)
				}
			}
			require.LessOrEqual(t, thresholdSum, node.SpillMem)
		})
	}
}

func TestMergeDistinctSetScopesAddsGlobalDedup(t *testing.T) {
	c := newDistinctSetTestCompile(t)
	left := newDistinctSetTestScope(c)
	right := newDistinctSetTestScope(c)
	node := &plan.Node{SpillMem: 1 << 20, ProjectList: []*plan.Expr{
		plan2.MakePlan2Int64ConstExprWithType(1),
		plan2.MakePlan2NullTextConstExprWithType(""),
	}}

	result := c.mergeDistinctSetScopes(node, []*Scope{left, right}, true)
	require.Len(t, result, 1)
	require.Len(t, result[0].PreScopes, 2)
	require.IsType(t, &group.Group{}, result[0].RootOp)
	require.False(t, c.anal.isFirst)

	groupOp := result[0].RootOp.(*group.Group)
	require.Equal(t, node.SpillMem, groupOp.SpillMem)
	require.Len(t, groupOp.GroupBy, 2)
	for i := range groupOp.GroupBy {
		require.NotSame(t, node.ProjectList[i], groupOp.GroupBy[i])
		require.False(t, groupOp.GroupBy[i].Typ.NotNullable)
	}

	result[0].FreeOperator(c)
	result[0].release()
	c.proc.Free()
}

func TestAggregateConsumesCompleteParallelDistinctOwners(t *testing.T) {
	c := newDistinctSetTestCompile(t)
	t.Cleanup(c.proc.Free)
	c.isPrepare = false
	c.addr = "127.0.0.1:18000"
	c.ncpu = 16
	c.cnList = engine.Nodes{{Addr: c.addr, Mcpu: 16}}
	input := newDistinctSetTestScopes(c, 2)
	owned := input
	t.Cleanup(func() {
		for _, s := range owned {
			s.FreeOperator(c)
			s.release()
		}
	})
	for _, s := range input {
		s.NodeInfo.Addr = c.addr
	}
	key := plan2.GetColExpr(types.T_int64.ToType().PlanType(), 0, 0)
	union := &plan.Node{NodeType: plan.Node_UNION, Children: []int32{0, 1},
		SpillMem: 512 << 20, Stats: &plan.Stats{Dop: 16}, ProjectList: []*plan.Expr{
			key, plan2.GetColExpr(types.T_int64.ToType().PlanType(), 0, 1),
		}}
	aggregate := &plan.Node{NodeType: plan.Node_AGG, Children: []int32{2},
		SpillMem: 128 << 20, GroupBy: []*plan.Expr{key}, GroupingFlag: []bool{true}}
	require.True(t, canPreserveParallelUnionDistinctOwners(aggregate, union))
	aggregate.GroupingFlag[0] = false
	require.False(t, canPreserveParallelUnionDistinctOwners(aggregate, union))
	aggregate.GroupingFlag[0] = true
	union.Limit = plan2.MakePlan2Int64ConstExprWithType(1)
	require.False(t, canPreserveParallelUnionDistinctOwners(aggregate, union))
	union.Limit = nil
	aggregate.ExtraOptions = "sort_rollup"
	require.False(t, canPreserveParallelUnionDistinctOwners(aggregate, union))
	aggregate.ExtraOptions = ""
	owners := c.compileUnion(union, input[:1], input[1:], true)
	owned = owners
	require.Len(t, owners, 4)
	for _, owner := range owners {
		require.IsType(t, &group.Group{}, owner.RootOp,
			"local aggregate must receive deduplicated tuples, not raw UNION branches")
	}
	// Different full UNION tuples can share the aggregate key: partials still
	// need the ordinary final MergeGroup, not just a gather.
	result := c.compileGroupWithoutShuffle(aggregate, owners, []*plan.Node{nil, nil, union}, false)
	owned = result // cleanup owns the complete resulting scope tree
	require.Len(t, result, 1)
	require.Equal(t, vm.MergeGroup, result[0].RootOp.OpType())
	require.Len(t, result[0].PreScopes, 4)
	for _, owner := range result[0].PreScopes {
		partial := owner.RootOp.GetOperatorBase().GetChildren(0).(*group.Group)
		require.IsType(t, &group.Group{}, partial.GetOperatorBase().GetChildren(0))
	}
}

func TestCompileUnionRetainsGlobalDistinctMerge(t *testing.T) {
	c := newDistinctSetTestCompile(t)
	node := &plan.Node{ProjectList: []*plan.Expr{plan2.MakePlan2Int64ConstExprWithType(1)}}

	result := c.compileUnion(
		node,
		[]*Scope{newDistinctSetTestScope(c)},
		[]*Scope{newDistinctSetTestScope(c)},
		false,
	)
	require.Len(t, result, 1)
	require.Len(t, result[0].PreScopes, 2)
	require.IsType(t, &group.Group{}, result[0].RootOp)
	require.Zero(t, result[0].RootOp.(*group.Group).SpillMem)
	require.False(t, c.anal.isFirst)

	result[0].FreeOperator(c)
	result[0].release()
	c.proc.Free()
}

func TestMergeDistinctSetScopesCompactsEqualityKeys(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		name := "reuse-visible"
		if mixed {
			name = "retain-distinct-domain"
		}
		t.Run(name, func(t *testing.T) {
			c := newDistinctSetTestCompile(t)
			t.Cleanup(c.proc.Free)
			typ := types.T_varchar.ToType().PlanType()
			visible := []*plan.Expr{plan2.GetColExpr(typ, 0, 0), plan2.GetColExpr(typ, 0, 1)}
			physical := plan2.DeepCopyExprList(visible)
			// The merge factory makes retained keys nullable. This metadata
			// difference must not create another copy of the same physical key.
			visible[0].Typ.NotNullable = true
			if mixed {
				physical[1].Typ.PadSpace = true
			}
			node := &plan.Node{ProjectList: visible, PhysicalEqualityKeyList: physical}
			before := proto.Clone(node)
			result := c.mergeDistinctSetScopes(node, newDistinctSetTestScopes(c, 2), true)
			t.Cleanup(func() {
				result[0].FreeOperator(c)
				result[0].release()
			})

			groupOp := result[0].RootOp.(*group.Group)
			if mixed {
				require.Len(t, groupOp.GroupBy, 3)
				require.Equal(t, []int32{0, 2}, groupOp.GroupByHashKey)
				require.True(t, proto.Equal(physical[1], groupOp.GroupBy[2]))
			} else {
				require.Len(t, groupOp.GroupBy, 2)
				require.Empty(t, groupOp.GroupByHashKey)
			}
			require.Len(t, groupOp.ProjectList, 2)
			for i, expr := range visible {
				require.NotSame(t, expr, groupOp.GroupBy[i])
				require.False(t, groupOp.GroupBy[i].Typ.NotNullable)
				require.Equal(t, int32(i), groupOp.ProjectList[i].GetCol().ColPos)
				require.Equal(t, expr.Typ, groupOp.ProjectList[i].Typ)
			}
			require.True(t, proto.Equal(before, node), "set operators still need the complete, unchanged key tuple")
		})
	}
}

func TestCompileParallelDistinctSetMergesWorkerResults(t *testing.T) {
	for _, nodeType := range []plan.Node_NodeType{plan.Node_INTERSECT, plan.Node_MINUS} {
		t.Run(nodeType.String(), func(t *testing.T) {
			c := newDistinctSetTestCompile(t)
			node := newParallelDistinctSetTestNode(nodeType)
			node.SpillMem = 1 << 20
			node.Stats.HashmapStats.Shuffle = true

			result := c.compileMinusAndIntersect(
				node,
				newDistinctSetTestScopes(c, 2),
				newDistinctSetTestScopes(c, 2),
				nodeType,
			)
			require.Len(t, result, 1)
			require.Len(t, result[0].PreScopes, 2)
			require.IsType(t, &group.Group{}, result[0].RootOp)
			require.Equal(t, node.SpillMem, result[0].RootOp.(*group.Group).SpillMem)
			require.False(t, c.anal.isFirst)

			result[0].FreeOperator(c)
			result[0].release()
			c.proc.Free()
		})
	}
}

func TestCompileSingleScopeMultisetAvoidsConsolidation(t *testing.T) {
	for _, execution := range []struct {
		name   string
		typeID plan2.ExecType
	}{
		{"TP", plan2.ExecTypeTP},
		{"AP", plan2.ExecTypeAP_ONECN},
	} {
		for _, nodeType := range []plan.Node_NodeType{plan.Node_INTERSECT_ALL, plan.Node_MINUS_ALL} {
			t.Run(execution.name+"/"+nodeType.String(), func(t *testing.T) {
				c := newDistinctSetTestCompile(t)
				c.execType = execution.typeID
				left := newDistinctSetTestScope(c)
				right := newDistinctSetTestScope(c)
				result := c.compileMinusAndIntersect(newParallelDistinctSetTestNode(nodeType), []*Scope{left}, []*Scope{right}, nodeType)
				t.Cleanup(func() {
					for _, scope := range result {
						scope.FreeOperator(c)
						scope.release()
					}
					c.proc.Free()
				})
				require.Len(t, result, 1)
				require.Len(t, result[0].PreScopes, 2)
				require.Same(t, left, result[0].PreScopes[0], "single-worker input must feed the owner directly")
				require.Same(t, right, result[0].PreScopes[1], "single-worker input must feed the owner directly")
				if nodeType == plan.Node_INTERSECT_ALL {
					require.IsType(t, &intersectall.IntersectAll{}, result[0].RootOp)
				} else {
					require.IsType(t, &minusall.MinusAll{}, result[0].RootOp)
				}
				require.False(t, c.anal.isFirst)
			})
		}
	}
}

func TestCompileBroadcastDistinctSetHasSingleOwner(t *testing.T) {
	for _, nodeType := range []plan.Node_NodeType{plan.Node_INTERSECT, plan.Node_MINUS} {
		t.Run(nodeType.String(), func(t *testing.T) {
			c := newDistinctSetTestCompile(t)
			node := newParallelDistinctSetTestNode(nodeType)
			left := newDistinctSetTestScopes(c, 2)
			right := newDistinctSetTestScopes(c, 2)
			node.PhysicalEqualityKeyList = []*plan.Expr{{
				Typ:  node.ProjectList[0].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
			}}
			result := c.compileMinusAndIntersect(node, left, right, nodeType)
			t.Cleanup(func() {
				for _, scope := range result {
					scope.FreeOperator(c)
					scope.release()
				}
				c.proc.Free()
			})
			require.Len(t, result, 1)
			require.Len(t, result[0].PreScopes, 2, "one owner must consume both complete inputs")
			switch nodeType {
			case plan.Node_INTERSECT:
				require.IsType(t, &intersect.Intersect{}, result[0].RootOp)
				require.Equal(t, node.PhysicalEqualityKeyList, result[0].RootOp.(*intersect.Intersect).KeyExprs)
			case plan.Node_MINUS:
				require.IsType(t, &minus.Minus{}, result[0].RootOp)
				require.Equal(t, node.PhysicalEqualityKeyList, result[0].RootOp.(*minus.Minus).KeyExprs)
			}
			for i, scopes := range [][]*Scope{left, right} {
				require.Len(t, result[0].PreScopes[i].PreScopes, len(scopes))
				for j, scope := range scopes {
					require.Same(t, scope, result[0].PreScopes[i].PreScopes[j], "keep every upstream scope")
				}
			}
			require.False(t, c.anal.isFirst)
		})
	}
}

func TestCompileParallelIntersectAllUsesSingleMultiplicityOwner(t *testing.T) {
	for _, tc := range []struct {
		name   string
		scopes int
		mcpu   int
	}{
		{"multiple-scopes", 2, 1},
		{"one-parallel-scope", 1, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newDistinctSetTestCompile(t)
			node := newParallelDistinctSetTestNode(plan.Node_INTERSECT_ALL)
			left := newDistinctSetTestScopes(c, tc.scopes)
			right := newDistinctSetTestScopes(c, tc.scopes)
			for _, input := range append(left, right...) {
				input.NodeInfo.Mcpu = tc.mcpu
			}
			result := c.compileMinusAndIntersect(node, left, right, plan.Node_INTERSECT_ALL)
			t.Cleanup(func() {
				for _, scope := range result {
					scope.FreeOperator(c)
					scope.release()
				}
				c.proc.Free()
			})
			require.Len(t, result, 1)
			require.IsType(t, &intersectall.IntersectAll{}, result[0].RootOp)
			require.Len(t, result[0].PreScopes, 2)
			for _, input := range result[0].PreScopes {
				require.Len(t, input.PreScopes, tc.scopes, "parallel producers must each feed the owner once")
			}
			require.False(t, c.anal.isFirst)
		})
	}
}

func TestCompileParallelMinusAllUsesSingleMultiplicityOwner(t *testing.T) {
	c := newDistinctSetTestCompile(t)
	node := newParallelDistinctSetTestNode(plan.Node_MINUS_ALL)

	result := c.compileMinusAndIntersect(
		node,
		newDistinctSetTestScopes(c, 2),
		newDistinctSetTestScopes(c, 2),
		plan.Node_MINUS_ALL,
	)
	require.Len(t, result, 1)
	require.IsType(t, &minusall.MinusAll{}, result[0].RootOp)
	require.Len(t, result[0].PreScopes, 2)
	require.False(t, c.anal.isFirst)

	result[0].FreeOperator(c)
	result[0].release()
	c.proc.Free()
}

func newDistinctSetTestCompile(t *testing.T) *Compile {
	c := NewMockCompile(t)
	c.anal = &AnalyzeModule{curNodeIdx: 1, isFirst: true}
	c.execType = plan2.ExecTypeAP_ONECN
	c.isPrepare = true
	return c
}

func newDistinctSetTestScope(c *Compile) *Scope {
	return &Scope{
		Magic:    Normal,
		NodeInfo: engine.Node{Mcpu: 1},
		Proc:     c.proc.NewNoContextChildProc(0),
		RootOp:   colexec.NewMockOperator(),
	}
}

func newDistinctSetTestScopes(c *Compile, count int) []*Scope {
	scopes := make([]*Scope, count)
	for i := range scopes {
		scopes[i] = newDistinctSetTestScope(c)
	}
	return scopes
}

func newParallelDistinctSetTestNode(nodeType plan.Node_NodeType) *plan.Node {
	return &plan.Node{
		NodeType:    nodeType,
		ProjectList: []*plan.Expr{plan2.MakePlan2Int64ConstExprWithType(1)},
		Stats: &plan.Stats{
			Dop:          2,
			HashmapStats: &plan.HashMapStats{},
		},
	}
}
