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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestMergeDistinctSetScopesAddsGlobalDedup(t *testing.T) {
	c := newDistinctSetTestCompile(t)
	left := newDistinctSetTestScope(c)
	right := newDistinctSetTestScope(c)
	node := &plan.Node{ProjectList: []*plan.Expr{
		plan2.MakePlan2Int64ConstExprWithType(1),
		plan2.MakePlan2NullTextConstExprWithType(""),
	}}

	result := c.mergeDistinctSetScopes(node, []*Scope{left, right}, true)
	require.Len(t, result, 1)
	require.Len(t, result[0].PreScopes, 2)
	require.IsType(t, &group.Group{}, result[0].RootOp)
	require.False(t, c.anal.isFirst)

	groupOp := result[0].RootOp.(*group.Group)
	require.Len(t, groupOp.GroupBy, 2)
	for i := range groupOp.GroupBy {
		require.NotSame(t, node.ProjectList[i], groupOp.GroupBy[i])
		require.False(t, groupOp.GroupBy[i].Typ.NotNullable)
	}

	result[0].FreeOperator(c)
	result[0].release()
	c.proc.Free()
}

func TestCompileUnionRetainsGlobalDistinctMerge(t *testing.T) {
	c := newDistinctSetTestCompile(t)
	node := &plan.Node{ProjectList: []*plan.Expr{plan2.MakePlan2Int64ConstExprWithType(1)}}

	result := c.compileUnion(
		node,
		[]*Scope{newDistinctSetTestScope(c)},
		[]*Scope{newDistinctSetTestScope(c)},
	)
	require.Len(t, result, 1)
	require.Len(t, result[0].PreScopes, 2)
	require.IsType(t, &group.Group{}, result[0].RootOp)
	require.False(t, c.anal.isFirst)

	result[0].FreeOperator(c)
	result[0].release()
	c.proc.Free()
}

func TestCompileParallelDistinctSetMergesWorkerResults(t *testing.T) {
	for _, nodeType := range []plan.Node_NodeType{plan.Node_INTERSECT, plan.Node_MINUS} {
		t.Run(nodeType.String(), func(t *testing.T) {
			c := newDistinctSetTestCompile(t)
			node := newParallelDistinctSetTestNode(nodeType)

			result := c.compileMinusAndIntersect(
				node,
				newDistinctSetTestScopes(c, 2),
				newDistinctSetTestScopes(c, 2),
				nodeType,
			)
			require.Len(t, result, 1)
			require.Len(t, result[0].PreScopes, 2)
			require.IsType(t, &group.Group{}, result[0].RootOp)
			require.False(t, c.anal.isFirst)

			result[0].FreeOperator(c)
			result[0].release()
			c.proc.Free()
		})
	}
}

func TestCompileParallelIntersectAllKeepsWorkerResults(t *testing.T) {
	c := newDistinctSetTestCompile(t)
	node := newParallelDistinctSetTestNode(plan.Node_INTERSECT_ALL)

	result := c.compileMinusAndIntersect(
		node,
		newDistinctSetTestScopes(c, 2),
		newDistinctSetTestScopes(c, 2),
		plan.Node_INTERSECT_ALL,
	)
	require.Len(t, result, 2)
	for _, scope := range result {
		require.IsType(t, &intersectall.IntersectAll{}, scope.RootOp)
	}
	require.False(t, c.anal.isFirst)

	for _, scope := range result {
		scope.FreeOperator(c)
		scope.release()
	}
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
