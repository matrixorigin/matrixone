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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestFilteredSetOperationProjection(t *testing.T) {
	for _, op := range []string{"union all", "union", "intersect", "intersect all", "except", "except all"} {
		t.Run(op, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t,
				"select k,v from (select 1 k,10 v "+op+" select 2,20) u where k>0")
			require.NoError(t, err)
			found := false
			for _, n := range p.GetQuery().Nodes {
				switch n.NodeType {
				case planpb.Node_UNION_ALL, planpb.Node_UNION, planpb.Node_INTERSECT, planpb.Node_INTERSECT_ALL, planpb.Node_MINUS, planpb.Node_MINUS_ALL:
					found = true
					require.Len(t, n.ProjectList, 2)
					for _, child := range n.Children {
						require.Len(t, p.GetQuery().Nodes[child].ProjectList, 2)
					}
				}
			}
			require.True(t, found)
		})
	}
}

// These cases independently check the edge adapter: equal widths alone cannot
// prove correspondence, and a wider inner set must keep its equality tuple.
func TestAlignSetOperationInput(t *testing.T) {
	for _, tc := range []struct {
		name        string
		slots       []int32
		wantProject bool
	}{
		{"identity", []int32{0, 1}, false}, {"reordered", []int32{1, 0}, true}, {"extra-equality-column", []int32{2, 0}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			width := 2
			if tc.name == "extra-equality-column" {
				width = 3
			}
			node := &planpb.Node{NodeType: planpb.Node_UNION, ProjectList: make([]*planpb.Expr, width)}
			for i := range node.ProjectList {
				node.ProjectList[i] = &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}}
			}
			builder := &QueryBuilder{qry: &planpb.Query{Nodes: []*planpb.Node{node}}, ctxByNode: []*BindContext{nil}}
			refs := [][2]int32{{10, 0}, {10, 1}}
			mapping := &ColRefRemapping{globalToLocal: map[[2]int32][2]int32{refs[0]: {0, tc.slots[0]}, refs[1]: {0, tc.slots[1]}}}
			id, err := builder.alignSetOperationInput(0, refs, []int{0, 1}, mapping)
			require.NoError(t, err)
			require.Equal(t, tc.wantProject, id != 0)
			require.Len(t, node.ProjectList, width, "inner membership schema must be unchanged")
			if tc.wantProject {
				project := builder.qry.Nodes[id]
				require.Equal(t, []int32{0}, project.Children)
				for i, e := range project.ProjectList {
					require.Equal(t, tc.slots[i], e.GetCol().ColPos)
				}
			}
		})
	}
	t.Run("missing-or-invalid-slot", func(t *testing.T) {
		builder := &QueryBuilder{compCtx: &MockCompilerContext{ctx: t.Context()}, qry: &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{{}}}}}}
		for _, m := range []map[[2]int32][2]int32{{}, {{10, 0}: {1, 0}}, {{10, 0}: {0, -1}}, {{10, 0}: {0, 1}}} {
			_, err := builder.alignSetOperationInput(0, [][2]int32{{10, 0}}, []int{0}, &ColRefRemapping{globalToLocal: m})
			require.ErrorContains(t, err, "missing set operation input column")
			require.Len(t, builder.qry.Nodes, 1)
		}
	})
}

func TestSetOperationAliasPruningIsBounded(t *testing.T) {
	add, err := function.GetFunctionByName(t.Context(), "+", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
	require.NoError(t, err)
	builder := &QueryBuilder{qry: &planpb.Query{}, tag2NodeID: make(map[int32]int32)}
	const depth = 40
	for i := 0; i <= depth; i++ {
		expr := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 1}}}}
		if i > 0 {
			ref := &planpb.Expr{Typ: expr.Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: int32(i), ColPos: 0}}}
			expr.Expr = &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "+", Obj: add.GetEncodedOverloadID()}, Args: []*planpb.Expr{ref, ref}}}
		}
		node := &planpb.Node{NodeId: int32(i), NodeType: planpb.Node_PROJECT, BindingTags: []int32{int32(i + 1)}, ProjectList: []*planpb.Expr{expr}}
		builder.qry.Nodes = append(builder.qry.Nodes, node)
		builder.tag2NodeID[int32(i+1)] = int32(i)
	}
	memo := make(map[[2]int32]bool)
	require.True(t, builder.setOperationOutputCanPrune(builder.qry.Nodes[depth], 0, memo))
	require.Len(t, memo, depth+1, "shared aliases must expand each owner once")
	// A cyclic projected slot is conservatively retained and terminates.
	cycle := builder.qry.Nodes[0]
	cycle.ProjectList[0].Expr = &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 0}}
	require.False(t, builder.setOperationOutputCanPrune(cycle, 0, make(map[[2]int32]bool)))
}
