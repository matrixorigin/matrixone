// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/stretchr/testify/require"
)

// A materialized source must provide every column its reachable readers use.
// Multi-target updates also have consumers of the full assignment projection,
// so coalescing the delete source must not narrow those consumers.
func TestMultiTargetUpdateSinkScanSourceColumns(t *testing.T) {
	for _, stmt := range []string{
		"UPDATE NATION a JOIN NATION b ON a.N_NATIONKEY = 1 AND b.N_NATIONKEY = 2 SET a.N_REGIONKEY = 10, b.N_REGIONKEY = 20",
		"UPDATE IGNORE NATION a JOIN NATION b ON a.N_NATIONKEY = 1 AND b.N_NATIONKEY = 2 SET a.N_REGIONKEY = 10, b.N_REGIONKEY = 20",
	} {
		t.Run(stmt, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(true), t, stmt)
			require.NoError(t, err)
			q := p.GetQuery()
			visited := make(map[int32]bool)
			var visit func(int32)
			visit = func(nodeID int32) {
				require.GreaterOrEqual(t, nodeID, int32(0))
				require.Less(t, int(nodeID), len(q.Nodes))
				if visited[nodeID] {
					return
				}
				visited[nodeID] = true
				node := q.Nodes[nodeID]
				for _, child := range node.Children {
					visit(child)
				}
				if node.NodeType != plan.Node_SINK_SCAN {
					return
				}
				require.Len(t, node.SourceStep, 1)
				step := node.SourceStep[0]
				require.GreaterOrEqual(t, step, int32(0))
				require.Less(t, int(step), len(q.Steps))
				producerID := q.Steps[step]
				require.GreaterOrEqual(t, producerID, int32(0))
				require.Less(t, int(producerID), len(q.Nodes))
				producer := q.Nodes[producerID]
				for _, expr := range node.ProjectList {
					if col := expr.GetCol(); col != nil {
						require.GreaterOrEqual(t, col.ColPos, int32(0))
						require.Less(t, int(col.ColPos), len(producer.ProjectList),
							"scan %d reads column %d from step %d with %d columns", nodeID, col.ColPos, step, len(producer.ProjectList))
					}
				}
				visit(producerID)
			}
			for _, root := range q.Steps {
				visit(root)
			}
		})
	}
}

func TestLegacyMultiTargetUpdateUsesCumulativeAssignmentOffsets(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_UPDATE, NewMockCompilerContext(true), false, true)
	projectList := make([]*plan.Expr, 10)
	for i := range projectList {
		projectList[i] = &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_text)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: int32(i)}},
		}
	}
	builder.qry.Nodes = append(builder.qry.Nodes, &plan.Node{ProjectList: projectList})

	column := func(name string) *plan.ColDef {
		return &plan.ColDef{
			Name: name,
			Typ:  plan.Type{Id: int32(types.T_varchar), Width: 3},
		}
	}
	planContexts := []*dmlPlanCtx{
		{
			tableDef:        &plan.TableDef{Cols: []*plan.ColDef{column("c1"), column("c2")}},
			updateColLength: 2,
			updateColPosMap: map[string]int{"c1": 2, "c2": 3},
		},
		{
			tableDef:        &plan.TableDef{Cols: []*plan.ColDef{column("c3")}},
			updateColLength: 1,
			updateColPosMap: map[string]int{"c3": 1},
		},
		{
			tableDef:        &plan.TableDef{Cols: []*plan.ColDef{column("c4"), column("c5"), column("c6")}},
			updateColLength: 1,
			updateColPosMap: map[string]int{"c6": 3},
		},
	}

	require.NoError(t, rewriteUpdateQueryLastNode(builder, planContexts, 0, true))
	for _, pos := range []int{2, 3, 5, 9} {
		castExpr := builder.qry.Nodes[0].ProjectList[pos]
		require.Equal(t, "cast_ignore", castExpr.GetF().GetFunc().GetObjName(),
			"target assignment at source block position %d must be cast", pos)
		arg := castExpr.GetF().GetArgs()[0].GetCol()
		require.NotNil(t, arg, "cast at position %d must preserve its source column", pos)
		require.Equal(t, int32(pos), arg.GetColPos(), "cast at position %d must wrap its own RHS", pos)
	}
	for pos, expr := range builder.qry.Nodes[0].ProjectList {
		source := expr
		if fn := expr.GetF(); fn != nil {
			require.NotEmpty(t, fn.GetArgs(), "projection at position %d must retain its source", pos)
			source = fn.GetArgs()[0]
		}
		col := source.GetCol()
		require.NotNil(t, col, "projection at position %d must retain a column source", pos)
		require.Equal(t, int32(pos), col.GetColPos(), "projection at position %d must preserve its source column", pos)
	}
}
