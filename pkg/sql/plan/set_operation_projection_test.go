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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestSetOperationFunctionColumnPruning(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		columns     []string
	}{
		{"path", `select path from (select n.path,n.value from unnest('[1,2]') n union all select n.path,n.value from unnest('[3,4]') n) u`, []string{"path"}},
		{"value", `select value from (select n.path,n.value from unnest('[1,2]') n union all select n.path,n.value from unnest('[3,4]') n) u`, []string{"value"}},
		{"expression-alias", `select p from (select concat(n.path,'') p,n.value from unnest('[1,2]') n union all select concat(n.path,''),n.value from unnest('[3,4]') n) u`, []string{"path"}},
		{"filter-demand", `select path from (select n.path,n.value from unnest('[1,2]') n where cast(n.value as varchar)='1' union all select n.path,n.value from unnest('[3,4]') n where cast(n.value as varchar)='3') u`, []string{"path", "value"}},
		{"count-carrier", `select count(*) from (select n.path,n.value from unnest('[1,2]') n union all select n.path,n.value from unnest('[3,4]') n) u`, []string{"path"}},
		{"membership-tuple", `select path from (select n.path,n.value from unnest('[1]') n union select n.path,n.value from unnest('[2]') n) u`, []string{"path", "value"}},
		{"lateral-input", `select path from (select n.path,n.value from (select '[1,2]' j) t,unnest(t.j) n union all select n.path,n.value from (select '[3,4]' j) t,unnest(t.j) n) u`, []string{"path"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			p, err := runOneStmt(mock, t, tc.query)
			require.NoError(t, err)
			q := p.GetQuery()
			seen := make(map[int32]bool)
			scans := 0
			var visit func(int32)
			visit = func(id int32) {
				if seen[id] {
					return
				}
				seen[id] = true
				n := q.Nodes[id]
				if n.NodeType == planpb.Node_FUNCTION_SCAN && n.TableDef.GetTblFunc().GetName() == "unnest" {
					scans++
					var cols []string
					for _, c := range n.TableDef.Cols {
						cols = append(cols, c.Name)
						if c.Name == "path" {
							require.Equal(t, int32(types.T_varchar), c.Typ.Id)
						}
						if c.Name == "value" {
							require.Equal(t, int32(types.T_json), c.Typ.Id)
						}
					}
					require.Equal(t, tc.columns, cols, "runtime attributes must reflect output and owner-local demands")
					require.Len(t, n.TblFuncExprList, 1, "pruning must retain the function argument")
				}
				for _, child := range n.Children {
					visit(child)
				}
			}
			for _, step := range q.Steps {
				visit(step)
			}
			require.Equal(t, 2, scans)
		})
	}
}

func TestSetOperationExternalColumnPruning(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		columns     []string
	}{
		{"first", "select a from (select a,b from ext union all select a,b from ext) u", []string{"a"}},
		{"second", "select b from (select a,b from ext union all select a,b from ext) u", []string{"b"}},
		{"expression-alias", "select x from (select a+1 x,b from ext union all select a+1,b from ext) u", []string{"a"}},
		{"filter-demand", "select a from (select a,b from ext where b>0 union all select a,b from ext where b>0) u", []string{"a", "b"}},
		{"error-record", "select e from (select a,b,__mo_error_message e from ext union all select a,b,__mo_error_message from ext) u", []string{"a", "b", catalog.ExternalErrorMessage}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			mock.ctxt.objects["ext"] = &planpb.ObjectRef{DbName: "tpch", ObjName: "ext", Obj: 4345}
			mock.ctxt.tables["ext"] = &planpb.TableDef{
				Name: "ext", TableType: catalog.SystemExternalRel,
				Createsql: mustMarshalLegacyExternParam(t, &tree.ExternParam{
					ExParamConst: tree.ExParamConst{ScanType: tree.INFILE, Filepath: "/data/pruning.csv", Format: tree.CSV, Option: []string{"format", "csv"}},
				}),
				Cols: []*planpb.ColDef{
					{ColId: 1, Name: "a", Typ: planpb.Type{Id: int32(types.T_int32), Table: "ext"}, Default: &planpb.Default{NullAbility: true}},
					{ColId: 2, Name: "b", Typ: planpb.Type{Id: int32(types.T_int32), Table: "ext"}, Default: &planpb.Default{NullAbility: true}},
				},
			}
			p, err := runOneStmt(mock, t, tc.query)
			require.NoError(t, err)
			q := p.GetQuery()
			seen := make(map[int32]bool)
			var scans, unions int
			var visit func(int32)
			visit = func(id int32) {
				if seen[id] {
					return
				}
				seen[id] = true
				n := q.Nodes[id]
				if n.NodeType == planpb.Node_EXTERNAL_SCAN {
					scans++
					var columns []string
					for _, col := range n.TableDef.Cols {
						columns = append(columns, col.Name)
						if col.Name == "a" || col.Name == "b" {
							field := int32(0)
							if col.Name == "b" {
								field = 1
							}
							got, ok := n.ExternScan.TbColToDataCol[col.Name]
							require.True(t, ok)
							require.Equal(t, field, got, "pruning must preserve the original CSV field index")
						}
					}
					require.Equal(t, tc.columns, columns)
				}
				if n.NodeType == planpb.Node_UNION_ALL {
					unions++
					require.Len(t, n.ProjectList, 1)
					for _, child := range n.Children {
						require.Len(t, q.Nodes[child].ProjectList, 1)
					}
				}
				for _, child := range n.Children {
					visit(child)
				}
			}
			for _, root := range q.Steps {
				visit(root)
			}
			require.Equal(t, 2, scans)
			require.Equal(t, 1, unions)
		})
	}
}

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
