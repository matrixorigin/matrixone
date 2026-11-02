// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestCorrelationSinkReductionPreservesMainOutput(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_DELETE, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
	x := MakePlan2Int64ConstExprWithType(1)
	builder.qry.Nodes = []*plan.Node{
		{NodeType: plan.Node_VALUE_SCAN, ProjectList: []*plan.Expr{x}},
		{NodeType: plan.Node_SINK, Children: []int32{0}, ProjectList: []*plan.Expr{x}},
		{NodeType: plan.Node_SINK_SCAN, SourceStep: []int32{0}, ProjectList: []*plan.Expr{x}},
		{NodeType: plan.Node_PROJECT, Children: []int32{2}, ProjectList: []*plan.Expr{x}},
		{NodeType: plan.Node_VALUE_SCAN, ProjectList: []*plan.Expr{x}},
		{NodeType: plan.Node_SINK, Children: []int32{4}, ProjectList: []*plan.Expr{x}},
		{NodeType: plan.Node_SINK_SCAN, SourceStep: []int32{2}, ProjectList: []*plan.Expr{x}},
		{NodeType: plan.Node_PROJECT, Children: []int32{6}, ProjectList: []*plan.Expr{x}},
	}
	for id, node := range builder.qry.Nodes {
		node.NodeId = int32(id)
	}
	builder.qry.Steps = []int32{1, 3, 5, 7}
	builder.qry.Correlation = &plan.CorrelationSpec{Version: plan.CorrelationVersion, MainOutputStep: 2, StepOwners: []uint32{0, 0, 0, 0}}
	reduceSinkSinkScanNodes(builder.qry)
	require.Equal(t, []int32{3, 5, 7}, builder.qry.Steps)
	require.Equal(t, int32(1), builder.qry.MainOutputStep())
	require.Equal(t, []int32{1}, builder.qry.Nodes[6].SourceStep)
	require.Equal(t, []int32{0}, builder.qry.Nodes[3].Children)
	require.NoError(t, builder.refreshCorrelationBindings())
	require.Equal(t, []uint32{0, 0, 0}, builder.qry.Correlation.StepOwners)
	require.NoError(t, builder.qry.ValidateCorrelation())
}

func TestDependentDMLStepOwnership(t *testing.T) {
	cases := []struct{ name, sql string }{
		{"ordinary_external", "INSERT INTO dependent_ext SELECT n.N_NATIONKEY FROM NATION n"},
		{"dependent_select", "SELECT (SELECT (SELECT n.N_NATIONKEY)) FROM NATION n"},
		{"dependent_external", "INSERT INTO dependent_ext SELECT (SELECT (SELECT n.N_NATIONKEY)) FROM NATION n"},
		{"ordinary_fulltext", "INSERT INTO constraint_test.docs_ft(id,body,payload,embedding) SELECT n.N_NATIONKEY,'body',1,'[1,2,3]' FROM NATION n"},
		{"dependent_no_index", "INSERT INTO constraint_test.docs_vec_raw(id,payload,embedding) SELECT (SELECT (SELECT n.N_NATIONKEY)),1,'[1,2,3]' FROM NATION n"},
		{"dependent_fulltext_insert", "INSERT INTO constraint_test.docs_ft(id,body,payload,embedding) SELECT (SELECT (SELECT n.N_NATIONKEY)),'body',1,'[1,2,3]' FROM NATION n"},
		{"dependent_fulltext_update", "UPDATE constraint_test.docs_ft t SET body=CAST((SELECT (SELECT t.id)) AS VARCHAR(80))"},
		{"dependent_fulltext_delete", "DELETE FROM constraint_test.docs_ft WHERE id=(SELECT (SELECT docs_ft.id))"},
		{"dependent_fulltext_replace", "REPLACE INTO constraint_test.docs_ft(id,body,payload,embedding) SELECT (SELECT (SELECT n.N_NATIONKEY)),'body',1,'[1,2,3]' FROM NATION n"},
		{"dependent_legacy_multi_delete", "DELETE a,b FROM constraint_test.docs_ft a JOIN constraint_test.docs_vec_raw b ON a.id=b.id WHERE a.id=(SELECT (SELECT a.id))"},
	}
	checkFulltextInputs := func(t *testing.T, q *plan.Query, name string) {
		count := 0
		for _, node := range q.Nodes {
			if node.NodeType != plan.Node_APPLY || len(node.Children) != 2 {
				continue
			}
			right := q.Nodes[node.Children[1]]
			if right.TableDef == nil || right.TableDef.TblFunc == nil || right.TableDef.TblFunc.Name != "fulltext_index_tokenize" {
				continue
			}
			count++
			for _, expr := range node.ProjectList {
				require.NotNil(t, expr.GetCol())
				require.Equal(t, int32(1), expr.GetCol().RelPos)
			}
			for _, consumer := range q.Nodes {
				if len(consumer.Children) != 1 || consumer.Children[0] != node.NodeId || consumer.NodeType != plan.Node_PROJECT {
					continue
				}
				for _, expr := range consumer.ProjectList {
					if col := expr.GetCol(); col != nil {
						require.Zero(t, col.RelPos)
					}
				}
			}
		}
		if strings.Contains(name, "fulltext") && !strings.Contains(name, "delete") {
			require.Positive(t, count)
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
				Format: tree.CSV,
				Option: []string{"filepath", "stage://dependent_stage/ownership_*.csv", "format", "csv", "write_file_pattern", "stage://dependent_stage/ownership_%U.csv"},
			}}
			metadata, err := json.Marshal(param)
			require.NoError(t, err)
			mock.ctxt.tables["dependent_ext"] = &plan.TableDef{
				TblId: 1001, Name: "dependent_ext", TableType: catalog.SystemExternalRel,
				Createsql: string(metadata), Name2ColIndex: map[string]int32{"k": 0},
				Cols: []*plan.ColDef{{Name: "k", Typ: plan.Type{Id: int32(types.T_int32)}}},
			}
			mock.ctxt.objects["dependent_ext"] = &plan.ObjectRef{Obj: 1001, ObjName: "dependent_ext", SchemaName: mock.ctxt.DefaultDatabase()}
			for range 2 {
				func() {
					stmts, err := mysql.Parse(mock.CurrentContext().GetContext(), tc.sql, 1)
					require.NoError(t, err)
					defer stmts[0].Free()
					bound, err := BuildPlan(mock.CurrentContext(), stmts[0], false)
					require.NoError(t, err)
					q := bound.GetQuery()
					require.NoError(t, q.ValidateCorrelation())
					checkFulltextInputs(t, q, tc.name)
					if strings.HasPrefix(tc.name, "dependent_") {
						require.NotNil(t, q.Correlation)
						require.Zero(t, q.Correlation.StepOwners[q.MainOutputStep()])
						if tc.name == "dependent_external" {
							require.Equal(t, plan.Node_INSERT, q.Nodes[q.Steps[q.MainOutputStep()]].NodeType)
						}
					}
					fresh, err := mysql.Parse(mock.CurrentContext().GetContext(), tc.sql, 1)
					require.NoError(t, err)
					defer fresh[0].Free()
					optimized, err := NewBaseOptimizer(mock.CurrentContext()).Optimize(fresh[0], false)
					require.NoError(t, err)
					require.NoError(t, optimized.ValidateCorrelation())
					checkFulltextInputs(t, optimized, tc.name)
				}()
			}
		})
	}
}
