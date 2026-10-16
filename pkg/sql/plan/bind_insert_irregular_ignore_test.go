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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestInsertIgnoreIrregularMaintenanceReadsAcceptedRows(t *testing.T) {
	for _, tc := range []struct {
		name, sql                  string
		reorder, ignore, irregular bool
	}{
		{"plain_insert", "insert into docs_ft(id,body,payload) values (1,'alpha',1)", false, false, true},
		{"primary_only", "insert ignore into docs_ft(id,body,payload) values (1,'alpha',1),(1,'beta',2)", false, true, true},
		{"generated_with_composite_unique", "insert ignore into docs_ft(body,payload) values ('alpha',1),('alpha',1),('gamma',2)", true, true, true},
		{"no_irregular_index", "insert ignore into docs_ft(id,body,payload) values (1,'alpha',1),(1,'beta',2)", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			table := mock.ctxt.tables["docs_ft"]
			if !tc.irregular {
				table.Indexes = nil
			}
			if tc.reorder {
				table.Cols[0].Typ.AutoIncr = true
				unique := *mock.ctxt.tables["dept"].Indexes[0]
				unique.IndexName = "uk_body_payload"
				unique.Parts = []string{"body", "payload"}
				table.Indexes = append(table.Indexes, &unique)
				idxTable := mock.ctxt.tables[unique.IndexTableName]
				idxTable.Cols[idxTable.Name2ColIndex[catalog.IndexTablePrimaryColName]].Typ = table.Cols[0].Typ
			}
			p, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			q := p.GetQuery()
			assertEverySinkStepHasConsumer(t, q)
			var reachesArbitration func(int32) bool
			reachesArbitration = func(id int32) bool {
				n := q.Nodes[id]
				if n.GetPreInsertUkCtx().GetInsertIgnoreMultiDedup() ||
					(n.NodeType == planpb.Node_JOIN && n.JoinType == planpb.Node_DEDUP && n.OnDuplicateAction == planpb.Node_IGNORE) {
					return true
				}
				for _, child := range n.Children {
					if reachesArbitration(child) {
						return true
					}
				}
				for _, step := range n.SourceStep {
					if reachesArbitration(q.Steps[step]) {
						return true
					}
				}
				return false
			}
			preInserts, tokenizers, sinks := 0, 0, 0
			for id := range reachableODKUPlanNodes(q) {
				n := q.Nodes[id]
				if n.NodeType == planpb.Node_SINK {
					sinks++
				}
				if n.NodeType == planpb.Node_PRE_INSERT && n.PreInsertCtx.TableDef.Name == "docs_ft" {
					preInserts++
				}
				if n.NodeType == planpb.Node_FUNCTION_SCAN && n.TableDef.GetTblFunc().GetName() == "fulltext_index_tokenize" {
					tokenizers++
				}
				if n.NodeType == planpb.Node_APPLY {
					require.Equal(t, tc.ignore, reachesArbitration(id), "tokenization input must be downstream of IGNORE arbitration")
				}
			}
			if tc.reorder {
				require.Equal(t, 1, preInserts, "the source and allocator must not be evaluated once per consumer")
			} else {
				require.Zero(t, preInserts, "a non-generated input does not need an allocator")
			}
			if tc.irregular {
				require.Equal(t, 1, tokenizers)
				require.Equal(t, 1, sinks, "move the existing shared image instead of adding another materialization")
			} else {
				require.Zero(t, tokenizers)
				require.Zero(t, sinks)
			}
		})
	}
}

func TestInsertImageSinkScanRetainsAuxiliaryColumns(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	builder := NewQueryBuilder(planpb.Query_INSERT, mock.CurrentContext(), false, true)
	ctx := NewBindContext(builder, nil)
	baseType := planpb.Type{Id: int32(types.T_int64)}
	auxType := planpb.Type{Id: int32(types.T_varbinary)}
	image := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		BindingTags: []int32{builder.genNewBindTag()},
		ProjectList: []*planpb.Expr{{Typ: baseType}, {Typ: auxType}},
	}, ctx)
	sink := appendSinkNodeWithTag(builder, ctx, image, builder.genNewBindTag())
	step := builder.appendStep(sink)
	scan := builder.appendImageSinkScanNode(ctx, step, builder.genNewBindTag(), &planpb.TableDef{
		Name: "t", Cols: []*planpb.ColDef{{Name: "id", Typ: baseType}, {Name: catalog.Row_ID}},
	})
	n := builder.qry.Nodes[scan]
	require.Len(t, n.TableDef.Cols, len(n.ProjectList))
	require.Equal(t, "id", n.TableDef.Cols[0].Name)
	require.True(t, n.TableDef.Cols[1].Hidden)
	require.Equal(t, auxType, n.TableDef.Cols[1].Typ)
}

func TestLoadIgnoreCheckPrecedesIrregularMaintenance(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	table := mock.ctxt.tables["docs_ft"]
	addPositiveCheck(t, mock, table.Name, "payload")
	unique := *mock.ctxt.tables["dept"].Indexes[0]
	unique.IndexName = "uk_body_payload"
	unique.Parts = []string{"body", "payload"}
	table.Indexes = append(table.Indexes, &unique)
	idxTable := mock.ctxt.tables[unique.IndexTableName]
	idxTable.Cols[idxTable.Name2ColIndex[catalog.IndexTablePrimaryColName]].Typ = table.Cols[0].Typ
	p, err := runOneStmt(mock, t, "load data inline format='csv', data='1,alpha,-1' ignore into table docs_ft fields terminated by ',' (id,body,payload)")
	require.NoError(t, err)
	q := p.GetQuery()
	assertEverySinkStepHasConsumer(t, q)
	var reachesCheck func(int32) bool
	reachesCheck = func(id int32) bool {
		n := q.Nodes[id]
		if n.NodeType == planpb.Node_FILTER && n.FilterIsBarrier {
			for _, expr := range n.FilterList {
				if exprContainsFuncName(expr, "_check_constraint_assert") {
					return true
				}
			}
		}
		for _, child := range n.Children {
			if reachesCheck(child) {
				return true
			}
		}
		for _, step := range n.SourceStep {
			if reachesCheck(q.Steps[step]) {
				return true
			}
		}
		return false
	}
	writes, tokenizers, images := 0, 0, 0
	for id := range reachableODKUPlanNodes(q) {
		n := q.Nodes[id]
		if n.NodeType == planpb.Node_SINK && reachesCheck(id) {
			images++
			require.Len(t, n.Children, 1)
			image := q.Nodes[n.Children[0]]
			require.Equal(t, planpb.Node_PROJECT, image.NodeType, "the shared image must execute its row projection after filtering")
			require.Len(t, image.Children, 1)
			input := q.Nodes[image.Children[0]]
			columns := 0
			for _, col := range table.Cols {
				if col.Name != catalog.Row_ID {
					columns++
				}
			}
			require.Len(t, image.ProjectList, columns+1, "preserve table columns and the computed composite unique lock key")
			for _, expr := range image.ProjectList {
				// Remapping has consumed binding tags; executable references must
				// address real columns of the filtered input batch.
				col := expr.GetCol()
				require.NotNil(t, col, "restoring the row image must not re-evaluate expressions")
				require.Zero(t, col.RelPos)
				require.GreaterOrEqual(t, col.ColPos, int32(0))
				require.Less(t, int(col.ColPos), len(input.ProjectList))
				require.Equal(t, input.ProjectList[col.ColPos].Typ, expr.Typ)
			}
		}
		if n.NodeType == planpb.Node_MULTI_UPDATE {
			writes++
			require.True(t, reachesCheck(id), "base and index writes must read filtered rows")
		}
		if n.NodeType == planpb.Node_APPLY {
			tokenizers++
			require.True(t, reachesCheck(id), "fulltext tokenization must read filtered rows")
		}
	}
	require.Positive(t, writes)
	require.Positive(t, tokenizers)
	require.Equal(t, 1, images, "base and fulltext writes share one accepted row image")
}
