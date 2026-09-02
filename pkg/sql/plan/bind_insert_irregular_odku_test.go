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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func reachableODKUPlanNodes(query *planpb.Query) map[int32]struct{} {
	reachable := make(map[int32]struct{}, len(query.Nodes))
	var visit func(int32)
	visit = func(nodeID int32) {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) {
			return
		}
		if _, ok := reachable[nodeID]; ok {
			return
		}
		reachable[nodeID] = struct{}{}
		node := query.Nodes[nodeID]
		if node == nil {
			return
		}
		for _, childID := range node.Children {
			visit(childID)
		}
		for _, sourceStep := range node.SourceStep {
			if sourceStep < 0 || int(sourceStep) >= len(query.Steps) {
				continue
			}
			visit(query.Steps[sourceStep])
		}
	}
	for _, stepRoot := range query.Steps {
		visit(stepRoot)
	}
	return reachable
}

func fulltextODKUPlanShape(t *testing.T, sql string) (hiddenScans map[string]int, tokenizers, newRowsOnlyFilters int) {
	t.Helper()
	logicPlan, err := runOneStmt(NewMockOptimizer(true), t, sql)
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotNil(t, query)

	hiddenScans = make(map[string]int)
	reachable := reachableODKUPlanNodes(query)
	for nodeID, node := range query.Nodes {
		if _, ok := reachable[int32(nodeID)]; !ok || node == nil {
			continue
		}
		switch node.NodeType {
		case planpb.Node_TABLE_SCAN:
			if node.ObjRef != nil && strings.HasPrefix(node.ObjRef.ObjName, catalog.FullTextIndexTableNamePrefix) {
				hiddenScans[node.ObjRef.ObjName]++
			}
		case planpb.Node_FUNCTION_SCAN:
			if node.TableDef != nil && node.TableDef.TblFunc != nil && node.TableDef.TblFunc.Name == "fulltext_index_tokenize" {
				tokenizers++
			}
		case planpb.Node_FILTER:
			if len(node.FilterList) != 1 || len(node.Children) != 1 ||
				node.Children[0] < 0 || int(node.Children[0]) >= len(query.Nodes) ||
				query.Nodes[node.Children[0]] == nil || query.Nodes[node.Children[0]].NodeType != planpb.Node_SINK_SCAN {
				continue
			}
			fn := node.FilterList[0].GetF()
			if fn != nil && fn.Func.ObjName == "isnull" && len(fn.Args) == 1 &&
				fn.Args[0].Typ.Id == int32(types.T_Rowid) {
				newRowsOnlyFilters++
			}
		}
	}
	return
}

func TestOnDuplicateIrregularMaintenanceUsesOnlyEligibleRows(t *testing.T) {
	t.Run("non-indexed update keeps only new rows", func(t *testing.T) {
		hiddenScans, tokenizers, newRowsOnlyFilters := fulltextODKUPlanShape(t,
			"insert into constraint_test.docs_ft(id, body, payload, embedding) values (1, 'incoming', 1, '[1,2,3]'), (2, 'new', 2, '[4,5,6]') on duplicate key update payload = values(payload)")
		require.Empty(t, hiddenScans, "unchanged fulltext postings must not be scanned or deleted")
		require.Equal(t, 1, tokenizers, "new rows in a mixed ODKU batch still need fulltext entries")
		require.Equal(t, 1, newRowsOnlyFilters, "fulltext insertion must receive only non-conflicting rows")
	})

	t.Run("indexed update keeps delete and rebuild", func(t *testing.T) {
		hiddenScans, tokenizers, newRowsOnlyFilters := fulltextODKUPlanShape(t,
			"insert into constraint_test.docs_ft(id, body, payload, embedding) values (1, 'incoming', 1, '[1,2,3]') on duplicate key update body = values(body)")
		require.Equal(t, 1, hiddenScans[catalog.FullTextIndexTableNamePrefix+"docs_ft_body"])
		require.Equal(t, 1, tokenizers)
		require.Zero(t, newRowsOnlyFilters)
	})

	t.Run("independent fulltext indexes use separate modes", func(t *testing.T) {
		hiddenScans, tokenizers, newRowsOnlyFilters := fulltextODKUPlanShape(t,
			"insert into constraint_test.docs_ft_dual(id, body, summary, payload) values (1, 'new body', 'same summary', 1), (2, 'fresh body', 'fresh summary', 2) on duplicate key update body = values(body)")
		require.Equal(t, 1, hiddenScans[catalog.FullTextIndexTableNamePrefix+"docs_ft_dual_body"])
		require.Zero(t, hiddenScans[catalog.FullTextIndexTableNamePrefix+"docs_ft_dual_summary"])
		require.Equal(t, 2, tokenizers, "affected and insert-only indexes both keep their insert leaf")
		require.Equal(t, 1, newRowsOnlyFilters, "all insert-only groups share one filtered source")
	})

	t.Run("plain insert with regular index keeps plain irregular source", func(t *testing.T) {
		hiddenScans, tokenizers, newRowsOnlyFilters := fulltextODKUPlanShape(t,
			"insert into constraint_test.docs_ft_dual(id, body, summary, payload) values (1, 'body', 'summary', 1)")
		require.Empty(t, hiddenScans)
		require.Equal(t, 2, tokenizers)
		require.Zero(t, newRowsOnlyFilters)
	})

	t.Run("raw vector without ANN index has no irregular maintenance", func(t *testing.T) {
		logicPlan, err := runOneStmt(NewMockOptimizer(true), t,
			"insert into constraint_test.docs_vec_raw(id, embedding, payload) values (1, '[1,2,3]', 1) on duplicate key update payload = values(payload)")
		require.NoError(t, err)
		for _, node := range logicPlan.GetQuery().Nodes {
			require.NotEqual(t, planpb.Node_FUNCTION_SCAN, node.NodeType)
			if node.NodeType == planpb.Node_TABLE_SCAN {
				require.Equal(t, "docs_vec_raw", node.ObjRef.ObjName)
			}
		}
	})

	t.Run("async fulltext has no orphan insert-only branch", func(t *testing.T) {
		hiddenScans, tokenizers, newRowsOnlyFilters := fulltextODKUPlanShape(t,
			"insert into constraint_test.docs_ft_async(id, body, payload) values (1, 'incoming', 1), (2, 'new', 2) on duplicate key update payload = values(payload)")
		require.Empty(t, hiddenScans)
		require.Zero(t, tokenizers)
		require.Zero(t, newRowsOnlyFilters, "CDC-only fulltext must not get a filtered source with no inline consumer")
	})
}

func TestSplitIrregularIndexesKeepsLogicalIndexGroupsTogether(t *testing.T) {
	ftBody := &planpb.IndexDef{IndexName: "ft_multi", IndexTableName: "ft_hidden", Parts: []string{"body"}}
	ftTitle := &planpb.IndexDef{IndexName: "ft_multi", IndexTableName: "ft_hidden", Parts: []string{"title"}}
	ftOther := &planpb.IndexDef{IndexName: "ft_other", IndexTableName: "ft_other_hidden", Parts: []string{"summary"}}
	ivfEntries := &planpb.IndexDef{IndexName: "vec_idx", IndexTableName: "vec_entries", Parts: []string{catalog.CreateAlias("embedding")}}
	ivfMetadata := &planpb.IndexDef{IndexName: "vec_idx", IndexTableName: "vec_meta"}

	affected, insertOnly, err := splitIrregularIndexesByUpdatedColumns(
		nil,
		[]*planpb.IndexDef{ftBody, ftTitle, ftOther, ivfEntries, ivfMetadata},
		map[string]*planpb.Expr{
			"title":     {},
			"embedding": {},
		},
	)
	require.NoError(t, err)

	require.Equal(t, []*planpb.IndexDef{ftBody, ftTitle, ivfEntries, ivfMetadata}, affected)
	require.Equal(t, []*planpb.IndexDef{ftOther}, insertOnly)

	t.Run("final implicit update and generated columns are affected", func(t *testing.T) {
		onUpdate := &planpb.IndexDef{IndexName: "ft_updated", Parts: []string{"updated_text"}}
		generated := &planpb.IndexDef{IndexName: "ft_generated", Parts: []string{"search_text"}}
		tableDef := &planpb.TableDef{
			Cols: []*planpb.ColDef{
				{Name: "updated_text", OnUpdate: &planpb.OnUpdate{}},
				{Name: "search_text"},
			},
			Name2ColIndex: map[string]int32{"updated_text": 0, "search_text": 1},
		}
		affected, insertOnly, err := splitIrregularIndexesByUpdatedColumns(
			tableDef,
			[]*planpb.IndexDef{onUpdate, generated},
			// Generated columns are present in the final ODKU update map, while
			// ON UPDATE columns are discovered from the table definition.
			map[string]*planpb.Expr{"search_text": {}},
		)
		require.NoError(t, err)
		require.Equal(t, []*planpb.IndexDef{onUpdate, generated}, affected)
		require.Empty(t, insertOnly)
	})

	t.Run("included column affects the whole vector index group", func(t *testing.T) {
		entries := &planpb.IndexDef{
			IndexName:       "vec_include",
			IndexAlgo:       catalog.MoIndexIvfFlatAlgo.ToString(),
			IndexTableName:  "vec_entries",
			Parts:           []string{"embedding"},
			IncludedColumns: []string{"title"},
		}
		metadata := &planpb.IndexDef{
			IndexName:      "vec_include",
			IndexAlgo:      catalog.MoIndexIvfFlatAlgo.ToString(),
			IndexTableName: "vec_metadata",
		}
		affected, insertOnly, err := splitIrregularIndexesByUpdatedColumns(
			nil,
			[]*planpb.IndexDef{entries, metadata},
			map[string]*planpb.Expr{"title": {}},
		)
		require.NoError(t, err)
		require.Equal(t, []*planpb.IndexDef{entries, metadata}, affected)
		require.Empty(t, insertOnly)
	})

	t.Run("async indexes do not create inline maintenance", func(t *testing.T) {
		async := &planpb.IndexDef{
			IndexName:       "vec_async",
			IndexAlgo:       catalog.MoIndexIvfFlatAlgo.ToString(),
			IndexAlgoParams: `{"async":"true"}`,
			IndexTableName:  "vec_async_entries",
			Parts:           []string{"embedding"},
		}
		asyncMetadata := &planpb.IndexDef{
			IndexName:      "vec_async",
			IndexAlgo:      catalog.MoIndexIvfFlatAlgo.ToString(),
			IndexTableName: "vec_async_metadata",
		}
		affected, insertOnly, err := splitIrregularIndexesByUpdatedColumns(
			nil,
			[]*planpb.IndexDef{async, asyncMetadata},
			map[string]*planpb.Expr{"payload": {}},
		)
		require.NoError(t, err)
		require.Empty(t, affected)
		require.Empty(t, insertOnly)
	})
}

func TestReduceSinkSinkScanKeepsOtherApplyInput(t *testing.T) {
	query := &planpb.Query{
		Nodes: []*planpb.Node{
			{NodeType: planpb.Node_VALUE_SCAN},                        // 0: sink input
			{NodeType: planpb.Node_SINK, Children: []int32{0}},        // 1: materialized step
			{NodeType: planpb.Node_SINK_SCAN, SourceStep: []int32{0}}, // 2: APPLY left input
			{NodeType: planpb.Node_FUNCTION_SCAN},                     // 3: APPLY right input
			{NodeType: planpb.Node_APPLY, Children: []int32{2, 3}},    // 4: consumer step
		},
		Steps: []int32{1, 4},
	}

	reduceSinkSinkScanNodes(query)

	require.Equal(t, []int32{1, 4}, query.Steps)
	require.Equal(t, []int32{2, 3}, query.Nodes[4].Children,
		"sink reduction must not replace both APPLY inputs with the sink input")
}
