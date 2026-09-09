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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	fulltextplan "github.com/matrixorigin/matrixone/pkg/fulltext/plugin/plan"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

type odkuNoOpTestPlanHooks struct {
	fulltextplan.Hooks
	columns   []string
	supported bool
	err       error
}

func (h odkuNoOpTestPlanHooks) DMLMaintenanceNoOpColumns(*planpb.TableDef, *planpb.IndexDef) ([]string, bool, error) {
	return h.columns, h.supported, h.err
}

type odkuNoOpTestPlugin struct {
	algo      string
	planHooks planplugin.Hooks
}

func (p odkuNoOpTestPlugin) Algo() string               { return p.algo }
func (odkuNoOpTestPlugin) Catalog() catalogplugin.Hooks { return nil }
func (odkuNoOpTestPlugin) Compile() compileplugin.Hooks { return nil }
func (p odkuNoOpTestPlugin) Plan() planplugin.Hooks     { return p.planHooks }
func (odkuNoOpTestPlugin) Idxcron() idxcronplugin.Hooks { return nil }

func scopedODKUPluginResolver(plugins ...indexplugin.AlgoPlugin) func(string) (indexplugin.AlgoPlugin, bool) {
	byAlgo := make(map[string]indexplugin.AlgoPlugin, len(plugins))
	for _, plugin := range plugins {
		byAlgo[strings.ToLower(strings.TrimSpace(plugin.Algo()))] = plugin
	}
	return func(algo string) (indexplugin.AlgoPlugin, bool) {
		if plugin, ok := byAlgo[strings.ToLower(strings.TrimSpace(algo))]; ok {
			return plugin, true
		}
		return indexplugin.Get(algo)
	}
}

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

func exprContainsODKUFunc(expr *planpb.Expr, name string) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil {
		return false
	}
	if fn.Func.ObjName == name {
		return true
	}
	for _, arg := range fn.Args {
		if exprContainsODKUFunc(arg, name) {
			return true
		}
	}
	return false
}

type odkuValueChangeFilter struct {
	outputStep int32
	markerExpr *planpb.Expr
}

type fulltextODKUShape struct {
	hiddenScans       map[string]int
	tokenizers        int
	newRowsOnlyFilter int
	valueChangeFilter []odkuValueChangeFilter
}

func inspectFulltextODKUPlan(t *testing.T, mock *MockOptimizer, sql string) fulltextODKUShape {
	t.Helper()
	logicPlan, err := runOneStmt(mock, t, sql)
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	assertEverySinkStepHasConsumer(t, query)

	shape := fulltextODKUShape{hiddenScans: make(map[string]int)}
	reachable := reachableODKUPlanNodes(query)
	for nodeID, node := range query.Nodes {
		if _, ok := reachable[int32(nodeID)]; !ok || node == nil {
			continue
		}
		switch node.NodeType {
		case planpb.Node_TABLE_SCAN:
			if node.ObjRef != nil && strings.HasPrefix(node.ObjRef.ObjName, catalog.FullTextIndexTableNamePrefix) {
				shape.hiddenScans[node.ObjRef.ObjName]++
			}
		case planpb.Node_FUNCTION_SCAN:
			if node.TableDef != nil && node.TableDef.TblFunc != nil && node.TableDef.TblFunc.Name == "fulltext_index_tokenize" {
				shape.tokenizers++
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
				shape.newRowsOnlyFilter++
			}
			if fn == nil || fn.Func.ObjName != "or" || len(fn.Args) != 2 ||
				!exprContainsODKUFunc(fn.Args[0], "isnull") {
				continue
			}
			markerCol := fn.Args[1].GetCol()
			require.NotNil(t, markerCol, "value-change branch must read its materialized boolean marker")
			sinkScan := query.Nodes[node.Children[0]]
			require.Len(t, sinkScan.SourceStep, 1)
			sourceStep := sinkScan.SourceStep[0]
			require.GreaterOrEqual(t, sourceStep, int32(0))
			require.Less(t, int(sourceStep), len(query.Steps))
			sinkNode := query.Nodes[query.Steps[sourceStep]]
			require.Equal(t, planpb.Node_SINK, sinkNode.NodeType)
			require.Len(t, sinkNode.Children, 1)
			sourceNode := query.Nodes[sinkNode.Children[0]]
			require.GreaterOrEqual(t, markerCol.ColPos, int32(0))
			require.Less(t, int(markerCol.ColPos), len(sourceNode.ProjectList))
			outputStep := int32(-1)
			for step, rootID := range query.Steps {
				root := query.Nodes[rootID]
				if root != nil && root.NodeType == planpb.Node_SINK && len(root.Children) == 1 && root.Children[0] == int32(nodeID) {
					outputStep = int32(step)
					break
				}
			}
			require.GreaterOrEqual(t, outputStep, int32(0), "value-change filter must materialize an independent maintenance source")
			shape.valueChangeFilter = append(shape.valueChangeFilter, odkuValueChangeFilter{
				outputStep: outputStep,
				markerExpr: sourceNode.ProjectList[markerCol.ColPos],
			})
		}
	}
	return shape
}

func fulltextODKUPlanShape(t *testing.T, sql string) fulltextODKUShape {
	t.Helper()
	return inspectFulltextODKUPlan(t, NewMockOptimizer(true), sql)
}

func nullSafeEqualityColumns(t *testing.T, marker *planpb.Expr) []string {
	t.Helper()
	require.NotNil(t, marker)
	notFn := marker.GetF()
	require.NotNil(t, notFn)
	require.Equal(t, "not", notFn.Func.ObjName)
	require.Len(t, notFn.Args, 1)

	var columns []string
	var collect func(*planpb.Expr)
	collect = func(expr *planpb.Expr) {
		fn := expr.GetF()
		require.NotNil(t, fn)
		if fn.Func.ObjName == "and" {
			require.Len(t, fn.Args, 2)
			collect(fn.Args[0])
			collect(fn.Args[1])
			return
		}
		require.Equal(t, "<=>", fn.Func.ObjName)
		require.Len(t, fn.Args, 2)
		oldCol := fn.Args[0].GetCol()
		newCol := fn.Args[1].GetCol()
		require.NotNil(t, oldCol)
		require.NotNil(t, newCol)
		require.NotEqual(t, oldCol.ColPos, newCol.ColPos,
			"the marker must compare distinct old-image and final-image columns")
		name := oldCol.Name
		if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
			name = name[dot+1:]
		}
		require.NotEmpty(t, name)
		columns = append(columns, name)
	}
	collect(notFn.Args[0])
	return columns
}

func TestOnDuplicateIrregularMaintenanceUsesOnlyEligibleRows(t *testing.T) {
	t.Run("non-indexed update keeps only new rows", func(t *testing.T) {
		shape := fulltextODKUPlanShape(t,
			"insert into constraint_test.docs_ft(id, body, payload, embedding) values (1, 'incoming', 1, '[1,2,3]'), (2, 'new', 2, '[4,5,6]') on duplicate key update payload = values(payload)")
		require.Empty(t, shape.hiddenScans, "unchanged fulltext postings must not be scanned or deleted")
		require.Equal(t, 1, shape.tokenizers, "new rows in a mixed ODKU batch still need fulltext entries")
		require.Equal(t, 1, shape.newRowsOnlyFilter, "fulltext insertion must receive only non-conflicting rows")
		require.Empty(t, shape.valueChangeFilter)
	})

	t.Run("indexed assignment guards delete and rebuild by final value", func(t *testing.T) {
		shape := fulltextODKUPlanShape(t,
			"insert into constraint_test.docs_ft(id, body, payload, embedding) values (1, 'incoming', 1, '[1,2,3]') on duplicate key update body = values(body)")
		require.Equal(t, 1, shape.hiddenScans[catalog.FullTextIndexTableNamePrefix+"docs_ft_body"])
		require.Equal(t, 1, shape.tokenizers)
		require.Zero(t, shape.newRowsOnlyFilter)
		require.Len(t, shape.valueChangeFilter, 1,
			"affected fulltext maintenance must receive only new rows or rows whose indexed value changed")
		require.Equal(t, []string{"id", "body"}, nullSafeEqualityColumns(t, shape.valueChangeFilter[0].markerExpr))
	})

	t.Run("independent fulltext indexes use separate modes", func(t *testing.T) {
		shape := fulltextODKUPlanShape(t,
			"insert into constraint_test.docs_ft_dual(id, body, summary, payload) values (1, 'new body', 'same summary', 1), (2, 'fresh body', 'fresh summary', 2) on duplicate key update body = values(body)")
		require.Equal(t, 1, shape.hiddenScans[catalog.FullTextIndexTableNamePrefix+"docs_ft_dual_body"])
		require.Zero(t, shape.hiddenScans[catalog.FullTextIndexTableNamePrefix+"docs_ft_dual_summary"])
		require.Equal(t, 2, shape.tokenizers, "affected and insert-only indexes both keep their insert leaf")
		require.Equal(t, 1, shape.newRowsOnlyFilter, "all insert-only groups share one filtered source")
		require.Len(t, shape.valueChangeFilter, 1, "only the affected fulltext index needs a value-change source")
		require.Equal(t, []string{"id", "body"}, nullSafeEqualityColumns(t, shape.valueChangeFilter[0].markerExpr))
	})

	t.Run("plain insert with regular index keeps plain irregular source", func(t *testing.T) {
		shape := fulltextODKUPlanShape(t,
			"insert into constraint_test.docs_ft_dual(id, body, summary, payload) values (1, 'body', 'summary', 1)")
		require.Empty(t, shape.hiddenScans)
		require.Equal(t, 2, shape.tokenizers)
		require.Zero(t, shape.newRowsOnlyFilter)
		require.Empty(t, shape.valueChangeFilter)
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
		shape := fulltextODKUPlanShape(t,
			"insert into constraint_test.docs_ft_async(id, body, payload) values (1, 'incoming', 1), (2, 'new', 2) on duplicate key update payload = values(payload)")
		require.Empty(t, shape.hiddenScans)
		require.Zero(t, shape.tokenizers)
		require.Zero(t, shape.newRowsOnlyFilter, "CDC-only fulltext must not get a filtered source with no inline consumer")
		require.Empty(t, shape.valueChangeFilter)
	})
}

func assertEverySinkStepHasConsumer(t *testing.T, query *planpb.Query) {
	t.Helper()
	consumerCount := make([]int, len(query.Steps))
	for _, node := range query.Nodes {
		for _, sourceStep := range node.SourceStep {
			if sourceStep >= 0 && int(sourceStep) < len(consumerCount) {
				consumerCount[sourceStep]++
			}
		}
	}
	for step, rootID := range query.Steps {
		if query.Nodes[rootID].NodeType == planpb.Node_SINK {
			require.Positive(t, consumerCount[step], "sink step %d must have a receiver", step)
		}
	}
}

func TestOnDuplicateIrregularMaintenanceBuildsPerIndexValueMarkers(t *testing.T) {
	t.Run("multi-column fulltext compares every indexed value", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		base := mock.ctxt.tables["docs_ft_dual"]
		require.NotNil(t, base)
		base.Indexes = []*planpb.IndexDef{base.Indexes[0]}
		base.Indexes[0].IndexName = "ft_body_summary"
		base.Indexes[0].Parts = []string{catalog.CreateAlias("body"), "summary"}

		shape := inspectFulltextODKUPlan(t, mock,
			"insert into constraint_test.docs_ft_dual(id, body, summary, payload) values (1, 'new body', 'new summary', 1) "+
				"on duplicate key update body = values(body), summary = values(summary)")
		require.Len(t, shape.valueChangeFilter, 1)
		require.Equal(t, []string{"id", "body", "summary"},
			nullSafeEqualityColumns(t, shape.valueChangeFilter[0].markerExpr))
	})

	t.Run("independent fulltext indexes keep independent markers", func(t *testing.T) {
		shape := fulltextODKUPlanShape(t,
			"insert into constraint_test.docs_ft_dual(id, body, summary, payload) values (1, 'new body', 'new summary', 1) "+
				"on duplicate key update body = values(body), summary = values(summary)")
		require.Len(t, shape.valueChangeFilter, 2)
		require.NotEqual(t, shape.valueChangeFilter[0].outputStep, shape.valueChangeFilter[1].outputStep)
		columns := []string{
			strings.Join(nullSafeEqualityColumns(t, shape.valueChangeFilter[0].markerExpr), ","),
			strings.Join(nullSafeEqualityColumns(t, shape.valueChangeFilter[1].markerExpr), ","),
		}
		require.ElementsMatch(t, []string{"id,body", "id,summary"}, columns)
	})
}

func configureMockGeneratedFulltext(t *testing.T, mock *MockOptimizer) {
	t.Helper()
	base := mock.ctxt.tables["docs_ft_dual"]
	require.NotNil(t, base)
	bodyPos := mockTableColPos(t, base, "body")
	summaryPos := mockTableColPos(t, base, "summary")
	base.Cols[bodyPos].GeneratedCol = &planpb.GeneratedCol{
		Expr:     generatedColumnRefExpr(base.Cols[bodyPos].Typ, summaryPos, "summary"),
		IsStored: true,
	}
	base.Indexes = []*planpb.IndexDef{base.Indexes[0]}
}

func TestOnDuplicateGeneratedFulltextUsesDependencyClosure(t *testing.T) {
	t.Run("unrelated update remains insert only", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		configureMockGeneratedFulltext(t, mock)
		shape := inspectFulltextODKUPlan(t, mock,
			"insert into constraint_test.docs_ft_dual(id, summary, payload) values (1, 'same summary', 1) "+
				"on duplicate key update payload = values(payload)")
		require.Empty(t, shape.hiddenScans)
		require.Equal(t, 1, shape.newRowsOnlyFilter)
		require.Empty(t, shape.valueChangeFilter,
			"recomputing every generated expression must not make an unrelated generated fulltext index affected")
	})

	t.Run("source update compares the final generated value", func(t *testing.T) {
		mock := NewMockOptimizer(true)
		configureMockGeneratedFulltext(t, mock)
		shape := inspectFulltextODKUPlan(t, mock,
			"insert into constraint_test.docs_ft_dual(id, summary, payload) values (1, 'new summary', 1) "+
				"on duplicate key update summary = values(summary)")
		require.Equal(t, 1, shape.hiddenScans[catalog.FullTextIndexTableNamePrefix+"docs_ft_dual_body"])
		require.Len(t, shape.valueChangeFilter, 1)
		require.Equal(t, []string{"id", "body"}, nullSafeEqualityColumns(t, shape.valueChangeFilter[0].markerExpr))
	})
}

func TestNonODKUIrregularMaintenanceHasNoValueMarker(t *testing.T) {
	for _, sql := range []string{
		"update constraint_test.docs_ft set body = 'changed' where id = 1",
		"replace into constraint_test.docs_ft(id, body, payload, embedding) values (1, 'changed', 1, '[1,2,3]')",
		"insert all into docs_ft (id, body) values (deptno, dname) into docs_ft (id, body) values (deptno + 10, loc) select deptno, dname, loc from dept",
	} {
		shape := fulltextODKUPlanShape(t, sql)
		require.Empty(t, shape.valueChangeFilter, sql)
	}
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
		map[string]struct{}{
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
			map[string]struct{}{"updated_text": {}, "search_text": {}},
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
			map[string]struct{}{"title": {}},
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
			map[string]struct{}{"payload": {}},
		)
		require.NoError(t, err)
		require.Empty(t, affected)
		require.Empty(t, insertOnly)
	})
}

func TestBuildIrregularIndexValueChangeFilters(t *testing.T) {
	tableDef := &planpb.TableDef{
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "body", Typ: planpb.Type{Id: int32(types.T_text)}},
			{Name: "summary", Typ: planpb.Type{Id: int32(types.T_varchar)}},
			{Name: "resource", Typ: planpb.Type{Id: int32(types.T_datalink)}},
		},
		Name2ColIndex: map[string]int32{"id": 0, "body": 1, "summary": 2, "resource": 3},
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: "id",
			Names:       []string{"id"},
		},
	}

	t.Run("groups physical definitions and resolves aliases", func(t *testing.T) {
		filters, err := buildIrregularIndexValueChangeFilters(tableDef, []*planpb.IndexDef{
			{
				IndexName: "FT_TEXT", IndexAlgo: catalog.MOIndexFullTextAlgo.ToString(),
				Parts: []string{catalog.CreateAlias("summary"), "summary"},
			},
			{
				IndexName: "ft_text", IndexAlgo: catalog.MOIndexFullTextAlgo.ToString(),
				Parts: []string{"body"},
			},
		})
		require.NoError(t, err)
		require.Equal(t, []irregularIndexValueChangeFilter{
			{groupKey: "ft_text", columns: []string{"id", "body", "summary"}},
		}, filters)
	})

	t.Run("one unsupported physical part keeps the whole group conservative", func(t *testing.T) {
		filters, err := buildIrregularIndexValueChangeFilters(tableDef, []*planpb.IndexDef{
			{IndexName: "ft_mixed", IndexAlgo: catalog.MOIndexFullTextAlgo.ToString(), Parts: []string{"body"}},
			{IndexName: "ft_mixed", IndexAlgo: catalog.MOIndexFullTextAlgo.ToString(), Parts: []string{"resource"}},
		})
		require.NoError(t, err)
		require.Empty(t, filters)
	})

	t.Run("empty logical names stay isolated by physical identity", func(t *testing.T) {
		filters, err := buildIrregularIndexValueChangeFilters(tableDef, []*planpb.IndexDef{
			{IndexAlgo: catalog.MOIndexFullTextAlgo.ToString(), IndexTableName: "hidden_a", Parts: []string{"body"}},
			{IndexAlgo: catalog.MOIndexFullTextAlgo.ToString(), IndexTableName: "hidden_b", Parts: []string{"summary"}},
		})
		require.NoError(t, err)
		require.Equal(t, []irregularIndexValueChangeFilter{
			{groupKey: catalog.MOIndexFullTextAlgo.ToString() + "\x00hidden_a", columns: []string{"id", "body"}},
			{groupKey: catalog.MOIndexFullTextAlgo.ToString() + "\x00hidden_b", columns: []string{"id", "summary"}},
		}, filters)
	})

	t.Run("missing plugin and plugin without hook are conservative", func(t *testing.T) {
		const noHookAlgo = "odku_noop_test_no_hook"
		filters, err := buildIrregularIndexValueChangeFiltersWithResolver(tableDef, []*planpb.IndexDef{
			{IndexName: "missing", IndexAlgo: "odku_noop_test_missing", Parts: []string{"body"}},
			{IndexName: "no_hook", IndexAlgo: noHookAlgo, Parts: []string{"body"}},
			{IndexName: "fulltext2", IndexAlgo: catalog.MoIndexFullText2Algo.ToString(), Parts: []string{"body"}},
		}, scopedODKUPluginResolver(odkuNoOpTestPlugin{algo: noHookAlgo}))
		require.NoError(t, err)
		require.Empty(t, filters)
	})

	t.Run("unresolvable proof column is conservative", func(t *testing.T) {
		const unresolvedAlgo = "odku_noop_test_unresolved"
		filters, err := buildIrregularIndexValueChangeFiltersWithResolver(tableDef, []*planpb.IndexDef{
			{IndexName: "unresolved", IndexAlgo: unresolvedAlgo, Parts: []string{"body"}},
		}, scopedODKUPluginResolver(odkuNoOpTestPlugin{
			algo: unresolvedAlgo,
			planHooks: odkuNoOpTestPlanHooks{
				columns: []string{"missing"}, supported: true,
			},
		}))
		require.NoError(t, err)
		require.Empty(t, filters)
	})

	t.Run("hook error fails closed", func(t *testing.T) {
		const errorAlgo = "odku_noop_test_error"
		_, err := buildIrregularIndexValueChangeFiltersWithResolver(tableDef, []*planpb.IndexDef{
			{IndexName: "error", IndexAlgo: errorAlgo, Parts: []string{"body"}},
		}, scopedODKUPluginResolver(odkuNoOpTestPlugin{
			algo: errorAlgo,
			planHooks: odkuNoOpTestPlanHooks{
				err: moerr.NewInternalErrorNoCtx("proof failed"),
			},
		}))
		require.ErrorContains(t, err, "proof failed")
	})
}

func TestOnDuplicateIrregularValueMarkerRejectsInvalidPositions(t *testing.T) {
	for _, tc := range []struct {
		name            string
		newRowMarkerPos int32
		valueMarkerPos  int32
	}{
		{name: "missing old row marker", newRowMarkerPos: -1, valueMarkerPos: 1},
		{name: "missing value marker", newRowMarkerPos: 0, valueMarkerPos: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewQueryBuilder(planpb.Query_INSERT, NewMockCompilerContext(true), false, true)
			bindCtx := NewBindContext(builder, nil)
			finalProjTag := builder.genNewBindTag()
			finalProjID := builder.appendNode(&planpb.Node{
				NodeType: planpb.Node_VALUE_SCAN,
				ProjectList: []*planpb.Expr{
					{Typ: planpb.Type{Id: int32(types.T_Rowid)}},
					{Typ: planpb.Type{Id: int32(types.T_bool)}},
				},
				BindingTags: []int32{finalProjTag},
			}, bindCtx)

			_, err := builder.appendOnDupIrregularMaintSource(
				bindCtx, finalProjID, finalProjTag, 0, planpb.Type{}, -1, -1,
				-1, nil, nil, tc.newRowMarkerPos, map[string]int32{"ft": tc.valueMarkerPos},
				&planpb.TableDef{}, nil,
			)
			require.ErrorContains(t, err, "cannot locate an irregular index value-change marker")
		})
	}
}

func TestDeletePkColExprUsesSourceSpecificPruneMap(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_INSERT, NewMockCompilerContext(true), false, true)
	builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
		NodeType:    planpb.Node_SINK,
		ProjectList: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_int64)}}},
	})
	builder.qry.Steps = append(builder.qry.Steps, 0)
	builder.irregularMaintDeletePkPos = 2
	builder.irregularMaintDeletePkTyp = planpb.Type{Id: int32(types.T_int64)}
	builder.sinkColRef = map[[2]int32]int{{0, 2}: 0}

	expr := builder.deletePkColExpr(7, 0)
	require.Equal(t, int32(types.T_int64), expr.Typ.Id)
	require.Equal(t, int32(7), expr.GetCol().RelPos)
	require.Equal(t, int32(0), expr.GetCol().ColPos)
}

func TestIrregularIVFMaintenancePropagatesInvalidParams(t *testing.T) {
	indexDef := &planpb.IndexDef{
		IndexName:          "vec_idx",
		IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
		IndexAlgoParams:    `{"async":123}`,
		IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
		IndexTableName:     "vec_entries",
		TableExist:         true,
	}
	tableDef := &planpb.TableDef{Indexes: []*planpb.IndexDef{indexDef}}

	t.Run("insert", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_INSERT, NewMockCompilerContext(true), false, true)
		bindCtx := NewBindContext(builder, nil)
		err := builder.buildIrregularIndexInsertMaintenance(bindCtx, 0, tableDef)
		require.Error(t, err)
	})

	t.Run("delete", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_INSERT, NewMockCompilerContext(true), false, true)
		bindCtx := NewBindContext(builder, nil)
		builder.irregularMaintTableDef = tableDef
		builder.irregularMaintDeleteStep = 0
		err := builder.buildIrregularIndexDeleteMaintenance(bindCtx)
		require.Error(t, err)
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
