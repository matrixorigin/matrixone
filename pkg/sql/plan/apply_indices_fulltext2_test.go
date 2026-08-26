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
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestFulltext2ParserFromParams(t *testing.T) {
	require.Equal(t, "", fulltext2ParserFromParams(""))
	require.Equal(t, "ngram", fulltext2ParserFromParams(`{"parser":"ngram"}`))
	require.Equal(t, "gojieba", fulltext2ParserFromParams(`{"parser":"gojieba","position_free":"true"}`))
	require.Equal(t, "", fulltext2ParserFromParams(`{no json`)) // malformed → default
	require.Equal(t, "", fulltext2ParserFromParams(`{"other":"x"}`))
}

func TestFulltext2PositionFreeFromParams(t *testing.T) {
	require.False(t, fulltext2PositionFreeFromParams(""))
	require.True(t, fulltext2PositionFreeFromParams(`{"position_free":"true"}`))
	require.False(t, fulltext2PositionFreeFromParams(`{"position_free":"false"}`))
	require.False(t, fulltext2PositionFreeFromParams(`{bad json`))
	require.False(t, fulltext2PositionFreeFromParams(`{"parser":"ngram"}`))
}

func ft2ScanNode(indexName, storeTbl, metaTbl string) *plan.Node {
	return &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Indexes: []*plan.IndexDef{
				{IndexName: indexName, IndexAlgoTableType: catalog.FullText2Index_TblType_Storage, IndexTableName: storeTbl},
				{IndexName: indexName, IndexAlgoTableType: catalog.FullText2Index_TblType_Metadata, IndexTableName: metaTbl},
			},
		},
	}
}

func TestFindFulltext2IndexTables(t *testing.T) {
	b := &QueryBuilder{}
	node := ft2ScanNode("ft2idx", "__store", "__meta")
	idxdef := &plan.IndexDef{IndexName: "ft2idx"}

	store, meta, ok := b.findFulltext2IndexTables(node, idxdef)
	require.True(t, ok)
	require.Equal(t, "__store", store)
	require.Equal(t, "__meta", meta)

	// nil guards.
	_, _, ok = b.findFulltext2IndexTables(nil, idxdef)
	require.False(t, ok)
	_, _, ok = b.findFulltext2IndexTables(node, nil)
	require.False(t, ok)

	// only the storage sibling present → incomplete → ok=false.
	partial := &plan.Node{TableDef: &plan.TableDef{Indexes: []*plan.IndexDef{
		{IndexName: "ft2idx", IndexAlgoTableType: catalog.FullText2Index_TblType_Storage, IndexTableName: "__store"},
	}}}
	_, _, ok = b.findFulltext2IndexTables(partial, idxdef)
	require.False(t, ok)
}

func TestBuildFulltext2SearchCfg(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	node := ft2ScanNode("ft2idx", "__store", "__meta")

	// positional index, NL mode → cfg JSON with parser.
	idxdef := &plan.IndexDef{IndexName: "ft2idx", IndexAlgoParams: `{"parser":"ngram"}`}
	cfg, err := b.buildFulltext2SearchCfg(node, idxdef, int64(tree.FULLTEXT_NL))
	require.NoError(t, err)
	var m map[string]string
	require.NoError(t, json.Unmarshal([]byte(cfg), &m))
	require.Equal(t, "db", m["db"])
	require.Equal(t, "__store", m["index"])
	require.Equal(t, "__meta", m["metadata"])
	require.Equal(t, "ngram", m["parser"])

	// tables not found → error.
	_, err = b.buildFulltext2SearchCfg(node, &plan.IndexDef{IndexName: "missing"}, int64(tree.FULLTEXT_NL))
	require.ErrorContains(t, err, "not found")

	// POSITION_FREE index + non-BM25 mode → rejected.
	pf := &plan.IndexDef{IndexName: "ft2idx", IndexAlgoParams: `{"position_free":"true"}`}
	_, err = b.buildFulltext2SearchCfg(node, pf, int64(tree.FULLTEXT_NL))
	require.ErrorContains(t, err, "POSITION_FREE")

	// POSITION_FREE index + BM25 mode → allowed.
	cfg, err = b.buildFulltext2SearchCfg(node, pf, int64(tree.FULLTEXT_BM25))
	require.NoError(t, err)
	require.Contains(t, cfg, "__store")
}

func fulltext2MatchFilter(fn *plan.Function) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: fn},
	}
}

func fulltext2ColumnExpr(tag, pos int32, name string, typ plan.Type) *plan.Expr {
	return &plan.Expr{
		Typ: typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: tag,
			ColPos: pos,
			Name:   name,
		}},
	}
}

func newFulltext2CoveredFixture(t *testing.T, withIncludeFilter, withSort bool) (*QueryBuilder, int32, *plan.Node, *plan.Node, *plan.IndexDef) {
	t.Helper()

	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(builder, nil)
	tableDef := &plan.TableDef{
		Name: "docs",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
			{Name: "body", Typ: plan.Type{Id: int32(types.T_text)}},
			{Name: "title", Typ: plan.Type{Id: int32(types.T_varchar), Width: 128}},
		},
		Name2ColIndex: map[string]int32{"id": 0, "body": 1, "title": 2},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
	}
	idxdef := &plan.IndexDef{
		IndexName:          "ft2_docs",
		IndexAlgo:          catalog.MoIndexFullText2Algo.ToString(),
		IndexAlgoTableType: catalog.FullText2Index_TblType_Storage,
		IndexTableName:     "__ft2_docs_store",
		IndexAlgoParams:    `{"parser":"ngram"}`,
		IncludedColumns:    []string{"title"},
		Parts:              []string{"body"},
		TableExist:         true,
	}
	metaDef := &plan.IndexDef{
		IndexName:          idxdef.IndexName,
		IndexAlgo:          idxdef.IndexAlgo,
		IndexAlgoTableType: catalog.FullText2Index_TblType_Metadata,
		IndexTableName:     "__ft2_docs_meta",
		IndexAlgoParams:    idxdef.IndexAlgoParams,
		TableExist:         true,
	}
	tableDef.Indexes = []*plan.IndexDef{idxdef, metaDef}

	tag := builder.genNewBindTag()
	match := ft2TestMatch("hello", 0, tag, "body")
	filters := []*plan.Expr{fulltext2MatchFilter(match)}
	if withIncludeFilter {
		filters = append(filters, &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "="},
				Args: []*plan.Expr{
					fulltext2ColumnExpr(tag, 0, "id", tableDef.Cols[0].Typ),
					makePlan2Int64ConstExprWithType(7),
				},
			}},
		})
	}
	scan := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    tableDef,
		ObjRef:      &plan.ObjectRef{SchemaName: "db", ObjName: "docs"},
		BindingTags: []int32{tag},
		FilterList:  filters,
		Stats:       &plan.Stats{},
	}
	scanID := builder.appendNode(scan, ctx)

	project := &plan.Node{
		NodeType: plan.Node_PROJECT,
		ProjectList: []*plan.Expr{
			fulltext2ColumnExpr(tag, 0, "id", tableDef.Cols[0].Typ),
			fulltext2MatchFilter(match),
			fulltext2ColumnExpr(tag, 2, "title", tableDef.Cols[2].Typ),
		},
	}
	if withSort {
		sort := &plan.Node{NodeType: plan.Node_SORT, Children: []int32{scanID}, Stats: &plan.Stats{}}
		sortID := builder.appendNode(sort, ctx)
		project.Children = []int32{sortID}
	} else {
		project.Children = []int32{scanID}
	}
	projectID := builder.appendNode(project, ctx)
	return builder, projectID, project, scan, idxdef
}

func TestFulltext2CoveredProjectionUsesTVFOutputs(t *testing.T) {
	for _, tc := range []struct {
		name              string
		withIncludeFilter bool
		withSort          bool
		wantArgs          int
	}{
		{name: "implicit score sort", wantArgs: 3},
		{name: "include prefilter and explicit sort", withIncludeFilter: true, withSort: true, wantArgs: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder, projectID, project, scan, idxdef := newFulltext2CoveredFixture(t, tc.withIncludeFilter, tc.withSort)
			filterIDs, filterDefs := builder.getFullTextMatchFiltersFromScanNode(scan)
			require.Equal(t, []int32{0}, filterIDs)
			require.Len(t, filterDefs, 1)
			projectIDs, projectDefs := builder.getFullTextMatchFromProject(project, scan)
			require.Equal(t, []int32{1}, projectIDs)
			require.Len(t, projectDefs, 1)
			var sortNode *plan.Node
			if tc.withSort {
				sortNode = builder.qry.Nodes[project.Children[0]]
			}
			if tc.withIncludeFilter {
				_, _, residual, err := buildFilterPredicateJSON(scan.FilterList[1:], scan, []string{"title"}, "id", true)
				require.NoError(t, err)
				require.Empty(t, residual)
			}

			newID, err := builder.applyIndicesForProjectionUsingFullTextIndex(
				projectID, project, sortNode, scan,
				[]int32{0}, []*plan.IndexDef{idxdef},
				[]int32{1}, []*plan.IndexDef{idxdef},
				nil, nil, nil, nil,
			)
			require.NoError(t, err)
			require.Equal(t, projectID, newID)
			require.Empty(t, scan.FilterList)

			if sortNode == nil {
				sortNode = builder.qry.Nodes[project.Children[0]]
			}
			require.Equal(t, plan.Node_SORT, sortNode.NodeType)
			search := builder.qry.Nodes[sortNode.Children[0]]
			require.Equal(t, plan.Node_FUNCTION_SCAN, search.NodeType)
			require.Equal(t, fulltext2_search_func_name, search.TableDef.TblFunc.Name)
			require.Len(t, search.TblFuncExprList, tc.wantArgs)
			require.Equal(t, scan.TableDef.Cols[0].Typ.Id, search.TableDef.Cols[0].Typ.Id)

			pk := project.ProjectList[0].GetCol()
			require.NotNil(t, pk)
			require.Equal(t, search.BindingTags[0], pk.RelPos)
			require.Equal(t, int32(0), pk.ColPos)
			score := project.ProjectList[1].GetCol()
			require.NotNil(t, score)
			require.Equal(t, search.BindingTags[0], score.RelPos)
			require.Equal(t, int32(1), score.ColPos)
			included := project.ProjectList[2].GetCol()
			require.NotNil(t, included)
			require.Equal(t, search.BindingTags[0], included.RelPos)
			require.Equal(t, int32(2), included.ColPos)
			if tc.withIncludeFilter {
				require.Contains(t, search.TblFuncExprList[3].GetLit().GetSval(), `"col":-1`)
			}
		})
	}
}

func TestFulltext2CoveredProjectionDeclinesUncoveredColumn(t *testing.T) {
	builder, projectID, project, scan, idxdef := newFulltext2CoveredFixture(t, false, false)
	project.ProjectList = append(project.ProjectList, fulltext2ColumnExpr(scan.BindingTags[0], 1, "body", scan.TableDef.Cols[1].Typ))

	newID, err := builder.applyIndicesForProjectionUsingFullTextIndex(
		projectID, project, nil, scan,
		[]int32{0}, []*plan.IndexDef{idxdef},
		[]int32{1}, []*plan.IndexDef{idxdef},
		nil, nil, nil, nil,
	)
	require.NoError(t, err)
	require.Equal(t, projectID, newID)
	sort := builder.qry.Nodes[project.Children[0]]
	require.Equal(t, plan.Node_SORT, sort.NodeType)
	join := builder.qry.Nodes[sort.Children[0]]
	require.Equal(t, plan.Node_JOIN, join.NodeType)
	require.Equal(t, plan.Node_TABLE_SCAN, builder.qry.Nodes[join.Children[0]].NodeType)
	require.Equal(t, plan.Node_FUNCTION_SCAN, builder.qry.Nodes[join.Children[1]].NodeType)
}

func fulltext2ComparisonExpr(op string, left, right *plan.Expr) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: op},
			Args: []*plan.Expr{left, right},
		}},
	}
}

func TestFulltext2MatchShapeHelpers(t *testing.T) {
	match := ft2TestMatch("hello", 0, 1, "body")
	matchExpr := ft2TestExpr(match)
	wrapped := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "round"},
			Args: []*plan.Expr{matchExpr},
		}},
	}
	list := &plan.Expr{Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
		wrapped,
		makePlan2Int64ConstExprWithType(1),
	}}}}

	require.True(t, exprCallsFunc(list, "fulltext_match"))
	require.False(t, exprCallsFunc(nil, "fulltext_match"))
	require.False(t, exprCallsFunc(makePlan2StringConstExprWithType("plain"), "fulltext_match"))
	matches := collectNestedFullTextMatches(list, nil)
	require.Len(t, matches, 1)
	require.Same(t, matchExpr, matches[0])
	require.Empty(t, collectNestedFullTextMatches(nil, nil))

	replaced := replaceScoreFnInExprBy(list, func(fn *plan.Function) *plan.Expr {
		if fn != nil && fn.Func != nil && fn.Func.ObjName == "fulltext_match" {
			return makePlan2Float32ConstExprWithType(0.75)
		}
		return nil
	})
	require.NotNil(t, replaced.GetList())
	require.Equal(t, float32(0.75), replaced.GetList().List[0].GetF().Args[0].GetLit().GetFval())
	require.Nil(t, replaceScoreFnInExprBy(nil, nil))

	for _, name := range []string{"round", "cast", "floor", "ceil"} {
		expr := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: name},
			Args: []*plan.Expr{matchExpr},
		}}}
		require.Same(t, matchExpr, monotoneWrappedFullTextMatch(expr), name)
	}
	require.Same(t, matchExpr, monotoneWrappedFullTextMatch(matchExpr))
	require.Nil(t, monotoneWrappedFullTextMatch(nil))
	require.Nil(t, monotoneWrappedFullTextMatch(&plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "round"},
	}}}))
	require.Nil(t, monotoneWrappedFullTextMatch(&plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "sqrt"},
		Args: []*plan.Expr{matchExpr},
	}}}))
}

func TestFulltext2DrivingMatchAndLiteralGuards(t *testing.T) {
	match := ft2TestMatch("hello", 0, 1, "body")
	matchExpr := ft2TestExpr(match)
	value := makePlan2Float64ConstExprWithType(0.5)

	require.Len(t, collectDrivingFullText2Matches(fulltext2ComparisonExpr(">", matchExpr, value), nil), 1)
	require.Len(t, collectDrivingFullText2Matches(fulltext2ComparisonExpr(">=", matchExpr, value), nil), 1)
	require.Len(t, collectDrivingFullText2Matches(fulltext2ComparisonExpr("and",
		fulltext2ComparisonExpr(">", matchExpr, value),
		fulltext2ComparisonExpr(">=", matchExpr, value)), nil), 2)
	for _, expr := range []*plan.Expr{
		fulltext2ComparisonExpr(">=", matchExpr, makePlan2Float64ConstExprWithType(0)),
		fulltext2ComparisonExpr(">", matchExpr, makePlan2Float64ConstExprWithType(-1)),
		fulltext2ComparisonExpr("<", matchExpr, value),
		fulltext2ComparisonExpr(">", matchExpr, makePlan2StringConstExprWithType("0.5")),
		fulltext2ComparisonExpr(">", makePlan2StringConstExprWithType("0.5"), value),
	} {
		require.Empty(t, collectDrivingFullText2Matches(expr, nil))
	}

	for _, tc := range []struct {
		expr *plan.Expr
		want float64
	}{
		{makePlan2Int64ConstExprWithType(-2), -2},
		{makePlan2Uint64ConstExprWithType(3), 3},
		{makePlan2Float32ConstExprWithType(0.25), 0.25},
		{makePlan2Float64ConstExprWithType(0.75), 0.75},
	} {
		got, ok := constValueAsFloat(tc.expr)
		require.True(t, ok)
		require.Equal(t, tc.want, got)
	}
	for _, expr := range []*plan.Expr{nil, makePlan2StringConstExprWithType("not numeric"), matchExpr} {
		_, ok := constValueAsFloat(expr)
		require.False(t, ok)
	}
}

func TestFulltext2WrappedMatchDiscovery(t *testing.T) {
	builder, _, project, scan, idxdef := newFulltext2CoveredFixture(t, false, false)
	tag := scan.BindingTags[0]
	second := ft2TestMatch("other", 0, tag, "body")
	scan.FilterList = append(scan.FilterList, fulltext2ComparisonExpr(">", ft2TestExpr(second), makePlan2Float64ConstExprWithType(0.5)))
	project.ProjectList = append(project.ProjectList, &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "round"},
		Args: []*plan.Expr{ft2TestExpr(second)},
	}}})

	gotExprs, gotDefs := builder.getWrappedFullText2Matches(project, scan, []int32{0}, []int32{1})
	require.Len(t, gotExprs, 1)
	require.Len(t, gotDefs, 1)
	require.Same(t, second, gotExprs[0].GetF())
	require.Same(t, idxdef, gotDefs[0])

	upperBuilder, _, upperProject, upperScan, _ := newFulltext2CoveredFixture(t, false, false)
	upperMatch := ft2TestMatch("upper", 0, upperScan.BindingTags[0], "body")
	upperScan.FilterList = []*plan.Expr{fulltext2ComparisonExpr("<", ft2TestExpr(upperMatch), makePlan2Float64ConstExprWithType(0.5))}
	upperProject.ProjectList = nil
	gotExprs, gotDefs = upperBuilder.getWrappedFullText2Matches(upperProject, upperScan, nil, nil)
	require.Empty(t, gotExprs)
	require.Empty(t, gotDefs)

	classicBuilder, _, classicProject, classicScan, _ := newFulltext2CoveredFixture(t, false, false)
	classicScan.TableDef.Indexes[0].IndexAlgo = catalog.MOIndexFullTextAlgo.ToString()
	classicMatch := ft2TestMatch("classic", 0, classicScan.BindingTags[0], "body")
	classicProject.ProjectList = []*plan.Expr{&plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "round"},
		Args: []*plan.Expr{ft2TestExpr(classicMatch)},
	}}}}
	gotExprs, gotDefs = classicBuilder.getWrappedFullText2Matches(classicProject, classicScan, nil, nil)
	require.Empty(t, gotExprs)
	require.Empty(t, gotDefs)

	malformedBuilder, _, malformedProject, malformedScan, _ := newFulltext2CoveredFixture(t, false, false)
	malformedProject.ProjectList = []*plan.Expr{{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "fulltext_match"},
		Args: []*plan.Expr{makePlan2StringConstExprWithType("missing mode")},
	}}}}
	gotExprs, gotDefs = malformedBuilder.getWrappedFullText2Matches(malformedProject, malformedScan, nil, nil)
	require.Empty(t, gotExprs)
	require.Empty(t, gotDefs)

	gotExprs, gotDefs = builder.getWrappedFullText2Matches(nil, nil, nil, nil)
	require.Empty(t, gotExprs)
	require.Empty(t, gotDefs)
}

func TestFulltext2ServingAndCoverageHelpers(t *testing.T) {
	match := ft2TestMatch("hello", 0, 1, "body")
	builder := &QueryBuilder{}
	served := []fulltextServedMatch{{fn: match, nodeID: 1, fulltext2: true}}
	require.True(t, builder.isServedFullText2Match(match, served))
	require.False(t, builder.isServedFullText2Match(nil, served))
	require.False(t, builder.isServedFullText2Match(&plan.Function{Func: &plan.ObjectRef{ObjName: "other"}}, served))
	require.False(t, builder.isServedFullText2Match(match, []fulltextServedMatch{{fn: match}}))

	scoreNode := &plan.Node{
		BindingTags: []int32{7},
		TableDef:    &plan.TableDef{Cols: []*plan.ColDef{{}, {Typ: plan.Type{Id: int32(types.T_float32)}}}},
	}
	scoreBuilder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{scoreNode}}}
	scoreRef := scoreBuilder.fullText2ScoreColRef(0)
	require.NotNil(t, scoreRef)
	require.Equal(t, int32(7), scoreRef.GetCol().RelPos)
	require.Equal(t, int32(1), scoreRef.GetCol().ColPos)
	invalidBuilder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{
		{},
		{TableDef: &plan.TableDef{Cols: []*plan.ColDef{{}, {}}}},
		{TableDef: &plan.TableDef{Cols: []*plan.ColDef{{}, {Typ: plan.Type{Id: int32(types.T_float32)}}}}},
	}}}
	require.Nil(t, invalidBuilder.fullText2ScoreColRef(-1))
	require.Nil(t, invalidBuilder.fullText2ScoreColRef(0))
	require.Nil(t, invalidBuilder.fullText2ScoreColRef(1))
	require.Nil(t, invalidBuilder.fullText2ScoreColRef(2))

	list := &plan.Expr{Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{ft2TestExpr(match)}}}}
	require.True(t, hasUnservedFullText2Match(list, func(*plan.Function) bool { return false }))
	require.False(t, hasUnservedFullText2Match(list, func(*plan.Function) bool { return true }))
	require.False(t, hasUnservedFullText2Match(nil, func(*plan.Function) bool { return false }))
	require.False(t, hasUnservedFullText2Match(makePlan2Int64ConstExprWithType(1), func(*plan.Function) bool { return false }))

	coveredBuilder, _, _, scan, _ := newFulltext2CoveredFixture(t, false, false)
	coveredMatch := ft2TestMatch("covered", 0, scan.BindingTags[0], "body")
	require.True(t, coveredBuilder.exprHasFullText2Match(ft2TestExpr(coveredMatch), scan))
	scan.TableDef.Indexes[0].IndexAlgo = catalog.MOIndexFullTextAlgo.ToString()
	require.False(t, coveredBuilder.exprHasFullText2Match(ft2TestExpr(coveredMatch), scan))
	require.False(t, coveredBuilder.exprHasFullText2Match(nil, scan))

	require.True(t, isProjIndexPosition([]int32{1, 3}, 3))
	require.False(t, isProjIndexPosition([]int32{1, 3}, 2))

	scanTag := int32(10)
	otherTag := int32(11)
	expr := &plan.Expr{Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
		fulltext2ColumnExpr(scanTag, 0, "", plan.Type{Id: int32(types.T_int64)}),
		{Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "pair"}, Args: []*plan.Expr{
			fulltext2ColumnExpr(scanTag, 2, "", plan.Type{Id: int32(types.T_varchar)}),
			fulltext2ColumnExpr(otherTag, 0, "other", plan.Type{Id: int32(types.T_int64)}),
		}}}},
	}}}}
	var positions []int32
	require.True(t, coveredBaseColRefs(expr, scanTag, func(pos int32, _ string) bool {
		positions = append(positions, pos)
		return true
	}))
	require.Equal(t, []int32{0, 2}, positions)
	require.False(t, coveredBaseColRefs(expr, scanTag, func(int32, string) bool { return false }))

	remapExpr := &plan.Expr{Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
		fulltext2ColumnExpr(scanTag, 0, "", plan.Type{Id: int32(types.T_int64)}),
		fulltext2ColumnExpr(scanTag, 2, "", plan.Type{Id: int32(types.T_varchar)}),
	}}}}
	remapCoveredBaseColRefs(remapExpr, scanTag, 20, 0, map[string]int{"title": 0}, &plan.Node{TableDef: &plan.TableDef{
		Cols: []*plan.ColDef{{Name: "id"}, {Name: "body"}, {Name: "title"}},
	}})
	require.Equal(t, int32(20), remapExpr.GetList().List[0].GetCol().RelPos)
	require.Equal(t, int32(0), remapExpr.GetList().List[0].GetCol().ColPos)
	require.Equal(t, int32(20), remapExpr.GetList().List[1].GetCol().RelPos)
	require.Equal(t, int32(2), remapExpr.GetList().List[1].GetCol().ColPos)
	require.Nil(t, remapCoveredBaseColRefs(nil, scanTag, 20, 0, nil, nil))
}

func TestFulltext2IndexNodeAndPkHelpers(t *testing.T) {
	fixtureBuilder, _, _, scan, _ := newFulltext2CoveredFixture(t, false, false)
	require.Equal(t, "id", fulltext2PeelablePkColName(scan))
	scan.TableDef.Cols[0].Typ.Id = int32(types.T_text)
	require.Empty(t, fulltext2PeelablePkColName(scan))
	noPk := &plan.Node{TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}}}}}
	require.Empty(t, fulltext2PeelablePkColName(noPk))

	typ, ok := baseColTypeByName(scan, "TITLE")
	require.True(t, ok)
	require.Equal(t, int32(types.T_varchar), typ.Id)
	_, ok = baseColTypeByName(scan, "missing")
	require.False(t, ok)
	_, ok = baseColTypeByName(nil, "title")
	require.False(t, ok)

	ctx := NewBindContext(fixtureBuilder, nil)
	nodeID, err := fixtureBuilder.buildFulltext2SearchNode(ctx, []*plan.Expr{
		makePlan2StringConstExprWithType("{}"),
		makePlan2StringConstExprWithType("hello"),
		makePlan2Int64ConstExprWithType(0),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, plan.Node_FUNCTION_SCAN, fixtureBuilder.qry.Nodes[nodeID].NodeType)
	require.Equal(t, fulltext2_search_func_name, fixtureBuilder.qry.Nodes[nodeID].TableDef.TblFunc.Name)
	require.Len(t, ft2SearchBaseColDefs(), 2)
}

func TestFulltext2ProjectionWithoutIndexBecomesScore(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	tag := builder.genNewBindTag()
	scan := &plan.Node{
		TableDef: &plan.TableDef{
			Name: "docs",
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "body", Typ: plan.Type{Id: int32(types.T_text)}},
			},
			Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
		},
		BindingTags: []int32{tag},
	}
	project := &plan.Node{ProjectList: []*plan.Expr{
		fulltext2MatchFilter(ft2TestMatch("hello", 0, tag, "body")),
	}}
	ids, defs := builder.getFullTextMatchFromProject(project, scan)
	require.Empty(t, ids)
	require.Empty(t, defs)
	require.Equal(t, "fulltext_match_score", project.ProjectList[0].GetF().Func.ObjName)
}
