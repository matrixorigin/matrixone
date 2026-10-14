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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func newIvfFulltextMembershipTestContext(t *testing.T, fulltext2 bool) *customMockCompilerContext {
	t.Helper()
	ctx := newVectorJoinMockCtx(t)
	resolve := ctx.resolveVarFunc
	ctx.resolveVarFunc = func(name string, system, global bool) (interface{}, error) {
		if name == "ft_relevancy_algorithm" {
			return "", nil
		}
		return resolve(name, system, global)
	}
	ctx.GetProcess().SetResolveVariableFunc(ctx.ResolveVariable)
	table := &planpb.TableDef{
		Name: "docs", TblId: 501,
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64), NotNullable: true}},
			{Name: "body", Typ: planpb.Type{Id: int32(types.T_text)}},
			{Name: "tag", Typ: planpb.Type{Id: int32(types.T_int32)}},
			{Name: "v", Typ: planpb.Type{Id: int32(types.T_array_float32), Width: 2, NotNullable: true}},
		},
		Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0, "body": 1, "tag": 2, "v": 3},
	}
	ivf := makeConsistentIvfMultiTableIndexForOptimizeTest("v", `{"op_type":"vector_l2_ops","lists":"1"}`)
	for _, kind := range []string{catalog.SystemSI_IVFFLAT_TblType_Metadata, catalog.SystemSI_IVFFLAT_TblType_Centroids, catalog.SystemSI_IVFFLAT_TblType_Entries} {
		idx := ivf.IndexDefs[kind]
		idx.TableExist = true
		table.Indexes = append(table.Indexes, idx)
	}
	ft := &planpb.IndexDef{IndexName: "ix_ft", IndexAlgo: catalog.MOIndexFullTextAlgo.ToString(), IndexTableName: "ft", TableExist: true, Parts: []string{"body"}}
	if fulltext2 {
		ft.IndexAlgo = catalog.MoIndexFullText2Algo.ToString()
		ft.IndexAlgoTableType = catalog.FullText2Index_TblType_Storage
		table.Indexes = append(table.Indexes, &planpb.IndexDef{IndexName: "ix_ft", IndexAlgo: ft.IndexAlgo, IndexAlgoTableType: catalog.FullText2Index_TblType_Metadata, IndexTableName: "ft_meta", TableExist: true, Parts: []string{"body"}})
	}
	table.Indexes = append(table.Indexes, ft)
	ctx.tables["docs"] = table
	ctx.objects["docs"] = &planpb.ObjectRef{Obj: 501, SchemaName: "test", ObjName: "docs"}
	for _, idx := range table.Indexes {
		ctx.tables[idx.IndexTableName] = &planpb.TableDef{Name: idx.IndexTableName, TblId: 502}
		ctx.objects[idx.IndexTableName] = &planpb.ObjectRef{Obj: 502, SchemaName: "test", ObjName: idx.IndexTableName}
	}
	return ctx
}

func TestIvfPreFulltextMembershipSearchesOnce(t *testing.T) {
	tests := []struct {
		name      string
		fulltext2 bool
		query     string
		searches  int
	}{
		{name: "filter only", query: "select id from docs where match(body) against('needle')", searches: 1},
		{name: "fulltext2", fulltext2: true, query: "select id from docs where match(body) against('needle')", searches: 1},
		{name: "scalar and null", query: "select id from docs where match(body) against('needle') and tag = 2 and body is not null", searches: 1},
		{name: "score threshold", query: "select id from docs where match(body) against('needle') > 0.5", searches: 1},
		{name: "mixed or", query: "select id from docs where match(body) against('needle') and (match(body) against('needle') or tag = 2)", searches: 1},
		{name: "two terms", query: "select id from docs where match(body) against('needle') and match(body) against('omega')", searches: 2},
		{name: "boolean mode", query: "select id from docs where match(body) against('+needle +omega' in boolean mode)", searches: 1},
		{name: "projected score", query: "select id, match(body) against('needle') from docs where match(body) against('needle')", searches: 2},
		{name: "wrapped projected score", query: "select id, round(match(body) against('needle'), 3) from docs where match(body) against('needle')", searches: 2},
		{name: "projected body", query: "select id, body from docs where match(body) against('needle')", searches: 1},
		{name: "volatile conjunct", query: "select id from docs where match(body) against('needle') and match(body) against('needle') > rand()", searches: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := newIvfFulltextMembershipTestContext(t, test.fulltext2)
			query := test.query + " order by l2_distance(v,'[0,0]') limit 7 by rank with option 'mode=pre'"
			qry := buildIvfFulltextMembershipTestPlan(t, ctx, query, false)
			fulltext, vector, membership := countIvfFulltextSearches(qry)
			require.Equal(t, 1, vector)
			require.Positive(t, membership, "PRE must retain exact membership, independently of its runtime filter")
			require.Equal(t, test.searches, fulltext, "PRE must not repeat a filter-only MATCH on row fetch")
		})
	}
}

func buildIvfFulltextMembershipTestPlan(t *testing.T, ctx CompilerContext, sql string, prepare bool) *planpb.Query {
	t.Helper()
	stmt, err := mysql.ParseOne(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(ctx, stmt, prepare)
	require.NoError(t, err)
	return p.GetQuery()
}

func countIvfFulltextSearches(qry *planpb.Query) (fulltext, vector, membership int) {
	var visit func(int32)
	visit = func(id int32) {
		n := qry.Nodes[id]
		if n.NodeType == planpb.Node_JOIN && n.JoinType == planpb.Node_SEMI {
			membership++
		}
		if n.IndexSearchScan != nil {
			switch n.IndexSearchScan.Index.IndexAlgo {
			case catalog.MOIndexFullTextAlgo.ToString(), catalog.MoIndexFullText2Algo.ToString():
				fulltext++
			case catalog.MoIndexIvfFlatAlgo.ToString():
				vector++
			}
		}
		for _, child := range n.Children {
			visit(child)
		}
	}
	for _, root := range qry.Steps {
		visit(root)
	}
	return
}

func TestIvfMembershipMatchRemovalKeepsBindingsAndFallbackFilters(t *testing.T) {
	for _, decline := range []bool{false, true} {
		name := "successful PK proof"
		if decline {
			name = "invalid membership proof"
		}
		t.Run(name, func(t *testing.T) {
			builder, ctx, scan, scanID, index := newIvfIncludeModeTestBuilder(t)
			tag := scan.BindingTags[0]
			match, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "fulltext_match", []*planpb.Expr{
				makePlan2StringConstExprWithType("needle", false), makePlan2Int64ConstExprWithType(0), ftjColExpr(scan.TableDef, tag, 4),
			})
			require.NoError(t, err)
			repeated, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "and", []*planpb.Expr{match, DeepCopyExpr(match)})
			require.NoError(t, err)
			scalar := makeIvfIncludeModeIsNotNullFilter(scan, 4)
			scan.FilterList = []*planpb.Expr{repeated, scalar}
			original := DeepCopyExprList(scan.FilterList)
			vctx := newIvfIncludeModeVectorSortContext(scan, scanID, "pre", 0, 4)
			builder.qry.Steps = []int32{builder.appendNode(vctx.projNode, ctx)}
			refs := map[[2]int32]int{{tag, 4}: 4} // two MATCH uses, scalar, projection
			if decline {
				vctx.hasMembership = true
				vctx.membershipNodeID = scanID // not a supported SEMI: rewrite must decline
			}
			_, err = builder.applyIndicesForSortUsingIvfflat(scanID, vctx, index, refs, make(map[[2]int32]*planpb.Expr))
			require.NoError(t, err)
			if decline {
				require.Equal(t, original, scan.FilterList)
				require.Equal(t, 4, refs[[2]int32{tag, 4}])
				return
			}
			require.Equal(t, []*planpb.Expr{scalar}, scan.FilterList)
			require.Equal(t, 2, refs[[2]int32{tag, 4}])
			var producer *planpb.Node
			for _, node := range builder.qry.Nodes {
				if node.NodeType == planpb.Node_TABLE_SCAN && node != scan {
					producer = node
				}
			}
			require.NotNil(t, producer)
			require.NotEqual(t, tag, producer.BindingTags[0])
			require.Len(t, producer.FilterList, 2)
			require.True(t, exprCallsFunc(producer.FilterList[0], "fulltext_match"))
			copiedRefs := make(map[[2]int32]int)
			extractColRefs(producer.FilterList[0], producer.BindingTags[0], copiedRefs)
			require.Equal(t, 2, copiedRefs[[2]int32{producer.BindingTags[0], 4}])
		})
	}
}

func TestIvfMembershipMatchRemovalRetainsAncestorScoreConsumer(t *testing.T) {
	builder, ctx, scan, scanID, _ := newIvfIncludeModeTestBuilder(t)
	match, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "fulltext_match", []*planpb.Expr{
		makePlan2StringConstExprWithType("needle", false), makePlan2Int64ConstExprWithType(0), ftjColExpr(scan.TableDef, scan.BindingTags[0], 4),
	})
	require.NoError(t, err)
	scan.FilterList = []*planpb.Expr{match}
	retained, removed := builder.ivfMembershipRowFetchFilters(scan)
	require.Empty(t, removed, "a detached scan cannot prove absence of external consumers")
	require.Equal(t, scan.FilterList, retained)
	ancestor := builder.appendNode(&planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{scanID}, ProjectList: []*planpb.Expr{DeepCopyExpr(match)}}, ctx)
	builder.qry.Steps = []int32{ancestor}
	retained, removed = builder.ivfMembershipRowFetchFilters(scan)
	require.Empty(t, removed)
	require.Equal(t, scan.FilterList, retained)
}

func TestIvfFulltextMembershipPaginationAndPostControls(t *testing.T) {
	for _, test := range []struct {
		name, suffix string
		prepare      bool
	}{
		{name: "post", suffix: "limit 7 by rank with option 'mode=post'"},
		{name: "zero", suffix: "limit 0 by rank with option 'mode=pre'"},
		{name: "offset", suffix: "limit 2 offset 1 by rank with option 'mode=pre'"},
		{name: "prepared", suffix: "limit 7 by rank with option 'mode=pre'", prepare: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			term := "'needle'"
			if test.prepare {
				term = "?"
			}
			sql := "select id from docs where match(body) against(" + term + ") order by l2_distance(v,'[0,0]') " + test.suffix
			qry := buildIvfFulltextMembershipTestPlan(t, newIvfFulltextMembershipTestContext(t, false), sql, test.prepare)
			fulltext, vector, membership := countIvfFulltextSearches(qry)
			require.Equal(t, 1, vector)
			require.Equal(t, 1, fulltext)
			if test.name == "post" {
				require.Zero(t, membership)
			} else {
				require.Positive(t, membership)
			}
		})
	}
}
