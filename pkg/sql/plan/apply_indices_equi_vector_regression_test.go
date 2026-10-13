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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func equiOrdinaryUnionSQL(branches int) string {
	parts := make([]string, branches)
	for i := range parts {
		parts[i] = fmt.Sprintf("select row_id+%d from equi_documents", i+1)
	}
	return strings.Join(parts, " union all ")
}

func TestEquiVectorGuardRejectsOrdinarySQLBeforeStatementScan(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		noIndex   bool
	}{
		{"union", equiOrdinaryUnionSQL(32), false},
		{"ordinary_sort", "select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id order by a.category limit 1", false},
		{"no_vector_index", "select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id order by l2_distance(a.v,'[0,0]') limit 1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads := 0
			q := equiVectorPlanFixture(t, tc.sql, false, false, func(ctx *customMockCompilerContext) {
				resolve := ctx.resolveVarFunc
				ctx.resolveVarFunc = func(name string, system, global bool) (interface{}, error) {
					if name == "enable_vector_auto_mode_by_default" {
						reads++
					}
					return resolve(name, system, global)
				}
				if tc.noIndex {
					ctx.tables["equi_chunks"].Indexes = nil
				}
			})
			require.Empty(t, equiVectorSearchNodes(q))
			require.Zero(t, reads, "ineligible projections must not enter statement-wide AUTO analysis")
		})
	}
}

func TestEquiVectorGuardStatementSafetyIsPassScoped(t *testing.T) {
	tc := newEquiVectorPlanCase(t)
	ctx := tc.builder.compCtx.(*customMockCompilerContext)
	resolve := ctx.resolveVarFunc
	reads := 0
	ctx.resolveVarFunc = func(name string, system, global bool) (interface{}, error) {
		if name == "enable_vector_auto_mode_by_default" {
			reads++
		}
		return resolve(name, system, global)
	}
	for i := 0; i < 8; i++ {
		require.NotNil(t, tc.builder.buildVectorEquiJoinContext(tc.projNode))
	}
	require.Equal(t, 1, reads)
	tc.builder.resetSpecialIndexGuards()
	tc.builder.qry.Nodes = append(tc.builder.qry.Nodes, &plan.Node{
		NodeId: int32(len(tc.builder.qry.Nodes)), RankOption: &plan.RankOption{Mode: "auto"},
	})
	require.Nil(t, tc.builder.buildVectorEquiJoinContext(tc.projNode), "a new pass must not reuse a safe decision from the previous plan")
	require.Equal(t, 2, reads)
	tc.builder.prepareSpecialIndexGuards(tc.projNodeID)
	require.Empty(t, tc.builder.projectSpecialGuards)
	require.Equal(t, 3, reads, "prepare must invalidate a previous pass even without reset")
}

func TestEquiVectorGuardRejectsImplicitAutoOutsideRegion(t *testing.T) {
	tc := newEquiVectorPlanCase(t)
	ctx := tc.builder.compCtx.(*customMockCompilerContext)
	resolve := ctx.resolveVarFunc
	ctx.resolveVarFunc = func(name string, system, global bool) (interface{}, error) {
		if name == "enable_vector_auto_mode_by_default" {
			return int8(1), nil
		}
		return resolve(name, system, global)
	}
	scan := DeepCopyNode(tc.builder.qry.Nodes[tc.mainScanNodeID])
	tc.builder.rebindScanNode(scan)
	scanID := tc.builder.appendNode(scan, tc.ctx)
	distance := DeepCopyExpr(tc.builder.qry.Nodes[tc.projNode.Children[0]].OrderBy[0].Expr)
	distance.GetF().Args[0].GetCol().RelPos = scan.BindingTags[0]
	sortID := tc.builder.appendNode(&plan.Node{NodeType: plan.Node_SORT, Children: []int32{scanID},
		OrderBy: []*plan.OrderBySpec{{Expr: distance}}, Limit: makePlan2Uint64ConstExprWithType(1)}, tc.ctx)
	tc.builder.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{sortID}}, tc.ctx)
	require.Nil(t, tc.builder.buildVectorEquiJoinContext(tc.projNode))
	require.NotEmpty(t, tc.builder.vectorCandidateAuto.implicit)
	tc.builder.resetSpecialIndexGuards()
	ctx.resolveVarFunc = resolve
	require.NotNil(t, tc.builder.buildVectorEquiJoinContext(tc.projNode))
}

func equiVectorOtherIndexFixture(ctx *customMockCompilerContext) {
	const indexTable = "__mo_index_equi_documents"
	b := ctx.tables["equi_documents"]
	b.Cols = append(b.Cols, &plan.ColDef{Name: "payload", Typ: plan.Type{Id: int32(types.T_varchar), Width: 64}})
	b.Name2ColIndex["payload"] = 3
	b.Indexes = []*plan.IndexDef{{
		IndexName: "idx_label_document", IndexAlgo: "btree", IndexTableName: indexTable,
		TableExist: true, Parts: []string{"label", "document_id", catalog.CreateAlias("row_id")},
	}}
	ctx.tables[indexTable] = &plan.TableDef{Name: indexTable, TblId: 899,
		Cols: []*plan.ColDef{
			{Name: catalog.IndexTableIndexColName, Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: catalog.IndexTablePrimaryColName, Typ: b.Cols[2].Typ},
		}, Name2ColIndex: map[string]int32{catalog.IndexTableIndexColName: 0, catalog.IndexTablePrimaryColName: 1},
	}
	ctx.objects[indexTable] = &plan.ObjectRef{SchemaName: "tpch", ObjName: indexTable, Obj: 899}
}

func TestEquiVectorTopKPreservesOtherIndexes(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		indexes   int
		baseScans int
	}{
		{"covering", "select a.id from equi_chunks a join equi_documents b on a.document_id=b.document_id where b.label='allowed' order by l2_distance(a.v,'[0,0]') limit 2", 2, 0},
		{"reversed", "select a.id,b.document_id from equi_documents b join equi_chunks a on b.document_id=a.document_id where b.label='allowed' order by l2_distance(a.v,'[0,0]') limit 2", 2, 0},
		{"right_projection", "select a.id,b.label from equi_chunks a join equi_documents b on a.document_id=b.document_id where b.label='allowed' order by l2_distance(a.v,'[0,0]') limit 2", 2, 0},
		{"noncovering_projection", "select a.id,b.payload from equi_chunks a join equi_documents b on a.document_id=b.document_id where b.label='allowed' order by l2_distance(a.v,'[0,0]') limit 2", 2, 1},
		{"ignore_index", "select a.id from equi_chunks a join equi_documents b ignore index(idx_label_document) on a.document_id=b.document_id where b.label='allowed' order by l2_distance(a.v,'[0,0]') limit 2", 0, 2},
		{"force_control", "select a.id,b.label from equi_chunks a join equi_documents b on a.document_id=b.document_id where b.label='allowed' order by l2_distance(a.v,'[0,0]') limit 2 by rank with option 'mode=force'", 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := equiVectorPlanFixture(t, tc.sql, false, false, equiVectorOtherIndexFixture)
			if tc.name != "force_control" {
				require.Len(t, equiVectorSearchNodes(q), 1)
			} else {
				require.Empty(t, equiVectorSearchNodes(q))
			}
			indexes, baseScans := 0, 0
			for id := range reachableVectorJoinNodeIDs(q, q.Steps[0]) {
				node := q.Nodes[id]
				if node.IndexScanInfo.IndexName == "idx_label_document" {
					indexes++
				}
				if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef.Name == "equi_documents" {
					baseScans++
				}
			}
			require.Equal(t, tc.indexes, indexes, "both original and eligibility B need their own scalar index path")
			require.Equal(t, tc.baseScans, baseScans, "only non-covering output needs a base-table lookup")
		})
	}
}

func TestEquiVectorDeclinedRewriteRestoresScalarIndexes(t *testing.T) {
	q := equiVectorPlanFixture(t, `select a.id,b.label from equi_chunks a join equi_documents b
		on a.document_id=b.document_id where a.category='x' and b.label='allowed'
		order by l2_distance(a.v,'[0,0]') limit 2`, false, false, func(ctx *customMockCompilerContext) {
		equiVectorOtherIndexFixture(ctx)
		a := ctx.tables["equi_chunks"]
		a.Cols[0].Typ = plan.Type{Id: int32(types.T_int64), NotNullable: true}
		const indexTable = "__mo_index_equi_category"
		a.Indexes = append(a.Indexes, &plan.IndexDef{IndexName: "idx_category", IndexAlgo: "btree",
			IndexTableName: indexTable, TableExist: true, Parts: []string{"category", catalog.CreateAlias("id")}})
		ctx.tables[indexTable] = &plan.TableDef{Name: indexTable,
			Cols: []*plan.ColDef{
				{Name: catalog.IndexTableIndexColName, Typ: plan.Type{Id: int32(types.T_varchar)}},
				{Name: catalog.IndexTablePrimaryColName, Typ: a.Cols[0].Typ},
			}, Name2ColIndex: map[string]int32{catalog.IndexTableIndexColName: 0, catalog.IndexTablePrimaryColName: 1},
		}
		ctx.objects[indexTable] = &plan.ObjectRef{SchemaName: "tpch", ObjName: indexTable}
		rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
		original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
		t.Cleanup(func() {
			if hadOriginal {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
			} else {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
			}
		})
		// CanApply accepts the shape; ApplyForSort must decline at the membership wire gate.
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion9)
	})
	require.Empty(t, equiVectorSearchNodes(q))
	var indexes []string
	for id := range reachableVectorJoinNodeIDs(q, q.Steps[0]) {
		if name := q.Nodes[id].IndexScanInfo.IndexName; name != "" {
			indexes = append(indexes, name)
		}
	}
	require.ElementsMatch(t, []string{"idx_category", "idx_label_document"}, indexes)
}

func TestEquiVectorDeclinedRewriteRestoresJoinIndexes(t *testing.T) {
	const left = "select a.id,b.label from equi_chunks a %s join equi_documents b on a.document_id=b.document_id where b.label='allowed'"
	const right = "select a.id,b.label from equi_documents b join equi_chunks a %s on b.document_id=a.document_id where b.label='allowed'"
	for _, tc := range []struct {
		name, shape, hint, filter, mode string
		indexes                         int
	}{
		{"force_join", left, "force index for join(idx_document)", "", "", 1},
		{"unforced_cost_control", left, "", "", "", 0},
		{"unforced_vector_force_control", left, "", "", " by rank with option 'mode=force'", 0},
		{"reversed_control", right, "force index for join(idx_document)", "", "", 1},
		{"vector_force_control", left, "force index for join(idx_document)", "", " by rank with option 'mode=force'", 1},
		{"force_precedes_filter", left, "force index for join(idx_document)", " and a.category='x'", "", 1},
		{"ignore_join", left, "ignore index for join(idx_document)", "", "", 0},
		{"use_other_join_index", left, "use index for join(idx_category_document)", "", "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf(tc.shape, tc.hint) + tc.filter + " order by l2_distance(a.v,'[0,0]') limit 2" + tc.mode
			q := equiVectorPlanFixture(t, sql, false, false, func(ctx *customMockCompilerContext) {
				a := ctx.tables["equi_chunks"]
				a.Cols[0].Typ = plan.Type{Id: int32(types.T_int64), NotNullable: true}
				equiVectorInputIndexFixture(ctx)
				const table = "__mo_index_equi_document"
				a.Indexes = append(a.Indexes, &plan.IndexDef{IndexName: "idx_document", IndexAlgo: "btree",
					IndexTableName: table, TableExist: true, Parts: []string{"document_id", catalog.CreateAlias("id")}})
				ctx.tables[table] = DeepCopyTableDef(ctx.tables["__mo_index_equi_chunks_category"], true)
				ctx.tables[table].Name, ctx.tables[table].TblId = table, 897
				ctx.objects[table] = &plan.ObjectRef{SchemaName: "tpch", ObjName: table, Obj: 897}
				rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
				original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
				t.Cleanup(func() {
					if hadOriginal {
						rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
					} else {
						rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
					}
				})
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion9)
			})
			require.Empty(t, equiVectorSearchNodes(q), "membership must decline below its protocol gate")
			var indexes []string
			for id := range reachableVectorJoinNodeIDs(q, q.Steps[0]) {
				if name := q.Nodes[id].IndexScanInfo.IndexName; name != "" {
					indexes = append(indexes, name)
				}
			}
			if tc.indexes == 1 {
				require.Equal(t, []string{"idx_document"}, indexes, "JOIN selection must precede competing filter access for FORCE JOIN")
			} else {
				require.Empty(t, indexes, "fallback must retain ordinary JOIN eligibility and hint restrictions")
			}
		})
	}
}

func TestEquiVectorFallbackRestoresCostBasedJoinIndex(t *testing.T) {
	builder, joinID, scanID, _ := makeIndexHintJoinBuilder(t)
	ctx := builder.ctxByNode[joinID]
	projectID := builder.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{joinID}}, ctx)
	builder.registerProjectGuard(projectID, specialIndexKindEquiVector, []int32{scanID})
	root, err := builder.applyIndicesForProject(projectID, builder.qry.Nodes[projectID], map[[2]int32]int{}, map[[2]int32]*plan.Expr{})
	require.NoError(t, err)
	require.Equal(t, projectID, root)
	var indexes []string
	for id := range reachableVectorJoinNodeIDs(builder.qry, root) {
		if name := builder.qry.Nodes[id].IndexScanInfo.IndexName; name != "" {
			indexes = append(indexes, name)
		}
	}
	require.Equal(t, []string{"idx_a"}, indexes, "selective JOIN inputs must retain cost-based access without FORCE")
	require.Empty(t, builder.projectSpecialGuards)
	require.Empty(t, builder.protectedScans)
}

func TestEquiVectorFallbackPropagatesJoinIndexError(t *testing.T) {
	builder, joinID, scanID, table := makeIndexHintJoinBuilder(t)
	ctx := builder.ctxByNode[joinID]
	projectID := builder.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{joinID}}, ctx)
	require.NoError(t, builder.recordIndexHints(scanID, table, []*tree.IndexHint{{
		HintType: tree.HintForce, HintScope: tree.HintForJoin, IndexNames: []string{"idx_a"},
	}}))
	// Exercise completion after an earlier guard's shape is no longer eligible.
	builder.registerProjectGuard(projectID, specialIndexKindEquiVector, []int32{scanID})
	builder.compCtx.(*fullTextJoinMockCompilerContext).tables["idx_join_a_table"].Cols = nil
	root, err := builder.applyIndicesForProject(projectID, builder.qry.Nodes[projectID], map[[2]int32]int{}, map[[2]int32]*plan.Expr{})
	require.ErrorContains(t, err, "invalid metadata for index idx_a")
	require.Equal(t, int32(-1), root)
	require.Empty(t, builder.projectSpecialGuards)
	require.Empty(t, builder.protectedScans)
}

func TestEquiVectorGuardCleanupOnPlanningPanic(t *testing.T) {
	tc := newEquiVectorPlanCase(t)
	tc.builder.prepareSpecialIndexGuards(tc.projNodeID)
	require.NotEmpty(t, tc.builder.projectSpecialGuards)
	ctx := tc.builder.compCtx.(*customMockCompilerContext)
	resolve := ctx.resolveVarFunc
	ctx.resolveVarFunc = func(name string, system, global bool) (interface{}, error) {
		if name == "ivf_threads_search" {
			panic("injected planning failure")
		}
		return resolve(name, system, global)
	}
	require.PanicsWithValue(t, "injected planning failure", func() {
		_, _ = tc.builder.applyIndicesForProject(tc.projNodeID, tc.projNode, nil, nil)
	})
	require.Empty(t, tc.builder.projectSpecialGuards)
	require.Empty(t, tc.builder.protectedScans)
}

func BenchmarkEquiVectorOrdinaryUnionBuildPlan(b *testing.B) {
	for _, branches := range []int{16, 64, 256} {
		b.Run(fmt.Sprintf("branches=%d", branches), func(b *testing.B) {
			sql := equiOrdinaryUnionSQL(branches)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				equiVectorPlanFixture(b, sql, false, false)
			}
		})
	}
}

func BenchmarkEquiVectorOrdinaryProjectionGuards(b *testing.B) {
	for _, branches := range []int{256, 1024, 4096} {
		b.Run(fmt.Sprintf("branches=%d", branches), func(b *testing.B) {
			builder := NewQueryBuilder(plan.Query_SELECT, newVectorJoinMockCtx(), false, false)
			ctx := NewBindContext(builder, nil)
			for i := 0; i < branches; i++ {
				scan := builder.appendNode(&plan.Node{NodeType: plan.Node_TABLE_SCAN}, ctx)
				builder.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{scan}}, ctx)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 1; j < len(builder.qry.Nodes); j += 2 {
					builder.detectEquiVectorJoinGuard(builder.qry.Nodes[j])
				}
			}
		})
	}
}

func equiVectorInputIndexFixture(ctx *customMockCompilerContext) {
	const indexTable = "__mo_index_equi_chunks_category"
	a := ctx.tables["equi_chunks"]
	a.Indexes = append(a.Indexes, &plan.IndexDef{
		IndexName: "idx_category_document", IndexAlgo: "btree", IndexTableName: indexTable,
		TableExist: true, Parts: []string{"category", "document_id", catalog.CreateAlias("id")},
	})
	ctx.tables[indexTable] = &plan.TableDef{Name: indexTable, TblId: 898,
		Cols: []*plan.ColDef{
			{Name: catalog.IndexTableIndexColName, Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: catalog.IndexTablePrimaryColName, Typ: a.Cols[0].Typ},
		}, Name2ColIndex: map[string]int32{catalog.IndexTableIndexColName: 0, catalog.IndexTablePrimaryColName: 1},
	}
	ctx.objects[indexTable] = &plan.ObjectRef{SchemaName: "tpch", ObjName: indexTable, Obj: 898}
}

func TestVectorCandidateCopiesPreserveInputIndexHints(t *testing.T) {
	for _, shape := range []struct{ name, sql string }{
		{"inner", "select a.id from equi_chunks a %s join equi_documents b on a.document_id=b.document_id where a.category='x'"},
		{"reversed", "select a.id from equi_documents b join equi_chunks a %s on b.document_id=a.document_id where a.category='x'"},
		{"semi", "select a.id from equi_chunks a %s where a.document_id in(select document_id from equi_documents) and a.category='x'"},
	} {
		for _, hint := range []string{"", "ignore index(idx_category_document)", "use index(idx_ivf_v)", "ignore index for join(idx_category_document)"} {
			t.Run(shape.name+"/"+hint, func(t *testing.T) {
				statement := fmt.Sprintf(shape.sql, hint) + " order by l2_distance(a.v,'[0,0]') limit 2"
				q := equiVectorPlanFixture(t, statement, false, false, equiVectorInputIndexFixture)
				require.Len(t, equiVectorSearchNodes(q), 1, "scalar restrictions retain IVF eligibility")
				indexes := 0
				for id := range reachableVectorJoinNodeIDs(q, q.Steps[0]) {
					if q.Nodes[id].IndexScanInfo.IndexName == "idx_category_document" {
						indexes++
					}
				}
				if hint == "" || hint == "ignore index for join(idx_category_document)" {
					require.Positive(t, indexes, "control must exercise the scalar index path")
				} else {
					require.Zero(t, indexes, "every reachable candidate/row-fetch scan obeys the input hint")
				}
			})
		}
	}
}
