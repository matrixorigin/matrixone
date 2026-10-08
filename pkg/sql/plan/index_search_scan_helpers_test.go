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
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	ftplan "github.com/matrixorigin/matrixone/pkg/fulltext/plugin/plan"
	ft2plan "github.com/matrixorigin/matrixone/pkg/fulltext2/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	ivfflatplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/plan"
	"github.com/stretchr/testify/require"
)

// testIvfScanOptions returns the ivfflat settings of spec.
func testIvfScanOptions(t *testing.T, spec *plan.IndexSearchScan) ivfflatplan.ScanOptions {
	t.Helper()
	opts, err := ivfflatplan.DecodeScanOptions(spec.GetAlgoOptions())
	require.NoError(t, err)
	return opts
}

// testAlgoExpr returns the algorithm expression of spec named name, or nil.
func testAlgoExpr(spec *plan.IndexSearchScan, name string) *plan.Expr {
	for i, n := range spec.GetAlgoExprNames() {
		if n == name {
			return spec.AlgoExprs[i]
		}
	}
	return nil
}

// testIvfAlgoOptions returns the algo_options bytes of opts.
func testIvfAlgoOptions(t *testing.T, opts ivfflatplan.ScanOptions) []byte {
	t.Helper()
	data, err := ivfflatplan.EncodeScanOptions(opts)
	require.NoError(t, err)
	return data
}

// isFullTextSearchScan reports whether node is the index search scan of a
// classic fulltext index.
func isFullTextSearchScan(node *plan.Node) bool {
	return node.GetNodeType() == plan.Node_INDEX_SEARCH_SCAN &&
		catalog.IsFullTextIndexAlgo(node.GetIndexSearchScan().GetIndex().GetIndexAlgo())
}

// fullTextScanOptions returns the classic fulltext settings of a scan.
func fullTextScanOptions(t *testing.T, node *plan.Node) ftplan.ScanOptions {
	t.Helper()
	opts, err := ftplan.DecodeScanOptions(node.GetIndexSearchScan().GetAlgoOptions())
	require.NoError(t, err)
	return opts
}

// isFulltext2SearchScan reports whether node is the index search scan of a
// fulltext2 index.
func isFulltext2SearchScan(node *plan.Node) bool {
	return node.GetNodeType() == plan.Node_INDEX_SEARCH_SCAN &&
		catalog.IsFullText2IndexAlgo(node.GetIndexSearchScan().GetIndex().GetIndexAlgo())
}

// fulltextScanLimit returns the pushed candidate limit of a fulltext scan.
func fulltextScanLimit(node *plan.Node) *plan.Expr {
	return node.GetIndexSearchScan().GetCandidateLimit()
}

// fulltextScanPattern returns the MATCH pattern expression of a fulltext scan.
func fulltextScanPattern(node *plan.Node) *plan.Expr {
	return node.GetIndexSearchScan().GetQueryPayload()
}

// The fulltext scan builders carry the pattern, the guard and the scan options
// of the MATCH they serve.
func TestFullTextSearchScanBuilders(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
	ctx := NewBindContext(builder, nil)
	scanNode := &plan.Node{
		NodeId: 0,
		ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "t"},
		TableDef: &plan.TableDef{
			Name: "t",
			Cols: []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}}, {Name: "tag", Typ: plan.Type{Id: int32(types.T_int32)}}},
		},
		ScanSnapshot: &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 9}},
	}
	pattern := makePlan2StringConstExprWithType("apple")
	guard := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}}}

	id, err := builder.buildFullTextSearchScan(ctx, scanNode,
		&plan.IndexDef{IndexName: "ft", IndexAlgo: catalog.MOIndexFullTextAlgo.ToString()},
		&plan.ObjectRef{SchemaName: "db", ObjName: "idx"},
		ftplan.ScanOptions{SourceTable: "`db`.`t`", IndexTable: "`db`.`idx`", Mode: 1}, pattern, guard, "SELECT 1")
	require.NoError(t, err)
	node := builder.qry.Nodes[id]
	require.True(t, isFullTextSearchScan(node))
	require.Equal(t, "SELECT 1", node.Stats.Sql)
	require.Equal(t, []string{fulltext.ZeroRelevanceGuardExpr}, node.IndexSearchScan.AlgoExprNames)
	require.Equal(t, "apple", fulltextScanPattern(node).GetLit().GetSval())
	require.Equal(t, int64(9), node.ScanSnapshot.TS.PhysicalTime)
	require.Equal(t, "`db`.`idx`", fullTextScanOptions(t, node).IndexTable)

	ft2def := &plan.IndexDef{IndexName: "ft2", IndexAlgo: catalog.MoIndexFullText2Algo.ToString()}
	id, err = builder.buildFulltext2SearchScan(ctx, scanNode, ft2def, ft2plan.ScanOptions{Config: "{}"}, pattern, guard, ft2SearchBaseColDefs())
	require.NoError(t, err)
	require.True(t, isFulltext2SearchScan(builder.qry.Nodes[id]))
	require.Equal(t, []string{fulltext.ZeroRelevanceGuardExpr}, builder.qry.Nodes[id].IndexSearchScan.AlgoExprNames)

	colDefs, err := builder.ft2CoveredColDefs(scanNode, []string{"TAG"})
	require.NoError(t, err)
	require.Len(t, colDefs, 3)
	require.Equal(t, int32(types.T_int32), colDefs[2].Typ.Id)
	_, err = builder.ft2CoveredColDefs(scanNode, []string{"missing"})
	require.ErrorContains(t, err, `INCLUDE column "missing" not found`)
}
