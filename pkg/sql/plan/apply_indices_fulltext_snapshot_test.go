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

// ScanSnapshot propagation into the fulltext2_search FUNCTION_SCAN node built by the covered
// fast path (#27941).

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// coveredFulltext2Fixture builds the minimal shape that clears every tryApplyCoveredFulltext2
// guard: one MATCH filter, one fulltext2 index with an INCLUDE column, no residual predicate,
// and a pk-only projection. snapshot is attached to the base scan.
func coveredFulltext2Fixture(t *testing.T, snapshot *plan.Snapshot) (
	builder *QueryBuilder, nodeID int32, projNode, sortNode, scanNode *plan.Node, idxdef *plan.IndexDef,
) {
	t.Helper()

	builder = NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)

	scanTag := builder.genNewBindTag()
	scanNode = &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		ObjRef:   &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Name: "t",
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
				{Name: "body", Typ: plan.Type{Id: int32(types.T_varchar), Width: 256}},
				{Name: "tag", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
			},
			Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id"},
			Name2ColIndex: map[string]int32{"id": 0, "body": 1, "tag": 2},
			Indexes: []*plan.IndexDef{
				{IndexName: "ft2idx", IndexAlgoTableType: catalog.FullText2Index_TblType_Storage, IndexTableName: "__store"},
				{IndexName: "ft2idx", IndexAlgoTableType: catalog.FullText2Index_TblType_Metadata, IndexTableName: "__meta"},
			},
		},
		BindingTags:  []int32{scanTag},
		ScanSnapshot: snapshot,
		// match(body, 'x' IN NATURAL LANGUAGE MODE)
		FilterList: []*plan.Expr{{
			Typ: plan.Type{Id: int32(types.T_float32)},
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &ObjectRef{ObjName: "match_against"},
				Args: []*plan.Expr{
					makePlan2StringConstExprWithType("x"),
					{
						Typ:  plan.Type{Id: int32(types.T_int64)},
						Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: int64(tree.FULLTEXT_NL)}}},
					},
				},
			}},
		}},
	}
	nodeID = builder.appendNode(scanNode, bindCtx)
	for i := 0; i < 10; i++ {
		builder.ctxByNode = append(builder.ctxByNode, bindCtx)
	}

	projNode = &plan.Node{
		NodeType: plan.Node_PROJECT,
		Children: []int32{nodeID},
		// pk only
		ProjectList: []*plan.Expr{{
			Typ:  plan.Type{Id: int32(types.T_int64), Width: 64},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 0}},
		}},
	}
	sortNode = &plan.Node{NodeType: plan.Node_SORT, Children: []int32{nodeID}}

	idxdef = &plan.IndexDef{
		IndexName:       "ft2idx",
		IndexAlgo:       catalog.MoIndexFullText2Algo.ToString(),
		IncludedColumns: []string{"tag"},
	}
	return
}

// The covered path's fulltext2_search TVF node carries a deep copy of the base scan's
// snapshot.
func TestTryApplyCoveredFulltext2PropagatesScanSnapshot(t *testing.T) {
	snapshot := &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 1700000000, LogicalTime: 7}}
	builder, nodeID, projNode, sortNode, scanNode, idxdef := coveredFulltext2Fixture(t, snapshot)

	handled, err := builder.tryApplyCoveredFulltext2(nodeID, projNode, sortNode, scanNode,
		[]int32{0}, []*plan.IndexDef{idxdef}, nil, nil, map[int32]int32{}, nil, nil)
	require.NoError(t, err)
	require.True(t, handled, "the fixture must clear every covered-path guard")

	tvf := findCoveredFulltext2TVF(t, builder)
	require.NotNil(t, tvf.ScanSnapshot, "the covered fulltext2 TVF must carry the base scan's snapshot")
	require.NotNil(t, tvf.ScanSnapshot.TS)
	assert.Equal(t, snapshot.TS.PhysicalTime, tvf.ScanSnapshot.TS.PhysicalTime)
	assert.Equal(t, snapshot.TS.LogicalTime, tvf.ScanSnapshot.TS.LogicalTime)
	assert.NotSame(t, snapshot, tvf.ScanSnapshot, "must be a deep copy, not the scan node's own Snapshot")
	assert.NotSame(t, snapshot.TS, tvf.ScanSnapshot.TS, "the TS must be deep-copied too")
}

// No snapshot on the base scan leaves the TVF node's ScanSnapshot nil.
func TestTryApplyCoveredFulltext2NoSnapshotLeavesTVFUnsnapshotted(t *testing.T) {
	builder, nodeID, projNode, sortNode, scanNode, idxdef := coveredFulltext2Fixture(t, nil)

	handled, err := builder.tryApplyCoveredFulltext2(nodeID, projNode, sortNode, scanNode,
		[]int32{0}, []*plan.IndexDef{idxdef}, nil, nil, map[int32]int32{}, nil, nil)
	require.NoError(t, err)
	require.True(t, handled)

	assert.Nil(t, findCoveredFulltext2TVF(t, builder).ScanSnapshot)
}

// findCoveredFulltext2TVF returns the single FUNCTION_SCAN node appended by the rewrite.
func findCoveredFulltext2TVF(t *testing.T, builder *QueryBuilder) *plan.Node {
	t.Helper()
	var found *plan.Node
	for _, n := range builder.qry.Nodes {
		if n.NodeType == plan.Node_FUNCTION_SCAN {
			require.Nil(t, found, "expected exactly one FUNCTION_SCAN node")
			found = n
		}
	}
	require.NotNil(t, found, "the covered rewrite must append a fulltext2_search FUNCTION_SCAN")
	return found
}
