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

// Plan-side half of the named-snapshot MATCH fix (#27941) for the fulltext2 COVERED fast
// path. That path is the one fulltext2 rewrite that drops the base-table JOIN and reads
// pk/score/include straight off the TVF, so it builds its FUNCTION_SCAN node in its own
// place -- and therefore needs its own copy of the snapshot hand-off. Miss it and a
// `{snapshot=...} MATCH` on a fully-covered projection silently reads the CURRENT index
// while the rest of the query time-travels.
//
// The JOIN path's twin of this is exercised by the fulltext BVT cases; this one has no
// unit-test fixture upstream of it, hence the hand-built one below.

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
// guard: one MATCH filter driving one fulltext2 index with an INCLUDE column, no residual
// predicate, and a projection of nothing but the pk (so coverage guard (d) holds). snapshot
// is attached to the BASE SCAN, which is where a `{snapshot=...}` query puts it.
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
			// The storage/metadata sibling pair buildFulltext2SearchCfg resolves the index by.
			Indexes: []*plan.IndexDef{
				{IndexName: "ft2idx", IndexAlgoTableType: catalog.FullText2Index_TblType_Storage, IndexTableName: "__store"},
				{IndexName: "ft2idx", IndexAlgoTableType: catalog.FullText2Index_TblType_Metadata, IndexTableName: "__meta"},
			},
		},
		BindingTags:  []int32{scanTag},
		ScanSnapshot: snapshot,
		// The single MATCH filter: match(body, 'x' IN NATURAL LANGUAGE MODE).
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
	// Pre-extend ctxByNode for the FUNCTION_SCAN / SORT nodes the rewrite appends.
	for i := 0; i < 10; i++ {
		builder.ctxByNode = append(builder.ctxByNode, bindCtx)
	}

	projNode = &plan.Node{
		NodeType: plan.Node_PROJECT,
		Children: []int32{nodeID},
		// pk only => fully covered.
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

// The covered fast path must hand the base scan's snapshot to the fulltext2_search TVF it
// builds, as a DEEP copy -- the TVF node must not alias the scan node's Snapshot, or a later
// rewrite of one would silently retarget the other's read.
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

// No snapshot on the base scan => none on the TVF, so the MATCH keeps reading the current
// index (DeepCopySnapshot(nil) is nil).
func TestTryApplyCoveredFulltext2NoSnapshotLeavesTVFUnsnapshotted(t *testing.T) {
	builder, nodeID, projNode, sortNode, scanNode, idxdef := coveredFulltext2Fixture(t, nil)

	handled, err := builder.tryApplyCoveredFulltext2(nodeID, projNode, sortNode, scanNode,
		[]int32{0}, []*plan.IndexDef{idxdef}, nil, nil, map[int32]int32{}, nil, nil)
	require.NoError(t, err)
	require.True(t, handled)

	assert.Nil(t, findCoveredFulltext2TVF(t, builder).ScanSnapshot)
}

// findCoveredFulltext2TVF returns the single FUNCTION_SCAN node the covered rewrite appended.
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
