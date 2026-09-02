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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan-side half of the named-snapshot vector search fix (#27927) for the two GPU
// algorithms. The TVF reads its hidden index tables through nested internal SQL, which
// runs at the CURRENT txn unless the snapshot TS reaches it; the only path from the
// `{snapshot=...}` scan to the TVF is Node.ScanSnapshot on the FUNCTION_SCAN node the
// optimizer appends here. These tests pin that hand-off for IVF-PQ and CAGRA:
// present-and-deep-copied when the base scan is snapshotted, and absent otherwise (so a
// non-snapshot query keeps reading the current index).
//
// The pushdown builders are GPU-agnostic Go (apply_indices_ivfpq.go / _cagra.go have no
// build tag), so this runs in the ordinary CPU test suite; the GPU-tagged half — that the
// TVF turns this field into a time-travelling read plus a TS-suffixed cache key — lives in
// pkg/sql/colexec/table_function/vector_search_snapshot_gpu_test.go.

// gpuVectorSnapshotFixture builds the scan/sort/project shape both applyIndicesForSortUsing*
// entry points expect, matching the existing _Success fixtures in
// apply_indices_ivfpq_test.go / apply_indices_cagra_test.go. snapshot is attached to the
// BASE SCAN node, which is where a `{snapshot=...}` query puts it.
func gpuVectorSnapshotFixture(
	t *testing.T, threadsVar string, windowVar string, snapshot *plan.Snapshot,
) (*QueryBuilder, int32, *vectorSortContext) {
	t.Helper()

	mock := &customMockCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		resolveVarFunc: func(name string, isSys, isGlobal bool) (interface{}, error) {
			switch name {
			case threadsVar:
				return int64(4), nil
			case windowVar:
				return int64(64), nil
			case "probe_limit":
				return int64(10), nil
			}
			return int64(0), nil
		},
	}
	builder := NewQueryBuilder(plan.Query_SELECT, mock, false, true)
	bindCtx := NewBindContext(builder, nil)

	tableDef := &plan.TableDef{
		Name: "t",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}},
			{Name: "v", Typ: plan.Type{Id: int32(types.T_array_float32)}},
		},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id"},
		Name2ColIndex: map[string]int32{"id": 0, "v": 1},
	}
	scanNode := &plan.Node{
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     tableDef,
		ObjRef:       &plan.ObjectRef{SchemaName: "db"},
		BindingTags:  []int32{builder.genNewBindTag()},
		ScanSnapshot: snapshot,
	}
	scanNodeID := builder.appendNode(scanNode, bindCtx)

	// Pre-extend ctxByNode for the JOIN/SORT/FUNCTION_SCAN nodes appended below.
	for i := 0; i < 30; i++ {
		builder.ctxByNode = append(builder.ctxByNode, bindCtx)
	}

	vecTyp := plan.Type{Id: int32(types.T_array_float32)}
	vecCtx := &vectorSortContext{
		scanNode: scanNode,
		sortNode: &plan.Node{NodeType: plan.Node_SORT, Offset: &plan.Expr{}},
		projNode: &plan.Node{
			NodeType: plan.Node_PROJECT,
			Children: []int32{scanNodeID},
			ProjectList: []*plan.Expr{
				{Typ: vecTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanNode.BindingTags[0], ColPos: 1}}},
			},
		},
		distFnExpr: &plan.Function{
			Func: &ObjectRef{ObjName: "l2_distance"},
			Args: []*plan.Expr{
				{Typ: vecTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanNode.BindingTags[0], ColPos: 1}}},
				{Typ: vecTyp, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[1,1,1]"}}}},
			},
		},
		orderExpr: &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_float64)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
		},
		limit:       &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}}}},
		resultLimit: makePlan2Uint64ConstExprWithType(10),
		rankOption:  &plan.RankOption{Mode: "pre"},
	}
	return builder, scanNodeID, vecCtx
}

func gpuVectorSnapshotMTI(algo, metaType, storageType string) *MultiTableIndex {
	idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`
	return &MultiTableIndex{
		IndexAlgo: algo,
		IndexDefs: map[string]*plan.IndexDef{
			metaType:    {IndexTableName: "meta", IndexAlgoParams: idxAlgoParams},
			storageType: {IndexTableName: "idx", Parts: []string{"v"}, IndexAlgoParams: idxAlgoParams},
		},
	}
}

// findVectorSearchTVF walks from the rewritten PROJECT down to the FUNCTION_SCAN the
// pushdown appended (PROJECT → SORT → JOIN(SCAN, FUNCTION_SCAN)).
func findVectorSearchTVF(t *testing.T, builder *QueryBuilder, vecCtx *vectorSortContext) *plan.Node {
	t.Helper()
	sort := builder.qry.Nodes[vecCtx.projNode.Children[0]]
	require.Equal(t, plan.Node_SORT, sort.NodeType)
	join := builder.qry.Nodes[sort.Children[0]]
	require.Equal(t, plan.Node_JOIN, join.NodeType)
	tvf := builder.qry.Nodes[join.Children[1]]
	require.Equal(t, plan.Node_FUNCTION_SCAN, tvf.NodeType)
	return tvf
}

func gpuVectorTestSnapshot() *plan.Snapshot {
	return &plan.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: 1700000000, LogicalTime: 7},
	}
}

// A snapshotted base scan must hand its read TS to the ivfpq_search TVF, as a DEEP COPY:
// the TVF node must not alias the scan node's Snapshot, or a later rewrite of one would
// silently retarget the other's read.
func TestApplyIndicesForSortUsingIvfpq_PropagatesScanSnapshot(t *testing.T) {
	snapshot := gpuVectorTestSnapshot()
	builder, scanNodeID, vecCtx := gpuVectorSnapshotFixture(
		t, "ivfpq_threads_search", "ivfpq_batch_window", snapshot)

	_, err := builder.applyIndicesForSortUsingIvfpq(scanNodeID, vecCtx,
		gpuVectorSnapshotMTI(catalog.MoIndexIvfpqAlgo.ToString(),
			catalog.Ivfpq_TblType_Metadata, catalog.Ivfpq_TblType_Storage), nil)
	require.NoError(t, err)

	tvf := findVectorSearchTVF(t, builder, vecCtx)
	require.NotNil(t, tvf.ScanSnapshot, "ivfpq_search TVF must carry the base scan's snapshot")
	require.NotNil(t, tvf.ScanSnapshot.TS)
	assert.Equal(t, snapshot.TS.PhysicalTime, tvf.ScanSnapshot.TS.PhysicalTime)
	assert.Equal(t, snapshot.TS.LogicalTime, tvf.ScanSnapshot.TS.LogicalTime)
	assert.NotSame(t, snapshot, tvf.ScanSnapshot, "must be a deep copy, not the scan node's own Snapshot")
	assert.NotSame(t, snapshot.TS, tvf.ScanSnapshot.TS, "the TS must be deep-copied too")
}

// No snapshot on the base scan => none on the TVF, so the search keeps reading the
// current index (DeepCopySnapshot(nil) is nil).
func TestApplyIndicesForSortUsingIvfpq_NoSnapshotLeavesTVFUnsnapshotted(t *testing.T) {
	builder, scanNodeID, vecCtx := gpuVectorSnapshotFixture(
		t, "ivfpq_threads_search", "ivfpq_batch_window", nil)

	_, err := builder.applyIndicesForSortUsingIvfpq(scanNodeID, vecCtx,
		gpuVectorSnapshotMTI(catalog.MoIndexIvfpqAlgo.ToString(),
			catalog.Ivfpq_TblType_Metadata, catalog.Ivfpq_TblType_Storage), nil)
	require.NoError(t, err)

	assert.Nil(t, findVectorSearchTVF(t, builder, vecCtx).ScanSnapshot)
}

// CAGRA twin of TestApplyIndicesForSortUsingIvfpq_PropagatesScanSnapshot.
func TestApplyIndicesForSortUsingCagra_PropagatesScanSnapshot(t *testing.T) {
	snapshot := gpuVectorTestSnapshot()
	builder, scanNodeID, vecCtx := gpuVectorSnapshotFixture(
		t, "cagra_threads_search", "cagra_batch_window", snapshot)

	_, err := builder.applyIndicesForSortUsingCagra(scanNodeID, vecCtx,
		gpuVectorSnapshotMTI(catalog.MoIndexCagraAlgo.ToString(),
			catalog.Cagra_TblType_Metadata, catalog.Cagra_TblType_Storage), nil)
	require.NoError(t, err)

	tvf := findVectorSearchTVF(t, builder, vecCtx)
	require.NotNil(t, tvf.ScanSnapshot, "cagra_search TVF must carry the base scan's snapshot")
	require.NotNil(t, tvf.ScanSnapshot.TS)
	assert.Equal(t, snapshot.TS.PhysicalTime, tvf.ScanSnapshot.TS.PhysicalTime)
	assert.Equal(t, snapshot.TS.LogicalTime, tvf.ScanSnapshot.TS.LogicalTime)
	assert.NotSame(t, snapshot, tvf.ScanSnapshot, "must be a deep copy, not the scan node's own Snapshot")
	assert.NotSame(t, snapshot.TS, tvf.ScanSnapshot.TS, "the TS must be deep-copied too")
}

// CAGRA twin of TestApplyIndicesForSortUsingIvfpq_NoSnapshotLeavesTVFUnsnapshotted.
func TestApplyIndicesForSortUsingCagra_NoSnapshotLeavesTVFUnsnapshotted(t *testing.T) {
	builder, scanNodeID, vecCtx := gpuVectorSnapshotFixture(
		t, "cagra_threads_search", "cagra_batch_window", nil)

	_, err := builder.applyIndicesForSortUsingCagra(scanNodeID, vecCtx,
		gpuVectorSnapshotMTI(catalog.MoIndexCagraAlgo.ToString(),
			catalog.Cagra_TblType_Metadata, catalog.Cagra_TblType_Storage), nil)
	require.NoError(t, err)

	assert.Nil(t, findVectorSearchTVF(t, builder, vecCtx).ScanSnapshot)
}
