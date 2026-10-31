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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestSingleColumnIndexFoldedInPredicate(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, true, false)
	filter := makeExprOptInExpr(t, ctx, 0, 1, 2, 3)
	filter.GetF().Args[1] = MakePlan2Int64VecExprWithType(ctx.GetProcess().Mp(), 2, 3)
	filter.GetF().Args[1].Typ.Id = int32(types.T_tuple)
	index := &planpb.IndexDef{Parts: []string{"b"}, Unique: true}
	table := &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "key", Typ: planpb.Type{Id: int32(types.T_int64)}}}}
	rewritten, err := builder.replaceNonEqualCondition(index, filter, 42, table)
	require.NoError(t, err)
	require.Equal(t, "in", rewritten.GetF().Func.ObjName)
	require.Equal(t, filter.GetF().Func.Obj, rewritten.GetF().Func.Obj)
	require.Equal(t, int32(42), rewritten.GetF().Args[0].GetCol().RelPos)
	require.Equal(t, int32(0), rewritten.GetF().Args[0].GetCol().ColPos)
	require.NotNil(t, rewritten.GetF().Args[1].GetVec())
}

func TestIndexOnlyConstructionFailureKeepsOriginalScan(t *testing.T) {
	compiler := NewMockCompilerContext(true, newPlanTestProcess(t))
	builder := NewQueryBuilder(planpb.Query_SELECT, compiler, false, true)
	ctx := NewBindContext(builder, nil)
	tag := builder.genNewBindTag()
	idx := &planpb.IndexDef{IndexName: "idx_status", IndexTableName: "idx_status_table", Parts: []string{"status", "id"}, TableExist: true}
	registerMockIndexTable(t, builder, idx.IndexTableName)
	// The optimizer can select the candidate, but inconsistent index metadata
	// makes its serialized comparison impossible to bind.
	compiler.tables[idx.IndexTableName].Cols[0].Typ = planpb.Type{Id: int32(types.T_json)}
	node := &planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN, ObjRef: &planpb.ObjectRef{SchemaName: "test", ObjName: "t"}, BindingTags: []int32{tag},
		TableDef: &planpb.TableDef{Name: "t", Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "status", Typ: planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
		}, Name2ColIndex: map[string]int32{"id": 0, "status": 1}, Pkey: &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}, Indexes: []*planpb.IndexDef{idx}},
		Stats:      &planpb.Stats{TableCnt: 100, Outcnt: 1, Selectivity: .01, Cost: 100},
		FilterList: []*planpb.Expr{makeStringEqFilterExpr(tag, 1, "active")},
	}
	id := builder.appendNode(node, ctx)
	colMap := make(map[[2]int32]*planpb.Expr)
	refs := map[[2]int32]int{{tag, 1}: 1}
	match, ok := builder.matchRegularIndexOnlyScan(idx, node, builder.newEncodedRegularIndexCostContext(node, refs))
	require.True(t, ok)
	require.Equal(t, int32(-1), builder.applyRegularIndexOnlyScan(idx, node, colMap, &Snapshot{}, match))
	got, indexErr := builder.applyIndicesForFiltersRegularIndex(id, node, refs, colMap)
	require.NoError(t, indexErr)
	require.Equal(t, id, got)
	require.Len(t, builder.qry.Nodes, 1)
	require.Empty(t, colMap)
	require.Equal(t, "=", node.FilterList[0].GetF().Func.ObjName)
}

func TestApplyIndicesReturnsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	compiler := NewMockCompilerContext(true, newPlanTestProcess(t))
	compiler.SetContext(ctx)
	builder := NewQueryBuilder(planpb.Query_SELECT, compiler, false, false)
	nodeID := builder.appendNode(&planpb.Node{NodeType: planpb.Node_VALUE_SCAN}, NewBindContext(builder, nil))
	_, err := builder.applyIndices(nodeID, nil, nil)
	require.ErrorIs(t, err, context.Canceled)
}
