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
	"github.com/stretchr/testify/require"
)

func TestMasterIndexPaginationIsAppliedOnce(t *testing.T) {
	compCtx := NewEmptyCompilerContext()
	compCtx.isDml = true
	compCtx.objects["__mo_master_idx"] = &planpb.ObjectRef{SchemaName: "test", ObjName: "__mo_master_idx"}
	compCtx.tables["__mo_master_idx"] = &planpb.TableDef{
		Name: "__mo_master_idx",
		Cols: []*planpb.ColDef{
			{Name: catalog.IndexTableIndexColName, Typ: planpb.Type{Id: int32(types.T_varchar)}, Seqnum: 0},
			{Name: catalog.IndexTablePrimaryColName, Typ: planpb.Type{Id: int32(types.T_int64)}, Seqnum: 1},
		},
		Name2ColIndex: map[string]int32{
			catalog.IndexTableIndexColName:   0,
			catalog.IndexTablePrimaryColName: 1,
		},
	}

	builder := NewQueryBuilder(planpb.Query_SELECT, compCtx, false, true)
	ctx := NewBindContext(builder, nil)
	baseTag := builder.genNewBindTag()
	baseDef := &planpb.TableDef{
		Name: "t",
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}, Seqnum: 0},
			{Name: "a", Typ: planpb.Type{Id: int32(types.T_varchar)}, Seqnum: 1},
		},
		Name2ColIndex: map[string]int32{"id": 0, "a": 1},
		Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
	}
	filter, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*planpb.Expr{
		GetColExpr(baseDef.Cols[1].Typ, baseTag, 1),
		makePlan2StringConstExprWithType("same", false),
	})
	require.NoError(t, err)

	scanID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_TABLE_SCAN,
		ObjRef:      &planpb.ObjectRef{SchemaName: "test", ObjName: "t"},
		TableDef:    baseDef,
		BindingTags: []int32{baseTag},
		FilterList:  []*planpb.Expr{filter},
		Limit:       makePlan2Uint64ConstExprWithType(10),
		Offset:      makePlan2Uint64ConstExprWithType(5),
	}, ctx)

	outerID := builder.applyIndicesForFiltersUsingMasterIndex(scanID, builder.qry.Nodes[scanID], &planpb.IndexDef{
		IndexName:      "idx_master",
		IndexAlgo:      catalog.MOIndexMasterAlgo.ToString(),
		IndexTableName: "__mo_master_idx",
		Parts:          []string{"a"},
		TableExist:     true,
	})

	outer := builder.qry.Nodes[outerID]
	require.Equal(t, planpb.Node_INDEX, outer.JoinType)
	require.Equal(t, uint64(10), outer.Limit.GetLit().GetU64Val())
	require.Equal(t, uint64(5), outer.Offset.GetLit().GetU64Val())

	inner := builder.qry.Nodes[outer.Children[1]]
	require.Equal(t, uint64(15), inner.Limit.GetLit().GetU64Val(), "inner index path must fetch LIMIT+OFFSET candidates")
	require.Nil(t, inner.Offset, "the user-visible OFFSET must only be consumed by the outer result")
	require.Nil(t, builder.qry.Nodes[scanID].Limit)
	require.Nil(t, builder.qry.Nodes[scanID].Offset)
}
