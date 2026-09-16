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
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
	mp := mpool.MustNew(t.Name())
	filter, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "in", []*planpb.Expr{
		GetColExpr(baseDef.Cols[1].Typ, baseTag, 1),
		MakePlan2StringVecExprWithType(mp, "same", "other"),
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
	require.Equal(t, "prefix_in", inner.FilterList[0].GetF().Func.ObjName)
	prefixValues := inner.FilterList[0].GetF().Args[1].GetVec()
	require.Equal(t, int32(2), prefixValues.GetLen(), "LiteralVec.Len must be the logical element count")
	require.True(t, prefixValues.GetIsSerialized())
	require.Nil(t, builder.qry.Nodes[scanID].Limit)
	require.Nil(t, builder.qry.Nodes[scanID].Offset)
}

// The prefix_in payload is binary-searched by zone-map pruning (ZM.PrefixIn, via
// colexec.zoneMapInVector), and an IN list arrives in whatever order it was
// written. Published unsorted, the consumer refuses to prune and every block is
// scanned -- correct, but the index buys nothing.
func TestMasterIndexPublishesSortedPrefixPayload(t *testing.T) {
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
	mp := mpool.MustNew(t.Name())
	// Descending on purpose: the encoded order follows the values here, since every
	// needle carries the same column-sequence prefix.
	filter, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "in", []*planpb.Expr{
		GetColExpr(baseDef.Cols[1].Typ, baseTag, 1),
		MakePlan2StringVecExprWithType(mp, "z", "m", "a"),
	})
	require.NoError(t, err)

	scanID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_TABLE_SCAN,
		ObjRef:      &planpb.ObjectRef{SchemaName: "test", ObjName: "t"},
		TableDef:    baseDef,
		BindingTags: []int32{baseTag},
		FilterList:  []*planpb.Expr{filter},
	}, ctx)

	outerID := builder.applyIndicesForFiltersUsingMasterIndex(scanID, builder.qry.Nodes[scanID], &planpb.IndexDef{
		IndexName:      "idx_master",
		IndexAlgo:      catalog.MOIndexMasterAlgo.ToString(),
		IndexTableName: "__mo_master_idx",
		Parts:          []string{"a"},
		TableExist:     true,
	})

	inner := builder.qry.Nodes[builder.qry.Nodes[outerID].Children[1]]
	require.Equal(t, "prefix_in", inner.FilterList[0].GetF().Func.ObjName)
	payload := inner.FilterList[0].GetF().Args[1].GetVec()

	got := vector.NewVec(types.T_any.ToType())
	defer got.Free(mp)
	require.NoError(t, got.UnmarshalBinary(payload.Data))

	require.True(t, got.GetSorted(), "payload must be flagged so pruning is allowed to use it")
	require.Equal(t, int32(got.Length()), payload.GetLen(), "Len must match the compacted payload")

	col, area := vector.MustVarlenaRawData(got)
	for i := 1; i < len(col); i++ {
		require.LessOrEqual(t,
			bytes.Compare(col[i-1].GetByteSlice(area), col[i].GetByteSlice(area)), 0,
			"needles must ascend in the byte order PrefixIn searches")
	}
}

func TestMasterIndexNative0900FilterUsesPhysicalKey(t *testing.T) {
	compCtx := NewEmptyCompilerContext()
	compCtx.isDml = true
	compCtx.objects["__mo_master_idx"] = &planpb.ObjectRef{SchemaName: "test", ObjName: "__mo_master_idx"}
	compCtx.tables["__mo_master_idx"] = &planpb.TableDef{
		Name: "__mo_master_idx",
		Cols: []*planpb.ColDef{
			{Name: catalog.IndexTableIndexColName, Typ: planpb.Type{Id: int32(types.T_varbinary), Width: types.MaxVarBinaryLen}, Seqnum: 0},
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
	native := planpb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8MB40900AI)}
	baseDef := &planpb.TableDef{
		Name: "t",
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}, Seqnum: 0},
			{Name: "a", Typ: native, Seqnum: 1},
		},
		Name2ColIndex: map[string]int32{"id": 0, "a": 1},
		Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
	}
	column := GetColExpr(native, baseTag, 1)
	literal := MakePlan2StringConstExprWithType("Alpha")
	literal.Typ = native
	columnKey, err := makeNativeCollationKeyExpr(builder.GetContext(), column, native)
	require.NoError(t, err)
	literalKey, err := makeNativeCollationKeyExpr(builder.GetContext(), literal, native)
	require.NoError(t, err)
	filter, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*planpb.Expr{columnKey, literalKey})
	require.NoError(t, err)

	scanID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_TABLE_SCAN,
		ObjRef:      &planpb.ObjectRef{SchemaName: "test", ObjName: "t"},
		TableDef:    baseDef,
		BindingTags: []int32{baseTag},
		FilterList:  []*planpb.Expr{filter},
	}, ctx)
	outerID := builder.applyIndicesForFiltersUsingMasterIndex(scanID, builder.qry.Nodes[scanID], &planpb.IndexDef{
		IndexName:      "idx_master",
		IndexAlgo:      catalog.MOIndexMasterAlgo.ToString(),
		IndexTableName: "__mo_master_idx",
		Parts:          []string{"a"},
		TableExist:     true,
	})

	inner := builder.qry.Nodes[builder.qry.Nodes[outerID].Children[1]]
	require.NotNil(t, inner)
	require.NotEmpty(t, inner.FilterList)
	lookup := inner.FilterList[0]
	require.NotNil(t, lookup)
	require.NotNil(t, lookup.GetF())
	require.Equal(t, "prefix_eq", lookup.GetF().Func.ObjName)
	require.Equal(t, types.T_varbinary, types.T(lookup.GetF().Args[0].Typ.Id))
	require.True(t, exprContainsFuncName(lookup.GetF().Args[1], "serial_full"))
}
