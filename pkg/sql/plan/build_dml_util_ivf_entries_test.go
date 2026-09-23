// Copyright 2025 Matrix Origin
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
	"github.com/stretchr/testify/require"
)

// ivfDMLFixture builds the three index tables and the base table that
// appendPreInsertSkVectorPlan reads, with the entries column typed as entryTyp.
func ivfDMLFixture(entryTyp Type) (*TableDef, []*TableDef, []*ObjectRef, *MultiTableIndex) {
	bigint := makePlan2Type(&bigIntType)
	intTyp := types.T_int32.ToType()
	varchar := Type{Id: int32(types.T_varchar), Width: 255}

	base := &TableDef{
		Name: "t",
		Cols: []*ColDef{
			{Name: "id", Typ: makePlan2Type(&intTyp), Primary: true},
			{Name: "v", Typ: Type{Id: int32(types.T_array_float32), Width: 4}},
		},
		Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
	}
	meta := &TableDef{Name: "meta", Cols: []*ColDef{
		{Name: catalog.SystemSI_IVFFLAT_TblCol_Metadata_key, Typ: varchar},
		{Name: catalog.SystemSI_IVFFLAT_TblCol_Metadata_val, Typ: varchar},
	}}
	centroids := &TableDef{Name: "centroids", Cols: []*ColDef{
		{Name: catalog.SystemSI_IVFFLAT_TblCol_Centroids_version, Typ: bigint},
		{Name: catalog.SystemSI_IVFFLAT_TblCol_Centroids_id, Typ: bigint},
		{Name: catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
			Typ: Type{Id: int32(types.T_array_float32), Width: 4}},
	}}
	entries := &TableDef{Name: "entries", Cols: []*ColDef{
		{Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_version, Typ: bigint},
		{Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_id, Typ: bigint},
		{Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_pk, Typ: makePlan2Type(&intTyp)},
		{Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_entry, Typ: entryTyp},
		{Name: catalog.CPrimaryKeyColName, Typ: varchar},
	}}
	// appendLockNode reads the entries pkey; the real entries table is keyed by the
	// (version, centroid_id, pk) composite that makeFinalProject serializes.
	entries.Pkey = &plan.PrimaryKeyDef{
		Names: []string{
			catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
			catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
			catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
		},
		PkeyColName: catalog.CPrimaryKeyColName,
		CompPkeyCol: &ColDef{Name: catalog.CPrimaryKeyColName, Typ: varchar},
	}

	params := `{"lists":"1","op_type":"vector_l2_ops"}`
	mti := &MultiTableIndex{
		IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(),
		IndexDefs: map[string]*plan.IndexDef{
			catalog.SystemSI_IVFFLAT_TblType_Metadata: {
				IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Metadata,
				IndexAlgoParams:    params, Parts: []string{"v"},
			},
			catalog.SystemSI_IVFFLAT_TblType_Centroids: {
				IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Centroids,
				IndexAlgoParams:    params, Parts: []string{"v"},
			},
			catalog.SystemSI_IVFFLAT_TblType_Entries: {
				IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
				IndexAlgoParams:    params, Parts: []string{"v"},
			},
		},
	}
	refs := []*ObjectRef{
		{SchemaName: "db", ObjName: "meta"},
		{SchemaName: "db", ObjName: "centroids"},
		{SchemaName: "db", ObjName: "entries"},
	}
	return base, []*TableDef{meta, centroids, entries}, refs, mti
}

// The DML plan must route the projected entry through the quantizer. Without the
// rewrite the base value is written verbatim into a narrower entries column -- a
// vecf32(4) landing as a 16-element vecint8 -- and the next search fails with
// "vector dimension not matched" (#27732).
func TestAppendPreInsertSkVectorPlanQuantizesEntry(t *testing.T) {
	run := func(t *testing.T, entryTyp Type) *Expr {
		builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
		bindCtx := NewBindContext(builder, nil)
		base, defs, refs, mti := ivfDMLFixture(entryTyp)

		src := builder.appendNode(&plan.Node{
			NodeType: plan.Node_TABLE_SCAN,
			ObjRef:   &ObjectRef{SchemaName: "db", ObjName: "t"},
			TableDef: base,
			ProjectList: []*Expr{
				{Typ: base.Cols[0].Typ, Expr: &plan.Expr_Col{
					Col: &plan.ColRef{ColPos: 0, Name: "id"}}},
				{Typ: base.Cols[1].Typ, Expr: &plan.Expr_Col{
					Col: &plan.ColRef{ColPos: 1, Name: "v"}}},
			},
		}, bindCtx)

		step, err := appendPreInsertSkVectorPlan(builder, bindCtx, base, src, mti, false, refs, defs)
		require.NoError(t, err)
		require.GreaterOrEqual(t, step, int32(0))

		// The entry the plan will insert: the projection slot that carries it, produced
		// AS the entries column type rather than the base type.
		for _, n := range builder.qry.Nodes {
			if n.NodeType != plan.Node_PROJECT || len(n.ProjectList) <= ivfEntryProjPos {
				continue
			}
			if e := n.ProjectList[ivfEntryProjPos]; e.Typ.Id == entryTyp.Id {
				return e
			}
		}
		require.Fail(t, "no projection produces the entry as the entries column type")
		return nil
	}

	t.Run("int8 entries are rescaled by the trained quantizer", func(t *testing.T) {
		entry := run(t, Type{Id: int32(types.T_array_int8), Width: 4})
		require.NotNil(t, entry.GetF(), "an expression, not the base column projected verbatim")
	})

	t.Run("float16 entries are narrowed by a cast", func(t *testing.T) {
		entry := run(t, Type{Id: int32(types.T_array_float16), Width: 4})
		require.NotNil(t, entry.GetF())
	})

	t.Run("a same-width float entry is left alone", func(t *testing.T) {
		// No QUANTIZATION: the verbatim projection already matches the column, and the
		// plan must not grow a metadata scan or a bounds join for nothing.
		builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
		bindCtx := NewBindContext(builder, nil)
		base, defs, refs, mti := ivfDMLFixture(Type{Id: int32(types.T_array_float32), Width: 4})
		src := builder.appendNode(&plan.Node{
			NodeType: plan.Node_TABLE_SCAN,
			ObjRef:   &ObjectRef{SchemaName: "db", ObjName: "t"},
			TableDef: base,
			ProjectList: []*Expr{
				{Typ: base.Cols[0].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: "id"}}},
				{Typ: base.Cols[1].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1, Name: "v"}}},
			},
		}, bindCtx)
		_, err := appendPreInsertSkVectorPlan(builder, bindCtx, base, src, mti, false, refs, defs)
		require.NoError(t, err)
		aggs := 0
		for _, n := range builder.qry.Nodes {
			if n.NodeType == plan.Node_AGG {
				aggs++
			}
		}
		require.Zero(t, aggs, "an unquantized index must not read the quantizer bounds")
	})
}
