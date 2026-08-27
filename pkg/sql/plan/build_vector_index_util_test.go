// Copyright 2023 Matrix Origin
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

func vecType(t types.T, dim int32) Type {
	return Type{Id: int32(t), Width: dim}
}

// TestIvfEntryRewritePlan pins the rule that decides whether a DML-projected entry
// needs rewriting. The interesting axis is not "narrow vs wide" but "does the entries
// column carry the trained affine map": int8/uint8 rescale even when the entry type
// equals the base type, while the float formats only narrow.
func TestIvfEntryRewritePlan(t *testing.T) {
	const dim = 4
	cases := []struct {
		name        string
		base, entry types.T
		wantRewrite bool
		wantAffine  bool
	}{
		// No QUANTIZATION, or one that resolves to the base type: nothing to do.
		{"f32 base, f32 entry", types.T_array_float32, types.T_array_float32, false, false},
		{"f64 base, f64 entry", types.T_array_float64, types.T_array_float64, false, false},
		{"f16 base, f16 entry", types.T_array_float16, types.T_array_float16, false, false},
		{"bf16 base, bf16 entry", types.T_array_bf16, types.T_array_bf16, false, false},

		// Float narrowing: a plain cast, no trained bounds needed.
		{"f32 base, f16 entry", types.T_array_float32, types.T_array_float16, true, false},
		{"f32 base, bf16 entry", types.T_array_float32, types.T_array_bf16, true, false},
		{"f64 base, f32 entry", types.T_array_float64, types.T_array_float32, true, false},
		{"f16 base, bf16 entry", types.T_array_float16, types.T_array_bf16, true, false},

		// Affine quantization from a wide base.
		{"f32 base, int8 entry", types.T_array_float32, types.T_array_int8, true, true},
		{"f32 base, uint8 entry", types.T_array_float32, types.T_array_uint8, true, true},
		{"f64 base, int8 entry", types.T_array_float64, types.T_array_int8, true, true},

		// Affine quantization from a NARROW base.
		{"f16 base, int8 entry", types.T_array_float16, types.T_array_int8, true, true},
		{"bf16 base, int8 entry", types.T_array_bf16, types.T_array_int8, true, true},
		{"f16 base, uint8 entry", types.T_array_float16, types.T_array_uint8, true, true},

		// Regression: the entry type EQUALS the base type and it still must be
		// rescaled. Skipping these mixes raw DML rows with scaled build rows in one
		// index -- a silently wrong ranking with no error raised.
		{"int8 base, int8 entry still rescales", types.T_array_int8, types.T_array_int8, true, true},
		{"uint8 base, uint8 entry still rescales", types.T_array_uint8, types.T_array_uint8, true, true},
		{"int8 base, uint8 entry", types.T_array_int8, types.T_array_uint8, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rewrite, affine := ivfEntryRewritePlan(vecType(tc.entry, dim), vecType(tc.base, dim))
			require.Equal(t, tc.wantRewrite, rewrite, "rewrite")
			require.Equal(t, tc.wantAffine, affine, "affine")
		})
	}
}

// A no-op rewrite must be reserved for the float formats. Stated as an invariant over
// every vector element type rather than as a list, so a newly supported type has to
// make a deliberate choice here instead of inheriting "same id means skip".
func TestIvfEntryRewriteNoOpOnlyForFloatFormats(t *testing.T) {
	for _, elem := range []types.T{
		types.T_array_float32, types.T_array_float64, types.T_array_float16,
		types.T_array_bf16, types.T_array_int8, types.T_array_uint8,
	} {
		rewrite, affine := ivfEntryRewritePlan(vecType(elem, 4), vecType(elem, 4))
		isIntegral := elem == types.T_array_int8 || elem == types.T_array_uint8
		require.Equal(t, isIntegral, rewrite, "same-type rewrite for %v", elem)
		require.Equal(t, isIntegral, affine, "same-type affine for %v", elem)
	}
}

func TestIvfEntriesEntryColType(t *testing.T) {
	entries := &TableDef{Cols: []*ColDef{
		{Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_version, Typ: makePlan2Type(&bigIntType)},
		{Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_id, Typ: makePlan2Type(&bigIntType)},
		{Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_pk, Typ: makePlan2Type(&bigIntType)},
		{Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_entry, Typ: vecType(types.T_array_int8, 4)},
	}}
	got, ok := ivfEntriesEntryColType(entries)
	require.True(t, ok)
	require.Equal(t, int32(types.T_array_int8), got.Id)
	require.Equal(t, int32(4), got.Width)

	_, ok = ivfEntriesEntryColType(&TableDef{Cols: []*ColDef{
		{Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_pk, Typ: makePlan2Type(&bigIntType)},
	}})
	require.False(t, ok, "a table without an entry column must not be reported as one")
}

// ivfTestIndexTables builds the metadata / centroids / entries defs that
// makeIvfEntriesQuantizeProject reads, with the entry column typed as entryTyp.
func ivfTestIndexTables(entryTyp Type) ([]*TableDef, []*ObjectRef) {
	varchar := Type{Id: int32(types.T_varchar), Width: 65535}
	meta := &TableDef{
		Name: "meta",
		Cols: []*ColDef{
			{Name: catalog.SystemSI_IVFFLAT_TblCol_Metadata_key, Typ: varchar},
			{Name: catalog.SystemSI_IVFFLAT_TblCol_Metadata_val, Typ: varchar},
		},
	}
	centroids := &TableDef{Name: "centroids", Cols: []*ColDef{
		{Name: catalog.SystemSI_IVFFLAT_TblCol_Centroids_version, Typ: makePlan2Type(&bigIntType)},
	}}
	entries := &TableDef{Name: "entries", Cols: []*ColDef{
		{Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_version, Typ: makePlan2Type(&bigIntType)},
		{Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_id, Typ: makePlan2Type(&bigIntType)},
		{Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_pk, Typ: makePlan2Type(&bigIntType)},
		{Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_entry, Typ: entryTyp},
	}}
	refs := []*ObjectRef{{ObjName: "meta"}, {ObjName: "centroids"}, {ObjName: "entries"}}
	return []*TableDef{meta, centroids, entries}, refs
}

// ivfTestJoinNode fakes the CENTROIDX join output that makeTblCrossJoinL2Centroids
// produces: (version, centroid_id, pk, base vector).
func ivfTestJoinNode(builder *QueryBuilder, bindCtx *BindContext, baseTyp Type) int32 {
	col := func(pos int32, typ Type, name string) *Expr {
		return &Expr{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos, Name: name}}}
	}
	bigint := makePlan2Type(&bigIntType)
	return builder.appendNode(&plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		ObjRef:   &ObjectRef{ObjName: "src"},
		TableDef: &TableDef{Name: "src"},
		ProjectList: []*Expr{
			col(0, bigint, "version"), col(1, bigint, "centroid_id"),
			col(2, bigint, "pk"), col(3, baseTyp, "v"),
		},
	}, bindCtx)
}

func TestMakeIvfEntriesQuantizeProject(t *testing.T) {
	const dim = 4
	t.Run("no-op when the entry already matches the base", func(t *testing.T) {
		builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
		bindCtx := NewBindContext(builder, nil)
		base := vecType(types.T_array_float32, dim)
		defs, refs := ivfTestIndexTables(base)
		in := ivfTestJoinNode(builder, bindCtx, base)
		before := len(builder.qry.Nodes)

		out, err := makeIvfEntriesQuantizeProject(builder, bindCtx, defs, refs, in, base)
		require.NoError(t, err)
		require.Equal(t, in, out, "must return the input node untouched")
		require.Len(t, builder.qry.Nodes, before, "must not add plan nodes")
	})

	t.Run("float narrowing casts without reading the trained bounds", func(t *testing.T) {
		builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
		bindCtx := NewBindContext(builder, nil)
		base := vecType(types.T_array_float32, dim)
		entry := vecType(types.T_array_float16, dim)
		defs, refs := ivfTestIndexTables(entry)
		in := ivfTestJoinNode(builder, bindCtx, base)

		out, err := makeIvfEntriesQuantizeProject(builder, bindCtx, defs, refs, in, base)
		require.NoError(t, err)
		node := builder.qry.Nodes[out]
		require.Equal(t, plan.Node_PROJECT, node.NodeType)
		require.Len(t, node.ProjectList, 4, "entries columns only")
		require.Equal(t, entry.Id, node.ProjectList[ivfEntryProjPos].Typ.Id)
		require.NotNil(t, node.ProjectList[ivfEntryProjPos].GetF(), "entry must be an expression, not a bare column")
		// No metadata scan: the float formats need no trained bounds.
		for _, n := range builder.qry.Nodes {
			require.NotEqual(t, plan.Node_AGG, n.NodeType, "float narrowing must not read metadata")
		}
	})

	for _, tc := range []struct {
		name  string
		entry types.T
	}{{"int8", types.T_array_int8}, {"uint8", types.T_array_uint8}} {
		t.Run("affine "+tc.name+" joins the trained bounds and trims them again", func(t *testing.T) {
			builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
			bindCtx := NewBindContext(builder, nil)
			base := vecType(types.T_array_float32, dim)
			entry := vecType(tc.entry, dim)
			defs, refs := ivfTestIndexTables(entry)
			in := ivfTestJoinNode(builder, bindCtx, base)

			out, err := makeIvfEntriesQuantizeProject(builder, bindCtx, defs, refs, in, base)
			require.NoError(t, err)
			node := builder.qry.Nodes[out]
			require.Equal(t, plan.Node_PROJECT, node.NodeType)
			// qmin/qmax fed the entry expression and must not reach the entries table.
			require.Len(t, node.ProjectList, 4, "bound columns must be trimmed off")
			require.Equal(t, entry.Id, node.ProjectList[ivfEntryProjPos].Typ.Id)

			// The bounds arrive through a scalar aggregate, which yields one row even
			// when neither bound key is present, so the join cannot drop entry rows.
			var agg, join int
			for _, n := range builder.qry.Nodes {
				switch n.NodeType {
				case plan.Node_AGG:
					agg++
					require.Empty(t, n.GroupBy, "must be a scalar aggregate")
					require.Len(t, n.AggList, 2, "quantize_min and quantize_max")
				case plan.Node_JOIN:
					join++
				}
			}
			require.Equal(t, 1, agg)
			require.Equal(t, 1, join)
		})
	}

	t.Run("entries table without an entry column is an error", func(t *testing.T) {
		builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
		bindCtx := NewBindContext(builder, nil)
		base := vecType(types.T_array_float32, dim)
		defs, refs := ivfTestIndexTables(base)
		defs[2].Cols = defs[2].Cols[:3]
		in := ivfTestJoinNode(builder, bindCtx, base)

		_, err := makeIvfEntriesQuantizeProject(builder, bindCtx, defs, refs, in, base)
		require.Error(t, err)
	})
}
