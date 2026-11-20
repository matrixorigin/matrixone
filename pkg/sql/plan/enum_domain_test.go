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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestEnumDomainPlanRoundTrip(t *testing.T) {
	for _, decl := range []string{"enum('a,b','c')", "enum('')", "enum('a','b')", "enum('2#3:a','b1:c')"} {
		t.Run(decl, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), "create table t(e "+decl+")", 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			ast := stmt.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef).Type
			typ, err := getTypeFromAst(t.Context(), ast)
			require.NoError(t, err)
			formatted, err := FormatColType(typ)
			require.NoError(t, err)
			replay, err := mysql.ParseOne(t.Context(), "create table t(e "+formatted+")", 1)
			require.NoError(t, err)
			t.Cleanup(replay.Free)
			got, err := getTypeFromAst(t.Context(), replay.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef).Type)
			require.NoError(t, err)
			require.Equal(t, typ, got)
			copied := DeepCopyType(&typ)
			data, err := copied.Marshal()
			require.NoError(t, err)
			var wire plan.Type
			require.NoError(t, wire.Unmarshal(data))
			require.Equal(t, typ, wire)
			native := MakeTypeByPlan2Type(wire)
			require.Equal(t, typ.EnumEncoding, native.GetEnumEncoding())
			data, err = native.Marshal()
			require.NoError(t, err)
			var read types.Type
			require.NoError(t, read.Unmarshal(data))
			restored := MakePlan2TypeValue(&read)
			restored.Enumvalues = typ.Enumvalues
			require.True(t, isSameColumnType(typ, restored))
			attrs := engine.PlanColsToExeCols([]*plan.ColDef{{Name: "e", Typ: wire}})
			attr := attrs[0].(*engine.AttributeDef).Attr
			require.Equal(t, typ.EnumEncoding, attr.Type.GetEnumEncoding())
			require.Equal(t, typ.Enumvalues, attr.EnumVlaues)
			ivfType, err := ivfPlanTypeToTreeType(typ)
			require.NoError(t, err)
			members, err := types.DecodeEnumMembers(typ.Enumvalues, typ.EnumEncoding)
			require.NoError(t, err)
			require.Equal(t, []string(members), ivfType.InternalType.EnumValues)
			require.Equal(t, "1", buildNotNullColumnVal(&plan.ColDef{Typ: typ}))
		})
	}
	legacy := plan.Type{Id: int32(types.T_enum), Enumvalues: "2#3:a,b1:c"}
	framed := legacy
	framed.EnumEncoding = 1
	require.False(t, isSameColumnType(legacy, framed))
	require.False(t, samePlanType(legacy, framed))
	require.False(t, mysqlSpecialOrderTypesCompatible(&legacy, &framed))
}

func TestEnumDisplayWrapperEncoding(t *testing.T) {
	for _, typ := range []plan.Type{
		{Id: int32(types.T_enum), Enumvalues: "a,b"},
		{Id: int32(types.T_enum), Enumvalues: "2#3:a,b1:c", EnumEncoding: 1},
	} {
		raw := &plan.Expr{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
		display, err := makeEnumOrSetDisplayValue(t.Context(), raw)
		require.NoError(t, err)
		wantArgs := 2
		if typ.EnumEncoding != 0 {
			wantArgs = 3
		}
		require.Len(t, display.GetF().Args, wantArgs)
		restored, ok := storedMySQLSpecialTypeExpr(display)
		require.True(t, ok)
		require.Equal(t, raw, restored)
		malformed := DeepCopyExpr(display)
		malformed.GetF().Args[0] = makePlan2StringConstExprWithType("2#9:bad")
		_, ok = storedMySQLSpecialTypeExpr(malformed)
		require.False(t, ok)
		dynamic := DeepCopyExpr(display)
		dynamic.GetF().Args[0] = &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}}
		_, ok = storedMySQLSpecialTypeExpr(dynamic)
		require.False(t, ok)
		if wantArgs == 3 {
			malformed = DeepCopyExpr(display)
			malformed.GetF().Args[2] = makePlan2Int64ConstExprWithType(257)
			_, ok = storedMySQLSpecialTypeExpr(malformed)
			require.False(t, ok)
		}
	}
	invalid := plan.Type{Id: int32(types.T_enum), Enumvalues: "x", EnumEncoding: 257}
	_, _, err := makeInsertIgnoreMySQLSpecialTypeConstExpr(t.Context(), tree.NewNumVal("x", "x", false, tree.P_char), invalid)
	require.Error(t, err)
}

func TestInsertDistinctEnumKeepsVisibleGroupKey(t *testing.T) {
	mock := NewMockOptimizer(true)
	typ := plan.Type{Id: int32(types.T_enum), Enumvalues: "2#3:a,b0:", EnumEncoding: 1}
	mock.ctxt.tables["nation"].Cols[1].Typ = typ
	addSetBitmapDestinationForTest(mock)
	mock.ctxt.tables["set_bitmap_destination"].Cols[1].Typ = typ

	query, err := runOneStmt(mock, t, `insert into set_bitmap_destination(id, bitmap)
		select 1, name from (select distinct n_name as name from nation) q`)
	require.NoError(t, err)
	var found bool
	for _, node := range query.GetQuery().Nodes {
		if node.NodeType == plan.Node_AGG && len(node.GroupBy) == 1 &&
			node.GroupBy[0].Typ.Id == int32(types.T_varchar) {
			found = true
		}
	}
	require.True(t, found, "DISTINCT must group the one visible value, without a hidden ordinal key")
}

func TestMalformedEnumMetadataCannotRenderDDL(t *testing.T) {
	for _, typ := range []plan.Type{
		{Id: int32(types.T_enum), Enumvalues: "2#9:a", EnumEncoding: 1},
		{Id: int32(types.T_enum), Enumvalues: "a,b", EnumEncoding: 257},
	} {
		_, err := FormatColType(typ)
		require.Error(t, err)
	}
}

func TestStoredEnumValueBinding(t *testing.T) {
	target := plan.Type{Id: int32(types.T_enum), Enumvalues: "3#3:a,b1:c0:", EnumEncoding: 1}
	binder := NewDefaultBinder(t.Context(), nil, nil, target, nil)
	for _, ordinal := range []uint16{0, 1, 3} {
		expr, err := binder.BindExpr(&tree.StoredEnumValue{
			Ordinal: ordinal, EnumValues: target.Enumvalues, EnumEncoding: target.EnumEncoding,
		}, 0, true)
		require.NoError(t, err)
		require.Equal(t, target, expr.Typ)
		require.Equal(t, uint32(ordinal), expr.GetLit().GetEnumVal())
	}
	for _, value := range []*tree.StoredEnumValue{
		{Ordinal: 4, EnumValues: target.Enumvalues, EnumEncoding: 1},
		{Ordinal: 0, EnumValues: "3#1:c3:a,b0:", EnumEncoding: 1},
		{Ordinal: 0, EnumValues: "3#9:bad", EnumEncoding: 1},
		{Ordinal: 0, EnumValues: target.Enumvalues, EnumEncoding: 257},
	} {
		_, err := binder.BindExpr(value, 0, true)
		require.Error(t, err)
	}
	// An invalid call must not poison the binder's previously validated domain.
	expr, err := binder.BindExpr(&tree.StoredEnumValue{Ordinal: 0, EnumValues: target.Enumvalues, EnumEncoding: 1}, 0, true)
	require.NoError(t, err)
	require.Zero(t, expr.GetLit().GetEnumVal())

	empty := plan.Type{Id: int32(types.T_enum), Enumvalues: "1#0:", EnumEncoding: 1}
	for _, ordinal := range []uint16{0, 1} {
		expr, err := NewDefaultBinder(t.Context(), nil, nil, empty, nil).BindExpr(
			&tree.StoredEnumValue{Ordinal: ordinal, EnumValues: "", EnumEncoding: 0}, 0, true)
		require.NoError(t, err)
		require.Equal(t, uint32(ordinal), expr.GetLit().GetEnumVal())
		require.Equal(t, empty, expr.Typ)
	}
	_, err = NewDefaultBinder(t.Context(), nil, nil, plan.Type{Id: int32(types.T_uint16)}, nil).BindExpr(
		&tree.StoredEnumValue{Ordinal: 0, EnumValues: "a,b"}, 0, true)
	require.Error(t, err)
}
