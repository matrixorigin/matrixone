// Copyright 2026 MatrixOne
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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestImplicitFilledChildDefaultExprMirrorsRuntimeFill pins the contract the
// INSERT IGNORE foreign-key filter relies on: for every column family whose
// implicit default has a plan constant, that constant preserves the column
// type, so the parent-existence check with coalesce validates exactly the
// value the write pipeline stores. Fillable families without a plan constant
// (uuid, json) must classify as implicitFillUnmirrored: the filter drops the
// MATCH-SIMPLE exemption but checks the raw column. Types without any implicit
// default (geometry, arrays, datalink) classify as implicitFillRejected: the
// write keeps the constraint violation and the filter leaves the exempt
// expression alone.
func TestImplicitFilledChildDefaultExprMirrorsRuntimeFill(t *testing.T) {
	ctx := context.Background()
	decimal := plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}
	supported := []plan.Type{
		{Id: int32(types.T_bool)},
		{Id: int32(types.T_bit)},
		{Id: int32(types.T_int8)},
		{Id: int32(types.T_int16)},
		{Id: int32(types.T_int32)},
		{Id: int32(types.T_int64)},
		{Id: int32(types.T_uint8)},
		{Id: int32(types.T_uint16)},
		{Id: int32(types.T_uint32)},
		{Id: int32(types.T_uint64)},
		{Id: int32(types.T_float32)},
		{Id: int32(types.T_float64)},
		// The low-precision float families are the float types the runtime
		// fill also covers (+0, all-zero bits), mirrored as float32(0)
		// assignment-cast to the column type.
		{Id: int32(types.T_bf16)},
		{Id: int32(types.T_float16)},
		{Id: int32(types.T_float8)},
		{Id: int32(types.T_float4)},
		decimal,
		{Id: int32(types.T_date)},
		{Id: int32(types.T_datetime)},
		{Id: int32(types.T_time)},
		{Id: int32(types.T_timestamp)},
		{Id: int32(types.T_year)},
		{Id: int32(types.T_char), Width: 5},
		{Id: int32(types.T_varchar), Width: 10},
		{Id: int32(types.T_binary), Width: 4},
		{Id: int32(types.T_varbinary), Width: 8},
		{Id: int32(types.T_blob)},
		{Id: int32(types.T_text)},
		{Id: int32(types.T_enum), Enumvalues: "'a','b'"},
	}
	for _, typ := range supported {
		expr, mode := implicitFilledChildDefaultExpr(ctx, typ)
		require.Equal(t, implicitFillMirrored, mode, "type %d must have a plan constant of its implicit default", typ.Id)
		require.NotNil(t, expr)
		require.Equal(t, typ.Id, expr.Typ.Id, "the default constant keeps the column type id")
		require.True(t, expr.Typ.NotNullable, "a constant default is never null")
		require.Equal(t, typ.Width, expr.Typ.Width, "width metadata survives (type %d)", typ.Id)
		require.Equal(t, typ.Scale, expr.Typ.Scale, "scale metadata survives (type %d)", typ.Id)
	}

	// The temporal fills store the zero sentinels ('0000-00-00' and friends,
	// encoded as -1), not the coded zero date ('0001-01-01'): the constants
	// the foreign-key predicate and the generated-column rewrite evaluate
	// must carry exactly the value the runtime vector fill stores.
	dateExpr, _ := implicitFilledChildDefaultExpr(ctx, plan.Type{Id: int32(types.T_date)})
	require.EqualValues(t, int32(types.ZeroDate), dateExpr.GetLit().GetDateval(),
		"DATE fills to the zero-date sentinel")
	datetimeExpr, _ := implicitFilledChildDefaultExpr(ctx, plan.Type{Id: int32(types.T_datetime)})
	require.EqualValues(t, int64(types.ZeroDatetime), datetimeExpr.GetLit().GetDatetimeval(),
		"DATETIME fills to the zero-datetime sentinel")
	timestampExpr, _ := implicitFilledChildDefaultExpr(ctx, plan.Type{Id: int32(types.T_timestamp)})
	require.EqualValues(t, int64(types.ZeroTimestamp), timestampExpr.GetLit().GetTimestampval(),
		"TIMESTAMP fills to the zero-timestamp sentinel")
	timeExpr, _ := implicitFilledChildDefaultExpr(ctx, plan.Type{Id: int32(types.T_time)})
	require.EqualValues(t, int64(0), timeExpr.GetLit().GetTimeval(), "TIME fills to zero")

	// The byte-string payloads keep their own contracts: VARBINARY fills
	// with the empty payload itself, while fixed-width BINARY(n) fills with
	// the padded empty-string assignment - n 0x00 bytes, the value an
	// ordinary empty-string assignment stores - so its mirror is the
	// assignment cast of the empty payload to the column type, the same
	// cast the assignment runs, rather than a raw empty constant.
	varbinExpr, varbinMode := implicitFilledChildDefaultExpr(ctx, plan.Type{Id: int32(types.T_varbinary), Width: 8})
	require.Equal(t, implicitFillMirrored, varbinMode)
	require.Equal(t, int32(types.T_varbinary), varbinExpr.Typ.Id)
	require.Equal(t, "", varbinExpr.GetLit().GetSval())
	binExpr, binMode := implicitFilledChildDefaultExpr(ctx, plan.Type{Id: int32(types.T_binary), Width: 2})
	require.Equal(t, implicitFillMirrored, binMode)
	require.Equal(t, int32(types.T_binary), binExpr.Typ.Id)
	require.EqualValues(t, 2, binExpr.Typ.Width, "the BINARY mirror keeps the declared width")
	binCast := binExpr.GetF()
	require.NotNil(t, binCast, "BINARY(n) mirrors its fill as an assignment cast")
	require.Equal(t, "cast_strict", binCast.Func.ObjName,
		"the BINARY mirror retains the width-constrained assignment boundary")
	require.Len(t, binCast.Args, 2)
	require.Equal(t, "", binCast.Args[0].GetLit().GetSval(),
		"the cast source is the empty payload")
	srcTyp := binCast.Args[0].GetTyp()
	require.Equal(t, int32(types.T_varbinary), srcTyp.GetId(),
		"the empty payload keeps its literal type before the cast")
	dstTyp := binCast.Args[1].GetTyp()
	require.Equal(t, int32(types.T_binary), dstTyp.GetId(),
		"the cast lands on the column type")

	// Exercise the conversion, not only the selected function name: a
	// width-constrained assignment must still mirror the runtime zero fill.
	proc := newPlanTestProcess(t)
	foldedBinary, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(binExpr), proc, false, true)
	require.NoError(t, err)
	require.NotNil(t, foldedBinary.GetLit())
	require.Equal(t, "\x00\x00", foldedBinary.GetLit().GetSval(), "BINARY(2) fills with exactly two zero bytes")
	foldedVarbinary, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(varbinExpr), proc, false, true)
	require.NoError(t, err)
	require.NotNil(t, foldedVarbinary.GetLit())
	require.Equal(t, "", foldedVarbinary.GetLit().GetSval(), "VARBINARY remains empty without fixed-width padding")

	// The write side stores a filled value for these families (zero uuid,
	// json null payload) but the plan has no constant for it: the caller must
	// drop the MATCH-SIMPLE exemption while checking the raw column.
	for _, typ := range []plan.Type{
		{Id: int32(types.T_json)}, {Id: int32(types.T_uuid)},
	} {
		expr, mode := implicitFilledChildDefaultExpr(ctx, typ)
		require.Equal(t, implicitFillUnmirrored, mode, "type %d is filled at runtime but has no plan constant", typ.Id)
		require.Nil(t, expr)
	}

	// No implicit default exists for these families: the write keeps the
	// constraint violation, so the filter must leave the exempt expression
	// untouched. A datalink holds a validated URL (load_file parses the
	// scheme); the empty byte string the shared string-family fill would
	// store is not a valid datalink value, so the family has no implicit
	// default at all and IGNORE keeps the constraint violation (the
	// geometry precedent).
	for _, typ := range []plan.Type{
		{Id: int32(types.T_geometry)},
		{Id: int32(types.T_geometry32)},
		{Id: int32(types.T_datalink)},
		{Id: int32(types.T_array_float32)},
		{Id: int32(types.T_array_float64)},
	} {
		expr, mode := implicitFilledChildDefaultExpr(ctx, typ)
		require.Equal(t, implicitFillRejected, mode, "type %d must not claim an implicit default", typ.Id)
		require.Nil(t, expr)
	}
}

// TestImplicitGeneratedColFillExpr pins the fill-constant source of the
// generated-column rewrite for the two families the foreign-key classifier
// leaves unmirrored: uuid has no plan literal at all and the JSON literal
// one-of is not a runnable vector value, so both get a typed cast of the
// constant text whose runtime evaluation produces exactly the stored fill
// (the zero uuid, the JSON null document). Every other fillable family
// resolves through the shared classifier, and rejected families report no
// fill constant.
func TestImplicitGeneratedColFillExpr(t *testing.T) {
	ctx := context.Background()
	for _, typ := range []struct {
		name string
		typ  plan.Type
		text string
	}{
		{"uuid", plan.Type{Id: int32(types.T_uuid)}, "00000000-0000-0000-0000-000000000000"},
		{"json", plan.Type{Id: int32(types.T_json)}, "null"},
	} {
		expr, ok := implicitGeneratedColFillExpr(ctx, typ.typ)
		require.True(t, ok, "%s must mirror its runtime fill with a typed expression", typ.name)
		require.NotNil(t, expr)
		require.Equal(t, typ.typ.Id, expr.Typ.Id, "%s cast keeps the column type", typ.name)
		require.True(t, expr.Typ.NotNullable, "%s fill constant is not null", typ.name)
		cast := expr.GetF()
		require.NotNil(t, cast, "%s fill constant is a typed cast of the constant text", typ.name)
		require.Equal(t, "cast", cast.Func.ObjName)
		require.Len(t, cast.Args, 2)
		require.Equal(t, typ.text, cast.Args[0].GetLit().GetSval(),
			"%s casts its stored fill text", typ.name)
		target := cast.Args[1].GetTyp()
		require.NotNil(t, target)
		require.Equal(t, typ.typ.Id, target.GetId(), "%s casts to the column type", typ.name)
	}

	// Ordinary mirrored families resolve through the shared classifier.
	for _, typ := range []plan.Type{
		{Id: int32(types.T_int64)},
		{Id: int32(types.T_date)},
		{Id: int32(types.T_char), Width: 5},
	} {
		expr, ok := implicitGeneratedColFillExpr(ctx, typ)
		require.True(t, ok)
		require.NotNil(t, expr)
		require.Equal(t, typ.Id, expr.Typ.Id)
	}

	// Families without any implicit default claim no fill constant.
	for _, typ := range []plan.Type{
		{Id: int32(types.T_geometry)},
		{Id: int32(types.T_geometry32)},
		{Id: int32(types.T_datalink)},
	} {
		expr, ok := implicitGeneratedColFillExpr(ctx, typ)
		require.False(t, ok)
		require.Nil(t, expr)
	}
}

// colRefForTest builds a row-local column reference like a generated-column
// expression stores it.
func colRefForTest(pos int32, typ plan.Type) *plan.Expr {
	return &plan.Expr{
		Typ:  typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: pos}},
	}
}

// TestCoalesceGeneratedColIgnoreFillRefsRewritesFillTargets pins the
// generated-column rewrite INSERT IGNORE applies: a reference that can still
// carry a supplied NULL into a column that forbids NULL is wrapped in
// coalesce(ref, implicit default) inside function arguments, for every
// family whose fill the rewrite can mirror - including uuid and json, whose
// fills only typed cast constants can express. References to families
// without any implicit default (geometry) stay raw, and references that
// cannot be supplied NULL stay raw.
func TestCoalesceGeneratedColIgnoreFillRefsRewritesFillTargets(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_INSERT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
	int64Typ := plan.Type{Id: int32(types.T_int64)}
	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "a", Typ: int64Typ, NotNull: true, Default: &plan.Default{NullAbility: false}},
			{Name: "u", Typ: plan.Type{Id: int32(types.T_uuid)}, NotNull: true, Default: &plan.Default{NullAbility: false}},
			{Name: "g", Typ: plan.Type{Id: int32(types.T_geometry)}, NotNull: true, Default: &plan.Default{NullAbility: false}},
			{Name: "opt", Typ: int64Typ, Default: &plan.Default{NullAbility: true}},
			{Name: "j", Typ: plan.Type{Id: int32(types.T_json)}, NotNull: true, Default: &plan.Default{NullAbility: false}},
		},
	}
	supplied := func() map[string]*plan.Expr {
		return map[string]*plan.Expr{
			"a": colRefForTest(100, int64Typ),
			"u": colRefForTest(102, plan.Type{Id: int32(types.T_uuid)}),
			"g": colRefForTest(103, plan.Type{Id: int32(types.T_geometry)}),
			"j": colRefForTest(104, plan.Type{Id: int32(types.T_json)}),
		}
	}
	plusOf := func(args ...*plan.Expr) *plan.Expr {
		return &plan.Expr{
			Typ:  int64Typ,
			Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{}, Args: args}},
		}
	}
	t.Run("wrapped reference inside function arguments", func(t *testing.T) {
		expr := plusOf(colRefForTest(0, int64Typ), makePlan2Int64ConstExprWithType(1))
		out, err := builder.coalesceGeneratedColIgnoreFillRefs(tableDef, supplied(), expr)
		require.NoError(t, err)
		f := out.GetF()
		require.NotNil(t, f, "top-level function node is preserved")
		require.Len(t, f.Args, 2)
		wrapped := f.Args[0].GetF()
		require.NotNil(t, wrapped, "the function argument holding the reference was rewritten in place")
		require.Equal(t, "coalesce", wrapped.Func.ObjName, "supplied NOT NULL base reference is coalesced to the implicit default")
		require.Len(t, wrapped.Args, 2)
		require.Equal(t, int32(0), wrapped.Args[0].GetCol().ColPos, "original reference survives as the first coalesce argument")
		lit := wrapped.Args[1].GetLit()
		require.NotNil(t, lit, "second coalesce argument is the implicit default constant")
		require.EqualValues(t, 0, lit.GetI64Val())
		kept := f.Args[1].GetLit()
		require.NotNil(t, kept, "non-reference arguments are untouched")
		require.EqualValues(t, 1, kept.GetI64Val())
	})
	t.Run("uuid and json wrap to typed cast constants, rejected families stay raw", func(t *testing.T) {
		for name, tc := range map[string]struct {
			pos int32
			typ plan.Type
		}{
			"uuid": {1, plan.Type{Id: int32(types.T_uuid)}},
			"json": {4, plan.Type{Id: int32(types.T_json)}},
		} {
			ref := colRefForTest(tc.pos, tc.typ)
			out, err := builder.coalesceGeneratedColIgnoreFillRefs(tableDef, supplied(), ref)
			require.NoError(t, err)
			f := out.GetF()
			require.NotNil(t, f, "%s reference is wrapped", name)
			require.Equal(t, "coalesce", f.Func.ObjName, "%s coalesces to its typed fill constant", name)
			require.Equal(t, tc.typ.Id, out.Typ.Id, "%s coalesce keeps the column type", name)
			require.Len(t, f.Args, 2)
			// The helper deep-copies the reference before wrapping it, so the
			// surviving reference is compared by value, not by identity.
			require.Equal(t, ref.GetCol().RelPos, f.Args[0].GetCol().RelPos,
				"%s original reference survives as the first argument", name)
			require.Equal(t, ref.GetCol().ColPos, f.Args[0].GetCol().ColPos,
				"%s original column position survives as the first argument", name)
			cast := f.Args[1].GetF()
			require.NotNil(t, cast, "%s second argument is the runtime cast of the constant text", name)
			require.Equal(t, "cast", cast.Func.ObjName)
		}
		// geometry keeps the raw reference: the statement aborts anyway.
		geoRef := colRefForTest(2, plan.Type{Id: int32(types.T_geometry)})
		out, err := builder.coalesceGeneratedColIgnoreFillRefs(tableDef, supplied(), geoRef)
		require.NoError(t, err)
		require.Same(t, geoRef, out, "geometry reference must stay raw")
		require.Nil(t, out.GetF())
	})
	t.Run("references that cannot carry a supplied NULL stay raw", func(t *testing.T) {
		ref := colRefForTest(3, int64Typ) // nullable column, never a fill target
		out, err := builder.coalesceGeneratedColIgnoreFillRefs(tableDef, supplied(), ref)
		require.NoError(t, err)
		require.Same(t, ref, out)

		// a NULL-default descriptor still qualifies even when the column
		// itself is not supplied
		defNull := colRefForTest(0, int64Typ)
		out, err = builder.coalesceGeneratedColIgnoreFillRefs(tableDef, supplied(), defNull)
		require.NoError(t, err)
		require.Equal(t, "coalesce", out.GetF().Func.ObjName)
	})
}
