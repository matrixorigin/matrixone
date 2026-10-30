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
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// withUnsignedDecimalAuthoringFloor supplies the D5 protocol floor the
// attributed-DECIMAL authoring and assignment boundaries require (#30940).
func withUnsignedDecimalAuthoringFloor(t *testing.T, proc *process.Process) {
	t.Helper()
	rt := runtime.ServiceRuntime(proc.GetService())
	origProtocol, hadProtocol := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	origFloor, hadFloor := rt.GetGlobalVariables(runtime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, origProtocol)
		}
		if hadFloor {
			rt.SetGlobalVariables(runtime.PersistedExpressionProtocolAuthoringFloor, origFloor)
		}
	})
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
	rt.SetGlobalVariables(runtime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCLatestVersion)
}

// TestBuildDDLUnsignedDecimalMaskIntoTableDef pins the authoring contract at
// the parser/table-def boundary: UNSIGNED carries NumericAttributeUnsigned,
// ZEROFILL normalizes to the full zerofill mask (both bits), the mask carries
// across decimal64/128/256 widths, and a plain signed DECIMAL keeps mask zero
// (I4: unattributed plans stay byte-identical).
func TestBuildDDLUnsignedDecimalMaskIntoTableDef(t *testing.T) {
	proc := newPlanTestProcess(t)
	ctx := NewMockCompilerContext(false, proc)
	withUnsignedDecimalAuthoringFloor(t, proc)

	cases := []struct {
		sql       string
		colName   string
		wantMask  uint32
		wantOid   types.T
		wantWidth int32
		wantScale int32
	}{
		{"create table ud_t1 (a decimal(10,2) unsigned)", "a", types.NumericAttributeUnsigned, types.T_decimal64, 10, 2},
		{"create table ud_t2 (a decimal(10,2) zerofill)", "a", types.NumericAttributeZerofillMask, types.T_decimal64, 10, 2},
		{"create table ud_t3 (a decimal(10,2) unsigned zerofill)", "a", types.NumericAttributeZerofillMask, types.T_decimal64, 10, 2},
		{"create table ud_t4 (a decimal(20,2) unsigned)", "a", types.NumericAttributeUnsigned, types.T_decimal128, 20, 2},
		{"create table ud_t5 (a decimal(65,6) unsigned)", "a", types.NumericAttributeUnsigned, types.T_decimal256, 65, 6},
		{"create table ud_t6 (a decimal(6,0) zerofill)", "a", types.NumericAttributeZerofillMask, types.T_decimal64, 6, 0},
		{"create table ud_t7 (a decimal(10,2))", "a", 0, types.T_decimal64, 10, 2},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			built, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			cols := built.GetDdl().GetCreateTable().GetTableDef().GetCols()
			require.NotEmpty(t, cols)
			require.Equal(t, tc.colName, cols[0].Name)
			require.Equal(t, int32(tc.wantOid), cols[0].Typ.Id, "oid")
			require.Equal(t, tc.wantWidth, cols[0].Typ.Width, "width")
			require.Equal(t, tc.wantScale, cols[0].Typ.Scale, "scale")
			require.Equal(t, tc.wantMask, cols[0].Typ.GetNumericAttributes(), "numeric attribute mask")
		})
	}

	t.Run("integer unsigned channels stay out of the decimal mask axis", func(t *testing.T) {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
			"create table ud_t8 (a tinyint unsigned, b decimal(10,2))", 1)
		require.NoError(t, err)
		defer stmt.Free()
		built, err := BuildPlan(ctx, stmt, false)
		require.NoError(t, err)
		cols := built.GetDdl().GetCreateTable().GetTableDef().GetCols()
		require.Zero(t, cols[0].Typ.GetNumericAttributes(), "integer columns keep the legacy signedness channel")
		require.Zero(t, cols[1].Typ.GetNumericAttributes())
	})
}

const unsignedDecimalAdmissionQuoted = `"` + unsignedDecimalAdmissionName + `"`
const unsignedDecimalAdmissionIgnoreQuoted = `"` + unsignedDecimalAdmissionIgnoreName + `"`
const unsignedDecimalAdmissionStrictQuoted = `"` + unsignedDecimalAdmissionStrictName + `"`

// TestBuildDDLUnsignedDecimalStrictConstantDefault pins the authoring-time
// validation contract: a constant negative DEFAULT on an attributed DECIMAL
// fails with ER_INVALID_DEFAULT (1067) regardless of session sql_mode;
// constant non-negative and volatile defaults pass authoring, and the
// persisted DEFAULT composition carries the mode-resolved admission entry
// (strict evaluation happens at replay, never at authoring).
func TestBuildDDLUnsignedDecimalStrictConstantDefault(t *testing.T) {
	proc := newPlanTestProcess(t)
	ctx := NewMockCompilerContext(false, proc)
	withUnsignedDecimalAuthoringFloor(t, proc)

	t.Run("constant negative defaults are 1067", func(t *testing.T) {
		for _, sql := range []string{
			"create table sd_neg1 (a decimal(10,2) unsigned default -1.5)",
			"create table sd_neg2 (a decimal(10,2) unsigned default (-2))",
			"create table sd_neg3 (a decimal(10,2) zerofill default '-1')",
		} {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			_, err = BuildPlan(ctx, stmt, false)
			require.Error(t, err, sql)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault),
				"%s must fail with ER_INVALID_DEFAULT, got %v", sql, err)
			stmt.Free()
		}
	})

	t.Run("constant negative on unattributed decimal stays legal (I4)", func(t *testing.T) {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
			"create table sd_signed (a decimal(10,2) default -1.5)", 1)
		require.NoError(t, err)
		defer stmt.Free()
		_, err = BuildPlan(ctx, stmt, false)
		require.NoError(t, err)
	})

	t.Run("constant non-negative default persists the admission composition", func(t *testing.T) {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
			"create table sd_ok (a decimal(10,2) unsigned default 1.25)", 1)
		require.NoError(t, err)
		defer stmt.Free()
		built, err := BuildPlan(ctx, stmt, false)
		require.NoError(t, err)
		def := built.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetDefault()
		require.NotNil(t, def)
		text := def.GetExpr().String()
		require.Equal(t, 1, strings.Count(text, unsignedDecimalAdmissionQuoted), text)
		require.Zero(t, strings.Count(text, unsignedDecimalAdmissionStrictQuoted), text)
		require.Zero(t, strings.Count(text, unsignedDecimalAdmissionIgnoreQuoted), text)
		require.Equal(t, 1, strings.Count(text, `sval:"a"`),
			"the persisted DEFAULT admission names its column: %s", text)
	})

	t.Run("default null builds without admission, byte-identical to unattributed (I4)", func(t *testing.T) {
		for _, sql := range []string{
			"create table sd_null_u (a decimal(10,2) unsigned default null)",
			"create table sd_null_s (a decimal(10,2) default null)",
		} {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			built, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err, sql)
			def := built.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetDefault()
			if def != nil && def.GetExpr() != nil {
				require.Zero(t, strings.Count(def.GetExpr().String(), "unsigned_decimal_admission"),
					"%s: NULL-by-construction defaults carry no admission node", sql)
			}
			stmt.Free()
		}
	})

	t.Run("volatile negative-producing default passes authoring", func(t *testing.T) {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
			"create table sd_vol (a decimal(10,2) unsigned default (-1-rand()))", 1)
		require.NoError(t, err)
		defer stmt.Free()
		built, err := BuildPlan(ctx, stmt, false)
		require.NoError(t, err, "volatile sources are validated at replay, never at authoring")
		def := built.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetDefault()
		require.NotNil(t, def)
		require.Equal(t, 1, strings.Count(def.GetExpr().String(), unsignedDecimalAdmissionQuoted),
			"the stored volatile default keeps the mode-resolved admission entry")
	})
}

// TestBuildDDLUnsignedDecimalOnUpdateStrictConstant pins the second default
// owner. The grammar authors ON UPDATE only as CURRENT_TIMESTAMP-family
// calls, so the generic expression path is driven directly: buildOnUpdate
// must validate a constant negative source exactly like the DEFAULT owner
// (ER_INVALID_DEFAULT, 1067) and compose the mode-resolved admission for a
// constant non-negative source.
func TestBuildDDLUnsignedDecimalOnUpdateStrictConstant(t *testing.T) {
	proc := newPlanTestProcess(t)
	_ = NewMockCompilerContext(false, proc)
	withUnsignedDecimalAuthoringFloor(t, proc)

	attributed := plan.Type{
		Id:                int32(types.T_decimal64),
		Width:             10,
		Scale:             2,
		NumericAttributes: types.NumericAttributeUnsigned,
	}

	parsedExpr := func(t *testing.T, sql string) tree.Expr {
		t.Helper()
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		sel, ok := stmt.(*tree.Select).Select.(*tree.SelectClause)
		require.True(t, ok)
		require.Len(t, sel.Exprs, 1)
		return sel.Exprs[0].Expr
	}
	onUpdateCol := func(expr tree.Expr) *tree.ColumnTableDef {
		return &tree.ColumnTableDef{
			Name:       tree.NewUnresolvedColName("a"),
			Attributes: []tree.ColumnAttribute{tree.NewAttributeOnUpdate(expr)},
		}
	}

	t.Run("constant negative on update source is 1067", func(t *testing.T) {
		_, err := buildOnUpdate(context.Background(), onUpdateCol(parsedExpr(t, "select -0.25")), attributed, proc)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault), "got %v", err)
	})

	t.Run("constant non-negative on update source keeps the mode-resolved admission", func(t *testing.T) {
		ou, err := buildOnUpdate(context.Background(), onUpdateCol(parsedExpr(t, "select 0.25")), attributed, proc)
		require.NoError(t, err)
		require.NotNil(t, ou)
		text := ou.GetExpr().String()
		require.Equal(t, 1, strings.Count(text, unsignedDecimalAdmissionQuoted), text)
		require.Zero(t, strings.Count(text, unsignedDecimalAdmissionStrictQuoted), text)
		require.Zero(t, strings.Count(text, unsignedDecimalAdmissionIgnoreQuoted), text)
	})
}

// unsignedDecimalDMLTable builds a no-real-PK TableDef in the fake_pk_t
// shape: hidden auto-valued fake-PK column plus rowid, and a PrimaryKeyDef
// naming the fake PK so the modern dedup/MULTI_UPDATE binder accepts the table.
func unsignedDecimalDMLTable(name string, cols ...*plan.ColDef) *plan.TableDef {
	fakePK := &plan.ColDef{
		Name:    catalog.FakePrimaryKeyColName,
		Typ:     plan.Type{Id: int32(types.T_uint64), NotNullable: true, AutoIncr: true},
		Hidden:  true,
		Pkidx:   1,
		Default: &plan.Default{NullAbility: false},
	}
	rowID := &plan.ColDef{
		Name:    catalog.Row_ID,
		Typ:     plan.Type{Id: int32(types.T_Rowid)},
		Hidden:  true,
		Default: &plan.Default{NullAbility: false},
	}
	all := append(append([]*plan.ColDef{}, cols...), fakePK, rowID)
	for i, col := range all {
		col.ColId = uint64(i)
		col.Primary = i == 0
	}
	return &plan.TableDef{
		Name:      name,
		TableType: catalog.SystemOrdinaryRel,
		Cols:      all,
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: catalog.FakePrimaryKeyColName,
			Names:       []string{catalog.FakePrimaryKeyColName},
			CompPkeyCol: fakePK,
		},
	}
}

// newUnsignedDecimalMockTable registers one attributed DECIMAL table plus a
// plain signed control column for DML plan tests.
func newUnsignedDecimalMockTable(ctx *MockCompilerContext) {
	ctx.tables["ud_ins"] = unsignedDecimalDMLTable("ud_ins",
		&plan.ColDef{Name: "a", OriginName: "a",
			Typ:     plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2, NumericAttributes: types.NumericAttributeUnsigned},
			Default: &plan.Default{NullAbility: true}},
		&plan.ColDef{Name: "b", OriginName: "b",
			Typ:     plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2},
			Default: &plan.Default{NullAbility: true}},
	)
	ctx.objects["ud_ins"] = &plan.ObjectRef{SchemaName: "tpch", ObjName: "ud_ins"}
}

// TestBuildInsertUnsignedDecimalAdmission pins the DML assignment contract:
// every write to an attributed DECIMAL target composes the plan-owned domain
// admission in front of the assignment cast. IGNORE statements take BOTH
// layers on the ignore axis; plain statements take the mode-resolved names;
// and the plain signed control column keeps the untouched assignment shape.
func TestBuildInsertUnsignedDecimalAdmission(t *testing.T) {
	proc := newPlanTestProcess(t)
	ctx := NewMockCompilerContext(true, proc)
	withUnsignedDecimalAuthoringFloor(t, proc)
	newUnsignedDecimalMockTable(ctx)

	t.Run("plain insert composes mode-resolved admission on the attributed column only", func(t *testing.T) {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
			"insert into ud_ins(a, b) values (-1.5, -2.5)", 1)
		require.NoError(t, err)
		defer stmt.Free()
		built, err := BuildPlan(ctx, stmt, false)
		require.NoError(t, err)
		text := built.String()
		// The composition appears at the VALUES-rowset layer and again at the
		// projection assignment boundary; the admission is policy-idempotent,
		// so layered evaluation emits exactly one diagnostic per adjusted row.
		admissions := strings.Count(text, unsignedDecimalAdmissionQuoted)
		require.GreaterOrEqual(t, admissions, 1,
			"the attributed column composes the field-targeted admission")
		require.Zero(t, strings.Count(text, unsignedDecimalAdmissionStrictQuoted), text)
		require.Zero(t, strings.Count(text, unsignedDecimalAdmissionIgnoreQuoted), text)
		require.Contains(t, text, `"cast_assign"`)
		require.Equal(t, admissions, strings.Count(text, `sval:"a"`),
			"every admission node names destination column a (F6 diagnostic channel): %s", text)
	})

	t.Run("insert ignore switches both boundary layers to the ignore entry", func(t *testing.T) {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
			"insert ignore into ud_ins(a, b) values (-1.5, -2.5)", 1)
		require.NoError(t, err)
		defer stmt.Free()
		built, err := BuildPlan(ctx, stmt, false)
		require.NoError(t, err)
		text := built.String()
		admissions := strings.Count(text, unsignedDecimalAdmissionIgnoreQuoted)
		require.GreaterOrEqual(t, admissions, 1, text)
		require.Zero(t, strings.Count(text, unsignedDecimalAdmissionStrictQuoted), text)
		require.Zero(t, strings.Count(text, unsignedDecimalAdmissionQuoted), text)
		require.Contains(t, text, `"cast_ignore"`)
		require.Equal(t, admissions, strings.Count(text, `sval:"a"`),
			"the ignore policy keeps the same destination-column channel: %s", text)
	})

	t.Run("unattributed targets never compose the admission (I4)", func(t *testing.T) {
		ctx.tables["sd_ins"] = unsignedDecimalDMLTable("sd_ins",
			&plan.ColDef{Name: "b", OriginName: "b",
				Typ:     plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2},
				Default: &plan.Default{NullAbility: true}},
		)
		ctx.objects["sd_ins"] = &plan.ObjectRef{SchemaName: "tpch", ObjName: "sd_ins"}
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
			"insert into sd_ins(b) values (-2.5)", 1)
		require.NoError(t, err)
		defer stmt.Free()
		built, err := BuildPlan(ctx, stmt, false)
		require.NoError(t, err)
		require.Zero(t, strings.Count(built.String(), "unsigned_decimal_admission"),
			"a plain signed DECIMAL keeps the pre-change assignment shapes")
	})
}

// TestUnsignedDecimalBoundaryRewriter pins the persisted-expression replay
// boundary (round-10 finding 1 resolution): the rewriter recognizes the
// plan-owned admission composition by typed shape, switches BOTH layers under
// the statement policy, is idempotent under a fixed policy, reproduces the
// DDL-authored boundary on an IGNORE -> non-IGNORE sequence, and passes
// through byte-identically anything outside the boundary shape or any shape
// carrying a non-distinctive target mask (I4).
func TestUnsignedDecimalBoundaryRewriter(t *testing.T) {
	tctx := context.Background()
	proc := newPlanTestProcess(t)
	withUnsignedDecimalAuthoringFloor(t, proc)

	attributed := plan.Type{
		Id:                int32(types.T_decimal64),
		Width:             10,
		Scale:             2,
		NumericAttributes: types.NumericAttributeUnsigned,
	}
	src := &plan.Expr{
		Typ:  attributed,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, RelPos: 0}},
	}

	names := func(expr *plan.Expr) (outer, inner string) {
		f := expr.GetF()
		require.NotNil(t, f)
		require.Len(t, f.Args, 2)
		ad := f.Args[0].GetF()
		require.NotNil(t, ad)
		return f.Func.ObjName, ad.Func.ObjName
	}
	// admissionColumn reads the destination-column constant the admission
	// node carries as its third argument, the diagnostic channel F6 adds.
	admissionColumn := func(expr *plan.Expr) string {
		ad := expr.GetF().Args[0].GetF()
		require.NotNil(t, ad)
		require.Len(t, ad.Args, 3, "the admission carries the destination column constant")
		return ad.Args[2].GetLit().GetSval()
	}

	composed, err := forceUnsignedDecimalAssignmentComposition(tctx, src, attributed, "cast_assign", "ud_amt")
	require.NoError(t, err)
	outer, inner := names(composed)
	require.Equal(t, "cast_assign", outer)
	require.Equal(t, unsignedDecimalAdmissionName, inner)
	require.Equal(t, "ud_amt", admissionColumn(composed))
	require.Same(t, src, composed.GetF().Args[0].GetF().Args[0], "composition wraps the exact source")

	t.Run("matcher extracts the exact source and column by typed shape", func(t *testing.T) {
		extracted, column, ok := matchUnsignedDecimalAssignmentComposition(composed)
		require.True(t, ok)
		require.Same(t, src, extracted)
		require.Equal(t, "ud_amt", column)
	})

	t.Run("legacy two-argument compositions still match with the empty column", func(t *testing.T) {
		legacy, err := forceUnsignedDecimalAssignmentComposition(tctx, src, attributed, "cast_assign", "ud_amt")
		require.NoError(t, err)
		legacy.GetF().Args[0].GetF().Args = legacy.GetF().Args[0].GetF().Args[:2]
		extracted, column, ok := matchUnsignedDecimalAssignmentComposition(legacy)
		require.True(t, ok, "pre-3-arg replays keep matching under the widened shape")
		require.Same(t, src, extracted)
		require.Equal(t, "", column, "the legacy shape carries no diagnostic column")
	})

	t.Run("non-ignore replay is identity under a fixed policy", func(t *testing.T) {
		out, err := rePoliceUnsignedDecimalAssignmentBoundary(tctx, composed, false, proc)
		require.NoError(t, err)
		outer, inner := names(out)
		require.Equal(t, "cast_assign", outer)
		require.Equal(t, unsignedDecimalAdmissionName, inner)
		require.Equal(t, "ud_amt", admissionColumn(out),
			"replay preserves the destination-column diagnostic channel")
		extracted, column, ok := matchUnsignedDecimalAssignmentComposition(out)
		require.True(t, ok)
		require.Equal(t, src, extracted)
		require.Equal(t, "ud_amt", column)
	})

	t.Run("ignore replay switches both layers and is idempotent", func(t *testing.T) {
		once, err := rePoliceUnsignedDecimalAssignmentBoundary(tctx, composed, true, proc)
		require.NoError(t, err)
		outer, inner := names(once)
		require.Equal(t, "cast_ignore", outer)
		require.Equal(t, unsignedDecimalAdmissionIgnoreName, inner)

		twice, err := rePoliceUnsignedDecimalAssignmentBoundary(tctx, once, true, proc)
		require.NoError(t, err)
		outer, inner = names(twice)
		require.Equal(t, "cast_ignore", outer)
		require.Equal(t, unsignedDecimalAdmissionIgnoreName, inner)
		require.Equal(t, "ud_amt", admissionColumn(twice))
	})

	t.Run("ignore then non-ignore reproduces the DDL-authored boundary", func(t *testing.T) {
		ignore, err := rePoliceUnsignedDecimalAssignmentBoundary(tctx, composed, true, proc)
		require.NoError(t, err)
		require.Equal(t, "ud_amt", admissionColumn(ignore),
			"the ignore replay keeps the same diagnostic column")
		back, err := rePoliceUnsignedDecimalAssignmentBoundary(tctx, ignore, false, proc)
		require.NoError(t, err)
		outer, inner := names(back)
		require.Equal(t, "cast_assign", outer)
		require.Equal(t, unsignedDecimalAdmissionName, inner)
		extracted, column, ok := matchUnsignedDecimalAssignmentComposition(back)
		require.True(t, ok)
		require.Equal(t, src, extracted)
		require.Equal(t, "ud_amt", column)
	})

	t.Run("strict-authored validation composition replays under the statement policy", func(t *testing.T) {
		// Mirror validateUnsignedDecimalStrictConstantDefault's authoring-time
		// composition exactly: the strict admission entry by name, cast_strict outer.
		strictInner, err := forceUnsignedDecimalAdmissionExpr(tctx, DeepCopyExpr(src), attributed, unsignedDecimalAdmissionStrictName, "ud_amt")
		require.NoError(t, err)
		strictComposed, err := forceAssignmentCastExprWithName(tctx, strictInner, attributed, "cast_strict")
		require.NoError(t, err)
		extracted, _, ok := matchUnsignedDecimalAssignmentComposition(strictComposed)
		require.True(t, ok)
		out, err := rePoliceUnsignedDecimalAssignmentBoundary(tctx, strictComposed, false, proc)
		require.NoError(t, err)
		outer, inner := names(out)
		require.Equal(t, "cast_assign", outer)
		require.Equal(t, unsignedDecimalAdmissionName, inner)
		require.Equal(t, "ud_amt", admissionColumn(out),
			"re-policing a strict-authored boundary keeps its destination column")
		extracted2, _, ok2 := matchUnsignedDecimalAssignmentComposition(out)
		require.True(t, ok2)
		require.Equal(t, extracted, extracted2)
	})

	t.Run("user-written casts and subexpressions are never matched", func(t *testing.T) {
		// A user CAST over the attributed type has a literal user arg where the
		// plan-owned admission node would sit: the rewriter must abstain and
		// return the input pointer unchanged.
		userCast := &plan.Expr{
			Typ: attributed,
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "cast"},
				Args: []*plan.Expr{src, {
					Typ:  attributed,
					Expr: &plan.Expr_T{T: &plan.TargetType{}},
				}},
			}},
		}
		_, _, ok := matchUnsignedDecimalAssignmentComposition(userCast)
		require.False(t, ok)
		out, err := rePoliceUnsignedDecimalAssignmentBoundary(tctx, userCast, true, proc)
		require.NoError(t, err)
		require.Same(t, userCast, out)

		// A bare admission node without the cast-family outer wrapper is not
		// the persisted boundary either.
		bare := composed.GetF().Args[0]
		_, _, ok = matchUnsignedDecimalAssignmentComposition(bare)
		require.False(t, ok)
		out2, err := rePoliceUnsignedDecimalAssignmentBoundary(tctx, bare, true, proc)
		require.NoError(t, err)
		require.Same(t, bare, out2)
	})

	t.Run("the identical shape on an unattributed type passes through (I4)", func(t *testing.T) {
		unattributed := plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}
		plainComposed, err := forceUnsignedDecimalAssignmentComposition(tctx, src, unattributed, "cast_assign", "ud_amt")
		require.NoError(t, err)
		_, _, ok := matchUnsignedDecimalAssignmentComposition(plainComposed)
		require.False(t, ok, "mask zero is outside the attributed boundary shape")
		out, err := rePoliceUnsignedDecimalAssignmentBoundary(tctx, plainComposed, true, proc)
		require.NoError(t, err)
		require.Same(t, plainComposed, out)
	})

	t.Run("NULL-by-construction sources degrade to the plain cast wrapper", func(t *testing.T) {
		nullLit := &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_any)},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}},
		}
		wrapped, err := forceUnsignedDecimalAssignmentComposition(tctx, nullLit, attributed, "cast_assign", "ud_amt")
		require.NoError(t, err)
		require.Equal(t, "cast", wrapped.GetF().Func.ObjName)
		_, _, ok := matchUnsignedDecimalAssignmentComposition(wrapped)
		require.False(t, ok, "a NULL boundary is outside the replay shape")
		out, err := rePoliceUnsignedDecimalAssignmentBoundary(tctx, wrapped, true, proc)
		require.NoError(t, err)
		require.Same(t, wrapped, out)
	})
}

// TestAlterModifyUnsignedDomainForcesCopy pins the algorithm-admission side
// of the narrowing contract: changing the DECIMAL UNSIGNED/ZEROFILL mask
// re-domains the stored values without changing their storage layout, so
// MODIFY must leave the metadata-only INPLACE path and take COPY, where every
// existing row re-enters through the write-side assignment admission. An
// identical definition keeps INPLACE.
func TestAlterModifyUnsignedDomainForcesCopy(t *testing.T) {
	proc := newPlanTestProcess(t)
	ctx := NewMockCompilerContext(false, proc)
	withUnsignedDecimalAuthoringFloor(t, proc)

	register := func(name string, mask uint32) {
		ctx.tables[name] = unsignedDecimalDMLTable(name,
			&plan.ColDef{Name: "a", OriginName: "a",
				Typ:     plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2, NumericAttributes: mask},
				Default: &plan.Default{NullAbility: true}},
		)
		ctx.objects[name] = &plan.ObjectRef{SchemaName: "tpch", ObjName: name}
	}
	register("ud_alt_s", 0)
	register("ud_alt_u", types.NumericAttributeUnsigned)

	algorithmOf := func(t *testing.T, sql string) plan.AlterTable_AlgorithmType {
		t.Helper()
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		built, err := BuildPlan(ctx, stmt, false)
		require.NoError(t, err, sql)
		return built.GetDdl().GetAlterTable().GetAlgorithmType()
	}

	require.Equal(t, plan.AlterTable_INPLACE,
		algorithmOf(t, "alter table ud_alt_s modify a decimal(10,2)"),
		"an identical definition keeps the metadata-only path")
	require.Equal(t, plan.AlterTable_INPLACE,
		algorithmOf(t, "alter table ud_alt_u modify a decimal(10,2) unsigned"),
		"restating the current mask keeps the metadata-only path")
	require.Equal(t, plan.AlterTable_COPY,
		algorithmOf(t, "alter table ud_alt_s modify a decimal(10,2) unsigned"),
		"gaining UNSIGNED must validate existing rows through the write path")
	require.Equal(t, plan.AlterTable_COPY,
		algorithmOf(t, "alter table ud_alt_s modify a decimal(10,2) zerofill"),
		"gaining ZEROFILL (which implies UNSIGNED) re-domains stored values")
	require.Equal(t, plan.AlterTable_COPY,
		algorithmOf(t, "alter table ud_alt_u modify a decimal(10,2)"),
		"dropping UNSIGNED swaps the public mask as well")
}

// TestUnsignedDecimalAdmissionNamesRejectPublicCalls pins plan-only
// visibility for the admission entries at public SQL binding. The names are
// registered for the plan-composed assignment boundary and their retType
// mirrors the source type, so a direct call with an ANY-typed source (a bare
// NULL) would publish an ANY result node that executor result allocation
// cannot allocate (vector functionTools panics for ANY). Every spelling of
// the call — with NULL, with unsupported source types, nested under ordinary
// functions, from arbitrary statement positions, and in any letter case —
// must fail binding with a clear diagnostic instead of reaching resolution.
// The plan-internal composition path is unaffected and is pinned by
// TestBuildInsertUnsignedDecimalAdmission above.
func TestUnsignedDecimalAdmissionNamesRejectPublicCalls(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	for _, sql := range []string{
		// ANY-typed bare NULL source: the exact reviewer reconstruction.
		"select unsigned_decimal_admission(NULL, cast(0 as decimal(10,2)), 'x')",
		"select unsigned_decimal_admission_strict(NULL, cast(0 as decimal(10,2)), 'x')",
		"select unsigned_decimal_admission_ignore(NULL, cast(0 as decimal(10,2)), 'x')",
		"select isnull(unsigned_decimal_admission(NULL, cast(0 as decimal(10,2)), 'x'))",
		// Unsupported source types are blocked by the same visibility guard
		// before overload resolution; checkFn's ANY-compatible first argument
		// is never consulted.
		"select unsigned_decimal_admission('not-a-number', cast(0 as decimal(10,2)), 'x')",
		"select unsigned_decimal_admission(n_name, cast(0 as decimal(10,2)), 'x') from nation",
		"select unsigned_decimal_admission(cast('2024-01-01' as date), cast(0 as decimal(10,2)), 'x')",
		// Arbitrary expression position and caller's letter case.
		"select 1 + unsigned_decimal_admission(n_nationkey, cast(0 as decimal(10,2)), 'x') from nation",
		"select UNSIGNED_DECIMAL_ADMISSION(NULL, cast(0 as decimal(10,2)), 'x')",
		"select u * unsigned_decimal_admission_ignore(NULL, cast(0 as decimal(10,2)), 'x') from (select 2 as u) dt",
	} {
		_, err := runOneStmt(mock, t, sql)
		require.Error(t, err, sql)
		require.Contains(t, err.Error(), "plan-internal",
			"direct admission calls must fail the visibility guard, not overload shape: %s", sql)
	}
}
