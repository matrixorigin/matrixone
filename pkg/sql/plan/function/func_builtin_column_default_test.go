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

package function

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// information_schema.COLUMNS.COLUMN_DEFAULT must expose the default value
// (MySQL behavior), not the SQL text of the literal.
func Test_BuiltIn_MoShowVisibleBinColumnDefault(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		origin string
		want   string
		null   bool
	}{
		{origin: "", null: true},
		{origin: "null", null: true},
		{origin: "NuLl", null: true},
		{origin: "'NULL'", want: "NULL"},
		{origin: "NULL", null: true},
		{origin: "'n'", want: "n"},
		{origin: "''", want: ""},
		{origin: "'it''s'", want: "it's"},
		{origin: `'a\\b'`, want: `a\b`},
		{origin: `'a\%b'`, want: `a\%b`},
		{origin: "'2024-01-01'", want: "2024-01-01"},
		{origin: `'a\nb\tc\r\0\b\Z'`, want: "a\nb\tc\r\x00\b\x1a"},
		{origin: `'a\_b'`, want: `a\_b`},
		{origin: `'a\qb'`, want: "aqb"},
		{origin: `'tail\\'`, want: `tail\`},
		{origin: `'tail\'`, want: `'tail\'`},
		{origin: `'unterminated`, want: `'unterminated`},
		{origin: "'a' + 'b'", want: "'a' + 'b'"},
		{origin: "9", want: "9"},
		{origin: "current_timestamp()", want: "current_timestamp()"},
		{origin: "(json_array())", want: "(json_array())"},
		{origin: "('x')", want: "('x')"},
		{origin: "'a' 'b'", want: "'a' 'b'"},
	} {
		t.Run(tc.origin, func(t *testing.T) {
			defBytes, err := types.Encode(&plan.Default{OriginString: tc.origin, NullAbility: true})
			require.NoError(t, err)
			input := tcTemp{
				info: "show visible bin column default",
				inputs: []FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{string(defBytes)}, nil),
					NewFunctionTestInput(types.T_uint8.ToType(), []uint8{defaultExpr}, nil),
				},
				expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{tc.want}, []bool{tc.null}),
			}
			tcc := NewFunctionTestCase(proc, input.inputs, input.expect, builtInMoShowVisibleBin)
			succeed, info := tcc.RunAndFree()
			require.True(t, succeed, input.info, info)
		})
	}
}

func TestMoShowVisibleBinDefaultModesRemainSeparate(t *testing.T) {
	proc := testutil.NewProcess(t)
	defBytes, err := types.Encode(&plan.Default{OriginString: "'n'"})
	require.NoError(t, err)
	updateBytes, err := types.Encode(&plan.OnUpdate{OriginString: "'n'"})
	require.NoError(t, err)
	for _, tc := range []struct {
		mode    uint8
		payload []byte
		want    string
	}{
		{onUpdateExpr, updateBytes, "'n'"},
		{defaultExpr, defBytes, "n"},
		{defExprInShowColumns, defBytes, "n"},
	} {
		inputs := []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{string(tc.payload)}, nil),
			NewFunctionTestInput(types.T_uint8.ToType(), []uint8{tc.mode}, nil),
		}
		expected := NewFunctionTestResult(types.T_varchar.ToType(), false, []string{tc.want}, nil)
		testCase := NewFunctionTestCase(proc, inputs, expected, builtInMoShowVisibleBin)
		succeed, info := testCase.RunAndFree()
		require.True(t, succeed, info)
	}
}

// SHOW COLUMNS / DESC Default must expose the default value like MySQL
// (decoded literal), not a re-escaped SQL fragment. OriginString is already a
// SQL literal, so mode 4 must not apply formatStr (which doubled quotes).
func Test_BuiltIn_MoShowVisibleBinShowColumnsDefault(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		origin string
		want   string
		null   bool
	}{
		{origin: "", null: true},
		{origin: "null", null: true},
		{origin: "NULL", null: true},
		{origin: "'n'", want: "n"},
		{origin: "''", want: ""},
		{origin: "'it''s'", want: "it's"},
		{origin: "'x`y'", want: "x`y"},
		{origin: `'a\\b'`, want: `a\b`},
		{origin: "'NULL'", want: "NULL"},
		{origin: "9", want: "9"},
		{origin: "current_timestamp()", want: "current_timestamp()"},
		{origin: "(json_array())", want: "(json_array())"},
		{origin: "('x')", want: "('x')"},
	} {
		t.Run(tc.origin, func(t *testing.T) {
			defBytes, err := types.Encode(&plan.Default{OriginString: tc.origin, NullAbility: true})
			require.NoError(t, err)
			input := tcTemp{
				info: "show visible bin show columns default",
				inputs: []FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{string(defBytes)}, nil),
					NewFunctionTestInput(types.T_uint8.ToType(), []uint8{defExprInShowColumns}, nil),
				},
				expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{tc.want}, []bool{tc.null}),
			}
			tcc := NewFunctionTestCase(proc, input.inputs, input.expect, builtInMoShowVisibleBin)
			succeed, info := tcc.RunAndFree()
			require.True(t, succeed, input.info, info)
		})
	}
}

// ---------------------------------------------------------------------------
// Issue #31161: typed COLUMN_DEFAULT rendering and the DEFAULT_GENERATED
// attribute. The plan.Default expression trees below are hand-built to the
// exact persisted shapes the planner probe pinned for each spelling (design
// 20261105-column-default-metadata-publication.md section 2.1). Every case
// asserts the Default rendering (modes 1 and 4) and the mode-7 expression
// classification together, so the two surfaces cannot drift (I3a/I4).
// ---------------------------------------------------------------------------

func cdType(id types.T, w, s int32) plan.Type {
	return plan.Type{Id: int32(id), Width: w, Scale: s}
}

func cdAttrType(id types.T, w, s int32, attrs uint32) plan.Type {
	return plan.Type{Id: int32(id), Width: w, Scale: s, NumericAttributes: attrs}
}

func cdCarrier(t plan.Type) *plan.Expr {
	return &plan.Expr{Typ: t, Expr: &plan.Expr_T{T: &plan.TargetType{}}}
}

func cdFn(name string, typ plan.Type, args ...*plan.Expr) *plan.Expr {
	return &plan.Expr{Typ: typ, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: name}, Args: args}}}
}

func cdLitExpr(typ plan.Type, lit *plan.Literal) *plan.Expr {
	return &plan.Expr{Typ: typ, Expr: &plan.Expr_Lit{Lit: lit}}
}

func cdLitDec64(w, s int32, a int64) *plan.Expr {
	return cdLitExpr(cdType(types.T_decimal64, w, s),
		&plan.Literal{Value: &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{A: a}}})
}

func cdLitDec128(w, s int32, a, b int64) *plan.Expr {
	return cdLitExpr(cdType(types.T_decimal128, w, s),
		&plan.Literal{Value: &plan.Literal_Decimal128Val{Decimal128Val: &plan.Decimal128{A: a, B: b}}})
}

func cdLitStrW(s string, w int32) *plan.Expr {
	return cdLitExpr(cdType(types.T_varchar, w, 0), &plan.Literal{Value: &plan.Literal_Sval{Sval: s}})
}

// cdLitStrText pins the binder-produced TEXT literal form of cast/admission
// internal string literals (probe: form=1).
func cdLitStrText(s string) *plan.Expr {
	return cdLitExpr(cdType(types.T_varchar, int32(len(s)), 0), &plan.Literal{
		Value:       &plan.Literal_Sval{Sval: s},
		LiteralForm: plan.StringLiteralForm_STRING_LITERAL_TEXT})
}

// cdLitStrBin pins an IsBin bit/hex literal payload (probe: form=4 BIT,
// form=3 HEX, one payload byte 0x33 for b'00110011' / 0x33 / x'33').
func cdLitStrBin(payload string, form plan.StringLiteralForm) *plan.Expr {
	return cdLitExpr(cdType(types.T_varchar, int32(len(payload)), 0), &plan.Literal{
		Value:       &plan.Literal_Sval{Sval: payload},
		IsBin:       true,
		LiteralForm: form})
}

func cdLitI64(v int64) *plan.Expr {
	return cdLitExpr(cdType(types.T_int64, 0, 0), &plan.Literal{Value: &plan.Literal_I64Val{I64Val: v}})
}

func cdLitF64(v float64) *plan.Expr {
	return cdLitExpr(cdType(types.T_float64, 0, 0), &plan.Literal{Value: &plan.Literal_Dval{Dval: v}})
}

func cdLitBool(v bool) *plan.Expr {
	return cdLitExpr(cdType(types.T_bool, 0, 0), &plan.Literal{Value: &plan.Literal_Bval{Bval: v}})
}

func cdLitBit(w int32, v uint64) *plan.Expr {
	return cdLitExpr(cdType(types.T_bit, w, -1), &plan.Literal{Value: &plan.Literal_U64Val{U64Val: v}})
}

// cdAdmission pins the volatile unsigned-decimal admission node: its Typ
// mirrors the source's Typ and its args are (source, attributed carrier,
// column name). With threeArg=false the two-argument legacy form is pinned.
func cdAdmission(src *plan.Expr, dst plan.Type, col string, threeArg bool) *plan.Expr {
	args := []*plan.Expr{src, cdCarrier(dst)}
	if threeArg {
		args = append(args, cdLitStrText(col))
	}
	return cdFn("unsigned_decimal_admission", src.Typ, args...)
}

// cdComposition pins the attributed-DECIMAL assignment boundary:
// cast_assign[unsigned_decimal_admission[src, carrier, col], carrier] with
// the declared attributed type on the outer node.
func cdComposition(src *plan.Expr, dst plan.Type, col string) *plan.Expr {
	return cdFn("cast_assign", dst, cdAdmission(src, dst, col, true), cdCarrier(dst))
}

func cdVarRef(name string, system, global bool) *plan.Expr {
	return &plan.Expr{Typ: plan.Type{Id: 71}, Expr: &plan.Expr_V{V: &plan.VarRef{
		Name: name, System: system, Global: global}}}
}

func cdColRef(typ plan.Type) *plan.Expr {
	return &plan.Expr{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}}
}

const cdDefaultGenerated = "DEFAULT_GENERATED"

func Test_BuiltIn_MoShowVisibleBinColumnDefaultTypedAndExtra(t *testing.T) {
	proc := testutil.NewProcess(t)

	dec64u104 := cdAttrType(types.T_decimal64, 10, 4, 1)
	dec64u100 := cdAttrType(types.T_decimal64, 10, 0, 1)
	dec64zf104 := cdAttrType(types.T_decimal64, 10, 4, 3)
	dec128u206 := cdAttrType(types.T_decimal128, 20, 6, 1)
	dec256u404 := cdAttrType(types.T_decimal256, 40, 4, 1)

	// Folded / internal literal shapes (probe 2.1 rows).
	litDec64S4 := cdLitDec64(10, 4, 12000) // decimal(10,4) default 1.2 / '1.2' / 1.2e0 / +1.2
	castLit12 := cdFn("cast", cdType(types.T_decimal64, 2, 1), cdLitStrText("1.2"), cdCarrier(cdType(types.T_decimal64, 2, 1)))
	castLit16 := cdFn("cast", cdType(types.T_decimal64, 2, 1), cdLitStrText("1.6"), cdCarrier(cdType(types.T_decimal64, 2, 1)))
	lit19 := cdLitStrText("1.4999999999999999999") // varchar(21)
	castLit19 := cdFn("cast", cdType(types.T_decimal128, 20, 19), lit19, cdCarrier(cdType(types.T_decimal128, 20, 19)))
	isBinBit := cdLitStrBin("\x33", plan.StringLiteralForm_STRING_LITERAL_BIT)
	isBinHex := cdLitStrBin("\x33", plan.StringLiteralForm_STRING_LITERAL_HEX)
	userCast := cdFn("cast", cdType(types.T_decimal64, 10, 0), castLit16, cdCarrier(cdType(types.T_decimal64, 10, 0)))
	plusNode := cdFn("+", cdType(types.T_int64, 0, 0), cdLitI64(1), cdLitI64(2))
	absNode := cdFn("abs", cdType(types.T_decimal64, 2, 1), castLit12)
	varTimeZone := cdVarRef("time_zone", true, false)
	colInt := cdColRef(cdType(types.T_int32, 32, -1))

	// A folded decimal256 const vector (LiteralVec{Len:1}) for the pure
	// decode path: decimal(40,4) value 1.2000.
	vecTyp := types.T_decimal256.ToType()
	vecTyp.Width, vecTyp.Scale = 40, 4
	vec := vector.NewVec(vecTyp)
	require.NoError(t, vector.AppendFixed(vec, types.Decimal256{B0_63: 12000}, false, proc.Mp()))
	vecBytes, err := vec.MarshalBinary()
	require.NoError(t, err)
	vec.Free(proc.Mp())
	litVecDec256 := &plan.Expr{Typ: cdType(types.T_decimal256, 40, 4),
		Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 1, Data: vecBytes}}}

	for _, tc := range []struct {
		name    string
		origin  string
		expr    *plan.Expr
		want    string
		null    bool
		wantExt string
	}{
		// Literal typed rendering (I1/I2).
		{"dec64 1.2", "1.2", litDec64S4, "1.2000", false, ""},
		{"dec64 1.200", "1.200", litDec64S4, "1.2000", false, ""},
		{"dec64 quoted 1.2", "'1.2'", litDec64S4, "1.2000", false, ""},
		{"dec64 scale0 rounded", "1.6", cdLitDec64(10, 0, 2), "2", false, ""},
		{"dec128 negative", "(-12.34)", cdLitDec128(20, 6, -12340000, -1), "-12.340000", false, ""},
		{"bit8 bit literal", "0b00110011", cdLitBit(8, 51), "b'110011'", false, ""},
		{"bit8 numeric", "51", cdLitBit(8, 51), "b'110011'", false, ""},
		{"bit1 zero", "0b0", cdLitBit(1, 0), "b'0'", false, ""},
		{"bit64 one", "0b1", cdLitBit(64, 1), "b'1'", false, ""},
		{"dec64 folded bit spelling", "0b00110011", cdLitDec64(10, 4, 51), "0.0051", false, ""},
		{"dec64 folded true", "true", cdLitDec64(10, 4, 10000), "1.0000", false, ""},
		{"dec64 folded scientific", "1.2e0", litDec64S4, "1.2000", false, ""},
		{"dec64 folded signed", "(+1.2)", litDec64S4, "1.2000", false, ""},
		{"dec256 const vec", "1.2", litVecDec256, "1.2000", false, ""},
		{"mediumint", "7", cdFn("cast_assign", cdType(types.T_int32, 24, -1), cdLitI64(7), cdCarrier(cdType(types.T_int32, 24, -1))), "7", false, ""},
		{"int quoted", "'7'", cdLitExpr(cdType(types.T_int32, 32, -1), &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 7}}), "7", false, ""},
		{"float raw", "1.50", cdLitExpr(cdType(types.T_float32, 0, -1), &plan.Literal{Value: &plan.Literal_Fval{Fval: 1.5}}), "1.50", false, ""},
		{"date", "'2024-01-01'", cdLitExpr(cdType(types.T_date, 0, 0), &plan.Literal{Value: &plan.Literal_Dateval{Dateval: 738885}}), "2024-01-01", false, ""},
		{"enum", "'b'", cdFn("cast_value_to_index", cdType(types.T_enum, 0, 0), cdLitStrText("a,b"), cdLitStrText("b")), "b", false, ""},
		// ENUM/SET literal defaults persist beneath the planner's sibling
		// value-to-index wrappers: the integer-input enum spelling
		// (cast_index_value_to_index) and both SET spellings are the same
		// conversion machinery and classify as literals like MySQL.
		{"enum numeric", "1",
			cdFn("cast_index_value_to_index", cdType(types.T_enum, 0, 0), cdLitStrText("T,E"),
				cdFn("cast", cdType(types.T_int64, 0, 0), cdLitI64(1), cdCarrier(cdType(types.T_int64, 0, 0)))),
			"1", false, ""},
		{"set string", "''",
			cdFn("cast_set_value_to_index", plan.Type{Id: int32(types.T_uint64), Enumvalues: ","}, cdLitStrText(","), cdLitStrText("")),
			"", false, ""},
		{"set numeric", "3",
			cdFn("cast_set_index_value_to_index", plan.Type{Id: int32(types.T_uint64), Enumvalues: "a,b"}, cdLitStrText("a,b"),
				cdFn("cast", cdType(types.T_int64, 0, 0), cdLitI64(3), cdCarrier(cdType(types.T_int64, 0, 0)))),
			"3", false, ""},
		{"enum wrapper col ref", "a",
			cdFn("cast_index_value_to_index", cdType(types.T_enum, 0, 0), cdLitStrText("T,E"), cdColRef(cdType(types.T_int64, 0, 0))),
			"a", false, cdDefaultGenerated},
		{"varchar", "'x'", cdLitStrW("x", 10), "x", false, ""},
		{"null", "null", cdLitExpr(cdType(types.T_int32, 32, -1), &plan.Literal{Isnull: true}), "", true, ""},
		{"no default", "", nil, "", true, ""},
		{"nil expr fallback", "1.2", nil, "1.2", false, ""},

		// Attributed-DECIMAL literal compositions (I1).
		{"attr dec64 1.2", "1.2", cdComposition(castLit12, dec64u104, "d"), "1.2000", false, ""},
		{"attr dec64 quoted", "'1.2'", cdComposition(cdLitStrText("1.2"), dec64u104, "d"), "1.2000", false, ""},
		{"attr dec64 zerofill", "1.2", cdComposition(castLit12, dec64zf104, "d"), "1.2000", false, ""},
		{"attr dec64 scale0 rounded", "1.6", cdComposition(castLit16, dec64u100, "d"), "2", false, ""},
		{"attr dec256 1.2", "1.2", cdComposition(castLit12, dec256u404, "d"), "1.2000", false, ""},
		{"attr dec64 legacy 2-arg", "1.2",
			cdFn("cast_assign", dec64u104, cdAdmission(cdLitStrText("1.2"), dec64u104, "", false), cdCarrier(dec64u104)), "1.2000", false, ""},
		{"attr scale0 19 digits quoted", "'1.4999999999999999999'", cdComposition(lit19, dec64u100, "d"), "1", false, ""},
		{"attr scale0 19 digits unquoted", "1.4999999999999999999", cdComposition(castLit19, dec64u100, "d"), "1", false, ""},
		{"attr dec128 19 digits", "'1.4999999999999999999'", cdComposition(lit19, dec128u206, "d"), "1.500000", false, ""},
		{"attr float64", "1.2e0", cdComposition(cdLitF64(1.2), dec64u104, "d"), "1.2000", false, ""},
		{"attr dec256 float64", "1.2e0", cdComposition(cdLitF64(1.2), dec256u404, "d"), "1.2000", false, ""},
		{"attr true", "true", cdComposition(cdLitBool(true), dec64u104, "d"), "1.0000", false, ""},
		{"attr false", "false", cdComposition(cdLitBool(false), dec64u104, "d"), "0.0000", false, ""},
		{"attr signed literal", "(+1.2)",
			cdComposition(cdFn("unary_plus", cdType(types.T_decimal64, 2, 1), castLit12), dec64u104, "d"), "1.2000", false, ""},
		{"attr signed float", "(+1.2e0)",
			cdComposition(cdFn("unary_plus", cdType(types.T_float64, 0, 0), cdLitF64(1.2)), dec64u104, "d"), "1.2000", false, ""},
		{"attr bit literal", "0b00110011", cdComposition(isBinBit, dec64u104, "d"), "0.0051", false, ""},
		{"attr bit literal scale0", "0b00110011", cdComposition(isBinBit, dec64u100, "d"), "51", false, ""},
		{"attr dec256 bit literal", "0b00110011", cdComposition(isBinBit, dec256u404, "d"), "0.0051", false, ""},
		{"attr hex literal", "0x33", cdComposition(isBinHex, dec64u104, "d"), "0.0051", false, ""},
		// Decimal-carrier widening (I1): an integer literal above uint64
		// binds as a raw decimal128 literal the attributed admission
		// preserves; the renderer widens and rescales it like the
		// executor's assignment cast. 2^64 = Decimal128{A:0, B:1} scale 0.
		{"attr dec256 uint64+1", "18446744073709551616",
			cdComposition(cdLitDec128(38, 0, 0, 1), dec256u404, "d"), "18446744073709551616.0000", false, ""},
		{"attr dec256 uint64+1 quoted", "'18446744073709551616'",
			cdComposition(cdLitStrText("18446744073709551616"), dec256u404, "d"), "18446744073709551616.0000", false, ""},
		{"dec256 cast dec128 carrier", "18446744073709551616",
			cdFn("cast", cdType(types.T_decimal256, 40, 4), cdLitDec128(38, 0, 0, 1), cdCarrier(cdType(types.T_decimal256, 40, 4))),
			"18446744073709551616.0000", false, ""},
		{"attr dec256 dec64 carrier", "1.2",
			cdComposition(cdLitDec64(18, 1, 12), dec256u404, "d"), "1.2000", false, ""},
		{"attr dec128 dec64 carrier", "1.2",
			cdComposition(cdLitDec64(18, 1, 12), dec128u206, "d"), "1.200000", false, ""},
		{"dec256 cast dec64 carrier", "1.2",
			cdFn("cast", cdType(types.T_decimal256, 40, 4), cdLitDec64(18, 1, 12), cdCarrier(cdType(types.T_decimal256, 40, 4))),
			"1.2000", false, ""},
		{"dec256 cast", "1.2", cdFn("cast", cdType(types.T_decimal256, 40, 4), cdLitStrText("1.2"), cdCarrier(cdType(types.T_decimal256, 40, 4))), "1.2000", false, ""},
		{"dec256 cast scale0", "1.6", cdFn("cast", cdType(types.T_decimal256, 40, 0), cdLitStrText("1.6"), cdCarrier(cdType(types.T_decimal256, 40, 0))), "2", false, ""},

		// Expression defaults: origin text kept and DEFAULT_GENERATED (I3/I3a).
		{"current_timestamp(6)", "current_timestamp(6)",
			cdFn("cast_strict", cdType(types.T_timestamp, 6, 6),
				cdFn("current_timestamp", cdType(types.T_timestamp, 0, 6), cdLitI64(6)), cdCarrier(cdType(types.T_timestamp, 6, 6))),
			"current_timestamp(6)", false, cdDefaultGenerated},
		{"uuid", "(uuid())",
			cdFn("cast_strict", cdType(types.T_char, 36, 0),
				cdFn("uuid", cdType(types.T_char, 0, 0)), cdCarrier(cdType(types.T_char, 36, 0))),
			"(uuid())", false, cdDefaultGenerated},
		{"json_array", "(json_array())", cdFn("json_array", cdType(types.T_json, 0, 0)), "(json_array())", false, cdDefaultGenerated},
		{"dec64 (1+2)", "(1 + 2)", cdLitDec64(10, 4, 30000), "(1 + 2)", false, cdDefaultGenerated},
		{"int (1+2)", "(1 + 2)", cdLitExpr(cdType(types.T_int32, 32, -1), &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 3}}), "(1 + 2)", false, cdDefaultGenerated},
		{"varchar ('x')", "('x')", cdLitStrW("x", 10), "('x')", false, cdDefaultGenerated},
		{"blob ('x')", "('x')", cdLitExpr(cdType(types.T_blob, 65535, 0), &plan.Literal{Value: &plan.Literal_Sval{Sval: "x"}}), "('x')", false, cdDefaultGenerated},
		{"dec256 (1+2)", "(1 + 2)", cdFn("cast", cdType(types.T_decimal256, 40, 4), cdLitI64(3), cdCarrier(cdType(types.T_decimal256, 40, 4))), "(1 + 2)", false, cdDefaultGenerated},
		{"attr (1+2)", "(1 + 2)", cdComposition(plusNode, dec64u104, "d"), "(1 + 2)", false, cdDefaultGenerated},
		{"attr explicit cast", "cast(1.6 as decimal(10))", cdComposition(userCast, dec64u104, "d"), "cast(1.6 as decimal(10))", false, cdDefaultGenerated},
		{"folded explicit cast", "cast(1.6 as decimal(10))", cdLitDec64(10, 4, 20000), "cast(1.6 as decimal(10))", false, cdDefaultGenerated},
		{"attr parenthesized cast", "(cast(1.6 as decimal(10)))", cdComposition(userCast, dec64u104, "d"), "(cast(1.6 as decimal(10)))", false, cdDefaultGenerated},
		{"folded abs", "abs(1.2)", litDec64S4, "abs(1.2)", false, cdDefaultGenerated},
		{"attr abs", "abs(1.2)", cdComposition(absNode, dec64u104, "d"), "abs(1.2)", false, cdDefaultGenerated},
		{"folded round", "round(1.25, 1)", cdLitDec64(10, 4, 13000), "round(1.25, 1)", false, cdDefaultGenerated},
		{"quoted cast control", "'cast(1.6 as decimal(10,0))'", cdLitStrW("cast(1.6 as decimal(10,0))", 40), "cast(1.6 as decimal(10,0))", false, ""},
		{"quoted abs control", "'abs(1.2)'", cdLitStrW("abs(1.2)", 40), "abs(1.2)", false, ""},
		{"doubled quote control", "'a''abs(1.2)'", cdLitStrW("a'abs(1.2)", 40), "a'abs(1.2)", false, ""},

		// Column-reference defaults (rule (d)).
		{"col ref bare", "a", colInt, "a", false, cdDefaultGenerated},
		{"col ref parenthesized", "(a)", colInt, "(a)", false, cdDefaultGenerated},
		{"col ref arithmetic", "(a + 1)",
			cdFn("cast", cdType(types.T_int32, 32, -1),
				cdFn("+", cdType(types.T_int64, 64, -1),
					cdFn("cast", cdType(types.T_int64, 64, -1), colInt, cdCarrier(cdType(types.T_int64, 64, -1))), cdLitI64(1)),
				cdCarrier(cdType(types.T_int32, 32, -1))),
			"(a + 1)", false, cdDefaultGenerated},
		{"attr col ref", "a", cdComposition(colInt, dec64u104, "b"), "a", false, cdDefaultGenerated},
		{"attr col ref parenthesized", "(a)", cdComposition(colInt, dec64u104, "b"), "(a)", false, cdDefaultGenerated},

		// Variable-reference defaults (rule (e)).
		{"sys var", "@@time_zone",
			cdFn("cast_strict", cdType(types.T_varchar, 64, 0), varTimeZone, cdCarrier(cdType(types.T_varchar, 64, 0))),
			"@@time_zone", false, cdDefaultGenerated},
		{"sys var parenthesized", "(@@time_zone)",
			cdFn("cast_strict", cdType(types.T_varchar, 64, 0), varTimeZone, cdCarrier(cdType(types.T_varchar, 64, 0))),
			"(@@time_zone)", false, cdDefaultGenerated},
		{"global var", "@@global.time_zone",
			cdFn("cast_strict", cdType(types.T_varchar, 64, 0), cdVarRef("time_zone", true, true), cdCarrier(cdType(types.T_varchar, 64, 0))),
			"@@global.time_zone", false, cdDefaultGenerated},
		{"user var", "@probe_user_var",
			cdFn("cast_strict", cdType(types.T_varchar, 64, 0), cdVarRef("probe_user_var", false, false), cdCarrier(cdType(types.T_varchar, 64, 0))),
			"@probe_user_var", false, cdDefaultGenerated},
		{"user var parenthesized", "(@probe_user_var)",
			cdFn("cast_strict", cdType(types.T_varchar, 64, 0), cdVarRef("probe_user_var", false, false), cdCarrier(cdType(types.T_varchar, 64, 0))),
			"(@probe_user_var)", false, cdDefaultGenerated},
		{"int var", "@@sort_buffer_size",
			cdFn("cast", cdType(types.T_int32, 32, -1), cdVarRef("sort_buffer_size", true, false), cdCarrier(cdType(types.T_int32, 32, -1))),
			"@@sort_buffer_size", false, cdDefaultGenerated},
		{"attr sys var", "@@time_zone", cdComposition(varTimeZone, dec64u104, "d"), "@@time_zone", false, cdDefaultGenerated},
		{"attr sys var parenthesized", "(@@time_zone)", cdComposition(varTimeZone, dec64u104, "d"), "(@@time_zone)", false, cdDefaultGenerated},
		{"quoted var control", "'@@time_zone'", cdLitStrW("@@time_zone", 64), "@@time_zone", false, ""},

		// Classifier negatives: shape-mirror and sign-chain guards.
		{"stray admission root", "1.2",
			cdFn("unsigned_decimal_admission", cdType(types.T_varchar, 3, 0), cdLitStrText("1.2"), cdCarrier(dec64u104)),
			"1.2", false, cdDefaultGenerated},
		{"admission carrier mismatch", "1.2",
			cdFn("cast_assign", dec64u104,
				cdFn("unsigned_decimal_admission", cdType(types.T_varchar, 3, 0),
					cdLitStrText("1.2"), cdCarrier(cdType(types.T_decimal64, 10, 4)), cdLitStrText("d")),
				cdCarrier(dec64u104)),
			"1.2", false, cdDefaultGenerated},
		{"sign over arithmetic", "+1+2", cdFn("unary_plus", cdType(types.T_int64, 0, 0), plusNode), "+1+2", false, cdDefaultGenerated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defBytes, err := types.Encode(&plan.Default{OriginString: tc.origin, NullAbility: true, Expr: tc.expr})
			require.NoError(t, err)
			for _, mode := range []struct {
				mode uint8
				want string
				null bool
			}{
				{defaultExpr, tc.want, tc.null},
				{defExprInShowColumns, tc.want, tc.null},
				{generatedDefaultExtra, tc.wantExt, false},
			} {
				input := tcTemp{
					info: "show visible bin typed default / extra",
					inputs: []FunctionTestInput{
						NewFunctionTestInput(types.T_varchar.ToType(), []string{string(defBytes)}, nil),
						NewFunctionTestInput(types.T_uint8.ToType(), []uint8{mode.mode}, nil),
					},
					expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{mode.want}, []bool{mode.null}),
				}
				tcc := NewFunctionTestCase(proc, input.inputs, input.expect, builtInMoShowVisibleBin)
				succeed, info := tcc.RunAndFree()
				require.True(t, succeed, input.info, info, "mode %d", mode.mode)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Branch coverage for the typed literal conversion helpers (issue #31161,
// design 20261105-column-default-metadata-publication.md section 4.3). Every
// literal carrier kind converts the way the executor's assignment cast does;
// unconvertible or mismatched shapes report ok=false so the metadata
// surfaces keep the origin text. Expected strings are the executor-typed
// values pinned by the planner probe in design section 2.1.
// ---------------------------------------------------------------------------

func cdLitI8(v int32) *plan.Expr {
	return cdLitExpr(cdType(types.T_int8, 0, 0), &plan.Literal{Value: &plan.Literal_I8Val{I8Val: v}})
}

func cdLitI16(v int32) *plan.Expr {
	return cdLitExpr(cdType(types.T_int16, 0, 0), &plan.Literal{Value: &plan.Literal_I16Val{I16Val: v}})
}

func cdLitI32(v int32) *plan.Expr {
	return cdLitExpr(cdType(types.T_int32, 0, 0), &plan.Literal{Value: &plan.Literal_I32Val{I32Val: v}})
}

func cdLitU8(v uint32) *plan.Expr {
	return cdLitExpr(cdType(types.T_uint8, 0, 0), &plan.Literal{Value: &plan.Literal_U8Val{U8Val: v}})
}

func cdLitU16(v uint32) *plan.Expr {
	return cdLitExpr(cdType(types.T_uint16, 0, 0), &plan.Literal{Value: &plan.Literal_U16Val{U16Val: v}})
}

func cdLitU32(v uint32) *plan.Expr {
	return cdLitExpr(cdType(types.T_uint32, 0, 0), &plan.Literal{Value: &plan.Literal_U32Val{U32Val: v}})
}

func cdLitU64(v uint64) *plan.Expr {
	return cdLitExpr(cdType(types.T_uint64, 0, 0), &plan.Literal{Value: &plan.Literal_U64Val{U64Val: v}})
}

func cdLitF32(v float32) *plan.Expr {
	return cdLitExpr(cdType(types.T_float32, 0, -1), &plan.Literal{Value: &plan.Literal_Fval{Fval: v}})
}

// cdLitDec64As builds a decimal64 carrier literal stamped with a mismatched
// declared type, pinning the id-mismatch rejection of the widening paths.
func cdLitDec64As(id types.T, w, s int32, a int64) *plan.Expr {
	return cdLitExpr(cdType(id, w, s),
		&plan.Literal{Value: &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{A: a}}})
}

func cdLitDec128As(id types.T, w, s int32, a, b int64) *plan.Expr {
	return cdLitExpr(cdType(id, w, s),
		&plan.Literal{Value: &plan.Literal_Decimal128Val{Decimal128Val: &plan.Decimal128{A: a, B: b}}})
}

func cdVecDec256(t *testing.T, proc *process.Process, w, s int32, values ...types.Decimal256) *plan.Expr {
	vecTyp := types.T_decimal256.ToType()
	vecTyp.Width, vecTyp.Scale = w, s
	vec := vector.NewVec(vecTyp)
	for _, v := range values {
		require.NoError(t, vector.AppendFixed(vec, v, false, proc.Mp()))
	}
	vecBytes, err := vec.MarshalBinary()
	require.NoError(t, err)
	vec.Free(proc.Mp())
	return &plan.Expr{Typ: cdType(types.T_decimal256, w, s),
		Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: int32(len(values)), Data: vecBytes}}}
}

func TestConvertLiteralToDecimal64Branches(t *testing.T) {
	for _, tc := range []struct {
		name  string
		inner *plan.Expr
		w, s  int32
		want  string
		ok    bool
	}{
		{"f32", cdLitF32(1.5), 10, 4, "1.5000", true},
		{"i8", cdLitI8(-8), 10, 4, "-8.0000", true},
		{"i16", cdLitI16(1600), 10, 2, "1600.00", true},
		{"i32", cdLitI32(-32), 10, 0, "-32", true},
		{"i64", cdLitI64(64), 10, 1, "64.0", true},
		{"u8", cdLitU8(8), 10, 4, "8.0000", true},
		{"u16", cdLitU16(1600), 10, 2, "1600.00", true},
		{"u32", cdLitU32(32), 10, 0, "32", true},
		{"u64", cdLitU64(64), 10, 1, "64.0", true},
		// A uint64 magnitude beyond int64 wraps negative inside Decimal64,
		// so the renderer rejects it like the assignment cast (#30367).
		{"u64 beyond int64", cdLitU64(math.MaxInt64 + 1), 18, 0, "", false},
		{"i64 width overflow", cdLitI64(12345), 4, 0, "", false},
		{"u64 width overflow", cdLitU64(12345), 4, 0, "", false},
		{"i64 scale overflow", cdLitI64(1), 18, 25, "", false},
		{"u64 scale overflow", cdLitU64(1), 18, 25, "", false},
		{"bool width overflow", cdLitBool(true), 1, 1, "", false},
		{"bool scale overflow", cdLitBool(true), 18, 25, "", false},
		{"dec64 carrier id mismatch", cdLitDec64As(types.T_decimal128, 20, 6, 12), 18, 1, "", false},
		{"dec64 carrier scale overflow", cdLitDec64(10, 0, 12000), 18, 25, "", false},
		{"dec64 carrier width overflow", cdLitDec64(10, 0, 12345), 4, 0, "", false},
		{"vec rejected", &plan.Expr{Typ: cdType(types.T_decimal64, 10, 4),
			Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 1, Data: []byte("x")}}}, 10, 4, "", false},
		{"col ref rejected", cdColRef(cdType(types.T_decimal64, 10, 4)), 10, 4, "", false},
		{"date rejected", cdLitExpr(cdType(types.T_date, 0, 0),
			&plan.Literal{Value: &plan.Literal_Dateval{Dateval: 738885}}), 10, 4, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, ok := convertLiteralToDecimal64(tc.inner, tc.w, tc.s)
			require.Equal(t, tc.ok, ok)
			if tc.ok {
				require.Equal(t, tc.want, v.Format(tc.s))
			}
		})
	}
}

func TestConvertLiteralToDecimal128Branches(t *testing.T) {
	for _, tc := range []struct {
		name  string
		inner *plan.Expr
		w, s  int32
		want  string
		ok    bool
	}{
		// An IsBin payload decodes as the big-endian base-256 coefficient,
		// so b'00110011' (0x33) renders 51 at the declared scale.
		{"bin", cdLitStrBin("\x33", plan.StringLiteralForm_STRING_LITERAL_BIT), 20, 6, "0.000051", true},
		{"f64", cdLitF64(1.5), 20, 6, "1.500000", true},
		{"f32", cdLitF32(2.5), 20, 6, "2.500000", true},
		{"bool false", cdLitBool(false), 20, 6, "0.000000", true},
		{"bool true", cdLitBool(true), 20, 6, "1.000000", true},
		{"bool width overflow", cdLitBool(true), 1, 1, "", false},
		{"bool scale overflow", cdLitBool(true), 38, 39, "", false},
		{"i8", cdLitI8(-8), 20, 6, "-8.000000", true},
		{"i16", cdLitI16(1600), 20, 2, "1600.00", true},
		{"i32", cdLitI32(-32), 20, 0, "-32", true},
		{"i64", cdLitI64(64), 20, 1, "64.0", true},
		{"i64 negative", cdLitI64(-64), 20, 1, "-64.0", true},
		{"i64 width overflow", cdLitI64(12345), 4, 0, "", false},
		{"i64 scale overflow", cdLitI64(1), 38, 39, "", false},
		{"u8", cdLitU8(8), 20, 6, "8.000000", true},
		{"u16", cdLitU16(1600), 20, 2, "1600.00", true},
		{"u32", cdLitU32(32), 20, 0, "32", true},
		{"u64", cdLitU64(64), 20, 1, "64.0", true},
		{"u64 max", cdLitU64(math.MaxUint64), 20, 0, "18446744073709551615", true},
		{"u64 width overflow", cdLitU64(12345), 4, 0, "", false},
		{"u64 scale overflow", cdLitU64(1), 38, 39, "", false},
		{"dec128 carrier id mismatch", cdLitDec128As(types.T_decimal64, 10, 1, 12, 0), 20, 6, "", false},
		{"dec128 carrier scale overflow", cdLitDec128(38, 0, 1, 0), 38, 39, "", false},
		{"dec128 carrier width overflow", cdLitDec128(20, 0, 12345, 0), 4, 0, "", false},
		{"dec64 carrier id mismatch", cdLitDec64As(types.T_decimal128, 20, 6, 12), 20, 6, "", false},
		{"dec64 carrier scale overflow", cdLitDec64(18, 1, 12), 38, 39, "", false},
		{"dec64 carrier width overflow", cdLitDec64(18, 1, 123456), 4, 1, "", false},
		{"vec rejected", &plan.Expr{Typ: cdType(types.T_decimal128, 20, 6),
			Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 1, Data: []byte("x")}}}, 20, 6, "", false},
		{"col ref rejected", cdColRef(cdType(types.T_decimal128, 20, 6)), 20, 6, "", false},
		{"date rejected", cdLitExpr(cdType(types.T_date, 0, 0),
			&plan.Literal{Value: &plan.Literal_Dateval{Dateval: 738885}}), 20, 6, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, ok := convertLiteralToDecimal128(tc.inner, tc.w, tc.s)
			require.Equal(t, tc.ok, ok)
			if tc.ok {
				require.Equal(t, tc.want, v.Format(tc.s))
			}
		})
	}
}

func TestConvertLiteralToDecimal256Branches(t *testing.T) {
	proc := testutil.NewProcess(t)
	// A decimal128 const vector never reaches the decimal256 vec decode
	// (type mismatch), and a corrupt payload fails the binary decode.
	dec128VecTyp := types.T_decimal128.ToType()
	dec128VecTyp.Width, dec128VecTyp.Scale = 38, 0
	dec128Vec := vector.NewVec(dec128VecTyp)
	require.NoError(t, vector.AppendFixed(dec128Vec, types.Decimal128{B0_63: 1}, false, proc.Mp()))
	dec128VecBytes, err := dec128Vec.MarshalBinary()
	require.NoError(t, err)
	dec128Vec.Free(proc.Mp())
	nullVec := func() *plan.Expr {
		vecTyp := types.T_decimal256.ToType()
		vecTyp.Width, vecTyp.Scale = 40, 4
		vec := vector.NewVec(vecTyp)
		require.NoError(t, vector.AppendFixed(vec, types.Decimal256{}, true, proc.Mp()))
		vecBytes, err := vec.MarshalBinary()
		require.NoError(t, err)
		vec.Free(proc.Mp())
		return &plan.Expr{Typ: cdType(types.T_decimal256, 40, 4),
			Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 1, Data: vecBytes}}}
	}()

	for _, tc := range []struct {
		name  string
		inner *plan.Expr
		w, s  int32
		want  string
		ok    bool
	}{
		{"f32", cdLitF32(1.5), 40, 4, "1.5000", true},
		{"bool false", cdLitBool(false), 40, 4, "0.0000", true},
		{"bool true", cdLitBool(true), 40, 4, "1.0000", true},
		{"bool width overflow", cdLitBool(true), 1, 1, "", false},
		{"bool scale overflow", cdLitBool(true), 76, 78, "", false},
		{"i8", cdLitI8(-8), 40, 4, "-8.0000", true},
		{"i16", cdLitI16(1600), 40, 2, "1600.00", true},
		{"i32", cdLitI32(-32), 40, 0, "-32", true},
		{"i64", cdLitI64(64), 40, 1, "64.0", true},
		{"i64 negative", cdLitI64(-64), 40, 1, "-64.0", true},
		{"i64 width overflow", cdLitI64(12345), 4, 0, "", false},
		{"i64 scale overflow", cdLitI64(1), 76, 78, "", false},
		{"u8", cdLitU8(8), 40, 4, "8.0000", true},
		{"u16", cdLitU16(1600), 40, 2, "1600.00", true},
		{"u32", cdLitU32(32), 40, 0, "32", true},
		{"u64", cdLitU64(64), 40, 1, "64.0", true},
		{"u64 max", cdLitU64(math.MaxUint64), 40, 4, "18446744073709551615.0000", true},
		{"u64 width overflow", cdLitU64(12345), 4, 0, "", false},
		{"u64 scale overflow", cdLitU64(1), 76, 78, "", false},
		{"dec64 carrier id mismatch", cdLitDec64As(types.T_decimal128, 20, 6, 12), 40, 4, "", false},
		{"dec64 carrier scale overflow", cdLitDec64(18, 1, 12), 76, 78, "", false},
		{"dec64 carrier width overflow", cdLitDec64(18, 1, 123456), 4, 1, "", false},
		{"dec128 carrier id mismatch", cdLitDec128As(types.T_decimal64, 10, 1, 12, 0), 40, 4, "", false},
		{"dec128 carrier scale overflow", cdLitDec128(38, 0, 1, 0), 76, 78, "", false},
		{"dec128 carrier width overflow", cdLitDec128(38, 0, 12345, 0), 4, 0, "", false},
		{"vec corrupt payload", &plan.Expr{Typ: cdType(types.T_decimal256, 40, 4),
			Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 1, Data: []byte("short-garbage")}}}, 40, 4, "", false},
		{"vec wrong type", &plan.Expr{Typ: cdType(types.T_decimal256, 40, 4),
			Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 1, Data: dec128VecBytes}}}, 40, 4, "", false},
		{"vec null entry", nullVec, 40, 4, "", false},
		{"vec two entries", cdVecDec256(t, proc, 40, 4,
			types.Decimal256{B0_63: 12000}, types.Decimal256{B0_63: 12000}), 40, 4, "", false},
		{"vec scale overflow", cdVecDec256(t, proc, 40, 1, types.Decimal256{B0_63: 12}), 76, 78, "", false},
		{"vec width overflow", cdVecDec256(t, proc, 40, 0, types.Decimal256{B0_63: 12345}), 4, 0, "", false},
		{"col ref rejected", cdColRef(cdType(types.T_decimal256, 40, 4)), 40, 4, "", false},
		{"null rejected", cdLitExpr(cdType(types.T_decimal256, 40, 4), &plan.Literal{Isnull: true}), 40, 4, "", false},
		{"date rejected", cdLitExpr(cdType(types.T_date, 0, 0),
			&plan.Literal{Value: &plan.Literal_Dateval{Dateval: 738885}}), 40, 4, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, ok := convertLiteralToDecimal256(tc.inner, tc.w, tc.s)
			require.Equal(t, tc.ok, ok)
			if tc.ok {
				require.Equal(t, tc.want, v.Format(tc.s))
			}
		})
	}
}

func TestRenderBitDefaultLiteralBranches(t *testing.T) {
	for _, tc := range []struct {
		name  string
		inner *plan.Expr
		want  string
		ok    bool
	}{
		{"u8", cdLitU8(5), "b'101'", true},
		{"u16", cdLitU16(5), "b'101'", true},
		{"u32", cdLitU32(5), "b'101'", true},
		{"i64", cdLitI64(5), "b'101'", true},
		{"i64 negative", cdLitI64(-1), "", false},
		{"i32", cdLitI32(5), "b'101'", true},
		{"i32 negative", cdLitI32(-1), "", false},
		{"string rejected", cdLitStrText("x"), "", false},
		{"null rejected", cdLitExpr(cdType(types.T_bit, 8, -1), &plan.Literal{Isnull: true}), "", false},
		{"col ref rejected", cdColRef(cdType(types.T_bit, 8, -1)), "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := renderBitDefaultLiteral(tc.inner)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRenderTypedLiteralDefaultFallbacks(t *testing.T) {
	minus := func(inner *plan.Expr) *plan.Expr {
		return cdFn("unary_minus", inner.Typ, inner)
	}
	dec128Cast := cdFn("cast", cdType(types.T_decimal128, 20, 6),
		cdLitStrText("12.34"), cdCarrier(cdType(types.T_decimal128, 20, 6)))
	dec256Cast := cdFn("cast", cdType(types.T_decimal256, 40, 4),
		cdLitStrText("1.2"), cdCarrier(cdType(types.T_decimal256, 40, 4)))
	for _, tc := range []struct {
		name string
		expr *plan.Expr
		want string
		ok   bool
	}{
		// A canonical sign chain flips the rendered typed value.
		{"dec64 unary minus", minus(cdLitDec64(10, 4, 12000)), "-1.2000", true},
		{"dec64 double minus", minus(minus(cdLitDec64(10, 4, 12000))), "1.2000", true},
		{"dec128 unary minus", minus(dec128Cast), "-12.340000", true},
		{"dec256 unary minus", minus(dec256Cast), "-1.2000", true},
		// Defensive fallbacks keep the origin text (ok=false): a transparent
		// chain into a nil leaf, and a decimal256-only const vector under a
		// decimal64 destination.
		{"nil leaf", cdFn("cast", cdType(types.T_decimal64, 10, 4), (*plan.Expr)(nil)), "", false},
		{"dec64 vec", &plan.Expr{Typ: cdType(types.T_decimal64, 10, 4),
			Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 1, Data: []byte("x")}}}, "", false},
		{"dec128 vec", &plan.Expr{Typ: cdType(types.T_decimal128, 20, 6),
			Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 1, Data: []byte("x")}}}, "", false},
		{"dec256 unconvertible", cdFn("cast", cdType(types.T_decimal256, 40, 4),
			cdLitExpr(cdType(types.T_date, 0, 0), &plan.Literal{Value: &plan.Literal_Dateval{Dateval: 738885}}),
			cdCarrier(cdType(types.T_decimal256, 40, 4))), "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := renderTypedLiteralDefault(&plan.Default{OriginString: "1.2", Expr: tc.expr})
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestPeelTransparentDefaultStops(t *testing.T) {
	// A non-transparent function stops the peel unchanged; a transparent
	// chain into a nil leaf yields nil.
	absNode := cdFn("abs", cdType(types.T_decimal64, 2, 1), cdLitStrText("1.2"))
	inner, sign := peelTransparentDefault(absNode)
	require.Same(t, absNode, inner)
	require.Equal(t, 1, sign)
	inner, sign = peelTransparentDefault(cdFn("cast", cdType(types.T_decimal64, 10, 4), (*plan.Expr)(nil)))
	require.Nil(t, inner)
	require.Equal(t, 1, sign)
}

func TestMatchAdmissionCompositionGuards(t *testing.T) {
	dec64u104 := cdAttrType(types.T_decimal64, 10, 4, 1)
	src := cdLitStrText("1.2")
	admission := func(name string, args ...*plan.Expr) *plan.Expr {
		return cdFn(name, src.Typ, args...)
	}
	for _, tc := range []struct {
		name string
		expr *plan.Expr
		ok   bool
	}{
		// A bare attributed literal has no function node to match.
		{"attributed literal root", cdLitExpr(dec64u104,
			&plan.Literal{Value: &plan.Literal_Sval{Sval: "1.2"}}), false},
		// A two-argument non-cast outer node never starts the composition.
		{"non cast outer", cdFn("abs", dec64u104, src, cdCarrier(dec64u104)), false},
		// A cast whose inner node is a literal or another function is not
		// the admission composition; the name probe reports false.
		{"inner literal", cdFn("cast", dec64u104, src, cdCarrier(dec64u104)), false},
		{"inner non admission fn", cdFn("cast", dec64u104,
			cdFn("abs", src.Typ, src), cdCarrier(dec64u104)), false},
		// The strict/ignore admission spellings are the same planner
		// machinery and match.
		{"strict admission", cdFn("cast_assign", dec64u104,
			admission("unsigned_decimal_admission_strict", src, cdCarrier(dec64u104)),
			cdCarrier(dec64u104)), true},
		{"ignore admission", cdFn("cast_assign", dec64u104,
			admission("unsigned_decimal_admission_ignore", src, cdCarrier(dec64u104)),
			cdCarrier(dec64u104)), true},
		// Four-argument and one-argument admissions are stray shapes.
		{"four arg admission", cdFn("cast_assign", dec64u104,
			admission("unsigned_decimal_admission", src, cdCarrier(dec64u104), src, src),
			cdCarrier(dec64u104)), false},
		// An attributed carrier must mirror the outer type exactly.
		{"carrier width mismatch", cdFn("cast_assign", dec64u104,
			admission("unsigned_decimal_admission", src, cdCarrier(cdAttrType(types.T_decimal64, 12, 4, 1))),
			cdCarrier(dec64u104)), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := matchAdmissionComposition(tc.expr)
			require.Equal(t, tc.ok, ok)
			if tc.ok {
				require.Same(t, src, got)
			}
		})
	}
}

func TestIsSignedNumericLiteralTextEdges(t *testing.T) {
	for _, tc := range []struct {
		s    string
		want bool
	}{
		{"+1.2", true},
		{"-12.34", true},
		{"+1.2e0", true},
		{"-1.2E-5", true},
		{"+1.2e+5", true},
		{"+.5", true},
		{"+1.", true},
		{"+", false},
		{"1.2", false},
		{"+e5", false},
		{"+.", false},
		{"+1.2e", false},
		{"+1.2e+", false},
		{"+ab", false},
		{"+1x", false},
	} {
		t.Run(tc.s, func(t *testing.T) {
			require.Equal(t, tc.want, isSignedNumericLiteralText(tc.s))
		})
	}
}

func TestOriginHasCallSyntaxQuotedSpans(t *testing.T) {
	for _, tc := range []struct {
		name   string
		origin string
		want   bool
	}{
		// Call syntax inside double-quoted or backtick spans is a string or
		// an identifier, not a call; unterminated spans scan to the end.
		{"double quoted call", `"a()"`, false},
		{"backtick quoted call", "`a()`", false},
		{"unterminated double quote", `"a()`, false},
		{"unterminated backtick", "`a()", false},
		{"call after quoted span", `"q" || f()`, true},
		{"plain call", "uuid()", true},
		{"space before paren", "x ()", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, originHasCallSyntax(tc.origin))
		})
	}
}
