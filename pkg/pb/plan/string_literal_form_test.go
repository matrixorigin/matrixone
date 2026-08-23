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

	"github.com/stretchr/testify/require"
)

func TestValidateStringLiteralFormsAfterWireDecode(t *testing.T) {
	original := &Expr{Expr: &Expr_Lit{Lit: &Literal{
		Value:       &Literal_Sval{Sval: "x"},
		LiteralForm: StringLiteralForm(99),
	}}}
	encoded, err := original.Marshal()
	require.NoError(t, err)
	decoded := &Expr{}
	require.ErrorContains(t, decoded.Unmarshal(encoded), "invalid string literal form 99")

	generated := &GeneratedCol{Expr: original}
	encoded, err = generated.MarshalBinary()
	require.NoError(t, err)
	require.ErrorContains(t, (&GeneratedCol{}).UnmarshalBinary(encoded), "invalid string literal form 99")
}

func TestValidateStringLiteralFormsTraversesSubqueryChild(t *testing.T) {
	expr := &Expr{Expr: &Expr_Sub{Sub: &SubqueryRef{Child: &Expr{
		Typ: Type{Id: 61}, Expr: &Expr_Lit{Lit: &Literal{
			Value: &Literal_Sval{Sval: "x"}, LiteralForm: StringLiteralForm(99),
		}},
	}}}}
	require.ErrorContains(t, expr.ValidateStringLiteralForms(), "invalid string literal form 99")
}

func TestVisitExprTreeTraversesEveryNestedVariant(t *testing.T) {
	param := &Expr{Expr: &Expr_P{P: &ParamRef{Pos: 3}}}
	literalSource := &Expr{Expr: &Expr_P{P: &ParamRef{Pos: 4}}}
	literal := &Expr{Expr: &Expr_Lit{Lit: &Literal{Src: literalSource}}}
	subquery := &Expr{Expr: &Expr_Sub{Sub: &SubqueryRef{Child: param}}}
	window := &Expr{Expr: &Expr_W{W: &WindowSpec{
		WindowFunc:  &Expr{Expr: &Expr_F{F: &Function{Args: []*Expr{literal}}}},
		PartitionBy: []*Expr{subquery},
		OrderBy:     []*OrderBySpec{{Expr: &Expr{Expr: &Expr_P{P: &ParamRef{Pos: 5}}}}},
		Frame: &FrameClause{
			Start: &FrameBound{Val: &Expr{Expr: &Expr_P{P: &ParamRef{Pos: 6}}}},
			End:   &FrameBound{Val: &Expr{Expr: &Expr_P{P: &ParamRef{Pos: 7}}}},
		},
	}}}

	var positions []int32
	require.NoError(t, VisitExprTree(window, func(expr *Expr) error {
		if param := expr.GetP(); param != nil {
			positions = append(positions, param.Pos)
		}
		return nil
	}))
	require.Equal(t, []int32{4, 3, 5, 6, 7}, positions)
}

func TestValidateStringLiteralFormsSkipsBytePayloads(t *testing.T) {
	owner := struct {
		Payload []byte
		Expr    *Expr
	}{Payload: make([]byte, 8<<20), Expr: &Expr{}}
	require.NoError(t, ValidateStringLiteralFormsInOwner(&owner))
}

func TestValidateStringLiteralFormRejectsNonStringLiteral(t *testing.T) {
	expr := &Expr{Typ: Type{Id: 23}, Expr: &Expr_Lit{Lit: &Literal{
		Value: &Literal_I64Val{I64Val: 1}, LiteralForm: StringLiteralForm_STRING_LITERAL_TEXT,
	}}}
	require.ErrorContains(t, expr.ValidateStringLiteralForms(), "requires a string literal")
}

func TestValidateStringLiteralFormsInNestedOwner(t *testing.T) {
	owner := struct{ Expressions []*Expr }{Expressions: []*Expr{{
		Typ: Type{Id: 61}, Expr: &Expr_Lit{Lit: &Literal{
			Value: &Literal_Sval{Sval: "x"}, LiteralForm: StringLiteralForm(99),
		}},
	}}}
	require.ErrorContains(t, ValidateStringLiteralFormsInOwner(&owner), "invalid string literal form 99")
}

func TestNormalizeTextLiteralFormsForCompatibility(t *testing.T) {
	expr := &Expr{Expr: &Expr_F{F: &Function{Args: []*Expr{
		{Typ: Type{Id: 61}, Expr: &Expr_Lit{Lit: &Literal{
			Value:       &Literal_Sval{Sval: "text"},
			LiteralForm: StringLiteralForm_STRING_LITERAL_TEXT,
		}}},
		{Typ: Type{Id: 61}, Expr: &Expr_Lit{Lit: &Literal{
			Value:       &Literal_Sval{Sval: "hex"},
			IsBin:       true,
			LiteralForm: StringLiteralForm_STRING_LITERAL_HEX,
		}}},
		{Typ: Type{Id: 65}, Expr: &Expr_Lit{Lit: &Literal{
			Value:       &Literal_Sval{Sval: "explicit text"},
			LiteralForm: StringLiteralForm_STRING_LITERAL_TEXT,
		}}},
	}}}}
	require.NoError(t, expr.NormalizeTextLiteralFormsForCompatibility())
	require.Equal(t, StringLiteralForm_STRING_LITERAL_NONE, expr.GetF().Args[0].GetLit().LiteralForm)
	require.Equal(t, StringLiteralForm_STRING_LITERAL_HEX, expr.GetF().Args[1].GetLit().LiteralForm)
	require.Equal(t, StringLiteralForm_STRING_LITERAL_TEXT, expr.GetF().Args[2].GetLit().LiteralForm)
}

func TestRequiresMORPCVersion23StringLiterals(t *testing.T) {
	tests := []struct {
		name string
		typ  Type
		form StringLiteralForm
		want bool
	}{
		{name: "ordinary text", typ: Type{Id: 61}, form: StringLiteralForm_STRING_LITERAL_TEXT},
		{name: "text override on binary", typ: Type{Id: 65}, form: StringLiteralForm_STRING_LITERAL_TEXT, want: true},
		{name: "binary override on text", typ: Type{Id: 61}, form: StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER, want: true},
		{name: "binary on binary", typ: Type{Id: 65}, form: StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER},
		{name: "legacy inherit", typ: Type{Id: 65}, form: StringLiteralForm_STRING_LITERAL_NONE},
		{name: "legacy hex", typ: Type{Id: 61}, form: StringLiteralForm_STRING_LITERAL_HEX},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expr := &Expr{Typ: test.typ, Expr: &Expr_Lit{Lit: &Literal{
				Value:       &Literal_Sval{Sval: "value"},
				LiteralForm: test.form,
				IsBin:       test.form == StringLiteralForm_STRING_LITERAL_HEX,
			}}}
			required, err := RequiresMORPCVersion23StringLiterals(&struct{ Expr *Expr }{Expr: expr})
			require.NoError(t, err)
			require.Equal(t, test.want, required)
		})
	}
}

func TestRequiresMORPCVersion26NumericPrefix(t *testing.T) {
	prefixCast := &Expr{
		Typ: Type{Id: 14, Charset: 255},
		Expr: &Expr_F{F: &Function{
			Func: &ObjectRef{ObjName: "cast"},
			Args: []*Expr{{
				Typ:  Type{Id: 61},
				Expr: &Expr_Lit{Lit: &Literal{Value: &Literal_Sval{Sval: "12.5tail"}}},
			}},
		}},
	}
	ordinaryCast := &Expr{
		Typ: Type{Id: 14},
		Expr: &Expr_F{F: &Function{
			Func: &ObjectRef{ObjName: "cast"},
			Args: prefixCast.GetF().Args,
		}},
	}

	required, err := RequiresMORPCVersion26NumericPrefix(&struct{ Expr *Expr }{Expr: prefixCast})
	require.NoError(t, err)
	require.True(t, required)
	nested := &Expr{
		Typ: Type{Id: 14},
		Expr: &Expr_F{F: &Function{
			Func: &ObjectRef{ObjName: "coalesce"},
			Args: []*Expr{ordinaryCast, prefixCast},
		}},
	}
	required, err = RequiresMORPCVersion26NumericPrefix(&struct{ Expr *Expr }{Expr: nested})
	require.NoError(t, err)
	require.True(t, required)
	required, err = RequiresMORPCVersion26NumericPrefix(&struct{ Expr *Expr }{Expr: ordinaryCast})
	require.NoError(t, err)
	require.False(t, required)
}

func TestRequiresMORPCVersion23DynamicStringProvenance(t *testing.T) {
	textType := Type{Id: 61}
	binaryType := Type{Id: 65}
	boolType := Type{Id: 10}
	column := func(typ Type, pos int32) *Expr {
		return &Expr{Typ: typ, Expr: &Expr_Col{Col: &ColRef{ColPos: pos}}}
	}
	function := func(name string, typ Type, overload int32, args ...*Expr) *Expr {
		return &Expr{Typ: typ, Expr: &Expr_F{F: &Function{
			Func: &ObjectRef{ObjName: name, Obj: int64(overload)},
			Args: args,
		}}}
	}
	implicitCast := func(expr *Expr, typ Type) *Expr {
		return function("cast", typ, 0, expr)
	}
	explicitCast := func(expr *Expr, typ Type) *Expr {
		return function("cast", typ, 1, expr)
	}
	condition := column(boolType, 0)
	textColumn := column(textType, 1)
	binaryColumn := column(binaryType, 2)

	tests := []struct {
		name string
		expr *Expr
		want bool
	}{
		{
			name: "if text column through implicit cast",
			expr: function("if", binaryType, 0,
				condition, implicitCast(textColumn, binaryType), binaryColumn),
			want: true,
		},
		{
			name: "case text column through implicit cast",
			expr: function("case", binaryType, 0,
				condition, implicitCast(textColumn, binaryType), binaryColumn),
			want: true,
		},
		{
			name: "coalesce binary column through implicit cast",
			expr: function("coalesce", textType, 0,
				implicitCast(binaryColumn, textType), textColumn),
			want: true,
		},
		{
			name: "nested producer consumer",
			expr: function("coalesce", binaryType, 0,
				function("if", binaryType, 0,
					condition, implicitCast(textColumn, binaryType), binaryColumn),
				binaryColumn),
			want: true,
		},
		{
			name: "explicit cast is semantic boundary",
			expr: function("if", binaryType, 0,
				condition, explicitCast(textColumn, binaryType), binaryColumn),
		},
		{
			name: "same domain columns",
			expr: function("if", binaryType, 0,
				condition, binaryColumn, binaryColumn),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			required, err := RequiresMORPCVersion23StringProvenance(test.expr)
			require.NoError(t, err)
			require.Equal(t, test.want, required)
		})
	}
}
