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
	"fmt"
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

func TestRequiresMORPCVersion30NumericPrefix(t *testing.T) {
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

	required, err := RequiresMORPCVersion30NumericPrefix(&struct{ Expr *Expr }{Expr: prefixCast})
	require.NoError(t, err)
	require.True(t, required)
	nested := &Expr{
		Typ: Type{Id: 14},
		Expr: &Expr_F{F: &Function{
			Func: &ObjectRef{ObjName: "coalesce"},
			Args: []*Expr{ordinaryCast, prefixCast},
		}},
	}
	required, err = RequiresMORPCVersion30NumericPrefix(&struct{ Expr *Expr }{Expr: nested})
	require.NoError(t, err)
	require.True(t, required)
	required, err = RequiresMORPCVersion30NumericPrefix(&struct{ Expr *Expr }{Expr: ordinaryCast})
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

func TestRequiresMORPCVersion36JSONComparisonParam(t *testing.T) {
	param := func(typ Type, pos int32) *Expr {
		return &Expr{Typ: typ, Expr: &Expr_P{P: &ParamRef{Pos: pos}}}
	}
	jsonComparison := func(arg *Expr) *Expr {
		return &Expr{Typ: Type{Id: 10}, Expr: &Expr_F{F: &Function{
			Func: &ObjectRef{Obj: int64(internalJSONComparisonFunctionID) << 32},
			Args: []*Expr{arg},
		}}}
	}

	owner := struct{ Expressions []*Expr }{Expressions: []*Expr{
		jsonComparison(param(Type{Id: 1}, 0)),
		jsonComparison(param(Type{Id: 61}, 1)),
	}}
	required, err := RequiresMORPCVersion36JSONComparisonParam(&owner)
	require.NoError(t, err)
	require.True(t, required)

	ordinary := &Expr{Expr: &Expr_F{F: &Function{
		Func: &ObjectRef{Obj: int64(21) << 32},
		Args: []*Expr{param(Type{Id: 61}, 0)},
	}}}
	required, err = RequiresMORPCVersion36JSONComparisonParam(ordinary)
	require.NoError(t, err)
	require.False(t, required)

	prefixCast := &Expr{
		Typ: Type{Id: 14, Charset: 255},
		Expr: &Expr_F{F: &Function{
			Func: &ObjectRef{ObjName: "cast"},
			Args: []*Expr{ordinary},
		}},
	}
	mixedOwner := &struct{ Expressions []*Expr }{Expressions: []*Expr{{
		Expr: &Expr_F{F: &Function{
			Func: &ObjectRef{ObjName: "coalesce"},
			Args: []*Expr{ordinary, prefixCast, jsonComparison(param(Type{Id: 1}, 0))},
		}},
	}}}
	features, err := RequiredRemoteExpressionFeatures(mixedOwner)
	require.NoError(t, err)
	require.True(t, features.NumericPrefix)
	require.True(t, features.JSONComparisonParam)
	require.False(t, features.MixedJSONBooleanEquality)
	require.True(t, features.Any())

	features, err = RequiredRemoteExpressionFeatures(ordinary)
	require.NoError(t, err)
	require.False(t, features.Any())
}

func TestRequiresMORPCVersion36MixedJSONBooleanEquality(t *testing.T) {
	operand := func(typeID int32, position int32) *Expr {
		return &Expr{
			Typ:  Type{Id: typeID},
			Expr: &Expr_Col{Col: &ColRef{ColPos: position}},
		}
	}
	comparison := func(functionID int32, leftType, rightType int32) *Expr {
		return &Expr{Typ: Type{Id: planBooleanTypeID}, Expr: &Expr_F{F: &Function{
			Func: &ObjectRef{Obj: int64(functionID) << 32},
			Args: []*Expr{operand(leftType, 0), operand(rightType, 1)},
		}}}
	}

	for _, functionID := range []int32{
		equalFunctionID,
		notEqualFunctionID,
		nullSafeEqualFunctionID,
	} {
		for _, orientation := range []struct {
			name      string
			leftType  int32
			rightType int32
		}{
			{name: "json_left", leftType: planJSONTypeID, rightType: planBooleanTypeID},
			{name: "json_right", leftType: planBooleanTypeID, rightType: planJSONTypeID},
		} {
			t.Run(fmt.Sprintf("function_%d_%s", functionID, orientation.name), func(t *testing.T) {
				required, err := RequiresMORPCVersion36MixedJSONBooleanEquality(
					comparison(functionID, orientation.leftType, orientation.rightType))
				require.NoError(t, err)
				require.True(t, required)
			})
		}
	}

	for _, control := range []*Expr{
		comparison(equalFunctionID, planJSONTypeID, planJSONTypeID),
		comparison(equalFunctionID, planBooleanTypeID, planBooleanTypeID),
		comparison(4, planJSONTypeID, planBooleanTypeID), // ordering is not a versioned equality overload
	} {
		required, err := RequiresMORPCVersion36MixedJSONBooleanEquality(control)
		require.NoError(t, err)
		require.False(t, required)
	}
}

func TestRequiresMORPCVersion59NumericFormatArguments(t *testing.T) {
	numeric := func(typeID int32, position int32) *Expr {
		return &Expr{Typ: Type{Id: typeID}, Expr: &Expr_Col{Col: &ColRef{ColPos: position}}}
	}
	stringArg := numeric(61, 0)
	scale := numeric(23, 1)
	locale := numeric(61, 2)
	format := func(obj int64, name string, first *Expr, args ...*Expr) *Expr {
		all := make([]*Expr, 0, len(args)+1)
		all = append(all, first)
		all = append(all, args...)
		return &Expr{Typ: Type{Id: 61}, Expr: &Expr_F{F: &Function{
			Func: &ObjectRef{Obj: obj, ObjName: name},
			Args: all,
		}}}
	}

	tests := []struct {
		name string
		expr *Expr
		want bool
	}{
		{
			name: "encoded two-argument numeric format",
			expr: format(int64(262)<<32, "format", numeric(23, 0), scale),
			want: true,
		},
		{
			name: "encoded three-argument numeric format",
			expr: format(int64(262)<<32|1, "format", numeric(31, 0), scale, locale),
			want: true,
		},
		{
			name: "name-only numeric format",
			expr: format(0, "FORMAT", numeric(32, 0), scale),
			want: true,
		},
		{
			name: "string format remains compatible",
			expr: format(int64(262)<<32, "format", stringArg, scale),
		},
		{
			name: "other numeric function",
			expr: format(int64(123)<<32, "other", numeric(23, 0), scale),
		},
		{
			name: "missing scale argument",
			expr: format(int64(262)<<32, "format", numeric(23, 0)),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := RequiresMORPCVersion59NumericFormatArguments(test.expr)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
	t.Run("missing first argument", func(t *testing.T) {
		_, err := RequiresMORPCVersion59NumericFormatArguments(
			format(int64(262)<<32, "format", nil, scale))
		require.ErrorContains(t, err, "FORMAT is missing its first argument")
	})

	features, err := RequiredRemoteExpressionFeatures(&struct{ Expressions []*Expr }{
		Expressions: []*Expr{
			format(int64(262)<<32, "format", numeric(23, 0), scale),
			format(int64(262)<<32, "format", stringArg, scale),
		},
	})
	require.NoError(t, err)
	require.True(t, features.FormatNumericArguments)
	require.True(t, features.Any())
}

func TestRequiresMORPCVersion64TypedConversion(t *testing.T) {
	arg := func(typeID int32) *Expr {
		return &Expr{Typ: Type{Id: typeID}, Expr: &Expr_Col{Col: &ColRef{ColPos: 0}}}
	}
	conversion := func(functionID, overload int32, name string, first *Expr) *Expr {
		args := []*Expr{first}
		if name == "conv" {
			args = append(args,
				arg(23), // INT64 from_base
				arg(23), // INT64 to_base
			)
		}
		return &Expr{
			Typ: Type{Id: 61},
			Expr: &Expr_F{F: &Function{
				Func: &ObjectRef{Obj: int64(functionID)<<32 | int64(overload), ObjName: name},
				Args: args,
			}},
		}
	}

	tests := []struct {
		name string
		expr *Expr
		want bool
	}{
		{
			name: "typed CONV integer overload",
			expr: conversion(convFunctionID, 3, "conv", arg(23)),
			want: true,
		},
		{
			name: "typed CONV fixed-width overload zero",
			expr: conversion(convFunctionID, 0, "conv", arg(32)), // DECIMAL64
			want: true,
		},
		{
			name: "dynamic CONV marker",
			expr: conversion(convFunctionID, 13, "conv", arg(0)), // T_ANY
			want: true,
		},
		{
			name: "typed BIN float overload",
			expr: conversion(binFunctionID, 8, "bin", arg(30)), // FLOAT32
			want: true,
		},
		{
			name: "dynamic BIN overload",
			expr: conversion(binFunctionID, 11, "bin", arg(10)), // BOOL
			want: true,
		},
		{
			name: "string CONV remains compatible",
			expr: conversion(convFunctionID, 0, "conv", arg(61)), // VARCHAR
		},
		{
			name: "integer BIN remains compatible",
			expr: conversion(binFunctionID, 7, "bin", arg(23)), // INT64
		},
		{
			name: "string BIN remains compatible",
			expr: conversion(binFunctionID, 10, "bin", arg(61)), // VARCHAR
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := RequiresMORPCVersion64TypedConversion(test.expr)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}

	features, err := RequiredRemoteExpressionFeatures(&struct{ Expressions []*Expr }{
		Expressions: []*Expr{
			conversion(convFunctionID, 0, "conv", arg(32)),
			conversion(binFunctionID, 10, "bin", arg(61)),
		},
	})
	require.NoError(t, err)
	require.True(t, features.TypedConversionFunctions)
	require.True(t, features.Any())
}

func TestRequiredRemoteExpressionFeaturesASCIIResultContract(t *testing.T) {
	ascii := func(resultType, functionID int32) *Expr {
		return &Expr{
			Typ: Type{Id: resultType},
			Expr: &Expr_F{F: &Function{
				Func: &ObjectRef{Obj: int64(functionID) << 32, ObjName: "ascii"},
				Args: []*Expr{{Typ: Type{Id: 61}, Expr: &Expr_Col{Col: &ColRef{ColPos: 0}}}},
			}},
		}
	}

	features, err := RequiredRemoteExpressionFeatures(ascii(asciiInt32ResultTypeID, asciiFunctionID))
	require.NoError(t, err)
	require.True(t, features.ASCIIInt32Result)
	require.True(t, features.Any())

	features, err = RequiredRemoteExpressionFeatures(ascii(25, asciiFunctionID))
	require.NoError(t, err)
	require.False(t, features.ASCIIInt32Result,
		"legacy UINT8 ASCII plans remain executable on a newer worker")

}

func TestRequiredRemoteExpressionFeaturesBoundedConditionalStringDomains(t *testing.T) {
	coalesce := func(overload int32) *Expr {
		return &Expr{
			Typ: Type{Id: 65, Width: 12, Charset: 2},
			Expr: &Expr_F{F: &Function{
				Func: &ObjectRef{
					Obj:     int64(coalesceFunctionID)<<32 | int64(overload),
					ObjName: "coalesce",
				},
			}},
		}
	}

	for _, overload := range []int32{30, 31} {
		features, err := RequiredRemoteExpressionFeatures(coalesce(overload))
		require.NoError(t, err)
		require.True(t, features.BoundedConditionalStringDomains)
		require.True(t, features.Any())

		required, err := RequiresMORPCVersion83BoundedConditionalStringDomains(coalesce(overload))
		require.NoError(t, err)
		require.True(t, required)
	}

	features, err := RequiredRemoteExpressionFeatures(coalesce(0))
	require.NoError(t, err)
	require.False(t, features.BoundedConditionalStringDomains,
		"the existing character overload remains wire-compatible")
}

func TestRequiredRemoteExpressionFeaturesFollowupResultContracts(t *testing.T) {
	expression := func(functionID, overloadID, resultType int32) *Expr {
		return &Expr{
			Typ: Type{Id: resultType},
			Expr: &Expr_F{F: &Function{Func: &ObjectRef{
				Obj:     int64(functionID)<<32 | int64(overloadID),
				ObjName: "contract-test",
			}}},
		}
	}

	for _, test := range []struct {
		name     string
		expr     *Expr
		wantTO64 bool
		wantIP   bool
	}{
		{
			name: "legacy character TO_BASE64",
			expr: expression(remoteTOBase64FunctionID, 0, 71), // TEXT
		},
		{
			name:     "bounded character TO_BASE64",
			expr:     expression(remoteTOBase64FunctionID, 0, planVarcharTypeID),
			wantTO64: true,
		},
		{
			name:     "binary TO_BASE64 overload",
			expr:     expression(remoteTOBase64FunctionID, 3, planVarcharTypeID),
			wantTO64: true,
		},
		{
			name:   "native INET_NTOA overload",
			expr:   expression(remoteINETNTOAFunctionID, 8, planVarcharTypeID),
			wantIP: false,
		},
		{
			name:   "dynamic INET_NTOA overload",
			expr:   expression(remoteINETNTOAFunctionID, 9, planVarcharTypeID),
			wantIP: true,
		},
		{
			name:   "legacy INT64 IP predicate",
			expr:   expression(remoteIPIsIPv4FunctionID, 0, 23), // INT64
			wantIP: false,
		},
		{
			name:   "corrected INT32 IP predicate",
			expr:   expression(remoteIPIsIPv4FunctionID, 0, ipInt32ResultTypeID),
			wantIP: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			features, err := RequiredRemoteExpressionFeatures(test.expr)
			require.NoError(t, err)
			require.Equal(t, test.wantTO64, features.TOBase64ResultContracts)
			require.Equal(t, test.wantIP, features.IPFunctionResultContracts)
			required, err := RequiresMORPCVersion85ExpressionResultContracts(test.expr)
			require.NoError(t, err)
			require.Equal(t, test.wantTO64 || test.wantIP, required)
		})
	}
}

func TestRequiredRemoteExpressionFeaturesMetadataResultContracts(t *testing.T) {
	column := func(typ Type, pos int32) *Expr {
		return &Expr{Typ: typ, Expr: &Expr_Col{Col: &ColRef{ColPos: pos}}}
	}
	integer := func(value int64) *Expr {
		return &Expr{Typ: Type{Id: 23}, Expr: &Expr_Lit{Lit: &Literal{
			Value: &Literal_I64Val{I64Val: value},
		}}}
	}
	expression := func(functionID int32, result Type, name string, args ...*Expr) *Expr {
		return &Expr{
			Typ: result,
			Expr: &Expr_F{F: &Function{
				Func: &ObjectRef{Obj: int64(functionID) << 32, ObjName: name},
				Args: args,
			}},
		}
	}
	implicitCast := func(source *Expr, result Type) *Expr {
		return expression(0, result, "cast", source)
	}
	explicitCast := func(source *Expr, result Type) *Expr {
		cast := implicitCast(source, result)
		cast.GetF().SyntaxExplicitCast = true
		return cast
	}
	temporal := func(id int32, scale int32, pos int32) *Expr {
		return column(Type{Id: id, Width: scale, Scale: scale}, pos)
	}
	boolCondition := column(Type{Id: planBooleanTypeID}, 0)

	tests := []struct {
		name string
		expr *Expr
		want bool
	}{
		{
			name: "bounded character substring",
			expr: expression(substringFunctionID, Type{Id: planVarcharTypeID, Width: 7}, "substring",
				column(Type{Id: planVarcharTypeID, Width: 64}, 0), integer(1), integer(7)),
			want: true,
		},
		{
			name: "bounded binary left",
			expr: expression(leftFunctionID, Type{Id: planVarbinaryTypeID, Width: 1}, "left",
				column(Type{Id: planVarbinaryTypeID, Width: 128}, 0), integer(1)),
			want: true,
		},
		{
			name: "legacy source-derived two-argument substring",
			expr: expression(substringFunctionID, Type{Id: planVarcharTypeID, Width: 64}, "substring",
				column(Type{Id: planVarcharTypeID, Width: 64}, 0), integer(1)),
		},
		{
			name: "legacy source-derived explicit substring",
			expr: expression(substringFunctionID, Type{Id: planVarcharTypeID, Width: 64}, "substring",
				column(Type{Id: planVarcharTypeID, Width: 64}, 0), integer(1), integer(7)),
		},
		{
			name: "fractional coalesce time",
			expr: expression(coalesceFunctionID, Type{Id: planTimeTypeID, Width: 6, Scale: 6}, "coalesce",
				column(Type{Id: planTimeTypeID, Width: 0, Scale: 0}, 0),
				column(Type{Id: planTimeTypeID, Width: 6, Scale: 6}, 1)),
			want: true,
		},
		{
			name: "fractional case datetime",
			expr: expression(caseFunctionID, Type{Id: planDatetimeTypeID, Width: 6, Scale: 6}, "case",
				column(Type{Id: planBooleanTypeID}, 0),
				column(Type{Id: planDatetimeTypeID, Width: 0, Scale: 0}, 1),
				column(Type{Id: planDatetimeTypeID, Width: 6, Scale: 6}, 2)),
			want: true,
		},
		{
			name: "legacy fractional coalesce datetime",
			expr: expression(coalesceFunctionID, Type{Id: planDatetimeTypeID, Width: 6, Scale: 6}, "coalesce",
				column(Type{Id: planDatetimeTypeID, Width: 6, Scale: 6}, 0),
				column(Type{Id: planDatetimeTypeID, Width: 3, Scale: 3}, 1)),
		},
		{
			name: "legacy fractional coalesce timestamp",
			expr: expression(coalesceFunctionID, Type{Id: planTimestampTypeID, Width: 6, Scale: 6}, "coalesce",
				column(Type{Id: planTimestampTypeID, Width: 0, Scale: 0}, 0),
				column(Type{Id: planTimestampTypeID, Width: 6, Scale: 6}, 1)),
		},
		{
			name: "legacy fractional case first branch already precise",
			expr: expression(caseFunctionID, Type{Id: planDatetimeTypeID, Width: 6, Scale: 6}, "case",
				column(Type{Id: planBooleanTypeID}, 0),
				column(Type{Id: planDatetimeTypeID, Width: 6, Scale: 6}, 1),
				column(Type{Id: planDatetimeTypeID, Width: 3, Scale: 3}, 2)),
		},
		{
			name: "mixed temporal case first branch already precise",
			expr: expression(caseFunctionID, Type{Id: planDatetimeTypeID, Width: 6, Scale: 6}, "case",
				boolCondition,
				temporal(planTimestampTypeID, 6, 1),
				temporal(planDatetimeTypeID, 3, 2)),
			want: true,
		},
		{
			name: "mixed temporal case reverse branch order",
			expr: expression(caseFunctionID, Type{Id: planDatetimeTypeID, Width: 6, Scale: 6}, "case",
				boolCondition,
				temporal(planDatetimeTypeID, 6, 1),
				temporal(planTimestampTypeID, 3, 2)),
			want: true,
		},
		{
			name: "mixed temporal if first branch already precise",
			expr: expression(iffFunctionID, Type{Id: planDatetimeTypeID, Width: 6, Scale: 6}, "if",
				boolCondition,
				temporal(planTimestampTypeID, 6, 1),
				temporal(planDatetimeTypeID, 3, 2)),
			want: true,
		},
		{
			name: "mixed temporal if reverse branch order",
			expr: expression(iffFunctionID, Type{Id: planDatetimeTypeID, Width: 6, Scale: 6}, "if",
				boolCondition,
				temporal(planDatetimeTypeID, 6, 1),
				temporal(planTimestampTypeID, 3, 2)),
			want: true,
		},
		{
			name: "case omitted else is fenced",
			expr: expression(caseFunctionID, Type{Id: planDatetimeTypeID, Width: 6, Scale: 6}, "case",
				boolCondition,
				temporal(planDatetimeTypeID, 6, 1)),
			want: true,
		},
		{
			name: "case untyped null value is fenced",
			expr: expression(caseFunctionID, Type{Id: planDatetimeTypeID, Width: 6, Scale: 6}, "case",
				boolCondition,
				temporal(planDatetimeTypeID, 6, 1),
				column(Type{Id: planAnyTypeID}, 2)),
			want: true,
		},
		{
			name: "case condition coercion is fenced",
			expr: expression(caseFunctionID, Type{Id: planDatetimeTypeID, Width: 6, Scale: 6}, "case",
				boolCondition,
				temporal(planDatetimeTypeID, 6, 1),
				column(Type{Id: 23}, 2),
				temporal(planDatetimeTypeID, 3, 3),
				temporal(planDatetimeTypeID, 6, 4)),
			want: true,
		},
		{
			name: "if ANY condition is fenced",
			expr: expression(iffFunctionID, Type{Id: planDatetimeTypeID, Width: 6, Scale: 6}, "if",
				column(Type{Id: planAnyTypeID}, 0),
				temporal(planDatetimeTypeID, 6, 1),
				temporal(planDatetimeTypeID, 3, 2)),
			want: true,
		},
		{
			name: "if numeric condition remains legacy compatible",
			expr: expression(iffFunctionID, Type{Id: planDatetimeTypeID, Width: 6, Scale: 6}, "if",
				column(Type{Id: 23}, 0),
				temporal(planDatetimeTypeID, 6, 1),
				temporal(planDatetimeTypeID, 3, 2)),
		},
		{
			name: "if string condition remains legacy compatible",
			expr: expression(iffFunctionID, Type{Id: planDatetimeTypeID, Width: 6, Scale: 6}, "if",
				column(Type{Id: planVarcharTypeID}, 0),
				temporal(planDatetimeTypeID, 6, 1),
				temporal(planDatetimeTypeID, 3, 2)),
		},
		{
			name: "if untyped null value is fenced",
			expr: expression(iffFunctionID, Type{Id: planDatetimeTypeID, Width: 6, Scale: 6}, "if",
				boolCondition,
				temporal(planDatetimeTypeID, 6, 1),
				column(Type{Id: planAnyTypeID}, 2)),
			want: true,
		},
		{
			name: "explicit cast remains a semantic boundary",
			expr: expression(iffFunctionID, Type{Id: planDatetimeTypeID, Width: 6, Scale: 6}, "if",
				boolCondition,
				explicitCast(temporal(planTimestampTypeID, 6, 1), Type{Id: planDatetimeTypeID, Width: 6, Scale: 6}),
				temporal(planDatetimeTypeID, 3, 2)),
		},
		{
			name: "zero precision conditional",
			expr: expression(iffFunctionID, Type{Id: planTimeTypeID, Width: 0, Scale: 0}, "iff",
				column(Type{Id: planBooleanTypeID}, 0),
				column(Type{Id: planTimeTypeID, Width: 0, Scale: 0}, 1),
				column(Type{Id: planTimeTypeID, Width: 0, Scale: 0}, 2)),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			features, err := RequiredRemoteExpressionFeatures(test.expr)
			require.NoError(t, err)
			require.Equal(t, test.want, features.ExpressionResultMetadataContracts)
			required, err := RequiresMORPCVersion85ExpressionResultContracts(test.expr)
			require.NoError(t, err)
			require.Equal(t, test.want, required)
		})
	}
}

func TestRequiredRemoteExpressionFeaturesMetadataResultContractsSurviveWireRoundTrip(t *testing.T) {
	expr := &Expr{
		Typ: Type{Id: planDatetimeTypeID, Width: 6, Scale: 6},
		Expr: &Expr_F{F: &Function{
			Func: &ObjectRef{Obj: int64(iffFunctionID) << 32, ObjName: "if"},
			Args: []*Expr{
				{Typ: Type{Id: planBooleanTypeID}, Expr: &Expr_Col{Col: &ColRef{ColPos: 0}}},
				{Typ: Type{Id: planTimestampTypeID, Width: 6, Scale: 6}, Expr: &Expr_Col{Col: &ColRef{ColPos: 1}}},
				{Typ: Type{Id: planDatetimeTypeID, Width: 3, Scale: 3}, Expr: &Expr_Col{Col: &ColRef{ColPos: 2}}},
			},
		}},
	}
	features, err := RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.ExpressionResultMetadataContracts)

	encoded, err := expr.Marshal()
	require.NoError(t, err)
	decoded := &Expr{}
	require.NoError(t, decoded.Unmarshal(encoded))
	features, err = RequiredRemoteExpressionFeatures(decoded)
	require.NoError(t, err)
	require.True(t, features.ExpressionResultMetadataContracts)
}

func TestRequiredRemoteExpressionFeaturesSpatialDistance(t *testing.T) {
	spatial := func(functionID, overloadID int32) *Expr {
		return &Expr{
			Typ: Type{Id: 30},
			Expr: &Expr_F{F: &Function{Func: &ObjectRef{
				Obj: int64(functionID)<<32 | int64(overloadID),
			}}},
		}
	}
	for _, tc := range []struct {
		name     string
		id       int32
		overload int32
		want     bool
	}{
		{name: "legacy frechet geometry", id: remoteFrechetDistanceFunctionID, overload: 0},
		{name: "legacy frechet geometry32", id: remoteFrechetDistanceFunctionID, overload: 1},
		{name: "unit frechet geometry", id: remoteFrechetDistanceFunctionID, overload: 2, want: true},
		{name: "unit frechet geometry32", id: remoteFrechetDistanceFunctionID, overload: 3, want: true},
		{name: "geodetic frechet geometry", id: remoteFrechetDistanceFunctionID, overload: 4, want: true},
		{name: "geodetic frechet geometry32", id: remoteFrechetDistanceFunctionID, overload: 5, want: true},
		{name: "legacy hausdorff geometry", id: remoteHausdorffDistanceFunctionID, overload: 0},
		{name: "legacy hausdorff geometry32", id: remoteHausdorffDistanceFunctionID, overload: 1},
		{name: "unit hausdorff geometry", id: remoteHausdorffDistanceFunctionID, overload: 2, want: true},
		{name: "unit hausdorff geometry32", id: remoteHausdorffDistanceFunctionID, overload: 3, want: true},
		{name: "geodetic hausdorff geometry", id: remoteHausdorffDistanceFunctionID, overload: 4, want: true},
		{name: "geodetic hausdorff geometry32", id: remoteHausdorffDistanceFunctionID, overload: 5, want: true},
		{name: "new distance geometry unit", id: remoteSpatialDistanceFunctionID, overload: 4, want: true},
		{name: "new distance geometry32 unit", id: remoteSpatialDistanceFunctionID, overload: 5, want: true},
		{name: "legacy distance", id: remoteSpatialDistanceFunctionID, overload: 0, want: false},
		{name: "explicit SRID distance", id: remoteSpatialDistanceFunctionID, overload: 1, want: false},
		{name: "geometry32 explicit SRID distance", id: remoteSpatialDistanceFunctionID, overload: 3, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			features, err := RequiredRemoteExpressionFeatures(spatial(tc.id, tc.overload))
			require.NoError(t, err)
			require.Equal(t, tc.want, features.SpatialDistanceSemantics)
			require.Equal(t, tc.want, features.Any())
		})
	}

	features, err := RequiredRemoteExpressionFeatures(&struct{ Expressions []*Expr }{
		Expressions: []*Expr{
			spatial(remoteSpatialDistanceFunctionID, 0),
			spatial(remoteSpatialDistanceFunctionID, 4),
		},
	})
	require.NoError(t, err)
	require.True(t, features.SpatialDistanceSemantics,
		"a nested/new unit overload must fence the whole owner")
}

func TestRequiredRemoteExpressionFeaturesDecimalLiteralSemantics(t *testing.T) {
	makeLiteral := func(required bool) *Expr {
		return &Expr{
			Typ: Type{Id: 33, Width: 40, Scale: 1},
			Expr: &Expr_Lit{Lit: &Literal{
				Value:                     &Literal_Sval{Sval: "12345678901234567890123456789012345678.1"},
				DecimalLiteralRequiresV82: required,
			}},
		}
	}

	original := makeLiteral(true)
	wire, err := original.Marshal()
	require.NoError(t, err)
	decoded := &Expr{}
	require.NoError(t, decoded.Unmarshal(wire))
	require.True(t, decoded.GetLit().GetDecimalLiteralRequiresV82())

	features, err := RequiredRemoteExpressionFeatures(decoded)
	require.NoError(t, err)
	require.True(t, features.DecimalLiteralSemantics)
	required, err := RequiresMORPCVersion89DecimalLiteralSemantics(makeLiteral(true))
	require.NoError(t, err)
	require.True(t, required)
	require.True(t, features.Any())

	features, err = RequiredRemoteExpressionFeatures(makeLiteral(false))
	require.NoError(t, err)
	require.False(t, features.DecimalLiteralSemantics)
}

func TestTemporalConversionProtocolSourceMatrix(t *testing.T) {
	column := func(id int32) *Expr {
		return &Expr{Typ: Type{Id: id}, Expr: &Expr_Col{Col: &ColRef{ColPos: 0}}}
	}
	literal := func(form StringLiteralForm) *Expr {
		return &Expr{Typ: Type{Id: planVarcharTypeID}, Expr: &Expr_Lit{Lit: &Literal{
			Value: &Literal_Sval{Sval: ""}, LiteralForm: form,
			IsBin: form == StringLiteralForm_STRING_LITERAL_HEX || form == StringLiteralForm_STRING_LITERAL_BIT,
		}}}
	}
	cast := func(source *Expr, result int32) *Expr {
		return &Expr{Typ: Type{Id: result}, Expr: &Expr_F{F: &Function{
			Func: &ObjectRef{Obj: int64(21) << 32},
			Args: []*Expr{source, {Typ: Type{Id: result}, Expr: &Expr_T{T: &TargetType{}}}},
		}}}
	}
	for _, tc := range []struct {
		name string
		expr *Expr
		want bool
	}{
		{"text to date", cast(column(planVarcharTypeID), planDateTypeID), true},
		{"numeric to time", cast(column(planInt64TypeID), planTimeTypeID), true},
		{"hex to signed", cast(literal(StringLiteralForm_STRING_LITERAL_HEX), planInt64TypeID), true},
		{"bit to double", cast(literal(StringLiteralForm_STRING_LITERAL_BIT), 31), true},
		{"ordinary text to signed", cast(literal(StringLiteralForm_STRING_LITERAL_TEXT), planInt64TypeID), false},
		{"binary payload to signed", cast(literal(StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER), planInt64TypeID), false},
		{"numeric to date", cast(column(planInt64TypeID), planDateTypeID), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			features, err := RequiredRemoteExpressionFeatures(tc.expr)
			require.NoError(t, err)
			require.Equal(t, tc.want, features.TemporalResultContracts)
		})
	}
}
