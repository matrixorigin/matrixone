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
	require.NoError(t, decoded.Unmarshal(encoded), "protobuf preserves unknown enum integers")
	require.ErrorContains(t, decoded.ValidateStringLiteralForms(), "invalid string literal form 99")

	generated := &GeneratedCol{Expr: original}
	encoded, err = generated.MarshalBinary()
	require.NoError(t, err)
	require.ErrorContains(t, (&GeneratedCol{}).UnmarshalBinary(encoded), "invalid string literal form 99")
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
	}}}}
	require.NoError(t, expr.NormalizeTextLiteralFormsForCompatibility())
	require.Equal(t, StringLiteralForm_STRING_LITERAL_NONE, expr.GetF().Args[0].GetLit().LiteralForm)
	require.Equal(t, StringLiteralForm_STRING_LITERAL_HEX, expr.GetF().Args[1].GetLit().LiteralForm)
}
