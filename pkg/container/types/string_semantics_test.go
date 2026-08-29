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

package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStringSemanticStateSourceCategories(t *testing.T) {
	textType := T_varchar.ToType()
	binaryType := T_varbinary.ToType()

	tests := []struct {
		name       string
		typ        Type
		runtime    RuntimeStringDomain
		source     StringSource
		literal    StringLiteralForm
		conversion StringConversionKind
		nullKind   StringNullKind
		domain     StringDomain
	}{
		{"text-literal", textType, RuntimeStringInherit, StringSourceLiteral, StringLiteralText,
			StringConversionString, StringNotNull, StringDomainText},
		{"binary-introducer", binaryType, RuntimeStringInherit, StringSourceLiteral,
			StringLiteralBinaryIntroducer, StringConversionString, StringNotNull, StringDomainBinary},
		{"raw-hex", T_any.ToType(), RuntimeStringInherit, StringSourceLiteral, StringLiteralHex,
			StringConversionString, StringNotNull, StringDomainNone},
		{"expression-binary-row", textType, RuntimeStringBinary, StringSourceExpression, StringLiteralNone,
			StringConversionString, StringNotNull, StringDomainBinary},
		{"user-variable", textType, RuntimeStringText, StringSourceUserVariable, StringLiteralNone,
			StringConversionInteger, StringNotNull, StringDomainText},
		{"sql-prepare-null", textType, RuntimeStringBinary, StringSourceSQLPrepare, StringLiteralNone,
			StringConversionString, StringTypedNull, StringDomainBinary},
		{"com-stmt-blob", binaryType, RuntimeStringBinary, StringSourceCOMStmt, StringLiteralNone,
			StringConversionString, StringNotNull, StringDomainBinary},
		{"ordinary-null", T_any.ToType(), RuntimeStringInherit, StringSourceLiteral, StringLiteralNone,
			StringConversionString, StringUntypedNull, StringDomainNone},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state, err := NewStringSemanticState(test.typ, test.runtime, test.source, test.literal,
				test.conversion, test.nullKind)
			require.NoError(t, err)
			require.Equal(t, test.typ, state.StaticType())
			require.Equal(t, test.runtime, state.RuntimeDomain())
			require.Equal(t, test.source, state.Source())
			require.Equal(t, test.literal, state.LiteralForm())
			require.Equal(t, test.conversion, state.ConversionKind())
			require.Equal(t, test.nullKind, state.NullKind())
			require.Equal(t, test.domain, state.EffectiveStringDomain())
		})
	}
}

func TestStringSemanticStateRejectsInvalidCombinations(t *testing.T) {
	textType := T_varchar.ToType()
	tests := []struct {
		name       string
		typ        Type
		runtime    RuntimeStringDomain
		source     StringSource
		literal    StringLiteralForm
		conversion StringConversionKind
		nullKind   StringNullKind
	}{
		{"unknown-runtime", textType, RuntimeStringDomain(99), StringSourceExpression,
			StringLiteralNone, StringConversionString, StringNotNull},
		{"literal-form-on-expression", textType, RuntimeStringInherit, StringSourceExpression,
			StringLiteralText, StringConversionString, StringNotNull},
		{"missing-literal-form", textType, RuntimeStringInherit, StringSourceLiteral,
			StringLiteralNone, StringConversionString, StringNotNull},
		{"binary-introducer-on-text", textType, RuntimeStringInherit, StringSourceLiteral,
			StringLiteralBinaryIntroducer, StringConversionString, StringNotNull},
		{"conversion-on-expression", textType, RuntimeStringInherit, StringSourceExpression,
			StringLiteralNone, StringConversionInteger, StringNotNull},
		{"untyped-null-with-type", textType, RuntimeStringInherit, StringSourceExpression,
			StringLiteralNone, StringConversionString, StringUntypedNull},
		{"typed-null-with-any", T_any.ToType(), RuntimeStringInherit, StringSourceExpression,
			StringLiteralNone, StringConversionString, StringTypedNull},
		{"runtime-domain-on-number", T_int64.ToType(), RuntimeStringBinary, StringSourceExpression,
			StringLiteralNone, StringConversionString, StringNotNull},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewStringSemanticState(test.typ, test.runtime, test.source, test.literal,
				test.conversion, test.nullKind)
			require.Error(t, err)
		})
	}
}

func TestMergeStringSemanticStates(t *testing.T) {
	textType := T_varchar.ToType()
	binaryType := T_varbinary.ToType()
	selected, err := NewStringSemanticState(textType, RuntimeStringBinary, StringSourceSQLPrepare,
		StringLiteralNone, StringConversionDecimal, StringNotNull)
	require.NoError(t, err)

	result, err := MergeStringSemanticStates(StringMergeSelectedValue, binaryType, selected)
	require.NoError(t, err)
	require.Equal(t, binaryType, result.StaticType())
	require.Equal(t, RuntimeStringBinary, result.RuntimeDomain())
	require.Equal(t, StringSourceSQLPrepare, result.Source())
	require.Equal(t, StringConversionDecimal, result.ConversionKind())

	selectedText, err := NewStringSemanticState(textType, RuntimeStringInherit, StringSourceLiteral,
		StringLiteralText, StringConversionString, StringNotNull)
	require.NoError(t, err)
	result, err = MergeStringSemanticStates(StringMergeSelectedValue, binaryType, selectedText)
	require.NoError(t, err)
	require.Equal(t, RuntimeStringText, result.RuntimeDomain())

	selectedBinary, err := NewStringSemanticState(binaryType, RuntimeStringInherit, StringSourceLiteral,
		StringLiteralBinaryIntroducer, StringConversionString, StringNotNull)
	require.NoError(t, err)
	result, err = MergeStringSemanticStates(StringMergeSelectedValue, textType, selectedBinary)
	require.NoError(t, err)
	require.Equal(t, RuntimeStringBinary, result.RuntimeDomain())
	require.Equal(t, StringLiteralBinaryIntroducer, result.LiteralForm())

	text, err := NewStringSemanticState(textType, RuntimeStringInherit, StringSourceLiteral,
		StringLiteralText, StringConversionString, StringNotNull)
	require.NoError(t, err)
	binary, err := NewStringSemanticState(binaryType, RuntimeStringInherit, StringSourceCOMStmt,
		StringLiteralNone, StringConversionString, StringNotNull)
	require.NoError(t, err)

	result, err = MergeStringSemanticStates(StringMergeCommonDomain, binaryType, text, binary)
	require.NoError(t, err)
	require.Equal(t, RuntimeStringInherit, result.RuntimeDomain())
	require.Equal(t, StringSourceExpression, result.Source())
	require.Equal(t, StringConversionString, result.ConversionKind())

	result, err = MergeStringSemanticStates(StringMergeContributingValues, textType, text, binary)
	require.NoError(t, err)
	require.Equal(t, RuntimeStringBinary, result.RuntimeDomain())
	require.Equal(t, StringDomainBinary, result.EffectiveStringDomain())

	typedNull, err := NewStringSemanticState(textType, RuntimeStringInherit, StringSourceSQLPrepare,
		StringLiteralNone, StringConversionString, StringTypedNull)
	require.NoError(t, err)
	result, err = MergeStringSemanticStates(StringMergeContributingValues, textType, typedNull)
	require.NoError(t, err)
	require.Equal(t, StringTypedNull, result.NullKind())

	_, err = MergeStringSemanticStates(StringMergeSelectedValue, textType, text, binary)
	require.Error(t, err)
	_, err = MergeStringSemanticStates(StringMergeCommonDomain, textType)
	require.Error(t, err)
}

func TestMergeStringSources(t *testing.T) {
	sources := []StringSource{
		StringSourceExpression,
		StringSourceLiteral,
		StringSourceUserVariable,
		StringSourceSQLPrepare,
		StringSourceCOMStmt,
	}
	for _, left := range sources {
		require.True(t, left.Valid())
		for _, right := range sources {
			merged, err := MergeStringSources(left, right)
			require.NoError(t, err)
			reverse, err := MergeStringSources(right, left)
			require.NoError(t, err)
			require.Equal(t, merged, reverse)
			if left == right {
				require.Equal(t, left, merged)
			} else {
				require.Equal(t, StringSourceExpression, merged)
			}
		}
	}
	unknown := StringSource(255)
	require.False(t, unknown.Valid())
	_, err := MergeStringSources(unknown, StringSourceLiteral)
	require.Error(t, err)
}

func TestStaticStringDomain(t *testing.T) {
	binaryText := T_varchar.ToType()
	binaryText.Charset = CharsetBinary
	require.Equal(t, StringDomainNone, StaticStringDomain(T_int64.ToType()))
	require.Equal(t, StringDomainText, StaticStringDomain(T_varchar.ToType()))
	require.Equal(t, StringDomainBinary, StaticStringDomain(T_blob.ToType()))
	require.Equal(t, StringDomainBinary, StaticStringDomain(binaryText))
	require.Equal(t, StringUntypedNull, StringNullKindForType(T_any.ToType(), true))
	require.Equal(t, StringTypedNull, StringNullKindForType(T_varchar.ToType(), true))
	require.Equal(t, StringNotNull, StringNullKindForType(T_varchar.ToType(), false))
}
