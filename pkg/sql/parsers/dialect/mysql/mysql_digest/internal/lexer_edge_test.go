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

package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClassifyIntegerBoundaries(t *testing.T) {
	tests := []struct {
		name string
		text string
		want int
	}{
		{name: "empty", text: "", want: NUM},
		{name: "short", text: "12345678", want: NUM},
		{name: "leading zeros", text: "000000001", want: NUM},
		{name: "positive int max", text: "2147483647", want: NUM},
		{name: "positive long", text: "2147483648", want: LONG_NUM},
		{name: "negative int min", text: "-2147483648", want: NUM},
		{name: "negative long", text: "-2147483649", want: LONG_NUM},
		{name: "signed long max", text: "9223372036854775807", want: LONG_NUM},
		{name: "signed long overflow", text: "9223372036854775808", want: ULONGLONG_NUM},
		{name: "negative signed long min", text: "-9223372036854775808", want: LONG_NUM},
		{name: "negative signed long overflow", text: "-9223372036854775809", want: DECIMAL_NUM},
		{name: "unsigned long max", text: "18446744073709551615", want: ULONGLONG_NUM},
		{name: "unsigned long overflow", text: "18446744073709551616", want: DECIMAL_NUM},
		{name: "very large", text: "999999999999999999999", want: DECIMAL_NUM},
		{name: "explicit plus", text: "+2147483648", want: LONG_NUM},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, ClassifyInteger(tc.text))
		})
	}
}

func TestLexerHelpersAndTokenMetadata(t *testing.T) {
	require.Equal(t, "A`B", stripIdentifierQuotes("`A``B`"))
	require.Equal(t, `A"B`, stripIdentifierQuotes(`"A""B"`))
	require.Equal(t, "``A``B", escapeBackticks("`A`B"))
	require.True(t, isIdentStart('_'))
	require.True(t, isIdentStart(0x80))
	require.False(t, isIdentStart('1'))
	require.True(t, isHexDigit('f'))
	require.True(t, isHexDigit('F'))
	require.False(t, isHexDigit('g'))

	lexer := NewLexer("SELECT 1")
	tok := lexer.Lex()
	text, err := lexer.TokenText(tok)
	require.NoError(t, err)
	require.Equal(t, "SELECT", text)
	_, err = lexer.TokenText(Token{Start: -1, End: 0})
	require.Error(t, err)
	_, err = lexer.TokenText(Token{Start: 2, End: 1})
	require.Error(t, err)
	_, err = lexer.TokenText(Token{Start: 0, End: 99})
	require.Error(t, err)
	require.Contains(t, (&LexError{Position: 1, Message: "bad", Input: "x"}).Error(), "near")
	require.NotContains(t, (&LexError{Position: 1, Message: "bad"}).Error(), "near")
	require.Equal(t, "SELECT", TokenString(SELECT_SYM))
	require.Equal(t, "!", TokenString('!'))
	require.Equal(t, "(unknown)", TokenString(9999))
	require.True(t, TokenAppendSpace(-1))
	require.False(t, TokenStartExpr(-1))
	require.False(t, TokenIsHintable(-1))
}
