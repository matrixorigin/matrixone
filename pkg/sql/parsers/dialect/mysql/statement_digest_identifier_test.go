// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestDigestIdentifierCasePolicy(t *testing.T) {
	for _, test := range [][2]string{
		{"", ""}, {"ascii", "ascii"}, {"ABC_x", "abc_x"},
		{"ẞß", "ẞß"}, {"Ⱥⱥ", "Ⱥⱥ"}, {"Აა", "Აა"}, {"Ꭰꭰ", "Ꭰꭰ"},
		{"İIı", "iiı"}, {"Σσς", "σσς"}, {"ÄЯＡ", "äяａ"},
		{"\xffA", "\xffa"}, {"A\xffB", "a\xffb"}, {"😀A", "😀a"},
	} {
		require.Equal(t, test[1], digestIdentifierKey(test[0]))
	}
	for i, pair := range digestIdentifierLower {
		if i > 0 {
			require.Greater(t, pair[0], digestIdentifierLower[i-1][0])
		}
		require.Equal(t, string(pair[1]), digestIdentifierKey(string(pair[0])))
		require.Equal(t, string(pair[1]), digestIdentifierKey(string(pair[1])))
	}
}

func TestDigestKeywordQualification(t *testing.T) {
	for word, symbol := range digestSymbols {
		if word[0] < 'A' || word[0] > 'Z' {
			continue
		}
		t.Run(word, func(t *testing.T) {
			scanner := NewScanner(dialect.MYSQL, word+".a")
			defer PutScanner(scanner)
			scanner.digestMode = true
			token, value := scanner.Scan()
			require.Equal(t, ID, token)
			require.Equal(t, word, value)
			require.True(t, scanner.qualifiedIdentifier)
			token, _ = scanner.Scan()
			require.Equal(t, int('.'), token)
			token, _ = scanner.ScanAfterDot()
			require.Equal(t, ID, token)
			require.True(t, scanner.qualifiedIdentifier)

			scanner.setSql(word + ".*")
			scanner.digestMode = true
			scanner.Scan()
			token, _ = scanner.Scan()
			if symbol.reserved {
				require.Equal(t, LEX_ERROR, token)
			} else {
				require.Equal(t, int('.'), token)
			}
		})
	}
}

func TestDigestQualifierStateReset(t *testing.T) {
	scanner := NewScanner(dialect.MYSQL, "integer /*gap*/ .a")
	defer PutScanner(scanner)
	scanner.digestMode = true
	scanner.ScanWithComments()
	require.True(t, scanner.digestReservedWord)
	token, _ := scanner.ScanWithComments()
	require.Equal(t, COMMENT, token)
	require.True(t, scanner.digestReservedWord)
	token, _ = scanner.ScanWithComments()
	require.Equal(t, LEX_ERROR, token)

	scanner.setSql("integer.*")
	require.False(t, scanner.digestReservedWord)
	require.False(t, scanner.qualifiedIdentifier)
	scanner.Scan()
	token, _ = scanner.Scan()
	require.Equal(t, int('.'), token, "ordinary MO lexical policy is unchanged")

	scanner.setSql("sounds.a")
	scanner.digestMode = true
	scanner.Scan()
	require.True(t, scanner.qualifiedIdentifier)
	scanner.reset(true, false)
	require.False(t, scanner.qualifiedIdentifier)
	require.False(t, scanner.digestReservedWord)
	require.False(t, scanner.digestMode)

	scanner.setSql("integer")
	scanner.digestMode = true
	scanner.Scan()
	require.True(t, scanner.digestReservedWord)
	scanner.reset(true, false)
	require.False(t, scanner.digestReservedWord)
}

func TestDigestIdentifierDisplayDoesNotChangeHash(t *testing.T) {
	var priorHash string
	for _, name := range []string{strings.Repeat("a", 65), strings.Repeat("b", 65), strings.Repeat("名", 21) + "ab"} {
		text, hash, err := StatementDigest(context.Background(), "SELECT `"+name+"`", "")
		require.NoError(t, err)
		require.Equal(t, "SELECT ...", text)
		require.NotEqual(t, priorHash, hash)
		priorHash = hash
	}
	name := strings.Repeat("名", 21) + "a"
	text, _, err := StatementDigest(context.Background(), "SELECT `"+name+"`,1", "")
	require.NoError(t, err)
	require.Equal(t, "SELECT `"+name+"` , ?", text)
	text, _, err = StatementDigest(context.Background(), "SELECT `"+name+"b`,1", "")
	require.NoError(t, err)
	require.Equal(t, "SELECT ..., ?", text)
}
