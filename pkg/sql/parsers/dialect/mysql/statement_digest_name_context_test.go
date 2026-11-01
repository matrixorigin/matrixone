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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestDigestTableNameCharacterDomain(t *testing.T) {
	for _, name := range []string{"t", strings.Repeat("a", 64), strings.Repeat("名", 64), strings.Repeat("😀", 64), "t\u00a0"} {
		require.True(t, tree.ValidMySQLTableName(name), "%q", name)
	}
	for _, name := range []string{"", "t ", "t\t", "t\r", "t\n", "t\v", "t\f", strings.Repeat("a", 65), strings.Repeat("名", 65), strings.Repeat("😀", 65)} {
		require.False(t, tree.ValidMySQLTableName(name), "%q", name)
	}
}

func TestDigestTableNameInvocationIsolation(t *testing.T) {
	ctx := context.Background()
	long := "`" + strings.Repeat("a", 65) + "`"
	for range 3 {
		for _, sql := range []string{
			"SELECT * FROM " + long,
			"EXPLAIN SELECT * FROM " + long,
			"WITH c AS (SELECT * FROM " + long + ") SELECT * FROM c",
		} {
			_, _, err := StatementDigest(ctx, sql, "")
			require.Error(t, err, sql)
			// Execution parsing still owns its original binder/name policy.
			stmts, err := Parse(ctx, sql, 0)
			require.NoError(t, err, sql)
			for _, stmt := range stmts {
				stmt.Free()
			}
		}
		for _, sql := range []string{
			"SELECT 1 AS " + long,
			"WITH c AS (SELECT * FROM " + long + ") SELECT 1",
			"DELETE " + long + " FROM t AS " + long,
			"CREATE TABLE t (a int REFERENCES " + long + "(a))",
			"WITH c AS (((SELECT * FROM t FOR SHARE) FOR UPDATE)) SELECT 1 AS " + long,
		} {
			_, _, err := StatementDigest(ctx, sql, "")
			require.NoError(t, err, sql)
		}
		text, hash, err := StatementDigest(ctx, "SELECT 1", "")
		require.NoError(t, err)
		require.Equal(t, "SELECT ?", text)
		require.Equal(t, "d1b44b0c19af710b5a679907e284acd2ddc285201794bc69a2389d77baedddae", hash)
	}
}

func TestDigestSupplementaryNameDomains(t *testing.T) {
	ctx := context.Background()
	for _, pair := range [][2]string{{"ABC名", "ABC名"}, {"😀A", "?A"}, {"a😀名😁b", "a?名?b"}, {"\xff😀A", "\xff?A"}} {
		require.Equal(t, pair[1], digestSystemIdentifier(pair[0]))
	}
	text, hash, err := StatementDigest(ctx, "SELECT `😀`,1", "")
	require.NoError(t, err)
	require.Equal(t, "SELECT , ?", text)
	require.Equal(t, "fcf53be7367b008461aaf305be2999dd26887a0f86c1dc0bda0e273da175cab7", hash)
	otherText, otherHash, err := StatementDigest(ctx, "SELECT `a😀b`,1", "")
	require.NoError(t, err)
	require.Equal(t, text, otherText)
	require.NotEqual(t, hash, otherHash, "display failure cannot rewrite stored identifier bytes")
	for _, mode := range []string{"", "ANSI_QUOTES", "NO_BACKSLASH_ESCAPES"} {
		quotedText, quotedHash, err := StatementDigest(ctx, "SELECT /*+ QB_NAME(`😀`) */ 1", mode)
		require.NoError(t, err)
		plainText, plainHash, err := StatementDigest(ctx, "SELECT /*+ QB_NAME(`?`) */ 1", mode)
		require.NoError(t, err)
		require.Equal(t, plainText, quotedText)
		require.Equal(t, plainHash, quotedHash, "quoted hint names convert before storage")
	}
}

func TestDigestHintScannerCharacterDomain(t *testing.T) {
	for _, test := range []struct {
		sql      string
		previous int
		kind     int
		value    string
	}{
		{"\v\f$a$b", 0, ID, "$a$b"},
		{"a😀b", 0, ID, "a😀b"},
		{"`a😀b`", 0, QUOTE_ID, "a?b"},
		{"'a\\b'", 0, STRING, "a\\b"},
		{"@1😀", '(', digestHintQueryBlock, "1😀"},
		{"@`😀`", ID, digestHintQueryBlock, "?"},
		{" @q", ID, '@', ""},
		{"@q", 0, '@', ""},
		{"@\"q\"", ID, LEX_ERROR, ""},
		{"@'q'", ID, LEX_ERROR, ""},
		{"@\xff", '(', LEX_ERROR, ""},
		{"a\xff", 0, LEX_ERROR, ""},
		{"1\xff", 0, LEX_ERROR, ""},
		{"`a\xff`", 0, LEX_ERROR, ""},
		{"``", 0, LEX_ERROR, ""},
		{"1K名", 0, INTEGRAL, "1K"},
		{"1k名", 0, ID, "1k名"},
		{".1名", 0, LEX_ERROR, ""},
		{".1", 0, FLOAT, ".1"},
		{".", 0, LEX_ERROR, ""},
	} {
		t.Run(test.sql, func(t *testing.T) {
			scanner := NewScanner(dialect.MYSQL, test.sql)
			defer PutScanner(scanner)
			scanner.digestMode = true
			kind, value := scanDigestHint(scanner, test.previous)
			require.Equal(t, test.kind, kind)
			require.Equal(t, test.value, value)
			require.Positive(t, scanner.Pos, "even invalid hint tokens must advance")
		})
	}
}

func TestDigestQualifiedKeywordBridge(t *testing.T) {
	for _, sql := range []string{"SELECT t. end FROM t", "SELECT t./*gap*/end FROM t"} {
		text, hash, err := StatementDigest(context.Background(), sql, "")
		require.NoError(t, err)
		require.Equal(t, "SELECT `t` . END FROM `t`", text)
		require.Equal(t, "c8c735aef1934ca1a7e0fcfbaef33a6429359c0bdaa25feb1a09a3a1e651b002", hash)
	}
	for _, sql := range []string{"SELECT t. integer FROM t", "SELECT t./*gap*/select FROM t"} {
		_, _, err := StatementDigest(context.Background(), sql, "")
		require.Error(t, err)
	}
}
