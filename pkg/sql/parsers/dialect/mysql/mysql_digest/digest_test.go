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

package digest

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDigestMatchesMySQL84TokenSemantics(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		text string
		hash string
	}{
		{
			name: "literal normalization",
			sql:  "SELECT 1",
			text: "SELECT ?",
			hash: "d1b44b0c19af710b5a679907e284acd2ddc285201794bc69a2389d77baedddae",
		},
		{
			name: "comparison preserves unary signs",
			sql:  "SELECT * FROM t WHERE a<>-1 AND b<=+2",
			text: "SELECT * FROM `t` WHERE `a` != - ? AND `b` <= + ?",
			hash: "cadbb8f33e0036281f950efa33d1a3e35d6bb536decfe2e92a9d00a972942c8e",
		},
		{
			name: "select absorbs unary sign",
			sql:  "SELECT -42",
			text: "SELECT ?",
			hash: "d1b44b0c19af710b5a679907e284acd2ddc285201794bc69a2389d77baedddae",
		},
		{
			name: "case branches preserve unary signs",
			sql:  "SELECT CASE WHEN a THEN -1 ELSE +2 END",
			text: "SELECT CASE WHEN `a` THEN - ? ELSE + ? END",
			hash: "b13883eec7cbb158f27ca4e1fda9be138c02d89a1407b7cfe3bfb53027c27958",
		},
		{
			name: "between absorbs unary signs",
			sql:  "SELECT * FROM t WHERE a BETWEEN -1 AND +2",
			text: "SELECT * FROM `t` WHERE `a` BETWEEN ? AND ?",
			hash: "d1c98f0ee5aba1c8428a14dfb200db12cb3172141afb469a0b75d3b0cc699edd",
		},
		{
			name: "in list reduction",
			sql:  "SELECT * FROM t WHERE a IN (-1,+2)",
			text: "SELECT * FROM `t` WHERE `a` IN (...)",
			hash: "8365a3050bedef8b815cff59ce14255fd9205b3e8f4d32cac07c2ae9b74544a8",
		},
		{
			name: "character set introducer",
			sql:  "SELECT _utf8mb4'hello'",
			text: "SELECT (_charset) ?",
			hash: "04144c90cfef7b8973c07fe5b12181df7996a6db61471dfe8b365d188cea8e19",
		},
		{
			name: "trailing semicolon is significant",
			sql:  "SELECT 1;",
			text: "SELECT ? ;",
			hash: "4b46f54bd8065b8dc5777a0cc14bcefc2be16c230edaa024ddd28ebf988a865c",
		},
		{
			name: "ordinary comment has empty token stream",
			sql:  "/* comment only */",
			text: "",
			hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name: "optimizer hint scaled number",
			sql:  "SELECT /*+ SET_VAR(sort_buffer_size=16M) */ 1",
			text: "SELECT /*+ SET_VAR ( `sort_buffer_size` = ? ) */ ?",
			hash: "c76de720d78ecfc18e2cf4e87e894bc134f7596270461ad343b3e9d069af2d31",
		},
		{
			name: "optimizer hint after ordinary comment is ignored",
			sql:  "SELECT /* ordinary */ /*+ MAX_EXECUTION_TIME(1) */ 1",
			text: "SELECT ?",
			hash: "d1b44b0c19af710b5a679907e284acd2ddc285201794bc69a2389d77baedddae",
		},
		{
			name: "empty optimizer hint is an ordinary comment",
			sql:  "/*+ */",
			text: "",
			hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name: "legacy with rollup uses synthetic token",
			sql:  "SELECT a FROM t GROUP BY a WITH /* comment */ ROLLUP",
			text: "SELECT `a` FROM `t` GROUP BY `a` WITH ROLLUP",
			hash: "8b73b88a9ea6da4ce94aaed3dc371d92791c705aa173693ee25e141acc4f02df",
		},
		{
			name: "ddl null keywords and default literal",
			sql:  "CREATE TABLE t(a INT NULL, b INT NOT NULL, c INT DEFAULT NULL, d INT NULL DEFAULT NULL)",
			text: "CREATE TABLE `t` ( `a` INTEGER NULL , `b` INTEGER NOT NULL , `c` INTEGER DEFAULT ? , `d` INTEGER NULL DEFAULT ? )",
			hash: "e9b16bb26e6f0a1adede7aaf756bf40913eae757496c8bc35e0207573b3452c9",
		},
		{
			name: "ddl nested null expressions stay literals",
			sql:  "CREATE TABLE t(a INT DEFAULT (NULL), b INT CHECK (NULL IS NULL), c INT COMMENT 'x' NULL)",
			text: "CREATE TABLE `t` ( `a` INTEGER DEFAULT (?) , `b` INTEGER CHECK ( ? IS NULL ) , `c` INTEGER COMMENT ? NULL )",
			hash: "6a2411936cd264885f006a75a992f5fe5880f064f828c0de047c87ff3b3d90c7",
		},
		{
			name: "foreign key set null remains keyword",
			sql:  "CREATE TABLE t(a INT, CONSTRAINT fk FOREIGN KEY(a) REFERENCES u(a) ON DELETE SET NULL ON UPDATE SET NULL)",
			text: "CREATE TABLE `t` ( `a` INTEGER , CONSTRAINT `fk` FOREIGN KEY ( `a` ) REFERENCES `u` ( `a` ) ON DELETE SET NULL ON UPDATE SET NULL )",
			hash: "22ab67e57bababdf388f6df22f5d3baff9280925b4e02c8a85264bd0d14d3cdf",
		},
		{
			name: "json table null response remains keyword",
			sql:  "SELECT * FROM JSON_TABLE('[1]', '$[*]' COLUMNS(x INT PATH '$' NULL ON EMPTY ERROR ON ERROR)) AS jt",
			text: "SELECT * FROM JSON_TABLE ( ?, ... FIELDS ( `x` INTEGER PATH ? NULL ON EMPTY ERROR ON ERROR ) ) AS `jt`",
			hash: "09ad64c067a141cf5fb830ae5a68f73cceef8de702a41f91b9f80a6c27a5f275",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Compute(test.sql)
			require.NoError(t, err)
			require.Equal(t, test.text, got.Text)
			require.Equal(t, test.hash, got.Hash)
		})
	}
}

func TestDigestLexicalEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		mode SQLMode
		err  bool
	}{
		{name: "hex and binary literals", sql: "SELECT 0xFF, 0b101, X'0F', B'101'"},
		{name: "decimal and exponent forms", sql: "SELECT .5, 1., 1.25, 1e10, 1e+2, 1e-2"},
		{name: "invalid exponent falls back to identifier", sql: "SELECT 1e, 1e+"},
		{name: "comparison and boolean operators", sql: "SELECT a = b, a != b, a <=> b, a && b, a || b, a := b"},
		{name: "json arrows", sql: "SELECT doc->'$.a', doc->>'$.a'"},
		{name: "user and system variables", sql: "SELECT @user_name, @@global.time_zone, @@`quoted`"},
		{name: "quoted variable", sql: "SELECT @'user_name'"},
		{name: "escaped quoted identifiers", sql: "SELECT `a``b`, \"a\"\"b\"", mode: ModeANSIQuotes},
		{name: "national and dollar quoted strings", sql: "SELECT N'abc', $tag$body$tag$, $$body$$"},
		{name: "national string honors backslash escapes", sql: `SELECT N'a\'b'`},
		{name: "line comment variants", sql: "SELECT 1 # trailing\n; SELECT 2 -- trailing\n"},
		{name: "version comments", sql: "/*! SELECT 1 */ /*!80000 SELECT 2 */ /*!99999 SELECT 3 */"},
		{name: "hint decimal and quoted arguments", sql: "SELECT /*+ MAX_EXECUTION_TIME(1.5) QB_NAME('q''b') */ 1"},
		{name: "hint scaled and identifier arguments", sql: "SELECT /*+ MAX_EXECUTION_TIME(1K) QB_NAME(foo) */ 1"},
		{name: "hint backtick identifier", sql: "SELECT /*+ QB_NAME(`q``b`) */ 1"},
		{name: "hint punctuation", sql: "SELECT /*+ BKA(t) NO_INDEX_MERGE(t) */ 1"},
		{name: "invalid hex literal", sql: "SELECT X'0G'", err: true},
		{name: "invalid binary literal", sql: "SELECT B'102'", err: true},
		{name: "invalid hint decimal", sql: "SELECT /*+ MAX_EXECUTION_TIME(1.) */ 1", err: true},
		{name: "unterminated hint", sql: "SELECT /*+ MAX_EXECUTION_TIME(1) ", err: true},
		{name: "unterminated tagged dollar quoted string", sql: "SELECT $tag$body", err: true},
		{name: "unterminated quoted identifier", sql: "SELECT `name", err: true},
		{name: "unterminated dollar quoted string", sql: "SELECT $$unterminated", err: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compute(tc.sql, Options{SQLMode: tc.mode})
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDigestSQLModeTokenIdentity(t *testing.T) {
	defaultDigest, err := Compute("SELECT 1 || 0")
	require.NoError(t, err)
	concatDigest, err := Compute("SELECT 1 || 0", Options{SQLMode: ModePipesAsConcat})
	require.NoError(t, err)
	require.NotEqual(t, defaultDigest.Hash, concatDigest.Hash)
	require.Equal(t, "SELECT ? || ?", defaultDigest.Text)
	require.Equal(t, "SELECT ? || ?", concatDigest.Text)

	defaultNot, err := Compute("SELECT NOT 1")
	require.NoError(t, err)
	highNot, err := Compute("SELECT NOT 1", Options{SQLMode: ModeHighNotPrecedence})
	require.NoError(t, err)
	require.NotEqual(t, defaultNot.Hash, highNot.Hash)
	isNotNull, err := Compute("SELECT a IS NOT NULL", Options{SQLMode: ModeHighNotPrecedence})
	require.NoError(t, err)
	require.Contains(t, isNotNull.Text, "NULL")
}

func TestDigestIgnoreSpaceFunctionTokenIdentity(t *testing.T) {
	withoutIgnoreSpace, err := Compute("SELECT COUNT (1)")
	require.NoError(t, err)
	withIgnoreSpace, err := Compute("SELECT COUNT (1)", Options{SQLMode: ModeIgnoreSpace})
	require.NoError(t, err)
	require.NotEqual(t, withoutIgnoreSpace.Hash, withIgnoreSpace.Hash)
	require.Equal(t, "SELECT `COUNT` (?)", withoutIgnoreSpace.Text)
	require.Equal(t, "SELECT COUNT (?)", withIgnoreSpace.Text)

	withoutSpace, err := Compute("SELECT COUNT(1)")
	require.NoError(t, err)
	require.Equal(t, withoutSpace, withIgnoreSpace)
}

func TestDigestOptimizerHintRequiresImmediateStatementKeyword(t *testing.T) {
	for _, tc := range []struct{ sql, plain string }{
		{sql: "SELECT 1 /*+ MAX_EXECUTION_TIME(1) */", plain: "SELECT 1"},
		{sql: "SELECT ( /*+ MAX_EXECUTION_TIME(1) */ 1)", plain: "SELECT (1)"},
	} {
		plain, err := Compute(tc.plain)
		require.NoError(t, err)
		got, err := Compute(tc.sql)
		require.NoError(t, err)
		require.Equal(t, plain, got, tc.sql)
	}
}

func TestDigestExecutableCommentVersionBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name       string
		versionID  int
		commentSQL string
		wantText   string
	}{
		{name: "below target executes", versionID: 80400, commentSQL: "/*!80399 SELECT 1 */", wantText: "SELECT ?"},
		{name: "target executes", versionID: 80400, commentSQL: "/*!80400 SELECT 1 */", wantText: "SELECT ?"},
		{name: "above target skips", versionID: 80400, commentSQL: "/*!80401 SELECT 1 */", wantText: ""},
		{name: "custom target executes newer guard", versionID: 80401, commentSQL: "/*!80401 SELECT 1 */", wantText: "SELECT ?"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Compute(tc.commentSQL, Options{MySQLVersionID: tc.versionID})
			require.NoError(t, err)
			require.Equal(t, tc.wantText, got.Text)
		})
	}
}

func TestDigestNCharSQLModeEscaping(t *testing.T) {
	sql := `SELECT N'a\'b'`
	_, err := Compute(sql)
	require.NoError(t, err)
	_, err = Compute(sql, Options{SQLMode: ModeNoBackslashEscapes})
	require.Error(t, err)
}

func TestDigestRejectsParameterMarkersWhenRequested(t *testing.T) {
	got, err := Compute("SELECT ?", Options{RejectParameterMarkers: true})
	require.Error(t, err)
	require.Equal(t, "SELECT", got.Text)

	got, err = Compute("SELECT '?'", Options{RejectParameterMarkers: true})
	require.NoError(t, err)
	require.Equal(t, "SELECT ?", got.Text)
}

func TestDigestMaxLengthIsTokenBufferLimit(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		maxLength int
		text      string
		hash      string
	}{
		{
			name:      "zero disables digest production",
			sql:       "SELECT 1",
			maxLength: 0,
			text:      "",
			hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name:      "partial token does not fit",
			sql:       "SELECT 1",
			maxLength: 1,
			text:      "",
			hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name:      "one token fits",
			sql:       "SELECT 1",
			maxLength: 2,
			text:      "SELECT",
			hash:      "5d7930059ca634d2a755dc51a7c39e0e1dceb5375d3e3dc9ad390344fd1d0a48",
		},
		{
			name:      "two tokens fit",
			sql:       "SELECT 1",
			maxLength: 4,
			text:      "SELECT ?",
			hash:      "d1b44b0c19af710b5a679907e284acd2ddc285201794bc69a2389d77baedddae",
		},
		{
			name:      "identifier is atomic when short by one byte",
			sql:       "SELECT abc",
			maxLength: 8,
			text:      "SELECT",
			hash:      "5d7930059ca634d2a755dc51a7c39e0e1dceb5375d3e3dc9ad390344fd1d0a48",
		},
		{
			name:      "identifier exactly fits",
			sql:       "SELECT abc",
			maxLength: 9,
			text:      "SELECT `abc`",
			hash:      "fcac0e2dab941659fbac1e6f3c13656a0376934cc2794eb352fb15d65abb13ae",
		},
		{
			name:      "official documentation boundary",
			sql:       "SELECT * FROM mytable WHERE cola = 10 AND colb = 20",
			maxLength: 16,
			text:      "SELECT * FROM",
			hash:      "9642da7ea9b8e3a69d62fcb050b8ac7c794e3dc4e4696c8dca7a8589cc9e0160",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			maxLength := test.maxLength
			got, err := Compute(test.sql, Options{MaxDigestLength: &maxLength})
			require.NoError(t, err)
			require.Equal(t, test.text, got.Text)
			require.Equal(t, test.hash, got.Hash)
		})
	}

	maxLength := 0
	got, err := Compute("SELECT FROM", Options{MaxDigestLength: &maxLength})
	require.NoError(t, err)
	require.False(t, got.CommentOnly)

	got, err = Compute("/* comment only */", Options{MaxDigestLength: &maxLength})
	require.NoError(t, err)
	require.True(t, got.CommentOnly)

	got, err = Compute("/* comment */\x00SELECT 1", Options{MaxDigestLength: &maxLength})
	require.NoError(t, err)
	require.False(t, got.CommentOnly)

	longSQL := "SELECT " + strings.Repeat("a+", 600) + "1"
	defaultDigest, err := Compute(longSQL)
	require.NoError(t, err)
	defaultLimit := DefaultMaxDigestLength
	explicitDefaultDigest, err := Compute(longSQL, Options{MaxDigestLength: &defaultLimit})
	require.NoError(t, err)
	require.Equal(t, explicitDefaultDigest, defaultDigest)
	largeLimit := 1048576
	largeDigest, err := Compute(longSQL, Options{MaxDigestLength: &largeLimit})
	require.NoError(t, err)
	require.NotEqual(t, largeDigest.Hash, defaultDigest.Hash)
}

func TestDigestSQLModeAndLexErrors(t *testing.T) {
	tests := []struct {
		name string
		mode SQLMode
		sql  string
		hash string
	}{
		{
			name: "ansi quoted identifiers",
			mode: ModeANSIQuotes,
			sql:  `SELECT "a""b" FROM "t"`,
			hash: "80f130df132fb6bef962e9abd2054dbc458d0c7903d03cb9d371c0bfbf705175",
		},
		{
			name: "no backslash escapes",
			mode: ModeNoBackslashEscapes,
			sql:  `SELECT _utf8mb4'a\b', N'a''b'`,
			hash: "08205ea7179f0c57c748cd5f584863d8aaa2768696820813929e9bea00994610",
		},
		{
			name: "combined modes in optimizer hint",
			mode: ModeANSIQuotes | ModeNoBackslashEscapes,
			sql:  `SELECT /*+ QB_NAME("q""b") */ "a" FROM "t"`,
			hash: "8c4cd5685767dcfe23e68c9d46d323c14bbcd967c5b495fdcfed7a711bfff24d",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Compute(test.sql, Options{SQLMode: test.mode})
			require.NoError(t, err)
			require.Equal(t, test.hash, got.Hash)
		})
	}

	_, err := Compute("SELECT 'unterminated")
	require.Error(t, err)
}
