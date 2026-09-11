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
		{name: "line comment variants", sql: "SELECT 1 # trailing\n; SELECT 2 -- trailing\n"},
		{name: "hint decimal and quoted arguments", sql: "SELECT /*+ MAX_EXECUTION_TIME(1.5) QB_NAME('q''b') */ 1"},
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

	// Function-only names are identifiers unless they form a call. This must
	// apply to bare names, aliases, operators, and qualified identifiers too.
	for _, sql := range []string{
		"SELECT COUNT FROM t",
		"SELECT COUNT AS c",
		"SELECT COUNT+1",
		"SELECT COUNT.x FROM t",
	} {
		got, err := Compute(sql)
		require.NoError(t, err)
		require.Contains(t, got.Text, "`COUNT`", sql)
		withIgnoreSpace, err := Compute(sql, Options{SQLMode: ModeIgnoreSpace})
		require.NoError(t, err)
		require.Equal(t, got, withIgnoreSpace, sql)
	}

	// MySQL only makes the functions listed in sql/lex.h whitespace-sensitive.
	// Other built-ins, such as AVG and JSON_ARRAYAGG, remain function tokens
	// when a space precedes the opening parenthesis.
	for _, name := range []string{"AVG", "JSON_ARRAYAGG"} {
		withoutIgnoreSpace, err := Compute("SELECT " + name + " (1)")
		require.NoError(t, err)
		withIgnoreSpace, err := Compute("SELECT "+name+" (1)", Options{SQLMode: ModeIgnoreSpace})
		require.NoError(t, err)
		require.Equal(t, withoutIgnoreSpace, withIgnoreSpace, name)
		require.Equal(t, "SELECT "+name+" (?)", withoutIgnoreSpace.Text, name)
	}

	withoutIgnoreSpace, err = Compute("SELECT VAR_SAMP (1)")
	require.NoError(t, err)
	withIgnoreSpace, err = Compute("SELECT VAR_SAMP (1)", Options{SQLMode: ModeIgnoreSpace})
	require.NoError(t, err)
	require.NotEqual(t, withoutIgnoreSpace, withIgnoreSpace)
	require.Equal(t, "SELECT `VAR_SAMP` (?)", withoutIgnoreSpace.Text)
	require.Equal(t, "SELECT VAR_SAMP (?)", withIgnoreSpace.Text)
}

func TestDigestUnparenthesizedTemporalAndUserKeywords(t *testing.T) {
	// These spellings are keyword tokens in MySQL 8.4 when no parentheses
	// follow. Their token values alias the canonical function spellings, so the
	// lexer must preserve the original spelling distinction before applying the
	// function-name lookahead.
	for _, tc := range []struct {
		name string
		sql  string
		text string
	}{
		{name: "current date", sql: "SELECT CURRENT_DATE", text: "SELECT CURDATE"},
		{name: "current time", sql: "SELECT CURRENT_TIME", text: "SELECT CURTIME"},
		{name: "current timestamp", sql: "SELECT CURRENT_TIMESTAMP", text: "SELECT NOW"},
		{name: "local time", sql: "SELECT LOCALTIME", text: "SELECT NOW"},
		{name: "local timestamp", sql: "SELECT LOCALTIMESTAMP", text: "SELECT NOW"},
		{name: "user", sql: "SELECT USER", text: "SELECT USER"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Compute(tc.sql)
			require.NoError(t, err)
			require.Equal(t, tc.text, got.Text)
		})
	}

	// The canonical function-only spellings retain the identifier fallback.
	for _, name := range []string{"CURDATE", "CURTIME", "NOW", "SYSDATE"} {
		got, err := Compute("SELECT " + name)
		require.NoError(t, err)
		require.Equal(t, "SELECT `"+name+"`", got.Text, name)
	}
}

func TestDigestDDLExpressionCounterexamples(t *testing.T) {
	for _, tc := range []struct {
		name string
		null string
		one  string
	}{
		{
			name: "create table check",
			null: "CREATE TABLE t (a INT, CHECK (BINARY NULL))",
			one:  "CREATE TABLE t (a INT, CHECK (BINARY 1))",
		},
		{
			name: "alter table check",
			null: "ALTER TABLE t ADD CONSTRAINT c CHECK (BINARY NULL)",
			one:  "ALTER TABLE t ADD CONSTRAINT c CHECK (BINARY 1)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nullDigest, err := Compute(tc.null)
			require.NoError(t, err)
			oneDigest, err := Compute(tc.one)
			require.NoError(t, err)
			require.Equal(t, oneDigest, nullDigest)
		})
	}
}

func TestDigestExecutableCommentTerminatorPreservesFollowingToken(t *testing.T) {
	withSpace, err := Compute("SELECT /*!80000 1 */ +2")
	require.NoError(t, err)

	for _, sql := range []string{
		"SELECT /*!80000 1 */+2",
		"SELECT /*!80000 1 */\t+2",
	} {
		got, err := Compute(sql)
		require.NoError(t, err)
		require.Equal(t, withSpace, got, sql)
	}

	for _, tc := range []struct {
		sql  string
		text string
	}{
		{sql: "SELECT /*!80000 1 */identifier", text: "SELECT ? `identifier`"},
		{sql: "SELECT /*!80000 1 */;", text: "SELECT ? ;"},
		{sql: "SELECT /*!80000 1 */", text: "SELECT ?"},
	} {
		got, err := Compute(tc.sql)
		require.NoError(t, err)
		require.Equal(t, tc.text, got.Text, tc.sql)
	}
}

func TestDigestCTASExpressionNullRemainsLiteral(t *testing.T) {
	withNull, err := Compute("CREATE TABLE t AS SELECT BINARY 'x'=NULL")
	require.NoError(t, err)
	withValue, err := Compute("CREATE TABLE t AS SELECT BINARY 'x'=1")
	require.NoError(t, err)
	require.Equal(t, withValue, withNull)
	require.Equal(t, "CREATE TABLE `t` AS SELECT BINARY ? = ?", withNull.Text)

	isNull, err := Compute("CREATE TABLE t AS SELECT BINARY 'x' IS NULL")
	require.NoError(t, err)
	require.Equal(t, "CREATE TABLE `t` AS SELECT BINARY ? IS NULL", isNull.Text)
}

func TestDigestNullOnResponseIsScopedToJSONTable(t *testing.T) {
	for _, tc := range []struct {
		name  string
		null  string
		value string
		text  string
	}{
		{
			name:  "create table column default",
			null:  "CREATE TABLE t (a TIMESTAMP DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP)",
			value: "CREATE TABLE t (a TIMESTAMP DEFAULT 1 ON UPDATE CURRENT_TIMESTAMP)",
			text:  "CREATE TABLE `t` ( `a` TIMESTAMP DEFAULT ? ON UPDATE NOW )",
		},
		{
			name:  "alter table column default",
			null:  "ALTER TABLE t ADD COLUMN a TIMESTAMP DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP",
			value: "ALTER TABLE t ADD COLUMN a TIMESTAMP DEFAULT 1 ON UPDATE CURRENT_TIMESTAMP",
			text:  "ALTER TABLE `t` ADD COLUMN `a` TIMESTAMP DEFAULT ? ON UPDATE NOW",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defaultNull, err := Compute(tc.null)
			require.NoError(t, err)
			defaultValue, err := Compute(tc.value)
			require.NoError(t, err)
			require.Equal(t, defaultValue, defaultNull)
			require.Equal(t, tc.text, defaultNull.Text)
		})
	}

	for _, tc := range []struct {
		name string
		sql  string
		text string
	}{
		{
			name: "null on empty",
			sql:  "SELECT * FROM JSON_TABLE('[1]', '$[*]' COLUMNS(x INT PATH '$' NULL ON EMPTY ERROR ON ERROR)) AS jt",
			text: "SELECT * FROM JSON_TABLE ( ?, ... FIELDS ( `x` INTEGER PATH ? NULL ON EMPTY ERROR ON ERROR ) ) AS `jt`",
		},
		{
			name: "null on error",
			sql:  "SELECT * FROM JSON_TABLE('[1]', '$[*]' COLUMNS(x INT PATH '$' ERROR ON EMPTY NULL ON ERROR)) AS jt",
			text: "SELECT * FROM JSON_TABLE ( ?, ... FIELDS ( `x` INTEGER PATH ? ERROR ON EMPTY NULL ON ERROR ) ) AS `jt`",
		},
		{
			name: "separate json table scopes",
			sql:  "SELECT * FROM JSON_TABLE('[1]', '$[*]' COLUMNS(x INT PATH '$' NULL ON EMPTY)) AS a JOIN JSON_TABLE('[2]', '$[*]' COLUMNS(y INT PATH '$' NULL ON ERROR)) AS b ON 1=1",
			text: "SELECT * FROM JSON_TABLE ( ?, ... FIELDS ( `x` INTEGER PATH ? NULL ON EMPTY ) ) AS `a` JOIN JSON_TABLE ( ?, ... FIELDS ( `y` INTEGER PATH ? NULL ON ERROR ) ) AS `b` ON ? = ?",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			jsonTable, err := Compute(tc.sql)
			require.NoError(t, err)
			require.Equal(t, tc.text, jsonTable.Text)
		})
	}
}

func TestDigestJSONResponseCounterexamples(t *testing.T) {
	for _, tc := range []struct{ name, sql, text string }{
		{"json value responses", "SELECT JSON_VALUE(doc, '$' NULL ON EMPTY NULL ON ERROR)", "SELECT JSON_VALUE ( `doc` , ? NULL ON EMPTY NULL ON ERROR )"},
		{"json value default literal", "SELECT JSON_VALUE(doc, '$' DEFAULT NULL ON EMPTY)", "SELECT JSON_VALUE ( `doc` , ? DEFAULT ? ON EMPTY )"},
		{"comment separated response", "SELECT JSON_VALUE(doc, '$' NULL /* a */ ON /* b */ ERROR)", "SELECT JSON_VALUE ( `doc` , ? NULL ON ERROR )"},
		{"response after scope exit", "SELECT * FROM JSON_TABLE(doc, '$' COLUMNS(x INT PATH '$' NULL ON EMPTY)) AS jt JOIN t ON t.x=NULL", "SELECT * FROM JSON_TABLE ( `doc` , ? FIELDS ( `x` INTEGER PATH ? NULL ON EMPTY ) ) AS `jt` JOIN `t` ON `t` . `x` = ?"},
		{"both table responses", "SELECT * FROM JSON_TABLE(doc, '$' COLUMNS(x INT PATH '$' NULL ON EMPTY NULL ON ERROR)) AS jt", "SELECT * FROM JSON_TABLE ( `doc` , ? FIELDS ( `x` INTEGER PATH ? NULL ON EMPTY NULL ON ERROR ) ) AS `jt`"},
		{"nested columns", "SELECT * FROM JSON_TABLE(doc, '$' COLUMNS(NESTED PATH '$' COLUMNS(x INT PATH '$' NULL ON ERROR))) AS jt", "SELECT * FROM JSON_TABLE ( `doc` , ? FIELDS ( NESTED PATH ? FIELDS ( `x` INTEGER PATH ? NULL ON ERROR ) ) ) AS `jt`"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Compute(tc.sql)
			require.NoError(t, err)
			require.Equal(t, tc.text, got.Text)
		})
	}
	// A JOIN inside the document expression is not a JSON response clause.
	for _, prefix := range []string{
		"SELECT * FROM JSON_TABLE((SELECT a.doc FROM a JOIN b JOIN c ON b.x=",
		"SELECT * FROM JSON_TABLE(doc, '$' COLUMNS(x INT PATH '$' NULL ON EMPTY)) AS jt; CREATE TABLE t (x TIMESTAMP DEFAULT ",
	} {
		suffix := " ON UPDATE CURRENT_TIMESTAMP)"
		if strings.HasPrefix(prefix, "SELECT * FROM JSON_TABLE((") {
			suffix = " ON a.x=b.x), '$' COLUMNS(x INT PATH '$' NULL ON ERROR)) AS jt"
		}
		a, err := Compute(prefix + "NULL" + suffix)
		require.NoError(t, err)
		b, err := Compute(prefix + "1" + suffix)
		require.NoError(t, err)
		require.Equal(t, b, a)
	}
	for _, sql := range []string{
		"SELECT JSON_VALUE(doc, '$' NULL ON /* unterminated",
		"SELECT JSON_VALUE(doc, '$' NULL ON ERROR) + 'unterminated",
	} {
		_, err := Compute(sql)
		require.Error(t, err, sql)
	}
}

func TestDigestCTASWithoutASLeavesDDLColumnContext(t *testing.T) {
	for _, tc := range []struct {
		name string
		null string
		one  string
		text string
	}{
		{
			name: "without AS",
			null: "CREATE TABLE t SELECT BINARY NULL",
			one:  "CREATE TABLE t SELECT BINARY 1",
			text: "CREATE TABLE `t` SELECT BINARY ?",
		},
		{
			name: "explicit column definitions",
			null: "CREATE TABLE t (a INT) SELECT BINARY NULL",
			one:  "CREATE TABLE t (a INT) SELECT BINARY 1",
			text: "CREATE TABLE `t` ( `a` INTEGER ) SELECT BINARY ?",
		},
		{
			name: "parenthesized select",
			null: "CREATE TABLE t (SELECT BINARY NULL)",
			one:  "CREATE TABLE t (SELECT BINARY 1)",
			text: "CREATE TABLE `t` ( SELECT BINARY ? )",
		},
	} {
		withNull, err := Compute(tc.null)
		require.NoError(t, err, tc.name)
		withValue, err := Compute(tc.one)
		require.NoError(t, err, tc.name)
		require.Equal(t, withValue, withNull, tc.name)
		require.Equal(t, tc.text, withNull.Text, tc.name)
	}

	for _, sql := range []string{
		"CREATE TABLE t (a BINARY NULL)",
		"CREATE TABLE t (a BINARY NOT NULL)",
	} {
		got, err := Compute(sql)
		require.NoError(t, err)
		require.Contains(t, got.Text, "NULL", sql)
	}

	defaultNull, err := Compute("CREATE TABLE t (a BINARY DEFAULT NULL)")
	require.NoError(t, err)
	defaultValue, err := Compute("CREATE TABLE t (a BINARY DEFAULT 1)")
	require.NoError(t, err)
	require.Equal(t, defaultValue, defaultNull)
}

func TestDigestColumnExpressionsNormalizeNullLiterals(t *testing.T) {
	for _, tc := range []struct{ null, one string }{
		{"CREATE TABLE t (a INT DEFAULT (BINARY NULL))", "CREATE TABLE t (a INT DEFAULT (BINARY 1))"},
		{"ALTER TABLE t ADD COLUMN a INT DEFAULT (BINARY NULL)", "ALTER TABLE t ADD COLUMN a INT DEFAULT (BINARY 1)"},
		{"CREATE TABLE t (a INT CHECK (BINARY NULL))", "CREATE TABLE t (a INT CHECK (BINARY 1))"},
		{"CREATE TABLE t (a INT DEFAULT BINARY NULL)", "CREATE TABLE t (a INT DEFAULT BINARY 1)"},
		{"CREATE TABLE t (a INT, CHECK (BINARY NULL IS NULL))", "CREATE TABLE t (a INT, CHECK (BINARY 1 IS NULL))"},
		{"CREATE TABLE t (CHECK (BINARY NULL IS NULL), a INT)", "CREATE TABLE t (CHECK (BINARY 1 IS NULL), a INT)"},
		{"CREATE TABLE t (a INT, CONSTRAINT c CHECK ((BINARY NULL IS NULL)))", "CREATE TABLE t (a INT, CONSTRAINT c CHECK ((BINARY 1 IS NULL)))"},
		{"CREATE TABLE t (CHECK ((BINARY NULL IS NULL) AND (BINARY NULL IS NULL)))", "CREATE TABLE t (CHECK ((BINARY 1 IS NULL) AND (BINARY 1 IS NULL)))"},
		{"CREATE TABLE t (a INT, CHECK (BINARY NULL IS NULL), CHECK (BINARY NULL IS NULL))", "CREATE TABLE t (a INT, CHECK (BINARY 1 IS NULL), CHECK (BINARY 1 IS NULL))"},
		{"ALTER TABLE t ADD CONSTRAINT c CHECK (BINARY NULL IS NULL)", "ALTER TABLE t ADD CONSTRAINT c CHECK (BINARY 1 IS NULL)"},
		{"ALTER TABLE t ADD CHECK (BINARY NULL IS NULL)", "ALTER TABLE t ADD CHECK (BINARY 1 IS NULL)"},
		{"ALTER TABLE t ADD CONSTRAINT c CHECK (BINARY NULL IS NULL), ADD CONSTRAINT d CHECK (BINARY NULL IS NULL)", "ALTER TABLE t ADD CONSTRAINT c CHECK (BINARY 1 IS NULL), ADD CONSTRAINT d CHECK (BINARY 1 IS NULL)"},
	} {
		withNull, err := Compute(tc.null)
		require.NoError(t, err, tc.null)
		withValue, err := Compute(tc.one)
		require.NoError(t, err, tc.one)
		require.Equal(t, withValue, withNull, tc.null)
	}
}

func TestDigestTableCheckExpressionDoesNotLeakColumnState(t *testing.T) {
	got, err := Compute("CREATE TABLE t (CHECK (BINARY NULL IS NULL), a BINARY NULL, b INT DEFAULT (NULL), c INT CHECK (NULL IS NULL))")
	require.NoError(t, err)
	require.Contains(t, got.Text, "CHECK ( BINARY ? IS NULL )")
	require.Contains(t, got.Text, "`a` BINARY NULL")
	require.Contains(t, got.Text, "`b` INTEGER DEFAULT (?)")
	require.Contains(t, got.Text, "`c` INTEGER CHECK ( ? IS NULL )")

	alter, err := Compute("ALTER TABLE t ADD CONSTRAINT c CHECK (BINARY NULL IS NULL), ADD COLUMN a BINARY NULL")
	require.NoError(t, err)
	require.Contains(t, alter.Text, "CHECK ( BINARY ? IS NULL )")
	require.Contains(t, alter.Text, "`a` BINARY NULL")
}

func TestDigestColumnAttributesResumeAfterExpressions(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		text string
	}{
		{"CREATE TABLE t (a INT DEFAULT 1 NOT NULL)", "CREATE TABLE `t` ( `a` INTEGER DEFAULT ? NOT NULL )"},
		{"CREATE TABLE t (a INT DEFAULT 1 NULL)", "CREATE TABLE `t` ( `a` INTEGER DEFAULT ? NULL )"},
		{"CREATE TABLE t (a INT DEFAULT NULL NULL)", "CREATE TABLE `t` ( `a` INTEGER DEFAULT ? NULL )"},
		{"CREATE TABLE t (a INT DEFAULT (1) NULL)", "CREATE TABLE `t` ( `a` INTEGER DEFAULT (?) NULL )"},
		{"CREATE TABLE t (a TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL)", "CREATE TABLE `t` ( `a` TIMESTAMP DEFAULT NOW NOT NULL )"},
		{"CREATE TABLE t (a DATE DEFAULT CURRENT_DATE NULL)", "CREATE TABLE `t` ( `a` DATE DEFAULT CURDATE NULL )"},
		{"CREATE TABLE t (a TIME DEFAULT CURRENT_TIME NOT NULL)", "CREATE TABLE `t` ( `a` TIME DEFAULT CURTIME NOT NULL )"},
		{"CREATE TABLE t (a BOOL DEFAULT TRUE NULL)", "CREATE TABLE `t` ( `a` BOOL DEFAULT TRUE NULL )"},
		{"CREATE TABLE t (a BOOL DEFAULT FALSE NOT NULL)", "CREATE TABLE `t` ( `a` BOOL DEFAULT FALSE NOT NULL )"},
		{"CREATE TABLE t (a BOOL DEFAULT (TRUE) NULL)", "CREATE TABLE `t` ( `a` BOOL DEFAULT ( TRUE ) NULL )"},
		{"ALTER TABLE t ADD COLUMN a BOOL DEFAULT FALSE NULL", "ALTER TABLE `t` ADD COLUMN `a` BOOL DEFAULT FALSE NULL"},
		{"CREATE TABLE t (a INT CHECK (a>0) NOT NULL)", "CREATE TABLE `t` ( `a` INTEGER CHECK ( `a` > ? ) NOT NULL )"},
		{"ALTER TABLE t ADD COLUMN a INT DEFAULT (1) NOT NULL", "ALTER TABLE `t` ADD COLUMN `a` INTEGER DEFAULT (?) NOT NULL"},
	} {
		got, err := Compute(tc.sql)
		require.NoError(t, err, tc.sql)
		require.Equal(t, tc.text, got.Text, tc.sql)
	}
}

func TestDigestRecognizesSupportedCharsetIntroducers(t *testing.T) {
	whitespace := []string{"", " ", "\t", "\r", "\n", "\v", "\f", "\v\f", "\r\n\t\f\v"}
	// Check each supported charset once for registry coverage. The complete
	// whitespace matrix is exercised below with utf8mb4; repeating it for every
	// charset only tests the same lexer boundary 5x without adding coverage.
	for _, charset := range []string{"latin1", "LATIN1", "utf8mb3", "utf8mb4", "binary"} {
		canonical, err := Compute("SELECT _" + charset + "'x'")
		require.NoError(t, err, charset)
		require.Equal(t, "SELECT (_charset) ?", canonical.Text, charset)
	}
	canonical, err := Compute("SELECT _utf8mb4'x'")
	require.NoError(t, err)
	for _, separator := range whitespace {
		got, err := Compute("SELECT _utf8mb4" + separator + "'x'")
		require.NoError(t, err, separator)
		require.Equal(t, canonical, got, separator)
	}

	unknown, err := Compute("SELECT _not_a_charset'x'")
	require.NoError(t, err)
	require.Equal(t, "SELECT `_not_a_charset` ?", unknown.Text)
	for _, separator := range []string{"\v", "\f", "/* comment */", "# comment\n", "-- comment\n"} {
		got, err := Compute("SELECT _not_a_charset" + separator + "'x'")
		require.NoError(t, err, separator)
		require.Equal(t, unknown, got, separator)
	}

	bare, err := Compute("SELECT _latin1")
	require.NoError(t, err)
	require.Equal(t, "SELECT `_latin1`", bare.Text)
	bareWithFormFeed, err := Compute("SELECT _latin1\f")
	require.NoError(t, err)
	require.Equal(t, bare, bareWithFormFeed)

	for _, separator := range []string{"/* comment */", "# comment\n", "-- comment\n", "/*!99999 */", "/*!80000 */"} {
		got, err := Compute("SELECT _utf8mb4" + separator + "'x'")
		require.NoError(t, err, separator)
		require.Equal(t, canonical, got, separator)
	}
	versionedCode := "SELECT _utf8mb4/*!80000 + */'x'"
	executedVersionedCode, err := Compute(versionedCode)
	require.NoError(t, err)
	require.NotEqual(t, canonical, executedVersionedCode)
	skippedVersionedCode, err := Compute(versionedCode, Options{MySQLVersionID: 79999})
	require.NoError(t, err)
	require.Equal(t, canonical, skippedVersionedCode)

	lineCommentAtEOF, err := Compute("SELECT _utf8mb4# comment 'x'")
	require.NoError(t, err)
	require.Equal(t, "SELECT `_utf8mb4`", lineCommentAtEOF.Text)
	_, err = Compute("SELECT _utf8mb4/*")
	require.Error(t, err)

	quoted, err := Compute("SELECT `_utf8mb4` 'x'")
	require.NoError(t, err)
	require.Equal(t, "SELECT `_utf8mb4` ?", quoted.Text)
	quotedANSI, err := Compute(`SELECT "_utf8mb4" 'x'`, Options{SQLMode: ModeANSIQuotes})
	require.NoError(t, err)
	require.Equal(t, quoted, quotedANSI)

	nonCommentDelimiter, err := Compute("SELECT _utf8mb4--'x'")
	require.NoError(t, err)
	require.NotEqual(t, canonical, nonCommentDelimiter)
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

	got, err = Compute("/", Options{MaxDigestLength: &maxLength})
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

func TestDigestOptimizerHintBackslashEscapes(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		mode SQLMode
	}{
		{name: "single quoted string", sql: "SELECT /*+ QB_NAME('a\\'b') */ 1"},
		{name: "double quoted string", sql: "SELECT /*+ QB_NAME(\"a\\\"b\") */ 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Compute(tc.sql, Options{SQLMode: tc.mode})
			require.NoError(t, err)
			plain, err := Compute("SELECT /*+ QB_NAME('ab') */ 1", Options{SQLMode: tc.mode})
			require.NoError(t, err)
			// Hint string contents are normalized as generic values, so changing
			// only the escaped spelling must not change the digest.
			require.Equal(t, plain, got)
		})
	}

	_, err := Compute("SELECT /*+ QB_NAME('a\\'b') */ 1", Options{SQLMode: ModeNoBackslashEscapes})
	require.Error(t, err)
}
