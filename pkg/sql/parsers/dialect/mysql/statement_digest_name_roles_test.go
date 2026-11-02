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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestDigestNameRolesAndReuse(t *testing.T) {
	ctx := context.Background()
	long := "`" + strings.Repeat("a", 65) + "`"
	for range 3 {
		for _, sql := range []string{
			"CREATE TABLE t (" + long + " INT)",
			"ALTER TABLE t ADD " + long + " INT",
			"ALTER TABLE t MODIFY " + long + " INT",
			"ALTER TABLE t CHANGE a " + long + " INT",
			"CREATE VIEW v (" + long + ") AS SELECT 1",
			"ALTER VIEW v (" + long + ") AS SELECT 1",
			"CREATE VIEW v AS WITH c AS (SELECT 1 AS " + long + ") SELECT * FROM c",
			"SHOW TABLES FROM " + long,
			"SHOW TABLE STATUS FROM " + long,
			"SHOW TRIGGERS FROM " + long,
			"SHOW EVENTS FROM " + long,
		} {
			_, _, err := StatementDigest(ctx, sql, "")
			require.Error(t, err, sql)
			// These restrictions belong to digest contextualization, not the
			// ordinary execution parser's catalog/binder error boundary.
			statements, err := Parse(ctx, sql, 0)
			require.NoError(t, err, sql)
			for _, statement := range statements {
				statement.Free()
			}
		}
		for _, sql := range []string{
			"ALTER TABLE t CHANGE " + long + " a INT",
			"ALTER TABLE t RENAME COLUMN a TO " + long,
			"ALTER TABLE t DROP " + long,
			"SELECT 1 AS " + long,
			"CREATE VIEW v AS WITH c AS (SELECT 1 AS " + long + ") SELECT 1",
			"CREATE VIEW v AS WITH c (" + long + ") AS (SELECT 1) SELECT * FROM c",
			"SHOW OPEN TABLES FROM " + long,
			"SHOW CREATE DATABASE " + long,
			"SHOW TABLES", "SHOW TABLE STATUS", "SHOW TRIGGERS", "SHOW EVENTS",
			"CREATE VIEW v AS SELECT 1", "ALTER VIEW v AS SELECT 1",
			"CREATE TABLE t (`` INT)",
			"CREATE VIEW v AS SELECT 1 AS 'a\x00" + strings.Repeat("b", 65) + "'",
			"CREATE TABLE t (a DOUBLE PRECISION, b DOUBLE PRECISION(10,2), c NUMERIC(0,0))",
		} {
			_, _, err := StatementDigest(ctx, sql, "")
			require.NoError(t, err, sql)
		}
		for _, sql := range []string{
			"CREATE VIEW v (``) AS SELECT 1", "CREATE VIEW v AS SELECT 1 AS ''",
			"CREATE VIEW v AS SELECT 1 ''", "ALTER VIEW v AS SELECT 1 AS ``",
			"SHOW TABLES FROM ``", "SHOW COLUMNS FROM t FROM ``", "SHOW INDEX FROM t FROM ``",
			"CREATE TABLE t (t.a INT)", "SELECT `a\x00b`",
			"CREATE VIEW v AS SELECT 1 AS '\x00'", "CREATE VIEW v AS SELECT 1 AS 'a \x00b'",
			"CREATE TABLE t (a DOUBLE PRECISION(1))",
		} {
			_, _, err := StatementDigest(ctx, sql, "")
			require.Error(t, err, sql)
		}
	}
}

func TestDigestReservedTypeNameRoles(t *testing.T) {
	ctx := context.Background()
	for _, word := range []string{"integer", "int", "decimal", "dec", "float4", "float8", "middleint", "varcharacter"} {
		for _, sql := range []string{"SELECT " + word + " FROM t", "SELECT 1 AS " + word, "CREATE TABLE t (" + word + " INT)"} {
			_, _, err := StatementDigest(ctx, sql, "")
			require.Error(t, err, sql)
		}
		for _, sql := range []string{"SELECT `" + word + "` FROM t", "SELECT t." + word + " FROM t", "SELECT " + word + ".a FROM t"} {
			_, _, err := StatementDigest(ctx, sql, "")
			require.NoError(t, err, sql)
		}
	}
	for _, sql := range []string{
		"CREATE TABLE t (a INTEGER, b DEC(0), c FLOAT4, d FLOAT8, e MIDDLEINT, f VARCHARACTER(0))",
		"SELECT CAST(1 AS SIGNED)", "SELECT CAST(1 AS DECIMAL(10,2))", "SELECT CHAR(1), BINARY(1)",
		"SELECT date, time, timestamp, text, bit, boolean, signed FROM t", "SELECT 1 FROM DUAL",
	} {
		_, _, err := StatementDigest(ctx, sql, "")
		require.NoError(t, err, sql)
	}
	statements, err := Parse(ctx, "SELECT integer FROM t", 0)
	require.NoError(t, err)
	for _, statement := range statements {
		statement.Free()
	}
}

// The oracle fixture checks exact MySQL text/hash/error. This test additionally
// proves ordinary-parser ownership and token/grammar reuse after rejection.
func TestDigestGrammarAdmissionAndReuse(t *testing.T) {
	for _, test := range []struct {
		name, sql, mode string
		reject          bool
	}{
		{"string-alias", "SELECT 1 AS '😀'", "", true},
		{"implicit-alias", "SELECT 1 '😀'", "", true},
		{"alias-after-nul", "SELECT 1 AS 'a\x00😀'", "", true},
		{"invalid-utf8-alias", "SELECT 1 AS '\xff'", "", true},
		{"bmp-alias", "SELECT 1 AS '名'", "", false},
		{"empty-alias", "SELECT 1 AS ''", "", false},
		{"quoted-name", "SELECT 1 AS `😀`", "", false},
		{"double-string-alias", "SELECT 1 AS \"😀\"", "", true},
		{"ansi-name", "SELECT 1 AS \"😀\"", "ANSI_QUOTES", false},
		{"string-value", "SELECT '😀'", "", false},
		{"introduced-alias", "SELECT 1 AS _utf8mb4'a'", "", true},
		{"introduced-implicit-alias", "SELECT 1 _utf8mb4'a'", "", true},
		{"introduced-value", "SELECT _utf8mb4'a' AS 'b'", "", false},
		{"national-alias", "SELECT 1 AS N'a'", "", true},
		{"national-value", "SELECT N'a' AS 'b'", "", false},
		{"float-binary-boundary", "CREATE TABLE t (a FLOAT(53))", "", false},
		{"float-binary-overflow", "CREATE TABLE t (a FLOAT(54))", "", true},
		{"float-decimal-boundary", "CREATE TABLE t (a FLOAT(255,30))", "", false},
		{"real-boundary", "CREATE TABLE t (a REAL(255,30))", "", false},
		{"real-width-overflow", "CREATE TABLE t (a REAL(256,30))", "", true},
		{"real-scale-overflow", "CREATE TABLE t (a REAL(255,31))", "", true},
		{"real-float-boundary", "CREATE TABLE t (a REAL(255,30))", "REAL_AS_FLOAT", false},
		{"real-float-overflow", "CREATE TABLE t (a REAL(256,30))", "REAL_AS_FLOAT", true},
		{"show-absent", "SHOW TABLE STATUS", "", false},
		{"show-from-only", "SHOW TABLE STATUS FROM", "", true},
		{"show-name-only", "SHOW TABLE STATUS db", "", true},
		{"show-paired", "SHOW TABLE STATUS FROM db", "", false},
		{"trigger-in-only", "SHOW TRIGGERS IN", "", true},
		{"event-name-only", "SHOW EVENTS db", "", true},
		{"event-paired", "SHOW EVENTS IN db", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			for range 3 {
				_, _, err := StatementDigest(t.Context(), test.sql, test.mode)
				if test.reject {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrDigestParse), "%v", err)
				} else {
					require.NoError(t, err)
				}
				func() {
					statements, err := ParseWithSQLMode(t.Context(), test.sql, 0, test.mode)
					defer func() {
						for _, statement := range statements {
							statement.Free()
						}
					}()
					if test.name == "national-alias" || test.name == "national-value" {
						// Ordinary MO parsing does not admit these national-string
						// forms; digest compatibility must not change that boundary.
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
				}()
				text, hash, err := StatementDigest(t.Context(), "SELECT 1", "")
				require.NoError(t, err)
				require.Equal(t, "SELECT ?", text)
				require.Equal(t, "d1b44b0c19af710b5a679907e284acd2ddc285201794bc69a2389d77baedddae", hash)
			}
		})
	}
}

func TestDigestRoleBoundariesAndReuse(t *testing.T) {
	for _, test := range []struct {
		name, sql, mode string
		reject          bool
	}{
		{"derived-string-alias", "SELECT * FROM (SELECT 1) AS 'a'", "", true},
		{"table-string-alias", "SELECT * FROM t 'a'", "", true},
		{"engine-string-not-alias", "CREATE TABLE t (a INT) ENGINE='InnoDB'", "", false},
		{"engine-double-string", "CREATE TABLE t (a INT) ENGINE=\"InnoDB\"", "", false},
		{"derived-name", "SELECT * FROM (SELECT 1) AS `a`", "", false},
		{"ansi-name", "SELECT * FROM (SELECT 1) AS \"a\"", "ANSI_QUOTES", false},
		{"utf8-hex-invalid", "SELECT _utf8mb4 X'FF'", "", true},
		{"utf8-hex-valid", "SELECT _utf8mb4 X'F09F9880'", "", false},
		{"utf8-hex-comment-invalid", "SELECT _utf8mb4 /*gap*/ X'FF'", "", true},
		{"utf8-hex-comment-valid", "SELECT _utf8mb4 /*gap*/ X'61'", "", false},
		{"utf8-hex-executable-comment", "SELECT /*!80411 _utf8mb4 X'FF' */", "", true},
		{"binary-hex", "SELECT _binary X'FF'", "", false},
		{"utf8mb3-supplementary", "SELECT _utf8mb3 X'F09F9880'", "", true},
		{"utf8mb3-bmp", "SELECT _utf8mb3 X'E5908D'", "", false},
		{"decimal-scale", "CREATE TABLE t (a DECIMAL(31,31))", "", true},
		{"decimal-boundary", "CREATE TABLE t (a DECIMAL(65,30))", "", false},
		{"bit-zero", "CREATE TABLE t (a BIT(0))", "", true},
		{"bit-width", "CREATE TABLE t (a BIT(65))", "", true},
		{"bit-boundary", "CREATE TABLE t (a BIT(64), b BIT)", "", false},
		{"char-width", "CREATE TABLE t (a CHAR(256))", "", true},
		{"char-boundary", "CREATE TABLE t (a CHAR(255), b CHAR(0))", "", false},
		{"character-synonym", "CREATE TABLE t (a CHARACTER(255), b NCHAR(255), c NATIONAL CHAR(255))", "", false},
		{"nchar-not-function", "SELECT NCHAR(1)", "", true},
		{"national-not-function", "SELECT national(1)", "", true},
		{"national-type-not-function", "SELECT NATIONAL CHAR(1)", "", true},
		{"national-name", "SELECT national FROM t", "", false},
		{"cast-float-pair", "SELECT CAST(1 AS FLOAT(10,2))", "", true},
		{"convert-float-pair", "SELECT CONVERT(1, FLOAT(10,2))", "", true},
		{"cast-float-single", "SELECT CAST(1 AS FLOAT(53))", "", false},
		{"ddl-float-pair", "CREATE TABLE t (a FLOAT(10,2))", "", false},
		{"cast-char-long", "SELECT CAST(1 AS CHAR(256))", "", false},
		{"cast-numeric", "SELECT CAST(1 AS NUMERIC(10,2))", "", true},
		{"cast-year-length", "SELECT CONVERT(1, YEAR(4))", "", true},
		{"convert-time-zero", "SELECT CONVERT(1, TIME(0))", "", false},
		{"convert-time-boundary", "SELECT CONVERT(1, TIME(6))", "", false},
		{"convert-time-overflow", "SELECT CONVERT(1, TIME(7))", "", true},
		{"convert-datetime-zero", "SELECT CONVERT(1, DATETIME(0))", "", false},
		{"convert-datetime-boundary", "SELECT CONVERT(1, DATETIME(6))", "", false},
		{"convert-datetime-overflow", "SELECT CONVERT(1, DATETIME(7))", "", true},
		{"show-both", "SHOW TABLES LIKE '%' WHERE 1", "", true},
		{"show-no-filter-target", "SHOW ENGINES WHERE 1", "", true},
		{"show-like-expression", "SHOW TABLES WHERE 'a' LIKE '%'", "", false},
		{"show-subquery", "SHOW TABLES WHERE EXISTS (SELECT 1 WHERE 1)", "", false},
		{"show-like-only", "SHOW TABLES LIKE '%'", "", false},
		{"show-engines", "SHOW ENGINES", "", false},
		{"show-local", "SHOW LOCAL STATUS WHERE 1", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			for range 3 {
				_, _, err := StatementDigest(t.Context(), test.sql, test.mode)
				if test.reject {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrDigestParse), "%v", err)
				} else {
					require.NoError(t, err)
				}
				text, hash, err := StatementDigest(t.Context(), "SELECT 1", "")
				require.NoError(t, err)
				require.Equal(t, "SELECT ?", text)
				require.Equal(t, "d1b44b0c19af710b5a679907e284acd2ddc285201794bc69a2389d77baedddae", hash)
			}
		})
	}
	// These are digest-only gates, not an ordinary parser policy change.
	for _, sql := range []string{
		"SELECT * FROM (SELECT 1) AS 'a'", "SELECT _utf8mb4 X'FF'",
		"CREATE TABLE t (a DECIMAL(31,31))", "CREATE TABLE t (a BIT(65))",
		"CREATE TABLE t (a CHAR(256))", "SELECT CAST(1 AS FLOAT(10,2))",
		"SHOW TABLES LIKE '%' WHERE 1", "SHOW ENGINES WHERE 1",
	} {
		func() {
			statements, err := Parse(t.Context(), sql, 0)
			defer func() {
				for _, statement := range statements {
					statement.Free()
				}
			}()
			require.NoError(t, err, sql)
		}()
	}
}

func TestDigestCastAndTextDomains(t *testing.T) {
	for _, test := range []struct {
		name, sql string
		reject    bool
	}{
		{"spatial-cast", "SELECT CAST(NULL AS POINT)", false},
		{"spatial-convert", "SELECT CONVERT(1, GEOMETRYCOLLECTION)", false},
		{"spatial-alias-cast", "SELECT CAST(NULL AS GEOMCOLLECTION)", false},
		{"spatial-alias-convert", "SELECT CONVERT(1, GEOMCOLLECTION)", false},
		{"spatial-alias-function", "SELECT geomcollection(POINT(1,2))", false},
		{"unknown-type-not-alias", "CREATE TABLE t (a unknown_type)", true},
		{"generic-geometry-not-target", "SELECT CAST(1 AS GEOMETRY)", true},
		{"mo-spatial-not-mysql", "CREATE TABLE t (a GEOMETRY32)", true},
		{"integer-limit", "CREATE TABLE t (a INT(255))", false},
		{"integer-overflow", "CREATE TABLE t (a INT(256))", true},
		{"integer-zero", "CREATE TABLE t (a INT(0))", false},
		{"bool-no-width", "CREATE TABLE t (a BOOL(1))", true},
		{"bit-no-modifiers", "CREATE TABLE t (a BIT UNSIGNED)", true},
		{"year-width", "CREATE TABLE t (a YEAR(4) UNSIGNED)", false},
		{"year-wrong-width", "CREATE TABLE t (a YEAR(0))", true},
		{"cast-signed-modifier", "SELECT CAST(1 AS FLOAT SIGNED)", true},
		{"cast-unsigned-modifier", "SELECT CAST(1 AS DECIMAL(10,2) UNSIGNED)", true},
		{"cast-zerofill-modifier", "SELECT CAST(1 AS DOUBLE ZEROFILL)", true},
		{"ddl-modifiers", "CREATE TABLE t (a FLOAT SIGNED, b DECIMAL(10,2) UNSIGNED ZEROFILL)", false},
		{"cast-signed-target", "SELECT CAST(1 AS SIGNED INTEGER)", false},
		{"cast-unsigned-target", "SELECT CONVERT(1, UNSIGNED)", false},
		{"engine-introducer", "CREATE TABLE t (a INT) ENGINE=_utf8mb4'a'", true},
		{"engine-text", "CREATE TABLE t (a INT) ENGINE='a'", false},
		{"comment-introducer", "CREATE TABLE t (a INT COMMENT _utf8mb4'a')", true},
		{"comment-executable-introducer", "CREATE TABLE t (a INT COMMENT /*!80411 _utf8mb4'a' */)", true},
		{"comment-text", "CREATE TABLE t (a INT COMMENT 'a')", false},
		{"expression-introducer", "SELECT _utf8mb4'SELECT'", false},
		{"expression-national", "SELECT N'a'", false},
		{"show-number", "SHOW TABLES LIKE 1", true},
		{"show-function", "SHOW TABLES LIKE CONCAT('a','b')", true},
		{"show-subquery", "SHOW TABLES LIKE (SELECT 'a')", true},
		{"show-parenthesized-text", "SHOW TABLES LIKE ('a')", true},
		{"show-introduced-text", "SHOW TABLES LIKE _utf8mb4'a'", true},
		{"show-text", "SHOW TABLES LIKE 'a'", false},
		{"show-where-like", "SHOW TABLES WHERE 'a' LIKE _utf8mb4'a'", false},
		{"show-where-nested-like", "SHOW TABLES WHERE EXISTS (SELECT 1 WHERE 'a' LIKE 'a')", false},
		{"user-comment", "CREATE USER u COMMENT 'a'", false},
		{"user-comment-introducer", "CREATE USER u COMMENT _utf8mb4'a'", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			for range 3 {
				_, _, err := StatementDigest(t.Context(), test.sql, "")
				if test.reject {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrDigestParse), "%v", err)
				} else {
					require.NoError(t, err)
				}
				text, hash, err := StatementDigest(t.Context(), "SELECT 1", "")
				require.NoError(t, err)
				require.Equal(t, "SELECT ?", text)
				require.Equal(t, "d1b44b0c19af710b5a679907e284acd2ddc285201794bc69a2389d77baedddae", hash)
			}
		})
	}
	for _, test := range []struct {
		sql    string
		reject bool
	}{
		{"CREATE TABLE t (a INT(256))", false},
		{"SELECT CAST(1 AS FLOAT SIGNED)", false},
		{"CREATE TABLE t (a INT) ENGINE=_utf8mb4'a'", false},
		{"SHOW TABLES LIKE 1", false},
		{"SELECT CONVERT(1, POINT)", true},
		{"SELECT CAST(NULL AS GEOMCOLLECTION)", true},
		{"CREATE TABLE t (a unknown_type)", true},
		{"CREATE USER u COMMENT 'a'", true},
		{"CREATE TABLE t (a YEAR UNSIGNED)", true},
	} {
		func() {
			statements, err := Parse(t.Context(), test.sql, 0)
			defer func() {
				for _, stmt := range statements {
					stmt.Free()
				}
			}()
			if test.reject {
				require.Error(t, err, "ordinary admission must remain unchanged: %s", test.sql)
			} else {
				require.NoError(t, err, test.sql)
			}
		}()
	}
}

// Test lexical provenance and SHOW scope at their owner, including resetting the
// same lexer after EOF/WHERE and after semicolons. Public syntax/hash are checked
// independently by the frozen MySQL fixture, not inferred from these token IDs.
func TestDigestTextTokenAndShowScopeReuse(t *testing.T) {
	lexer := &Lexer{}
	for _, test := range []struct {
		sql                        string
		digest                     bool
		showLike, like, introduced int
	}{
		{"SHOW TABLES LIKE 'a'", true, 1, 0, 0},
		{"SHOW TABLES WHERE 'a' LIKE 'a'", true, 0, 1, 0},
		{"SHOW TABLES WHERE EXISTS (SELECT 1 WHERE 'a' LIKE 'a')", true, 0, 1, 0},
		{"SELECT 'a' LIKE 'a'", true, 0, 1, 0},
		{"SHOW TABLES WHERE 1; SHOW TABLES LIKE 'a'", true, 1, 0, 0},
		{"SHOW TABLES /*!80411 LIKE 'a' */", true, 1, 0, 0},
		{"SELECT _utf8mb4'SELECT'", true, 0, 0, 1},
		{"SELECT /*!80411 _utf8mb4'SELECT' */", true, 0, 0, 1},
		{"SELECT N'a'", true, 0, 0, 1},
		{"SHOW TABLES LIKE 1", false, 0, 1, 0},
		{"SELECT _utf8mb4'SELECT'", false, 0, 0, 0},
	} {
		func() {
			scanner := NewScanner(dialect.MYSQL, test.sql)
			defer PutScanner(scanner)
			scanner.digestMode = test.digest
			lexer.setScanner(scanner, 0, 0)
			lexer.digest = nil
			if test.digest {
				lexer.digest = &statementDigestSyntax{nulls: make(map[int]struct{})}
			}
			require.False(t, lexer.digestShow)
			require.False(t, lexer.digestShowWhere)
			counts := make(map[int]int)
			for i := 0; ; i++ {
				require.Less(t, i, 100)
				var value yySymType
				token := lexer.Lex(&value)
				require.NotEqual(t, LEX_ERROR, token, test.sql)
				if token == 0 {
					break
				}
				counts[token]++
				if token == DIGEST_INTRODUCED_STRING {
					require.False(t, scanner.digestReservedWord, "literal contents are not a keyword")
				}
			}
			require.Equal(t, test.showLike, counts[DIGEST_SHOW_LIKE], test.sql)
			require.Equal(t, test.like, counts[LIKE], test.sql)
			require.Equal(t, test.introduced, counts[DIGEST_INTRODUCED_STRING], test.sql)
		}()
	}
}

func TestDigestHintCompactionOwnsBytes(t *testing.T) {
	for _, test := range []struct {
		input string
		mode  string
		value string
	}{
		{"`a😀b`", "", "a?b"},
		{"`a😀b```", "", "a😀b`"},
		{"```a😀b`", "", "`a😀b"},
		{"`a``😀b`", "", "a`😀b"},
		{"`a名b```", "", "a名b`"},
		{"\"a😀b\"\"\"", "ANSI_QUOTES", "a😀b\""},
		{"`a😀b```", "NO_BACKSLASH_ESCAPES", "a😀b`"},
	} {
		t.Run(test.input+test.mode, func(t *testing.T) {
			scanner := NewScannerWithSQLMode(dialect.MYSQL, test.input, ParseSQLModeFlags(test.mode))
			defer PutScanner(scanner)
			scanner.digestMode = true
			kind, value := scanDigestHint(scanner, 0)
			require.Equal(t, QUOTE_ID, kind)
			require.Equal(t, test.value, value)
			require.Equal(t, len(test.input), scanner.Pos)
		})
	}
	text, hash, err := StatementDigest(context.Background(), "SELECT /*+ QB_NAME(`a😀b```) */ 1", "")
	require.NoError(t, err)
	require.Equal(t, "SELECT /*+ QB_NAME ( ) */ ?", text)
	require.Equal(t, "c3ab319ece7b992ec51f33f4a9862fbd14f0ed0c316a1a21ad66e7b755dc0f9d", hash)
}
