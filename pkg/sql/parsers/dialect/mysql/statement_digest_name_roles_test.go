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
