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

package mysql

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestDigestEffectiveColumnAttributes(t *testing.T) {
	for _, test := range []struct {
		sql    string
		reject bool
	}{
		{"CREATE TABLE t(a INT DEFAULT (@v) DEFAULT (0))", false},
		{"CREATE TABLE t(a INT DEFAULT ((SELECT 1)) DEFAULT (0))", false},
		{"CREATE TABLE t(a INT DEFAULT (0) DEFAULT ((SELECT 1)))", true},
		{"CREATE TABLE t(a INT DEFAULT (SELECT 1) DEFAULT (0))", true},
		{"CREATE TABLE t(a INT DEFAULT (0) DEFAULT (@v))", true},
		{"CREATE TABLE t(a INT DEFAULT (ABS()) DEFAULT (0))", false},
		{"CREATE TABLE t(a INT DEFAULT (DATE 'bad') DEFAULT (0))", false},
		{"CREATE TABLE t(a INT DEFAULT 1+1 DEFAULT 0)", true},
		{"CREATE TABLE t(a INT DEFAULT CURRENT_TIMESTAMP DEFAULT NULL)", false},
		{"CREATE TABLE t(a TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP)", false},
		{"CREATE TABLE t(a TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(3))", true},
		{"CREATE TABLE t(a INT NOT NULL AUTO_INCREMENT DEFAULT NULL)", false},
		{"CREATE TABLE t(a INT PRIMARY KEY AUTO_INCREMENT DEFAULT NULL)", false},
		{"CREATE TABLE t(a SERIAL DEFAULT NULL)", false},
		{"CREATE TABLE t(a FLOAT AUTO_INCREMENT)", true},
		{"CREATE TABLE t(a YEAR AUTO_INCREMENT)", true},
		{"CREATE TABLE t(a BIT AUTO_INCREMENT)", true},
		{"CREATE TABLE t(a BOOL AUTO_INCREMENT)", false},
		{"CREATE TABLE t(a INT1 NOT NULL AUTO_INCREMENT DEFAULT NULL)", false},
		{"CREATE TABLE t(a INT8 NOT NULL AUTO_INCREMENT DEFAULT NULL)", false},
		{"SELECT db.abs()", false},
		{"SELECT db.count(1,2)", false},
		{"CREATE TABLE t(a INT DEFAULT (db.serial(1)))", true},
		{"CREATE TABLE t(a INT DEFAULT (db.serial(1)) DEFAULT (0))", false},
		{"CREATE TABLE t(a INT AUTO_INCREMENT DEFAULT (NULL))", true},
		{"SELECT serial(1)", true},
		{"SELECT serial (1)", true},
		{"WITH c AS (SELECT serial(1)) SELECT 1", true},
		{"SELECT db.serial(1)", false},
		{"SELECT `serial`(1)", false},
		{"CREATE TABLE t(a BLOB COLLATE utf8mb4_bin)", true},
		{"CREATE TABLE t(a JSON COLLATE binary)", false},
		{"CREATE TABLE t(a BINARY(10) DEFAULT '' COLLATE utf8mb4_bin DEFAULT '')", true},
		{"CREATE TABLE t(a NCHAR(10) COLLATE utf8mb3_bin)", false},
		{"CREATE TABLE t(a VARCHAR(10) CHARSET latin1 COLLATE utf8mb4_bin)", true},
		{"CREATE TABLE t(a INT DEFAULT (0) COLLATE binary)", false},
		{"CREATE TABLE t(a INT DEFAULT (0 COLLATE binary))", true},
		{"CREATE TABLE t(a INT DEFAULT (0 COLLATE binary) DEFAULT (1))", true},
		{"WITH c AS (SELECT 'x' COLLATE binary) SELECT 1", true},
		{"SELECT 'x' COLLATE binary", true},
		{"SELECT 'x' COLLATE `binary`", false},
		{"CREATE DATABASE d COLLATE binary", false},
		{"ALTER TABLE t COLLATE binary", false},
	} {
		t.Run(test.sql, func(t *testing.T) {
			for range 3 {
				_, _, err := StatementDigest(t.Context(), test.sql, "")
				if test.reject {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrDigestParse), "%v", err)
				} else {
					require.NoError(t, err)
				}
				text, _, err := StatementDigest(t.Context(), "SELECT 1", "")
				require.NoError(t, err)
				require.Equal(t, "SELECT ?", text)
			}
		})
	}
	for _, sql := range []string{"SELECT serial(1)", "SELECT serial (1)", "SELECT 1 COLLATE binary", "CREATE TABLE t(a FLOAT AUTO_INCREMENT)"} {
		func() {
			stmts, err := Parse(t.Context(), sql, 0)
			defer func() {
				for _, stmt := range stmts {
					stmt.Free()
				}
			}()
			require.NoError(t, err)
			require.Len(t, stmts, 1)
		}()
	}
}

func TestOrdinaryQualifiedFunctionAdmission(t *testing.T) {
	stmts, err := Parse(t.Context(), "SELECT db.serial(1)", 0)
	defer func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	}()
	require.Error(t, err)
}

func TestDigestColumnTypeAttributeDomains(t *testing.T) {
	for _, test := range []struct {
		sql, mode string
		reject    bool
	}{
		{"CREATE TABLE t (a VARCHAR(10) DEFAULT CHARACTER SET utf8mb4)", "", true},
		{"CREATE TABLE t (a VARCHAR(10) CHARSET = utf8mb4)", "", true},
		{"CREATE TABLE t (a VARCHAR(10) NOT NULL CHARSET utf8mb4)", "", true},
		{"CREATE TABLE t (a VARCHAR(10) CHARSET utf8mb4 CHARSET latin1)", "", true},
		{"CREATE TABLE t (a INT CHARSET utf8mb4)", "", true},
		{"CREATE TABLE t (a BLOB CHARSET utf8mb4)", "", true},
		{"CREATE TABLE t (a NCHAR(10) CHARSET utf8mb4)", "", true},
		{"CREATE TABLE t (a NATIONAL CHAR(10) CHARSET utf8mb4)", "", true},
		{"CREATE TABLE t (a VARCHAR(10) CHARSET utf8mb4 NOT NULL)", "", false},
		{"CREATE TABLE t (a TEXT CHARSET utf8mb4 DEFAULT ('x'))", "", false},
		{"CREATE TABLE t (a ENUM('a') CHARSET utf8mb4)", "", false},
		{"CREATE TABLE t (a TEXT DEFAULT 'x')", "", true},
		{"CREATE TABLE t (a JSON DEFAULT 0)", "", true},
		{"CREATE TABLE t (a TEXT DEFAULT '')", "", false},
		{"CREATE TABLE t (a TEXT DEFAULT '')", "STRICT_TRANS_TABLES", true},
		{"CREATE TABLE t (a BLOB DEFAULT X'')", "", false},
		{"CREATE TABLE t (a BLOB DEFAULT B'')", "", false},
		{"CREATE TABLE t (a CHAR VARYING(10) CHARSET utf8mb4)", "", false},
		{"CREATE TABLE t (a CHAR VARYING)", "", true},
		{"CREATE TABLE t (a NATIONAL CHAR VARYING(10))", "", false},
		{"CREATE TABLE t (a NCHAR VARYING(10) CHARSET utf8mb4)", "", true},
		{"CREATE TABLE t (a TEXT NOT NULL DEFAULT NULL DEFAULT ('x'))", "", true},
		{"CREATE TABLE t (a INT AUTO_INCREMENT DEFAULT NULL DEFAULT (0))", "", true},
		{"CREATE TABLE t (a TEXT DEFAULT 'x' DEFAULT ('y') DEFAULT NULL)", "", false},
		{"CREATE TABLE t (a BLOB DEFAULT X'')", "STRICT_ALL_TABLES", true},
		{"CREATE TABLE t (a BLOB DEFAULT X'00')", "", true},
		{"CREATE TABLE t (a JSON DEFAULT '')", "TRADITIONAL", true},
		{"CREATE TABLE t (a JSON DEFAULT ('x'))", "STRICT_TRANS_TABLES", false},
		{"CREATE TABLE t (a GEOMETRY DEFAULT '')", "", true},
		{"CREATE TABLE t (a GEOMETRY DEFAULT NULL)", "", false},
		{"CREATE TABLE t (a GEOMETRY DEFAULT ('x'))", "", false},
		{"CREATE TABLE t (a TEXT DEFAULT 'x' DEFAULT NULL)", "STRICT_TRANS_TABLES", false},
		{"CREATE TABLE t (a TEXT DEFAULT NULL DEFAULT 'x')", "", true},
		{"CREATE TABLE t (a TEXT DEFAULT ('x') DEFAULT ('y'))", "", false},
		{"CREATE TABLE t (a TEXT DEFAULT NULL DEFAULT ('x'))", "", false},
		{"CREATE TABLE t (a VARCHAR(10) DEFAULT 'x' DEFAULT ('y'))", "", true},
		{"ALTER TABLE t ADD a TEXT DEFAULT 'x'", "", true},
		{"ALTER TABLE t MODIFY a TEXT DEFAULT ('x')", "", false},
		{"ALTER TABLE t CHANGE b a TEXT DEFAULT 'x'", "", true},
		{"ALTER TABLE t ALTER a SET DEFAULT 'x'", "", false},
		{"CREATE TABLE t (a INT) DEFAULT CHARSET = utf8mb4", "", false},
		{"SELECT CAST(1 AS CHAR CHARACTER SET utf8mb4)", "", false},
	} {
		t.Run(test.sql+"/"+test.mode, func(t *testing.T) {
			for range 3 {
				_, _, err := StatementDigest(t.Context(), test.sql, test.mode)
				if test.reject {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrDigestParse), "%v", err)
				} else {
					require.NoError(t, err)
				}
				text, digest, err := StatementDigest(t.Context(), "SELECT 1", "")
				require.NoError(t, err)
				require.Equal(t, "SELECT ?", text)
				require.Equal(t, "d1b44b0c19af710b5a679907e284acd2ddc285201794bc69a2389d77baedddae", digest)
			}
		})
	}
	// Ordinary MO owns its historical column attribute and default admission.
	for _, sql := range []string{
		"CREATE TABLE t (a VARCHAR(10) DEFAULT CHARACTER SET utf8mb4)",
		"CREATE TABLE t (a VARCHAR(10) CHARSET = utf8mb4)",
		"CREATE TABLE t (a VARCHAR(10) NOT NULL CHARSET utf8mb4)",
		"CREATE TABLE t (a INT CHARSET utf8mb4)",
		"CREATE TABLE t (a TEXT DEFAULT 'x')",
		"CREATE TABLE t (a JSON DEFAULT 0)",
	} {
		func() {
			stmts, err := Parse(t.Context(), sql, 0)
			defer func() {
				for _, stmt := range stmts {
					stmt.Free()
				}
			}()
			require.NoError(t, err)
			require.Len(t, stmts, 1)
		}()
	}
}

func TestDigestWindowArgumentDomains(t *testing.T) {
	for _, test := range []struct {
		sql, mode string
		reject    bool
	}{
		{"SELECT NTILE(1+1) OVER ()", "", true},
		{"WITH c AS (SELECT NTILE(1+1) OVER ()) SELECT 1", "", true},
		{"SELECT NTILE(0) OVER ()", "", false},
		{"SELECT NTILE(18446744073709551615) OVER ()", "", false},
		{"SELECT NTILE(18446744073709551616) OVER ()", "", true},
		{"SELECT NTILE(-1) OVER ()", "", true},
		{"SELECT NTILE((1)) OVER ()", "", true},
		{"SELECT NTILE(@v) OVER ()", "", false},
		{"SELECT NTILE(@@session.sql_mode) OVER ()", "", true},
		{"SELECT NTILE(?) OVER ()", "", true},
		{"SELECT NTILE(a) OVER ()", "", true},
		{"WITH c AS (SELECT NTILE(a) OVER ()) SELECT 1", "", false},
		{"WITH c AS (SELECT NTILE(a) OVER ()) SELECT * FROM c", "", true},
		{"WITH c AS (SELECT NTILE(a) OVER ()), d AS (WITH c AS (SELECT 1) SELECT * FROM c) SELECT * FROM d", "", false},
		{"SELECT LEAD(1,1+1) OVER ()", "", true},
		{"SELECT LAG(1,(1),2) OVER ()", "", true},
		{"SELECT LEAD(1+1,1,1+1) OVER ()", "", false},
		{"WITH c AS (SELECT LEAD(1,a) OVER ()) SELECT 1", "", false},
		{"WITH c AS (SELECT LEAD(1,a) OVER ()) SELECT * FROM c", "", true},
		{"SELECT NTH_VALUE(1,1+1) OVER ()", "", true},
		{"SELECT NTH_VALUE(1,(1+1)) OVER ()", "", false},
		{"SELECT NTH_VALUE(1,a) OVER ()", "", false},
		{"SELECT NTH_VALUE(1,-1) OVER ()", "", false},
		{"SELECT NTH_VALUE(1,NOT 1) OVER ()", "", true},
		{"SELECT NTH_VALUE(1,!1) OVER ()", "", false},
		{"SELECT FIRST_VALUE(1+1) IGNORE NULLS OVER ()", "", false},
		{"SELECT LAST_VALUE(NULL) IGNORE NULLS OVER ()", "", false},
		{"SELECT NTH_VALUE(1,1) FROM LAST IGNORE NULLS OVER ()", "", false},
		{"SELECT NTILE(1) IGNORE NULLS OVER ()", "", true},
		{"SELECT `NTILE`(1+1)", "", false},
		{"SELECT `NTILE`(1+1) OVER ()", "", true},
		{"SELECT NTH_VALUE(1,1||2) OVER ()", "", true},
		{"SELECT NTH_VALUE(1,1||2) OVER ()", "PIPES_AS_CONCAT", false},
	} {
		t.Run(test.sql+"/"+test.mode, func(t *testing.T) {
			for range 3 {
				_, _, err := StatementDigest(t.Context(), test.sql, test.mode)
				if test.reject {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrDigestParse), "%v", err)
				} else {
					require.NoError(t, err)
				}
				text, digest, err := StatementDigest(t.Context(), "SELECT 1", "")
				require.NoError(t, err)
				require.Equal(t, "SELECT ?", text)
				require.Equal(t, "d1b44b0c19af710b5a679907e284acd2ddc285201794bc69a2389d77baedddae", digest)
			}
		})
	}
	for _, test := range []struct {
		sql    string
		reject bool
	}{
		{"SELECT NTILE(1+1) OVER ()", false},
		{"SELECT LEAD(1,1+1) OVER ()", false},
		{"SELECT LAG(1,(1),2) OVER ()", false},
		{"SELECT NTH_VALUE(1,1+1) OVER ()", false},
		{"SELECT FIRST_VALUE(1) IGNORE NULLS OVER ()", true},
		{"SELECT NTH_VALUE(1,1) FROM LAST OVER ()", true},
	} {
		func() {
			stmts, err := Parse(t.Context(), test.sql, 0)
			defer func() {
				for _, stmt := range stmts {
					stmt.Free()
				}
			}()
			if test.reject {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Len(t, stmts, 1)
			}
		}()
	}
}

func TestDigestAggregateArgumentDomains(t *testing.T) {
	for _, test := range []struct {
		sql, mode string
		reject    bool
	}{
		{"SELECT COUNT(1)", "", false},
		{"SELECT COUNT(1,2)", "", true},
		{"SELECT COUNT(ALL 1,2)", "", true},
		{"SELECT COUNT(DISTINCT 1,2)", "", false},
		{"SELECT COUNT(DISTINCTROW 1,2)", "", false},
		{"SELECT BIT_AND(DISTINCTROW 1)", "", true},
		{"SELECT COUNT(*)", "", false},
		{"SELECT COUNT(ALL *)", "", false},
		{"SELECT COUNT(DISTINCT *)", "", true},
		{"SELECT COUNT(DISTINCT ALL 1)", "", true},
		{"SELECT BIT_AND(DISTINCT 1)", "", true},
		{"SELECT BIT_AND(ALL 1)", "", false},
		{"SELECT STDDEV_POP(DISTINCT 1)", "", true},
		{"SELECT STDDEV_POP(ALL 1)", "", false},
		{"SELECT GROUP_CONCAT(ALL 1)", "", true},
		{"SELECT GROUP_CONCAT(DISTINCT 1,2)", "", false},
		{"SELECT JSON_ARRAYAGG(DISTINCT 1)", "", true},
		{"SELECT JSON_ARRAYAGG(ALL 1)", "", false},
		{"SELECT JSON_OBJECTAGG(ALL 1,ALL 2)", "", false},
		{"SELECT JSON_OBJECTAGG(DISTINCT 1,2)", "", true},
		{"SELECT GROUPING(ALL 1)", "", true},
		{"SELECT GROUPING(1) OVER ()", "", true},
		{"SELECT SUM(DISTINCT ALL 1)", "", false},
		{"SELECT MIN(DISTINCT ALL 1)", "", false},
		{"WITH c AS (SELECT COUNT(1,2)) SELECT 1", "", true},
		{"WITH c AS (SELECT BIT_AND(DISTINCT 1)) SELECT 1", "", true},
		{"WITH c AS (SELECT GROUP_CONCAT(ALL 1)) SELECT 1", "", true},
		{"WITH c AS (SELECT ABS()) SELECT 1", "", false},
		{"SELECT COUNT (1,2)", "", false},
		{"SELECT COUNT (1,2)", "IGNORE_SPACE", true},
		{"SELECT `COUNT`(1,2)", "IGNORE_SPACE", false},
		{"SELECT JSON_ARRAYAGG (1,2)", "", false},
		{"SELECT JSON_ARRAYAGG (1,2)", "IGNORE_SPACE", true},
		{"SELECT JSON_ARRAYAGG/*gap*/(1,2)", "IGNORE_SPACE", false},
		{"SELECT COUNT(DISTINCT 1,2) OVER ()", "", false},
	} {
		t.Run(test.sql+"/"+test.mode, func(t *testing.T) {
			for range 3 {
				_, _, err := StatementDigest(t.Context(), test.sql, test.mode)
				if test.reject {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrDigestParse), "%v", err)
				} else {
					require.NoError(t, err)
				}
				text, digest, err := StatementDigest(t.Context(), "SELECT 1", "")
				require.NoError(t, err)
				require.Equal(t, "SELECT ?", text)
				require.Equal(t, "d1b44b0c19af710b5a679907e284acd2ddc285201794bc69a2389d77baedddae", digest)
			}
		})
	}
	for _, test := range []struct {
		sql    string
		reject bool
	}{
		{"SELECT COUNT(1,2)", false},
		{"SELECT BIT_AND(DISTINCT 1)", false},
		{"SELECT GROUP_CONCAT(ALL 1)", false},
		{"SELECT COUNT(ALL *)", true},
		{"SELECT SUM(DISTINCT ALL 1)", true},
		{"SELECT JSON_OBJECTAGG(1,ALL 2)", true},
	} {
		func() {
			stmts, err := Parse(t.Context(), test.sql, 0)
			defer func() {
				for _, stmt := range stmts {
					stmt.Free()
				}
			}()
			if test.reject {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Len(t, stmts, 1)
			}
		}()
	}
}

func TestDigestFunctionArgumentDomains(t *testing.T) {
	for _, test := range []struct {
		sql, mode string
		reject    bool
	}{
		{"SELECT CURDATE(1)", "", true},
		{"WITH c AS (SELECT CURDATE(1)) SELECT 1", "", true},
		{"SELECT SUBSTRING(1)", "", true},
		{"SELECT SUBSTRING(1,2,3)", "", false},
		{"SELECT CURDATE (1)", "", false},
		{"SELECT CURDATE (1)", "IGNORE_SPACE", true},
		{"SELECT `CURDATE`(1)", "IGNORE_SPACE", false},
		{"SELECT ABS()", "", true},
		{"SELECT CONCAT()", "", true},
		{"SELECT JSON_OBJECT(1)", "", true},
		{"SELECT JSON_ARRAY()", "", false},
		{"WITH c AS (SELECT ABS()) SELECT 1", "", false},
		{"WITH c AS (SELECT ABS()) SELECT * FROM c", "", true},
		{"WITH c AS (SELECT ABS()), d AS (SELECT * FROM c) SELECT 1", "", false},
		{"WITH c AS (SELECT ABS()), d AS (SELECT * FROM c) SELECT * FROM d", "", true},
		{"WITH c AS (SELECT CURRENT_ROLE(1)) SELECT 1", "", false},
		{"WITH c AS (SELECT CURRENT_ROLE(1)) SELECT * FROM c", "", true},
		{"WITH c AS (SELECT LOG()) SELECT 1", "", true},
		{"WITH c AS (SELECT `LOG`()) SELECT 1", "", false},
		{"SELECT NOW(ABS(1))", "", true},
		{"SELECT NOW(7)", "", false},
		{"SELECT NOW(2147483648)", "", true},
		{"SELECT CURTIME(2147483648)", "", true},
		{"SELECT LINESTRING()", "", true},
		{"SELECT LINESTRING(1)", "", false},
		{"SELECT GEOMETRYCOLLECTION()", "", false},
		{"SELECT JSON_VALUE(1,_utf8mb3'$.a')", "", false},
		{"SELECT JSON_VALUE(1,NULL)", "", true},
		{"SELECT JSON_VALUE(1,_utf8mb4 X'61')", "", true},
		{"SELECT _utf8mb4 X'73656C656374'", "", false},
		{"SELECT WEIGHT_STRING(1,1.0,1e0,0x01)", "", false},
		{"SELECT WEIGHT_STRING(1,ABS(1),1,1)", "", true},
		{"SELECT MAKE_SET()", "", false},
		{"SELECT POİNT()", "", false},
	} {
		t.Run(test.sql+"/"+test.mode, func(t *testing.T) {
			for range 3 {
				_, _, err := StatementDigest(t.Context(), test.sql, test.mode)
				if test.reject {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrDigestParse), "%v", err)
				} else {
					require.NoError(t, err)
				}
				text, digest, err := StatementDigest(t.Context(), "SELECT 1", "")
				require.NoError(t, err)
				require.Equal(t, "SELECT ?", text)
				require.Equal(t, "d1b44b0c19af710b5a679907e284acd2ddc285201794bc69a2389d77baedddae", digest)
			}
		})
	}
	// 普通 MO parser 的参数准入仍由原有 binder/function owner 处理。
	for _, sql := range []string{"SELECT CURDATE(1)", "SELECT SUBSTRING(1)", "SELECT ABS()", "SELECT CONCAT()"} {
		func() {
			stmts, err := Parse(t.Context(), sql, 0)
			defer func() {
				for _, stmt := range stmts {
					stmt.Free()
				}
			}()
			require.NoError(t, err)
			require.Len(t, stmts, 1)
		}()
	}
}

func TestDigestDateArithmeticArity(t *testing.T) {
	for _, name := range []string{"ADDDATE", "SUBDATE"} {
		for _, test := range []struct {
			args   string
			reject bool
		}{
			{"", true}, {"1", true}, {"1,2", false}, {"1,2,3", true},
			{"1,INTERVAL 1 DAY", false}, {"1,2,INTERVAL 1 DAY", true},
		} {
			sql := "SELECT " + name + "(" + test.args + ")"
			t.Run(sql, func(t *testing.T) {
				for range 3 {
					_, _, err := StatementDigest(t.Context(), sql, "")
					if test.reject {
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrDigestParse), "%v", err)
					} else {
						require.NoError(t, err)
					}
					text, _, err := StatementDigest(t.Context(), "SELECT 1", "")
					require.NoError(t, err)
					require.Equal(t, "SELECT ?", text)
				}
			})
		}
	}
	// The same spelling in a generic-call role is not native ADDDATE syntax.
	for _, sql := range []string{"SELECT ADDDATE (1)", "SELECT SUBDATE /*gap*/ (1)", "SELECT `ADDDATE`(1)"} {
		_, _, err := StatementDigest(t.Context(), sql, "")
		require.NoError(t, err)
	}
}

func TestOrdinaryParserDiagnosticOwnership(t *testing.T) {
	for _, test := range []struct{ sql, detail string }{
		{"SELECT MAKE_SET(CAST(5 AS INT32), 'a', 'b', 'c') AS make_set_int32;", "syntax error at line 1 column 31 near \" INT32), 'a', 'b', 'c') AS make_set_int32;\";"},
		{"create user if not exists user1 ,user2 ,user3 identified by '12345678';", "syntax error at line 1 column 33 near \" ,user2 ,user3 identified by '12345678';\";"},
	} {
		func() {
			stmts, err := Parse(t.Context(), test.sql, 0)
			defer func() {
				for _, stmt := range stmts {
					stmt.Free()
				}
			}()
			require.Error(t, err)
			require.Contains(t, err.Error(), test.detail)
		}()
	}
}

func TestDigestLiteralAndIntervalDomains(t *testing.T) {
	for _, test := range []struct {
		name, sql, mode string
		reject          bool
	}{
		{"match-introduced", "SELECT MATCH(a) AGAINST(_utf8mb4'a') FROM t", "", false},
		{"match-gap", "SELECT MATCH(a) AGAINST(_utf8mb4 /*gap*/ 'a' IN BOOLEAN MODE) FROM t", "", false},
		{"match-national", "SELECT MATCH(a) AGAINST(N'a') FROM t", "", false},
		{"match-empty", "SELECT MATCH(a) AGAINST(_utf8mb4'') FROM t", "", false},
		{"match-no-columns", "SELECT MATCH() AGAINST('a')", "", true},
		{"interval-not-value", "SELECT INTERVAL 1 DAY", "", true},
		{"interval-parenthesis", "SELECT (INTERVAL 1 DAY) + 1", "", true},
		{"interval-wrong-operator", "SELECT 1 * INTERVAL 1 DAY", "", true},
		{"interval-right", "SELECT 1 - INTERVAL 1 DAY", "", false},
		{"interval-left", "SELECT INTERVAL 1 DAY + 1", "", false},
		{"interval-prefix-unary", "SELECT -INTERVAL 1 DAY + 1", "", false},
		{"interval-nested-prefix", "SELECT INTERVAL 1 DAY + INTERVAL 2 DAY + 1", "", false},
		{"interval-function", "SELECT INTERVAL(1,2,3)", "", false},
		{"interval-native-date-add", "SELECT DATE_ADD(1,INTERVAL 1 DAY)", "", false},
		{"interval-date-add-scalar", "SELECT DATE_ADD(1,INTERVAL 1 DAY + 1)", "", true},
		{"interval-generic-function", "SELECT foo(1,INTERVAL 1 DAY)", "", true},
		{"interval-window", "SELECT SUM(1) OVER (ORDER BY a RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM t", "", false},
		{"date-invalid-calendar", "SELECT DATE'2020-02-30'", "", true},
		{"date-invalid-allowed", "SELECT DATE'2020-02-30'", "ALLOW_INVALID_DATES", false},
		{"date-leap-century", "SELECT DATE'1900-02-29'", "", true},
		{"date-leap-four-century", "SELECT DATE'2000-02-29'", "", false},
		{"date-zero", "SELECT DATE'0000-00-00'", "", false},
		{"date-year-zero-not-leap", "SELECT DATE'0000-02-29'", "", true},
		{"date-no-zero", "SELECT DATE'0000-00-00'", "NO_ZERO_DATE", true},
		{"date-no-zero-in-date", "SELECT DATE'2020-00-01'", "NO_ZERO_IN_DATE", true},
		{"date-not-datetime", "SELECT DATE'2020-01-01 00:00:00'", "", true},
		{"time-extended-hour", "SELECT TIME'25:00:00'", "", false},
		{"time-max", "SELECT TIME'838:59:59'", "", false},
		{"time-overflow", "SELECT TIME'839:00:00'", "", true},
		{"time-invalid-minute", "SELECT TIME'25:61:00'", "", true},
		{"timestamp-date-only", "SELECT TIMESTAMP'2020-01-01'", "", true},
		{"timestamp-extra-undelimited-digits", "SELECT TIMESTAMP'202001010000001'", "", true},
		{"time-native-unsigned-accumulator", "SELECT TIME'18446744073709551616:00:00'", "", false},
		{"timestamp-fraction-overflow", "SELECT TIMESTAMP'9999-12-31 23:59:59.9999999'", "", true},
		{"timestamp-offset", "SELECT TIMESTAMP'2020-01-01 01:02:03+14:00'", "", false},
		{"timestamp-invalid-offset", "SELECT TIMESTAMP'2020-01-01 01:02:03+14:01'", "", true},
		{"cast-not-literal", "SELECT CAST('bad' AS DATE)", "", false},
		{"date-function-not-literal", "SELECT DATE('bad')", "", false},
		{"unused-cte", "WITH c AS (SELECT DATE'bad') SELECT 1", "", false},
		{"used-cte", "WITH c AS (SELECT DATE'bad') SELECT * FROM c", "", true},
		{"explain-itemizes", "EXPLAIN SELECT DATE'bad'", "", true},
		{"derived-itemizes", "SELECT 1 FROM (SELECT DATE'bad') d", "", true},
		{"concat-precedence-control", "SELECT 'a' || 'b'", "PIPES_AS_CONCAT", false},
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
	for _, sql := range []string{"SELECT INTERVAL 1 DAY", "SELECT ABS(INTERVAL 1 DAY)", "SELECT DATE'bad'", "SELECT TIME'25:61:00'", "SELECT TIMESTAMP'2020-02-30 00:00:00'"} {
		func() {
			stmts, err := Parse(t.Context(), sql, 0)
			defer func() {
				for _, stmt := range stmts {
					stmt.Free()
				}
			}()
			require.NoError(t, err, "ordinary parser owns its old admission: %s", sql)
		}()
	}
}
