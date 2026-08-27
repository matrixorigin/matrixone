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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

// TestIssue27443BinaryPreparedDMLAndAggregate exercises the same COM_STMT_PREPARE
// and COM_STMT_EXECUTE path as database/sql clients.  Runtime specialization
// must preserve DML execution metadata and must not feed an integer literal to
// an aggregate executor that was bound to a floating-point parameter domain.
func TestIssue27443BinaryPreparedDMLAndAggregate(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)

		dbName := testutils.GetDatabaseName(t)
		execSQLRequire(t, ctx, db, "create database `"+dbName+"`")
		defer execSQLMaybe(t, ctx, db, "drop database if exists `"+dbName+"`")

		execSQLRequire(t, ctx, db, "create table `"+dbName+"`.src (id int primary key, tenant int, payload varchar(32), amount decimal(12,2))")
		execSQLRequire(t, ctx, db, "create table `"+dbName+"`.dst (id int primary key, tenant int, payload varchar(32), amount decimal(12,2))")
		execSQLRequire(t, ctx, db, "insert into `"+dbName+"`.src values (1, 7, 'new', 12.34), (2, 8, 'other', 56.78)")
		execSQLRequire(t, ctx, db, "insert into `"+dbName+"`.dst values (1, 7, 'old', 1.00)")

		replaceStmt, err := db.PrepareContext(ctx, "replace into `"+dbName+"`.dst (id, tenant, payload, amount) select id, tenant, payload, amount from `"+dbName+"`.src where tenant = ?")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, replaceStmt.Close())
		}()
		_, err = replaceStmt.ExecContext(ctx, int64(7))
		require.NoError(t, err)

		var payload string
		var amount string
		require.NoError(t, db.QueryRowContext(ctx, "select payload, amount from `"+dbName+"`.dst where id = 1").Scan(&payload, &amount))
		require.Equal(t, "new", payload)
		require.Equal(t, "12.34", amount)

		updateStmt, err := db.PrepareContext(ctx, "update `"+dbName+"`.dst set payload = ?, amount = ? where tenant = ? and id = ?")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, updateStmt.Close())
		}()
		_, err = updateStmt.ExecContext(ctx, "updated", "23.45", int64(7), int64(1))
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx, "select payload, amount from `"+dbName+"`.dst where id = 1").Scan(&payload, &amount))
		require.Equal(t, "updated", payload)
		require.Equal(t, "23.45", amount)

		execSQLRequire(t, ctx, db, "create table `"+dbName+"`.pp_dst (tenant int, id int, status int, amount decimal(12,2), dt datetime, tm time, bin varbinary(16), u bigint unsigned, primary key (tenant, id))")
		execSQLRequire(t, ctx, db, "insert into `"+dbName+"`.pp_dst values (1, 1, 999, 1.00, '2026-08-21 01:02:03', '01:02:03', x'6131', 7)")
		ppStmt, err := db.PrepareContext(ctx, "update `"+dbName+"`.pp_dst set status=?, amount=?, dt=?, tm=?, bin=?, u=? where tenant=? and id=?")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, ppStmt.Close())
		}()
		_, err = ppStmt.ExecContext(ctx, int64(200), "2.50", "2026-08-22 04:05:06", "04:05:06", []byte("b2"), uint64(42), int64(1), int64(1))
		require.NoError(t, err)
		var status int64
		var dt, tm string
		var bin []byte
		var unsigned uint64
		require.NoError(t, db.QueryRowContext(ctx, "select status, amount, dt, tm, bin, u from `"+dbName+"`.pp_dst where tenant=1 and id=1").Scan(&status, &amount, &dt, &tm, &bin, &unsigned))
		require.Equal(t, int64(200), status)
		require.Equal(t, "2.50", amount)
		require.Equal(t, "2026-08-22 04:05:06", dt)
		require.Equal(t, "04:05:06", tm)
		require.Equal(t, []byte("b2"), bin)
		require.Equal(t, uint64(42), unsigned)

		// DML predicates still need execute-time numeric binding. The first
		// parameter is a binary integer while the second is a text value that
		// represents the same number; a cached TEXT/TEXT comparison would be
		// false and leave the row unchanged.
		execSQLRequire(t, ctx, db, "create table `"+dbName+"`.predicate_dst (id int primary key, status int)")
		execSQLRequire(t, ctx, db, "insert into `"+dbName+"`.predicate_dst values (1, 0)")
		predicateStmt, err := db.PrepareContext(ctx, "update `"+dbName+"`.predicate_dst set status = 1 where ? = ?")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, predicateStmt.Close())
		}()
		_, err = predicateStmt.ExecContext(ctx, int64(1), "1.00")
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx, "select status from `"+dbName+"`.predicate_dst where id=1").Scan(&status))
		require.Equal(t, int64(1), status)

		// A write parameter and a numeric predicate can share the same DML
		// plan. Runtime specialization must rebind the predicate while retaining
		// the target assignment cast for the positional write expression.
		execSQLRequire(t, ctx, db, "update `"+dbName+"`.predicate_dst set status = 0 where id = 1")
		mixedPredicateStmt, err := db.PrepareContext(ctx, "update `"+dbName+"`.predicate_dst set status = ? where ? = ?")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, mixedPredicateStmt.Close())
		}()
		_, err = mixedPredicateStmt.ExecContext(ctx, int64(2), int64(1), "1.00")
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx, "select status from `"+dbName+"`.predicate_dst where id=1").Scan(&status))
		require.Equal(t, int64(2), status)

		// The assigned value itself may contain a domain-sensitive expression.
		// Preserve the assignment cast, but specialize the nested comparison so
		// the binary integer and text numeric values compare numerically.
		execSQLRequire(t, ctx, db, "update `"+dbName+"`.predicate_dst set status = 0 where id = 1")
		writeExpressionStmt, err := db.PrepareContext(ctx, "update `"+dbName+"`.predicate_dst set status = (? = ?) where id = 1")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, writeExpressionStmt.Close())
		}()
		_, err = writeExpressionStmt.ExecContext(ctx, int64(1), "1.00")
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx, "select status from `"+dbName+"`.predicate_dst where id=1").Scan(&status))
		require.Equal(t, int64(1), status)

		// CASE is another write-root expression whose predicate must be
		// specialized independently of the positional assignment wrapper.
		execSQLRequire(t, ctx, db, "update `"+dbName+"`.predicate_dst set status = 0 where id = 1")
		caseWriteStmt, err := db.PrepareContext(ctx, "update `"+dbName+"`.predicate_dst set status = case when ? = ? then 3 else 4 end where id = 1")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, caseWriteStmt.Close())
		}()
		_, err = caseWriteStmt.ExecContext(ctx, int64(1), "1.00")
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx, "select status from `"+dbName+"`.predicate_dst where id=1").Scan(&status))
		require.Equal(t, int64(3), status)

		// A scalar subquery can hide a domain-sensitive expression in a derived
		// table. Only the final assignment cast is positional; the nested
		// comparison must still be rebound for the binary parameter domains.
		execSQLRequire(t, ctx, db, "update `"+dbName+"`.predicate_dst set status = 0 where id = 1")
		nestedWriteStmt, err := db.PrepareContext(ctx, "update `"+dbName+"`.predicate_dst set status = (select d.v from (select ? = ? as v) d) where id = 1")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, nestedWriteStmt.Close())
		}()
		_, err = nestedWriteStmt.ExecContext(ctx, int64(1), "1.00")
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx, "select status from `"+dbName+"`.predicate_dst where id=1").Scan(&status))
		require.Equal(t, int64(1), status)

		// MySQL evaluates text/numeric comparisons in the DOUBLE domain.
		// Exponent-form text must not be forced through a bounded DECIMAL128
		// representation before the comparison is evaluated.
		execSQLRequire(t, ctx, db, "update `"+dbName+"`.predicate_dst set status = 0 where id = 1")
		numericTextPredicateStmt, err := db.PrepareContext(ctx, "update `"+dbName+"`.predicate_dst set status = 7 where id = 1 and ? = ?")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, numericTextPredicateStmt.Close())
		}()
		_, err = numericTextPredicateStmt.ExecContext(ctx, float64(1e100), "1e100")
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx, "select status from `"+dbName+"`.predicate_dst where id=1").Scan(&status))
		require.Equal(t, int64(7), status)
		execSQLRequire(t, ctx, db, "update `"+dbName+"`.predicate_dst set status = 0 where id = 1")
		_, err = numericTextPredicateStmt.ExecContext(ctx, float64(1e-100), "1e-100")
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx, "select status from `"+dbName+"`.predicate_dst where id=1").Scan(&status))
		require.Equal(t, int64(7), status)
		// The DOUBLE rule also covers integer-looking text. These adjacent INT64
		// values compare equal after conversion, matching MySQL's documented
		// string/numeric comparison behavior.
		execSQLRequire(t, ctx, db, "update `"+dbName+"`.predicate_dst set status = 0 where id = 1")
		_, err = numericTextPredicateStmt.ExecContext(ctx, int64(9223372036854775806), "9223372036854775807")
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx, "select status from `"+dbName+"`.predicate_dst where id=1").Scan(&status))
		require.Equal(t, int64(7), status)

		// Numeric comparison must use the engine's ordinary MySQL string-to-
		// DOUBLE conversion for the entire text domain, not only complete finite
		// numeric literals. This preserves numeric prefixes, non-numeric-to-zero,
		// range handling, and the corresponding truncation diagnostics.
		for _, test := range []struct {
			name        string
			numeric     any
			text        string
			wantWarning bool
		}{
			{name: "non numeric becomes zero", numeric: int64(0), text: "foo", wantWarning: true},
			{name: "numeric prefix", numeric: int64(1), text: "1abc", wantWarning: true},
			{name: "non mysql whitespace is not skipped", numeric: int64(0), text: "\u00a01", wantWarning: true},
			{name: "overflow follows double range", numeric: float64(1.7976931348623157e308), text: "1e309"},
		} {
			t.Run(test.name, func(t *testing.T) {
				execSQLRequire(t, ctx, db, "update `"+dbName+"`.predicate_dst set status = 0 where id = 1")
				_, err = numericTextPredicateStmt.ExecContext(ctx, test.numeric, test.text)
				require.NoError(t, err)
				if test.wantWarning {
					var level, message string
					var code uint16
					require.NoError(t, db.QueryRowContext(ctx, "show warnings").Scan(&level, &code, &message))
					require.Equal(t, "Warning", level)
					require.Equal(t, uint16(1292), code)
					require.Contains(t, message, "Truncated incorrect DOUBLE value")
				}
				require.NoError(t, db.QueryRowContext(ctx, "select status from `"+dbName+"`.predicate_dst where id=1").Scan(&status))
				require.Equal(t, int64(7), status)
			})
		}

		// Indexed numeric columns must use the same text-to-DOUBLE conversion
		// as marker-only comparisons. The cached prepare-time integer cast
		// rejects numeric prefixes and non-numeric text before MySQL can apply
		// its prefix/zero rules.
		execSQLRequire(t, ctx, db, "update `"+dbName+"`.predicate_dst set status = 0 where id = 1")
		indexedTextPredicateStmt, err := db.PrepareContext(ctx,
			"update `"+dbName+"`.predicate_dst set status = 16 where id = ?")
		require.NoError(t, err)
		var indexedLevel, indexedMessage string
		var indexedCode uint16
		defer func() {
			require.NoError(t, indexedTextPredicateStmt.Close())
		}()
		_, err = indexedTextPredicateStmt.ExecContext(ctx, "1abc")
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx, "show warnings").Scan(&indexedLevel, &indexedCode, &indexedMessage))
		require.Equal(t, "Warning", indexedLevel)
		require.Equal(t, uint16(1292), indexedCode)
		require.Contains(t, indexedMessage, "Truncated incorrect DOUBLE value")
		require.NoError(t, db.QueryRowContext(ctx,
			"select status from `"+dbName+"`.predicate_dst where id=1").Scan(&status))
		require.Equal(t, int64(16), status)

		execSQLRequire(t, ctx, db, "update `"+dbName+"`.predicate_dst set status = 0 where id = 1")
		nonNumericIndexedStmt, err := db.PrepareContext(ctx,
			"update `"+dbName+"`.predicate_dst set status = 17 where id = ?")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, nonNumericIndexedStmt.Close())
		}()
		_, err = nonNumericIndexedStmt.ExecContext(ctx, "foo")
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx, "show warnings").Scan(&indexedLevel, &indexedCode, &indexedMessage))
		require.Equal(t, "Warning", indexedLevel)
		require.Equal(t, uint16(1292), indexedCode)
		require.NoError(t, db.QueryRowContext(ctx,
			"select status from `"+dbName+"`.predicate_dst where id=1").Scan(&status))
		require.Equal(t, int64(0), status)
		execSQLRequire(t, ctx, db, "update `"+dbName+"`.predicate_dst set status = 0 where id = 1")
		_, err = indexedTextPredicateStmt.ExecContext(ctx, "0.9")
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx,
			"select status from `"+dbName+"`.predicate_dst where id=1").Scan(&status))
		require.Equal(t, int64(0), status)

		// NULL keeps SQL NULL comparison semantics and must not fall back to the
		// prepare-time strict integer cast. The row remains untouched.
		execSQLRequire(t, ctx, db, "update `"+dbName+"`.predicate_dst set status = 0 where id = 1")
		_, err = nonNumericIndexedStmt.ExecContext(ctx, nil)
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx,
			"select status from `"+dbName+"`.predicate_dst where id=1").Scan(&status))
		require.Equal(t, int64(0), status)

		// Range-overflow text values use the common DOUBLE domain and must be a
		// no-match, not an invalid integer-cast error.
		execSQLRequire(t, ctx, db, "update `"+dbName+"`.predicate_dst set status = 0 where id = 1")
		overflowIndexedStmt, err := db.PrepareContext(ctx,
			"update `"+dbName+"`.predicate_dst set status = 18 where id = ?")
		require.NoError(t, err)
		defer func() { require.NoError(t, overflowIndexedStmt.Close()) }()
		_, err = overflowIndexedStmt.ExecContext(ctx, "1e309")
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx,
			"select status from `"+dbName+"`.predicate_dst where id=1").Scan(&status))
		require.Equal(t, int64(0), status)
		execSQLRequire(t, ctx, db, "update `"+dbName+"`.predicate_dst set status = 0 where id = 1")
		_, err = overflowIndexedStmt.ExecContext(ctx, "2147483648")
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx,
			"select status from `"+dbName+"`.predicate_dst where id=1").Scan(&status))
		require.Equal(t, int64(0), status)

		// Text values above DOUBLE's exact-integer range must keep the filter in
		// the common DOUBLE domain. Narrowing the converted value back to BIGINT
		// would silently select only 9007199254740992 instead of both adjacent
		// values, which compare equal after MySQL's string/numeric conversion.
		execSQLRequire(t, ctx, db, "create table `"+dbName+"`.bigint_precision (id bigint primary key, status int)")
		execSQLRequire(t, ctx, db, "insert into `"+dbName+"`.bigint_precision values (9007199254740992, 0), (9007199254740993, 0)")
		bigintPrecisionStmt, err := db.PrepareContext(ctx,
			"update `"+dbName+"`.bigint_precision set status = 24 where id = ?")
		require.NoError(t, err)
		defer func() { require.NoError(t, bigintPrecisionStmt.Close()) }()
		_, err = bigintPrecisionStmt.ExecContext(ctx, "9007199254740993")
		require.NoError(t, err)
		var matched int64
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from `"+dbName+"`.bigint_precision where status = 24").Scan(&matched))
		require.Equal(t, int64(2), matched)

		// A fractional text prefix can round to an integral DOUBLE at the edge
		// of DOUBLE's exact-integer range. It must stay in the common DOUBLE
		// comparison domain; narrowing it back to BIGINT would select only the
		// lower adjacent key instead of both keys that MySQL compares equal.
		execSQLRequire(t, ctx, db, "update `"+dbName+"`.bigint_precision set status = 0")
		_, err = bigintPrecisionStmt.ExecContext(ctx, "9007199254740992.5")
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from `"+dbName+"`.bigint_precision where status = 24").Scan(&matched))
		require.Equal(t, int64(2), matched)

		// DECIMAL/text comparison has the same common DOUBLE domain. Casting the
		// text through DOUBLE and then back to DECIMAL changes this value to
		// 9007199254740992 and incorrectly produces no match.
		execSQLRequire(t, ctx, db, "create table `"+dbName+"`.decimal_precision (id decimal(38,0) primary key, status int)")
		execSQLRequire(t, ctx, db, "insert into `"+dbName+"`.decimal_precision values (9007199254740993, 0)")
		decimalPrecisionStmt, err := db.PrepareContext(ctx,
			"update `"+dbName+"`.decimal_precision set status = 25 where id = ?")
		require.NoError(t, err)
		defer func() { require.NoError(t, decimalPrecisionStmt.Close()) }()
		_, err = decimalPrecisionStmt.ExecContext(ctx, "9007199254740993")
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx,
			"select status from `"+dbName+"`.decimal_precision where id = 9007199254740993").Scan(&status))
		require.Equal(t, int64(25), status)

		// Multi-operand numeric predicates must use the same conversion while
		// retaining the indexed column side. This covers the index rewrite paths
		// for IN and BETWEEN in addition to direct equality.
		for _, test := range []struct {
			name       string
			where      string
			value      any
			status     int64
			wantUpdate bool
			warning    bool
		}{
			{name: "in prefix", where: "id in (?)", value: "1abc", status: 20, wantUpdate: true, warning: true},
			{name: "between prefix", where: "id between ? and 1", value: "1abc", status: 21, wantUpdate: true, warning: true},
			{name: "in null", where: "id in (?)", value: nil, status: 22},
			{name: "between overflow", where: "id between ? and 2", value: "1e309", status: 23},
		} {
			t.Run(test.name, func(t *testing.T) {
				execSQLRequire(t, ctx, db, "update `"+dbName+"`.predicate_dst set status = 0 where id = 1")
				stmt, err := db.PrepareContext(ctx,
					"update `"+dbName+"`.predicate_dst set status = ? where "+test.where)
				require.NoError(t, err)
				defer func() { require.NoError(t, stmt.Close()) }()
				_, err = stmt.ExecContext(ctx, test.status, test.value)
				require.NoError(t, err)
				if test.warning {
					var level, message string
					var code uint16
					require.NoError(t, db.QueryRowContext(ctx, "show warnings").Scan(&level, &code, &message))
					require.Equal(t, "Warning", level)
					require.Equal(t, uint16(1292), code)
					require.Contains(t, message, "Truncated incorrect DOUBLE value")
				}
				require.NoError(t, db.QueryRowContext(ctx,
					"select status from `"+dbName+"`.predicate_dst where id=1").Scan(&status))
				if test.wantUpdate {
					require.Equal(t, test.status, status)
				} else {
					require.Equal(t, int64(0), status)
				}
			})
		}

		// The same protocol conversion is required for a non-indexed numeric
		// column; no index rewrite should be needed for the heap path. Exercise
		// the same prefix, missing-prefix, NULL, and range-overflow matrix.
		execSQLRequire(t, ctx, db, "create table `"+dbName+"`.heap_predicate (id int, status int)")
		execSQLRequire(t, ctx, db, "insert into `"+dbName+"`.heap_predicate values (1, 0), (2, 0)")
		for _, test := range []struct {
			name        string
			value       any
			status      int64
			wantStatus  int64
			wantWarning bool
		}{
			{name: "prefix", value: "1abc", status: 19, wantStatus: 19, wantWarning: true},
			{name: "missing prefix", value: "foo", status: 20, wantStatus: 0, wantWarning: true},
			{name: "null", value: nil, status: 21, wantStatus: 0},
			{name: "range overflow", value: "1e309", status: 22, wantStatus: 0},
		} {
			t.Run("heap "+test.name, func(t *testing.T) {
				stmt, err := db.PrepareContext(ctx,
					fmt.Sprintf("update `%s`.heap_predicate set status = %d where id = ?", dbName, test.status))
				require.NoError(t, err)
				defer func() { require.NoError(t, stmt.Close()) }()
				_, err = stmt.ExecContext(ctx, test.value)
				require.NoError(t, err)
				if test.wantWarning {
					var level, message string
					var code uint16
					require.NoError(t, db.QueryRowContext(ctx, "show warnings").Scan(&level, &code, &message))
					require.Equal(t, "Warning", level)
					require.Equal(t, uint16(1292), code)
					require.Contains(t, message, "Truncated incorrect DOUBLE value")
				}
				require.NoError(t, db.QueryRowContext(ctx,
					"select status from `"+dbName+"`.heap_predicate where id=1").Scan(&status))
				require.Equal(t, test.wantStatus, status)
				execSQLRequire(t, ctx, db, "update `"+dbName+"`.heap_predicate set status = 0 where id = 1")
			})
		}

		// A numeric literal can make the prepare-time binder wrap the marker in
		// an implicit integer cast. The execute-time text domain must still own
		// MySQL numeric comparison conversion instead of executing that stale
		// provisional cast.
		execSQLRequire(t, ctx, db, "update `"+dbName+"`.predicate_dst set status = 0 where id = 1")
		implicitNumericPredicateStmt, err := db.PrepareContext(ctx,
			"update `"+dbName+"`.predicate_dst set status = 8 where id = 1 and 0 = ?")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, implicitNumericPredicateStmt.Close())
		}()
		_, err = implicitNumericPredicateStmt.ExecContext(ctx, "foo")
		require.NoError(t, err)
		var level, message string
		var code uint16
		require.NoError(t, db.QueryRowContext(ctx, "show warnings").Scan(&level, &code, &message))
		require.Equal(t, "Warning", level)
		require.Equal(t, uint16(1292), code)
		require.Contains(t, message, "Truncated incorrect DOUBLE value")
		require.NoError(t, db.QueryRowContext(ctx, "select status from `"+dbName+"`.predicate_dst where id=1").Scan(&status))
		require.Equal(t, int64(8), status)

		// The numeric comparison domain must reach text markers through nested
		// expressions and the multi-operand comparison families as well.
		for _, test := range []struct {
			name   string
			where  string
			status int64
		}{
			{name: "nested abs", where: "0 = abs(?)", status: 9},
			{name: "nested arithmetic", where: "0 = (? + 0)", status: 10},
			{name: "in list", where: "0 in (?, 2)", status: 11},
			{name: "between", where: "? between 0 and 0", status: 12},
			{name: "not in list", where: "1 not in (?, 2)", status: 13},
			{name: "not between", where: "? not between 1 and 2", status: 14},
		} {
			t.Run(test.name, func(t *testing.T) {
				execSQLRequire(t, ctx, db, "update `"+dbName+"`.predicate_dst set status = 0 where id = 1")
				stmt, err := db.PrepareContext(ctx,
					fmt.Sprintf("update `%s`.predicate_dst set status = %d where id = 1 and %s", dbName, test.status, test.where))
				require.NoError(t, err)
				defer func() {
					require.NoError(t, stmt.Close())
				}()
				_, err = stmt.ExecContext(ctx, "foo")
				require.NoError(t, err)
				var level, message string
				var code uint16
				require.NoError(t, db.QueryRowContext(ctx, "show warnings").Scan(&level, &code, &message))
				require.Equal(t, "Warning", level)
				require.Equal(t, uint16(1292), code)
				require.Contains(t, message, "Truncated incorrect DOUBLE value")
				require.NoError(t, db.QueryRowContext(ctx,
					"select status from `"+dbName+"`.predicate_dst where id=1").Scan(&status))
				require.Equal(t, test.status, status)
			})
		}

		t.Run("string function keeps text input", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "update `"+dbName+"`.predicate_dst set status = 0 where id = 1")
			stmt, err := db.PrepareContext(ctx,
				"update `"+dbName+"`.predicate_dst set status = 15 where id = 1 and 3 = length(?)")
			require.NoError(t, err)
			defer func() {
				require.NoError(t, stmt.Close())
			}()
			_, err = stmt.ExecContext(ctx, "foo")
			require.NoError(t, err)
			require.NoError(t, db.QueryRowContext(ctx,
				"select status from `"+dbName+"`.predicate_dst where id=1").Scan(&status))
			require.Equal(t, int64(15), status)
		})

		sumStmt, err := db.PrepareContext(ctx, "select sum(?)")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, sumStmt.Close())
		}()
		var sum int64
		require.NoError(t, sumStmt.QueryRowContext(ctx, int64(7)).Scan(&sum))
		require.Equal(t, int64(7), sum)

		windowStmt, err := db.PrepareContext(ctx, "select sum(?) over () from `"+dbName+"`.src")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, windowStmt.Close())
		}()
		require.NoError(t, windowStmt.QueryRowContext(ctx, int64(7)).Scan(&sum))
		require.Equal(t, int64(14), sum)

		mixedStmt, err := db.PrepareContext(ctx, "select ?, ? = ?")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, mixedStmt.Close())
		}()
		var directValue, equalValue int64
		require.NoError(t, mixedStmt.QueryRowContext(ctx, int64(7), "same", "same").Scan(
			&directValue, &equalValue))
		require.Equal(t, int64(7), directValue)
		require.Equal(t, int64(1), equalValue)

		for _, name := range []string{"max_by", "max_by_non_null"} {
			stmt, err := db.PrepareContext(ctx, "select "+name+"(?, 1, 1)")
			require.NoError(t, err)
			defer func() {
				require.NoError(t, stmt.Close())
			}()
			var value int64
			require.NoError(t, stmt.QueryRowContext(ctx, int64(7)).Scan(&value))
			require.Equal(t, int64(7), value)
		}
	})
}
