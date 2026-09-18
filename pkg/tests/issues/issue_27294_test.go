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
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// withIssue27294PinnedConnection owns the pool checkout for the callback's scope.
// Install Close before the callback can exit through require.FailNow.
func withIssue27294PinnedConnection(t *testing.T, ctx context.Context, db *sql.DB, callback func(*sql.Conn)) error {
	t.Helper()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close pinned connection: %v", err)
		}
	}()

	callback(conn)
	return nil
}

func TestIssue27294PinnedConnectionReleasesOnGoexit(t *testing.T) {
	const regressionTimeout = 3 * time.Second
	const cleanupSQL = "drop database if exists issue_27294_numeric_db"

	type goexitResult struct {
		queryErr    error
		checkoutErr error
		inUse       int
	}
	tests := []struct {
		name      string
		failAtUse bool
	}{
		{name: "mode lookup failure"},
		{name: "USE failure after mode read", failAtUse: true},
	}

	for i := range tests {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			db.SetMaxOpenConns(1)
			dbClosed := false
			defer func() {
				if !dbClosed {
					_ = db.Close()
				}
			}()

			var wantRestoreErr error
			var gotRestoreErr error
			if tc.failAtUse {
				wantRestoreErr = errors.New("server-side sql_mode restore failed")
				mock.ExpectQuery("^select @@sql_mode$").
					WillReturnRows(sqlmock.NewRows([]string{"@@sql_mode"}).AddRow("STRICT_TRANS_TABLES"))
				mock.ExpectExec("^use issue_27294_numeric_db$").
					WillReturnError(errors.New("server-side USE failed"))
				mock.ExpectExec("^set session sql_mode = 'STRICT_TRANS_TABLES'$").
					WillReturnError(wantRestoreErr)
			} else {
				mock.ExpectQuery("^select @@sql_mode$").
					WillReturnError(errors.New("server-side sql_mode lookup failed"))
			}
			mock.ExpectExec("^drop database if exists issue_27294_numeric_db$").
				WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectClose()

			ctx, cancel := context.WithTimeout(context.Background(), regressionTimeout)
			resultCh := make(chan goexitResult, 1)
			continueToGoexit := make(chan struct{})
			done := make(chan struct{})
			defer func() {
				cancel()
				doneCtx, cancelDone := context.WithTimeout(context.Background(), regressionTimeout)
				defer cancelDone()
				select {
				case <-done:
				case <-doneCtx.Done():
					t.Errorf("pinned-connection defers did not finish before %s", doneCtx.Err())
				}
			}()
			go func() {
				defer close(done)
				err := withIssue27294PinnedConnection(t, ctx, db, func(conn *sql.Conn) {
					var queryErr error
					var originalSQLMode string
					queryErr = conn.QueryRowContext(ctx, "select @@sql_mode").Scan(&originalSQLMode)
					if tc.failAtUse && queryErr == nil {
						defer func() {
							cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), regressionTimeout)
							defer cancelCleanup()
							_, gotRestoreErr = conn.ExecContext(cleanupCtx, fmt.Sprintf(
								"set session sql_mode = '%s'", strings.ReplaceAll(originalSQLMode, "'", "''")))
						}()
						_, queryErr = conn.ExecContext(ctx, "use issue_27294_numeric_db")
					}
					resultCh <- goexitResult{queryErr: queryErr, inUse: db.Stats().InUse}
					select {
					case <-continueToGoexit:
					case <-ctx.Done():
					}
					runtime.Goexit()
				})
				if err != nil {
					resultCh <- goexitResult{checkoutErr: err}
				}
			}()

			resultCtx, cancelResult := context.WithTimeout(context.Background(), regressionTimeout)
			defer cancelResult()
			var result goexitResult
			select {
			case result = <-resultCh:
			case <-resultCtx.Done():
				t.Fatalf("pinned callback did not report its query result: %s", resultCtx.Err())
			}
			require.NoError(t, result.checkoutErr)
			require.Error(t, result.queryErr)
			if tc.failAtUse {
				require.Contains(t, result.queryErr.Error(), "server-side USE failed")
			} else {
				require.Contains(t, result.queryErr.Error(), "server-side sql_mode lookup failed")
			}
			require.Equal(t, 1, result.inUse, "the checked-out connection must be in-use before callback exit")
			close(continueToGoexit)

			doneCtx, cancelDone := context.WithTimeout(context.Background(), regressionTimeout)
			select {
			case <-done:
			case <-doneCtx.Done():
				t.Fatalf("pinned-connection defers did not finish before %s", doneCtx.Err())
			}
			cancelDone()
			if tc.failAtUse {
				require.ErrorIs(t, gotRestoreErr, wantRestoreErr)
			}

			cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), regressionTimeout)
			_, err = db.ExecContext(cleanupCtx, cleanupSQL)
			cancelCleanup()
			require.NoError(t, err, "database-level cleanup must reuse the sole pool connection")
			require.Zero(t, db.Stats().InUse)
			err = db.Close()
			dbClosed = true
			require.NoError(t, err)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestIssue27294PreparedNumericOverloads exercises the COM_STMT_EXECUTE path.
// The Go driver uses the binary protocol when interpolateParams is disabled;
// string arguments cover clients that bind a numeric value as VAR_STRING.
func TestIssue27294PreparedNumericOverloads(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()
		// Keep all session-level setup and prepared statements on one COM_STMT
		// connection. In particular, USE and sql_mode must remain paired with
		// the statement being exercised below.
		db.SetMaxOpenConns(1)
		_, err = db.ExecContext(ctx, "drop database if exists issue_27294_numeric_db")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "create database issue_27294_numeric_db")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, err := db.ExecContext(cleanupCtx, "drop database if exists issue_27294_numeric_db"); err != nil {
				t.Errorf("drop database issue_27294_numeric_db: %v", err)
			}
		}()
		_, err = db.ExecContext(ctx, "use issue_27294_numeric_db")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "drop table if exists issue_27294_numeric_src")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "create table issue_27294_numeric_src (v bigint)")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, err := db.ExecContext(cleanupCtx, "drop table if exists issue_27294_numeric_src"); err != nil {
				t.Errorf("drop table issue_27294_numeric_src: %v", err)
			}
		}()
		_, err = db.ExecContext(ctx, "insert into issue_27294_numeric_src values (-9007199254740993)")
		require.NoError(t, err)

		sleep, err := db.PrepareContext(ctx, "select sleep(?)")
		require.NoError(t, err)
		defer sleep.Close()
		// Reuse one server-side statement across integer, fractional, and textual
		// bindings.  The cached plan must keep the deferred DOUBLE domain for every
		// execution instead of retaining the first parameter's integer overload.
		for _, value := range []any{int64(0), float64(0.01), "0.02", int64(0)} {
			var result int
			require.NoError(t, sleep.QueryRowContext(ctx, value).Scan(&result))
			require.Zero(t, result)
		}

		// ROUND/TRUNCATE keep the first argument's deferred numeric source, but
		// their precision argument has a separate integer contract. Exercise the
		// real COM_STMT_EXECUTE path with malformed string precision values and
		// then change domains repeatedly on the same cached statement. A broad
		// DOUBLE rebinding of the second marker either accepts these values or
		// leaves the prior execution's type in the cache.
		type preparedMathResult struct {
			value        float64
			valid        bool
			databaseType string
		}
		queryPreparedMath := func(stmt *sql.Stmt, value, precision any) (preparedMathResult, error) {
			rows, queryErr := stmt.QueryContext(ctx, value, precision)
			if queryErr != nil {
				return preparedMathResult{}, queryErr
			}
			defer rows.Close()
			columnTypes, err := rows.ColumnTypes()
			if err != nil {
				return preparedMathResult{}, err
			}
			if len(columnTypes) != 1 {
				return preparedMathResult{}, fmt.Errorf("expected one result column, got %d", len(columnTypes))
			}
			if !rows.Next() {
				if err := rows.Err(); err != nil {
					return preparedMathResult{}, err
				}
				return preparedMathResult{}, fmt.Errorf("prepared math query returned no rows")
			}
			var valueResult sql.NullFloat64
			if err := rows.Scan(&valueResult); err != nil {
				return preparedMathResult{}, err
			}
			if err := rows.Err(); err != nil {
				return preparedMathResult{}, err
			}
			return preparedMathResult{
				value: valueResult.Float64, valid: valueResult.Valid,
				databaseType: strings.ToUpper(columnTypes[0].DatabaseTypeName()),
			}, nil
		}

		for _, mathName := range []string{"round", "truncate"} {
			mathStmt, err := db.PrepareContext(ctx, "select "+mathName+"(?, ?)")
			require.NoError(t, err)
			func() {
				defer func() { require.NoError(t, mathStmt.Close()) }()
				for _, precision := range []string{"0.5tail", "1.5tail", "-0.5tail", "abc", ""} {
					_, queryErr := queryPreparedMath(mathStmt, "1.5", precision)
					require.Error(t, queryErr, "%s precision=%q must retain integer-cast errors", mathName, precision)
					require.Contains(t, queryErr.Error(), "bad value "+precision,
						"%s precision=%q must report the original invalid marker", mathName, precision)
				}

				// A string value with an actual integer precision must succeed and
				// expose the exact DECIMAL result domain selected for the numeric
				// text value (rather than changing the precision marker to DOUBLE).
				result, queryErr := queryPreparedMath(mathStmt, "1.5", int64(0))
				require.NoError(t, queryErr)
				require.True(t, result.valid)
				require.Contains(t, result.databaseType, "DECIMAL")
				want := float64(2)
				if mathName == "truncate" {
					want = 1
				}
				require.Equal(t, want, result.value)

				// A native integer value must still select the integer overload
				// after the string execution, proving the two markers are not
				// coupled in the cached prepared plan.
				result, queryErr = queryPreparedMath(mathStmt, int64(15), int64(0))
				require.NoError(t, queryErr)
				require.True(t, result.valid)
				require.Contains(t, result.databaseType, "INT")
				require.Equal(t, float64(15), result.value)

				// Return to a malformed precision after a successful integer
				// execution, then exercise NULL and a final string execution. This
				// catches stale overload/type state across error and NULL paths.
				_, queryErr = queryPreparedMath(mathStmt, "1.5", "1.5tail")
				require.Error(t, queryErr)
				require.Contains(t, queryErr.Error(), "bad value 1.5tail")
				result, queryErr = queryPreparedMath(mathStmt, nil, int64(0))
				require.NoError(t, queryErr)
				require.False(t, result.valid)
				result, queryErr = queryPreparedMath(mathStmt, "1.5", int64(1))
				require.NoError(t, queryErr)
				require.True(t, result.valid)
				require.Contains(t, result.databaseType, "DECIMAL")
				require.Equal(t, 1.5, result.value)
			}()
		}

		abs, err := db.PrepareContext(ctx, "select abs(?)")
		require.NoError(t, err)
		defer abs.Close()
		for _, test := range []struct {
			value any
			want  float64
		}{
			{value: float64(-1.5), want: 1.5},
			{value: "-2.25", want: 2.25},
			{value: int64(-3), want: 3},
		} {
			var result float64
			require.NoError(t, abs.QueryRowContext(ctx, test.value).Scan(&result))
			require.Equal(t, test.want, result)
		}

		wide, err := db.PrepareContext(ctx, "select abs(?)")
		require.NoError(t, err)
		defer wide.Close()
		wideRows, err := wide.QueryContext(ctx, int64(-9007199254740993))
		require.NoError(t, err)
		var exact int64
		func() {
			defer wideRows.Close()
			wideColumns, err := wideRows.ColumnTypes()
			require.NoError(t, err)
			require.Len(t, wideColumns, 1)
			require.Contains(t, strings.ToUpper(wideColumns[0].DatabaseTypeName()), "INT")
			require.True(t, wideRows.Next())
			require.NoError(t, wideRows.Scan(&exact))
			require.NoError(t, wideRows.Err())
		}()
		require.Equal(t, int64(9007199254740993), exact)

		nestedArithmetic, err := db.PrepareContext(ctx, "select abs(? + 0)")
		require.NoError(t, err)
		defer nestedArithmetic.Close()
		var nestedArithmeticResult int64
		require.NoError(t, nestedArithmetic.QueryRowContext(
			ctx, int64(-9007199254740993)).Scan(&nestedArithmeticResult))
		require.Equal(t, int64(9007199254740993), nestedArithmeticResult)

		queryPreparedNumeric := func(t *testing.T, query string, args ...any) (float64, error) {
			t.Helper()
			stmt, err := db.PrepareContext(ctx, query)
			require.NoError(t, err, query)
			defer func() { require.NoError(t, stmt.Close()) }()
			var result float64
			err = stmt.QueryRowContext(ctx, args...).Scan(&result)
			return result, err
		}

		// First assert the server-default SQL mode through a fresh connection and
		// prepared statement. This must not accidentally inherit the mode changes
		// exercised below by the cached-statement mode-flip regression.
		defaultModeDB, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defaultModeDB.SetMaxOpenConns(1)
		defer func() { require.NoError(t, defaultModeDB.Close()) }()
		defaultModeConn, err := defaultModeDB.Conn(ctx)
		require.NoError(t, err)
		defer func() { require.NoError(t, defaultModeConn.Close()) }()
		var defaultSQLMode string
		require.NoError(t, defaultModeConn.QueryRowContext(ctx,
			"select @@sql_mode").Scan(&defaultSQLMode))
		t.Logf("fresh COM_STMT session default @@sql_mode: %q", defaultSQLMode)
		defaultHasNativeMode := false
		defaultHasNumericCompatibilityMode := false
		for _, mode := range strings.Split(defaultSQLMode, ",") {
			switch strings.ToUpper(strings.TrimSpace(mode)) {
			case "MATRIXONE_NATIVE":
				defaultHasNativeMode = true
			case "MYSQL_NUMERIC_COMPATIBILITY":
				defaultHasNumericCompatibilityMode = true
			}
		}
		require.False(t, defaultHasNativeMode,
			"server-default @@sql_mode must not contain MATRIXONE_NATIVE")
		require.False(t, defaultHasNumericCompatibilityMode,
			"server-default @@sql_mode must not enable numeric-prefix compatibility")
		defaultAbs, err := defaultModeConn.PrepareContext(ctx, "select abs(?)")
		require.NoError(t, err)
		defer func() { require.NoError(t, defaultAbs.Close()) }()
		var defaultPrefixResult float64
		err = defaultAbs.QueryRowContext(ctx, "1.5tail").Scan(&defaultPrefixResult)
		require.Error(t, err, "the default COM_STMT session must reject a numeric prefix")
		require.Contains(t, err.Error(), "invalid numeric string")
		warningRows, err := defaultModeConn.QueryContext(ctx, "show warnings")
		require.NoError(t, err)
		var hasNumericTruncationWarning bool
		var warningScanErr error
		for warningRows.Next() {
			var level, message string
			var code uint16
			if warningScanErr = warningRows.Scan(&level, &code, &message); warningScanErr != nil {
				break
			}
			if strings.EqualFold(level, "Warning") && code == 1292 &&
				strings.Contains(message, "Truncated incorrect DOUBLE value") {
				hasNumericTruncationWarning = true
			}
		}
		warningRowsErr := warningRows.Err()
		warningCloseErr := warningRows.Close()
		require.NoError(t, warningScanErr)
		require.NoError(t, warningRowsErr)
		require.NoError(t, warningCloseErr)
		require.False(t, hasNumericTruncationWarning,
			"strict conversion errors must not emit MySQL numeric-truncation warnings")
		_, err = defaultModeConn.ExecContext(ctx,
			"set session sql_mode = 'MYSQL_NUMERIC_COMPATIBILITY'")
		require.NoError(t, err)
		require.NoError(t, defaultAbs.QueryRowContext(ctx, "1.5tail").Scan(&defaultPrefixResult))
		require.Equal(t, float64(1.5), defaultPrefixResult,
			"the explicit MySQL-compatible COM_STMT session must consume the numeric prefix")
		var defaultWarningLevel, defaultWarningMessage string
		var defaultWarningCode uint16
		require.NoError(t, defaultModeConn.QueryRowContext(ctx, "show warnings").Scan(
			&defaultWarningLevel, &defaultWarningCode, &defaultWarningMessage))
		require.Equal(t, "Warning", defaultWarningLevel)
		require.Equal(t, uint16(1292), defaultWarningCode)
		require.Contains(t, defaultWarningMessage, "Truncated incorrect DOUBLE value")

		// A numeric-prefix math source is only correct in explicit MySQL-compatible mode.
		// Pin the connection so SET sql_mode and COM_STMT_EXECUTE use the same
		// session, and reuse the statement across mode changes to cover plan cache
		// invalidation as well as the runtime cast contract.
		err = withIssue27294PinnedConnection(t, ctx, db, func(modeConn *sql.Conn) {
			var originalSQLMode string
			require.NoError(t, modeConn.QueryRowContext(ctx, "select @@sql_mode").Scan(&originalSQLMode))
			defer func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, restoreErr := modeConn.ExecContext(cleanupCtx, fmt.Sprintf(
					"set session sql_mode = '%s'", strings.ReplaceAll(originalSQLMode, "'", "''")))
				if restoreErr != nil {
					t.Errorf("restore sql_mode: %v", restoreErr)
				}
			}()

			_, err = modeConn.ExecContext(ctx, "use issue_27294_numeric_db")
			require.NoError(t, err)
			prefixModeStmt, err := modeConn.PrepareContext(ctx, "select abs(? + 0)")
			require.NoError(t, err)
			defer prefixModeStmt.Close()
			_, err = modeConn.ExecContext(ctx,
				"set session sql_mode = 'STRICT_TRANS_TABLES,MYSQL_NUMERIC_COMPATIBILITY'")
			require.NoError(t, err)
			var literalCastResult float64
			require.NoError(t, modeConn.QueryRowContext(ctx,
				"select abs(cast(concat('1', '01') as char))").Scan(&literalCastResult))
			require.Equal(t, float64(101), literalCastResult,
				"the explicit-cast regression oracle is the ordinary literal SQL result")
			var prefixModeResult float64
			require.NoError(t, prefixModeStmt.QueryRowContext(ctx, "1.5tail").Scan(&prefixModeResult))
			require.Equal(t, float64(1.5), prefixModeResult,
				"MySQL-compatible mode consumes the numeric prefix for a string-math value")
			func() {
				nestedLength, err := modeConn.PrepareContext(ctx, "select length(abs(?))")
				require.NoError(t, err)
				defer func() {
					require.NoError(t, nestedLength.Close())
				}()
				var nestedLengthResult float64
				require.NoError(t, nestedLength.QueryRowContext(ctx, "1.5tail").Scan(&nestedLengthResult))
				require.Equal(t, float64(3), nestedLengthResult,
					"LENGTH must preserve the independently nested ABS owner")
			}()
			_, err = modeConn.ExecContext(ctx,
				"set session sql_mode = 'STRICT_TRANS_TABLES,MATRIXONE_NATIVE,MYSQL_NUMERIC_COMPATIBILITY'")
			require.NoError(t, err)
			err = prefixModeStmt.QueryRowContext(ctx, "1.5tail").Scan(&prefixModeResult)
			require.Error(t, err,
				"MATRIXONE_NATIVE must win over explicit numeric compatibility and reject trailing text")
			err = modeConn.QueryRowContext(ctx, "select mod(2, '1.5tail')").Scan(&prefixModeResult)
			require.Error(t, err,
				"MATRIXONE_NATIVE must win over explicit numeric compatibility in MOD's right operand")
			require.Contains(t, err.Error(), `invalid input: "1.5tail" is invalid numeric string`)
			_, err = modeConn.ExecContext(ctx,
				"set session sql_mode = 'STRICT_TRANS_TABLES,MYSQL_NUMERIC_COMPATIBILITY'")
			require.NoError(t, err)
			require.NoError(t, prefixModeStmt.QueryRowContext(ctx, "1.5tail").Scan(&prefixModeResult))
			require.Equal(t, float64(1.5), prefixModeResult,
				"returning to MySQL-compatible mode must restore prefix behavior on the cached statement")
		})
		require.NoError(t, err)

		// Numeric-prefix candidates are only an eligibility superset. An outer
		// math owner must not rewrite text-domain arguments before LENGTH,
		// CONCAT, or REPLACE consumes them.
		for _, test := range []struct {
			name  string
			query string
			args  []any
			want  float64
		}{
			{
				name:  "numeric-return string argument",
				query: "select abs(length(?))",
				args:  []any{"abc"},
				want:  3,
			},
			{
				name:  "round outside concat",
				query: "select round(concat('1', ?), ?)",
				args:  []any{"01", int64(0)},
				want:  101,
			},
			{
				name:  "abs outside replace",
				query: "select abs(replace('11', '1', ?))",
				args:  []any{"01"},
				want:  101,
			},
		} {
			t.Run("string-domain boundary/"+test.name, func(t *testing.T) {
				got, err := queryPreparedNumeric(t, test.query, test.args...)
				require.NoError(t, err, test.query)
				require.Equal(t, test.want, got, test.query)
			})
		}

		// These controls ensure a boundary fix does not suppress the nearest
		// ROUND precision contract or explicit CAST's text-preserving subtree.
		t.Run("numeric-owner control/nested round precision remains owned", func(t *testing.T) {
			_, err := queryPreparedNumeric(t, "select abs(round(1, ?))", "0.5tail")
			require.Error(t, err)
			require.Contains(t, err.Error(), "bad value 0.5tail")
		})
		t.Run("numeric-owner control/explicit cast remains explicit", func(t *testing.T) {
			got, err := queryPreparedNumeric(t,
				"select abs(cast(concat('1', ?) as char))", "01")
			require.NoError(t, err)
			require.Equal(t, float64(101), got)
		})

		multiMarker, err := db.PrepareContext(ctx, "select abs(? + ?)")
		require.NoError(t, err)
		defer multiMarker.Close()
		multiMarkerRows, err := multiMarker.QueryContext(
			ctx, int64(-9007199254740993), int64(0))
		require.NoError(t, err)
		func() {
			defer multiMarkerRows.Close()
			columnTypes, err := multiMarkerRows.ColumnTypes()
			require.NoError(t, err)
			require.Len(t, columnTypes, 1)
			require.Contains(t, strings.ToUpper(columnTypes[0].DatabaseTypeName()), "INT")
			require.True(t, multiMarkerRows.Next())
			var result int64
			require.NoError(t, multiMarkerRows.Scan(&result))
			require.Equal(t, int64(9007199254740993), result)
			require.False(t, multiMarkerRows.Next())
			require.NoError(t, multiMarkerRows.Err())
		}()
		multiMarkerRows, err = multiMarker.QueryContext(
			ctx, int64(-9007199254740993), "0.5")
		require.NoError(t, err)
		func() {
			defer multiMarkerRows.Close()
			columnTypes, err := multiMarkerRows.ColumnTypes()
			require.NoError(t, err)
			require.Len(t, columnTypes, 1)
			require.Contains(t, strings.ToUpper(columnTypes[0].DatabaseTypeName()), "DECIMAL")
			require.True(t, multiMarkerRows.Next())
			var result string
			require.NoError(t, multiMarkerRows.Scan(&result))
			require.Equal(t, "9007199254740992.5", result)
			require.False(t, multiMarkerRows.Next())
			require.NoError(t, multiMarkerRows.Err())
		}()

		for _, query := range []string{
			"select abs(if(1, ?, ?))",
			"select abs(case when 1 then ? else ? end)",
			"select abs((select ? + ?))",
			"select abs((select ?) + (select ?))",
		} {
			func() {
				stmt, err := db.PrepareContext(ctx, query)
				require.NoError(t, err, query)
				defer func() {
					require.NoError(t, stmt.Close())
				}()
				var result int64
				require.NoError(t, stmt.QueryRowContext(
					ctx, int64(-9007199254740993), int64(0)).Scan(&result), query)
				require.Equal(t, int64(9007199254740993), result, query)
			}()
		}

		nestedControlFlow, err := db.PrepareContext(ctx, "select abs(if(1, ?, 0))")
		require.NoError(t, err)
		defer nestedControlFlow.Close()
		var nestedControlFlowResult int64
		require.NoError(t, nestedControlFlow.QueryRowContext(
			ctx, int64(-9007199254740993)).Scan(&nestedControlFlowResult))
		require.Equal(t, int64(9007199254740993), nestedControlFlowResult)

		nestedCase, err := db.PrepareContext(ctx, "select abs(case when 1 then ? else 0 end)")
		require.NoError(t, err)
		defer nestedCase.Close()
		var nestedCaseResult int64
		require.NoError(t, nestedCase.QueryRowContext(
			ctx, int64(-9007199254740993)).Scan(&nestedCaseResult))
		require.Equal(t, int64(9007199254740993), nestedCaseResult)

		conditionOnlyCase, err := db.PrepareContext(ctx,
			"select abs(case when ? then v else v end) from issue_27294_numeric_src")
		require.NoError(t, err)
		defer conditionOnlyCase.Close()
		var conditionOnlyCaseResult int64
		require.NoError(t, conditionOnlyCase.QueryRowContext(ctx, true).Scan(&conditionOnlyCaseResult))
		require.Equal(t, int64(9007199254740993), conditionOnlyCaseResult,
			"a control-flow-only parameter must not coerce BIGINT value branches to DOUBLE")

		unsigned, err := db.PrepareContext(ctx, "select abs(?)")
		require.NoError(t, err)
		defer unsigned.Close()
		unsignedRows, err := unsigned.QueryContext(ctx, uint64(9007199254740993))
		require.NoError(t, err)
		var unsignedResult uint64
		func() {
			defer unsignedRows.Close()
			unsignedColumns, err := unsignedRows.ColumnTypes()
			require.NoError(t, err)
			require.Len(t, unsignedColumns, 1)
			require.Contains(t, strings.ToUpper(unsignedColumns[0].DatabaseTypeName()), "INT")
			require.True(t, unsignedRows.Next())
			require.NoError(t, unsignedRows.Scan(&unsignedResult))
			require.NoError(t, unsignedRows.Err())
		}()
		require.Equal(t, uint64(9007199254740993), unsignedResult)
		var maxUnsignedResult uint64
		require.NoError(t, unsigned.QueryRowContext(ctx, uint64(math.MaxUint64)).Scan(&maxUnsignedResult))
		require.Equal(t, uint64(math.MaxUint64), maxUnsignedResult)

		minInt, err := db.PrepareContext(ctx, "select abs(?)")
		require.NoError(t, err)
		defer minInt.Close()
		var minIntResult int64
		require.Error(t, minInt.QueryRowContext(ctx, int64(math.MinInt64)).Scan(&minIntResult),
			"ABS(MININT64) must retain the native integer overflow contract")

		decimal, err := db.PrepareContext(ctx, "select abs(?)")
		require.NoError(t, err)
		defer decimal.Close()
		const decimalValue = "12345678901234567890123456789012345.6789"
		decimalRows, err := decimal.QueryContext(ctx, decimalValue)
		require.NoError(t, err)
		var decimalResult string
		func() {
			defer decimalRows.Close()
			decimalColumns, err := decimalRows.ColumnTypes()
			require.NoError(t, err)
			require.Len(t, decimalColumns, 1)
			require.Contains(t, strings.ToUpper(decimalColumns[0].DatabaseTypeName()), "DECIMAL")
			require.True(t, decimalRows.Next())
			require.NoError(t, decimalRows.Scan(&decimalResult))
			require.NoError(t, decimalRows.Err())
		}()
		require.Equal(t, decimalValue, decimalResult)

		subquery, err := db.PrepareContext(ctx, "select abs((select ?))")
		require.NoError(t, err)
		defer subquery.Close()
		var subqueryResult float64
		require.NoError(t, subquery.QueryRowContext(ctx, float64(-1.5)).Scan(&subqueryResult))
		require.Equal(t, 1.5, subqueryResult)

		sleepSubquery, err := db.PrepareContext(ctx, "select sleep((select ?))")
		require.NoError(t, err)
		defer sleepSubquery.Close()
		var sleepSubqueryResult int
		require.NoError(t, sleepSubquery.QueryRowContext(ctx, float64(0.01)).Scan(&sleepSubqueryResult))
		require.Zero(t, sleepSubqueryResult)

		nestedExact, err := db.PrepareContext(ctx,
			"select abs((select ? from issue_27294_numeric_src limit 1))")
		require.NoError(t, err)
		defer nestedExact.Close()
		var nestedExactResult int64
		require.NoError(t, nestedExact.QueryRowContext(ctx, int64(-9007199254740993)).Scan(&nestedExactResult))
		require.Equal(t, int64(9007199254740993), nestedExactResult)

		// At v29 the numeric-prefix retention path is disabled. The deferred ABS
		// specialization must still restore unrelated parameters before caching,
		// otherwise the second execution reuses the first execution's literal.
		serviceRuntime := moruntime.ServiceRuntime(cn.GetServiceConfig().CN.UUID)
		oldProtocol, hadProtocol := serviceRuntime.GetGlobalVariables(moruntime.MOProtocolVersion)
		serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion29)
		defer func() {
			if hadProtocol {
				serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
			} else {
				serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
			}
		}()

		cacheIsolation, err := db.PrepareContext(ctx, "select abs(?), ?, ? + 0")
		require.NoError(t, err)
		defer cacheIsolation.Close()
		assertCacheIsolation := func(
			absValue, directValue, nestedValue,
			wantAbs, wantDirect, wantNested int64,
		) {
			t.Helper()
			var gotAbs, gotDirect, gotNested int64
			require.NoError(t, cacheIsolation.QueryRowContext(
				ctx, absValue, directValue, nestedValue).Scan(&gotAbs, &gotDirect, &gotNested))
			require.Equal(t, wantAbs, gotAbs)
			require.Equal(t, wantDirect, gotDirect)
			require.Equal(t, wantNested, gotNested)
		}
		assertCacheIsolation(-1, 11, 111, 1, 11, 111)
		assertCacheIsolation(-2, 22, 222, 2, 22, 222)

		// Decimal256 has no literal oneof and is represented as a text literal
		// under a DECIMAL256 cast. At v30 two values with identical metadata share
		// the runtime cache key, so the inner literal must retain its ParamRef too.
		serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion30)
		decimalCacheIsolation, err := db.PrepareContext(ctx,
			"select abs(?), cast(? as decimal(65,4))")
		require.NoError(t, err)
		defer decimalCacheIsolation.Close()
		assertDecimalCacheIsolation := func(absValue int64, decimalValue string, wantAbs int64) {
			t.Helper()
			var gotAbs int64
			var gotDecimal string
			require.NoError(t, decimalCacheIsolation.QueryRowContext(
				ctx, absValue, decimalValue).Scan(&gotAbs, &gotDecimal))
			require.Equal(t, wantAbs, gotAbs)
			require.Equal(t, decimalValue, gotDecimal)
		}
		assertDecimalCacheIsolation(-3, "1234567890123456789012345678901234567890.1234", 3)
		assertDecimalCacheIsolation(-4, "2234567890123456789012345678901234567890.1234", 4)
	})
}
