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
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	dbClosed := false
	defer func() {
		if !dbClosed {
			_ = db.Close()
		}
	}()

	mock.ExpectQuery("^select @@sql_mode$").
		WillReturnError(errors.New("server-side sql_mode lookup failed"))
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
			var sqlMode string
			queryErr := conn.QueryRowContext(ctx, "select @@sql_mode").Scan(&sqlMode)
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
	require.ErrorContains(t, result.queryErr, "server-side sql_mode lookup failed")
	require.Equal(t, 1, result.inUse, "the checked-out connection must be in-use before callback exit")
	close(continueToGoexit)

	doneCtx, cancelDone := context.WithTimeout(context.Background(), regressionTimeout)
	select {
	case <-done:
	case <-doneCtx.Done():
		t.Fatalf("pinned-connection defers did not finish before %s", doneCtx.Err())
	}
	cancelDone()
	require.Zero(t, db.Stats().InUse)

	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), regressionTimeout)
	_, err = db.ExecContext(cleanupCtx, cleanupSQL)
	cancelCleanup()
	require.NoError(t, err, "database-level cleanup must reuse the sole pool connection")
	require.Zero(t, db.Stats().InUse)
	err = db.Close()
	dbClosed = true
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
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

		// Keep the two precision roles separate on a real COM_STMT handle:
		// text values select DECIMAL while precision remains an integer cast.
		type preparedMathResult struct {
			value        float64
			valid        bool
			databaseType string
		}
		queryPreparedMath := func(stmt *sql.Stmt, value, precision any) (preparedMathResult, error) {
			rows, err := stmt.QueryContext(ctx, value, precision)
			if err != nil {
				return preparedMathResult{}, err
			}
			defer rows.Close()
			columns, err := rows.ColumnTypes()
			if err != nil {
				return preparedMathResult{}, err
			}
			if len(columns) != 1 {
				return preparedMathResult{}, fmt.Errorf("expected one result column, got %d", len(columns))
			}
			var got sql.NullFloat64
			if !rows.Next() {
				if err := rows.Err(); err != nil {
					return preparedMathResult{}, err
				}
				return preparedMathResult{}, fmt.Errorf("prepared math query returned no rows")
			}
			if err := rows.Scan(&got); err != nil {
				return preparedMathResult{}, err
			}
			if err := rows.Err(); err != nil {
				return preparedMathResult{}, err
			}
			return preparedMathResult{value: got.Float64, valid: got.Valid,
				databaseType: strings.ToUpper(columns[0].DatabaseTypeName())}, nil
		}

		round, err := db.PrepareContext(ctx, "select round(?, ?)")
		require.NoError(t, err)
		defer func() { require.NoError(t, round.Close()) }()
		result, err := queryPreparedMath(round, "1.5", int64(0))
		require.NoError(t, err)
		require.True(t, result.valid)
		require.Contains(t, result.databaseType, "DECIMAL")
		require.Equal(t, float64(2), result.value)
		_, err = queryPreparedMath(round, "1.5", "1.5tail")
		require.Error(t, err)
		require.Contains(t, err.Error(), "bad value 1.5tail")
		result, err = queryPreparedMath(round, "1.5", int64(1))
		require.NoError(t, err, "same handle must recover after rejected precision")
		require.True(t, result.valid)
		require.Contains(t, result.databaseType, "DECIMAL")
		require.Equal(t, 1.5, result.value)
		result, err = queryPreparedMath(round, nil, int64(0))
		require.NoError(t, err)
		require.False(t, result.valid)

		truncate, err := db.PrepareContext(ctx, "select truncate(?, ?)")
		require.NoError(t, err)
		defer func() { require.NoError(t, truncate.Close()) }()
		result, err = queryPreparedMath(truncate, "1.5", int64(0))
		require.NoError(t, err)
		require.True(t, result.valid)
		require.Contains(t, result.databaseType, "DECIMAL")
		require.Equal(t, float64(1), result.value)

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

		t.Run("numeric consumers across query boundaries", func(t *testing.T) {
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()
			for _, tc := range []struct {
				name, query, first, second string
				want                       float64
			}{
				{"scalar abs", "select abs((select ?))", "-1.5", "-2.5", 1.5},
				{"derived sum", "select sum(x) from (select ? x limit 1) d", "1.5", "2.5", 1.5},
			} {
				for _, binary := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/binary=%t", tc.name, binary), func(t *testing.T) {
						var stmt *sql.Stmt
						if binary {
							stmt, err = conn.PrepareContext(ctx, tc.query)
							require.NoError(t, err)
							defer stmt.Close()
						} else {
							_, err = conn.ExecContext(ctx, "prepare projected_numeric from '"+tc.query+"'")
							require.NoError(t, err)
							defer conn.ExecContext(ctx, "deallocate prepare projected_numeric")
						}
						for i, value := range []any{tc.first, tc.second, nil, tc.first} {
							var got sql.NullFloat64
							if binary {
								err = stmt.QueryRowContext(ctx, value).Scan(&got)
							} else {
								assignment := "null"
								if value != nil {
									assignment = "'" + value.(string) + "'"
								}
								_, err = conn.ExecContext(ctx, "set @projected_numeric="+assignment)
								require.NoError(t, err)
								err = conn.QueryRowContext(ctx, "execute projected_numeric using @projected_numeric").Scan(&got)
							}
							require.NoError(t, err, "binding %d", i)
							if value == nil {
								require.False(t, got.Valid)
							} else {
								require.True(t, got.Valid)
								want := tc.want
								if i == 1 {
									want = 2.5
								}
								require.Equal(t, want, got.Float64)
							}
						}
					})
				}
			}
		})

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
		var prefixResult float64
		prefixErr := wide.QueryRowContext(ctx, "abc").Scan(&prefixResult)
		require.ErrorContains(t, prefixErr, `"abc" is invalid numeric string`)
		var exactText string
		require.NoError(t, wide.QueryRowContext(ctx, "-9007199254740993").Scan(&exactText))
		require.Equal(t, "9007199254740993", exactText)

		nestedArithmetic, err := db.PrepareContext(ctx, "select abs(? + 0)")
		require.NoError(t, err)
		defer nestedArithmetic.Close()
		var nestedArithmeticResult int64
		require.NoError(t, nestedArithmetic.QueryRowContext(
			ctx, int64(-9007199254740993)).Scan(&nestedArithmeticResult))
		require.Equal(t, int64(9007199254740993), nestedArithmeticResult)

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
			stmt, err := modeConn.PrepareContext(ctx, "select abs(? + 0)")
			require.NoError(t, err)
			defer func() { require.NoError(t, stmt.Close()) }()

			_, err = modeConn.ExecContext(ctx, "set session sql_mode = 'STRICT_TRANS_TABLES'")
			require.NoError(t, err)
			var value float64
			err = stmt.QueryRowContext(ctx, "1.5tail").Scan(&value)
			require.ErrorContains(t, err, "invalid numeric string")

			_, err = modeConn.ExecContext(ctx,
				"set session sql_mode = 'STRICT_TRANS_TABLES,MYSQL_NUMERIC_COMPATIBILITY'")
			require.NoError(t, err)
			require.NoError(t, stmt.QueryRowContext(ctx, "1.5tail").Scan(&value))
			require.Equal(t, float64(1.5), value)
			var level, message string
			var code uint16
			require.NoError(t, modeConn.QueryRowContext(ctx, "show warnings").Scan(&level, &code, &message))
			require.Equal(t, "Warning", level)
			require.Equal(t, uint16(1292), code)
			require.Contains(t, message, "Truncated incorrect DOUBLE value")

			// The outer numeric owner must not consume text owned by CONCAT.
			concat, err := modeConn.PrepareContext(ctx, "select abs(concat('1', ?))")
			require.NoError(t, err)
			defer func() { require.NoError(t, concat.Close()) }()
			var concatResult float64
			require.NoError(t, concat.QueryRowContext(ctx, "01").Scan(&concatResult))
			require.Equal(t, float64(101), concatResult)
			explicitCast, err := modeConn.PrepareContext(ctx, "select abs(cast(concat('1', ?) as char))")
			require.NoError(t, err)
			defer func() { require.NoError(t, explicitCast.Close()) }()
			var castResult float64
			require.NoError(t, explicitCast.QueryRowContext(ctx, "01").Scan(&castResult))
			require.Equal(t, float64(101), castResult)

			_, err = modeConn.ExecContext(ctx,
				"set session sql_mode = 'STRICT_TRANS_TABLES,MATRIXONE_NATIVE,MYSQL_NUMERIC_COMPATIBILITY'")
			require.NoError(t, err)
			err = stmt.QueryRowContext(ctx, "1.5tail").Scan(&value)
			require.ErrorContains(t, err, "invalid numeric string",
				"MATRIXONE_NATIVE must win over explicit compatibility")
			_, err = modeConn.ExecContext(ctx, "set session sql_mode = 'STRICT_TRANS_TABLES'")
			require.NoError(t, err)
			err = stmt.QueryRowContext(ctx, "1.5tail").Scan(&value)
			require.ErrorContains(t, err, "invalid numeric string",
				"the same prepared handle returns to strict mode")
		})
		require.NoError(t, err)

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
		// Both executions have the same source types. An integer spelling
		// must not cache a DOUBLE plan for the later exact-decimal spelling.
		var discarded string
		require.NoError(t, multiMarker.QueryRowContext(
			ctx, int64(-9007199254740993), "0").Scan(&discarded))
		var fractional string
		require.NoError(t, multiMarker.QueryRowContext(
			ctx, int64(-9007199254740993), "0.5").Scan(&fractional))
		require.Equal(t, "9007199254740992.5", fractional)

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
