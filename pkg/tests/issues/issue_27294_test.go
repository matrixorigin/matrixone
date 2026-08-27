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
	"math"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

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
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		defer db.Close()
		_, err = db.ExecContext(ctx, "drop database if exists issue_27294_numeric_db")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "create database issue_27294_numeric_db")
		require.NoError(t, err)
		defer func() {
			_, _ = db.ExecContext(context.Background(), "drop database if exists issue_27294_numeric_db")
		}()
		_, err = db.ExecContext(ctx, "use issue_27294_numeric_db")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "drop table if exists issue_27294_numeric_src")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "create table issue_27294_numeric_src (v bigint)")
		require.NoError(t, err)
		defer func() {
			_, _ = db.ExecContext(context.Background(), "drop table if exists issue_27294_numeric_src")
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
	})
}
