// Copyright 2021 - 2026 Matrix Origin
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
)

// TestIssue26879PreparedDecimalRuntimeDomains uses real COM_STMT packets. The
// driver has interpolateParams disabled, so each QueryContext reuses one
// server-side statement while changing the native protocol parameter type.
func TestIssue26879PreparedDecimalRuntimeDomains(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		for _, query := range []string{
			"drop database if exists issue26879",
			"create database issue26879",
			"create table issue26879.t(d decimal(38,10))",
			"insert into issue26879.t values (2)",
		} {
			_, err = conn.ExecContext(ctx, query)
			require.NoError(t, err)
		}

		stmt, err := conn.PrepareContext(ctx,
			"select coalesce(?,d),greatest(?,d),least(?,d) from issue26879.t")
		require.NoError(t, err)
		defer stmt.Close()

		assertRow := func(value any, wantType string, want []string) {
			t.Helper()
			rows, queryErr := stmt.QueryContext(ctx, value, value, value)
			require.NoError(t, queryErr)
			defer rows.Close()
			columnTypes, typeErr := rows.ColumnTypes()
			require.NoError(t, typeErr)
			require.Len(t, columnTypes, 3)
			for _, columnType := range columnTypes {
				require.Equal(t, wantType, columnType.DatabaseTypeName())
			}
			require.True(t, rows.Next())
			actual := make([]string, 3)
			require.NoError(t, rows.Scan(&actual[0], &actual[1], &actual[2]))
			require.Equal(t, want, actual)
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		}

		assertRow(float64(1e100), "DOUBLE", []string{"1e+100", "1e+100", "2"})
		assertRow(float64(1e-40), "DOUBLE", []string{"1e-40", "2", "1e-40"})
		assertRow("1.234567", "DECIMAL", []string{"1.234567000000000000000000000000", "2.000000000000000000000000000000", "1.234567000000000000000000000000"})
		assertRow("abc", "DECIMAL", []string{"0.000000000000000000000000000000", "2.000000000000000000000000000000", "0.000000000000000000000000000000"})
		assertRow("12.5tail", "DECIMAL", []string{"12.500000000000000000000000000000", "12.500000000000000000000000000000", "2.000000000000000000000000000000"})
		assertRow("001.200e2", "DECIMAL", []string{"120.000000000000000000000000000000", "120.000000000000000000000000000000", "2.000000000000000000000000000000"})
		assertRow("2026-08-10 12:34:56", "DECIMAL", []string{"2026.000000000000000000000000000000", "2026.000000000000000000000000000000", "2.000000000000000000000000000000"})
		assertRow("9007199254740993tail", "DECIMAL", []string{"9007199254740993.000000000000000000000000000000", "9007199254740993.000000000000000000000000000000", "2.000000000000000000000000000000"})
		assertRow("9007199254740993e0tail", "DECIMAL", []string{"9007199254740993.000000000000000000000000000000", "9007199254740993.000000000000000000000000000000", "2.000000000000000000000000000000"})
		assertRow("1e100tail", "DECIMAL", []string{"99999999999999999999999999999999999.999999999999999999999999999999", "99999999999999999999999999999999999.999999999999999999999999999999", "2.000000000000000000000000000000"})
		assertRow("\v1.25", "DECIMAL", []string{"1.250000000000000000000000000000", "2.000000000000000000000000000000", "1.250000000000000000000000000000"})
		assertRow("\f1.25", "DECIMAL", []string{"1.250000000000000000000000000000", "2.000000000000000000000000000000", "1.250000000000000000000000000000"})
		assertRow(int64(10), "DECIMAL", []string{"10.0000000000", "10.0000000000", "2.0000000000"})
		assertRow(false, "DECIMAL", []string{"0.0000000000", "2.0000000000", "0.0000000000"})

		_, err = conn.ExecContext(ctx, "create table issue26879.extreme(d decimal(65,0))")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "insert into issue26879.extreme values (2)")
		require.NoError(t, err)
		extremeStmt, err := conn.PrepareContext(ctx, "select coalesce(?,d) from issue26879.extreme")
		require.NoError(t, err)
		defer extremeStmt.Close()
		var extremeValue string
		require.NoError(t, extremeStmt.QueryRowContext(ctx, int64(10)).Scan(&extremeValue))
		require.Equal(t, "10", extremeValue)
		// DECIMAL256 cannot represent the full declared 77-digit aggregate
		// domain. Do not introduce a new PREPARE-time failure for this existing
		// representation boundary.
		require.NoError(t, extremeStmt.QueryRowContext(ctx, "0.123456789012").Scan(&extremeValue))
		require.Equal(t, "0.123456789012", extremeValue)

		_, err = conn.ExecContext(ctx, "create table issue26879.small(d decimal(10,2))")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "insert into issue26879.small values (2)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx,
			"prepare issue26879_sql from 'select coalesce(?,d),greatest(?,d),least(?,d) from issue26879.small'")
		require.NoError(t, err)
		defer conn.ExecContext(context.Background(), "deallocate prepare issue26879_sql") //nolint:errcheck

		assertSQLPrepareType := func(value string, wantType string, wantPrecision, wantScale int64) {
			t.Helper()
			_, setErr := conn.ExecContext(ctx, "set @issue26879_p=?", value)
			require.NoError(t, setErr)
			rows, queryErr := conn.QueryContext(ctx,
				"execute issue26879_sql using @issue26879_p,@issue26879_p,@issue26879_p")
			require.NoError(t, queryErr)
			defer rows.Close()
			columnTypes, typeErr := rows.ColumnTypes()
			require.NoError(t, typeErr)
			for _, columnType := range columnTypes {
				require.Equal(t, wantType, columnType.DatabaseTypeName())
				if wantType == "DECIMAL" {
					precision, scale, ok := columnType.DecimalSize()
					require.True(t, ok)
					require.Equal(t, wantPrecision, precision)
					require.Equal(t, wantScale, scale)
				}
			}
			require.True(t, rows.Next())
			var values [3]string
			require.NoError(t, rows.Scan(&values[0], &values[1], &values[2]))
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		}
		assertSQLPrepareType("1.234567", "DECIMAL", 65, 30)
		assertSQLPrepareType("1e100", "DOUBLE", 0, 0)
	})
}
