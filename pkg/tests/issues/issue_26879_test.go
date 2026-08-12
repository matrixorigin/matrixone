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
	"strings"
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
		assertRow("1.234567", "DECIMAL", []string{"1.2345670000", "2.0000000000", "1.2345670000"})
		assertRow("abc", "DECIMAL", []string{"0.0000000000", "2.0000000000", "0.0000000000"})
		assertRow("12.5tail", "DECIMAL", []string{"12.5000000000", "12.5000000000", "2.0000000000"})
		assertRow("001.200e2", "DECIMAL", []string{"120.0000000000", "120.0000000000", "2.0000000000"})
		assertRow("2026-08-10 12:34:56", "DECIMAL", []string{"2026.0000000000", "2026.0000000000", "2.0000000000"})
		assertRow("9007199254740993tail", "DECIMAL", []string{"9007199254740993.0000000000", "9007199254740993.0000000000", "2.0000000000"})
		assertRow("9007199254740993e0tail", "DECIMAL", []string{"9007199254740993.0000000000", "9007199254740993.0000000000", "2.0000000000"})
		assertRow("123456789012345678901234567890123456", "DECIMAL", []string{"123456789012345678901234567890123456.0000000000", "123456789012345678901234567890123456.0000000000", "2.0000000000"})
		assertRow("1e35", "DECIMAL", []string{"100000000000000000000000000000000000.0000000000", "100000000000000000000000000000000000.0000000000", "2.0000000000"})
		assertRow("1e100tail", "DECIMAL", []string{"99999999999999999999999999999999999999999999999999999999999999999.0000000000", "99999999999999999999999999999999999999999999999999999999999999999.0000000000", "2.0000000000"})
		assertRow("1E2tail", "DECIMAL", []string{"100.0000000000", "100.0000000000", "2.0000000000"})
		assertRow("1e-2147483648tail", "DECIMAL", []string{"0.000000000000000000000000000000", "2.000000000000000000000000000000", "0.000000000000000000000000000000"})
		assertRow("1e-31", "DECIMAL", []string{"0.000000000000000000000000000000", "2.000000000000000000000000000000", "0.000000000000000000000000000000"})
		assertRow("0.1e35", "DECIMAL", []string{"10000000000000000000000000000000000.0000000000", "10000000000000000000000000000000000.0000000000", "2.0000000000"})
		assertRow("\v1.25", "DECIMAL", []string{"1.2500000000", "2.0000000000", "1.2500000000"})
		assertRow("\f1.25", "DECIMAL", []string{"1.2500000000", "2.0000000000", "1.2500000000"})
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

		assertSQLPrepareType := func(value string, wantType string, wantPrecision, wantScale int64) [3]string {
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
			return values
		}
		assertSQLPrepareType("1.234567", "DECIMAL", 65, 30)
		values := assertSQLPrepareType("1e-40", "DECIMAL", 65, 30)
		require.Equal(t, "0.000000000000000000000000000000", values[0])
		assertSQLPrepareType("1e100", "DOUBLE", 0, 0)
		for _, value := range []string{
			"999999999999999999999999999999999999999999999999999999999999999",
			"9999999999999999999999999999999999999999999999999999999999999999",
			"99999999999999999999999999999999999999999999999999999999999999999",
			"999999999999999999999999999999999999999999999999999999999999999999",
			"9999999999999999999999999999999999999999999999999999999999999999999",
			"1e62",
			"1e63",
		} {
			want := value
			if value == "1e62" {
				want = "1" + strings.Repeat("0", 62)
			} else if value == "1e63" {
				want = "1" + strings.Repeat("0", 63)
			}
			values := assertSQLPrepareType(value, "DECIMAL", 76, 9)
			require.Equal(t, want+".000000000", values[0])
			require.Equal(t, want+".000000000", values[1])
			require.Equal(t, "2.000000000", values[2])
		}
		values = assertSQLPrepareType("1e100tail", "DECIMAL", 74, 9)
		maxPrefix := strings.Repeat("9", 65) + ".000000000"
		require.Equal(t, maxPrefix, values[0])
		require.Equal(t, maxPrefix, values[1])
		require.Equal(t, "2.000000000", values[2])

		_, err = conn.ExecContext(ctx,
			"prepare issue26879_set from 'set @issue26879_out = coalesce(?, cast(2 as decimal(10,2)))'")
		require.NoError(t, err)
		defer conn.ExecContext(context.Background(), "deallocate prepare issue26879_set") //nolint:errcheck
		_, err = conn.ExecContext(ctx, "set @issue26879_p = '1e100'")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "execute issue26879_set using @issue26879_p")
		require.NoError(t, err)
		var dclValue string
		require.NoError(t, conn.QueryRowContext(ctx, "select @issue26879_out").Scan(&dclValue))
		require.Equal(t, "1e+100", dclValue)

		_, err = conn.ExecContext(ctx,
			"prepare issue26879_window from 'select sum(coalesce(?,d)) over () from issue26879.small'")
		require.NoError(t, err)
		defer conn.ExecContext(context.Background(), "deallocate prepare issue26879_window") //nolint:errcheck
		var windowValue string
		require.NoError(t, conn.QueryRowContext(ctx,
			"execute issue26879_window using @issue26879_p").Scan(&windowValue))
		require.Equal(t, "1e+100", windowValue)

		_, err = conn.ExecContext(ctx,
			"prepare issue26879_ctas from 'create table issue26879.ctas as select coalesce(?,d) x from issue26879.small'")
		require.NoError(t, err)
		defer conn.ExecContext(context.Background(), "deallocate prepare issue26879_ctas") //nolint:errcheck
		_, err = conn.ExecContext(ctx, "execute issue26879_ctas using @issue26879_p")
		require.NoError(t, err)
		var ctasValue string
		require.NoError(t, conn.QueryRowContext(ctx, "select x from issue26879.ctas").Scan(&ctasValue))
		require.Equal(t, "1e+100", ctasValue)

		for _, query := range []string{
			"create table issue26879.schema_change(v varchar(32))",
			"insert into issue26879.schema_change values ('2')",
			"prepare issue26879_schema from 'select coalesce(?,v),greatest(?,v),least(?,v) from issue26879.schema_change'",
			"set @issue26879_schema_p = '1e100'",
			"alter table issue26879.schema_change modify v decimal(10,2)",
		} {
			_, err = conn.ExecContext(ctx, query)
			require.NoError(t, err)
		}
		defer conn.ExecContext(context.Background(), "deallocate prepare issue26879_schema") //nolint:errcheck
		rows, err := conn.QueryContext(ctx,
			"execute issue26879_schema using @issue26879_schema_p,@issue26879_schema_p,@issue26879_schema_p")
		require.NoError(t, err)
		defer rows.Close()
		columnTypes, err := rows.ColumnTypes()
		require.NoError(t, err)
		for _, columnType := range columnTypes {
			require.Equal(t, "DOUBLE", columnType.DatabaseTypeName())
		}
		require.True(t, rows.Next())
		var schemaValues [3]string
		require.NoError(t, rows.Scan(&schemaValues[0], &schemaValues[1], &schemaValues[2]))
		require.Equal(t, [3]string{"1e+100", "1e+100", "2"}, schemaValues)
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())

		_, err = conn.ExecContext(ctx, "create table issue26879.com_schema_change(v varchar(32))")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "insert into issue26879.com_schema_change values ('2')")
		require.NoError(t, err)
		comSchemaStmt, err := conn.PrepareContext(ctx,
			"select coalesce(?,v),greatest(?,v),least(?,v) from issue26879.com_schema_change")
		require.NoError(t, err)
		defer comSchemaStmt.Close()
		_, err = conn.ExecContext(ctx, "alter table issue26879.com_schema_change modify v decimal(10,2)")
		require.NoError(t, err)
		var comSchemaValues [3]string
		require.NoError(t, comSchemaStmt.QueryRowContext(ctx, "1e100", "1e100", "1e100").Scan(
			&comSchemaValues[0], &comSchemaValues[1], &comSchemaValues[2]))
		require.Equal(t, [3]string{"1e+100", "1e+100", "2"}, comSchemaValues)
	})
}
