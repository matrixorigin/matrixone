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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestSelectDivisionByZeroWarnings(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		conn, err := db.Conn(ctx)
		cancel()
		require.NoError(t, err)
		defer conn.Close()
		exec := func(query string) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			_, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		query := func(query string) [][]string {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			rows, err := conn.QueryContext(ctx, query)
			require.NoError(t, err, query)
			defer rows.Close()
			columns, err := rows.Columns()
			require.NoError(t, err)
			var result [][]string
			for rows.Next() {
				values := make([]sql.NullString, len(columns))
				dest := make([]any, len(columns))
				for i := range values {
					dest[i] = &values[i]
				}
				require.NoError(t, rows.Scan(dest...))
				row := make([]string, len(columns))
				for i, value := range values {
					row[i] = "NULL"
					if value.Valid {
						row[i] = value.String
					}
				}
				result = append(result, row)
			}
			require.NoError(t, rows.Err())
			return result
		}
		checkWarnings := func(want int) {
			rows := query("SHOW WARNINGS")
			require.Len(t, rows, want)
			for _, row := range rows {
				require.Equal(t, []string{"Warning", "1365", "Division by 0"}, row)
			}
		}
		for _, mode := range []string{"", "STRICT_TRANS_TABLES", "ERROR_FOR_DIVISION_BY_ZERO", "TRADITIONAL"} {
			exec("SET SESSION sql_mode='" + mode + "'")
			want := 0
			if mode == "ERROR_FOR_DIVISION_BY_ZERO" || mode == "TRADITIONAL" {
				want = 1
			}
			for _, expr := range []string{"1 / 0", "1 DIV 0", "MOD(1, 0)", "1 % 0", "CAST(1 AS DOUBLE) / 0", "CAST(1 AS DECIMAL(65, 2)) / 0"} {
				t.Log(mode, expr)
				require.Equal(t, [][]string{{"NULL"}}, query("SELECT "+expr))
				checkWarnings(want)
			}
		}
		for _, expr := range []string{"NULL / 0", "1 / NULL", "NULL DIV 0", "MOD(NULL, 0)", "IF(FALSE, 1/0, 7)", "CASE WHEN FALSE THEN 1 DIV 0 ELSE 7 END", "4 DIV 2"} {
			t.Log(expr)
			query("SELECT " + expr)
			checkWarnings(0)
		}
		query("EXPLAIN SELECT 1/0")
		checkWarnings(0)
		query("SELECT 1/0 WHERE FALSE")
		checkWarnings(0)
		query("SELECT 1/0, 1 DIV 0, MOD(1,0)")
		checkWarnings(3)
		require.Equal(t, [][]string{{"7"}}, query("SELECT COALESCE(1/0,7)"))
		checkWarnings(1)

		// Prepare while warnings are disabled, then exercise the same plan under
		// both modes. Parent folding must not erase the division subtree.
		exec("SET SESSION sql_mode=''")
		exec("PREPARE div_warning FROM 'SELECT COALESCE(1/0,7), COALESCE(1 DIV ?,7)'")
		defer exec("DEALLOCATE PREPARE div_warning")
		exec("SET @divisor=0")
		for _, mode := range []string{"", "ERROR_FOR_DIVISION_BY_ZERO", "", "TRADITIONAL"} {
			exec("SET SESSION sql_mode='" + mode + "'")
			require.Equal(t, [][]string{{"7", "7"}}, query("EXECUTE div_warning USING @divisor"))
			want := 0
			if mode != "" {
				want = 2
			}
			checkWarnings(want)
		}
		for _, operator := range []string{"IN", "NOT IN"} {
			func() {
				exec("SET SESSION sql_mode=''")
				exec("PREPARE div_warning_list FROM 'SELECT 1 " + operator + " (1 DIV 0,2)'")
				defer exec("DEALLOCATE PREPARE div_warning_list")
				for _, mode := range []string{"", "ERROR_FOR_DIVISION_BY_ZERO", "", "TRADITIONAL"} {
					exec("SET SESSION sql_mode='" + mode + "'")
					require.Equal(t, [][]string{{"NULL"}}, query("EXECUTE div_warning_list"))
					want := 0
					if mode != "" {
						want = 1
					}
					checkWarnings(want)
				}
			}()
		}

		// Two independently flushed blocks let both CNs evaluate arithmetic.
		// Reuse the existing cluster and placement switch instead of creating
		// a large table merely to cross the cost-based scheduling threshold.
		exec("CREATE DATABASE select_division_warning_remote")
		defer exec("DROP DATABASE select_division_warning_remote")
		// Constant arithmetic is evaluated once for each selected input row,
		// including beneath a runtime conditional or a constant-looking parent.
		exec("CREATE TABLE select_division_warning_remote.local_rows(id INT)")
		exec("INSERT INTO select_division_warning_remote.local_rows VALUES(1),(2),(3)")
		for _, expression := range []string{"1/0", "1 DIV 0", "MOD(1,0)"} {
			t.Log("row diagnostics", expression)
			require.Equal(t, [][]string{{"NULL"}, {"NULL"}, {"NULL"}}, query("SELECT "+expression+" FROM select_division_warning_remote.local_rows ORDER BY id"))
			checkWarnings(3)
			require.Equal(t, [][]string{{"NULL"}, {"NULL"}, {"7"}}, query("SELECT IF(id<=2,"+expression+",7) FROM select_division_warning_remote.local_rows ORDER BY id"))
			checkWarnings(2)
			require.Equal(t, [][]string{{"7"}, {"NULL"}, {"NULL"}}, query("SELECT CASE WHEN id>=2 THEN "+expression+" ELSE 7 END FROM select_division_warning_remote.local_rows ORDER BY id"))
			checkWarnings(2)
			require.Equal(t, [][]string{{"7"}, {"7"}, {"7"}}, query("SELECT COALESCE("+expression+",7) FROM select_division_warning_remote.local_rows ORDER BY id"))
			checkWarnings(3)
			query("SELECT " + expression + " FROM select_division_warning_remote.local_rows WHERE FALSE")
			checkWarnings(0)
		}
		exec("CREATE TABLE select_division_warning_remote.src(n INT,d INT)")
		for range 2 {
			exec("INSERT INTO select_division_warning_remote.src VALUES(1,0)")
			query("SELECT mo_ctl('dn','flush','select_division_warning_remote.src')")
		}
		func() {
			oldForce := plan.GetForceScanOnMultiCN()
			plan.SetForceScanOnMultiCN(true)
			defer plan.SetForceScanOnMultiCN(oldForce)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			const statement = "SELECT SUM(n/d) FROM select_division_warning_remote.src"
			explain, err := testutils.QueryTextResult(ctx, conn, "EXPLAIN "+statement)
			require.NoError(t, err)
			require.Contains(t, explain.ColumnName, "AP QUERY PLAN ON MULTICN")
			require.Equal(t, [][]string{{"NULL"}}, query(statement))
			checkWarnings(2)
		}()
	})
}
