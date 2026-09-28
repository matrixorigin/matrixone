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

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestCTASDivisionByZero(t *testing.T) {
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
		// Bound each operation independently; the full SQL-mode matrix must
		// not share a deadline consumed by earlier cases on a busy CI runner.
		execSQL := func(query string) error {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			_, err := conn.ExecContext(ctx, query)
			return err
		}
		queryScan := func(query string, dest ...any) error {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			return conn.QueryRowContext(ctx, query).Scan(dest...)
		}
		exec := func(query string) {
			require.NoError(t, execSQL(query), query)
		}
		name := testutils.GetDatabaseName(t)
		exec("CREATE DATABASE `" + name + "`")
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err := conn.ExecContext(cleanupCtx, "DROP DATABASE `"+name+"`")
			require.NoError(t, err)
		}()
		exec("USE `" + name + "`")
		exec("CREATE TABLE src(id INT PRIMARY KEY,n INT,d INT)")
		exec("INSERT INTO src VALUES(1,10,2),(2,10,0),(3,9,3)")
		for _, temporary := range []bool{false, true} {
			t.Run(fmt.Sprintf("temporary=%t", temporary), func(t *testing.T) {
				prefix := "CREATE TABLE copied"
				if temporary {
					prefix = "CREATE TEMPORARY TABLE copied"
				}
				for _, mode := range []string{"STRICT_TRANS_TABLES", "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO", "ERROR_FOR_DIVISION_BY_ZERO", "STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO"} {
					t.Run(mode, func(t *testing.T) {
						err := execSQL("SET SESSION sql_mode='" + mode + "'")
						require.NoError(t, err)
						for _, expression := range []string{"n/d", "10/0", "n DIV d", "n % d", "CAST(n AS DECIMAL(18,4))/d", "CAST(n AS DOUBLE)/d"} {
							t.Run(expression, func(t *testing.T) {
								t.Cleanup(func() {
									err := execSQL("DROP TABLE IF EXISTS copied")
									require.NoError(t, err)
								})
								err := execSQL(prefix + " AS SELECT id," + expression + " AS v FROM src ORDER BY id")
								strict := mode == "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO" || mode == "STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO"
								if strict {
									var sqlErr *mysql.MySQLError
									require.ErrorAs(t, err, &sqlErr)
									require.Equal(t, uint16(1365), sqlErr.Number)
									err = execSQL("SELECT * FROM copied")
									require.ErrorAs(t, err, &sqlErr)
									require.Equal(t, uint16(1146), sqlErr.Number, "failed CTAS must remove the target")
									err = execSQL(prefix + "(marker INT)")
									require.NoError(t, err, "failed temporary CTAS must release its alias")
								} else {
									require.NoError(t, err)
									var count, nulls int
									require.NoError(t, queryScan("SELECT COUNT(*),COUNT(*)-COUNT(v) FROM copied", &count, &nulls))
									require.Equal(t, 3, count)
									wantNulls := 1
									if expression == "10/0" {
										wantNulls = 3
									}
									require.Equal(t, wantNulls, nulls)
								}
							})
						}
						var value sql.NullFloat64
						require.NoError(t, queryScan("SELECT 10/0", &value))
						require.False(t, value.Valid, "CTAS policy must not leak into retrieval")
					})
				}
			})
		}
	})
}
