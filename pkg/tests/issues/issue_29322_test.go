// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue29322PreparedWideIntegerRebinding(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		name := testutils.GetDatabaseName(t)
		mustExec(t, ctx, conn, "create database "+name)
		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), 20*time.Second)
			defer done()
			_, err := conn.ExecContext(cleanup, "drop database if exists "+name)
			require.NoError(t, err)
		}()
		mustExec(t, ctx, conn, "use "+name)
		mustExec(t, ctx, conn, "create table t(id int,k int,key idx(k))")
		mustExec(t, ctx, conn, "insert into t values(1,-2147483648),(2,42),(3,128),(4,2147483647),(5,null)")
		read := func(rows *sql.Rows) []int {
			var result []int
			for rows.Next() {
				var id int
				require.NoError(t, rows.Scan(&id))
				result = append(result, id)
			}
			return result
		}
		for _, predicate := range []string{"k=?", "k>?", "k in (?,?)", "k not in (?,?)"} {
			func() {
				query := "select id from t where " + predicate + " order by id"
				stmt, err := conn.PrepareContext(ctx, query)
				require.NoError(t, err)
				defer stmt.Close()
				// Cross each narrowing boundary and return to the original range on the
				// same server statement. NULL/fractional bindings cannot inherit its proof.
				for _, value := range []any{int64(42), int64(128), int64(2147483647), int64(2147483648), int64(-2147483649), int64(-2147483648), nil, float64(42.5), int64(42)} {
					func() {
						count := strings.Count(predicate, "?")
						args := make([]any, count)
						for i := range args {
							args[i] = value
						}
						rows, err := stmt.QueryContext(ctx, args...)
						require.NoError(t, err)
						defer rows.Close()
						got := read(rows)
						require.NoError(t, rows.Err())
						require.NoError(t, rows.Close())
						var warnings int
						require.NoError(t, conn.QueryRowContext(ctx, "show count(*) warnings").Scan(&warnings))
						require.Zero(t, warnings)
						literal := "null"
						if value != nil {
							literal = fmt.Sprint(value)
						}
						rows, err = conn.QueryContext(ctx, strings.ReplaceAll(query, "?", literal))
						require.NoError(t, err)
						defer rows.Close()
						want := read(rows)
						require.NoError(t, rows.Err())
						require.NoError(t, rows.Close())
						require.Equal(t, want, got, "predicate=%s binding=%v", predicate, value)
					}()
				}
			}()
		}
		arithmetic, err := conn.PrepareContext(ctx, "select ?+1")
		require.NoError(t, err)
		defer arithmetic.Close()
		for _, value := range []int64{42, 2147483647, 2147483648, 42} {
			var result int64
			require.NoError(t, arithmetic.QueryRowContext(ctx, value).Scan(&result))
			require.Equal(t, value+1, result, "comparison narrowing must not alter the source arithmetic domain")
		}
	})
}
