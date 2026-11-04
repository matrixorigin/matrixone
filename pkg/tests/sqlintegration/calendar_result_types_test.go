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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// Reuse the canonical single-CN fixture for the wire and materialized contracts.
func TestCalendarResultWireAndCTAS(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=true", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const schema = "calendar_result_types"
		exec := func(q string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		exec("drop database if exists " + schema)
		exec("create database " + schema)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec("create table " + schema + ".dates(id int primary key,d date,dt datetime)")
		exec("insert into " + schema + ".dates values(1,'2024-12-31','2024-12-31 12:34:56'),(2,'2025-01-01','2025-01-01 12:34:56'),(3,NULL,NULL)")
		check := func(t *testing.T, rows *sql.Rows, wantTypes []string, want [][]sql.NullInt64) {
			t.Helper()
			cols, err := rows.ColumnTypes()
			require.NoError(t, err)
			require.Len(t, cols, len(wantTypes))
			for i, col := range cols {
				require.Equal(t, wantTypes[i], col.DatabaseTypeName(), col.Name())
			}
			for _, expected := range want {
				require.True(t, rows.Next())
				actual := make([]sql.NullInt64, len(cols))
				dest := make([]any, len(cols))
				for i := range dest {
					dest[i] = &actual[i]
				}
				require.NoError(t, rows.Scan(dest...))
				require.Equal(t, expected, actual)
			}
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		}
		n := func(v int64) sql.NullInt64 { return sql.NullInt64{Int64: v, Valid: true} }
		t.Run("column vectors and signed direct metadata", func(t *testing.T) {
			exec("set default_week_format=0")
			rows, err := conn.QueryContext(ctx, "select week(d),week(d,3),week(dt),week(dt,3),dayofyear(d),weekofyear(d) from "+schema+".dates order by id")
			require.NoError(t, err)
			defer rows.Close()
			check(t, rows, []string{"BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT"}, [][]sql.NullInt64{
				{n(52), n(1), n(52), n(1), n(366), n(1)},
				{n(0), n(1), n(0), n(1), n(1), n(1)},
				{{}, {}, {}, {}, {}, {}},
			})
			require.NoError(t, rows.Err())
		})
		for _, prepared := range []bool{false, true} {
			t.Run(fmt.Sprintf("prepared=%v", prepared), func(t *testing.T) {
				exec("set default_week_format=0")
				const q = "select week(?),week(?,3),week(cast(? as datetime)),dayofyear(?),weekofyear(?)"
				var stmt *sql.Stmt
				if prepared {
					stmt, err = conn.PrepareContext(ctx, q)
					require.NoError(t, err)
					defer stmt.Close()
				}
				for i, value := range []any{"2024-12-31", nil, "2024-12-31"} {
					if i == 1 {
						exec("set default_week_format=3")
					}
					func() {
						args := []any{value, value, value, value, value}
						var rows *sql.Rows
						var err error
						if prepared {
							rows, err = stmt.QueryContext(ctx, args...)
						} else {
							rows, err = conn.QueryContext(ctx, q, args...)
						}
						require.NoError(t, err)
						defer rows.Close()
						want := []sql.NullInt64{{}, {}, {}, {}, {}}
						if value != nil {
							week := int64(52)
							if i == 2 {
								week = 1
							}
							want = []sql.NullInt64{n(week), n(1), n(week), n(366), n(1)}
						}
						check(t, rows, []string{"BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT"}, [][]sql.NullInt64{want})
						require.NoError(t, rows.Err())
					}()
				}
			})
		}
		t.Run("CTAS stores signed INT and permits wider inserts", func(t *testing.T) {
			exec("create table " + schema + ".derived as select week(d,3) w,dayofyear(d) doy from " + schema + ".dates")
			var table, ddl string
			require.NoError(t, conn.QueryRowContext(ctx, "show create table "+schema+".derived").Scan(&table, &ddl))
			require.Contains(t, ddl, "`w` int")
			require.Contains(t, ddl, "`doy` int")
			require.NotContains(t, ddl, "unsigned")
			exec("insert into " + schema + ".derived(w,doy) values(-1,-1),(255,65535),(256,65536)")
			rows, err := conn.QueryContext(ctx, "select w,doy from "+schema+".derived order by w,doy")
			require.NoError(t, err)
			defer rows.Close()
			check(t, rows, []string{"INT", "INT"}, [][]sql.NullInt64{{{}, {}}, {n(-1), n(-1)}, {n(1), n(1)}, {n(1), n(366)}, {n(255), n(65535)}, {n(256), n(65536)}})
			require.NoError(t, rows.Err())
			// Guard against inventing a BIGINT presentation for materialized INT.
			rows, err = conn.QueryContext(ctx, "select w as week,doy as dayofyear from "+schema+".derived where w=256")
			require.NoError(t, err)
			defer rows.Close()
			check(t, rows, []string{"INT", "INT"}, [][]sql.NullInt64{{n(256), n(65536)}})
			require.NoError(t, rows.Err())
		})
	})
}
