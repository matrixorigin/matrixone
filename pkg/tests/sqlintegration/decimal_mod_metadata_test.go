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

func TestDecimalModMetadata(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const schema = "decimal_mod_metadata"
		execSQLRequire(t, ctx, db, "drop database if exists "+schema)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		execSQLRequire(t, ctx, db, "create database "+schema)
		execSQLRequire(t, ctx, db, "create table "+schema+".src(id int, d decimal(10,3), narrow_d decimal(3,2), wide_d decimal(18,3), d38 decimal(38,3), d50 decimal(50,3), i int, b bigint, u bigint unsigned, ms mediumint, mu mediumint unsigned)")
		execSQLRequire(t, ctx, db, "insert into "+schema+".src values (1,1.234,1.23,1.234,1.234,1.234,2,2,2,2,2),(2,-1.234,-1.23,-1.234,-1.234,-1.234,-2,-2,2,-2,2),(3,null,null,null,null,null,null,null,null,null,null)")
		projection := "MOD(d,i) as by_column, MOD(d,2) as by_literal, d % i as by_percent, " +
			"MOD(d,CAST(i AS DECIMAL(10,0))) as decimal_control, MOD(wide_d,b) as signed_b, MOD(wide_d,u) as unsigned_b, " +
			"MOD(b,wide_d) as reversed_b, MOD(u,wide_d) as reversed_u, MOD(d38,b) as control38, MOD(d50,u) as control50, " +
			"MOD(narrow_d,2) as narrow_literal, CASE WHEN id=1 THEN MOD(d,i) ELSE d END as by_case, COALESCE(MOD(d,i),d) as by_coalesce, MOD(narrow_d,ms) as signed_medium, MOD(narrow_d,mu) as unsigned_medium"
		execSQLRequire(t, ctx, db, "create table "+schema+".dst as select "+projection+" from "+schema+".src")
		execSQLRequire(t, ctx, db, "create view "+schema+".v as select "+projection+" from "+schema+".src")
		expected := []string{"decimal(10,3)", "decimal(10,3)", "decimal(10,3)", "decimal(10,3)", "decimal(19,3)", "decimal(20,3)", "decimal(19,3)", "decimal(20,3)", "decimal(38,3)", "decimal(50,3)", "decimal(3,2)", "decimal(10,3)", "decimal(10,3)", "decimal(7,2)", "decimal(8,2)"}
		for _, table := range []string{"dst", "v"} {
			t.Run(table, func(t *testing.T) {
				rows, err := db.QueryContext(ctx, "select column_type from information_schema.columns where table_schema=? and table_name=? order by ordinal_position", schema, table)
				require.NoError(t, err)
				defer rows.Close()
				var got []string
				for rows.Next() {
					var typ string
					require.NoError(t, rows.Scan(&typ))
					got = append(got, typ)
				}
				require.NoError(t, rows.Err())
				require.Equal(t, expected, got)
			})
		}
		assertResult := func(rows *sql.Rows) {
			t.Helper()
			columns, err := rows.ColumnTypes()
			require.NoError(t, err)
			require.Len(t, columns, len(expected))
			for i, column := range columns {
				var width, scale int64
				_, err := fmt.Sscanf(expected[i], "decimal(%d,%d)", &width, &scale)
				require.NoError(t, err)
				gotWidth, gotScale, ok := column.DecimalSize()
				require.True(t, ok, column.Name())
				require.Equal(t, width, gotWidth, column.Name())
				require.Equal(t, scale, gotScale, column.Name())
			}
			for row := 0; row < 3; row++ {
				require.True(t, rows.Next())
				values := make([]sql.NullString, len(expected))
				dest := make([]any, len(expected))
				for i := range values {
					dest[i] = &values[i]
				}
				require.NoError(t, rows.Scan(dest...))
				for i, value := range values {
					if row == 2 {
						require.False(t, value.Valid)
						continue
					}
					want := "1.234"
					if i == 6 || i == 7 {
						want = "0.766"
					}
					if i == 10 || i == 13 || i == 14 {
						want = "1.23"
					}
					if row == 1 && i != 7 {
						want = "-" + want
					}
					require.Equal(t, sql.NullString{String: want, Valid: true}, value, columns[i].Name())
				}
			}
			require.False(t, rows.Next())
		}
		for _, query := range []string{
			"select " + projection + " from " + schema + ".src order by id",
			"select * from " + schema + ".dst order by by_column desc",
			"select * from " + schema + ".v order by by_column desc",
		} {
			func() {
				rows, err := db.QueryContext(ctx, query)
				require.NoError(t, err)
				defer rows.Close()
				assertResult(rows)
				require.NoError(t, rows.Err())
			}()
		}
		stmt, err := db.PrepareContext(ctx, "select "+projection+" from "+schema+".src order by id")
		require.NoError(t, err)
		defer stmt.Close()
		rows, err := stmt.QueryContext(ctx)
		require.NoError(t, err)
		defer rows.Close()
		assertResult(rows)
		require.NoError(t, rows.Err())
		var zero sql.NullString
		require.NoError(t, db.QueryRowContext(ctx, "select MOD(d,0) from "+schema+".src where id=1").Scan(&zero))
		require.False(t, zero.Valid)
	})
}
