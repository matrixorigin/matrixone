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

// Reuse the canonical one-CN fixture; each scenario owns two source rows and
// one ODKU target. Repeat on the same instance after checking database teardown.
func TestDecimalIntegerDivPublicContracts(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		setupCtx, cancelSetup := context.WithTimeout(t.Context(), 120*time.Second)
		defer cancelSetup()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(setupCtx)
		require.NoError(t, err)
		defer conn.Close()
		const schema = "decimal_div_domain_30158"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		_, err = conn.ExecContext(setupCtx, "drop database if exists "+schema)
		require.NoError(t, err)
		cancelSetup()
		for round := 0; round < 2; round++ {
			t.Run(fmt.Sprintf("round_%d", round), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
				defer cancel()
				// Fatal SQL errors belong to this round, never the parent test.
				exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
				exec("create database " + schema)
				exec("use " + schema)
				exec("create table src(id int primary key,d decimal(10,3),w decimal(18,3),i int,u bigint unsigned)")
				exec("insert into src values(1,5.678,5.678,2,2),(2,-5.678,-5.678,-2,2)")
				for _, object := range []string{"table", "view"} {
					for _, tc := range []struct{ name, query, want string }{
						{"narrow", "select d div i as q from src", "int"},
						{"wide", "select w div i as q from src", "bigint"},
						{"unsigned", "select d div u as q from src where id=1", "int unsigned"},
						{"folded", "select cast(5.678 as decimal(10,3)) div cast(2 as signed) as q", "int"},
						{"derived", "select q from (select d div i as q from src) s", "int"},
						{"ordered", "select d div i as q from src order by id desc limit 1", "int"},
						{"signed_threshold", "select cast(d as decimal(11,3)) div i as q from src", "bigint"},
						{"unsigned_threshold", "select cast(d as decimal(11,3)) div u as q from src where id=1", "int unsigned"},
					} {
						name := object + "_" + tc.name
						exec("create " + object + " " + name + " as " + tc.query)
						var got string
						require.NoError(t, conn.QueryRowContext(ctx, "select column_type from information_schema.columns where table_schema=? and table_name=? and column_name='q'", schema, name).Scan(&got))
						require.Equal(t, tc.want, got, name)
						var q int64
						require.NoError(t, conn.QueryRowContext(ctx, "select q from "+name+" limit 1").Scan(&q))
						require.Equal(t, int64(2), q, name)
						if object == "view" {
							for _, consumer := range []string{"table", "view"} {
								outer := consumer + "_via_" + name
								exec("create " + consumer + " " + outer + " as select q from " + name)
								require.NoError(t, conn.QueryRowContext(ctx, "select column_type from information_schema.columns where table_schema=? and table_name=? and column_name='q'", schema, outer).Scan(&got))
								require.Equal(t, tc.want, got, outer)
								require.NoError(t, conn.QueryRowContext(ctx, "select q from "+outer+" limit 1").Scan(&q))
								require.Equal(t, int64(2), q, outer)
							}
						}
					}
				}
				exec("insert into src values(3,null,null,2,2)")
				for _, peer := range []string{"i", "u"} {
					name := "null_" + peer
					exec("create view " + name + " as select d div " + peer + " as q from src where id=3")
					for _, consumer := range []string{"table", "view"} {
						outer := consumer + "_via_" + name
						exec("create " + consumer + " " + outer + " as select q from " + name)
						var isNull bool
						require.NoError(t, conn.QueryRowContext(ctx, "select q is null from "+outer).Scan(&isNull))
						require.True(t, isNull, outer)
					}
				}
				exec("create view negative_unsigned as select d div u as q from src where id=2")
				var negative int64
				require.ErrorContains(t, conn.QueryRowContext(ctx, "select q from negative_unsigned").Scan(&negative), "out of range")
				// The unsigned overload shares SELECT diagnostics with scalar DIV.
				for _, mode := range []string{"", "ERROR_FOR_DIVISION_BY_ZERO"} {
					exec("set session sql_mode='" + mode + "'")
					for _, tc := range []struct {
						expression string
						null       bool
						value      int64
						warnings   int
					}{
						{expression: "cast(1 as decimal(10,3)) div cast(0 as unsigned)", null: true, warnings: 1},
						{expression: "cast(null as decimal(10,3)) div cast(0 as unsigned)", null: true},
						{expression: "if(false,cast(1 as decimal(10,3)) div cast(0 as unsigned),7)", value: 7},
						{expression: "cast(5 as decimal(10,3)) div cast(2 as unsigned)", value: 2},
					} {
						var q sql.NullInt64
						require.NoError(t, conn.QueryRowContext(ctx, "select "+tc.expression).Scan(&q))
						require.Equal(t, !tc.null, q.Valid, tc.expression)
						if q.Valid {
							require.Equal(t, tc.value, q.Int64, tc.expression)
						}
						func() {
							rows, err := conn.QueryContext(ctx, "show warnings")
							require.NoError(t, err)
							defer rows.Close()
							count := 0
							for rows.Next() {
								var level, message string
								var code int
								require.NoError(t, rows.Scan(&level, &code, &message))
								require.Equal(t, "Warning", level)
								require.Equal(t, 1365, code)
								require.Equal(t, "Division by 0", message)
								count++
							}
							require.NoError(t, rows.Err())
							want := tc.warnings
							if mode == "" {
								want = 0
							}
							require.Equal(t, want, count, tc.expression)
						}()
					}
				}
				exec("set session sql_mode=''")
				for _, mode := range []string{"", "NO_UNSIGNED_SUBTRACTION"} {
					exec("set session sql_mode='" + mode + "'")
					var q int64
					err := conn.QueryRowContext(ctx, "select d div u from src where id=2").Scan(&q)
					require.ErrorContains(t, err, "out of range")
					require.NoError(t, conn.QueryRowContext(ctx, "select cast(-1 as decimal(10,3)) div cast(2 as unsigned)").Scan(&q))
					require.Zero(t, q)
				}
				var max string
				require.NoError(t, conn.QueryRowContext(ctx, "select cast('18446744073709551615' as decimal(20,0)) div cast(1 as unsigned)").Scan(&max))
				require.Equal(t, "18446744073709551615", max)
				stmt, err := conn.PrepareContext(ctx, "select d div ? from src where id=2")
				require.NoError(t, err)
				func() {
					defer stmt.Close()
					for _, tc := range []struct {
						v    any
						want int64
						bad  bool
					}{{uint64(2), 0, true}, {int64(2), -2, false}, {int64(-2), 2, false}, {uint64(2), 0, true}} {
						var got int64
						err := stmt.QueryRowContext(ctx, tc.v).Scan(&got)
						if tc.bad {
							require.ErrorContains(t, err, "out of range")
						} else {
							require.NoError(t, err)
							require.Equal(t, tc.want, got)
						}
					}
				}()
				exec("create table dst(id int primary key,q bigint)")
				exec("insert into dst values(2,99)")
				_, err = conn.ExecContext(ctx, "insert into dst select id,d div u from src where id=2 on duplicate key update q=values(q)")
				require.ErrorContains(t, err, "out of range")
				var kept int64
				require.NoError(t, conn.QueryRowContext(ctx, "select q from dst where id=2").Scan(&kept))
				require.Equal(t, int64(99), kept)
				// Teardown must not inherit the remaining arithmetic/DDL budget.
				// Still require deletion and verify it before reusing the fixture.
				cleanup, cancelCleanup := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancelCleanup()
				_, err = conn.ExecContext(cleanup, "use mo_catalog")
				require.NoError(t, err)
				_, err = conn.ExecContext(cleanup, "drop database "+schema)
				require.NoError(t, err)
				var remaining int
				require.NoError(t, conn.QueryRowContext(cleanup, "select count(*) from information_schema.schemata where schema_name=?", schema).Scan(&remaining))
				require.Zero(t, remaining)
			})
			if t.Failed() {
				return
			}
		}
	})
}
