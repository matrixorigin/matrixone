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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnicodeUniqueDiagnostics(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		for cnIndex := 0; cnIndex < 2; cnIndex++ {
			t.Run(fmt.Sprintf("CN%d", cnIndex), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
				defer cancel()
				cn, err := c.GetCNService(cnIndex)
				require.NoError(t, err)
				db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
				require.NoError(t, err)
				defer db.Close()
				conn, err := db.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				exec := func(t *testing.T, q string) {
					t.Helper()
					_, err := conn.ExecContext(ctx, q)
					require.NoError(t, err, q)
				}
				database := fmt.Sprintf("unicode_unique_diagnostic_cn%d", cnIndex)
				exec(t, "create database "+database)
				defer func() {
					cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
					defer stop()
					_, err := conn.ExecContext(cleanup, "drop database "+database)
					assert.NoError(t, err)
				}()
				exec(t, "use "+database)
				checkError := func(t *testing.T, err error, entry, key string) {
					t.Helper()
					var me *mysql.MySQLError
					require.ErrorAs(t, err, &me)
					require.EqualValues(t, 1062, me.Number)
					require.Equal(t, "23000", string(me.SQLState[:]))
					require.Equal(t, fmt.Sprintf("Duplicate entry '%s' for key '%s'", entry, key), me.Message)
				}
				duplicate := func(t *testing.T, q, entry, key string) {
					t.Helper()
					_, err := conn.ExecContext(ctx, q)
					checkError(t, err, entry, key)
				}
				warning := func(t *testing.T, q, entry, key string, affected int64, expectedWarnings int) {
					t.Helper()
					res, err := conn.ExecContext(ctx, q)
					require.NoError(t, err)
					count, err := res.RowsAffected()
					require.NoError(t, err)
					require.Equal(t, affected, count)
					rows, err := conn.QueryContext(ctx, "show warnings")
					require.NoError(t, err)
					defer rows.Close()
					warnings := 0
					for rows.Next() {
						var level, message string
						var code int
						require.NoError(t, rows.Scan(&level, &code, &message))
						require.Equal(t, "Warning", level)
						require.Equal(t, 1062, code)
						require.Equal(t, fmt.Sprintf("Duplicate entry '%s' for key '%s'", entry, key), message)
						warnings++
					}
					require.NoError(t, rows.Err())
					require.Equal(t, expectedWarnings, warnings)
				}
				for _, tc := range []struct{ name, key, entry string }{
					{"single", "s", "alpha"}, {"prefix", "s(3)", "alp"}, {"composite", "s,n", "(alpha,7)"},
				} {
					t.Run(tc.name, func(t *testing.T) {
						table := "u_" + tc.name
						exec(t, "create table "+table+"(id int primary key,s varchar(30) collate utf8mb4_unicode_ci,n int,unique key uk_named("+tc.key+"))")
						exec(t, "insert into "+table+" values(1,'Alpha',7),(2,NULL,9),(3,'Beta',8)")
						t.Run("existing", func(t *testing.T) { duplicate(t, "insert into "+table+" values(4,'alpha',7)", tc.entry, "uk_named") })
						t.Run("null_before_conflict", func(t *testing.T) {
							duplicate(t, "insert into "+table+" values(6,NULL,11),(7,'alpha',7)", tc.entry, "uk_named")
						})
						t.Run("batch", func(t *testing.T) {
							exec(t, "create table b_"+tc.name+" like "+table)
							duplicate(t, "insert into b_"+tc.name+" values(4,NULL,10),(5,'Alpha',7),(6,'alpha',7)", tc.entry, "uk_named")
							var count int
							require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from b_"+tc.name).Scan(&count))
							require.Zero(t, count)
						})
						t.Run("ignore", func(t *testing.T) {
							warning(t, "insert ignore into "+table+" values(4,'alpha',7),(5,'Gamma',10)", tc.entry, "uk_named", 1, 1)
						})
						t.Run("update", func(t *testing.T) {
							duplicate(t, "update "+table+" set s='alpha',n=7 where id=3", tc.entry, "uk_named")
						})
						t.Run("update_ignore", func(t *testing.T) {
							warning(t, "update ignore "+table+" set s='alpha',n=7 where id=3", tc.entry, "uk_named", 0, 1)
						})
						var value string
						var count int
						require.NoError(t, conn.QueryRowContext(ctx, "select s from "+table+" where id=3").Scan(&value))
						require.Equal(t, "Beta", value)
						require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+table).Scan(&count))
						require.Equal(t, 4, count)
					})
				}
				t.Run("single_constraint_ignore", func(t *testing.T) {
					exec(t, "create table u_only(s varchar(30) collate utf8mb4_unicode_ci,unique key uk_only(s))")
					exec(t, "insert into u_only values('Alpha')")
					warning(t, "insert ignore into u_only values('alpha')", "alpha", "uk_only", 0, 1)
					warning(t, "insert ignore into u_only values('Beta'),('beta')", "beta", "uk_only", 1, 1)
				})

				t.Run("auto_increment_ignore", func(t *testing.T) {
					exec(t, "create table u_auto(id int auto_increment primary key,s varchar(30) collate utf8mb4_unicode_ci,unique key uk_auto(s))")
					exec(t, "insert into u_auto(s) values('Alpha')")
					warning(t, "insert ignore into u_auto(s) values('alpha')", "alpha", "uk_auto", 0, 1)
					exec(t, "insert into u_auto(s) values('Beta')")
				})
				t.Run("repeated_target_update_ignore", func(t *testing.T) {
					// Preserve the two existing alias-stage warnings; both must render the rejected spelling.
					warning(t, "update ignore u_single a join u_single b on a.id=1 and b.id=3 set a.s='Alpha',b.s='ALPHA'", "ALPHA", "uk_named", 0, 2)
				})
				t.Run("prepared_reuse", func(t *testing.T) {
					stmt, err := conn.PrepareContext(ctx, "insert into u_single values(?,?,7)")
					require.NoError(t, err)
					defer stmt.Close()
					for _, value := range []string{"alpha", "BETA"} {
						_, err := stmt.ExecContext(ctx, 20, value)
						checkError(t, err, value, "uk_named")
					}
				})
				t.Run("controls", func(t *testing.T) {
					duplicate(t, "insert into u_single values(1,'Other',1)", "1", "id")
					exec(t, "create table u_binary(s varchar(30) collate utf8mb4_bin unique)")
					exec(t, "insert into u_binary values('Alpha'),('alpha')")
					duplicate(t, "insert into u_binary values('alpha')", "alpha", "s")
				})
				if cnIndex == 0 {
					t.Run("snapshot_clone_restore", func(t *testing.T) {
						snapshot := database + "_snapshot"
						clone := database + "_clone"
						exec(t, "create snapshot "+snapshot+" for database "+database)
						defer func() {
							cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
							defer stop()
							_, err := conn.ExecContext(cleanup, "drop snapshot "+snapshot)
							assert.NoError(t, err)
						}()
						exec(t, "create database "+clone+" clone "+database+" {snapshot='"+snapshot+"'}")
						defer func() {
							cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
							defer stop()
							_, err := conn.ExecContext(cleanup, "drop database "+clone)
							assert.NoError(t, err)
						}()
						duplicate(t, "insert into "+clone+".u_composite values(10,'alpha',7)", "(alpha,7)", "uk_named")
						exec(t, "restore table "+database+".u_prefix{snapshot='"+snapshot+"'}")
						duplicate(t, "insert into "+database+".u_prefix values(10,'alpha',7)", "alp", "uk_named")
					})
				}
			})
		}
	})
}
