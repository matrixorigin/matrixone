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
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
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
				t.Run("nullable_sibling_keys", func(t *testing.T) {
					for _, tc := range []struct{ name, typ, value, entry string }{
						{"integer", "int", "7", "7"},
						{"binary", "varchar(30) collate utf8mb4_bin", "'dup'", "dup"},
					} {
						t.Run(tc.name, func(t *testing.T) {
							table := "u_sibling_" + tc.name
							exec(t, "create table "+table+"(id int primary key,s varchar(30) collate utf8mb4_unicode_ci,k "+tc.typ+",unique key uk_s(s),unique key uk_k(k))")
							exec(t, "insert into "+table+" values(1,'Alpha',"+tc.value+")")
							duplicate(t, "insert into "+table+" values(6,'Beta',NULL),(7,'Gamma',"+tc.value+")", tc.entry, "uk_k")
							var count int
							require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+table).Scan(&count))
							require.Equal(t, 1, count)
						})
					}
				})
				t.Run("optimistic_si", func(t *testing.T) {
					exec(t, "create table u_concurrent(id int primary key,s varchar(30) collate utf8mb4_unicode_ci,unique key uk_s(s))")
					var previousPKCheck int
					require.NoError(t, conn.QueryRowContext(ctx, "select @@mo_pk_check_by_dn").Scan(&previousPKCheck))
					defer func() {
						cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
						defer stop()
						_, err := conn.ExecContext(cleanup, fmt.Sprintf("set session mo_pk_check_by_dn=%d", previousPKCheck))
						assert.NoError(t, err)
					}()
					restore := setIssue27718TxnConfig([]embed.ServiceOperator{cn}, pbtxn.TxnMode_Optimistic, pbtxn.TxnIsolation_SI)
					defer restore()
					defer func() {
						fence, stop := context.WithTimeout(context.Background(), 10*time.Second)
						defer stop()
						assert.NoError(t, waitIssue27718Commits(fence, []issue27718CommitClient{cn.RawService().(cnservice.Service).GetTxnClient()}))
					}()
					rollback := func(c *sql.Conn) {
						cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
						defer stop()
						_, err := c.ExecContext(cleanup, "rollback")
						assert.NoError(t, err)
					}
					for _, pkCheck := range []int{0, 1} {
						t.Run(fmt.Sprintf("tn_check=%d", pkCheck), func(t *testing.T) {
							exec(t, fmt.Sprintf("set session mo_pk_check_by_dn=%d", pkCheck))
							for _, tc := range []struct{ name, query, entry string }{
								{"single", "insert into u_single values(40,'alpha',7)", "alpha"},
								{"prefix", "insert into u_prefix values(40,'alphaZ',7)", "alp"},
								{"composite", "insert into u_composite values(40,'alpha',7)", "(alpha,7)"},
								{"update", "update u_single set s='alpha',n=7 where id=3", "alpha"},
							} {
								t.Run(tc.name, func(t *testing.T) {
									exec(t, "begin")
									defer rollback(conn)
									_, err := conn.ExecContext(ctx, tc.query)
									if err == nil {
										_, err = conn.ExecContext(ctx, "commit")
									}
									checkError(t, err, tc.entry, "uk_named")
								})
							}
							t.Run("prepared_reuse", func(t *testing.T) {
								stmt, err := conn.PrepareContext(ctx, "insert into u_single values(?,?,7)")
								require.NoError(t, err)
								defer stmt.Close()
								for _, value := range []string{"alpha", "BETA"} {
									func(stmt *sql.Stmt) {
										exec(t, "begin")
										defer rollback(conn)
										_, err := stmt.ExecContext(ctx, 40, value)
										if err == nil {
											_, err = conn.ExecContext(ctx, "commit")
										}
										checkError(t, err, value, "uk_named")
									}(stmt)
								}
							})
							var count int
							var value string
							require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from u_single").Scan(&count))
							require.Equal(t, 4, count)
							require.NoError(t, conn.QueryRowContext(ctx, "select s from u_single where id=3").Scan(&value))
							require.Equal(t, "Beta", value)
							t.Run("self_update", func(t *testing.T) {
								exec(t, "begin")
								defer rollback(conn)
								exec(t, "update u_single set s='Alpha' where id=1")
								exec(t, "commit")
							})
							t.Run("delete_reinsert", func(t *testing.T) {
								exec(t, "begin")
								defer rollback(conn)
								exec(t, "delete from u_single where id=1")
								exec(t, "insert into u_single values(1,'Alpha',7)")
								exec(t, "commit")
							})
						})
					}
					t.Run("concurrent_snapshot", func(t *testing.T) {
						exec(t, "set session mo_pk_check_by_dn=off")
						other, err := db.Conn(ctx)
						require.NoError(t, err)
						defer other.Close()
						for _, c := range []*sql.Conn{conn, other} {
							_, err := c.ExecContext(ctx, "begin")
							require.NoError(t, err)
							defer rollback(c)
							var count int
							require.NoError(t, c.QueryRowContext(ctx, "select count(*) from "+database+".u_concurrent").Scan(&count))
							require.Zero(t, count)
						}
						exec(t, "insert into u_concurrent values(1,'Alpha')")
						_, err = other.ExecContext(ctx, "insert into "+database+".u_concurrent values(2,'alpha')")
						require.NoError(t, err)
						exec(t, "commit")
						_, err = other.ExecContext(ctx, "commit")
						var me *mysql.MySQLError
						require.ErrorAs(t, err, &me)
						require.EqualValues(t, 20619, me.Number)
						require.Equal(t, "w-w conflict", me.Message)
						require.NoError(t, waitIssue27718Commits(ctx, []issue27718CommitClient{cn.RawService().(cnservice.Service).GetTxnClient()}))
						var count int
						require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from u_concurrent").Scan(&count))
						require.Equal(t, 1, count)
					})
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
