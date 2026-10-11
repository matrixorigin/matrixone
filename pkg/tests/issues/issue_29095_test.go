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
	"github.com/stretchr/testify/require"
)

// Reuse the authenticated base cluster: the invariant needs the real role,
// snapshot, catalog and frontend boundaries, but no additional service fixture.
func TestIssue29095RewriteMetadataVisibility(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		open := func(user string) *sql.DB {
			db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", user, cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			db.SetMaxOpenConns(1)
			return db
		}
		admin := open("dump")
		defer admin.Close()
		var oldMode int
		require.NoError(t, admin.QueryRowContext(ctx, "select @@global.lower_case_table_names").Scan(&oldMode))
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			execSQLRequire(t, cleanup, admin, fmt.Sprintf("set global lower_case_table_names=%d", oldMode))
		}()
		for _, mode := range []int{1, 2} {
			t.Run(fmt.Sprintf("mode%d", mode), func(t *testing.T) {
				execSQLRequire(t, ctx, admin, fmt.Sprintf("set global lower_case_table_names=%d", mode))
				owner := open("dump")
				defer owner.Close()
				const database = "DbCase29095"
				const role = "rewrite_metadata_role"
				const user = "rewrite_metadata_user"
				const snapshot = "rewrite_metadata_snapshot"
				defer func() {
					cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
					defer stop()
					cleanupOwner := owner
					if mode == 2 {
						// DROP's internal catalog lookup currently normalizes names.
						// Clean preserved-case objects in mode0 through a fresh session.
						execSQLRequire(t, cleanup, owner, "set global lower_case_table_names=0")
						cleanupOwner = open("dump")
						defer cleanupOwner.Close()
					}
					for _, statement := range []string{
						"drop user if exists " + user,
						"drop role if exists " + role,
						"drop snapshot if exists " + snapshot,
						"drop database if exists " + database,
					} {
						execSQLRequire(t, cleanup, cleanupOwner, statement)
					}
					var n int
					require.NoError(t, cleanupOwner.QueryRowContext(cleanup, "select count(*) from mo_catalog.mo_user where user_name='"+user+"'").Scan(&n))
					require.Zero(t, n)
					require.NoError(t, cleanupOwner.QueryRowContext(cleanup, "select count(*) from mo_catalog.mo_role where role_name='"+role+"'").Scan(&n))
					require.Zero(t, n)
					require.NoError(t, cleanupOwner.QueryRowContext(cleanup, "select count(*) from mo_catalog.mo_database where lower(datname)=lower('"+database+"')").Scan(&n))
					require.Zero(t, n)
				}()
				for _, statement := range []string{
					"create database " + database,
					"create table " + database + ".Controlled(id int, secret int)",
					"create table " + database + ".Unrelated(id int, secret int)",
					"create table " + database + ".PublicControl(id int, secret int)",
					"create role " + role,
					"create user " + user + " identified by '111' default role " + role,
					"grant connect on account * to " + role,
					"grant select on table " + database + ".Controlled to " + role,
					"grant select on table " + database + ".PublicControl to " + role,
				} {
					execSQLRequire(t, ctx, owner, statement)
				}
				if mode == 1 {
					for _, statement := range []string{
						"create table " + database + ".Historical(id int)",
						"create snapshot " + snapshot + " for table " + database + " Historical",
						"alter table " + database + ".Historical add column secret int",
						"grant select on table " + database + ".Historical to " + role,
					} {
						execSQLRequire(t, ctx, owner, statement)
					}
				}
				if mode == 2 {
					// Confirm this is the preserved-spelling domain reported in review.
					var schema, table string
					require.NoError(t, owner.QueryRowContext(ctx, "select reldatabase,relname from mo_catalog.mo_tables where lower(reldatabase)=lower('"+database+"') and lower(relname)='controlled'").Scan(&schema, &table))
					require.Equal(t, database, schema)
					require.Equal(t, "Controlled", table)
				}
				type scenario struct {
					name, table, rule string
					direct, metadata  []string
					invalid           bool
				}
				scenarios := []scenario{
					{"qualified column", "Controlled", "select Controlled.id from " + database + ".Controlled", []string{"id"}, []string{"id"}, false},
					{"alias column", "Controlled", "select a.id from " + database + ".Controlled as a", []string{"id"}, []string{"id"}, false},
					{"qualified star", "Controlled", "select Controlled.* from " + database + ".Controlled", []string{"id", "secret"}, []string{"id", "secret"}, false},
					{"invalid column", "Controlled", "select absent.secret from " + database + ".Controlled", nil, nil, true},
					{"invalid star", "Controlled", "select absent.* from " + database + ".Controlled", nil, nil, true},
					{"star then invalid column", "Controlled", "select *, absent.secret from " + database + ".Controlled", nil, nil, true},
				}
				if mode == 1 {
					scenarios = append(scenarios,
						scenario{"historical schema", "Historical", "select * from " + database + ".Historical {snapshot='" + snapshot + "'}", []string{"id"}, nil, false},
						scenario{"current schema", "Historical", "select * from " + database + ".Historical", []string{"id", "secret"}, []string{"id", "secret"}, false},
					)
				}
				for _, scenario := range scenarios {
					t.Run(scenario.name, func(t *testing.T) {
						execSQLRequire(t, ctx, owner, "alter role "+role+" add rule \""+scenario.rule+"\" on table "+database+"."+scenario.table)
						defer execSQLRequire(t, ctx, owner, "alter role "+role+" drop rule on table "+database+"."+scenario.table)
						restricted := open(user)
						defer restricted.Close()
						execSQLRequire(t, ctx, restricted, "set enable_remap_hint=1")
						var activeMode int
						require.NoError(t, restricted.QueryRowContext(ctx, "select @@lower_case_table_names").Scan(&activeMode))
						require.Equal(t, mode, activeMode)
						for _, prepared := range []bool{false, true} {
							query := func(statement string, invalid bool, consume func(*sql.Rows)) {
								var rows *sql.Rows
								var err error
								if prepared {
									stmt, prepareErr := restricted.PrepareContext(ctx, statement)
									if prepareErr != nil {
										require.True(t, invalid, "prepare failed: %s", statement)
										var sqlErr *mysql.MySQLError
										require.ErrorAs(t, prepareErr, &sqlErr)
										return
									}
									defer stmt.Close()
									rows, err = stmt.QueryContext(ctx)
								} else {
									rows, err = restricted.QueryContext(ctx, statement)
								}
								if rows != nil {
									defer rows.Close()
								}
								if invalid {
									require.Error(t, err)
									var sqlErr *mysql.MySQLError
									require.ErrorAs(t, err, &sqlErr)
									return
								}
								require.NoError(t, err)
								consume(rows)
								require.NoError(t, rows.Err())
							}
							query("select * from "+database+"."+scenario.table, scenario.invalid, func(rows *sql.Rows) {
								cols, err := rows.Columns()
								require.NoError(t, err)
								require.Equal(t, scenario.direct, cols)
							})
							query("select column_name from information_schema.columns where lower(table_schema)=lower('"+database+"') and lower(table_name)=lower('"+scenario.table+"') order by ordinal_position", false, func(rows *sql.Rows) {
								var cols []string
								for rows.Next() {
									var col string
									require.NoError(t, rows.Scan(&col))
									cols = append(cols, col)
								}
								require.Equal(t, scenario.metadata, cols, "prepared=%v", prepared)
							})
							// Caller predicates and existing object authorization remain effective.
							var n int
							require.NoError(t, restricted.QueryRowContext(ctx, "select count(*) from information_schema.columns where lower(table_schema)=lower('"+database+"') and lower(table_name)='unrelated'").Scan(&n))
							require.Zero(t, n)
							require.NoError(t, restricted.QueryRowContext(ctx, "select count(*) from information_schema.columns where lower(table_schema)=lower('"+database+"') and lower(table_name)='publiccontrol'").Scan(&n))
							require.Equal(t, 2, n)
						}
					})
				}
			})
		}
	})
}
