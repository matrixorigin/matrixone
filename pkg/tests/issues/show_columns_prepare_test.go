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
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestShowColumnsPreparedWhere(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(statement string) {
			_, err := conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		name := testutils.GetDatabaseName(t)
		exec("create database `" + name + "`")
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, err := conn.ExecContext(cleanupCtx, "drop database `"+name+"`")
			require.NoError(t, err)
		}()
		exec("use `" + name + "`")
		exec("create table src (x int, y varchar(9))")
		exec("create view v as select x,y from src")

		for _, tc := range []struct {
			name string
			sql  string
			full bool
		}{
			{"table columns", "show columns from src where Field=?", false},
			{"table full fields", "show full fields from src where Field=?", true},
			{"view fields", "show fields from v where Field=?", false},
			{"view full columns", "show full columns from v where Field=?", true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				stmt, err := conn.PrepareContext(ctx, tc.sql)
				require.NoError(t, err, "must use COM_STMT_PREPARE, not client interpolation")
				defer stmt.Close()
				_, err = conn.ExecContext(ctx, "prepare show_where from '"+strings.ReplaceAll(tc.sql, "'", "''")+"'")
				require.NoError(t, err)
				defer func() {
					_, err := conn.ExecContext(ctx, "deallocate prepare show_where")
					require.NoError(t, err)
				}()
				for _, value := range []any{"x", "absent", "y", nil, "x"} {
					rows, err := stmt.QueryContext(ctx, value)
					require.NoError(t, err)
					checkShowColumnsPreparedRows(t, rows, tc.full, value, "int")
					_, err = conn.ExecContext(ctx, "set @show_field=?", value)
					require.NoError(t, err)
					rows, err = conn.QueryContext(ctx, "execute show_where using @show_field")
					require.NoError(t, err)
					checkShowColumnsPreparedRows(t, rows, tc.full, value, "int")
				}

				// The saved predicate must survive both parameter reuse and a
				// schema-dependent replan, including the live view description.
				_, err = conn.ExecContext(ctx, "alter table src modify column x bigint")
				require.NoError(t, err)
				defer func() {
					_, err := conn.ExecContext(ctx, "alter table src modify column x int")
					require.NoError(t, err)
				}()
				rows, err := stmt.QueryContext(ctx, "x")
				require.NoError(t, err)
				checkShowColumnsPreparedRows(t, rows, tc.full, "x", "bigint")
				_, err = conn.ExecContext(ctx, "set @show_field='x'")
				require.NoError(t, err)
				rows, err = conn.QueryContext(ctx, "execute show_where using @show_field")
				require.NoError(t, err)
				checkShowColumnsPreparedRows(t, rows, tc.full, "x", "bigint")
			})
		}

		t.Run("unauthorized targets remain inaccessible", func(t *testing.T) {
			user := strings.ToLower(name + "_reader")
			_, err := conn.ExecContext(ctx, "create user `"+user+"` identified by 'show_where_password'")
			require.NoError(t, err)
			defer func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cleanupCancel()
				_, err := conn.ExecContext(cleanupCtx, "drop user `"+user+"`")
				require.NoError(t, err)
			}()
			reader, err := sql.Open("mysql", fmt.Sprintf("%s:show_where_password@tcp(127.0.0.1:%d)/?interpolateParams=false", user, cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			defer reader.Close()
			readerConn, err := reader.Conn(ctx)
			require.NoError(t, err)
			defer readerConn.Close()
			for _, target := range []string{"src", "v"} {
				t.Run(target, func(t *testing.T) {
					query := "show columns from `" + name + "`." + target + " where Field="
					rows, plainErr := readerConn.QueryContext(ctx, query+"'x'")
					if rows != nil {
						defer rows.Close()
						require.NoError(t, rows.Err())
					}
					var plainMySQLError *mysql.MySQLError
					require.ErrorAs(t, plainErr, &plainMySQLError)
					stmt, prepareErr := readerConn.PrepareContext(ctx, query+"?")
					if stmt != nil {
						defer stmt.Close()
					}
					var prepareMySQLError *mysql.MySQLError
					require.ErrorAs(t, prepareErr, &prepareMySQLError)
					require.Equal(t, plainMySQLError.Number, prepareMySQLError.Number)
					_, prepareErr = readerConn.ExecContext(ctx, "prepare denied_show from '"+query+"?'")
					require.ErrorAs(t, prepareErr, &prepareMySQLError)
					require.Equal(t, plainMySQLError.Number, prepareMySQLError.Number)
					var one int
					require.NoError(t, readerConn.QueryRowContext(ctx, "select 1").Scan(&one))
					require.Equal(t, 1, one)
				})
			}
		})
	})
}

func checkShowColumnsPreparedRows(t *testing.T, rows *sql.Rows, full bool, value any, xType string) {
	t.Helper()
	defer func() { require.NoError(t, rows.Close()) }()
	wantHeadings := []string{"Field", "Type", "Null", "Key", "Default", "Extra", "Comment"}
	if full {
		wantHeadings = []string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}
	}
	headings, err := rows.Columns()
	require.NoError(t, err)
	require.Equal(t, wantHeadings, headings)
	if value == nil || value == "absent" {
		require.False(t, rows.Next(), "an unmatched or NULL binding must return no columns")
		require.NoError(t, rows.Err())
		return
	}
	require.True(t, rows.Next())
	values := make([]sql.NullString, len(headings))
	args := make([]any, len(headings))
	for i := range args {
		args[i] = &values[i]
	}
	require.NoError(t, rows.Scan(args...))
	require.Equal(t, sql.NullString{String: value.(string), Valid: true}, values[0])
	wantType := xType
	if value == "y" {
		wantType = "varchar(9)"
	}
	require.Equal(t, sql.NullString{String: wantType, Valid: true}, values[1])
	nullIndex := 2
	if full {
		nullIndex = 3
		require.Equal(t, "select,insert,update,references", values[7].String)
	}
	require.Equal(t, "YES", values[nullIndex].String)
	require.False(t, values[nullIndex+2].Valid, "Default is SQL NULL")
	require.False(t, rows.Next(), "only the bound column must be returned")
	require.NoError(t, rows.Err())
}
