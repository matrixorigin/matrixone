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
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue27088VolatilePreparedINEvaluatesLeftOnce(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := testutils.GetDatabaseName(t)
		execIssue27088VolatileSQL(t, ctx, conn, fmt.Sprintf("create database `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()
		execIssue27088VolatileSQL(t, ctx, conn, fmt.Sprintf("use `%s`", dbName))
		execIssue27088VolatileSQL(t, ctx, conn, "create table volatile_rows (id int)")
		execIssue27088VolatileSQL(t, ctx, conn, "insert into volatile_rows values (1)")

		for _, test := range []struct {
			name  string
			notIn bool
			tuple bool
		}{
			{name: "scalar in"},
			{name: "scalar not in", notIn: true},
			{name: "tuple in", tuple: true},
			{name: "tuple not in", notIn: true, tuple: true},
		} {
			t.Run(test.name, func(t *testing.T) {
				operator := "in"
				if test.notIn {
					operator = "not in"
				}
				want := 0
				if test.notIn {
					want = 1
				}

				var query string
				var args []any
				if test.tuple {
					execIssue27088VolatileSQL(t, ctx, conn,
						"drop sequence if exists volatile_tuple_a")
					execIssue27088VolatileSQL(t, ctx, conn,
						"drop sequence if exists volatile_tuple_b")
					execIssue27088VolatileSQL(t, ctx, conn,
						"create sequence volatile_tuple_a increment 1 start with 1 no cycle")
					execIssue27088VolatileSQL(t, ctx, conn,
						"create sequence volatile_tuple_b increment 1 start with 10 no cycle")
					query = fmt.Sprintf(
						"select count(*) from volatile_rows where "+
							"(nextval('volatile_tuple_a'), nextval('volatile_tuple_b')) %s ((?, ?), (?, ?))",
						operator)
					args = []any{"1", "9", "2", "11"}
				} else {
					execIssue27088VolatileSQL(t, ctx, conn,
						"drop sequence if exists volatile_scalar")
					execIssue27088VolatileSQL(t, ctx, conn,
						"create sequence volatile_scalar increment 1 start with 1 no cycle")
					query = fmt.Sprintf(
						"select count(*) from volatile_rows where nextval('volatile_scalar') %s (?, ?)", operator)
					args = []any{"0", "2"}
				}

				stmt, err := conn.PrepareContext(ctx, query)
				require.NoError(t, err)
				defer stmt.Close()
				var got int
				require.NoError(t, stmt.QueryRowContext(ctx, args...).Scan(&got))

				if test.tuple {
					var currentA, currentB int64
					require.NoError(t, conn.QueryRowContext(ctx,
						"select currval('volatile_tuple_a'), currval('volatile_tuple_b')").
						Scan(&currentA, &currentB))
					require.Equal(t, int64(1), currentA)
					require.Equal(t, int64(10), currentB)
				} else {
					var current int64
					require.NoError(t, conn.QueryRowContext(ctx,
						"select currval('volatile_scalar')").Scan(&current))
					require.Equal(t, int64(1), current)
				}
				require.Equal(t, want, got)
			})
		}
	})
}

func execIssue27088VolatileSQL(t *testing.T, ctx context.Context, conn *sql.Conn, stmt string) {
	t.Helper()
	_, err := conn.ExecContext(ctx, stmt)
	require.NoError(t, err)
}
