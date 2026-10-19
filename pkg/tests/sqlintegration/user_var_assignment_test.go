// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestUserVariableAssignmentResolvedAdmissionBoundaries(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		const schema = "user_var_admission_29909"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec := func(statement string) {
			t.Helper()
			_, err := db.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		exec("create database " + schema)
		exec("use " + schema)
		exec("create table src(id int primary key)")
		exec("insert into src values(1),(2),(3)")
		defer func() { _, _ = db.ExecContext(context.Background(), "set session sql_select_limit=default") }()
		unchanged := func(t *testing.T) {
			t.Helper()
			var a int
			require.NoError(t, db.QueryRowContext(ctx, "select @a").Scan(&a))
			require.Zero(t, a, "rejected execution must not publish any assignment")
		}
		for _, group := range []string{"1", "k"} {
			t.Run("group by "+group, func(t *testing.T) {
				exec("set @a=0")
				_, err := db.ExecContext(ctx, "select @a:=id as k from src group by "+group)
				require.ErrorContains(t, err, "GROUP BY")
				unchanged(t)
			})
		}
		var groups int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from (select id as k from src group by k) s").Scan(&groups))
		require.Equal(t, 3, groups)
		// Reuse the same prepared pipeline after a successful unlimited run.
		exec("prepare assignment_limit from 'select @a:=@a+1 from src'")
		defer func() { _, _ = db.ExecContext(context.Background(), "deallocate prepare assignment_limit") }()
		for round := 0; round < 2; round++ {
			exec("set session sql_select_limit=default")
			exec("set @a=0")
			exec("execute assignment_limit")
			var a int
			require.NoError(t, db.QueryRowContext(ctx, "select @a").Scan(&a))
			require.Equal(t, 3, a)
			exec("set session sql_select_limit=1")
			for _, statement := range []string{"select @a:=@a+1 from src", "execute assignment_limit"} {
				t.Run(fmt.Sprintf("round %d %s", round, statement), func(t *testing.T) {
					exec("set @a=0")
					_, err := db.ExecContext(ctx, statement)
					require.ErrorContains(t, err, "LIMIT")
					unchanged(t)
				})
			}
		}
	})
}

func TestUserVariableAssignmentViewAuthoringIsRejected(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		schema := "user_var_assignment_29909"
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)

		require.NoError(t, db.PingContext(ctx))
		_, err = db.ExecContext(ctx, "drop database if exists "+schema)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "create database "+schema)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "use "+schema)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "create view v as select 17 as x")
		require.NoError(t, err)

		for _, statement := range []string{
			"create or replace view v as select @view_var := 1 as x",
			"alter view v as select @view_var := 2 as x",
		} {
			_, err = db.ExecContext(ctx, statement)
			require.ErrorContains(t, err, "view definitions is not supported")

			var value int
			require.NoError(t, db.QueryRowContext(ctx, "select x from v").Scan(&value))
			require.Equal(t, 17, value)
		}
	})
}

func TestUserVariableAssignmentNestedReadBeforeWriteIsRejected(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		defer db.Close()
		require.NoError(t, db.PingContext(ctx))

		for _, statement := range []string{
			"select @b as prior, @a := @b := @b + 1 as current",
			"select @B as prior, @a := @b := @B + 1 as current",
		} {
			_, err = db.ExecContext(ctx, "set @b = 0")
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, statement)
			require.ErrorContains(t, err, "reading a user variable before its assignment projection")

			var retained int
			require.NoError(t, db.QueryRowContext(ctx, "select @b").Scan(&retained))
			require.Equal(t, 0, retained)

			var outer sql.NullString
			require.NoError(t, db.QueryRowContext(ctx, "select @a").Scan(&outer))
			require.False(t, outer.Valid, "the rejected admission path must not publish the outer assignment")
		}
	})
}

func TestUserVariableAssignmentWhereReadBeforeWriteIsRejected(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		schema := "user_var_assignment_where_29909"
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		require.NoError(t, db.PingContext(ctx))
		_, err = db.ExecContext(ctx, "drop database if exists "+schema)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "create database "+schema)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "use "+schema)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "create table src (id int)")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "insert into src values (1), (2), (3), (4), (5), (6)")
		require.NoError(t, err)

		for _, statement := range []string{
			"select @a := @a + 1 from src where @a < 2",
			"select @a := @b := @b + 1 from src where @B < 2",
		} {
			_, err = db.ExecContext(ctx, "set @a = 0, @b = 0")
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, statement)
			require.ErrorContains(t, err,
				"reading a user variable in WHERE before its assignment projection")

			var a, b int
			require.NoError(t, db.QueryRowContext(ctx, "select @a, @b").Scan(&a, &b))
			require.Equal(t, 0, a)
			require.Equal(t, 0, b)
		}
	})
}

func TestUserVariableAssignmentCaseConditionHistory(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		schema := "user_var_assignment_case_29909"
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)

		require.NoError(t, db.PingContext(ctx))
		_, err = db.ExecContext(ctx, "drop database if exists "+schema)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "create database "+schema)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "use "+schema)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "create table src (id int)")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "insert into src values (1), (2), (3)")
		require.NoError(t, err)

		for _, tc := range []struct {
			name      string
			query     string
			want      [][3]int64
			finalWant [3]int64
		}{
			{
				name: "skip before first write",
				query: `select @a := case
					when @a = 0 then 1
					when (@b := 1) = 1 then 2
					else 3
				end, @b, @c from src`,
				want:      [][3]int64{{1, 0, 0}, {2, 1, 0}, {2, 1, 0}},
				finalWant: [3]int64{2, 1, 0},
			},
			{
				name: "skip after write",
				query: `select @a := case
					when id = 2 then 2
					when (@b := 1) = 1 then 1
					else 3
				end, @b, @c from src`,
				want:      [][3]int64{{1, 1, 0}, {2, 1, 0}, {1, 1, 0}},
				finalWant: [3]int64{1, 1, 0},
			},
			{
				name: "wrapped nested target",
				query: `select @a := case
					when id = 1 then 2
					when abs(@b := @c := 1) = 1 then 1
					else 3
				end, @b, @c from src`,
				want:      [][3]int64{{2, 0, 0}, {1, 1, 1}, {1, 1, 1}},
				finalWant: [3]int64{1, 1, 1},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err = db.ExecContext(ctx, "set @a = 0, @b = 0, @c = 0")
				require.NoError(t, err)
				rows, err := db.QueryContext(ctx, tc.query)
				require.NoError(t, err)
				defer func() { require.NoError(t, rows.Close()) }()
				for row, expected := range tc.want {
					require.True(t, rows.Next(), "missing result row %d", row)
					var a, b, c int64
					require.NoError(t, rows.Scan(&a, &b, &c))
					require.Equal(t, expected[0], a, "row %d", row)
					require.Equal(t, expected[1], b, "row %d", row)
					require.Equal(t, expected[2], c, "row %d", row)
				}
				require.False(t, rows.Next(), "unexpected extra result row")
				require.NoError(t, rows.Err())

				var a, b, c int64
				require.NoError(t, db.QueryRowContext(ctx, "select @a, @b, @c").Scan(&a, &b, &c))
				require.Equal(t, tc.finalWant[0], a)
				require.Equal(t, tc.finalWant[1], b)
				require.Equal(t, tc.finalWant[2], c)
			})
		}
	})
}
