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

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue29572LogicalViewAuthorization(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		require.False(t, cn.GetServiceConfig().CN.Frontend.SkipCheckUser)
		open := func(t *testing.T, user string) *sql.DB {
			t.Helper()
			db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", user, cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			return db
		}
		sys := open(t, "dump")
		const account = "issue_29572"
		execSQLRequire(t, ctx, sys, "create account "+account+" admin_name 'admin' identified by '111'")
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			_, err := sys.ExecContext(cleanup, "drop account if exists "+account)
			require.NoError(t, err)
		})
		admin := open(t, account+"#admin#accountadmin")
		for _, q := range []string{
			"create database app", "create table app.t(id int)", "insert into app.t values (42)",
			"create table app.allowed(id int)", "insert into app.allowed values (7)", "create table app.dest(id int)",
			"create view app.v_const as select 42 as id", "create view app.v_chain as select id from app.v_const",
			"create view app.`v_const@ts=42` as select 99 as id", "create view app.`v_table@ts=42` as select id from app.t",
			"create view app.v_table as select id from app.t", "set view_security_type='INVOKER'",
			"create view app.v_invoker as select id from app.v_const", "set view_security_type='DEFINER'",
			"create role reader", "create user observer identified by '111' default role reader", "grant reader to observer",
			"grant connect on account * to reader", "grant all on table app.allowed to reader",
			"grant insert on table app.dest to reader", "grant create table on database app to reader",
		} {
			execSQLRequire(t, ctx, admin, q)
		}
		denied := func(t *testing.T, err error) {
			t.Helper()
			var sqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &sqlErr)
			require.Equal(t, uint16(20101), sqlErr.Number)
		}
		for _, mode := range []string{"off", "on"} {
			t.Run("cache="+mode, func(t *testing.T) {
				db := open(t, account+"#observer#reader")
				conn, err := db.Conn(ctx)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, conn.Close()) })
				_, err = conn.ExecContext(ctx, "set enable_privilege_cache="+mode)
				require.NoError(t, err)
				read := func(query string, want int) {
					t.Helper()
					var value int
					require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&value), query)
					require.Equal(t, want, value, query)
				}
				for _, v := range []string{"v_const", "v_chain", "v_table", "`v_const@ts=42`", "`v_table@ts=42`"} {
					execSQLRequire(t, ctx, admin, "grant select on view app."+v+" to reader")
				}
				read("select id from app.v_const", 42)
				read("select id from app.v_chain", 42)
				read("select id from app.v_table", 42)
				read("select id from app.`v_const@ts=42`", 99)
				read("select id from app.`v_table@ts=42`", 42)
				binary, err := conn.PrepareContext(ctx, "select id from app.v_chain where id=?")
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, binary.Close()) })
				_, err = conn.ExecContext(ctx, "prepare view_text from 'select id from app.v_chain where id=?'")
				require.NoError(t, err)
				_, err = conn.ExecContext(ctx, "set @view_id=42")
				require.NoError(t, err)
				read("execute view_text using @view_id", 42)
				var value int
				require.NoError(t, binary.QueryRowContext(ctx, 42).Scan(&value))
				require.Equal(t, 42, value)
				for _, v := range []string{"v_const", "v_chain", "v_table", "`v_const@ts=42`", "`v_table@ts=42`"} {
					execSQLRequire(t, ctx, admin, "revoke select on view app."+v+" from reader")
				}
				var grants int
				require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_role_privs where role_name='reader' and obj_type='view'").Scan(&grants))
				require.Zero(t, grants)
				for _, query := range []string{
					"select id from app.v_const", "select id from app.v_chain", "select id from app.v_table",
					"select id from app.`v_const@ts=42`", "select id from app.`v_table@ts=42`",
					"select count(*) from app.v_const where false", "select count(*) from (select id from app.v_const limit 0) x",
					"select v.id from app.v_const v cross join app.allowed a",
					"select v.id from app.v_table v cross join app.allowed a",
					"with active as (select id from app.v_const) select id from active", "execute view_text using @view_id",
				} {
					denied(t, conn.QueryRowContext(ctx, query).Scan(&value))
				}
				denied(t, binary.QueryRowContext(ctx, 42).Scan(&value))
				_, err = conn.ExecContext(ctx, "insert into app.dest select id from app.v_const")
				denied(t, err)
				_, err = conn.ExecContext(ctx, "create table app.blocked as select id from app.v_const")
				denied(t, err)
				require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from app.dest").Scan(&value))
				require.Zero(t, value, "denied INSERT must not publish rows")
				require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='app' and relname='blocked'").Scan(&value))
				require.Zero(t, value, "denied CTAS must not publish its target")
				read("select 42", 42)
				read("with unused as (select id from app.v_const) select 42", 42)
				read("select count(*) from information_schema.tables where false", 0)
				// Only the outer DEFINER view is granted: inner authorization uses its
				// definer, not the caller. The same cached prepared handles remain usable.
				execSQLRequire(t, ctx, admin, "grant select on view app.v_chain to reader")
				read("select id from app.v_chain", 42)
				read("execute view_text using @view_id", 42)
				require.NoError(t, binary.QueryRowContext(ctx, 42).Scan(&value))
				require.Equal(t, 42, value)
				execSQLRequire(t, ctx, admin, "grant select on view app.v_invoker to reader")
				denied(t, conn.QueryRowContext(ctx, "select id from app.v_invoker").Scan(&value))
				execSQLRequire(t, ctx, admin, "grant select on view app.v_const to reader")
				read("select id from app.v_invoker", 42)
				denied(t, conn.QueryRowContext(ctx, "select id from app.`v_const@ts=42`").Scan(&value))
				for _, v := range []string{"v_chain", "v_invoker", "v_const"} {
					execSQLRequire(t, ctx, admin, "revoke select on view app."+v+" from reader")
				}
				_, err = conn.ExecContext(ctx, "deallocate prepare view_text")
				require.NoError(t, err)
			})
		}
	})
}
