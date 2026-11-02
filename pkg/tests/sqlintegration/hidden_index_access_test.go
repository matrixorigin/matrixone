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
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestHiddenIndexUserAccess(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop user if exists hidden_access_user", "drop user if exists hidden_access_connect_user", "drop role if exists hidden_access_role", "drop role if exists hidden_access_connect_role", "drop database if exists hidden_access_db")
		port := cn.GetServiceConfig().CN.Frontend.Port
		cleanups := make([]func(), 0, 4)
		defer func() {
			for i := len(cleanups) - 1; i >= 0; i-- {
				cleanups[i]()
			}
		}()
		open := func(login string) *sql.Conn {
			db, e := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", login, port))
			require.NoError(t, e)
			cleanups = append(cleanups, func() { require.NoError(t, db.Close()) })
			conn, e := db.Conn(ctx)
			require.NoError(t, e)
			cleanups = append(cleanups, func() { require.NoError(t, conn.Close()) })
			return conn
		}
		admin := open("dump")
		exec := func(conn *sql.Conn, q string) { t.Helper(); _, e := conn.ExecContext(ctx, q); require.NoError(t, e, q) }
		for _, q := range []string{
			"create database hidden_access_db",
			"create table hidden_access_db.t(id int primary key,y int,u int, key iy(y),unique key iu(u))",
			"insert into hidden_access_db.t values(1,10,100),(2,20,200)",
			"create role hidden_access_role",
			"create user hidden_access_user identified by '111' default role hidden_access_role",
			"grant connect on account * to hidden_access_role",
			"grant all on database hidden_access_db to hidden_access_role",
			"grant all on table hidden_access_db.* to hidden_access_role",
			"create role hidden_access_connect_role",
			"create user hidden_access_connect_user identified by '111' default role hidden_access_connect_role",
			"grant connect on account * to hidden_access_connect_role",
		} {
			exec(admin, q)
		}
		var hidden []string
		func() {
			rows, e := admin.QueryContext(ctx, "select distinct index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='hidden_access_db' and relname='t') and index_table_name is not null and index_table_name<>'' order by index_table_name")
			require.NoError(t, e)
			defer rows.Close()
			for rows.Next() {
				var name string
				require.NoError(t, rows.Scan(&name))
				hidden = append(hidden, name)
			}
			require.NoError(t, rows.Err())
		}()
		require.Len(t, hidden, 2)
		user := open("sys#hidden_access_user#hidden_access_role")
		exec(user, "use hidden_access_db")
		deny := func(conn *sql.Conn, q string) {
			t.Helper()
			rows, e := conn.QueryContext(ctx, q)
			if rows != nil {
				defer rows.Close()
				for rows.Next() {
				}
				if e == nil {
					e = rows.Err()
				}
			}
			var server *mysql.MySQLError
			require.ErrorAs(t, e, &server, q)
			require.Contains(t, strings.ToLower(server.Message), "privilege", q)
		}
		connectOnly := open("sys#hidden_access_connect_user#hidden_access_connect_role")
		deny(connectOnly, "select count(*) from hidden_access_db.t")
		deny(connectOnly, "select count(*) from hidden_access_db.`"+hidden[0]+"`")
		count := func(conn *sql.Conn, table string) int {
			t.Helper()
			var n int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+table).Scan(&n))
			return n
		}
		for _, name := range hidden {
			target := "hidden_access_db.`" + name + "`"
			func() {
				for _, q := range []string{
					"select count(*) from " + target,
					"insert into " + target + " select * from " + target + " limit 0",
					"update " + target + " set __mo_index_pri_col=99999",
					"delete from " + target,
					"truncate table " + target,
					"alter table " + target + " add column hidden_attack int",
					"drop table " + target,
					"show create table " + target,
					"show columns from " + target,
					"explain select * from " + target,
					"select (select count(*) from " + target + ")",
					"values row(2),row(1) order by case when (select count(*) from " + target + ")=2 then column_0 else -column_0 end",
					"show sequences where (select count(*) from " + target + ")>0",
					"select sum(id) from (select id,case when id=1 then cast('2026-01-01 00:00:00' as datetime) else cast('2026-01-01 00:20:00' as datetime) end ts from hidden_access_db.t) s interval(ts,10,minute) gapfill(partition) fill(value,(select count(*) from " + target + "))",

					"prepare denied_hidden from 'select count(*) from " + target + "'",
					"select mo_table_rows('hidden_access_db','" + name + "')",
					"select mo_table_size('hidden_access_db','" + name + "')",
					"select mo_table_col_min('hidden_access_db','" + name + "','__mo_index_pri_col')",
					"select mo_table_col_max('hidden_access_db','" + name + "','__mo_index_pri_col')",
					"select count(*) from metadata_scan('hidden_access_db." + name + "','*') m",
					"select * from table_stats('hidden_access_db." + name + "') m",
					"select * from table_stats('hidden_access_db." + name + "','patch','{\"table_cnt\":9}') m",
				} {
					deny(user, q)
				}
				exec(user, "set @hidden_access_name='"+name+"'")
				deny(user, "select mo_table_rows('hidden_access_db',@hidden_access_name)")
				deniedStmt, e := user.PrepareContext(ctx, "select count(*) from "+target)
				if deniedStmt != nil {
					defer deniedStmt.Close()
				}
				var deniedErr *mysql.MySQLError
				require.ErrorAs(t, e, &deniedErr)
				require.Contains(t, strings.ToLower(deniedErr.Message), "privilege")
				require.Equal(t, 2, count(admin, target))
			}()
		}
		// Runtime metadata APIs still accept the authorized base target.
		for _, q := range []string{
			"select mo_table_rows('hidden_access_db','t')",
			"select mo_table_size('hidden_access_db','t')",
			"select mo_table_col_min('hidden_access_db','t','id')",
			"select mo_table_col_max('hidden_access_db','t','id')",
			"select count(*) from metadata_scan('hidden_access_db.t','*') m",
			"select table_name from table_stats('hidden_access_db.t') m",
		} {
			func() {
				rows, e := user.QueryContext(ctx, q)
				require.NoError(t, e, q)
				defer rows.Close()
				for rows.Next() {
				}
				require.NoError(t, rows.Err(), q)
			}()
		}
		deny(user, "select count(*) from metadata_scan('hidden_access_db.t.?iy','*') m")
		exec(user, "create index iy_extra on hidden_access_db.t(y)")
		exec(user, "drop index iy_extra on hidden_access_db.t")
		func() {
			rows, e := admin.QueryContext(ctx, "select count(*) from metadata_scan('hidden_access_db.t.?iy','*') m")
			require.NoError(t, e)
			defer rows.Close()
			for rows.Next() {
			}
			require.NoError(t, rows.Err())
		}()
		exec(admin, "create view hidden_access_db.physical_view as select count(*) n from hidden_access_db.`"+hidden[0]+"`")
		exec(admin, "grant select on view hidden_access_db.physical_view to hidden_access_role")
		deny(user, "select n from hidden_access_db.physical_view")
		exec(user, "create view hidden_access_db.base_view as select id from hidden_access_db.t")
		exec(admin, "grant select on view hidden_access_db.base_view to hidden_access_role")
		var viewID int
		require.NoError(t, user.QueryRowContext(ctx, "select id from hidden_access_db.base_view where id=1").Scan(&viewID))
		require.Equal(t, 1, viewID)
		// Stored SQL created by an administrator is still subject to the
		// executing caller's physical-index policy, including nested execution.
		hiddenTarget := "hidden_access_db.`" + hidden[0] + "`"
		exec(admin, "create procedure hidden_access_db.p_inner() 'begin prepare hidden_proc_read from ''select count(*) from "+hiddenTarget+"''; execute hidden_proc_read; deallocate prepare hidden_proc_read; end'")
		exec(admin, "create procedure hidden_access_db.p_outer() 'begin call hidden_access_db.p_inner(); end'")
		exec(admin, "create function hidden_access_db.hidden_count() returns bigint language sql as 'select count(*) from "+hiddenTarget+"'")
		exec(admin, "create procedure hidden_access_db.p_base() 'begin select count(*) from hidden_access_db.t; end'")
		exec(admin, "create function hidden_access_db.base_count() returns bigint language sql as 'select count(*) from hidden_access_db.t'")
		var baseRoutineCount int
		require.NoError(t, user.QueryRowContext(ctx, "call hidden_access_db.p_base()").Scan(&baseRoutineCount))
		require.Equal(t, 2, baseRoutineCount)
		require.NoError(t, user.QueryRowContext(ctx, "select base_count()").Scan(&baseRoutineCount))
		require.Equal(t, 2, baseRoutineCount)
		var adminRoutineCount int
		require.NoError(t, admin.QueryRowContext(ctx, "call hidden_access_db.p_inner()").Scan(&adminRoutineCount))
		require.Equal(t, 2, adminRoutineCount)
		deny(user, "call hidden_access_db.p_inner()")
		deny(user, "call hidden_access_db.p_outer()")
		deny(user, "select hidden_count()")
		exec(admin, "create procedure hidden_access_db.p_expr() 'begin declare n bigint; set n=internal_auto_increment(''hidden_access_db'',''"+hidden[0]+"''); select n; end'")
		exec(admin, "create procedure hidden_access_db.p_expr_base() 'begin declare n bigint; set n=internal_auto_increment(''hidden_access_db'',''t''); select n; end'")
		exec(user, "call hidden_access_db.p_expr_base()")
		exec(admin, "call hidden_access_db.p_expr()")
		deny(user, "call hidden_access_db.p_expr()")
		// mo.sql reports errors to the Starlark program; rethrow it so the
		// public call observes the exact denial rather than swallowing it.
		exec(admin, "create procedure hidden_access_db.p_starlark() language 'starlark' $$\nrows, err = mo.sql(\"select count(*) from "+hiddenTarget+"\")\nif err:\n    fail(str(err))\n$$")
		exec(admin, "create procedure hidden_access_db.p_starlark_base() language 'starlark' $$\nrows, err = mo.sql(\"select count(*) from hidden_access_db.t\")\nif err:\n    fail(str(err))\n$$")
		exec(user, "call hidden_access_db.p_starlark_base()")
		deny(user, "call hidden_access_db.p_starlark()")
		require.Equal(t, 2, count(admin, "hidden_access_db.t"))
		// A refused implicit-commit DDL must not commit prior user DML.
		exec(user, "begin")
		exec(user, "insert into hidden_access_db.t values(9,90,900)")
		deny(user, "truncate table hidden_access_db.`"+hidden[0]+"`")
		exec(user, "rollback")
		require.Equal(t, 2, count(admin, "hidden_access_db.t"))
		// CTAS reparses its generated INSERT through a delegated compiler
		// context; its optimizer-generated index lookup stays authorized.
		exec(user, "create table hidden_access_db.copied as select id,y,u from hidden_access_db.t force index(iu) order by id")
		require.Equal(t, 2, count(user, "hidden_access_db.copied"))
		exec(user, "drop table hidden_access_db.copied")
		// All ordinary writes maintain both physical indexes through generated paths.
		exec(user, "insert into hidden_access_db.t values(3,30,300)")
		exec(user, "update hidden_access_db.t set y=31,u=301 where id=3")
		for _, hint := range []string{"force index(iy)", "force index(iu)"} {
			var id int
			require.NoError(t, user.QueryRowContext(ctx, "select id from hidden_access_db.t "+hint+" where y=31 and u=301").Scan(&id))
			require.Equal(t, 3, id)
		}
		exec(user, "delete from hidden_access_db.t where id=3")
		for _, name := range hidden {
			require.Equal(t, 2, count(admin, "hidden_access_db.`"+name+"`"))
		}
		// SHOW's implementation follows index children while computing sizes;
		// this is server-authored work, not a direct physical-name capability.
		func() {
			rows, e := user.QueryContext(ctx, "show table status from hidden_access_db")
			require.NoError(t, e)
			defer rows.Close()
			seen := 0
			for rows.Next() {
				seen++
			}
			require.NoError(t, rows.Err())
			require.Positive(t, seen)
		}()
		// Reusing an ordinary indexed plan must not mistake generated dependencies
		// for explicit user access; both protocols keep the base-table grant model.
		for i := 0; i < 2; i++ {
			var id int
			require.NoError(t, user.QueryRowContext(ctx, "select id from hidden_access_db.t force index(iy) where y=10").Scan(&id))
			require.Equal(t, 1, id)
		}
		stmt, e := user.PrepareContext(ctx, "select id from hidden_access_db.t force index(iu) where u=?")
		require.NoError(t, e)
		defer stmt.Close()
		var id int
		require.NoError(t, stmt.QueryRowContext(ctx, 100).Scan(&id))
		require.Equal(t, 1, id)
		exec(user, "prepare allowed_hidden_base from 'select id from hidden_access_db.t force index(iy) where y=10'")
		defer func() { _, e := user.ExecContext(ctx, "deallocate prepare allowed_hidden_base"); require.NoError(t, e) }()
		require.NoError(t, user.QueryRowContext(ctx, "execute allowed_hidden_base").Scan(&id))
		require.Equal(t, 1, id)
		// Cached physical queries prepared by moadmin must lose access after an
		// active-role change, including already-created wire handles.
		physical := "hidden_access_db.`" + hidden[0] + "`"
		for i := 0; i < 2; i++ {
			require.Equal(t, 2, count(admin, physical))
		}
		exec(admin, "prepare cached_hidden_admin from 'select count(*) from "+physical+"'")
		defer func() {
			_, err := admin.ExecContext(ctx, "deallocate prepare cached_hidden_admin")
			require.NoError(t, err)
		}()
		privilegedStmt, e := admin.PrepareContext(ctx, "select count(*) from "+physical)
		require.NoError(t, e)
		defer privilegedStmt.Close()
		var privilegedCount int
		require.NoError(t, privilegedStmt.QueryRowContext(ctx).Scan(&privilegedCount))
		require.Equal(t, 2, privilegedCount)
		_, e = admin.ExecContext(ctx, "set role hidden_access_missing_role")
		require.Error(t, e)
		require.Equal(t, 2, count(admin, physical))
		exec(admin, "set role public")
		deny(admin, "select count(*) from "+physical)
		deny(admin, "execute cached_hidden_admin")
		e = privilegedStmt.QueryRowContext(ctx).Scan(&privilegedCount)
		var roleError *mysql.MySQLError
		require.ErrorAs(t, e, &roleError)
		require.Contains(t, strings.ToLower(roleError.Message), "privilege")
		exec(admin, "set role moadmin")
		require.Equal(t, 2, count(admin, physical))
		// A supported plugin child retains its physical identity after a
		// diagnostic rename removes the reserved-name signal.
		func() {
			exec(admin, "set experimental_fulltext_index=1")
			exec(admin, "create table hidden_access_db.docs(id bigint primary key,body text,fulltext index ft(body))")
			exec(user, "insert into hidden_access_db.docs values(1,'aurora'),(2,'nebula')")
			var matchedID int
			require.NoError(t, user.QueryRowContext(ctx, "select id from hidden_access_db.docs where match(body) against('aurora' in natural language mode)>0").Scan(&matchedID))
			require.Equal(t, 1, matchedID)
			var child string
			require.NoError(t, admin.QueryRowContext(ctx, "select distinct index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='hidden_access_db' and relname='docs') and index_table_name is not null and index_table_name<>'' limit 1").Scan(&child))
			original := "hidden_access_db.`" + child + "`"
			renamed := "hidden_access_db.renamed_child"
			physicalRows := count(admin, original)
			require.Positive(t, physicalRows)
			exec(admin, "alter table "+original+" rename to renamed_child")
			defer exec(admin, "alter table "+renamed+" rename to `"+child+"`")
			require.Equal(t, physicalRows, count(admin, renamed))
			_, err := admin.ExecContext(ctx, "grant select on table "+renamed+" to hidden_access_role")
			var grantError *mysql.MySQLError
			require.ErrorAs(t, err, &grantError)
			require.Contains(t, strings.ToLower(grantError.Message), "internal relation")
			var exactGrants int
			require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_role_privs where role_name='hidden_access_role' and obj_type='table' and obj_id=(select rel_logical_id from mo_catalog.mo_tables where reldatabase='hidden_access_db' and relname='renamed_child')").Scan(&exactGrants))
			require.Zero(t, exactGrants)
			exec(admin, "grant select on table hidden_access_db.t to hidden_access_role")
			for _, q := range []string{
				"select count(*) from " + renamed,
				"delete from " + renamed,
				"select mo_table_rows('hidden_access_db','renamed_child')",
				"select mo_table_size('hidden_access_db','renamed_child')",
				"select count(*) from metadata_scan('hidden_access_db.renamed_child','*') m",
				"select * from table_stats('hidden_access_db.renamed_child') m",
			} {
				deny(user, q)
			}
			exec(user, "begin")
			exec(user, "insert into hidden_access_db.t values(9,90,900)")
			deny(user, "truncate table "+renamed)
			exec(user, "rollback")
			require.Equal(t, 2, count(admin, "hidden_access_db.t"))
			// Both temporary creation and rename operate independently of
			// the protected permanent relation with the same name.
			for _, rename := range []bool{false, true} {
				func() {
					if rename {
						exec(user, "create temporary table hidden_access_db.shadow_source(id int)")
						defer exec(user, "drop temporary table if exists hidden_access_db.shadow_source")
						exec(user, "insert into hidden_access_db.shadow_source values(7)")
						exec(user, "alter table hidden_access_db.shadow_source rename to renamed_child")
					} else {
						exec(user, "create temporary table hidden_access_db.renamed_child(id int)")
					}
					defer exec(user, "drop temporary table hidden_access_db.renamed_child")
					if !rename {
						exec(user, "insert into hidden_access_db.renamed_child values(7)")
					}
					require.Equal(t, 1, count(user, renamed))
					deny(user, "dump table "+renamed+" to 'stage://hidden_access_unavailable/' metadata only")
					deny(user, "load table "+renamed+" from 'stage://hidden_access_unavailable/'")
				}()
			}

			// Prepared temporary DDL probes the temporary destination only;
			// a new alias introduced after PREPARE is checked at EXECUTE.
			for _, alter := range []bool{false, true} {
				for _, binary := range []bool{false, true} {
					func() {
						createDestination := "create temporary table hidden_access_db.renamed_child(id int)"
						query := createDestination
						if alter {
							exec(user, "create temporary table hidden_access_db.prepared_source(id int)")
							defer exec(user, "drop temporary table if exists hidden_access_db.prepared_source")
							query = "alter table hidden_access_db.prepared_source rename to renamed_child"
						}
						check := func(execute func() error) {
							t.Helper()
							exec(user, createDestination)
							defer exec(user, "drop temporary table if exists hidden_access_db.renamed_child")
							var collision *mysql.MySQLError
							require.ErrorAs(t, execute(), &collision)
							require.EqualValues(t, 1050, collision.Number)
							exec(user, "drop temporary table hidden_access_db.renamed_child")
							require.NoError(t, execute())
							exec(user, "insert into hidden_access_db.renamed_child values(8)")
							require.Equal(t, 1, count(user, renamed))
						}
						if binary {
							stmt, err := user.PrepareContext(ctx, query)
							require.NoError(t, err)
							defer func() { require.NoError(t, stmt.Close()) }()
							check(func() error { _, err := stmt.ExecContext(ctx); return err })
							return
						}
						exec(user, "prepare temp_shadow from '"+query+"'")
						defer exec(user, "deallocate prepare temp_shadow")
						check(func() error { _, err := user.ExecContext(ctx, "execute temp_shadow"); return err })
					}()
				}
			}

		}()
		var restoredMatchID int
		require.NoError(t, user.QueryRowContext(ctx, "select id from hidden_access_db.docs where match(body) against('aurora' in natural language mode)>0").Scan(&restoredMatchID))
		require.Equal(t, 1, restoredMatchID)
		// Tenant accountadmin is not a physical-index diagnostic capability,
		// including frontend DUMP/LOAD owners that bypass the SQL planner.
		func() {
			exec(admin, "create account hidden_access_tenant admin_name 'admin' identified by '111'")
			defer exec(admin, "drop account hidden_access_tenant")
			db, err := sql.Open("mysql", fmt.Sprintf("hidden_access_tenant#admin:111@tcp(127.0.0.1:%d)/", port))
			require.NoError(t, err)
			defer db.Close()
			tenant, err := db.Conn(ctx)
			require.NoError(t, err)
			defer tenant.Close()
			exec(tenant, "create database tenant_data")
			exec(tenant, "create table tenant_data.base(id int primary key,y int,key iy(y))")
			exec(tenant, "insert into tenant_data.base values(1,10)")
			require.Equal(t, 1, count(tenant, "tenant_data.base"))
			var child string
			require.NoError(t, tenant.QueryRowContext(ctx, "select distinct index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='tenant_data' and relname='base') and index_table_name is not null and index_table_name<>'' limit 1").Scan(&child))
			target := "tenant_data.`" + child + "`"
			deny(tenant, "select count(*) from "+target)
			deny(tenant, "dump table "+target+" to 'stage://hidden_access_unavailable/' metadata only")
			deny(tenant, "load table "+target+" from 'stage://hidden_access_unavailable/'")
		}()
		// The reserved-looking identifier below is a CTE, not a physical relation.
		require.NoError(t, user.QueryRowContext(ctx, "with __mo_index_cte as (select 7 as n) select n from __mo_index_cte").Scan(&id))
		require.Equal(t, 7, id)
		exec(user, "begin")
		deny(user, "delete from hidden_access_db.`"+hidden[0]+"`")
		exec(user, "rollback")
		require.NoError(t, user.QueryRowContext(ctx, "select 1").Scan(&id))
		require.Equal(t, 1, id)
	})
}
