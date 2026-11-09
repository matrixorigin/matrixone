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
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

func TestHiddenIndexUserAccess(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop snapshot if exists hidden_access_snapshot", "drop user if exists index_show_reader", "drop role if exists index_show_role", "drop user if exists hidden_access_user", "drop user if exists hidden_access_connect_user", "drop role if exists hidden_access_role", "drop role if exists hidden_access_connect_role", "drop database if exists hidden_metadata_clone", "drop database if exists hidden_cross_dst", "drop database if exists hidden_access_db")
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
		// The metadata view's physical-index predicate and main's vector
		// functions must bind to distinct registry entries in the same query.
		const vec = `cast('{"g":1,"s":[1],"v":[1,2]}' as vecf8(2))`
		var vecJSON string
		var vecBytes int
		err = user.QueryRowContext(ctx, "select vecblock_json("+vec+"), length(vecblock_binary("+vec+")) from information_schema.tables where table_schema='hidden_access_db' and table_name='t'").Scan(&vecJSON, &vecBytes)
		require.NoError(t, err)
		require.JSONEq(t, `{"g":1,"s":[1],"v":[1,2]}`, vecJSON)
		require.Positive(t, vecBytes)
		var matmul string
		err = user.QueryRowContext(ctx, "select vector_matmul(1,id,cast('[1,0]' as vecf8(2)),'[[1,0]]') from hidden_access_db.t where id=1").Scan(&matmul)
		require.NoError(t, err)
		require.JSONEq(t, `[[["1",-1]]]`, matmul)
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
		// The normal LOAD path must retain main's inline source options while
		// direct LOAD into physical index tables remains forbidden.
		exec(user, "load data inline format='csv', data='3,30,0\n' into table hidden_access_db.t fields terminated by ',' set u=nullif(u,0)")
		var loadedU sql.NullInt64
		require.NoError(t, user.QueryRowContext(ctx, "select u from hidden_access_db.t where id=3").Scan(&loadedU))
		require.False(t, loadedU.Valid)
		exec(user, "delete from hidden_access_db.t where id=3")
		for _, name := range hidden {
			target := "hidden_access_db.`" + name + "`"
			func() {
				for _, q := range []string{
					"select count(*) from " + target,
					"load data inline format='csv', data='3,30\n' into table " + target + " fields terminated by ','",
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
		exec(admin, "create snapshot hidden_access_snapshot for account")
		for _, hint := range []string{"", " {snapshot='hidden_access_snapshot'}"} {
			func() {
				exec(user, "create table hidden_access_db.cloned_base clone hidden_access_db.t"+hint)
				defer exec(user, "drop table hidden_access_db.cloned_base")
				require.Equal(t, 2, count(user, "hidden_access_db.cloned_base"))
			}()
			for _, name := range hidden {
				deny(user, "create table hidden_access_db.denied_clone clone hidden_access_db.`"+name+"`"+hint)
			}
		}
		var deniedClones int
		require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='hidden_access_db' and relname='denied_clone'").Scan(&deniedClones))
		require.Zero(t, deniedClones)
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
			exec(user, "create table hidden_access_db.partitioned(id int) partition by range(id) (partition p0 values less than (10), partition p1 values less than (100))")
			exec(user, "insert into hidden_access_db.partitioned values(1),(11)")
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
			defer func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				_, err := admin.ExecContext(cleanupCtx, "alter table "+renamed+" rename to `"+child+"`")
				require.NoError(t, err)
			}()
			require.Equal(t, physicalRows, count(admin, renamed))
			// The diagnostic rename leaves docs' stored index name stale.
			// Exercise listing admission with the child and a valid ordinary
			// base; computing docs' size would independently follow that old name.
			for _, query := range []string{
				"show table status from hidden_access_db where Name in ('renamed_child','t')",
				"show tables from hidden_access_db",
				"show full tables from hidden_access_db",
				"select table_name,table_rows,data_length from information_schema.tables where table_schema='hidden_access_db'",
				"select table_name,table_rows,data_length from information_schema.partitions where table_schema='hidden_access_db' and table_name in ('renamed_child','t')",
				"select distinct table_name from information_schema.columns where table_schema='hidden_access_db' and table_name in ('renamed_child','t')",
			} {
				func() {
					rows, err := user.QueryContext(ctx, query)
					require.NoError(t, err, query)
					defer rows.Close()
					columns, err := rows.Columns()
					require.NoError(t, err)
					values := make([]sql.RawBytes, len(columns))
					args := make([]any, len(columns))
					for i := range values {
						args[i] = &values[i]
					}
					sawBase := false
					for rows.Next() {
						require.NoError(t, rows.Scan(args...))
						require.NotEqual(t, "renamed_child", string(values[0]), query)
						sawBase = sawBase || string(values[0]) == "t"
					}
					require.NoError(t, rows.Err(), query)
					require.True(t, sawBase, query)
				}()
			}
			exec(admin, "create database hidden_metadata_clone clone information_schema")
			exec(admin, "grant select on view hidden_metadata_clone.columns to hidden_access_role")
			for _, tc := range []struct {
				table string
				want  int
			}{{"docs", 2}, {"renamed_child", 0}} {
				var visible int
				require.NoError(t, user.QueryRowContext(ctx, "select count(*) from hidden_metadata_clone.columns where table_schema='hidden_access_db' and table_name=?", tc.table).Scan(&visible))
				require.Equal(t, tc.want, visible, tc.table)
			}
			func() {
				rows, err := user.QueryContext(ctx, "select partition_name,table_rows,data_length from information_schema.partitions where table_schema='hidden_access_db' and table_name='partitioned' order by partition_ordinal_position")
				require.NoError(t, err)
				defer rows.Close()
				var partitions []string
				for rows.Next() {
					var name string
					var rowCount, size sql.NullInt64
					require.NoError(t, rows.Scan(&name, &rowCount, &size))
					partitions = append(partitions, name)
				}
				require.NoError(t, rows.Err())
				require.Equal(t, []string{"p0", "p1"}, partitions)
			}()
			for _, tableKeyword := range []string{"table ", ""} {
				_, err := admin.ExecContext(ctx, "grant select on "+tableKeyword+renamed+" to hidden_access_role")
				var grantError *mysql.MySQLError
				require.ErrorAs(t, err, &grantError)
				require.Contains(t, strings.ToLower(grantError.Message), "internal relation")
				var exactGrants int
				require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_role_privs where role_name='hidden_access_role' and obj_type='table' and obj_id=(select rel_logical_id from mo_catalog.mo_tables where reldatabase='hidden_access_db' and relname='renamed_child')").Scan(&exactGrants))
				require.Zero(t, exactGrants)
			}
			exec(admin, "grant select on hidden_access_db.t to hidden_access_connect_role")
			require.Equal(t, 2, count(connectOnly, "hidden_access_db.t"))
			deny(connectOnly, "select count(*) from "+renamed)
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
		// Public calls naming physical fulltext storage require diagnostic
		// authority; normal base-table index maintenance retains its owner.
		func() {
			exec(user, "set experimental_fulltext2_index=1")
			exec(user, "create table hidden_access_db.fulltext_runtime(id bigint primary key,body text)")
			exec(user, "insert into hidden_access_db.fulltext_runtime values(1,'alpha'),(2,'beta')")
			exec(user, "create fulltext2 index fr on hidden_access_db.fulltext_runtime(body)")
			var store, meta string
			for kind, dst := range map[string]*string{"ftv2_index": &store, "ftv2_meta": &meta} {
				require.NoError(t, admin.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='hidden_access_db' and relname='fulltext_runtime') and algo_table_type='"+kind+"' limit 1").Scan(dst))
			}
			deny(user, "select * from fulltext2_compact('hidden_access_db','"+store+"','"+meta+"','1') f")
			config, err := json.Marshal(fulltext2.TableConfig{DbName: "hidden_access_db", SrcTable: "fulltext_runtime", IndexTable: store, MetadataTable: meta, PKey: "id"})
			require.NoError(t, err)
			deny(user, "select f.* from hidden_access_db.fulltext_runtime s cross apply fulltext2_create('{}','"+strings.ReplaceAll(string(config), "'", "''")+"',s.id,s.body) f")
			exec(user, "alter table hidden_access_db.fulltext_runtime alter reindex fr fulltext2 merge force_sync")
			exec(user, "alter table hidden_access_db.fulltext_runtime alter reindex fr fulltext2 force_sync")
			var found int
			require.NoError(t, user.QueryRowContext(ctx, "select id from hidden_access_db.fulltext_runtime where match(body) against('alpha')").Scan(&found))
			require.Equal(t, 1, found)
			require.Equal(t, 2, count(user, "hidden_access_db.fulltext_runtime"))
		}()
		// Runtime build functions cannot turn a caller's JSON into authority
		// over another index, even with an empty source and no build work.
		func() {
			exec(user, "create table hidden_access_db.tvf_source(id bigint primary key,v vecf32(2))")
			config, err := json.Marshal(vectorindex.IndexTableConfig{
				DbName: "hidden_access_db", SrcTable: "tvf_source",
				MetadataTable: hidden[0], IndexTable: hidden[0], EntriesTable: hidden[0],
				PKey: "id", KeyPart: "v", KeyPartType: int32(types.T_array_float32), KeyPartWidth: 2,
			})
			require.NoError(t, err)
			for _, function := range []string{"hnsw_create", "ivf_create"} {
				deny(user, "select * from "+function+"('{\"lists\":\"1\",\"op_type\":\"vector_l2_ops\"}','"+strings.ReplaceAll(string(config), "'", "''")+"') f")
				require.Equal(t, 2, count(admin, sqlquote.QualifiedIdent("hidden_access_db", hidden[0])))
				require.Equal(t, 2, count(user, "hidden_access_db.t"))
			}
			exec(user, "create table hidden_access_db.tvf_meta(id int)")
			exec(user, "create table hidden_access_db.tvf_store(id int)")
			exec(user, "create view hidden_access_db.tvf_ordinary_view as select id,v from hidden_access_db.tvf_source where base_count()>0")
			ordinary, err := json.Marshal(vectorindex.IndexTableConfig{
				DbName: "hidden_access_db", SrcTable: "tvf_ordinary_view", MetadataTable: "tvf_meta", IndexTable: "tvf_store",
				PKey: "id", KeyPart: "v", KeyPartType: int32(types.T_array_float32), KeyPartWidth: 2,
			})
			require.NoError(t, err)
			exec(admin, "create view hidden_access_db.tvf_source_view as select 1 as id,cast('[1,1]' as vecf32(2)) as v from "+sqlquote.QualifiedIdent("hidden_access_db", hidden[0]))
			viewConfig, err := json.Marshal(vectorindex.IndexTableConfig{
				DbName: "hidden_access_db", SrcTable: "tvf_source_view", MetadataTable: "tvf_meta", IndexTable: "tvf_store",
				PKey: "id", KeyPart: "v", KeyPartType: int32(types.T_array_float32), KeyPartWidth: 2,
			})
			require.NoError(t, err)
			exec(user, "insert into hidden_access_db.tvf_store values(7)")
			deny(user, "select * from hnsw_create('{\"op_type\":\"vector_l2_ops\"}','"+strings.ReplaceAll(string(viewConfig), "'", "''")+"') f")
			require.Equal(t, 1, count(user, "hidden_access_db.tvf_store"))
			exec(admin, "use hidden_access_db")
			exec(admin, "create view hidden_access_db.tvf_udf_view as select id,v from hidden_access_db.tvf_source where hidden_count()>0")
			udfConfig, err := json.Marshal(vectorindex.IndexTableConfig{
				DbName: "hidden_access_db", SrcTable: "tvf_udf_view", MetadataTable: "tvf_meta", IndexTable: "tvf_store",
				PKey: "id", KeyPart: "v", KeyPartType: int32(types.T_array_float32), KeyPartWidth: 2,
			})
			require.NoError(t, err)
			deny(user, "select * from hnsw_create('{\"op_type\":\"vector_l2_ops\"}','"+strings.ReplaceAll(string(udfConfig), "'", "''")+"') f")
			require.Equal(t, 1, count(user, "hidden_access_db.tvf_store"))
			exec(user, "delete from hidden_access_db.tvf_store")
			for _, binary := range []bool{false, true} {
				func() {
					query := "select * from hnsw_create('{\"op_type\":\"vector_l2_ops\"}',cast(? as varchar)) f"
					var execute func(string) error
					if binary {
						stmt, err := user.PrepareContext(ctx, query)
						require.NoError(t, err)
						defer func() { require.NoError(t, stmt.Close()) }()
						execute = func(value string) error { _, err := stmt.ExecContext(ctx, value); return err }
					} else {
						exec(user, "prepare tvf_build from '"+strings.ReplaceAll(query, "'", "''")+"'")
						defer exec(user, "deallocate prepare tvf_build")
						execute = func(value string) error {
							exec(user, "set @tvf_config='"+strings.ReplaceAll(value, "'", "''")+"'")
							_, err := user.ExecContext(ctx, "execute tvf_build using @tvf_config")
							return err
						}
					}
					require.NoError(t, execute(string(ordinary)))
					var rejected *mysql.MySQLError
					require.ErrorAs(t, execute(string(config)), &rejected)
					require.Contains(t, strings.ToLower(rejected.Message), "privilege")
					require.Equal(t, 2, count(admin, sqlquote.QualifiedIdent("hidden_access_db", hidden[0])))
					require.NoError(t, execute(string(ordinary)))
				}()
			}
			exec(user, "insert into hidden_access_db.tvf_source values(1,'[1,1]'),(2,'[2,2]')")
			exec(user, "alter table hidden_access_db.tvf_source add column "+sqlquote.Ident("v`x")+" vecf32(2)")
			exec(user, "update hidden_access_db.tvf_source set "+sqlquote.Ident("v`x")+"=v")
			exec(user, "create index `Σ` using ivfflat on hidden_access_db.tvf_source("+sqlquote.Ident("v`x")+") lists=1 op_type 'vector_l2_ops'")
			exec(user, "alter table hidden_access_db.tvf_source alter reindex `σ` ivfflat force_sync")
			exec(user, "create index `ς` using ivfflat on hidden_access_db.tvf_source(v) lists=1 op_type 'vector_l2_ops'")
			exec(user, "alter table hidden_access_db.tvf_source alter reindex `Σ` ivfflat force_sync")
			exec(user, "alter table hidden_access_db.tvf_source alter reindex `ς` ivfflat force_sync")
			func() {
				rows, err := admin.QueryContext(ctx, `select i.name, i.index_table_name, t.rel_id
					from mo_catalog.mo_indexes i join mo_catalog.mo_tables t
					on i.index_table_name=t.relname and t.reldatabase='hidden_access_db'
					where i.table_id=(select rel_id from mo_catalog.mo_tables
					where reldatabase='hidden_access_db' and relname='tvf_source')`)
				require.NoError(t, err)
				defer rows.Close()
				groups := map[string]map[string]uint64{"σ": {}, "ς": {}}
				for rows.Next() {
					var name, physical string
					var id uint64
					require.NoError(t, rows.Scan(&name, &physical, &id))
					if group, ok := groups[strings.ToLower(name)]; ok {
						group[physical] = id
					}
				}
				require.NoError(t, rows.Err())
				require.Len(t, groups["σ"], 4)
				require.Len(t, groups["ς"], 4)
				firstIDs := make(map[uint64]bool, len(groups["σ"]))
				for _, id := range groups["σ"] {
					require.NotZero(t, id)
					firstIDs[id] = true
				}
				for physical, id := range groups["ς"] {
					require.NotContains(t, groups["σ"], physical)
					require.NotZero(t, id)
					require.False(t, firstIDs[id])
				}
			}()
			for _, column := range []string{"v`x", "v"} {
				var nearest int
				require.NoError(t, user.QueryRowContext(ctx, "select id from hidden_access_db.tvf_source order by l2_distance("+sqlquote.Ident(column)+",'[1,1]') limit 1").Scan(&nearest))
				require.Equal(t, 1, nearest)
			}
		}()
		// Server-authored SHOW scans remain authorized on behalf of the base,
		// while caller-written WHERE subqueries retain their own permissions.
		func() {
			exec(admin, "set experimental_hnsw_index=1")
			exec(admin, "create table hidden_access_db.hnsw_base(id bigint primary key,v vecf32(2))")
			exec(admin, "insert into hidden_access_db.hnsw_base values(1,'[1,1]'),(2,'[100000,1]')")
			exec(admin, "create index ih using hnsw on hidden_access_db.hnsw_base(v) op_type 'vector_l2_ops' quantization 'float16' max_bad_records 1")
			exec(admin, "create table hidden_access_db.show_private(id int)")
			exec(admin, "insert into hidden_access_db.show_private values(1)")
			exec(admin, "create role index_show_role")
			exec(admin, "create user index_show_reader identified by '111' default role index_show_role")
			exec(admin, "grant connect on account * to index_show_role")
			reader := open("sys#index_show_reader#index_show_role")
			show := func(q string) {
				t.Helper()
				rows, err := reader.QueryContext(ctx, q)
				require.NoError(t, err, q)
				defer rows.Close()
				cols, err := rows.Columns()
				require.NoError(t, err)
				seen := 0
				for rows.Next() {
					values := make([]sql.RawBytes, len(cols))
					args := make([]any, len(cols))
					for i := range args {
						args[i] = &values[i]
					}
					require.NoError(t, rows.Scan(args...))
					require.Equal(t, "ih", string(values[0]))
					metadata := make(map[string]string, len(cols))
					for i, name := range cols {
						metadata[name] = string(values[i])
					}
					if strings.HasPrefix(q, "show index ignored") {
						require.Equal(t, "2", metadata["Primary_key"])
						require.Equal(t, "20348", metadata["Error_code"])
					} else {
						require.Equal(t, "1", metadata["Ignored_rows"])
						require.Equal(t, "1", metadata["Max_bad_records"])
					}
					seen++
				}
				require.NoError(t, rows.Err())
				require.Equal(t, 1, seen)
			}
			var physical string
			require.NoError(t, admin.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='hidden_access_db' and relname='hnsw_base') and algo_table_type='ignored' limit 1").Scan(&physical))
			for _, mode := range []string{"ignored", "status"} {
				q := "show index " + mode + " from hidden_access_db.hnsw_base"
				deny(reader, q)
			}
			exec(admin, "grant select on table hidden_access_db.hnsw_base to index_show_role")
			for _, mode := range []string{"ignored", "status"} {
				q := "show index " + mode + " from hidden_access_db.hnsw_base"
				show(q)
				show(q + " where Key_name='ih'")
				deny(reader, q+" where exists(select 1 from hidden_access_db.show_private)")
				deny(reader, q+" where exists(select 1 from hidden_access_db.`"+physical+"`)")
				deny(reader, "show index "+mode+" from hidden_access_db.`"+physical+"`")
			}
			exec(admin, "grant select on table hidden_access_db.show_private to index_show_role")
			for _, mode := range []string{"ignored", "status"} {
				show("show index " + mode + " from hidden_access_db.hnsw_base where exists(select 1 from hidden_access_db.show_private)")
			}
		}()
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
			func() {
				exec(admin, "create database hidden_cross_dst")
				exec(tenant, "create database hidden_cross_dst")
				exec(admin, "grant create table on database hidden_cross_dst to index_show_role")
				exec(admin, "grant select on table hidden_access_db.t to index_show_role")
				reader := open("sys#index_show_reader#index_show_role")
				var snapshotTS int64
				require.NoError(t, admin.QueryRowContext(ctx, "select ts from mo_catalog.mo_snapshots where sname='hidden_access_snapshot'").Scan(&snapshotTS))
				hints := []string{" {snapshot='hidden_access_snapshot'}", fmt.Sprintf(" {MO_TS=%d}", snapshotTS)}
				indexName := func(conn *sql.Conn) string {
					t.Helper()
					var name string
					require.NoError(t, conn.QueryRowContext(ctx, "select distinct index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='hidden_cross_dst' and relname='owner') and index_table_name is not null and index_table_name<>'' limit 1").Scan(&name))
					return name
				}
				commands := func(name, hint string) []string {
					return []string{
						"create table hidden_cross_dst." + name + " clone hidden_access_db.t" + hint + " to account hidden_access_tenant",
						"data branch create table hidden_cross_dst." + name + " from hidden_access_db.t" + hint + " to account hidden_access_tenant",
					}
				}
				exec(admin, "create table hidden_cross_dst.owner(id int primary key,y int,key iy(y))")
				func() {
					physical := indexName(admin)
					exec(admin, "alter table hidden_cross_dst.`"+physical+"` rename to cross_shadow")
					defer exec(admin, "alter table hidden_cross_dst.cross_shadow rename to `"+physical+"`")
					for _, q := range commands("cross_shadow", "") {
						_, err := reader.ExecContext(ctx, q)
						var missingSnapshot *mysql.MySQLError
						require.ErrorAs(t, err, &missingSnapshot)
						require.Contains(t, strings.ToLower(missingSnapshot.Message), "snapshot")
					}
					for _, hint := range hints {
						for _, q := range commands("cross_shadow", hint) {
							func() {
								exec(reader, q)
								defer exec(tenant, "drop table hidden_cross_dst.cross_shadow")
								require.Equal(t, 2, count(tenant, "hidden_cross_dst.cross_shadow"))
							}()
							if strings.HasPrefix(q, "create table") {
								for _, binary := range []bool{false, true} {
									func() {
										verify := func(execute func() error) {
											require.NoError(t, execute())
											defer exec(tenant, "drop table hidden_cross_dst.cross_shadow")
											require.Equal(t, 2, count(tenant, "hidden_cross_dst.cross_shadow"))
										}
										if binary {
											stmt, err := reader.PrepareContext(ctx, q)
											require.NoError(t, err, q)
											defer func() { require.NoError(t, stmt.Close()) }()
											verify(func() error { _, err := stmt.ExecContext(ctx); return err })
											return
										}
										exec(reader, "prepare prepared_cross from '"+strings.ReplaceAll(q, "'", "''")+"'")
										defer exec(reader, "deallocate prepare prepared_cross")
										verify(func() error { _, err := reader.ExecContext(ctx, "execute prepared_cross"); return err })
									}()
								}
							}
						}
					}
				}()
				for _, binary := range []bool{false, true} {
					func() {
						query := commands("revoked_clone", hints[0])[0]
						var execute func() error
						if binary {
							stmt, err := reader.PrepareContext(ctx, query)
							require.NoError(t, err)
							defer func() { require.NoError(t, stmt.Close()) }()
							execute = func() error { _, err := stmt.ExecContext(ctx); return err }
						} else {
							exec(reader, "prepare revoked_cross from '"+strings.ReplaceAll(query, "'", "''")+"'")
							defer exec(reader, "deallocate prepare revoked_cross")
							execute = func() error { _, err := reader.ExecContext(ctx, "execute revoked_cross"); return err }
						}
						exec(admin, "revoke select on table hidden_access_db.t from index_show_role")
						defer exec(admin, "grant select on table hidden_access_db.t to index_show_role")
						var denied *mysql.MySQLError
						require.ErrorAs(t, execute(), &denied)
						require.Contains(t, strings.ToLower(denied.Message), "privilege")
						var published int
						require.NoError(t, tenant.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='hidden_cross_dst' and relname='revoked_clone'").Scan(&published))
						require.Zero(t, published)
					}()
				}
				// Destination forward references belong to the target's CREATE
				// planner at EXECUTE, including both prepared protocols.
				for _, binary := range []bool{false, true} {
					func() {
						exec(tenant, "set foreign_key_checks=0")
						defer exec(tenant, "set foreign_key_checks=1")
						exec(tenant, "create table hidden_cross_dst.fk_child(id int primary key,parent_id int,constraint fk_clone foreign key(parent_id) references hidden_cross_dst.fk_parent(id))")
						defer func() {
							exec(tenant, "drop table hidden_cross_dst.fk_child")
							exec(tenant, "drop table if exists hidden_cross_dst.fk_parent")
						}()
						exec(tenant, "set foreign_key_checks=1")
						query := commands("fk_parent", hints[0])[0]
						if binary {
							stmt, err := reader.PrepareContext(ctx, query)
							require.NoError(t, err)
							defer func() { require.NoError(t, stmt.Close()) }()
							_, err = stmt.ExecContext(ctx)
							require.NoError(t, err)
						} else {
							exec(reader, "prepare fk_cross from '"+strings.ReplaceAll(query, "'", "''")+"'")
							defer exec(reader, "deallocate prepare fk_cross")
							exec(reader, "execute fk_cross")
						}
						require.Equal(t, 2, count(tenant, "hidden_cross_dst.fk_parent"))
						exec(tenant, "insert into hidden_cross_dst.fk_child values(1,1)")
						_, err := tenant.ExecContext(ctx, "insert into hidden_cross_dst.fk_child values(2,999)")
						var foreignKeyError *mysql.MySQLError
						require.ErrorAs(t, err, &foreignKeyError)
						require.Contains(t, strings.ToLower(foreignKeyError.Message), "foreign key")
					}()
				}
				exec(tenant, "create table hidden_cross_dst.owner(id int primary key,y int,key iy(y))")
				exec(tenant, "insert into hidden_cross_dst.owner values(1,10)")
				physical := indexName(tenant)
				var targetID uint32
				require.NoError(t, admin.QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_name='hidden_access_tenant'").Scan(&targetID))
				// Privileged kernel setup renames genuine tenant index storage;
				// the client under test still has only its explicit base grants.
				internal := func(q string) {
					t.Helper()
					result, err := testutils.GetSQLExecutor(cn).Exec(ctx, q, executor.Options{}.WithAccountID(targetID).WithWaitCommittedLogApplied())
					require.NoError(t, err)
					result.Close()
				}
				preparedQuery := commands("blocked_clone", hints[0])[0]
				preparedBlocked, err := reader.PrepareContext(ctx, preparedQuery)
				require.NoError(t, err)
				defer func() { require.NoError(t, preparedBlocked.Close()) }()
				exec(reader, "prepare prepared_blocked from '"+strings.ReplaceAll(preparedQuery, "'", "''")+"'")
				defer exec(reader, "deallocate prepare prepared_blocked")
				internal("alter table hidden_cross_dst.`" + physical + "` rename to blocked_clone")
				defer internal("alter table hidden_cross_dst.blocked_clone rename to `" + physical + "`")
				deny(reader, "execute prepared_blocked")
				_, err = preparedBlocked.ExecContext(ctx)
				var blockedError *mysql.MySQLError
				require.ErrorAs(t, err, &blockedError)
				require.Contains(t, strings.ToLower(blockedError.Message), "privilege")
				for _, hint := range hints {
					for _, q := range commands("blocked_clone", hint) {
						deny(reader, q)
					}
				}
				require.Equal(t, 1, count(tenant, "hidden_cross_dst.owner"))
			}()
			exec(tenant, "set experimental_hnsw_index=1")
			exec(tenant, "create table tenant_data.h(id bigint primary key,v vecf32(2))")
			exec(tenant, "insert into tenant_data.h values(1,'[1,1]'),(2,'[100000,1]')")
			exec(tenant, "create index ih using hnsw on tenant_data.h(v) op_type 'vector_l2_ops' quantization 'float16' max_bad_records 1")
			for _, mode := range []string{"ignored", "status"} {
				func() {
					rows, err := tenant.QueryContext(ctx, "show index "+mode+" from tenant_data.h")
					require.NoError(t, err)
					defer rows.Close()
					seen := 0
					for rows.Next() {
						seen++
					}
					require.NoError(t, rows.Err())
					require.Equal(t, 1, seen)
				}()
			}
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
