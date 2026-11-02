// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlintegration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestRenameTableImplicitCommit(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "rollback")
			_ = conn.Close()
			_ = db.Close()
			cleanupSQLIntegration(t, cn, "drop database if exists issue29295")
		}()
		exec := func(s string) { t.Helper(); _, err := conn.ExecContext(ctx, s); require.NoError(t, err, s) }
		// db uses a different connection from the pinned writer. Every read
		// therefore proves committed visibility, not just the writer's workspace.
		count := func(s string, want int) {
			t.Helper()
			var got int
			require.NoError(t, db.QueryRowContext(ctx, s).Scan(&got))
			require.Equal(t, want, got, s)
		}
		marker := func(id, want int) {
			t.Helper()
			count(fmt.Sprintf("select count(*) from issue29295.markers where id=%d", id), want)
		}
		table := func(name string, want int) {
			t.Helper()
			count("select count(*) from information_schema.tables where table_schema='issue29295' and table_name='"+name+"'", want)
		}
		exec("create database issue29295")
		exec("use issue29295")
		exec("create table markers(id int primary key)")
		exec("create table nc_workflow_executions(id int primary key)")
		exec("begin")
		exec("insert into markers values(1)")
		exec("rename table nc_workflow_executions to nc_automation_executions")
		exec("rollback")
		marker(1, 1)
		table("nc_workflow_executions", 0)
		table("nc_automation_executions", 1)

		// A failed DDL still commits preceding work but publishes no rename.
		exec("create table occupied(id int)")
		exec("begin")
		exec("insert into markers values(2)")
		_, err = conn.ExecContext(ctx, "rename table nc_automation_executions to occupied")
		require.Error(t, err)
		exec("rollback")
		marker(2, 1)
		table("nc_automation_executions", 1)
		exec("begin")
		exec("insert into markers values(3)")
		_, err = conn.ExecContext(ctx, "rename table nc_automation_executions to moved, absent to another")
		require.Error(t, err)
		exec("rollback")
		marker(3, 1)
		table("nc_automation_executions", 1)
		table("moved", 0)

		// PREPARE itself cannot commit; EXECUTE must use the saved DDL policy.
		exec("begin")
		exec("insert into markers values(4)")
		exec("prepare rename_stmt from 'rename table nc_automation_executions to prepared_name'")
		marker(4, 0)
		exec("execute rename_stmt")
		exec("rollback")
		marker(4, 1)
		table("prepared_name", 1)
		exec("deallocate prepare rename_stmt")
		exec("begin")
		exec("insert into markers values(5)")
		prepared, err := conn.PrepareContext(ctx, "rename table prepared_name to binary_name")
		require.NoError(t, err)
		defer func() { require.NoError(t, prepared.Close()) }()
		marker(5, 0)
		_, err = prepared.ExecContext(ctx)
		require.NoError(t, err)
		exec("rollback")
		marker(5, 1)
		table("binary_name", 1)

		exec("begin")
		exec("insert into markers values(6)")
		_, err = conn.ExecContext(ctx, "execute missing_prepared_stmt")
		require.Error(t, err)
		marker(6, 0)
		exec("rollback")
		marker(6, 0)

		// Reset the implicit-commit flag between statements in one COM_QUERY.
		exec("set autocommit=0")
		exec("insert into markers values(7); rename table binary_name to chain_tmp, chain_tmp to final_name; insert into markers values(8); rollback")
		marker(7, 1)
		marker(8, 0)
		table("final_name", 1)
		var auto int
		require.NoError(t, conn.QueryRowContext(ctx, "select @@autocommit").Scan(&auto))
		require.Zero(t, auto)
		exec("insert into markers values(9)")
		_, err = conn.ExecContext(ctx, "rename table final_name to occupied")
		require.Error(t, err)
		exec("rollback")
		marker(9, 1)
		table("final_name", 1)
		exec("set autocommit=1")

		// Internal execution retains the enclosing transaction's ownership.
		sentinel := errors.New("rollback shared rename")
		err = testutils.GetSQLExecutor(cn).ExecTxn(ctx, func(tx executor.TxnExecutor) error {
			for _, s := range []string{"insert into markers values(10)", "rename table final_name to internal_name"} {
				r, err := tx.Exec(s, executor.StatementOption{})
				if err != nil {
					return err
				}
				r.Close()
			}
			return sentinel
		}, executor.Options{}.WithDatabase("issue29295"))
		require.ErrorIs(t, err, sentinel)
		marker(10, 0)
		table("final_name", 1)
		table("internal_name", 0)

		// Migration DDL created in the preceding transaction must be visible
		// to the fresh rename transaction after its implicit precommit.
		exec("begin")
		exec("create table created_in_txn(id int)")
		exec("insert into created_in_txn values(11)")
		exec("rename table created_in_txn to migrated")
		exec("rollback")
		table("created_in_txn", 0)
		count("select count(*) from issue29295.migrated where id=11", 1)

		// ReplaceDef is prepended to each composed ALTER. Its stale CHECK
		// snapshot must not overwrite the final table/column rename identities.
		for _, tc := range []struct{ name, oldName, newName, actions string }{
			{"table_first", "combo_first", "renamed_first", "rename to renamed_first, rename column a to b"},
			{"column_first", "combo_last", "renamed_last", "rename column a to b, rename to renamed_last"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
				defer cancel()
				conn, err := db.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				exec := func(statement string) {
					t.Helper()
					_, err := conn.ExecContext(ctx, statement)
					require.NoError(t, err, statement)
				}
				exec("use issue29295")
				explicitName := "named_" + tc.name
				exec("create table " + tc.oldName + " (a int, check(a > 0), constraint " + explicitName + " check(a < 100))")
				exec("alter table " + tc.oldName + " " + tc.actions)
				var tableName, ddl string
				require.NoError(t, conn.QueryRowContext(ctx, "show create table "+tc.newName).Scan(&tableName, &ddl))
				require.Equal(t, tc.newName, tableName)
				require.Contains(t, ddl, "CONSTRAINT `"+tc.newName+"_chk_1` CHECK (`b` > 0)")
				require.Contains(t, ddl, "CONSTRAINT `"+explicitName+"` CHECK (`b` < 100)")
				require.False(t, strings.Contains(ddl, tc.oldName+"_chk_1"))
				_, err = conn.ExecContext(ctx, "insert into "+tc.newName+" values(-1)")
				var checkErr *mysql.MySQLError
				require.ErrorAs(t, err, &checkErr)
				require.Equal(t, uint16(3819), checkErr.Number)
				require.Contains(t, checkErr.Message, tc.newName+"_chk_1")
				var rows int
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+tc.newName).Scan(&rows))
				require.Zero(t, rows)
				exec("insert into " + tc.newName + " values(1)")
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+tc.newName+" where b=1").Scan(&rows))
				require.Equal(t, 1, rows)
				exec("create table " + tc.oldName + " (a int, check(a > 0))")
				require.NoError(t, conn.QueryRowContext(ctx, "show create table "+tc.oldName).Scan(&tableName, &ddl))
				require.Contains(t, ddl, "CONSTRAINT `"+tc.oldName+"_chk_1` CHECK (`a` > 0)")
			})
		}
	})
}

// Standalone RENAME uses persisted names at both ends, while ordinary SQL and
// ALTER continue to see the pinned session's temporary aliases (#31085).
func TestRenameTableIgnoresTemporaryAliases(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists rename_shadow")
		owner, err := db.Conn(ctx)
		require.NoError(t, err)
		defer owner.Close()
		observer, err := db.Conn(ctx)
		require.NoError(t, err)
		defer observer.Close()
		exec := func(s string) { t.Helper(); _, err := owner.ExecContext(ctx, s); require.NoError(t, err, s) }
		value := func(conn *sql.Conn, s string, want int) {
			t.Helper()
			var got int
			require.NoError(t, conn.QueryRowContext(ctx, s).Scan(&got), s)
			require.Equal(t, want, got, s)
		}
		fails := func(conn *sql.Conn, s string, code uint16) {
			t.Helper()
			_, err := conn.ExecContext(ctx, s)
			var sqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &sqlErr, s)
			require.Equal(t, code, sqlErr.Number, s)
		}
		identity := func(name string) uint64 {
			t.Helper()
			var id uint64
			require.NoError(t, observer.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='rename_shadow' and relname=?", name).Scan(&id))
			return id
		}
		exec("create database rename_shadow")
		exec("use rename_shadow")
		_, err = observer.ExecContext(ctx, "use rename_shadow")
		require.NoError(t, err)
		exec("create table src(x int)")
		exec("insert into src values(11)")
		id := identity("src")
		exec("create temporary table src(y int)")
		exec("insert into src values(22)")
		exec("rename table rename_shadow.src to rename_shadow.dst")
		value(owner, "select y from src", 22)
		value(owner, "select x from dst", 11)
		value(observer, "select x from dst", 11)
		require.Equal(t, id, identity("dst"))
		fails(observer, "select * from src", 1146)
		fails(owner, "rename table src to absent", 1146)
		value(owner, "select y from src", 22)

		// Target aliases do not occupy the persisted namespace.
		exec("create temporary table target(z int)")
		exec("insert into target values(44)")
		exec("rename table dst to target")
		value(owner, "select z from target", 44)
		value(observer, "select x from target", 11)
		exec("drop temporary table target")
		value(owner, "select x from target", 11)
		require.Equal(t, id, identity("target"))
		exec("create table occupied(x int)")
		exec("insert into occupied values(55)")
		occupiedID := identity("occupied")
		exec("create temporary table occupied(y int)")
		exec("insert into occupied values(66)")
		fails(owner, "rename table target to occupied", 1050)
		value(owner, "select y from occupied", 66)
		value(observer, "select x from occupied", 55)
		require.Equal(t, occupiedID, identity("occupied"))
		require.Equal(t, id, identity("target"))

		// A later pair's planning failure cannot publish the earlier rename.
		exec("create temporary table target(y int)")
		exec("insert into target values(123)")
		for _, tail := range []string{"missing to another", "occupied to moved"} {
			code := uint16(1146)
			if strings.HasPrefix(tail, "occupied") {
				code = 1050
			}
			fails(owner, "rename table target to moved, "+tail, code)
			require.Equal(t, id, identity("target"))
			fails(observer, "select * from moved", 1146)
		}
		exec("create temporary table intermediate(y int)")
		exec("insert into intermediate values(77)")
		exec("rename table target to intermediate, intermediate to final_name")
		value(owner, "select y from intermediate", 77)
		value(observer, "select x from final_name", 11)
		require.Equal(t, id, identity("final_name"))

		// PREPARE before and after alias creation must select the persisted source.
		exec("prepare before_shadow from 'rename table final_name to prepared_name'")
		exec("create temporary table final_name(y int)")
		exec("insert into final_name values(88)")
		exec("execute before_shadow")
		exec("deallocate prepare before_shadow")
		value(owner, "select y from final_name", 88)
		exec("create temporary table prepared_name(y int)")
		exec("insert into prepared_name values(99)")
		exec("prepare after_shadow from 'rename table prepared_name to final_base'")
		exec("execute after_shadow")
		exec("deallocate prepare after_shadow")
		value(owner, "select y from prepared_name", 99)
		value(observer, "select x from final_base", 11)
		require.Equal(t, id, identity("final_base"))

		// A prepared rename must track the persisted source, not its temporary
		// shadow, when another session replaces that source with a view.
		exec("create table replaced_src(x int)")
		exec("create temporary table replaced_src(y int)")
		exec("insert into replaced_src values(101)")
		exec("prepare replaced_source from 'rename table replaced_src to replaced_dst'")
		_, err = observer.ExecContext(ctx, "drop table replaced_src")
		require.NoError(t, err)
		_, err = observer.ExecContext(ctx, "create view replaced_src as select 202 as x")
		require.NoError(t, err)
		_, preparedErr := owner.ExecContext(ctx, "execute replaced_source")
		var preparedSQL *mysql.MySQLError
		require.ErrorAs(t, preparedErr, &preparedSQL)
		_, freshErr := owner.ExecContext(ctx, "rename table replaced_src to replaced_dst")
		var freshSQL *mysql.MySQLError
		require.ErrorAs(t, freshErr, &freshSQL)
		require.Contains(t, freshSQL.Message, "alter view")
		require.Equal(t, freshSQL.Number, preparedSQL.Number)
		require.Equal(t, freshSQL.Message, preparedSQL.Message)
		exec("deallocate prepare replaced_source")
		value(owner, "select y from replaced_src", 101)
		value(observer, "select x from replaced_src", 202)
		fails(observer, "select * from replaced_dst", 1146)
		exec("drop temporary table replaced_src")

		// A temporary-only destination is captured as absent in the persisted
		// namespace; creating a persisted destination invalidates the plan too.
		exec("create table prepared_src(x int)")
		exec("insert into prepared_src values(303)")
		preparedSourceID := identity("prepared_src")
		exec("create temporary table prepared_dst(y int)")
		exec("insert into prepared_dst values(404)")
		exec("prepare occupied_target from 'rename table prepared_src to prepared_dst'")
		_, err = observer.ExecContext(ctx, "create table prepared_dst(x int)")
		require.NoError(t, err)
		_, err = observer.ExecContext(ctx, "insert into prepared_dst values(505)")
		require.NoError(t, err)
		preparedTargetID := identity("prepared_dst")
		fails(owner, "execute occupied_target", 1050)
		exec("deallocate prepare occupied_target")
		value(owner, "select y from prepared_dst", 404)
		value(observer, "select x from prepared_src", 303)
		value(observer, "select x from prepared_dst", 505)
		require.Equal(t, preparedSourceID, identity("prepared_src"))
		require.Equal(t, preparedTargetID, identity("prepared_dst"))
		exec("drop temporary table prepared_dst")

		exec("alter table src add column extra int default 7")
		value(owner, "select extra from src", 7)
		exec("alter table src rename to a")
		value(owner, "select y from a", 22)

		// The new operation must not turn owned physical temporary roots into
		// ordinary tables or expose another session's physical root. The short
		// ALTER alias also keeps the physical name within the destination identifier limit.
		var physical string
		require.NoError(t, owner.QueryRowContext(ctx, "select relname from mo_catalog.mo_tables where reldatabase='rename_shadow' and relkind='temporary_table' and relname like '%_a'").Scan(&physical))
		qualified := "`rename_shadow`.`" + physical + "`"
		fails(owner, "rename table "+qualified+" to illicit", 20102)
		fails(observer, "rename table "+qualified+" to illicit", 1146)
		fails(owner, "rename table final_base to "+qualified, 1050)
		value(observer, "select x from final_base", 11)
		value(owner, "select y from a", 22)
		for _, name := range []string{"a", "occupied", "target", "intermediate", "final_name", "prepared_name"} {
			exec("drop temporary table " + name)
		}
	})
}
