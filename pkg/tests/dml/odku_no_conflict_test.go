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

package dml

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestODKUWithoutConflict(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		open := func(t *testing.T, user string) *sql.DB {
			t.Helper()
			db, e := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", user, cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, e)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			return db
		}
		sys := open(t, "dump")
		conn, err := sys.Conn(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		exec := func(t *testing.T, q string) { t.Helper(); _, e := conn.ExecContext(ctx, q); require.NoError(t, e, q) }
		constraintError := func(t *testing.T, err error, code uint16) {
			t.Helper()
			var mysqlError *mysqlDriver.MySQLError
			require.ErrorAs(t, err, &mysqlError)
			require.Equal(t, code, mysqlError.Number)
		}
		exec(t, "create database odku_no_conflict")
		t.Cleanup(func() { cleanupODKUSQL(t, conn, "drop database if exists odku_no_conflict") })
		exec(t, "use odku_no_conflict")

		t.Run("prepared parameters and insert constraints", func(t *testing.T) {
			exec(t, "create table t (v int not null,c int)")
			exec(t, "create table source(n int)")
			exec(t, "insert into source values(1),(2)")
			exec(t, "insert into t values(1,10) on duplicate key update c=1/0")
			stmt, e := conn.PrepareContext(ctx, "insert into t values(?,?) on duplicate key update c=?")
			require.NoError(t, e)
			defer stmt.Close()
			_, e = stmt.ExecContext(ctx, 2, 20, "not-an-integer")
			require.NoError(t, e, "the unused update argument must not be converted")
			require.NoError(t, stmt.Close())
			stmt, e = conn.PrepareContext(ctx, "insert into t values(?,?) on duplicate key update c=(select ? from source where n=?)")
			require.NoError(t, e)
			defer stmt.Close()
			_, e = stmt.ExecContext(ctx, 3, 30, "unused", 1)
			require.NoError(t, e)
			_, e = stmt.ExecContext(ctx, 4, 40, "still-unused", 2)
			require.NoError(t, e, "a repeated execution must keep the original parameter order")
			require.NoError(t, stmt.Close())
			_, e = conn.ExecContext(ctx, "insert into t values(5,50) on duplicate key update c=nonexistent_column")
			require.ErrorContains(t, e, "nonexistent_column")
			badStmt, e := conn.PrepareContext(ctx, "insert into t values(?,?) on duplicate key update c=(select ? from nonexistent_table)")
			if badStmt != nil {
				defer badStmt.Close()
			}
			require.ErrorContains(t, e, "nonexistent_table")
			_, e = conn.ExecContext(ctx, "insert into t values(5,50) on duplicate key update c=in_range(5,c,10,0)")
			require.ErrorContains(t, e, "2nd argument of in_range must be constant")
			var count, sum int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*),sum(c) from t").Scan(&count, &sum))
			require.Equal(t, 4, count)
			require.Equal(t, 100, sum)

			exec(t, "create table parent(id int primary key)")
			exec(t, "insert into parent values(1)")
			exec(t, "create table nullable_uk(u int unique,v int not null,pid int, constraint positive check(v>0), foreign key(pid) references parent(id))")
			stmt, e = conn.PrepareContext(ctx, "insert into nullable_uk(v,pid) values(?,?) on duplicate key update v=?")
			require.NoError(t, e)
			defer stmt.Close()
			_, e = stmt.ExecContext(ctx, 5, 1, "unused")
			require.NoError(t, e)
			_, e = stmt.ExecContext(ctx, 6, 1, "unused")
			require.NoError(t, e)
			_, e = stmt.ExecContext(ctx, -1, 1, "unused")
			constraintError(t, e, moerr.ER_CHECK_CONSTRAINT_VIOLATED)
			_, e = stmt.ExecContext(ctx, 7, 999, "unused")
			constraintError(t, e, moerr.ErrInternal)
			require.ErrorContains(t, e, "Cannot add or update a child row: a foreign key constraint fails")
			require.NoError(t, stmt.Close())
			_, e = conn.ExecContext(ctx, "insert into nullable_uk(v,pid) values(7,999)")
			constraintError(t, e, moerr.ErrInternal)
			require.ErrorContains(t, e, "Cannot add or update a child row: a foreign key constraint fails")
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*),sum(v) from nullable_uk").Scan(&count, &sum))
			require.Equal(t, 2, count)
			require.Equal(t, 11, sum)

			exec(t, "create table live_unique(u int unique,v int)")
			exec(t, "insert into live_unique values(1,10)")
			stmt, e = conn.PrepareContext(ctx, "insert into live_unique values(?,?) on duplicate key update v=values(v)")
			require.NoError(t, e)
			defer stmt.Close()
			_, e = stmt.ExecContext(ctx, 1, 20)
			require.NoError(t, e, "a parameterized unique key must still resolve conflicts")
			require.NoError(t, stmt.Close())
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*),sum(v) from live_unique").Scan(&count, &sum))
			require.Equal(t, 1, count)
			require.Equal(t, 20, sum)
			require.NoError(t, conn.QueryRowContext(ctx, "select v from live_unique force index(u) where u=1").Scan(&sum))
			require.Equal(t, 20, sum)
		})

		t.Run("internal prepared parameters with pruned columns", func(t *testing.T) {
			exec(t, "create table internal_nokey(id int,v int)")
			exec(t, "create table internal_key(id int primary key,v int)")
			exec(t, "insert into internal_key values(1,5)")
			internal := cn.RawService().(cnservice.Service).GetSQLExecutor()
			// The unused middle column is pruned; the final RHS is dead without
			// a key but evaluated when the keyed insert conflicts.
			for _, tc := range []struct {
				table  string
				params []string
				id, v  int
			}{
				{"internal_nokey", []string{"2", "999", "17", "73"}, 2, 17},
				{"internal_key", []string{"1", "999", "17", "73"}, 1, 73},
			} {
				query := "insert into " + tc.table + " select q.a,q.c from (select ? a, ? b, ? c) q on duplicate key update v=?"
				result, err := internal.Exec(ctx, query, executor.Options{}.WithDatabase("odku_no_conflict").WithForceRebuildPlan().WithStatementOption(executor.StatementOption{}.WithParams(tc.params)))
				result.Close()
				require.NoError(t, err, query)
				var count, id, v int
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*),min(id),min(v) from "+tc.table).Scan(&count, &id, &v))
				require.Equal(t, 1, count)
				require.Equal(t, tc.id, id)
				require.Equal(t, tc.v, v)
			}
		})

		t.Run("prepared validation dependencies", func(t *testing.T) {
			exec(t, "create table prepared_target(v int,c int)")
			exec(t, "create table prepared_source(n int,other int)")
			exec(t, "insert into prepared_source values(1,2)")
			stmt, e := conn.PrepareContext(ctx, "insert into prepared_target values(?,?) on duplicate key update c=(select n from prepared_source)")
			require.NoError(t, e)
			defer stmt.Close()
			_, e = stmt.ExecContext(ctx, 1, 10)
			require.NoError(t, e)
			exec(t, "alter table prepared_source drop column n")
			_, e = stmt.ExecContext(ctx, 2, 20)
			require.Error(t, e, "a changed RHS schema must invalidate the prepared plan")
			var count int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from prepared_target").Scan(&count))
			require.Equal(t, 1, count)
		})

		t.Run("unreachable subqueries have no effects", func(t *testing.T) {
			exec(t, "create table dead(v int,s varchar(1024))")
			exec(t, "create table dead_source(n int)")
			exec(t, "insert into dead_source values(1),(2)")
			const key = "CODEX_31979_NO_KEY_DEAD"
			t.Cleanup(func() {
				cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
				defer stop()
				// Catalog writes belong to the internal executor, as in the
				// builtin under test. Public SQL privilege checks remain enabled.
				res, err := cn.RawService().(cnservice.Service).GetSQLExecutor().Exec(cleanupCtx,
					"delete from mo_catalog.mo_feature_registry where feature_code='"+key+"'",
					executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied())
				res.Close()
				if err != nil {
					t.Errorf("feature registry cleanup: %v", err)
				}
			})
			checkNoEffect := func() {
				t.Helper()
				var count int
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_feature_registry where feature_code='"+key+"'").Scan(&count))
				require.Zero(t, count)
			}
			exec(t, `insert into dead values(1,'one') on duplicate key update s=mo_feature_registry_upsert('CODEX_31979_NO_KEY_DEAD','never','{"allowed_scope":[]}',true)`)
			checkNoEffect()
			exec(t, `insert into dead values(2,'two') on duplicate key update s=(select mo_feature_registry_upsert('CODEX_31979_NO_KEY_DEAD','never','{"allowed_scope":[]}',true) from dead_source)`)
			checkNoEffect()
			stmt, e := conn.PrepareContext(ctx, "insert into dead values(?,?) on duplicate key update s=if(? in (select ?),'yes','no')")
			require.NoError(t, e)
			defer stmt.Close()
			_, e = stmt.ExecContext(ctx, 3, "three", 7, 8)
			require.NoError(t, e)
			require.NoError(t, stmt.Close())
			_, e = conn.ExecContext(ctx, "insert into dead values(5,'five') on duplicate key update s=(select nth_value(n,0) over(order by n) from dead_source limit 1)")
			require.Error(t, e, "window-function arguments are still validated")
			_, e = conn.ExecContext(ctx, `insert into dead values(6,'six') on duplicate key update s=in_range(5,length(mo_feature_registry_upsert('CODEX_31979_NO_KEY_DEAD','never','{"allowed_scope":[]}',true)),10,0)`)
			require.Error(t, e, "an effectful expression cannot be a constant range bound")
			checkNoEffect()
			var max int
			require.NoError(t, conn.QueryRowContext(ctx, "with recursive r(n) as (select 1 union all select n+1 from r where n<2) select max(n) from r").Scan(&max))
			require.Equal(t, 2, max)
			exec(t, `insert into dead values(4,'four') on duplicate key update s=(with recursive r(n,s) as (select 1,mo_feature_registry_upsert('CODEX_31979_NO_KEY_DEAD','never','{"allowed_scope":[]}',true) union all select n+1,s from r where n<2) select s from r limit 1)`)
			checkNoEffect()
		})

		t.Run("authorization survives optimization", func(t *testing.T) {
			exec(t, "create account odku_no_conflict_auth admin_name 'admin' identified by '111'")
			t.Cleanup(func() { cleanupODKUSQL(t, conn, "drop account if exists odku_no_conflict_auth") })
			admin := open(t, "odku_no_conflict_auth#admin")
			execAdmin := func(q string) { t.Helper(); _, e := admin.ExecContext(ctx, q); require.NoError(t, e, q) }
			for _, q := range []string{
				"create database app", "create table app.target(v int,c int)", "create table app.allowed(n int)",
				"create table app.secret(n int)", "insert into app.allowed values(1)", "insert into app.secret values(9)",
				"create role writer", "grant connect on account * to writer", "create user u identified by '111' default role writer",
				"grant writer to u", "grant insert,update on table app.target to writer", "grant select on table app.allowed to writer",
			} {
				execAdmin(q)
			}
			writerDB := open(t, "odku_no_conflict_auth#u#writer")
			writer, e := writerDB.Conn(ctx)
			require.NoError(t, e)
			t.Cleanup(func() { require.NoError(t, writer.Close()) })
			for _, q := range []string{
				"insert into app.target values(1,10) on duplicate key update c=5",
				"insert into app.target select 2,20 from app.allowed on duplicate key update c=5",
			} {
				_, e := writer.ExecContext(ctx, q)
				require.NoError(t, e, q)
			}
			for _, q := range []string{
				"insert into app.target values(3,30) on duplicate key update c=(select n from app.secret)",
				"insert into app.target select 4,40 from app.allowed on duplicate key update c=(select n from app.secret)",
			} {
				_, e := writer.ExecContext(ctx, q)
				require.ErrorContains(t, e, "privilege", q)
			}
			execAdmin("grant select on table app.secret to writer")
			queries := []string{
				"insert into app.target values(5,50) on duplicate key update c=(select n from app.secret)",
				"insert into app.target select 6,60 from app.allowed on duplicate key update c=(select n from app.secret)",
			}
			statements := make([]*sql.Stmt, 0, len(queries))
			for _, q := range queries {
				stmt, e := writer.PrepareContext(ctx, q)
				require.NoError(t, e, q)
				t.Cleanup(func() { require.NoError(t, stmt.Close()) })
				statements = append(statements, stmt)
			}
			execAdmin("revoke select on table app.secret from writer")
			for _, stmt := range statements {
				_, e := stmt.ExecContext(ctx)
				require.ErrorContains(t, e, "privilege")
			}

			execAdmin("create view app.secret_view as select n from app.secret")
			execAdmin("grant select on view app.secret_view to writer")
			var value int
			require.NoError(t, writer.QueryRowContext(ctx, "select n from app.secret_view").Scan(&value))
			require.Equal(t, 9, value)
			viewStmt, e := writer.PrepareContext(ctx, "insert into app.target values(?,?) on duplicate key update c=(select n from app.secret_view)")
			require.NoError(t, e)
			t.Cleanup(func() { require.NoError(t, viewStmt.Close()) })
			_, e = viewStmt.ExecContext(ctx, 7, 70)
			require.NoError(t, e, "the view grant must suffice without direct table access")
			execAdmin("revoke select on view app.secret_view from writer")
			_, e = viewStmt.ExecContext(ctx, 8, 80)
			require.ErrorContains(t, e, "privilege")
			execAdmin("grant select on view app.secret_view to writer")
			_, e = viewStmt.ExecContext(ctx, 9, 90)
			require.NoError(t, e)
			execAdmin("drop view app.secret_view")
			_, e = viewStmt.ExecContext(ctx, 10, 100)
			require.Error(t, e, "a dropped view must invalidate the prepared plan")
			var count int
			require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from app.target").Scan(&count))
			require.Equal(t, 4, count)
			require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from app.target where v in (8,10)").Scan(&count))
			require.Zero(t, count)
		})
	})
}
