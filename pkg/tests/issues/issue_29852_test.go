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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestIssue29852ForwardCreateNeighborSchedules(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn0, err := cluster.GetCNService(0)
		require.NoError(t, err)
		cn1, err := cluster.GetCNService(1)
		require.NoError(t, err)
		db0, err := sql.Open("mysql", issue27487DSN(cn0.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db0.Close()
		db1, err := sql.Open("mysql", issue27487DSN(cn1.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db1.Close()
		var registryID, tablesID uint64
		require.NoError(t, db0.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname='mo_feature_registry'").Scan(&registryID))
		require.NoError(t, db0.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname='mo_tables'").Scan(&tablesID))
		for _, pending := range []bool{true, false} {
			t.Run(fmt.Sprintf("pending_parent_%t", pending), func(t *testing.T) {
				database := fmt.Sprintf("issue29852_neighbor_%t", pending)
				execSQLRequire(t, ctx, db0, "create database "+database)
				defer func() {
					cleanupCtx, stop := context.WithTimeout(context.Background(), 20*time.Second)
					defer stop()
					_, _ = db0.ExecContext(cleanupCtx, "drop database if exists "+database)
				}()
				execSQLRequire(t, ctx, db0, "create table "+database+".guard(id int)")
				holder, err := db0.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer holder.Rollback()
				if pending {
					_, err = holder.ExecContext(ctx, "drop table "+database+".guard")
					require.NoError(t, err)
				} else {
					_, err = holder.ExecContext(ctx, "create table "+database+".parent(id int primary key)")
					require.NoError(t, err)
					_, err = holder.ExecContext(ctx, "drop table "+database+".parent")
					require.NoError(t, err)
				}
				conn, err := db1.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				_, err = conn.ExecContext(ctx, "set foreign_key_checks=0")
				require.NoError(t, err)
				operationCtx, stop := context.WithTimeout(ctx, 20*time.Second)
				defer stop()
				queued := make(chan struct{}, 1)
				expected := tablesID
				query := "create table " + database + ".parent(id int primary key)"
				if pending {
					expected = registryID
					query = "create table " + database + ".child(id int primary key,pid int,constraint fk_p foreign key(pid) references " + database + ".parent(id))"
				}
				restore := lockservice.SetWaiterEnqueuedHookForTest(func(id uint64, _ []byte, _ [][]byte) {
					if id == expected {
						select {
						case queued <- struct{}{}:
						default:
						}
					}
				})
				defer restore()
				done := make(chan error, 1)
				finished := false
				defer func() {
					stop()
					_ = holder.Rollback()
					if !finished {
						select {
						case <-done:
						case <-ctx.Done():
							t.Error("CREATE did not terminate")
						}
					}
				}()
				go func() { _, err := conn.ExecContext(operationCtx, query); done <- err }()
				select {
				case <-queued:
				case err := <-done:
					finished = true
					t.Fatalf("CREATE did not wait: %v", err)
				case <-operationCtx.Done():
					t.Fatal("no target lock waiter")
				}
				if pending {
					_, err = holder.ExecContext(ctx, "create table "+database+".parent(id int primary key)")
				} else {
					_, err = holder.ExecContext(ctx, "set foreign_key_checks=0")
					require.NoError(t, err)
					_, err = holder.ExecContext(ctx, "create table "+database+".child(id int primary key,pid int,constraint fk_p foreign key(pid) references "+database+".parent(id))")
				}
				require.NoError(t, err)
				require.NoError(t, holder.Commit())
				err = <-done
				finished = true
				require.NoError(t, err)
				var name, ddl string
				require.NoError(t, db0.QueryRowContext(ctx, "show create table "+database+".child").Scan(&name, &ddl))
				t.Logf("child definition after both CREATEs committed: %s", ddl)
				_, err = db0.ExecContext(ctx, "set foreign_key_checks=1")
				require.NoError(t, err)
				_, err = db0.ExecContext(ctx, "insert into "+database+".child values(1,999)")
				require.Error(t, err, "both tables exist; pending FK must be materialized and reject invalid insert")
			})
		}
	})
}

func TestIssue29852ForwardCreateReplacementGeneration(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		cn0, err := cluster.GetCNService(0)
		require.NoError(t, err)
		cn1, err := cluster.GetCNService(1)
		require.NoError(t, err)
		db0, err := sql.Open("mysql", issue27487DSN(cn0.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db0.Close()
		db1, err := sql.Open("mysql", issue27487DSN(cn1.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db1.Close()
		const database = "issue29852_generation"
		execSQLRequire(t, ctx, db0, "create database "+database)
		defer func() {
			clean, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			_, _ = db0.ExecContext(clean, "drop database if exists "+database)
		}()
		conn, err := db0.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, "set foreign_key_checks=0")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "create table "+database+".child(id int primary key,pid int,constraint fk_forward foreign key(pid) references "+database+".parent(id))")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "set foreign_key_checks=1")
		require.NoError(t, err)
		execSQLRequire(t, ctx, db0, "create table "+database+".guard(id int)")
		var registryID, oldChild uint64
		require.NoError(t, db0.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname='mo_feature_registry'").Scan(&registryID))
		require.NoError(t, db0.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='child'", database).Scan(&oldChild))
		holder, err := db0.BeginTx(ctx, nil)
		require.NoError(t, err)
		opctx, stop := context.WithTimeout(ctx, 25*time.Second)
		defer stop()
		var done chan error
		finished := false
		defer func() {
			stop()
			_ = holder.Rollback()
			if done != nil && !finished {
				select {
				case <-done:
				case <-ctx.Done():
					t.Error("parent CREATE did not terminate")
				}
			}
		}()
		_, err = holder.ExecContext(ctx, "drop table "+database+".guard")
		require.NoError(t, err)
		queued := make(chan struct{}, 1)
		restore := lockservice.SetWaiterEnqueuedHookForTest(func(id uint64, _ []byte, _ [][]byte) {
			if id == registryID {
				select {
				case queued <- struct{}{}:
				default:
				}
			}
		})
		defer restore()
		done = make(chan error, 1)
		go func() {
			_, e := db1.ExecContext(opctx, "create table "+database+".parent(id int primary key)")
			done <- e
		}()
		select {
		case <-queued:
		case e := <-done:
			finished = true
			t.Fatalf("parent CREATE did not wait on G: %v", e)
		case <-opctx.Done():
			t.Fatal("parent CREATE did not enqueue")
		}
		_, err = holder.ExecContext(ctx, "drop table "+database+".child")
		require.NoError(t, err)
		_, err = holder.ExecContext(ctx, "create table "+database+".child(id int primary key,pid int)")
		require.NoError(t, err)
		require.NoError(t, holder.Commit())
		select {
		case err = <-done:
			finished = true
		case <-opctx.Done():
			t.Fatal("parent CREATE did not finish")
		}
		require.NoError(t, err, "parent CREATE must finish successfully after the stale declaration is removed")
		var newChild uint64
		var n int
		require.NoError(t, db0.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='child'", database).Scan(&newChild))
		require.NotEqual(t, oldChild, newChild)
		require.NoError(t, db0.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_foreign_keys where db_name=? and table_name='child'", database).Scan(&n))
		require.Zero(t, n)
		var tableName, ddl string
		require.NoError(t, db0.QueryRowContext(ctx, "show create table "+database+".child").Scan(&tableName, &ddl))
		t.Log(ddl)
		require.NotContains(t, strings.ToLower(ddl), "foreign key", "stale forward repair must not mutate a replacement child that declares no FK")
		_, err = db0.ExecContext(ctx, "insert into "+database+".child values(1,999)")
		require.NoError(t, err, "the replacement child was explicitly created without any FK")
	})
}

func TestPendingForeignKeySurvivesInternalReplacement(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const database = "pending_fk_replacement"
		execSQLRequire(t, ctx, db, "create database "+database)
		defer func() {
			clean, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			_, _ = db.ExecContext(clean, "drop database if exists "+database)
		}()
		for _, operation := range []string{"truncate", "copy"} {
			t.Run(operation, func(t *testing.T) {
				child, parent := operation+"_child", operation+"_parent"
				for _, q := range []string{
					"set foreign_key_checks=0",
					"create table " + database + "." + child + "(id int primary key,pid int,constraint fk_pending foreign key(pid) references " + database + "." + parent + "(id))",
				} {
					_, err := conn.ExecContext(ctx, q)
					require.NoError(t, err, q)
				}
				q := "truncate table " + database + "." + child
				if operation == "copy" {
					q = "alter table " + database + "." + child + " add column extra int, algorithm=copy"
				}
				_, err := conn.ExecContext(ctx, "insert into "+database+"."+child+"(id,pid) values(7,null)")
				require.NoError(t, err)
				var count int
				if operation == "copy" {
					var originalID, restoredID uint64
					require.NoError(t, conn.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname=?", database, child).Scan(&originalID))
					for _, statement := range []string{"begin", q, "rollback"} {
						_, err = conn.ExecContext(ctx, statement)
						require.NoError(t, err, statement)
					}
					require.NoError(t, conn.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname=?", database, child).Scan(&restoredID))
					require.Equal(t, originalID, restoredID, "rollback restores the original physical generation")
					require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+database+"."+child+" where id=7").Scan(&count))
					require.Equal(t, 1, count, "rollback restores source rows")
				}
				_, err = conn.ExecContext(ctx, q)
				require.NoError(t, err, q)
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_foreign_keys where db_name=? and table_name=?", database, child).Scan(&count))
				require.Equal(t, 1, count, "catalog-only declarations must survive replacement")
				_, err = conn.ExecContext(ctx, "set foreign_key_checks=1")
				require.NoError(t, err)
				_, err = conn.ExecContext(ctx, "create table "+database+"."+parent+"(id int primary key)")
				require.NoError(t, err)
				_, err = conn.ExecContext(ctx, "insert into "+database+"."+child+"(id,pid) values(1,999)")
				require.Error(t, err, "recreated child must acquire its declared FK when parent appears")
				_, err = conn.ExecContext(ctx, "drop table "+database+"."+child)
				require.NoError(t, err)
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_foreign_keys where db_name=? and table_name=?", database, child).Scan(&count))
				require.Zero(t, count, "ordinary DROP must remove declarations after replacement")
			})
		}
		t.Run("multiple_constraints_one_child", func(t *testing.T) {
			defer func() { _, _ = conn.ExecContext(context.Background(), "rollback") }()
			for _, q := range []string{
				"create table " + database + ".existing_parent(id int primary key)",
				"set foreign_key_checks=0",
				"begin",
				"create table " + database + ".multi_child(id int primary key,a int,b int,existing_id int,unique key uq(a),constraint fk_a foreign key(a) references " + database + ".multi_parent(id),constraint fk_b foreign key(b) references " + database + ".multi_parent(id),constraint fk_existing foreign key(existing_id) references " + database + ".existing_parent(id))",
				"set foreign_key_checks=1",
				"create table " + database + ".multi_parent(id int primary key)",
				"insert into " + database + ".multi_parent values(1)",
				"insert into " + database + ".existing_parent values(1)",
				"insert into " + database + ".multi_child values(1,1,1,1)",
				"commit",
			} {
				_, err := conn.ExecContext(ctx, q)
				require.NoError(t, err, q)
			}
			for _, values := range []string{"(2,999,1,1)", "(2,null,999,1)", "(2,null,1,999)", "(2,1,1,1)"} {
				_, err := conn.ExecContext(ctx, "insert into "+database+".multi_child values"+values)
				require.Error(t, err, "both repaired FKs, unrelated FK and unique constraint must remain enforced: %s", values)
			}
		})
	})
}

func TestDropIndexAdmissionDoesNotHoldNamesWhileWaiting(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		cn0, err := cluster.GetCNService(0)
		require.NoError(t, err)
		cn1, err := cluster.GetCNService(1)
		require.NoError(t, err)
		db0, err := sql.Open("mysql", issue27487DSN(cn0.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db0.Close()
		db1, err := sql.Open("mysql", issue27487DSN(cn1.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db1.Close()
		const database = "drop_index_admission"
		execSQLRequire(t, ctx, db0, "create database "+database)
		defer func() {
			clean, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			_, _ = db0.ExecContext(clean, "drop database if exists "+database)
		}()
		for _, terminal := range []string{"commit", "cancel", "timeout"} {
			t.Run(terminal, func(t *testing.T) {
				table := database + "." + terminal + "_table"
				execSQLRequire(t, ctx, db0, "create table "+table+"(id int primary key,v int,unique key uq(v))")
				var tableID uint64
				require.NoError(t, db0.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname=?", database, terminal+"_table").Scan(&tableID))
				holder, err := db0.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer holder.Rollback()
				_, err = holder.ExecContext(ctx, "insert into "+table+" values(1,7)")
				require.NoError(t, err)
				conn, err := db1.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				var connectionID uint64
				require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
				if terminal == "timeout" {
					_, err = conn.ExecContext(ctx, "set session lock_wait_timeout=1")
					require.NoError(t, err)
				}
				opctx, stop := context.WithTimeout(ctx, 15*time.Second)
				defer stop()
				queued := make(chan struct{}, 1)
				restore := lockservice.SetWaiterEnqueuedHookForTest(func(id uint64, _ []byte, _ [][]byte) {
					if id == tableID {
						select {
						case queued <- struct{}{}:
						default:
						}
					}
				})
				defer restore()
				done := make(chan error, 1)
				finished := false
				defer func() {
					stop()
					_ = holder.Rollback()
					if !finished {
						select {
						case <-done:
						case <-ctx.Done():
							t.Error("DROP INDEX failed to terminate")
						}
					}
				}()
				go func() { _, err := conn.ExecContext(opctx, "drop index uq on "+table); done <- err }()
				select {
				case <-queued:
				case err := <-done:
					finished = true
					t.Fatalf("physical lock did not wait: %v", err)
				case <-opctx.Done():
					t.Fatal("physical waiter missing")
				}
				// The DML holder can still enter DDL: the waiter owns no name lock.
				_, err = holder.ExecContext(ctx, "create table "+database+"."+terminal+"_witness(id int)")
				require.NoError(t, err)
				if terminal == "commit" {
					require.NoError(t, holder.Commit())
				} else if terminal == "cancel" {
					_, err = db1.ExecContext(ctx, fmt.Sprintf("kill query %d", connectionID))
					require.NoError(t, err)
				}
				select {
				case err = <-done:
					finished = true
				case <-ctx.Done():
					t.Fatal("DROP INDEX hung")
				}
				if terminal == "commit" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					require.NoError(t, holder.Rollback())
					var rows int
					require.NoError(t, db0.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_indexes where table_id=? and name='uq'", tableID).Scan(&rows))
					require.Equal(t, 1, rows, "failed admission must preserve the index")
					_, err = db0.ExecContext(ctx, "drop index uq on "+table)
					require.NoError(t, err, "aborted waiter must release all ownership")
				}
				_, err = db0.ExecContext(ctx, "insert into "+table+" values(2,7),(3,7)")
				require.NoError(t, err, "successful DROP must remove uniqueness enforcement")
			})
		}
	})
}

func TestTruncatePreservesMaterializedForeignKeyIdentity(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const database = "truncate_fk_identity"
		execSQLRequire(t, ctx, db, "create database "+database)
		defer func() {
			clean, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			_, _ = db.ExecContext(clean, "drop database if exists "+database)
		}()
		for _, q := range []string{
			"create table " + database + ".parent(id int primary key,v int,unique key z(v))",
			"create table " + database + ".child(id int primary key,pid int,constraint fk_selected foreign key(pid) references " + database + ".parent(v),unique key child_unique(pid))",
			"create unique index a on " + database + ".parent(v)",
			"insert into " + database + ".parent values(1,7)",
			"insert into " + database + ".child values(1,7)",
			"truncate table " + database + ".child",
		} {
			execSQLRequire(t, ctx, db, q)
		}
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, "set foreign_key_checks=0")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "truncate table "+database+".parent")
		require.Error(t, err, "referenced parents cannot be truncated even with checks disabled")
		var parentBefore, parentAfter uint64
		require.NoError(t, conn.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='parent'", database).Scan(&parentBefore))
		for _, q := range []string{"begin", "delete from " + database + ".parent", "rollback"} {
			_, err = conn.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		require.NoError(t, conn.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='parent'", database).Scan(&parentAfter))
		require.Equal(t, parentBefore, parentAfter, "transactional DELETE rollback restores the parent generation")
		var parentRows int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+database+".parent where id=1 and v=7").Scan(&parentRows))
		require.Equal(t, 1, parentRows, "transactional DELETE rollback restores parent rows")
		_, err = conn.ExecContext(ctx, "delete from "+database+".parent")
		require.NoError(t, err)
		require.NoError(t, conn.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='parent'", database).Scan(&parentAfter))
		require.NotEqual(t, parentBefore, parentAfter, "whole-table DELETE exercises its internal truncate optimization")
		_, err = conn.ExecContext(ctx, "set foreign_key_checks=1")
		require.NoError(t, err)
		require.NoError(t, conn.Close())
		execSQLRequire(t, ctx, db, "insert into "+database+".parent values(1,7)")
		var selected string
		require.NoError(t, db.QueryRowContext(ctx, "select referenced_index_name from mo_catalog.mo_foreign_keys where db_name=? and table_name='child' and constraint_name='fk_selected'", database).Scan(&selected))
		require.Equal(t, "z", selected, "recreation must preserve the declared selected key")
		_, err = db.ExecContext(ctx, "drop index z on "+database+".parent")
		require.Error(t, err, "engine FK protection must agree with the catalog key")
		_, err = db.ExecContext(ctx, "drop index a on "+database+".parent")
		require.NoError(t, err, "new compatible key was not selected by the existing FK")
		_, err = db.ExecContext(ctx, "insert into "+database+".child values(1,999)")
		require.Error(t, err)
		execSQLRequire(t, ctx, db, "insert into "+database+".child values(1,7)")
		_, err = db.ExecContext(ctx, "insert into "+database+".child values(2,7)")
		require.Error(t, err, "unrelated uniqueness must survive")
		// Explicit DROP/CREATE owns a new schema generation and uses normal key selection.
		conn, err = db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		for _, q := range []string{
			"set foreign_key_checks=0",
			"drop table " + database + ".parent",
			"create table " + database + ".parent(id int primary key,v int,unique key z(v),unique key a(v))",
			"set foreign_key_checks=1",
		} {
			_, err = conn.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		require.NoError(t, conn.QueryRowContext(ctx, "select referenced_index_name from mo_catalog.mo_foreign_keys where db_name=? and table_name='child' and constraint_name='fk_selected'", database).Scan(&selected))
		require.Equal(t, "a", selected, "ordinary new-parent binding keeps its default selection policy")
		_, err = conn.ExecContext(ctx, "drop index z on "+database+".parent")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "drop index a on "+database+".parent")
		require.Error(t, err)
		require.NoError(t, conn.Close())
		for _, q := range []string{
			"create table " + database + ".self(id int primary key,pid int,constraint fk_self foreign key(pid) references " + database + ".self(id))",
			"insert into " + database + ".self values(1,1)",
			"truncate table " + database + ".self",
		} {
			execSQLRequire(t, ctx, db, q)
		}
		var rows int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_foreign_keys where db_name=? and table_name='self'", database).Scan(&rows))
		require.Equal(t, 1, rows)
		_, err = db.ExecContext(ctx, "insert into "+database+".self values(1,999)")
		require.Error(t, err)
		execSQLRequire(t, ctx, db, "insert into "+database+".self values(1,1)")
	})
}

func TestCreateNameAdmissionCloneAndSIFixedSnapshotControl(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const database = "clone_si_admission"
		_, err = conn.ExecContext(ctx, `select mo_feature_registry_upsert('snapshot','Snapshot feature','{"allowed_scope":["account","database","table"]}',true)`)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "create database "+database)
		require.NoError(t, err)
		defer func() {
			clean, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			_, _ = conn.ExecContext(clean, "rollback")
			_, _ = db.ExecContext(clean, "drop database if exists "+database)
		}()
		for _, q := range []string{"create table " + database + ".source(id int primary key,v int,unique key uq(v))", "insert into " + database + ".source values(1,7)", "data branch create table " + database + ".copy from " + database + ".source"} {
			_, err = conn.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}

		var value int
		require.NoError(t, conn.QueryRowContext(ctx, "select v from "+database+".copy where id=1").Scan(&value))
		require.Equal(t, 7, value)
		for _, q := range []string{"set session transaction_isolation='REPEATABLE-READ'", "begin", "drop index uq on " + database + ".source", "commit", "insert into " + database + ".source values(2,7)"} {
			_, err = conn.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
	})
}

// Physical partition tables borrow the root replacement frontier, but never
// inherit its foreign-key catalog payload.
func TestPartitionReplacementLifecycle(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const database = "partition_replacement_lifecycle"
		execSQLRequire(t, ctx, db, "create database "+database)
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			_, _ = db.ExecContext(cleanup, "drop database if exists "+database)
		}()
		value, ok := runtime.ServiceRuntime(cn.ServiceID()).GetGlobalVariables(runtime.InternalSQLExecutor)
		require.True(t, ok)
		exec := value.(executor.SQLExecutor)
		for _, partition := range []struct{ name, clause string }{
			{"by_key", "partition by key(id) partitions 2"},
			{"by_hash", "partition by hash(id) partitions 2"},
			{"by_range", "partition by range(id) (partition p0 values less than (2), partition p1 values less than (MAXVALUE))"},
		} {
			table := database + "." + partition.name
			execSQLRequire(t, ctx, db, "create table "+table+"(id int primary key, v int) "+partition.clause)
			execSQLRequire(t, ctx, db, "insert into "+table+" values(1,10),(2,20)")
			abort := errors.New("rollback partition replacement")
			err = exec.ExecTxn(defines.AttachAccountId(ctx, 0), func(tx executor.TxnExecutor) error {
				result, err := exec.Exec(defines.AttachAccountId(ctx, 0), "truncate table "+table,
					executor.Options{}.WithAccountID(0).WithTxn(tx.Txn()).WithDisableIncrStatement())
				if err != nil {
					return err
				}
				result.Close()
				return abort
			}, executor.Options{}.WithAccountID(0))
			require.ErrorIs(t, err, abort)
			var count int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+table).Scan(&count))
			require.Equal(t, 2, count, "owning transaction rollback restores root and physical partitions")
			for _, clear := range []string{"truncate table ", "delete from "} {
				execSQLRequire(t, ctx, db, clear+table)
				require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+table).Scan(&count))
				require.Zero(t, count)
				execSQLRequire(t, ctx, db, "insert into "+table+" values(1,10),(2,20)")
				require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+table+" where v=20").Scan(&count))
				require.Equal(t, 1, count, "replacement remains writable with primary-key enforcement")
			}
		}
	})
}

func TestBorrowedCreateAdmissionPreservesCallerWrites(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		ctx = defines.AttachAccountId(ctx, 0)
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		value, ok := runtime.ServiceRuntime(cn.ServiceID()).GetGlobalVariables(runtime.InternalSQLExecutor)
		require.True(t, ok)
		exec := value.(executor.SQLExecutor)
		const database = "borrowed_create_admission"
		result, err := exec.Exec(ctx, "create database "+database, executor.Options{}.WithAccountID(0))
		require.NoError(t, err)
		result.Close()
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			res, _ := exec.Exec(cleanup, "drop database if exists "+database, executor.Options{}.WithAccountID(0))
			res.Close()
		}()
		result, err = exec.Exec(ctx, "create table "+database+".existing(id int primary key,created datetime)", executor.Options{}.WithAccountID(0))
		require.NoError(t, err)
		result.Close()
		require.NoError(t, exec.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
			res, err := tx.Exec("insert into "+database+".existing values(7,'2023-11-11 10:00:01')", executor.StatementOption{})
			if err != nil {
				return err
			}
			res.Close()
			res, err = exec.Exec(ctx, "create table "+database+".new_table as select date_format(created, '%W %M %Y') from "+database+".existing", executor.Options{}.WithAccountID(0).WithTxn(tx.Txn()).WithDisableIncrStatement())
			if err != nil {
				return err
			}
			res.Close()
			return nil
		}, executor.Options{}.WithAccountID(0)))
		result, err = exec.Exec(ctx, "select id from "+database+".existing", executor.Options{}.WithAccountID(0))
		require.NoError(t, err)
		defer result.Close()
		var rows []int32
		result.ReadRows(func(_ int, cols []*vector.Vector) bool {
			rows = append(rows, executor.GetFixedRows[int32](cols[0])...)
			return true
		})
		require.Equal(t, []int32{7}, rows, "CREATE admission cannot roll back its caller's statement")
		// The borrowed CREATE uses pre-effect admission refresh. Check metadata
		// and population through the public SQL interface after its caller commits.
		db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		var name, formatted string
		require.NoError(t, db.QueryRowContext(ctx, "select column_name from information_schema.columns where table_schema=? and table_name='new_table'", database).Scan(&name))
		require.Equal(t, "date_format(created, '%W %M %Y')", name)
		require.NoError(t, db.QueryRowContext(ctx, "select * from "+database+".new_table").Scan(&formatted))
		require.Equal(t, "Saturday November 2023", formatted)
		prepared, err := db.PrepareContext(ctx, "create table "+database+".prepared_ctas as select date_format(created, '%W %M %Y'), ? as marker from "+database+".existing")
		require.NoError(t, err)
		defer prepared.Close()
		for _, value := range []int{2, 7} {
			_, err = prepared.ExecContext(ctx, value)
			require.NoError(t, err)
			var marker int
			require.NoError(t, db.QueryRowContext(ctx, "select * from "+database+".prepared_ctas").Scan(&formatted, &marker))
			require.Equal(t, "Saturday November 2023", formatted)
			require.Equal(t, value, marker)
			require.NoError(t, db.QueryRowContext(ctx, "select column_name from information_schema.columns where table_schema=? and table_name='prepared_ctas' and ordinal_position=1", database).Scan(&name))
			require.Equal(t, "date_format(created, '%W %M %Y')", name)
			_, err = db.ExecContext(ctx, "drop table "+database+".prepared_ctas")
			require.NoError(t, err)
		}

		// MERGE creates several staging tables through frontend shared
		// background execution. Its children must preserve earlier caller work.
		for _, query := range []string{
			"create table " + database + ".branch_base(id int primary key, value int unique)",
			"insert into " + database + ".branch_base values(1,10),(2,20)",
			"data branch create table " + database + ".branch_src from " + database + ".branch_base",
			"data branch create table " + database + ".branch_dst from " + database + ".branch_base",
			"delete from " + database + ".branch_src where id=1",
			"update " + database + ".branch_src set value=200 where id=2",
			"insert into " + database + ".branch_src values(3,30)",
		} {
			_, err = db.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		// A unique-key conflict fails application after staging and deletion.
		// With statement rollback configured, retain the preceding statement.
		_, err = db.ExecContext(ctx, "insert into "+database+".branch_dst values(4,30)")
		require.NoError(t, err)
		failedTxn, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer failedTxn.Rollback()
		_, err = failedTxn.ExecContext(ctx, "set mo_rollback_txn_on_error=0")
		require.NoError(t, err)
		_, err = failedTxn.ExecContext(ctx, "insert into "+database+".existing values(9,'2023-11-12 10:00:01')")
		require.NoError(t, err)
		_, err = failedTxn.ExecContext(ctx, "data branch merge "+database+".branch_src into "+database+".branch_dst when conflict accept")
		require.ErrorContains(t, err, "Duplicate entry")
		var retained, unchanged, residue int
		require.NoError(t, failedTxn.QueryRowContext(ctx, "select count(*) from "+database+".existing where id=9").Scan(&retained))
		require.Equal(t, 1, retained)
		require.NoError(t, failedTxn.QueryRowContext(ctx, "select sum(value) from "+database+".branch_dst").Scan(&unchanged))
		require.Equal(t, 60, unchanged)
		require.NoError(t, failedTxn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase=? and relname like '__mo_diff_%'", database).Scan(&residue))
		require.Zero(t, residue)
		require.NoError(t, failedTxn.Commit())
		var afterCommit int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+database+".existing where id=9").Scan(&afterCommit))
		require.Equal(t, 1, afterCommit)
		_, err = db.ExecContext(ctx, "delete from "+database+".branch_dst where id=4")
		require.NoError(t, err)

		for _, commit := range []bool{false, true} {
			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			_, err = tx.ExecContext(ctx, "insert into "+database+".existing values(8,'2023-11-12 10:00:01')")
			require.NoError(t, err)
			_, err = tx.ExecContext(ctx, "data branch merge "+database+".branch_src into "+database+".branch_dst when conflict accept")
			require.NoError(t, err)
			var prior, merged int
			require.NoError(t, tx.QueryRowContext(ctx, "select count(*) from "+database+".existing where id=8").Scan(&prior))
			require.Equal(t, 1, prior)
			require.NoError(t, tx.QueryRowContext(ctx, "select sum(value) from "+database+".branch_dst").Scan(&merged))
			require.Equal(t, 230, merged)
			if commit {
				require.NoError(t, tx.Commit())
			} else {
				require.NoError(t, tx.Rollback())
			}
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+database+".existing where id=8").Scan(&prior))
			require.NoError(t, db.QueryRowContext(ctx, "select sum(value) from "+database+".branch_dst").Scan(&merged))
			if commit {
				require.Equal(t, 1, prior)
				require.Equal(t, 230, merged)
			} else {
				require.Zero(t, prior)
				require.Equal(t, 30, merged)
			}
			var stages int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase=? and relname like '__mo_diff_%'", database).Scan(&stages))
			require.Zero(t, stages)
		}

	})
}
