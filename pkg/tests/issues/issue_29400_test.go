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

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

func TestIssue29400DropDatabaseDoesNotHoldBranchDAGAfterTable(t *testing.T) {
	faultEnabledHere := fault.Enable()
	if faultEnabledHere {
		defer fault.Disable()
	}
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn0, err := cluster.GetCNService(0)
		require.NoError(t, err)
		cn1, err := cluster.GetCNService(1)
		require.NoError(t, err)
		open := func(port int64) *sql.DB {
			db, err := sql.Open("mysql", issue27487DSN(port))
			require.NoError(t, err)
			return db
		}
		db0 := open(cn0.GetServiceConfig().CN.Frontend.Port)
		defer db0.Close()
		db1 := open(cn1.GetServiceConfig().CN.Frontend.Port)
		defer db1.Close()
		const a, b = "issue_29400_dag_a", "issue_29400_dag_b"
		defer func() {
			cleanupCtx, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			for _, name := range []string{a, b} {
				if _, err := db0.ExecContext(cleanupCtx, "drop database if exists "+name); err != nil {
					t.Errorf("cleanup %s: %v", name, err)
				}
			}
		}()
		for _, q := range []string{
			"drop database if exists " + a, "drop database if exists " + b,
			"create database " + a, "create database " + b,
			"create table " + a + ".a_root (id int primary key)",
			"insert into " + a + ".a_root values (1)",
			"data branch create table " + a + ".b_child from " + a + ".a_root",
			"create table " + b + ".a_root (id int primary key)",
			"insert into " + b + ".a_root values (1)",
			"data branch create table " + b + ".b_child from " + b + ".a_root",
		} {
			execSQLRequire(t, ctx, db0, q)
		}
		var childID uint64
		require.NoError(t, db0.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='b_child'", a).Scan(&childID))
		// Either table in A participates in the same branch component. Stop
		// after the first physical table, independent of relation scan order.
		const barrier = "drop_database_after_table"
		const probe = "issue29400_drop_database_waiters"
		tx, err := db0.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		require.NoError(t, fault.AddFaultPoint(ctx, barrier, "1:1::", "wait", 0, "", false))
		require.NoError(t, fault.AddFaultPoint(ctx, probe, ":::", "getwaiters", 0, barrier, false))
		release := func() {
			_, _ = fault.RemoveFaultPoint(context.Background(), barrier)
			_, _ = fault.RemoveFaultPoint(context.Background(), probe)
		}
		defer release()
		dropCtx, dropCancel := context.WithTimeout(ctx, 30*time.Second)
		defer dropCancel()
		dropA := make(chan error, 1)
		go func() {
			_, err := tx.ExecContext(dropCtx, "drop database "+a)
			dropA <- err
		}()
		// The old path held a whole-DAG lock at this exact point, blocking B
		// for as long as A's later work took.
		require.Eventually(t, func() bool {
			count, _, ok := fault.TriggerFault(probe)
			return ok && count == 1
		}, 10*time.Second, 20*time.Millisecond)
		select {
		case err := <-dropA:
			t.Fatalf("A finished before the post-table barrier was released: %v", err)
		default:
		}
		fastCtx, fastCancel := context.WithTimeout(ctx, 2*time.Second)
		defer fastCancel()
		_, err = db1.ExecContext(fastCtx, "drop table "+b+".b_child")
		require.NoError(t, err, "unrelated branch DROP waited for A's post-table work")
		release()
		require.NoError(t, <-dropA)
		// A now retains all statement locks until COMMIT. Its component must
		// not pin B's root during this explicit transaction tail.
		otherCtx, otherCancel := context.WithTimeout(ctx, 2*time.Second)
		defer otherCancel()
		_, err = db1.ExecContext(otherCtx, "drop table "+b+".a_root")
		require.NoError(t, err, "unrelated branch DROP waited for A's COMMIT")
		require.NoError(t, tx.Commit())
		var deleted bool
		require.NoError(t, db0.QueryRowContext(ctx,
			"select table_deleted from mo_catalog.mo_branch_metadata where table_id=?", childID).Scan(&deleted))
		require.True(t, deleted)
		var snapshots int
		require.NoError(t, db0.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_snapshots where sname=?",
			fmt.Sprintf("__mo_branch_%d", childID)).Scan(&snapshots))
		require.Zero(t, snapshots)
	})
}

func TestIssue29400BranchDeletePartitionChildrenStayInternal(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const source, clone = "issue_29400_partition_source", "issue_29400_partition_clone"
		defer func() {
			for _, name := range []string{clone, source} {
				_, _ = db.Exec("drop database if exists " + name)
			}
		}()
		for _, q := range []string{
			"drop database if exists " + clone,
			"drop database if exists " + source,
			"create database " + source,
			"create table " + source + ".src(id int primary key, v int) partition by hash(id) partitions 2",
			"insert into " + source + ".src values (1, 1)",
			"data branch create table " + source + ".child from " + source + ".src",
		} {
			execSQLRequire(t, ctx, db, q)
		}
		var childID uint64
		require.NoError(t, db.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='child'", source).Scan(&childID))
		execSQLRequire(t, ctx, db, "data branch delete table "+source+".child")
		var count int
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_tables where reldatabase=? and (relname='child' or relname like '%!%child')", source).Scan(&count))
		require.Zero(t, count)
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_snapshots where sname=?", fmt.Sprintf("__mo_branch_%d", childID)).Scan(&count))
		require.Zero(t, count)
		execSQLRequire(t, ctx, db, "data branch create database "+clone+" from "+source)
		execSQLRequire(t, ctx, db, "data branch delete database "+clone)
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_database where datname=?", clone).Scan(&count))
		require.Zero(t, count)
	})
}

func TestIssue29400BranchCloneFromForeignKeyBranch(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const name = "issue_29400_fk_branch"
		defer func() { _, _ = db.Exec("drop database if exists " + name) }()
		for _, query := range []string{
			"drop database if exists " + name,
			"create database " + name,
			"create table " + name + ".p (id int primary key)",
			"create table " + name + ".c (id int primary key, pid int, constraint fk_c_p foreign key(pid) references " + name + ".p(id))",
			"data branch create table " + name + ".c1 from " + name + ".c",
			"data branch create table " + name + ".c2 from " + name + ".c1",
		} {
			execSQLRequire(t, ctx, db, query)
		}
		var count int
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_foreign_keys where db_name=? and table_name='c2' and refer_table_name='p'", name).Scan(&count))
		require.Equal(t, 1, count)
	})
}
