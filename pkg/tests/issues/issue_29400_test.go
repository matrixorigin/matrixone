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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

func TestIssue29400CopyAlterRetainedGatePromotionFastFails(t *testing.T) {
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
		const target, witness, solo = "issue_29400_promotion", "issue_29400_promotion_witness", "issue_29400_promotion_solo"
		defer func() {
			cleanupCtx, done := context.WithTimeout(context.Background(), 20*time.Second)
			defer done()
			_, _ = db0.ExecContext(cleanupCtx, "drop database if exists "+target)
			_, _ = db0.ExecContext(cleanupCtx, "drop database if exists "+witness)
			_, _ = db0.ExecContext(cleanupCtx, "drop database if exists "+solo)
		}()
		for _, query := range []string{
			"drop database if exists " + target,
			"drop database if exists " + witness,
			"create database " + target,
			"create database " + witness,
			"create table " + target + ".t (i int)",
			"create table " + target + ".u (i int)",
			"create table " + witness + ".marker (i int)",
		} {
			execSQLRequire(t, ctx, db0, query)
		}
		tx, err := db0.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		_, err = tx.ExecContext(ctx, "insert into "+witness+".marker values (1)")
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, "drop table "+target+".t")
		require.NoError(t, err)

		// B has G shared and waits for A's retained D before A attempts G X.
		waiter := make(chan struct{}, 1)
		restore := lockservice.SetWaiterEnqueuedHookForTest(func(tableID uint64, _ []byte, _ [][]byte) {
			if tableID == catalog.MO_DATABASE_ID {
				select {
				case waiter <- struct{}{}:
				default:
				}
			}
		})
		defer restore()
		dropCtx, dropCancel := context.WithTimeout(ctx, 30*time.Second)
		defer dropCancel()
		dropDone := make(chan error, 1)
		dropFinished := false
		go func() {
			_, err := db1.ExecContext(dropCtx, "drop database "+target)
			dropDone <- err
		}()
		defer func() {
			_ = tx.Rollback()
			dropCancel()
			if !dropFinished {
				select {
				case <-dropDone:
				case <-time.After(5 * time.Second):
				}
			}
		}()
		select {
		case <-waiter:
		case <-time.After(15 * time.Second):
			t.Fatal("DROP DATABASE did not wait for A's database lock")
		}
		alterCtx, alterCancel := context.WithTimeout(ctx, 10*time.Second)
		defer alterCancel()
		_, err = tx.ExecContext(alterCtx, "alter table "+target+".u modify column i bigint")
		var mysqlErr *mysql.MySQLError
		require.ErrorAs(t, err, &mysqlErr)
		require.Equal(t, moerr.ErrLockConflict, mysqlErr.Number,
			"retained G promotion must fail without a deadlock-detector wait")
		require.NoError(t, <-dropDone, "B should finish when A's whole transaction rolls back")
		dropFinished = true
		var rows int
		require.NoError(t, db0.QueryRowContext(ctx, "select count(*) from "+witness+".marker").Scan(&rows))
		require.Zero(t, rows, "the earlier insert must not commit after the G conflict")
		_ = tx.Commit()
		require.NoError(t, db0.QueryRowContext(ctx, "select count(*) from "+witness+".marker").Scan(&rows))
		require.Zero(t, rows)

		for _, query := range []string{
			"create database " + solo,
			"create table " + solo + ".t (i int)",
			"create table " + solo + ".u (i int)",
			"create table " + solo + ".v (i int)",
			"insert into " + solo + ".v values (1)",
		} {
			execSQLRequire(t, ctx, db0, query)
		}
		sole, err := db0.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer sole.Rollback()
		_, err = sole.ExecContext(ctx, "drop table "+solo+".t")
		require.NoError(t, err)
		_, err = sole.ExecContext(ctx, "alter table "+solo+".u modify column i bigint")
		require.NoError(t, err, "sole G shared holder should promote for COPY ALTER")
		require.NoError(t, sole.Commit())
		_, err = db0.ExecContext(ctx, "truncate table "+solo+".v")
		require.NoError(t, err, "TRUNCATE should enter the same broad RC gate")
		require.NoError(t, db0.QueryRowContext(ctx, "select count(*) from "+solo+".v").Scan(&rows))
		require.Zero(t, rows)

		// Public TRUNCATE starts a fresh transaction, but still must fail
		// promptly if another transaction holds the shared lifecycle gate.
		execSQLRequire(t, ctx, db0, "create table "+solo+".w (i int)")
		holder, err := db0.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer holder.Rollback()
		_, err = holder.ExecContext(ctx, "drop table "+solo+".w")
		require.NoError(t, err)
		truncateCtx, truncateCancel := context.WithTimeout(ctx, 10*time.Second)
		defer truncateCancel()
		_, err = db1.ExecContext(truncateCtx, "truncate table "+solo+".v")
		require.ErrorAs(t, err, &mysqlErr)
		require.Equal(t, moerr.ErrLockConflict, mysqlErr.Number)
		require.NoError(t, holder.Commit())
		_, err = db1.ExecContext(ctx, "truncate table "+solo+".v")
		require.NoError(t, err, "TRUNCATE should succeed after the competing gate owner commits")

		// A DML statement can retain T without having crossed G. Either G mode
		// would form a wait cycle if A's next lifecycle statement queued behind
		// B while B was waiting for A's T.
		for _, scenario := range []struct {
			name, otherSQL, nextSQL string
		}{
			{"shared_gate", "drop database %s", "alter table %s.t modify column v bigint"},
			{"exclusive_gate", "alter table %s.t modify column v bigint", "drop table %s.u"},
		} {
			t.Run(scenario.name, func(t *testing.T) {
				database := "issue_29400_prior_dml_" + scenario.name
				unrelated := database + "_other"
				for _, query := range []string{
					"create database " + database,
					"create database " + unrelated,
					"create table " + database + ".t (id int primary key, v int)",
					"create table " + database + ".u (i int)",
					"create table " + unrelated + ".u (i int)",
					"insert into " + database + ".t values (1, 1)",
				} {
					execSQLRequire(t, ctx, db0, query)
				}
				defer func() {
					cleanupCtx, done := context.WithTimeout(context.Background(), 20*time.Second)
					defer done()
					_, _ = db0.ExecContext(cleanupCtx, "drop database if exists "+database)
					_, _ = db0.ExecContext(cleanupCtx, "drop database if exists "+unrelated)
				}()
				owner, err := db0.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer owner.Rollback()
				_, err = owner.ExecContext(ctx, "update "+database+".t set v=2 where id=1")
				require.NoError(t, err)

				waitingForT := make(chan struct{}, 1)
				restoreHook := lockservice.SetWaiterEnqueuedHookForTest(func(tableID uint64, _ []byte, _ [][]byte) {
					if tableID == catalog.MO_TABLES_ID {
						select {
						case waitingForT <- struct{}{}:
						default:
						}
					}
				})
				defer restoreHook()
				otherCtx, cancelOther := context.WithTimeout(ctx, 30*time.Second)
				defer cancelOther()
				otherDone := make(chan error, 1)
				go func() {
					_, otherErr := db1.ExecContext(otherCtx, fmt.Sprintf(scenario.otherSQL, database))
					otherDone <- otherErr
				}()
				otherFinished := false
				defer func() {
					_ = owner.Rollback()
					cancelOther()
					if !otherFinished {
						select {
						case <-otherDone:
						case <-time.After(5 * time.Second):
						}
					}
				}()
				select {
				case <-waitingForT:
				case <-time.After(15 * time.Second):
					t.Fatal("competing lifecycle owner did not wait for the prior UPDATE lock")
				}
				select {
				case earlyErr := <-otherDone:
					t.Fatalf("competing lifecycle owner finished before the reciprocal wait: %v", earlyErr)
				default:
				}
				if scenario.name == "shared_gate" {
					fastCtx, cancelFast := context.WithTimeout(ctx, 2*time.Second)
					defer cancelFast()
					_, err = db0.ExecContext(fastCtx, "drop table "+unrelated+".u")
					require.NoError(t, err, "unrelated DROP must progress while target DROP waits for T")
				}
				nextCtx, cancelNext := context.WithTimeout(ctx, 10*time.Second)
				defer cancelNext()
				_, ownerErr := owner.ExecContext(nextCtx, fmt.Sprintf(scenario.nextSQL, database))
				otherErr := <-otherDone
				otherFinished = true
				if ownerErr != nil {
					var deadlock *mysql.MySQLError
					require.ErrorAs(t, ownerErr, &deadlock)
					require.Equal(t, moerr.ErrDeadLockDetected, deadlock.Number)
					require.NoError(t, otherErr, "the competing owner must finish after victim rollback")
				} else {
					var deadlock *mysql.MySQLError
					require.ErrorAs(t, otherErr, &deadlock)
					require.Equal(t, moerr.ErrDeadLockDetected, deadlock.Number)
					require.NoError(t, owner.Commit(), "the surviving owner must commit")
					var value int
					require.NoError(t, db0.QueryRowContext(ctx,
						"select v from "+database+".t where id=1").Scan(&value))
					require.Equal(t, 2, value, "the surviving UPDATE must commit")
				}
				if scenario.name == "exclusive_gate" && ownerErr != nil {
					var value int
					require.NoError(t, db0.QueryRowContext(ctx,
						"select v from "+database+".t where id=1").Scan(&value))
					require.Equal(t, 1, value, "the earlier UPDATE must roll back")
					require.NoError(t, db0.QueryRowContext(ctx,
						"select count(*) from mo_catalog.mo_tables where reldatabase=? and relname='u'", database).Scan(&value))
					require.Equal(t, 1, value, "the rejected DROP must leave its target intact")
				}
			})
		}
	})
}

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

func TestIssue29400DropTableRCReclaimFailureKeepsTemporaryOrder(t *testing.T) {
	faultEnabledHere := fault.Enable()
	if faultEnabledHere {
		defer fault.Disable()
	}
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const injection = "drop_table_rc_branch_reclaim_mutation_fail"
		for _, tc := range []struct {
			name, target string
			tempFirst    bool
		}{
			{name: "persistent_first_mark_failure", target: "update mo_catalog.mo_branch_metadata"},
			{name: "temporary_first_snapshot_failure", target: "delete from mo_catalog.mo_snapshots", tempFirst: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				name := "issue_29400_reclaim_" + tc.name
				defer func() {
					cleanupCtx, done := context.WithTimeout(context.Background(), 20*time.Second)
					defer done()
					_, _ = conn.ExecContext(cleanupCtx, "drop database if exists "+name)
				}()
				for _, query := range []string{
					"drop database if exists " + name,
					"create database " + name,
					"create table " + name + ".src (id int primary key)",
					"data branch create table " + name + ".child from " + name + ".src",
					"create table " + name + ".later (id int)",
					"create table " + name + ".guard (id int)",
					"create temporary table " + name + ".tmp (v int)",
					"insert into " + name + ".tmp values (7)",
					"set mo_rollback_txn_on_error=0",
					"begin",
					"insert into " + name + ".guard values (1)",
				} {
					_, err = conn.ExecContext(ctx, query)
					require.NoError(t, err, query)
				}
				defer func() { _, _ = conn.ExecContext(context.Background(), "rollback") }()
				var childID uint64
				require.NoError(t, conn.QueryRowContext(ctx,
					"select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='child'", name).Scan(&childID))
				require.NoError(t, fault.AddFaultPoint(ctx, injection, ":::", "echo", 0, tc.target, false))
				defer func() { _, _ = fault.RemoveFaultPoint(context.Background(), injection) }()
				members := name + ".child," + name + ".tmp," + name + ".later"
				if tc.tempFirst {
					members = name + ".tmp," + name + ".child," + name + ".later"
				}
				_, dropErr := conn.ExecContext(ctx, "drop table "+members)
				require.ErrorContains(t, dropErr, "injected RC branch reclaim mutation failure")
				_, err = fault.RemoveFaultPoint(ctx, injection)
				require.NoError(t, err)
				_, err = conn.ExecContext(ctx, "commit")
				require.NoError(t, err)
				var count int
				for _, check := range []struct {
					query string
					want  int
				}{
					{"select count(*) from " + name + ".guard", 1},
					{"select count(*) from mo_catalog.mo_tables where reldatabase='" + name + "' and relname in ('child','later')", 2},
					{fmt.Sprintf("select count(*) from mo_catalog.mo_branch_metadata where table_id=%d and table_deleted=false", childID), 1},
					{fmt.Sprintf("select count(*) from mo_catalog.mo_snapshots where sname='__mo_branch_%d'", childID), 1},
				} {
					require.NoError(t, conn.QueryRowContext(ctx, check.query).Scan(&count), check.query)
					require.Equal(t, check.want, count, check.query)
				}
				if tc.tempFirst {
					require.Error(t, conn.QueryRowContext(ctx, "select v from "+name+".tmp").Scan(&count))
				} else {
					require.NoError(t, conn.QueryRowContext(ctx, "select v from "+name+".tmp").Scan(&count))
					require.Equal(t, 7, count)
					_, err = conn.ExecContext(ctx, "drop temporary table "+name+".tmp")
					require.NoError(t, err)
				}
				_, err = conn.ExecContext(ctx, "create temporary table "+name+".tmp (v int)")
				require.NoError(t, err, "a later temporary generation must not inherit stale retirement")
				_, err = conn.ExecContext(ctx, "insert into "+name+".tmp values (9)")
				require.NoError(t, err)
				require.NoError(t, conn.QueryRowContext(ctx, "select v from "+name+".tmp").Scan(&count))
				require.Equal(t, 9, count)
			})
		}
	})
}

func TestIssue29400DropDatabaseRCReclaimFailureRollsBackWholeDatabase(t *testing.T) {
	faultEnabledHere := fault.Enable()
	if faultEnabledHere {
		defer fault.Disable()
	}
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const name = "issue_29400_db_reclaim_rollback"
		defer func() {
			cleanupCtx, done := context.WithTimeout(context.Background(), 20*time.Second)
			defer done()
			_, _ = conn.ExecContext(cleanupCtx, "rollback")
			_, _ = conn.ExecContext(cleanupCtx, "drop database if exists "+name)
		}()
		for _, query := range []string{
			"drop database if exists " + name,
			"create database " + name,
			"create table " + name + ".src (id int primary key)",
			"data branch create table " + name + ".child from " + name + ".src",
			"set experimental_fulltext2_index=1",
			"create table " + name + ".later (id bigint auto_increment primary key, body text, FULLTEXT2 ft_tail(body))",
			"insert into " + name + ".later values (null, 'beforemarker')",
			"create table " + name + ".guard (id int)",
			"create temporary table " + name + ".tmp (v int)",
			"insert into " + name + ".tmp values (7)",
		} {
			_, err = conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		var laterID, hiddenID, jobID uint64
		require.NoError(t, conn.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='later'", name).Scan(&laterID))
		var hiddenName string
		require.NoError(t, conn.QueryRowContext(ctx,
			"select index_table_name from mo_catalog.mo_indexes where table_id=? and name='ft_tail' and algo='fulltext2' and algo_table_type='ftv2_index'", laterID).Scan(&hiddenName))
		require.NoError(t, conn.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase=? and relname=?", name, hiddenName).Scan(&hiddenID))
		var jobCount int
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_iscp_log where table_id=? and job_name='index_ft_tail' and drop_at is null", laterID).Scan(&jobCount))
		require.Equal(t, 1, jobCount)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select job_id from mo_catalog.mo_iscp_log where table_id=? and job_name='index_ft_tail' and drop_at is null", laterID).Scan(&jobID))
		tailQuery := fmt.Sprintf("select coalesce(max(chunk_id), -1) from `%s`.`%s` where index_id='cdc_tail' and tag=1", name, hiddenName)
		var tailBefore int64 = -1
		require.Eventually(t, func() bool {
			require.NoError(t, conn.QueryRowContext(ctx, tailQuery).Scan(&tailBefore))
			return tailBefore >= 0
		}, 120*time.Second, time.Second, "initial ISCP consumer did not advance")
		for _, query := range []string{
			"set mo_rollback_txn_on_error=0",
			"begin",
			"insert into " + name + ".guard values (1)",
		} {
			_, err = conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		var childID uint64
		require.NoError(t, conn.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='child'", name).Scan(&childID))
		const injection = "drop_table_rc_branch_reclaim_mutation_fail"
		require.NoError(t, fault.AddFaultPoint(ctx, injection, ":::", "echo", 0,
			"update mo_catalog.mo_branch_metadata", false))
		defer func() { _, _ = fault.RemoveFaultPoint(context.Background(), injection) }()
		_, dropErr := conn.ExecContext(ctx, "drop database "+name)
		require.ErrorContains(t, dropErr, "injected RC branch reclaim mutation failure")
		_, err = fault.RemoveFaultPoint(ctx, injection)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "commit")
		require.NoError(t, err)

		var count int
		for _, check := range []struct {
			query string
			want  int
		}{
			{"select count(*) from " + name + ".guard", 1},
			{"select count(*) from mo_catalog.mo_tables where reldatabase='" + name + "' and relname in ('src','child','later','guard')", 4},
			{fmt.Sprintf("select count(*) from mo_catalog.mo_branch_metadata where table_id=%d and table_deleted=false", childID), 1},
			{fmt.Sprintf("select count(*) from mo_catalog.mo_snapshots where sname='__mo_branch_%d'", childID), 1},
			{"select v from " + name + ".tmp", 7},
		} {
			require.NoError(t, conn.QueryRowContext(ctx, check.query).Scan(&count), check.query)
			require.Equal(t, check.want, count, check.query)
		}
		var survivingID uint64
		require.NoError(t, conn.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='later'", name).Scan(&survivingID))
		require.Equal(t, laterID, survivingID)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase=? and relname=?", name, hiddenName).Scan(&survivingID))
		require.Equal(t, hiddenID, survivingID)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_iscp_log where table_id=? and job_name='index_ft_tail' and drop_at is null", laterID).Scan(&jobCount))
		require.Equal(t, 1, jobCount)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select job_id from mo_catalog.mo_iscp_log where table_id=? and job_name='index_ft_tail' and drop_at is null", laterID).Scan(&survivingID))
		require.Equal(t, jobID, survivingID)
		var tailRestored int64
		require.NoError(t, conn.QueryRowContext(ctx, tailQuery).Scan(&tailRestored))
		require.GreaterOrEqual(t, tailRestored, tailBefore, "rolled-back DROP lost preexisting CDC tail")
		_, err = conn.ExecContext(ctx, "insert into "+name+".later values (null, 'aftermarker')")
		require.NoError(t, err, "allocator must remain usable after rolled-back database removal")
		require.NoError(t, conn.QueryRowContext(ctx, "select max(id) from "+name+".later").Scan(&count))
		require.Greater(t, count, 1)
		var tailAfter int64 = -1
		require.Eventually(t, func() bool {
			require.NoError(t, conn.QueryRowContext(ctx, tailQuery).Scan(&tailAfter))
			return tailAfter > tailRestored
		}, 120*time.Second, time.Second, "original ISCP consumer did not advance after rollback: restored=%d after=%d", tailRestored, tailAfter)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select job_id from mo_catalog.mo_iscp_log where table_id=? and job_name='index_ft_tail' and drop_at is null", laterID).Scan(&survivingID))
		require.Equal(t, jobID, survivingID)
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
