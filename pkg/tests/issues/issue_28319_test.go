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
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

func TestIssue28319StatementRollbackRetainsLifecycleLock(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		sqlExec := testutils.GetSQLExecutor(cn)
		gateSQL := databranchutils.LineageOwnerLifecycleLockSQL()
		rollback := errors.New("roll back the complete lock owner")
		err = sqlExec.ExecTxn(ctx, func(owner executor.TxnExecutor) error {
			result, execErr := owner.Exec(gateSQL, executor.StatementOption{})
			result.Close()
			if execErr != nil {
				return execErr
			}
			if execErr = owner.Txn().GetWorkspace().RollbackLastStatement(ctx); execErr != nil {
				return execErr
			}
			result, contenderErr := sqlExec.Exec(ctx, gateSQL, executor.Options{}.
				WithAccountID(0).
				WithStatementOption(executor.StatementOption{}.WithWaitPolicy(lock.WaitPolicy_FastFail)))
			result.Close()
			var lockErr *moerr.Error
			if !errors.As(contenderErr, &lockErr) || !moerr.IsMoErrCode(lockErr, moerr.ErrLockConflict) {
				return fmt.Errorf("statement rollback must retain the gate lock, got %v", contenderErr)
			}
			return rollback
		}, executor.Options{}.WithAccountID(0))
		require.ErrorIs(t, err, rollback)
		result, err := sqlExec.Exec(ctx, gateSQL, executor.Options{}.
			WithAccountID(0).
			WithStatementOption(executor.StatementOption{}.WithWaitPolicy(lock.WaitPolicy_FastFail)))
		result.Close()
		require.NoError(t, err, "whole-transaction rollback must release the gate")
	})
}

// An ordinary RC statement retry before publication must preserve executor
// ownership and its admission decision even after a child CREATE set HaveDDL.
func TestIssue28319PreparationStatementRetry(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		sqlExec := testutils.GetSQLExecutor(cn)
		run := func(statement string) error {
			result, err := sqlExec.Exec(ctx, statement, executor.Options{}.WithAccountID(0))
			result.Close()
			return err
		}
		require.NoError(t, run("create database issue_28319_statement_retry"))
		defer func() {
			// DROP DATABASE requires frontend session metadata on this baseline.
			db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			defer db.Close()
			execSQLRequire(t, ctx, db, "drop database issue_28319_statement_retry")
		}()
		require.NoError(t, run("create table issue_28319_statement_retry.t(id int)"))
		require.NoError(t, run("insert into issue_28319_statement_retry.t values(1),(2)"))
		var copies, prepared atomic.Int32
		var firstTxn []byte
		restore := compile.SetAlterCopyPhaseHookForTest(func(callCtx context.Context, database, table, phase string, op client.TxnOperator) error {
			if database != "issue_28319_statement_retry" {
				return nil
			}
			if phase == "data-copied" && copies.Add(1) == 1 {
				firstTxn = append([]byte(nil), op.Txn().ID...)
				return moerr.NewTxnNeedRetry(callCtx)
			}
			if phase == "prepared" {
				prepared.Add(1)
				if string(firstTxn) != string(op.Txn().ID) {
					return errors.New("ordinary preparation retry changed the transaction owner")
				}
			}
			return nil
		})
		defer restore()
		require.NoError(t, run("alter table issue_28319_statement_retry.t add primary key(id)"))
		require.Equal(t, int32(2), copies.Load())
		require.Equal(t, int32(1), prepared.Load())
		require.NoError(t, run("insert into issue_28319_statement_retry.t values(3)"))
		result, err := sqlExec.Exec(ctx, "select sum(id) from issue_28319_statement_retry.t", executor.Options{}.WithAccountID(0))
		require.NoError(t, err)
		defer result.Close()
		result.ReadRows(func(_ int, cols []*vector.Vector) bool {
			require.Equal(t, int64(6), executor.GetFixedRows[int64](cols[0])[0])
			return false
		})
	})
}

func TestIssue28319CopyAlterCompatibility(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		const database = "issue_28319_compat"
		execSQLRequire(t, ctx, db, "create database "+database)
		defer execSQLRequire(t, ctx, db, "drop database "+database)
		var preparations atomic.Int32
		restoreHook := compile.SetAlterCopyPhaseHookForTest(func(_ context.Context, name, _, phase string, _ client.TxnOperator) error {
			if name == database && phase == "prepared" {
				preparations.Add(1)
			}
			return nil
		})
		defer restoreHook()
		for _, tc := range []struct {
			name      string
			mode      pbtxn.TxnMode
			isolation pbtxn.TxnIsolation
		}{
			{"pessimistic_si", pbtxn.TxnMode_Pessimistic, pbtxn.TxnIsolation_SI},
			{"optimistic_si", pbtxn.TxnMode_Optimistic, pbtxn.TxnIsolation_SI},
		} {
			t.Run(tc.name, func(t *testing.T) {
				restoreMode := setIssue27718TxnConfig([]embed.ServiceOperator{cn}, tc.mode, tc.isolation)
				defer restoreMode()
				// The already-open SQL session owns its isolation override; changing
				// the service default alone does not change that session's transaction.
				execSQLRequire(t, ctx, db, "set session transaction_isolation='REPEATABLE-READ'")
				defer execSQLRequire(t, ctx, db, "set session transaction_isolation='READ-COMMITTED'")
				table := database + "." + tc.name
				execSQLRequire(t, ctx, db, "create table "+table+"(id int)")
				execSQLRequire(t, ctx, db, "insert into "+table+" values(1),(2)")
				execSQLRequire(t, ctx, db, "alter table "+table+" add primary key(id)")
				var sum int
				require.NoError(t, db.QueryRowContext(ctx, "select sum(id) from "+table).Scan(&sum))
				require.Equal(t, 3, sum)
			})
		}
		table := database + ".caller_owned"
		execSQLRequire(t, ctx, db, "create table "+table+"(id int)")
		execSQLRequire(t, ctx, db, "insert into "+table+" values(1),(2)")
		rollback := errors.New("caller owns rollback")
		err = testutils.GetSQLExecutor(cn).ExecTxn(ctx, func(tx executor.TxnExecutor) error {
			for _, statement := range []string{"insert into " + table + " values(3)", "alter table " + table + " add primary key(id)"} {
				result, execErr := tx.Exec(statement, executor.StatementOption{})
				result.Close()
				if execErr != nil {
					return execErr
				}
			}
			return rollback
		}, executor.Options{}.WithAccountID(0))
		require.ErrorIs(t, err, rollback)
		var sum int
		require.NoError(t, db.QueryRowContext(ctx, "select sum(id) from "+table).Scan(&sum))
		require.Equal(t, 3, sum)
		execSQLRequire(t, ctx, db, "insert into "+table+" values(1)")
		require.Zero(t, preparations.Load(), "fallback entries must retain the legacy protocol")
	})
}

func TestIssue28319SynchronousIndexPreparation(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		// Cluster startup precedes daemon-task scheduling. Observe the assigned
		// executor before testing DDL; do not spend the DDL drain deadline waiting
		// for this independent fixture initialization.
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			runner, readyErr := iscp.GetTaskRunner(ctx, cn.ServiceID(), nil)
			require.NoError(t, readyErr)
			if exec, ready := iscp.GetExecutorRuntime(runner); ready && exec != nil {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal("ISCP fixture did not become ready")
			case <-ticker.C:
			}
		}
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		sqlExec := testutils.GetSQLExecutor(cn)
		for _, tc := range []struct {
			name, column, values, createIndex, alter, storage string
			storageReady                                      bool
			initializations                                   int
		}{
			{"fulltext2", "body varchar(100)", "(1,'alpha kernel'),(2,'beta database')", "create fulltext2 index idx on %s.t(body)", "modify body text", "ftv2_index", true, 0},
			{"hnsw", "embedding vecf32(3)", "(1,'[1,2,3]'),(2,'[4,5,6]')", "create index idx using hnsw on %s.t(embedding)", "modify embedding vecf64(3)", "hnsw_index", true, 0},
			{"hnsw", "embedding vecf32(3)", "(1,'[1,2,3]'),(2,'[4,5,6]')", "create index idx using hnsw on %s.t(embedding) async", "modify embedding vecf64(3)", "hnsw_index", false, 0},
			{"ivf", "embedding vecf32(3)", "(1,'[1,2,3]'),(2,'[4,5,6]')", "create index idx using ivfflat on %s.t(embedding) lists=1 op_type 'vector_l2_ops' async", "modify embedding vecf64(3)", "centroids", true, 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				database := "issue_28319_" + tc.name
				execSQLRequire(t, ctx, db, "set experimental_"+tc.name+"_index=1")
				execSQLRequire(t, ctx, db, "create database "+database)
				defer func() {
					cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cleanupCancel()
					execSQLRequire(t, cleanupCtx, db, "drop database "+database)
				}()
				execSQLRequire(t, ctx, db, "create table "+database+".t(id bigint primary key,"+tc.column+")")
				execSQLRequire(t, ctx, db, "insert into "+database+".t values"+tc.values)
				execSQLRequire(t, ctx, db, fmt.Sprintf(tc.createIndex, database))
				originalID := currentRelationID(t, ctx, db, database, "t")
				var observed atomic.Int32
				restore := compile.SetAlterCopyPhaseHookForTest(func(callCtx context.Context, dbName, table, phase string, op client.TxnOperator) error {
					if dbName != database || table != "t" || phase != "prepared" {
						return nil
					}
					observed.Add(1)
					opts := executor.Options{}.WithAccountID(0).WithTxn(op).WithKeepTxnAlive().WithDisableIncrStatement()
					target := fmt.Sprintf("select rel_id from mo_catalog.mo_tables where reldatabase='%s' and relkind='r' and rel_id != %d", database, originalID)
					result, e := sqlExec.Exec(callCtx, "select distinct index_table_name from mo_catalog.mo_indexes where table_id in ("+target+") and algo_table_type='"+tc.storage+"'", opts)
					if e != nil {
						result.Close()
						return e
					}
					var storage []string
					result.ReadRows(func(n int, cols []*vector.Vector) bool {
						for i := 0; i < n; i++ {
							storage = append(storage, cols[0].GetStringAt(i))
						}
						return true
					})
					result.Close()
					if len(storage) != 1 {
						return fmt.Errorf("expected one prepared storage relation, got %d", len(storage))
					}
					for _, check := range []struct {
						sql      string
						nonempty bool
					}{
						{"select count(*) from " + sqlquote.QualifiedIdent(database, storage[0]), tc.storageReady},
						{"select count(*) from mo_catalog.mo_iscp_log where table_id in (" + target + ")", false},
					} {
						result, e = sqlExec.Exec(callCtx, check.sql, opts)
						if e != nil {
							result.Close()
							return e
						}
						var count int64
						result.ReadRows(func(_ int, cols []*vector.Vector) bool {
							count = executor.GetFixedRows[int64](cols[0])[0]
							return false
						})
						result.Close()
						if (count > 0) != check.nonempty {
							return fmt.Errorf("prepared index/task state mismatch: count %d, expected nonempty %v", count, check.nonempty)
						}
					}
					return nil
				})
				defer restore()
				execSQLRequire(t, ctx, db, "alter table "+database+".t "+tc.alter)
				require.Equal(t, int32(1), observed.Load())
				finalID := currentRelationID(t, ctx, db, database, "t")
				require.NotEqual(t, originalID, finalID)
				var tasks, initializations int
				require.NoError(t, db.QueryRowContext(ctx, "select count(*),sum(case when coalesce(json_unquote(json_extract(job_spec,'$.InitSQL')),'') != '' then 1 else 0 end) from mo_catalog.mo_iscp_log where table_id=? and job_name='index_idx'", finalID).Scan(&tasks, &initializations))
				require.Equal(t, 1, tasks)
				require.Equal(t, tc.initializations, initializations, "preserve synchronous versus asynchronous initialization")
			})
		}
	})
}

func TestIssue28319CopyAlterIndependentTables(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		peerCN, err := cluster.GetCNService(1)
		require.NoError(t, err)
		peerDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", peerCN.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer peerDB.Close()
		db.SetMaxOpenConns(4)
		const database = "issue_28319_independent"
		execSQLRequire(t, ctx, db, "create database "+database)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cleanupCancel()
			_, cleanupErr := db.ExecContext(cleanupCtx, "drop database "+database)
			require.NoError(t, cleanupErr)
		}()
		for _, table := range []string{"a", "b"} {
			execSQLRequire(t, ctx, db, "create table "+database+"."+table+"(id int not null, v int)")
			execSQLRequire(t, ctx, db, "insert into "+database+"."+table+" values(1,10),(2,20),(3,30)")
		}
		execSQLRequire(t, ctx, db, "create view "+database+".a_view as select id, v from "+database+".a")
		const existingSnapshot = "issue_28319_shared_owner"
		execSQLRequire(t, ctx, db, "create snapshot "+existingSnapshot+" for database "+database)
		defer execSQLRequire(t, ctx, db, "drop snapshot "+existingSnapshot)

		copied := make(chan struct{})
		release := make(chan struct{})
		var entered, released sync.Once
		var aCopyCount atomic.Int32
		unblock := func() { released.Do(func() { close(release) }) }
		defer unblock()
		restore := compile.SetAlterCopyPhaseHookForTest(func(callCtx context.Context, dbName, table, phase string, op client.TxnOperator) error {
			if dbName != database || table != "a" || phase != "data-copied" {
				return nil
			}
			aCopyCount.Add(1)
			entered.Do(func() { close(copied) })
			select {
			case <-release:
				return nil
			case <-callCtx.Done():
				return context.Cause(callCtx)
			}
		})
		defer restore()
		done := make(chan error, 1)
		go func() {
			_, alterErr := db.ExecContext(ctx, "alter table "+database+".a add primary key(id)")
			done <- alterErr
		}()
		select {
		case <-copied:
		case alterErr := <-done:
			t.Fatalf("ALTER did not reach the copy boundary: %v", alterErr)
		case <-ctx.Done():
			unblock()
			<-done
			t.Fatal("ALTER did not reach the copy boundary")
		}
		otherCtx, otherCancel := context.WithTimeout(ctx, 15*time.Second)
		// Publish an owner while A is copying. A must read current ownership
		// after its catalog refresh and retain the old generation.
		const snapshot = "issue_28319_during_copy"
		_, snapshotErr := peerDB.ExecContext(otherCtx, "create snapshot "+snapshot+" for table "+database+" a")
		_, otherErr := peerDB.ExecContext(otherCtx, "alter table "+database+".b add primary key(id)")
		otherCancel()
		unblock()
		firstErr := <-done
		if snapshotErr == nil {
			defer execSQLRequire(t, ctx, db, "drop snapshot "+snapshot)
		}
		require.NoError(t, snapshotErr)
		require.NoError(t, otherErr, "independent ALTER must commit while its peer is paused before publication")
		require.NoError(t, firstErr)
		require.Equal(t, int32(1), aCopyCount.Load(), "publication gate refresh must not recopy the prepared relation")
		for _, table := range []string{"a", "b"} {
			var rows, total, primaryColumns int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*), sum(v) from "+database+"."+table).Scan(&rows, &total))
			require.Equal(t, 3, rows)
			require.Equal(t, 60, total)
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from information_schema.columns where table_schema=? and table_name=? and column_key='PRI'", database, table).Scan(&primaryColumns))
			require.Equal(t, 1, primaryColumns)
		}
		var viewRows, viewTotal int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*), sum(v) from "+database+".a_view").Scan(&viewRows, &viewTotal))
		require.Equal(t, 3, viewRows)
		require.Equal(t, 60, viewTotal)
		execSQLRequire(t, ctx, db, "update "+database+".a set v=100 where id=1")
		var historicalTotal int
		require.NoError(t, db.QueryRowContext(ctx, "select sum(v) from "+database+".a {snapshot='"+snapshot+"'}").Scan(&historicalTotal))
		require.Equal(t, 60, historicalTotal)
	})
}

func TestIssue28319CopyAlterSameTable(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const database = "issue_28319_same_table"
		execSQLRequire(t, ctx, db, "create database "+database)
		defer execSQLRequire(t, ctx, db, "drop database "+database)
		execSQLRequire(t, ctx, db, "create table "+database+".a(id int not null, v int)")
		execSQLRequire(t, ctx, db, "insert into "+database+".a values(1,10),(2,20)")
		services := issue27487LockServices(cluster)
		require.NotEmpty(t, services)
		copied := make(chan []byte, 1)
		release := make(chan struct{})
		var first, released sync.Once
		unblock := func() { released.Do(func() { close(release) }) }
		restore := compile.SetAlterCopyPhaseHookForTest(func(callCtx context.Context, dbName, table, phase string, op client.TxnOperator) error {
			if dbName != database || table != "a" || phase != "data-copied" {
				return nil
			}
			pause := false
			first.Do(func() { pause = true })
			if !pause {
				return nil
			}
			copied <- bytes.Clone(op.Txn().ID)
			select {
			case <-release:
				return nil
			case <-callCtx.Done():
				return context.Cause(callCtx)
			}
		})
		defer restore()
		var workers sync.WaitGroup
		defer func() { unblock(); workers.Wait() }()
		alter := func(statement string) <-chan error {
			done := make(chan error, 1)
			workers.Add(1)
			go func() {
				defer workers.Done()
				_, execErr := db.ExecContext(ctx, statement)
				done <- execErr
			}()
			return done
		}
		a := alter("alter table " + database + ".a add primary key(id)")
		var owner []byte
		select {
		case owner = <-copied:
		case err := <-a:
			t.Fatalf("first ALTER did not reach preparation: %v", err)
		case <-ctx.Done():
			t.Fatal("first ALTER did not reach preparation")
		}
		b := alter("alter table " + database + ".a modify column v bigint")
		// Observe a real waiter on a metadata lock held by A before releasing
		// A. Timing alone cannot prove same-table mutual exclusion.
		require.Eventually(t, func() bool {
			waiting := false
			for _, service := range services {
				service.IterLocks(func(tableID uint64, keys [][]byte, held lockservice.Lock) bool {
					if tableID != catalog.MO_TABLES_ID {
						return true
					}
					owns := false
					held.IterHolders(func(holder lock.WaitTxn) bool {
						owns = owns || bytes.Equal(holder.TxnID, owner)
						return !owns
					})
					if owns {
						held.IterWaiters(func(lock.WaitTxn) bool { waiting = true; return false })
					}
					return !waiting
				})
			}
			return waiting
		}, 15*time.Second, 10*time.Millisecond)
		unblock()
		require.NoError(t, <-a)
		require.NoError(t, <-b)
		var count, sum, primary int
		var columnType string
		require.NoError(t, db.QueryRowContext(ctx, "select count(*), sum(v) from "+database+".a").Scan(&count, &sum))
		require.Equal(t, 2, count)
		require.Equal(t, 30, sum)
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from information_schema.columns where table_schema=? and table_name='a' and column_key='PRI'", database).Scan(&primary))
		require.Equal(t, 1, primary, "the waiting ALTER must preserve its predecessor's new primary key")
		require.NoError(t, db.QueryRowContext(ctx, "select data_type from information_schema.columns where table_schema=? and table_name='a' and column_name='v'", database).Scan(&columnType))
		require.Equal(t, "bigint", columnType)
	})
}

func TestIssue28319ConcurrentPreparedPublicationDoesNotRecopy(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(4)
		const database = "issue_28319_concurrent_publish"
		execSQLRequire(t, ctx, db, "create database "+database)
		defer func() {
			cleanup, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cleanupCancel()
			execSQLRequire(t, cleanup, db, "drop database "+database)
		}()
		for _, table := range []string{"a", "b"} {
			execSQLRequire(t, ctx, db, "create table "+database+"."+table+"(id int not null, v int)")
			execSQLRequire(t, ctx, db, "insert into "+database+"."+table+" values(1,10),(2,20)")
		}

		var copies, prepared atomic.Int32
		preparedBoth := make(chan struct{})
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(preparedBoth) }) }
		restore := compile.SetAlterCopyPhaseHookForTest(func(callCtx context.Context, dbName, table, phase string, _ client.TxnOperator) error {
			if dbName != database || (table != "a" && table != "b") {
				return nil
			}
			switch phase {
			case "data-copied":
				copies.Add(1)
			case "prepared":
				if prepared.Add(1) == 2 {
					release()
				}
				select {
				case <-preparedBoth:
					return nil
				case <-callCtx.Done():
					return context.Cause(callCtx)
				}
			}
			return nil
		})
		defer restore()

		done := make(chan error, 2)
		for _, table := range []string{"a", "b"} {
			go func(table string) {
				_, execErr := db.ExecContext(ctx, "alter table "+database+"."+table+" add primary key(id)")
				done <- execErr
			}(table)
		}
		select {
		case <-preparedBoth:
		case <-ctx.Done():
			release()
			t.Fatal("concurrent COPY ALTER did not reach the shared prepared barrier")
		}
		require.NoError(t, <-done)
		require.NoError(t, <-done)
		require.Equal(t, int32(2), copies.Load(), "publication contention must reuse each prepared relation")
		require.Equal(t, int32(2), prepared.Load())
		for _, table := range []string{"a", "b"} {
			var primary int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from information_schema.columns where table_schema=? and table_name=? and column_key='PRI'", database, table).Scan(&primary))
			require.Equal(t, 1, primary)
		}
	})
}

func TestIssue28319PublicationGateWaitDoesNotRecopy(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const database = "issue_28319_exhaustion"
		execSQLRequire(t, ctx, db, "create database "+database)
		defer execSQLRequire(t, ctx, db, "drop database "+database)
		for _, mode := range []string{"frontend", "executor"} {
			t.Run(mode, func(t *testing.T) {
				table := database + "." + mode
				execSQLRequire(t, ctx, db, "create table "+table+"(id int)")
				execSQLRequire(t, ctx, db, "insert into "+table+" values(1),(2)")
				ready, releaseGate, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
				var once sync.Once
				release := func() { once.Do(func() { close(releaseGate) }) }
				defer func() {
					release()
					for e := range done {
						require.NoError(t, e)
					}
				}()
				go func() {
					defer close(done)
					done <- testutils.GetSQLExecutor(cn).ExecTxn(ctx, func(owner executor.TxnExecutor) error {
						r, e := owner.Exec(databranchutils.LineageOwnerLifecyclePessimisticLockSQL(), executor.StatementOption{})
						r.Close()
						if e != nil {
							return e
						}
						close(ready)
						select {
						case <-releaseGate:
							return nil
						case <-ctx.Done():
							return ctx.Err()
						}
					}, executor.Options{}.WithAccountID(0))
				}()
				select {
				case <-ready:
				case e := <-done:
					t.Fatalf("gate owner failed: %v", e)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				var copies atomic.Int32
				restore := compile.SetAlterCopyPhaseHookForTest(func(_ context.Context, name, relation, phase string, _ client.TxnOperator) error {
					if name == database && relation == mode && phase == "data-copied" {
						copies.Add(1)
					}
					if name == database && relation == mode && phase == "prepared" {
						release()
					}
					return nil
				})
				defer restore()
				statement := "alter table " + table + " add primary key(id)"
				if mode == "frontend" {
					_, err = db.ExecContext(ctx, statement)
					require.NoError(t, err)
				} else {
					r, e := testutils.GetSQLExecutor(cn).Exec(ctx, statement, executor.Options{}.WithAccountID(0))
					r.Close()
					require.NoError(t, e)
				}
				require.Equal(t, int32(1), copies.Load(), "waiting for a publication owner must reuse the prepared relation")
				var total int
				require.NoError(t, db.QueryRowContext(ctx, "select sum(id) from "+table).Scan(&total))
				require.Equal(t, 3, total)
				release()
				require.NoError(t, <-done)
				execSQLRequire(t, ctx, db, "alter table "+table+" drop primary key")
				execSQLRequire(t, ctx, db, statement)
			})
		}
	})
}

// A SNAPSHOT owner is released after preparation. Every entry point must wait
// for publication and reuse its prepared relation rather than restart COPY.
func TestIssue28319CopyAlterPublicationWaitReusesPrepared(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		const database = "issue_28319_retry"
		execSQLRequire(t, ctx, db, "create database "+database)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cleanupCancel()
			execSQLRequire(t, cleanupCtx, db, "drop database "+database)
		}()
		execSQLRequire(t, ctx, db, "use "+database)
		for _, mode := range []string{"query", "binary", "text", "executor"} {
			t.Run(mode, func(t *testing.T) {
				execSQLRequire(t, ctx, db, "create table "+database+".t(id int not null, v int)")
				execSQLRequire(t, ctx, db, "insert into "+database+".t values(1,10),(2,20)")
				statement := "alter table t add primary key(id)"
				run := func() error { _, e := db.ExecContext(ctx, statement); return e }
				if mode == "binary" {
					stmt, e := db.PrepareContext(ctx, statement)
					require.NoError(t, e)
					defer stmt.Close()
					run = func() error { _, e := stmt.ExecContext(ctx); return e }
				}
				if mode == "text" {
					execSQLRequire(t, ctx, db, "prepare alter_copy_stmt from '"+statement+"'")
					defer execSQLRequire(t, ctx, db, "deallocate prepare alter_copy_stmt")
					run = func() error { _, e := db.ExecContext(ctx, "execute alter_copy_stmt"); return e }
				}
				if mode == "executor" {
					run = func() error {
						result, e := testutils.GetSQLExecutor(cn).Exec(ctx, statement, executor.Options{}.WithAccountID(0).WithDatabase(database))
						result.Close()
						return e
					}
				}
				gateReady, releaseGate, gateDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(releaseGate) }) }
				defer release()
				go func() {
					gateDone <- testutils.GetSQLExecutor(cn).ExecTxn(ctx, func(owner executor.TxnExecutor) error {
						res, e := owner.Exec(databranchutils.LineageOwnerLifecyclePessimisticLockSQL(), executor.StatementOption{})
						res.Close()
						if e != nil {
							return e
						}
						close(gateReady)
						select {
						case <-releaseGate:
							return nil
						case <-ctx.Done():
							return context.Cause(ctx)
						}
					}, executor.Options{}.WithAccountID(0))
				}()
				select {
				case <-gateReady:
				case e := <-gateDone:
					t.Fatalf("gate owner: %v", e)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				var copyCount atomic.Int32
				restore := compile.SetAlterCopyPhaseHookForTest(func(callCtx context.Context, dbName, table, phase string, op client.TxnOperator) error {
					if dbName != database || table != "t" {
						return nil
					}
					if phase == "data-copied" {
						copyCount.Add(1)
					}
					if phase == "prepared" {
						release()
					}
					return nil
				})
				defer restore()
				err = run()
				release()
				require.NoError(t, err)
				require.Equal(t, int32(1), copyCount.Load(), "publication wait must not recopy")
				var rows, total, tables int
				require.NoError(t, db.QueryRowContext(ctx, "select count(*),sum(v) from "+database+".t").Scan(&rows, &total))
				require.Equal(t, 2, rows)
				require.Equal(t, 30, total)
				require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase=?", database).Scan(&tables))
				require.Equal(t, 1, tables, "no abandoned preparation relation")
				execSQLRequire(t, ctx, db, "alter table "+database+".t drop primary key")
				require.NoError(t, run(), "reuse the same SQL/prepared object after transaction retry")
				var currentDB string
				require.NoError(t, db.QueryRowContext(ctx, "select database()").Scan(&currentDB))
				require.Equal(t, database, currentDB)
				execSQLRequire(t, ctx, db, "drop table "+database+".t")
			})
		}
	})
}

// A binary prepared ALTER must rebuild through the prepared initializer after
// a publication conflict.  In particular, the client may switch databases
// between PREPARE and EXECUTE; the retry must keep the database captured by
// PREPARE while using a fresh transaction generation.
func TestIssue28319BinaryPreparedPublicationRetryRestoresDatabase(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		const (
			sourceDB = "issue_28319_prepared_source"
			otherDB  = "issue_28319_prepared_other"
		)
		execSQLRequire(t, ctx, db, "create database "+sourceDB)
		execSQLRequire(t, ctx, db, "create database "+otherDB)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cleanupCancel()
			execSQLRequire(t, cleanupCtx, db, "drop database "+sourceDB)
			execSQLRequire(t, cleanupCtx, db, "drop database "+otherDB)
		}()
		execSQLRequire(t, ctx, db, "create table "+sourceDB+".t(id int not null, v int)")
		execSQLRequire(t, ctx, db, "insert into "+sourceDB+".t values(1,10),(2,20)")
		execSQLRequire(t, ctx, db, "use "+sourceDB)
		stmt, err := db.PrepareContext(ctx, "alter table t add primary key(id)")
		require.NoError(t, err)
		defer stmt.Close()
		paramStmt, err := db.PrepareContext(ctx, "select v from "+sourceDB+".t where id=?")
		require.NoError(t, err)
		defer paramStmt.Close()
		// Keep the single pooled connection, but make the session database differ
		// from the one captured by the binary prepared statement.
		execSQLRequire(t, ctx, db, "use "+otherDB)
		var parameterValue int
		require.NoError(t, paramStmt.QueryRowContext(ctx, 1).Scan(&parameterValue))
		require.Equal(t, 10, parameterValue)

		gateReady, releaseGate, gateDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(releaseGate) }) }
		defer release()
		go func() {
			gateDone <- testutils.GetSQLExecutor(cn).ExecTxn(ctx, func(owner executor.TxnExecutor) error {
				result, gateErr := owner.Exec(databranchutils.LineageOwnerLifecyclePessimisticLockSQL(), executor.StatementOption{})
				result.Close()
				if gateErr != nil {
					return gateErr
				}
				close(gateReady)
				select {
				case <-releaseGate:
					return nil
				case <-ctx.Done():
					return context.Cause(ctx)
				}
			}, executor.Options{}.WithAccountID(0))
		}()
		select {
		case <-gateReady:
		case gateErr := <-gateDone:
			t.Fatalf("publication owner: %v", gateErr)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}

		var copies atomic.Int32
		var conflicts atomic.Int32
		var txnMu sync.Mutex
		var txnIDs [][]byte
		restore := compile.SetAlterCopyPhaseHookForTest(func(callCtx context.Context, dbName, table, phase string, op client.TxnOperator) error {
			if dbName != sourceDB || table != "t" {
				return nil
			}
			switch phase {
			case "data-copied":
				copies.Add(1)
				txnMu.Lock()
				txnIDs = append(txnIDs, bytes.Clone(op.Txn().ID))
				txnMu.Unlock()
			case "publication-conflict":
				if conflicts.Add(1) == 1 {
					release()
				}
			}
			return nil
		})
		defer restore()

		_, err = stmt.ExecContext(ctx)
		require.NoError(t, err)
		require.NoError(t, <-gateDone)
		require.Equal(t, int32(1), conflicts.Load(), "the first publication conflict is observed by the retry hook")
		require.Equal(t, int32(2), copies.Load(), "a full transaction retry must recopy from its new transaction")
		txnMu.Lock()
		require.Len(t, txnIDs, 2)
		require.NotEqual(t, txnIDs[0], txnIDs[1], "retry must use a new transaction ID")
		txnMu.Unlock()
		var primaryColumns int
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from information_schema.columns where table_schema=? and table_name='t' and column_key='PRI'",
			sourceDB).Scan(&primaryColumns))
		require.Equal(t, 1, primaryColumns)
		require.NoError(t, paramStmt.QueryRowContext(ctx, 2).Scan(&parameterValue))
		require.Equal(t, 20, parameterValue)

		// Reuse the same prepared object after the first execution.  The current
		// database is still otherDB, so this also proves the PREPARE binding is
		// retained for subsequent executions.
		execSQLRequire(t, ctx, db, "alter table "+sourceDB+".t drop primary key")
		_, err = stmt.ExecContext(ctx)
		require.NoError(t, err)
		require.NoError(t, paramStmt.QueryRowContext(ctx, 1).Scan(&parameterValue))
		require.Equal(t, 10, parameterValue)
	})
}

// A text PREPARE captures the remap policy on its bound AST.  A catalog change
// forces EXECUTE to rebuild that plan; the rebuild must keep applying the
// PREPARE-time mapping instead of resolving the original source database.
func TestIssue28319PreparedAlterRebuildRestoresRemapDatabase(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		const (
			sourceDB = "issue_28319_remap_source"
			destDB   = "issue_28319_remap_dest"
		)
		execSQLRequire(t, ctx, db, "create database "+sourceDB)
		execSQLRequire(t, ctx, db, "create database "+destDB)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cleanupCancel()
			execSQLRequire(t, cleanupCtx, db, "drop database "+sourceDB)
			execSQLRequire(t, cleanupCtx, db, "drop database "+destDB)
		}()
		execSQLRequire(t, ctx, db, "create table "+sourceDB+".t(id int not null, source_only int)")
		execSQLRequire(t, ctx, db, "create table "+destDB+".t(id int not null, dest_only int)")
		execSQLRequire(t, ctx, db, "insert into "+sourceDB+".t values(1,10)")
		execSQLRequire(t, ctx, db, "insert into "+destDB+".t values(1,20)")

		// The inline remap is applied while PREPARE binds the ALTER to destDB.
		execSQLRequire(t, ctx, db, "set enable_remap_hint = on")
		execSQLRequire(t, ctx, db, `/*+ {"remapdb":{"`+sourceDB+`":"`+destDB+`"}} */ prepare remap_alter from 'alter table `+sourceDB+`.t add primary key(id)'`)
		defer execSQLRequire(t, ctx, db, "deallocate prepare remap_alter")
		// Change the catalog after PREPARE so EXECUTE must rebuild its plan.
		execSQLRequire(t, ctx, db, "create table "+destDB+".catalog_change(id int)")
		execSQLRequire(t, ctx, db, "execute remap_alter")

		var sourcePrimary, destPrimary int
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from information_schema.columns where table_schema=? and table_name='t' and column_key='PRI'",
			sourceDB).Scan(&sourcePrimary))
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from information_schema.columns where table_schema=? and table_name='t' and column_key='PRI'",
			destDB).Scan(&destPrimary))
		require.Equal(t, 0, sourcePrimary, "the original source database must not be altered")
		require.Equal(t, 1, destPrimary, "the PREPARE remap must survive plan rebuild")
	})
}

// The optimized publication path must synchronously maintain references for
// the replaced generation while leaving unrelated catalog-wide lineage
// compaction to the background GC.
func TestIssue28319CopyAlterPublicationSkipsGlobalLineageCompaction(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const database = "issue_28319_lineage_publish"
		execSQLRequire(t, ctx, db, "create database "+database)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cleanupCancel()
			execSQLRequire(t, cleanupCtx, db, "drop database "+database)
		}()
		execSQLRequire(t, ctx, db, "create table "+database+".source(id int primary key, v int)")
		execSQLRequire(t, ctx, db, "insert into "+database+".source values(1,10),(2,20)")
		execSQLRequire(t, ctx, db, "data branch create table "+database+".source_branch from "+database+".source")
		for i := 0; i < 6; i++ {
			base := fmt.Sprintf("history_base_%d", i)
			branch := fmt.Sprintf("history_branch_%d", i)
			execSQLRequire(t, ctx, db, "create table "+database+"."+base+"(id int primary key, v int)")
			execSQLRequire(t, ctx, db, "insert into "+database+"."+base+" values(1,10)")
			execSQLRequire(t, ctx, db, "data branch create table "+database+"."+branch+" from "+database+"."+base)
		}

		var compactions atomic.Int32
		restore := compile.SetAlterCopyPhaseHookForTest(func(_ context.Context, dbName, _, phase string, _ client.TxnOperator) error {
			if dbName == database && phase == "lineage-compaction" {
				compactions.Add(1)
			}
			return nil
		})
		defer restore()
		execSQLRequire(t, ctx, db, "alter table "+database+".source modify column v bigint")
		require.Zero(t, compactions.Load(), "COPY ALTER publication must leave unrelated lineage compaction to GC")
		var sourceType string
		require.NoError(t, db.QueryRowContext(ctx,
			"select data_type from information_schema.columns where table_schema=? and table_name='source' and column_name='v'",
			database).Scan(&sourceType))
		require.Equal(t, "bigint", sourceType)
		var branchRows, branchMetadata int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+database+".source_branch").Scan(&branchRows))
		require.Equal(t, 2, branchRows)
		require.NoError(t, db.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_branch_metadata where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='source_branch')",
			database).Scan(&branchMetadata))
		require.Equal(t, 1, branchMetadata, "the old generation reference must remain synchronously maintained")
	})
}

// A peer commits after the target has prepared its writes. Both committing and
// aborting after AdvanceSnapshot must preserve the atomic replacement contract.
func TestIssue28319CopyAlterRefreshTerminal(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const database = "issue_28319_refresh"
		execSQLRequire(t, ctx, db, "create database "+database)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cleanupCancel()
			execSQLRequire(t, cleanupCtx, db, "drop database "+database)
		}()
		for _, rows := range []int{3, 16384} {
			for _, failurePhase := range []string{"", "catalog-refreshed", "old-dropped", "renamed", "tasks-published", "renamed-cancel"} {
				abort := failurePhase != ""
				t.Run(fmt.Sprintf("rows=%d/failure=%s", rows, failurePhase), func(t *testing.T) {
					alterCtx, cancelAlter := context.WithCancel(ctx)
					defer cancelAlter()
					execSQLRequire(t, ctx, db, "create table "+database+".a(id int not null, v varchar(1024))")
					execSQLRequire(t, ctx, db, "create table "+database+".b(id int not null)")
					execSQLRequire(t, ctx, db, fmt.Sprintf("insert into %s.a select result, repeat('x',1024) from generate_series(1,%d) g", database, rows))
					originalID := currentRelationID(t, ctx, db, database, "a")
					stopForceFlush := func() {}
					if rows > 3 {
						fault.Enable()
						defer fault.Disable()
						remove, injectErr := objectio.SimpleInject(objectio.FJ_CNWorkspaceForceFlush)
						require.NoError(t, injectErr)
						var removeOnce sync.Once
						stopForceFlush = func() { removeOnce.Do(remove) }
						defer stopForceFlush()
					}
					prepared := make(chan struct{})
					release := make(chan struct{})
					var once, released sync.Once
					unblock := func() { released.Do(func() { close(release) }) }
					defer unblock()
					var copies, refreshes atomic.Int32
					var spilled atomic.Bool
					restore := compile.SetAlterCopyPhaseHookForTest(func(callCtx context.Context, dbName, table, phase string, op client.TxnOperator) error {
						if dbName != database || table != "a" {
							return nil
						}
						switch phase {
						case "data-copied":
							stopForceFlush()
							copies.Add(1)
						case "prepared":
							result, inspectErr := testutils.GetSQLExecutor(cn).Exec(callCtx,
								"select rel_id, reldatabase_id from mo_catalog.mo_tables where reldatabase='"+database+"' and relname not in ('a','b')",
								executor.Options{}.WithAccountID(0).WithTxn(op).WithKeepTxnAlive().WithDisableIncrStatement())
							if inspectErr != nil {
								result.Close()
								return inspectErr
							}
							result.ReadRows(func(n int, cols []*vector.Vector) bool {
								ids := executor.GetFixedRows[uint64](cols[0])
								dbIDs := executor.GetFixedRows[uint64](cols[1])
								workspace := op.GetWorkspace().(*disttae.Transaction)
								for i := 0; i < n; i++ {
									workspace.ForEachTableWrites(dbIDs[i], ids[i], int(workspace.WriteOffset()), func(entry disttae.Entry) {
										if entry.FileName() != "" {
											spilled.Store(true)
										}
									})
								}
								return true
							})
							result.Close()
							once.Do(func() { close(prepared) })
							select {
							case <-release:
							case <-callCtx.Done():
								return context.Cause(callCtx)
							}
						case "catalog-refreshed":
							refreshes.Add(1)
						}
						if phase == "renamed" && failurePhase == "renamed-cancel" {
							cancelAlter()
							return context.Canceled
						}
						if phase == failurePhase {
							return moerr.NewInternalError(callCtx, "issue 28319 abort at "+failurePhase)
						}
						return nil
					})
					defer restore()
					done := make(chan error, 1)
					go func() {
						_, e := db.ExecContext(alterCtx, "alter table "+database+".a add primary key(id)")
						done <- e
					}()
					select {
					case <-prepared:
					case e := <-done:
						t.Fatalf("did not prepare: %v", e)
					case <-ctx.Done():
						unblock()
						<-done
						t.Fatal(ctx.Err())
					}
					_, peerErr := db.ExecContext(ctx, "alter table "+database+".b add primary key(id)")
					unblock()
					alterErr := <-done
					require.NoError(t, peerErr)
					if abort {
						if failurePhase == "renamed-cancel" {
							require.Error(t, alterErr)
						} else {
							require.ErrorContains(t, alterErr, "issue 28319 abort at "+failurePhase)
						}
						require.Equal(t, originalID, currentRelationID(t, ctx, db, database, "a"))
					} else {
						require.NoError(t, alterErr)
						require.NotEqual(t, originalID, currentRelationID(t, ctx, db, database, "a"))
					}
					require.Equal(t, int32(1), copies.Load())
					require.Equal(t, int32(1), refreshes.Load())
					require.Equal(t, rows > 3, spilled.Load(), "exercise both in-memory and object-backed preparation writes")
					var actualRows, bytes, tables int
					require.NoError(t, db.QueryRowContext(ctx, "select count(*),sum(length(v)) from "+database+".a").Scan(&actualRows, &bytes))
					require.Equal(t, rows, actualRows)
					require.Equal(t, rows*1024, bytes)
					require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase=?", database).Scan(&tables))
					require.Equal(t, 2, tables)
					restore()
					if abort {
						execSQLRequire(t, ctx, db, "alter table "+database+".a add primary key(id)")
					}
					execSQLRequire(t, ctx, db, "alter table "+database+".a drop primary key")
					execSQLRequire(t, ctx, db, "drop table "+database+".a")
					execSQLRequire(t, ctx, db, "drop table "+database+".b")
				})
				if t.Failed() {
					return
				}
			}
		}
	})
}
