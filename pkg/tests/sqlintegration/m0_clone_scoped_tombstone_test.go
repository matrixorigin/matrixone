//go:build mo_m0

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
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

// The S3 tombstone is injected into the real transaction workspace. This is
// a real clone and SQL reader test, not an end-to-end SQL DELETE test.
func TestM0CloneScopedWorkspaceTombstoneSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openM0SQL(t, cn)
		defer db.Close()
		const schema = "m0_scoped_workspace_tombstone"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "create database "+schema)
		m0Exec(t, ctx, db, "create table "+schema+".parent (a int not null, b int not null, primary key(a,b))")
		m0Exec(t, ctx, db, "insert into "+schema+".parent values (1,1),(2,2)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+schema+".parent')")
		m0Exec(t, ctx, db, "alter table "+schema+".parent add column extra int")
		m0Exec(t, ctx, db, "create table "+schema+".child clone "+schema+".parent")
		service := cn.RawService().(cnservice.Service)
		txnCtx := defines.AttachAccountId(ctx, 0)
		rollback := errors.New("rollback injected workspace tombstone")
		err = service.GetSQLExecutor().ExecTxn(txnCtx, func(tx executor.TxnExecutor) error {
			returned := false
			defer func() {
				if !returned {
					cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
					defer stop()
					_ = tx.Txn().Rollback(cleanupCtx)
				}
			}()

			query := func(sql string) executor.Result {
				t.Helper()
				result, err := tx.Exec(sql, executor.StatementOption{})
				require.NoError(t, err, sql)
				return result
			}
			readIDs := func(table string) []types.Rowid {
				result := query("select __mo_rowid from " + schema + "." + table + " where a=1")
				defer result.Close()
				var ids []types.Rowid
				result.ReadRows(func(_ int, cols []*vector.Vector) bool {
					ids = append(ids, executor.GetFixedRows[types.Rowid](cols[0])...)
					return true
				})
				return ids
			}
			parentIDs, childIDs := readIDs("parent"), readIDs("child")
			require.Len(t, parentIDs, 1)
			require.Equal(t, parentIDs, childIDs, "clone must read the same persisted row")
			workspace, ok := tx.Txn().GetWorkspace().(*disttae.Transaction)
			require.True(t, ok)
			proc := workspace.GetProc()
			fs, err := colexec.GetSharedFSFromProc(proc)
			require.NoError(t, err)
			pkType := types.T_int32.ToType()
			writer := colexec.NewCNS3TombstoneWriter(proc.Mp(), fs, pkType, -1)
			defer writer.Close()
			rows := readutil.NewCNTombstoneBatch(&pkType, objectio.HiddenColumnSelection_None)
			defer rows.Clean(proc.Mp())
			require.NoError(t, vector.AppendFixed(rows.Vecs[0], childIDs[0], false, proc.Mp()))
			require.NoError(t, vector.AppendFixed(rows.Vecs[1], int32(1), false, proc.Mp()))
			rows.SetRowCount(1)
			require.NoError(t, writer.Write(txnCtx, rows))
			stats, err := writer.Sync(txnCtx)
			require.NoError(t, err)
			require.Len(t, stats, 1)
			result := colexec.AllocCNS3ResultBat(true)
			defer result.Clean(proc.Mp())
			require.NoError(t, vector.AppendBytes(result.Vecs[0], stats[0].Marshal(), false, proc.Mp()))
			result.SetRowCount(1)
			database, err := service.GetEngine().Database(txnCtx, schema, tx.Txn())
			require.NoError(t, err)
			dbID, err := strconv.ParseUint(database.GetDatabaseId(txnCtx), 10, 64)
			require.NoError(t, err)
			child, err := database.Relation(txnCtx, "child", nil)
			require.NoError(t, err)
			require.NoError(t, workspace.WriteFile(disttae.DELETE, 0, dbID, child.GetTableID(txnCtx), schema, "child", stats[0].ObjectLocation().String(), result, disttae.DNStore{}))
			workspace.UpdateSnapshotWriteOffset()
			require.Len(t, readIDs("parent"), 1)
			require.Empty(t, readIDs("child"))
			returned = true
			return rollback
		}, executor.Options{}.WithWaitCommittedLogApplied())
		require.ErrorIs(t, err, rollback)
		cloneRollback := errors.New("rollback same-transaction clone")
		err = service.GetSQLExecutor().ExecTxn(txnCtx, func(tx executor.TxnExecutor) error {
			returned := false
			defer func() {
				if !returned {
					cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
					defer stop()
					_ = tx.Txn().Rollback(cleanupCtx)
				}
			}()

			result, err := tx.Exec("create table "+schema+".txn_child clone "+schema+".parent", executor.StatementOption{})
			require.NoError(t, err)
			result.Close()
			readIDs := func(table string) []types.Rowid {
				result, err := tx.Exec("select __mo_rowid from "+schema+"."+table+" where a=1", executor.StatementOption{})
				require.NoError(t, err)
				defer result.Close()
				var ids []types.Rowid
				result.ReadRows(func(_ int, cols []*vector.Vector) bool {
					ids = append(ids, executor.GetFixedRows[types.Rowid](cols[0])...)
					return true
				})
				return ids
			}
			parentIDs, childIDs := readIDs("parent"), readIDs("txn_child")
			require.Len(t, parentIDs, 1)
			require.Equal(t, parentIDs, childIDs)
			database, err := service.GetEngine().Database(txnCtx, schema, tx.Txn())
			require.NoError(t, err)
			parent, err := database.Relation(txnCtx, "parent", nil)
			require.NoError(t, err)
			child, err := database.Relation(txnCtx, "txn_child", nil)
			require.NoError(t, err)
			workspace := tx.Txn().GetWorkspace().(*disttae.Transaction)
			server := colexec.MustGetServer(workspace.GetProc().GetService())
			segment := parentIDs[0].BorrowSegmentID()
			t.Logf("clone segment registration: parent=%d child=%d", server.GetCnSegmentType(segment, parent.GetTableID(txnCtx), tx.Txn().Txn().ID), server.GetCnSegmentType(segment, child.GetTableID(txnCtx), tx.Txn().Txn().ID))
			result, err = tx.Exec("delete from "+schema+".txn_child where a=1", executor.StatementOption{})
			require.NoError(t, err)
			result.Close()
			for _, relation := range []struct {
				name string
				rel  engine.Relation
				want int
			}{{"parent", parent, 1}, {"txn_child", child, 0}} {
				tombstones, err := relation.rel.CollectTombstones(txnCtx, int(workspace.WriteOffset()), engine.Policy_CollectUncommittedTombstones)
				require.NoError(t, err)
				left := tombstones.ApplyInMemTombstones(parentIDs[0].BorrowBlockID(), []int64{int64(parentIDs[0].GetRowOffset())}, nil)
				require.Len(t, left, relation.want, "transaction tombstones must be scoped to %s", relation.name)
			}
			require.Len(t, readIDs("parent"), 1)
			require.Empty(t, readIDs("txn_child"))
			returned = true
			return cloneRollback
		}, executor.Options{}.WithWaitCommittedLogApplied())
		require.ErrorIs(t, err, cloneRollback)
		deleteRollback := errors.New("rollback real SQL delete")
		err = service.GetSQLExecutor().ExecTxn(txnCtx, func(tx executor.TxnExecutor) error {
			result, err := tx.Exec("delete from "+schema+".child where a=1", executor.StatementOption{})
			require.NoError(t, err)
			result.Close()
			workspace := tx.Txn().GetWorkspace().(*disttae.Transaction)
			database, err := service.GetEngine().Database(txnCtx, schema, tx.Txn())
			require.NoError(t, err)
			dbID, err := strconv.ParseUint(database.GetDatabaseId(txnCtx), 10, 64)
			require.NoError(t, err)
			child, err := database.Relation(txnCtx, "child", nil)
			require.NoError(t, err)
			flushed := 0
			workspace.ForEachTableWrites(dbID, child.GetTableID(txnCtx), int(workspace.WriteOffset()), func(entry disttae.Entry) {
				if entry.Type() == disttae.DELETE && entry.FileName() != "" {
					flushed++
				}
			})
			t.Logf("real SQL delete: %d workspace S3 tombstone entries", flushed)
			for _, tc := range []struct {
				table string
				want  int
			}{{"parent", 1}, {"child", 0}} {
				res, err := tx.Exec("select a from "+schema+"."+tc.table+" where a=1", executor.StatementOption{})
				require.NoError(t, err)
				rows := 0
				res.ReadRows(func(n int, _ []*vector.Vector) bool { rows += n; return true })
				res.Close()
				require.Equal(t, tc.want, rows, "real DELETE visibility in %s", tc.table)
			}
			return deleteRollback
		}, executor.Options{}.WithWaitCommittedLogApplied())
		require.ErrorIs(t, err, deleteRollback)
		for _, table := range []string{"parent", "child"} {
			var count int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+schema+"."+table+" where a=1").Scan(&count))
			require.Equal(t, 1, count, "rollback must preserve the committed row in %s", table)
		}
		// After commit, use an ordinary row projection from fresh statements;
		// a COUNT rewrite must not replace this reader check.
		m0Exec(t, ctx, db, "delete from "+schema+".child where a=1")
		var value int
		require.NoError(t, db.QueryRowContext(ctx, "select a from "+schema+".parent where a=1").Scan(&value))
		require.Equal(t, 1, value)
		err = db.QueryRowContext(ctx, "select a from "+schema+".child where a=1").Scan(&value)
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}

func TestM0CloneMultipleWorkspaceOwnersCommit(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openM0SQL(t, cn)
		defer db.Close()
		const schema = "m0_clone_workspace_owners"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "create database "+schema)
		m0Exec(t, ctx, db, "create table "+schema+".parent (a int not null, b int not null, primary key(a,b))")
		m0Exec(t, ctx, db, "insert into "+schema+".parent values (1,1),(2,2)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+schema+".parent')")
		m0Exec(t, ctx, db, "alter table "+schema+".parent add column extra int")
		service := cn.RawService().(cnservice.Service)
		txnCtx := defines.AttachAccountId(ctx, 0)
		for _, tc := range []struct {
			name       string
			statements []string
			deleted    string
			second     bool
		}{
			{"shared-owner", []string{
				"create table " + schema + ".child1 clone " + schema + ".parent",
				"create table " + schema + ".child2 clone " + schema + ".parent",
				"delete from " + schema + ".child1 where a=1",
			}, "child1", true},
			{"shared-last-owner", []string{
				"create table " + schema + ".child1 clone " + schema + ".parent",
				"create table " + schema + ".child2 clone " + schema + ".parent",
				"delete from " + schema + ".child2 where a=1",
			}, "child2", true},
			{"rewrite-failure", []string{
				"create table " + schema + ".child1 clone " + schema + ".parent",
				"delete from " + schema + ".child1 where a=1",
			}, "child1", false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				err := service.GetSQLExecutor().ExecTxn(txnCtx, func(tx executor.TxnExecutor) error {
					for _, statement := range tc.statements {
						result, err := tx.Exec(statement, executor.StatementOption{})
						if err != nil {
							return err
						}
						result.Close()
					}
					return nil
				}, executor.Options{}.WithWaitCommittedLogApplied())
				require.NoError(t, err)
				var parentCount int
				require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+schema+".parent where a=1").Scan(&parentCount))
				require.Equal(t, 1, parentCount)
				children := []string{"child1"}
				if tc.second {
					children = append(children, "child2")
				}
				for _, child := range children {
					var count int
					require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+schema+"."+child+" where a=1").Scan(&count))
					if child == tc.deleted {
						require.Zero(t, count)
					} else {
						require.Equal(t, 1, count)
					}
					m0Exec(t, ctx, db, "drop table "+schema+"."+child)
				}
			})
		}
	})
}

// Cleanup is engine-owned after integration with failure-atomic compaction.
// Drive its existing bounded pass explicitly rather than assuming asynchronous
// deletion has completed before the failed transaction returns.
func m0AssertRewriteOutputCollected(t *testing.T, ctx context.Context, eng *disttae.Engine, name string) {
	t.Helper()
	require.NoError(t, eng.DrainUnpublishedPendingForTest(ctx))
	_, statErr := eng.FS().StatFile(ctx, name)
	require.True(t, moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound), "unpublished rewrite output must be removed")
	queued, _, _ := eng.InspectUnpublishedPendingForTest()
	require.NotContains(t, queued, name, "collected output must retire from engine cleanup")
}

func TestM0CloneCompactionFailureCannotCommit(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openM0SQL(t, cn)
		defer db.Close()
		const schema = "m0_clone_failed_compaction"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "create database "+schema)
		m0Exec(t, ctx, db, "create table "+schema+".parent (a int not null, b int not null, primary key(a,b))")
		m0Exec(t, ctx, db, "insert into "+schema+".parent values (1,1),(2,2)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+schema+".parent')")
		m0Exec(t, ctx, db, "alter table "+schema+".parent add column extra int")
		const point = "fj/m0/rewrite/after-sync"
		m0AddFaultPoint(t, ctx, db, point, "echo", "injected rewrite failure")
		defer m0RemoveFaultPointQuiet(ctx, db, point)
		service := cn.RawService().(cnservice.Service)
		txnCtx := defines.AttachAccountId(ctx, 0)
		var maintenanceErr error
		err = service.GetSQLExecutor().ExecTxn(txnCtx, func(tx executor.TxnExecutor) error {
			for _, statement := range []string{
				"create table " + schema + ".child clone " + schema + ".parent",
				"delete from " + schema + ".child where a=1",
			} {
				result, err := tx.Exec(statement, executor.StatementOption{})
				if err != nil {
					return err
				}
				result.Close()
			}
			result, err := tx.Exec("select a from "+schema+".parent where a=1", executor.StatementOption{})
			if err == nil {
				result.Close()
			}
			maintenanceErr = err
			// A caller may choose to continue after a statement error. The
			// damaged workspace must not then commit a lost delete.
			return nil
		}, executor.Options{}.WithWaitCommittedLogApplied())
		require.ErrorContains(t, maintenanceErr, "injected rewrite failure")
		require.Error(t, err, "a failed compaction must prevent later commit")
		_, names, ok := strings.Cut(maintenanceErr.Error(), "objects=")
		require.True(t, ok)
		name := strings.TrimSpace(strings.SplitN(names, "\n", 2)[0])
		require.NotEmpty(t, name, "the fault must run after an object reaches storage")
		engine, ok := service.GetEngine().(*disttae.Engine)
		require.True(t, ok)
		m0AssertRewriteOutputCollected(t, ctx, engine, name)
		var count int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+schema+".parent").Scan(&count))
		require.Equal(t, 2, count)
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='"+schema+"' and relname='child'").Scan(&count))
		require.Zero(t, count)
	})
}

func TestM0ClonePhysicalRewriteCancelAfterSync(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openM0SQL(t, cn)
		defer db.Close()
		const schema = "m0_clone_rewrite_cancel"
		const point = "fj/m0/rewrite/after-sync"
		const getter = point + "/waiters"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		defer m0RemoveFaultPointQuiet(context.Background(), db, getter)
		defer m0RemoveFaultPointQuiet(context.Background(), db, point)
		m0Exec(t, ctx, db, "create database "+schema)
		m0Exec(t, ctx, db, "create table "+schema+".parent (a int not null, b int not null, primary key(a,b))")
		m0Exec(t, ctx, db, "insert into "+schema+".parent values (1,1),(2,2)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+schema+".parent')")
		m0Exec(t, ctx, db, "alter table "+schema+".parent add column extra int")
		m0AddFaultPoint(t, ctx, db, point, "wait", "")
		m0AddFaultPoint(t, ctx, db, getter, "getwaiters", point)
		service := cn.RawService().(cnservice.Service)
		runCtx, cancelRun := context.WithCancel(defines.AttachAccountId(ctx, 0))
		done := make(chan error, 1)
		finished := false
		defer func() {
			cancelRun()
			m0RemoveFaultPointQuiet(context.Background(), db, point)
			if !finished {
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Error("physical rewrite did not stop after cancellation")
				}
			}
		}()
		go func() {
			done <- service.GetSQLExecutor().ExecTxn(runCtx, func(tx executor.TxnExecutor) error {
				for _, statement := range []string{
					"create table " + schema + ".child clone " + schema + ".parent",
					"delete from " + schema + ".child where a=1",
				} {
					result, err := tx.Exec(statement, executor.StatementOption{})
					if err != nil {
						return err
					}
					result.Close()
				}
				return nil
			}, executor.Options{}.WithWaitCommittedLogApplied())
		}()
		m0WaitForFaultWaiter(t, ctx, db, getter, 1)
		cancelRun()
		err = m0WaitForError(t, done)
		finished = true
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
		_, names, ok := strings.Cut(err.Error(), "objects=")
		require.True(t, ok, "cancellation must preserve the output ownership evidence")
		name := strings.TrimSpace(strings.SplitN(names, "\n", 2)[0])
		require.NotEmpty(t, name)
		engine, ok := service.GetEngine().(*disttae.Engine)
		require.True(t, ok)
		m0AssertRewriteOutputCollected(t, ctx, engine, name)
		var count int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+schema+".parent").Scan(&count))
		require.Equal(t, 2, count)
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='"+schema+"' and relname='child'").Scan(&count))
		require.Zero(t, count)
		m0WaitForFaultWaiter(t, ctx, db, getter, 0)
	})
}

func TestM0ClonePhysicalRewritePanicAfterSync(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openM0SQL(t, cn)
		defer db.Close()
		const schema = "m0_clone_rewrite_panic"
		const point = "fj/m0/rewrite/after-sync"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		defer m0RemoveFaultPointQuiet(context.Background(), db, point)
		m0Exec(t, ctx, db, "create database "+schema)
		m0Exec(t, ctx, db, "create table "+schema+".parent (a int not null, b int not null, primary key(a,b))")
		m0Exec(t, ctx, db, "insert into "+schema+".parent values (1,1),(2,2)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+schema+".parent')")
		m0Exec(t, ctx, db, "alter table "+schema+".parent add column extra int")
		m0AddFaultPoint(t, ctx, db, point, "panic", "injected rewrite panic")
		service := cn.RawService().(cnservice.Service)
		txnCtx := defines.AttachAccountId(ctx, 0)
		var maintenanceErr error
		err = service.GetSQLExecutor().ExecTxn(txnCtx, func(tx executor.TxnExecutor) error {
			for _, statement := range []string{
				"create table " + schema + ".child clone " + schema + ".parent",
				"delete from " + schema + ".child where a=1",
			} {
				result, err := tx.Exec(statement, executor.StatementOption{})
				if err != nil {
					return err
				}
				result.Close()
			}
			result, err := tx.Exec("select a from "+schema+".parent where a=1", executor.StatementOption{})
			if err == nil {
				result.Close()
			}
			maintenanceErr = err
			return nil // A damaged workspace cannot be committed.
		}, executor.Options{}.WithWaitCommittedLogApplied())
		require.ErrorContains(t, maintenanceErr, "injected rewrite panic")
		require.Error(t, err)
		_, names, ok := strings.Cut(maintenanceErr.Error(), "objects=")
		require.True(t, ok, "panic must retain the output cleanup identity")
		name := strings.TrimSpace(strings.SplitN(names, "\n", 2)[0])
		require.NotEmpty(t, name)
		engine, ok := service.GetEngine().(*disttae.Engine)
		require.True(t, ok)
		m0AssertRewriteOutputCollected(t, ctx, engine, name)
		var count int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+schema+".parent").Scan(&count))
		require.Equal(t, 2, count)
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='"+schema+"' and relname='child'").Scan(&count))
		require.Zero(t, count)
	})
}

func TestM0CloneOldLayoutDeleteCommit(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openM0SQL(t, cn)
		defer db.Close()
		const schema = "m0_clone_old_layout_delete"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "create database "+schema)
		m0Exec(t, ctx, db, "create table "+schema+".parent (c_id int not null, b int not null, primary key(c_id,b))")
		m0Exec(t, ctx, db, "insert into "+schema+".parent values (1,1),(2,2)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+schema+".parent')")
		m0Exec(t, ctx, db, "alter table "+schema+".parent add column extra int")
		before := m0ObjectHeaderProofs(t, ctx, db, cn, schema+".parent")
		require.Len(t, before, 1)
		service := cn.RawService().(cnservice.Service)
		txnCtx := defines.AttachAccountId(ctx, 0)
		err = service.GetSQLExecutor().ExecTxn(txnCtx, func(tx executor.TxnExecutor) error {
			for _, statement := range []string{
				"create table " + schema + ".child clone " + schema + ".parent",
				"delete from " + schema + ".child where c_id=1",
			} {
				result, err := tx.Exec(statement, executor.StatementOption{})
				if err != nil {
					return err
				}
				result.Close()
			}
			return nil
		}, executor.Options{}.WithWaitCommittedLogApplied())
		require.NoError(t, err)
		require.Equal(t, before, m0ObjectHeaderProofs(t, ctx, db, cn, schema+".parent"))
		var value int
		require.NoError(t, db.QueryRowContext(ctx, "select c_id from "+schema+".parent where c_id=1").Scan(&value))
		require.Equal(t, 1, value)
		require.ErrorIs(t, db.QueryRowContext(ctx, "select c_id from "+schema+".child where c_id=1").Scan(&value), sql.ErrNoRows)
		var extra sql.NullInt64
		require.NoError(t, db.QueryRowContext(ctx, "select c_id, extra from "+schema+".child where c_id=2").Scan(&value, &extra))
		require.Equal(t, 2, value)
		require.False(t, extra.Valid)
		after := m0ObjectHeaderProofs(t, ctx, db, cn, schema+".child")
		require.Len(t, after, 1)
		require.NotEqual(t, before, after, "child must reference the rewritten object")
		require.Contains(t, before[0], ":0:[2]")
		require.Contains(t, after[0], ":0:[2]", "physical rewrite must preserve the old object version and width")
		err = service.GetSQLExecutor().ExecTxn(txnCtx, func(tx executor.TxnExecutor) error {
			for _, statement := range []string{
				"create table " + schema + ".empty_child clone " + schema + ".parent",
				"delete from " + schema + ".empty_child where c_id in (1,2)",
			} {
				result, err := tx.Exec(statement, executor.StatementOption{})
				if err != nil {
					return err
				}
				result.Close()
			}
			return nil
		}, executor.Options{}.WithWaitCommittedLogApplied())
		require.NoError(t, err)
		var count int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+schema+".empty_child").Scan(&count))
		require.Zero(t, count)
		require.Equal(t, before, m0ObjectHeaderProofs(t, ctx, db, cn, schema+".parent"))
	})
}

func TestM0CloneIntermediateLayoutDeleteCommit(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openM0SQL(t, cn)
		defer db.Close()
		const schema = "m0_clone_intermediate_layout"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "create database "+schema)
		m0Exec(t, ctx, db, "create table "+schema+".parent (c_id int not null, b int not null, primary key(c_id,b))")
		m0Exec(t, ctx, db, "alter table "+schema+".parent add column extra int")
		m0Exec(t, ctx, db, "insert into "+schema+".parent values (1,1,11),(2,2,22)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+schema+".parent')")
		m0Exec(t, ctx, db, "alter table "+schema+".parent add column later int")
		before := m0ObjectHeaderProofs(t, ctx, db, cn, schema+".parent")
		require.Len(t, before, 1)
		require.Contains(t, before[0], ":1:[3]")
		service := cn.RawService().(cnservice.Service)
		txnCtx := defines.AttachAccountId(ctx, 0)
		err = service.GetSQLExecutor().ExecTxn(txnCtx, func(tx executor.TxnExecutor) error {
			for _, statement := range []string{
				"create table " + schema + ".child clone " + schema + ".parent",
				"delete from " + schema + ".child where c_id=1",
			} {
				result, err := tx.Exec(statement, executor.StatementOption{})
				if err != nil {
					return err
				}
				result.Close()
			}
			return nil
		}, executor.Options{}.WithWaitCommittedLogApplied())
		require.NoError(t, err)
		require.Equal(t, before, m0ObjectHeaderProofs(t, ctx, db, cn, schema+".parent"))
		after := m0ObjectHeaderProofs(t, ctx, db, cn, schema+".child")
		require.Len(t, after, 1)
		require.NotEqual(t, before, after)
		require.Contains(t, after[0], ":1:[3]", "rewritten object must retain its intermediate version and physical width")
		var id, extra int
		var later sql.NullInt64
		require.NoError(t, db.QueryRowContext(ctx, "select c_id,extra,later from "+schema+".child where c_id=2").Scan(&id, &extra, &later))
		require.Equal(t, 2, id)
		require.Equal(t, 22, extra)
		require.False(t, later.Valid)
		require.ErrorIs(t, db.QueryRowContext(ctx, "select c_id from "+schema+".child where c_id=1").Scan(&id), sql.ErrNoRows)
		require.NoError(t, db.QueryRowContext(ctx, "select c_id from "+schema+".parent where c_id=1").Scan(&id))
		// Merge the preserved old-width output with a new-width object. The
		// merge must consume both layouts without changing the source relation.
		m0Exec(t, ctx, db, "insert into "+schema+".child values (3,3,33,NULL)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+schema+".child')")
		m0RunL0Merge(t, ctx, db, schema+".child")
		require.Equal(t, before, m0ObjectHeaderProofs(t, ctx, db, cn, schema+".parent"))
		require.NoError(t, db.QueryRowContext(ctx, "select c_id,extra,later from "+schema+".child where c_id=2").Scan(&id, &extra, &later))
		require.Equal(t, 22, extra)
		require.False(t, later.Valid)
		require.NoError(t, db.QueryRowContext(ctx, "select c_id,extra,later from "+schema+".child where c_id=3").Scan(&id, &extra, &later))
		require.Equal(t, 33, extra)
		require.False(t, later.Valid)
	})
}

func TestM0CloneMultiBlockPhysicalRewrite(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openM0SQL(t, cn)
		defer db.Close()
		const schema = "m0_clone_multi_block_rewrite"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "create database "+schema)
		m0Exec(t, ctx, db, "create table "+schema+".parent (c_id int not null, b int not null, primary key(c_id,b))")
		m0Exec(t, ctx, db, "insert into "+schema+".parent select result,result from generate_series(1,8193) g")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+schema+".parent')")
		m0Exec(t, ctx, db, "alter table "+schema+".parent add column extra int")
		before := m0ObjectHeaderProofs(t, ctx, db, cn, schema+".parent")
		require.Len(t, before, 1)
		require.Contains(t, before[0], ":0:[2 2]", "fixture must create one two-block old-layout object")
		service := cn.RawService().(cnservice.Service)
		txnCtx := defines.AttachAccountId(ctx, 0)
		err = service.GetSQLExecutor().ExecTxn(txnCtx, func(tx executor.TxnExecutor) error {
			for _, statement := range []string{
				"create table " + schema + ".child clone " + schema + ".parent",
				"delete from " + schema + ".child where c_id in (1,8193)",
			} {
				result, err := tx.Exec(statement, executor.StatementOption{})
				if err != nil {
					return err
				}
				result.Close()
			}
			return nil
		}, executor.Options{}.WithWaitCommittedLogApplied())
		require.NoError(t, err)
		require.Equal(t, before, m0ObjectHeaderProofs(t, ctx, db, cn, schema+".parent"))
		var parentRows, childRows, nullRows, deletedRows int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+schema+".parent").Scan(&parentRows))
		require.NoError(t, db.QueryRowContext(ctx, "select count(*),count(*)-count(extra) from "+schema+".child").Scan(&childRows, &nullRows))
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+schema+".child where c_id in (1,8193)").Scan(&deletedRows))
		require.Equal(t, 8193, parentRows)
		require.Equal(t, 8191, childRows)
		require.Equal(t, childRows, nullRows)
		require.Zero(t, deletedRows)
	})
}
