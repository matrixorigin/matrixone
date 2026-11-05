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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

// Full-width objects require no missing-column authorization. This exercises
// relation-qualified workspace compaction independently of M0 classification.
func TestCloneSharedWorkspaceObjectDeleteCommit(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		execSQL := func(statement string) {
			t.Helper()
			_, err := db.ExecContext(ctx, statement)
			require.NoError(t, err)
		}
		defer db.Close()
		const schema = "clone_shared_workspace_delete"
		const snapshot = "clone_shared_workspace_delete_snapshot"
		defer cleanupSQLIntegration(t, cn, "drop snapshot if exists "+snapshot, "drop database if exists "+schema)
		execSQL("create database " + schema)
		execSQL("create table " + schema + ".parent (a int not null, b int not null, primary key(a,b))")
		execSQL("insert into " + schema + ".parent values (1,1),(2,2)")
		execSQL("select mo_ctl('dn','flush','" + schema + ".parent')")
		execSQL("create snapshot " + snapshot + " for table " + schema + " parent")
		service := cn.RawService().(cnservice.Service)
		txnCtx := defines.AttachAccountId(ctx, 0)
		err = service.GetSQLExecutor().ExecTxn(txnCtx, func(tx executor.TxnExecutor) error {
			for _, statement := range []string{
				"create table " + schema + ".child1 clone " + schema + ".parent",
				"create table " + schema + ".child2 clone " + schema + ".parent",
				"delete from " + schema + ".child1 where a=1",
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
		for _, tc := range []struct {
			name string
			want bool
		}{{"parent", true}, {"child1", false}, {"child2", true}} {
			var value int
			err := db.QueryRowContext(ctx, "select a from "+schema+"."+tc.name+" where a=1").Scan(&value)
			if tc.want {
				require.NoError(t, err, tc.name)
				require.Equal(t, 1, value)
			} else {
				require.ErrorIs(t, err, sql.ErrNoRows)
			}
		}
		err = service.GetSQLExecutor().ExecTxn(txnCtx, func(tx executor.TxnExecutor) error {
			for _, statement := range []string{
				"create table " + schema + ".child3 clone " + schema + ".parent",
				"create table " + schema + ".child4 clone " + schema + ".parent",
				"delete from " + schema + ".child4 where a=2",
				"delete from " + schema + ".child3 where a=1",
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
		for _, tc := range []struct {
			name string
			a    int
			want bool
		}{{"parent", 1, true}, {"parent", 2, true}, {"child3", 1, false}, {"child3", 2, true}, {"child4", 1, true}, {"child4", 2, false}} {
			var value int
			err := db.QueryRowContext(ctx, "select a from "+schema+"."+tc.name+" where a=?", tc.a).Scan(&value)
			if tc.want {
				require.NoError(t, err, tc.name)
				require.Equal(t, tc.a, value)
			} else {
				require.ErrorIs(t, err, sql.ErrNoRows)
			}
		}
		// Two real relation Delete calls before one compaction preserve each
		// relation's offsets even when the underlying block is shared.
		err = service.GetSQLExecutor().ExecTxn(txnCtx, func(tx executor.TxnExecutor) error {
			for _, child := range []string{"child5", "child6"} {
				result, err := tx.Exec("create table "+schema+"."+child+" clone "+schema+".parent", executor.StatementOption{})
				if err != nil {
					return err
				}
				result.Close()
			}
			result, err := tx.Exec("select __mo_rowid from "+schema+".parent order by a", executor.StatementOption{})
			if err != nil {
				return err
			}
			var rowIDs []types.Rowid
			result.ReadRows(func(_ int, cols []*vector.Vector) bool {
				rowIDs = append(rowIDs, executor.GetFixedRows[types.Rowid](cols[0])...)
				return true
			})
			result.Close()
			if len(rowIDs) != 2 {
				return fmt.Errorf("expected two parent row IDs, got %d", len(rowIDs))
			}
			database, err := service.GetEngine().Database(txnCtx, schema, tx.Txn())
			if err != nil {
				return err
			}
			workspace := tx.Txn().GetWorkspace().(*disttae.Transaction)
			for i, child := range []string{"child5", "child6"} {
				relation, err := database.Relation(txnCtx, child, nil)
				if err != nil {
					return err
				}
				bat := batch.NewWithSize(1)
				bat.SetAttributes([]string{catalog.Row_ID})
				bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
				ids := []types.Rowid{rowIDs[i]}
				if i == 0 {
					ids = append(ids, rowIDs[i]) // The same row can reach two delete passes.
				}
				if err = vector.AppendFixedList(bat.Vecs[0], ids, nil, workspace.GetProc().Mp()); err == nil {
					bat.SetRowCount(len(ids))
					err = relation.Delete(txnCtx, bat, catalog.Row_ID)
				}
				bat.Clean(workspace.GetProc().Mp())
				if err != nil {
					return err
				}
			}
			return nil
		}, executor.Options{}.WithWaitCommittedLogApplied())
		require.NoError(t, err)
		for _, tc := range []struct {
			name string
			a    int
			want bool
		}{{"parent", 1, true}, {"parent", 2, true}, {"child5", 1, false}, {"child5", 2, true}, {"child6", 1, true}, {"child6", 2, false}} {
			var value int
			err := db.QueryRowContext(ctx, "select a from "+schema+"."+tc.name+" where a=?", tc.a).Scan(&value)
			if tc.want {
				require.NoError(t, err, tc.name)
				require.Equal(t, tc.a, value)
			} else {
				require.ErrorIs(t, err, sql.ErrNoRows)
			}
		}
		rollback := errors.New("rollback after workspace compaction")
		err = service.GetSQLExecutor().ExecTxn(txnCtx, func(tx executor.TxnExecutor) error {
			for _, statement := range []string{
				"create table " + schema + ".rollback_child clone " + schema + ".parent",
				"delete from " + schema + ".rollback_child where a=1",
				"select a from " + schema + ".rollback_child where a=2",
			} {
				result, err := tx.Exec(statement, executor.StatementOption{})
				if err != nil {
					return err
				}
				result.Close()
			}
			return rollback
		}, executor.Options{}.WithWaitCommittedLogApplied())
		require.ErrorIs(t, err, rollback)
		var parentCount, childCount int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+schema+".parent").Scan(&parentCount))
		require.Equal(t, 2, parentCount)
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='"+schema+"' and relname='rollback_child'").Scan(&childCount))
		require.Zero(t, childCount)
		for _, id := range []int{1, 2} {
			var value int
			err := db.QueryRowContext(ctx, "select a from "+schema+".parent{snapshot='"+snapshot+"'} where a=?", id).Scan(&value)
			require.NoError(t, err)
			require.Equal(t, id, value)
		}
	})
}
