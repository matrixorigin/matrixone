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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestIssue27666CDCWatermarkWriteSerializesWithDrop(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		cn0, err := c.GetCNService(0)
		require.NoError(t, err)
		cn1, err := c.GetCNService(1)
		require.NoError(t, err)
		openDB := func(port int64) *sql.DB {
			db, openErr := sql.Open("mysql", fmt.Sprintf("sys#root#moadmin:111@tcp(127.0.0.1:%d)/", port))
			require.NoError(t, openErr)
			db.SetMaxOpenConns(1)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			return db
		}
		queryDB := openDB(cn0.GetServiceConfig().CN.Frontend.Port)
		dropExec := testutils.GetSQLExecutor(cn0)
		writerExec := testutils.GetSQLExecutor(cn1)

		var taskTableID uint64
		require.NoError(t, queryDB.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where account_id = 0 and reldatabase = 'mo_catalog' and relname = 'mo_cdc_task'",
		).Scan(&taskTableID))
		require.NotZero(t, taskTableID)

		testCases := []struct {
			name        string
			taskID      string
			taskName    string
			writerFirst bool
		}{
			{name: "drop wins", taskID: "27666000-0000-0000-0000-000000000001", taskName: "issue-27666-drop-first"},
			{name: "writer wins", taskID: "27666000-0000-0000-0000-000000000002", taskName: "issue-27666-writer-first", writerFirst: true},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				cleanupCDCFixture(t, ctx, dropExec, tc.taskID)
				defer cleanupCDCFixture(t, context.Background(), dropExec, tc.taskID)
				require.NoError(t, execInternalSQL(ctx, dropExec, fmt.Sprintf(
					"insert into mo_catalog.mo_cdc_task (account_id, task_id, task_name, source_uri, sink_uri, tables, task_create_time, state) "+
						"values (0, '%s', '%s', 'source', 'sink', 'db.table', now(), 'running')",
					tc.taskID, tc.taskName)))

				writerSQL := cdc.CDCSQLBuilder.GuardedWatermarkInsertSQL(
					fmt.Sprintf(
						"SELECT 0 AS account_id, '%s' AS task_id, 'db' AS db_name, 'table-1' AS table_name, '1-1' AS watermark, '' AS err_msg "+
							"UNION ALL SELECT 0, '%s', 'db', 'table-2', '1-1', ''",
						tc.taskID,
						tc.taskID,
					),
					fmt.Sprintf("(account_id = 0 AND task_id = '%s')", tc.taskID),
				)
				if tc.writerFirst {
					testCDCWriterWinsDropRace(t, ctx, c, writerExec, dropExec, taskTableID, tc.taskID, writerSQL)
				} else {
					testCDCDropWinsWriterRace(t, ctx, c, dropExec, writerExec, taskTableID, tc.taskID, writerSQL)
				}

				var taskCount, watermarkCount int
				require.NoError(t, queryDB.QueryRowContext(ctx,
					"select count(*) from mo_catalog.mo_cdc_task where account_id = 0 and task_id = ?", tc.taskID,
				).Scan(&taskCount))
				require.NoError(t, queryDB.QueryRowContext(ctx,
					"select count(*) from mo_catalog.mo_cdc_watermark where account_id = 0 and task_id = ?", tc.taskID,
				).Scan(&watermarkCount))
				require.Zero(t, taskCount)
				require.Zero(t, watermarkCount)
			})
		}
	})
}

func testCDCDropWinsWriterRace(
	t *testing.T,
	ctx context.Context,
	c embed.Cluster,
	dropExec, writerExec executor.SQLExecutor,
	taskTableID uint64,
	taskID, writerSQL string,
) {
	t.Helper()
	holderReady := make(chan error, 1)
	releaseHolder := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- dropExec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			if err := execInternalTxnSQL(txn, fmt.Sprintf(
				"delete from mo_catalog.mo_cdc_task where account_id = 0 and task_id = '%s'", taskID)); err != nil {
				holderReady <- err
				return err
			}
			if err := execInternalTxnSQL(txn, fmt.Sprintf(
				"delete from mo_catalog.mo_cdc_watermark where account_id = 0 and task_id = '%s'", taskID)); err != nil {
				holderReady <- err
				return err
			}
			holderReady <- nil
			select {
			case <-releaseHolder:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}, executor.Options{}.WithAccountID(0))
	}()
	require.NoError(t, <-holderReady)
	holderReleased := false
	defer func() {
		if !holderReleased {
			close(releaseHolder)
		}
	}()

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- execInternalSQL(ctx, writerExec, writerSQL)
	}()
	require.Eventually(t, func() bool {
		return clusterHasLockWaiter(c, taskTableID)
	}, 30*time.Second, 10*time.Millisecond,
		"guarded watermark writer did not wait for the task-row delete")

	close(releaseHolder)
	holderReleased = true
	require.NoError(t, <-holderDone)
	select {
	case writerErr := <-writerDone:
		require.NoError(t, writerErr)
	case <-time.After(30 * time.Second):
		t.Fatal("guarded watermark writer did not finish after DROP committed")
	}
}

func testCDCWriterWinsDropRace(
	t *testing.T,
	ctx context.Context,
	c embed.Cluster,
	writerExec, dropExec executor.SQLExecutor,
	taskTableID uint64,
	taskID, writerSQL string,
) {
	t.Helper()
	holderReady := make(chan error, 1)
	releaseHolder := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- writerExec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			if err := execInternalTxnSQL(txn, writerSQL); err != nil {
				holderReady <- err
				return err
			}
			holderReady <- nil
			select {
			case <-releaseHolder:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}, executor.Options{}.WithAccountID(0))
	}()
	require.NoError(t, <-holderReady)
	holderReleased := false
	defer func() {
		if !holderReleased {
			close(releaseHolder)
		}
	}()

	dropDone := make(chan error, 1)
	go func() {
		dropDone <- dropExec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			if err := execInternalTxnSQL(txn, fmt.Sprintf(
				"delete from mo_catalog.mo_cdc_task where account_id = 0 and task_id = '%s'", taskID)); err != nil {
				return err
			}
			return execInternalTxnSQL(txn, fmt.Sprintf(
				"delete from mo_catalog.mo_cdc_watermark where account_id = 0 and task_id = '%s'", taskID))
		}, executor.Options{}.WithAccountID(0))
	}()
	require.Eventually(t, func() bool {
		return clusterHasLockWaiter(c, taskTableID)
	}, 30*time.Second, 10*time.Millisecond,
		"DROP did not wait for the guarded watermark writer's task-row lock")

	close(releaseHolder)
	holderReleased = true
	require.NoError(t, <-holderDone)
	select {
	case dropErr := <-dropDone:
		require.NoError(t, dropErr)
	case <-time.After(30 * time.Second):
		t.Fatal("DROP did not finish after the guarded writer committed")
	}
}

func cleanupCDCFixture(t *testing.T, ctx context.Context, sqlExec executor.SQLExecutor, taskID string) {
	t.Helper()
	cleanupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	require.NoError(t, execInternalSQL(cleanupCtx, sqlExec,
		fmt.Sprintf("delete from mo_catalog.mo_cdc_watermark where account_id = 0 and task_id = '%s'", taskID)))
	require.NoError(t, execInternalSQL(cleanupCtx, sqlExec,
		fmt.Sprintf("delete from mo_catalog.mo_cdc_task where account_id = 0 and task_id = '%s'", taskID)))
}

func execInternalSQL(ctx context.Context, sqlExec executor.SQLExecutor, statement string) error {
	result, err := sqlExec.Exec(ctx, statement, executor.Options{}.WithAccountID(0))
	result.Close()
	return err
}

func execInternalTxnSQL(txn executor.TxnExecutor, statement string) error {
	result, err := txn.Exec(statement, executor.StatementOption{}.WithAccountID(0))
	result.Close()
	return err
}
