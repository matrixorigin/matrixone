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
				// Transaction cleanups registered below run first, including on FailNow.
				t.Cleanup(func() { cleanupCDCFixture(t, context.Background(), dropExec, tc.taskID) })
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
				testCDCWatermarkDropRace(t, ctx, c, dropExec, writerExec, taskTableID, tc.taskID, writerSQL, tc.writerFirst)

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

func testCDCWatermarkDropRace(
	t *testing.T,
	ctx context.Context,
	c embed.Cluster,
	dropExec, writerExec executor.SQLExecutor,
	taskTableID uint64,
	taskID, writerSQL string,
	writerFirst bool,
) {
	t.Helper()
	holderExec, contenderExec := dropExec, writerExec
	holderSQL := []string{
		fmt.Sprintf("delete from mo_catalog.mo_cdc_task where account_id = 0 and task_id = '%s'", taskID),
		fmt.Sprintf("delete from mo_catalog.mo_cdc_watermark where account_id = 0 and task_id = '%s'", taskID),
	}
	contenderSQL := []string{writerSQL}
	if writerFirst {
		holderExec, contenderExec = contenderExec, holderExec
		holderSQL, contenderSQL = contenderSQL, holderSQL
	}

	releaseHolder := make(chan struct{})
	holder := startCDCRaceTxn(t, ctx, holderExec, releaseHolder, holderSQL...)
	require.NoError(t, holder.waitReady(ctx))
	contender := startCDCRaceTxn(t, ctx, contenderExec, nil, contenderSQL...)
	require.Eventually(t, func() bool {
		return clusterHasLockWaiter(c, taskTableID)
	}, 30*time.Second, 10*time.Millisecond,
		"contender did not wait for the holder's task-row lock")

	close(releaseHolder)
	finishCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	require.NoError(t, holder.wait(finishCtx))
	require.NoError(t, contender.wait(finishCtx))
}

// cdcRaceTxn publishes readiness only after all statements succeed, and completion
// on every ExecTxn return, including errors before the callback is entered.
// Closing done publishes err and lets both the assertion and cleanup join it.
type cdcRaceTxn struct {
	ready chan struct{}
	done  chan struct{}
	err   error
}

func startCDCRaceTxn(t *testing.T, parent context.Context, sqlExec executor.SQLExecutor, release <-chan struct{}, statements ...string) *cdcRaceTxn {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	run := &cdcRaceTxn{ready: make(chan struct{}), done: make(chan struct{})}
	// Register before launching work: even a failed readiness assertion must
	// cancel and join the transaction before the fixture's catalog cleanup.
	t.Cleanup(func() {
		cancel()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		select {
		case <-run.done:
		case <-cleanupCtx.Done():
			t.Error("CDC race transaction did not stop after cancellation")
		}
	})
	go func() {
		defer close(run.done)
		run.err = sqlExec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			for _, statement := range statements {
				if err := execInternalTxnSQL(txn, statement); err != nil {
					return err
				}
			}
			close(run.ready)
			if release == nil {
				return nil
			}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}, executor.Options{}.WithAccountID(0))
	}()
	return run
}

func (r *cdcRaceTxn) waitReady(ctx context.Context) error {
	select {
	case <-r.ready:
		return nil
	case <-r.done:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *cdcRaceTxn) wait(ctx context.Context) error {
	select {
	case <-r.done:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
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
