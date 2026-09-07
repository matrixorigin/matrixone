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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
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
		dropLockService := lockservice.GetLockServiceByServiceID(cn0.ServiceID())
		writerLockService := lockservice.GetLockServiceByServiceID(cn1.ServiceID())

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
				testCDCWatermarkDropRace(t, ctx,
					dropExec, writerExec, dropLockService, writerLockService,
					tc.taskID, writerSQL, tc.writerFirst)

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
	dropExec, writerExec executor.SQLExecutor,
	dropLockService, writerLockService lockservice.LockService,
	taskID, writerSQL string,
	writerFirst bool,
) {
	t.Helper()
	holderExec, contenderExec := dropExec, writerExec
	holderLockService := dropLockService
	holderSQL := []string{
		fmt.Sprintf("delete from mo_catalog.mo_cdc_task where account_id = 0 and task_id = '%s'", taskID),
		fmt.Sprintf("delete from mo_catalog.mo_cdc_watermark where account_id = 0 and task_id = '%s'", taskID),
	}
	contenderSQL := []string{writerSQL}
	if writerFirst {
		holderExec, contenderExec = contenderExec, holderExec
		holderLockService = writerLockService
		holderSQL, contenderSQL = contenderSQL, holderSQL
	}

	releaseHolder := make(chan struct{})
	holder := startCDCRaceTxnWithTxnID(t, ctx, holderExec, releaseHolder, holderSQL...)
	require.NoError(t, holder.waitReady(ctx))
	contender := startCDCRaceTxnWithTxnID(t, ctx, contenderExec, nil, contenderSQL...)
	require.NoError(t, contender.waitTxnID(ctx))
	probeCtx, cancelProbe := context.WithTimeout(ctx, 30*time.Second)
	defer cancelProbe()
	observedWaiterTxnIDs, err := waitForCDCWaiter(probeCtx, func(probeCtx context.Context) (bool, []string, error) {
		return holder.hasWaiter(probeCtx, holderLockService, contender.txnID)
	})
	require.NoError(t, err,
		"contender transaction %x did not wait for holder transaction %x; observed waiter transaction IDs: %v",
		contender.txnID, holder.txnID, observedWaiterTxnIDs)

	close(releaseHolder)
	finishCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	require.NoError(t, holder.wait(finishCtx))
	require.NoError(t, contender.wait(finishCtx))
}

// cdcRaceTxn publishes its transaction ID, when requested, as ExecTxn enters
// its callback; readiness only after all statements succeed; and completion on
// every ExecTxn return, including errors before the callback is entered.
// Closing done publishes err and lets both the assertion and cleanup join it.
type cdcRaceTxn struct {
	started chan struct{}
	ready   chan struct{}
	done    chan struct{}
	err     error
	txnID   []byte
}

func startCDCRaceTxn(t *testing.T, parent context.Context, sqlExec executor.SQLExecutor, release <-chan struct{}, statements ...string) *cdcRaceTxn {
	return startCDCRaceTxnInternal(t, parent, sqlExec, release, false, statements...)
}

func startCDCRaceTxnWithTxnID(
	t *testing.T,
	parent context.Context,
	sqlExec executor.SQLExecutor,
	release <-chan struct{},
	statements ...string,
) *cdcRaceTxn {
	return startCDCRaceTxnInternal(t, parent, sqlExec, release, true, statements...)
}

func startCDCRaceTxnInternal(
	t *testing.T,
	parent context.Context,
	sqlExec executor.SQLExecutor,
	release <-chan struct{},
	captureTxnID bool,
	statements ...string,
) *cdcRaceTxn {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	run := &cdcRaceTxn{
		started: make(chan struct{}),
		ready:   make(chan struct{}),
		done:    make(chan struct{}),
	}
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
			if captureTxnID {
				run.txnID = append(run.txnID[:0], txn.Txn().Txn().ID...)
			}
			close(run.started)
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

func (r *cdcRaceTxn) waitTxnID(ctx context.Context) error {
	select {
	case <-r.started:
		if len(r.txnID) == 0 {
			return fmt.Errorf("CDC race transaction did not publish a transaction ID")
		}
		return nil
	case <-r.done:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
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

// hasWaiter asks the holder's lock service for the wait graph rooted at the
// holder transaction. Unlike IterLocks, the query follows the holder's
// authoritative lock route when the table is bound to another CN.
func (r *cdcRaceTxn) hasWaiter(
	ctx context.Context,
	lockService lockservice.LockService,
	waiterTxnID []byte,
) (bool, []string, error) {
	if lockService == nil {
		return false, nil, fmt.Errorf("holder transaction has no lock service")
	}
	if len(r.txnID) == 0 {
		return false, nil, fmt.Errorf("holder transaction has no transaction ID")
	}
	if len(waiterTxnID) == 0 {
		return false, nil, fmt.Errorf("contender transaction has no transaction ID")
	}
	found, waiters, err := lockService.GetWaitingList(ctx, r.txnID)
	if err != nil {
		return false, nil, err
	}
	if !found {
		return false, nil, fmt.Errorf("holder transaction is not active on lock service %q", lockService.GetServiceID())
	}
	foundWaiter, observedTxnIDs := cdcWaiterMatches(waiters, waiterTxnID)
	return foundWaiter, observedTxnIDs, nil
}

func cdcWaiterMatches(waiters []lockpb.WaitTxn, waiterTxnID []byte) (bool, []string) {
	observedTxnIDs := make([]string, 0, len(waiters))
	for _, waiter := range waiters {
		observedTxnIDs = append(observedTxnIDs, fmt.Sprintf("%x", waiter.TxnID))
		if bytes.Equal(waiter.TxnID, waiterTxnID) {
			return true, observedTxnIDs
		}
	}
	return false, observedTxnIDs
}

// waitForCDCWaiter performs each lock-service query in the caller goroutine.
// The probe context bounds both the retry window and any in-flight remote
// query, so transaction cleanup cannot overlap an observation goroutine.
func waitForCDCWaiter(
	ctx context.Context,
	probe func(context.Context) (bool, []string, error),
) ([]string, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	var lastWaiterTxnIDs []string
	var lastErr error
	for {
		found, waiterTxnIDs, err := probe(ctx)
		lastWaiterTxnIDs = waiterTxnIDs
		lastErr = err
		if err == nil && found {
			return waiterTxnIDs, nil
		}
		if ctx.Err() != nil {
			if lastErr != nil {
				return lastWaiterTxnIDs, lastErr
			}
			return lastWaiterTxnIDs, ctx.Err()
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return lastWaiterTxnIDs, fmt.Errorf("%w (last probe error: %v)", ctx.Err(), lastErr)
			}
			return lastWaiterTxnIDs, ctx.Err()
		case <-ticker.C:
		}
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
