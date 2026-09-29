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
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

func TestIssue28079ConcurrentCreateAccountsShareLifecycleGate(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		cn0, err := cluster.GetCNService(0)
		require.NoError(t, err)
		cn1, err := cluster.GetCNService(1)
		require.NoError(t, err)
		db0, err := sql.Open("mysql", issue27487DSN(cn0.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db0.Close()) })
		db1, err := sql.Open("mysql", issue27487DSN(cn1.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db1.Close()) })

		const accountA = "issue_28079_account_a"
		const accountB = "issue_28079_account_b"
		accounts := []string{accountA, accountB}
		cleanupAccounts := func() error {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cleanupCancel()
			for _, account := range accounts {
				if _, err := db0.ExecContext(
					cleanupCtx,
					"drop account if exists `"+account+"`",
				); err != nil {
					return fmt.Errorf("drop account %s: %w", account, err)
				}
			}
			var remaining int
			if err := db0.QueryRowContext(
				cleanupCtx,
				"select count(*) from mo_catalog.mo_account where account_name in (?, ?)",
				accountA,
				accountB,
			).Scan(&remaining); err != nil {
				return fmt.Errorf("verify account cleanup: %w", err)
			}
			if remaining != 0 {
				return fmt.Errorf("account cleanup left %d rows", remaining)
			}
			return nil
		}
		require.NoError(t, cleanupAccounts())
		t.Cleanup(func() {
			if err := cleanupAccounts(); err != nil {
				t.Errorf("issue 28079 account cleanup: %v", err)
			}
		})

		var snapshotTableID uint64
		require.NoError(t, db0.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where account_id=0 and "+
				"reldatabase='mo_catalog' and relname='mo_feature_registry'").Scan(&snapshotTableID))
		services := issue27487LockServices(cluster)
		require.NotEmpty(t, services)

		holderA := issue28079HoldAccountName(t, ctx, db0, accountA)
		holderB := issue28079HoldAccountName(t, ctx, db1, accountB)
		viewHolder := issue28079OpenTxn(t, ctx, db0)
		require.NoError(t, viewHolder.Exec(ctx, catalog.ViewMetadataLifecycleSharedGateSQL))
		viewGateKey := issue28079GateKey(
			t,
			services,
			catalog.MO_TABLES_ID,
			[]byte(catalog.MO_VIEW_REFRESH),
		)

		restoreTxnDefaults := issue28079SetTxnDefaults(
			[]embed.ServiceOperator{cn0, cn1},
			pbtxn.TxnMode_Optimistic,
			pbtxn.TxnIsolation_SI,
		)
		defer restoreTxnDefaults()

		createCtx, cancelCreates := context.WithCancel(ctx)
		defer cancelCreates()
		type createResult struct {
			account string
			err     error
		}
		createDone := make(chan createResult, len(accounts))
		startCreate := func(db *sql.DB, account string, start <-chan struct{}) {
			go func() {
				<-start
				_, createErr := db.ExecContext(createCtx, fmt.Sprintf(
					"create account `%s` admin_name 'admin' identified by '111'", account))
				createDone <- createResult{account: account, err: createErr}
			}()
		}

		beforeView := make(chan uint32, len(accounts))
		beforeViewReleased := make(chan uint32, len(accounts))
		releaseBeforeView := make(chan struct{})
		var releaseBeforeViewOnce sync.Once
		releaseBeforeViewReaders := func() {
			releaseBeforeViewOnce.Do(func() { close(releaseBeforeView) })
		}
		restoreBeforeViewHook := frontend.SetCreateAccountBeforeViewLifecycleHookForTest(func(accountID uint32) {
			select {
			case beforeView <- accountID:
			case <-createCtx.Done():
				return
			}
			select {
			case <-releaseBeforeView:
				beforeViewReleased <- accountID
			case <-createCtx.Done():
			}
		})
		defer restoreBeforeViewHook()
		defer releaseBeforeViewReaders()

		viewLocked := make(chan uint32, len(accounts))
		releaseViewReaders := make(chan struct{})
		var releaseViewReadersOnce sync.Once
		releaseReaders := func() {
			releaseViewReadersOnce.Do(func() { close(releaseViewReaders) })
		}
		restoreHook := frontend.SetCreateAccountViewLifecycleLockedHookForTest(func(accountID uint32) {
			select {
			case viewLocked <- accountID:
			case <-createCtx.Done():
				return
			}
			select {
			case <-releaseViewReaders:
			case <-createCtx.Done():
			}
		})
		defer restoreHook()
		defer releaseReaders()

		start := make(chan struct{})
		startCreate(db0, accountA, start)
		startCreate(db1, accountB, start)
		close(start)

		require.Eventually(t, func() bool {
			return issue28079GateState(
				services,
				snapshotTableID,
				[]byte(catalog.SnapshotLifecycleFeatureCode),
			).sharedHolders >= 2
		}, 30*time.Second, 10*time.Millisecond,
			"two CREATE ACCOUNT transactions did not concurrently hold the shared SNAPSHOT gate")
		// The observable holders above prove CREATE forced real pessimistic locks
		// while both CN defaults were optimistic/SI. Restore the defaults before
		// creating the test-only writer transactions so their FOR UPDATE locks are
		// real as well.
		restoreTxnDefaults()
		snapshotGateKey := issue28079GateKey(
			t,
			services,
			snapshotTableID,
			[]byte(catalog.SnapshotLifecycleFeatureCode),
		)
		require.NoError(t, holderA.Close())
		require.NoError(t, holderB.Close())
		observedBeforeViewAccountIDs := make(map[uint32]struct{}, len(accounts))
		for range accounts {
			accountID := issue28079Wait(t, beforeView, "CREATE before View lifecycle hook")
			observedBeforeViewAccountIDs[accountID] = struct{}{}
		}
		require.Len(t, observedBeforeViewAccountIDs, len(accounts))
		type lifecycleLockEvent struct {
			gate     string
			acquired bool
		}
		lockEvents := make(chan lifecycleLockEvent, 4*len(accounts))
		restoreLockEventHook := frontend.SetAccountLifecycleLockEventHookForTest(
			func(gate string, acquired bool) {
				lockEvents <- lifecycleLockEvent{gate: gate, acquired: acquired}
			},
		)
		defer restoreLockEventHook()

		snapshotWriter, snapshotWriterDone := issue28079StartExclusiveGate(
			t, ctx, services[0], snapshotTableID, snapshotGateKey,
			[]byte("issue28079-snapshot-writer"),
		)
		require.Eventually(t, func() bool {
			return issue28079GateState(
				services,
				snapshotTableID,
				[]byte(catalog.SnapshotLifecycleFeatureCode),
			).waiters >= 1
		}, 30*time.Second, 10*time.Millisecond,
			"SNAPSHOT writer did not queue behind CREATE readers")

		viewWriter, viewWriterDone := issue28079StartExclusiveGate(
			t, ctx, services[len(services)-1], catalog.MO_TABLES_ID, viewGateKey,
			[]byte("issue28079-view-writer"),
		)
		require.Eventually(t, func() bool {
			return issue28079GateState(
				services,
				catalog.MO_TABLES_ID,
				[]byte(catalog.MO_VIEW_REFRESH),
			).waiters >= 1
		}, 30*time.Second, 10*time.Millisecond,
			"View writer did not queue behind the shared View holder")

		releaseBeforeViewReaders()
		for range accounts {
			issue28079Wait(t, beforeViewReleased, "CREATE release from before View hook")
		}
		viewQueueDeadline := time.NewTimer(30 * time.Second)
		defer viewQueueDeadline.Stop()
		var snapshotReentries, viewAttempts int
		for snapshotReentries < len(accounts) || viewAttempts < len(accounts) {
			select {
			case event := <-lockEvents:
				switch {
				case event.gate == "snapshot" && event.acquired:
					snapshotReentries++
				case event.gate == "view" && !event.acquired:
					viewAttempts++
				case event.gate == "view" && event.acquired:
					require.FailNow(t,
						"CREATE reader acquired View before the queued writer")
				}
			case result := <-createDone:
				require.FailNowf(t,
					"CREATE ACCOUNT ended before reaching the View queue",
					"account=%s err=%v", result.account, result.err)
			case <-viewQueueDeadline.C:
				state := issue28079GateState(
					services,
					catalog.MO_TABLES_ID,
					[]byte(catalog.MO_VIEW_REFRESH),
				)
				snapshotState := issue28079GateState(
					services,
					snapshotTableID,
					[]byte(catalog.SnapshotLifecycleFeatureCode),
				)
				createWaits := issue28079TxnWaits(services, snapshotState.sharedTxnIDs)
				require.FailNowf(t,
					"CREATE readers did not re-enter SNAPSHOT and queue behind the View writer",
					"view state: %+v, SNAPSHOT state: %+v, CREATE waits: %v, snapshot reentries: %d, View attempts: %d",
					state, snapshotState, createWaits, snapshotReentries, viewAttempts)
			}
		}
		select {
		case writerErr := <-snapshotWriterDone:
			require.Failf(t, "SNAPSHOT writer overtook existing readers", "result: %v", writerErr)
		default:
		}

		require.NoError(t, viewHolder.Close())
		require.NoError(t, issue28079Wait(t, viewWriterDone, "View lifecycle writer"))
		require.Eventually(t, func() bool {
			state := issue28079GateState(
				services,
				catalog.MO_TABLES_ID,
				[]byte(catalog.MO_VIEW_REFRESH),
			)
			return state.exclusiveHolders == 1 && state.sharedHolders == 0
		}, 30*time.Second, 10*time.Millisecond,
			"queued View writer was not admitted before later CREATE readers")
		require.NoError(t, viewWriter.Close())

		observedAccountIDs := make(map[uint32]struct{}, len(accounts))
		for range accounts {
			accountID := issue28079Wait(t, viewLocked, "CREATE View lifecycle hook")
			observedAccountIDs[accountID] = struct{}{}
		}
		require.Len(t, observedAccountIDs, len(accounts))
		require.Eventually(t, func() bool {
			return issue28079GateState(
				services,
				catalog.MO_TABLES_ID,
				[]byte(catalog.MO_VIEW_REFRESH),
			).sharedHolders >= 2
		}, 30*time.Second, 10*time.Millisecond,
			"two CREATE ACCOUNT transactions did not concurrently hold the real View gate")

		releaseReaders()
		for range accounts {
			result := issue28079Wait(t, createDone, "CREATE ACCOUNT")
			require.NoError(t, result.err, result.account)
		}
		require.NoError(t, issue28079Wait(t, snapshotWriterDone, "SNAPSHOT lifecycle writer"))
		require.NoError(t, snapshotWriter.Close())
		cancelCreates()

		var created int
		var queryErr error
		require.Eventually(t, func() bool {
			queryErr = db0.QueryRowContext(ctx,
				"select count(*) from mo_catalog.mo_account where account_name in (?, ?)",
				accountA, accountB).Scan(&created)
			return queryErr == nil && created == len(accounts)
		}, 30*time.Second, 10*time.Millisecond,
			"CREATE ACCOUNT rows did not become visible after both transactions completed")
		require.NoError(t, queryErr)
		require.Equal(t, len(accounts), created)
		require.Eventually(t, func() bool {
			snapshot := issue28079GateState(
				services,
				snapshotTableID,
				[]byte(catalog.SnapshotLifecycleFeatureCode),
			)
			view := issue28079GateState(
				services,
				catalog.MO_TABLES_ID,
				[]byte(catalog.MO_VIEW_REFRESH),
			)
			return snapshot.total() == 0 && view.total() == 0
		}, 30*time.Second, 10*time.Millisecond,
			"lifecycle transactions or waiters remained after completion")
		require.NoError(t, cleanupAccounts())
	})
}

func TestIssue28079LifecycleRefreshAndViewFailure(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn0, err := cluster.GetCNService(0)
		require.NoError(t, err)
		cn1, err := cluster.GetCNService(1)
		require.NoError(t, err)
		db0, err := sql.Open("mysql", issue27487DSN(cn0.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db0.Close()) })
		const successful = "issue_28079_refresh_success"
		const rejected = "issue_28079_refresh_rejected"
		cleanup := func() error {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			var cleanupErr error
			for _, account := range []string{successful, rejected} {
				_, dropErr := db0.ExecContext(cleanupCtx, "drop account if exists `"+account+"`")
				if dropErr != nil {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("drop %s: %w", account, dropErr))
				}
			}
			var count int
			if err := db0.QueryRowContext(cleanupCtx,
				"select count(*) from mo_catalog.mo_account where account_name in (?, ?)",
				successful, rejected).Scan(&count); err != nil {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("verify account cleanup: %w", err))
			} else if count != 0 {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("account cleanup left %d rows", count))
			}
			return cleanupErr
		}
		require.NoError(t, cleanup())
		t.Cleanup(func() {
			if err := cleanup(); err != nil {
				t.Errorf("issue 28079 refresh account cleanup: %v", err)
			}
		})

		service, ok := cn1.RawService().(cnservice.Service)
		require.True(t, ok)
		require.NoError(t, compile.RequireViewMetadataRevalidation(ctx, service.GetSQLExecutor()))
		var originalGeneration uint64
		require.NoError(t, db0.QueryRowContext(ctx,
			"select dependency_generation from mo_catalog.mo_view_dependencies "+
				"where account_id=0 and target_relation_id=0 and dependency_ordinal=0",
		).Scan(&originalGeneration))
		var snapshotTableID uint64
		require.NoError(t, db0.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where account_id=0 and "+
				"reldatabase='mo_catalog' and relname='mo_feature_registry'",
		).Scan(&snapshotTableID))

		firstSnapshot := make(chan uint32, 1)
		releaseSnapshot := make(chan struct{})
		var firstSnapshotOnce, releaseSnapshotOnce sync.Once
		defer releaseSnapshotOnce.Do(func() { close(releaseSnapshot) })
		restoreLockHook := frontend.SetAccountLifecycleLockEventHookForTest(func(gate string, acquired bool) {
			if gate == "snapshot" && !acquired {
				firstSnapshotOnce.Do(func() {
					firstSnapshot <- 0
					select {
					case <-releaseSnapshot:
					case <-ctx.Done():
					}
				})
			}
		})
		defer restoreLockHook()

		beforeView := make(chan uint32, 1)
		releaseView := make(chan struct{})
		var releaseViewOnce sync.Once
		defer releaseViewOnce.Do(func() { close(releaseView) })
		restoreViewHook := frontend.SetCreateAccountBeforeViewLifecycleHookForTest(func(accountID uint32) {
			select {
			case beforeView <- accountID:
			case <-ctx.Done():
				return
			}
			select {
			case <-releaseView:
			case <-ctx.Done():
			}
		})
		defer restoreViewHook()

		createDone := make(chan error, 1)
		go func() {
			_, createErr := db0.ExecContext(ctx,
				"create account `"+successful+"` admin_name 'admin' identified by '111'")
			createDone <- createErr
		}()
		created := false
		defer func() {
			if !created {
				cancel()
				releaseSnapshotOnce.Do(func() { close(releaseSnapshot) })
				releaseViewOnce.Do(func() { close(releaseView) })
				select {
				case <-createDone:
				case <-time.After(30 * time.Second):
					t.Error("CREATE did not exit after lifecycle barrier cleanup")
				}
			}
		}()

		issue28079Wait(t, firstSnapshot, "initial SNAPSHOT direct admission")
		createTxnID := make(chan []byte, 1)
		testingContext := moruntime.MustGetTestingContext(cn0.ServiceID())
		testingContext.SetBeforeLockFunc(func([]byte, uint64) {})
		defer testingContext.SetBeforeLockFunc(nil)
		testingContext.SetAdjustLockResultFunc(func(txnID []byte, tableID uint64, _ *pblock.Result) {
			if tableID == snapshotTableID {
				select {
				case createTxnID <- append([]byte(nil), txnID...):
				default:
				}
			}
		})
		defer testingContext.SetAdjustLockResultFunc(nil)
		_ = issue28079CommitLifecycleGate(t, ctx, service.GetSQLExecutor(),
			catalog.SnapshotLifecycleGateSQL,
			"update mo_catalog.mo_feature_registry set scope_spec=scope_spec, updated_at=updated_at "+
				"where feature_code='SNAPSHOT'")
		releaseSnapshotOnce.Do(func() { close(releaseSnapshot) })
		var newAccountID uint32
		select {
		case newAccountID = <-beforeView:
		case createErr := <-createDone:
			created = true
			t.Fatalf("CREATE ended before View admission: %v", createErr)
		case <-ctx.Done():
			t.Fatalf("CREATE did not reach View admission: %v", ctx.Err())
		}
		ownerTxnID := issue28079Wait(t, createTxnID, "CREATE transaction lock identity")
		viewCommitTS := issue28079CommitLifecycleGate(t, ctx, service.GetSQLExecutor(),
			catalog.ViewMetadataLifecycleGateSQL,
			"update mo_catalog.mo_view_dependencies set dependency_generation=dependency_generation+1 "+
				"where account_id=0 and target_relation_id=0 and dependency_ordinal=0")
		require.False(t, viewCommitTS.IsEmpty())
		// The writer committed a newer View generation, but a View-row SELECT
		// alone does not leave a new mo_tables row version. Model the owner's
		// committed-lock timestamp for this one CREATE lock attempt; the SQL
		// verification and inherited marker still execute on the real engine.
		var refreshInjected atomic.Bool
		testingContext.SetAdjustLockResultFunc(func(txnID []byte, tableID uint64, result *pblock.Result) {
			if tableID == catalog.MO_TABLES_ID && bytes.Equal(txnID, ownerTxnID) &&
				refreshInjected.CompareAndSwap(false, true) {
				result.HasConflict = true
				result.HasPrevCommit = true
				result.Timestamp = viewCommitTS
			}
		})
		defer testingContext.SetAdjustLockResultFunc(nil)
		releaseViewOnce.Do(func() { close(releaseView) })
		createErr := issue28079Wait(t, createDone, "refreshed CREATE ACCOUNT")
		created = true
		require.NoError(t, createErr)
		require.True(t, refreshInjected.Load(), "View gate did not consume the committed refresh")
		testingContext.SetAdjustLockResultFunc(nil)
		testingContext.SetBeforeLockFunc(nil)

		var storedAccountID uint32
		require.NoError(t, db0.QueryRowContext(ctx,
			"select account_id from mo_catalog.mo_account where account_name=?", successful,
		).Scan(&storedAccountID))
		require.Equal(t, newAccountID, storedAccountID)
		var inheritedGeneration uint64
		require.NoError(t, db0.QueryRowContext(ctx,
			"select dependency_generation from mo_catalog.mo_view_dependencies "+
				"where account_id=? and target_relation_id=0 and dependency_ordinal=0",
			newAccountID).Scan(&inheritedGeneration))
		require.Equal(t, originalGeneration+1, inheritedGeneration)
		var markerCount int
		require.NoError(t, db0.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_view_dependencies "+
				"where account_id=? and target_relation_id=0 and dependency_ordinal=0",
			newAccountID).Scan(&markerCount))
		require.Equal(t, 1, markerCount)

		// The second CREATE has already inserted account-local catalog rows when
		// its View lock fails. Its owner transaction must roll all of them back.
		failedBeforeView := make(chan uint32, 1)
		failedRelease := make(chan struct{})
		var failedReleaseOnce sync.Once
		defer failedReleaseOnce.Do(func() { close(failedRelease) })
		restoreViewHook()
		restoreFailedHook := frontend.SetCreateAccountBeforeViewLifecycleHookForTest(func(accountID uint32) {
			select {
			case failedBeforeView <- accountID:
			case <-ctx.Done():
				return
			}
			select {
			case <-failedRelease:
			case <-ctx.Done():
			}
		})
		defer restoreFailedHook()
		faultEnabledHere := fault.Enable()
		if faultEnabledHere {
			defer fault.Disable()
		}
		failedDone := make(chan error, 1)
		go func() {
			_, createErr := db0.ExecContext(ctx,
				"create account `"+rejected+"` admin_name 'admin' identified by '111'")
			failedDone <- createErr
		}()
		failedConsumed := false
		defer func() {
			if !failedConsumed {
				cancel()
				failedReleaseOnce.Do(func() { close(failedRelease) })
				select {
				case <-failedDone:
				case <-time.After(30 * time.Second):
					t.Error("failed CREATE did not exit after barrier cleanup")
				}
			}
		}()
		failedAccountID := issue28079Wait(t, failedBeforeView, "CREATE before injected View error")
		removeFault, err := objectio.InjectLogging(
			objectio.FJ_CNNeedRetryError, catalog.MO_CATALOG, catalog.MO_TABLES, 0, false)
		require.NoError(t, err)
		var removeFaultOnce sync.Once
		defer removeFaultOnce.Do(removeFault)
		failedReleaseOnce.Do(func() { close(failedRelease) })
		failedErr := issue28079Wait(t, failedDone, "injected View failure")
		failedConsumed = true
		require.ErrorContains(t, failedErr, "txn need retry")
		removeFaultOnce.Do(removeFault)
		for _, query := range []string{
			"select count(*) from mo_catalog.mo_account where account_id=?",
			"select count(*) from mo_catalog.mo_database where account_id=?",
			"select count(*) from mo_catalog.mo_view_dependencies where account_id=?",
		} {
			var count int
			require.NoError(t, db0.QueryRowContext(ctx, query, failedAccountID).Scan(&count))
			require.Zero(t, count, query)
		}
		services := issue27487LockServices(cluster)
		require.Eventually(t, func() bool {
			return issue28079GateState(services, snapshotTableID,
				[]byte(catalog.SnapshotLifecycleFeatureCode)).total() == 0 &&
				issue28079GateState(services, catalog.MO_TABLES_ID,
					[]byte(catalog.MO_VIEW_REFRESH)).total() == 0
		}, 30*time.Second, 10*time.Millisecond, "account lifecycle locks remained after success/failure")
	})
}

func issue28079CommitLifecycleGate(
	t *testing.T, ctx context.Context, sqlExecutor executor.SQLExecutor, gate, update string,
) timestamp.Timestamp {
	t.Helper()
	var writerTxn executor.TxnExecutor
	err := sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		writerTxn = txn
		result, execErr := txn.Exec(gate, executor.StatementOption{})
		if execErr != nil {
			return execErr
		}
		result.Close()
		result, execErr = txn.Exec(update, executor.StatementOption{})
		if execErr != nil {
			return execErr
		}
		rows := result.AffectedRows
		result.Close()
		if rows != 1 {
			return fmt.Errorf("lifecycle write affected %d rows, want 1", rows)
		}
		return nil
	}, executor.Options{}.
		WithAccountID(catalog.System_Account).
		WithTxnMode(pbtxn.TxnMode_Pessimistic).
		WithTxnIsolation(pbtxn.TxnIsolation_RC).
		WithWaitCommittedLogApplied())
	require.NoError(t, err)
	return writerTxn.Txn().Txn().CommitTS
}

type issue28079OwnedTxn struct {
	conn *sql.Conn
	once sync.Once
	err  error
}

type issue28079OwnedLockTxn struct {
	service  lockservice.LockService
	txnID    []byte
	registry lockservice.ExternalTxnLivenessRegistry
	cancel   context.CancelFunc
	once     sync.Once
	err      error
}

func issue28079OpenTxn(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
) *issue28079OwnedTxn {
	t.Helper()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	txn := &issue28079OwnedTxn{conn: conn}
	t.Cleanup(func() {
		if err := txn.Close(); err != nil {
			t.Errorf("issue 28079 transaction cleanup: %v", err)
		}
	})
	require.NoError(t, txn.Exec(ctx, "begin"))
	return txn
}

func issue28079HoldAccountName(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	account string,
) *issue28079OwnedTxn {
	t.Helper()
	txn := issue28079OpenTxn(t, ctx, db)
	require.NoError(t, txn.Exec(ctx, fmt.Sprintf(
		"select account_name from mo_catalog.__mo_account_lock where account_name = '%s' for update",
		account,
	)))
	return txn
}

func (txn *issue28079OwnedTxn) Exec(ctx context.Context, statement string) error {
	_, err := txn.conn.ExecContext(ctx, statement)
	return err
}

func (txn *issue28079OwnedTxn) Close() error {
	txn.once.Do(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, rollbackErr := txn.conn.ExecContext(cleanupCtx, "rollback")
		txn.err = errors.Join(rollbackErr, txn.conn.Close())
	})
	return txn.err
}

func issue28079StartExclusiveGate(
	t *testing.T,
	ctx context.Context,
	service lockservice.LockService,
	tableID uint64,
	key []byte,
	txnID []byte,
) (*issue28079OwnedLockTxn, <-chan error) {
	t.Helper()
	lockCtx, cancel := context.WithCancel(ctx)
	owned := &issue28079OwnedLockTxn{
		service: service,
		txnID:   append([]byte(nil), txnID...),
		cancel:  cancel,
	}
	if registry, ok := service.(lockservice.ExternalTxnLivenessRegistry); ok {
		require.NoError(t, registry.RegisterExternalTxn(owned.txnID))
		owned.registry = registry
	}
	t.Cleanup(func() {
		if err := owned.Close(); err != nil {
			t.Errorf("issue 28079 lock transaction cleanup: %v", err)
		}
	})
	done := make(chan error, 1)
	go func() {
		_, err := service.Lock(lockCtx, tableID, [][]byte{key}, owned.txnID, pblock.LockOptions{
			Granularity: pblock.Granularity_Row,
			Mode:        pblock.LockMode_Exclusive,
			Policy:      pblock.WaitPolicy_Wait,
			Group:       0,
		})
		done <- err
	}()
	return owned, done
}

func (txn *issue28079OwnedLockTxn) Close() error {
	txn.once.Do(func() {
		txn.cancel()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		txn.err = txn.service.Unlock(cleanupCtx, txn.txnID, timestamp.Timestamp{})
		if txn.registry != nil {
			txn.registry.UnregisterExternalTxn(txn.txnID)
		}
	})
	return txn.err
}

type issue28079LockState struct {
	sharedHolders    int
	exclusiveHolders int
	waiters          int
	sharedTxnIDs     []string
	exclusiveTxnIDs  []string
	waiterTxnIDs     []string
}

func (s issue28079LockState) total() int {
	return s.sharedHolders + s.exclusiveHolders + s.waiters
}

func issue28079GateState(
	services []lockservice.LockService,
	tableID uint64,
	keyFragment []byte,
) issue28079LockState {
	sharedHolders := make(map[string]struct{})
	exclusiveHolders := make(map[string]struct{})
	waiters := make(map[string]struct{})
	for _, service := range services {
		service.IterLocks(func(lockedTableID uint64, keys [][]byte, lock lockservice.Lock) bool {
			if !issue28079IsGate(lockedTableID, keys, tableID, keyFragment) {
				return true
			}
			holders := sharedHolders
			if lock.GetLockMode() == pblock.LockMode_Exclusive {
				holders = exclusiveHolders
			}
			lock.IterHolders(func(holder pblock.WaitTxn) bool {
				holders[string(holder.TxnID)] = struct{}{}
				return true
			})
			lock.IterWaiters(func(waiter pblock.WaitTxn) bool {
				waiters[string(waiter.TxnID)] = struct{}{}
				return true
			})
			return true
		})
	}
	return issue28079LockState{
		sharedHolders:    len(sharedHolders),
		exclusiveHolders: len(exclusiveHolders),
		waiters:          len(waiters),
		sharedTxnIDs:     issue28079SortedTxnIDs(sharedHolders),
		exclusiveTxnIDs:  issue28079SortedTxnIDs(exclusiveHolders),
		waiterTxnIDs:     issue28079SortedTxnIDs(waiters),
	}
}

func issue28079GateKey(
	t *testing.T,
	services []lockservice.LockService,
	tableID uint64,
	keyFragment []byte,
) []byte {
	t.Helper()
	var result []byte
	require.Eventually(t, func() bool {
		for _, service := range services {
			service.IterLocks(func(lockedTableID uint64, keys [][]byte, _ lockservice.Lock) bool {
				if lockedTableID != tableID {
					return true
				}
				for _, key := range keys {
					if bytes.Contains(key, keyFragment) {
						result = append(result[:0], key...)
						return false
					}
				}
				return true
			})
			if len(result) > 0 {
				return true
			}
		}
		return false
	}, 30*time.Second, 10*time.Millisecond,
		"lifecycle gate key was not observable")
	return result
}

func issue28079SortedTxnIDs(txns map[string]struct{}) []string {
	ids := make([]string, 0, len(txns))
	for txnID := range txns {
		ids = append(ids, fmt.Sprintf("%x", txnID))
	}
	sort.Strings(ids)
	return ids
}

func issue28079TxnWaits(
	services []lockservice.LockService,
	txnIDs []string,
) []string {
	wanted := make(map[string]struct{}, len(txnIDs))
	for _, txnID := range txnIDs {
		wanted[txnID] = struct{}{}
	}
	var waits []string
	for _, service := range services {
		service.IterLocks(func(tableID uint64, keys [][]byte, lock lockservice.Lock) bool {
			lock.IterWaiters(func(waiter pblock.WaitTxn) bool {
				txnID := fmt.Sprintf("%x", waiter.TxnID)
				if _, ok := wanted[txnID]; ok {
					waits = append(waits, fmt.Sprintf(
						"txn=%s table=%d mode=%s keys=%x",
						txnID, tableID, lock.GetLockMode(), keys))
				}
				return true
			})
			return true
		})
	}
	sort.Strings(waits)
	return waits
}

func issue28079IsGate(
	tableID uint64,
	keys [][]byte,
	wantTableID uint64,
	keyFragment []byte,
) bool {
	if tableID != wantTableID {
		return false
	}
	for _, key := range keys {
		if bytes.Contains(key, keyFragment) {
			return true
		}
	}
	return false
}

func issue28079Wait[T any](t *testing.T, done <-chan T, operation string) T {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(90 * time.Second):
		t.Fatalf("%s did not finish", operation)
		var zero T
		return zero
	}
}

func TestIssue28079TemporaryAlterKeepsOptimisticTransaction(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		restoreDefaults := issue28079SetTxnDefaults(
			[]embed.ServiceOperator{cn}, pbtxn.TxnMode_Optimistic, pbtxn.TxnIsolation_SI)
		defer restoreDefaults()

		db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		const schema = "issue_28079_temp_alter"
		_, err = conn.ExecContext(ctx, "drop database if exists "+schema)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "create database "+schema)
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "rollback")
			if _, cleanupErr := conn.ExecContext(cleanupCtx, "drop temporary table if exists "+schema+".t"); cleanupErr != nil {
				t.Errorf("drop temporary table: %v", cleanupErr)
			}
			if _, cleanupErr := conn.ExecContext(cleanupCtx, "drop database if exists "+schema); cleanupErr != nil {
				t.Errorf("drop test database: %v", cleanupErr)
			}
		}()

		_, err = conn.ExecContext(ctx, "create table "+schema+".t (a int)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "create temporary table "+schema+".t (a int)")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "begin")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "alter table "+schema+".t add column b int")
		require.NoError(t, err, "temporary alias should bypass persistent lifecycle admission")
		_, err = conn.ExecContext(ctx, "rollback")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "drop temporary table "+schema+".t")
		require.NoError(t, err)

		_, err = conn.ExecContext(ctx, "begin")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "alter table "+schema+".t add column b int")
		require.ErrorContains(t, err, "lifecycle statements require an existing pessimistic transaction")
		_, err = conn.ExecContext(ctx, "rollback")
		require.NoError(t, err)
	})
}

func issue28079SetTxnDefaults(
	services []embed.ServiceOperator,
	mode pbtxn.TxnMode,
	isolation pbtxn.TxnIsolation,
) func() {
	type previousTxnConfig struct {
		runtime      moruntime.Runtime
		mode         any
		hadMode      bool
		isolation    any
		hadIsolation bool
	}
	previous := make([]previousTxnConfig, 0, len(services))
	for _, service := range services {
		rt := moruntime.ServiceRuntime(service.ServiceID())
		oldMode, hadMode := rt.GetGlobalVariables(moruntime.TxnMode)
		oldIsolation, hadIsolation := rt.GetGlobalVariables(moruntime.TxnIsolation)
		previous = append(previous, previousTxnConfig{
			runtime: rt, mode: oldMode, hadMode: hadMode,
			isolation: oldIsolation, hadIsolation: hadIsolation,
		})
		rt.SetGlobalVariables(moruntime.TxnMode, mode)
		rt.SetGlobalVariables(moruntime.TxnIsolation, isolation)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			for _, config := range previous {
				if config.hadMode {
					config.runtime.SetGlobalVariables(moruntime.TxnMode, config.mode)
				} else {
					config.runtime.SetGlobalVariables(moruntime.TxnMode, pbtxn.TxnMode_Pessimistic)
				}
				if config.hadIsolation {
					config.runtime.SetGlobalVariables(moruntime.TxnIsolation, config.isolation)
				} else {
					config.runtime.SetGlobalVariables(moruntime.TxnIsolation, pbtxn.TxnIsolation_RC)
				}
			}
		})
	}
}
