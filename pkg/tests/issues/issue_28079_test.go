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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
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
		require.NoError(t, db0.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_account where account_name in (?, ?)",
			accountA, accountB).Scan(&created))
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
