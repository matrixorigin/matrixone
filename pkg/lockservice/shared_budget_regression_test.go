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

package lockservice

import (
	"context"
	"testing"
	"time"

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func requireExactTxnTableBookkeeping(
	t *testing.T,
	s *service,
	txnID []byte,
	table uint64,
	expected int,
) {
	t.Helper()
	txn := s.activeTxnHolder.getActiveTxn(txnID, false, "")
	require.NotNil(t, txn)
	txn.RLock()
	defer txn.RUnlock()
	h := txn.lockHolders[0]
	require.NotNil(t, h)
	require.Equal(t, expected, h.tableKeys[table].mustGet().len())
	require.Contains(t, h.nonCoarsenableTables, table)
}

func requireExactMixedModeLockStore(
	t *testing.T,
	s *service,
	table uint64,
) {
	t.Helper()
	lt := s.tableGroups.get(0, table).(*localLockTable)
	lt.mu.RLock()
	defer lt.mu.RUnlock()
	for _, tc := range []struct {
		key  byte
		mode pb.LockMode
	}{
		{key: 1, mode: pb.LockMode_Shared},
		{key: 2, mode: pb.LockMode_Shared},
		{key: 3, mode: pb.LockMode_Exclusive},
		{key: 4, mode: pb.LockMode_Exclusive},
		{key: 5, mode: pb.LockMode_Exclusive},
	} {
		lock, ok := lt.mu.store.Get(newTestRows(tc.key)[0])
		require.True(t, ok, "missing row %d", tc.key)
		require.True(t, lock.isLockRow(), "row %d was unexpectedly coarsened", tc.key)
		require.Equal(t, tc.mode, lock.GetLockMode(), "row %d mode changed", tc.key)
	}
}

func TestMixedModeBudgetKeepsExactOwnership(t *testing.T) {
	for _, foreignHolder := range []bool{false, true} {
		name := "sole-holder"
		if foreignHolder {
			name = "foreign-shared-holder"
		}
		t.Run(name, func(t *testing.T) {
			runLockServiceTestsWithAdjustConfig(
				t,
				[]string{"s1"},
				time.Second*10,
				func(_ *lockTableAllocator, services []*service) {
					s := services[0]
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()
					const table = uint64(26711)
					txnA := []byte("mixed-budget-a")
					txnB := []byte("mixed-budget-b")

					_, err := s.Lock(ctx, table, newTestRows(1, 2), txnA, newTestRowSharedOptions())
					require.NoError(t, err)
					// A repeated statement must not lose the monotonic table-level
					// eligibility state or duplicate authoritative bookkeeping.
					_, err = s.Lock(ctx, table, newTestRows(1), txnA, newTestRowSharedOptions())
					require.NoError(t, err)
					if foreignHolder {
						_, err = s.Lock(ctx, table, newTestRows(1), txnB, newTestRowSharedOptions())
						require.NoError(t, err)
					}

					exclusive := newTestRowExclusiveOptions()
					exclusive.Policy = pb.WaitPolicy_FastFail
					_, err = s.Lock(ctx, table, newTestRows(3, 4), txnA, exclusive)
					require.NoError(t, err)
					_, err = s.Lock(ctx, table, newTestRows(5), txnA, exclusive)
					require.NoError(t, err)

					requireExactTxnTableBookkeeping(t, s, txnA, table, 5)
					requireExactMixedModeLockStore(t, s, table)
					if foreignHolder {
						require.NoError(t, s.Unlock(ctx, txnB, timestamp.Timestamp{}))
					}
					require.NoError(t, s.Unlock(ctx, txnA, timestamp.Timestamp{}))
				},
				func(c *Config) {
					c.MaxLockRowCount = 3
					c.MaxFixedSliceSize = 8
				},
			)
		})
	}
}

func TestRemoteMixedModeBudgetKeepsOwnerAndOriginExact(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1", "s2"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			owner, origin := services[0], services[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			const table = uint64(26712)
			_, err := owner.getLockTableWithCreate(ctx, 0, table, nil, pb.Sharding_None)
			require.NoError(t, err)

			txnA := []byte("remote-mixed-a")
			txnB := []byte("remote-mixed-b")
			_, err = origin.Lock(ctx, table, newTestRows(1, 2), txnA, newTestRowSharedOptions())
			require.NoError(t, err)
			_, err = origin.Lock(ctx, table, newTestRows(1), txnA, newTestRowSharedOptions())
			require.NoError(t, err)
			_, err = owner.Lock(ctx, table, newTestRows(1), txnB, newTestRowSharedOptions())
			require.NoError(t, err)

			exclusive := newTestRowExclusiveOptions()
			exclusive.Policy = pb.WaitPolicy_FastFail
			_, err = origin.Lock(ctx, table, newTestRows(3, 4), txnA, exclusive)
			require.NoError(t, err)
			_, err = origin.Lock(ctx, table, newTestRows(5), txnA, exclusive)
			require.NoError(t, err)

			// The authoritative owner reports that the repeated request added no
			// ownership, so the origin stays in lockstep instead of consuming another
			// fixed-slice slot. Both sides keep the table ineligible and exact.
			requireExactTxnTableBookkeeping(t, origin, txnA, table, 5)
			requireExactTxnTableBookkeeping(t, owner, txnA, table, 5)
			requireExactMixedModeLockStore(t, owner, table)
			require.NoError(t, owner.Unlock(ctx, txnB, timestamp.Timestamp{}))
			require.NoError(t, origin.Unlock(ctx, txnA, timestamp.Timestamp{}))
		},
		func(c *Config) {
			c.MaxLockRowCount = 3
			c.MaxFixedSliceSize = 8
		},
	)
}

func TestSharedBudgetPreservesCompatibility(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			s := services[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			const table = uint64(26706)
			txnA := []byte("shared-budget-a")
			txnB := []byte("shared-budget-b")
			shared := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Shared,
				Policy:      pb.WaitPolicy_FastFail,
			}

			_, err := s.Lock(ctx, table, newTestRows(1, 2, 5), txnA, shared)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, s.Unlock(ctx, txnA, timestamp.Timestamp{}))
			}()
			_, err = s.Lock(ctx, table, newTestRows(3, 4, 6), txnB, shared)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, s.Unlock(ctx, txnB, timestamp.Timestamp{}))
			}()

			// Crossing an internal representation budget cannot turn compatible
			// Shared row locking into a conflict or a synthetic wait-for edge.
			_, err = s.Lock(ctx, table, newTestRows(7), txnA, shared)
			require.NoError(t, err)
			_, err = s.Lock(ctx, table, newTestRows(0), txnB, shared)
			require.NoError(t, err)

			for _, txnID := range [][]byte{txnA, txnB} {
				txn := s.activeTxnHolder.getActiveTxn(txnID, false, "")
				require.NotNil(t, txn)
				txn.RLock()
				lockCount := txn.lockHolders[0].tableKeys[table].mustGet().len()
				txn.RUnlock()
				require.Equal(t, 4, lockCount)
			}
		},
		func(c *Config) {
			c.MaxLockRowCount = 3
			c.MaxFixedSliceSize = 8
		},
	)
}

func TestSharedBudgetWaitPolicyDoesNotWaitForCompatibleHolder(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			s := services[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			const table = uint64(26709)
			txnA := []byte("shared-wait-a")
			txnB := []byte("shared-wait-b")
			shared := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Shared,
				Policy:      pb.WaitPolicy_Wait,
			}

			_, err := s.Lock(ctx, table, newTestRows(1, 2, 5), txnA, shared)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, s.Unlock(ctx, txnA, timestamp.Timestamp{}))
			}()
			_, err = s.Lock(ctx, table, newTestRows(3), txnB, shared)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, s.Unlock(ctx, txnB, timestamp.Timestamp{}))
			}()

			requestCtx, requestCancel := context.WithTimeout(ctx, time.Second*2)
			defer requestCancel()
			_, err = s.Lock(requestCtx, table, newTestRows(6), txnA, shared)
			require.NoError(t, err)
		},
		func(c *Config) {
			c.MaxLockRowCount = 3
			c.MaxFixedSliceSize = 8
		},
	)
}

func TestConcurrentSharedRangeMergesRemainDeadlockDetectable(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			s := services[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			const table = uint64(26710)
			txnA := []byte("shared-merge-cycle-a")
			txnB := []byte("shared-merge-cycle-b")
			sharedRows := newTestRowSharedOptions()
			_, err := s.Lock(ctx, table, newTestRows(1, 2), txnA, sharedRows)
			require.NoError(t, err)
			_, err = s.Lock(ctx, table, newTestRows(1, 2), txnB, sharedRows)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, s.Unlock(ctx, txnA, timestamp.Timestamp{}))
				require.NoError(t, s.Unlock(ctx, txnB, timestamp.Timestamp{}))
			}()

			type result struct {
				txn string
				err error
			}
			results := make(chan result, 2)
			sharedRange := newTestRangeSharedOptions()
			go func() {
				_, lockErr := s.Lock(ctx, table, newTestRows(1, 4), txnA, sharedRange)
				results <- result{txn: "a", err: lockErr}
			}()
			waitWaiters(t, s, table, newTestRows(1)[0], 1)
			go func() {
				_, lockErr := s.Lock(ctx, table, newTestRows(1, 6), txnB, sharedRange)
				results <- result{txn: "b", err: lockErr}
			}()

			var victim result
			select {
			case victim = <-results:
			case <-time.After(time.Second * 3):
				t.Fatal("deadlock detector did not select a Shared range-merge victim")
			}
			require.ErrorIs(t, victim.err, ErrDeadLockDetected)
			victimID := txnA
			survivor := "b"
			if victim.txn == "b" {
				victimID = txnB
				survivor = "a"
			}
			require.NoError(t, s.Unlock(ctx, victimID, timestamp.Timestamp{}))

			select {
			case completed := <-results:
				require.Equal(t, survivor, completed.txn)
				require.NoError(t, completed.err)
			case <-time.After(time.Second * 3):
				t.Fatal("surviving Shared range merge did not progress after victim unlock")
			}
		},
		func(c *Config) {
			c.MaxLockRowCount = 3
			c.MaxFixedSliceSize = 8
		},
	)
}

func TestProxySharedRangeBypassesSingletonCache(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1", "s2"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			owner, caller := services[0], services[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			const table = uint64(26707)
			warmup := []byte("proxy-range-warmup")
			_, err := owner.Lock(ctx, table, newTestRows(9), warmup, newTestRowSharedOptions())
			require.NoError(t, err)
			require.NoError(t, owner.Unlock(ctx, warmup, timestamp.Timestamp{}))

			txnID := []byte("proxy-shared-range")
			opts := newTestRangeSharedOptions()
			_, err = caller.Lock(ctx, table, newTestRows(1, 4), txnID, opts)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, caller.Unlock(ctx, txnID, timestamp.Timestamp{}))
			}()
			_, ok := caller.tableGroups.get(0, table).(*localLockTableProxy)
			require.True(t, ok)
		},
		func(c *Config) {
			c.EnableRemoteLocalProxy = true
		},
	)
}

func TestProxyRepeatedSharedLockDoesNotDuplicateBookkeeping(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1", "s2"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			owner, caller := services[0], services[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			const table = uint64(26708)
			warmup := []byte("proxy-repeat-warmup")
			_, err := owner.Lock(ctx, table, newTestRows(9), warmup, newTestRowSharedOptions())
			require.NoError(t, err)
			require.NoError(t, owner.Unlock(ctx, warmup, timestamp.Timestamp{}))

			representativeID := []byte("proxy-repeat-representative")
			txnID := []byte("proxy-repeat-shared")
			rows := newTestRows(1)
			_, err = caller.Lock(ctx, table, rows, representativeID, newTestRowSharedOptions())
			require.NoError(t, err)
			defer func() {
				require.NoError(t, caller.Unlock(ctx, representativeID, timestamp.Timestamp{}))
			}()
			_, err = caller.Lock(ctx, table, rows, txnID, newTestRowSharedOptions())
			require.NoError(t, err)
			defer func() {
				require.NoError(t, caller.Unlock(ctx, txnID, timestamp.Timestamp{}))
			}()
			for range 3 {
				_, err = caller.Lock(ctx, table, rows, txnID, newTestRowSharedOptions())
				require.NoError(t, err)
			}

			txn := caller.activeTxnHolder.getActiveTxn(txnID, false, "")
			require.NotNil(t, txn)
			txn.RLock()
			lockCount := txn.lockHolders[0].tableKeys[table].mustGet().len()
			txn.RUnlock()
			proxy := caller.tableGroups.get(0, table).(*localLockTableProxy)
			proxy.mu.RLock()
			proxyHolderCount := len(proxy.mu.holders[string(rows[0])].txns)
			proxy.mu.RUnlock()
			require.Equal(t, 1, lockCount)
			require.Equal(t, 2, proxyHolderCount)
		},
		func(c *Config) {
			c.EnableRemoteLocalProxy = true
			c.MaxLockRowCount = 3
			c.MaxFixedSliceSize = 8
		},
	)
}
