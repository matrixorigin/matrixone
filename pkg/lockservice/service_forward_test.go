// Copyright 2023 Matrix Origin
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
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestForwardLock(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)

			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			_, err := l2.getLockTableWithCreate(0, tableID, nil, pb.Sharding_None)
			require.NoError(t, err)

			txn1 := []byte("txn1")
			row1 := []byte{1}

			_, err = l1.Lock(ctx, tableID, [][]byte{row1}, txn1, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
				ForwardTo:   "s2",
			})
			require.NoError(t, err)

			txn := l2.activeTxnHolder.getActiveTxn(txn1, false, "")
			require.NotNil(t, txn)
			require.Equal(t, l1.serviceID, txn.remoteService)
			require.True(t, l2.activeTxnHolder.hasRemoteLockBind(l1.serviceID, l2.tableGroups.get(0, tableID).getBind(), time.Second))
		},
	)
}

func TestForwardLockUsesEffectiveLockDeadline(t *testing.T) {
	holderTxn := []byte("holder")
	waiterTxn := []byte("waiter")
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1", "s2"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			origin := services[0]
			owner := services[1]
			const tableID = uint64(11)
			_, err := owner.getLockTableWithCreate(0, tableID, nil, pb.Sharding_None)
			require.NoError(t, err)

			options := newTestRowExclusiveOptions()
			options.ForwardTo = "s2"
			options.LockWaitTimeout = 1
			// Forwarded txns are normally created by the frontend txn lifecycle on
			// the origin CN. Register both here so owner-side orphan detection sees
			// the same liveness state as production while the waiter is blocked.
			origin.activeTxnHolder.getActiveTxn(holderTxn, true, "")
			origin.activeTxnHolder.getActiveTxn(waiterTxn, true, "")

			// A deadline-less background context previously reached morpc.Send
			// unchanged and panicked. Service entry must inject and propagate the
			// effective lock deadline to the forwarded RPC.
			_, err = origin.Lock(context.Background(), tableID, [][]byte{{1}}, holderTxn, options)
			require.NoError(t, err)

			start := time.Now()
			_, err = origin.Lock(context.Background(), tableID, [][]byte{{1}}, waiterTxn, options)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockWaitTimeout), "unexpected error: %v", err)
			require.GreaterOrEqual(t, time.Since(start), time.Second)
			require.Less(t, time.Since(start), 3*time.Second)
		},
		func(c *Config) {
			c.TxnIterFunc = func(fn func([]byte) bool) {
				if fn(holderTxn) {
					fn(waiterTxn)
				}
			}
		},
	)
}

func TestNewLockRPCContextPreservesEarlierParentDeadline(t *testing.T) {
	parent, cancelParent := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelParent()
	parentDeadline, ok := parent.Deadline()
	require.True(t, ok)

	rpcCtx, cancelRPC := newLockRPCContext(parent, pb.LockOptions{
		LockWaitTimeout:  60,
		LockWaitDeadline: time.Now().Add(time.Second).UnixNano(),
	})
	defer cancelRPC()
	rpcDeadline, ok := rpcCtx.Deadline()
	require.True(t, ok)
	require.Equal(t, parentDeadline, rpcDeadline)
}

func TestDeadLockWithForward(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			l2 := s[1]

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			row1 := []byte{1}
			row2 := []byte{2}

			mustAddTestLock(t, ctx, l1, 1, txn1, [][]byte{row1}, pb.Granularity_Row)
			_, err := l1.Lock(ctx, 1, [][]byte{row2}, txn2, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
				ForwardTo:   "s2",
			})
			require.NoError(t, err)

			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				maybeAddTestLockWithDeadlock(t, ctx, l1, 1, txn1, [][]byte{row2},
					pb.Granularity_Row)
				require.NoError(t, l1.Unlock(ctx, txn1, timestamp.Timestamp{}))
			}()
			go func() {
				defer wg.Done()
				maybeAddTestLockWithDeadlock(t, ctx, l2, 1, txn2, [][]byte{row1},
					pb.Granularity_Row)
				require.NoError(t, l2.Unlock(ctx, txn2, timestamp.Timestamp{}))
			}()
			wg.Wait()
		},
	)
}
