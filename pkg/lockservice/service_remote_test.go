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
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnlockAfterTimeoutOnRemote(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1", "s2"},
		time.Second*10,
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txn1 := []byte{1}
			txn2 := []byte{2}
			table1 := uint64(1)

			// table1 on l1
			mustAddTestLock(t, ctx, l1, table1, txn1, [][]byte{{1}}, pb.Granularity_Row)

			// txn2 lock row 2 on remote.
			mustAddTestLock(t, ctx, l2, table1, txn2, [][]byte{{2}}, pb.Granularity_Row)
			// l2 shutdown
			assert.NoError(t, l2.Close())

			// wait until txn2 unlocked
			for {
				txn := l1.activeTxnHolder.getActiveTxn(txn2, false, "")
				if txn == nil {
					return
				}
				time.Sleep(time.Millisecond * 100)
			}
		},
		func(c *Config) {
			c.RemoteLockTimeout.Duration = time.Second
			c.KeepRemoteLockDuration.Duration = time.Millisecond * 100
		},
	)
}

func TestLockBlockedOnRemote(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)

			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			row1 := []byte{1}

			// txn1 hold lock row1 on l1
			mustAddTestLock(t, ctx, l1, tableID, txn1, [][]byte{row1}, pb.Granularity_Row)
			c := make(chan struct{})
			go func() {
				// txn2 try lock row1 on l2
				mustAddTestLock(t, ctx, l2, tableID, txn2, [][]byte{row1}, pb.Granularity_Row)
				close(c)
			}()
			waitWaiters(t, l1, tableID, row1, 1)
			require.NoError(t, l1.Unlock(ctx, txn1, timestamp.Timestamp{}))
			<-c
		},
	)
}

func TestLockResultWithNoConflictOnRemote(t *testing.T) {
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

			// txn1 hold lock row1 on l1
			mustAddTestLock(t, ctx, l1, 1, txn1, [][]byte{row1}, pb.Granularity_Row)
			c := make(chan struct{})
			go func() {
				// txn2 try lock row1 on l2
				res := mustAddTestLock(t, ctx, l2, 1, txn2, [][]byte{row2}, pb.Granularity_Row)
				require.False(t, res.Timestamp.IsEmpty())
				close(c)
			}()
			<-c
		},
	)
}

func TestLockResultWithConflictAndTxnCommittedOnRemote(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)

			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			row1 := []byte{1}
			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			// txn1 hold lock row1 on l1
			mustAddTestLock(t, ctx, l1, tableID, txn1, [][]byte{row1}, pb.Granularity_Row)
			c := make(chan struct{})
			go func() {
				defer close(c)
				// txn2 try lock row1 on l2
				// blocked by txn1
				res, err := l2.Lock(
					ctx,
					tableID,
					[][]byte{row1},
					txn2,
					option)
				require.NoError(t, err)
				assert.True(
					t,
					!res.Timestamp.IsEmpty())
			}()
			waitWaiters(t, l1, tableID, row1, 1)
			require.NoError(t, l1.Unlock(
				ctx,
				txn1,
				timestamp.Timestamp{PhysicalTime: 1}))
			<-c
		},
	)
}

func TestLockResultWithConflictAndTxnAbortedOnRemote(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)

			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			row1 := []byte{1}
			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			// txn1 hold lock row1 on l1
			mustAddTestLock(t, ctx, l1, tableID, txn1, [][]byte{row1}, pb.Granularity_Row)
			c := make(chan struct{})
			go func() {
				defer close(c)
				// txn2 try lock row1 on l2
				// blocked by txn1
				res, err := l2.Lock(
					ctx,
					tableID,
					[][]byte{row1},
					txn2,
					option)
				require.NoError(t, err)
				assert.False(t, res.Timestamp.IsEmpty())
			}()
			waitWaiters(t, l1, tableID, row1, 1)
			require.NoError(t, l1.Unlock(ctx, txn1, timestamp.Timestamp{}))
			<-c
		},
	)
}

func TestGetActiveTxnWithRemote(t *testing.T) {
	hold := newMapBasedTxnHandler(
		"s1",
		newFixedSlicePool(16)).(*mapBasedTxnHolder)

	txnID := []byte("txn1")
	st := time.Now()
	txn := hold.getActiveTxn(txnID, true, "s1")
	assert.NotNil(t, txn)
	assert.Equal(t, "s1", txn.remoteService)
	assert.Equal(t, 1, len(hold.mu.remoteServices))
	assert.Equal(t, 1, hold.mu.dequeue.Len())
	e := hold.mu.dequeue.PopFront()
	assert.Equal(t, "s1", e.Value.id)
	assert.True(t, e.Value.time.After(st))
}

func TestGetTimeoutRemoveTxn(t *testing.T) {
	hold := newMapBasedTxnHandler(
		"s1",
		newFixedSlicePool(16)).(*mapBasedTxnHolder)

	txnID1 := []byte("txn1")
	hold.getActiveTxn(txnID1, true, "s1")
	txnID2 := []byte("txn2")
	hold.getActiveTxn(txnID2, true, "s2")

	// s1(now-10s), s2(now-5s)
	now := time.Now()
	hold.mu.remoteServices["s1"].Value.time = now.Add(-time.Second * 10)
	hold.mu.remoteServices["s2"].Value.time = now.Add(-time.Second * 5)

	txns, wait := hold.getTimeoutRemoveTxn(make(map[string]struct{}), nil, time.Second*20)
	assert.Equal(t, 0, len(txns))
	assert.NotEqual(t, time.Duration(0), wait)

	// s1 timeout
	txns, wait = hold.getTimeoutRemoveTxn(make(map[string]struct{}), nil, time.Second*8)
	assert.Equal(t, 1, len(txns))
	assert.NotEqual(t, time.Duration(0), wait)
	assert.Equal(t, txnID1, txns[0])

	// s2 timeout
	txns, wait = hold.getTimeoutRemoveTxn(make(map[string]struct{}), nil, time.Second*2)
	assert.Equal(t, 1, len(txns))
	assert.Equal(t, time.Duration(0), wait)
	assert.Equal(t, txnID2, txns[0])
}

func TestKeepRemoteActiveTxn(t *testing.T) {
	hold := newMapBasedTxnHandler(
		"s1",
		newFixedSlicePool(16)).(*mapBasedTxnHolder)

	txnID1 := []byte("txn1")
	txnID2 := []byte("txn2")
	hold.getActiveTxn(txnID1, true, "s1")
	hold.getActiveTxn(txnID2, true, "s2")
	var ids []string
	hold.mu.dequeue.Iter(0, func(r remote) bool {
		ids = append(ids, r.id)
		return true
	})
	assert.Equal(t, []string{"s1", "s2"}, ids)

	hold.keepRemoteActiveTxn("s1")
	ids = ids[:0]
	hold.mu.dequeue.Iter(0, func(r remote) bool {
		ids = append(ids, r.id)
		return true
	})
	assert.Equal(t, []string{"s2", "s1"}, ids)
}

func TestLockWithBindIsStale(t *testing.T) {
	runBindChangedTests(
		t,
		true,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2 *service,
			table uint64) {
			txnID2 := []byte("txn2")
			_, err := l2.Lock(ctx, table, [][]byte{{3}}, txnID2, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			})
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged) ||
				moerr.IsMoErrCode(err, moerr.ErrLockTableNotFound))

			checkBind(
				t,
				pb.LockTable{ServiceID: l1.serviceID, Version: 2, Table: table, Valid: true},
				l2)
		},
	)
}

func TestUnlockWithBindIsStable(t *testing.T) {
	runBindChangedTests(
		t,
		true,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2 *service,
			table uint64) {

			txnID2 := []byte("txn2")
			l2.Unlock(ctx, txnID2, timestamp.Timestamp{})

			checkBind(
				t,
				pb.LockTable{ServiceID: l1.serviceID, Version: 2, Table: table, Valid: true},
				l2)
		},
	)
}

func TestGetLockWithBindIsStable(t *testing.T) {
	runBindChangedTests(
		t,
		true,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2 *service,
			table uint64) {

			txnID2 := []byte("txn2")
			lt, err := l2.getLockTable(table)
			require.NoError(t, err)
			lt.getLock(txnID2, pb.WaitTxn{TxnID: []byte{1}}, func(l Lock) {})

			checkBind(
				t,
				pb.LockTable{ServiceID: l1.serviceID, Version: 2, Table: table, Valid: true},
				l2)
		},
	)
}

func TestLockWithBindTimeout(t *testing.T) {
	runBindChangedTests(
		t,
		false,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2 *service,
			table uint64) {
			// stop l1 let old bind invalid
			require.NoError(t, l1.Close())

			waitBindDisabled(t, alloc, l1.serviceID)

			txnID2 := []byte("txn2")
			// l2 hold the old bind, and can not connect to s1, and wait bind changed
			for {
				_, err := l2.Lock(ctx, table, [][]byte{{3}}, txnID2, pb.LockOptions{
					Granularity: pb.Granularity_Row,
					Mode:        pb.LockMode_Exclusive,
					Policy:      pb.WaitPolicy_Wait,
				})
				if err == nil {
					// l2 get the bind
					v := l2.tables.get(table)
					assert.Equal(t, l2.serviceID, v.getBind().ServiceID)
					return
				}
				time.Sleep(time.Millisecond * 100)
			}
		},
	)
}

func TestUnlockWithBindTimeout(t *testing.T) {
	runBindChangedTests(
		t,
		false,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2 *service,
			table uint64) {
			// stop l1 let old bind invalid
			require.NoError(t, l1.Close())

			waitBindDisabled(t, alloc, l1.serviceID)

			txnID2 := []byte("txn2")
			assert.NoError(t, l2.Unlock(ctx, txnID2, timestamp.Timestamp{}))
			// l2 get the bind
			v := l2.tables.get(table)
			assert.Equal(t, l2.serviceID, v.getBind().ServiceID)
		},
	)
}

func TestGetLockWithBindTimeout(t *testing.T) {
	runBindChangedTests(
		t,
		false,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2 *service,
			table uint64) {
			// stop l1 let old bind invalid
			require.NoError(t, l1.Close())

			waitBindDisabled(t, alloc, l1.serviceID)

			txnID2 := []byte("txn2")
			lt, err := l2.getLockTable(table)
			require.NoError(t, err)
			lt.getLock(txnID2, pb.WaitTxn{TxnID: []byte{1}}, func(l Lock) {})
			// l2 get the bind
			v := l2.tables.get(table)
			assert.Equal(t, l2.serviceID, v.getBind().ServiceID)
		},
	)
}

func TestLockWithBindNotFound(t *testing.T) {
	runBindChangedTests(
		t,
		false,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2 *service,
			table uint64) {

			// change l2's bind to s3, no bind in s3
			l2.handleBindChanged(pb.LockTable{Table: table, ServiceID: "s3", Valid: true, Version: 1})

			txnID2 := []byte("txn2")
			_, err := l2.Lock(ctx, table, [][]byte{{3}}, txnID2, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			})
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableNotFound))

			checkBind(
				t,
				pb.LockTable{ServiceID: l1.serviceID, Version: 1, Table: table, Valid: true},
				l2)
		},
	)
}

func TestUnlockWithBindNotFound(t *testing.T) {
	runBindChangedTests(
		t,
		false,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2 *service,
			table uint64) {

			// change l2's bind to s3, no bind in s3
			l2.handleBindChanged(pb.LockTable{Table: table, ServiceID: "s3", Valid: true, Version: 1})

			txnID2 := []byte("txn2")
			l2.Unlock(ctx, txnID2, timestamp.Timestamp{})

			checkBind(
				t,
				pb.LockTable{ServiceID: l1.serviceID, Version: 1, Table: table, Valid: true},
				l2)
		},
	)
}

func TestGetLockWithBindNotFound(t *testing.T) {
	runBindChangedTests(
		t,
		false,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2 *service,
			table uint64) {

			// change l2's bind to s3, no bind in s3
			l2.handleBindChanged(pb.LockTable{Table: table, ServiceID: "s3", Valid: true, Version: 1})

			txnID2 := []byte("txn2")
			lt, err := l2.getLockTable(table)
			require.NoError(t, err)
			lt.getLock(txnID2, pb.WaitTxn{TxnID: []byte{1}}, func(l Lock) {})

			checkBind(
				t,
				pb.LockTable{ServiceID: l1.serviceID, Version: 1, Table: table, Valid: true},
				l2)
		},
	)
}

func TestIssue12554(t *testing.T) {
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
			table := uint64(10)

			// txn1 hold lock row1 on l1
			mustAddTestLock(t, ctx, l1, table, txn1, [][]byte{row1}, pb.Granularity_Row)

			oldBind := alloc.Get(l1.serviceID, table)
			// mock l1 restart, changed serviceID
			l1.serviceID = getServiceIdentifier("s1", time.Now().UnixNano())
			l1.tables.removeWithFilter(func(u uint64, lt lockTable) bool { return u == table })
			newLockTable := l1.createLockTableByBind(oldBind)
			l1.tables.set(table, newLockTable)

			_, err := l2.Lock(ctx, table, [][]byte{row1}, txn2, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			})
			assert.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))
		},
	)
}

func runBindChangedTests(
	t *testing.T,
	makeBindChanged bool,
	fn func(
		ctx context.Context,
		alloc *lockTableAllocator,
		l1, l2 *service,
		table uint64)) {
	var skip atomic.Bool
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1", "s2", "s3"},
		time.Second,
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txnID1 := []byte("txn1")
			txnID2 := []byte("txn2")
			table1 := uint64(10)
			// make table bind on l1
			mustAddTestLock(t, ctx, l1, table1, txnID1, [][]byte{{1}}, pb.Granularity_Row)

			// l2 get the table1's bind
			mustAddTestLock(t, ctx, l2, table1, txnID2, [][]byte{{2}}, pb.Granularity_Row)
			v, err := l2.getLockTable(table1)
			require.NoError(t, err)
			require.Equal(t, l1.serviceID, v.getBind().ServiceID)

			if makeBindChanged {
				// stop l1 keep lock bind
				skip.Store(true)
				lt, err := l1.getLockTable(table1)
				require.NoError(t, err)
				old := lt.getBind()
				waitBindDisabled(t, alloc, l1.serviceID)
				skip.Store(false)

				// make l1 get bind again, but version is changed
				waitBindChanged(t, old, l1)
			}

			fn(ctx, alloc, l1, l2, table1)
		},
		func(c *Config) {
			c.KeepBindDuration.Duration = time.Millisecond * 50
			c.KeepRemoteLockDuration.Duration = time.Millisecond * 50

			c.RPC.BackendOptions = append(c.RPC.BackendOptions,
				morpc.WithBackendFilter(func(m morpc.Message, s string) bool {
					if req, ok := m.(*pb.Request); ok && req.Method == pb.Method_KeepLockTableBind &&
						getUUIDFromServiceIdentifier(req.KeepLockTableBind.ServiceID) == "s1" {
						return !skip.Load()
					}
					return true
				}))
		},
	)
}

func waitBindDisabled(t *testing.T, alloc *lockTableAllocator, sid string) {
	for {
		b := alloc.getServiceBinds(sid)
		if b == nil {
			return
		}
		b.RLock()
		disabled := b.disabled
		b.RUnlock()
		if disabled {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func waitBindChanged(
	t *testing.T,
	old pb.LockTable,
	l *service) {
	for {
		lt, err := l.getLockTableWithCreate(old.Table, true)
		require.NoError(t, err)
		new := lt.getBind()
		if new.Changed(old) {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func checkBind(
	t *testing.T,
	bind pb.LockTable,
	s *service) {
	v := s.tables.get(bind.Table)
	assert.Equal(t, bind, v.getBind())
}
