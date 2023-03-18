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

func TestLockAndUnlockOnRemote(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			option := LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			txn1 := []byte{1}
			txn2 := []byte{2}

			table1 := uint64(1)
			table2 := uint64(2)

			// make table1 on l1, table2 on l2
			_, err := l1.Lock(ctx, table1, [][]byte{{1}}, txn1, option)
			require.NoError(t, err)
			checkTxn(t, l1, txn1, "")
			checkTxnLocks(t, l1, txn1, table1, []byte{1})

			_, err = l2.Lock(ctx, table2, [][]byte{{2}}, txn2, option)
			require.NoError(t, err)
			checkTxn(t, l2, txn2, "")
			checkTxnLocks(t, l2, txn2, table2, []byte{2})

			// lock remote
			_, err = l1.Lock(ctx, table2, [][]byte{{3}}, txn1, option)
			require.NoError(t, err)
			checkTxn(t, l1, txn1, "")
			checkTxn(t, l2, txn1, "s1")
			checkTxnLocks(t, l1, txn1, table1, []byte{1})
			checkTxnLocks(t, l1, txn1, table2, []byte{3})

			_, err = l2.Lock(ctx, table1, [][]byte{{4}}, txn2, option)
			require.NoError(t, err)
			checkTxn(t, l2, txn2, "")
			checkTxn(t, l1, txn2, "s2")
			checkTxnLocks(t, l2, txn2, table2, []byte{2})
			checkTxnLocks(t, l2, txn2, table1, []byte{4})

			// unlock
			require.NoError(t, l1.Unlock(ctx, txn1, timestamp.Timestamp{}))
			require.NoError(t, l2.Unlock(ctx, txn2, timestamp.Timestamp{}))

			checkTxnNotExist(t, l1, txn1)
			checkTxnNotExist(t, l1, txn2)
			checkTxnNotExist(t, l2, txn1)
			checkTxnNotExist(t, l2, txn2)
			checkLockRemoved(t, l1, table1, [][]byte{{1}, {4}})
			checkLockRemoved(t, l2, table2, [][]byte{{2}, {3}})
		},
	)
}

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
			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			row1 := []byte{1}

			// txn1 hold lock row1 on l1
			mustAddTestLock(t, ctx, l1, 1, txn1, [][]byte{row1}, pb.Granularity_Row)
			c := make(chan struct{})
			go func() {
				// txn2 try lock row1 on l2
				mustAddTestLock(t, ctx, l2, 1, txn2, [][]byte{row1}, pb.Granularity_Row)
				close(c)
			}()
			waitWaiters(t, l1, 1, row1, 1)
			require.NoError(t, l1.Unlock(ctx, txn1, timestamp.Timestamp{}))
			<-c
		},
	)
}

func TestLockResultWithNoConfictOnRemote(t *testing.T) {
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

func TestLockResultWithConfictAndTxnCommittedOnRemote(t *testing.T) {
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
			option := LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			// txn1 hold lock row1 on l1
			mustAddTestLock(t, ctx, l1, 1, txn1, [][]byte{row1}, pb.Granularity_Row)
			c := make(chan struct{})
			go func() {
				defer close(c)
				// txn2 try lock row1 on l2
				// blocked by txn1
				res, err := l2.Lock(
					ctx,
					1,
					[][]byte{row1},
					txn2,
					option)
				require.NoError(t, err)
				assert.Equal(
					t,
					timestamp.Timestamp{PhysicalTime: 1},
					res.Timestamp)
			}()
			waitWaiters(t, l1, 1, row1, 1)
			require.NoError(t, l1.Unlock(
				ctx,
				txn1,
				timestamp.Timestamp{PhysicalTime: 1}))
			<-c
		},
	)
}

func TestLockResultWithConfictAndTxnAbortedOnRemote(t *testing.T) {
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
			option := LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			// txn1 hold lock row1 on l1
			mustAddTestLock(t, ctx, l1, 1, txn1, [][]byte{row1}, pb.Granularity_Row)
			c := make(chan struct{})
			go func() {
				defer close(c)
				// txn2 try lock row1 on l2
				// blocked by txn1
				res, err := l2.Lock(
					ctx,
					1,
					[][]byte{row1},
					txn2,
					option)
				require.NoError(t, err)
				assert.False(t, res.Timestamp.IsEmpty())
			}()
			waitWaiters(t, l1, 1, row1, 1)
			require.NoError(t, l1.Unlock(ctx, txn1, timestamp.Timestamp{}))
			<-c
		},
	)
}

func TestDeadlockOnRemote(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2", "s3"},
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			l2 := s[1]
			l3 := s[2]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			txn3 := []byte("txn3")
			row1 := []byte{1}
			row2 := []byte{2}
			row3 := []byte{3}

			// txn1 hold table1[row1] on l1
			mustAddTestLock(t, ctx, l1, 1, txn1, [][]byte{row1}, pb.Granularity_Row)
			// txn2 hold table2[row2] on l2
			mustAddTestLock(t, ctx, l2, 2, txn2, [][]byte{row2}, pb.Granularity_Row)
			// txn3 hold table3[row3] on l3
			mustAddTestLock(t, ctx, l3, 3, txn3, [][]byte{row3}, pb.Granularity_Row)

			var wg sync.WaitGroup
			wg.Add(3)
			go func() {
				defer wg.Done()
				// txn1 try lock tabl2[row2], txn1 wait txn2
				maybeAddTestLockWithDeadlock(t, ctx, l1, 2, txn1, [][]byte{row2},
					pb.Granularity_Row)
				require.NoError(t, l1.Unlock(ctx, txn1, timestamp.Timestamp{}))
			}()
			go func() {
				defer wg.Done()
				// txn2 try lock tabl3[row3], txn2 wait txn3
				maybeAddTestLockWithDeadlock(t, ctx, l2, 3, txn2, [][]byte{row3},
					pb.Granularity_Row)
				require.NoError(t, l2.Unlock(ctx, txn2, timestamp.Timestamp{}))
			}()
			go func() {
				defer wg.Done()
				// txn3 try lock tabl1[row1], txn3 wait txn1
				maybeAddTestLockWithDeadlock(t, ctx, l3, 1, txn3, [][]byte{row1},
					pb.Granularity_Row)
				require.NoError(t, l3.Unlock(ctx, txn3, timestamp.Timestamp{}))
			}()
			wg.Wait()
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

func TestLockWithBindIsStable(t *testing.T) {
	runBindChangedTests(
		t,
		true,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2 *service,
			table uint64) {

			txnID2 := []byte("txn2")
			_, err := l2.Lock(ctx, table, [][]byte{{3}}, txnID2, LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			})
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged))

			checkBind(
				t,
				pb.LockTable{ServiceID: "s1", Version: 2, Table: table, Valid: true},
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
				pb.LockTable{ServiceID: "s1", Version: 2, Table: table, Valid: true},
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
			lt.getLock(txnID2, []byte{1}, func(l Lock) {})

			checkBind(
				t,
				pb.LockTable{ServiceID: "s1", Version: 2, Table: table, Valid: true},
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

			waitBindDisabled(t, alloc, "s1")

			txnID2 := []byte("txn2")
			// l2 hold the old bind, and can not connect to s1, and wait bind changed
			for {
				_, err := l2.Lock(ctx, table, [][]byte{{3}}, txnID2, LockOptions{
					Granularity: pb.Granularity_Row,
					Mode:        pb.LockMode_Exclusive,
					Policy:      pb.WaitPolicy_Wait,
				})
				if err == nil {
					// l2 get the bind
					v, ok := l2.tables.Load(table)
					assert.True(t, ok)
					l := v.(lockTable)
					assert.Equal(t, "s2", l.getBind().ServiceID)
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

			waitBindDisabled(t, alloc, "s1")

			txnID2 := []byte("txn2")
			assert.NoError(t, l2.Unlock(ctx, txnID2, timestamp.Timestamp{}))
			// l2 get the bind
			v, ok := l2.tables.Load(table)
			assert.True(t, ok)
			l := v.(lockTable)
			assert.Equal(t, "s2", l.getBind().ServiceID)
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

			waitBindDisabled(t, alloc, "s1")

			txnID2 := []byte("txn2")
			lt, err := l2.getLockTable(table)
			require.NoError(t, err)
			lt.getLock(txnID2, []byte{1}, func(l Lock) {})
			// l2 get the bind
			v, ok := l2.tables.Load(table)
			assert.True(t, ok)
			l := v.(lockTable)
			assert.Equal(t, "s2", l.getBind().ServiceID)
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
			_, err := l2.Lock(ctx, table, [][]byte{{3}}, txnID2, LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			})
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableNotFound))

			checkBind(
				t,
				pb.LockTable{ServiceID: "s1", Version: 1, Table: table, Valid: true},
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
				pb.LockTable{ServiceID: "s1", Version: 1, Table: table, Valid: true},
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
			lt.getLock(txnID2, []byte{1}, func(l Lock) {})

			checkBind(
				t,
				pb.LockTable{ServiceID: "s1", Version: 1, Table: table, Valid: true},
				l2)
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
		time.Millisecond*200,
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10000)
			defer cancel()

			txnID1 := []byte("txn1")
			txnID2 := []byte("txn2")
			table1 := uint64(1)
			// make table bind on l1
			mustAddTestLock(t, ctx, l1, table1, txnID1, [][]byte{{1}}, pb.Granularity_Row)

			// l2 get the table1's bind
			mustAddTestLock(t, ctx, l2, table1, txnID2, [][]byte{{2}}, pb.Granularity_Row)
			v, err := l2.getLockTable(table1)
			require.NoError(t, err)
			require.Equal(t, "s1", v.getBind().ServiceID)

			if makeBindChanged {
				// stop l1 keep lock bind
				skip.Store(true)
				lt, err := l1.getLockTable(table1)
				require.NoError(t, err)
				old := lt.getBind()
				waitBindDisabled(t, alloc, "s1")
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
					req := m.(*pb.Request)
					if req.Method == pb.Method_KeepLockTableBind &&
						req.KeepLockTableBind.ServiceID == "s1" {
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
		lt, err := l.getLockTable(old.Table)
		require.NoError(t, err)
		new := lt.getBind()
		if new.Changed(old) {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func checkLockRemoved(
	t *testing.T,
	l *service,
	table uint64,
	locks [][]byte) {
	store, err := l.getLockTable(table)
	require.NoError(t, err)
	store.(*localLockTable).mu.Lock()
	defer store.(*localLockTable).mu.Unlock()
	for _, lock := range locks {
		_, ok := store.(*localLockTable).mu.store.Get(lock)
		require.False(t, ok)
	}
}

func checkTxnNotExist(
	t *testing.T,
	l *service,
	txnID []byte) {
	txn := l.activeTxnHolder.getActiveTxn(txnID, false, "")
	assert.Nil(t, txn)
}

func checkTxn(
	t *testing.T,
	l *service,
	txnID []byte,
	remote string) {
	txn := l.activeTxnHolder.getActiveTxn(txnID, false, "")
	assert.NotNil(t, txn)
	txn.Lock()
	defer txn.Unlock()
	assert.Equal(t, txn.txnID, txnID)
	assert.Equal(t, txn.remoteService, remote)
}

func checkTxnLocks(
	t *testing.T,
	l *service,
	txnID []byte,
	table uint64,
	locks ...[]byte) {
	txn := l.activeTxnHolder.getActiveTxn(txnID, false, "")
	assert.NotNil(t, txn)
	txn.Lock()
	defer txn.Unlock()
	assert.Equal(t, txn.txnID, txnID)
	sp := txn.holdLocks[table].slice()
	assert.Equal(t, len(locks), sp.len())
	var values [][]byte
	sp.iter(func(v []byte) bool {
		values = append(values, v)
		return true
	})
	assert.Equal(t, locks, values)
}

func checkBind(
	t *testing.T,
	bind pb.LockTable,
	s *service) {
	v, ok := s.tables.Load(bind.Table)
	assert.True(t, ok)
	l := v.(lockTable)
	assert.Equal(t, bind, l.getBind())
}
