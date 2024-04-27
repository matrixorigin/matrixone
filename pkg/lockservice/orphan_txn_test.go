// Copyright 2024 Matrix Origin
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

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestUnlockOrphanTxn(t *testing.T) {
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

func TestCannotUnlockOrphanTxnWithCommunicationInterruption(t *testing.T) {
	var pause atomic.Bool
	remoteLockTimeout := time.Second
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

			// skip all keep remote lock request and valid service request
			pause.Store(true)

			time.Sleep(remoteLockTimeout * 2)
			txn := l1.activeTxnHolder.getActiveTxn(txn2, false, "")
			assert.NotNil(t, txn)
		},
		func(c *Config) {
			c.RemoteLockTimeout.Duration = remoteLockTimeout
			c.KeepRemoteLockDuration.Duration = time.Millisecond * 100
			c.RPC.BackendOptions = append(c.RPC.BackendOptions,
				morpc.WithBackendFilter(
					func(req morpc.Message, _ string) bool {
						if m, ok := req.(*pb.Request); ok {
							skip := pause.Load() &&
								(m.Method == pb.Method_KeepRemoteLock ||
									m.Method == pb.Method_ValidateService)
							return !skip
						}
						return true
					}))
		},
	)
}

func TestCannotUnlockOrphanTxnWithCommittingInAllocator(t *testing.T) {
	txn1 := []byte{1}
	txn2 := []byte{2}
	activeTxns := [][]byte{txn1, txn2}

	remoteLockTimeout := time.Second
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1", "s2"},
		time.Second*10,
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			l2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10000)
			defer cancel()

			txn1 := []byte{1}
			txn2 := []byte{2}
			table1 := uint64(1)

			alloc.getCtl(l1.GetServiceID()).add(string(txn1), committingState)
			alloc.getCtl(l2.GetServiceID()).add(string(txn2), committingState)

			// table1 on l1
			mustAddTestLock(t, ctx, l1, table1, txn1, [][]byte{{1}}, pb.Granularity_Row)
			// txn2 lock row 2 on remote.
			mustAddTestLock(t, ctx, l2, table1, txn2, [][]byte{{2}}, pb.Granularity_Row)

			require.NoError(t, l2.Close())

			time.Sleep(remoteLockTimeout * 2)
			txn := l1.activeTxnHolder.getActiveTxn(txn2, false, "")
			assert.NotNil(t, txn)
		},
		func(c *Config) {
			c.RemoteLockTimeout.Duration = remoteLockTimeout
			c.KeepRemoteLockDuration.Duration = time.Millisecond * 100
			c.TxnIterFunc = func(f func([]byte) bool) {
				for _, txn := range activeTxns {
					if !f(txn) {
						return
					}
				}
			}
		},
	)
}

func TestUnlockOrphanTxnWithServiceRestart(t *testing.T) {
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

			// l2 restart
			assert.NoError(t, l2.Close())
			cfg := l2.cfg
			l22 := NewLockService(cfg)
			defer l22.Close()

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

func TestGetTimeoutRemoveTxn(t *testing.T) {
	hold := newMapBasedTxnHandler(
		"s1",
		newFixedSlicePool(16),
		func(sid string) (bool, error) { return false, nil },
		func(ot []pb.OrphanTxn) ([][]byte, error) { return nil, nil },
		func(txn pb.WaitTxn) (bool, error) { return true, nil },
	).(*mapBasedTxnHolder)

	txnID1 := []byte("txn1")
	hold.getActiveTxn(txnID1, true, "s1")
	txnID2 := []byte("txn2")
	hold.getActiveTxn(txnID2, true, "s2")

	// s1(now-10s), s2(now-5s)
	now := time.Now()
	hold.mu.remoteServices["s1"].Value.time = now.Add(-time.Second * 10)
	hold.mu.remoteServices["s2"].Value.time = now.Add(-time.Second * 5)

	txns := hold.getTimeoutRemoveTxn(make(map[string]struct{}), nil, time.Second*20)
	assert.Equal(t, 0, len(txns))

	// s1 timeout
	txns = hold.getTimeoutRemoveTxn(make(map[string]struct{}), nil, time.Second*8)
	assert.Equal(t, 1, len(txns))
	hold.mu.RLock()
	assert.Equal(t, 1, hold.mu.dequeue.Len())
	assert.Equal(t, 1, len(hold.mu.remoteServices))
	hold.mu.RUnlock()

	// s2 timeout
	txns = hold.getTimeoutRemoveTxn(make(map[string]struct{}), nil, time.Second*2)
	assert.Equal(t, 1, len(txns))
	hold.mu.RLock()
	assert.Equal(t, 0, hold.mu.dequeue.Len())
	assert.Equal(t, 0, len(hold.mu.remoteServices))
	hold.mu.RUnlock()
}

func TestGetTimeoutRemoveTxnWithValid(t *testing.T) {
	hold := newMapBasedTxnHandler(
		"s1",
		newFixedSlicePool(16),
		func(sid string) (bool, error) { return sid == "s1", nil },
		func(ot []pb.OrphanTxn) ([][]byte, error) { return nil, nil },
		func(txn pb.WaitTxn) (bool, error) { return true, nil },
	).(*mapBasedTxnHolder)

	txnID1 := []byte("txn1")
	hold.getActiveTxn(txnID1, true, "s1")
	txnID2 := []byte("txn2")
	hold.getActiveTxn(txnID2, true, "s2")

	// s1(now-10s), s2(now-5s)
	now := time.Now()
	hold.mu.remoteServices["s1"].Value.time = now.Add(-time.Second * 10)
	hold.mu.remoteServices["s2"].Value.time = now.Add(-time.Second * 5)

	txns := hold.getTimeoutRemoveTxn(make(map[string]struct{}), nil, time.Second*20)
	assert.Equal(t, 0, len(txns))
	hold.mu.RLock()
	assert.Equal(t, 2, hold.mu.dequeue.Len())
	assert.Equal(t, 2, len(hold.mu.remoteServices))
	hold.mu.RUnlock()

	// s1 timeout
	txns = hold.getTimeoutRemoveTxn(make(map[string]struct{}), nil, time.Second*8)
	assert.Equal(t, 0, len(txns))
	hold.mu.RLock()
	assert.Equal(t, 2, hold.mu.dequeue.Len())
	assert.Equal(t, 2, len(hold.mu.remoteServices))
	hold.mu.RUnlock()

	// s2 timeout
	txns = hold.getTimeoutRemoveTxn(make(map[string]struct{}), nil, time.Second*2)
	assert.Equal(t, 1, len(txns))
	hold.mu.RLock()
	assert.Equal(t, 1, hold.mu.dequeue.Len())
	assert.Equal(t, 1, len(hold.mu.remoteServices))
	hold.mu.RUnlock()
}

func TestGetTimeoutRemoveTxnWithValidErrorAndNotifyOK(t *testing.T) {
	hold := newMapBasedTxnHandler(
		"s1",
		newFixedSlicePool(16),
		func(sid string) (bool, error) { return false, ErrTxnNotFound },
		func(ot []pb.OrphanTxn) ([][]byte, error) { return nil, nil },
		func(txn pb.WaitTxn) (bool, error) { return true, nil },
	).(*mapBasedTxnHolder)

	txnID1 := []byte("txn1")
	hold.getActiveTxn(txnID1, true, "s1")

	// s1(now-10s)
	now := time.Now()
	hold.mu.remoteServices["s1"].Value.time = now.Add(-time.Second * 10)

	// s1 timeout
	txns := hold.getTimeoutRemoveTxn(make(map[string]struct{}), nil, time.Second*8)
	assert.Equal(t, 1, len(txns))
	hold.mu.RLock()
	assert.Equal(t, 0, hold.mu.dequeue.Len())
	assert.Equal(t, 0, len(hold.mu.remoteServices))
	hold.mu.RUnlock()
}

func TestGetTimeoutRemoveTxnWithValidErrorAndNotifyFailed(t *testing.T) {
	hold := newMapBasedTxnHandler(
		"s1",
		newFixedSlicePool(16),
		func(sid string) (bool, error) { return false, ErrTxnNotFound },
		func(ot []pb.OrphanTxn) ([][]byte, error) { return nil, ErrTxnNotFound },
		func(txn pb.WaitTxn) (bool, error) { return true, nil },
	).(*mapBasedTxnHolder)

	txnID1 := []byte("txn1")
	hold.getActiveTxn(txnID1, true, "s1")

	// s1(now-10s)
	now := time.Now()
	hold.mu.remoteServices["s1"].Value.time = now.Add(-time.Second * 10)

	// s1 timeout
	txns := hold.getTimeoutRemoveTxn(make(map[string]struct{}), nil, time.Second*8)
	assert.Equal(t, 0, len(txns))
	hold.mu.RLock()
	assert.Equal(t, 1, hold.mu.dequeue.Len())
	assert.Equal(t, 1, len(hold.mu.remoteServices))
	hold.mu.RUnlock()
}

func TestCannotCommitTxnCanBeRemovedWithNotInActiveTxn(t *testing.T) {
	var mu sync.Mutex
	var actives [][]byte

	fn := func(f func(txn []byte) bool) {
		mu.Lock()
		defer mu.Unlock()
		for _, txn := range actives {
			if !f(txn) {
				return
			}
		}
	}

	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1", "s2"},
		time.Millisecond*20,
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			mu.Lock()
			actives = append(actives, []byte{1}, []byte{2})
			mu.Unlock()

			alloc.AddCannotCommit([]pb.OrphanTxn{
				{
					Service: l1.serviceID,
					Txn:     [][]byte{{1}, {3}},
				},
			})

			// wait txn 3 removed
			for {
				v, ok := alloc.ctl.Load(l1.serviceID)
				require.True(t, ok)

				c := v.(*commitCtl)
				_, ok = c.states.Load(string([]byte{1}))
				n := 0
				c.states.Range(func(key, value any) bool {
					n++
					return true
				})
				if n == 1 && ok {
					return
				}
				time.Sleep(time.Millisecond * 10)
			}
		},
		func(c *Config) {
			c.TxnIterFunc = fn
		},
	)
}

func TestCannotCommitTxnCanBeRemovedWithRestart(t *testing.T) {
	var mu sync.Mutex
	var actives [][]byte

	fn := func(f func(txn []byte) bool) {
		mu.Lock()
		defer mu.Unlock()
		for _, txn := range actives {
			if !f(txn) {
				return
			}
		}
	}

	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1", "s2"},
		time.Millisecond*20,
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			mu.Lock()
			actives = append(actives, []byte{1}, []byte{3})
			mu.Unlock()

			alloc.AddCannotCommit([]pb.OrphanTxn{
				{
					Service: l1.serviceID,
					Txn:     [][]byte{{1}, {3}},
				},
			})

			cfg := l1.GetConfig()
			require.NoError(t, l1.Close())
			l1 = NewLockService(cfg).(*service)
			defer func() {
				l1.Close()
			}()

			// wait txn 3 removed
			for {
				n := 0
				alloc.ctl.Range(func(key, value any) bool {
					n++
					return true
				})

				if n == 0 {
					return
				}
				time.Sleep(time.Millisecond * 10)
			}
		},
		func(c *Config) {
			c.TxnIterFunc = fn
			c.removeDisconnectDuration = time.Millisecond * 50
		},
	)
}

func TestCannotHungWithUnstableNetwork(t *testing.T) {
	runLockServiceTestsWithLevel(
		t,
		zapcore.DebugLevel,
		[]string{"s1", "s2", "s3"},
		time.Second*10,
		func(alloc *lockTableAllocator, services []*service) {
			s1 := services[0]
			s2 := services[1]
			s3 := services[2]

			tableID := uint64(10)
			rows := newTestRows(1)
			txn1 := newTestTxnID(1)
			opts := newTestRowExclusiveOptions()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			_, err := s1.Lock(ctx, tableID, rows, txn1, opts)
			require.NoError(t, err)
			require.NoError(t, s1.Unlock(ctx, txn1, timestamp.Timestamp{}))

			var wg sync.WaitGroup
			n := 10000
			fn := func(s *service, idx int) {
				defer wg.Done()
				for i := 0; i < n; i++ {
					txnID := append([]byte{byte(idx)}, buf.Int2Bytes(i)...)
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
					s.Lock(ctx, tableID, rows, txnID, opts)
					require.NoError(t, s.Unlock(ctx, txnID, timestamp.Timestamp{}))
					cancel()
				}
			}

			wg.Add(3)
			go fn(s1, 0)
			go fn(s2, 1)
			go fn(s3, 2)

			wg.Wait()
		},
		func(c *Config) {
			// close the connection after every 5 messages received
			c.disconnectAfterRead = 5
			c.RemoteLockTimeout.Duration = time.Millisecond * 200
		})
}

func TestOrphanTxnHolderCanBeRelease(t *testing.T) {
	ch := make(chan struct{})
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second*5)
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			s1 := s[0]
			s2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()

			txn1 := []byte("txn1")
			txn2 := []byte("txn2")
			txn3 := []byte("txn3")
			table := uint64(10)
			rows := newTestRows(1)

			mustAddTestLock(t, ctx, s1, table, txn1, rows, pb.Granularity_Row)
			require.NoError(t, s1.Unlock(ctx, txn1, timestamp.Timestamp{}))

			_, err := s2.Lock(ctx2, table, rows, txn2, newTestRowExclusiveOptions())
			require.Error(t, err)

			// make unlock use new connection
			client := s2.remote.client.(*client)
			require.NoError(t, client.client.CloseBackend())
			require.NoError(t, s2.Unlock(ctx, txn2, timestamp.Timestamp{}))
			close(ch)

			v, err := s1.getLockTable(0, table)
			require.NoError(t, err)
			lt := v.(*localLockTable)

		OUT:
			for {
				select {
				case <-ctx.Done():
					t.Fail()
					return
				default:
					lt.mu.RLock()
					_, ok := lt.mu.store.Get(rows[0])
					lt.mu.RUnlock()
					if ok {
						break OUT
					}
				}
				time.Sleep(time.Millisecond * 10)
			}

			mustAddTestLock(t, ctx, s1, table, txn3, rows, pb.Granularity_Row)
			require.NoError(t, s1.Unlock(ctx, txn3, timestamp.Timestamp{}))
		},
		func(s *service) {
			s.option.serverOpts = append(s.option.serverOpts,
				WithServerMessageFilter(func(r *pb.Request) bool {
					// make handle lock after unlock
					if r.Method == pb.Method_Lock {
						cancel2()
						<-ch
					}
					return true
				}))
		},
	)
}
