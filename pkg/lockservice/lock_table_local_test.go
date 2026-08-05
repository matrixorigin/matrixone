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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloseLocalLockTable(t *testing.T) {
	table := uint64(10)
	getRunner(false)(
		t,
		table,
		func(ctx context.Context, s *service, lt *localLockTable) {
			rows := newTestRows(1)
			txnID := newTestTxnID(1)
			mustAddTestLock(
				t,
				ctx,
				s,
				table,
				txnID,
				rows,
				pb.Granularity_Row)
			lt.close(closeReasonServiceClose)
			lt.mu.Lock()
			defer lt.mu.Unlock()
			assert.True(t, lt.mu.closed)
			assert.Equal(t, 0, lt.mu.store.Len())
		},
	)
}

func TestCloseLocalLockTableNotifiesOutsideTableMutex(t *testing.T) {
	runtime.RunTest(
		"",
		func(rt runtime.Runtime) {
			reuse.RunReuseTests(func() {
				logger := getLogger("")
				events := newWaiterEvents(1, nil, nil, time.Hour, nil, logger)
				// A one-element queue makes the old close -> eventC -> worker ->
				// table mutex cycle deterministic with only three waiters.
				events.eventC = make(chan *lockContext, 1)
				events.start()

				lt := newLocalLockTable(
					pb.LockTable{Table: 1, ServiceID: "test"},
					nil,
					events,
					rt.Clock(),
					nil,
					logger,
				).(*localLockTable)

				lock := newRowLock(logger, &lockContext{
					waitTxn: pb.WaitTxn{TxnID: []byte("holder")},
					opts: LockOptions{LockOptions: pb.LockOptions{
						Mode: pb.LockMode_Exclusive,
					}},
				})
				handled := make(chan struct{}, 3)
				waiters := make([]*waiter, 0, 3)
				for i := 0; i < 3; i++ {
					w := acquireWaiter(
						pb.WaitTxn{TxnID: []byte(fmt.Sprintf("waiter-%d", i))},
						"test",
						logger,
					)
					w.setStatus(blocking)
					w.event = event{
						c: &lockContext{
							txn: &activeTxn{RWMutex: &sync.RWMutex{}},
							lockFunc: func(_ *lockContext, _ bool) {
								// The real terminal async path re-enters l.mu while
								// removing the notified waiter.
								lt.mu.Lock()
								lt.mu.Unlock()
								handled <- struct{}{}
							},
						},
						eventC: events.eventC,
					}
					lock.addWaiter(logger, w)
					waiters = append(waiters, w)
				}
				lt.mu.store.Add([]byte{1}, lock)

				closed := make(chan struct{})
				go func() {
					lt.close(closeReasonServiceClose)
					close(closed)
				}()
				select {
				case <-closed:
				case <-time.After(5 * time.Second):
					t.Fatal("local lock table close deadlocked with a full waiter event queue")
				}
				for _, w := range waiters {
					require.ErrorIs(t, w.mustRecvNotification(logger).err, ErrLockTableNotFound)
					w.close("test", logger)
				}

				events.close()
				require.Len(t, handled, 3)
				lt.mu.RLock()
				require.True(t, lt.mu.closed)
				require.Zero(t, lt.mu.store.Len())
				lt.mu.RUnlock()
			})
		},
	)
}

func TestRemoveLocalLockTableClosesOutsideHolderMutex(t *testing.T) {
	runtime.RunTest(
		"",
		func(rt runtime.Runtime) {
			reuse.RunReuseTests(func() {
				logger := getLogger("")
				events := newWaiterEvents(1, nil, nil, time.Hour, nil, logger)
				events.eventC = make(chan *lockContext, 1)
				events.start()

				lt := newLocalLockTable(
					pb.LockTable{Table: 1, ServiceID: "test"},
					nil,
					events,
					rt.Clock(),
					nil,
					logger,
				).(*localLockTable)
				holder := &lockTableHolder{
					tables: map[uint64]lockTable{1: lt},
				}
				holders := &lockTableHolders{
					holders: map[uint32]*lockTableHolder{0: holder},
				}

				lock := newRowLock(logger, &lockContext{
					waitTxn: pb.WaitTxn{TxnID: []byte("holder")},
					opts: LockOptions{LockOptions: pb.LockOptions{
						Mode: pb.LockMode_Exclusive,
					}},
				})
				handled := make(chan struct{}, 3)
				waiters := make([]*waiter, 0, 3)
				for i := 0; i < 3; i++ {
					w := acquireWaiter(
						pb.WaitTxn{TxnID: []byte(fmt.Sprintf("waiter-%d", i))},
						"test",
						logger,
					)
					w.setStatus(blocking)
					w.event = event{
						c: &lockContext{
							txn: &activeTxn{RWMutex: &sync.RWMutex{}},
							lockFunc: func(_ *lockContext, _ bool) {
								// A real completion callback validates that its
								// table is still current through this holder.
								_ = holder.get(1)
								handled <- struct{}{}
							},
						},
						eventC: events.eventC,
					}
					lock.addWaiter(logger, w)
					waiters = append(waiters, w)
				}
				lt.mu.store.Add([]byte{1}, lock)

				removed := make(chan int, 1)
				go func() {
					removed <- holders.removeWithFilter(
						func(_ uint64, _ lockTable) bool { return true },
						closeReasonServiceClose,
					)
				}()
				select {
				case count := <-removed:
					require.Equal(t, 1, count)
				case <-time.After(5 * time.Second):
					t.Fatal("table close retained the holder mutex while the waiter queue was full")
				}
				for _, w := range waiters {
					require.ErrorIs(t, w.mustRecvNotification(logger).err, ErrLockTableNotFound)
					w.close("test", logger)
				}

				events.close()
				require.Len(t, handled, 3)
				require.Nil(t, holder.get(1))
			})
		},
	)
}

func TestCloseLocalLockTableWithBlockedWaiter(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			tableID := uint64(10)

			l := s[0]
			ctx, cancel := context.WithTimeout(context.Background(),
				time.Second*10)
			defer cancel()

			mustAddTestLock(
				t,
				ctx,
				l,
				tableID,
				[]byte{1},
				[][]byte{{1}},
				pb.Granularity_Row)

			var wg sync.WaitGroup
			wg.Add(2)
			// txn2 wait txn1 or txn3
			go func() {
				defer wg.Done()
				_, err := l.Lock(
					ctx,
					tableID,
					[][]byte{{1}},
					[]byte{2},
					newTestRowExclusiveOptions(),
				)
				require.Equal(t, ErrLockTableNotFound, err)
			}()

			// txn3 wait txn2 or txn1
			go func() {
				defer wg.Done()
				_, err := l.Lock(
					ctx,
					tableID,
					[][]byte{{1}},
					[]byte{3},
					newTestRowExclusiveOptions(),
				)
				require.Equal(t, ErrLockTableNotFound, err)
			}()

			v, err := l.getLockTable(context.Background(), 0, tableID)
			require.NoError(t, err)
			lt := v.(*localLockTable)
			for {
				lt.mu.RLock()
				lock, ok := lt.mu.store.Get([]byte{1})
				require.True(t, ok)
				lt.mu.RUnlock()
				if lock.waiters.size() == 2 {
					break
				}
				time.Sleep(time.Millisecond * 10)
			}

			v.close(closeReasonServiceClose)
			wg.Wait()
		})
}

func TestSynchronousWaiterRemovedFromEventsBeforeAfterWait(t *testing.T) {
	table := uint64(10)
	getRunner(false)(
		t,
		table,
		func(ctx context.Context, s *service, lt *localLockTable) {
			rows := newTestRows(1, 2)
			txn1 := newTestTxnID(1)
			txn2 := newTestTxnID(2)

			_, err := s.Lock(ctx, table, rows, txn1, newTestRangeExclusiveOptions())
			require.NoError(t, err)

			waiting := make(chan struct{})
			removed := make(chan bool, 1)
			lt.options.beforeWait = func(c *lockContext) func() {
				if bytes.Equal(c.txn.txnID, txn2) {
					return func() { close(waiting) }
				}
				return func() {}
			}
			lt.options.afterWait = func(c *lockContext) func() {
				if !bytes.Equal(c.txn.txnID, txn2) {
					return func() {}
				}
				return func() {
					lt.events.mu.RLock()
					defer lt.events.mu.RUnlock()
					for _, w := range lt.events.mu.blockedWaiters {
						if w == c.w {
							removed <- false
							return
						}
					}
					removed <- true
				}
			}

			done := make(chan error, 1)
			go func() {
				_, err := s.Lock(ctx, table, rows, txn2, newTestRangeExclusiveOptions())
				done <- err
			}()

			select {
			case <-waiting:
			case <-time.After(time.Second):
				t.Fatal("txn2 did not begin waiting for the range lock")
			}

			require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
			require.True(t, <-removed)
			require.NoError(t, <-done)
			require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
		},
	)
}

func TestMergeRangeWithNoConflict(t *testing.T) {
	cases := []struct {
		txnID         string
		existsLock    [][][]byte
		waitOnLock    [][]byte
		existsWaiters [][]string
		newLock       [][]byte
		mergedLocks   [][]byte
		mergedWaiters [][]string
		flags         []byte
	}{
		{
			txnID:         "[] + [1, 2] = [1, 2]",
			existsLock:    [][][]byte{},
			newLock:       [][]byte{{1}, {2}},
			mergedLocks:   [][]byte{{1}, {2}},
			mergedWaiters: [][]string{nil},
			flags:         []byte{flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:         "[1] + [2,3] = [1, 2, 3]",
			existsLock:    [][][]byte{{{1}}},
			newLock:       [][]byte{{2}, {3}},
			mergedLocks:   [][]byte{{1}, {2}, {3}},
			waitOnLock:    [][]byte{{1}},
			existsWaiters: [][]string{{"1"}},
			mergedWaiters: [][]string{{"1"}, nil},
			flags:         []byte{flagLockRow, flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:         "[1] + [1,3] = [1, 3]",
			existsLock:    [][][]byte{{{1}}},
			newLock:       [][]byte{{1}, {3}},
			mergedLocks:   [][]byte{{1}, {3}},
			waitOnLock:    [][]byte{{1}},
			existsWaiters: [][]string{{"1"}},
			mergedWaiters: [][]string{{"1"}},
			flags:         []byte{flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:         "[1] + [2] + [1, 3] = [1, 3]",
			existsLock:    [][][]byte{{{1}}, {{2}}},
			newLock:       [][]byte{{1}, {3}},
			mergedLocks:   [][]byte{{1}, {3}},
			waitOnLock:    [][]byte{{1}, {2}},
			existsWaiters: [][]string{{"1"}, {"2"}},
			mergedWaiters: [][]string{{"1", "2"}},
			flags:         []byte{flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:         "[1] + [2] + [3] + [1, 3] = [1, 3]",
			existsLock:    [][][]byte{{{1}}, {{2}}, {{3}}},
			newLock:       [][]byte{{1}, {3}},
			mergedLocks:   [][]byte{{1}, {3}},
			waitOnLock:    [][]byte{{1}, {2}, {3}},
			existsWaiters: [][]string{{"1"}, {"2"}, {"3"}},
			mergedWaiters: [][]string{{"1", "2", "3"}},
			flags:         []byte{flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:         "[1] + [2] + [3] + [4] + [1, 3] = [1, 3] + [4]",
			existsLock:    [][][]byte{{{1}}, {{2}}, {{3}}, {{4}}},
			newLock:       [][]byte{{1}, {3}},
			mergedLocks:   [][]byte{{1}, {3}, {4}},
			waitOnLock:    [][]byte{{1}, {2}, {3}, {4}},
			existsWaiters: [][]string{{"1"}, {"2"}, {"3"}, {"4"}},
			mergedWaiters: [][]string{{"1", "2", "3"}, {"4"}},
			flags:         []byte{flagLockRangeStart, flagLockRangeEnd, flagLockRow},
		},

		{
			txnID:         "[1, 2] + [3, 4] = [1, 2] + [3, 4]",
			existsLock:    [][][]byte{{{1}, {2}}},
			newLock:       [][]byte{{3}, {4}},
			mergedLocks:   [][]byte{{1}, {2}, {3}, {4}},
			waitOnLock:    [][]byte{{2}},
			existsWaiters: [][]string{{"1"}},
			mergedWaiters: [][]string{{"1"}, nil},
			flags:         []byte{flagLockRangeStart, flagLockRangeEnd, flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:       "[3, 4] + [1, 2] = [1, 2] + [3, 4]",
			existsLock:  [][][]byte{{{3}, {4}}},
			newLock:     [][]byte{{1}, {2}},
			mergedLocks: [][]byte{{1}, {2}, {3}, {4}},
			flags:       []byte{flagLockRangeStart, flagLockRangeEnd, flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:       "[1, 4] + [1, 3] = [1, 4]",
			existsLock:  [][][]byte{{{1}, {4}}},
			newLock:     [][]byte{{1}, {3}},
			mergedLocks: [][]byte{{1}, {4}},
			flags:       []byte{flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:       "[1, 4] + [1, 4] = [1, 4]",
			existsLock:  [][][]byte{{{1}, {4}}},
			newLock:     [][]byte{{1}, {4}},
			mergedLocks: [][]byte{{1}, {4}},
			flags:       []byte{flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:       "[1, 4] + [1, 5] = [1, 5]",
			existsLock:  [][][]byte{{{1}, {4}}},
			newLock:     [][]byte{{1}, {5}},
			mergedLocks: [][]byte{{1}, {5}},
			flags:       []byte{flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:       "[2, 4] + [1, 5] = [1, 5]",
			existsLock:  [][][]byte{{{2}, {4}}},
			newLock:     [][]byte{{1}, {5}},
			mergedLocks: [][]byte{{1}, {5}},
			flags:       []byte{flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:       "[1, 4] + [2, 5] = [1, 5]",
			existsLock:  [][][]byte{{{1}, {4}}},
			newLock:     [][]byte{{2}, {5}},
			mergedLocks: [][]byte{{1}, {5}},
			flags:       []byte{flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:       "[2, 5] + [1, 4] = [1, 5]",
			existsLock:  [][][]byte{{{2}, {5}}},
			newLock:     [][]byte{{1}, {4}},
			mergedLocks: [][]byte{{1}, {5}},
			flags:       []byte{flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:       "[1, 5] + [2, 5] = [1, 5]",
			existsLock:  [][][]byte{{{1}, {5}}},
			newLock:     [][]byte{{2}, {5}},
			mergedLocks: [][]byte{{1}, {5}},
			flags:       []byte{flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:       "[2, 5] + [1, 5] = [1, 5]",
			existsLock:  [][][]byte{{{2}, {5}}},
			newLock:     [][]byte{{1}, {5}},
			mergedLocks: [][]byte{{1}, {5}},
			flags:       []byte{flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:       "[2, 6] + [1, 5] = [1, 6]",
			existsLock:  [][][]byte{{{2}, {6}}},
			newLock:     [][]byte{{1}, {5}},
			mergedLocks: [][]byte{{1}, {6}},
			flags:       []byte{flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:       "[1, 5] + [2, 6] = [1, 6]",
			existsLock:  [][][]byte{{{1}, {5}}},
			newLock:     [][]byte{{2}, {6}},
			mergedLocks: [][]byte{{1}, {6}},
			flags:       []byte{flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:       "[5, 6] + [1, 5] = [1, 6]",
			existsLock:  [][][]byte{{{5}, {6}}},
			newLock:     [][]byte{{1}, {5}},
			mergedLocks: [][]byte{{1}, {6}},
			flags:       []byte{flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:       "[1, 5] + [5, 6] = [1, 6]",
			existsLock:  [][][]byte{{{1}, {5}}},
			newLock:     [][]byte{{5}, {6}},
			mergedLocks: [][]byte{{1}, {6}},
			flags:       []byte{flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:       "[2, 3] + [1, 4] = [1, 4]",
			existsLock:  [][][]byte{{{2}, {3}}, {{1}, {4}}},
			newLock:     [][]byte{{1}, {4}},
			mergedLocks: [][]byte{{1}, {4}},
			flags:       []byte{flagLockRangeStart, flagLockRangeEnd},
		},

		{
			txnID:         "[1, 2] + [3, 4] + [5] + [6] + [1, 5] = [1, 5] + [6]",
			existsLock:    [][][]byte{{{1}, {2}}, {{3}, {4}}, {{5}}, {{6}}},
			newLock:       [][]byte{{1}, {5}},
			mergedLocks:   [][]byte{{1}, {5}, {6}},
			waitOnLock:    [][]byte{{2}, {4}, {5}},
			existsWaiters: [][]string{{"1", "2"}, {"3", "4"}, {"5"}},
			mergedWaiters: [][]string{{"1", "2", "3", "4", "5"}, nil},
			flags:         []byte{flagLockRangeStart, flagLockRangeEnd, flagLockRow},
		},
	}

	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l := s[0]
			ctx, cancel := context.WithTimeout(context.Background(),
				time.Second*10)
			defer cancel()

			table := uint64(10)
			for _, c := range cases {
				stopper := stopper.NewStopper("")
				v, err := l.getLockTableWithCreate(context.Background(), 0, table, nil, pb.Sharding_None)
				require.NoError(t, err)
				lt := v.(*localLockTable)

				for _, rows := range c.existsLock {
					opts := pb.LockOptions{}
					if len(rows) > 1 {
						opts.Granularity = pb.Granularity_Range
					}
					_, err := l.Lock(ctx, table, rows, []byte(c.txnID), opts)
					require.NoError(t, err)
				}
				for i, lock := range c.waitOnLock {
					lt.mu.Lock()
					lock, ok := lt.mu.store.Get(lock)
					if !ok {
						panic(ok)
					}
					var wg sync.WaitGroup
					for _, txnID := range c.existsWaiters[i] {
						w := acquireWaiter(pb.WaitTxn{TxnID: []byte(txnID)}, "", nil)
						w.setStatus(blocking)
						lock.waiters.put(w)
						wg.Add(1)
						require.NoError(t, stopper.RunTask(func(ctx context.Context) {
							wg.Done()
							w.wait(ctx, getLogger(""))
							w.close("", nil)
						}))
					}
					wg.Wait()
					lt.mu.Unlock()
				}

				opts := pb.LockOptions{}
				opts.Granularity = pb.Granularity_Range
				_, err = l.Lock(ctx, table, c.newLock, []byte(c.txnID), opts)
				require.NoError(t, err)

				lt.mu.Lock()
				var keys [][]byte
				var flags []byte
				idx := 0
				lt.mu.store.Iter(func(b []byte, l Lock) bool {
					keys = append(keys, b)
					flags = append(flags, l.value)
					if !l.isLockRangeStart() {
						if len(c.mergedWaiters) == 0 {
							assert.Equal(t, 0, l.waiters.size())
						} else {
							var waitTxns []string
							l.waiters.iter(func(v *waiter) bool {
								waitTxns = append(waitTxns, string(v.txn.TxnID))
								return true
							})
							require.Equal(t, c.mergedWaiters[idx], waitTxns)
							idx++
						}
					}
					return true
				})
				lt.mu.Unlock()
				require.Equal(t, c.mergedLocks, keys)
				for idx, v := range flags {
					assert.NotEqual(t, 0, v&c.flags[idx])
				}

				txn := l.activeTxnHolder.getActiveTxn([]byte(c.txnID), false, "")
				require.NotNil(t, txn)
				fn := func(values [][]byte) [][]byte {
					sort.Slice(values, func(i, j int) bool {
						return bytes.Compare(values[i], values[j]) < 0
					})
					return values
				}
				assert.Equal(t, fn(c.mergedLocks), fn(txn.getHoldLocksLocked(0).tableKeys[table].slice().all()))

				assert.NoError(t, l.Unlock(ctx, []byte(c.txnID), timestamp.Timestamp{}))
				stopper.Stop()
				table++
			}
		})
}

func TestLocalLockTableMultipleRowLocksCannotMissIfFoundSelfTxn(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			tableID := uint64(10)
			l := s[0]
			ctx, cancel := context.WithTimeout(context.Background(),
				time.Second*10)
			defer cancel()

			mustAddTestLock(
				t,
				ctx,
				l,
				tableID,
				[]byte{2},
				[][]byte{{1}},
				pb.Granularity_Row)

			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				mustAddTestLock(
					t,
					ctx,
					l,
					tableID,
					[]byte{1},
					[][]byte{{1}},
					pb.Granularity_Row)
			}()
			go func() {
				defer wg.Done()
				waitWaiters(t, l, tableID, []byte{1}, 1)
				mustAddTestLock(
					t,
					ctx,
					l,
					tableID,
					[]byte{1},
					[][]byte{{1}, {2}},
					pb.Granularity_Row)
			}()

			waitWaiters(t, l, tableID, []byte{1}, 2)
			require.NoError(t, l.Unlock(ctx, []byte{2}, timestamp.Timestamp{}))

			wg.Wait()
			v, err := l.getLockTable(context.Background(), 0, tableID)
			require.NoError(t, err)
			lt := v.(*localLockTable)
			lt.mu.Lock()
			defer lt.mu.Unlock()
			require.Equal(t, 2, lt.mu.store.Len())
		})
}

func TestIssue9856(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)

			l := s[0]
			ctx := context.Background()
			option := pb.LockOptions{
				Granularity: pb.Granularity_Range,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			values := `{"start": "073a150a3a153100", "end": "083a15083a1608c000", "mode": "Exclusive"}
			{"start": "013a15093a150100", "end": "033a15093a160bb800", "mode": "Exclusive"}
			{"start": "013a15013a150100", "end": "033a15013a160bb800", "mode": "Exclusive"}
			{"start": "093a15053a150100", "end": "0a3a15053a160bb800", "mode": "Exclusive"}
			{"start": "053a15043a160ba300", "end": "083a15013a160bb800", "mode": "Exclusive"}
			{"start": "093a15023a150100", "end": "0a3a15023a160baf00", "mode": "Exclusive"}
			{"start": "013a15073a150c00", "end": "043a15063a160bb800", "mode": "Exclusive"}
			{"start": "093a15083a150100", "end": "0a3a15083a160bb800", "mode": "Exclusive"}
			{"start": "053a15023a1608b300", "end": "073a15023a160bb800", "mode": "Exclusive"}
			{"start": "013a15033a150100", "end": "043a15013a160bb800", "mode": "Exclusive"}
			{"start": "053a15093a150100", "end": "083a15053a160bb800", "mode": "Exclusive"}
			{"start": "013a15063a150100", "end": "043a15043a160bb800", "mode": "Exclusive"}
			{"start": "053a15083a150100", "end": "083a15043a160bb800", "mode": "Exclusive"}
			{"start": "013a15043a1605d500", "end": "043a15033a160bb800", "mode": "Exclusive"}
			{"start": "053a15063a150100", "end": "073a15053a160bb800", "mode": "Exclusive"}
			{"start": "013a150a3a1605db00", "end": "053a15013a1602b200", "mode": "Exclusive"}
			{"start": "083a15083a1608c100", "end": "093a15013a16059800", "mode": "Exclusive"}
			{"start": "013a15093a160b8600", "end": "043a15083a160bb800", "mode": "Exclusive"}
			{"start": "093a15013a16059900", "end": "0a3a15013a16031f00", "mode": "Exclusive"}
			{"start": "093a15063a1602e000", "end": "0a3a15063a160bb800", "mode": "Exclusive"}
			{"start": "053a15053a150100", "end": "083a15023a16055200", "mode": "Exclusive"}
			{"start": "013a15083a150100", "end": "043a15073a16057300", "mode": "Exclusive"}
			{"start": "013a15063a1605a300", "end": "043a15053a160bb800", "mode": "Exclusive"}
			{"start": "093a15073a160b7000", "end": "0a3a15073a1608ff00", "mode": "Exclusive"}
			{"start": "073a15053a150100", "end": "083a15023a160bb800", "mode": "Exclusive"}
			{"start": "053a15033a16058b00", "end": "073a15033a160bb800", "mode": "Exclusive"}
			{"start": "033a15093a150100", "end": "043a15073a160bb800", "mode": "Exclusive"}
			{"start": "013a15023a150100", "end": "033a15023a160bb800", "mode": "Exclusive"}
			{"start": "013a15073a150100", "end": "023a15073a160bb800", "mode": "Exclusive"}
			{"start": "093a15093a1605d800", "end": "0a3a150a3a1602af00", "mode": "Exclusive"}
			{"start": "013a150a3a150100", "end": "023a150a3a160bb800", "mode": "Exclusive"}
			{"start": "053a15073a150100", "end": "083a15033a160bb800", "mode": "Exclusive"}
			{"start": "093a15033a150100", "end": "0a3a15033a160bb800", "mode": "Exclusive"}
			{"start": "013a15053a150100", "end": "033a15053a160bb800", "mode": "Exclusive"}
			{"start": "053a15083a1602ed00", "end": "073a15083a160b7c00", "mode": "Exclusive"}
			{"start": "023a15023a16056900", "end": "043a15013a1602ed00", "mode": "Exclusive"}
			{"start": "0a3a150a3a1602b000", "end": "0a3a150a3a160bb800", "mode": "Exclusive"}
			{"start": "053a15043a16026300", "end": "053a15043a160ba200", "mode": "Exclusive"}
			{"start": "053a15013a1602b300", "end": "073a15013a160b4200", "mode": "Exclusive"}
			{"start": "013a15033a160b7e00", "end": "043a15023a160bb800", "mode": "Exclusive"}
			{"start": "023a15053a160b3d00", "end": "043a15043a1608ca00", "mode": "Exclusive"}
			{"start": "073a15083a160b7d00", "end": "083a15053a16090700", "mode": "Exclusive"}
			{"start": "053a150a3a150100", "end": "083a15063a160bb800", "mode": "Exclusive"}
			{"start": "093a15043a16088800", "end": "0a3a15043a16060700", "mode": "Exclusive"}
			{"start": "053a15023a150100", "end": "073a15013a160bb800", "mode": "Exclusive"}`
			for _, r := range strings.Split(values, "\n") {
				v := &target{}
				json.MustUnmarshal([]byte(r), v)
				_, err := l.Lock(ctx, tableID, [][]byte{[]byte(v.Start), []byte(v.End)}, []byte("txn1"), option)
				require.NoError(t, err)
				vv, err := l.getLockTable(context.Background(), 0, tableID)
				require.NoError(t, err)
				lt := vv.(*localLockTable)
				lt.mu.Lock()
				var keys []string
				lt.mu.store.Iter(func(b []byte, l Lock) bool {
					keys = append(keys, fmt.Sprintf("%s(%p)", string(b), l.holders))
					return true
				})
				lt.mu.Unlock()
			}
		},
	)
}

func TestRangeLockConflict(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l := s[0]
			ctx, cancel := context.WithTimeout(context.Background(),
				time.Second*1000)
			defer cancel()

			tableID := uint64(10)
			txnID1 := []byte{1}
			txnID2 := []byte{2}

			cases := []struct {
				rows        [][]byte
				g           pb.Granularity
				hasConflict bool
				ranges      [][]byte
			}{
				{
					rows:        [][]byte{{3}},
					g:           pb.Granularity_Row,
					hasConflict: false,
					ranges:      [][]byte{{1}, {2}},
				},
				{
					rows:        [][]byte{{3}},
					g:           pb.Granularity_Row,
					hasConflict: true,
					ranges:      [][]byte{{1}, {3}},
				},
				{
					rows:        [][]byte{{3}},
					g:           pb.Granularity_Row,
					hasConflict: true,
					ranges:      [][]byte{{1}, {4}},
				},
				{
					rows:        [][]byte{{3}},
					g:           pb.Granularity_Row,
					hasConflict: true,
					ranges:      [][]byte{{3}, {4}},
				},
				{
					rows:        [][]byte{{3}},
					g:           pb.Granularity_Row,
					hasConflict: false,
					ranges:      [][]byte{{4}, {5}},
				},
				{
					rows:        [][]byte{{3}, {5}},
					g:           pb.Granularity_Range,
					hasConflict: false,
					ranges:      [][]byte{{1}, {2}},
				},
				{
					rows:        [][]byte{{3}, {5}},
					g:           pb.Granularity_Range,
					hasConflict: true,
					ranges:      [][]byte{{1}, {3}},
				},
				{
					rows:        [][]byte{{3}, {5}},
					g:           pb.Granularity_Range,
					hasConflict: true,
					ranges:      [][]byte{{1}, {4}},
				},
				{
					rows:        [][]byte{{3}, {5}},
					g:           pb.Granularity_Range,
					hasConflict: true,
					ranges:      [][]byte{{3}, {4}},
				},
				{
					rows:        [][]byte{{3}, {5}},
					g:           pb.Granularity_Range,
					hasConflict: true,
					ranges:      [][]byte{{3}, {5}},
				},
				{
					rows:        [][]byte{{3}, {5}},
					g:           pb.Granularity_Range,
					hasConflict: true,
					ranges:      [][]byte{{3}, {6}},
				},
				{
					rows:        [][]byte{{3}, {5}},
					g:           pb.Granularity_Range,
					hasConflict: true,
					ranges:      [][]byte{{5}, {6}},
				},
				{
					rows:        [][]byte{{3}, {5}},
					g:           pb.Granularity_Range,
					hasConflict: false,
					ranges:      [][]byte{{6}, {7}},
				},
			}

			for _, c := range cases {
				mustAddTestLock(
					t,
					ctx,
					l,
					tableID,
					txnID1,
					c.rows,
					c.g)

				var wg sync.WaitGroup
				wg.Add(1)
				fn := func() {
					defer func() {
						require.NoError(t, l.Unlock(ctx, txnID2, timestamp.Timestamp{}))
						wg.Done()
					}()
					mustAddTestLock(
						t,
						ctx,
						l,
						tableID,
						txnID2,
						c.ranges,
						pb.Granularity_Range)
				}

				if !c.hasConflict {
					fn()
					require.NoError(t, l.Unlock(ctx, txnID1, timestamp.Timestamp{}))
				} else {
					go fn()
					waitWaiters(t, l, tableID, c.rows[0], 1)
					require.NoError(t, l.Unlock(ctx, txnID1, timestamp.Timestamp{}))
				}

				wg.Wait()
			}
		})
}

func TestLockedTSIsLastCommittedTS(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l := s[0]
			ctx, cancel := context.WithTimeout(context.Background(),
				time.Second*10)
			defer cancel()

			tableID := uint64(10)
			v, err := l.getLockTableWithCreate(context.Background(), 0, tableID, nil, pb.Sharding_None)
			require.NoError(t, err)
			lt := v.(*localLockTable)
			lt.mu.Lock()
			lt.mu.tableCommittedAt = timestamp.Timestamp{PhysicalTime: 1}
			lt.mu.Unlock()

			txnID := []byte{1}
			mustAddTestLock(
				t,
				ctx,
				l,
				tableID,
				txnID,
				[][]byte{{1}},
				pb.Granularity_Row)
			require.NoError(t, l.Unlock(ctx, txnID, timestamp.Timestamp{PhysicalTime: 0}))
			lt.mu.Lock()
			require.Equal(t, timestamp.Timestamp{PhysicalTime: 1}, lt.mu.tableCommittedAt)
			lt.mu.Unlock()

			txnID = []byte{2}
			mustAddTestLock(
				t,
				ctx,
				l,
				tableID,
				txnID,
				[][]byte{{1}},
				pb.Granularity_Row)
			require.NoError(t, l.Unlock(ctx, txnID, timestamp.Timestamp{PhysicalTime: 2}))
			lt.mu.Lock()
			require.Equal(t, timestamp.Timestamp{PhysicalTime: 2}, lt.mu.tableCommittedAt)
			lt.mu.Unlock()

			txnID = []byte{3}
			mustAddTestLock(
				t,
				ctx,
				l,
				tableID,
				txnID,
				[][]byte{{1}},
				pb.Granularity_Row)
			require.NoError(t, l.Unlock(ctx, txnID, timestamp.Timestamp{PhysicalTime: 1}))
			lt.mu.Lock()
			require.Equal(t, timestamp.Timestamp{PhysicalTime: 2}, lt.mu.tableCommittedAt)
			lt.mu.Unlock()

			txnID = []byte{4}
			res, err := l.Lock(ctx, tableID, [][]byte{{1}}, txnID, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			})
			require.NoError(t, err)
			require.Equal(t, timestamp.Timestamp{PhysicalTime: 2}, res.Timestamp)
		})
}

func TestLockedTSIsLastCommittedTSWithRange(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l := s[0]
			ctx, cancel := context.WithTimeout(context.Background(),
				time.Second*10)
			defer cancel()

			tableID := uint64(10)
			v, err := l.getLockTableWithCreate(context.Background(), 0, tableID, nil, pb.Sharding_None)
			require.NoError(t, err)
			lt := v.(*localLockTable)
			lt.mu.Lock()
			lt.mu.tableCommittedAt = timestamp.Timestamp{PhysicalTime: 1}
			lt.mu.Unlock()

			txnID := []byte{1}
			mustAddTestLock(
				t,
				ctx,
				l,
				tableID,
				txnID,
				[][]byte{{1}, {2}},
				pb.Granularity_Range)
			require.NoError(t, l.Unlock(ctx, txnID, timestamp.Timestamp{PhysicalTime: 0}))
			lt.mu.Lock()
			require.Equal(t, timestamp.Timestamp{PhysicalTime: 1}, lt.mu.tableCommittedAt)
			lt.mu.Unlock()

			txnID = []byte{2}
			mustAddTestLock(
				t,
				ctx,
				l,
				tableID,
				txnID,
				[][]byte{{1}, {2}},
				pb.Granularity_Range)
			require.NoError(t, l.Unlock(ctx, txnID, timestamp.Timestamp{PhysicalTime: 2}))
			lt.mu.Lock()
			require.Equal(t, timestamp.Timestamp{PhysicalTime: 2}, lt.mu.tableCommittedAt)
			lt.mu.Unlock()

			txnID = []byte{3}
			mustAddTestLock(
				t,
				ctx,
				l,
				tableID,
				txnID,
				[][]byte{{1}, {2}},
				pb.Granularity_Range)
			require.NoError(t, l.Unlock(ctx, txnID, timestamp.Timestamp{PhysicalTime: 1}))
			lt.mu.Lock()
			require.Equal(t, timestamp.Timestamp{PhysicalTime: 2}, lt.mu.tableCommittedAt)
			lt.mu.Unlock()

			txnID = []byte{4}
			res, err := l.Lock(ctx, tableID, [][]byte{{1}, {2}}, txnID, pb.LockOptions{
				Granularity: pb.Granularity_Range,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			})
			require.NoError(t, err)
			require.Equal(t, timestamp.Timestamp{PhysicalTime: 2}, res.Timestamp)
		})
}

func Test15608(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			s1 := s[0]
			ctx, cancel := context.WithTimeout(context.Background(),
				time.Second*10)
			defer cancel()

			option := newTestRowExclusiveOptions()
			rows := newTestRows(1)
			txn1 := newTestTxnID(1)
			txn2 := newTestTxnID(2)
			txn3 := newTestTxnID(3)
			table := uint64(10)

			// txn1 hold lock
			_, err := s1.Lock(ctx, table, rows, txn1, option)
			require.NoError(t, err, err)

			v, err := s1.getLockTable(context.Background(), 0, table)
			require.NoError(t, err)
			lt := v.(*localLockTable)
			lt.options.beforeCloseFirstWaiter = func(c *lockContext) {
				c.txn.Unlock()
				defer c.txn.Lock()

				// txn3 hold lock
				_, err = s1.Lock(ctx, table, rows, txn3, option)
				require.NoError(t, err, err)
			}

			// txn2 wait for lock, is first waiter
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				option := newTestRowExclusiveOptions()
				_, _ = s1.Lock(ctx, table, rows, txn2, option)
			}()

			waitWaiters(t, s1, table, rows[0], 1)

			// unlock txn1 and txn2
			require.NoError(t, s1.Unlock(ctx, txn2, timestamp.Timestamp{}))
			require.NoError(t, s1.Unlock(ctx, txn1, timestamp.Timestamp{}))

			wg.Wait()

			checkLock(t, lt, rows[0], [][]byte{txn3}, nil, nil)
			require.NoError(t, s1.Unlock(ctx, txn3, timestamp.Timestamp{}))
		})
}

func TestLocalCoarsensBeforeFixedSliceExhaustion(t *testing.T) {
	for _, tt := range []struct {
		name string
		mode pb.LockMode
	}{
		{name: "exclusive", mode: pb.LockMode_Exclusive},
		{name: "shared", mode: pb.LockMode_Shared},
	} {
		t.Run(tt.name, func(t *testing.T) {
			runLockServiceTestsWithAdjustConfig(
				t,
				[]string{"s1"},
				time.Second*10,
				func(_ *lockTableAllocator, s []*service) {
					table := uint64(1)
					s1 := s[0]
					ctx, cancel := context.WithTimeout(context.Background(),
						time.Second*10)
					defer cancel()
					rows := newTestRows(1, 2, 3, 4, 5)
					txnID := newTestTxnID(1)
					_, err := s1.Lock(ctx, table, rows, txnID, pb.LockOptions{
						Granularity: pb.Granularity_Row,
						Mode:        tt.mode,
						Policy:      pb.WaitPolicy_Wait,
					})
					require.NoError(t, err)
					txn := s1.activeTxnHolder.getActiveTxn(txnID, false, "")
					require.NotNil(t, txn)
					txn.RLock()
					require.Equal(t, 2, txn.lockHolders[0].tableKeys[table].mustGet().len())
					txn.RUnlock()
					require.NoError(t, s1.Unlock(ctx, txnID, timestamp.Timestamp{}))
				},
				func(c *Config) {
					c.MaxLockRowCount = 3
					c.MaxFixedSliceSize = 5
				},
			)
		})
	}
}

func TestSharedLockBudgetCoarsensAcrossRequests(t *testing.T) {
	tests := []struct {
		name       string
		serviceIDs []string
		caller     int
	}{
		{name: "local", serviceIDs: []string{"s1"}, caller: 0},
		{name: "remote", serviceIDs: []string{"s1", "s2"}, caller: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runLockServiceTestsWithAdjustConfig(
				t,
				tt.serviceIDs,
				time.Second*10,
				func(_ *lockTableAllocator, services []*service) {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					const table = uint64(26635)
					owner := services[0]
					caller := services[tt.caller]
					sharedRows := pb.LockOptions{
						Granularity: pb.Granularity_Row,
						Mode:        pb.LockMode_Shared,
						Policy:      pb.WaitPolicy_Wait,
					}
					warmup := []byte("shared-warmup")
					_, err := owner.Lock(ctx, table, newTestRows(9), warmup, sharedRows)
					require.NoError(t, err)
					require.NoError(t, owner.Unlock(ctx, warmup, timestamp.Timestamp{}))

					txnID := []byte("shared-coarsen")
					_, err = caller.Lock(ctx, table, newTestRows(1, 2), txnID, sharedRows)
					require.NoError(t, err)

					// The actual transaction/table total crosses the budget even though
					// both calls are individually below it. Escalate to the least range
					// covering the keys observed by this transaction.
					_, err = caller.Lock(ctx, table, newTestRows(3, 4), txnID, sharedRows)
					require.NoError(t, err)
					_, err = caller.Lock(ctx, table, newTestRows(5), txnID, sharedRows)
					require.NoError(t, err)
					_, err = caller.Lock(ctx, table, newTestRows(6), txnID, sharedRows)
					require.NoError(t, err)

					// Both a remote origin and the lock owner must replace their old
					// bookkeeping; bounding only one side still leaks transaction state.
					for _, service := range []*service{caller, owner} {
						txn := service.activeTxnHolder.getActiveTxn(txnID, false, "")
						require.NotNil(t, txn)
						txn.RLock()
						require.Equal(t, 2, txn.lockHolders[0].tableKeys[table].mustGet().len())
						txn.RUnlock()
						if caller == owner {
							break
						}
					}

					lt := owner.tableGroups.get(0, table).(*localLockTable)
					lt.mu.RLock()
					start, ok := lt.mu.store.Get(newTestRows(1)[0])
					require.True(t, ok)
					require.True(t, start.isLockRangeStart())
					end, ok := lt.mu.store.Get(newTestRows(6)[0])
					require.True(t, ok)
					require.True(t, end.isLockRangeEnd())
					lt.mu.RUnlock()
					require.NoError(t, caller.Unlock(ctx, txnID, timestamp.Timestamp{}))
				},
				func(c *Config) {
					c.MaxLockRowCount = 3
					c.MaxFixedSliceSize = 5
				},
			)
		})
	}
}

func TestSharedRangeEscalationWaitsForOtherHolders(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			s := services[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			const table = uint64(26636)
			txn1 := []byte("shared-owner-1")
			txn2 := []byte("shared-owner-2")
			sharedRows := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Shared,
				Policy:      pb.WaitPolicy_Wait,
			}
			_, err := s.Lock(ctx, table, newTestRows(1, 2), txn1, sharedRows)
			require.NoError(t, err)
			_, err = s.Lock(ctx, table, newTestRows(1), txn2, sharedRows)
			require.NoError(t, err)

			done := make(chan error, 1)
			go func() {
				_, err := s.Lock(ctx, table, newTestRows(3, 4), txn1, sharedRows)
				done <- err
			}()
			waitWaiters(t, s, table, newTestRows(1)[0], 1)

			// The conversion is not representable while row 1 has another Shared
			// holder. It waits without mutating ownership or bookkeeping.
			lt := s.tableGroups.get(0, table).(*localLockTable)
			checkLock(t, lt, newTestRows(1)[0], [][]byte{txn1, txn2}, [][]byte{txn1}, []int32{3})
			checkLock(t, lt, newTestRows(2)[0], [][]byte{txn1}, nil, nil)
			lt.mu.RLock()
			_, hasStart := lt.mu.store.Get(newTestRows(3)[0])
			_, hasEnd := lt.mu.store.Get(newTestRows(4)[0])
			lt.mu.RUnlock()
			require.False(t, hasStart)
			require.False(t, hasEnd)
			active := s.activeTxnHolder.getActiveTxn(txn1, false, "")
			active.RLock()
			require.Equal(t, 2, active.lockHolders[0].tableKeys[table].mustGet().len())
			active.RUnlock()

			require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
			require.NoError(t, <-done)
			lt.mu.RLock()
			start, hasStart := lt.mu.store.Get(newTestRows(1)[0])
			end, hasEnd := lt.mu.store.Get(newTestRows(4)[0])
			lt.mu.RUnlock()
			require.True(t, hasStart)
			require.True(t, hasEnd)
			require.True(t, start.isLockRangeStart())
			require.True(t, end.isLockRangeEnd())
			require.Equal(t, pb.LockMode_Shared, start.GetLockMode())
			require.Equal(t, pb.LockMode_Shared, end.GetLockMode())
			require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
		},
		func(c *Config) {
			c.MaxLockRowCount = 3
			c.MaxFixedSliceSize = 5
		},
	)
}

func TestRemoteSharedRangeEscalationWaitsForOtherHolders(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1", "s2"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			owner := services[0]
			caller := services[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			const table = uint64(26638)
			sharedRows := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Shared,
				Policy:      pb.WaitPolicy_Wait,
			}
			warmup := []byte("remote-shared-warmup")
			_, err := owner.Lock(ctx, table, newTestRows(9), warmup, sharedRows)
			require.NoError(t, err)
			require.NoError(t, owner.Unlock(ctx, warmup, timestamp.Timestamp{}))

			txn1 := []byte("remote-shared-owner-1")
			txn2 := []byte("remote-shared-owner-2")
			_, err = caller.Lock(ctx, table, newTestRows(1, 2), txn1, sharedRows)
			require.NoError(t, err)
			_, err = owner.Lock(ctx, table, newTestRows(1), txn2, sharedRows)
			require.NoError(t, err)

			done := make(chan error, 1)
			go func() {
				_, err := caller.Lock(ctx, table, newTestRows(3, 4), txn1, sharedRows)
				done <- err
			}()
			waitWaiters(t, owner, table, newTestRows(1)[0], 1)

			// Waiting before conversion preserves exact bookkeeping on both sides.
			for _, service := range []*service{caller, owner} {
				active := service.activeTxnHolder.getActiveTxn(txn1, false, "")
				require.NotNil(t, active)
				active.RLock()
				locks := active.lockHolders[0].tableKeys[table].slice()
				require.Equal(t, newTestRows(1, 2), locks.all())
				locks.unref()
				active.RUnlock()
			}
			lt := owner.tableGroups.get(0, table).(*localLockTable)
			checkLock(t, lt, newTestRows(1)[0], [][]byte{txn1, txn2}, [][]byte{txn1}, []int32{3})
			checkLock(t, lt, newTestRows(2)[0], [][]byte{txn1}, nil, nil)
			lt.mu.RLock()
			_, hasStart := lt.mu.store.Get(newTestRows(3)[0])
			_, hasEnd := lt.mu.store.Get(newTestRows(4)[0])
			lt.mu.RUnlock()
			require.False(t, hasStart)
			require.False(t, hasEnd)

			require.NoError(t, owner.Unlock(ctx, txn2, timestamp.Timestamp{}))
			require.NoError(t, <-done)
			lt.mu.RLock()
			start, hasStart := lt.mu.store.Get(newTestRows(1)[0])
			end, hasEnd := lt.mu.store.Get(newTestRows(4)[0])
			lt.mu.RUnlock()
			require.True(t, hasStart)
			require.True(t, hasEnd)
			require.Equal(t, pb.LockMode_Shared, start.GetLockMode())
			require.Equal(t, pb.LockMode_Shared, end.GetLockMode())
			require.NoError(t, caller.Unlock(ctx, txn1, timestamp.Timestamp{}))
		},
		func(c *Config) {
			c.MaxLockRowCount = 3
			c.MaxFixedSliceSize = 5
		},
	)
}

func TestRangeEscalationPreservesStrongestHeldMode(t *testing.T) {
	tests := []struct {
		name       string
		serviceIDs []string
		caller     int
	}{
		{name: "local", serviceIDs: []string{"s1"}, caller: 0},
		{name: "remote", serviceIDs: []string{"s1", "s2"}, caller: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runLockServiceTestsWithAdjustConfig(
				t,
				tt.serviceIDs,
				time.Second*10,
				func(_ *lockTableAllocator, services []*service) {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					const table = uint64(26640)
					owner := services[0]
					caller := services[tt.caller]
					exclusive := newTestRowExclusiveOptions()
					shared := exclusive
					shared.Mode = pb.LockMode_Shared

					// Establish the table on s1 so the remote case exercises the
					// same authoritative owner path as a forwarded request.
					warmupTxn := []byte("mixed-mode-warmup")
					_, err := owner.Lock(ctx, table, newTestRows(9), warmupTxn, exclusive)
					require.NoError(t, err)
					require.NoError(t, owner.Unlock(ctx, warmupTxn, timestamp.Timestamp{}))

					txnID := []byte("mixed-mode-owner")
					_, err = caller.Lock(ctx, table, newTestRows(1, 2), txnID, exclusive)
					require.NoError(t, err)
					_, err = caller.Lock(ctx, table, newTestRows(3, 4), txnID, shared)
					require.NoError(t, err)

					lt := owner.tableGroups.get(0, table).(*localLockTable)
					lt.mu.RLock()
					start, hasStart := lt.mu.store.Get(newTestRows(1)[0])
					end, hasEnd := lt.mu.store.Get(newTestRows(4)[0])
					lt.mu.RUnlock()
					require.True(t, hasStart)
					require.True(t, hasEnd)
					require.True(t, start.isLockRangeStart())
					require.True(t, end.isLockRangeEnd())
					require.Equal(t, pb.LockMode_Exclusive, start.GetLockMode())
					require.Equal(t, pb.LockMode_Exclusive, end.GetLockMode())

					// A later Shared requester must still conflict with a key that
					// was Exclusive before the cumulative replacement.
					shared.Policy = pb.WaitPolicy_FastFail
					otherTxn := []byte("mixed-mode-other")
					_, err = owner.Lock(ctx, table, newTestRows(1), otherTxn, shared)
					require.ErrorIs(t, err, ErrLockConflict)
					require.NoError(t, owner.Unlock(ctx, otherTxn, timestamp.Timestamp{}))
					require.NoError(t, caller.Unlock(ctx, txnID, timestamp.Timestamp{}))
				},
				func(c *Config) {
					c.MaxLockRowCount = 3
					c.MaxFixedSliceSize = 8
				},
			)
		})
	}
}

func TestSharedRangeEscalationGapHolderWaitsWithoutMutation(t *testing.T) {
	tests := []struct {
		name       string
		serviceIDs []string
		caller     int
	}{
		{name: "local", serviceIDs: []string{"s1"}, caller: 0},
		{name: "remote", serviceIDs: []string{"s1", "s2"}, caller: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runLockServiceTestsWithAdjustConfig(
				t,
				tt.serviceIDs,
				time.Second*10,
				func(_ *lockTableAllocator, services []*service) {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					const table = uint64(26641)
					owner := services[0]
					caller := services[tt.caller]
					shared := pb.LockOptions{
						Granularity: pb.Granularity_Row,
						Mode:        pb.LockMode_Shared,
						Policy:      pb.WaitPolicy_Wait,
					}

					warmupTxn := []byte("gap-holder-warmup")
					_, err := owner.Lock(ctx, table, newTestRows(9), warmupTxn, shared)
					require.NoError(t, err)
					require.NoError(t, owner.Unlock(ctx, warmupTxn, timestamp.Timestamp{}))

					txnA := []byte("gap-holder-a")
					txnB := []byte("gap-holder-b")
					// Fill the retained-row budget exactly. Once the foreign
					// Shared gap is released, the coarsening must not transiently
					// promote it to another row holder (which would require a fifth
					// bookkeeping slot before the range replacement is committed).
					originalA := newTestRows(1, 2, 5, 7)
					_, err = caller.Lock(ctx, table, originalA, txnA, shared)
					require.NoError(t, err)
					_, err = owner.Lock(ctx, table, newTestRows(3), txnB, shared)
					require.NoError(t, err)

					done := make(chan error, 1)
					go func() {
						_, err := caller.Lock(ctx, table, newTestRows(8), txnA, shared)
						done <- err
					}()
					waitWaiters(t, owner, table, newTestRows(3)[0], 1)

					// The compatible Shared lock in the gap becomes a merge dependency,
					// not an ownership mutation. Both owner and origin remain exact while
					// the request waits.
					lt := owner.tableGroups.get(0, table).(*localLockTable)
					checkLock(t, lt, newTestRows(1)[0], [][]byte{txnA}, nil, nil)
					checkLock(t, lt, newTestRows(2)[0], [][]byte{txnA}, nil, nil)
					checkLock(t, lt, newTestRows(3)[0], [][]byte{txnB}, [][]byte{txnA}, []int32{3})
					checkLock(t, lt, newTestRows(5)[0], [][]byte{txnA}, nil, nil)
					checkLock(t, lt, newTestRows(7)[0], [][]byte{txnA}, nil, nil)
					checkLock(t, lt, newTestRows(8)[0], nil, nil, nil)
					for _, service := range []*service{caller, owner} {
						active := service.activeTxnHolder.getActiveTxn(txnA, false, "")
						require.NotNil(t, active)
						active.RLock()
						locks := active.lockHolders[0].tableKeys[table].slice()
						require.Equal(t, originalA, locks.all())
						locks.unref()
						active.RUnlock()
						if caller == owner {
							break
						}
					}

					require.NoError(t, owner.Unlock(ctx, txnB, timestamp.Timestamp{}))
					require.NoError(t, <-done)
					lt.mu.RLock()
					start, hasStart := lt.mu.store.Get(newTestRows(1)[0])
					end, hasEnd := lt.mu.store.Get(newTestRows(8)[0])
					lt.mu.RUnlock()
					require.True(t, hasStart)
					require.True(t, hasEnd)
					require.True(t, start.isLockRangeStart())
					require.True(t, end.isLockRangeEnd())
					require.Equal(t, pb.LockMode_Shared, start.GetLockMode())
					require.Equal(t, pb.LockMode_Shared, end.GetLockMode())

					require.NoError(t, caller.Unlock(ctx, txnA, timestamp.Timestamp{}))
				},
				func(c *Config) {
					c.MaxLockRowCount = 4
					c.MaxFixedSliceSize = 6
				},
			)
		})
	}
}

func TestSharedRangeEscalationTransfersWaitersBehindReleasedGap(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			s := services[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			const table = uint64(26644)
			txnA := []byte("gap-merge-a")
			txnB := []byte("gap-merge-b")
			txnC := []byte("gap-merge-c")
			shared := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Shared,
				Policy:      pb.WaitPolicy_Wait,
			}

			// A fills its bookkeeping slice; B occupies a compatible gap inside
			// A's eventual observed range.
			_, err := s.Lock(ctx, table, newTestRows(1, 2, 5, 7), txnA, shared)
			require.NoError(t, err)
			_, err = s.Lock(ctx, table, newTestRows(3), txnB, shared)
			require.NoError(t, err)

			mergeDone := make(chan error, 1)
			go func() {
				_, err := s.Lock(ctx, table, newTestRows(8), txnA, shared)
				mergeDone <- err
			}()
			waitWaiters(t, s, table, newTestRows(3)[0], 1)

			// C queues behind A. Once B releases, A consumes only its own
			// notified position; C must be transferred to the replacement range,
			// not dropped or spuriously granted.
			waiterDone := make(chan error, 1)
			go func() {
				_, err := s.Lock(ctx, table, newTestRows(3), txnC, newTestRowExclusiveOptions())
				waiterDone <- err
			}()
			waitWaiters(t, s, table, newTestRows(3)[0], 2)

			require.NoError(t, s.Unlock(ctx, txnB, timestamp.Timestamp{}))
			require.NoError(t, <-mergeDone)

			lt := s.tableGroups.get(0, table).(*localLockTable)
			checkLock(t, lt, newTestRows(1)[0], [][]byte{txnA}, [][]byte{txnC}, []int32{3})
			checkLock(t, lt, newTestRows(8)[0], [][]byte{txnA}, [][]byte{txnC}, []int32{3})
			require.Never(t, func() bool {
				select {
				case err := <-waiterDone:
					require.NoError(t, err)
					return true
				default:
					return false
				}
			}, time.Millisecond*100, time.Millisecond*10)

			require.NoError(t, s.Unlock(ctx, txnA, timestamp.Timestamp{}))
			require.NoError(t, <-waiterDone)
			require.NoError(t, s.Unlock(ctx, txnC, timestamp.Timestamp{}))
		},
		func(c *Config) {
			c.MaxLockRowCount = 4
			c.MaxFixedSliceSize = 6
		},
	)
}

func TestSharedRangeEscalationCancellationDetachesMergeWaiter(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			s := services[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			const table = uint64(26645)
			txnA := []byte("merge-cancel-a")
			txnB := []byte("merge-cancel-b")
			shared := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Shared,
				Policy:      pb.WaitPolicy_Wait,
			}
			_, err := s.Lock(ctx, table, newTestRows(1, 2), txnA, shared)
			require.NoError(t, err)
			_, err = s.Lock(ctx, table, newTestRows(1), txnB, shared)
			require.NoError(t, err)

			waitCtx, waitCancel := context.WithTimeout(ctx, time.Millisecond*50)
			defer waitCancel()
			done := make(chan error, 1)
			go func() {
				_, err := s.Lock(waitCtx, table, newTestRows(3, 4), txnA, shared)
				done <- err
			}()
			waitWaiters(t, s, table, newTestRows(1)[0], 1)
			require.ErrorIs(t, <-done, context.DeadlineExceeded)

			// The waiting conversion has no persistent ownership side effects when
			// its caller gives up: A keeps only the original rows and B's queue is
			// no longer blocked by a stale merge waiter.
			lt := s.tableGroups.get(0, table).(*localLockTable)
			checkLock(t, lt, newTestRows(1)[0], [][]byte{txnA, txnB}, nil, nil)
			checkLock(t, lt, newTestRows(2)[0], [][]byte{txnA}, nil, nil)
			active := s.activeTxnHolder.getActiveTxn(txnA, false, "")
			active.RLock()
			require.Equal(t, 2, active.lockHolders[0].tableKeys[table].mustGet().len())
			active.RUnlock()

			require.NoError(t, s.Unlock(ctx, txnB, timestamp.Timestamp{}))
			require.NoError(t, s.Unlock(ctx, txnA, timestamp.Timestamp{}))
		},
		func(c *Config) {
			c.MaxLockRowCount = 3
			c.MaxFixedSliceSize = 5
		},
	)
}

func TestRangeMergePreservesModeAndFailureAtomicity(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, services []*service) {
			s := services[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			t.Run("ordinary merge preserves exclusive", func(t *testing.T) {
				const table = uint64(26642)
				txnA := []byte("ordinary-mixed-a")
				exclusive := newTestRowExclusiveOptions()
				outsideTxn := []byte("ordinary-mixed-outside")
				outsideRange := exclusive
				outsideRange.Granularity = pb.Granularity_Range
				_, err := s.Lock(ctx, table, newTestRows(8, 9), outsideTxn, outsideRange)
				require.NoError(t, err)
				_, err = s.Lock(ctx, table, newTestRows(1), txnA, exclusive)
				require.NoError(t, err)

				sharedRange := exclusive
				sharedRange.Granularity = pb.Granularity_Range
				sharedRange.Mode = pb.LockMode_Shared
				_, err = s.Lock(ctx, table, newTestRows(1, 3), txnA, sharedRange)
				require.NoError(t, err)

				lt := s.tableGroups.get(0, table).(*localLockTable)
				lt.mu.RLock()
				start, hasStart := lt.mu.store.Get(newTestRows(1)[0])
				end, hasEnd := lt.mu.store.Get(newTestRows(3)[0])
				lt.mu.RUnlock()
				require.True(t, hasStart)
				require.True(t, hasEnd)
				require.Equal(t, pb.LockMode_Exclusive, start.GetLockMode())
				require.Equal(t, pb.LockMode_Exclusive, end.GetLockMode())

				sharedRange.Granularity = pb.Granularity_Row
				sharedRange.Policy = pb.WaitPolicy_FastFail
				txnB := []byte("ordinary-mixed-b")
				_, err = s.Lock(ctx, table, newTestRows(2), txnB, sharedRange)
				require.ErrorIs(t, err, ErrLockConflict)
				require.NoError(t, s.Unlock(ctx, txnB, timestamp.Timestamp{}))
				require.NoError(t, s.Unlock(ctx, txnA, timestamp.Timestamp{}))
				require.NoError(t, s.Unlock(ctx, outsideTxn, timestamp.Timestamp{}))
			})

			t.Run("ordinary shared merge waits before mutation", func(t *testing.T) {
				const table = uint64(26643)
				txnA := []byte("ordinary-gap-a")
				txnB := []byte("ordinary-gap-b")
				sharedRows := pb.LockOptions{
					Granularity: pb.Granularity_Row,
					Mode:        pb.LockMode_Shared,
					Policy:      pb.WaitPolicy_Wait,
				}
				_, err := s.Lock(ctx, table, newTestRows(1), txnA, sharedRows)
				require.NoError(t, err)
				_, err = s.Lock(ctx, table, newTestRows(2), txnB, sharedRows)
				require.NoError(t, err)

				sharedRange := sharedRows
				sharedRange.Granularity = pb.Granularity_Range
				done := make(chan error, 1)
				go func() {
					_, err := s.Lock(ctx, table, newTestRows(1, 3), txnA, sharedRange)
					done <- err
				}()
				waitWaiters(t, s, table, newTestRows(2)[0], 1)

				lt := s.tableGroups.get(0, table).(*localLockTable)
				checkLock(t, lt, newTestRows(1)[0], [][]byte{txnA}, nil, nil)
				checkLock(t, lt, newTestRows(2)[0], [][]byte{txnB}, [][]byte{txnA}, []int32{3})
				checkLock(t, lt, newTestRows(3)[0], nil, nil, nil)
				for _, txnID := range [][]byte{txnA, txnB} {
					active := s.activeTxnHolder.getActiveTxn(txnID, false, "")
					require.NotNil(t, active)
					active.RLock()
					require.Equal(t, 1,
						active.lockHolders[0].tableKeys[table].mustGet().len())
					active.RUnlock()
				}

				require.NoError(t, s.Unlock(ctx, txnB, timestamp.Timestamp{}))
				require.NoError(t, <-done)
				lt.mu.RLock()
				start, hasStart := lt.mu.store.Get(newTestRows(1)[0])
				end, hasEnd := lt.mu.store.Get(newTestRows(3)[0])
				lt.mu.RUnlock()
				require.True(t, hasStart)
				require.True(t, hasEnd)
				require.Equal(t, pb.LockMode_Shared, start.GetLockMode())
				require.Equal(t, pb.LockMode_Shared, end.GetLockMode())
				require.NoError(t, s.Unlock(ctx, txnA, timestamp.Timestamp{}))
			})
		},
	)
}

func TestExclusiveLockBudgetAppliesAcrossRequests(t *testing.T) {
	tests := []struct {
		name       string
		serviceIDs []string
		caller     int
	}{
		{name: "local", serviceIDs: []string{"s1"}, caller: 0},
		{name: "remote", serviceIDs: []string{"s1", "s2"}, caller: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runLockServiceTestsWithAdjustConfig(
				t,
				tt.serviceIDs,
				time.Second*10,
				func(_ *lockTableAllocator, services []*service) {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					const table = uint64(26630)
					owner := services[0]
					caller := services[tt.caller]
					opts := newTestRowExclusiveOptions()

					// Establish the table on s1 so the remote case exercises both the
					// origin and owner transaction bookkeeping paths.
					warmupTxn := newTestTxnID(1)
					_, err := owner.Lock(ctx, table, newTestRows(9), warmupTxn, opts)
					require.NoError(t, err)
					require.NoError(t, owner.Unlock(ctx, warmupTxn, timestamp.Timestamp{}))

					txnID := newTestTxnID(2)
					_, err = caller.Lock(ctx, table, newTestRows(1, 2), txnID, opts)
					require.NoError(t, err)
					_, err = caller.Lock(ctx, table, newTestRows(4), txnID, opts)
					require.NoError(t, err)
					_, err = caller.Lock(ctx, table, newTestRows(5), txnID, opts)
					require.NoError(t, err)
					// A later generation must compact the previous range together with
					// newly retained points, not start a fresh per-call budget.
					_, err = caller.Lock(ctx, table, newTestRows(7, 8), txnID, opts)
					require.NoError(t, err)

					// The budget is transaction/table scoped: four individually small
					// requests must compact to one bounded range, on both the lock owner
					// and a remote origin.
					for _, service := range []*service{caller, owner} {
						txn := service.activeTxnHolder.getActiveTxn(txnID, false, "")
						require.NotNil(t, txn)
						txn.RLock()
						holder := txn.lockHolders[0]
						require.NotNil(t, holder)
						require.Equal(t, 2, holder.tableKeys[table].mustGet().len())
						txn.RUnlock()
						if caller == owner {
							break
						}
					}

					lt := owner.tableGroups.get(0, table).(*localLockTable)
					lt.mu.RLock()
					start, ok := lt.mu.store.Get(newTestRows(1)[0])
					require.True(t, ok)
					require.True(t, start.isLockRangeStart())
					end, ok := lt.mu.store.Get(newTestRows(8)[0])
					require.True(t, ok)
					require.True(t, end.isLockRangeEnd())
					lt.mu.RUnlock()

					// Coarsening may cover gaps but must not become a table lock.
					outsideTxn := newTestTxnID(3)
					_, err = owner.Lock(ctx, table, newTestRows(9), outsideTxn, opts)
					require.NoError(t, err)
					require.NoError(t, owner.Unlock(ctx, outsideTxn, timestamp.Timestamp{}))

					conflictOpts := opts
					conflictOpts.Policy = pb.WaitPolicy_FastFail
					conflictTxn := newTestTxnID(4)
					_, err = owner.Lock(ctx, table, newTestRows(3), conflictTxn, conflictOpts)
					require.ErrorIs(t, err, ErrLockConflict)
					require.NoError(t, owner.Unlock(ctx, conflictTxn, timestamp.Timestamp{}))
					require.NoError(t, caller.Unlock(ctx, txnID, timestamp.Timestamp{}))
				},
				func(c *Config) {
					c.MaxLockRowCount = 3
				})
		})
	}
}

func TestExclusiveLockBudgetRemainsBoundedAcrossExecutionBatches(t *testing.T) {
	const (
		batchSize = 8192
		batches   = 6
		budget    = 20000
		table     = uint64(26633)
	)
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			s := services[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			txnID := []byte("execution-batches")

			encodeKey := func(value uint64) []byte {
				key := make([]byte, 8)
				binary.BigEndian.PutUint64(key, value)
				return key
			}
			for batch := 0; batch < batches; batch++ {
				rows := make([][]byte, batchSize)
				for row := range rows {
					rows[row] = encodeKey(uint64(batch*batchSize + row))
				}
				_, err := s.Lock(ctx, table, rows, txnID, newTestRowExclusiveOptions())
				require.NoError(t, err)

				txn := s.activeTxnHolder.getActiveTxn(txnID, false, "")
				require.NotNil(t, txn)
				txn.RLock()
				retained := txn.lockHolders[0].tableKeys[table].mustGet().len()
				txn.RUnlock()
				require.LessOrEqual(t, retained, budget,
					"retained lock keys exceeded the transaction/table budget after batch %d", batch)
			}

			txn := s.activeTxnHolder.getActiveTxn(txnID, false, "")
			txn.RLock()
			require.Equal(t, 2, txn.lockHolders[0].tableKeys[table].mustGet().len())
			txn.RUnlock()
			lt := s.tableGroups.get(0, table).(*localLockTable)
			lt.mu.RLock()
			start, ok := lt.mu.store.Get(encodeKey(0))
			require.True(t, ok)
			require.True(t, start.isLockRangeStart())
			end, ok := lt.mu.store.Get(encodeKey(batchSize*batches - 1))
			require.True(t, ok)
			require.True(t, end.isLockRangeEnd())
			lt.mu.RUnlock()
			require.NoError(t, s.Unlock(ctx, txnID, timestamp.Timestamp{}))
		},
		func(c *Config) {
			c.MaxLockRowCount = budget
		})
}

func TestExclusiveLockBudgetConflictRollsBackCompaction(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			s := services[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			const table = uint64(26631)
			opts := newTestRowExclusiveOptions()
			blockerTxn := newTestTxnID(1)
			_, err := s.Lock(ctx, table, newTestRows(3), blockerTxn, opts)
			require.NoError(t, err)

			txnID := newTestTxnID(2)
			_, err = s.Lock(ctx, table, newTestRows(1, 2), txnID, opts)
			require.NoError(t, err)
			_, err = s.Lock(ctx, table, newTestRows(4), txnID, opts)
			require.NoError(t, err)

			fastFail := opts
			fastFail.Policy = pb.WaitPolicy_FastFail
			_, err = s.Lock(ctx, table, newTestRows(5), txnID, fastFail)
			require.ErrorIs(t, err, ErrLockConflict)

			// addRangeLockLocked stages row removal in mergeContext. A conflict
			// must roll that staging back so the transaction can retry safely.
			txn := s.activeTxnHolder.getActiveTxn(txnID, false, "")
			require.NotNil(t, txn)
			txn.RLock()
			require.Equal(t, 3, txn.lockHolders[0].tableKeys[table].mustGet().len())
			txn.RUnlock()
			lt := s.tableGroups.get(0, table).(*localLockTable)
			for _, row := range newTestRows(1, 2, 4) {
				checkLock(t, lt, row, [][]byte{txnID}, nil, nil)
			}

			require.NoError(t, s.Unlock(ctx, blockerTxn, timestamp.Timestamp{}))
			_, err = s.Lock(ctx, table, newTestRows(5), txnID, opts)
			require.NoError(t, err)
			txn.RLock()
			require.Equal(t, 2, txn.lockHolders[0].tableKeys[table].mustGet().len())
			txn.RUnlock()
			require.NoError(t, s.Unlock(ctx, txnID, timestamp.Timestamp{}))
		},
		func(c *Config) {
			c.MaxLockRowCount = 3
		})
}

func TestExclusiveLockBudgetReplacementFailurePreservesOwnership(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			s := services[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			const table = uint64(26634)
			txnID := []byte("replacement-failure")
			opts := newTestRowExclusiveOptions()
			_, err := s.Lock(ctx, table, newTestRows(1, 2), txnID, opts)
			require.NoError(t, err)
			_, err = s.Lock(ctx, table, newTestRows(4), txnID, opts)
			require.NoError(t, err)

			txn := s.activeTxnHolder.getActiveTxn(txnID, false, "")
			require.NotNil(t, txn)
			txn.Lock()
			txn.beforeLockAdded = func([]byte, [][]byte) error { return ErrTxnNotFound }
			txn.Unlock()

			_, err = s.Lock(ctx, table, newTestRows(5), txnID, opts)
			require.ErrorIs(t, err, ErrTxnNotFound)

			// Replacement preparation failed before the merge was committed: both
			// transaction bookkeeping and lock-store ownership must remain intact.
			txn.RLock()
			require.Equal(t, 3, txn.lockHolders[0].tableKeys[table].mustGet().len())
			txn.RUnlock()
			lt := s.tableGroups.get(0, table).(*localLockTable)
			for _, row := range newTestRows(1, 2, 4) {
				checkLock(t, lt, row, [][]byte{txnID}, nil, nil)
			}

			txn.Lock()
			txn.beforeLockAdded = nil
			txn.Unlock()
			_, err = s.Lock(ctx, table, newTestRows(5), txnID, opts)
			require.NoError(t, err)
			txn.RLock()
			require.Equal(t, 2, txn.lockHolders[0].tableKeys[table].mustGet().len())
			txn.RUnlock()
			require.NoError(t, s.Unlock(ctx, txnID, timestamp.Timestamp{}))
		},
		func(c *Config) {
			c.MaxLockRowCount = 3
		})
}

func TestSharedLockBudgetReplacementFailurePreservesOwnership(t *testing.T) {
	runLockServiceTestsWithAdjustConfig(
		t,
		[]string{"s1"},
		time.Second*10,
		func(_ *lockTableAllocator, services []*service) {
			s := services[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			const table = uint64(26637)
			txnID := []byte("shared-replacement-failure")
			opts := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Shared,
				Policy:      pb.WaitPolicy_Wait,
			}
			_, err := s.Lock(ctx, table, newTestRows(1, 2), txnID, opts)
			require.NoError(t, err)
			_, err = s.Lock(ctx, table, newTestRows(4), txnID, opts)
			require.NoError(t, err)

			txn := s.activeTxnHolder.getActiveTxn(txnID, false, "")
			require.NotNil(t, txn)
			txn.Lock()
			txn.beforeLockAdded = func([]byte, [][]byte) error { return ErrTxnNotFound }
			txn.Unlock()

			_, err = s.Lock(ctx, table, newTestRows(5), txnID, opts)
			require.ErrorIs(t, err, ErrTxnNotFound)
			txn.RLock()
			require.Equal(t, 3, txn.lockHolders[0].tableKeys[table].mustGet().len())
			txn.RUnlock()
			lt := s.tableGroups.get(0, table).(*localLockTable)
			for _, row := range newTestRows(1, 2, 4) {
				checkLock(t, lt, row, [][]byte{txnID}, nil, nil)
			}

			txn.Lock()
			txn.beforeLockAdded = nil
			txn.Unlock()
			_, err = s.Lock(ctx, table, newTestRows(5), txnID, opts)
			require.NoError(t, err)
			txn.RLock()
			require.Equal(t, 2, txn.lockHolders[0].tableKeys[table].mustGet().len())
			txn.RUnlock()
			require.NoError(t, s.Unlock(ctx, txnID, timestamp.Timestamp{}))
		},
		func(c *Config) {
			c.MaxLockRowCount = 3
		},
	)
}

func TestCannotHungIfRangeConflictWithRowMultiTimes(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l := s[0]
			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Second*10,
			)
			defer cancel()

			tableID := uint64(10)
			add := func(
				txn []byte,
				rows [][]byte,
				g pb.Granularity,
			) {
				mustAddTestLock(
					t,
					ctx,
					l,
					tableID,
					txn,
					rows,
					g,
				)
			}

			// workflow
			//
			// txn1 lock k4
			//      k4: holder(txn1) waiters()
			//
			// txn3 lock [k1, k4], wait at k4
			//      k4: holder(txn1) waiters(txn3)
			//
			// txn1 unlock, notify txn3 ------------------|
			//      k4: holder() waiters(txn3)            |
			//                                            |
			// txn2 lock range k2, k3                     |
			//      k2: holder(txn2) waiters()            |
			//      k3: holder(txn2) waiters()            |
			//      k4: holder() waiters(txn3)            |
			//                                            |
			// txn3 lock [k1, k4] retry, wait at k2 <-----|
			//      k2: holder(txn2) waiters(txn3)
			//      k3: holder(txn2) waiters()
			//      k4: holder() waiters(txn3)
			//
			// txn4 lock k2, wait at k2 --------------------------------|
			//      k2: holder(txn2) waiters(txn3, txn4)                |
			//      k3: holder(txn2) waiters()                          |
			//      k4: holder() waiters(txn3)                          |
			//                                                          |
			//                                                          |
			// txn2 unlock, notify txn3, txn4                           |
			//      k2: holder(txn2) waiters(txn3, txn4) -> deleted     |
			//      k3: holder(txn2) waiters()           -> deleted     |
			//      k4: holder() waiters(txn3)                          |
			//                                                          |
			// txn4 lock k2 retry <-------------------------------------|
			//      k2: holder(txn4) waiters(txn3)
			//      k4: holder() waiters(txn3)
			//
			// txn3 lock [k1, k4] retry, wait at k2
			//      k2: holder(txn4) waiters(txn3)
			//      k4: holder() waiters(txn3)
			//
			// txn4 lock k4, wait txn3
			//      k2: holder(txn4) waiters()
			//      k4: holder() waiters(txn3, txn4)

			// txn1 hold row1
			txn1 := []byte{1}
			txn2 := []byte{2}
			txn3 := []byte{3}
			txn4 := []byte{4}

			key2 := newTestRows(2)
			key4 := newTestRows(4)
			range23 := newTestRows(2, 3)
			range14 := newTestRows(1, 4)

			txn2Locked := make(chan struct{})
			txn4WaitAt2 := make(chan struct{})
			txn4GetLockAt1 := make(chan struct{})
			startTxn3 := make(chan struct{})
			txn3WaitAt2 := make(chan struct{})
			txn3WaitAt2Again := make(chan struct{})
			txn3WaitAt4 := make(chan struct{})
			txn3NotifiedAt4 := make(chan struct{})
			var once sync.Once

			// txn1 lock k4
			add(txn1, key4, pb.Granularity_Row)
			close(startTxn3)

			v, err := l.getLockTable(context.Background(), 0, tableID)
			require.NoError(t, err)
			lt := v.(*localLockTable)
			txn3WaitTimes := 0
			lt.options.beforeWait = func(c *lockContext) func() {
				if bytes.Equal(c.txn.txnID, txn3) {
					return func() {
						if txn3WaitTimes == 0 {
							// txn3 wait at key4
							close(txn3WaitAt4)
							txn3WaitTimes++
							return
						}

						if txn3WaitTimes == 1 {
							close(txn3WaitAt2)
							txn3WaitTimes++
							return
						}

						if txn3WaitTimes == 2 {
							// step10: txn4 retry lock and wait at key2 again
							close(txn3WaitAt2Again)
							txn3WaitTimes++
							return
						}
					}
				}

				if bytes.Equal(c.txn.txnID, txn4) {
					return func() {
						once.Do(func() {
							close(txn4WaitAt2)
						})
					}
				}

				return func() {}
			}

			txn3NotifiedTimes := 0
			lt.options.afterWait = func(c *lockContext) func() {
				if bytes.Equal(c.txn.txnID, txn3) {
					return func() {
						if txn3NotifiedTimes == 0 {
							// txn1 closed and txn3 get notified
							close(txn3NotifiedAt4)
							txn3NotifiedTimes++
							<-txn2Locked
							return
						}

						if txn3NotifiedTimes == 1 {
							<-txn4GetLockAt1
						}
					}
				}
				return func() {}
			}

			var wg sync.WaitGroup
			wg.Add(5)

			go func() {
				defer wg.Done()
				<-startTxn3
				// txn3 lock range [k1, k4]
				add(txn3, range14, pb.Granularity_Range)
			}()

			go func() {
				defer wg.Done()
				<-txn3WaitAt4
				// txn1 unlock
				require.NoError(t, l.Unlock(ctx, txn1, timestamp.Timestamp{}))
			}()

			go func() {
				defer wg.Done()
				<-txn3NotifiedAt4
				// txn2 lock range [k3, k3]
				add(txn2, range23, pb.Granularity_Range)
				close(txn2Locked)
			}()

			go func() {
				defer wg.Done()
				<-txn3WaitAt2
				// txn4 lock k2
				add(txn4, key2, pb.Granularity_Row)
				close(txn4GetLockAt1)
				<-txn3WaitAt2Again

				// txn4 lock k4
				add(txn4, key4, pb.Granularity_Row)

				require.NoError(t, l.Unlock(ctx, txn4, timestamp.Timestamp{}))
			}()

			go func() {
				defer wg.Done()
				<-txn4WaitAt2
				require.NoError(t, l.Unlock(ctx, txn2, timestamp.Timestamp{}))
			}()

			wg.Wait()
		},
	)
}

func TestUnlockLockNotHeldByCurrentTxn(t *testing.T) {
	table := uint64(10)
	getRunner(false)(
		t,
		table,
		func(ctx context.Context, s *service, lt *localLockTable) {
			// Create two different transactions
			txn1 := newTestTxnID(1)
			txn2 := newTestTxnID(2)
			rows := newTestRows(1)

			// Add lock with txn1
			mustAddTestLock(
				t,
				ctx,
				s,
				table,
				txn1,
				rows,
				pb.Granularity_Row)

			// Create a cowSlice for the unlock call
			ls, err := newCowSlice(lt.fsp, rows)
			require.NoError(t, err)
			defer ls.close()

			// Get txn2 and add it to the active txn holder
			txn2Active := s.activeTxnHolder.getActiveTxn(txn2, true, "")
			require.NotNil(t, txn2Active)

			// Add the same bind to txn2's hold locks to ensure bind is not changed
			txn2Active.Lock()
			err = txn2Active.lockAdded(0, lt.bind, rows, lt.logger)
			require.NoError(t, err)
			txn2Active.Unlock()

			// Try to unlock with txn2
			// This should trigger the fatal error
			require.Panics(t, func() {
				lt.unlock(
					txn2Active,
					ls,
					timestamp.Timestamp{},
				)
			}, "should panic when trying to unlock a lock not held by current transaction")
		},
	)
}

type target struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

// TestExclusiveHolderMustBlockSharedRequests verifies that when an Exclusive
// waiter is promoted to holder (after all Shared holders release), the lock
// entry's mode is correctly updated from Shared to Exclusive, so subsequent
// Shared requests are blocked.
//
// Without the setMode fix, the lock entry's mode stays Shared after the
// Exclusive waiter is promoted (because Lock is a value type and addHolder
// operates on a copy), allowing new Shared requests to slip through.
func TestExclusiveHolderMustBlockSharedRequests(t *testing.T) {
	table := uint64(10)
	getRunner(false)(
		t,
		table,
		func(ctx context.Context, s *service, lt *localLockTable) {
			rows := newTestRows(1)
			txn1 := newTestTxnID(1) // Shared holder
			txn2 := newTestTxnID(2) // Exclusive waiter → holder
			txn3 := newTestTxnID(3) // Shared requester (should be blocked)

			// Step 1: txn1 acquires Shared lock
			_, err := s.Lock(ctx, table, rows, txn1, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Shared,
				Policy:      pb.WaitPolicy_Wait,
			})
			require.NoError(t, err)

			// Step 2: txn2 requests Exclusive lock → blocked (waiter)
			c2 := make(chan error, 1)
			go func() {
				_, err := s.Lock(ctx, table, rows, txn2, pb.LockOptions{
					Mode:     pb.LockMode_Exclusive,
					Sharding: pb.Sharding_None,
					Policy:   pb.WaitPolicy_Wait,
				})
				c2 <- err
			}()
			waitWaiters(t, s, table, rows[0], 1)

			// Step 3: txn1 releases Shared lock → txn2 promoted to Exclusive holder
			require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
			require.NoError(t, <-c2)

			// Step 4: txn3 requests Shared lock → must be blocked
			c3 := make(chan error, 1)
			go func() {
				_, err := s.Lock(ctx, table, rows, txn3, pb.LockOptions{
					Mode:     pb.LockMode_Shared,
					Sharding: pb.Sharding_None,
					Policy:   pb.WaitPolicy_Wait,
				})
				c3 <- err
			}()
			waitWaiters(t, s, table, rows[0], 1)

			// Verify txn3 is still waiting (not immediately granted)
			select {
			case <-c3:
				t.Fatal("Shared request should be blocked by Exclusive holder")
			case <-time.After(100 * time.Millisecond):
				// Expected: txn3 is blocked
			}

			// Cleanup: release txn2 → txn3 can proceed
			require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
			require.NoError(t, <-c3)
			require.NoError(t, s.Unlock(ctx, txn3, timestamp.Timestamp{}))
		},
	)
}

// TestSharedAfterExclusiveRelease verifies that when an Exclusive holder
// releases and a Shared waiter is promoted, the lock entry's mode is correctly
// updated from Exclusive to Shared, so subsequent Shared requests are allowed.
func TestSharedAfterExclusiveRelease(t *testing.T) {
	table := uint64(10)
	getRunner(false)(
		t,
		table,
		func(ctx context.Context, s *service, lt *localLockTable) {
			rows := newTestRows(1)
			txn1 := newTestTxnID(1) // Exclusive holder
			txn2 := newTestTxnID(2) // Shared waiter → holder
			txn3 := newTestTxnID(3) // Shared requester (should be allowed)

			// Step 1: txn1 acquires Exclusive lock
			mustAddTestLock(t, ctx, s, table, txn1, rows, pb.Granularity_Row)

			// Step 2: txn2 requests Shared lock → blocked
			c2 := make(chan error, 1)
			go func() {
				_, err := s.Lock(ctx, table, rows, txn2, pb.LockOptions{
					Mode:     pb.LockMode_Shared,
					Sharding: pb.Sharding_None,
					Policy:   pb.WaitPolicy_Wait,
				})
				c2 <- err
			}()
			waitWaiters(t, s, table, rows[0], 1)

			// Step 3: txn1 releases Exclusive lock → txn2 promoted to Shared holder
			require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
			require.NoError(t, <-c2)

			// Step 4: txn3 requests Shared lock → must be allowed (not blocked)
			c3 := make(chan error, 1)
			go func() {
				_, err := s.Lock(ctx, table, rows, txn3, pb.LockOptions{
					Mode:     pb.LockMode_Shared,
					Sharding: pb.Sharding_None,
					Policy:   pb.WaitPolicy_Wait,
				})
				c3 <- err
			}()

			select {
			case err := <-c3:
				require.NoError(t, err) // Expected: txn3 granted immediately
			case <-time.After(3 * time.Second):
				t.Fatal("Shared request should be allowed when only Shared holders exist")
			}

			// Cleanup
			require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
			require.NoError(t, s.Unlock(ctx, txn3, timestamp.Timestamp{}))
		},
	)
}

// TestRangeLockModeUpgradeUpdatesBothEnds verifies that when a waiter is promoted
// to holder with a different mode (e.g., Shared -> Exclusive), both ends of the
// range lock (range-start and range-end) are updated to the new mode.
// This tests the setModePairedRangeLock helper.
func TestRangeLockModeUpgradeUpdatesBothEnds(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l := s[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			tableID := uint64(10)
			rangeStart := []byte{1}
			rangeEnd := []byte{5}
			rangeRows := [][]byte{rangeStart, rangeEnd}

			sharedOpt := pb.LockOptions{
				Granularity: pb.Granularity_Range,
				Mode:        pb.LockMode_Shared,
				Policy:      pb.WaitPolicy_Wait,
			}
			exclusiveOpt := pb.LockOptions{
				Granularity: pb.Granularity_Range,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			// Step 1: txn1 acquires Shared range lock
			txn1 := newTestTxnID(1)
			_, err := l.Lock(ctx, tableID, rangeRows, txn1, sharedOpt)
			require.NoError(t, err)

			// Verify both ends are Shared
			v, err := l.getLockTable(context.Background(), 0, tableID)
			require.NoError(t, err)
			lt := v.(*localLockTable)

			lt.mu.RLock()
			startLock, ok1 := lt.mu.store.Get(rangeStart)
			endLock, ok2 := lt.mu.store.Get(rangeEnd)
			lt.mu.RUnlock()
			require.True(t, ok1, "range-start should exist")
			require.True(t, ok2, "range-end should exist")
			require.True(t, startLock.isShared(), "range-start should be Shared initially")
			require.True(t, endLock.isShared(), "range-end should be Shared initially")

			// Step 2: txn2 requests Exclusive range lock → blocked
			txn2 := newTestTxnID(2)
			txn2Done := make(chan struct{}, 1)
			go func() {
				_, err := l.Lock(ctx, tableID, rangeRows, txn2, exclusiveOpt)
				require.NoError(t, err)
				txn2Done <- struct{}{}
			}()
			time.Sleep(100 * time.Millisecond)

			// Step 3: txn1 releases → txn2 promoted to Exclusive holder
			require.NoError(t, l.Unlock(ctx, txn1, timestamp.Timestamp{PhysicalTime: 1}))
			select {
			case <-txn2Done:
			case <-time.After(5 * time.Second):
				t.Fatal("txn2 (Exclusive) did not acquire range lock in time")
			}

			// Step 4: Verify BOTH ends are now Exclusive (this is what setModePairedRangeLock fixes)
			lt.mu.RLock()
			startLock, ok1 = lt.mu.store.Get(rangeStart)
			endLock, ok2 = lt.mu.store.Get(rangeEnd)
			lt.mu.RUnlock()
			require.True(t, ok1, "range-start should exist after promotion")
			require.True(t, ok2, "range-end should exist after promotion")
			require.False(t, startLock.isShared(),
				"range-start should be Exclusive after Exclusive waiter promoted (setModePairedRangeLock)")
			require.False(t, endLock.isShared(),
				"range-end should be Exclusive after Exclusive waiter promoted (setModePairedRangeLock)")
			require.Equal(t, pb.LockMode_Exclusive, startLock.GetLockMode(),
				"range-start mode should be Exclusive")
			require.Equal(t, pb.LockMode_Exclusive, endLock.GetLockMode(),
				"range-end mode should be Exclusive")

			// Step 5: txn3 requests Shared range lock → should be blocked by Exclusive holder
			txn3 := newTestTxnID(3)
			txn3Done := make(chan struct{}, 1)
			go func() {
				_, err := l.Lock(ctx, tableID, rangeRows, txn3, sharedOpt)
				require.NoError(t, err)
				txn3Done <- struct{}{}
			}()

			select {
			case <-txn3Done:
				t.Fatal("txn3 (Shared) should be BLOCKED by txn2 (Exclusive range lock), " +
					"but it was granted. This means setModePairedRangeLock did not update both ends.")
			case <-time.After(500 * time.Millisecond):
				// Expected: txn3 is blocked
			}

			// Cleanup
			require.NoError(t, l.Unlock(ctx, txn2, timestamp.Timestamp{PhysicalTime: 2}))
			select {
			case <-txn3Done:
			case <-time.After(5 * time.Second):
				t.Fatal("txn3 did not acquire lock after txn2 released")
			}
			require.NoError(t, l.Unlock(ctx, txn3, timestamp.Timestamp{PhysicalTime: 3}))
		},
	)
}

// TestSetModePairedRangeLockDirect directly tests the setModePairedRangeLock helper
// by manually creating range locks and verifying the paired entry is updated.
func TestSetModePairedRangeLockDirect(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l := s[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			tableID := uint64(10)
			rangeStart := []byte{1}
			rangeEnd := []byte{5}
			rangeRows := [][]byte{rangeStart, rangeEnd}

			sharedOpt := pb.LockOptions{
				Granularity: pb.Granularity_Range,
				Mode:        pb.LockMode_Shared,
				Policy:      pb.WaitPolicy_Wait,
			}

			// Create a Shared range lock
			txn1 := newTestTxnID(1)
			_, err := l.Lock(ctx, tableID, rangeRows, txn1, sharedOpt)
			require.NoError(t, err)

			// Get the lock table and directly test setModePairedRangeLock
			v, err := l.getLockTable(context.Background(), 0, tableID)
			require.NoError(t, err)
			lt := v.(*localLockTable)

			// Test 1: Update range-start, verify range-end is also updated
			lt.mu.Lock()
			startLock, _ := lt.mu.store.Get(rangeStart)
			require.True(t, startLock.isLockRangeStart(), "should be range-start")
			require.True(t, startLock.isShared(), "should be Shared initially")

			// Simulate mode upgrade on range-start
			updatedStart, changed := startLock.setMode(pb.LockMode_Exclusive)
			require.True(t, changed, "mode should change from Shared to Exclusive")
			lt.mu.store.Add(rangeStart, updatedStart)

			// Call setModePairedRangeLock to update range-end
			lt.setModePairedRangeLock(rangeStart, updatedStart, pb.LockMode_Exclusive)

			// Verify range-end is now Exclusive
			endLock, _ := lt.mu.store.Get(rangeEnd)
			require.False(t, endLock.isShared(),
				"range-end should be Exclusive after setModePairedRangeLock called on range-start")
			lt.mu.Unlock()

			// Cleanup
			require.NoError(t, l.Unlock(ctx, txn1, timestamp.Timestamp{PhysicalTime: 1}))
		},
	)
}

// TestSetModePairedRangeLockFromRangeEnd tests setModePairedRangeLock when called
// from the range-end entry (backward scan to find range-start).
func TestSetModePairedRangeLockFromRangeEnd(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l := s[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			tableID := uint64(10)
			rangeStart := []byte{1}
			rangeEnd := []byte{5}
			rangeRows := [][]byte{rangeStart, rangeEnd}

			sharedOpt := pb.LockOptions{
				Granularity: pb.Granularity_Range,
				Mode:        pb.LockMode_Shared,
				Policy:      pb.WaitPolicy_Wait,
			}

			// Create a Shared range lock
			txn1 := newTestTxnID(1)
			_, err := l.Lock(ctx, tableID, rangeRows, txn1, sharedOpt)
			require.NoError(t, err)

			// Get the lock table and directly test setModePairedRangeLock from range-end
			v, err := l.getLockTable(context.Background(), 0, tableID)
			require.NoError(t, err)
			lt := v.(*localLockTable)

			// Test: Update range-end, verify range-start is also updated
			lt.mu.Lock()
			endLock, _ := lt.mu.store.Get(rangeEnd)
			require.True(t, endLock.isLockRangeEnd(), "should be range-end")
			require.True(t, endLock.isShared(), "should be Shared initially")

			// Simulate mode upgrade on range-end
			updatedEnd, changed := endLock.setMode(pb.LockMode_Exclusive)
			require.True(t, changed, "mode should change from Shared to Exclusive")
			lt.mu.store.Add(rangeEnd, updatedEnd)

			// Call setModePairedRangeLock to update range-start (backward scan)
			lt.setModePairedRangeLock(rangeEnd, updatedEnd, pb.LockMode_Exclusive)

			// Verify range-start is now Exclusive
			startLock, _ := lt.mu.store.Get(rangeStart)
			require.False(t, startLock.isShared(),
				"range-start should be Exclusive after setModePairedRangeLock called on range-end")
			lt.mu.Unlock()

			// Cleanup
			require.NoError(t, l.Unlock(ctx, txn1, timestamp.Timestamp{PhysicalTime: 1}))
		},
	)
}

// TestSetModePairedRangeLockRowLockNoOp verifies that setModePairedRangeLock
// is a no-op for row locks.
func TestSetModePairedRangeLockRowLockNoOp(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l := s[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			tableID := uint64(10)
			rowKey := []byte{3}

			sharedOpt := newTestRowSharedOptions()

			// Create a Shared row lock
			txn1 := newTestTxnID(1)
			_, err := l.Lock(ctx, tableID, [][]byte{rowKey}, txn1, sharedOpt)
			require.NoError(t, err)

			// Get the lock table
			v, err := l.getLockTable(context.Background(), 0, tableID)
			require.NoError(t, err)
			lt := v.(*localLockTable)

			// Verify it's a row lock and call setModePairedRangeLock (should be no-op)
			lt.mu.Lock()
			rowLock, _ := lt.mu.store.Get(rowKey)
			require.True(t, rowLock.isLockRow(), "should be row lock")

			// This should be a no-op (early return)
			lt.setModePairedRangeLock(rowKey, rowLock, pb.LockMode_Exclusive)
			lt.mu.Unlock()

			// Cleanup
			require.NoError(t, l.Unlock(ctx, txn1, timestamp.Timestamp{PhysicalTime: 1}))
		},
	)
}

// TestRangeLockWithInterleavedRowLocks verifies that setModePairedRangeLock
// correctly scans past interleaved row locks to find the paired range entry.
// Row locks outside the range can coexist with range locks in the btree.
func TestRangeLockWithInterleavedRowLocks(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l := s[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			tableID := uint64(10)
			// Use non-overlapping keys: row lock at key 0 (before range), range [1, 10]
			rowKey := []byte{0}
			rangeStart := []byte{1}
			rangeEnd := []byte{10}
			rangeRows := [][]byte{rangeStart, rangeEnd}

			sharedRangeOpt := pb.LockOptions{
				Granularity: pb.Granularity_Range,
				Mode:        pb.LockMode_Shared,
				Policy:      pb.WaitPolicy_Wait,
			}
			exclusiveRangeOpt := pb.LockOptions{
				Granularity: pb.Granularity_Range,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}
			sharedRowOpt := newTestRowSharedOptions()

			// Step 1: txn1 acquires Shared row lock on key 0 (before the range)
			txn1 := newTestTxnID(1)
			_, err := l.Lock(ctx, tableID, [][]byte{rowKey}, txn1, sharedRowOpt)
			require.NoError(t, err)

			// Step 2: txn2 acquires Shared range lock [1, 10]
			txn2 := newTestTxnID(2)
			_, err = l.Lock(ctx, tableID, rangeRows, txn2, sharedRangeOpt)
			require.NoError(t, err)

			// Verify btree structure: [0:row] [1:range-start] [10:range-end]
			v, err := l.getLockTable(context.Background(), 0, tableID)
			require.NoError(t, err)
			lt := v.(*localLockTable)

			lt.mu.RLock()
			rowLock, ok1 := lt.mu.store.Get(rowKey)
			startLock, ok2 := lt.mu.store.Get(rangeStart)
			endLock, ok3 := lt.mu.store.Get(rangeEnd)
			lt.mu.RUnlock()
			require.True(t, ok1 && ok2 && ok3, "all three locks should exist")
			require.True(t, rowLock.isLockRow(), "should be row lock")
			require.True(t, startLock.isLockRangeStart(), "should be range-start")
			require.True(t, endLock.isLockRangeEnd(), "should be range-end")

			// Step 3: txn3 requests Exclusive range lock → blocked by txn2's Shared range lock
			txn3 := newTestTxnID(3)
			txn3Done := make(chan struct{}, 1)
			go func() {
				_, err := l.Lock(ctx, tableID, rangeRows, txn3, exclusiveRangeOpt)
				require.NoError(t, err)
				txn3Done <- struct{}{}
			}()
			time.Sleep(100 * time.Millisecond)

			// Step 4: txn2 releases range lock → txn3 promoted to Exclusive holder
			require.NoError(t, l.Unlock(ctx, txn2, timestamp.Timestamp{PhysicalTime: 1}))
			select {
			case <-txn3Done:
			case <-time.After(5 * time.Second):
				t.Fatal("txn3 (Exclusive) did not acquire range lock in time")
			}

			// Step 5: Verify both range-start and range-end are Exclusive
			// The row lock at key 0 should not interfere with setModePairedRangeLock
			lt.mu.RLock()
			startLock, _ = lt.mu.store.Get(rangeStart)
			endLock, _ = lt.mu.store.Get(rangeEnd)
			lt.mu.RUnlock()
			require.False(t, startLock.isShared(),
				"range-start should be Exclusive after promotion")
			require.False(t, endLock.isShared(),
				"range-end should be Exclusive after promotion")

			// Cleanup
			require.NoError(t, l.Unlock(ctx, txn1, timestamp.Timestamp{PhysicalTime: 2}))
			require.NoError(t, l.Unlock(ctx, txn3, timestamp.Timestamp{PhysicalTime: 3}))
		},
	)
}

func TestHandleLockConflictLockedLogOnMissingRangeKey(t *testing.T) {
	runtime.RunTest(
		"",
		func(rt runtime.Runtime) {
			reuse.RunReuseTests(func() {
				logger := getLogger("")
				events := newWaiterEvents(1, nil, nil, time.Second, nil, logger)
				defer events.close()

				lt := newLocalLockTable(
					pb.LockTable{Table: 1, ServiceID: "test"},
					nil,
					events,
					rt.Clock(),
					nil,
					logger,
				).(*localLockTable)

				txnID := []byte("txn1")
				fsp := newFixedSlicePool(2)
				txn := newActiveTxn(txnID, string(txnID), fsp, "")
				defer reuse.Free(txn, nil)

				waitTxn := pb.WaitTxn{TxnID: txnID, CreatedOn: "test"}
				w := acquireWaiter(waitTxn, "test", logger)

				staleKey := []byte{2}
				staleWaiters := newWaiterQueue()
				staleWaiters.init(logger)
				staleWaiters.put(w)
				lt.mu.store.Add(staleKey, Lock{
					createAt: time.Now(),
					holders:  newHolders(),
					waiters:  staleWaiters,
				})

				recreatedKey := []byte{99}
				recreatedHolders := newHolders()
				recreatedHolders.add(pb.WaitTxn{TxnID: []byte("recreated-holder"), CreatedOn: "test"})
				recreatedWaiters := newWaiterQueue()
				recreatedWaiters.init(logger)
				lt.mu.store.Add(recreatedKey, Lock{
					createAt: time.Now(),
					holders:  recreatedHolders,
					waiters:  recreatedWaiters,
				})

				nextConflictKey := []byte{1}
				holderTxnID := []byte("holder1")
				holderWaitTxn := pb.WaitTxn{TxnID: holderTxnID, CreatedOn: "test"}
				h := newHolders()
				h.add(holderWaitTxn)
				wq := newWaiterQueue()
				wq.init(logger)
				conflictWith := Lock{
					createAt: time.Now(),
					holders:  h,
					waiters:  wq,
				}
				lt.mu.store.Add(nextConflictKey, conflictWith)

				c := &lockContext{
					ctx:     context.Background(),
					txn:     txn,
					waitTxn: waitTxn,
					opts: LockOptions{
						LockOptions: pb.LockOptions{
							Granularity: pb.Granularity_Range,
							Mode:        pb.LockMode_Exclusive,
							Policy:      pb.WaitPolicy_Wait,
						},
					},
					w:                w,
					rangeLastWaitKey: recreatedKey,
					result:           pb.Result{},
				}

				// rangeLastWaitKey was removed by a range merge and recreated by
				// another txn without this waiter, but the waiter may still exist
				// in another stale no-holder lock queue.
				err := lt.handleLockConflictLocked(c, nextConflictKey, conflictWith)
				assert.NoError(t, err)
				assert.Equal(t, nextConflictKey, c.rangeLastWaitKey)
				_, ok := lt.mu.store.Get(staleKey)
				assert.False(t, ok)
				_, ok = lt.mu.store.Get(recreatedKey)
				assert.True(t, ok)

				nextLock, ok := lt.mu.store.Get(nextConflictKey)
				require.True(t, ok)
				assert.Equal(t, 1, nextLock.waiters.size())
			})
		},
	)
}
