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
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCloseLocalLockTable(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l := s[0]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			mustAddTestLock(
				t,
				ctx,
				l,
				1,
				[]byte{1},
				[][]byte{{1}},
				pb.Granularity_Row)
			v, err := l.getLockTable(1)
			require.NoError(t, err)
			v.close()
			lt := v.(*localLockTable)
			lt.mu.Lock()
			defer lt.mu.Unlock()
			assert.True(t, lt.mu.closed)
			assert.Equal(t, 0, lt.mu.store.Len())
		})
}

func TestCloseLocalLockTableWithBlockedWaiter(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l := s[0]
			ctx, cancel := context.WithTimeout(context.Background(),
				time.Second*10)
			defer cancel()

			mustAddTestLock(
				t,
				ctx,
				l,
				1,
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
					1,
					[][]byte{{1}},
					[]byte{2},
					getRowOptions(),
				)
				require.Equal(t, ErrLockTableNotFound, err)
			}()

			// txn3 wait txn2 or txn1
			go func() {
				defer wg.Done()
				_, err := l.Lock(
					ctx,
					1,
					[][]byte{{1}},
					[]byte{3},
					getRowOptions(),
				)
				require.Equal(t, ErrLockTableNotFound, err)
			}()

			v, err := l.getLockTable(1)
			require.NoError(t, err)
			lt := v.(*localLockTable)
			for {
				lt.mu.RLock()
				lock, ok := lt.mu.store.Get([]byte{1})
				require.True(t, ok)
				lt.mu.RUnlock()
				if lock.waiter.waiters.len() == 2 {
					break
				}
				time.Sleep(time.Millisecond * 10)
			}

			v.close()
			wg.Wait()
		})
}

func TestCloseLocalLockTableWithBlockedSameTxnWaiters(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l := s[0]
			ctx, cancel := context.WithTimeout(context.Background(),
				time.Second*10)
			defer cancel()

			mustAddTestLock(
				t,
				ctx,
				l,
				1,
				[]byte{1},
				[][]byte{{1}},
				pb.Granularity_Row)

			var wg sync.WaitGroup
			n := 10
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := l.Lock(
						ctx,
						1,
						[][]byte{{1}},
						[]byte{2},
						getRowOptions(),
					)
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockTableNotFound) ||
						moerr.IsMoErrCode(err, moerr.ErrInvalidState))
				}()
			}

			v, err := l.getLockTable(1)
			require.NoError(t, err)
			lt := v.(*localLockTable)
			for {
				lt.mu.RLock()
				lock, ok := lt.mu.store.Get([]byte{1})
				require.True(t, ok)
				if lock.waiter.waiters.len() == 1 &&
					len(lock.waiter.waiters.all()[0].sameTxnWaiters) == n-1 {
					lt.mu.RUnlock()
					break
				}
				lt.mu.RUnlock()
				time.Sleep(time.Millisecond * 10)
			}

			v.close()
			wg.Wait()
		})
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

			table := uint64(1)
			for _, c := range cases {
				stopper := stopper.NewStopper("")
				v, err := l.getLockTable(table)
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
						w := acquireWaiter("", []byte(txnID))
						w.setStatus("", blocking)
						lock.waiter.add("", w)
						wg.Add(1)
						require.NoError(t, stopper.RunTask(func(ctx context.Context) {
							wg.Done()
							w.wait(ctx, "")
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
							assert.Equal(t, 0, l.waiter.waiters.len())
						} else {
							var waitTxns []string
							l.waiter.waiters.iter(func(v *waiter) bool {
								waitTxns = append(waitTxns, string(v.txnID))
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
				assert.Equal(t, fn(c.mergedLocks), fn(txn.holdLocks[table].slice().all()))

				assert.NoError(t, l.Unlock(ctx, []byte(c.txnID), timestamp.Timestamp{}))
				stopper.Stop()
				table++
			}
		})
}

func TestMergeRangeWithConflict(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l := s[0]
			v, err := l.getLockTable(1)
			require.NoError(t, err)
			lt := v.(*localLockTable)

			ctx, cancel := context.WithTimeout(context.Background(),
				time.Second*10)
			defer cancel()

			_, err = l.Lock(ctx, 1, [][]byte{{1}}, []byte("txn1"), pb.LockOptions{})
			require.NoError(t, err)
			_, err = l.Lock(ctx, 1, [][]byte{{2}}, []byte("txn1"), pb.LockOptions{})
			require.NoError(t, err)
			_, err = l.Lock(ctx, 1, [][]byte{{3}}, []byte("txn2"), pb.LockOptions{})
			require.NoError(t, err)
			var wg sync.WaitGroup
			wg.Add(3)

			go func() {
				defer wg.Done()
				_, err = l.Lock(ctx, 1, [][]byte{{1}}, []byte("txn3"), getRowOptions())
				require.NoError(t, err)

				defer func() {
					require.NoError(t, l.Unlock(ctx, []byte("txn3"), timestamp.Timestamp{}))
				}()
			}()
			waitWaiters(t, l, 1, []byte{1}, 1)

			go func() {
				defer wg.Done()
				_, err = l.Lock(ctx, 1, [][]byte{{2}}, []byte("txn4"), getRowOptions())
				require.NoError(t, err)

				defer func() {
					require.NoError(t, l.Unlock(ctx, []byte("txn4"), timestamp.Timestamp{}))
				}()
			}()
			waitWaiters(t, l, 1, []byte{2}, 1)

			go func() {
				defer wg.Done()
				_, err = l.Lock(ctx, 1, [][]byte{{1}, {3}}, []byte("txn1"), getRangeOptions())
				require.NoError(t, err)

				defer func() {
					require.NoError(t, l.Unlock(ctx, []byte("txn1"), timestamp.Timestamp{}))
				}()

				lt.mu.Lock()
				defer lt.mu.Unlock()

				var locks [][]byte
				var w *waiter
				lt.mu.store.Iter(func(b []byte, l Lock) bool {
					locks = append(locks, b)
					w = l.waiter
					return true
				})
				assert.Equal(t, [][]byte{{1}, {3}}, locks)
				assert.Equal(t, 2, w.waiters.len())
				assert.Equal(t, []byte("txn3"), w.waiters.all()[0].txnID)
				assert.Equal(t, []byte("txn4"), w.waiters.all()[1].txnID)
			}()
			waitWaiters(t, l, 1, []byte{3}, 1)

			lt.mu.Lock()
			var rows [][]byte
			var waiters []*waiter
			lt.mu.store.Iter(func(b []byte, l Lock) bool {
				rows = append(rows, b)
				waiters = append(waiters, l.waiter)
				return true
			})
			lt.mu.Unlock()
			assert.Equal(t, [][]byte{{1}, {2}, {3}}, rows)
			assert.Equal(t, 1, waiters[0].waiters.len())
			assert.Equal(t, []byte("txn3"), waiters[0].waiters.all()[0].txnID)
			assert.Equal(t, 1, waiters[1].waiters.len())
			assert.Equal(t, []byte("txn4"), waiters[1].waiters.all()[0].txnID)
			assert.Equal(t, 1, waiters[2].waiters.len())
			assert.Equal(t, []byte("txn1"), waiters[2].waiters.all()[0].txnID)

			require.NoError(t, l.Unlock(ctx, []byte("txn2"), timestamp.Timestamp{}))
			wg.Wait()
		})
}

func TestLocalLockTableMulitiRowLocksCannotMissIfFoundSelfTxn(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(_ *lockTableAllocator, s []*service) {
			l := s[0]
			ctx, cancel := context.WithTimeout(context.Background(),
				time.Second*10)
			defer cancel()

			mustAddTestLock(
				t,
				ctx,
				l,
				1,
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
					1,
					[]byte{1},
					[][]byte{{1}},
					pb.Granularity_Row)
			}()
			go func() {
				defer wg.Done()
				waitWaiters(t, l, 1, []byte{1}, 1)
				mustAddTestLock(
					t,
					ctx,
					l,
					1,
					[]byte{1},
					[][]byte{{1}, {2}},
					pb.Granularity_Row)
			}()

			waitWaiters(t, l, 1, []byte{1}, 1, 1)
			require.NoError(t, l.Unlock(ctx, []byte{2}, timestamp.Timestamp{}))

			wg.Wait()
			v, err := l.getLockTable(1)
			require.NoError(t, err)
			lt := v.(*localLockTable)
			lt.mu.Lock()
			defer lt.mu.Unlock()
			require.Equal(t, 2, lt.mu.store.Len())
		})
}

func getRowOptions() pb.LockOptions {
	return pb.LockOptions{Granularity: pb.Granularity_Row}
}

func getRangeOptions() pb.LockOptions {
	return pb.LockOptions{Granularity: pb.Granularity_Range}
}

func TestIssue9856(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
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
			for idx, r := range strings.Split(values, "\n") {
				getLogger().Info(">>>>>>>>>>>>>>", zap.Int("idx", idx))
				v := &target{}
				json.MustUnmarshal([]byte(r), v)
				_, err := l.Lock(ctx, 1, [][]byte{[]byte(v.Start), []byte(v.End)}, []byte("txn1"), option)
				require.NoError(t, err)
				vv, err := l.getLockTable(1)
				require.NoError(t, err)
				lt := vv.(*localLockTable)
				lt.mu.Lock()
				var keys []string
				lt.mu.store.Iter(func(b []byte, l Lock) bool {
					keys = append(keys, fmt.Sprintf("%s(%p)", string(b), l.waiter))
					return true
				})
				lt.mu.Unlock()
				getLogger().Info(">>>>>>>>>>>>>>, keys", zap.Int("idx", idx), zap.Strings("keys", keys))
			}
		},
	)
}

type target struct {
	Start string `json:"start"`
	End   string `json:"end"`
}
