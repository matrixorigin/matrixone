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

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetWaitingList(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			l1 := s[0]
			l2 := s[1]

			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Second*10)
			defer cancel()
			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			// txn1
			_, err := l1.Lock(
				ctx,
				0,
				[][]byte{{1}},
				[]byte("txn1"),
				option)
			require.NoError(t, err)

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				// blocked by txn1
				res, err := l2.Lock(
					ctx,
					0,
					[][]byte{{1}},
					[]byte("txn2"),
					option)
				require.NoError(t, err)
				assert.True(t, !res.Timestamp.IsEmpty())
			}()

			waitWaiters(t, l1, 0, []byte{1}, 1)

			ok, waiters, err := l1.GetWaitingList(ctx, []byte("txn1"))
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, 1, len(waiters))

			ok, waiters, err = l2.GetWaitingList(ctx, []byte("txn1"))
			require.NoError(t, err)
			assert.False(t, ok)
			assert.Equal(t, 0, len(waiters))

			require.NoError(t, l1.Unlock(
				ctx,
				[]byte("txn1"),
				timestamp.Timestamp{PhysicalTime: 1}))
			wg.Wait()
		},
	)
}

func TestForceRefreshLockTableBinds(t *testing.T) {
	runBindChangedTests(
		t,
		false,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2 *service,
			table uint64) {
			l1.ForceRefreshLockTableBinds(nil, nil)
			l2.ForceRefreshLockTableBinds(nil, nil)

			mustAddTestLock(
				t,
				ctx,
				l1,
				table,
				[]byte("txn1"),
				[][]byte{{1}},
				pb.Granularity_Row,
			)

			mustAddTestLock(
				t,
				ctx,
				l2,
				table,
				[]byte("txn2"),
				[][]byte{{2}},
				pb.Granularity_Row,
			)
		},
	)
}

func TestGetLockTableBind(t *testing.T) {
	runBindChangedTests(
		t,
		false,
		func(
			ctx context.Context,
			alloc *lockTableAllocator,
			l1, l2 *service,
			table uint64) {
			bind1, err := l1.GetLockTableBind(table)
			require.NoError(t, err)

			bind2, err := l2.GetLockTableBind(table)
			require.NoError(t, err)

			assert.Equal(t, bind1, bind2)
		},
	)
}

func TestIterLocks(t *testing.T) {
	table := uint64(10)
	getRunner(false)(
		t,
		table,
		func(
			ctx context.Context,
			s *service,
			lt *localLockTable) {
			exclusiveOpts := newTestRowExclusiveOptions()
			sharedOpts := newTestRangeSharedOptions()
			txn1 := newTestTxnID(1)
			txn2 := newTestTxnID(2)
			txn3 := newTestTxnID(3)
			txn4 := newTestTxnID(4)
			txn5 := newTestTxnID(5)

			rows := newTestRows(1)
			rangeRows := newTestRows(2, 3)

			// txn1 hold 1
			_, err := s.Lock(ctx, table, rows, txn1, exclusiveOpts)
			require.NoError(t, err)

			// txn2 and txn3 hold [2,3]
			_, err = s.Lock(ctx, table, rangeRows, txn2, sharedOpts)
			require.NoError(t, err)
			_, err = s.Lock(ctx, table, rangeRows, txn3, sharedOpts)
			require.NoError(t, err)

			var wg sync.WaitGroup
			// txn4 wait txn1 on row1
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err = s.Lock(ctx, table, rows, txn4, exclusiveOpts)
				require.NoError(t, err)
				require.NoError(t, s.Unlock(ctx, txn4, timestamp.Timestamp{}))
			}()

			// txn5 wait txn2 and txn3 on [3,4]
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err = s.Lock(ctx, table, rangeRows, txn5, exclusiveOpts)
				require.NoError(t, err)
				require.NoError(t, s.Unlock(ctx, txn5, timestamp.Timestamp{}))
			}()

			require.NoError(t, waitLocalWaiters(lt, rows[0], 1))
			require.NoError(t, waitLocalWaiters(lt, rangeRows[0], 1))

			expectedKeys := make([][][]byte, 0, 2)
			expectedModes := make([]pb.LockMode, 0, 2)
			expectedHolders := make([][][]byte, 0, 2)
			expectedWaiters := make([][][]byte, 0, 2)

			expectedKeys = append(expectedKeys, newTestRows(1))
			expectedModes = append(expectedModes, pb.LockMode_Exclusive)
			expectedHolders = append(expectedHolders, [][]byte{txn1})
			expectedWaiters = append(expectedWaiters, [][]byte{txn4})

			expectedKeys = append(expectedKeys, newTestRows(2, 3))
			expectedModes = append(expectedModes, pb.LockMode_Shared)
			expectedHolders = append(expectedHolders, [][]byte{txn2, txn3})
			expectedWaiters = append(expectedWaiters, [][]byte{txn5})

			n := 0
			s.IterLocks(func(tableID uint64, keys [][]byte, lock Lock) bool {
				require.Equal(t, table, tableID)
				require.Equal(t, expectedKeys[n], keys)
				require.Equal(t, expectedModes[n], lock.GetLockMode())

				i := 0
				lock.IterHolders(func(holder pb.WaitTxn) bool {
					require.Equal(t, expectedHolders[n][i], holder.TxnID)
					i++
					return true
				})

				i = 0
				lock.IterWaiters(func(holder pb.WaitTxn) bool {
					require.Equal(t, expectedWaiters[n][i], holder.TxnID)
					i++
					return true
				})
				n++
				return true
			})

			require.NoError(t, s.Unlock(ctx, txn1, timestamp.Timestamp{}))
			require.NoError(t, s.Unlock(ctx, txn2, timestamp.Timestamp{}))
			require.NoError(t, s.Unlock(ctx, txn3, timestamp.Timestamp{}))
			wg.Wait()
		})
}
