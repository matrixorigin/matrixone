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
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
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

func TestGetLockHolder(t *testing.T) {
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

			table := uint64(10)
			row := []byte{1}
			txnID := []byte("txn1")
			option := pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			}

			_, err := l1.Lock(
				ctx,
				table,
				[][]byte{row},
				txnID,
				option)
			require.NoError(t, err)

			holder, ok, err := l2.GetLockHolder(ctx, table, row, option)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, txnID, holder.TxnID)

			_, ok, err = l2.GetLockHolder(ctx, table, []byte{2}, option)
			require.NoError(t, err)
			require.False(t, ok)

			require.NoError(t, l1.Unlock(
				ctx,
				txnID,
				timestamp.Timestamp{PhysicalTime: 1}))
		},
	)
}

func TestGetLockHolderReacquiresLockTableAfterBindChanged(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			l2 := s[1]

			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Second*10)
			defer cancel()

			table := uint64(11)
			row := []byte{1}
			holder := pb.WaitTxn{TxnID: []byte("txn-new-holder")}
			oldBind := pb.LockTable{
				Group:       0,
				Table:       table,
				OriginTable: table,
				ServiceID:   "stale-service",
				Version:     1,
				Valid:       true,
			}
			newBind := pb.LockTable{
				Group:       0,
				Table:       table,
				OriginTable: table,
				ServiceID:   l2.serviceID,
				Version:     2,
				Valid:       true,
			}

			newTable := &getLockHolderTestTable{
				bind:   newBind,
				holder: holder,
				found:  true,
			}
			client := &getLockHolderBindChangedClient{
				t:        t,
				wantBind: oldBind,
				newBind:  newBind,
			}
			staleRemote := newRemoteLockTable(
				l2.serviceID,
				time.Second,
				oldBind,
				client,
				func(bind pb.LockTable) {
					require.Equal(t, newBind, bind)
					l2.tableGroups.set(bind.Group, bind.Table, newTable)
				},
				l2.logger,
			)
			l2.tableGroups.set(oldBind.Group, oldBind.Table, staleRemote)

			got, ok, err := l2.GetLockHolder(ctx, table, row, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			})
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, holder, got)
			require.Equal(t, 1, client.calls)
			require.Equal(t, 1, newTable.calls)
			require.Same(t, newTable, l2.tableGroups.get(newBind.Group, newBind.Table))
		},
	)
}

func TestGetLockHolderHoldsBindChangeLockAcrossLocalLookup(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1"},
		func(alloc *lockTableAllocator, s []*service) {
			l := s[0]

			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Second*10)
			defer cancel()

			table := uint64(12)
			row := []byte{1}
			holder := pb.WaitTxn{TxnID: []byte("txn-holder")}
			bind := pb.LockTable{
				Group:       0,
				Table:       table,
				OriginTable: table,
				ServiceID:   l.serviceID,
				Version:     1,
				Valid:       true,
			}
			lockTable := &getLockHolderTestTable{
				bind:   bind,
				holder: holder,
				found:  true,
				onGetLockHolder: func() {
					if l.bindChangeMu.TryLock() {
						l.bindChangeMu.Unlock()
						require.Fail(t, "GetLockHolder must hold bindChangeMu while reading a local lock table")
					}
				},
			}
			l.tableGroups.set(bind.Group, bind.Table, lockTable)

			got, ok, err := l.GetLockHolder(ctx, table, row, pb.LockOptions{
				Granularity: pb.Granularity_Row,
				Mode:        pb.LockMode_Exclusive,
				Policy:      pb.WaitPolicy_Wait,
			})
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, holder, got)
			require.Equal(t, 1, lockTable.calls)
		},
	)
}

type getLockHolderBindChangedClient struct {
	t        *testing.T
	wantBind pb.LockTable
	newBind  pb.LockTable
	calls    int
}

func (c *getLockHolderBindChangedClient) Send(
	ctx context.Context,
	req *pb.Request) (*pb.Response, error) {
	c.calls++
	require.Equal(c.t, pb.Method_GetLockHolder, req.Method)
	require.Equal(c.t, c.wantBind, req.LockTable)
	resp := acquireResponse()
	resp.NewBind = &c.newBind
	return resp, nil
}

func (c *getLockHolderBindChangedClient) AsyncSend(
	ctx context.Context,
	req *pb.Request) (*morpc.Future, error) {
	panic("unexpected async send")
}

func (c *getLockHolderBindChangedClient) Close() error {
	return nil
}

type getLockHolderTestTable struct {
	bind            pb.LockTable
	holder          pb.WaitTxn
	found           bool
	calls           int
	onGetLockHolder func()
}

func (l *getLockHolderTestTable) lock(
	ctx context.Context,
	txn *activeTxn,
	rows [][]byte,
	options LockOptions,
	cb func(pb.Result, error)) {
	panic("unexpected lock")
}

func (l *getLockHolderTestTable) unlock(
	txn *activeTxn,
	ls *cowSlice,
	commitTS timestamp.Timestamp,
	mutations ...pb.ExtraMutation) {
	panic("unexpected unlock")
}

func (l *getLockHolderTestTable) getLock(
	key []byte,
	txn pb.WaitTxn,
	fn func(Lock)) {
	panic("unexpected getLock")
}

func (l *getLockHolderTestTable) getLockHolder(
	ctx context.Context,
	key []byte) (pb.WaitTxn, bool, error) {
	l.calls++
	if l.onGetLockHolder != nil {
		l.onGetLockHolder()
	}
	return l.holder, l.found, nil
}

func (l *getLockHolderTestTable) getBind() pb.LockTable {
	return l.bind
}

func (l *getLockHolderTestTable) close(reason closeReason) {}

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
			bind1, err := l1.GetLockTableBind(0, table)
			require.NoError(t, err)

			bind2, err := l2.GetLockTableBind(0, table)
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

			s.cfg.TxnIterFunc = func(f func([]byte) bool) {
				f(txn1)
				f(txn2)
				f(txn3)
				f(txn4)
				f(txn5)
			}

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
				_, err := s.Lock(ctx, table, rows, txn4, exclusiveOpts)
				require.NoError(t, err)
				require.NoError(t, s.Unlock(ctx, txn4, timestamp.Timestamp{}))
			}()

			// txn5 wait txn2 and txn3 on [2,3]
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := s.Lock(ctx, table, rangeRows, txn5, exclusiveOpts)
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

			// txn1 : 1
			// txn2 : 2, 3 shares
			// txn3 : 2, 3 shares
			// txn4 -> txn1 on 1
			// txn5 -> txn2, txn3 on 2, 3
			n := 0
			s.IterLocks(func(tableID uint64, keys [][]byte, lock Lock) bool {
				require.Equal(t, table, tableID)
				require.Equal(t, expectedKeys[n], keys)
				require.Equal(t, expectedModes[n], lock.GetLockMode())

				i := 0
				lock.IterHolders(func(holder pb.WaitTxn) bool {
					if n == 1 {
						// holders in map
						require.True(t, bytes.Equal(expectedHolders[n][0], holder.TxnID) || bytes.Equal(expectedHolders[n][1], holder.TxnID))
					} else {
						require.Equal(t, expectedHolders[n][i], holder.TxnID)
					}
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
