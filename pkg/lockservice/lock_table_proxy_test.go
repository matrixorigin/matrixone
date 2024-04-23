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
	"fmt"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestProxySharedLock(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)
			s1 := s[0]
			s2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
			defer cancel()

			option := newTestRowSharedOptions()
			rows := newTestRows(1)
			txn1 := newTestTxnID(1)
			txn2 := newTestTxnID(2)
			txn3 := newTestTxnID(3)
			txn4 := newTestTxnID(4)
			txn5 := newTestTxnID(5)

			s1.cfg.EnableRemoteLocalProxy = true
			_, err := s1.Lock(ctx, tableID, rows, txn1, option)
			require.NoError(t, err, err)
			require.NoError(t, s1.Unlock(ctx, txn1, timestamp.Timestamp{}))

			v := s1.tableGroups.get(0, tableID)
			lt := v.(*localLockTable)

			// s2 will enable shared remote proxy
			s2.cfg.EnableRemoteLocalProxy = true
			_, err = s2.Lock(ctx, tableID, rows, txn2, option)
			require.NoError(t, err)
			checkLock(t, lt, rows[0], [][]byte{txn2}, nil, nil)
			v = s2.tableGroups.get(0, tableID)
			ltp := v.(*localLockTableProxy)
			require.Equal(t, ltp.mu.currentHolder[string(rows[0])], txn2)
			require.Equal(t, 1, len(ltp.mu.holders[string(rows[0])].txns))
			require.Equal(t, ltp.mu.holders[string(rows[0])].txns[0].txnID, txn2)
			require.True(t, ltp.mu.holders[string(rows[0])].cbs[0] == nil)
			require.True(t, ltp.mu.holders[string(rows[0])].waiters[0] == nil)

			_, err = s2.Lock(ctx, tableID, rows, txn3, option)
			require.NoError(t, err)
			checkLock(t, lt, rows[0], [][]byte{txn2}, nil, nil)
			require.Equal(t, ltp.mu.currentHolder[string(rows[0])], txn2)
			require.Equal(t, 2, len(ltp.mu.holders[string(rows[0])].txns))
			require.Equal(t, ltp.mu.holders[string(rows[0])].txns[1].txnID, txn3)
			require.True(t, ltp.mu.holders[string(rows[0])].cbs[1] == nil)
			require.True(t, ltp.mu.holders[string(rows[0])].waiters[1] == nil)

			_, err = s2.Lock(ctx, tableID, rows, txn4, option)
			require.NoError(t, err)
			checkLock(t, lt, rows[0], [][]byte{txn2}, nil, nil)
			require.Equal(t, ltp.mu.currentHolder[string(rows[0])], txn2)
			require.Equal(t, 3, len(ltp.mu.holders[string(rows[0])].txns))
			require.Equal(t, ltp.mu.holders[string(rows[0])].txns[2].txnID, txn4)
			require.True(t, ltp.mu.holders[string(rows[0])].cbs[2] == nil)
			require.True(t, ltp.mu.holders[string(rows[0])].waiters[2] == nil)

			require.NoError(t, s2.Unlock(ctx, txn2, timestamp.Timestamp{}))
			checkLock(t, lt, rows[0], [][]byte{txn4}, nil, nil)
			require.Equal(t, ltp.mu.currentHolder[string(rows[0])], txn4)

			require.NoError(t, s2.Unlock(ctx, txn4, timestamp.Timestamp{}))
			checkLock(t, lt, rows[0], [][]byte{txn3}, nil, nil)
			require.Equal(t, ltp.mu.currentHolder[string(rows[0])], txn3)

			require.NoError(t, s2.Unlock(ctx, txn3, timestamp.Timestamp{}))
			checkLock(t, lt, rows[0], [][]byte{}, nil, nil)
			require.Empty(t, ltp.mu.currentHolder)

			_, err = s1.Lock(ctx, tableID, rows, txn5, newTestRowExclusiveOptions())
			require.NoError(t, err, err)
			require.NoError(t, s1.Unlock(ctx, txn5, timestamp.Timestamp{}))
		},
	)
}

func TestProxySharedUnlock(t *testing.T) {
	runLockServiceTests(
		t,
		[]string{"s1", "s2"},
		func(alloc *lockTableAllocator, s []*service) {
			tableID := uint64(10)
			s1 := s[0]
			s2 := s[1]
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000000)
			defer cancel()

			option := newTestRowSharedOptions()
			rows := newTestRows(1)
			txn1 := newTestTxnID(1)
			txn2 := newTestTxnID(2)
			txn3 := newTestTxnID(3)
			txn4 := newTestTxnID(3)

			s1.cfg.EnableRemoteLocalProxy = true
			_, err := s1.Lock(ctx, tableID, rows, txn1, option)
			require.NoError(t, err, err)
			require.NoError(t, s1.Unlock(ctx, txn1, timestamp.Timestamp{}))

			v := s1.tableGroups.get(0, tableID)
			lt := v.(*localLockTable)

			// s2 will enable shared remote proxy
			s2.cfg.EnableRemoteLocalProxy = true
			_, err = s2.Lock(ctx, tableID, rows, txn2, option)
			require.NoError(t, err)

			_, err = s2.Lock(ctx, tableID, rows, txn3, option)
			require.NoError(t, err)
			require.NoError(t, s2.Unlock(ctx, txn3, timestamp.Timestamp{}))

			require.NoError(t, s2.Unlock(ctx, txn2, timestamp.Timestamp{}))
			checkLock(t, lt, rows[0], [][]byte{}, nil, nil)

			_, err = s1.Lock(ctx, tableID, rows, txn4, newTestRowExclusiveOptions())
			require.NoError(t, err, err)
			require.NoError(t, s1.Unlock(ctx, txn4, timestamp.Timestamp{}))
		},
	)
}

func TestAAA(t *testing.T) {
	data := make([]byte, 100)

	offset := 0
	n := (*node)(unsafe.Pointer(&data[offset]))
	n.v1 = 1
	n.v2 = 2
	n.data[0].Store(3)
	n.data[1].Store(4)

	fmt.Printf("%+v\n", data)
	t.Fail()
}

type node struct {
	v1   uint32
	v2   uint32
	data [2]atomic.Uint32
}
