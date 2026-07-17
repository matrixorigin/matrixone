// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lockservice

import (
	"testing"
	"time"

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/require"
)

func TestWaiterEventsRemoveBlockedWaiter(t *testing.T) {
	w := newWaiter()
	w.txn = pb.WaitTxn{TxnID: []byte("waiter")}
	require.Equal(t, int32(1), w.ref("test", nil))

	events := &waiterEvents{}
	events.addToLazyCheckDeadlockC(w)
	require.Equal(t, int32(2), w.refCount.Load())

	events.removeBlockedWaiter(w)

	events.mu.RLock()
	require.Empty(t, events.mu.blockedWaiters)
	events.mu.RUnlock()
	require.Equal(t, int32(1), w.refCount.Load())
}

func TestWaiterEventsCloseDropsBlockedWaiters(t *testing.T) {
	logger := getLogger("")
	events := newWaiterEvents(0, nil, nil, time.Second, nil, logger)
	w := newWaiter()
	w.txn = pb.WaitTxn{TxnID: []byte("waiter")}
	require.Equal(t, int32(1), w.ref("test", logger))

	events.addToLazyCheckDeadlockC(w)
	require.Equal(t, int32(2), w.refCount.Load())

	events.close()

	events.mu.RLock()
	require.Empty(t, events.mu.blockedWaiters)
	events.mu.RUnlock()
	require.Equal(t, int32(1), w.refCount.Load())

	events.removeBlockedWaiter(w)
	require.Equal(t, int32(1), w.refCount.Load())
}
