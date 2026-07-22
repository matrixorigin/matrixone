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
	"context"
	"sync"
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

func TestAsyncLockWaitDeadlinePreservesCarriedAbsoluteBudget(t *testing.T) {
	createAt := time.Unix(100, 0)
	carriedDeadline := createAt.Add(250 * time.Millisecond)

	deadline, err := getLockWaitDeadline(createAt, LockOptions{
		LockOptions: pb.LockOptions{
			LockWaitTimeout:  1,
			LockWaitDeadline: carriedDeadline.UnixNano(),
		},
		remoteLockOwnerWaitTimeout: time.Second,
	})
	require.Equal(t, carriedDeadline, deadline)
	require.ErrorIs(t, err, ErrLockTimeout)

	ownerDeadline, err := getLockWaitDeadline(createAt, LockOptions{
		LockOptions: pb.LockOptions{
			LockWaitTimeout:  1,
			LockWaitDeadline: carriedDeadline.UnixNano(),
		},
		remoteLockOwnerWaitTimeout: 100 * time.Millisecond,
	})
	require.Equal(t, createAt.Add(100*time.Millisecond), ownerDeadline)
	require.ErrorIs(t, err, ErrRemoteLockWaitTimeout)
}
func TestWaiterEventsTimeoutDoesNotSendToFullOwnEventChannel(t *testing.T) {
	logger := getLogger("")
	events := &waiterEvents{
		logger:       logger,
		eventC:       make(chan *lockContext, 1),
		checkOrphanC: make(chan checkOrphan, 1),
	}
	events.eventC <- &lockContext{} // make the consumer's own queue full

	// Use a raw waiter rather than the reuse pool: this test exercises check()
	// directly and must not return a pooled object outside RunReuseTests.
	w := newWaiter()
	w.txn = pb.WaitTxn{TxnID: []byte("waiter")}
	w.beforeSwapStatusAdjustFunc = func() {}
	require.Equal(t, int32(1), w.ref("test", logger))
	w.setStatus(blocking)
	w.startWait()
	w.waitAt.Store(time.Now().Add(-time.Second))
	w.lockWaitTimeout = time.Millisecond
	w.lockWaitTimeoutErr = ErrLockTimeout
	conflictKey := []byte{1}
	w.conflictKey.Store(&conflictKey)

	resultC := make(chan error, 1)
	c := &lockContext{
		txn: &activeTxn{RWMutex: &sync.RWMutex{}},
		w:   w,
		lockFunc: func(c *lockContext, _ bool) {
			resultC <- c.w.wait(context.Background(), logger).err
		},
	}
	w.event = event{c: c, eventC: events.eventC}
	events.addToLazyCheckDeadlockC(w)

	checked := make(chan struct{})
	go func() {
		events.check(time.Hour)
		close(checked)
	}()
	select {
	case <-checked:
	case <-time.After(time.Second):
		t.Fatal("waiterEvents.check blocked sending to its own full event channel")
	}
	require.ErrorIs(t, <-resultC, ErrLockTimeout)
	require.Len(t, events.eventC, 1)
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
