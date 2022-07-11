// Copyright 2021 - 2022 Matrix Origin
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

package service

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/assert"
)

func TestAcquireAndCloseWaiter(t *testing.T) {
	checker := func(w *waiter) {
		assert.False(t, w.mu.closed)
		assert.False(t, w.mu.notified)
	}

	w := acquireWaiter()
	assert.Equal(t, 1, w.mu.ref)
	checker(w)

	w.ref()
	w.notify(txn.TxnStatus_Active)
	w.close()
	assert.True(t, w.mu.closed)
	assert.True(t, w.mu.notified)

	w2 := w
	w.unref()
	assert.Equal(t, 0, w2.mu.ref)
	checker(w2)
}

func TestWaiterNotify(t *testing.T) {
	w := acquireWaiter()
	defer w.close()

	w.notify(txn.TxnStatus_Committed)
	s, err := w.wait(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, txn.TxnStatus_Committed, s)
}

func TestWaiterNotifyMoreTimesWillPanic(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			assert.Fail(t, "must panic")
		}
	}()

	w := acquireWaiter()
	defer w.close()

	w.notify(txn.TxnStatus_Committed)
	w.notify(txn.TxnStatus_Aborted)
}

func TestWaiterTimeout(t *testing.T) {
	w := acquireWaiter()
	defer w.close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := w.wait(ctx)
	assert.Error(t, err)
}

func TestNotifyWithEmptyWaiters(t *testing.T) {
	nt := acquireNotifier()
	defer nt.close(txn.TxnStatus_Aborted)

	assert.Equal(t, 0, nt.notify(txn.TxnStatus_Active))
}

func TestNotify(t *testing.T) {
	nt := acquireNotifier()
	defer nt.close(txn.TxnStatus_Aborted)

	w := acquireWaiter()
	defer func() {
		w.close()
		assert.Equal(t, 0, w.mu.ref)
		assert.False(t, w.mu.closed)
		assert.False(t, w.mu.notified)
	}()

	nt.addWaiter(w, txn.TxnStatus_Prepared)
	assert.Equal(t, 0, nt.notify(txn.TxnStatus_Active))
	assert.Equal(t, 1, nt.notify(txn.TxnStatus_Prepared))

	s, err := w.wait(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, txn.TxnStatus_Prepared, s)
}

func TestNotifyWithFinalStatus(t *testing.T) {
	nt := acquireNotifier()

	w := acquireWaiter()
	defer w.close()

	nt.addWaiter(w, txn.TxnStatus_Prepared)

	nt.close(txn.TxnStatus_Aborted)

	s, err := w.wait(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, txn.TxnStatus_Aborted, s)
}

func TestNotifyAfterWaiterClose(t *testing.T) {
	nt := acquireNotifier()

	w := acquireWaiter()
	nt.addWaiter(w, txn.TxnStatus_Prepared)

	w.close()
	assert.Equal(t, 1, w.mu.ref)
	assert.True(t, w.mu.closed)
	assert.False(t, w.mu.notified)

	assert.Equal(t, 0, nt.notify(txn.TxnStatus_Prepared))

	assert.Equal(t, 0, w.mu.ref)
	assert.False(t, w.mu.closed)
	assert.False(t, w.mu.notified)
}
