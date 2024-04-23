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
	"testing"

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPut(t *testing.T) {
	q := newWaiterQueue()
	defer q.close(notifyValue{})
	w := acquireWaiter(pb.WaitTxn{TxnID: []byte("w")})
	defer w.close()
	q.put(w)
	assert.Equal(t, 1, q.size())
}

func TestReset(t *testing.T) {
	q := newWaiterQueue()
	defer q.close(notifyValue{})

	w1 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w1")})
	defer w1.close()
	w2 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w2")})
	defer w2.close()
	w3 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w3")})
	defer w3.close()

	q.put(w1, w2, w3)
	assert.Equal(t, 3, q.size())

	q.reset()
	assert.Equal(t, 0, q.size())
}

func TestIterTxns(t *testing.T) {
	q := newWaiterQueue()
	defer q.close(notifyValue{})

	w1 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w1")})
	defer w1.close()
	w2 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w2")})
	defer w2.close()
	w3 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w3")})
	defer w3.close()

	q.put(w1, w2, w3)

	var values [][]byte
	v := 0
	q.iter(func(w *waiter) bool {
		values = append(values, w.txn.TxnID)
		v++
		return v < 2
	})
	assert.Equal(t, [][]byte{[]byte("w1"), []byte("w2")}, values)
}

func TestIterTxnsCannotReadUncommitted(t *testing.T) {
	q := newWaiterQueue()
	defer q.close(notifyValue{})

	w1 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w1")})
	defer w1.close()

	q.put(w1)

	w2 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w2")})
	defer w2.close()
	q.beginChange()
	q.put(w2)

	var values [][]byte
	q.iter(func(w *waiter) bool {
		values = append(values, w.txn.TxnID)
		return true
	})
	assert.Equal(t, [][]byte{[]byte("w1")}, values)
	q.rollbackChange()
}

func TestChange(t *testing.T) {
	q := newWaiterQueue().(*sliceBasedWaiterQueue)
	defer q.close(notifyValue{})

	w1 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w1")})
	defer w1.close()

	q.put(w1)

	q.beginChange()
	assert.Equal(t, 1, q.beginChangeIdx)

	w2 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w2")})
	defer w2.close()
	w3 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w3")})
	defer w3.close()

	q.put(w2, w3)
	assert.Equal(t, 3, len(q.waiters))

	q.rollbackChange()
	assert.Equal(t, 1, len(q.waiters))
	assert.Equal(t, -1, q.beginChangeIdx)

	q.beginChange()
	assert.Equal(t, 1, q.beginChangeIdx)

	w4 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w4")})
	defer w4.close()
	w5 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w5")})
	defer w5.close()

	q.put(w4, w5)
	assert.Equal(t, 3, len(q.waiters))

	q.commitChange()
	assert.Equal(t, 3, len(q.waiters))
	assert.Equal(t, -1, q.beginChangeIdx)
}

func TestChangeRef(t *testing.T) {
	q := newWaiterQueue().(*sliceBasedWaiterQueue)
	defer q.close(notifyValue{})

	w1 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w1")})
	defer w1.close()

	q.beginChange()
	q.put(w1)
	assert.Equal(t, int32(2), w1.refCount.Load())
	q.rollbackChange()
	assert.Equal(t, int32(1), w1.refCount.Load())

	q.beginChange()
	q.put(w1)
	assert.Equal(t, int32(2), w1.refCount.Load())
	q.commitChange()
	assert.Equal(t, int32(2), w1.refCount.Load())
}

func TestSkipCompletedWaiters(t *testing.T) {
	q := newWaiterQueue()

	// w1 will skipped
	w1 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w1")})
	w1.setStatus(completed)
	defer w1.close()

	// w2 get the notify
	w2 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w2")})
	w2.setStatus(blocking)
	defer func() {
		w2.wait(context.Background())
		w2.close()
	}()

	// w3 get notify when queue closed
	w3 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w3")})
	w3.setStatus(blocking)
	defer func() {
		w3.wait(context.Background())
		w3.close()
	}()

	q.put(w1, w2, w3)

	q.notify(notifyValue{})
	ws := make([]*waiter, 0, q.size())
	q.iter(func(w *waiter) bool {
		ws = append(ws, w)
		return true
	})
	assert.Equal(t, 2, len(ws))
	assert.Equal(t, []byte("w2"), ws[0].txn.TxnID)
	assert.Equal(t, []byte("w3"), ws[1].txn.TxnID)

	q.close(notifyValue{})
}

func TestCanGetCommitTSInWaitQueue(t *testing.T) {
	q := newWaiterQueue()
	defer q.close(notifyValue{})

	w2 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w2")})
	w2.setStatus(blocking)
	defer w2.close()

	w3 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w3")})
	w3.setStatus(blocking)
	defer w3.close()

	w4 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w4")})
	w4.setStatus(blocking)
	defer func() {
		w4.close()
	}()

	w5 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w5")})
	w5.setStatus(blocking)
	defer w5.close()

	q.put(w2, w3, w4, w5)

	// commit at 1
	q.notify(notifyValue{ts: timestamp.Timestamp{PhysicalTime: 1}})

	// w2 get notify and abort
	assert.Equal(t, int64(1), w2.wait(context.Background()).ts.PhysicalTime)
	q.removeByTxnID(w2.txn.TxnID)
	q.notify(notifyValue{})

	// w3 get notify and commit at 3
	assert.Equal(t, int64(1), w3.wait(context.Background()).ts.PhysicalTime)
	q.removeByTxnID(w3.txn.TxnID)
	q.notify(notifyValue{ts: timestamp.Timestamp{PhysicalTime: 3}})

	// w4 get notify and commit at 2
	assert.Equal(t, int64(3), w4.wait(context.Background()).ts.PhysicalTime)
	q.removeByTxnID(w4.txn.TxnID)
	q.notify(notifyValue{ts: timestamp.Timestamp{PhysicalTime: 2}})

	// w5 get notify
	assert.Equal(t, int64(3), w5.wait(context.Background()).ts.PhysicalTime)
	q.removeByTxnID(w5.txn.TxnID)
}

func TestMoveToCannotCloseWaiter(t *testing.T) {
	from := newWaiterQueue()
	defer from.close(notifyValue{})

	to := newWaiterQueue()

	w1 := acquireWaiter(pb.WaitTxn{TxnID: []byte("w1")})
	defer w1.close()

	from.put(w1)
	require.Equal(t, int32(2), w1.refCount.Load())

	to.beginChange()
	from.moveTo(to)
	require.Equal(t, int32(3), w1.refCount.Load())
	to.rollbackChange()

	require.Equal(t, int32(2), w1.refCount.Load())
}
