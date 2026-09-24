// Copyright 2022 Matrix Origin
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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTryHoldEmptyLockReturnsError(t *testing.T) {
	l := Lock{
		holders: newHolders(),
		waiters: newWaiterQueue(),
	}

	_, _, err := l.tryHold(nil, nil, nil)
	assert.ErrorIs(t, err, errEmptyLock)
}

func TestNewRowLock(t *testing.T) {
	txnID := []byte("1")
	opts := LockOptions{}
	opts.Mode = pb.LockMode_Exclusive
	l := newRowLock(getLogger(""), &lockContext{txn: &activeTxn{txnID: txnID}, opts: opts})
	assert.True(t, l.isLockRow())
	assert.Equal(t, pb.LockMode_Exclusive, l.GetLockMode())
}

func TestNewRangeLock(t *testing.T) {
	txnID := []byte("1")
	opts := LockOptions{}
	opts.Mode = pb.LockMode_Shared
	sl, el := newRangeLock(getLogger(""), &lockContext{txn: &activeTxn{txnID: txnID}, opts: opts})

	assert.Equal(t, pb.LockMode_Shared, sl.GetLockMode())
	assert.True(t, el.isLockRangeEnd())
	assert.Equal(t, pb.LockMode_Shared, el.GetLockMode())
}

func TestWriterFairSharedAdmissionPreservesQueuedOrder(t *testing.T) {
	reuse.RunReuseTests(func() {
		logger := getLogger("")
		l := newRowLock(logger, &lockContext{
			waitTxn: pb.WaitTxn{TxnID: []byte("holder")},
			opts: LockOptions{LockOptions: pb.LockOptions{
				Mode: pb.LockMode_Exclusive,
			}},
		})

		newQueued := func(id string, mode pb.LockMode) *waiter {
			w := acquireWaiter(pb.WaitTxn{TxnID: []byte(id)}, "test", logger)
			w.lockWaitMode = mode
			l.addWaiter(logger, w)
			return w
		}
		reader1 := newQueued("reader-1", pb.LockMode_Shared)
		reader2 := newQueued("reader-2", pb.LockMode_Shared)
		writer := newQueued("writer", pb.LockMode_Exclusive)
		reader2.setStatus(blocking)
		writer.setStatus(blocking)

		fairShared := func(id string, w *waiter) *lockContext {
			return &lockContext{
				txn:     &activeTxn{txnID: []byte(id)},
				waitTxn: pb.WaitTxn{TxnID: []byte(id)},
				w:       w,
				opts: LockOptions{LockOptions: pb.LockOptions{
					Mode:       pb.LockMode_Shared,
					WriterFair: true,
				}},
			}
		}

		// A fresh reader cannot pass the queued writer. Once the Exclusive
		// predecessor leaves, the queued readers before that writer must become
		// one compatible Shared cohort.
		require.False(t, l.canHold(fairShared("late-reader", nil)))
		l.holders.clear()
		require.True(t, l.canHold(fairShared("reader-1", reader1)))

		reader1Ctx := fairShared("reader-1", reader1)
		reader1Ctx.opts.WriterFair = false
		held, added, err := l.tryHold(
			logger,
			reader1Ctx,
			func() error { return nil },
		)
		require.NoError(t, err)
		require.True(t, held)
		require.True(t, added)
		var changed bool
		l, changed = l.setMode(pb.LockMode_Shared)
		require.True(t, changed)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, reader2.wait(ctx, logger).err,
			"admitting the first reader must wake the next reader in the leading cohort")
		reader2.resetWait(logger)
		require.True(t, l.canHold(fairShared("reader-2", reader2)))
		held, added, err = l.tryHold(
			logger,
			fairShared("reader-2", reader2),
			func() error { return nil },
		)
		require.NoError(t, err)
		require.True(t, held)
		require.True(t, added)
		require.Equal(t, blocking, writer.getStatus(),
			"the Shared cohort must stop at the first Exclusive waiter")

		// Re-entry by an existing holder must bypass the queued writer or the
		// account-creation transaction can deadlock with itself.
		held, added, err = l.tryHold(
			logger,
			fairShared("reader-1", nil),
			func() error { return nil },
		)
		require.NoError(t, err)
		require.True(t, held)
		require.False(t, added)

		removed, _ := l.waiters.remove(writer)
		require.True(t, removed)
		reader1.close("test", logger)
		reader2.close("test", logger)
		writer.close("test", logger)
		l.release()
	})
}

func TestWriterFairCancelLeadingWriterWakesSharedCohort(t *testing.T) {
	reuse.RunReuseTests(func() {
		logger := getLogger("")
		l := newRowLock(logger, &lockContext{
			waitTxn: pb.WaitTxn{TxnID: []byte("holder")},
			opts: LockOptions{LockOptions: pb.LockOptions{
				Mode: pb.LockMode_Shared,
			}},
		})
		writer := acquireWaiter(pb.WaitTxn{TxnID: []byte("writer")}, "test", logger)
		writer.lockWaitMode = pb.LockMode_Exclusive
		writer.setStatus(blocking)
		reader := acquireWaiter(pb.WaitTxn{TxnID: []byte("reader")}, "test", logger)
		reader.lockWaitMode = pb.LockMode_Shared
		reader.setStatus(blocking)
		l.addWaiter(logger, writer)
		l.addWaiter(logger, reader)

		removed, empty := l.removeWaiter(writer, logger)
		require.True(t, removed)
		require.False(t, empty)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, reader.wait(ctx, logger).err,
			"removing the leading writer must wake the newly leading Shared reader")

		removed, _ = l.waiters.remove(reader)
		require.True(t, removed)
		writer.close("test", logger)
		reader.close("test", logger)
		l.release()
	})
}

func BenchmarkHoldersContains(b *testing.B) {
	// Preparation phase: create holders and add 2000 txns
	h := newHolders()
	for i := 0; i < 2000; i++ {
		txn := pb.WaitTxn{
			TxnID: []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)},
		}
		h.add(txn)
	}

	// Reset timer to exclude preparation time
	b.ResetTimer()

	// Test searching for a non-existent txn
	txnID := []byte{0xFF, 0xFF, 0xFF, 0xFF} // A non-existent txn

	for i := 0; i < b.N; i++ {
		h.contains(txnID)
	}
}

func TestHoldersGetTxnSlice(t *testing.T) {
	// Test empty holders
	h := newHolders()
	slice := h.getTxnSlice()
	assert.Equal(t, 0, len(slice))

	// Test with multiple holders
	txn1 := pb.WaitTxn{TxnID: []byte("1")}
	txn2 := pb.WaitTxn{TxnID: []byte("2")}
	txn3 := pb.WaitTxn{TxnID: []byte("3")}

	h.add(txn1)
	h.add(txn2)
	h.add(txn3)

	slice = h.getTxnSlice()
	assert.Equal(t, 3, len(slice))

	// Create a map to check if all txns are present
	txnMap := make(map[string]bool)
	for _, txn := range slice {
		txnMap[string(txn.TxnID)] = true
	}

	assert.True(t, txnMap["1"])
	assert.True(t, txnMap["2"])
	assert.True(t, txnMap["3"])
}

func TestHoldersReplaceMigratesTxnKey(t *testing.T) {
	h := newHolders()
	from := pb.WaitTxn{TxnID: []byte("from")}
	to := pb.WaitTxn{TxnID: []byte("to")}
	h.add(from)

	h.replace(from.TxnID, to)

	assert.False(t, h.contains(from.TxnID))
	assert.True(t, h.contains(to.TxnID))
	assert.Equal(t, []pb.WaitTxn{to}, h.getTxnSlice())
}
