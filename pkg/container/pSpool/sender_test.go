// Copyright 2026 Matrix Origin
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

package pSpool

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// TestPipelineSpoolForceCleanupRetainsUntilReceiversDrained verifies that
// ForceCleanup does NOT free spool memory while a receiver still has an
// unconsumed batch (a pending reference). Freeing it then would let that
// receiver later read emptied memory (silent batch loss / early EOS). Once every
// receiver has drained to its End-message, ForceCleanup reclaims the memory.
func TestPipelineSpoolForceCleanupRetainsUntilReceiversDrained(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})

	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	src := batch.NewWithSize(1)
	src.Vecs[0] = vector.NewVec(types.New(types.T_int64, 0, 0))
	values := make([]int64, 1024)
	for i := range values {
		values[i] = int64(i + 1)
	}
	require.NoError(t, vector.AppendFixedList[int64](src.Vecs[0], values, nil, srcMP))
	src.SetRowCount(len(values))
	t.Cleanup(func() {
		src.Clean(srcMP)
	})

	sp := InitMyPipelineSpool(mp, 1)

	// first batch: consumed and released.
	queryDone, err := sp.SendBatch(context.Background(), 0, src, nil)
	require.NoError(t, err)
	require.False(t, queryDone)
	got, info := sp.ReceiveBatch(0)
	require.NoError(t, info)
	require.NotNil(t, got)
	sp.ReleaseCurrent(0)

	// second batch: sent but NOT consumed, so the receiver still holds a pending
	// reference to it.
	queryDone, err = sp.SendBatch(context.Background(), 0, src, nil)
	require.NoError(t, err)
	require.False(t, queryDone)
	require.Greater(t, mp.CurrNB(), int64(0))

	// ForceCleanup must retain the memory because the receiver has not drained.
	// It must also be retryable (cleanupOnce not consumed on retain).
	sp.ForceCleanup()
	require.Greater(t, mp.CurrNB(), int64(0))
	sp.ForceCleanup()
	require.Greater(t, mp.CurrNB(), int64(0))

	// Drain the receiver: consume the pending batch, then read the End marker so
	// the spool records the receiver as done.
	got, info = sp.ReceiveBatch(0)
	require.NoError(t, info)
	require.NotNil(t, got)
	queryDone, err = sp.SendBatch(context.Background(), 0, nil, nil)
	require.NoError(t, err)
	require.False(t, queryDone)
	got, info = sp.ReceiveBatch(0)
	require.NoError(t, info)
	require.Nil(t, got)

	// Now fully drained: ForceCleanup reclaims, and stays idempotent.
	sp.ForceCleanup()
	require.Equal(t, int64(0), mp.CurrNB())
	sp.ForceCleanup()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPipelineSpoolForceCleanupAfterTerminalSignalDoesNotNeedNilEndMessage(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})

	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	src := newSpoolTestBatch(t, srcMP, 1024)
	t.Cleanup(func() {
		src.Clean(srcMP)
	})

	sp := InitMyPipelineSpool(mp, 1)
	queryDone, err := sp.SendBatch(context.Background(), 0, src, nil)
	require.NoError(t, err)
	require.False(t, queryDone)
	require.Greater(t, mp.CurrNB(), int64(0))

	got, info := sp.ReceiveBatch(0)
	require.NoError(t, info)
	require.NotNil(t, got)
	sp.ReleaseCurrent(0)
	require.Greater(t, mp.CurrNB(), int64(0))

	sp.ForceCleanupAfterTerminalSignal()
	require.Equal(t, int64(0), mp.CurrNB())
	sp.ForceCleanupAfterTerminalSignal()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPipelineSpoolLateReleaseAfterTerminalCleanupFreesDirectly(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})

	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	src := newSpoolTestBatch(t, srcMP, 1024)
	t.Cleanup(func() {
		src.Clean(srcMP)
	})

	sp := InitMyPipelineSpool(mp, 1)
	for i := 0; i < 2; i++ {
		queryDone, err := sp.SendBatch(context.Background(), 0, src, nil)
		require.NoError(t, err)
		require.False(t, queryDone)
	}
	require.Greater(t, mp.CurrNB(), int64(0))

	got, info := sp.ReceiveBatch(0)
	require.NoError(t, info)
	require.NotNil(t, got)
	sp.ReleaseCurrent(0)
	require.Greater(t, mp.CurrNB(), int64(0))

	got, info = sp.ReceiveBatch(0)
	require.NoError(t, info)
	require.NotNil(t, got)
	sp.ForceCleanupAfterTerminalSignal()
	require.Greater(t, mp.CurrNB(), int64(0))

	sp.ReleaseCurrent(0)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPipelineSpoolCloseWithTimeoutCleansMemoryExactlyOnce(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})

	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	src := batch.NewWithSize(1)
	src.Vecs[0] = vector.NewVec(types.New(types.T_int64, 0, 0))
	values := []int64{1, 2, 3, 4}
	require.NoError(t, vector.AppendFixedList[int64](src.Vecs[0], values, nil, srcMP))
	src.SetRowCount(len(values))
	t.Cleanup(func() {
		src.Clean(srcMP)
	})

	sp := InitMyPipelineSpool(mp, 1)

	queryDone, err := sp.SendBatch(context.Background(), 0, src, nil)
	require.NoError(t, err)
	require.False(t, queryDone)
	got, info := sp.ReceiveBatch(0)
	require.NoError(t, info)
	require.NotNil(t, got)

	queryDone, err = sp.SendBatch(context.Background(), 0, nil, nil)
	require.NoError(t, err)
	require.False(t, queryDone)
	got, info = sp.ReceiveBatch(0)
	require.NoError(t, info)
	require.Nil(t, got)

	require.True(t, sp.CloseWithTimeout(time.Second))
	require.Equal(t, int64(0), mp.CurrNB())

	sp.ForceCleanup()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPipelineSpoolAbortReleasesPendingBatch(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})
	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	src := newSpoolTestBatch(t, srcMP, 1024)
	t.Cleanup(func() {
		src.Clean(srcMP)
	})

	sp := InitMyPipelineSpool(mp, 1)
	queryDone, err := sp.SendBatch(context.Background(), 0, src, nil)
	require.NoError(t, err)
	require.False(t, queryDone)
	require.Greater(t, mp.CurrNB(), int64(0))

	sp.Abort()
	require.Equal(t, int64(0), mp.CurrNB())

	got, info := sp.ReceiveBatch(0)
	require.Nil(t, got)
	require.ErrorIs(t, info, ErrPipelineSpoolAborted)

	sp.Abort()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPipelineSpoolAbortDefersCurrentBatchRelease(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})
	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	src := newSpoolTestBatch(t, srcMP, 1024)
	t.Cleanup(func() {
		src.Clean(srcMP)
	})

	sp := InitMyPipelineSpool(mp, 1)
	queryDone, err := sp.SendBatch(context.Background(), 0, src, nil)
	require.NoError(t, err)
	require.False(t, queryDone)

	got, info := sp.ReceiveBatch(0)
	require.NoError(t, info)
	require.NotNil(t, got)
	require.Equal(t, 1024, got.RowCount())
	require.Greater(t, mp.CurrNB(), int64(0))

	sp.Abort()
	require.Greater(t, mp.CurrNB(), int64(0))
	require.Equal(t, 1024, got.RowCount())

	sp.ReleaseCurrent(0)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPipelineSpoolAbortUnblocksSendBatchWaitingForFreeSlot(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})
	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	src := newSpoolTestBatch(t, srcMP, 1024)
	t.Cleanup(func() {
		src.Clean(srcMP)
	})

	sp := InitMyPipelineSpool(mp, 1)
	for i := 0; i < 2; i++ {
		queryDone, err := sp.SendBatch(context.Background(), 0, src, nil)
		require.NoError(t, err)
		require.False(t, queryDone)
	}

	done := make(chan error, 1)
	go func() {
		queryDone, err := sp.SendBatch(context.Background(), 0, src, nil)
		if err != nil {
			done <- err
			return
		}
		if !queryDone {
			done <- context.Canceled
			return
		}
		done <- nil
	}()

	sp.Abort()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("SendBatch did not unblock after Abort")
	}
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPipelineSpoolAbortWaitsForAllCurrentBroadcastReceivers(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})
	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	src := newSpoolTestBatch(t, srcMP, 1024)
	t.Cleanup(func() {
		src.Clean(srcMP)
	})

	sp := InitMyPipelineSpool(mp, 2)
	queryDone, err := sp.SendBatch(context.Background(), SendToAllLocal, src, nil)
	require.NoError(t, err)
	require.False(t, queryDone)

	got0, info := sp.ReceiveBatch(0)
	require.NoError(t, info)
	require.NotNil(t, got0)
	got1, info := sp.ReceiveBatch(1)
	require.NoError(t, info)
	require.NotNil(t, got1)
	require.Greater(t, mp.CurrNB(), int64(0))

	sp.Abort()
	require.Greater(t, mp.CurrNB(), int64(0))

	sp.ReleaseCurrent(0)
	require.Greater(t, mp.CurrNB(), int64(0))

	sp.ReleaseCurrent(1)
	require.Equal(t, int64(0), mp.CurrNB())
}

func newSpoolTestBatch(t *testing.T, mp *mpool.MPool, rows int) *batch.Batch {
	t.Helper()
	src := batch.NewWithSize(1)
	src.Vecs[0] = vector.NewVec(types.New(types.T_int64, 0, 0))
	values := make([]int64, rows)
	for i := range values {
		values[i] = int64(i + 1)
	}
	require.NoError(t, vector.AppendFixedList[int64](src.Vecs[0], values, nil, mp))
	src.SetRowCount(len(values))
	return src
}
