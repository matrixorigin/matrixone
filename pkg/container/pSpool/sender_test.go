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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

	sp.Abort(nil)
	require.Equal(t, int64(0), mp.CurrNB())

	got, info := sp.ReceiveBatch(0)
	require.Nil(t, got)
	require.Same(t, ErrPipelineSpoolAborted, info)

	sp.Abort(moerr.NewInternalErrorNoCtx("ignored second abort"))
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPipelineSpoolAbortPreservesFirstCause(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})

	sp := InitMyPipelineSpool(mp, 1)
	firstCause := moerr.NewCheckRecursiveLevel(context.Background())
	sp.Abort(firstCause)
	sp.Abort(moerr.NewInternalErrorNoCtx("ignored second abort"))

	got, info := sp.ReceiveBatch(0)
	require.Nil(t, got)
	require.Same(t, firstCause, info)
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

	sp.Abort(nil)
	require.Greater(t, mp.CurrNB(), int64(0))
	require.Equal(t, 1024, got.RowCount())

	sp.ReleaseCurrent(0)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPipelineSpoolReleasedSlotDoesNotRetainOwnership(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})
	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	src := newSpoolTestBatch(t, srcMP, 1)
	t.Cleanup(func() {
		src.Clean(srcMP)
	})

	sp := InitMyPipelineSpool(mp, 1)
	batchErr := moerr.NewInternalErrorNoCtx("batch error")
	queryDone, err := sp.SendBatch(context.Background(), 0, src, batchErr)
	require.NoError(t, err)
	require.False(t, queryDone)

	got, info := sp.ReceiveBatch(0)
	require.NotNil(t, got)
	require.Same(t, batchErr, info)
	slot, ok := sp.rs[0].getLastPop()
	require.True(t, ok)

	sp.ReleaseCurrent(0)

	msg := sp.shardPool[slot]
	require.Nil(t, msg.dataContent)
	require.Nil(t, msg.errContent)
	require.False(t, msg.useCache)
	require.Zero(t, msg.cacheID)
	require.False(t, sp.doRefCheck[slot])

	sp.ForceCleanupAfterTerminalSignal()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPipelineSpoolAbortDoesNotCleanReusedCurrentBatch(t *testing.T) {
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
	first, info := sp.ReceiveBatch(0)
	require.NoError(t, info)
	require.NotNil(t, first)
	sp.ReleaseCurrent(0)

	queryDone, err = sp.SendBatch(context.Background(), 0, src, nil)
	require.NoError(t, err)
	require.False(t, queryDone)
	current, info := sp.ReceiveBatch(0)
	require.NoError(t, info)
	require.Same(t, first, current, "the test must exercise batch reuse across different slots")

	sp.Abort(nil)
	require.Equal(t, 1024, current.RowCount())
	values := vector.MustFixedColWithTypeCheck[int64](current.Vecs[0])
	require.Equal(t, int64(1), values[0])
	require.Equal(t, int64(1024), values[len(values)-1])

	sp.ReleaseCurrent(0)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPipelineSpoolAbortDoesNotCleanReusedCurrentBroadcastBatch(t *testing.T) {
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
	first0, info := sp.ReceiveBatch(0)
	require.NoError(t, info)
	first1, info := sp.ReceiveBatch(1)
	require.NoError(t, info)
	require.Same(t, first0, first1)
	sp.ReleaseCurrent(0)
	sp.ReleaseCurrent(1)

	queryDone, err = sp.SendBatch(context.Background(), SendToAllLocal, src, nil)
	require.NoError(t, err)
	require.False(t, queryDone)
	current0, info := sp.ReceiveBatch(0)
	require.NoError(t, info)
	current1, info := sp.ReceiveBatch(1)
	require.NoError(t, info)
	require.Same(t, first0, current0, "the test must exercise batch reuse across different slots")
	require.Same(t, current0, current1)

	sp.Abort(nil)
	require.Equal(t, 1024, current0.RowCount())
	require.Equal(t, int64(1), vector.MustFixedColWithTypeCheck[int64](current1.Vecs[0])[0])

	sp.ReleaseCurrent(0)
	require.Greater(t, mp.CurrNB(), int64(0))
	require.Equal(t, 1024, current1.RowCount())
	sp.ReleaseCurrent(1)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPipelineSpoolConcurrentAbortAndBroadcastRelease(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})
	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	src := newSpoolTestBatch(t, srcMP, 16)
	t.Cleanup(func() {
		src.Clean(srcMP)
	})

	for iteration := 0; iteration < 256; iteration++ {
		sp := InitMyPipelineSpool(mp, 2)
		queryDone, err := sp.SendBatch(context.Background(), SendToAllLocal, src, nil)
		require.NoError(t, err)
		require.False(t, queryDone)
		got0, info := sp.ReceiveBatch(0)
		require.NoError(t, info)
		got1, info := sp.ReceiveBatch(1)
		require.NoError(t, info)
		require.Same(t, got0, got1)

		start := make(chan struct{})
		done := make(chan struct{}, 3)
		run := func(fn func()) {
			go func() {
				<-start
				fn()
				done <- struct{}{}
			}()
		}
		run(func() { sp.Abort(nil) })
		run(func() { sp.ReleaseCurrent(0) })
		run(func() { sp.ReleaseCurrent(1) })
		close(start)

		timer := time.NewTimer(5 * time.Second)
		for range 3 {
			select {
			case <-done:
			case <-timer.C:
				t.Fatalf("Abort and ReleaseCurrent did not finish at iteration %d", iteration)
			}
		}
		timer.Stop()
		require.Equal(t, int64(0), mp.CurrNB(), "iteration %d", iteration)
	}
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

	sp.Abort(nil)
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

	sp.Abort(nil)
	require.Greater(t, mp.CurrNB(), int64(0))

	sp.ReleaseCurrent(0)
	require.Greater(t, mp.CurrNB(), int64(0))

	sp.ReleaseCurrent(1)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPipelineSpoolSendBatchCopyFailureRestoresCapacity(t *testing.T) {
	tests := []struct {
		name      string
		newVector func(*testing.T, *mpool.MPool) *vector.Vector
	}{
		{
			name: "flat vector",
			newVector: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				vec := vector.NewVec(types.T_varchar.ToType())
				require.NoError(t, vector.AppendBytes(vec, make([]byte, 2<<20), false, mp))
				return vec
			},
		},
		{
			name: "const vector",
			newVector: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				vec, err := vector.NewConstBytes(types.T_varchar.ToType(), make([]byte, 2<<20), 1, mp)
				require.NoError(t, err)
				return vec
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dstMP, err := mpool.NewMPool("p-spool-copy-failure", 1<<20, mpool.NoFixed)
			require.NoError(t, err)
			t.Cleanup(func() {
				mpool.DeleteMPool(dstMP)
			})

			srcMP := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() {
				mpool.DeleteMPool(srcMP)
			})
			src := batch.NewWithSize(1)
			src.Vecs[0] = test.newVector(t, srcMP)
			src.SetRowCount(1)
			t.Cleanup(func() {
				src.Clean(srcMP)
			})

			sp := InitMyPipelineSpool(dstMP, 1)
			t.Cleanup(func() {
				sp.Abort(nil)
			})

			for range 3 {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				done, err := sp.SendBatch(ctx, 0, src, nil)
				cancel()
				require.Error(t, err)
				require.False(t, done)
			}

			require.Equal(t, cap(sp.freeShardPool), len(sp.freeShardPool))
			require.Equal(t, cap(sp.freeShardPool), len(sp.cache.buffer.readyToUse))

			small := newSpoolTestBatch(t, srcMP, 1)
			t.Cleanup(func() {
				small.Clean(srcMP)
			})
			done, err := sp.SendBatch(context.Background(), 0, small, nil)
			require.NoError(t, err)
			require.False(t, done)
			got, info := sp.ReceiveBatch(0)
			require.NoError(t, info)
			require.Equal(t, 1, got.RowCount())
			sp.ReleaseCurrent(0)

			sp.Abort(nil)
			require.Equal(t, int64(0), dstMP.CurrNB())
		})
	}
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
