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
	"fmt"
	"sync"
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

// Two receivers own the first broadcast batch; two have not consumed anything,
// and a second batch is pending for all four. Cleanup must reclaim pending
// ownership without invalidating the batch retained by the slow receiver.
func TestPipelineSpoolMixedBroadcastFailureCleanup(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(mp)
	srcMP := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(srcMP)
	src := newSpoolTestBatch(t, srcMP, 16)
	defer src.Clean(srcMP)
	cause := moerr.NewInternalErrorNoCtx("broadcast consumer failed")
	for iteration := 0; iteration < 32; iteration++ {
		sp := InitMyPipelineSpool(mp, 4)
		for range 2 {
			done, err := sp.SendBatch(context.Background(), SendToAllLocal, src, nil)
			require.NoError(t, err)
			require.False(t, done)
		}
		first, err := sp.ReceiveBatch(0)
		require.NoError(t, err)
		slow, err := sp.ReceiveBatch(1)
		require.NoError(t, err)
		require.Same(t, first, slow)
		start := make(chan struct{})
		done := make(chan struct{}, 4)
		for _, fn := range []func(){
			func() { sp.Abort(cause) },
			func() { sp.Abort(cause) },
			sp.ForceCleanupAfterTerminalSignal,
			func() { sp.ReleaseCurrent(0) },
		} {
			go func() {
				<-start
				fn()
				done <- struct{}{}
			}()
		}
		close(start)
		timer := time.NewTimer(5 * time.Second)
		for range 4 {
			select {
			case <-done:
			case <-timer.C:
				t.Fatalf("mixed broadcast cleanup blocked: generation %d", iteration)
			}
		}
		timer.Stop()
		require.Equal(t, 16, slow.RowCount())
		require.Equal(t, int64(16), vector.MustFixedColWithTypeCheck[int64](slow.Vecs[0])[15])
		for _, idx := range []int{2, 3} {
			got, err := sp.ReceiveBatch(idx)
			require.Nil(t, got)
			require.Same(t, cause, err)
		}
		sp.ReleaseCurrent(1)
		sp.ReleaseCurrent(1)
		sp.Abort(nil)
		sp.ForceCleanupAfterTerminalSignal()
		require.Zero(t, mp.CurrNB(), "generation %d", iteration)
	}
}

func TestPipelineSpoolPartialCopyFailureRollsBack(t *testing.T) {
	for failureColumn := 0; failureColumn < 4; failureColumn++ {
		t.Run(fmt.Sprintf("column_%d", failureColumn), func(t *testing.T) {
			mp, err := mpool.NewMPool("partial-copy-failure", 1<<20, mpool.NoFixed)
			require.NoError(t, err)
			defer mpool.DeleteMPool(mp)
			srcMP := mpool.MustNewZeroNoFixed()
			defer mpool.DeleteMPool(srcMP)
			src := batch.NewWithSize(3)
			defer src.Clean(srcMP)
			failAt := failureColumn
			if failAt == 3 {
				failAt = 2
			}
			for col := range src.Vecs {
				if col == failAt {
					src.Vecs[col] = vector.NewVec(types.T_varchar.ToType())
					require.NoError(t, vector.AppendBytes(src.Vecs[col], make([]byte, 2<<20), false, srcMP))
				} else {
					src.Vecs[col] = vector.NewVec(types.T_int64.ToType())
					require.NoError(t, vector.AppendFixed(src.Vecs[col], int64(col), false, srcMP))
				}
			}
			if failureColumn == 3 {
				src.Vecs[1].Free(srcMP)
				src.Vecs[1] = src.Vecs[0]
			}
			src.SetRowCount(1)
			sp := InitMyPipelineSpool(mp, 1)
			defer sp.Abort(nil)
			// More failures than slots exposes either leaked slot or cache IDs.
			for range 4 {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				done, err := sp.SendBatch(ctx, 0, src, nil)
				cancel()
				require.Error(t, err)
				require.False(t, done)
				require.Len(t, sp.freeShardPool, cap(sp.freeShardPool))
				require.Len(t, sp.cache.buffer.readyToUse, cap(sp.freeShardPool))
			}
			small := newSpoolTestBatch(t, srcMP, 16)
			defer small.Clean(srcMP)
			done, err := sp.SendBatch(context.Background(), 0, small, nil)
			require.NoError(t, err)
			require.False(t, done)
			got, err := sp.ReceiveBatch(0)
			require.NoError(t, err)
			require.Equal(t, 16, got.RowCount())
			require.Equal(t, int64(16), vector.MustFixedColWithTypeCheck[int64](got.Vecs[0])[15])
			sp.ReleaseCurrent(0)
			sp.Abort(nil)
			require.Zero(t, mp.CurrNB())
		})
	}
}

type spoolWaitObservedContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *spoolWaitObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestPipelineSpoolCancellationBeforeAndDuringSlotWait(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		t.Run(fmt.Sprintf("blocked_%v", blocked), func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			defer mpool.DeleteMPool(mp)
			srcMP := mpool.MustNewZeroNoFixed()
			defer mpool.DeleteMPool(srcMP)
			src := newSpoolTestBatch(t, srcMP, 16)
			defer src.Clean(srcMP)
			sp := InitMyPipelineSpool(mp, 1)
			defer sp.Abort(nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if !blocked {
				cancel()
				done, err := sp.SendBatch(ctx, 0, src, nil)
				require.NoError(t, err)
				require.True(t, done)
				require.Len(t, sp.freeShardPool, cap(sp.freeShardPool))
				require.Zero(t, mp.CurrNB())
			} else {
				for range cap(sp.freeShardPool) {
					done, err := sp.SendBatch(ctx, 0, src, nil)
					require.NoError(t, err)
					require.False(t, done)
				}
				observed := &spoolWaitObservedContext{Context: ctx, entered: make(chan struct{})}
				type result struct {
					done bool
					err  error
				}
				finished := make(chan result, 1)
				go func() {
					done, err := sp.SendBatch(observed, 0, src, nil)
					finished <- result{done, err}
				}()
				select {
				case <-observed.entered:
				case <-time.After(time.Second):
					t.Fatal("sender did not reach the blocked slot select")
				}
				cancel()
				select {
				case got := <-finished:
					require.True(t, got.done)
					require.NoError(t, got.err)
				case <-time.After(time.Second):
					t.Fatal("query cancellation did not release blocked sender")
				}
			}
			sp.Abort(nil)
			require.Zero(t, mp.CurrNB())
		})
	}
}

// Exercise the fixed-width allocation branch as well as the varchar data/area
// and partial-column allocation failures covered above.
func TestPipelineSpoolFixedCopyAllocationFailure(t *testing.T) {
	for _, constant := range []bool{false, true} {
		t.Run(fmt.Sprintf("constant_%v", constant), func(t *testing.T) {
			mp, err := mpool.NewMPool("fixed-copy-failure", 1<<20, mpool.NoFixed)
			require.NoError(t, err)
			defer mpool.DeleteMPool(mp)
			filler, err := mp.Alloc(1<<20, true)
			require.NoError(t, err)
			defer func() {
				if filler != nil {
					mp.Free(filler)
				}
			}()
			srcMP := mpool.MustNewZeroNoFixed()
			defer mpool.DeleteMPool(srcMP)
			src := batch.NewWithSize(1)
			defer src.Clean(srcMP)
			if constant {
				src.Vecs[0], err = vector.NewConstFixed(types.T_int64.ToType(), int64(42), 1, srcMP)
				require.NoError(t, err)
			} else {
				src.Vecs[0] = vector.NewVec(types.T_int64.ToType())
				require.NoError(t, vector.AppendFixed(src.Vecs[0], int64(42), false, srcMP))
			}
			src.SetRowCount(1)
			sp := InitMyPipelineSpool(mp, 1)
			defer sp.Abort(nil)
			for range 4 {
				done, err := sp.SendBatch(context.Background(), 0, src, nil)
				require.False(t, done)
				require.Error(t, err)
				require.Len(t, sp.freeShardPool, cap(sp.freeShardPool))
				require.Len(t, sp.cache.buffer.readyToUse, cap(sp.freeShardPool))
			}
			sp.Abort(nil)
			require.Equal(t, int64(len(filler)), mp.CurrNB())
			mp.Free(filler)
			filler = nil
			require.Zero(t, mp.CurrNB())
		})
	}
}

// A rejected notification retracts only its newest queue entry, including
// when the ring is full (head == tail). Older queued/current data stays valid.
func TestPipelineSpoolDiscardUnsentPreservesDelivered(t *testing.T) {
	for _, receivers := range []uint32{1, 2} {
		for _, held := range []bool{false, true} {
			for _, cleaned := range []bool{false, true} {
				t.Run(fmt.Sprintf("receivers_%d/current_%v/cleanup_%v", receivers, held, cleaned), func(t *testing.T) {
					mp := mpool.MustNewZeroNoFixed()
					defer mpool.DeleteMPool(mp)
					srcMP := mpool.MustNewZeroNoFixed()
					defer mpool.DeleteMPool(srcMP)
					src := newSpoolTestBatch(t, srcMP, 1)
					defer src.Clean(srcMP)
					sp := InitMyPipelineSpool(mp, receivers)
					defer sp.Abort(nil)
					rounds := 2*cap(sp.freeShardPool) + 1
					if cleaned {
						rounds = 1
					}
					for range rounds {
						values := vector.MustFixedColWithTypeCheck[int64](src.Vecs[0])
						values[0] = 1
						done, err := sp.SendBatch(context.Background(), SendToAllLocal, src, nil)
						require.NoError(t, err)
						require.False(t, done)
						var current *batch.Batch
						if held {
							current, err = sp.ReceiveBatch(0)
							require.NoError(t, err)
						}
						values[0] = 2
						done, err = sp.SendBatch(context.Background(), SendToAllLocal, src, nil)
						require.NoError(t, err)
						require.False(t, done)
						if cleaned {
							sp.ForceCleanupAfterTerminalSignal()
						}
						sp.DiscardUnsent(int(receivers) - 1)
						if !held {
							current, err = sp.ReceiveBatch(0)
							require.NoError(t, err)
						}
						require.Equal(t, int64(1), vector.MustFixedColWithTypeCheck[int64](current.Vecs[0])[0])
						if receivers == 2 {
							older, info := sp.ReceiveBatch(1)
							require.NoError(t, info)
							require.Equal(t, int64(1), vector.MustFixedColWithTypeCheck[int64](older.Vecs[0])[0])
							sp.ReleaseCurrent(1)
							newer, info := sp.ReceiveBatch(0)
							require.NoError(t, info)
							require.Equal(t, int64(2), vector.MustFixedColWithTypeCheck[int64](newer.Vecs[0])[0])
						}
						sp.ReleaseCurrent(0)
						if !cleaned {
							require.Len(t, sp.freeShardPool, cap(sp.freeShardPool))
							require.Len(t, sp.cache.buffer.readyToUse, cap(sp.freeShardPool))
						}
					}
					sp.ForceCleanupAfterTerminalSignal()
					require.Zero(t, mp.CurrNB())
				})
			}
		}
	}
}

func TestPipelineSpoolDiscardUnsentWithAbortAndRelease(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(mp)
	srcMP := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(srcMP)
	src := newSpoolTestBatch(t, srcMP, 1)
	defer src.Clean(srcMP)
	sp := InitMyPipelineSpool(mp, 2)
	defer sp.Abort(nil)
	done, err := sp.SendBatch(context.Background(), SendToAllLocal, src, nil)
	require.NoError(t, err)
	require.False(t, done)
	_, err = sp.ReceiveBatch(0)
	require.NoError(t, err)
	current, err := sp.ReceiveBatch(1)
	require.NoError(t, err)
	done, err = sp.SendBatch(context.Background(), SendToAllLocal, src, nil)
	require.NoError(t, err)
	require.False(t, done)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, fn := range []func(){func() { sp.DiscardUnsent(1) }, func() { sp.Abort(context.Canceled) }, func() { sp.ReleaseCurrent(0) }} {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; fn() }()
	}
	close(start)
	wg.Wait()
	require.Equal(t, int64(1), vector.MustFixedColWithTypeCheck[int64](current.Vecs[0])[0])
	sp.ReleaseCurrent(1)
	require.Zero(t, mp.CurrNB())
}

func TestPipelineSpoolDiscardUnsentAcrossSendModes(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(mp)
	srcMP := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(srcMP)
	src := newSpoolTestBatch(t, srcMP, 1)
	defer src.Clean(srcMP)
	sp := InitMyPipelineSpool(mp, 2)
	defer sp.Abort(nil)
	// Alternate broadcast and indexed ownership as both physical slots wrap.
	for range 5 {
		for _, latest := range []int{0, SendToAllLocal} {
			older := SendToAllLocal
			if latest == SendToAllLocal {
				older = 0
			}
			done, err := sp.SendBatch(context.Background(), older, src, nil)
			require.NoError(t, err)
			require.False(t, done)
			done, err = sp.SendBatch(context.Background(), latest, src, nil)
			require.NoError(t, err)
			require.False(t, done)
			sp.DiscardUnsent(0)
			if latest == SendToAllLocal {
				sp.DiscardUnsent(1)
			}
			count := 1
			if older == SendToAllLocal {
				count = 2
			}
			for idx := 0; idx < count; idx++ {
				got, info := sp.ReceiveBatch(idx)
				require.NoError(t, info)
				require.Equal(t, int64(1), vector.MustFixedColWithTypeCheck[int64](got.Vecs[0])[0])
				sp.ReleaseCurrent(idx)
			}
			require.Len(t, sp.freeShardPool, cap(sp.freeShardPool))
		}
	}
	sp.ForceCleanupAfterTerminalSignal()
	require.Zero(t, mp.CurrNB())
}
