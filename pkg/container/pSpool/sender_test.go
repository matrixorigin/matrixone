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
	"bytes"
	"context"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// poison models a lender reusing its backing at its next call: it overwrites
// the bytes, so any read through a stale alias observes garbage.
func poison(backing []byte) {
	for i := range backing {
		backing[i] = 0xFF
	}
}

// The spool copies an aliased producer batch: the copy must stay valid after
// the producer, the lender, releases the lent backing at its next call.
func TestSpoolCopiesBorrowedFixedPayload(t *testing.T) {
	mp := mpool.MustNewZero()
	data := types.EncodeSlice([]int64{11, 22})
	vec, err := vector.NewVecWithData(types.T_int64.ToType(), 2, data, nil, nil), error(nil)
	require.NoError(t, err)
	source := batch.New([]string{"v"})
	source.Vecs[0] = vec
	source.SetRowCount(2)

	cache := newBatchCache(mp, 1)
	copied, owned, aliased, err := cache.copyBatch(source)
	require.NoError(t, err)
	require.True(t, owned)
	require.NotEqual(t,
		uintptr(unsafe.Pointer(unsafe.SliceData(source.Vecs[0].GetData()))),
		uintptr(unsafe.Pointer(unsafe.SliceData(copied.Vecs[0].GetData()))),
	)
	require.False(t, copied.Vecs[0].IsReadOnly())
	source.Clean(mp)
	poison(data) // the producer's next call reuses what it lent
	require.Equal(t, []int64{11, 22}, vector.MustFixedColNoTypeCheck[int64](copied.Vecs[0]))

	cache.put(copied, aliased)
	cache.free()
	require.Zero(t, mp.CurrNB())
}

func TestSpoolCopiesBorrowedVarlenPayload(t *testing.T) {
	mp := mpool.MustNewZero()
	first := []byte("first payload longer than twenty three bytes")
	second := []byte("second payload longer than twenty three bytes")
	area := append(append([]byte(nil), first...), second...)

	vec := vector.NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, vec.PreExtend(2, mp))
	vec.SetLength(2)
	descriptors := vector.MustFixedColNoTypeCheck[types.Varlena](vec)
	descriptors[0].SetOffsetLen(0, uint32(len(first)))
	descriptors[1].SetOffsetLen(uint32(len(first)), uint32(len(second)))
	// A read-only vector over the owner's descriptors and the lent area.
	owner := vec
	vec = vector.NewVecWithData(types.T_varchar.ToType(), 2, owner.GetData(), area, nil)

	source := batch.New([]string{"v"})
	source.Vecs[0] = vec
	source.SetRowCount(2)
	sourceDataPointer := uintptr(unsafe.Pointer(unsafe.SliceData(vec.GetData())))
	sourceAreaPointer := uintptr(unsafe.Pointer(unsafe.SliceData(vec.GetArea())))

	cache := newBatchCache(mp, 1)
	copied, owned, aliased, err := cache.copyBatch(source)
	require.NoError(t, err)
	require.True(t, owned)
	require.NotEqual(t, sourceDataPointer,
		uintptr(unsafe.Pointer(unsafe.SliceData(copied.Vecs[0].GetData()))))
	require.NotEqual(t, sourceAreaPointer,
		uintptr(unsafe.Pointer(unsafe.SliceData(copied.Vecs[0].GetArea()))),
		"the borrowed area is copied at the spool boundary")
	require.False(t, copied.Vecs[0].IsReadOnly())

	source.Clean(mp)
	poison(area) // the producer's next call reuses what it lent
	require.Equal(t, string(first), copied.Vecs[0].GetStringAt(0))
	require.Equal(t, string(second), copied.Vecs[0].GetStringAt(1))

	cache.put(copied, aliased)
	cache.free()
	owner.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestCachedBatchRejectsSelectedBatch(t *testing.T) {
	mp := mpool.MustNewZero()
	source := batch.NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, vector.AppendFixedList(source.Vecs[0], []int64{1, 2, 3}, nil, mp))
	var selection bitmap.Bitmap
	selection.InitWithSize(3)
	selection.Add(1)
	source.SetSelection(&selection)

	cache := newBatchCache(mp, 1)
	copied, owned, _, err := cache.copyBatch(source)
	require.Error(t, err)
	require.Contains(t, err.Error(), "selected batch")
	require.Nil(t, copied)
	require.False(t, owned)
	cache.free()
	source.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestCachedBatchMovesAccountedExtraBufferOwnership(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)

	payload, err := mpool.NewAccountedBuffer(mp, account, 1, 1)
	require.NoError(t, err)
	_, err = payload.Write([]byte("accounted partial aggregate state"))
	require.NoError(t, err)
	source := batch.NewWithSize(0)
	require.NoError(t, source.SetAccountedExtraBuffer(payload))
	require.Positive(t, account.Snapshot().Used)

	cache := newBatchCache(mp, 1)
	copied, owned, aliased, err := cache.copyBatch(source)
	require.NoError(t, err)
	require.True(t, owned)
	require.Empty(t, source.ExtraBuf())
	require.Equal(t, "accounted partial aggregate state", string(copied.ExtraBuf()))

	// Returning the copied batch to the spool cache is the terminal owner
	// transition for ExtraBuf and must release the physical charge exactly once.
	cache.put(copied, aliased)
	require.Zero(t, account.Snapshot().Used)
	source.Clean(mp)
	cache.free()
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

// TestPipelineSpoolReleaseCacheRetainsPendingBatches verifies that ReleaseCache
// frees only batches receivers have released: a batch a receiver has not
// consumed yet stays intact, and is freed directly once it is released.
func TestPipelineSpoolReleaseCacheRetainsPendingBatches(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})

	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	src := batch.NewWithSize(1)
	src.Vecs[0] = vector.NewVec(types.New(types.T_int64, 0, 0), srcMP)
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

	// ReleaseCache must retain the pending batch; it is idempotent.
	sp.ReleaseCache()
	require.Greater(t, mp.CurrNB(), int64(0))
	sp.ReleaseCache()
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

	// Draining released the pending batch, which the closed cache freed.
	sp.ReleaseCache()
	require.Equal(t, int64(0), mp.CurrNB())
	sp.ReleaseCache()
	require.Equal(t, int64(0), mp.CurrNB())
}

// The spool charges its copies through the MPool it was built with: an
// accounted view charges every copy, and reused cached backing stays charged
// once: copying into a reused vector that already has the backing it needs
// charges nothing new.
func TestCachedBatchChargesSpoolAllocationView(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(2, 32)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	view, err := mp.WithAccount(account, 1, 1, mpool.AllocationCapacityClassDefault)
	require.NoError(t, err)
	newSource := func(value string) *batch.Batch {
		source := batch.NewWithSize(1)
		vec := vector.NewVec(types.T_varchar.ToType(), mp)
		source.SetVector(0, vec)
		require.NoError(t, vector.AppendBytes(
			vec,
			[]byte(value),
			false,
			mp,
		))
		source.SetRowCount(1)
		return source
	}
	firstSource := newSource("first cached allocation payload")
	secondSource := newSource("second")
	secondSource.Vecs[0].ToConst()
	require.Zero(t, account.Snapshot().Used)
	cache := newBatchCache(view, 1)

	first, owned, aliased, err := cache.copyBatch(firstSource)
	require.NoError(t, err)
	require.True(t, owned)
	require.Equal(t, "first cached allocation payload", first.Vecs[0].GetStringAt(0))
	require.Positive(t, account.Snapshot().Used)
	cache.put(first, aliased)
	beforeReuse := account.Snapshot().Used

	second, owned, aliased, err := cache.copyBatch(secondSource)
	require.NoError(t, err)
	require.True(t, owned)
	require.Equal(t, beforeReuse, account.Snapshot().Used)
	require.True(t, second.Vecs[0].IsConst())
	require.Equal(t, "second", second.Vecs[0].GetStringAt(0))
	cache.put(second, aliased)
	require.Equal(t, beforeReuse, account.Snapshot().Used)

	// A spool on the base pool leaves the account untouched.
	plain := newBatchCache(mp, 1)
	other, _, aliased, err := plain.copyBatch(firstSource)
	require.NoError(t, err)
	require.Equal(t, beforeReuse, account.Snapshot().Used)
	plain.put(other, aliased)

	firstSource.Clean(mp)
	secondSource.Clean(mp)
	cache.free()
	plain.free()
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, registry.LiveAllocationMetadata())
	require.Zero(t, mp.CurrNB())
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestCachedBatchAllocationFailureReleasesCacheOwnership(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 4)
	require.NoError(t, err)
	account, err := registry.Open(64)
	require.NoError(t, err)
	view, err := mp.WithAccount(account, 1, 1, mpool.AllocationCapacityClassDefault)
	require.NoError(t, err)
	source := batch.NewWithSize(1)
	source.SetVector(0, vector.NewVec(types.T_int64.ToType(), view))
	require.NoError(t, vector.AppendFixedList(
		source.Vecs[0],
		[]int64{1, 2, 3, 4, 5, 6, 7, 8},
		nil,
		view,
	))
	source.SetRowCount(8)
	require.Equal(t, uint64(64), account.Snapshot().Used)
	cache := newBatchCache(view, 1)

	_, _, _, err = cache.copyBatch(source)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Empty(t, cache.bats)
	require.Equal(t, uint64(64), account.Snapshot().Used)

	source.Clean(view)
	cache.free()
	require.Zero(t, account.Snapshot().Used)
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestPipelineSpoolReleaseCacheDoesNotNeedNilEndMessage(t *testing.T) {
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

	sp.ReleaseCache()
	require.Equal(t, int64(0), mp.CurrNB())
	sp.ReleaseCache()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPipelineSpoolReleaseReusableCacheBeforeFinalization(t *testing.T) {
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
	for range 2 {
		done, err := sp.SendBatch(context.Background(), 0, src, nil)
		require.NoError(t, err)
		require.False(t, done)
	}
	got, info := sp.ReceiveBatch(0)
	require.NoError(t, info)
	require.NotNil(t, got)
	sp.ReleaseCurrent(0)
	got, info = sp.ReceiveBatch(0)
	require.NoError(t, info)
	require.NotNil(t, got)
	beforeRelease := mp.CurrNB()

	sp.ReleaseCache()
	require.Positive(t, mp.CurrNB())
	require.Less(t, mp.CurrNB(), beforeRelease)

	// The cache is closed: a later release frees its batch directly.
	sp.ReleaseCurrent(0)
	require.Zero(t, mp.CurrNB())
	sp.FinalizeAfterConsumersQuiesced()
	require.Zero(t, mp.CurrNB())
}

func TestPipelineSpoolFinalizeAfterConsumersQuiescedReleasesPendingBatch(t *testing.T) {
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
	done, err := sp.SendBatch(context.Background(), 0, src, nil)
	require.NoError(t, err)
	require.False(t, done)
	require.Greater(t, mp.CurrNB(), int64(0))

	sp.ReleaseCache()
	require.Greater(t, mp.CurrNB(), int64(0))

	sp.FinalizeAfterConsumersQuiesced()
	require.Equal(t, int64(0), mp.CurrNB())
	sp.FinalizeAfterConsumersQuiesced()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestPipelineSpoolFinalizeAfterConsumersQuiescedReleasesBroadcastCurrentAndPending(t *testing.T) {
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
	done, err := sp.SendBatch(context.Background(), SendToAllLocal, src, nil)
	require.NoError(t, err)
	require.False(t, done)
	got, info := sp.ReceiveBatch(0)
	require.NoError(t, info)
	require.NotNil(t, got)
	require.Greater(t, mp.CurrNB(), int64(0))

	sp.FinalizeAfterConsumersQuiesced()
	require.Equal(t, int64(0), mp.CurrNB())
	sp.FinalizeAfterConsumersQuiesced()
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
	sp.ReleaseCache()
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
	src.Vecs[0] = vector.NewVec(types.New(types.T_int64, 0, 0), srcMP)
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

	sp.ReleaseCache()
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
	require.False(t, msg.owned)
	require.False(t, msg.aliased)
	require.False(t, sp.doRefCheck[slot])

	sp.ReleaseCache()
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
				vec := vector.NewVec(types.T_varchar.ToType(), mp)
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
			require.LessOrEqual(t, len(sp.cache.bats), cap(sp.freeShardPool))

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
	src.Vecs[0] = vector.NewVec(types.New(types.T_int64, 0, 0), mp)
	values := make([]int64, rows)
	for i := range values {
		values[i] = int64(i + 1)
	}
	require.NoError(t, vector.AppendFixedList[int64](src.Vecs[0], values, nil, mp))
	src.SetRowCount(len(values))
	return src
}

// Cached vectors keep backing sized for their column position. When two
// varlen columns swap sizes, reusing that backing needs more memory than a
// fresh copy; under an allocation account the spool must still admit the
// batch, as a fresh spool would.
func TestPipelineSpoolSwappedColumnSizesFitAllocationAccount(t *testing.T) {
	const (
		large = 256 << 10
		small = 64 << 10
	)
	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { defer mpool.DeleteMPool(srcMP); require.Zero(t, srcMP.CurrNB()) })
	newSource := func(sizes ...int) *batch.Batch {
		source := batch.NewWithSize(len(sizes))
		for i, size := range sizes {
			source.Vecs[i] = vector.NewVec(types.T_text.ToType(), srcMP)
			require.NoError(t, vector.AppendBytes(source.Vecs[i], bytes.Repeat([]byte{byte(i + 1)}, size), false, srcMP))
		}
		source.SetRowCount(1)
		t.Cleanup(func() { source.Clean(srcMP) })
		return source
	}
	first := newSource(large, small)
	swapped := newSource(small, large)

	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	registry, err := mpool.NewAllocationAccountRegistry(1, 32)
	require.NoError(t, err)
	account, err := registry.Open(384 << 10)
	require.NoError(t, err)
	view, err := mp.WithAccount(account, 1, 1, mpool.AllocationCapacityClassDefault)
	require.NoError(t, err)

	send := func(sp *PipelineSpool, source *batch.Batch) {
		t.Helper()
		_, err := sp.SendBatch(context.Background(), 0, source, nil)
		require.NoError(t, err)
		got, info := sp.ReceiveBatch(0)
		require.NoError(t, info)
		for i, vec := range source.Vecs {
			require.Equal(t, vec.GetBytesAt(0), got.Vecs[i].GetBytesAt(0))
		}
		sp.ReleaseCurrent(0)
	}

	// Control: a fresh spool admits the swapped batch.
	fresh := InitMyPipelineSpool(view, 1)
	send(fresh, swapped)
	fresh.Abort(nil)
	require.Zero(t, account.Snapshot().Used)

	// A spool warmed by the first shape admits it too.
	sp := InitMyPipelineSpool(view, 1)
	send(sp, first)
	send(sp, swapped)
	// And keeps working, reusing the backing again for the same shape.
	send(sp, swapped)
	sp.Abort(nil)

	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, mp.CurrNB())
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}
