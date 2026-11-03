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

package pSpool

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func spoolOwnerInput(t *testing.T, mp *mpool.MPool, columns int) *batch.Batch {
	t.Helper()
	b := batch.NewWithSize(columns)
	t.Cleanup(func() { b.Clean(mp) })
	for i := range b.Vecs {
		b.Vecs[i] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(b.Vecs[i], []int64{11, 12}, nil, mp))
	}
	b.SetRowCount(2)
	return b
}

func TestSpoolAttemptOwnerReclaimsMissingEndAfterReadersStop(t *testing.T) {
	for _, broadcast := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "broadcast"}[broadcast], func(t *testing.T) {
			sourceMP := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { mpool.DeleteMPool(sourceMP) })
			source := spoolOwnerInput(t, sourceMP, 1)
			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			count, target := uint32(1), 0
			if broadcast {
				count, target = 2, SendToAllLocal
			}
			sp := InitMyPipelineSpool(mp, count)
			t.Cleanup(sp.CloseAfterReadersStopped)
			done, err := sp.SendBatch(context.Background(), target, source, nil)
			require.NoError(t, err)
			require.False(t, done)
			borrowed, err := sp.ReceiveBatch(0)
			require.NoError(t, err)
			sp.ForceCleanupAfterTerminalSignal()
			require.Positive(t, mp.AllocatedBytes(), "normal cleanup must preserve borrowed/queued frames")
			require.Equal(t, []int64{11, 12}, vector.MustFixedColNoTypeCheck[int64](borrowed.Vecs[0]))
			// The test now stops all readers without consuming End, matching the
			// query root after its pipeline goroutines and cleanup have joined.
			sp.CloseAfterReadersStopped()
			require.Zero(t, mp.AllocatedBytes())
			sp.CloseAfterReadersStopped()
			require.Zero(t, mp.AllocatedBytes(), "terminal teardown is idempotent")
		})
	}
}

func TestSpoolCopyFailureReturnsMessageAndCacheSlots(t *testing.T) {
	sourceMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(sourceMP) })
	source := spoolOwnerInput(t, sourceMP, 2)
	mp, err := mpool.NewMPool("spool-copy-failure", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	// MPool's supported minimum cap is 1MiB. Leave exactly enough room for
	// one 16-byte column, making the second column fail deterministically.
	filler, err := mp.Alloc((1<<20)-24, true)
	require.NoError(t, err)
	t.Cleanup(func() { mp.Free(filler) })
	baseline := mp.AllocatedBytes()
	sp := InitMyPipelineSpool(mp, 1)
	t.Cleanup(sp.CloseAfterReadersStopped)
	for range 3 {
		_, err = sp.SendBatch(context.Background(), 0, source, nil)
		require.Error(t, err, "second column must fail after the first allocation")
		require.Len(t, sp.freeShardPool, 2)
		require.Len(t, sp.cache.buffer.readyToUse, 2)
		require.Equal(t, baseline, mp.AllocatedBytes())
	}
	// A subsequent valid frame uses the same spool without losing a slot or
	// inheriting the failed frame's partially initialized columns.
	small := source.SelectColumns([]int{0}, nil)
	_, err = sp.SendBatch(context.Background(), 0, small, nil)
	require.NoError(t, err)
	got, err := sp.ReceiveBatch(0)
	require.NoError(t, err)
	require.Equal(t, []int64{11, 12}, vector.MustFixedColNoTypeCheck[int64](got.Vecs[0]))
	sp.ReleaseCurrent(0)
	_, err = sp.SendBatch(context.Background(), 0, nil, nil)
	require.NoError(t, err)
	got, err = sp.ReceiveBatch(0)
	require.NoError(t, err)
	require.Nil(t, got)
	sp.ForceCleanupAfterTerminalSignal()
	require.Equal(t, baseline, mp.AllocatedBytes())
	sp.CloseAfterReadersStopped()
	require.Equal(t, baseline, mp.AllocatedBytes())
}

// Cache cleanup may run while a terminal receiver still retains data. Its Once
// cannot also guard the joined-attempt slot destructor.
func TestSpoolFinalDestructorAfterCacheCleanup(t *testing.T) {
	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(srcMP) })
	src := spoolOwnerInput(t, srcMP, 1)
	for _, receivers := range []uint32{1, 2} {
		t.Run(fmt.Sprintf("receivers_%d", receivers), func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { require.Zero(t, mp.AllocatedBytes()); mpool.DeleteMPool(mp) })
			sp := InitMyPipelineSpool(mp, receivers)
			t.Cleanup(sp.CloseAfterReadersStopped)
			_, err := sp.SendBatch(context.Background(), SendToAllLocal, src, nil)
			require.NoError(t, err)
			held, err := sp.ReceiveBatch(0)
			require.NoError(t, err)
			sp.ForceCleanupAfterTerminalSignal()
			require.True(t, sp.cleanupDone)
			require.Equal(t, []int64{11, 12}, vector.MustFixedColNoTypeCheck[int64](held.Vecs[0]))
			// No readers can run beyond this explicit fixture boundary.
			sp.CloseAfterReadersStopped()
			require.Zero(t, mp.AllocatedBytes())
			for _, slot := range sp.shardPool {
				require.Nil(t, slot.dataContent)
			}
			for _, receiver := range sp.rs {
				require.False(t, receiver.hasLastPop)
			}
			sp.ReleaseCurrent(0)
			sp.Abort(nil)
			sp.CloseAfterReadersStopped()
			require.Zero(t, mp.AllocatedBytes())
		})
	}
}

func TestSpoolAbortReusesAdmittedBookkeeping(t *testing.T) {
	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(srcMP) })
	src := spoolOwnerInput(t, srcMP, 1)
	for _, receivers := range []uint32{1, 3} {
		t.Run(fmt.Sprintf("receivers_%d", receivers), func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { require.Zero(t, mp.AllocatedBytes()); mpool.DeleteMPool(mp) })
			sp := InitMyPipelineSpool(mp, receivers)
			t.Cleanup(sp.CloseAfterReadersStopped)
			refs, scratch := &sp.shardRefs[0], &sp.abortCurrentRefs[0]
			require.Len(t, sp.shardRefs, len(sp.shardPool))
			require.Len(t, sp.abortCurrentRefs, len(sp.shardPool))
			_, err := sp.SendBatch(context.Background(), SendToAllLocal, src, nil)
			require.NoError(t, err)
			held, err := sp.ReceiveBatch(0)
			require.NoError(t, err)
			sp.Abort(context.Canceled)
			require.Same(t, refs, &sp.shardRefs[0])
			require.Same(t, scratch, &sp.abortCurrentRefs[0])
			require.Equal(t, int32(1), sp.abortCurrentRefs[0]+sp.abortCurrentRefs[1])
			require.Equal(t, []int64{11, 12}, vector.MustFixedColNoTypeCheck[int64](held.Vecs[0]))
			require.Zero(t, testing.AllocsPerRun(1, func() { sp.Abort(context.Canceled) }))
			sp.ReleaseCurrent(0)
			require.Zero(t, mp.AllocatedBytes())
		})
	}
}

func TestSpoolFirstAbortAllocatesNoScratch(t *testing.T) {
	for _, receivers := range []uint32{1, 3} {
		mp := mpool.MustNewZeroNoFixed()
		spools := []*PipelineSpool{InitMyPipelineSpool(mp, receivers), InitMyPipelineSpool(mp, receivers)}
		next := 0
		// AllocsPerRun warms up once and then measures the other spool's first Abort.
		require.Zero(t, testing.AllocsPerRun(1, func() { spools[next].Abort(nil); next++ }))
		for _, sp := range spools {
			sp.CloseAfterReadersStopped()
		}
		mpool.DeleteMPool(mp)
	}
}
