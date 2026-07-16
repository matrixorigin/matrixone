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

package shuffle

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestShufflePoolStopsOnlyAfterEveryWriter(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	sp := NewShufflePool(2, 2, false)
	require.True(t, sp.hold())
	require.True(t, sp.hold())

	sp.stopWriting()
	require.False(t, sp.allStop())
	sp.stopWriting()
	require.True(t, sp.allStop())
	sp.release(proc.Mp(), false)
	sp.release(proc.Mp(), false)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestShufflePoolDrainAllBucketsIsFair(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	sp := NewShufflePool(3, 1, true)

	for bucket := int32(0); bucket < 3; bucket++ {
		bat := testutil.NewBatch([]types.Type{types.T_int64.ToType()}, false, 1, proc.Mp())
		require.NoError(t, sp.putAllBatchIntoPoolByShuffleIdx(bat, proc, bucket))
		bat.Clean(proc.Mp())
	}

	for expected := int32(0); expected < 3; expected++ {
		bat := sp.getAnyLastBatch()
		require.NotNil(t, bat)
		require.Equal(t, expected, bat.ShuffleIDX)
		bat.Clean(proc.Mp())
	}
	require.Nil(t, sp.getAnyLastBatch())
	sp.abort(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestShufflePoolAbortDefersCleanupUntilLastHolderAndIsIdempotent(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	sp := NewShufflePool(2, 2, false)
	require.True(t, sp.hold())

	bat := testutil.NewBatch([]types.Type{types.T_int64.ToType()}, false, 8, proc.Mp())
	require.NoError(t, sp.putAllBatchIntoPoolByShuffleIdx(bat, proc, 0))
	bat.Clean(proc.Mp())

	sp.abort(proc.Mp())
	require.Positive(t, proc.Mp().CurrNB(), "an admitted holder can still access pool batches")
	require.False(t, sp.hold(), "abort must reject holders from a later prepare")
	sp.abort(proc.Mp())
	sp.release(proc.Mp(), false)
	sp.abort(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestShufflePoolGracefulCleanupWaitsForAllExpectedHolders(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	sp := NewShufflePool(1, 2, false)
	require.True(t, sp.hold())
	require.True(t, sp.hold())

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.NewInt64Vector(8, types.T_int64.ToType(), proc.Mp(), false, nil, []int64{0, 1, 2, 3, 4, 5, 6, 7})
	bat.SetRowCount(8)
	sp.putBatchToPool(bat, proc.Mp())

	peak, ownsStats := sp.release(proc.Mp(), false)
	require.Zero(t, peak)
	require.False(t, ownsStats)
	require.False(t, sp.cleaned)
	require.Len(t, sp.batchPool, 1)
	peak, ownsStats = sp.release(proc.Mp(), false)
	require.Positive(t, peak)
	require.True(t, ownsStats)
	require.True(t, sp.cleaned)
	require.Empty(t, sp.batchPool)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestShufflePoolBoundsReadyBatchesAndResumes(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	sp := NewShufflePool(1, 1, true)
	require.True(t, sp.hold())

	rows := objectio.BlockMaxRows * 4
	input := testutil.NewBatch([]types.Type{types.T_int64.ToType()}, false, rows, proc.Mp())
	sels := make([][]int32, 1)
	sels[0] = make([]int32, rows)
	for i := range sels[0] {
		sels[0][i] = int32(i)
	}

	bucket, offset, waiter, done, err := sp.tryWrite(input, sels, 0, 0, proc)
	require.NoError(t, err)
	require.False(t, done)
	require.Equal(t, 0, bucket)
	require.Equal(t, objectio.BlockMaxRows*2, offset)
	require.NotNil(t, waiter)
	require.Equal(t, sp.readyLimit, sp.readyCount)

	for !done {
		first := sp.getAnyFullBatch()
		require.NotNil(t, first)
		sp.discardBatch(first, proc.Mp())
		select {
		case <-waiter:
		default:
			t.Fatal("freeing a ready batch did not wake blocked writers")
		}
		bucket, offset, waiter, done, err = sp.tryWrite(input, sels, bucket, offset, proc)
		require.NoError(t, err)
		require.LessOrEqual(t, sp.readyCount, sp.readyLimit)
	}
	require.True(t, done)
	require.LessOrEqual(t, sp.readyCount, sp.readyLimit)

	for bat := sp.getAnyFullBatch(); bat != nil; bat = sp.getAnyFullBatch() {
		sp.discardBatch(bat, proc.Mp())
	}
	last := sp.getAnyLastBatch()
	if last != nil {
		sp.discardBatch(last, proc.Mp())
	}
	input.Clean(proc.Mp())
	peak, ownsStats := sp.release(proc.Mp(), false)
	require.True(t, ownsStats)
	require.Positive(t, peak)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestShufflePoolRecycleCacheUsesWorkerBound(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	sp := NewShufflePool(128, 2, false)
	for range sp.readyLimit + 3 {
		bat := batch.NewOffHeapWithSize(1)
		bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
		require.NoError(t, bat.Vecs[0].PreExtend(1, proc.Mp()))
		sp.putBatchToPool(bat, proc.Mp())
	}
	require.Len(t, sp.batchPool, sp.readyLimit)
	sp.abort(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestShufflePoolPeakIsReportedByExactlyOneHolder(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	sp := NewShufflePool(2, 2, false)
	args := []*Shuffle{NewArgument(), NewArgument()}
	defer func() {
		for _, arg := range args {
			arg.Free(proc, false, nil)
			arg.Release()
		}
	}()
	for i, arg := range args {
		arg.BucketNum = 2
		arg.CurrentShuffleIdx = int32(i)
		arg.SetShufflePool(sp)
		require.NoError(t, arg.Prepare(proc))
	}

	buf := batch.NewOffHeapWithSize(1)
	buf.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, buf.Vecs[0].PreExtend(128, proc.Mp()))
	sp.putBatchToPool(buf, proc.Mp())

	args[0].Reset(proc, false, nil)
	require.Zero(t, args[0].OpAnalyzer.GetOpStats().MemorySize)
	args[1].Reset(proc, false, nil)
	require.Positive(t, args[1].OpAnalyzer.GetOpStats().MemorySize)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestShufflePoolRetainsOwnershipWhenBatchSetWriteFails(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(*ShufflePool, *batch.Batch, *process.Process) error
		check func(*testing.T, *ShufflePool)
	}{
		{
			name: "extend returns unconsumed reuse buffer to pool",
			write: func(sp *ShufflePool, input *batch.Batch, proc *process.Process) error {
				return sp.putAllBatchIntoPoolByShuffleIdx(input, proc, 0)
			},
			check: func(t *testing.T, sp *ShufflePool) {
				require.Len(t, sp.batchPool, 1)
				require.Zero(t, sp.batchSets[0].Length())
			},
		},
		{
			name: "union transfers consumed reuse buffer to batch set",
			write: func(sp *ShufflePool, input *batch.Batch, proc *process.Process) error {
				sels := make([]int32, 512)
				for i := range sels {
					sels[i] = int32(i * 2)
				}
				return sp.putBatchIntoShuffledPoolsBySels(input, [][]int32{sels}, proc)
			},
			check: func(t *testing.T, sp *ShufflePool) {
				require.Empty(t, sp.batchPool)
				require.Equal(t, 1, sp.batchSets[0].Length())
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp, err := mpool.NewMPool("shuffle-write-error", 1024*1024, mpool.NoFixed)
			require.NoError(t, err)
			proc := testutil.NewProcessWithMPool(t, "", mp)
			sp := NewShufflePool(1, 1, false)
			require.True(t, sp.hold())
			var (
				input    *batch.Batch
				filler   []byte
				released bool
			)
			defer func() {
				if input != nil {
					input.Clean(mp)
				}
				if filler != nil {
					mp.Free(filler)
				}
				if !released {
					sp.release(mp, true)
				}
				proc.Free()
				mpool.DeleteMPool(mp)
			}()

			reuseBuf := batch.NewOffHeapWithSize(1)
			reuseBuf.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
			sp.putBatchToPool(reuseBuf, mp)
			input = batch.NewWithSize(1)
			input.Vecs[0] = testutil.NewInt64Vector(1024, types.T_int64.ToType(), mp, false, nil, nil)
			input.SetRowCount(1024)
			remaining := mp.Cap() - mp.CurrNB()
			require.Greater(t, remaining, int64(16*1024))
			filler, err = mp.Alloc(int(remaining-1024), true)
			require.NoError(t, err)

			err = tc.write(sp, input, proc)
			require.Error(t, err)
			tc.check(t, sp)

			input.Clean(mp)
			input = nil
			mp.Free(filler)
			filler = nil
			sp.release(mp, true)
			released = true
			require.Equal(t, int64(0), mp.CurrNB())
		})
	}
}

func TestShufflePoolEndWakesEveryAllBucketDrainer(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	sp := NewShufflePool(2, 3, true)
	for range 3 {
		require.True(t, sp.hold())
	}

	done := make(chan struct{}, 3)
	for range 3 {
		go func() {
			sp.waitAnyBatchOrEnd(proc)
			done <- struct{}{}
		}()
	}
	for range 3 {
		sp.stopWriting()
	}
	for range 3 {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("all-bucket drainer remained blocked after shuffle ended")
		}
	}
	for range 3 {
		sp.release(proc.Mp(), false)
	}
}

func TestShufflePrepareRejectsAbortedSharedPool(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	sp := NewShufflePool(1, 1, false)
	sp.abort(proc.Mp())
	arg := NewArgument()
	defer arg.Release()
	arg.BucketNum = 1
	arg.SetShufflePool(sp)

	err := arg.Prepare(proc)
	require.Error(t, err)
	arg.Reset(proc, true, err)
	arg.Free(proc, true, err)
}
