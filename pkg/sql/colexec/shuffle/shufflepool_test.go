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
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func writeSelectionsForTest(sp *ShufflePool, src *batch.Batch, sels [][]int32, proc *process.Process) (bool, error) {
	_, _, _, done, err := sp.tryWrite(src, sels, 0, 0, proc)
	return done, err
}

func writeBatchToBucketForTest(sp *ShufflePool, src *batch.Batch, proc *process.Process, bucket int32) (bool, error) {
	sels := make([][]int32, sp.bucketNum)
	sels[bucket] = make([]int32, src.RowCount())
	for i := range sels[bucket] {
		sels[bucket][i] = int32(i)
	}
	return writeSelectionsForTest(sp, src, sels, proc)
}

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
		done, err := writeBatchToBucketForTest(sp, bat, proc, bucket)
		require.NoError(t, err)
		require.True(t, done)
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
	done, err := writeBatchToBucketForTest(sp, bat, proc, 0)
	require.NoError(t, err)
	require.True(t, done)
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
	require.Equal(t, 1, sp.batchPoolLength())
	peak, ownsStats = sp.release(proc.Mp(), false)
	require.Positive(t, peak)
	require.True(t, ownsStats)
	require.True(t, sp.cleaned)
	require.Zero(t, sp.batchPoolLength())
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

func TestShufflePoolReservesReadyCreditForProvenanceChange(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	defer proc.Free()
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)

	sp := NewShufflePool(1, 1, true)
	unaccounted := testutil.NewBatch([]types.Type{types.T_int64.ToType()}, false, 2, mp)
	done, err := writeBatchToBucketForTest(sp, unaccounted, proc, 0)
	require.NoError(t, err)
	require.True(t, done)
	require.Zero(t, sp.readyCount)

	accounted := batch.NewWithSchema(
		true,
		nil,
		[]types.Type{types.T_int64.ToType()},
	)
	require.NoError(t, accounted.SetAllocationAccount(selection))
	require.NoError(t, vector.AppendFixed(accounted.Vecs[0], int64(7), false, mp))
	accounted.SetRowCount(1)
	done, err = writeBatchToBucketForTest(sp, accounted, proc, 0)
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, 1, sp.readyCount)

	ready := sp.getAnyFullBatch()
	require.NotNil(t, ready)
	require.Equal(t, 2, ready.RowCount())
	sp.discardBatch(ready, mp)
	require.Zero(t, sp.readyCount)
	tail := sp.getAnyLastBatch()
	require.NotNil(t, tail)
	require.Equal(t, 1, tail.RowCount())
	require.Same(t, selection, tail.AllocationAccountSelection())
	sp.discardBatch(tail, mp)

	unaccounted.Clean(mp)
	accounted.Clean(mp)
	sp.abort(mp)
	require.Zero(t, account.Seal().Used)
	_, err = registry.Finalize(account)
	require.NoError(t, err)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestShufflePoolFixedBucketsHaveIndependentBackpressure(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	sp := NewShufflePool(2, 2, false)

	input := testutil.NewBatch(
		[]types.Type{types.T_int64.ToType()}, false, objectio.BlockMaxRows, proc.Mp())
	defer input.Clean(proc.Mp())

	// Fill bucket 0 until its writer is backpressured. With the old global
	// credit pool, this also exhausted the credits needed by every other bucket.
	var bucket0Waiter <-chan struct{}
	for attempts := 0; attempts <= sp.readyLimit; attempts++ {
		sels := make([][]int32, sp.bucketNum)
		sels[0] = make([]int32, input.RowCount())
		for i := range sels[0] {
			sels[0][i] = int32(i)
		}
		_, _, waiter, done, err := sp.tryWrite(input, sels, 0, 0, proc)
		require.NoError(t, err)
		if !done {
			bucket0Waiter = waiter
			break
		}
	}
	require.NotNil(t, bucket0Waiter)

	// Bucket 1 has its own consumer and capacity. A hot bucket must not prevent
	// publishing a batch that can wake this independent consumer.
	done, err := writeBatchToBucketForTest(sp, input, proc, 1)
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, 3, sp.readyCount)

	bat := sp.getFullBatch(0)
	require.NotNil(t, bat)
	sp.discardBatch(bat, proc.Mp())
	select {
	case <-bucket0Waiter:
	default:
		t.Fatal("draining a fixed bucket did not wake that bucket's writer")
	}

	for bucket := int32(0); bucket < sp.bucketNum; bucket++ {
		for bat = sp.getFullBatch(bucket); bat != nil; bat = sp.getFullBatch(bucket) {
			sp.discardBatch(bat, proc.Mp())
		}
	}
	require.Zero(t, sp.readyCount)
	sp.abort(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestShufflePoolFinalDrainDoesNotStealClaimedReadyBatch(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rows     int
		tailRows int
	}{
		{name: "full batch only", rows: objectio.BlockMaxRows},
		{name: "full batch and partial tail", rows: objectio.BlockMaxRows + 1, tailRows: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			sp := NewShufflePool(1, 2, true)
			require.True(t, sp.hold())
			require.True(t, sp.hold())

			input := testutil.NewBatch([]types.Type{types.T_int64.ToType()}, false, tc.rows, proc.Mp())
			done, err := writeBatchToBucketForTest(sp, input, proc, 0)
			require.NoError(t, err)
			require.True(t, done)
			input.Clean(proc.Mp())
			sp.stopWriting()
			sp.stopWriting()

			claimed := make(chan int32)
			resume := make(chan struct{}, 1)
			readyBatch := make(chan *batch.Batch, 1)
			defer func() {
				select {
				case resume <- struct{}{}:
				default:
				}
			}()
			go func() {
				bucket := <-sp.readyBuckets
				claimed <- bucket
				<-resume
				readyBatch <- sp.popReadyBatch(bucket)
			}()

			require.Equal(t, int32(0), <-claimed)
			tail := sp.getAnyLastBatch()
			resume <- struct{}{}
			full := <-readyBatch

			if tc.tailRows == 0 {
				require.Nil(t, tail)
			} else {
				require.NotNil(t, tail)
				require.Equal(t, tc.tailRows, tail.RowCount())
				sp.discardBatch(tail, proc.Mp())
			}
			require.Equal(t, objectio.BlockMaxRows, full.RowCount())
			sp.discardBatch(full, proc.Mp())
			require.Zero(t, sp.readyCount)

			sp.release(proc.Mp(), false)
			sp.release(proc.Mp(), false)
			require.Equal(t, int64(0), proc.Mp().CurrNB())
		})
	}
}

func TestShufflePoolRecycleCacheUsesWorkerBound(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	sp := NewShufflePool(128, 2, false)
	for i := range sp.readyLimit + 3 {
		bat := batch.NewOffHeapWithSize(1)
		bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
		require.NoError(t, bat.Vecs[0].PreExtend(1, proc.Mp()))
		bat.ShuffleIDX = int32(i) % sp.maxHolders
		sp.putBatchToPool(bat, proc.Mp())
	}
	require.Equal(t, sp.readyLimit, sp.batchPoolLength())
	sp.abort(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func fillShufflePoolRecycleCache(
	tb testing.TB,
	sp *ShufflePool,
	proc *process.Process,
	rows int,
) map[*batch.Batch]struct{} {
	tb.Helper()
	pooled := make(map[*batch.Batch]struct{}, sp.readyLimit)
	for i := range sp.readyLimit {
		bat := batch.NewOffHeapWithSize(1)
		bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
		if err := bat.Vecs[0].PreExtend(rows, proc.Mp()); err != nil {
			tb.Fatal(err)
		}
		bat.ShuffleIDX = int32(i) % sp.bucketNum
		pooled[bat] = struct{}{}
		sp.putBatchToPool(bat, proc.Mp())
	}
	return pooled
}

func TestShufflePoolRecycleCacheReusesAcrossBucketLayouts(t *testing.T) {
	for _, tc := range []struct {
		name       string
		bucketNum  int32
		maxHolders int32
		drainAll   bool
	}{
		{name: "single-cn", bucketNum: 16, maxHolders: 16},
		{name: "multi-cn", bucketNum: 128, maxHolders: 16},
		{name: "drain-all", bucketNum: 128, maxHolders: 1, drainAll: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			sp := NewShufflePool(tc.bucketNum, tc.maxHolders, tc.drainAll)

			pooled := fillShufflePoolRecycleCache(t, sp, proc, 128)
			require.Equal(t, sp.readyLimit, sp.batchPoolLength())
			input := testutil.NewBatch(
				[]types.Type{types.T_int64.ToType()}, false, 128, proc.Mp())

			// Visit every bucket after warming only the holder-bounded recycle
			// shards. Reuse must not grow linearly with the global bucket count.
			for bucket := int32(0); bucket < tc.bucketNum; bucket++ {
				done, err := writeBatchToBucketForTest(sp, input, proc, bucket)
				require.NoError(t, err)
				require.True(t, done)
				output := sp.getLastBatch(bucket)
				require.NotNil(t, output)
				_, reused := pooled[output]
				require.True(t, reused,
					"bucket %d allocated instead of reusing its holder shard", bucket)
				output.CleanOnlyData()
				sp.putBatchToPool(output, proc.Mp())
			}

			require.Equal(t, sp.readyLimit, sp.batchPoolLength())
			input.Clean(proc.Mp())
			sp.abort(proc.Mp())
			require.Equal(t, int64(0), proc.Mp().CurrNB())
		})
	}
}

func TestShufflePoolRecycleCacheWarmsColdShardOnce(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	sp := NewShufflePool(128, 16, false)

	for range shuffleBatchPoolShardSize {
		bat := batch.NewOffHeapWithSize(1)
		bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
		require.NoError(t, bat.Vecs[0].PreExtend(1, proc.Mp()))
		bat.ShuffleIDX = 0
		sp.putBatchToPool(bat, proc.Mp())
	}
	require.Equal(t, shuffleBatchPoolShardSize, sp.batchPoolLength())

	input := testutil.NewBatch(
		[]types.Type{types.T_int64.ToType()}, false, 1, proc.Mp())
	var warmed *batch.Batch
	for _, bucket := range []int32{1, 17, 33, 49, 65, 81, 97, 113} {
		done, err := writeBatchToBucketForTest(sp, input, proc, bucket)
		require.NoError(t, err)
		require.True(t, done)
		output := sp.getLastBatch(bucket)
		require.NotNil(t, output)
		if warmed == nil {
			warmed = output
		} else {
			require.Same(t, warmed, output,
				"a warmed holder shard must not repeatedly allocate after a phase change")
		}
		output.CleanOnlyData()
		sp.putBatchToPool(output, proc.Mp())
	}

	require.Equal(t, shuffleBatchPoolShardSize+1, sp.batchPoolLength())
	input.Clean(proc.Mp())
	sp.abort(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestShufflePoolRecycleCacheConcurrentReuse(t *testing.T) {
	const (
		shards          = 16
		workersPerShard = 2
		iterations      = 100
	)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	sp := NewShufflePool(128, shards, false)
	pooled := fillShufflePoolRecycleCache(t, sp, proc, 1)

	var wg sync.WaitGroup
	errC := make(chan string, shards*workersPerShard)
	for worker := range shards * workersPerShard {
		wg.Add(1)
		go func(bucket int32) {
			defer wg.Done()
			for i := range iterations {
				buf := sp.getBatchFromPool(bucket)
				if buf == nil {
					errC <- "recycle shard unexpectedly empty"
					return
				}
				if _, ok := pooled[buf]; !ok {
					sp.putBatchToPool(buf, proc.Mp())
					errC <- "recycle shard returned an unknown batch"
					return
				}
				buf.ShuffleIDX = bucket + int32(i%8)*shards
				sp.putBatchToPool(buf, proc.Mp())
			}
		}(int32(worker % shards))
	}
	wg.Wait()
	close(errC)
	for err := range errC {
		t.Error(err)
	}

	require.Equal(t, sp.readyLimit, sp.batchPoolLength())
	sp.abort(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func BenchmarkShufflePoolRecycleCache(b *testing.B) {
	for _, tc := range []struct {
		name       string
		bucketNum  int32
		maxHolders int32
	}{
		{name: "single-cn-16-buckets", bucketNum: 16, maxHolders: 16},
		{name: "multi-cn-128-buckets", bucketNum: 128, maxHolders: 16},
		{name: "multi-cn-1024-buckets", bucketNum: 1024, maxHolders: 16},
	} {
		b.Run("pool-round-robin/"+tc.name, func(b *testing.B) {
			proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
			defer proc.Free()
			sp := NewShufflePool(tc.bucketNum, tc.maxHolders, false)
			defer sp.abort(proc.Mp())
			fillShufflePoolRecycleCache(b, sp, proc, 1)

			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				buf := sp.getBatchFromPool(int32(i) % tc.bucketNum)
				if buf == nil {
					b.Fatal("recycle cache unexpectedly empty")
				}
				buf.ShuffleIDX = int32(i) % tc.bucketNum
				sp.putBatchToPool(buf, proc.Mp())
			}
			b.StopTimer()
		})
	}

	b.Run("pool-parallel/fixed-16-workers", func(b *testing.B) {
		const workers = 16
		proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
		defer proc.Free()
		sp := NewShufflePool(128, workers, false)
		defer sp.abort(proc.Mp())
		fillShufflePoolRecycleCache(b, sp, proc, 1)

		b.ReportAllocs()
		b.ResetTimer()
		var wg sync.WaitGroup
		wg.Add(workers)
		for worker := range workers {
			iterations := b.N / workers
			if worker < b.N%workers {
				iterations++
			}
			go func(bucket int32, iterations int) {
				defer wg.Done()
				for range iterations {
					buf := sp.getBatchFromPool(bucket)
					if buf == nil {
						panic("recycle cache unexpectedly empty")
					}
					buf.ShuffleIDX = bucket
					sp.putBatchToPool(buf, proc.Mp())
				}
			}(int32(worker), iterations)
		}
		wg.Wait()
		b.StopTimer()
	})
	b.Run("try-write-parallel/fixed-16-workers", func(b *testing.B) {
		benchmarkShufflePoolTryWriteParallel(b)
	})

	for _, tc := range []struct {
		name       string
		bucketNum  int32
		maxHolders int32
		drainAll   bool
	}{
		{name: "single-cn-16-buckets", bucketNum: 16, maxHolders: 16},
		{name: "multi-cn-128-buckets", bucketNum: 128, maxHolders: 16},
		{name: "multi-cn-1024-buckets", bucketNum: 1024, maxHolders: 16},
		{name: "drain-all-128-buckets", bucketNum: 128, maxHolders: 1, drainAll: true},
	} {
		b.Run("try-write-round-robin/"+tc.name, func(b *testing.B) {
			benchmarkShufflePoolTryWrite(b, tc.bucketNum, tc.maxHolders, tc.drainAll, false)
		})
	}
	b.Run("try-write-phased/multi-cn-128-buckets", func(b *testing.B) {
		benchmarkShufflePoolTryWrite(b, 128, 16, false, true)
	})
}

func benchmarkShufflePoolTryWrite(
	b *testing.B,
	bucketNum int32,
	maxHolders int32,
	drainAll bool,
	phased bool,
) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	defer proc.Free()
	sp := NewShufflePool(bucketNum, maxHolders, drainAll)
	defer sp.abort(proc.Mp())
	fillShufflePoolRecycleCache(b, sp, proc, 1)

	input := testutil.NewBatch(
		[]types.Type{types.T_int64.ToType()}, false, 1, proc.Mp())
	defer input.Clean(proc.Mp())
	sels := make([][]int32, bucketNum)
	oneRow := []int32{0}
	previousBucket := int32(-1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		bucket := int32(i) % bucketNum
		if phased {
			phase := min(3, i*4/max(1, b.N))
			bucket = [...]int32{0, bucketNum - 1, bucketNum / 2, 1}[phase]
		}
		if previousBucket != bucket {
			if previousBucket >= 0 {
				sels[previousBucket] = nil
			}
			sels[bucket] = oneRow
			previousBucket = bucket
		}
		done, err := writeSelectionsForTest(sp, input, sels, proc)
		if err != nil {
			b.Fatal(err)
		}
		if !done {
			b.Fatal("recycle benchmark was backpressured")
		}
		output := sp.getLastBatch(bucket)
		if output == nil {
			b.Fatal("recycle benchmark produced no output")
		}
		output.CleanOnlyData()
		sp.putBatchToPool(output, proc.Mp())
	}
	b.StopTimer()
}

func benchmarkShufflePoolTryWriteParallel(b *testing.B) {
	const workers = 16
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	defer proc.Free()
	sp := NewShufflePool(128, workers, false)
	defer sp.abort(proc.Mp())
	fillShufflePoolRecycleCache(b, sp, proc, 1)

	input := testutil.NewBatch(
		[]types.Type{types.T_int64.ToType()}, false, 1, proc.Mp())
	defer input.Clean(proc.Mp())
	selsByWorker := make([][][]int32, workers)
	for worker := range workers {
		selsByWorker[worker] = make([][]int32, sp.bucketNum)
		selsByWorker[worker][worker] = []int32{0}
	}

	b.ReportAllocs()
	b.ResetTimer()
	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := range workers {
		iterations := b.N / workers
		if worker < b.N%workers {
			iterations++
		}
		go func(bucket int32, iterations int) {
			defer wg.Done()
			for range iterations {
				done, err := writeSelectionsForTest(
					sp, input, selsByWorker[bucket], proc)
				if err != nil {
					panic(err)
				}
				if !done {
					panic("parallel recycle benchmark was backpressured")
				}
				output := sp.getLastBatch(bucket)
				if output == nil {
					panic("parallel recycle benchmark produced no output")
				}
				output.CleanOnlyData()
				sp.putBatchToPool(output, proc.Mp())
			}
		}(int32(worker), iterations)
	}
	wg.Wait()
	b.StopTimer()
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

func TestShufflePoolMemoryAccountingAcrossShards(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	sp := NewShufflePool(64, 64, false)

	batches := make([]*batch.Batch, 64)
	var expected int64
	for i := range batches {
		batches[i] = batch.NewOffHeapWithSize(1)
		batches[i].Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
		require.NoError(t, batches[i].Vecs[0].PreExtend(128+i, proc.Mp()))
		batches[i].ShuffleIDX = int32(i)
		expected += int64(batches[i].Allocated())
	}

	var wg sync.WaitGroup
	wg.Add(len(batches))
	for _, bat := range batches {
		go func() {
			defer wg.Done()
			sp.putBatchToPool(bat, proc.Mp())
		}()
	}
	wg.Wait()

	require.Equal(t, expected, sp.current.Load())
	require.Equal(t, expected, sp.memoryPeak())
	sp.abort(proc.Mp())
	require.Zero(t, sp.current.Load())
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
				_, err := writeBatchToBucketForTest(sp, input, proc, 0)
				return err
			},
			check: func(t *testing.T, sp *ShufflePool) {
				require.Equal(t, 1, sp.batchPoolLength())
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
				_, err := writeSelectionsForTest(sp, input, [][]int32{sels}, proc)
				return err
			},
			check: func(t *testing.T, sp *ShufflePool) {
				require.Zero(t, sp.batchPoolLength())
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

func TestVMPrepareRejectsAbortedSharedPool(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	sp := NewShufflePool(1, 1, false)
	sp.abort(proc.Mp())
	arg := NewArgument()
	defer arg.Release()
	arg.BucketNum = 1
	arg.SetShufflePool(sp)

	err := vm.Prepare(arg, proc)
	require.Error(t, err)
	producerCtx := arg.ctr.producerProc.Ctx
	arg.Reset(proc, true, err)
	select {
	case <-producerCtx.Done():
	default:
		t.Fatal("failed VM preparation leaked the producer context")
	}
	arg.Free(proc, true, err)
}
