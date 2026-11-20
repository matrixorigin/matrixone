// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/stretchr/testify/require"
)

func TestShuffleDictionaryPartitionsOwnBacking(t *testing.T) {
	testShuffleDictionaryPartitions(t, 0, true)
}

func TestShuffleDictionaryCapacityRefusalReleasesScratch(t *testing.T) {
	// Source plus two ordinary int64 partitions fit exactly. Optional selector
	// scratch must not prevent the second destination from using that capacity.
	testShuffleDictionaryPartitions(t, 4*objectio.BlockMaxRows*8, false)
}

func testShuffleDictionaryPartitions(t *testing.T, capacity uint64, expectViews bool) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	var allocation *vector.AllocationAccountSelection
	var err error
	if capacity > 0 {
		registry, err := mpool.NewAllocationAccountRegistry(1, 128)
		require.NoError(t, err)
		account, err := registry.Open(capacity)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.Zero(t, account.Snapshot().Used)
			_, _, err := registry.CompleteTerminal(account)
			require.NoError(t, err)
		})
		allocation, err = vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2)
		require.NoError(t, err)
	}
	var input *batch.Batch
	if expectViews {
		input = newShuffleDictionaryInput(t, proc.Mp(), 0)
	} else {
		input = batch.NewWithSchema(true, nil, []types.Type{types.T_int64.ToType()})
		t.Cleanup(func() { input.Clean(proc.Mp()) })
		require.NoError(t, input.SetAllocationAccount(allocation))
		values := make([]int64, 2*objectio.BlockMaxRows)
		for row := range values {
			values[row] = int64(row)
		}
		require.NoError(t, vector.AppendFixedList(input.Vecs[0], values, nil, proc.Mp()))
		input.SetRowCount(len(values))
	}
	selections := make([][]int32, 2)
	for row := range input.RowCount() {
		selections[row%2] = append(selections[row%2], int32(row))
	}
	sp := NewShufflePool(2, 1, true)
	require.True(t, sp.hold())
	t.Cleanup(func() { sp.release(proc.Mp(), true) })
	var views *shufflePartitionViews
	t.Cleanup(func() {
		if views != nil {
			views.free(proc.Mp())
		}
	})
	_, _, _, done, err := sp.tryWriteModeWithViews(input, selections, 0, 0, 0, false, &views, proc)
	require.NoError(t, err)
	require.True(t, done)
	if expectViews {
		require.NotNil(t, views)
		views.free(proc.Mp())
		views = nil
	} else {
		require.Nil(t, views, "the pinned backing bound must decline before allocating view state")
	}
	input.CleanOnlyData()
	// Reusing the producer must not mutate queued rows.
	if expectViews {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], types.Decimal128{B0_63: ^uint64(0)}, false, proc.Mp()))
	} else {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(-1), false, proc.Mp()))
	}
	for bucket := range 2 {
		output := sp.getAnyFullBatch()
		require.NotNil(t, output)
		t.Cleanup(func() { sp.discardBatch(output, proc.Mp()) })
		require.Equal(t, expectViews, output.HasDictionary())
		require.Equal(t, objectio.BlockMaxRows, output.RowCount())
		for row := range output.RowCount() {
			if expectViews {
				require.Equal(t, types.Decimal128{B0_63: uint64(1 + 2*(row%2))}, vector.GetFixedAtNoTypeCheck[types.Decimal128](output.Vecs[0], row))
			} else {
				require.Equal(t, int64(2*row+bucket), vector.GetFixedAtNoTypeCheck[int64](output.Vecs[0], row))
			}
		}
	}
	require.Nil(t, sp.getAnyFullBatch())
}

// Compact physical storage makes retained output cheaper than the owned copy,
// including its full backing cost. Its original flat owner is released here.
func newShuffleDictionaryInput(t *testing.T, mp *mpool.MPool, producer int) *batch.Batch {
	t.Helper()
	input := batch.NewOffHeapWithSize(1)
	t.Cleanup(func() { input.Clean(mp) })
	base := vector.NewOffHeapVecWithType(types.T_decimal128.ToType())
	defer base.Free(mp)
	require.NoError(t, vector.AppendFixedList(base, []types.Decimal128{
		{B0_63: uint64(producer*100000 + 1)}, {B0_63: uint64(producer*100000 + 3)},
	}, nil, mp))
	rows := make([]int64, 2*objectio.BlockMaxRows)
	for row := range rows {
		rows[row] = int64((row / 2) % 2)
	}
	admitted, err := vector.TrySelectDictionaryVectors([]*vector.Vector{base}, input.Vecs, rows, 2, mp, nil)
	require.NoError(t, err)
	require.True(t, admitted)
	input.SetRowCount(len(rows))
	return input
}

func TestShuffleDictionaryPreservesShortCoalescing(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	sp := NewShufflePool(2, 1, true)
	require.True(t, sp.hold())
	t.Cleanup(func() { sp.release(proc.Mp(), true) })
	var views *shufflePartitionViews
	for _, values := range [][]int64{{10, 11}, {20, 21}} {
		input := batch.NewWithSize(1)
		input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		t.Cleanup(func() { input.Clean(proc.Mp()) })
		require.NoError(t, vector.AppendFixedList(input.Vecs[0], values, nil, proc.Mp()))
		input.SetRowCount(2)
		_, _, _, done, err := sp.tryWriteModeWithViews(input, [][]int32{{0}, {1}}, 0, 0, 0, false, &views, proc)
		require.NoError(t, err)
		require.True(t, done)
	}
	require.Nil(t, views, "short fragments must not freeze or allocate view state")
	require.Nil(t, sp.getAnyFullBatch())
	sp.stopWriting()
	for bucket := range 2 {
		output := sp.getAnyStoppedBatch()
		require.NotNil(t, output)
		t.Cleanup(func() { sp.discardBatch(output, proc.Mp()) })
		require.False(t, output.HasDictionary())
		require.Equal(t, []int64{int64(10 + bucket), int64(20 + bucket)}, vector.MustFixedColNoTypeCheck[int64](output.Vecs[0]))
	}
	require.Nil(t, sp.getAnyStoppedBatch())
}

func TestShuffleDictionaryReleasesRowsBeforeCreditWait(t *testing.T) {
	for _, useViews := range []bool{false, true} {
		t.Run(map[bool]string{false: "owned", true: "retained"}[useViews], func(t *testing.T) {
			pool, err := mpool.NewMPool(t.Name(), 1<<20, mpool.NoFixed)
			require.NoError(t, err)
			proc := testutil.NewProcessWithOwnedMPool(t, "", pool)
			t.Cleanup(func() { require.Zero(t, pool.CurrNB()) })
			sp := NewShufflePool(2, 2, false)
			t.Cleanup(func() { sp.abort(pool) })
			input := newShuffleDictionaryInput(t, pool, 0)
			var views *shufflePartitionViews
			t.Cleanup(func() {
				if views != nil {
					views.free(pool)
				}
			})
			var workspace **shufflePartitionViews
			if useViews {
				workspace = &views
			}
			sels := make([][]int32, 2)
			for row := range input.RowCount() {
				sels[row%2] = append(sels[row%2], int32(row))
			}
			for range shuffleFixedBucketReadyLimit {
				_, _, _, done, err := sp.tryWrite(input, [][]int32{nil, sels[1]}, 0, 0, 0, proc)
				require.NoError(t, err)
				require.True(t, done)
			}
			const pressureBytes = 256 << 10
			filler, err := pool.Alloc(int(pool.Cap()-pool.CurrNB())-pressureBytes, true)
			require.NoError(t, err)
			t.Cleanup(func() { pool.Free(filler) })

			bucket, offset, waiter, done, err := sp.tryWriteModeWithViews(input, sels, 0, 0, 0, false, workspace, proc)
			require.NoError(t, err)
			require.False(t, done)
			require.Equal(t, 1, bucket)
			require.Zero(t, offset)
			require.NotNil(t, waiter)

			recycle := func(bucket int32, retained bool) *batch.Batch {
				output := sp.getFullBatch(bucket)
				require.NotNil(t, output)
				consumer := Shuffle{ctr: container{shufflePool: sp, buf: output, bufFromPool: true}}
				defer consumer.releasePreviousLocalBatch(proc)
				require.Equal(t, retained, output.HasDictionary())
				require.Equal(t, objectio.BlockMaxRows, output.RowCount())
				for row := range output.RowCount() {
					want := types.Decimal128{B0_63: uint64(1 + 2*(row%2))}
					if got := vector.GetFixedAtNoTypeCheck[types.Decimal128](output.Vecs[0], row); got != want {
						t.Fatalf("bucket %d row %d: got %v, want %v", bucket, row, got, want)
					}
				}
				return output
			}
			first := recycle(0, useViews)
			// Return the recycled header's payload capacity to the pool.
			recycled := sp.getBatchFromPool(0)
			if recycled != nil {
				sp.discardBatch(recycled, pool)
			}
			require.Same(t, first, recycled)
			select {
			case <-waiter:
				t.Fatal("recycling bucket 0 woke the writer waiting on bucket 1")
			default:
			}
			buffer, err := pool.Alloc(pressureBytes, true)
			require.NoError(t, err, "the blocked producer's temporary map must not consume this capacity")
			pool.Free(buffer)
			require.Equal(t, pool.Cap()-pressureBytes, pool.CurrNB())

			reuse := recycle(1, false)
			select {
			case <-waiter:
			default:
				t.Fatal("releasing bucket 1 credit did not wake its writer")
			}
			_, _, _, done, err = sp.tryWriteModeWithViews(input, sels, 0, bucket, offset, false, workspace, proc)
			require.NoError(t, err)
			require.True(t, done)
			recycle(1, false)
			require.Same(t, reuse, recycle(1, false))
			require.Zero(t, sp.readyCount)
		})
	}
}

func TestShuffleDictionaryMixedPartitionsDoNotRetainCopyScratch(t *testing.T) {
	for _, shape := range []struct {
		name        string
		typ         types.Type
		holders     int
		capacity    uint64
		recycle     bool
		unaccounted bool
	}{
		{name: "int64", typ: types.T_int64.ToType(), holders: 8, capacity: 128 << 10},
		{name: "narrow int32", typ: types.T_int32.ToType(), holders: 16, capacity: 68 << 10},
		{name: "recycled int64", typ: types.T_int64.ToType(), holders: 8, capacity: 128 << 10, recycle: true},
		{name: "unaccounted finite pool", typ: types.T_int64.ToType(), holders: 8, capacity: 1 << 20, unaccounted: true},
	} {
		for _, useViews := range []bool{false, true} {
			t.Run(shape.name+"/"+map[bool]string{false: "ordinary", true: "views"}[useViews], func(t *testing.T) {
				var pool *mpool.MPool
				if shape.unaccounted {
					var err error
					pool, err = mpool.NewMPool(t.Name(), int64(shape.capacity), mpool.NoFixed)
					require.NoError(t, err)
				} else {
					pool = mpool.MustNewZero()
				}
				proc := testutil.NewProcessWithOwnedMPool(t, "", pool)
				t.Cleanup(proc.Free)
				registry, err := mpool.NewAllocationAccountRegistry(1, 128)
				require.NoError(t, err)
				account, err := registry.Open(shape.capacity)
				require.NoError(t, err)
				t.Cleanup(func() {
					require.Zero(t, account.Snapshot().Used)
					_, _, err := registry.CompleteTerminal(account)
					require.NoError(t, err)
				})
				allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2)
				require.NoError(t, err)
				input := batch.NewWithSchema(true, nil, []types.Type{shape.typ})
				t.Cleanup(func() { input.Clean(proc.Mp()) })
				if !shape.unaccounted {
					require.NoError(t, input.SetAllocationAccount(allocation))
				}
				require.NoError(t, input.Vecs[0].PreExtend(objectio.BlockMaxRows, proc.Mp()))
				for row := range objectio.BlockMaxRows {
					if shape.typ.Oid == types.T_int32 {
						require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(row), false, proc.Mp()))
					} else {
						require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(row), false, proc.Mp()))
					}
				}
				input.SetRowCount(objectio.BlockMaxRows)
				expected := uint64(2 * objectio.BlockMaxRows * shape.typ.TypeSize())
				liveBytes := func() uint64 { return account.Snapshot().Used }
				if shape.unaccounted {
					filler, err := proc.Mp().Alloc(int(proc.Mp().Cap()-proc.Mp().CurrNB()-int64(expected/2)), true)
					require.NoError(t, err)
					t.Cleanup(func() { proc.Mp().Free(filler) })
					expected = shape.capacity
					liveBytes = func() uint64 { return uint64(proc.Mp().CurrNB()) }
				}
				fullRows := objectio.BlockMaxRows * shuffleProducerTailBlocksPerBucket / shape.holders
				shortRows := fullRows / 2
				buckets := 1 + (objectio.BlockMaxRows-fullRows)/shortRows
				sp := NewShufflePool(int32(buckets), int32(shape.holders), true)
				for range shape.holders {
					require.True(t, sp.hold())
					t.Cleanup(func() { sp.release(proc.Mp(), true) })
				}
				// Another producer generation selected the shared lane's provenance.
				seed := batch.NewWithSchema(true, nil, []types.Type{types.T_int64.ToType()})
				t.Cleanup(func() { seed.Clean(proc.Mp()) })
				if shape.unaccounted {
					// Select a different shared-lane policy; this unaccounted input
					// must exercise its smaller producer-local full partition.
					require.NoError(t, seed.SetAllocationAccount(allocation))
				}
				require.True(t, sp.matchesSharedLane(seed))
				require.Equal(t, fullRows, sp.producerBatchMaxRows)
				if shape.recycle {
					reuse := batch.NewWithSchema(true, nil, []types.Type{shape.typ})
					require.NoError(t, reuse.SetAllocationAccount(allocation))
					require.NoError(t, reuse.Vecs[0].PreExtend(fullRows, proc.Mp()))
					sp.putBatchToPool(reuse, proc.Mp())
				}
				sels := make([][]int32, buckets)
				for row := range objectio.BlockMaxRows {
					bucket := 0
					if row >= fullRows {
						bucket = 1 + (row-fullRows)/shortRows
					}
					sels[bucket] = append(sels[bucket], int32(row))
				}
				var views *shufflePartitionViews
				t.Cleanup(func() {
					if views != nil {
						views.free(proc.Mp())
					}
				})
				var workspace **shufflePartitionViews
				if useViews {
					workspace = &views
				}
				_, _, _, done, err := sp.tryWriteModeWithViews(input, sels, 0, 0, 0, false, workspace, proc)
				require.NoError(t, err)
				require.True(t, done)
				if useViews && !shape.unaccounted {
					require.Nil(t, views)
				}
				require.Equal(t, expected, liveBytes())
				// Keep the queue live while the upstream producer resets and
				// refills its input. A retained partition must not pin capacity
				// which the ordinary path would reuse for the next generation.
				if views != nil {
					views.free(proc.Mp())
					views = nil
				}
				input.CleanOnlyData()
				require.NoError(t, input.Vecs[0].PreExtend(objectio.BlockMaxRows, proc.Mp()))
				for row := range objectio.BlockMaxRows {
					if shape.typ.Oid == types.T_int32 {
						require.NoError(t, vector.AppendFixed(input.Vecs[0], -int32(row+1), false, proc.Mp()))
					} else {
						require.NoError(t, vector.AppendFixed(input.Vecs[0], -int64(row+1), false, proc.Mp()))
					}
				}
				require.Equal(t, expected, liveBytes())
				full := sp.getAnyFullBatch()
				require.NotNil(t, full)
				t.Cleanup(func() { sp.discardBatch(full, proc.Mp()) })
				require.False(t, full.HasDictionary())
				valueAt := func(v *vector.Vector, row int) int64 {
					if shape.typ.Oid == types.T_int32 {
						return int64(vector.GetFixedAtNoTypeCheck[int32](v, row))
					}
					return vector.GetFixedAtNoTypeCheck[int64](v, row)
				}
				got := make([]int64, 0, objectio.BlockMaxRows)
				for row := range full.RowCount() {
					got = append(got, valueAt(full.Vecs[0], row))
				}
				for range shape.holders {
					sp.stopWriting()
				}
				for tail := sp.getAnyLastBatch(); tail != nil; tail = sp.getAnyLastBatch() {
					for row := range tail.RowCount() {
						got = append(got, valueAt(tail.Vecs[0], row))
					}
					sp.discardBatch(tail, proc.Mp())
				}
				require.Equal(t, objectio.BlockMaxRows, len(got))
				for row, value := range got {
					require.Equal(t, int64(row), value)
				}
			})
		}
	}
}

func TestShuffleDictionaryRetentionCostMetadataAndComposition(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		dictionary, nullable, dense bool
		pressure                    string
	}{
		{name: "wide flat"}, {name: "composed dictionary", dictionary: true}, {name: "nullable metadata", nullable: true},
		{name: "composed metadata capacity", dictionary: true, pressure: "metadata"},
		{name: "composed pool capacity", dictionary: true, pressure: "pool"},
		{name: "unaccounted dense dictionary", dictionary: true, dense: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var pool *mpool.MPool
			if tc.pressure == "pool" {
				var err error
				pool, err = mpool.NewMPool(t.Name(), 1<<20, mpool.NoFixed)
				require.NoError(t, err)
			} else {
				pool = mpool.MustNewZero()
			}
			proc := testutil.NewProcessWithOwnedMPool(t, "", pool)
			t.Cleanup(proc.Free)
			slots := uint64(128)
			if tc.pressure == "metadata" {
				slots = 4 // The source owns two allocations; an owned gather needs one more.
			}
			registry, err := mpool.NewAllocationAccountRegistry(1, slots)
			require.NoError(t, err)
			account, err := registry.Open(1 << 20)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.Zero(t, account.Snapshot().Used)
				_, _, err := registry.CompleteTerminal(account)
				require.NoError(t, err)
			})
			allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2)
			require.NoError(t, err)
			if tc.dense {
				allocation = nil
			}
			typ := types.T_decimal128.ToType()
			if tc.nullable {
				typ = types.T_int64.ToType()
			}
			input := batch.NewWithSchema(true, nil, []types.Type{typ})
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			require.NoError(t, input.SetAllocationAccount(allocation))
			if tc.nullable {
				require.NoError(t, vector.AppendFixedList(input.Vecs[0], []int64{1, 2, 3, 4}, []bool{true, false, false, false}, proc.Mp()))
			} else {
				values := []types.Decimal128{{B0_63: 1}, {B0_63: 2}, {B0_63: 3}, {B0_63: 4}}
				if tc.dense {
					values = make([]types.Decimal128, 16)
					for row := range values {
						values[row].B0_63 = uint64(row + 1)
					}
				}
				require.NoError(t, vector.AppendFixedList(input.Vecs[0], values, nil, proc.Mp()))
			}
			input.SetRowCount(input.Vecs[0].Length())
			if tc.dictionary {
				selected := make([]*vector.Vector, 1)
				mapping := []int64{3, 1, 2, 0, 3, 1, 2, 0, 3, 1, 2, 0, 3, 1, 2, 0}
				if tc.dense {
					for row := range mapping {
						mapping[row] = int64(row)
					}
				}
				admitted, err := vector.TrySelectDictionaryVectors(input.Vecs, selected, mapping, input.RowCount(), proc.Mp(), allocation)
				require.NoError(t, err)
				require.True(t, admitted)
				input.Vecs[0].Free(proc.Mp())
				input.Vecs = selected
				input.SetRowCount(16)
			}
			var views *shufflePartitionViews
			t.Cleanup(func() {
				if views != nil {
					views.free(proc.Mp())
				}
			})
			before := account.Snapshot().Used
			beforePool := proc.Mp().CurrNB()
			sp := NewShufflePool(1, 1, true)
			rows := []int32{0, 2}
			if tc.dictionary {
				rows = []int32{0, 2, 4, 6, 8, 10, 12, 14}
			}
			if tc.pressure == "pool" {
				// The ordinary 128-byte gather fits; three concurrent 64-byte
				// maps do not. Keep pressure private to this process's pool.
				filler, err := proc.Mp().Alloc(int(proc.Mp().Cap()-proc.Mp().CurrNB()-128), true)
				require.NoError(t, err)
				t.Cleanup(func() { proc.Mp().Free(filler) })
			}
			output, err := sp.selectPartition(input, rows, &views, proc)
			require.NoError(t, err)
			if tc.pressure != "" {
				require.Nil(t, output)
				require.Equal(t, before, account.Snapshot().Used)
				output = batch.NewWithSchema(true, nil, []types.Type{typ})
				t.Cleanup(func() { output.Clean(proc.Mp()) })
				require.NoError(t, output.SetAllocationAccount(allocation))
				require.NoError(t, output.Vecs[0].Union(input.Vecs[0], []int64{0, 2, 4, 6, 8, 10, 12, 14}, proc.Mp()))
				for row := range 8 {
					require.Equal(t, types.Decimal128{B0_63: uint64(4 - row%2)}, vector.GetFixedAtNoTypeCheck[types.Decimal128](output.Vecs[0], row))
				}
				return
			}
			if !tc.dictionary || tc.dense {
				require.Nil(t, output)
				require.Equal(t, before, account.Snapshot().Used, "rejected metadata must release all unpublished capacity")
				require.Equal(t, beforePool, proc.Mp().CurrNB())
				return
			}
			require.NotNil(t, output)
			t.Cleanup(func() { output.Clean(proc.Mp()) })
			require.True(t, output.HasDictionary())
			views.free(proc.Mp())
			views = nil
			want := uint64(1)
			if tc.dictionary {
				want = 4
			}
			require.Equal(t, types.Decimal128{B0_63: want}, vector.GetFixedAtNoTypeCheck[types.Decimal128](output.Vecs[0], 0))
			require.Equal(t, types.Decimal128{B0_63: 3}, vector.GetFixedAtNoTypeCheck[types.Decimal128](output.Vecs[0], 1))
		})
	}
}

func TestShuffleDictionaryConcurrentRelease(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "full partitions", true: "full and short partitions"}[mixed], func(t *testing.T) {
			testShuffleDictionaryConcurrentRelease(t, mixed)
		})
	}
}

func testShuffleDictionaryConcurrentRelease(t *testing.T, mixed bool) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	buckets := 2
	if mixed {
		buckets = 3
	}
	sp := NewShufflePool(int32(buckets), 2, true)
	require.True(t, sp.hold())
	require.True(t, sp.hold())
	t.Cleanup(func() { sp.release(proc.Mp(), true); sp.release(proc.Mp(), true) })
	inputs := make([]*batch.Batch, 2)
	selections := make([][]int32, buckets)
	for row := range 2 * objectio.BlockMaxRows {
		bucket := row % 2
		if mixed && bucket == 1 && row >= objectio.BlockMaxRows {
			bucket = 2
		}
		selections[bucket] = append(selections[bucket], int32(row))
	}
	for producer := range inputs {
		inputs[producer] = newShuffleDictionaryInput(t, proc.Mp(), producer)
	}
	start, stop := make(chan struct{}), make(chan struct{})
	errors := make(chan error, len(inputs))
	var writers sync.WaitGroup
	t.Cleanup(func() { close(stop); writers.Wait() })
	for producer, input := range inputs {
		writers.Add(1)
		go func() {
			defer writers.Done()
			defer sp.stopWriting()
			var views *shufflePartitionViews
			defer func() {
				if views != nil {
					views.free(proc.Mp())
				}
			}()
			<-start
			position, offset := 0, 0
			for {
				next, end, wait, done, err := sp.tryWriteModeWithViews(input, selections, int32(producer), position, offset, false, &views, proc)
				if err != nil {
					errors <- err
					return
				}
				if done {
					// Reuse while another goroutine may read/release the outputs.
					input.CleanOnlyData()
					errors <- vector.AppendFixed(input.Vecs[0], types.Decimal128{B0_63: ^uint64(0)}, false, proc.Mp())
					return
				}
				position, offset = next, end
				select {
				case <-wait:
				case <-stop:
					return
				}
			}
		}()
	}
	close(start)
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	outputs := 0
	dictionaryOutputs := 0
	for {
		output := sp.getAnyFullBatch()
		if output == nil && sp.allStop() {
			output = sp.getAnyStoppedBatch()
			if output == nil {
				break
			}
		}
		if output == nil {
			select {
			case <-sp.anyBatchWaiter:
			case <-sp.endingWaiter:
			case <-deadline.C:
				t.Fatal("concurrent dictionary shuffle did not terminate")
			}
			continue
		}
		// Own cleanup before checking a consumed output.
		func() {
			defer sp.discardBatch(output, proc.Mp())
			if !mixed || output.HasDictionary() {
				require.True(t, output.HasDictionary())
				require.Equal(t, objectio.BlockMaxRows, output.RowCount())
			} else {
				// Another producer may reuse an owned buffer returned by the
				// consumer. Both the full and short owned sizes are valid.
				require.Contains(t, []int{objectio.BlockMaxRows, objectio.BlockMaxRows / 2}, output.RowCount())
			}
			if output.HasDictionary() {
				dictionaryOutputs++
			}
			first := vector.GetFixedAtNoTypeCheck[types.Decimal128](output.Vecs[0], 0)
			require.Contains(t, []uint64{1, 100001}, first.B0_63)
			for row := range output.RowCount() {
				want := types.Decimal128{B0_63: first.B0_63 + uint64(2*(row%2))}
				if got := vector.GetFixedAtNoTypeCheck[types.Decimal128](output.Vecs[0], row); got != want {
					require.Equal(t, want, got)
				}
			}
		}()
		outputs++
	}
	writers.Wait()
	require.Equal(t, 2*buckets, outputs)
	require.Positive(t, dictionaryOutputs)
	for range inputs {
		require.NoError(t, <-errors)
	}
}
