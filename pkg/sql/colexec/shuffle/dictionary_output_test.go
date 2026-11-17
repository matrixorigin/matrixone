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
	testShuffleDictionaryPartitions(t, 8<<20, true)
}

func TestShuffleDictionaryCapacityRefusalReleasesScratch(t *testing.T) {
	// Source plus two ordinary int64 partitions fit exactly. Optional selector
	// scratch must not prevent the second destination from using that capacity.
	testShuffleDictionaryPartitions(t, 4*objectio.BlockMaxRows*8, false)
}

func testShuffleDictionaryPartitions(t *testing.T, capacity uint64, secondView bool) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err := registry.Open(capacity)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Snapshot().Used)
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	})
	allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
	require.NoError(t, err)
	input := batch.NewOffHeapWithSize(1)
	require.NoError(t, input.SetAllocationAccount(allocation))
	input.Vecs[0], err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_int64.ToType(), allocation)
	require.NoError(t, err)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	values := make([]int64, 2*objectio.BlockMaxRows)
	selections := make([][]int32, 2)
	for row := range values {
		values[row] = int64(row)
		selections[row%2] = append(selections[row%2], int32(row))
	}
	require.NoError(t, vector.AppendFixedList(input.Vecs[0], values, nil, proc.Mp()))
	input.SetRowCount(len(values))
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
	require.NotNil(t, views)
	views.free(proc.Mp())
	views = nil
	if secondView {
		input.CleanOnlyData()
		// Reusing the producer must not mutate the queued immutable snapshot.
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(-1), false, proc.Mp()))
	}
	for bucket := range 2 {
		output := sp.getAnyFullBatch()
		require.NotNil(t, output)
		t.Cleanup(func() { sp.discardBatch(output, proc.Mp()) })
		require.Equal(t, bucket == 0 || secondView, output.HasDictionary())
		require.Equal(t, objectio.BlockMaxRows, output.RowCount())
		for row := range output.RowCount() {
			require.Equal(t, int64(2*row+bucket), vector.GetFixedAtNoTypeCheck[int64](output.Vecs[0], row))
		}
	}
	require.Nil(t, sp.getAnyFullBatch())
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

func TestShuffleDictionaryConcurrentRelease(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	sp := NewShufflePool(2, 2, true)
	require.True(t, sp.hold())
	require.True(t, sp.hold())
	t.Cleanup(func() { sp.release(proc.Mp(), true); sp.release(proc.Mp(), true) })
	inputs := make([]*batch.Batch, 2)
	selections := make([][]int32, 2)
	for row := range 2 * objectio.BlockMaxRows {
		selections[row%2] = append(selections[row%2], int32(row))
	}
	for producer := range inputs {
		input := batch.NewWithSize(1)
		input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		values := make([]int64, 2*objectio.BlockMaxRows)
		for row := range values {
			values[row] = int64(producer*100000 + row)
		}
		require.NoError(t, vector.AppendFixedList(input.Vecs[0], values, nil, proc.Mp()))
		input.SetRowCount(len(values))
		inputs[producer] = input
		t.Cleanup(func() { input.Clean(proc.Mp()) })
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
					errors <- vector.AppendFixed(input.Vecs[0], int64(-1), false, proc.Mp())
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
			require.True(t, output.HasDictionary())
			require.Equal(t, objectio.BlockMaxRows, output.RowCount())
			first := vector.GetFixedAtNoTypeCheck[int64](output.Vecs[0], 0)
			require.NotEqual(t, int64(-1), first)
			require.Equal(t, first+2*(objectio.BlockMaxRows-1), vector.GetFixedAtNoTypeCheck[int64](output.Vecs[0], output.RowCount()-1))
		}()
		outputs++
	}
	writers.Wait()
	require.Equal(t, 4, outputs)
	for range inputs {
		require.NoError(t, <-errors)
	}
}
