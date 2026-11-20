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

package shuffle

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Small producer-local tails make ready-credit exhaustion deterministic without
// a large fixture. A distinct source account keeps this lane separate from the
// pool's initially admitted unaccounted provenance.
func smallShuffleRetryFixture(t *testing.T) (*process.Process, *ShufflePool, *batch.Batch) {
	t.Helper()
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	t.Cleanup(func() {
		defer proc.Free()
		require.Zero(t, mp.CurrNB())
	})
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<16)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	t.Cleanup(func() {
		stats := account.Seal()
		_, err := registry.Finalize(account)
		require.Zero(t, stats.Used)
		require.NoError(t, err)
	})
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2)
	require.NoError(t, err)
	input := batch.NewWithSchema(true, nil, []types.Type{types.T_int64.ToType()})
	t.Cleanup(func() { input.Clean(mp) })
	require.NoError(t, input.SetAllocationAccount(selection))
	require.NoError(t, vector.AppendFixedList(input.Vecs[0], []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}, nil, mp))
	input.SetRowCount(14)
	sp := NewShufflePool(3, 2, false)
	t.Cleanup(func() { sp.abort(mp) })
	unaccounted := batch.NewWithSchema(true, nil, []types.Type{types.T_int64.ToType()})
	t.Cleanup(func() { unaccounted.Clean(mp) })
	require.True(t, sp.matchesSharedLane(unaccounted))
	sp.producerBatchMaxRows = 2
	for _, lanes := range sp.batchSets {
		for lane := range lanes {
			lanes[lane] = batch.NewBatchSet(2)
		}
	}
	return proc, sp, input
}

func TestShufflePoolRotatedMixedResume(t *testing.T) {
	for _, tc := range []struct {
		name   string
		closed int32
	}{
		{name: "open", closed: -1},
		{name: "closed-first", closed: 1},
		{name: "closed-middle", closed: 2},
		{name: "closed-wrapped", closed: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc, sp, input := smallShuffleRetryFixture(t)
			if tc.closed >= 0 {
				sp.closeConsumer(tc.closed, proc.Mp())
			}
			sels := [][]int32{{13, 12, 11, 10, 9, 8}, {5, 1, 4, 0, 3, 2}, {7, 6}}
			got := make([][]int64, 3)
			consume := func(bucket int32, output *batch.Batch) {
				t.Helper()
				require.NotNil(t, output)
				got[bucket] = append(got[bucket], vector.MustFixedColNoTypeCheck[int64](output.Vecs[0])...)
				sp.discardBatch(output, proc.Mp())
			}
			position, offset, waiter, done, err := sp.tryWrite(input, sels, 1, 0, 0, proc)
			require.NoError(t, err)
			var blockedPositions []int
			for !done {
				blockedPositions = append(blockedPositions, position)
				require.Contains(t, []int{0, 2}, position)
				require.Equal(t, 4, offset)
				require.NotNil(t, waiter)
				physical := int32((1 + position) % 3)
				consume(physical, sp.getFullBatch(physical))
				select {
				case <-waiter:
				default:
					t.Fatal("draining the physical destination did not release credit")
				}
				position, offset, waiter, done, err = sp.tryWrite(input, sels, 1, position, offset, proc)
				require.NoError(t, err)
			}
			wantBlocked := []int{0, 2}
			if tc.closed == 1 {
				wantBlocked = []int{2}
			} else if tc.closed == 0 {
				wantBlocked = []int{0}
			}
			require.Equal(t, wantBlocked, blockedPositions)
			require.True(t, done)
			require.Equal(t, 3, position)
			require.Zero(t, offset)
			require.Nil(t, waiter)
			for bucket := int32(0); bucket < 3; bucket++ {
				for output := sp.getFullBatch(bucket); output != nil; output = sp.getFullBatch(bucket) {
					consume(bucket, output)
				}
				if output := sp.getLastBatch(bucket); output != nil {
					consume(bucket, output)
				}
				var want []int64
				if bucket != tc.closed {
					for _, row := range sels[bucket] {
						want = append(want, int64(row))
					}
				}
				require.Equal(t, want, got[bucket])
			}
		})
	}
}

func TestShufflePoolWholeResumeKeepsPhysicalBucket(t *testing.T) {
	proc, sp, input := smallShuffleRetryFixture(t)
	var got []int64
	consume := func(output *batch.Batch) {
		t.Helper()
		require.NotNil(t, output)
		require.EqualValues(t, 2, output.ShuffleIDX)
		got = append(got, vector.MustFixedColNoTypeCheck[int64](output.Vecs[0])...)
		sp.discardBatch(output, proc.Mp())
	}
	position, offset, waiter, done, err := sp.tryWriteMode(input, nil, 1, 2, 0, true, proc)
	require.NoError(t, err)
	for !done {
		require.Equal(t, 2, position)
		require.NotNil(t, waiter)
		consume(sp.getFullBatch(2))
		position, offset, waiter, done, err = sp.tryWriteMode(input, nil, 1, position, offset, true, proc)
		require.NoError(t, err)
	}
	for output := sp.getFullBatch(2); output != nil; output = sp.getFullBatch(2) {
		consume(output)
	}
	if output := sp.getLastBatch(2); output != nil {
		consume(output)
	}
	require.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}, got)
	for bucket := int32(0); bucket < 2; bucket++ {
		require.Nil(t, sp.getFullBatch(bucket))
		require.Nil(t, sp.getLastBatch(bucket))
	}
}
