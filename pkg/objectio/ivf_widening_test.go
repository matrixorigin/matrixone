// Copyright 2024 Matrix Origin
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

package objectio

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// Tests the batched-approximate widening stop on the real IndexReaderTopOp.
// Rule: stop iff the heap holds K AND the next block's nearest centroid rank is
// at/beyond the base nprobe batch. K=3 here (heap "full" at 3 entries).
func TestIndexReaderTopOp_ShouldStopWidening(t *testing.T) {
	newOp := func(heap []float64, nprobe int) *IndexReaderTopOp {
		return &IndexReaderTopOp{Limit: 3, DistHeap: Float64Heap(heap), Nprobe: nprobe}
	}
	heapFull := []float64{10, 4, 7} // 3 entries == K
	heapPartial := []float64{10, 4} // < K

	t.Run("heap not full -> keep widening even past nprobe", func(t *testing.T) {
		op := newOp(heapPartial, 5)
		require.False(t, op.ShouldStopWidening(100)) // selective filter: widen on
	})

	t.Run("full but still inside nprobe batch -> keep probing the batch", func(t *testing.T) {
		op := newOp(heapFull, 5)
		require.False(t, op.ShouldStopWidening(4)) // nextBlockRank 4 < nprobe 5
	})

	t.Run("full and next block beyond nprobe -> stop", func(t *testing.T) {
		op := newOp(heapFull, 5)
		require.True(t, op.ShouldStopWidening(5)) // covered nearest nprobe, have K
		require.True(t, op.ShouldStopWidening(40))
	})
}

func TestIndexReaderTopOp_WideningEnabled(t *testing.T) {
	require.False(t, (*IndexReaderTopOp)(nil).WideningEnabled())
	require.False(t, (&IndexReaderTopOp{}).WideningEnabled())
	require.True(t, (&IndexReaderTopOp{RankedCentroids: []int64{1}}).WideningEnabled())
}

// Ordering entries blocks by BlockBestRank yields a nearest-centroid-first walk.
func TestIvfWideningParam_BlockBestRank(t *testing.T) {
	// query ranks centroids: 5 (nearest), 2, 8, 1 (farthest)
	p := &IvfWideningParam{RankedCentroids: []int64{5, 2, 8, 1}}
	rank := p.CentroidRank()
	require.Equal(t, map[int64]int{5: 0, 2: 1, 8: 2, 1: 3}, rank)

	// A block's centroid_id zonemap is an inclusive [min,max] range (entries are
	// clustered by centroid_id). Best rank = nearest centroid the block covers.
	require.Equal(t, 0, p.BlockBestRank(4, 6, rank))  // covers centroid 5 -> rank 0
	require.Equal(t, 1, p.BlockBestRank(1, 2, rank))  // covers 1,2 -> best rank(2)=1
	require.Equal(t, 2, p.BlockBestRank(7, 9, rank))  // covers 8 -> rank 2
	require.Equal(t, 4, p.BlockBestRank(10, 20, rank)) // none ranked -> sorts last

	// Sorting blocks by BlockBestRank == rank order.
	type blk struct{ min, max int64 }
	blocks := []blk{{7, 9}, {1, 2}, {4, 6}} // physical (centroid_id) order
	sort.SliceStable(blocks, func(i, j int) bool {
		return p.BlockBestRank(blocks[i].min, blocks[i].max, rank) <
			p.BlockBestRank(blocks[j].min, blocks[j].max, rank)
	})
	require.Equal(t, []blk{{4, 6}, {1, 2}, {7, 9}}, blocks) // nearest-centroid-first
}

// The O(log L) CentroidRankIndex must return identical results to the naive
// O(L) BlockBestRank for every range -- exhaustively cross-checked.
func TestCentroidRankIndex_MatchesNaive(t *testing.T) {
	p := &IvfWideningParam{RankedCentroids: []int64{7, 4, 6, 3, 1, 8, 2, 5, 12, 9}}
	rank := p.CentroidRank()
	idx := p.NewCentroidRankIndex()
	require.NotNil(t, idx)
	for lo := int64(-2); lo <= 15; lo++ {
		for hi := lo; hi <= 15; hi++ {
			require.Equalf(t, p.BlockBestRank(lo, hi, rank), idx.BestRank(lo, hi),
				"range [%d,%d]", lo, hi)
		}
	}
	// empty param -> nil index
	require.Nil(t, (&IvfWideningParam{}).NewCentroidRankIndex())
}
