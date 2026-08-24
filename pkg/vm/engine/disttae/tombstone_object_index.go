// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"slices"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

type tombstoneObjectRange struct {
	min     types.Blockid
	max     types.Blockid
	maxSeen types.Blockid
	ordinal int
}

// tombstoneObjectIndex preserves the original object order in objects while
// indexing initialized Rowid zone maps by their covered block-id interval.
// Objects without an indexable zone map remain unconditional candidates.
type tombstoneObjectIndex struct {
	objects   []objectio.ObjectStats
	ranges    []tombstoneObjectRange
	unindexed []int
}

func newTombstoneObjectIndex(objects []objectio.ObjectStats) tombstoneObjectIndex {
	idx := tombstoneObjectIndex{
		objects: objects,
		ranges:  make([]tombstoneObjectRange, 0, len(objects)),
	}
	for ordinal := range objects {
		minBlock, maxBlock, ok := tombstoneObjectBlockRange(&objects[ordinal])
		if !ok {
			idx.unindexed = append(idx.unindexed, ordinal)
			continue
		}
		idx.ranges = append(idx.ranges, tombstoneObjectRange{
			min:     minBlock,
			max:     maxBlock,
			ordinal: ordinal,
		})
	}
	slices.SortFunc(idx.ranges, func(a, b tombstoneObjectRange) int {
		if cmp := a.min.Compare(&b.min); cmp != 0 {
			return cmp
		}
		return a.max.Compare(&b.max)
	})
	var maxSeen types.Blockid
	for pos := range idx.ranges {
		if pos == 0 || idx.ranges[pos].max.GT(&maxSeen) {
			maxSeen = idx.ranges[pos].max
		}
		idx.ranges[pos].maxSeen = maxSeen
	}
	return idx
}

func tombstoneObjectBlockRange(
	stats *objectio.ObjectStats,
) (minBlock types.Blockid, maxBlock types.Blockid, ok bool) {
	zm := stats.SortKeyZoneMap()
	if !zm.IsInited() || zm.GetType() != types.T_Rowid {
		return minBlock, maxBlock, false
	}
	var minRow, maxRow types.Rowid
	copy(minRow[:], zm.GetMinBuf())
	copy(maxRow[:], zm.GetMaxBuf())
	return minRow.CloneBlockID(), maxRow.CloneBlockID(), true
}

// selectCandidates appends object ordinals whose object-level Rowid zone maps
// may cover block. Returned ordinals follow the original visible-object order.
func (idx *tombstoneObjectIndex) selectCandidates(
	block *types.Blockid,
	dst []int,
) []int {
	dst = append(dst[:0], idx.unindexed...)
	end := sort.Search(len(idx.ranges), func(pos int) bool {
		return idx.ranges[pos].min.GT(block)
	})
	start := sort.Search(end, func(pos int) bool {
		return !idx.ranges[pos].maxSeen.LT(block)
	})
	for pos := start; pos < end; pos++ {
		if !idx.ranges[pos].max.LT(block) {
			dst = append(dst, idx.ranges[pos].ordinal)
		}
	}
	slices.Sort(dst)
	return dst
}

type indexedObjectStatsIter struct {
	index      *tombstoneObjectIndex
	candidates []int
	pos        int
}

func (i *indexedObjectStatsIter) next() (*objectio.ObjectStats, error) {
	if i.pos >= len(i.candidates) {
		return nil, nil
	}
	ordinal := i.candidates[i.pos]
	i.pos++
	return &i.index.objects[ordinal], nil
}
